//! Immich Sync Manager server.

mod activity;
mod activity_blockers;
mod admission;
mod attention;
mod auth;
mod config;
mod crypto;
mod db;
mod error;
mod files;
mod immich;
mod inbox_evidence;
mod local_evidence;
mod maintenance;
mod ops;
mod peer_evidence;
mod peers;
mod queue_evidence;
mod routes;
mod settings;
mod state;
mod watch;
mod worker_evidence;

use std::time::Duration;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::crypto::SecretBox;
use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_env("ISM_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let config = Config::from_env()?;
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("cannot create {}", config.data_dir.display()))?;
    let pool = db::connect(&config.data_dir).await?;
    let secrets =
        SecretBox::open_or_create(&config.data_dir).context("cannot open the key file")?;
    let listen = config.listen;
    let state = AppState::new(config, pool, secrets).await?;

    activity::recover(&state)
        .await
        .context("activity restart recovery failed")?;
    activity::spawn(
        &state,
        "Activity recorder",
        worker_evidence::Kind::Other,
        activity::run(state.clone()),
    );

    maintenance::recover_after_restart(&state)
        .await
        .context("restart recovery failed")?;
    peers::transfer::recover_after_restart(&state)
        .await
        .context("peer transfer restart recovery failed")?;
    activity::spawn(
        &state,
        "Maintenance controller",
        worker_evidence::Kind::Other,
        maintenance::run(state.clone()),
    );
    activity::spawn(
        &state,
        "Operation executor",
        worker_evidence::Kind::Operations,
        ops::exec::run(state.clone()),
    );
    activity::spawn(
        &state,
        "Peer workers",
        worker_evidence::Kind::PeerWorkers,
        peers::run(state.clone()),
    );
    activity::spawn(
        &state,
        "Share scanner",
        worker_evidence::Kind::Other,
        peers::scan_worker(state.clone()),
    );
    activity::spawn(
        &state,
        "Share content verifier",
        worker_evidence::Kind::Other,
        peers::verify_worker(state.clone()),
    );
    activity::spawn(
        &state,
        "Transfer dispatcher",
        worker_evidence::Kind::Transfers,
        peers::transfer::run(state.clone()),
    );
    activity::spawn(
        &state,
        "Recovery worker",
        worker_evidence::Kind::Recovery,
        peers::recovery::run(state.clone()),
    );
    activity::spawn(
        &state,
        "File watcher",
        worker_evidence::Kind::Other,
        watch::run(state.clone()),
    );
    activity::spawn(
        &state,
        "Library scheduler",
        worker_evidence::Kind::Other,
        watch::scheduler::run(state.clone()),
    );
    let janitor = state.clone();
    activity::spawn(
        &state,
        "Upload janitor",
        worker_evidence::Kind::Other,
        async move {
            loop {
                ops::uploads::expire(&janitor).await;
                tokio::time::sleep(Duration::from_secs(600)).await;
            }
        },
    );

    let app = routes::router(state.clone());
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    tracing::info!(
        "Immich Sync Manager {} listening on {listen}",
        env!("CARGO_PKG_VERSION")
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    state.db.close().await;
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = term => {}
    }
    tracing::info!("shutting down");
}
