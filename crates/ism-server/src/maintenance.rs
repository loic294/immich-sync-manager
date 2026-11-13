//! The Immich maintenance barrier.
//!
//! Before ISM touches files of an external library, Immich must not react to them: file watching
//! and periodic scans are turned off, then the queues that read library files must drain. Work is
//! then admitted. After 15 minutes without new work, ISM scans the affected libraries, waits for
//! the scan to finish and restores exactly the settings it changed. Any failure to verify the
//! barrier keeps writes refused (fail closed).

use std::time::Duration;

use chrono::Utc;
use ism_core::maintenance::{Admission, MaintenanceState, Phase, SavedSettings, restore_value};
use serde_json::Value;

use crate::error::{AppError, AppResult};
use crate::immich::INTERFERING_QUEUES;
use crate::immich::api::{self, Api, SCAN_ENABLED, WATCH_ENABLED};
use crate::immich::registry::{self, Server};
use crate::state::AppState;

async fn update<T>(
    state: &AppState,
    server_id: &str,
    f: impl FnOnce(&mut MaintenanceState) -> T,
) -> AppResult<(T, MaintenanceState)> {
    let _guard = state.maintenance_lock.lock().await;
    let server = registry::server(state, server_id).await?;
    let mut m = server.maintenance;
    let out = f(&mut m);
    registry::save_maintenance(state, server_id, &m).await?;
    Ok((out, m))
}

/// Waits until the server admits work on `library_ids`, starting an epoch if needed.
pub async fn acquire(state: &AppState, server_id: &str, library_ids: &[String]) -> AppResult<()> {
    let deadline = tokio::time::Instant::now() + state.config.timing.admission_timeout;
    loop {
        let now = Utc::now();
        let (admission, _) = update(state, server_id, |m| {
            let a = m.admission();
            match a {
                Admission::Admit => m.admit(now, library_ids),
                Admission::Prepare => m.begin_prepare(),
                _ => {}
            }
            a
        })
        .await?;
        match admission {
            Admission::Admit => return Ok(()),
            Admission::Deny(message) => {
                return Err(AppError::conflict("maintenance_blocked", message));
            }
            Admission::Prepare | Admission::Wait => {
                state.maintenance_wake.notify_waiters();
                if tokio::time::Instant::now() >= deadline {
                    return Err(AppError::conflict(
                        "maintenance_blocked",
                        "Immich did not become ready for changes in time",
                    ));
                }
                tokio::time::sleep(state.config.timing.poll).await;
            }
        }
    }
}

/// One admitted unit of work finished; the quiet timer restarts.
pub async fn release(state: &AppState, server_id: &str) -> AppResult<()> {
    update(state, server_id, |m| m.settle(Utc::now())).await?;
    Ok(())
}

/// Something changed on disk but its Immich side could not be completed.
pub async fn report_recovery(state: &AppState, server_id: &str, reason: &str) -> AppResult<()> {
    update(state, server_id, |m| {
        m.unresolved_recoveries += 1;
        m.require_recovery(reason);
    })
    .await?;
    Ok(())
}

/// Admin action: all recoveries were handled; the normal quiet period, scan and restore follow.
pub async fn resolve_recovery(state: &AppState, server_id: &str) -> AppResult<()> {
    update(state, server_id, |m| {
        m.unresolved_recoveries = m.unresolved_recoveries.saturating_sub(1);
        if m.unresolved_recoveries == 0 && m.phase == Phase::RecoveryRequired {
            m.phase = Phase::Active;
            m.active_ops = 0;
            m.message = None;
            m.settle(Utc::now());
        }
    })
    .await?;
    state.maintenance_wake.notify_waiters();
    Ok(())
}

/// Admin action after a blocked preparation: allow the next operation to try again.
pub async fn retry(state: &AppState, server_id: &str) -> AppResult<()> {
    let ((), m) = update(state, server_id, |m| {
        if m.phase == Phase::Blocked && m.saved == SavedSettings::default() {
            m.phase = Phase::Idle;
            m.message = None;
        } else if m.phase == Phase::Blocked {
            // Settings we changed could not be put back: restore them first.
            m.begin_restore();
        }
    })
    .await?;
    if m.phase == Phase::Blocked {
        return Err(AppError::conflict("maintenance_blocked", "cannot retry"));
    }
    state.maintenance_wake.notify_waiters();
    Ok(())
}

/// Startup: operations interrupted mid-flight may have left files and Immich out of step.
pub async fn recover_after_restart(state: &AppState) -> AppResult<()> {
    let interrupted: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, admitted_server_ids_json FROM operations WHERE status = 'running'",
    )
    .fetch_all(&state.db)
    .await?;
    for (op_id, servers) in &interrupted {
        sqlx::query(
            "UPDATE operations SET status = 'recovery_required', finished_at = ? WHERE id = ?",
        )
        .bind(crate::db::now())
        .bind(op_id)
        .execute(&state.db)
        .await?;
        sqlx::query("UPDATE operation_items SET status = 'recovery_required' WHERE operation_id = ? AND status = 'running'")
            .bind(op_id)
            .execute(&state.db)
            .await?;
        for server_id in crate::db::json_vec(servers) {
            update(state, &server_id, |m| {
                m.active_ops = 0;
                m.unresolved_recoveries += 1;
                m.require_recovery(format!("operation {op_id} was interrupted by a restart"));
            })
            .await?;
        }
        tracing::warn!(operation = %op_id, "operation interrupted by restart; recovery required");
    }
    // Admitted-but-idle epochs lose their counters on restart.
    for server in registry::servers(state).await? {
        if server.maintenance.phase == Phase::Active && server.maintenance.active_ops > 0 {
            update(state, &server.id, |m| {
                m.active_ops = 0;
                m.settle(Utc::now());
            })
            .await?;
        }
    }
    Ok(())
}

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        if let Err(e) = step(&state).await {
            tracing::warn!("maintenance: {}", e.message());
        }
        tokio::select! {
            () = tokio::time::sleep(tick) => {}
            () = state.maintenance_wake.notified() => {}
        }
    }
}

/// Advances every server's maintenance state by one phase.
pub async fn step(state: &AppState) -> AppResult<()> {
    for server in registry::servers(state).await? {
        let result = match server.maintenance.phase {
            Phase::Preparing => prepare(state, &server).await,
            Phase::Active if server.maintenance.ready_to_reconcile(Utc::now()) => {
                update(state, &server.id, MaintenanceState::begin_reconcile).await?;
                reconcile(state, &server).await
            }
            Phase::Reconciling => reconcile(state, &server).await,
            Phase::Restoring => restore(state, &server).await,
            _ => Ok(()),
        };
        if let Err(e) = result {
            tracing::warn!(server = %server.name, "maintenance step failed: {}", e.message());
        }
    }
    Ok(())
}

fn immich(e: api::ApiError) -> AppError {
    AppError::conflict("immich_error", crate::error::redact(&e.to_string()))
}

async fn queues_idle(api: &Api, names: &[&str]) -> Result<bool, AppError> {
    let queues = api.queues().await.map_err(immich)?;
    for name in names {
        let Some(q) = queues.iter().find(|q| q.name == *name) else {
            return Err(AppError::conflict(
                "maintenance_blocked",
                format!("Immich queue '{name}' not found"),
            ));
        };
        if !q.is_idle() {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn wait_idle(
    state: &AppState,
    api: &Api,
    names: &[&str],
    timeout: Duration,
) -> Result<bool, AppError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if queues_idle(api, names).await? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(state.config.timing.poll).await;
    }
}

/// Turns watchers and scans off and waits for Immich to go quiet. On any failure everything we
/// changed is put back and the server is blocked; no file has been touched yet.
async fn prepare(state: &AppState, server: &Server) -> AppResult<()> {
    let outcome = try_prepare(state, server).await;
    match outcome {
        Ok(saved) => {
            update(state, &server.id, |m| m.prepared(saved, Utc::now())).await?;
            tracing::info!(server = %server.name, "watchers paused; changes admitted");
            Ok(())
        }
        Err((saved, err)) => {
            let message = format!("could not pause Immich safely: {}", err.message());
            let restored = match registry::admin_api(state, server).await {
                Ok(api) => undo(&api, &saved).await.is_ok(),
                Err(_) => saved == SavedSettings::default(),
            };
            update(state, &server.id, |m| {
                m.saved = if restored {
                    SavedSettings::default()
                } else {
                    saved
                };
                m.block(message.clone());
            })
            .await?;
            Err(err)
        }
    }
}

async fn try_prepare(
    state: &AppState,
    server: &Server,
) -> Result<SavedSettings, (SavedSettings, AppError)> {
    let mut saved = SavedSettings::default();
    let api = registry::admin_api(state, server)
        .await
        .map_err(|e| (saved.clone(), e))?;
    let mut config = api
        .get_config()
        .await
        .map_err(|e| (saved.clone(), immich(e)))?;
    let watch = api::config_bool(&config, WATCH_ENABLED);
    let scan = api::config_bool(&config, SCAN_ENABLED);
    let (Some(watch), Some(scan)) = (watch, scan) else {
        return Err((
            saved,
            AppError::conflict("maintenance_blocked", "unexpected Immich config shape"),
        ));
    };
    if watch || scan {
        api::set_config_bool(&mut config, WATCH_ENABLED, false);
        api::set_config_bool(&mut config, SCAN_ENABLED, false);
        if watch {
            saved.watch_enabled = Some(true);
        }
        if scan {
            saved.scan_enabled = Some(true);
        }
        api.put_config(&config).await.map_err(|e| {
            // Nothing was written; nothing to undo.
            (SavedSettings::default(), immich(e))
        })?;
        let verify = api
            .get_config()
            .await
            .map_err(|e| (saved.clone(), immich(e)))?;
        if api::config_bool(&verify, WATCH_ENABLED) != Some(false)
            || api::config_bool(&verify, SCAN_ENABLED) != Some(false)
        {
            return Err((
                saved,
                AppError::conflict(
                    "maintenance_blocked",
                    "Immich did not apply the watcher change",
                ),
            ));
        }
    }
    match wait_idle(
        state,
        &api,
        INTERFERING_QUEUES,
        state.config.timing.quiesce_timeout,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err((
                saved,
                AppError::conflict("maintenance_blocked", "Immich queues did not drain in time"),
            ));
        }
        Err(e) => return Err((saved, e)),
    }
    api.set_queue_paused("library", true)
        .await
        .map_err(|e| (saved.clone(), immich(e)))?;
    saved.paused_queues.push("library".into());
    tokio::time::sleep(state.config.timing.settle).await;
    match queues_idle(&api, INTERFERING_QUEUES).await {
        Ok(true) => Ok(saved),
        Ok(false) => Err((
            saved,
            AppError::conflict(
                "maintenance_blocked",
                "Immich queued new work while pausing",
            ),
        )),
        Err(e) => Err((saved, e)),
    }
}

/// Puts back settings changed by `try_prepare` when it failed.
async fn undo(api: &Api, saved: &SavedSettings) -> Result<(), AppError> {
    for q in &saved.paused_queues {
        api.set_queue_paused(q, false).await.map_err(immich)?;
    }
    restore_config(api, saved).await
}

async fn restore_config(api: &Api, saved: &SavedSettings) -> Result<(), AppError> {
    if saved.watch_enabled.is_none() && saved.scan_enabled.is_none() {
        return Ok(());
    }
    let mut config: Value = api.get_config().await.map_err(immich)?;
    let mut changed = false;
    for (path, before) in [
        (WATCH_ENABLED, saved.watch_enabled),
        (SCAN_ENABLED, saved.scan_enabled),
    ] {
        let current = api::config_bool(&config, path).unwrap_or(false);
        if let Some(value) = restore_value(before, false, current) {
            changed |= api::set_config_bool(&mut config, path, value);
        }
    }
    if changed {
        api.put_config(&config).await.map_err(immich)?;
    }
    Ok(())
}

/// Resumes the library queue, scans every affected library and waits for the scan to finish.
async fn reconcile(state: &AppState, server: &Server) -> AppResult<()> {
    let api = registry::admin_api(state, server).await?;
    let m = registry::server(state, &server.id).await?.maintenance;
    for q in &m.saved.paused_queues {
        api.set_queue_paused(q, false).await.map_err(immich)?;
    }
    update(state, &server.id, |m| m.saved.paused_queues.clear()).await?;
    for library in &m.affected_libraries {
        api.scan_library(library).await.map_err(immich)?;
    }
    if !m.affected_libraries.is_empty() {
        // Give the scan job time to be queued before watching the counters.
        tokio::time::sleep(state.config.timing.settle).await;
        let names = ["library", "sidecar", "metadataExtraction"];
        if !wait_idle(state, &api, &names, state.config.timing.scan_timeout).await? {
            tracing::warn!(server = %server.name, "reconciliation scan still running; restoring watchers anyway");
        }
    }
    update(state, &server.id, MaintenanceState::begin_restore).await?;
    restore(state, server).await
}

async fn restore(state: &AppState, server: &Server) -> AppResult<()> {
    let api = registry::admin_api(state, server).await?;
    let m = registry::server(state, &server.id).await?.maintenance;
    for q in &m.saved.paused_queues {
        api.set_queue_paused(q, false).await.map_err(immich)?;
    }
    if let Err(e) = restore_config(&api, &m.saved).await {
        update(state, &server.id, |m| {
            m.message = Some(format!("restoring watchers failed: {}", e.message()))
        })
        .await?;
        return Err(e);
    }
    update(state, &server.id, MaintenanceState::restored).await?;
    tracing::info!(server = %server.name, "watchers restored");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;
    use crate::config::Config;
    use crate::crypto::SecretBox;

    /// A tiny stateful Immich: `/admin/config` keeps what was PUT; queues report `busy` jobs.
    #[derive(Clone)]
    struct FakeImmich {
        config: Arc<Mutex<Value>>,
        busy_queue: Arc<Mutex<Option<&'static str>>>,
        paused: Arc<Mutex<Vec<String>>>,
    }

    impl Respond for FakeImmich {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let p = req.url.path();
            match (req.method.as_str(), p) {
                ("GET", "/api/admin/config") => {
                    ResponseTemplate::new(200).set_body_json(self.config.lock().unwrap().clone())
                }
                ("PUT", "/api/admin/config") => {
                    let v: Value = serde_json::from_slice(&req.body).unwrap();
                    *self.config.lock().unwrap() = v.clone();
                    ResponseTemplate::new(200).set_body_json(v)
                }
                ("GET", "/api/queues") => {
                    let busy = *self.busy_queue.lock().unwrap();
                    let paused = self.paused.lock().unwrap().clone();
                    let names = [
                        "library",
                        "sidecar",
                        "metadataExtraction",
                        "storageTemplateMigration",
                        "thumbnailGeneration",
                    ];
                    let qs: Vec<Value> = names
                        .iter()
                        .map(|n| {
                            json!({ "name": n, "isPaused": paused.iter().any(|x| x == n),
                                    "statistics": { "active": i64::from(busy == Some(*n)), "waiting": 0, "delayed": 0, "paused": 0 } })
                        })
                        .collect();
                    ResponseTemplate::new(200).set_body_json(qs)
                }
                ("PUT", q) if q.starts_with("/api/queues/") => {
                    let name = q.trim_start_matches("/api/queues/").to_owned();
                    let v: Value = serde_json::from_slice(&req.body).unwrap();
                    let mut paused = self.paused.lock().unwrap();
                    paused.retain(|x| *x != name);
                    if v["isPaused"] == true {
                        paused.push(name);
                    }
                    ResponseTemplate::new(200).set_body_json(json!({}))
                }
                ("POST", s) if s.starts_with("/api/libraries/") && s.ends_with("/scan") => {
                    ResponseTemplate::new(204)
                }
                _ => ResponseTemplate::new(404),
            }
        }
    }

    async fn setup(
        busy: Option<&'static str>,
    ) -> (tempfile::TempDir, AppState, MockServer, FakeImmich) {
        let dir = tempfile::tempdir().unwrap();
        let pool = crate::db::connect(dir.path()).await.unwrap();
        let secrets = SecretBox::open_or_create(dir.path()).unwrap();
        let state = AppState::new(Config::for_tests(dir.path()), pool, secrets)
            .await
            .unwrap();
        let fake = FakeImmich {
            config: Arc::new(Mutex::new(json!({
                "library": { "watch": { "enabled": true }, "scan": { "enabled": true, "cronExpression": "0 0 * * *" } },
                "trash": { "enabled": true, "days": 30 }
            }))),
            busy_queue: Arc::new(Mutex::new(busy)),
            paused: Arc::new(Mutex::new(Vec::new())),
        };
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(fake.clone())
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(fake.clone())
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/libraries/lib1/scan"))
            .respond_with(fake.clone())
            .mount(&server)
            .await;
        let now = crate::db::now();
        sqlx::query(
            "INSERT INTO immich_servers (id, name, url, version, compatible, maintenance_json, created_at) VALUES ('s1', 'Home', ?, '3.4.1', 1, ?, ?)",
        )
        .bind(server.uri())
        .bind(serde_json::to_string(&MaintenanceState::default()).unwrap())
        .bind(&now)
        .execute(&state.db)
        .await
        .unwrap();
        let key = state
            .secrets
            .encrypt(registry::API_KEY_PURPOSE, "test-key")
            .unwrap();
        sqlx::query(
            "INSERT INTO immich_accounts (id, server_id, label, email, immich_user_id, is_admin, api_key_enc, status, created_at)
             VALUES ('a1', 's1', 'Admin', 'admin@ism.example', 'u1', 1, ?, 'ok', ?)",
        )
        .bind(key)
        .bind(&now)
        .execute(&state.db)
        .await
        .unwrap();
        (dir, state, server, fake)
    }

    async fn scans(server: &MockServer) -> usize {
        let reqs = server.received_requests().await.unwrap();
        reqs.iter()
            .filter(|r| r.url.path() == "/api/libraries/lib1/scan")
            .count()
    }

    fn flag(fake: &FakeImmich, p: &[&str]) -> Option<bool> {
        api::config_bool(&fake.config.lock().unwrap(), p)
    }

    #[tokio::test]
    async fn pauses_admits_scans_and_restores() {
        let (_dir, state, server, fake) = setup(None).await;
        let worker = tokio::spawn(run(state.clone()));

        acquire(&state, "s1", &["lib1".to_owned()]).await.unwrap();
        assert_eq!(
            flag(&fake, WATCH_ENABLED),
            Some(false),
            "watcher is off before any file is touched"
        );
        assert_eq!(flag(&fake, SCAN_ENABLED), Some(false));
        assert_eq!(fake.paused.lock().unwrap().as_slice(), ["library"]);
        assert_eq!(
            registry::server(&state, "s1")
                .await
                .unwrap()
                .maintenance
                .phase,
            Phase::Active
        );

        release(&state, "s1").await.unwrap();
        // Nothing happens before the quiet period ends.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            registry::server(&state, "s1")
                .await
                .unwrap()
                .maintenance
                .phase,
            Phase::Active
        );
        // Skip the 15 minutes.
        update(&state, "s1", |m| {
            m.quiet_deadline = Some(Utc::now() - chrono::Duration::seconds(1))
        })
        .await
        .unwrap();
        state.maintenance_wake.notify_waiters();

        for _ in 0..100 {
            if registry::server(&state, "s1")
                .await
                .unwrap()
                .maintenance
                .phase
                == Phase::Idle
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        worker.abort();
        let m = registry::server(&state, "s1").await.unwrap().maintenance;
        assert_eq!(m.phase, Phase::Idle, "{m:?}");
        assert_eq!(flag(&fake, WATCH_ENABLED), Some(true));
        assert_eq!(flag(&fake, SCAN_ENABLED), Some(true));
        assert_eq!(
            fake.config.lock().unwrap()["library"]["scan"]["cronExpression"],
            "0 0 * * *"
        );
        assert!(fake.paused.lock().unwrap().is_empty());
        assert_eq!(
            scans(&server).await,
            1,
            "the affected library is scanned once"
        );
    }

    #[tokio::test]
    async fn busy_queue_blocks_and_restores() {
        let (_dir, state, server, fake) = setup(Some("metadataExtraction")).await;
        let worker = tokio::spawn(run(state.clone()));
        let err = acquire(&state, "s1", &["lib1".to_owned()])
            .await
            .unwrap_err();
        worker.abort();
        assert_eq!(err.code(), "maintenance_blocked");
        let m = registry::server(&state, "s1").await.unwrap().maintenance;
        assert_eq!(m.phase, Phase::Blocked, "{m:?}");
        assert_eq!(
            flag(&fake, WATCH_ENABLED),
            Some(true),
            "settings are put back when pausing fails"
        );
        assert_eq!(flag(&fake, SCAN_ENABLED), Some(true));
        assert!(fake.paused.lock().unwrap().is_empty());
        // A blocked server keeps refusing work until an admin retries.
        let err = acquire(&state, "s1", &["lib1".to_owned()])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "maintenance_blocked");
        assert_eq!(scans(&server).await, 0);
    }
}
