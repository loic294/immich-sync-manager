//! Two real instances talking over HTTP on localhost.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "isolation_tests.rs"]
mod isolation;

#[path = "folder_decision_tests.rs"]
mod folder_decisions;

#[path = "notification_tests.rs"]
mod notifications;

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ism_core::dto::{
    ConflictResolution, ConflictSide, Location, OperationAction, OperationRequest, PeerAcceptInput,
    ShareInput,
};
use ism_core::version::VersionVector;

use super::admin;
use crate::auth::Principal;
use crate::config::Config;
use crate::crypto::SecretBox;
use crate::state::AppState;

struct Instance {
    _dir: tempfile::TempDir,
    state: AppState,
    url: String,
    shared: PathBuf,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    requests: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    notification_status: std::sync::Arc<std::sync::atomic::AtomicU16>,
    snapshot_status: std::sync::Arc<std::sync::atomic::AtomicU16>,
    recovery_status: std::sync::Arc<std::sync::atomic::AtomicU16>,
}

impl Drop for Instance {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

async fn instance(name: &str) -> Instance {
    let dir = tempfile::tempdir().unwrap();
    let pool = crate::db::connect(dir.path()).await.unwrap();
    let secrets = SecretBox::open_or_create(dir.path()).unwrap();
    let state = AppState::new(Config::for_tests(dir.path()), pool, secrets)
        .await
        .unwrap();
    crate::db::set_setting(&state.db, "instance_name", name)
        .await
        .unwrap();
    let root = dir.path().join("photos");
    let shared = root.join("shared");
    std::fs::create_dir_all(&shared).unwrap();
    std::fs::write(root.join(".ism-root"), "photos\n").unwrap();
    sqlx::query("INSERT INTO mappings (root_id, name, container_path, created_at) VALUES ('photos', 'Photos', ?, ?)")
        .bind(root.to_string_lossy().to_string())
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let notification_status = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(0));
    let snapshot_status = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(0));
    let recovery_status = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(0));
    let app = crate::routes::router(state.clone()).layer(axum::middleware::from_fn({
        let requests = requests.clone();
        let status = notification_status.clone();
        let snapshot_status = snapshot_status.clone();
        let recovery_status = recovery_status.clone();
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let requests = requests.clone();
            let status = status.clone();
            let snapshot_status = snapshot_status.clone();
            let recovery_status = recovery_status.clone();
            async move {
                let path = request.uri().path().to_owned();
                *requests.lock().unwrap().entry(path.clone()).or_insert(0) += 1;
                let status = status.load(std::sync::atomic::Ordering::SeqCst);
                if path == "/api/peer/v1/notifications" && status != 0 {
                    return axum::response::IntoResponse::into_response(
                        axum::http::StatusCode::from_u16(status).unwrap(),
                    );
                }
                let status = snapshot_status.load(std::sync::atomic::Ordering::SeqCst);
                let recovery_status = recovery_status.load(std::sync::atomic::Ordering::SeqCst);
                if path == "/api/peer/v1/recovery" && recovery_status != 0 {
                    return axum::response::IntoResponse::into_response(
                        axum::http::StatusCode::from_u16(recovery_status).unwrap(),
                    );
                }
                if matches!(
                    path.as_str(),
                    "/api/peer/v1/recovery" | "/api/peer/v1/sync-activity"
                ) && status != 0
                {
                    return axum::response::IntoResponse::into_response(
                        axum::http::StatusCode::from_u16(status).unwrap(),
                    );
                }
                next.run(request).await
            }
        }
    }));
    let tasks = vec![
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }),
        tokio::spawn(crate::ops::exec::run(state.clone())),
        tokio::spawn(super::run(state.clone())),
        tokio::spawn(super::transfer::run(state.clone())),
        tokio::spawn(super::scan_worker(state.clone())),
    ];
    Instance {
        _dir: dir,
        state,
        url,
        shared,
        tasks,
        requests,
        notification_status,
        snapshot_status,
        recovery_status,
    }
}

async fn scheduler_fixture(
    activity: bool,
) -> (
    Instance,
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    use std::sync::Arc;
    use tokio::sync::Notify;

    let mut local = instance("Scheduler").await;
    stop_workers(&mut local).await;
    let entered = Arc::new(Notify::new());
    let respond = Arc::new(Notify::new());
    let app = axum::Router::new()
        .route(
            "/api/peer/v1/notifications",
            axum::routing::get(|| async { axum::http::StatusCode::NOT_FOUND }),
        )
        .fallback({
            let entered = entered.clone();
            let respond = respond.clone();
            move || {
                let entered = entered.clone();
                let respond = respond.clone();
                async move {
                    entered.notify_one();
                    respond.notified().await;
                    axum::Json(serde_json::json!({
                        "instance_id": "different-instance", "name": "Remote", "shares": [],
                        "total": 0, "rows": []
                    }))
                }
            }
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    local.tasks.push(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    let secret = local
        .state
        .secrets
        .encrypt(super::net::SECRET_PURPOSE, "synthetic-secret")
        .unwrap();
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         VALUES('remote','Remote',?,'expected-instance',?,'online',?)",
    )
    .bind(url)
    .bind(secret)
    .bind(crate::db::now())
    .execute(&local.state.db)
    .await
    .unwrap();
    let state = local.state.clone();
    local.tasks.push(tokio::spawn(async move {
        if activity {
            super::activity::run(state).await;
        } else {
            super::replicate(state).await;
        }
    }));
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("scheduler must admit the peer worker");
    (local, entered, respond)
}

async fn scheduler_releases_worker_connection(activity: bool) {
    let (mut local, _, respond) = scheduler_fixture(activity).await;
    // Keep seven connections, including the writer, leaving the eighth for the worker.
    let mut writer = local.state.db.acquire().await.unwrap();
    let mut transaction = sqlx::Connection::begin(&mut *writer).await.unwrap();
    sqlx::query("UPDATE peers SET name=name WHERE id='remote'")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let mut held = Vec::new();
    for _ in 0..6 {
        held.push(local.state.db.acquire().await.unwrap());
    }
    respond.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while local.state.db.num_idle() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker must acquire the last connection");

    // The next queue check now waits for the connection owned by the worker.
    if activity {
        tokio::time::sleep(Duration::from_secs(5)).await;
    } else {
        local.state.sync_wake.notify_one();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    transaction.commit().await.unwrap();
    let available = tokio::time::timeout(Duration::from_secs(2), local.state.db.acquire())
        .await
        .expect("admitted worker must release its connection while the scheduler loads peers")
        .unwrap();
    drop(available);
    stop_workers(&mut local).await;
}

#[tokio::test]
async fn replication_scheduler_does_not_starve_admitted_worker() {
    scheduler_releases_worker_connection(false).await;
}

#[tokio::test]
async fn activity_scheduler_does_not_starve_admitted_worker() {
    scheduler_releases_worker_connection(true).await;
}

#[tokio::test]
async fn schedulers_suppress_duplicates_and_cancel_admitted_workers() {
    for activity in [false, true] {
        let (mut local, entered, respond) = scheduler_fixture(activity).await;
        local.state.sync_wake.notify_one();
        tokio::time::sleep(Duration::from_secs(if activity { 5 } else { 2 })).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), entered.notified())
                .await
                .is_err(),
            "a scheduler must not admit a second worker for a running peer"
        );
        let scheduler = local.tasks.pop().unwrap();
        scheduler.abort();
        assert!(scheduler.await.unwrap_err().is_cancelled());
        if activity {
            respond.notify_waiters();
            tokio::time::sleep(Duration::from_millis(250)).await;
            let saved: i64 = sqlx::query_scalar(
                "SELECT (SELECT COUNT(*) FROM peer_recovery_state)
                      + (SELECT COUNT(*) FROM peer_sync_state)",
            )
            .fetch_one(&local.state.db)
            .await
            .unwrap();
            assert_eq!(saved, 0, "shutdown must cancel the fetch, not detach it");
            stop_workers(&mut local).await;
            continue;
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = crate::activity::page(&local.state, 100, 0).await.unwrap();
                if page.current.iter().chain(&page.history).any(|row| {
                    row.status == ism_core::dto::BackgroundStatus::Interrupted
                        && row.task_key == "peer:remote"
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduler shutdown must cancel the admitted worker, not detach it");
        stop_workers(&mut local).await;
    }
}

/// Writes a file that looks settled to the scanner.
fn write_old(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
    let f = std::fs::File::options().write(true).open(path).unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(120))
        .unwrap();
}

async fn scan(i: &Instance) {
    for share in super::shares(&i.state).await.unwrap() {
        super::scan_share(&i.state, &share).await.unwrap();
    }
    i.state.sync_wake.notify_waiters();
}

async fn eventually(what: &str, peers: &[&Instance], mut check: impl FnMut() -> bool) {
    for _ in 0..300 {
        if check() {
            return;
        }
        for p in peers {
            p.state.sync_wake.notify_waiters();
            p.state.ops_wake.notify_waiters();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

async fn pair() -> (Instance, Instance) {
    let a = instance("Alpha").await;
    let b = instance("Beta").await;
    let invite = admin::create_invite(&a.state).await.unwrap();
    admin::accept(
        &b.state,
        PeerAcceptInput {
            name: String::new(),
            url: a.url.clone(),
            invite: invite.invite,
            callback_url: b.url.clone(),
        },
    )
    .await
    .unwrap();
    for (me, _) in [(&a, &b), (&b, &a)] {
        let peers = admin::list_peers(&me.state).await.unwrap();
        assert_eq!(peers.len(), 1);
        admin::create_share(
            &me.state,
            ShareInput {
                name: "Family".into(),
                location: Location {
                    root_id: "photos".into(),
                    path: "shared".into(),
                },
                peer_ids: vec![peers[0].id.clone()],
                enabled: true,
            },
        )
        .await
        .unwrap();
    }
    settle_share_announcements(&a, &b).await;
    (a, b)
}

/// Waits until each side pulled the other's `share_enabled` announcement and finished the
/// resulting (empty) backfill, so tests start from a quiet replication state.
async fn settle_share_announcements(a: &Instance, b: &Instance) {
    async fn settled(me: &Instance, other: &Instance) -> bool {
        let last: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM event_log")
            .fetch_one(&other.state.db)
            .await
            .unwrap();
        let peer = super::net::peers(&me.state).await.unwrap().remove(0);
        let backfills: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM share_backfills")
            .fetch_one(&me.state.db)
            .await
            .unwrap();
        peer.cursor >= last && backfills == 0
    }
    for _ in 0..400 {
        if settled(a, b).await && settled(b, a).await {
            return;
        }
        a.state.sync_wake.notify_waiters();
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("share announcements did not settle");
}

async fn stop_workers(instance: &mut Instance) {
    while instance.tasks.len() > 1 {
        let task = instance.tasks.pop().unwrap();
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn idle_peer_checks_coalesce_last_seen_writes() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query("UPDATE peers SET status = 'online', last_seen_at = ? WHERE id = ?")
        .bind(crate::db::now())
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE peer_seen_writes (id INTEGER)")
        .execute(&b.state.db)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER count_peer_seen AFTER UPDATE OF last_seen_at ON peers
         BEGIN INSERT INTO peer_seen_writes VALUES (1); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    for _ in 0..5 {
        let hello: super::net::Hello = super::net::get_json(&b.state, &peer, "/api/peer/v1/hello")
            .await
            .unwrap();
        assert_eq!(
            peer.instance_id.as_deref(),
            Some(hello.instance_id.as_str())
        );
        super::net::mark(&b.state, &peer.id, Ok(())).await;
    }
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_seen_writes")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(
        writes, 0,
        "fresh idle checks should not update last_seen_at"
    );
    for (age, expected_writes) in [(18, 0), (20, 1)] {
        let seen = crate::db::ts(chrono::Utc::now() - chrono::Duration::seconds(age));
        sqlx::query("UPDATE peers SET last_seen_at = ? WHERE id = ?")
            .bind(seen)
            .bind(&peer.id)
            .execute(&b.state.db)
            .await
            .unwrap();
        sqlx::query("DELETE FROM peer_seen_writes")
            .execute(&b.state.db)
            .await
            .unwrap();
        super::net::mark(&b.state, &peer.id, Ok(())).await;
        let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_seen_writes")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
        assert_eq!(writes, expected_writes, "last-seen age: {age}s");
    }
}

#[tokio::test]
async fn unchanged_peer_status_does_not_wait_for_sqlite_writer() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "UPDATE peers SET status = 'online', last_seen_at = ?, last_error = NULL WHERE id = ?",
    )
    .bind(crate::db::now())
    .bind(&peer.id)
    .execute(&b.state.db)
    .await
    .unwrap();
    let mut writer = b.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE settings SET value = value WHERE key = 'instance_name'")
        .execute(&mut *writer)
        .await
        .unwrap();
    let completed = tokio::time::timeout(Duration::from_millis(500), async {
        super::net::mark(&b.state, &peer.id, Ok(())).await;
        super::net::mark_processing_result(&b.state, &peer.id, None).await;
    })
    .await;
    writer.rollback().await.unwrap();
    assert!(
        completed.is_ok(),
        "unchanged peer status waited for a writer"
    );
}

#[tokio::test]
async fn concurrent_stale_peer_checks_refresh_last_seen_once() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query("UPDATE peers SET status = 'online', last_seen_at = ? WHERE id = ?")
        .bind(crate::db::ts(
            chrono::Utc::now() - chrono::Duration::seconds(60),
        ))
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE peer_seen_writes (id INTEGER)")
        .execute(&b.state.db)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER count_peer_seen AFTER UPDATE OF last_seen_at ON peers
         BEGIN INSERT INTO peer_seen_writes VALUES (1); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let writer = b.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let checks: Vec<_> = (0..5)
        .map(|_| {
            let state = b.state.clone();
            let id = peer.id.clone();
            tokio::spawn(async move { super::net::mark(&state, &id, Ok(())).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    writer.rollback().await.unwrap();
    for check in checks {
        check.await.unwrap();
    }
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_seen_writes")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(writes, 1);
    let seen: String = sqlx::query_scalar("SELECT last_seen_at FROM peers WHERE id = ?")
        .bind(&peer.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert!(
        crate::db::parse_ts(&seen).unwrap() > chrono::Utc::now() - chrono::Duration::seconds(20)
    );
}

#[tokio::test]
async fn peer_status_transitions_and_error_changes_are_notified() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "UPDATE peers SET status = 'online', last_seen_at = ?, last_error = NULL WHERE id = ?",
    )
    .bind(crate::db::now())
    .bind(&peer.id)
    .execute(&b.state.db)
    .await
    .unwrap();
    let mut updates = b.state.events.subscribe();
    super::net::mark(&b.state, &peer.id, Err("unreachable".into())).await;
    assert!(matches!(
        updates.try_recv(),
        Ok(crate::state::ServerEvent::SyncChanged)
    ));
    super::net::mark(&b.state, &peer.id, Err("unreachable".into())).await;
    assert!(updates.try_recv().is_err());
    super::net::mark(&b.state, &peer.id, Err("timed out".into())).await;
    assert!(matches!(
        updates.try_recv(),
        Ok(crate::state::ServerEvent::SyncChanged)
    ));
    super::net::mark(&b.state, &peer.id, Ok(())).await;
    assert!(matches!(
        updates.try_recv(),
        Ok(crate::state::ServerEvent::SyncChanged)
    ));
    let (status, error): (String, Option<String>) =
        sqlx::query_as("SELECT status, last_error FROM peers WHERE id = ?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), error.as_deref()),
        ("online", Some("timed out"))
    );
    super::net::mark_processing_result(&b.state, &peer.id, Some("processing failed")).await;
    assert!(matches!(
        updates.try_recv(),
        Ok(crate::state::ServerEvent::SyncChanged)
    ));
    super::net::mark_processing_result(&b.state, &peer.id, Some("processing failed")).await;
    assert!(updates.try_recv().is_err());
    super::net::mark_processing_result(&b.state, &peer.id, None).await;
    assert!(matches!(
        updates.try_recv(),
        Ok(crate::state::ServerEvent::SyncChanged)
    ));
    let error: Option<String> = sqlx::query_scalar("SELECT last_error FROM peers WHERE id = ?")
        .bind(&peer.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(error, None);
}

#[tokio::test]
async fn recovery_automatically_finishes_local_bookkeeping_once() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    sqlx::query(
        "CREATE TRIGGER reject_recovery_event BEFORE INSERT ON event_log
         WHEN json_extract(NEW.data_json,'$.path')='recoverable'
         BEGIN SELECT RAISE(FAIL,'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "recover-local-bookkeeping".into(),
            label: Some("Create recoverable folder".into()),
            actions: vec![OperationAction::CreateFolder {
                target: Location {
                    root_id: "photos".into(),
                    path: "shared/recoverable".into(),
                },
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER reject_recovery_event")
        .execute(&b.state.db)
        .await
        .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &accepted.operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Completed,
    );
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM event_log WHERE json_extract(data_json,'$.path')='recoverable'",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(count, 1);
    let batch = ism_core::dto::RecoveryBatchInput {
        targets: vec![ism_core::dto::RecoveryTarget {
            kind: ism_core::dto::RecoveryKind::Operation,
            id: accepted.operation_id.clone(),
            peer_id: None,
        }],
        all: false,
        resolution: None,
    };
    let response = super::recovery::submit(&b.state, batch.clone())
        .await
        .unwrap();
    assert_eq!(response.items[0].state, ism_core::dto::RecoveryState::Fixed);
    super::recovery::submit(&b.state, batch).await.unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM event_log WHERE json_extract(data_json,'$.path')='recoverable'",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap(),
        count,
    );
}

#[tokio::test]
async fn recovery_batch_preserves_decisions_and_never_dismisses_unverified_work() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"alpha");
    write_old(&b.shared.join("photo.jpg"), b"beta");
    scan(&a).await;
    scan(&b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    assert!(
        super::pull(&b.state, &peer)
            .await
            .unwrap_err()
            .contains("conflict resolution")
    );
    let conflicts = admin::list_conflicts(&b.state, false).await.unwrap();
    assert_eq!(conflicts.len(), 1);
    let result = super::recovery::submit(
        &b.state,
        ism_core::dto::RecoveryBatchInput {
            targets: Vec::new(),
            all: true,
            resolution: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.items.len(), 1);
    super::recovery::tick(&b.state).await.unwrap();
    let recovery = super::recovery::get(
        &b.state,
        ism_core::dto::RecoveryKind::Conflict,
        &conflicts[0].id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(recovery.state, ism_core::dto::RecoveryState::NeedsDecision);
    assert!(!recovery.can_dismiss);
    assert_eq!(std::fs::read(b.shared.join("photo.jpg")).unwrap(), b"beta");
    let error = super::recovery::submit(
        &b.state,
        ism_core::dto::RecoveryBatchInput {
            targets: vec![recovery.target],
            all: false,
            resolution: Some(ConflictResolution::Skip),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "recovery_not_exhausted");
}

#[tokio::test]
async fn peer_original_deletion_preserves_the_surviving_orphan_sidecar() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"original");
    write_old(&a.shared.join("photo.xmp"), b"surviving metadata");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pending: i64 = sqlx::query_scalar(
                "SELECT (SELECT COUNT(*) FROM transfers)
                 + (SELECT COUNT(*) FROM operations WHERE status IN ('queued','running','waiting_maintenance'))",
            )
            .fetch_one(&b.state.db)
            .await
            .unwrap();
            if pending == 0 {
                break;
            }
            b.state.sync_wake.notify_waiters();
            b.state.ops_wake.notify_one();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("both initial files must finish publication");
    stop_workers(&mut b).await;
    super::pull(&b.state, &peer).await.unwrap();
    assert_eq!(
        std::fs::read(b.shared.join("photo.xmp")).unwrap(),
        b"surviving metadata"
    );
    std::fs::remove_file(a.shared.join("photo.jpg")).unwrap();
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let deletion: String = sqlx::query_scalar(
        "SELECT operation_id FROM pending_peer_ops WHERE kind='delete' AND to_path='photo.jpg'",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &deletion)
        .await
        .unwrap();
    assert!(!b.shared.join("photo.jpg").exists());
    assert_eq!(
        std::fs::read(b.shared.join("photo.xmp")).unwrap(),
        b"surviving metadata",
        "an original-only peer deletion must not remove an unannounced sidecar",
    );
    let share = super::shares(&b.state).await.unwrap().remove(0);
    assert!(
        !super::row(&b.state, &share.id, "photo.xmp")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
}

#[tokio::test]
async fn peer_and_reviewed_conflict_sidecar_deletions_preserve_the_original() {
    for diverged in [false, true] {
        let (mut a, mut b) = pair().await;
        stop_workers(&mut a).await;
        stop_workers(&mut b).await;
        write_old(&a.shared.join("photo.jpg"), b"original");
        write_old(&a.shared.join("photo.xmp"), b"metadata");
        scan(&a).await;
        let peer = super::net::peers(&b.state).await.unwrap().remove(0);
        super::pull(&b.state, &peer).await.unwrap();
        b.tasks
            .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
        b.tasks
            .push(tokio::spawn(super::transfer::run(b.state.clone())));
        tokio::time::timeout(Duration::from_secs(5),async {
            loop {
                let pending:i64=sqlx::query_scalar(
                    "SELECT (SELECT COUNT(*) FROM transfers)
                     + (SELECT COUNT(*) FROM operations WHERE status IN ('queued','running','waiting_maintenance'))",
                ).fetch_one(&b.state.db).await.unwrap();
                if pending==0 { break; }
                b.state.sync_wake.notify_waiters();
                b.state.ops_wake.notify_one();
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await.expect("initial publication must settle");
        stop_workers(&mut b).await;
        super::pull(&b.state, &peer).await.unwrap();
        if diverged {
            write_old(&b.shared.join("photo.xmp"), b"changed metadata");
            scan(&b).await;
        }
        std::fs::remove_file(a.shared.join("photo.xmp")).unwrap();
        scan(&a).await;
        if diverged {
            super::pull(&b.state, &peer).await.unwrap_err();
            let id: String = sqlx::query_scalar(
                "SELECT id FROM conflicts WHERE rel_path='photo.xmp' AND status='open'",
            )
            .fetch_one(&b.state.db)
            .await
            .unwrap();
            let reviewed = super::versions::get(
                &b.state,
                &ism_core::dto::RecoveryTarget {
                    kind: ism_core::dto::RecoveryKind::Conflict,
                    id: id.clone(),
                    peer_id: None,
                },
            )
            .await
            .unwrap();
            admin::resolve_reviewed(
                &b.state,
                "test",
                &id,
                ConflictResolution::KeepRemote,
                Some(&reviewed.revision),
            )
            .await
            .unwrap();
        } else {
            super::pull(&b.state, &peer).await.unwrap();
        }
        let operation: String = sqlx::query_scalar(
            "SELECT operation_id FROM pending_peer_ops WHERE kind='delete' AND to_path='photo.xmp'",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap();
        crate::ops::exec::run_operation(&b.state, &operation)
            .await
            .unwrap();
        assert!(!b.shared.join("photo.xmp").exists());
        assert_eq!(
            std::fs::read(b.shared.join("photo.jpg")).unwrap(),
            b"original"
        );
        let progress: String = sqlx::query_scalar(
            "SELECT progress_json FROM operation_items WHERE operation_id=? AND idx=0",
        )
        .bind(&operation)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&progress).unwrap()["files"][0]["role"],
            "sidecar"
        );
    }
}

#[tokio::test]
async fn recovery_fetches_exact_peer_trash_content_without_restoring_ancestor_collateral() {
    use ism_core::dto::{RecoveryBatchInput, RecoveryKind, RecoveryTarget};
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("lost/orphan.xmp"), b"retained metadata");
    write_old(&a.shared.join("lost/unrelated.jpg"), b"unrelated photo");
    let source = super::net::peers(&b.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let trashed = crate::ops::submit(
        &a.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "retain-folder-for-exact-repair".into(),
            label: None,
            actions: vec![OperationAction::TrashFolder {
                source: loc("shared/lost"),
                expected_entries: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&a.state, &trashed.operation_id)
        .await
        .unwrap();
    assert!(!a.shared.join("lost").exists());
    sqlx::query(
        "INSERT INTO transfers (id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at,error)
         VALUES ('retained-repair',?,?, 'lost/orphan.xmp',?,?,?,'failed',?,?,'source file missing')",
    ).bind(&source.id).bind(&share.id).bind(crate::crypto::sha256_hex(b"retained metadata")).bind(17_i64)
    .bind("{}").bind(crate::db::now()).bind(crate::db::now())
    .execute(&b.state.db).await.unwrap();
    super::recovery::submit(
        &b.state,
        RecoveryBatchInput {
            targets: vec![RecoveryTarget {
                kind: RecoveryKind::Transfer,
                id: "retained-repair".into(),
                peer_id: None,
            }],
            all: false,
            resolution: None,
        },
    )
    .await
    .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    let queued: (String, Option<String>) = sqlx::query_as(
        "SELECT status,source_retained_token FROM transfers WHERE id='retained-repair'",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(queued.0, "queued");
    assert!(queued.1.is_some());
    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    eventually("exact retained sidecar publication", &[&b], || {
        b.shared.join("lost/orphan.xmp").exists()
    })
    .await;
    assert_eq!(
        std::fs::read(b.shared.join("lost/orphan.xmp")).unwrap(),
        b"retained metadata"
    );
    assert!(!b.shared.join("lost/unrelated.jpg").exists());
    assert!(!a.shared.join("lost").exists());
    let retained: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM trash WHERE restored_at IS NULL AND purged_at IS NULL",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(retained, 1);
}

#[tokio::test]
async fn recovery_comparison_does_not_wait_for_unrelated_peer_operations() {
    use ism_core::dto::{RecoveryKind, RecoveryTarget};
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("compare.jpg"), b"source version");
    write_old(&b.shared.join("compare.jpg"), b"receiving version");
    scan(&a).await;
    scan(&b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap_err();
    let id: String = sqlx::query_scalar("SELECT id FROM conflicts WHERE rel_path='compare.jpg'")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let target = RecoveryTarget {
        kind: RecoveryKind::Conflict,
        id,
        peer_id: None,
    };
    let _source_operation = a.state.replication_operation_lock.lock().await;
    let _receiving_operation = b.state.replication_operation_lock.lock().await;
    let reviewed = tokio::time::timeout(
        Duration::from_secs(2),
        super::versions::get(&b.state, &target),
    )
    .await
    .expect("comparison waited for an unrelated operation and would hit the proxy timeout")
    .unwrap();
    assert_eq!(
        reviewed.source.files[0].sha256,
        crate::crypto::sha256_hex(b"source version")
    );
    assert_eq!(
        reviewed.receiving[0].files[0].sha256,
        crate::crypto::sha256_hex(b"receiving version")
    );
    assert!(reviewed.source.files[0].modified_at.is_some());
}

#[tokio::test]
async fn reviewed_batch_admission_queues_without_contacting_the_peer() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use ism_core::dto::{
        RecoveryBatchResult, RecoveryDecision, RecoveryDecisionInput, RecoveryKind, RecoveryState,
        RecoveryTarget,
    };
    use tower::ServiceExt;
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    for index in 0..200 {
        let path = format!("compare-{index}.jpg");
        write_old(&a.shared.join(&path), b"source version");
        write_old(&b.shared.join(&path), b"receiving version");
    }
    scan(&a).await;
    scan(&b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap_err();
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM conflicts ORDER BY id")
        .fetch_all(&b.state.db)
        .await
        .unwrap();
    assert_eq!(ids.len(), 200);
    let mut decisions = Vec::new();
    for id in ids {
        let target = RecoveryTarget {
            kind: RecoveryKind::Conflict,
            id,
            peer_id: None,
        };
        let review = super::versions::get(&b.state, &target).await.unwrap();
        decisions.push(RecoveryDecision {
            target,
            revision: review.revision,
        });
    }
    a.tasks[0].abort();
    let stalled_peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    sqlx::query("UPDATE peers SET url=? WHERE id=?")
        .bind(format!("http://{}", stalled_peer.local_addr().unwrap()))
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let user: i64 = sqlx::query_scalar(
        "INSERT INTO admin_users(username,password_hash,created_at)
         VALUES ('test','unused synthetic hash',?) RETURNING id",
    )
    .bind(crate::db::now())
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    let token = crate::auth::create_session(&b.state, user).await.unwrap();
    let input = RecoveryDecisionInput {
        decisions,
        resolution: ConflictResolution::KeepRemote,
    };
    let request = Request::builder()
        .method("POST")
        .uri("/api/admin/recovery/decisions")
        .header("content-type", "application/json")
        .header("x-ism-request", "1")
        .header("cookie", format!("ism_session={token}"))
        .body(Body::from(serde_json::to_vec(&input).unwrap()))
        .unwrap();
    let _operation = b.state.replication_operation_lock.lock().await;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        crate::routes::router(b.state.clone()).oneshot(request),
    )
    .await
    .expect("batch admission waited for files, peers, or an unrelated operation")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result: RecoveryBatchResult =
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
            .unwrap();
    assert_eq!(result.items.len(), 200);
    assert!(
        result
            .items
            .iter()
            .all(|item| item.state == RecoveryState::Queued)
    );
    let queued: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_recovery WHERE state='queued'")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(queued, 200);
    assert_eq!(
        std::fs::read(b.shared.join("compare-0.jpg")).unwrap(),
        b"receiving version"
    );
    let replay = super::recovery::submit_decisions(&b.state, input.clone())
        .await
        .unwrap();
    assert!(
        replay
            .items
            .iter()
            .all(|item| item.state == RecoveryState::Queued)
    );
    sqlx::query("UPDATE sync_recovery SET state='fixing' WHERE target_id=?")
        .bind(&input.decisions[0].target.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut missing = input.decisions[0].clone();
    missing.target.id = "missing-conflict".into();
    let partial = super::recovery::submit_decisions(
        &b.state,
        RecoveryDecisionInput {
            decisions: vec![
                input.decisions[0].clone(),
                input.decisions[1].clone(),
                missing,
                input.decisions[1].clone(),
            ],
            resolution: ConflictResolution::KeepLocal,
        },
    )
    .await
    .unwrap();
    assert_eq!(partial.items.len(), 3);
    assert_eq!(
        partial.items[0].code.as_deref(),
        Some("recovery_decision_in_progress")
    );
    assert_eq!(partial.items[1].state, RecoveryState::Queued);
    assert_eq!(partial.items[2].code.as_deref(), Some("not_found"));
    let saved: String =
        sqlx::query_scalar("SELECT resolution FROM sync_recovery WHERE target_id=?")
            .bind(&input.decisions[0].target.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(saved, "\"keep_remote\"");
    let replay = super::recovery::submit_decisions(
        &b.state,
        RecoveryDecisionInput {
            decisions: vec![input.decisions[0].clone()],
            resolution: ConflictResolution::KeepRemote,
        },
    )
    .await
    .unwrap();
    assert_eq!(replay.items[0].state, RecoveryState::Fixing);
}

#[tokio::test]
async fn recovery_previews_are_owner_scoped_current_and_independent_of_immich_indexing() {
    use ism_core::dto::{RecoveryKind, RecoveryTarget};

    fn jpeg(width: u32, height: u32, noisy: bool) -> Vec<u8> {
        let mut seed = 0x1234_5678_u32;
        let image = image::RgbImage::from_fn(width, height, |_, _| {
            if noisy {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                image::Rgb([seed as u8, (seed >> 8) as u8, (seed >> 16) as u8])
            } else {
                image::Rgb([200, 30, 20])
            }
        });
        let mut bytes = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, 95)
            .encode_image(&image)
            .unwrap();
        bytes
    }

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let source = jpeg(3000, 2200, true);
    assert!(source.len() > 4 * 1024 * 1024);
    let receiving = jpeg(24, 16, false);
    write_old(&a.shared.join("preview.jpg"), &source);
    write_old(&b.shared.join("preview.jpg"), &receiving);
    scan(&a).await;
    scan(&b).await;
    let source_peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &source_peer).await.unwrap_err();
    let id: String = sqlx::query_scalar("SELECT id FROM conflicts WHERE rel_path='preview.jpg'")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let target = RecoveryTarget {
        kind: RecoveryKind::Conflict,
        id,
        peer_id: None,
    };
    let reviewed = super::versions::get(&b.state, &target).await.unwrap();
    assert!(reviewed.source.files[0].modified_at.is_some());
    let receiver = super::net::peers(&a.state).await.unwrap().remove(0);
    let mut receiving_preview = Vec::new();
    for source in [false, true] {
        let query = reqwest::Url::parse_with_params(
            "http://ism.example",
            [
                ("kind", "conflict"),
                ("id", target.id.as_str()),
                ("revision", reviewed.revision.as_str()),
                ("path", "preview.jpg"),
                ("source", if source { "true" } else { "false" }),
            ],
        )
        .unwrap();
        let response = super::net::request(
            &a.state,
            &receiver.url,
            &receiver.secret,
            reqwest::Method::GET,
            &format!("/api/peer/v1/recovery/preview?{}", query.query().unwrap()),
            None,
        )
        .send()
        .await
        .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response.headers().get("cache-control").unwrap(),
            "private, no-store"
        );
        assert_eq!(
            response.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "image/jpeg"
        );
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= 4 * 1024 * 1024);
        let image = image::load_from_memory(&bytes).unwrap();
        if source {
            assert_eq!(image.width(), 1024);
            assert!(image.height() <= 1024);
        } else {
            assert_eq!((image.width(), image.height()), (24, 16));
            receiving_preview = bytes.to_vec();
        }
    }
    super::recovery::refresh(&a.state, &receiver).await.unwrap();
    let mut query = super::versions::PreviewQuery {
        target: super::versions::Query {
            kind: target.kind,
            id: target.id.clone(),
            peer_id: Some(receiver.id),
        },
        revision: reviewed.revision.clone(),
        path: "preview.jpg".into(),
        source: false,
    };
    let response = super::versions::preview(&a.state, &query).await.unwrap();
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap()
            .as_ref(),
        receiving_preview.as_slice()
    );
    query.source = true;
    let response = super::versions::preview(&a.state, &query).await.unwrap();
    assert_eq!(
        response.headers().get("cache-control").unwrap(),
        "private, no-store"
    );
    assert_eq!(
        response.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(image::load_from_memory(&bytes).unwrap().width(), 1024);
    query.source = false;
    query.target.peer_id = None;
    query.path = "../outside.jpg".into();
    assert_eq!(
        super::versions::preview(&b.state, &query)
            .await
            .unwrap_err()
            .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    query.path = "preview.jpg".into();
    write_old(&b.shared.join("preview.jpg"), b"changed after review");
    assert_eq!(
        super::versions::preview(&b.state, &query)
            .await
            .unwrap_err()
            .code(),
        "preview_changed"
    );
    assert_eq!(
        admin::resolve_reviewed(
            &b.state,
            "test",
            &target.id,
            ConflictResolution::KeepRemote,
            Some(&reviewed.revision)
        )
        .await
        .unwrap_err()
        .code(),
        "version_conflict"
    );
    assert_eq!(
        std::fs::read(b.shared.join("preview.jpg")).unwrap(),
        b"changed after review"
    );
    super::versions::expire(&b.state, &target.id);
    assert_eq!(
        super::versions::preview(&b.state, &query)
            .await
            .unwrap_err()
            .code(),
        "preview_expired"
    );
}

#[tokio::test]
async fn recovery_outgoing_fix_is_persisted_offline_and_replayed_after_restart() {
    use ism_core::dto::{RecoveryBatchInput, RecoveryKind, RecoveryState, RecoveryTarget};

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let owner_peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "INSERT INTO transfers (id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at,error)
         VALUES ('offline-repair',?,?, 'photo.xmp',?,3,'{}','failed',?,?, 'peer unavailable')",
    )
    .bind(&owner_peer.id)
    .bind(&share.id)
    .bind(crate::crypto::sha256_hex(b"xmp"))
    .bind(crate::db::now())
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    let receiver = super::net::peers(&a.state).await.unwrap().remove(0);
    super::recovery::refresh(&a.state, &receiver).await.unwrap();
    sqlx::query("UPDATE peers SET url='http://127.0.0.1:1' WHERE id=?")
        .bind(&receiver.id)
        .execute(&a.state.db)
        .await
        .unwrap();
    let request = RecoveryBatchInput {
        targets: vec![RecoveryTarget {
            kind: RecoveryKind::Transfer,
            id: "offline-repair".into(),
            peer_id: Some(receiver.id.clone()),
        }],
        all: false,
        resolution: None,
    };
    for _ in 0..2 {
        let result = super::recovery::submit(&a.state, request.clone())
            .await
            .unwrap();
        assert_eq!(result.items[0].state, RecoveryState::Queued);
    }
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outgoing_recovery_requests WHERE state!='accepted'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(
        pending, 1,
        "repeated Fix must coalesce while the owner is offline"
    );
    let restarted = AppState::new(
        Config::for_tests(a._dir.path()),
        a.state.db.clone(),
        SecretBox::open_or_create(a._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE peers SET url=? WHERE id=?")
        .bind(&b.url)
        .bind(&receiver.id)
        .execute(&a.state.db)
        .await
        .unwrap();
    let worker = tokio::spawn(super::recovery::run(restarted));
    a.tasks.push(worker);
    for _ in 0..200 {
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outgoing_recovery_requests WHERE state!='accepted'",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        if pending == 0 {
            let accepted: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sync_recovery WHERE target_id='offline-repair' AND state='queued'",
            )
            .fetch_one(&b.state.db)
            .await
            .unwrap();
            assert_eq!(
                accepted, 1,
                "only the receiving owner may execute the persisted Fix"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the persisted outgoing Fix was not delivered after restart");
}

#[tokio::test]
async fn recovery_versions_are_current_owner_scoped_and_stale_decisions_do_not_execute() {
    use ism_core::dto::{
        RecoveryDecision, RecoveryDecisionInput, RecoveryKind, RecoveryState, RecoveryTarget,
    };
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("orphan.xmp"), b"old source metadata");
    write_old(&b.shared.join("orphan.xmp"), b"receiver metadata");
    scan(&a).await;
    scan(&b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let source_share = super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let version = super::row(&a.state, &source_share.id, "orphan.xmp")
        .await
        .unwrap()
        .unwrap()
        .vv;
    super::conflict(
        &b.state,
        super::ConflictInput {
            share: &share,
            rel: "orphan.xmp",
            kind: "changed_on_both",
            detail: "Choose a version".into(),
            remote: ConflictSide {
                instance: "Alpha".into(),
                sha256: Some(crate::crypto::sha256_hex(b"old source metadata")),
                size: Some(19),
                modified_at: None,
                deleted: false,
            },
            remote_vv: &version,
            peer_id: &peer.id,
            from: None,
        },
    )
    .await
    .unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM conflicts WHERE rel_path='orphan.xmp'")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    write_old(&a.shared.join("orphan.xmp"), b"current source metadata");
    scan(&a).await;
    let target = RecoveryTarget {
        kind: RecoveryKind::Conflict,
        id,
        peer_id: None,
    };
    let current = super::versions::get(&b.state, &target).await.unwrap();
    assert_eq!(
        current.source.files[0].sha256,
        crate::crypto::sha256_hex(b"current source metadata")
    );
    assert_eq!(current.source.files[0].role, "sidecar");
    assert_eq!(
        current.receiving[0].files[0].sha256,
        crate::crypto::sha256_hex(b"receiver metadata")
    );
    let receiver = super::net::peers(&a.state).await.unwrap().remove(0);
    super::recovery::refresh(&a.state, &receiver).await.unwrap();
    let forwarded = super::versions::get(
        &a.state,
        &RecoveryTarget {
            peer_id: Some(receiver.id),
            ..target.clone()
        },
    )
    .await
    .unwrap();
    assert_eq!(forwarded.revision, current.revision);
    assert_eq!(forwarded.receiving[0].instance, "Beta");
    assert_eq!(forwarded.source.instance, "Alpha");
    write_old(&b.shared.join("orphan.xmp"), b"new receiver metadata");
    let result = super::recovery::submit_decisions(
        &b.state,
        RecoveryDecisionInput {
            decisions: vec![RecoveryDecision {
                target: target.clone(),
                revision: current.revision,
            }],
            resolution: ConflictResolution::KeepRemote,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.items[0].state, RecoveryState::Queued);
    super::versions::expire(&b.state, &target.id);
    super::recovery::tick(&b.state).await.unwrap();
    assert_eq!(
        super::recovery::get(&b.state, target.kind, &target.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        RecoveryState::NeedsDecision
    );
    let pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pending_peer_ops WHERE conflict_id=?")
            .bind(&target.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(pending, 0);
    assert_eq!(
        std::fs::read(b.shared.join("orphan.xmp")).unwrap(),
        b"new receiver metadata"
    );
    let current = super::versions::get(&b.state, &target).await.unwrap();
    super::recovery::submit_decisions(
        &b.state,
        RecoveryDecisionInput {
            decisions: vec![RecoveryDecision {
                target: target.clone(),
                revision: current.revision,
            }],
            resolution: ConflictResolution::KeepRemote,
        },
    )
    .await
    .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    eventually(
        "the reviewed current source sidecar publishes",
        &[&b],
        || {
            std::fs::read(b.shared.join("orphan.xmp")).ok().as_deref()
                == Some(b"current source metadata")
        },
    )
    .await;
}

#[tokio::test]
async fn recovery_accepts_a_verified_causal_deletion_without_resurrecting_content() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("obsolete.jpg"), b"obsolete source version");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let transfer_id: String =
        sqlx::query_scalar("SELECT id FROM transfers WHERE rel_path='obsolete.jpg'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    let deleted = crate::ops::submit(
        &a.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "delete-before-recovery-download".into(),
            label: None,
            actions: vec![OperationAction::Trash {
                source: Location {
                    root_id: "photos".into(),
                    path: "shared/obsolete.jpg".into(),
                },
                expected_revision: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&a.state, &deleted.operation_id)
        .await
        .unwrap();
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM transfers WHERE id=?")
                .bind(&transfer_id)
                .fetch_one(&b.state.db)
                .await
                .unwrap();
            if status == "failed" {
                break;
            }
            b.state.sync_wake.notify_one();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("missing source must enter recovery");
    stop_workers(&mut b).await;
    let staging = b
        .shared
        .parent()
        .unwrap()
        .join(".ism-staging/peer")
        .join(&transfer_id);
    write_old(&staging, b"obsolete source version");
    super::recovery::tick(&b.state).await.unwrap();
    let recovered = super::recovery::get(
        &b.state,
        ism_core::dto::RecoveryKind::Transfer,
        &transfer_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        recovered.state,
        ism_core::dto::RecoveryState::Fixed,
        "{recovered:?}"
    );
    assert!(!b.shared.join("obsolete.jpg").exists());
    assert_eq!(std::fs::read(staging).unwrap(), b"obsolete source version");
    let queued: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers WHERE id=?")
        .bind(&transfer_id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(queued, 0);
    super::pull(&b.state, &peer).await.unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let local = super::row(&b.state, &share.id, "obsolete.jpg")
        .await
        .unwrap()
        .unwrap();
    assert!(local.deleted);
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_inbox WHERE status!='done'")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(
        pending, 0,
        "the original change and its superseding deletion must apply"
    );
    let applied: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE origin=? AND kind='file_deleted'
         AND json_extract(data_json,'$.path')='obsolete.jpg'",
    )
    .bind(&a.state.instance_id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(applied, 1);
    let obsolete: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE origin=? AND kind='file_changed'
         AND json_extract(data_json,'$.path')='obsolete.jpg'",
    )
    .bind(&a.state.instance_id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(
        obsolete, 0,
        "superseded bytes must not be advertised as applied"
    );
    let retained: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM trash WHERE restored_at IS NULL AND purged_at IS NULL",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(
        retained, 1,
        "recovery must not restore deliberately deleted bytes"
    );
}

#[tokio::test]
async fn recovery_follows_committed_folder_moves_before_retrying_content() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("old/photo.jpg"), b"moved content");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let transfer_id: String =
        sqlx::query_scalar("SELECT id FROM transfers WHERE rel_path='old/photo.jpg'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    let accepted = crate::ops::submit(
        &a.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "recovery-folder-move".into(),
            label: Some("Move source folder".into()),
            actions: vec![OperationAction::MoveFolder {
                source: Location {
                    root_id: "photos".into(),
                    path: "shared/old".into(),
                },
                target: Location {
                    root_id: "photos".into(),
                    path: "shared/current".into(),
                },
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&a.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&a.state, &accepted.operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Completed
    );
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM transfers WHERE id=?")
                .bind(&transfer_id)
                .fetch_one(&b.state.db)
                .await
                .unwrap();
            if status == "failed" {
                break;
            }
            b.state.sync_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("missing queued source must be retained for recovery");
    stop_workers(&mut b).await;
    super::recovery::tick(&b.state).await.unwrap();
    let (path, status): (String, String) =
        sqlx::query_as("SELECT rel_path,status FROM transfers WHERE id=?")
            .bind(&transfer_id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(
        path,
        "current/photo.jpg",
        "recovery={:?}; moves={:?}",
        super::recovery::get(
            &b.state,
            ism_core::dto::RecoveryKind::Transfer,
            &transfer_id
        )
        .await
        .unwrap(),
        sqlx::query_as::<_, (String, String)>(
            "SELECT kind,data_json FROM event_log WHERE kind='folder_moved'",
        )
        .fetch_all(&a.state.db)
        .await
        .unwrap(),
    );
    assert_eq!(status, "queued");
    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    eventually("publication at recovered path", &[&a, &b], || {
        std::fs::read(b.shared.join("current/photo.jpg"))
            .ok()
            .as_deref()
            == Some(b"moved content")
    })
    .await;
    assert!(!b.shared.join("old/photo.jpg").exists());
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            let completed:bool=sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM transfer_history h JOIN operations o ON o.id=h.operation_id
                 WHERE h.id=? AND o.status='completed')",
            ).bind(&transfer_id).fetch_one(&b.state.db).await.unwrap();
            if completed {break;}
            b.state.ops_wake.notify_one();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("publication must commit its bookkeeping");
    super::pull(&b.state, &peer).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_log WHERE kind='folder_moved'")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        1,
        "the original folder event must complete after its recovered content",
    );
    let source_cursor: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT cursor FROM peers WHERE id=?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        source_cursor,
    );
    let admission = super::recovery::submit(
        &b.state,
        ism_core::dto::RecoveryBatchInput {
            targets: vec![ism_core::dto::RecoveryTarget {
                kind: ism_core::dto::RecoveryKind::Transfer,
                id: transfer_id.clone(),
                peer_id: None,
            }],
            all: false,
            resolution: None,
        },
    )
    .await
    .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    assert_eq!(
        super::recovery::get(
            &b.state,
            ism_core::dto::RecoveryKind::Transfer,
            &transfer_id
        )
        .await
        .unwrap()
        .unwrap()
        .state,
        ism_core::dto::RecoveryState::Fixed,
        "admission={admission:?}",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_peer_events")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn recovery_replacement_transfer_does_not_wait_on_its_failed_parent() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"recover publication");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    let operation = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let id: Option<String> = sqlx::query_scalar(
                "SELECT operation_id FROM transfer_history WHERE rel_path='photo.jpg'",
            )
            .fetch_optional(&b.state.db)
            .await
            .unwrap();
            if let Some(id) = id {
                break id;
            }
            b.state.sync_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    stop_workers(&mut b).await;
    std::fs::create_dir(b.shared.join("photo.jpg")).unwrap();
    crate::ops::exec::run_operation(&b.state, &operation)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &operation)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Failed
    );
    std::fs::remove_dir(b.shared.join("photo.jpg")).unwrap();
    let upload: String = sqlx::query_scalar("SELECT id FROM uploads WHERE operation_id=?")
        .bind(&operation)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let root = crate::files::root(&b.state.db, "photos").await.unwrap();
    std::fs::remove_file(
        crate::ops::plan::staging_dir(&root, &upload)
            .unwrap()
            .join("0"),
    )
    .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    let transfer: String = sqlx::query_scalar(
        "SELECT target_id FROM sync_recovery_links WHERE recovery_id=? AND kind='transfer'",
    )
    .bind(format!("operation:{operation}"))
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    super::inbox::require_stable_transfer(&b.state, &share, &["photo.jpg"], &transfer)
        .await
        .unwrap();
    assert_eq!(
        super::inbox::require_stable_transfer(
            &b.state,
            &share,
            &["photo.jpg"],
            "unrelated-transfer"
        )
        .await
        .unwrap_err()
        .code(),
        "recovery_required",
        "ordinary transfers must still respect the parent's footprint",
    );
    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            let completed:bool=sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM transfer_history h JOIN operations o ON o.id=h.operation_id
                 WHERE h.id=? AND o.status='completed')",
            ).bind(&transfer).fetch_one(&b.state.db).await.unwrap();
            if completed {break;}
            b.state.sync_wake.notify_waiters();
            b.state.ops_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("the authorized recovery must make progress");
    super::recovery::submit(
        &b.state,
        ism_core::dto::RecoveryBatchInput {
            targets: vec![ism_core::dto::RecoveryTarget {
                kind: ism_core::dto::RecoveryKind::Operation,
                id: operation.clone(),
                peer_id: None,
            }],
            all: false,
            resolution: None,
        },
    )
    .await
    .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    assert_eq!(
        std::fs::read(b.shared.join("photo.jpg")).unwrap(),
        b"recover publication"
    );
    assert_eq!(
        crate::ops::load(&b.state, &operation)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Completed
    );
}

#[tokio::test]
async fn recovery_waiting_for_a_mount_keeps_its_retry_budget_and_resumes() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let created = crate::ops::submit(
        &a.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "recovery-mount".into(),
            label: None,
            actions: vec![OperationAction::CreateFolder {
                target: loc("shared/recovered"),
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&a.state, &created.operation_id)
        .await
        .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let operation: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let root = crate::files::root(&b.state.db, "photos").await.unwrap();
    let marker = root.base().join(".ism-root");
    std::fs::remove_file(&marker).unwrap();
    crate::ops::exec::run_operation(&b.state, &operation)
        .await
        .unwrap();
    let batch = ism_core::dto::RecoveryBatchInput {
        targets: vec![ism_core::dto::RecoveryTarget {
            kind: ism_core::dto::RecoveryKind::Operation,
            id: operation.clone(),
            peer_id: None,
        }],
        all: false,
        resolution: None,
    };
    for _ in 0..3 {
        super::recovery::submit(&b.state, batch.clone())
            .await
            .unwrap();
        super::recovery::tick(&b.state).await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT attempts FROM sync_recovery WHERE target_id=?")
            .bind(&operation)
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM operations")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        1,
        "waiting must not create doomed replacement operations"
    );
    assert_eq!(
        super::recovery::get(&b.state, ism_core::dto::RecoveryKind::Operation, &operation)
            .await
            .unwrap()
            .unwrap()
            .state,
        ism_core::dto::RecoveryState::Waiting
    );
    std::fs::write(marker, "photos\n").unwrap();
    super::recovery::submit(&b.state, batch.clone())
        .await
        .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    let replacement: String = sqlx::query_scalar(
        "SELECT target_id FROM sync_recovery_links WHERE recovery_id=? AND kind='operation'",
    )
    .bind(format!("operation:{operation}"))
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &replacement)
        .await
        .unwrap();
    super::recovery::submit(&b.state, batch).await.unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    assert!(b.shared.join("recovered").is_dir());
    assert_eq!(
        crate::ops::load(&b.state, &operation)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Completed
    );
}

async fn gate_peer_content(
    a: &mut Instance,
    b: &Instance,
) -> tokio::sync::mpsc::UnboundedReceiver<(String, tokio::sync::oneshot::Sender<()>)> {
    let (started, requests) = tokio::sync::mpsc::unbounded_channel();
    let source = crate::routes::router(a.state.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let started = started.clone();
            async move {
                if request.uri().path() == "/api/peer/v1/content" {
                    let query = axum::extract::Query::<std::collections::HashMap<String, String>>::try_from_uri(request.uri()).unwrap();
                    let (release, wait) = tokio::sync::oneshot::channel();
                    started.send((query["path"].clone(), release)).unwrap();
                    wait.await.unwrap();
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_url = format!("http://{}", listener.local_addr().unwrap());
    a.tasks.push(tokio::spawn(async move {
        axum::serve(listener, source).await.unwrap();
    }));
    sqlx::query("UPDATE peers SET url = ?, status = 'online'")
        .bind(source_url)
        .execute(&b.state.db)
        .await
        .unwrap();
    requests
}

#[tokio::test]
async fn large_peer_transfers_use_bounded_ranges() {
    const MAX_RANGE: u64 = 16 * 1024 * 1024;

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let contents = vec![7_u8; MAX_RANGE as usize + 1];
    write_old(&a.shared.join("large.mov"), &contents);
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();

    let (ranges_tx, mut ranges_rx) = tokio::sync::mpsc::unbounded_channel();
    let source = crate::routes::router(a.state.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let ranges_tx = ranges_tx.clone();
            async move {
                if request.uri().path() == "/api/peer/v1/content" {
                    let range = request
                        .headers()
                        .get(axum::http::header::RANGE)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.strip_prefix("bytes="))
                        .and_then(|value| value.split_once('-'))
                        .and_then(|(start, end)| {
                            Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?))
                        });
                    let Some((start, end)) = range.filter(|(start, end)| end >= start) else {
                        return axum::response::Response::builder()
                            .status(axum::http::StatusCode::GATEWAY_TIMEOUT)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    };
                    if end - start + 1 > MAX_RANGE {
                        return axum::response::Response::builder()
                            .status(axum::http::StatusCode::GATEWAY_TIMEOUT)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    ranges_tx.send((start, end)).unwrap();
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_url = format!("http://{}", listener.local_addr().unwrap());
    a.tasks.push(tokio::spawn(async move {
        axum::serve(listener, source).await.unwrap();
    }));
    sqlx::query("UPDATE peers SET url = ?, status = 'online'")
        .bind(source_url)
        .execute(&b.state.db)
        .await
        .unwrap();
    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if std::fs::read(b.shared.join("large.mov")).ok().as_deref() == Some(&contents) {
                break;
            }
            b.state.sync_wake.notify_waiters();
            b.state.ops_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("bounded transfer should complete");

    let mut ranges = Vec::new();
    while let Ok(range) = ranges_rx.try_recv() {
        ranges.push(range);
    }
    assert!(ranges.len() >= 2);
    assert!(
        ranges
            .iter()
            .all(|(start, end)| end >= start && end - start < MAX_RANGE)
    );
}

#[tokio::test]
async fn three_independent_transfers_refill_slots_and_update_receiver_activity() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    // Manual snapshot refreshes below exercise legacy timestamp freshness, without a stream.
    a.state.peer_streams.retain(&[]);
    b.state.peer_streams.retain(&[]);
    for name in ["one.jpg", "two.jpg", "three.jpg", "four.jpg", "five.jpg"] {
        write_old(&a.shared.join(name), name.as_bytes());
    }
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let mut requests = gate_peer_content(&mut a, &b).await;

    let client = reqwest::Client::new();
    let setup = client
        .post(format!("{}/api/admin/setup", b.url))
        .header("x-ism-request", "1")
        .json(&serde_json::json!({
            "username": "admin", "password": "synthetic test password", "instance_name": "Beta"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let cookie = setup.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    async fn page(b: &Instance, cookie: &str, status: &str) -> ism_core::dto::SyncPage {
        reqwest::Client::new()
            .get(format!("{}/api/admin/sync/page?status={status}", b.url))
            .header("cookie", cookie)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    let mut pending = Vec::new();
    for _ in 0..3 {
        pending.push(
            tokio::time::timeout(Duration::from_secs(3), requests.recv())
                .await
                .expect("three downloads must reach the source concurrently")
                .unwrap(),
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), requests.recv())
            .await
            .is_err(),
        "a fourth download must wait for a free slot"
    );
    let active = page(&b, &cookie, "active").await;
    assert_eq!(active.total, 5);
    assert_eq!(
        active.rows.iter().filter(|r| r.status == "running").count(),
        3
    );
    assert_eq!(
        active.rows.iter().filter(|r| r.status == "queued").count(),
        2
    );
    assert!(
        active
            .rows
            .iter()
            .all(|r| r.source == "ISM: Alpha" && r.destination == "ISM: Beta")
    );
    assert_eq!(page(&b, &cookie, "completed").await.total, 0);
    let destination = super::net::peers(&a.state).await.unwrap().remove(0);
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    let outgoing = super::activity::page(&a.state, "active", 2, 0, None)
        .await
        .unwrap();
    assert_eq!(outgoing.total, 5);
    assert_eq!(
        outgoing.rows.len(),
        2,
        "the source queue must stay paginated"
    );
    assert!(outgoing.rows.iter().all(|row| row.outgoing
        && row.source == "ISM: Alpha"
        && row.destination == "ISM: Beta"
        && row.location.path.starts_with("shared/")));
    assert_eq!(outgoing.progress.files_total, 5);
    assert_eq!(outgoing.progress.files_completed, 0);
    assert_eq!(outgoing.progress.bytes_total, active.progress.bytes_total);

    let (first, release) = pending.pop().unwrap();
    release.send(()).unwrap();
    pending.push(
        tokio::time::timeout(Duration::from_secs(3), requests.recv())
            .await
            .expect("the next image must start while the other two remain blocked")
            .unwrap(),
    );
    let publishing = page(&b, &cookie, "active").await;
    let received = publishing
        .rows
        .iter()
        .find(|r| r.relative_path == first)
        .unwrap();
    assert_eq!(received.status, "publishing");
    assert!(received.finished_at.is_none());
    assert_eq!(page(&b, &cookie, "completed").await.total, 0);
    crate::ops::exec::run_operation(&b.state, received.operation_id.as_deref().unwrap())
        .await
        .unwrap();
    let completed = page(&b, &cookie, "completed").await;
    assert_eq!(completed.total, 1);
    assert_eq!(completed.rows[0].relative_path, first);
    assert!(completed.rows[0].finished_at.is_some());
    assert_eq!(
        std::fs::read(b.shared.join(&first)).unwrap(),
        first.as_bytes()
    );
    assert_eq!(page(&b, &cookie, "active").await.total, 4);
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    let outgoing = super::activity::page(&a.state, "completed", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(outgoing.total, 1);
    assert_eq!(outgoing.rows[0].relative_path, first);
    assert_eq!(outgoing.progress.files_completed, 1);
    assert_eq!(outgoing.progress.bytes_done, first.len() as u64);

    super::activity::unavailable(&a.state, &destination.id, "unreachable").await;
    let stale = super::activity::page(&a.state, "active", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(
        stale.total, 4,
        "keep the last-known queue while disconnected"
    );
    assert_eq!(stale.warnings.len(), 1);
    assert_eq!(stale.progress.bytes_per_second, 0.0);
    assert!(stale.progress.eta_seconds.is_none());

    b.tasks
        .push(tokio::spawn(crate::ops::exec::run(b.state.clone())));
    pending.pop().unwrap().1.send(()).unwrap();
    pending.push(
        tokio::time::timeout(Duration::from_secs(3), requests.recv())
            .await
            .expect("each completed task must independently refill its slot")
            .unwrap(),
    );
    for (_, release) in pending {
        release.send(()).unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if page(&b, &cookie, "completed").await.total == 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(page(&b, &cookie, "active").await.total, 0);
    for name in ["one.jpg", "two.jpg", "three.jpg", "four.jpg", "five.jpg"] {
        assert_eq!(std::fs::read(b.shared.join(name)).unwrap(), name.as_bytes());
    }
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    let outgoing = super::activity::page(&a.state, "completed", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(outgoing.total, 5);
    assert_eq!(outgoing.progress.files_completed, 5);
    assert_eq!(outgoing.progress.bytes_done, outgoing.progress.bytes_total);
    assert!(outgoing.warnings.is_empty());
    // Mirrored outgoing records must not bounce back as new incoming transfers.
    super::activity::refresh(&b.state, &peer).await.unwrap();
    assert_eq!(page(&b, &cookie, "completed").await.total, 5);
    sqlx::query("DELETE FROM transfer_history WHERE rel_path = ?")
        .bind(&first)
        .execute(&b.state.db)
        .await
        .unwrap();
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    assert_eq!(
        super::activity::page(&a.state, "completed", 250, 0, None)
            .await
            .unwrap()
            .total,
        4
    );
}

#[tokio::test]
async fn source_queue_keeps_mirroring_while_replication_is_waiting_for_the_operation_barrier() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    sqlx::query("DELETE FROM peer_sync_state")
        .execute(&a.state.db)
        .await
        .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"synthetic"),
        1000,
        &ism_core::version::VersionVector::default(),
    )
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO transfer_history
         (id,peer_id,share_id,rel_path,sha256,size,queued_at,finished_at,run_id)
         SELECT 'completed-photo',peer_id,share_id,'done.jpg',sha256,size,queued_at,?,run_id
         FROM transfers",
    )
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE transfers SET status = 'running'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let _operation_guard = a.state.replication_operation_lock.lock().await;
    a.tasks.push(tokio::spawn(super::run(a.state.clone())));
    async fn mirrored(source: &Instance, bytes: u64) -> ism_core::dto::SyncPage {
        tokio::time::timeout(Duration::from_secs(7), async {
            loop {
                let page = super::activity::page(&source.state, "active", 250, 0, None)
                    .await
                    .unwrap();
                if page.total == 1 && page.rows[0].bytes_done == bytes {
                    return page;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("source queue must mirror the receiver even while replication is blocked")
    }
    let initial = mirrored(&a, 0).await;
    assert!(initial.rows[0].outgoing);
    assert_eq!(initial.rows[0].status, "running");
    assert_eq!(initial.progress.files_total, 2);
    assert_eq!(initial.progress.files_completed, 1);
    assert_eq!(initial.progress.bytes_total, 2000);
    assert_eq!(initial.progress.bytes_done, 1000);
    assert!(initial.warnings.is_empty());
    let completed = super::activity::page(&a.state, "completed", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(completed.total, 1);
    assert_eq!(completed.rows[0].relative_path, "done.jpg");
    assert!(completed.rows[0].outgoing);

    let id: String = sqlx::query_scalar("SELECT id FROM transfers")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert!(
        super::activity::record_progress(&b.state, &id, &peer.id, 400, 400)
            .await
            .unwrap()
    );
    let updated = mirrored(&a, 400).await;
    assert_eq!(updated.progress.bytes_done, 1400);
    assert!(updated.progress.bytes_per_second > 0.0);
}

#[tokio::test]
async fn sync_activity_warns_until_the_receivers_first_status_response() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    a.state.peer_streams.retain(&[]);
    sqlx::query("DELETE FROM peer_sync_state")
        .execute(&a.state.db)
        .await
        .unwrap();
    let unknown = super::activity::page(&a.state, "active", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(unknown.warnings.len(), 1);
    assert!(unknown.warnings[0].contains("No recent status from the receiver"));

    let peer = super::net::peers(&a.state).await.unwrap().remove(0);
    super::activity::refresh(&a.state, &peer).await.unwrap();
    let idle = super::activity::page(&a.state, "active", 250, 0, None)
        .await
        .unwrap();
    assert!(idle.warnings.is_empty());
    assert_eq!(idle.total, 0);
}

#[tokio::test]
async fn mirrored_sync_drains_multiple_pages_and_never_exposes_another_peers_queue() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let source = super::net::peers(&b.state).await.unwrap().remove(0);
    let destination = super::net::peers(&a.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let secret = b
        .state
        .secrets
        .encrypt(super::net::SECRET_PURPOSE, "synthetic peer secret")
        .unwrap();
    sqlx::query(
        "INSERT INTO peers (id,name,url,secret_enc,status,created_at)
         VALUES ('unrelated','Unrelated','https://unrelated.example',?,'offline',?)",
    )
    .bind(secret)
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 501)
         INSERT INTO transfers (id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
         SELECT printf('transfer-%04d',i),?1,?2,printf('photo-%04d.jpg',i),?3,100,'{}','queued',?4,?4 FROM n",
    )
    .bind(&source.id).bind(&share.id).bind(crate::crypto::sha256_hex(b"synthetic"))
    .bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    sqlx::query(
        "INSERT INTO transfers (id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
         VALUES ('private-transfer','unrelated',?,'private.jpg',?,100,'{}','queued',?,?)",
    ).bind(&share.id).bind(crate::crypto::sha256_hex(b"private synthetic"))
    .bind(crate::db::now()).bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    let first = super::activity::page(&a.state, "active", 250, 0, None)
        .await
        .unwrap();
    let last = super::activity::page(&a.state, "active", 250, 500, None)
        .await
        .unwrap();
    assert_eq!(first.total, 501);
    assert_eq!(first.rows.len(), 250);
    assert_eq!(last.rows.len(), 1);
    assert_eq!(last.rows[0].id, "transfer-0501");
    assert!(first.rows.iter().all(|r| r.relative_path != "private.jpg"));

    sqlx::query(
        "UPDATE transfers SET bytes_done = 75,status = 'running' WHERE id = 'transfer-0001'",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    let updated = super::activity::page(&a.state, "active", 1, 0, None)
        .await
        .unwrap();
    assert_eq!(
        updated.total, 501,
        "incremental updates must not duplicate queue entries"
    );
    assert_eq!(updated.rows[0].bytes_done, 75);
    assert_eq!(updated.rows[0].status, "running");
    let cursor: i64 = sqlx::query_scalar("SELECT cursor FROM peer_sync_state WHERE peer_id = ?")
        .bind(&destination.id)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    super::activity::refresh(&a.state, &destination)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT cursor FROM peer_sync_state WHERE peer_id = ?")
            .bind(&destination.id)
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
        cursor
    );

    sqlx::query("UPDATE peer_sync_state SET updated_at = '2020-01-01T00:00:00Z'")
        .execute(&a.state.db)
        .await
        .unwrap();
    let stale = super::activity::page(&a.state, "active", 1, 0, None)
        .await
        .unwrap();
    assert_eq!(stale.total, 501);
    assert_eq!(stale.warnings.len(), 1);
    assert!(stale.progress.eta_seconds.is_none());
}

#[tokio::test]
async fn sync_runs_use_last_minute_bytes_for_eta_and_reset_without_losing_history() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    a.state.peer_streams.retain(&[]);
    b.state.peer_streams.retain(&[]);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::activity::refresh(&b.state, &peer).await.unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "first.jpg",
        &crate::crypto::sha256_hex(b"synthetic"),
        1000,
        &ism_core::version::VersionVector::default(),
    )
    .await
    .unwrap();
    let now = chrono::Utc::now();
    sqlx::query("UPDATE sync_runs SET started_at = ?")
        .bind(crate::db::ts(now - chrono::Duration::minutes(2)))
        .execute(&b.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE transfers SET bytes_done = 400, status = 'running'")
        .execute(&b.state.db)
        .await
        .unwrap();
    for (age, bytes) in [(61_000, 60_000), (30_000, 600)] {
        sqlx::query("INSERT INTO sync_byte_samples (peer_id,at,bytes) VALUES (?,?,?)")
            .bind(&peer.id)
            .bind(now.timestamp_millis() - age)
            .bind(bytes)
            .execute(&b.state.db)
            .await
            .unwrap();
    }
    let progress = super::activity::page(&b.state, "active", 250, 0, None)
        .await
        .unwrap()
        .progress;
    assert_eq!(progress.files_total, 1);
    assert_eq!(progress.files_completed, 0);
    assert_eq!(progress.bytes_done, 400);
    assert_eq!(progress.bytes_total, 1000);
    assert_eq!(
        progress.bytes_per_second, 10.0,
        "ignore samples older than a minute"
    );
    assert_eq!(
        progress.eta_seconds,
        Some(60),
        "600 bytes remain at 10 bytes/sec"
    );
    sqlx::query("UPDATE sync_byte_samples SET at = ?")
        .bind(now.timestamp_millis() - 61_000)
        .execute(&b.state.db)
        .await
        .unwrap();
    let idle = super::activity::page(&b.state, "active", 250, 0, None)
        .await
        .unwrap()
        .progress;
    assert_eq!(idle.bytes_per_second, 0.0);
    assert!(
        idle.eta_seconds.is_none(),
        "never invent an ETA without throughput"
    );
    sqlx::query(
        "INSERT INTO transfer_history (id,peer_id,share_id,rel_path,sha256,size,queued_at,finished_at,run_id)
         SELECT id,peer_id,share_id,rel_path,sha256,size,queued_at,?,run_id FROM transfers",
    ).bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    sqlx::query("DELETE FROM transfers")
        .execute(&b.state.db)
        .await
        .unwrap();
    let finished = super::activity::page(&b.state, "completed", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(finished.progress.files_completed, 1);
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "second.jpg",
        &crate::crypto::sha256_hex(b"new synthetic"),
        2000,
        &ism_core::version::VersionVector::default(),
    )
    .await
    .unwrap();
    let next = super::activity::page(&b.state, "active", 250, 0, None)
        .await
        .unwrap();
    assert_eq!(next.progress.files_total, 1);
    assert_eq!(next.progress.files_completed, 0);
    assert_eq!(next.progress.bytes_total, 2000);
    assert_eq!(next.progress.bytes_done, 0);
    assert_eq!(
        super::activity::page(&b.state, "completed", 250, 0, None)
            .await
            .unwrap()
            .total,
        1
    );
}

#[tokio::test]
async fn downloads_recheck_recovery_and_local_changes_before_publication() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"remote photo");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let mut requests = gate_peer_content(&mut a, &b).await;
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    let (_, release) = tokio::time::timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "INSERT INTO operations (id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES ('interrupted','interrupted','admin:test','admin','{}','recovery_required',?)",
    )
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (status, attempts): (String, i64) =
                sqlx::query_as("SELECT status, attempts FROM transfers")
                    .fetch_one(&b.state.db)
                    .await
                    .unwrap();
            assert_eq!(attempts, 0, "recovery must not consume retries");
            if status == "queued" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!b.shared.join("photo.jpg").exists());
    let history: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfer_history")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(history, 0, "an interrupted download is not completed");

    sqlx::query("UPDATE operations SET recovery_resolved = 1 WHERE id = 'interrupted'")
        .execute(&b.state.db)
        .await
        .unwrap();
    b.state.sync_wake.notify_waiters();
    write_old(&b.shared.join("photo.jpg"), b"local edit during download");
    scan(&b).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            b.state.sync_wake.notify_waiters();
            if !admin::list_conflicts(&b.state, false)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(b.shared.join("photo.jpg")).unwrap(),
        b"local edit during download"
    );
    let history: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfer_history")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(history, 0, "conflicted files must not appear completed");
}

#[tokio::test]
async fn share_scan_excludes_synology_metadata_but_replicates_originals_and_companions() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let ordinary = [
        "Edited/pilot/source.png",
        "Edited/pilot/source.png.xmp",
        "Edited/pilot/source.png.json",
        "Edited/pilot/source.png.xml",
        "@ordinary/photo.jpg",
        ".ordinary/photo.jpg",
        "Edited/@eaDirectory/photo.jpg",
    ];
    for path in ordinary {
        write_old(&a.shared.join(path), b"original or companion");
    }
    for directory in ["@eaDir", "Edited/pilot/@eaDir"] {
        for name in [
            "SYNOPHOTO_THUMB_XL.jpg",
            "SYNOPHOTO_THUMB_M.jpg",
            "SYNOPHOTO_THUMB_SM.jpg",
            "SYNOPHOTO_THUMB_B.jpg",
            "SYNOPHOTO_THUMB_S.jpg",
            "SYNOINDEX_MEDIA_INFO",
        ] {
            write_old(
                &a.shared.join(directory).join("source.png").join(name),
                b"generated metadata",
            );
        }
    }
    for path in [".ism-staging/tmp.jpg", "Edited/.ism-trash/old.jpg"] {
        write_old(&a.shared.join(path), b"reserved");
    }
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let queued: Vec<String> = sqlx::query_scalar(
        "SELECT rel_path FROM transfers WHERE status = 'queued' ORDER BY rel_path",
    )
    .fetch_all(&b.state.db)
    .await
    .unwrap();
    assert!(
        queued
            .iter()
            .all(|path| !path.split('/').any(|p| p == "@eaDir")),
        "Synology metadata entered the transfer queue: {queued:?}"
    );
    let mut expected = ordinary.map(str::to_owned).to_vec();
    expected.sort();
    assert_eq!(queued, expected);
    let tracked: Vec<String> =
        sqlx::query_scalar("SELECT rel_path FROM replica_files ORDER BY rel_path")
            .fetch_all(&a.state.db)
            .await
            .unwrap();
    assert_eq!(tracked, expected);
}

#[tokio::test]
async fn tracked_synology_metadata_is_not_deleted_advertised_or_served() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let share = super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::net::peers(&a.state).await.unwrap().remove(0);
    let mut vv = ism_core::version::VersionVector::default();
    vv.increment(&a.state.instance_id);
    let old = super::Row {
        size: 5,
        mtime_ns: 1,
        sha256: Some(crate::crypto::sha256_hex(b"thumb")),
        deleted: false,
        vv,
    };
    let metadata = "Edited/@eaDir/photo.jpg/thumb.jpg";
    write_old(&a.shared.join(metadata), b"thumb");
    super::put_row(&a.state, &share.id, metadata, &old)
        .await
        .unwrap();
    // Include a missing tracked file and a historical tombstone as well as a file on disk.
    for (path, deleted) in [
        ("Edited/@eaDir/missing.jpg", false),
        ("Edited/@eaDir/deleted.jpg", true),
    ] {
        super::put_row(
            &a.state,
            &share.id,
            path,
            &super::Row {
                deleted,
                ..old.clone()
            },
        )
        .await
        .unwrap();
    }
    let before: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    for (kind, path, from) in [
        ("file_changed", metadata, None),
        ("file_deleted", "Edited/@eaDir/deleted.jpg", None),
        ("file_moved", "rescued.jpg", Some(metadata)),
        ("file_moved", metadata, Some("ordinary.jpg")),
    ] {
        super::append_local(
            &a.state,
            &share.name,
            kind,
            &super::EventData {
                path: path.into(),
                from: from.map(str::to_owned),
                sha256: old.sha256.clone(),
                size: Some(5),
                vv: old.vv.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    let last: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    scan(&a).await;
    super::record_local(
        &a.state,
        None,
        &[
            super::LocalChange::FileTrashed {
                at: loc(&format!("shared/{metadata}")),
            },
            super::LocalChange::FolderCreated {
                at: loc("shared/Edited/@eaDir/new"),
            },
            super::LocalChange::FileMoved {
                from: loc(&format!("shared/{metadata}")),
                to: loc("shared/rescued.jpg"),
            },
        ],
    )
    .await
    .unwrap();
    let after: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(
        after, last,
        "ignoring metadata must not emit deletions or moves"
    );
    for path in [metadata, "Edited/@eaDir/missing.jpg"] {
        let row = super::row(&a.state, &share.id, path)
            .await
            .unwrap()
            .unwrap();
        assert!(!row.deleted);
        assert_eq!(row.vv, old.vv);
    }
    let page = super::events_for(&a.state, &peer.id, &b.state.instance_id, before)
        .await
        .unwrap();
    assert!(page.events.is_empty());
    assert_eq!(page.next, last);
    // More than a page of legacy metadata must not produce an empty, invalid inventory page.
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 501)
         INSERT INTO replica_files (share_id, rel_path, size, mtime_ns, deleted, vv_json)
         SELECT ?, '@eaDir/old-' || i, 0, 0, 1, '{}' FROM n",
    )
    .bind(&share.id)
    .execute(&a.state.db)
    .await
    .unwrap();
    write_old(&a.shared.join("z-original.png"), b"original");
    scan(&a).await;
    let inventory = super::inventory_for(&a.state, &peer.id, &share.name, "")
        .await
        .unwrap();
    inventory.validate("").unwrap();
    assert_eq!(inventory.entries.len(), 1);
    assert_eq!(inventory.entries[0].path, "z-original.png");
    assert!(inventory.next.is_none());
    let receiver_peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let uri = format!(
        "/api/peer/v1/content?share=Family&path={}&sha256={}",
        super::transfer::encode(metadata),
        old.sha256.as_deref().unwrap()
    );
    let response = super::net::request(
        &b.state,
        &receiver_peer.url,
        &receiver_peer.secret,
        reqwest::Method::GET,
        &uri,
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(std::fs::read(a.shared.join(metadata)).unwrap(), b"thumb");
}

#[tokio::test]
async fn legacy_synology_events_and_backfill_do_not_apply_metadata_or_stall_the_cursor() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let mut peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let server = MockServer::start().await;
    peer.url = server.uri();
    let metadata = "Edited/@eaDir/photo.jpg/thumb.jpg";
    write_old(&b.shared.join(metadata), b"local metadata");
    write_old(&b.shared.join("@eaDir/deleted.jpg"), b"keep metadata");
    let mut vv = ism_core::version::VersionVector::default();
    vv.increment(&a.state.instance_id);
    let data = super::EventData {
        path: metadata.into(),
        sha256: Some(crate::crypto::sha256_hex(b"remote")),
        size: Some(6),
        vv: vv.clone(),
        ..Default::default()
    };
    let mut events = Vec::new();
    for (kind, path, from) in [
        ("file_changed", metadata, None),
        ("file_deleted", metadata, None),
        ("folder_created", "Edited/@eaDir/new", None),
        ("folder_trashed", "Edited/@eaDir/photo.jpg", None),
        ("file_moved", "rescued.jpg", Some(metadata)),
        ("folder_moved", "rescued", Some("Edited/@eaDir")),
        ("file_moved", metadata, Some("original.jpg")),
        ("file_changed", "original.jpg", None),
    ] {
        events.push(super::PeerEvent {
            seq: peer.cursor + events.len() as i64 + 1,
            origin: a.state.instance_id.clone(),
            origin_seq: 100 + events.len() as i64,
            share: share.name.clone(),
            kind: kind.into(),
            data: super::EventData {
                path: path.into(),
                from: from.map(str::to_owned),
                ..data.clone()
            },
        });
    }
    let last = events.last().unwrap().seq;
    Mock::given(matchers::path("/api/peer/v1/events"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(super::EventPage { events, next: last }),
        )
        .mount(&server)
        .await;
    super::pull(&b.state, &peer).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT received_cursor FROM peers WHERE id = ?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        last
    );
    assert!(super::net::peer(&b.state, &peer.id).await.unwrap().cursor < last);
    let mut tx = b.state.db.begin().await.unwrap();
    super::mark_backfill_in(&mut tx, &share.id, &peer.id)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    Mock::given(matchers::path("/api/peer/v1/inventory"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(super::InventoryPage {
                entries: vec![
                    super::InventoryEntry {
                        path: "@eaDir/deleted.jpg".into(),
                        sha256: None,
                        size: 0,
                        mtime_ns: 0,
                        deleted: true,
                        vv: vv.clone(),
                    },
                    super::InventoryEntry {
                        path: metadata.into(),
                        sha256: data.sha256.clone(),
                        size: 6,
                        mtime_ns: 0,
                        deleted: false,
                        vv: vv.clone(),
                    },
                    super::InventoryEntry {
                        path: "original.jpg".into(),
                        sha256: data.sha256.clone(),
                        size: 6,
                        mtime_ns: 0,
                        deleted: false,
                        vv,
                    },
                ],
                next: None,
            }),
        )
        .mount(&server)
        .await;
    super::backfill(&b.state, &peer).await.unwrap();
    let queued: Vec<String> = sqlx::query_scalar("SELECT rel_path FROM transfers")
        .fetch_all(&b.state.db)
        .await
        .unwrap();
    assert_eq!(queued, ["original.jpg"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM operations")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        std::fs::read(b.shared.join(metadata)).unwrap(),
        b"local metadata"
    );
    assert_eq!(
        std::fs::read(b.shared.join("@eaDir/deleted.jpg")).unwrap(),
        b"keep metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM share_backfills")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        0
    );
    server.reset().await;
    peer.cursor = super::net::peer(&b.state, &peer.id).await.unwrap().cursor;
    Mock::given(matchers::path("/api/peer/v1/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(super::EventPage {
            events: vec![super::PeerEvent {
                seq: last + 1,
                origin: a.state.instance_id.clone(),
                origin_seq: 200,
                share: share.name,
                kind: "file_changed".into(),
                data: super::EventData {
                    sha256: None,
                    ..data
                },
            }],
            next: last + 1,
        }))
        .mount(&server)
        .await;
    assert!(super::pull(&b.state, &peer).await.is_err());
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor,
        "exclusion must not bypass event validation"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT received_cursor FROM peers WHERE id = ?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        last
    );
}

#[tokio::test]
async fn legacy_queued_synology_metadata_fails_without_adoption_or_publication() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let metadata = "Edited/@eaDir/photo.jpg/thumb.jpg";
    write_old(&b.shared.join(metadata), b"thumb");
    assert!(
        super::queue_transfer(
            &b.state,
            &peer.id,
            &share,
            metadata,
            &crate::crypto::sha256_hex(b"thumb"),
            5,
            &Default::default(),
        )
        .await
        .is_err()
    );
    // Seed pre-upgrade state directly: one adoptable copy and one interrupted download.
    for (id, path, status) in [
        ("legacy-copy", metadata, "queued"),
        ("legacy-download", "@eaDir/missing.jpg", "running"),
    ] {
        sqlx::query(
            "INSERT INTO transfers (id, peer_id, share_id, rel_path, sha256, size, vv_json,
                                    status, queued_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 5, '{}', ?, ?, ?)",
        )
        .bind(id)
        .bind(&peer.id)
        .bind(&share.id)
        .bind(path)
        .bind(crate::crypto::sha256_hex(b"thumb"))
        .bind(status)
        .bind(crate::db::now())
        .bind(crate::db::now())
        .execute(&b.state.db)
        .await
        .unwrap();
    }
    let staging = b
        .shared
        .parent()
        .unwrap()
        .join(".ism-staging/peer/legacy-download");
    std::fs::create_dir_all(staging.parent().unwrap()).unwrap();
    std::fs::write(&staging, b"partial").unwrap();
    super::transfer::recover_after_restart(&b.state)
        .await
        .unwrap();
    sqlx::query("UPDATE peers SET status = 'online'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let worker = tokio::spawn(super::transfer::run(b.state.clone()));
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let failed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM transfers WHERE status = 'failed'
                 AND error = 'NAS metadata and ignored names are excluded from peer replication'",
            )
            .fetch_one(&b.state.db)
            .await
            .unwrap();
            if failed == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.abort();
    let _ = worker.await;
    result.unwrap();
    assert_eq!(std::fs::read(b.shared.join(metadata)).unwrap(), b"thumb");
    assert!(!b.shared.join("@eaDir/missing.jpg").exists());
    assert_eq!(std::fs::read(staging).unwrap(), b"partial");
    let changes: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM operations) + (SELECT COUNT(*) FROM replica_files)",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(changes, 0);
}

#[tokio::test]
async fn stalled_peer_does_not_block_polling_other_peers() {
    let (_a, mut b) = pair().await;
    let old_worker = b.tasks.remove(2);
    old_worker.abort();
    let _ = old_worker.await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let stalled_server = tokio::spawn(async move {
        let (_connection, _) = listener.accept().await.unwrap();
        let _ = connected_tx.send(());
        std::future::pending::<()>().await;
    });
    let secret = b
        .state
        .secrets
        .encrypt(super::net::SECRET_PURPOSE, "synthetic-peer-secret")
        .unwrap();
    sqlx::query(
        "INSERT INTO peers (id, name, url, instance_id, secret_enc, status, created_at)
         VALUES ('stalled', 'A stalled', ?, 'stalled-instance', ?, 'offline', ?)",
    )
    .bind(url)
    .bind(secret)
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE peers SET last_seen_at = NULL WHERE id != 'stalled'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let poller = tokio::spawn(super::run(b.state.clone()));
    tokio::time::timeout(Duration::from_secs(3), connected_rx)
        .await
        .expect("stalled peer was not contacted")
        .unwrap();
    let mut reached_healthy = true;
    for _ in 0..2 {
        let reached = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let seen: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM peers WHERE id != 'stalled' AND last_seen_at IS NOT NULL)",
                )
                .fetch_one(&b.state.db)
                .await
                .unwrap();
                if seen {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        if reached.is_err() {
            reached_healthy = false;
            break;
        }
        sqlx::query("UPDATE peers SET last_seen_at = NULL WHERE id != 'stalled'")
            .execute(&b.state.db)
            .await
            .unwrap();
    }
    assert!(!stalled_server.is_finished());
    poller.abort();
    stalled_server.abort();
    assert!(reached_healthy, "healthy peer polling was blocked");
}

#[tokio::test]
async fn corrupted_schedule_does_not_allow_unrestricted_transfers() {
    let (_a, b) = pair().await;
    for task in b.tasks.iter().skip(1) {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    assert!(
        super::transfer::allowed(&b.state, &share.id, &peer.id)
            .await
            .unwrap()
            .0
    );
    for invalid in [
        "invalid JSON",
        r#"{"timezone":"not-a-timezone","windows":[]}"#,
    ] {
        crate::db::set_setting(&b.state.db, "sync_schedule", invalid)
            .await
            .unwrap();
        assert!(admin::schedule(&b.state).await.is_err());
        assert!(admin::sync_status(&b.state).await.is_err());
        assert!(
            super::transfer::allowed(&b.state, &share.id, &peer.id)
                .await
                .is_err()
        );
    }
    let override_view = admin::create_override(
        &b.state,
        ism_core::dto::OverrideInput {
            share_id: Some(share.id.clone()),
            peer_id: Some(peer.id.clone()),
            minutes: 1,
            reason: "test".into(),
        },
    )
    .await
    .unwrap();
    assert!(
        super::transfer::allowed(&b.state, &share.id, &peer.id)
            .await
            .unwrap()
            .0
    );
    admin::revoke_override(&b.state, &override_view.id)
        .await
        .unwrap();
    admin::set_schedule(
        &b.state,
        ism_core::dto::ScheduleView {
            timezone: "UTC".into(),
            windows: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert!(
        super::transfer::allowed(&b.state, &share.id, &peer.id)
            .await
            .unwrap()
            .0
    );
}

async fn run_op(i: &Instance, key: &str, actions: Vec<OperationAction>) {
    let principal = Principal::Admin {
        username: "test".into(),
    };
    let req = OperationRequest {
        idempotency_key: key.into(),
        label: Some(key.into()),
        actions,
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(&i.state, &principal, req).await.unwrap();
    i.state.ops_wake.notify_waiters();
    for _ in 0..200 {
        let (op, _) = crate::ops::load(&i.state, &accepted.operation_id)
            .await
            .unwrap();
        let s = format!("{:?}", op.status).to_lowercase();
        if s == "completed" {
            return;
        }
        assert!(
            !matches!(
                s.as_str(),
                "failed" | "partial" | "cancelled" | "recoveryrequired"
            ),
            "{op:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("operation {key} did not finish");
}

fn loc(path: &str) -> Location {
    Location {
        root_id: "photos".into(),
        path: path.into(),
    }
}

fn trash_count(i: &Instance) -> usize {
    let trash = i.shared.parent().unwrap().join(crate::files::TRASH_DIR);
    std::fs::read_dir(trash).map_or(0, |d| d.count())
}

async fn backfills_pending(i: &Instance) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM share_backfills")
        .fetch_one(&i.state.db)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_re_enabled_share_backfills_what_both_sides_missed() {
    let (a, b) = pair().await;
    for _ in 0..200 {
        if backfills_pending(&a).await + backfills_pending(&b).await == 0 {
            break;
        }
        a.state.sync_wake.notify_waiters();
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(backfills_pending(&a).await + backfills_pending(&b).await, 0);
    let share = admin::list_shares(&a.state).await.unwrap().remove(0);
    let input = |enabled| ShareInput {
        name: share.name.clone(),
        location: share.location.clone(),
        peer_ids: share.peer_ids.clone(),
        enabled,
    };
    admin::update_share(&a.state, &share.id, input(false))
        .await
        .unwrap();
    // Both sides change while the share is off: neither event reaches the other side.
    write_old(&a.shared.join("from-a.jpg"), b"alpha-photo");
    write_old(&b.shared.join("from-b.jpg"), b"beta-photo");
    scan(&a).await;
    scan(&b).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!b.shared.join("from-a.jpg").exists());
    assert!(!a.shared.join("from-b.jpg").exists());

    admin::update_share(&a.state, &share.id, input(true))
        .await
        .unwrap();
    eventually("both sides backfilled", &[&a, &b], || {
        a.shared.join("from-b.jpg").is_file() && b.shared.join("from-a.jpg").is_file()
    })
    .await;
    assert_eq!(
        std::fs::read(a.shared.join("from-b.jpg")).unwrap(),
        b"beta-photo"
    );
    assert_eq!(
        std::fs::read(b.shared.join("from-a.jpg")).unwrap(),
        b"alpha-photo"
    );
    for _ in 0..200 {
        if backfills_pending(&a).await + backfills_pending(&b).await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(backfills_pending(&a).await + backfills_pending(&b).await, 0);
}

#[tokio::test]
async fn backfill_trashes_files_deleted_while_the_share_was_off() {
    let (a, b) = pair().await;
    write_old(&a.shared.join("gone.jpg"), b"soon-deleted");
    scan(&a).await;
    eventually("initial replication", &[&a, &b], || {
        b.shared.join("gone.jpg").is_file()
    })
    .await;
    let share = admin::list_shares(&b.state).await.unwrap().remove(0);
    let input = |enabled| ShareInput {
        name: share.name.clone(),
        location: share.location.clone(),
        peer_ids: share.peer_ids.clone(),
        enabled,
    };
    admin::update_share(&b.state, &share.id, input(false))
        .await
        .unwrap();
    let before = trash_count(&b);
    run_op(
        &a,
        "delete-while-off",
        vec![OperationAction::Trash {
            source: loc("shared/gone.jpg"),
            expected_revision: None,
        }],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(b.shared.join("gone.jpg").is_file());
    admin::update_share(&b.state, &share.id, input(true))
        .await
        .unwrap();
    eventually("deletion backfilled", &[&a, &b], || {
        !b.shared.join("gone.jpg").exists()
    })
    .await;
    assert_eq!(trash_count(&b), before + 1);
}

#[tokio::test]
async fn content_and_management_events_replicate_through_managed_operations() {
    let (a, b) = pair().await;
    write_old(&a.shared.join("x.jpg"), b"new-photo");
    write_old(&a.shared.join("y.jpg"), b"second-photo");
    scan(&a).await;
    eventually("peer content publication", &[&a, &b], || {
        std::fs::read(b.shared.join("x.jpg")).is_ok_and(|d| d == b"new-photo")
            && b.shared.join("y.jpg").is_file()
    })
    .await;
    // Publication is a managed upload operation run on behalf of the peer.
    let mut published = 0;
    for _ in 0..200 {
        published = sqlx::query_scalar(
            "SELECT COUNT(*) FROM operations WHERE principal LIKE 'peer:%' AND status = 'completed' AND request_json LIKE '%publish_upload%'",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap();
        if published == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(published, 2);
    let history: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfer_history h JOIN operations o ON o.id = h.operation_id
         WHERE o.status = 'completed' AND h.rel_path IN ('x.jpg','y.jpg')",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(
        history, 2,
        "completed transfers remain inspectable after leaving the queue"
    );

    // A newer version replaces the copy and keeps the old one in ISM trash.
    let before = trash_count(&b);
    write_old(&a.shared.join("x.jpg"), b"newer-photo");
    scan(&a).await;
    eventually("peer content replacement", &[&a, &b], || {
        std::fs::read(b.shared.join("x.jpg")).is_ok_and(|d| d == b"newer-photo")
    })
    .await;
    assert_eq!(trash_count(&b), before + 1);

    run_op(
        &a,
        "move",
        vec![OperationAction::Move {
            source: loc("shared/x.jpg"),
            target: loc("shared/moved/x.jpg"),
            expected_revision: None,
        }],
    )
    .await;
    eventually("peer move", &[&a, &b], || {
        b.shared.join("moved/x.jpg").is_file() && !b.shared.join("x.jpg").exists()
    })
    .await;
    run_op(
        &a,
        "rename",
        vec![OperationAction::Rename {
            source: loc("shared/moved/x.jpg"),
            target: loc("shared/moved/z.jpg"),
            expected_revision: None,
        }],
    )
    .await;
    eventually("peer rename", &[&a, &b], || {
        b.shared.join("moved/z.jpg").is_file()
    })
    .await;
    let before = trash_count(&b);
    run_op(
        &a,
        "trash",
        vec![OperationAction::Trash {
            source: loc("shared/y.jpg"),
            expected_revision: None,
        }],
    )
    .await;
    eventually("peer trash", &[&a, &b], || !b.shared.join("y.jpg").exists()).await;
    assert_eq!(trash_count(&b), before + 1, "deletes go to the ISM trash");

    run_op(
        &a,
        "move-folder",
        vec![OperationAction::MoveFolder {
            source: loc("shared/moved"),
            target: loc("shared/2024"),
        }],
    )
    .await;
    eventually("peer folder move", &[&a, &b], || {
        b.shared.join("2024/z.jpg").is_file() && !b.shared.join("moved").exists()
    })
    .await;
    run_op(
        &a,
        "trash-folder",
        vec![OperationAction::TrashFolder {
            source: loc("shared/2024"),
            expected_entries: None,
        }],
    )
    .await;
    eventually("peer folder trash", &[&a, &b], || {
        !b.shared.join("2024").exists()
    })
    .await;
    let observer = super::net::peers(&b.state).await.unwrap().remove(0);
    for _ in 0..100 {
        if super::events_for(&b.state, &observer.id, "observer-instance", 0)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| event.kind == "folder_trashed")
        {
            break;
        }
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let applied: Vec<String> =
        sqlx::query_scalar("SELECT kind FROM event_log WHERE origin != ? ORDER BY seq")
            .bind(&b.state.instance_id)
            .fetch_all(&b.state.db)
            .await
            .unwrap();
    for kind in [
        "file_moved",
        "file_deleted",
        "folder_moved",
        "folder_trashed",
    ] {
        assert!(
            applied.iter().any(|k| k == kind),
            "{kind} was logged with its operation: {applied:?}"
        );
    }
    let failed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conflicts")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(failed, 0);
}

#[tokio::test]
async fn incoming_events_recheck_content_even_when_metadata_is_unchanged() {
    let (_a, b) = pair().await;
    let path = b.shared.join("same.jpg");
    write_old(&path, b"old-content");
    scan(&b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let before = super::row(&b.state, &share.id, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, b"new-content").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    assert_eq!(
        crate::files::mtime_ns(&std::fs::metadata(&path).unwrap()),
        before.mtime_ns
    );
    let root = crate::files::root(&b.state.db, &share.root_id)
        .await
        .unwrap();
    let after = super::register_local(&b.state, &share, &root, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(before.sha256, after.sha256);
    assert_eq!(
        after.vv.compare(&before.vv),
        ism_core::version::Causality::Dominates
    );
}

#[tokio::test]
async fn periodic_scan_detects_content_change_with_unchanged_metadata() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let path = b.shared.join("same.jpg");
    write_old(&path, b"old-content");
    super::scan_share(&b.state, &share).await.unwrap();
    let before = super::row(&b.state, &share.id, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, b"new-content").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified)
        .unwrap();

    super::scan_share(&b.state, &share).await.unwrap();
    let after = super::row(&b.state, &share.id, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.size, after.size);
    assert_eq!(before.mtime_ns, after.mtime_ns);
    assert_ne!(before.sha256, after.sha256);
    assert_eq!(
        after.vv.compare(&before.vv),
        ism_core::version::Causality::Dominates
    );
    super::scan_share(&b.state, &share).await.unwrap();
    let unchanged = super::row(&b.state, &share.id, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.vv, after.vv);
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'file_changed'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(events, 2);
}

#[tokio::test]
async fn conflict_resolutions_skip_or_apply_the_peer_side() {
    let (a, b) = pair().await;
    // Pull by hand so the move below cannot be applied before the local edit is made.
    a.tasks[2].abort();
    b.tasks[2].abort();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    write_old(&a.shared.join("photo.jpg"), b"remote");
    write_old(&a.shared.join("gone.jpg"), b"prior");
    scan(&a).await;
    std::fs::remove_file(a.shared.join("gone.jpg")).unwrap();
    scan(&a).await;
    write_old(&b.shared.join("photo.jpg"), b"local");
    write_old(&b.shared.join("gone.jpg"), b"local-gone");
    scan(&b).await;
    for (rel, kind, deleted) in [
        ("photo.jpg", "changed_on_both", false),
        ("gone.jpg", "changed_here_deleted_there", true),
    ] {
        let vv = ism_core::version::VersionVector::default();
        super::conflict(
            &b.state,
            super::ConflictInput {
                share: &share,
                rel,
                kind,
                detail: "conflict".into(),
                remote: ConflictSide {
                    instance: "Alpha".into(),
                    sha256: (!deleted).then(|| crate::crypto::sha256_hex(b"remote")),
                    size: (!deleted).then_some(6),
                    modified_at: None,
                    deleted,
                },
                remote_vv: &vv,
                peer_id: &peer.id,
                from: None,
            },
        )
        .await
        .unwrap();
    }
    let open = admin::list_conflicts(&b.state, false).await.unwrap();
    let id_of = |rel: &str| {
        open.iter()
            .find(|c| c.location.path == format!("shared/{rel}"))
            .unwrap()
            .id
            .clone()
    };

    let error = admin::resolve(
        &b.state,
        "test",
        &id_of("photo.jpg"),
        ConflictResolution::Skip,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "recovery_not_exhausted");
    assert_eq!(std::fs::read(b.shared.join("photo.jpg")).unwrap(), b"local");
    admin::resolve(
        &b.state,
        "test",
        &id_of("photo.jpg"),
        ConflictResolution::KeepLocal,
    )
    .await
    .unwrap();

    // Keeping the peer's deletion trashes the local copy and announces it afterwards.
    let trashed = trash_count(&b);
    admin::resolve(
        &b.state,
        "test",
        &id_of("gone.jpg"),
        ConflictResolution::KeepRemote,
    )
    .await
    .unwrap();
    eventually("local copy trashed", &[&b], || {
        !b.shared.join("gone.jpg").exists()
    })
    .await;
    assert_eq!(trash_count(&b), trashed + 1);
    for _ in 0..200 {
        if admin::list_conflicts(&b.state, false)
            .await
            .unwrap()
            .is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        admin::list_conflicts(&b.state, false)
            .await
            .unwrap()
            .is_empty()
    );
    let announced: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE origin = ? AND kind = 'file_deleted' AND data_json LIKE '%gone.jpg%'",
    )
    .bind(&b.state.instance_id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(announced, 1);

    // A move on the peer of a file edited here: keeping theirs fetches their copy at the new
    // path and sends the local edit to the ISM trash.
    let (a, b) = pair().await;
    a.tasks[2].abort();
    b.tasks[2].abort();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    write_old(&a.shared.join("src.jpg"), b"original");
    scan(&a).await;
    super::pull(&b.state, &peer).await.unwrap();
    eventually("initial copy", &[&a, &b], || {
        b.shared.join("src.jpg").is_file()
    })
    .await;
    let a_peer = super::net::peers(&a.state).await.unwrap().remove(0);
    super::pull(&a.state, &a_peer).await.unwrap();
    write_old(&b.shared.join("src.jpg"), b"edited here");
    scan(&b).await;
    run_op(
        &a,
        "move-src",
        vec![OperationAction::Move {
            source: loc("shared/src.jpg"),
            target: loc("shared/moved/src.jpg"),
            expected_revision: None,
        }],
    )
    .await;
    let peer = super::net::peer(&b.state, &peer.id).await.unwrap();
    super::pull(&b.state, &peer).await.unwrap();
    let conflict = admin::list_conflicts(&b.state, false)
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.kind == "move")
        .expect("the move conflicts with the local edit");
    assert_eq!(conflict.moved_from.as_ref().unwrap().path, "shared/src.jpg");
    let trashed = trash_count(&b);
    admin::resolve(
        &b.state,
        "test",
        &conflict.id,
        ConflictResolution::KeepRemote,
    )
    .await
    .unwrap();
    eventually("peer layout applied", &[&b], || {
        std::fs::read(b.shared.join("moved/src.jpg")).is_ok_and(|d| d == b"original")
            && !b.shared.join("src.jpg").exists()
    })
    .await;
    assert_eq!(trash_count(&b), trashed + 1);
}

/// A local edit landing between a peer publication's file write and its replica bookkeeping
/// must stay a local change: absorbing it under the peer's version would let a later peer move
/// or edit overwrite it without a conflict.
#[tokio::test]
async fn local_edit_during_publication_bookkeeping_stays_a_local_change() {
    let (a, b) = pair().await;
    a.tasks[2].abort();
    b.tasks[2].abort();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let (reached_tx, mut reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    super::PUBLICATION_PAUSES.lock().unwrap().push((
        share.id.clone(),
        super::PublicationPause {
            reached: reached_tx,
            resume: resume_rx,
        },
    ));
    write_old(&a.shared.join("src.jpg"), b"original");
    scan(&a).await;
    super::pull(&b.state, &peer).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                result = &mut reached => break result.unwrap(),
                () = tokio::time::sleep(Duration::from_millis(50)) => {
                    b.state.sync_wake.notify_waiters();
                    b.state.ops_wake.notify_waiters();
                }
            }
        }
    })
    .await
    .expect("the publication reaches its bookkeeping");
    assert_eq!(
        std::fs::read(b.shared.join("src.jpg")).unwrap(),
        b"original"
    );
    write_old(&b.shared.join("src.jpg"), b"edited here");
    resume.send(()).unwrap();
    let mut status = String::new();
    for _ in 0..400 {
        status = sqlx::query_scalar("SELECT status FROM operations")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
        if !matches!(status.as_str(), "queued" | "running") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(status, "completed", "the publication bookkeeping completes");
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_peer_ops")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(pending, 0);
    let published = super::row(&b.state, &share.id, "src.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        published.sha256.as_deref(),
        Some(crate::crypto::sha256_hex(b"original").as_str()),
        "bookkeeping records the published bytes, not the later local edit"
    );
    // The publication's inbox event awaits content verification, which registers the disk.
    super::pull(&b.state, &peer).await.unwrap();
    scan(&b).await;
    let local = super::row(&b.state, &share.id, "src.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        local.sha256.as_deref(),
        Some(crate::crypto::sha256_hex(b"edited here").as_str())
    );
    assert_eq!(
        local.vv.compare(&published.vv),
        ism_core::version::Causality::Dominates
    );
    let announced: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE origin = ? AND kind = 'file_changed' AND data_json LIKE '%src.jpg%'",
    )
    .bind(&b.state.instance_id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(announced, 1);

    run_op(
        &a,
        "move-src",
        vec![OperationAction::Move {
            source: loc("shared/src.jpg"),
            target: loc("shared/moved/src.jpg"),
            expected_revision: None,
        }],
    )
    .await;
    let peer = super::net::peer(&b.state, &peer.id).await.unwrap();
    // The move is the only pending effect, so the pull reports that it waits on the conflict.
    if let Err(error) = super::pull(&b.state, &peer).await {
        assert_eq!(error, "peer effect waits for conflict resolution");
    }
    assert!(
        admin::list_conflicts(&b.state, false)
            .await
            .unwrap()
            .iter()
            .any(|c| c.kind == "move"),
        "the peer move conflicts with the local edit"
    );
    assert_eq!(
        std::fs::read(b.shared.join("src.jpg")).unwrap(),
        b"edited here"
    );
}

#[tokio::test]
async fn keep_local_resolution_and_event_are_atomic() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"remote");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"local");
    super::scan_share(&b.state, &share).await.unwrap();
    let before = super::row(&b.state, &share.id, "photo.jpg")
        .await
        .unwrap()
        .unwrap();
    super::conflict(
        &b.state,
        super::ConflictInput {
            share: &share,
            rel: "photo.jpg",
            kind: "changed_on_both",
            detail: "conflict".into(),
            remote: ConflictSide {
                instance: "Alpha".into(),
                sha256: Some(crate::crypto::sha256_hex(b"remote")),
                size: Some(6),
                modified_at: None,
                deleted: false,
            },
            remote_vv: &ism_core::version::VersionVector::default(),
            peer_id: &peer.id,
            from: None,
        },
    )
    .await
    .unwrap();
    let id = admin::list_conflicts(&b.state, false).await.unwrap()[0]
        .id
        .clone();
    sqlx::query(
        "CREATE TRIGGER reject_resolution_event BEFORE INSERT ON event_log BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    assert!(
        admin::resolve(&b.state, "test", &id, ConflictResolution::KeepLocal)
            .await
            .is_err()
    );
    assert_eq!(
        admin::list_conflicts(&b.state, false).await.unwrap().len(),
        1
    );
    let after = super::row(&b.state, &share.id, "photo.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.vv, after.vv);
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind != 'share_enabled'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(events, 1);
    sqlx::query("DROP TRIGGER reject_resolution_event")
        .execute(&b.state.db)
        .await
        .unwrap();
    admin::resolve(&b.state, "test", &id, ConflictResolution::KeepLocal)
        .await
        .unwrap();
    assert!(
        admin::list_conflicts(&b.state, false)
            .await
            .unwrap()
            .is_empty()
    );
    let final_row = super::row(&b.state, &share.id, "photo.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_row.vv.compare(&before.vv),
        ism_core::version::Causality::Dominates
    );
}

#[cfg(unix)]
#[tokio::test]
async fn incomplete_scan_does_not_record_deletions() {
    use std::os::unix::fs::PermissionsExt;

    let (_a, b) = pair().await;
    let path = b.shared.join("inaccessible/known.jpg");
    write_old(&path, b"photo");
    scan(&b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let before = super::row(&b.state, &share.id, "inaccessible/known.jpg")
        .await
        .unwrap()
        .unwrap();
    assert!(!before.deleted);

    let inaccessible = b.shared.join("inaccessible");
    std::fs::remove_file(path).unwrap();
    std::fs::set_permissions(&inaccessible, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(&inaccessible).is_ok() {
        std::fs::set_permissions(&inaccessible, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }
    let result = super::scan_share(&b.state, &share).await;
    std::fs::set_permissions(&inaccessible, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    let after = super::row(&b.state, &share.id, "inaccessible/known.jpg")
        .await
        .unwrap()
        .unwrap();
    assert!(!after.deleted);
    assert_eq!(before.vv, after.vv);
}

#[tokio::test]
async fn scan_rolls_back_replica_rows_when_event_recording_fails() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "CREATE TRIGGER reject_event BEFORE INSERT ON event_log BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let path = b.shared.join("known.jpg");
    write_old(&path, b"photo");
    assert!(super::scan_share(&b.state, &share).await.is_err());
    assert!(
        super::row(&b.state, &share.id, "known.jpg")
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("DROP TRIGGER reject_event")
        .execute(&b.state.db)
        .await
        .unwrap();
    super::scan_share(&b.state, &share).await.unwrap();
    let before = super::row(&b.state, &share.id, "known.jpg")
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_event BEFORE INSERT ON event_log BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(super::scan_share(&b.state, &share).await.is_err());
    let after = super::row(&b.state, &share.id, "known.jpg")
        .await
        .unwrap()
        .unwrap();
    assert!(!after.deleted);
    assert_eq!(after.vv, before.vv);
    sqlx::query("DROP TRIGGER reject_event")
        .execute(&b.state.db)
        .await
        .unwrap();
    super::scan_share(&b.state, &share).await.unwrap();
    assert!(
        super::row(&b.state, &share.id, "known.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
}

#[tokio::test]
async fn operation_requires_recovery_if_peer_bookkeeping_fails() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "CREATE TRIGGER reject_event BEFORE INSERT ON event_log WHEN json_extract(NEW.data_json, '$.path') = 'second' BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let request = OperationRequest {
        idempotency_key: "peer-bookkeeping-failure".into(),
        label: None,
        actions: vec![
            OperationAction::CreateFolder {
                target: loc("shared/first"),
            },
            OperationAction::CreateFolder {
                target: loc("shared/second"),
            },
        ],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        request,
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    let (operation, _) = crate::ops::load(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        operation.status,
        ism_core::dto::OperationStatus::RecoveryRequired
    );
    assert_eq!(
        operation.items[1].error.as_ref().unwrap().code,
        "peer_bookkeeping_failed"
    );
    assert!(b.shared.join("first").is_dir());
    assert!(b.shared.join("second").is_dir());
    let event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind != 'share_enabled'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(event_count, 0);
    super::scan_share(&b.state, &share).await.unwrap();
    for path in ["first", "second"] {
        assert_eq!(
            super::require_stable_share(&b.state, &share, &[path])
                .await
                .unwrap_err()
                .code(),
            "recovery_required"
        );
    }
    super::require_stable_share(&b.state, &share, &["independent.jpg"])
        .await
        .unwrap();
}

#[tokio::test]
async fn recheck_finishes_only_verified_interrupted_peer_publication() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"remote photo");
    scan(&a).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    let operation_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let operation_id: Option<String> =
                sqlx::query_scalar("SELECT operation_id FROM pending_peer_ops LIMIT 1")
                    .fetch_optional(&b.state.db)
                    .await
                    .unwrap();
            if let Some(operation_id) = operation_id {
                break operation_id;
            }
            b.state.sync_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the download must reach publication");
    b.tasks.pop().unwrap().abort();

    sqlx::query(
        "CREATE TRIGGER reject_rechecked_publish BEFORE INSERT ON replica_files
         WHEN NEW.rel_path = 'photo.jpg' BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER reject_rechecked_publish")
        .execute(&b.state.db)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::RecoveryRequired
    );
    assert_eq!(
        std::fs::read(b.shared.join("photo.jpg")).unwrap(),
        b"remote photo"
    );
    let progress_json: String = sqlx::query_scalar(
        "SELECT progress_json FROM operation_items WHERE operation_id = ? AND idx = 0",
    )
    .bind(&operation_id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    let mut progress: serde_json::Value = serde_json::from_str(&progress_json).unwrap();
    progress["steps"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap()["status"] = "running".into();
    sqlx::query(
        "UPDATE operation_items
         SET error_code = NULL, error_message = NULL, progress_json = ?
         WHERE operation_id = ? AND idx = 0",
    )
    .bind(progress.to_string())
    .bind(&operation_id)
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE operations SET recovery_resolved = 1 WHERE id = ?")
        .bind(&operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();

    let lock = b.state.replication_operation_lock.lock().await;
    let waiting_state = b.state.clone();
    let waiting_id = operation_id.clone();
    let waiting = tokio::spawn(async move {
        super::transfer::recheck_publish_recovery(&waiting_state, &waiting_id).await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !waiting.is_finished(),
        "recovery must wait for busy replication instead of failing"
    );
    waiting.abort();
    let _ = waiting.await;
    drop(lock);

    write_old(&b.shared.join("photo.jpg"), b"wrong photo");
    let error = super::transfer::recheck_publish_recovery(&b.state, &operation_id)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "recovery_recheck_failed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pending_peer_ops WHERE operation_id = ?",
        )
        .bind(&operation_id)
        .fetch_one(&b.state.db)
        .await
        .unwrap(),
        1
    );

    std::fs::copy(a.shared.join("photo.jpg"), b.shared.join("photo.jpg")).unwrap();
    super::transfer::recheck_publish_recovery(&b.state, &operation_id)
        .await
        .unwrap();
    let operation = crate::ops::load(&b.state, &operation_id).await.unwrap().0;
    assert_eq!(operation.status, ism_core::dto::OperationStatus::Completed);
    assert!(
        operation
            .items
            .iter()
            .all(|item| item.status == ism_core::dto::ItemStatus::Done)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pending_peer_ops WHERE operation_id = ?",
        )
        .bind(&operation_id)
        .fetch_one(&b.state.db)
        .await
        .unwrap(),
        0
    );
    let upload_status: String =
        sqlx::query_scalar("SELECT status FROM uploads WHERE operation_id = ?")
            .bind(&operation_id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(upload_status, "committed");
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let row = super::row(&b.state, &share.id, "photo.jpg")
        .await
        .unwrap()
        .unwrap();
    let expected_hash = crate::crypto::sha256_hex(b"remote photo");
    assert_eq!(row.sha256.as_deref(), Some(expected_hash.as_str()));
}

/// Publication bookkeeping records only the bytes of the upload its own operation published.
/// If that upload can no longer be tied to the operation it fails closed, keeping the pending
/// bookkeeping, and the recheck retry completes it once the tie is restored.
#[tokio::test]
async fn publication_bookkeeping_fails_closed_without_its_exact_upload() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    write_old(&a.shared.join("photo.jpg"), b"remote photo");
    scan(&a).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    b.tasks
        .push(tokio::spawn(super::transfer::run(b.state.clone())));
    let operation_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let operation_id: Option<String> =
                sqlx::query_scalar("SELECT operation_id FROM pending_peer_ops LIMIT 1")
                    .fetch_optional(&b.state.db)
                    .await
                    .unwrap();
            if let Some(operation_id) = operation_id {
                break operation_id;
            }
            b.state.sync_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the download must reach publication");
    b.tasks.pop().unwrap().abort();

    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    super::PUBLICATION_PAUSES.lock().unwrap().push((
        share.id.clone(),
        super::PublicationPause {
            reached: reached_tx,
            resume: resume_rx,
        },
    ));
    let running_state = b.state.clone();
    let running_id = operation_id.clone();
    let running =
        tokio::spawn(
            async move { crate::ops::exec::run_operation(&running_state, &running_id).await },
        );
    tokio::time::timeout(Duration::from_secs(15), reached)
        .await
        .expect("the publication reaches its bookkeeping")
        .unwrap();
    sqlx::query("UPDATE uploads SET operation_id = 'foreign-operation' WHERE operation_id = ?")
        .bind(&operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    resume.send(()).unwrap();
    running.await.unwrap().unwrap();

    let operation = crate::ops::load(&b.state, &operation_id).await.unwrap().0;
    assert_eq!(
        operation.status,
        ism_core::dto::OperationStatus::RecoveryRequired
    );
    assert_eq!(
        operation.items[0].error.as_ref().unwrap().code,
        "peer_bookkeeping_failed"
    );
    assert_eq!(
        std::fs::read(b.shared.join("photo.jpg")).unwrap(),
        b"remote photo"
    );
    assert!(
        super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .is_none_or(|row| row.sha256.is_none()),
        "no version is recorded without the exact upload"
    );
    let pending = || async {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_peer_ops WHERE operation_id = ?")
            .bind(&operation_id)
            .fetch_one(&b.state.db)
            .await
            .unwrap()
    };
    assert_eq!(pending().await, 1);

    sqlx::query("UPDATE uploads SET operation_id = ? WHERE operation_id = 'foreign-operation'")
        .bind(&operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let progress_json: String = sqlx::query_scalar(
        "SELECT progress_json FROM operation_items WHERE operation_id = ? AND idx = 0",
    )
    .bind(&operation_id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    let mut progress: serde_json::Value = serde_json::from_str(&progress_json).unwrap();
    progress["steps"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap()["status"] = "running".into();
    sqlx::query(
        "UPDATE operation_items
         SET error_code = NULL, error_message = NULL, progress_json = ?
         WHERE operation_id = ? AND idx = 0",
    )
    .bind(progress.to_string())
    .bind(&operation_id)
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE operations SET recovery_resolved = 1 WHERE id = ?")
        .bind(&operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    super::transfer::recheck_publish_recovery(&b.state, &operation_id)
        .await
        .unwrap();
    assert_eq!(pending().await, 0);
    let row = super::row(&b.state, &share.id, "photo.jpg")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.sha256.as_deref(),
        Some(crate::crypto::sha256_hex(b"remote photo").as_str())
    );
}

#[tokio::test]
async fn failed_move_event_rolls_back_replica_rename() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("old.jpg"), b"photo");
    super::scan_share(&b.state, &share).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_move BEFORE INSERT ON event_log WHEN NEW.kind = 'file_moved' BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "peer-move-event-failure".into(),
            label: None,
            actions: vec![OperationAction::Move {
                source: loc("shared/old.jpg"),
                target: loc("shared/new.jpg"),
                expected_revision: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    let (operation, _) = crate::ops::load(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        operation.status,
        ism_core::dto::OperationStatus::RecoveryRequired
    );
    assert!(b.shared.join("new.jpg").is_file());
    assert!(
        super::row(&b.state, &share.id, "old.jpg")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::row(&b.state, &share.id, "new.jpg")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn scans_and_managed_operations_wait_for_the_same_barrier() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("before.jpg"), b"photo");
    super::scan_share(&b.state, &share).await.unwrap();

    let request = OperationRequest {
        idempotency_key: "scan-barrier".into(),
        label: None,
        actions: vec![OperationAction::Move {
            source: loc("shared/before.jpg"),
            target: loc("shared/after.jpg"),
            expected_revision: None,
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        request,
    )
    .await
    .unwrap();
    let guard = b.state.replication_operation_lock.lock().await;
    let scan = super::scan_share(&b.state, &share);
    let operation = crate::ops::exec::run_operation(&b.state, &accepted.operation_id);
    tokio::pin!(scan, operation);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut scan)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut operation)
            .await
            .is_err()
    );
    assert!(b.shared.join("before.jpg").exists());
    assert!(!b.shared.join("after.jpg").exists());
    assert_eq!(
        crate::ops::load(&b.state, &accepted.operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Queued
    );

    drop(guard);
    scan.await.unwrap();
    operation.await.unwrap();
    super::scan_share(&b.state, &share).await.unwrap();
    assert!(!b.shared.join("before.jpg").exists());
    assert_eq!(std::fs::read(b.shared.join("after.jpg")).unwrap(), b"photo");
    assert!(
        super::row(&b.state, &share.id, "before.jpg")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !super::row(&b.state, &share.id, "after.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
    let deletions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'file_deleted'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(deletions, 0);
}

#[tokio::test]
async fn managed_trash_immediately_records_original_and_sidecar_deletions() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"photo");
    write_old(&b.shared.join("photo.jpg.xmp"), b"sidecar");
    super::scan_share(&b.state, &share).await.unwrap();
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "trash-replica-group".into(),
            label: None,
            actions: vec![OperationAction::Trash {
                source: loc("shared/photo.jpg"),
                expected_revision: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    for path in ["photo.jpg", "photo.jpg.xmp"] {
        assert!(
            super::row(&b.state, &share.id, path)
                .await
                .unwrap()
                .unwrap()
                .deleted
        );
    }
    let deletions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'file_deleted'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(deletions, 2);
    super::scan_share(&b.state, &share).await.unwrap();
    let after_scan: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'file_deleted'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(after_scan, deletions);
}

#[tokio::test]
async fn managed_trash_event_failure_rolls_back_all_replica_deletions() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"photo");
    write_old(&b.shared.join("photo.jpg.xmp"), b"sidecar");
    super::scan_share(&b.state, &share).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_sidecar_deletion BEFORE INSERT ON event_log
         WHEN NEW.kind = 'file_deleted' AND json_extract(NEW.data_json, '$.path') = 'photo.jpg.xmp'
         BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "trash-replica-group-failure".into(),
            label: None,
            actions: vec![OperationAction::Trash {
                source: loc("shared/photo.jpg"),
                expected_revision: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &accepted.operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::RecoveryRequired
    );
    for path in ["photo.jpg", "photo.jpg.xmp"] {
        assert!(
            !super::row(&b.state, &share.id, path)
                .await
                .unwrap()
                .unwrap()
                .deleted
        );
    }
    let deletions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'file_deleted'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(deletions, 0);
}

#[tokio::test]
async fn unicode_folder_moves_and_trash_update_only_their_replica_rows() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("café/photo.jpg"), b"photo");
    write_old(&b.shared.join("café-other/other.jpg"), b"other");
    super::scan_share(&b.state, &share).await.unwrap();

    let principal = Principal::Admin {
        username: "test".into(),
    };
    for (key, action) in [
        (
            "unicode-folder-move",
            OperationAction::MoveFolder {
                source: loc("shared/café"),
                target: loc("shared/été"),
            },
        ),
        (
            "unicode-folder-trash",
            OperationAction::TrashFolder {
                source: loc("shared/été"),
                expected_entries: None,
            },
        ),
    ] {
        let accepted = crate::ops::submit(
            &b.state,
            &principal,
            OperationRequest {
                idempotency_key: key.into(),
                label: None,
                actions: vec![action],
                allow_owner_transfer: false,
                allow_partial: false,
            },
        )
        .await
        .unwrap();
        crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
            .await
            .unwrap();
        assert_eq!(
            crate::ops::load(&b.state, &accepted.operation_id)
                .await
                .unwrap()
                .0
                .status,
            ism_core::dto::OperationStatus::Completed
        );
        assert!(
            super::row(&b.state, &share.id, "café/photo.jpg")
                .await
                .unwrap()
                .is_none()
        );
        let moved = super::row(&b.state, &share.id, "été/photo.jpg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.deleted, key == "unicode-folder-trash");
        assert!(
            !super::row(&b.state, &share.id, "café-other/other.jpg")
                .await
                .unwrap()
                .unwrap()
                .deleted
        );
    }
}

#[tokio::test]
async fn scans_wait_for_operation_recovery_before_recording_deletions() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let file = b.shared.join("photo.jpg");
    write_old(&file, b"photo");
    super::scan_share(&b.state, &share).await.unwrap();

    let request = OperationRequest {
        idempotency_key: "scan-recovery".into(),
        label: None,
        actions: vec![OperationAction::CreateFolder {
            target: loc("shared/photo.xmp"),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        request,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE operations SET status = 'recovery_required' WHERE id = ?")
        .bind(&accepted.operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    std::fs::remove_file(file).unwrap();
    super::scan_share(&b.state, &share).await.unwrap();
    let error = super::require_stable_share(&b.state, &share, &["photo.jpg"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), "recovery_required");
    assert!(
        !super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );

    for corrupt in ["not-json", "{}"] {
        sqlx::query(
            "UPDATE operations SET status = 'partial', admitted_server_ids_json = ? WHERE id = ?",
        )
        .bind(corrupt)
        .bind(&accepted.operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
        let error = super::scan_share(&b.state, &share).await.unwrap_err();
        assert_eq!(error.code(), "recovery_required");
        assert!(
            crate::ops::resolve_recovery(&b.state, &accepted.operation_id)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE operations SET status = 'recovery_required', admitted_server_ids_json = '[]' WHERE id = ?")
        .bind(&accepted.operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    crate::ops::resolve_recovery(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    super::scan_share(&b.state, &share).await.unwrap();
    assert!(
        super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
}

#[tokio::test]
async fn peer_events_wait_for_operation_recovery_without_advancing_cursor() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    write_old(&a.shared.join("photo.jpg"), b"photo");
    scan(&a).await;
    let request = OperationRequest {
        idempotency_key: "peer-recovery".into(),
        label: None,
        actions: vec![OperationAction::CreateFolder {
            target: loc("shared/photo.jpg"),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        request,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE operations SET status = 'recovery_required' WHERE id = ?")
        .bind(&accepted.operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let error = super::pull(&b.state, &peer).await.unwrap_err();
    assert!(error.contains("unresolved managed operations"));
    let after = super::net::peer(&b.state, &peer.id).await.unwrap();
    assert_eq!(after.cursor, peer.cursor);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(count, 0);

    crate::ops::resolve_recovery(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(super::pull(&b.state, &after).await.unwrap(), 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn malformed_file_events_do_not_advance_the_peer_cursor() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let share = super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    for data in [
        super::EventData {
            path: "photo.jpg".into(),
            size: Some(5),
            ..Default::default()
        },
        super::EventData {
            path: "photo.jpg".into(),
            sha256: Some(crate::crypto::sha256_hex(b"photo")),
            ..Default::default()
        },
        super::EventData {
            path: "photo.jpg".into(),
            sha256: Some("invalid".into()),
            size: Some(5),
            ..Default::default()
        },
        super::EventData {
            path: "".into(),
            sha256: Some(crate::crypto::sha256_hex(b"photo")),
            size: Some(5),
            ..Default::default()
        },
        super::EventData {
            path: "photo.jpg".into(),
            sha256: Some(crate::crypto::sha256_hex(b"photo")),
            size: Some(u64::MAX),
            ..Default::default()
        },
        super::EventData {
            path: "photo.jpg".into(),
            from: Some("../outside.jpg".into()),
            sha256: Some(crate::crypto::sha256_hex(b"photo")),
            size: Some(5),
            ..Default::default()
        },
        super::EventData {
            path: "photo.jpg".into(),
            from: Some(String::new()),
            sha256: Some(crate::crypto::sha256_hex(b"photo")),
            size: Some(5),
            ..Default::default()
        },
    ] {
        sqlx::query("DELETE FROM event_log")
            .execute(&a.state.db)
            .await
            .unwrap();
        super::append_local(&a.state, &share.name, "file_changed", &data)
            .await
            .unwrap();
        super::pull(&b.state, &peer).await.unwrap_err();
        let current = super::net::peer(&b.state, &peer.id).await.unwrap();
        assert_eq!(current.cursor, peer.cursor);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn malformed_file_event_does_not_refresh_an_open_conflict() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let source_share = super::shares(&a.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::conflict(
        &b.state,
        super::ConflictInput {
            share: &share,
            rel: "photo.jpg",
            kind: "changed_on_both",
            detail: "existing conflict".into(),
            remote: ConflictSide {
                instance: "Alpha".into(),
                sha256: Some(crate::crypto::sha256_hex(b"old")),
                size: Some(3),
                modified_at: None,
                deleted: false,
            },
            remote_vv: &Default::default(),
            peer_id: &peer.id,
            from: None,
        },
    )
    .await
    .unwrap();
    let before: (String, String) =
        sqlx::query_as("SELECT detail, remote_json FROM conflicts WHERE share_id = ?")
            .bind(&share.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    super::append_local(
        &a.state,
        &source_share.name,
        "file_changed",
        &super::EventData {
            path: "photo.jpg".into(),
            from: Some("../outside.jpg".into()),
            sha256: Some(crate::crypto::sha256_hex(b"new")),
            size: Some(3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    super::pull(&b.state, &peer).await.unwrap_err();
    let after: (String, String) =
        sqlx::query_as("SELECT detail, remote_json FROM conflicts WHERE share_id = ?")
            .bind(&share.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
}

#[tokio::test]
async fn conflicting_event_identity_does_not_advance_the_peer_cursor() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let share = super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let original = super::EventData {
        path: "photo.jpg".into(),
        sha256: Some(crate::crypto::sha256_hex(b"one")),
        size: Some(3),
        ..Default::default()
    };
    super::append_local(&a.state, &share.name, "file_changed", &original)
        .await
        .unwrap();
    let mut conflicting = original.clone();
    conflicting.sha256 = Some(crate::crypto::sha256_hex(b"two"));
    let origin_seq: i64 = sqlx::query_scalar("SELECT MAX(origin_seq) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO event_log (origin, origin_seq, share_name, kind, data_json, at)
         VALUES (?, ?, ?, 'file_changed', ?, ?)",
    )
    .bind(&a.state.instance_id)
    .bind(origin_seq)
    .bind(&share.name)
    .bind(serde_json::to_string(&conflicting).unwrap())
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    let error = super::pull(&b.state, &peer).await.unwrap_err();
    assert!(error.contains("event identity"));
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
}

#[tokio::test]
async fn equal_file_versions_with_different_contents_do_not_advance_cursor() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let source_share = super::shares(&a.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"local");
    super::scan_share(&b.state, &share).await.unwrap();
    let local = super::row(&b.state, &share.id, "photo.jpg")
        .await
        .unwrap()
        .unwrap();
    super::append_local(
        &a.state,
        &source_share.name,
        "file_changed",
        &super::EventData {
            path: "photo.jpg".into(),
            sha256: Some(crate::crypto::sha256_hex(b"other")),
            size: Some(5),
            vv: local.vv,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let error = super::pull(&b.state, &peer).await.unwrap_err();
    assert!(error.contains("version"));
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
    assert_eq!(std::fs::read(b.shared.join("photo.jpg")).unwrap(), b"local");
}

#[tokio::test]
async fn unsupported_peer_events_are_not_silently_acknowledged() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let share = super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::append_local(
        &a.state,
        &share.name,
        "future_kind",
        &super::EventData {
            path: "photo.jpg".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let error = super::pull(&b.state, &peer).await.unwrap_err();
    assert!(error.contains("unsupported"));
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
}

#[tokio::test]
async fn incoming_folder_creation_is_acknowledged_only_after_managed_execution() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let share = super::shares(&a.state).await.unwrap().remove(0);
    super::append_local(
        &a.state,
        &share.name,
        "folder_created",
        &super::EventData {
            path: "new-folder".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    assert!(!b.shared.join("new-folder").exists());
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
    let operation_id: String =
        sqlx::query_scalar("SELECT operation_id FROM pending_peer_events WHERE peer_id = ?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    let logged: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'folder_created'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(logged, 0);
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    assert!(b.shared.join("new-folder").is_dir());
    assert!(super::net::peer(&b.state, &peer.id).await.unwrap().cursor > peer.cursor);
    let logged: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE kind = 'folder_created'")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(logged, 1);
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(pending, 0);
}

async fn pending_folder_fixture() -> (Instance, Instance, super::net::Peer, String) {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let share = super::shares(&a.state).await.unwrap().remove(0);
    super::append_local(
        &a.state,
        &share.name,
        "folder_created",
        &super::EventData {
            path: "new-folder".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    (a, b, peer, share.name)
}

#[tokio::test]
async fn malformed_event_pages_cannot_apply_or_skip_events() {
    let (_a, b, mut peer, share) = pending_folder_fixture().await;
    let first = super::PeerEvent {
        seq: peer.cursor + 1,
        origin: "synthetic-origin".into(),
        origin_seq: 1,
        share,
        kind: "folder_created".into(),
        data: super::EventData {
            path: "new-folder".into(),
            ..Default::default()
        },
    };
    let mut second = first.clone();
    second.seq += 1;
    second.origin_seq += 1;
    second.data.path = "other-folder".into();

    let page = std::sync::Arc::new(std::sync::Mutex::new(super::EventPage {
        events: Vec::new(),
        next: peer.cursor,
    }));
    let handler_page = page.clone();
    let app = axum::Router::new().route(
        "/api/peer/v1/events",
        axum::routing::get(move || {
            let page = handler_page.lock().unwrap().clone();
            async move { axum::Json(page) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    peer.url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut invalid_identity = second.clone();
    invalid_identity.origin_seq = 0;
    let mut empty_origin = second.clone();
    empty_origin.origin.clear();
    let cases = [
        super::EventPage {
            events: vec![first.clone(), first.clone()],
            next: second.seq,
        },
        super::EventPage {
            events: vec![second.clone(), first.clone()],
            next: second.seq,
        },
        super::EventPage {
            events: vec![first.clone(), second.clone()],
            next: first.seq,
        },
        super::EventPage {
            events: vec![first.clone(), invalid_identity],
            next: second.seq,
        },
        super::EventPage {
            events: vec![first.clone(), empty_origin],
            next: second.seq,
        },
        super::EventPage {
            events: vec![first.clone(); 501],
            next: second.seq,
        },
        super::EventPage {
            events: Vec::new(),
            next: peer.cursor - 1,
        },
    ];
    for invalid in cases {
        *page.lock().unwrap() = invalid;
        assert!(super::pull(&b.state, &peer).await.is_err());
        assert_eq!(
            super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
            peer.cursor
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_peer_events")
                .fetch_one(&b.state.db)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM event_log WHERE kind != 'share_enabled'"
            )
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
            0
        );
        assert!(!b.shared.join("new-folder").exists());
    }

    std::fs::create_dir(b.shared.join("new-folder")).unwrap();
    *page.lock().unwrap() = super::EventPage {
        events: vec![first],
        next: second.seq + 2,
    };
    super::pull(&b.state, &peer).await.unwrap();
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        second.seq + 2
    );
    server.abort();
}

#[tokio::test]
async fn peer_folder_queue_failure_does_not_leave_an_operation_or_acknowledge() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    sqlx::query(
        "CREATE TRIGGER reject_pending_event BEFORE INSERT ON pending_peer_events
         BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    super::pull(&b.state, &peer).await.unwrap_err();
    let operations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operations")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(operations, 0);
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
    assert!(!b.shared.join("new-folder").exists());
    sqlx::query("DROP TRIGGER reject_pending_event")
        .execute(&b.state.db)
        .await
        .unwrap();
    super::pull(&b.state, &peer).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_peer_events")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn peer_folder_bookkeeping_failure_requires_recovery_before_acknowledgement() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    super::pull(&b.state, &peer).await.unwrap();
    let operation_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_remote_folder BEFORE INSERT ON event_log
         WHEN NEW.kind = 'folder_created' BEGIN SELECT RAISE(FAIL, 'injected'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::RecoveryRequired
    );
    assert!(b.shared.join("new-folder").is_dir());
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
    assert!(super::pull(&b.state, &peer).await.is_err());
    sqlx::query("DROP TRIGGER reject_remote_folder")
        .execute(&b.state.db)
        .await
        .unwrap();
    crate::ops::resolve_recovery(&b.state, &operation_id)
        .await
        .unwrap();
    super::recovery::tick(&b.state).await.unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Completed
    );
    assert!(super::net::peer(&b.state, &peer.id).await.unwrap().cursor > peer.cursor);
}

#[tokio::test]
async fn pending_peer_folder_blocks_share_changes_and_cancelled_work_is_not_applied() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    let share = admin::list_shares(&b.state).await.unwrap().remove(0);
    super::pull(&b.state, &peer).await.unwrap();
    let operation_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let error = admin::delete_share(&b.state, &share.id).await.unwrap_err();
    assert_eq!(error.code(), "peer_operation_pending");
    let error = admin::delete_peer(&b.state, &peer.id).await.unwrap_err();
    assert_eq!(error.code(), "peer_operation_pending");
    let error = admin::update_share(
        &b.state,
        &share.id,
        ShareInput {
            name: share.name,
            location: share.location,
            peer_ids: share.peer_ids,
            enabled: false,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "peer_operation_pending");
    crate::ops::cancel(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        &operation_id,
    )
    .await
    .unwrap();
    assert!(super::pull(&b.state, &peer).await.is_err());
    admin::delete_share(&b.state, &share.id).await.unwrap();
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    assert!(!b.shared.join("new-folder").exists());
}

#[tokio::test]
async fn failed_peer_folder_creation_waits_for_manual_readd() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    super::pull(&b.state, &peer).await.unwrap();
    let operation_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    std::fs::write(b.shared.join("new-folder"), b"blocking file").unwrap();
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Failed
    );
    assert!(super::pull(&b.state, &peer).await.is_err());
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
    let page = super::activity::page(&b.state, "active", 50, 0, None)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].operation_id.as_deref(),
        Some(operation_id.as_str())
    );
    assert!(page.rows[0].readd_available);
    std::fs::remove_file(b.shared.join("new-folder")).unwrap();
    admin::readd_sync_operation(&b.state, &operation_id, 0)
        .await
        .unwrap();
    let retry_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_ne!(retry_id, operation_id);
    crate::ops::exec::run_operation(&b.state, &retry_id)
        .await
        .unwrap();
    assert!(b.shared.join("new-folder").is_dir());
    assert!(super::net::peer(&b.state, &peer.id).await.unwrap().cursor > peer.cursor);
}

#[tokio::test]
async fn failed_peer_file_move_blocks_later_sync_until_readded() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("old.jpg"), b"photo");
    super::scan_share(&b.state, &share).await.unwrap();
    std::fs::write(b.shared.join("new.jpg"), b"blocking destination").unwrap();

    let principal = Principal::Peer {
        peer_id: peer.id.clone(),
    };
    let request = OperationRequest {
        idempotency_key: "peer-move-blocked".into(),
        label: Some("Move from peer".into()),
        actions: vec![OperationAction::Move {
            source: loc("shared/old.jpg"),
            target: loc("shared/new.jpg"),
            expected_revision: None,
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let mut tx = b.state.db.begin().await.unwrap();
    let accepted = crate::ops::insert_in(&mut tx, &principal, &request, None)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id)
         VALUES (?, 0, ?, 'move', 'old.jpg', 'new.jpg', ?, ?)",
    )
    .bind(&accepted.operation_id)
    .bind(&share.id)
    .bind(serde_json::to_string(&VersionVector::default()).unwrap())
    .bind(&peer.id)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &accepted.operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Failed
    );
    let page = super::activity::page(&b.state, "active", 50, 0, None)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.rows[0].readd_available);
    assert_eq!(page.rows[0].relative_path, "new.jpg");
    assert_eq!(page.progress.failed, 1);
    assert_eq!(super::pull(&b.state, &peer).await.unwrap(), 0);
    super::scan_share(&b.state, &share).await.unwrap();
    assert_eq!(
        super::require_stable_share(&b.state, &share, &["new.jpg"])
            .await
            .unwrap_err()
            .code(),
        "recovery_required"
    );

    std::fs::remove_file(b.shared.join("new.jpg")).unwrap();
    admin::readd_sync_operation(&b.state, &accepted.operation_id, 0)
        .await
        .unwrap();
    let retry_id: String =
        sqlx::query_scalar("SELECT operation_id FROM pending_peer_ops WHERE operation_id != ?")
            .bind(&accepted.operation_id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    crate::ops::exec::run_operation(&b.state, &retry_id)
        .await
        .unwrap();
    assert!(!b.shared.join("old.jpg").exists());
    assert_eq!(std::fs::read(b.shared.join("new.jpg")).unwrap(), b"photo");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_peer_ops")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn cancelled_peer_folder_creation_can_be_explicitly_retried() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    super::pull(&b.state, &peer).await.unwrap();
    assert_eq!(
        admin::retry_peer(&b.state, &peer.id)
            .await
            .unwrap_err()
            .code(),
        "peer_operation_pending"
    );
    let operation_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    crate::ops::cancel(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        &operation_id,
    )
    .await
    .unwrap();
    assert_eq!(
        admin::retry_peer(&b.state, &peer.id)
            .await
            .unwrap_err()
            .code(),
        "peer_operation_pending"
    );
    admin::readd_sync_operation(&b.state, &operation_id, 0)
        .await
        .unwrap();
    let retry_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_ne!(retry_id, operation_id);
    crate::ops::exec::run_operation(&b.state, &retry_id)
        .await
        .unwrap();
    assert!(b.shared.join("new-folder").is_dir());
}

#[tokio::test]
async fn incoming_empty_folder_is_not_created_without_queue_access() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    sqlx::query(
        "INSERT INTO immich_servers (id, name, url, database_url_enc, direct_writes_enabled, version, compatible, maintenance_json, created_at)
         VALUES ('immich', 'Immich', 'https://immich.example', 'unused', 1, '3.4.1', 1, ?, ?)",
    )
    .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE mappings SET server_id = 'immich' WHERE root_id = 'photos'")
        .execute(&b.state.db)
        .await
        .unwrap();
    super::pull(&b.state, &peer).await.unwrap();
    let operation_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    let (operation, _) = crate::ops::load(&b.state, &operation_id).await.unwrap();
    assert_eq!(operation.status, ism_core::dto::OperationStatus::Failed);
    assert_eq!(
        operation.items[0]
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("maintenance_blocked")
    );
    assert!(!b.shared.join("new-folder").exists());
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
}

#[tokio::test]
async fn queued_peer_folder_creation_survives_restart_recovery() {
    let (_a, b, peer, _) = pending_folder_fixture().await;
    super::pull(&b.state, &peer).await.unwrap();
    let operation_id: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    crate::maintenance::recover_after_restart(&b.state)
        .await
        .unwrap();
    assert_eq!(
        crate::ops::load(&b.state, &operation_id)
            .await
            .unwrap()
            .0
            .status,
        ism_core::dto::OperationStatus::Queued
    );
    assert_eq!(
        super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
    crate::ops::exec::run_operation(&b.state, &operation_id)
        .await
        .unwrap();
    assert!(b.shared.join("new-folder").is_dir());
    assert!(super::net::peer(&b.state, &peer.id).await.unwrap().cursor > peer.cursor);
}

#[tokio::test]
async fn malformed_peer_folder_events_do_not_change_files_or_advance_cursor() {
    let (a, b, peer, share_name) = pending_folder_fixture().await;
    for path in ["", "../escape", ".ism-private", "album/.ism-private"] {
        sqlx::query("DELETE FROM event_log")
            .execute(&a.state.db)
            .await
            .unwrap();
        super::append_local(
            &a.state,
            &share_name,
            "folder_created",
            &super::EventData {
                path: path.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(super::pull(&b.state, &peer).await.is_err());
        assert_eq!(
            super::net::peer(&b.state, &peer.id).await.unwrap().cursor,
            peer.cursor
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM operations")
                .fetch_one(&b.state.db)
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn corrupted_replication_data_is_not_silently_discarded() {
    let (a, b) = pair().await;
    for task in &a.tasks {
        task.abort();
    }
    let share = super::shares(&a.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&a.state).await.unwrap().remove(0);
    sqlx::query("INSERT INTO event_log (origin, origin_seq, share_name, kind, data_json, at) VALUES ('broken', 1, ?, 'file_changed', 'not json', ?)")
        .bind(&share.name)
        .bind(crate::db::now())
        .execute(&a.state.db)
        .await
        .unwrap();
    assert!(
        super::events_for(&a.state, &peer.id, &b.state.instance_id, 0)
            .await
            .is_err()
    );

    for (path, vv) in [("old/a.jpg", "{}"), ("old/b.jpg", "not json")] {
        sqlx::query("INSERT INTO replica_files (share_id, rel_path, size, mtime_ns, deleted, vv_json) VALUES (?, ?, 1, 1, 0, ?)")
            .bind(&share.id)
            .bind(path)
            .bind(vv)
            .execute(&a.state.db)
            .await
            .unwrap();
    }
    assert!(
        super::rename_rows(&a.state, &share.id, "old", "new", None)
            .await
            .is_err()
    );
    let paths: Vec<String> = sqlx::query_scalar(
        "SELECT rel_path FROM replica_files WHERE share_id = ? ORDER BY rel_path",
    )
    .bind(&share.id)
    .fetch_all(&a.state.db)
    .await
    .unwrap();
    assert_eq!(paths, ["old/a.jpg", "old/b.jpg"]);
}

#[tokio::test]
async fn cancelled_transfer_cannot_be_claimed() {
    let (_a, b) = pair().await;
    for task in &b.tasks {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        1,
        &ism_core::version::VersionVector::default(),
    )
    .await
    .unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM transfers WHERE share_id = ?")
        .bind(&share.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    admin::cancel_transfer(&b.state, &id).await.unwrap();
    assert!(!super::transfer::claim(&b.state, &id, None).await.unwrap());
    let status: String = sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
        .bind(&id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(status, "cancelled");
}

#[tokio::test]
async fn cancellation_before_transfer_runs_does_not_adopt_the_local_file() {
    let (_a, b) = pair().await;
    for task in b.tasks.iter().skip(1) {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"photo");
    sqlx::query("UPDATE peers SET status = 'online' WHERE id = ?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut vv = ism_core::version::VersionVector::default();
    vv.increment("remote");
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        5,
        &vv,
    )
    .await
    .unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM transfers WHERE share_id = ?")
        .bind(&share.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let guard = b.state.replication_operation_lock.lock().await;
    let worker = tokio::spawn(super::transfer::run(b.state.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
                .bind(&id)
                .fetch_one(&b.state.db)
                .await
                .unwrap();
            if status == "running" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    admin::cancel_transfer(&b.state, &id).await.unwrap();
    let mut updates = b.state.events.subscribe();
    drop(guard);
    tokio::time::timeout(Duration::from_secs(3), updates.recv())
        .await
        .unwrap()
        .unwrap();
    worker.abort();
    assert!(
        super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .is_none()
    );
    let status: String = sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
        .bind(&id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(status, "cancelled");
}

#[tokio::test]
async fn identical_file_adoption_rolls_back_if_queue_removal_fails() {
    let (_a, b) = pair().await;
    for task in b.tasks.iter().skip(1) {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"photo");
    sqlx::query("UPDATE peers SET status = 'online' WHERE id = ?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut vv = ism_core::version::VersionVector::default();
    vv.increment("remote");
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        5,
        &vv,
    )
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_transfer_delete BEFORE DELETE ON transfers
         BEGIN SELECT RAISE(FAIL, 'test failure'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let worker = tokio::spawn(super::transfer::run(b.state.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let attempts: i64 =
                sqlx::query_scalar("SELECT attempts FROM transfers WHERE share_id = ?")
                    .bind(&share.id)
                    .fetch_one(&b.state.db)
                    .await
                    .unwrap();
            if attempts > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    assert!(
        super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn identical_file_adoption_rejects_a_mismatched_advertised_size() {
    let (_a, b) = pair().await;
    for task in b.tasks.iter().skip(1) {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("photo.jpg"), b"photo");
    sqlx::query("UPDATE peers SET status = 'online' WHERE id = ?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut vv = ism_core::version::VersionVector::default();
    vv.increment("remote");
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        4,
        &vv,
    )
    .await
    .unwrap();
    let worker = tokio::spawn(super::transfer::run(b.state.clone()));
    let error = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row: (String, Option<String>) =
                sqlx::query_as("SELECT status, error FROM transfers WHERE share_id = ?")
                    .bind(&share.id)
                    .fetch_one(&b.state.db)
                    .await
                    .unwrap();
            if row.0 == "failed" {
                break row.1.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    assert!(error.contains("different sizes"));
    assert!(
        super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn restart_requeues_interrupted_and_newly_recoverable_transfers() {
    let (_a, b) = pair().await;
    for task in &b.tasks {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    let vv = ism_core::version::VersionVector::default();
    for path in [
        "running.jpg",
        "cancelled.jpg",
        "failed.jpg",
        "http-504.mov",
        "paired.xmp",
    ] {
        super::queue_transfer(
            &b.state,
            &peer.id,
            &share,
            path,
            &crate::crypto::sha256_hex(path.as_bytes()),
            1,
            &vv,
        )
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE transfers
         SET status = CASE rel_path
                 WHEN 'running.jpg' THEN 'running'
                 WHEN 'cancelled.jpg' THEN 'cancelled'
                 ELSE 'failed'
             END,
             attempts = CASE rel_path WHEN 'running.jpg' THEN 0 ELSE 8 END,
             error = CASE rel_path
                 WHEN 'http-504.mov' THEN 'HTTP 504 from peer'
                 WHEN 'paired.xmp' THEN 'companion ''paired.xmp'' belongs to multiple originals'
                 ELSE 'unresolved failure'
             END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_transfer_recovery BEFORE UPDATE OF status ON transfers
         WHEN OLD.status = 'running' BEGIN SELECT RAISE(FAIL, 'test failure'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    assert!(
        super::transfer::recover_after_restart(&b.state)
            .await
            .is_err()
    );
    sqlx::query("DROP TRIGGER fail_transfer_recovery")
        .execute(&b.state.db)
        .await
        .unwrap();
    super::transfer::recover_after_restart(&b.state)
        .await
        .unwrap();
    let statuses: Vec<(String, String, i64, Option<String>)> =
        sqlx::query_as("SELECT rel_path, status, attempts, error FROM transfers ORDER BY rel_path")
            .fetch_all(&b.state.db)
            .await
            .unwrap();
    assert_eq!(
        statuses,
        [
            (
                "cancelled.jpg".into(),
                "cancelled".into(),
                8,
                Some("unresolved failure".into())
            ),
            (
                "failed.jpg".into(),
                "failed".into(),
                8,
                Some("unresolved failure".into())
            ),
            ("http-504.mov".into(), "queued".into(), 0, None),
            ("paired.xmp".into(), "queued".into(), 0, None),
            ("running.jpg".into(), "queued".into(), 0, None)
        ]
    );
}

#[tokio::test]
async fn replacing_a_queued_transfer_is_atomic_and_rejects_invalid_metadata() {
    let (_a, b) = pair().await;
    for task in &b.tasks {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    let mut vv = ism_core::version::VersionVector::default();
    vv.increment("alpha");
    let old_hash = crate::crypto::sha256_hex(b"old");
    super::queue_transfer(&b.state, &peer.id, &share, "photo.jpg", &old_hash, 3, &vv)
        .await
        .unwrap();
    let original: (String, String, i64) =
        sqlx::query_as("SELECT id, sha256, size FROM transfers WHERE share_id = ?")
            .bind(&share.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_transfer_insert BEFORE INSERT ON transfers
         BEGIN SELECT RAISE(FAIL, 'test failure'); END",
    )
    .execute(&b.state.db)
    .await
    .unwrap();
    let new_hash = crate::crypto::sha256_hex(b"new");
    vv.increment("alpha");
    let error = super::queue_transfer(&b.state, &peer.id, &share, "photo.jpg", &new_hash, 3, &vv)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "internal");
    let after: (String, String, i64) =
        sqlx::query_as("SELECT id, sha256, size FROM transfers WHERE share_id = ?")
            .bind(&share.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(after, original);
    sqlx::query("DROP TRIGGER fail_transfer_insert")
        .execute(&b.state.db)
        .await
        .unwrap();
    for (hash, size) in [("invalid", 3), (new_hash.as_str(), u64::MAX)] {
        assert!(
            super::queue_transfer(&b.state, &peer.id, &share, "photo.jpg", hash, size, &vv)
                .await
                .is_err()
        );
    }
    let after_invalid: (String, String, i64) =
        sqlx::query_as("SELECT id, sha256, size FROM transfers WHERE share_id = ?")
            .bind(&share.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(after_invalid, original);
}

#[tokio::test]
async fn queued_versions_are_not_replaced_by_stale_or_concurrent_content() {
    let (_a, b) = pair().await;
    for task in &b.tasks {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    let old_hash = crate::crypto::sha256_hex(b"old");
    let new_hash = crate::crypto::sha256_hex(b"new");
    let mut newest = ism_core::version::VersionVector::default();
    newest.increment("alpha");
    newest.increment("alpha");
    let mut stale = ism_core::version::VersionVector::default();
    stale.increment("alpha");
    let mut concurrent = ism_core::version::VersionVector::default();
    concurrent.increment("beta");

    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &old_hash,
        3,
        &newest,
    )
    .await
    .unwrap();
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &new_hash,
        3,
        &stale,
    )
    .await
    .unwrap();
    for vv in [&concurrent, &newest] {
        assert!(
            super::queue_transfer(&b.state, &peer.id, &share, "photo.jpg", &new_hash, 3, vv)
                .await
                .is_err()
        );
    }
    let hash: String = sqlx::query_scalar("SELECT sha256 FROM transfers WHERE share_id = ?")
        .bind(&share.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(hash, old_hash);

    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &old_hash,
        3,
        &concurrent,
    )
    .await
    .unwrap();
    let vv_json: String = sqlx::query_scalar("SELECT vv_json FROM transfers WHERE share_id = ?")
        .bind(&share.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let mut merged: ism_core::version::VersionVector = serde_json::from_str(&vv_json).unwrap();
    assert_eq!(merged.get("alpha"), 2);
    assert_eq!(merged.get("beta"), 1);
    merged.increment("alpha");
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &new_hash,
        3,
        &merged,
    )
    .await
    .unwrap();
    let hash: String = sqlx::query_scalar("SELECT sha256 FROM transfers WHERE share_id = ?")
        .bind(&share.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(hash, new_hash);
}

#[tokio::test]
async fn competing_peer_event_stays_unacknowledged_and_reports_the_error() {
    let (a, b) = pair().await;
    for instance in [&a, &b] {
        for task in instance.tasks.iter().skip(1) {
            task.abort();
        }
    }
    let share_a = super::shares(&a.state).await.unwrap().remove(0);
    let share_b = super::shares(&b.state).await.unwrap().remove(0);
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let mut existing = ism_core::version::VersionVector::default();
    existing.increment("alpha");
    let old_hash = crate::crypto::sha256_hex(b"old");
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share_b,
        "photo.jpg",
        &old_hash,
        3,
        &existing,
    )
    .await
    .unwrap();
    let mut incoming = ism_core::version::VersionVector::default();
    incoming.increment("beta");
    super::append_local(
        &a.state,
        &share_a.name,
        "file_changed",
        &super::EventData {
            path: "photo.jpg".into(),
            sha256: Some(crate::crypto::sha256_hex(b"new")),
            size: Some(3),
            vv: incoming,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE peers SET status = 'online', last_error = NULL WHERE id = ?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut updates = b.state.events.subscribe();
    super::poll_peer(&b.state, &peer).await;
    let (status, error, cursor): (String, Option<String>, i64) =
        sqlx::query_as("SELECT status, last_error, cursor FROM peers WHERE id = ?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!(status, "online");
    assert!(error.is_some_and(|error| error.contains("conflicting contents")));
    assert_eq!(cursor, peer.cursor);
    let hash: String = sqlx::query_scalar("SELECT sha256 FROM transfers WHERE share_id = ?")
        .bind(&share_b.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(hash, old_hash);
    let mut sync_changed = || {
        let mut changed = false;
        while let Ok(event) = updates.try_recv() {
            match event {
                crate::state::ServerEvent::SyncChanged => changed = true,
                crate::state::ServerEvent::ActivityChanged => {}
                other => panic!("unexpected sync event: {other:?}"),
            }
        }
        changed
    };
    assert!(sync_changed());
    super::poll_peer(&b.state, &peer).await;
    assert!(
        !sync_changed(),
        "unchanged errors should not reload sync state"
    );
    sqlx::query("DELETE FROM event_log WHERE origin = ?")
        .bind(&a.state.instance_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    super::poll_peer(&b.state, &peer).await;
    let error: Option<String> = sqlx::query_scalar("SELECT last_error FROM peers WHERE id = ?")
        .bind(&peer.id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert!(error.is_some_and(|error| error.contains("conflicting contents")));
    assert!(
        !sync_changed(),
        "durable input is not discarded when the peer removes its log"
    );
}

#[tokio::test]
async fn transfer_wakes_when_operation_finishes_during_eligibility_scan() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    sqlx::query("UPDATE peers SET status = 'online' WHERE id = ?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "album/photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        5,
        &VersionVector::default(),
    )
    .await
    .unwrap();
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "transfer-wake-race".into(),
            label: None,
            actions: vec![OperationAction::CreateFolder {
                target: loc("shared/album"),
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    while tokio::time::timeout(Duration::from_millis(20), b.state.sync_wake.notified())
        .await
        .is_ok()
    {}
    let (reached, scanned) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(super::transfer::run_paused(
        b.state.clone(),
        super::transfer::EligibilityPause {
            reached,
            resume: resumed,
        },
    ));
    tokio::time::timeout(Duration::from_secs(2), scanned)
        .await
        .expect("transfer never encountered the blocking operation")
        .unwrap();
    let before: (String, i64) = sqlx::query_as(
        "SELECT status, attempts FROM transfers WHERE share_id = ? AND rel_path = 'album/photo.jpg'",
    )
    .bind(&share.id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!((before.0.as_str(), before.1), ("queued", 0));
    crate::ops::exec::complete_for_test(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id = ?")
        .bind(&accepted.operation_id)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    assert_eq!(status, "completed");
    resume.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status: Option<String> = sqlx::query_scalar(
                "SELECT status FROM transfers WHERE share_id = ? AND rel_path = 'album/photo.jpg'",
            )
            .bind(&share.id)
            .fetch_optional(&b.state.db)
            .await
            .unwrap();
            if status.as_deref() != Some("queued") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.abort();
    assert!(
        result.is_ok(),
        "queued transfer was not claimed promptly after the operation finished"
    );
}

#[tokio::test]
async fn transfers_are_deferred_without_consuming_retries_during_recovery() {
    let (_a, b) = pair().await;
    for task in &b.tasks {
        task.abort();
    }
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    sqlx::query("UPDATE peers SET status = 'online' WHERE id = ?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        5,
        &ism_core::version::VersionVector::default(),
    )
    .await
    .unwrap();
    let request = OperationRequest {
        idempotency_key: "transfer-recovery".into(),
        label: None,
        actions: vec![OperationAction::CreateFolder {
            target: loc("shared/photo.jpg"),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        request,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE operations SET status = 'recovery_required' WHERE id = ?")
        .bind(&accepted.operation_id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut events = b.state.events.subscribe();
    let worker = tokio::spawn(super::transfer::run(b.state.clone()));
    assert!(
        tokio::time::timeout(Duration::from_millis(250), events.recv())
            .await
            .is_err(),
        "blocked transfers must wait without claim/defer churn"
    );
    let (status, attempts): (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM transfers WHERE share_id = ?")
            .bind(&share.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    assert_eq!((status.as_str(), attempts), ("queued", 0));
    let page = super::activity::page(&b.state, "active", 250, 0, None)
        .await
        .unwrap();
    assert!(
        page.warnings
            .iter()
            .any(|warning| warning.contains("Independent sync continues"))
    );
    crate::ops::resolve_recovery(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    for _ in 0..100 {
        // The peer does not have the file: the resumed transfer is dropped or fails.
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM transfers WHERE share_id = ?")
                .bind(&share.id)
                .fetch_optional(&b.state.db)
                .await
                .unwrap();
        if status.as_deref().is_none_or(|s| s == "failed") {
            worker.abort();
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    worker.abort();
    panic!("transfer did not resume after recovery");
}

#[tokio::test]
async fn full_scans_wait_for_a_peer_transfer_backlog() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let share = super::shares(&b.state).await.unwrap().remove(0);
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "photo.jpg",
        &crate::crypto::sha256_hex(b"photo"),
        5,
        &ism_core::version::VersionVector::default(),
    )
    .await
    .unwrap();

    assert!(super::full_scan_allowed(&b.state).await.unwrap());
    assert!(
        super::blocked_footprints(&b.state, &share)
            .await
            .unwrap()
            .contains(&"photo.jpg".to_owned())
    );
    write_old(&b.shared.join("independent.jpg"), b"independent");
    super::scan_share(&b.state, &share).await.unwrap();
    assert!(
        super::row(&b.state, &share.id, "independent.jpg")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::row(&b.state, &share.id, "photo.jpg")
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("DELETE FROM transfers")
        .execute(&b.state.db)
        .await
        .unwrap();
    assert!(
        super::full_scan_allowed(&b.state).await.unwrap(),
        "the safety scan must resume after peer work drains"
    );
}

#[tokio::test]
async fn transfer_does_not_adopt_stale_cached_hash() {
    let (_a, b) = pair().await;
    let path = b.shared.join("same.jpg");
    write_old(&path, b"old-content");
    scan(&b).await;
    let share = super::shares(&b.state).await.unwrap().remove(0);
    let previous = super::row(&b.state, &share.id, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, b"new-content").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    super::queue_transfer(
        &b.state,
        &peer.id,
        &share,
        "same.jpg",
        previous.sha256.as_deref().unwrap(),
        previous.size as u64,
        &previous.vv,
    )
    .await
    .unwrap();
    let mut status = None;
    for _ in 0..200 {
        status = sqlx::query_scalar::<_, String>(
            "SELECT status FROM transfers WHERE share_id = ? AND rel_path = 'same.jpg'",
        )
        .bind(&share.id)
        .fetch_optional(&b.state.db)
        .await
        .unwrap();
        if status.as_deref().is_none_or(|s| s == "failed") {
            break;
        }
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Not adopted as identical: the transfer had to download, and the peer no longer has it.
    assert!(
        status.as_deref().is_none_or(|s| s == "failed"),
        "{status:?}"
    );
    assert_eq!(std::fs::read(path).unwrap(), b"new-content");
}

#[tokio::test]
async fn peer_content_rechecks_hash_with_unchanged_metadata() {
    let (a, b) = pair().await;
    let path = a.shared.join("same.jpg");
    write_old(&path, b"old-content");
    scan(&a).await;
    let share = super::shares(&a.state).await.unwrap().remove(0);
    let previous = super::row(&a.state, &share.id, "same.jpg")
        .await
        .unwrap()
        .unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, b"new-content").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    let peer = super::net::peers(&b.state).await.unwrap().remove(0);
    let uri = format!(
        "/api/peer/v1/content?share=Family&path=same.jpg&sha256={}",
        previous.sha256.as_deref().unwrap()
    );
    let response = super::net::request(
        &b.state,
        &peer.url,
        &peer.secret,
        reqwest::Method::GET,
        &uri,
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn replicates_orphan_sidecars_and_replaces_their_own_revision() {
    let (a, b) = pair().await;
    let rel = "orphan/nested/photo.xmp";
    assert!(!b.shared.join("orphan").exists());
    write_old(&a.shared.join(rel), b"<xmp>first</xmp>");
    write_old(
        &a.shared.join("orphan/nested/photo.dop"),
        b"editor settings",
    );
    scan(&a).await;
    eventually("orphan XMP reaches an absent folder", &[&a, &b], || {
        std::fs::read(b.shared.join(rel)).ok().as_deref() == Some(b"<xmp>first</xmp>")
            && std::fs::read(b.shared.join("orphan/nested/photo.dop"))
                .ok()
                .as_deref()
                == Some(b"editor settings")
    })
    .await;

    write_old(&a.shared.join(rel), b"<xmp>replacement</xmp>");
    scan(&a).await;
    eventually("orphan XMP replacement reaches B", &[&a, &b], || {
        std::fs::read(b.shared.join(rel)).ok().as_deref() == Some(b"<xmp>replacement</xmp>")
    })
    .await;

    // Shared-stem originals must not prevent independent sidecar replication.
    for instance in [&a, &b] {
        write_old(&instance.shared.join("ambiguous/photo.jpg"), b"jpeg");
        write_old(&instance.shared.join("ambiguous/photo.png"), b"png");
    }
    write_old(&a.shared.join("ambiguous/photo.xmp"), b"<xmp>shared</xmp>");
    scan(&a).await;
    eventually("ambiguous XMP reaches B", &[&a, &b], || {
        std::fs::read(b.shared.join("ambiguous/photo.xmp"))
            .ok()
            .as_deref()
            == Some(b"<xmp>shared</xmp>")
    })
    .await;

    for _ in 0..100 {
        let committed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM uploads u
             JOIN operation_items i ON i.operation_id = u.operation_id
             WHERE u.status = 'committed' AND i.status = 'done' AND json_valid(i.result_json)
               AND json_extract(u.request_json, '$.files[0].role') IN ('sidecar', 'companion')",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap();
        if committed == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let uploads: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT u.id, u.request_json, i.result_json FROM uploads u
         JOIN operation_items i ON i.operation_id = u.operation_id
         WHERE u.status = 'committed' AND i.status = 'done' AND json_valid(i.result_json)
           AND json_extract(u.request_json, '$.files[0].role') IN ('sidecar', 'companion')",
    )
    .fetch_all(&b.state.db)
    .await
    .unwrap();
    assert_eq!(uploads.len(), 4);
    let peer = admin::list_peers(&b.state).await.unwrap().remove(0);
    let principal = Principal::Peer { peer_id: peer.id };
    for (id, request, result) in uploads {
        let request: ism_core::dto::CreateUpload = serde_json::from_str(&request).unwrap();
        assert!(request.existing_original.is_none());
        let (role, serialized_role) = if request.files[0].name == "photo.dop" {
            (ism_core::dto::FileRole::Companion, "companion")
        } else {
            (ism_core::dto::FileRole::Sidecar, "sidecar")
        };
        assert_eq!(request.files[0].role, role);
        if request.files[0].sha256 == crate::crypto::sha256_hex(b"<xmp>replacement</xmp>") {
            assert!(request.files[0].expected_revision.is_some());
            assert_eq!(
                request.files[0].expected_sha256.as_deref(),
                Some(crate::crypto::sha256_hex(b"<xmp>first</xmp>").as_str())
            );
            assert_eq!(
                request.on_conflict,
                ism_core::dto::OnConflict::ReplaceIfExpected
            );
        }
        let result: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(result["asset_id"].is_null());
        assert_eq!(result["files"][0]["role"], serialized_role);
        assert_eq!(
            crate::ops::uploads::get(&b.state, &principal, &id)
                .await
                .unwrap()
                .status,
            "committed"
        );
    }
    assert!(!b.shared.join("orphan/nested/photo.jpg").exists());
    // Independent content has no Immich asset to wait for, even on a scan callback.
    assert_eq!(
        crate::ops::uploads::fill_asset_ids(&b.state, "unconfigured", "unconfigured")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn standalone_peer_companions_keep_upload_validation_and_revision_guards() {
    use ism_core::dto::{CreateUpload, FileRole, OnConflict, UploadFileSpec};

    let i = instance("Receiver").await;
    let principal = Principal::Peer {
        peer_id: "synthetic-peer".into(),
    };
    write_old(&i.shared.join("photo.dop"), b"old editor settings");
    let (revision, hash) = crate::files::replacement_fingerprint(&i.shared.join("photo.dop"))
        .unwrap()
        .unwrap();
    let req = CreateUpload {
        idempotency_key: None,
        target: loc("shared"),
        provenance: None,
        existing_original: None,
        files: vec![UploadFileSpec {
            name: "photo.dop".into(),
            role: FileRole::Companion,
            size: 3,
            sha256: crate::crypto::sha256_hex(b"new"),
            expected_revision: Some(revision),
            expected_sha256: Some(hash),
        }],
        on_conflict: OnConflict::ReplaceIfExpected,
    };
    let client = Principal::Client(crate::auth::ClientGrant {
        token_id: "synthetic-client".into(),
        kind: "ila".into(),
        roots: vec!["photos".into()],
        actions: vec![ism_core::dto::ClientAction::Upload],
    });
    assert_eq!(
        crate::ops::uploads::create(&i.state, &client, req.clone())
            .await
            .unwrap_err()
            .code(),
        "bad_upload"
    );
    let mut invalid = req.clone();
    invalid.files[0].role = FileRole::Sidecar;
    assert_eq!(
        crate::ops::uploads::create(&i.state, &principal, invalid)
            .await
            .unwrap_err()
            .code(),
        "bad_upload"
    );
    let upload = crate::ops::uploads::create(&i.state, &principal, req)
        .await
        .unwrap();
    crate::ops::uploads::put_chunk(
        &i.state,
        &principal,
        &upload.upload_id,
        0,
        &axum::http::HeaderMap::new(),
        bytes::Bytes::from_static(b"new"),
    )
    .await
    .unwrap();
    // Change only bytes, leaving the metadata revision unchanged.
    let target = i.shared.join("photo.dop");
    let modified = std::fs::metadata(&target).unwrap().modified().unwrap();
    std::fs::write(&target, b"bad editor settings").unwrap();
    std::fs::File::open(&target)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    let accepted = crate::ops::uploads::commit(&i.state, &principal, &upload.upload_id)
        .await
        .unwrap();
    for _ in 0..200 {
        let (op, _) = crate::ops::load(&i.state, &accepted.operation_id)
            .await
            .unwrap();
        if matches!(op.status, ism_core::dto::OperationStatus::Failed) {
            assert_eq!(op.items[0].error.as_ref().unwrap().code, "revision_changed");
            assert_eq!(std::fs::read(&target).unwrap(), b"bad editor settings");
            return;
        }
        i.state.ops_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("changed standalone companion must fail without replacement");
}

#[tokio::test]
async fn replicates_files_moves_and_conflicts() {
    let (a, b) = pair().await;

    // A new photo and its sidecar travel to the peer.
    write_old(&a.shared.join("2024/x.jpg"), b"jpeg-v1");
    write_old(&a.shared.join("2024/x.jpg.xmp"), b"<xmp/>");
    scan(&a).await;
    eventually("new files reach B", &[&a, &b], || {
        std::fs::read(b.shared.join("2024/x.jpg")).ok().as_deref() == Some(b"jpeg-v1")
            && b.shared.join("2024/x.jpg.xmp").is_file()
    })
    .await;

    // A managed move on A is replayed on B as a move, sidecar included.
    run_op(
        &a,
        "move-x",
        vec![
            OperationAction::CreateFolder {
                target: loc("shared/2024/Trip"),
            },
            OperationAction::Move {
                source: loc("shared/2024/x.jpg"),
                target: loc("shared/2024/Trip/x.jpg"),
                expected_revision: None,
            },
        ],
    )
    .await;
    let move_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE kind = 'file_moved'
         AND json_extract(data_json, '$.from') = '2024/x.jpg'
         AND json_extract(data_json, '$.path') = '2024/Trip/x.jpg'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    let sidecar_move_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE kind = 'file_moved'
         AND json_extract(data_json, '$.from') LIKE '2024/x.jpg.%'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(move_events, 1);
    assert_eq!(sidecar_move_events, 0);
    eventually("move reaches B", &[&a, &b], || {
        b.shared.join("2024/Trip/x.jpg").is_file()
            && b.shared.join("2024/Trip/x.jpg.xmp").is_file()
            && !b.shared.join("2024/x.jpg").exists()
    })
    .await;

    // Both sides edit the same file before syncing: a conflict, not a silent overwrite.
    write_old(&a.shared.join("2024/Trip/x.jpg"), b"edited on alpha");
    write_old(&b.shared.join("2024/Trip/x.jpg"), b"edited on beta!!");
    scan(&a).await;
    scan(&b).await;
    let mut conflict = None;
    for _ in 0..300 {
        for i in [&a, &b] {
            let open = admin::list_conflicts(&i.state, false).await.unwrap();
            if let Some(c) = open.into_iter().next() {
                conflict = Some((i, c));
            }
        }
        if conflict.is_some() {
            break;
        }
        a.state.sync_wake.notify_waiters();
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (side, c) = conflict.expect("a conflict is reported");
    assert_eq!(c.location.path, "shared/2024/Trip/x.jpg");
    assert_eq!(
        std::fs::read(a.shared.join("2024/Trip/x.jpg")).unwrap(),
        b"edited on alpha"
    );
    assert_eq!(
        std::fs::read(b.shared.join("2024/Trip/x.jpg")).unwrap(),
        b"edited on beta!!"
    );

    // Keeping both versions converges: each instance ends up with both files.
    admin::resolve(
        &side.state,
        "admin:test",
        &c.id,
        ConflictResolution::KeepBoth,
    )
    .await
    .unwrap();
    eventually("both versions on both sides", &[&a, &b], || {
        let names = |dir: &Path| -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".jpg"))
                .collect();
            v.sort();
            v
        };
        let (na, nb) = (
            names(&a.shared.join("2024/Trip")),
            names(&b.shared.join("2024/Trip")),
        );
        na.len() == 2 && na == nb
    })
    .await;
    let copy = std::fs::read_dir(a.shared.join("2024/Trip"))
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.contains("conflict") && name.ends_with(".jpg")
        })
        .unwrap()
        .file_name();
    let ca = std::fs::read(a.shared.join("2024/Trip").join(&copy)).unwrap();
    let cb = std::fs::read(b.shared.join("2024/Trip").join(&copy)).unwrap();
    assert_eq!(
        ca, cb,
        "the conflict copy has the same content on both sides"
    );
    let xa = std::fs::read(a.shared.join("2024/Trip/x.jpg")).unwrap();
    let xb = std::fs::read(b.shared.join("2024/Trip/x.jpg")).unwrap();
    assert_eq!(xa, xb, "the original name converges too");
    assert_ne!(xa, ca);
}

#[tokio::test]
async fn the_primary_file_settings_reach_peers_and_lock_their_editing() {
    use ism_core::dto::{FileSettings, SettingsInput};

    let (a, b) = pair().await;
    let files = FileSettings {
        sidecar_extensions: vec!["xmp".into(), "dop".into()],
        junk_names: vec!["Thumbs.db".into()],
        junk_prefixes: vec!["._".into(), "~$".into()],
        name_max_len: 143,
    };
    crate::settings::apply_admin(
        &a.state,
        &SettingsInput {
            instance_name: "Alpha".into(),
            primary_instance_id: Some(a.state.instance_id.clone()),
            files: Some(files.clone()),
        },
    )
    .await
    .unwrap();

    eventually("B adopts A's file settings", &[&a, &b], || {
        let s = &b.state.shared_settings().settings;
        s.primary_id() == Some(a.state.instance_id.as_str()) && s.files == files
    })
    .await;
    assert!(b.state.junk_rules().is_junk("~$draft.docx"));
    let view = crate::settings::admin_view(&b.state).await.unwrap();
    assert!(!view.files_editable);
    assert_eq!(view.primary.unwrap().name, "Alpha");

    let refused = crate::settings::apply_admin(
        &b.state,
        &SettingsInput {
            instance_name: "Beta".into(),
            primary_instance_id: Some(a.state.instance_id.clone()),
            files: Some(FileSettings {
                name_max_len: 200,
                ..files.clone()
            }),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code(), "settings_managed_by_primary");

    // B clears the primary: the latest choice wins on both sides and each keeps its values.
    crate::settings::apply_admin(
        &b.state,
        &SettingsInput {
            instance_name: "Beta".into(),
            primary_instance_id: None,
            files: None,
        },
    )
    .await
    .unwrap();
    eventually("A sees that no instance is primary", &[&a, &b], || {
        a.state.shared_settings().settings.primary_id().is_none()
    })
    .await;
    assert_eq!(a.state.shared_settings().settings.files, files);
    assert!(
        crate::settings::admin_view(&b.state)
            .await
            .unwrap()
            .files_editable
    );
}
