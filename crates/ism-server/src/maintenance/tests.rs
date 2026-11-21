#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use ism_core::dto::{
    CreateUpload, FileRole, Location, OnConflict, OperationAction, OperationRequest, UploadFileSpec,
};
use serde_json::{Value, json};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::*;
use crate::config::Config;
use crate::crypto::SecretBox;

/// A tiny stateful Immich: `/admin/config` keeps what was PUT, queues can be paused, and the
/// library queue reports `busy_reads` reads with an active job (then none; `u32::MAX` means
/// always) and `waiting` held jobs. With `ignore_pause` pause requests succeed but have no effect.
#[derive(Clone)]
pub(crate) struct FakeImmich {
    pub config: Arc<Mutex<Value>>,
    pub busy_reads: Arc<Mutex<u32>>,
    pub paused: Arc<Mutex<Vec<String>>>,
    pub fail_pause: Arc<Mutex<bool>>,
    pub ignore_pause: Arc<Mutex<bool>>,
    pub waiting: Arc<Mutex<i64>>,
    /// When set, every pause/resume request records `(paused, probe exists)`.
    pub probe: Arc<Mutex<Option<std::path::PathBuf>>>,
    pub queue_log: Arc<Mutex<Vec<(bool, bool)>>>,
    /// Imported assets as `(originalPath, id)`, returned by metadata searches.
    pub assets: Arc<Mutex<Vec<(String, String)>>>,
}

impl Respond for FakeImmich {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let p = req.url.path();
        match (req.method.as_str(), p) {
            ("GET", "/api/api-keys/me") => ResponseTemplate::new(200).set_body_json(json!({
                "id": "test-key-id", "permissions": ["library.update"]
            })),
            ("GET", "/api/users/me") => ResponseTemplate::new(200).set_body_json(json!({
                "id": "u1", "isAdmin": true
            })),
            ("GET", "/api/admin/config") => {
                ResponseTemplate::new(200).set_body_json(self.config.lock().unwrap().clone())
            }
            ("PUT", "/api/admin/config") => {
                let v: Value = serde_json::from_slice(&req.body).unwrap();
                *self.config.lock().unwrap() = v.clone();
                ResponseTemplate::new(200).set_body_json(v)
            }
            ("GET", "/api/queues") => {
                let busy = {
                    let mut reads = self.busy_reads.lock().unwrap();
                    let busy = *reads > 0;
                    if busy && *reads != u32::MAX {
                        *reads -= 1;
                    }
                    busy
                };
                let paused = self.paused.lock().unwrap().clone();
                let waiting = *self.waiting.lock().unwrap();
                let qs: Vec<Value> = ["library", "sidecar", "metadataExtraction"]
                    .iter()
                    .map(|n| {
                        let library = *n == "library";
                        let active = i64::from(busy && library);
                        let waiting = if library { waiting } else { 0 };
                        json!({ "name": n, "isPaused": paused.iter().any(|x| x == n),
                                "statistics": { "active": active, "waiting": waiting, "delayed": 0, "paused": 0 } })
                    })
                    .collect();
                ResponseTemplate::new(200).set_body_json(qs)
            }
            ("PUT", q) if q.starts_with("/api/queues/") => {
                if *self.fail_pause.lock().unwrap() {
                    return ResponseTemplate::new(500);
                }
                if *self.ignore_pause.lock().unwrap() {
                    return ResponseTemplate::new(200).set_body_json(json!({}));
                }
                let name = q.trim_start_matches("/api/queues/").to_owned();
                let v: Value = serde_json::from_slice(&req.body).unwrap();
                if let Some(probe) = self.probe.lock().unwrap().as_ref() {
                    self.queue_log
                        .lock()
                        .unwrap()
                        .push((v["isPaused"] == true, probe.exists()));
                }
                let mut paused = self.paused.lock().unwrap();
                paused.retain(|x| *x != name);
                if v["isPaused"] == true {
                    paused.push(name);
                }
                ResponseTemplate::new(200).set_body_json(json!({}))
            }
            ("POST", "/api/search/metadata") => {
                let v: Value = serde_json::from_slice(&req.body).unwrap();
                let items: Vec<Value> = self
                    .assets
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(path, _)| v["originalPath"] == path.as_str())
                    .map(|(path, id)| json!({ "id": id, "originalPath": path }))
                    .collect();
                ResponseTemplate::new(200).set_body_json(json!({ "assets": { "items": items } }))
            }
            ("POST", s) if s.starts_with("/api/libraries/") && s.ends_with("/scan") => {
                ResponseTemplate::new(204)
            }
            _ => ResponseTemplate::new(404),
        }
    }
}

/// Mounts a fake Immich and registers it as server 's1' with administrator account 'a1'.
pub(crate) async fn fake_server(state: &AppState) -> (MockServer, FakeImmich) {
    let fake = FakeImmich {
        config: Arc::new(Mutex::new(json!({
            "library": { "watch": { "enabled": false }, "scan": { "enabled": false, "cronExpression": "0 0 * * *" } },
            "trash": { "enabled": true, "days": 30 }
        }))),
        busy_reads: Arc::new(Mutex::new(0)),
        paused: Arc::new(Mutex::new(Vec::new())),
        fail_pause: Arc::new(Mutex::new(false)),
        ignore_pause: Arc::new(Mutex::new(false)),
        waiting: Arc::new(Mutex::new(0)),
        probe: Arc::new(Mutex::new(None)),
        queue_log: Arc::new(Mutex::new(Vec::new())),
        assets: Arc::new(Mutex::new(Vec::new())),
    };
    let server = MockServer::start().await;
    for m in ["GET", "PUT", "POST"] {
        Mock::given(method(m))
            .respond_with(fake.clone())
            .mount(&server)
            .await;
    }
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
    (server, fake)
}

/// A server 's1' with administrator account 'a1'; Immich's watcher and scheduled scan are off.
pub(crate) async fn setup() -> (tempfile::TempDir, AppState, MockServer, FakeImmich) {
    let dir = tempfile::tempdir().unwrap();
    let pool = crate::db::connect(dir.path()).await.unwrap();
    let secrets = SecretBox::open_or_create(dir.path()).unwrap();
    let state = AppState::new(Config::for_tests(dir.path()), pool, secrets)
        .await
        .unwrap();
    let (server, fake) = fake_server(&state).await;
    (dir, state, server, fake)
}

pub(crate) fn set_flags(fake: &FakeImmich, watch: bool, scan: bool) {
    let mut config = fake.config.lock().unwrap();
    api::set_config_bool(&mut config, WATCH_ENABLED, watch);
    api::set_config_bool(&mut config, SCAN_ENABLED, scan);
}

fn flag(fake: &FakeImmich, p: &[&str]) -> Option<bool> {
    api::config_bool(&fake.config.lock().unwrap(), p)
}

fn paused(fake: &FakeImmich) -> Vec<String> {
    fake.paused.lock().unwrap().clone()
}

async fn maintenance(state: &AppState) -> MaintenanceState {
    registry::server(state, "s1").await.unwrap().maintenance
}

#[tokio::test]
async fn overlapping_operations_share_one_library_queue_pause() {
    let (_dir, state, _server, fake) = setup().await;
    acquire(&state, "s1").await.unwrap();
    assert_eq!(paused(&fake), ["library"]);
    acquire(&state, "s1").await.unwrap();
    let m = maintenance(&state).await;
    assert_eq!((m.phase, m.active_ops, m.epoch), (Phase::Active, 2, 1));
    assert!(m.queue_paused_by_ism);

    release(&state, "s1").await.unwrap();
    assert_eq!(paused(&fake), ["library"], "still held by one operation");
    release(&state, "s1").await.unwrap();
    assert!(paused(&fake).is_empty());
    let m = maintenance(&state).await;
    assert_eq!((m.phase, m.active_ops), (Phase::Idle, 0));
    assert!(!m.queue_paused_by_ism);
    assert!(m.scans_allowed());
}

#[tokio::test]
async fn jobs_held_by_the_paused_queue_do_not_delay_admission() {
    let (_dir, state, _server, fake) = setup().await;
    // An administrator's scan queued in Immich: it waits in the paused queue until release.
    *fake.waiting.lock().unwrap() = 2;
    acquire(&state, "s1").await.unwrap();
    let m = maintenance(&state).await;
    assert_eq!((m.phase, m.active_ops), (Phase::Active, 1));
    assert_eq!(paused(&fake), ["library"]);
    release(&state, "s1").await.unwrap();
    assert!(paused(&fake).is_empty());
}

#[tokio::test]
async fn a_queue_that_does_not_report_paused_blocks() {
    let (_dir, state, _server, fake) = setup().await;
    *fake.ignore_pause.lock().unwrap() = true;
    let err = acquire(&state, "s1").await.unwrap_err();
    assert_eq!(err.code(), "maintenance_blocked");
    assert!(
        err.message().contains("did not stay paused"),
        "{}",
        err.message()
    );
    assert_eq!(maintenance(&state).await.phase, Phase::Blocked);
}

#[tokio::test]
async fn waits_for_the_active_library_job_to_finish() {
    let (_dir, state, _server, fake) = setup().await;
    *fake.busy_reads.lock().unwrap() = 3;
    acquire(&state, "s1").await.unwrap();
    assert_eq!(maintenance(&state).await.phase, Phase::Active);
    assert_eq!(paused(&fake), ["library"]);
}

#[tokio::test]
async fn a_job_that_never_finishes_blocks_and_leaves_the_queue_running() {
    let (_dir, state, _server, fake) = setup().await;
    *fake.busy_reads.lock().unwrap() = u32::MAX;
    let err = acquire(&state, "s1").await.unwrap_err();
    assert_eq!(err.code(), "maintenance_blocked");
    let m = maintenance(&state).await;
    assert_eq!(m.phase, Phase::Blocked, "{m:?}");
    assert!(!m.queue_paused_by_ism);
    assert!(paused(&fake).is_empty());
    // A blocked server keeps refusing work until an admin retries.
    assert!(acquire(&state, "s1").await.is_err());
    *fake.busy_reads.lock().unwrap() = 0;
    retry(&state, "s1").await.unwrap();
    acquire(&state, "s1").await.unwrap();
}

#[tokio::test]
async fn an_unpausable_queue_blocks() {
    let (_dir, state, _server, fake) = setup().await;
    *fake.fail_pause.lock().unwrap() = true;
    let err = acquire(&state, "s1").await.unwrap_err();
    assert!(
        err.message().contains("could not pause"),
        "{}",
        err.message()
    );
    assert_eq!(maintenance(&state).await.phase, Phase::Blocked);
}

#[tokio::test]
async fn drift_blocks_writes_until_settings_are_reapplied() {
    let (_dir, state, _server, fake) = setup().await;
    set_flags(&fake, true, false);
    let err = acquire(&state, "s1").await.unwrap_err();
    assert_eq!(err.code(), "maintenance_blocked");
    let m = maintenance(&state).await;
    assert!(m.drift.is_some(), "{m:?}");
    assert_eq!(
        flag(&fake, WATCH_ENABLED),
        Some(true),
        "never re-disabled silently"
    );
    assert!(paused(&fake).is_empty());
    let server = registry::server(&state, "s1").await.unwrap();
    assert_eq!(server.maintenance.phase, Phase::Idle);

    // Turning it off in Immich alone does not clear the alert.
    set_flags(&fake, false, false);
    assert!(acquire(&state, "s1").await.is_err());

    set_flags(&fake, true, true);
    reapply_settings(&state, "s1").await.unwrap();
    assert_eq!(flag(&fake, WATCH_ENABLED), Some(false));
    assert_eq!(flag(&fake, SCAN_ENABLED), Some(false));
    assert_eq!(
        fake.config.lock().unwrap()["library"]["scan"]["cronExpression"],
        "0 0 * * *"
    );
    assert!(maintenance(&state).await.drift.is_none());
    acquire(&state, "s1").await.unwrap();
}

#[tokio::test]
async fn the_health_check_detects_drift() {
    let (_dir, state, _server, fake) = setup().await;
    let mut checked = HashMap::new();
    step(&state, &mut checked).await.unwrap();
    assert!(maintenance(&state).await.drift.is_none());
    set_flags(&fake, false, true);
    step(&state, &mut checked).await.unwrap();
    let m = maintenance(&state).await;
    assert!(m.drift.unwrap().contains("scheduled"));
    assert_eq!(flag(&fake, SCAN_ENABLED), Some(true));
}

#[tokio::test]
async fn enrollment_turns_watcher_and_scheduled_scan_off() {
    let (_dir, state, _server, fake) = setup().await;
    set_flags(&fake, true, true);
    enroll(&state, "s1").await.unwrap();
    assert_eq!(flag(&fake, WATCH_ENABLED), Some(false));
    assert_eq!(flag(&fake, SCAN_ENABLED), Some(false));
}

#[tokio::test]
async fn restart_resumes_a_pause_no_operation_holds() {
    let (_dir, state, _server, fake) = setup().await;
    acquire(&state, "s1").await.unwrap();
    assert_eq!(paused(&fake), ["library"]);
    // The process died while the operation held the pause but before it was marked running.
    recover_after_restart(&state).await.unwrap();
    let m = maintenance(&state).await;
    assert_eq!((m.phase, m.active_ops), (Phase::Resuming, 0));
    step(&state, &mut HashMap::new()).await.unwrap();
    assert!(paused(&fake).is_empty());
    let m = maintenance(&state).await;
    assert_eq!(m.phase, Phase::Idle);
    assert!(!m.queue_paused_by_ism);
}

#[tokio::test]
async fn a_failed_resume_blocks_and_retry_resumes() {
    let (_dir, state, _server, fake) = setup().await;
    acquire(&state, "s1").await.unwrap();
    *fake.fail_pause.lock().unwrap() = true;
    release(&state, "s1").await.unwrap();
    let m = maintenance(&state).await;
    assert_eq!(m.phase, Phase::Blocked);
    assert!(m.queue_paused_by_ism);
    *fake.fail_pause.lock().unwrap() = false;
    retry(&state, "s1").await.unwrap();
    assert_eq!(maintenance(&state).await.phase, Phase::Resuming);
    step(&state, &mut HashMap::new()).await.unwrap();
    assert!(paused(&fake).is_empty());
    assert_eq!(maintenance(&state).await.phase, Phase::Idle);
}

#[tokio::test]
async fn restart_recovery_updates_operations_uploads_and_server_atomically() {
    let (_dir, state, _, _) = setup().await;
    update(&state, "s1", |m| {
        m.phase = Phase::Active;
        m.active_ops = 1;
    })
    .await
    .unwrap();
    for (id, status, servers) in [
        ("running", "running", r#"["s1"]"#),
        ("waiting", "waiting_maintenance", "[]"),
    ] {
        sqlx::query("INSERT INTO operations (id, idempotency_key, principal, source, request_json, status, admitted_server_ids_json, created_at) VALUES (?, ?, 'admin:test', 'admin', '{}', ?, ?, ?)")
            .bind(id)
            .bind(id)
            .bind(status)
            .bind(servers)
            .bind(crate::db::now())
            .execute(&state.db)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO operation_items (operation_id, idx, kind, action_json, status) VALUES ('running', 0, 'publish_upload', '{}', 'running')")
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO uploads (id, principal, request_json, status, operation_id, created_at) VALUES ('upload', 'admin:test', '{}', 'committed', 'running', ?)")
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();

    recover_after_restart(&state).await.unwrap();
    recover_after_restart(&state).await.unwrap();
    let operations: Vec<(String, String)> =
        sqlx::query_as("SELECT id, status FROM operations ORDER BY id")
            .fetch_all(&state.db)
            .await
            .unwrap();
    assert_eq!(
        operations,
        vec![
            ("running".into(), "recovery_required".into()),
            ("waiting".into(), "queued".into())
        ]
    );
    let upload: String = sqlx::query_scalar("SELECT status FROM uploads WHERE id = 'upload'")
        .fetch_one(&state.db)
        .await
        .unwrap();
    assert_eq!(upload, "failed");
    let item: String =
        sqlx::query_scalar("SELECT status FROM operation_items WHERE operation_id = 'running'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(item, "recovery_required");
    let m = registry::server(&state, "s1").await.unwrap().maintenance;
    assert_eq!(m.phase, Phase::RecoveryRequired);
    assert_eq!(m.unresolved_recoveries, 1);
    assert_eq!(m.active_ops, 0);
}

#[tokio::test]
async fn restart_requeues_an_untouched_peer_upload() {
    let (dir, state, _, _) = setup().await;
    let root = dir.path().join("photos");
    std::fs::create_dir_all(root.join("incoming")).unwrap();
    std::fs::write(root.join(crate::files::SENTINEL), "photos\n").unwrap();
    sqlx::query(
        "INSERT INTO mappings (root_id,name,container_path,created_at) VALUES ('photos','Photos',?,?)",
    )
    .bind(root.to_string_lossy().to_string())
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();

    let data = b"synthetic peer upload";
    let upload = CreateUpload {
        idempotency_key: Some("peer-transfer:transfer".into()),
        target: Location {
            root_id: "photos".into(),
            path: "incoming".into(),
        },
        provenance: None,
        files: vec![UploadFileSpec {
            name: "photo.jpg".into(),
            role: FileRole::Original,
            size: data.len() as u64,
            sha256: crate::crypto::sha256_hex(data),
            expected_revision: None,
            expected_sha256: None,
        }],
        on_conflict: OnConflict::Fail,
        existing_original: None,
    };
    let staging = crate::ops::plan::staging_dir(
        &crate::files::root(&state.db, "photos").await.unwrap(),
        "upload",
    )
    .unwrap();
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join("0"), data).unwrap();
    let request = OperationRequest {
        idempotency_key: "upload:upload".into(),
        label: Some("incoming/photo.jpg from peer".into()),
        actions: vec![OperationAction::PublishUpload {
            upload_id: "upload".into(),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    sqlx::query(
        "INSERT INTO operations (id,idempotency_key,principal,source,label,request_json,status,admitted_server_ids_json,created_at,started_at)
         VALUES ('op','upload:upload','peer:test','peer','incoming/photo.jpg from peer',?,'running','[]',?,?)",
    )
    .bind(serde_json::to_string(&request).unwrap())
    .bind(crate::db::now())
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO operation_items (operation_id,idx,kind,action_json,status)
         VALUES ('op',0,'publish_upload',?,'running')",
    )
    .bind(serde_json::to_string(&request.actions[0]).unwrap())
    .execute(&state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO uploads (id,operation_id,principal,request_json,status,created_at)
         VALUES ('upload','op','peer:test',?,'publishing',?)",
    )
    .bind(serde_json::to_string(&upload).unwrap())
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();

    recover_after_restart(&state).await.unwrap();

    let operation: (String, Option<String>) =
        sqlx::query_as("SELECT status,finished_at FROM operations WHERE id = 'op'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(operation, ("queued".into(), None));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM operation_items WHERE operation_id = 'op'"
        )
        .fetch_one(&state.db)
        .await
        .unwrap(),
        "pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM uploads WHERE id = 'upload'")
            .fetch_one(&state.db)
            .await
            .unwrap(),
        "publishing"
    );

    sqlx::query("UPDATE operations SET status = 'failed', finished_at = ? WHERE id = 'op'")
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE uploads SET status = 'failed' WHERE id = 'upload'")
        .execute(&state.db)
        .await
        .unwrap();

    std::fs::write(staging.join("0"), b"changed").unwrap();
    let error = retry_failed_publish_upload(&state, "op").await.unwrap_err();
    assert_eq!(error.code(), "publish_retry_unverified");
    std::fs::write(staging.join("0"), data).unwrap();
    retry_failed_publish_upload(&state, "op").await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE id = 'op'")
            .fetch_one(&state.db)
            .await
            .unwrap(),
        "queued",
        "a failed untouched upload can be manually resumed"
    );
    crate::ops::exec::run_operation(&state, "op").await.unwrap();
    assert_eq!(
        std::fs::read(root.join("incoming/photo.jpg")).unwrap(),
        data
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE id = 'op'")
            .fetch_one(&state.db)
            .await
            .unwrap(),
        "completed"
    );
}

#[tokio::test]
async fn operation_recovery_resolution_is_atomic_and_idempotent() {
    let (_dir, state, _, _) = setup().await;
    update(&state, "s1", |m| {
        m.phase = Phase::RecoveryRequired;
        m.unresolved_recoveries = 1;
    })
    .await
    .unwrap();
    sqlx::query("INSERT INTO operations (id, idempotency_key, principal, source, request_json, status, admitted_server_ids_json, created_at) VALUES ('op', 'key', 'admin:test', 'admin', '{}', 'recovery_required', '[\"s1\"]', ?)")
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_resolution BEFORE UPDATE OF recovery_resolved ON operations BEGIN SELECT RAISE(FAIL, 'injected'); END")
        .execute(&state.db)
        .await
        .unwrap();
    assert!(crate::ops::resolve_recovery(&state, "op").await.is_err());
    let resolved: i64 =
        sqlx::query_scalar("SELECT recovery_resolved FROM operations WHERE id = 'op'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(resolved, 0);
    let m = registry::server(&state, "s1").await.unwrap().maintenance;
    assert_eq!(m.phase, Phase::RecoveryRequired);
    assert_eq!(m.unresolved_recoveries, 1);

    sqlx::query("DROP TRIGGER reject_resolution")
        .execute(&state.db)
        .await
        .unwrap();
    crate::ops::resolve_recovery(&state, "op").await.unwrap();
    crate::ops::resolve_recovery(&state, "op").await.unwrap();
    let resolved: i64 =
        sqlx::query_scalar("SELECT recovery_resolved FROM operations WHERE id = 'op'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(resolved, 1);
    let m = registry::server(&state, "s1").await.unwrap().maintenance;
    assert_eq!(m.phase, Phase::Idle);
    assert_eq!(m.unresolved_recoveries, 0);
}

#[tokio::test]
async fn corrupt_maintenance_state_prevents_restart_recovery() {
    let (_dir, state, _, _) = setup().await;
    sqlx::query("UPDATE immich_servers SET maintenance_json = 'not-json' WHERE id = 's1'")
        .execute(&state.db)
        .await
        .unwrap();
    assert!(registry::server(&state, "s1").await.is_err());
    assert!(recover_after_restart(&state).await.is_err());
}

#[tokio::test]
async fn restart_recovery_rolls_back_if_recording_fails() {
    let (_dir, state, _, _) = setup().await;
    sqlx::query("INSERT INTO operations (id, idempotency_key, principal, source, request_json, status, admitted_server_ids_json, created_at) VALUES ('op', 'key', 'admin:test', 'admin', '{}', 'running', '[\"s1\"]', ?)")
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO uploads (id, principal, request_json, status, operation_id, created_at) VALUES ('up', 'admin:test', '{}', 'publishing', 'op', ?)")
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_upload_recovery BEFORE UPDATE ON uploads BEGIN SELECT RAISE(FAIL, 'injected'); END")
        .execute(&state.db)
        .await
        .unwrap();
    assert!(recover_after_restart(&state).await.is_err());
    let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id = 'op'")
        .fetch_one(&state.db)
        .await
        .unwrap();
    assert_eq!(status, "running");
    assert_eq!(
        registry::server(&state, "s1")
            .await
            .unwrap()
            .maintenance
            .phase,
        Phase::Idle
    );
}
