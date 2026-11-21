//! The Immich write barrier.
//!
//! ISM owns file detection, so Immich's library watcher and scheduled library scan stay off
//! permanently: [`apply_immich_settings`] turns them off at enrollment, and a periodic check
//! raises a drift alert (refusing writes) when either comes back on. Before a managed operation
//! touches library files, the barrier pauses Immich's `library` queue, the only queue that
//! imports files or marks them offline, and confirms on [`IDLE_CONFIRMATIONS`](ism_core::maintenance::IDLE_CONFIRMATIONS) consecutive polls
//! that it is paused with no active job. Jobs queued while it is paused (a scan an administrator
//! starts in Immich, say) wait in `waiting` and only run after the release, which was verified
//! on stock Immich v3.4.1, so they do not hold up admission. Overlapping operations share the pause through a persisted reference
//! count; the last release resumes the queue and wakes the scan scheduler. Anything that cannot
//! be verified keeps writes refused (fail closed).
//!
//! Other queues keep running. `sidecar` and `metadataExtraction` jobs read files by the asset's
//! database path; a managed move changes the file and the row in one checked transaction, so a
//! job racing a move reads a missing file and fails without changing the asset, and a sidecar
//! row that changed meanwhile makes the transaction's row check fail (recovery, not data loss).

use std::collections::HashMap;
use std::time::Duration;

use ism_core::dto::{CreateUpload, OperationAction};
use ism_core::maintenance::{Admission, IdleConfirmation, MaintenanceState, Phase};

use crate::error::{AppError, AppResult};
use crate::immich::api::{self, Api, SCAN_ENABLED, WATCH_ENABLED};
use crate::immich::registry::{self, Server};
use crate::state::{AppState, ServerEvent};

/// The queue the barrier pauses.
pub const LIBRARY_QUEUE: &str = "library";

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

fn immich(e: api::ApiError) -> AppError {
    AppError::conflict("immich_error", crate::error::redact(&e.to_string()))
}

fn blocked(message: impl Into<String>) -> AppError {
    AppError::conflict("maintenance_blocked", message)
}

/// Waits until the server admits one managed operation, pausing the library queue if needed.
/// The barrier was verified end to end against stock Immich v3.4.1
/// (`docker/immich-proof/RESULTS.md`).
pub async fn acquire(state: &AppState, server_id: &str) -> AppResult<()> {
    let deadline = tokio::time::Instant::now() + state.config.timing.admission_timeout;
    loop {
        let (admission, _) = update(state, server_id, |m| {
            let a = m.admission();
            match a {
                Admission::Admit => m.admit(),
                Admission::Prepare => m.begin_prepare(),
                Admission::Wait | Admission::Deny(_) => {}
            }
            a
        })
        .await?;
        match admission {
            Admission::Admit => return Ok(()),
            Admission::Prepare => return prepare(state, server_id).await,
            Admission::Deny(message) => return Err(blocked(message)),
            Admission::Wait => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(blocked("Immich did not become ready for changes in time"));
                }
                state.maintenance_wake.notify_waiters();
                tokio::time::sleep(state.config.timing.poll).await;
            }
        }
    }
}

/// Checks the settings, pauses the library queue and confirms it idle. On failure the queue is
/// resumed (when possible) and the server blocked; no file has been touched.
async fn prepare(state: &AppState, server_id: &str) -> AppResult<()> {
    let server = registry::server(state, server_id).await?;
    let outcome = match registry::admin_api(state, &server).await {
        Ok(api) => pause_confirmed(state, &api)
            .await
            .map_err(|e| (Some(api), e)),
        Err(e) => Err((None, PauseError::Other(e))),
    };
    match outcome {
        Ok(()) => {
            update(state, server_id, MaintenanceState::prepared).await?;
            tracing::info!(server = %server.name, "library queue paused; changes admitted");
            Ok(())
        }
        Err((api, PauseError::Drift(message))) => {
            let resumed = resume_queue(api.as_ref()).await;
            update(state, server_id, |m| {
                m.drift = Some(message.clone());
                if resumed {
                    m.resumed();
                } else {
                    m.block("could not resume Immich's library queue; retry");
                }
            })
            .await?;
            tracing::warn!(server = %server.name, "{message}");
            Err(blocked(message))
        }
        Err((api, PauseError::Other(e))) => {
            let message = format!("could not pause Immich safely: {}", e.message());
            let resumed = resume_queue(api.as_ref()).await;
            update(state, server_id, |m| {
                if resumed {
                    m.queue_paused_by_ism = false;
                }
                m.block(message.clone());
            })
            .await?;
            Err(blocked(message))
        }
    }
}

/// Resumes the library queue; false when that failed or no API is available.
async fn resume_queue(api: Option<&Api>) -> bool {
    match api {
        Some(api) => api.set_queue_paused(LIBRARY_QUEUE, false).await.is_ok(),
        None => false,
    }
}

enum PauseError {
    Drift(String),
    Other(AppError),
}

impl From<AppError> for PauseError {
    fn from(e: AppError) -> Self {
        Self::Other(e)
    }
}

/// The drift message when Immich's watcher or scheduled scan is not off.
async fn settings_drift(api: &Api) -> AppResult<Option<String>> {
    let config = api.get_config().await.map_err(immich)?;
    Ok(ism_core::maintenance::settings_drift(
        api::config_bool(&config, WATCH_ENABLED),
        api::config_bool(&config, SCAN_ENABLED),
    ))
}

async fn library_queue(api: &Api) -> AppResult<api::Queue> {
    let queues = api.queues().await.map_err(immich)?;
    queues
        .into_iter()
        .find(|q| q.name == LIBRARY_QUEUE)
        .ok_or_else(|| blocked("Immich queue 'library' not found"))
}

/// Pauses the library queue and requires several consecutive reads showing it paused with no
/// active job. Jobs queued meanwhile (an administrator clicking Scan, for example) wait in the
/// paused queue and only run after the release, so `waiting`/`delayed` jobs do not delay
/// admission; waiting for them would deadlock behind a held scan.
async fn pause_confirmed(state: &AppState, api: &Api) -> Result<(), PauseError> {
    let timing = &state.config.timing;
    if let Some(drift) = settings_drift(api).await? {
        return Err(PauseError::Drift(drift));
    }
    let deadline = tokio::time::Instant::now() + timing.quiesce_timeout;
    api.set_queue_paused(LIBRARY_QUEUE, true)
        .await
        .map_err(immich)?;
    tokio::time::sleep(timing.settle).await;
    let mut confirmation = IdleConfirmation::default();
    loop {
        let queue = library_queue(api).await?;
        if confirmation.observe(queue.is_held()) {
            break;
        }
        if !queue.is_paused {
            // Someone resumed it in between: pause again and start counting over.
            api.set_queue_paused(LIBRARY_QUEUE, true)
                .await
                .map_err(immich)?;
        }
        if tokio::time::Instant::now() >= deadline {
            let reason = if queue.is_paused {
                "a library job was still running"
            } else {
                "the queue did not stay paused"
            };
            return Err(blocked(format!(
                "could not hold Immich's library queue in time: {reason}"
            ))
            .into());
        }
        tokio::time::sleep(timing.poll).await;
    }
    // Settings may have changed while we waited.
    if let Some(drift) = settings_drift(api).await? {
        return Err(PauseError::Drift(drift));
    }
    Ok(())
}

/// One admitted operation finished. The last one resumes the queue and wakes due scans.
pub async fn release(state: &AppState, server_id: &str) -> AppResult<()> {
    let (last, _) = update(state, server_id, MaintenanceState::settle).await?;
    if last {
        resume(state, server_id).await?;
    }
    Ok(())
}

/// Resumes a queue ISM paused. Failures block the server (shown with a retry action).
async fn resume(state: &AppState, server_id: &str) -> AppResult<()> {
    let server = registry::server(state, server_id).await?;
    let result = match registry::admin_api(state, &server).await {
        Ok(api) => api
            .set_queue_paused(LIBRARY_QUEUE, false)
            .await
            .map_err(immich),
        Err(e) => Err(e),
    };
    match result {
        Ok(()) => {
            update(state, server_id, |m| {
                if m.phase == Phase::Resuming {
                    m.resumed();
                }
            })
            .await?;
            state.scan_wake.notify_one();
        }
        Err(e) => {
            tracing::warn!(server = %server.name, "cannot resume the library queue: {}", e.message());
            update(state, server_id, |m| {
                if m.phase == Phase::Resuming {
                    m.block(format!(
                        "could not resume Immich's library queue: {}; retry",
                        e.message()
                    ));
                }
            })
            .await?;
        }
    }
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

/// Admin action: all recoveries were handled.
pub async fn resolve_recovery(state: &AppState, server_id: &str) -> AppResult<()> {
    update(state, server_id, MaintenanceState::resolve_one).await?;
    state.maintenance_wake.notify_waiters();
    Ok(())
}

/// Resolves an operation and its server barriers in one SQLite transaction.
pub async fn resolve_operation_recovery(state: &AppState, id: &str) -> AppResult<()> {
    let _guard = state.maintenance_lock.lock().await;
    let mut tx = state.db.begin().await?;
    let row: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT status, admitted_server_ids_json, recovery_resolved FROM operations WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((status, servers, resolved)) = row else {
        return Err(AppError::not_found("operation not found"));
    };
    let servers: Vec<String> = serde_json::from_str(&servers)?;
    if status != "recovery_required"
        && !(status == "partial" && resolved == 0 && !servers.is_empty())
    {
        return Err(AppError::conflict(
            "not_recoverable",
            "this operation does not need recovery",
        ));
    }
    if resolved != 0 {
        return Ok(());
    }
    let servers: std::collections::BTreeSet<_> = servers.into_iter().collect();
    for server_id in &servers {
        let json: String =
            sqlx::query_scalar("SELECT maintenance_json FROM immich_servers WHERE id = ?")
                .bind(server_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| AppError::not_found("Immich server not found"))?;
        let mut maintenance: MaintenanceState = serde_json::from_str(&json)?;
        maintenance.resolve_one();
        sqlx::query("UPDATE immich_servers SET maintenance_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&maintenance)?)
            .bind(server_id)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("UPDATE operations SET recovery_resolved = 1 WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    for server_id in servers {
        state.emit(ServerEvent::MaintenanceChanged { server_id });
    }
    state.maintenance_wake.notify_waiters();
    Ok(())
}

/// Admin action after a blocked barrier: resume a queue ISM still holds, then allow work.
pub async fn retry(state: &AppState, server_id: &str) -> AppResult<()> {
    let (retried, _) = update(state, server_id, MaintenanceState::retry).await?;
    if !retried {
        return Err(blocked("the barrier is not blocked"));
    }
    state.maintenance_wake.notify_waiters();
    Ok(())
}

/// Turns Immich's file watcher and scheduled library scan off and verifies the result.
pub async fn apply_immich_settings(api: &Api) -> AppResult<()> {
    let mut config = api.get_config().await.map_err(immich)?;
    let watch = api::config_bool(&config, WATCH_ENABLED);
    let scan = api::config_bool(&config, SCAN_ENABLED);
    if watch.is_none() || scan.is_none() {
        return Err(blocked("unexpected Immich config shape"));
    }
    if watch != Some(false) || scan != Some(false) {
        api::set_config_bool(&mut config, WATCH_ENABLED, false);
        api::set_config_bool(&mut config, SCAN_ENABLED, false);
        api.put_config(&config).await.map_err(immich)?;
    }
    if settings_drift(api).await?.is_some() {
        return Err(blocked(
            "Immich did not apply the watcher and scheduled scan settings",
        ));
    }
    Ok(())
}

/// Enrollment: once an administrator account exists, Immich's watcher and scheduled scan go off.
/// If that fails the server shows a drift alert (writes refused) until the settings are applied.
pub async fn enroll(state: &AppState, server_id: &str) -> AppResult<()> {
    let server = registry::server(state, server_id).await?;
    let result = match registry::admin_api(state, &server).await {
        Ok(api) => apply_immich_settings(&api).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(()) => {
            update(state, server_id, |m| m.drift = None).await?;
            Ok(())
        }
        Err(e) => {
            let message = format!(
                "could not turn off Immich's file watcher and scheduled library scan: {}",
                e.message()
            );
            update(state, server_id, |m| m.drift = Some(message.clone())).await?;
            Err(blocked(message))
        }
    }
}

/// Admin action acknowledging a drift alert: re-applies ISM's Immich settings and clears it.
pub async fn reapply_settings(state: &AppState, server_id: &str) -> AppResult<()> {
    enroll(state, server_id).await?;
    tracing::info!(server = %server_id, "Immich watcher and scheduled scan re-disabled");
    state.maintenance_wake.notify_waiters();
    Ok(())
}

/// Detects drift on one server. The alert stays until an admin re-applies the settings, even if
/// the setting is turned off again in Immich: changes may have been imported meanwhile.
async fn check_drift(state: &AppState, server: &Server) -> AppResult<()> {
    if !server.compatible || server.maintenance.drift.is_some() {
        return Ok(());
    }
    let api = registry::admin_api(state, server).await?;
    if let Some(message) = settings_drift(&api).await? {
        tracing::warn!(server = %server.name, "{message}");
        update(state, &server.id, |m| m.drift = Some(message)).await?;
    }
    Ok(())
}

/// Startup: operations interrupted mid-flight may have left files and Immich out of step; a
/// pause nobody holds any more is resumed by the run loop.
pub async fn recover_after_restart(state: &AppState) -> AppResult<()> {
    let _guard = state.maintenance_lock.lock().await;
    let resumable = resumable_publish_uploads(state).await?;
    let mut servers: std::collections::BTreeMap<_, _> = registry::servers(state)
        .await?
        .into_iter()
        .map(|s| (s.id, s.maintenance))
        .collect();
    let mut tx = state.db.begin().await?;
    let interrupted: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, admitted_server_ids_json FROM operations WHERE status = 'running'",
    )
    .fetch_all(&mut *tx)
    .await?;
    for (op_id, admitted_servers) in &interrupted {
        if resumable.contains_key(op_id) {
            continue;
        }
        for server_id in serde_json::from_str::<Vec<String>>(admitted_servers)? {
            let m = servers.get_mut(&server_id).ok_or_else(|| {
                AppError::internal(format!(
                    "interrupted operation {op_id} references a missing server"
                ))
            })?;
            m.unresolved_recoveries += 1;
            m.require_recovery(format!("operation {op_id} was interrupted by a restart"));
        }
        sqlx::query(
            "UPDATE operations SET status = 'recovery_required', finished_at = ? WHERE id = ?",
        )
        .bind(crate::db::now())
        .bind(op_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE operation_items SET status = 'recovery_required' WHERE operation_id = ? AND status = 'running'")
            .bind(op_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE uploads SET status = 'failed' WHERE operation_id = ? AND status IN ('publishing', 'committed')")
            .bind(op_id)
            .execute(&mut *tx)
            .await?;
        tracing::warn!(operation = %op_id, "operation interrupted by restart; recovery required");
    }
    for (op_id, upload_id) in resumable {
        sqlx::query(
            "UPDATE operations
             SET status = 'queued', started_at = NULL, finished_at = NULL
             WHERE id = ? AND status IN ('running', 'recovery_required') AND recovery_resolved = 0",
        )
        .bind(&op_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE operation_items
             SET status = 'pending', error_code = NULL, error_message = NULL,
                 result_json = NULL, journal_json = NULL, progress_json = NULL
             WHERE operation_id = ?",
        )
        .bind(&op_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE uploads SET status = 'publishing' WHERE id = ? AND operation_id = ?")
            .bind(upload_id)
            .bind(&op_id)
            .execute(&mut *tx)
            .await?;
        tracing::info!(operation = %op_id, "untouched upload requeued after restart");
    }
    sqlx::query("UPDATE operations SET status = 'queued' WHERE status = 'waiting_maintenance'")
        .execute(&mut *tx)
        .await?;
    for (server_id, m) in &mut servers {
        m.after_restart();
        sqlx::query("UPDATE immich_servers SET maintenance_json = ? WHERE id = ?")
            .bind(serde_json::to_string(m)?)
            .bind(server_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    state.ops_wake.notify_waiters();
    Ok(())
}

/// Requeues an otherwise untouched peer upload that was failed before its first execution.
///
/// The staging files must still match the recorded sizes and hashes. No library file is changed by
/// this function; the normal managed operation performs and verifies publication afterwards.
pub async fn retry_failed_publish_upload(state: &AppState, operation_id: &str) -> AppResult<()> {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM operations WHERE id = ?")
        .bind(operation_id)
        .fetch_optional(&state.db)
        .await?;
    let Some(status) = status else {
        return Err(AppError::not_found("operation not found"));
    };
    if status != "failed" {
        return Err(AppError::conflict(
            "publish_retry_invalid",
            "only a failed publication can be retried this way",
        ));
    }
    let Some(upload_id) = resumable_publish_upload(state, operation_id).await? else {
        return Err(AppError::conflict(
            "publish_retry_unverified",
            "the staged upload is missing, changed, or no longer matches an untouched peer publication",
        ));
    };
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let operation = sqlx::query(
        "UPDATE operations
         SET status = 'queued', started_at = NULL, finished_at = NULL
         WHERE id = ? AND status = 'failed' AND recovery_resolved = 0",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    if operation.rows_affected() != 1 {
        return Err(AppError::conflict(
            "publish_retry_invalid",
            "the operation changed while it was being verified",
        ));
    }
    let item = sqlx::query(
        "UPDATE operation_items
         SET status = 'pending', error_code = NULL, error_message = NULL,
             result_json = NULL, journal_json = NULL, progress_json = NULL
         WHERE operation_id = ? AND status = 'pending' AND kind = 'publish_upload'",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    if item.rows_affected() != 1 {
        return Err(AppError::conflict(
            "publish_retry_invalid",
            "the upload action changed while it was being verified",
        ));
    }
    let upload = sqlx::query(
        "UPDATE uploads
         SET status = 'publishing'
         WHERE id = ? AND operation_id = ? AND status = 'failed'",
    )
    .bind(&upload_id)
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    if upload.rows_affected() != 1 {
        return Err(AppError::conflict(
            "publish_retry_invalid",
            "the upload record changed while it was being verified",
        ));
    }
    tx.commit().await?;
    state.ops_wake.notify_waiters();
    state.emit(ServerEvent::OperationUpdated {
        operation_id: operation_id.to_owned(),
    });
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn resumable_publish_uploads(state: &AppState) -> AppResult<HashMap<String, String>> {
    let candidates: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM operations
         WHERE status = 'running'
            OR (status = 'recovery_required' AND recovery_resolved = 0)",
    )
    .fetch_all(&state.db)
    .await?;
    let mut found = HashMap::new();
    for id in candidates {
        if let Some(upload_id) = resumable_publish_upload(state, &id).await? {
            found.insert(id, upload_id);
        }
    }
    Ok(found)
}

async fn resumable_publish_upload(
    state: &AppState,
    operation_id: &str,
) -> AppResult<Option<String>> {
    type OperationRow = (String, String, String, i64);
    let Some((principal, request, admitted, cancelled)): Option<OperationRow> = sqlx::query_as(
        "SELECT principal,request_json,admitted_server_ids_json,cancel_requested
         FROM operations WHERE id = ?",
    )
    .bind(operation_id)
    .fetch_optional(&state.db)
    .await?
    else {
        return Ok(None);
    };
    let admitted: Vec<String> = serde_json::from_str(&admitted)?;
    if cancelled != 0 || !admitted.is_empty() {
        return Ok(None);
    }
    let request: serde_json::Value = serde_json::from_str(&request)?;
    let Some(actions) = request["actions"].as_array() else {
        return Ok(None);
    };
    let [action] = actions.as_slice() else {
        return Ok(None);
    };
    let action = crate::ops::exec::parse_action(&serde_json::to_string(action)?)?;
    let OperationAction::PublishUpload { upload_id } = action else {
        return Ok(None);
    };

    type ItemRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let items: Vec<ItemRow> = sqlx::query_as(
        "SELECT kind,action_json,status,error_code,error_message,result_json,journal_json,progress_json
         FROM operation_items WHERE operation_id = ? ORDER BY idx",
    )
    .bind(operation_id)
    .fetch_all(&state.db)
    .await?;
    let [(kind, action, status, error_code, error_message, result, journal, progress)] =
        items.as_slice()
    else {
        return Ok(None);
    };
    if kind != "publish_upload"
        || !matches!(status.as_str(), "pending" | "running" | "recovery_required")
        || error_code.is_some()
        || error_message.is_some()
        || result.is_some()
        || journal.is_some()
        || progress.is_some()
    {
        return Ok(None);
    }
    match crate::ops::exec::parse_action(action)? {
        OperationAction::PublishUpload {
            upload_id: item_upload,
        } if item_upload == upload_id => {}
        _ => return Ok(None),
    }

    type UploadRow = (String, String, String, String);
    let uploads: Vec<UploadRow> = sqlx::query_as(
        "SELECT id,principal,request_json,status FROM uploads WHERE operation_id = ?",
    )
    .bind(operation_id)
    .fetch_all(&state.db)
    .await?;
    let [(id, owner, upload_request, upload_status)] = uploads.as_slice() else {
        return Ok(None);
    };
    if id != &upload_id
        || owner != &principal
        || !matches!(upload_status.as_str(), "publishing" | "failed")
    {
        return Ok(None);
    }
    let upload: CreateUpload = serde_json::from_str(upload_request)?;
    if upload.files.is_empty() {
        return Ok(None);
    }
    let root = match crate::files::root(&state.db, &upload.target.root_id).await {
        Ok(root) => root,
        Err(_) => return Ok(None),
    };
    if root.ensure_available().is_err() {
        return Ok(None);
    }
    let staging = crate::ops::plan::staging_dir(&root, &upload_id)?;
    let checks = upload
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            (
                staging.join(index.to_string()),
                file.size,
                file.sha256.clone(),
            )
        })
        .collect::<Vec<_>>();
    let valid = tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
        for (path, expected_size, expected_hash) in checks {
            let Some((_, hash)) = crate::files::replacement_fingerprint(&path)? else {
                return Ok(false);
            };
            if std::fs::metadata(path)?.len() != expected_size || hash != expected_hash {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
    .map_err(AppError::internal)??;
    Ok(valid.then_some(upload_id))
}

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    let mut checked: HashMap<String, tokio::time::Instant> = HashMap::new();
    loop {
        if let Err(e) = step(&state, &mut checked).await {
            tracing::warn!("maintenance: {}", e.message());
        }
        tokio::select! {
            () = tokio::time::sleep(tick) => {}
            () = state.maintenance_wake.notified() => {}
        }
    }
}

/// Resumes pauses nobody holds and checks settings drift (at most every `drift_check`).
async fn step(
    state: &AppState,
    checked: &mut HashMap<String, tokio::time::Instant>,
) -> AppResult<()> {
    let now = tokio::time::Instant::now();
    for server in registry::servers(state).await? {
        if server.maintenance.phase == Phase::Resuming
            && let Err(e) = resume(state, &server.id).await
        {
            tracing::warn!(server = %server.name, "resume failed: {}", e.message());
        }
        let due = checked
            .get(&server.id)
            .is_none_or(|last| now.duration_since(*last) >= state.config.timing.drift_check);
        if due {
            checked.insert(server.id.clone(), now);
            if let Err(e) = check_drift(state, &server).await {
                tracing::debug!(server = %server.name, "drift check skipped: {}", e.message());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
