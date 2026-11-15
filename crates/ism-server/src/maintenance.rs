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
    Ok(())
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
