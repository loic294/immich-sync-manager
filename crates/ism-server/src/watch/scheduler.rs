//! Debounced, API-first Immich synchronization with folder-scoped scan fallbacks.
//!
//! Requests retain exact Immich paths across restarts. Every request restarts the library's
//! two-minute timer. Once the write barrier allows it, file APIs handle additions/removals;
//! only unsupported work scans affected folders. Successful synchronization announces
//! [`ServerEvent::LibraryScanned`] even when no scan was necessary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use ism_core::dto::{ImmichMoveMode, ScanScheduleView};
use ism_core::maintenance::IdleConfirmation;
use serde::{Deserialize, Serialize};

use super::selfwrite::now_ms;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::immich::registry;
use crate::state::{AppState, ServerEvent};

const MAX_REASONS: usize = 10;
const REMOVED_FILE_EXISTS: &str = "Removed file still exists";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WorkKind {
    Added,
    Changed,
    Removed,
    Folder,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Work {
    pub path: String,
    pub kind: WorkKind,
}

impl Work {
    pub fn folder_for_file(path: &str) -> AppResult<Self> {
        let parent = Path::new(path)
            .parent()
            .filter(|p| p.is_absolute())
            .and_then(Path::to_str)
            .ok_or_else(|| AppError::invalid_location("file has no absolute parent folder"))?;
        Ok(Self {
            path: parent.to_owned(),
            kind: WorkKind::Folder,
        })
    }
}

fn delay_ms(state: &AppState) -> i64 {
    i64::try_from(state.config.timing.scan_delay.as_millis()).unwrap_or(i64::MAX / 4)
}

/// Schedules (or postpones) scoped synchronization, preserving requests made during a run.
pub(crate) async fn request_work(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    reason: &str,
    work: &[Work],
) -> AppResult<()> {
    request_work_at(state, server_id, library_id, reason, work, now_ms()).await
}

pub(crate) async fn request_work_at(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    reason: &str,
    work: &[Work],
    now_ms: i64,
) -> AppResult<()> {
    if work.is_empty() || work.iter().any(|w| !Path::new(&w.path).is_absolute()) {
        return Err(AppError::invalid_location(
            "synchronization requires absolute scoped paths",
        ));
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let reasons: Option<String> = sqlx::query_scalar(
        "SELECT reasons_json FROM scan_schedule WHERE library_id = ? AND due_at IS NOT NULL",
    )
    .bind(library_id)
    .fetch_optional(&mut *tx)
    .await?;
    let mut reasons: Vec<String> = reasons
        .and_then(|r| serde_json::from_str(&r).ok())
        .unwrap_or_default();
    reasons.retain(|r| r != reason);
    reasons.push(reason.to_owned());
    if reasons.len() > MAX_REASONS {
        reasons.drain(..reasons.len() - MAX_REASONS);
    }
    sqlx::query(
        "INSERT INTO scan_schedule (library_id, server_id, due_at, reasons_json) VALUES (?, ?, ?, ?)
         ON CONFLICT(library_id) DO UPDATE SET server_id = excluded.server_id, due_at = excluded.due_at,
           reasons_json = excluded.reasons_json",
    )
    .bind(library_id)
    .bind(server_id)
    .bind(now_ms.saturating_add(delay_ms(state)))
    .bind(serde_json::to_string(&reasons)?)
    .execute(&mut *tx)
    .await?;
    for item in work {
        sqlx::query("INSERT INTO scan_work (library_id, work_json) VALUES (?, ?)")
            .bind(library_id)
            .bind(serde_json::to_string(item)?)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    state.scan_wake.notify_one();
    state.emit(ServerEvent::WatchChanged);
    Ok(())
}

pub async fn run(state: AppState) {
    if let Err(e) = recover(&state).await {
        tracing::warn!("scan scheduler: {e}");
    }
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        let wait = match step(&state, now_ms()).await {
            Ok(Some(next)) => {
                Duration::from_millis(u64::try_from(next - now_ms()).unwrap_or(0)).min(tick)
            }
            Ok(None) => tick,
            Err(e) => {
                tracing::warn!("scan scheduler: {}", e.message());
                tick
            }
        };
        tokio::select! {
            () = tokio::time::sleep(wait.max(Duration::from_millis(10))) => {}
            () = state.scan_wake.notified() => {}
        }
    }
}

pub(crate) async fn recover(state: &AppState) -> AppResult<()> {
    sqlx::query(
        "UPDATE scan_schedule SET running = 0, running_through = NULL,
         due_at = COALESCE(due_at, ?) WHERE running = 1",
    )
    .bind(now_ms())
    .execute(&state.db)
    .await?;
    let restored = sqlx::query(
        "UPDATE scan_schedule SET due_at = ?
         WHERE running = 0 AND due_at IS NULL AND last_error = ?
           AND EXISTS (SELECT 1 FROM scan_work WHERE scan_work.library_id = scan_schedule.library_id)",
    )
    .bind(now_ms())
    .bind(format!("Immich answered 400: {REMOVED_FILE_EXISTS}"))
    .execute(&state.db)
    .await?
    .rows_affected();
    if restored > 0 {
        tracing::info!(
            libraries = restored,
            "rescheduled retained stale-removal work"
        );
    }
    Ok(())
}

async fn recreated_file_exists(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    path: &str,
) -> AppResult<bool> {
    let roots: Vec<_> = files::load_roots(&state.db)
        .await?
        .into_iter()
        .filter(|root| {
            root.server_id.as_deref() == Some(server_id)
                && root.library_id.as_deref() == Some(library_id)
        })
        .collect();
    let (root, rel) = files::resolve_immich_path(&roots, path).ok_or_else(|| {
        AppError::conflict(
            "unverified_removal",
            "Cannot verify the file against an available library mapping; synchronization remains queued",
        )
    })?;
    root.ensure_available()?;
    let path = root.abs(&files::parse_rel(rel.as_str())?)?;
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn verify_recreated_files(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    paths: &[String],
) -> AppResult<()> {
    for path in paths {
        if !recreated_file_exists(state, server_id, library_id, path).await? {
            return Err(AppError::conflict(
                "unverified_removal",
                "A file changed during synchronization; its removal remains queued",
            ));
        }
    }
    if !paths.is_empty() {
        tracing::info!(library = %library_id, files = paths.len(), "synchronized files that reappeared after removal");
    }
    Ok(())
}

/// Starts every due synchronization whose server allows it.
pub(crate) async fn step(state: &AppState, now_ms: i64) -> AppResult<Option<i64>> {
    let due: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT library_id, server_id, due_at FROM scan_schedule WHERE due_at IS NOT NULL AND running = 0",
    )
    .fetch_all(&state.db)
    .await?;
    let mut next: Option<i64> = None;
    for (library_id, server_id, due_at) in due {
        if due_at > now_ms {
            next = Some(next.map_or(due_at, |n| n.min(due_at)));
            continue;
        }
        let server = registry::server(state, &server_id).await?;
        if !server.maintenance.scans_allowed() {
            // The barrier release wakes the scheduler.
            continue;
        }
        let started = sqlx::query(
            "UPDATE scan_schedule SET due_at = NULL, running = 1, last_started_at = ?, last_error = NULL,
             running_through = (SELECT MAX(id) FROM scan_work WHERE library_id = ?)
             WHERE library_id = ? AND due_at = ? AND running = 0",
        )
        .bind(crate::db::now())
        .bind(&library_id)
        .bind(&library_id)
        .bind(due_at)
        .execute(&state.db)
        .await?
        .rows_affected();
        if started == 1 {
            state.emit(ServerEvent::WatchChanged);
            let state = state.clone();
            tokio::spawn(async move {
                let result = scan(&state, &server_id, &library_id).await;
                if let Err(e) = finish(&state, &server_id, &library_id, result).await {
                    tracing::warn!(library = %library_id, "scan bookkeeping failed: {}", e.message());
                }
            });
        }
    }
    Ok(next)
}

async fn scan(state: &AppState, server_id: &str, library_id: &str) -> AppResult<bool> {
    let task = crate::activity::Task::start(
        state,
        format!("library:{library_id}"),
        "Scoped library synchronization",
        library_id,
        Some("/mappings"),
    )
    .await?;
    let result = scan_reporting(state, server_id, library_id, &task).await;
    task.finish_with_phase(
        &result,
        if matches!(result, Ok(false)) {
            "Attempt deferred; scoped work remains queued behind maintenance"
        } else {
            "Scoped synchronization finished"
        },
    )
    .await?;
    result
}

async fn scan_reporting(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    task: &crate::activity::Reporter,
) -> AppResult<bool> {
    // Direct file APIs do not pass through Immich's library queue. Serialize dispatch
    // with barrier admission, but release this lock before waiting for queued scans.
    task.waiting("Waiting for maintenance admission");
    let guard = state.maintenance_lock.lock().await;
    task.phase("Preparing scoped synchronization");
    let server = registry::server(state, server_id).await?;
    if !server.maintenance.scans_allowed() {
        return Ok(false);
    }
    let api = registry::admin_api(state, &server).await?;
    let immich = |e: crate::immich::api::ApiError| {
        AppError::conflict("immich_error", crate::error::redact(&e.to_string()))
    };
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT work_json FROM scan_work WHERE library_id = ?
         AND id <= (SELECT running_through FROM scan_schedule WHERE library_id = ?) ORDER BY id",
    )
    .bind(library_id)
    .bind(library_id)
    .fetch_all(&state.db)
    .await?;
    if rows.is_empty() {
        return Err(AppError::conflict(
            "missing_scan_scope",
            "synchronization has no scoped paths",
        ));
    }
    let mut files = BTreeMap::new();
    let mut folders = BTreeSet::new();
    let mut recreated = Vec::new();
    for row in rows {
        let work: Work = serde_json::from_str(&row)?;
        if work.kind == WorkKind::Folder {
            folders.insert(work.path);
        } else {
            let kind = match (files.get(&work.path), work.kind) {
                (Some(WorkKind::Added), WorkKind::Changed) => WorkKind::Added,
                (Some(WorkKind::Removed | WorkKind::Changed), WorkKind::Added) => WorkKind::Changed,
                _ => work.kind,
            };
            files.insert(work.path, kind);
        }
    }
    let total = files.len() as u64;
    task.progress(0, Some(total), "paths dispatched");
    for (index, (path, kind)) in files.into_iter().enumerate() {
        task.phase("Synchronizing files through the Immich API");
        task.progress(index as u64, Some(total), "paths dispatched");
        if server.external_library_move_mode == ImmichMoveMode::DbOnly {
            if kind == WorkKind::Removed {
                return Err(AppError::conflict(
                    "file_sync_unavailable",
                    "outside removals require the external-library file remove API; folder scans cannot reconcile removals",
                ));
            }
            folders.insert(Work::folder_for_file(&path)?.path);
            continue;
        }
        // The add endpoint is deliberately a no-op for existing assets. There is no update
        // endpoint; changed files (including sidecars) can only request a folder scan.
        let result = match kind {
            WorkKind::Added => Some(api.sync_library_file_add(library_id, &path).await),
            WorkKind::Removed => Some(api.sync_library_file_remove(library_id, &path).await),
            WorkKind::Changed | WorkKind::Folder => None,
        };
        match result {
            Some(Ok(())) => continue,
            Some(Err(error)) => {
                let stale_removal = kind == WorkKind::Removed
                    && matches!(&error, crate::immich::api::ApiError::Status {
                        status: 400, message
                    } if message == REMOVED_FILE_EXISTS);
                if stale_removal
                    && recreated_file_exists(state, server_id, library_id, &path).await?
                {
                    api.sync_library_file_add(library_id, &path)
                        .await
                        .map_err(immich)?;
                    recreated.push(path);
                    continue;
                }
                let unavailable = error.is_external_library_api_unavailable()
                    && server.external_library_move_mode == ImmichMoveMode::ApiWithDbFallback
                    && server.external_library_api_capable != Some(true);
                let unsupported = matches!(&error, crate::immich::api::ApiError::Status {
                    status: 400, message
                } if message == "Added file is not a supported image or video");
                // Folder scans only discover files in the fork; they cannot reconcile
                // failed removals. Keep that failure visible rather than reporting success.
                if kind == WorkKind::Removed || !(unavailable || unsupported) {
                    return Err(immich(error));
                }
                tracing::warn!(library = %library_id, "file synchronization failed; requesting a scoped fallback: {}", crate::error::redact(&error.to_string()));
            }
            None => {}
        }
        folders.insert(Work::folder_for_file(&path)?.path);
    }
    let mut scanned: Vec<String> = Vec::new();
    task.progress(total, Some(total), "paths dispatched");
    if !folders.is_empty() {
        let capable = match server.external_library_api_capable {
            Some(capable) => capable,
            None => api.external_library_api_capable().await.map_err(immich)?,
        };
        if !capable {
            return Err(AppError::conflict(
                "targeted_scan_unavailable",
                "the Immich fork's scoped scan API is required; refusing a potentially full-library scan",
            ));
        }
    }
    for folder in folders {
        if scanned
            .iter()
            .any(|parent| Path::new(&folder).starts_with(parent))
        {
            continue;
        }
        task.phase("Dispatching scoped folder scans to Immich");
        api.scan_library_folder(library_id, &folder)
            .await
            .map_err(immich)?;
        scanned.push(folder);
    }
    drop(guard);
    if scanned.is_empty() {
        verify_recreated_files(state, server_id, library_id, &recreated).await?;
        return Ok(true);
    }
    let timing = &state.config.timing;
    let mut deadline = tokio::time::Instant::now() + timing.scan_timeout;
    let mut idle = IdleConfirmation::default();
    task.waiting("Waiting for Immich to finish the library queue and confirm it idle");
    loop {
        tokio::time::sleep(timing.poll).await;
        let queues = api.queues().await.map_err(immich)?;
        let library = queues
            .iter()
            .find(|q| q.name == crate::maintenance::LIBRARY_QUEUE);
        if let Some(queue) = library {
            task.progress(
                queue.statistics.active.max(0) as u64,
                None,
                "active Immich jobs",
            );
        }
        task.waiting(if library.is_some_and(|q| q.is_paused) {
            "Immich library queue is paused by a write barrier; waiting for release"
        } else {
            "Waiting for Immich library queue idle confirmation"
        });
        if idle.observe(library.is_some_and(crate::immich::api::Queue::is_idle)) {
            verify_recreated_files(state, server_id, library_id, &recreated).await?;
            return Ok(true);
        }
        if library.is_some_and(|q| q.is_paused) {
            // A write barrier holds the scan job; the timeout counts only while it may run.
            deadline = tokio::time::Instant::now() + timing.scan_timeout;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(AppError::conflict(
                "scan_timeout",
                "the library scan did not finish in time",
            ));
        }
    }
}

async fn finish(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    result: AppResult<bool>,
) -> AppResult<()> {
    if matches!(result, Ok(false)) {
        sqlx::query(
            "UPDATE scan_schedule SET running = 0, running_through = NULL, due_at = COALESCE(due_at, ?)
             WHERE library_id = ?",
        )
        .bind(now_ms())
        .bind(library_id)
        .execute(&state.db)
        .await?;
        state.emit(ServerEvent::WatchChanged);
        state.scan_wake.notify_one();
        return Ok(());
    }
    let error = result.as_ref().err().map(|e| e.message().to_owned());
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    if error.is_none() {
        sqlx::query(
            "DELETE FROM scan_work WHERE library_id = ?
             AND id <= (SELECT running_through FROM scan_schedule WHERE library_id = ?)",
        )
        .bind(library_id)
        .bind(library_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE scan_schedule SET running = 0, running_through = NULL, last_finished_at = ?, last_error = ? WHERE library_id = ?",
    )
    .bind(crate::db::now())
    .bind(&error)
    .bind(library_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    match error {
        Some(e) => tracing::warn!(library = %library_id, "library synchronization failed: {e}"),
        None => library_scanned(state, server_id, library_id),
    }
    state.emit(ServerEvent::WatchChanged);
    state.scan_wake.notify_one();
    Ok(())
}

/// Hook after ISM-requested synchronization finished. Published
/// uploads get their new Immich asset IDs here; other listeners use `LibraryScanned`.
fn library_scanned(state: &AppState, server_id: &str, library_id: &str) {
    tracing::info!(library = %library_id, "library synchronization finished");
    state.emit(ServerEvent::LibraryScanned {
        server_id: server_id.to_owned(),
        library_id: library_id.to_owned(),
    });
    let (state, server_id, library_id) =
        (state.clone(), server_id.to_owned(), library_id.to_owned());
    tokio::spawn(async move {
        match crate::ops::uploads::fill_asset_ids(&state, &server_id, &library_id).await {
            Ok(0) => {}
            Ok(n) => {
                tracing::info!(library = %library_id, "recorded {n} imported upload asset ids")
            }
            Err(e) => {
                tracing::warn!(library = %library_id, "upload asset lookup failed: {}", e.message());
            }
        }
    });
}

/// The scan queue for the admin UI.
pub async fn views(state: &AppState) -> AppResult<Vec<ScanScheduleView>> {
    type Row = (
        String,
        String,
        Option<i64>,
        String,
        bool,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT s.library_id, s.server_id, s.due_at, s.reasons_json, s.running, s.last_started_at,
                s.last_finished_at, s.last_error, l.name
         FROM scan_schedule s LEFT JOIN immich_libraries l ON l.id = s.library_id
         ORDER BY s.due_at IS NULL, s.due_at, s.library_id",
    )
    .fetch_all(&state.db)
    .await?;
    let now = now_ms();
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let allowed = registry::server(state, &r.1)
            .await
            .map(|s| s.maintenance.scans_allowed())
            .unwrap_or(false);
        let due = r.2.filter(|_| !r.4);
        out.push(ScanScheduleView {
            library_id: r.0,
            library_name: r.8,
            server_id: r.1,
            due_at: due
                .and_then(chrono::DateTime::from_timestamp_millis)
                .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            due_in_secs: due.map(|ms| ((ms - now).max(0) + 999) / 1000),
            reasons: serde_json::from_str(&r.3).unwrap_or_default(),
            running: r.4,
            waiting_for_barrier: due.is_some_and(|ms| ms <= now) && !allowed,
            last_started_at: r.5,
            last_finished_at: r.6,
            last_error: r.7,
        });
    }
    Ok(out)
}
