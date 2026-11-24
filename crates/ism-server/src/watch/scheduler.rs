//! Debounced, API-first Immich synchronization with folder-scoped scan fallbacks.
//!
//! Requests retain exact Immich paths across restarts. Every request restarts the library's
//! two-minute timer. Once the write barrier allows it, file APIs handle additions/removals;
//! only unsupported work scans affected folders. Successful synchronization announces
//! [`ServerEvent::LibraryScanned`] even when no scan was necessary.
//!
//! The backlog stays bounded: admission coalesces each exact path into at most a couple of
//! pending rows, a run reads its batch in pages, success acknowledges the batch with one
//! value (`acked_through`), and acknowledged rows are deleted afterwards in small chunks.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::{Duration, Instant};

use ism_core::dto::{ImmichMoveMode, ScanScheduleView};
use ism_core::maintenance::IdleConfirmation;
use serde::{Deserialize, Serialize};
use sqlx::Connection;

use super::selfwrite::now_ms;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::immich::registry;
use crate::state::{AppState, ServerEvent};

const MAX_REASONS: usize = 10;
const REMOVED_FILE_EXISTS: &str = "Removed file still exists";
const READ_PAGE: i64 = 2_000;
const ADMIT_CHUNK: usize = 500;
const COLLECT_CHUNK: i64 = 1_000;
// Lets writers queued behind SQLite's busy handler in between acknowledged-row deletions.
const COLLECT_PAUSE: Duration = Duration::from_millis(25);
const SLOW_WRITER_HOLD: Duration = Duration::from_millis(250);
const SLOW_WRITER_WAIT: Duration = Duration::from_secs(1);

/// Times one scan-backlog write transaction: waiting for a pooled connection, waiting for
/// SQLite's writer lock, and holding it until commit.
struct WriterClock {
    op: &'static str,
    started: Instant,
    pooled: Option<Instant>,
    locked: Option<Instant>,
}

impl WriterClock {
    fn start(op: &'static str) -> Self {
        Self {
            op,
            started: Instant::now(),
            pooled: None,
            locked: None,
        }
    }

    fn pooled(&mut self) {
        self.pooled = Some(Instant::now());
    }

    fn locked(&mut self) {
        self.locked = Some(Instant::now());
    }

    fn report(&self, library_id: &str, items: u64) {
        let pooled = self.pooled.unwrap_or(self.started);
        let locked = self.locked.unwrap_or(pooled);
        let pool_wait = pooled.duration_since(self.started);
        let lock_wait = locked.duration_since(pooled);
        let held = locked.elapsed();
        let (pool_wait_ms, lock_wait_ms, held_ms) = (
            pool_wait.as_millis() as u64,
            lock_wait.as_millis() as u64,
            held.as_millis() as u64,
        );
        if held >= SLOW_WRITER_HOLD || pool_wait + lock_wait >= SLOW_WRITER_WAIT {
            tracing::warn!(op = self.op, library = %library_id, items, pool_wait_ms, lock_wait_ms, held_ms, "slow scan backlog write");
        } else {
            tracing::debug!(op = self.op, library = %library_id, items, pool_wait_ms, lock_wait_ms, held_ms, "scan backlog write");
        }
    }
}

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

    /// Requests for the same key coalesce; files and folders with one path stay separate.
    fn key(&self) -> String {
        match self.kind {
            WorkKind::Folder => format!("folder:{}", self.path),
            _ => format!("file:{}", self.path),
        }
    }
}

/// The kind a path's requests amount to once `next` follows `prev`.
fn coalesce(prev: Option<WorkKind>, next: WorkKind) -> WorkKind {
    match (prev, next) {
        (Some(WorkKind::Added), WorkKind::Changed) => WorkKind::Added,
        (Some(WorkKind::Removed | WorkKind::Changed), WorkKind::Added) => WorkKind::Changed,
        _ => next,
    }
}

fn fold_from(
    start: Option<WorkKind>,
    kinds: impl IntoIterator<Item = WorkKind>,
) -> Option<WorkKind> {
    kinds
        .into_iter()
        .fold(start, |prev, kind| Some(coalesce(prev, kind)))
}

#[derive(Debug, Clone, Copy)]
struct PendingRow {
    id: Option<i64>,
    kind: WorkKind,
    dirty: bool,
}

/// Adds `kind` to one key's pending rows without changing what a run concludes: rows are
/// folded in id order either on their own (the in-flight batch succeeds) or after
/// `in_flight` (it fails and is retried with them). The last row is rewritten when a kind
/// keeps both results exact; otherwise a row is appended, so a key keeps at most a couple of
/// pending rows however many changes arrive.
fn admit(in_flight: &[WorkKind], rows: &mut Vec<PendingRow>, kind: WorkKind) {
    if let Some((last, prefix)) = rows.split_last() {
        let last = last.kind;
        let starts = [None, fold_from(None, in_flight.iter().copied())];
        let before = starts.map(|start| fold_from(start, prefix.iter().map(|r| r.kind)));
        let target = before.map(|b| coalesce(Some(coalesce(b, last)), kind));
        let candidates = [
            coalesce(Some(last), kind),
            kind,
            WorkKind::Added,
            WorkKind::Changed,
            WorkKind::Removed,
        ];
        if let Some(z) = candidates.into_iter().find(|&z| {
            before
                .iter()
                .zip(&target)
                .all(|(&b, &t)| coalesce(b, z) == t)
        }) && let Some(row) = rows.last_mut()
        {
            if row.kind != z {
                row.kind = z;
                row.dirty = true;
            }
            return;
        }
    }
    rows.push(PendingRow {
        id: None,
        kind,
        dirty: true,
    });
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
    let mut arrivals: BTreeMap<String, (&str, Vec<WorkKind>)> = BTreeMap::new();
    for item in work {
        arrivals
            .entry(item.key())
            .or_insert_with(|| (item.path.as_str(), Vec::new()))
            .1
            .push(item.kind);
    }
    let mut clock = WriterClock::start("admit");
    let mut conn = state.db.acquire().await?;
    clock.pooled();
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
    clock.locked();
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
    // Rows above `pending_after` are not yet claimed by a run and may be rewritten; claimed
    // rows between `acked_through` and it only inform the exactness check.
    let (acked_through, pending_after): (i64, i64) = sqlx::query_as(
        "SELECT acked_through,
                CASE WHEN running = 1 THEN COALESCE(running_through, acked_through) ELSE acked_through END
         FROM scan_schedule WHERE library_id = ?",
    )
    .bind(library_id)
    .fetch_one(&mut *tx)
    .await?;
    let arrivals: Vec<_> = arrivals.into_iter().collect();
    let mut written = 0u64;
    // Each statement covers a chunk of keys, so a large batch costs a few statements per
    // chunk rather than several per path while the writer lock is held.
    for chunk in arrivals.chunks(ADMIT_CHUNK) {
        let keys: Vec<&str> = chunk.iter().map(|(key, _)| key.as_str()).collect();
        let existing: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT id, work_key, work_json FROM scan_work
             WHERE library_id = ? AND id > ? AND work_key IN (SELECT value FROM json_each(?))
             ORDER BY work_key, id",
        )
        .bind(library_id)
        .bind(acked_through)
        .bind(serde_json::to_string(&keys)?)
        .fetch_all(&mut *tx)
        .await?;
        let mut stored: HashMap<String, Vec<(i64, WorkKind)>> = HashMap::new();
        for (id, key, json) in existing {
            let kind = serde_json::from_str::<Work>(&json)?.kind;
            stored.entry(key).or_default().push((id, kind));
        }
        let mut updates: Vec<(i64, String)> = Vec::new();
        let mut inserts: Vec<String> = Vec::new();
        for (key, (path, kinds)) in chunk {
            let mut in_flight = Vec::new();
            let mut rows = Vec::new();
            for &(id, kind) in stored.get(key).into_iter().flatten() {
                if id <= pending_after {
                    in_flight.push(kind);
                } else {
                    rows.push(PendingRow {
                        id: Some(id),
                        kind,
                        dirty: false,
                    });
                }
            }
            for &kind in kinds {
                admit(&in_flight, &mut rows, kind);
            }
            for row in rows.iter().filter(|row| row.dirty) {
                let json = serde_json::to_string(&Work {
                    path: (*path).to_owned(),
                    kind: row.kind,
                })?;
                match row.id {
                    Some(id) => updates.push((id, json)),
                    None => inserts.push(json),
                }
            }
        }
        written += (updates.len() + inserts.len()) as u64;
        if !updates.is_empty() {
            let updates = serde_json::to_string(&updates)?;
            sqlx::query(
                "UPDATE scan_work SET work_json = u.work_json
                 FROM (SELECT json_extract(value, '$[0]') AS id,
                              json_extract(value, '$[1]') AS work_json FROM json_each(?)) u
                 WHERE scan_work.id = u.id",
            )
            .bind(&updates)
            .execute(&mut *tx)
            .await?;
        }
        if !inserts.is_empty() {
            // Ids follow array order, which keeps each key's appended rows in arrival order.
            sqlx::query(
                "INSERT INTO scan_work (library_id, work_json)
                 SELECT ?, value FROM json_each(?) ORDER BY key",
            )
            .bind(library_id)
            .bind(serde_json::to_string(&inserts)?)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    clock.report(library_id, work.len() as u64);
    if written < work.len() as u64 {
        tracing::debug!(library = %library_id, requests = work.len(), written, "coalesced scoped work");
    }
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
        spawn_collect(&state);
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
           AND EXISTS (SELECT 1 FROM scan_work WHERE scan_work.library_id = scan_schedule.library_id
                       AND scan_work.id > scan_schedule.acked_through)",
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
    spawn_collect(state);
    Ok(())
}

/// Coalesces cleanup requests so one task per process deletes acknowledged rows.
#[derive(Debug, Default)]
pub struct Collector {
    running: bool,
    again: bool,
}

/// Releases the collector slot if the cleanup task panics.
struct CollectorRun {
    state: AppState,
    stopped: bool,
}

impl Drop for CollectorRun {
    fn drop(&mut self) {
        if !self.stopped {
            let mut collector = self
                .state
                .scan_collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            collector.running = false;
        }
    }
}

/// Starts the cleanup task unless it is running, in which case that task makes another pass.
/// The scheduler loop calls this every tick, so a failed cleanup is retried without a restart.
fn spawn_collect(state: &AppState) {
    {
        let mut collector = state
            .scan_collector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if collector.running {
            collector.again = true;
            return;
        }
        collector.running = true;
    }
    let mut run = CollectorRun {
        state: state.clone(),
        stopped: false,
    };
    tokio::spawn(async move {
        while !run.stopped {
            if let Err(e) = collect_acknowledged(&run.state).await {
                tracing::warn!("acknowledged scan work cleanup failed: {}", e.message());
            }
            let mut collector = run
                .state
                .scan_collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if collector.again {
                collector.again = false;
            } else {
                collector.running = false;
                run.stopped = true;
            }
            drop(collector);
        }
    });
}

/// Deletes rows successful runs acknowledged, one library and one short write transaction at
/// a time. They are not work anymore, so an interruption only leaves garbage for the next pass.
pub(crate) async fn collect_acknowledged(state: &AppState) -> AppResult<u64> {
    let libraries: Vec<String> = sqlx::query_scalar(
        "SELECT library_id FROM scan_schedule s WHERE EXISTS
           (SELECT 1 FROM scan_work w WHERE w.library_id = s.library_id AND w.id <= s.acked_through)",
    )
    .fetch_all(&state.db)
    .await?;
    let mut total = 0;
    for library_id in libraries {
        loop {
            let mut clock = WriterClock::start("collect");
            let deleted = {
                let mut conn = state.db.acquire().await?;
                clock.pooled();
                let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
                clock.locked();
                let deleted = sqlx::query(
                    "DELETE FROM scan_work WHERE id IN (
                       SELECT id FROM scan_work WHERE library_id = ?
                         AND id <= (SELECT acked_through FROM scan_schedule WHERE library_id = ?)
                       ORDER BY id LIMIT ?)",
                )
                .bind(&library_id)
                .bind(&library_id)
                .bind(COLLECT_CHUNK)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                tx.commit().await?;
                deleted
            };
            clock.report(&library_id, deleted);
            total += deleted;
            if deleted < COLLECT_CHUNK as u64 {
                break;
            }
            // The pooled connection is already returned; others can write in between.
            tokio::time::sleep(COLLECT_PAUSE).await;
        }
    }
    Ok(total)
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
             running_through = (SELECT MAX(id) FROM scan_work WHERE library_id = ? AND id > scan_schedule.acked_through)
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
    let (after, through): (i64, Option<i64>) = sqlx::query_as(
        "SELECT acked_through, running_through FROM scan_schedule WHERE library_id = ?",
    )
    .bind(library_id)
    .fetch_one(&state.db)
    .await?;
    let through = through.unwrap_or(after);
    let (requests,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM scan_work WHERE library_id = ? AND id > ? AND id <= ?",
    )
    .bind(library_id)
    .bind(after)
    .bind(through)
    .fetch_one(&state.db)
    .await?;
    if requests == 0 {
        return Err(AppError::conflict(
            "missing_scan_scope",
            "synchronization has no scoped paths",
        ));
    }
    // Distinct keys come from the ordered key index; counting them in the same pass as the
    // rows would evaluate every row's generated key and sort them in a temporary B-tree.
    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM (
           SELECT DISTINCT work_key FROM scan_work INDEXED BY scan_work_key
           WHERE library_id = ? AND work_key < 'folder:' AND id > ? AND id <= ?)",
    )
    .bind(library_id)
    .bind(after)
    .bind(through)
    .fetch_one(&state.db)
    .await?;
    let total = total as u64;
    let mut folders = BTreeSet::new();
    let mut recreated = Vec::new();
    let mut dispatched = 0u64;
    task.phase("Synchronizing files through the Immich API");
    task.progress(0, Some(total), "paths dispatched");
    // Claimed rows never change. Reading them in key order (files sort before folders, each by
    // path) folds one path at a time, so only the current page and path stay in memory and
    // files are dispatched in the same order as one large read.
    let mut cursor = (String::new(), 0i64);
    let mut current: Option<(String, WorkKind)> = None;
    loop {
        let page: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT id, work_key, work_json FROM scan_work
             WHERE library_id = ? AND (work_key, id) > (?, ?) AND id > ? AND id <= ?
             ORDER BY work_key, id LIMIT ?",
        )
        .bind(library_id)
        .bind(&cursor.0)
        .bind(cursor.1)
        .bind(after)
        .bind(through)
        .bind(READ_PAGE)
        .fetch_all(&state.db)
        .await?;
        let done = page.len() < READ_PAGE as usize;
        let mut ready = Vec::new();
        for (id, key, json) in page {
            let work: Work = serde_json::from_str(&json)?;
            cursor = (key, id);
            if work.kind == WorkKind::Folder {
                folders.insert(work.path);
                continue;
            }
            match &mut current {
                Some((path, kind)) if *path == work.path => {
                    *kind = coalesce(Some(*kind), work.kind);
                }
                _ => ready.extend(current.replace((work.path, work.kind))),
            }
        }
        if done {
            ready.extend(current.take());
        }
        for (path, kind) in ready {
            task.progress(dispatched, Some(total), "paths dispatched");
            dispatch_file(
                state,
                server_id,
                library_id,
                &server,
                &api,
                path,
                kind,
                &mut folders,
                &mut recreated,
            )
            .await?;
            dispatched += 1;
        }
        if done {
            break;
        }
    }
    let mut scanned: Vec<String> = Vec::new();
    task.progress(dispatched, Some(total), "paths dispatched");
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

/// Synchronizes one coalesced file request; unsupported work falls back to its folder's scan.
#[allow(clippy::too_many_arguments)]
async fn dispatch_file(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    server: &registry::Server,
    api: &crate::immich::api::Api,
    path: String,
    kind: WorkKind,
    folders: &mut BTreeSet<String>,
    recreated: &mut Vec<String>,
) -> AppResult<()> {
    let immich = |e: crate::immich::api::ApiError| {
        AppError::conflict("immich_error", crate::error::redact(&e.to_string()))
    };
    if server.external_library_move_mode == ImmichMoveMode::DbOnly {
        if kind == WorkKind::Removed {
            return Err(AppError::conflict(
                "file_sync_unavailable",
                "outside removals require the external-library file remove API; folder scans cannot reconcile removals",
            ));
        }
        folders.insert(Work::folder_for_file(&path)?.path);
        return Ok(());
    }
    // The add endpoint is deliberately a no-op for existing assets. There is no update
    // endpoint; changed files (including sidecars) can only request a folder scan.
    let result = match kind {
        WorkKind::Added => Some(api.sync_library_file_add(library_id, &path).await),
        WorkKind::Removed => Some(api.sync_library_file_remove(library_id, &path).await),
        WorkKind::Changed | WorkKind::Folder => None,
    };
    match result {
        Some(Ok(())) => return Ok(()),
        Some(Err(error)) => {
            let stale_removal = kind == WorkKind::Removed
                && matches!(&error, crate::immich::api::ApiError::Status {
                    status: 400, message
                } if message == REMOVED_FILE_EXISTS);
            if stale_removal && recreated_file_exists(state, server_id, library_id, &path).await? {
                api.sync_library_file_add(library_id, &path)
                    .await
                    .map_err(immich)?;
                recreated.push(path);
                return Ok(());
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
    Ok(())
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
    let mut clock = WriterClock::start(if error.is_none() {
        "acknowledge"
    } else {
        "retain"
    });
    let mut conn = state.db.acquire().await?;
    clock.pooled();
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
    clock.locked();
    // One row update acknowledges the whole batch. Work that arrived during the run but was
    // never claimed is made due again if no request already did so.
    sqlx::query(
        "UPDATE scan_schedule SET running = 0, last_finished_at = ?, last_error = ?,
           acked_through = CASE WHEN ? IS NULL
             THEN MAX(acked_through, COALESCE(running_through, acked_through)) ELSE acked_through END,
           due_at = CASE WHEN ? IS NULL AND due_at IS NULL AND EXISTS (
               SELECT 1 FROM scan_work w WHERE w.library_id = scan_schedule.library_id
                 AND w.id > COALESCE(scan_schedule.running_through, scan_schedule.acked_through))
             THEN ? ELSE due_at END,
           running_through = NULL
         WHERE library_id = ?",
    )
    .bind(crate::db::now())
    .bind(&error)
    .bind(&error)
    .bind(&error)
    .bind(now_ms())
    .bind(library_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    clock.report(library_id, 1);
    match error {
        Some(e) => tracing::warn!(library = %library_id, "library synchronization failed: {e}"),
        None => {
            library_scanned(state, server_id, library_id);
            spawn_collect(state);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    const FILE_KINDS: [WorkKind; 3] = [WorkKind::Added, WorkKind::Changed, WorkKind::Removed];

    fn sequences(len: usize) -> Vec<Vec<WorkKind>> {
        (0..len).fold(vec![Vec::new()], |acc, _| {
            acc.into_iter()
                .flat_map(|seq| {
                    FILE_KINDS.map(|kind| {
                        let mut next = seq.clone();
                        next.push(kind);
                        next
                    })
                })
                .collect()
        })
    }

    #[test]
    fn admission_is_exact_for_both_run_outcomes_and_stays_bounded() {
        for in_flight in (0..=2).flat_map(sequences) {
            for arrivals in (1..=6).flat_map(sequences) {
                let mut rows = Vec::new();
                for (i, &kind) in arrivals.iter().enumerate() {
                    admit(&in_flight, &mut rows, kind);
                    let seen = &arrivals[..=i];
                    let kinds = || rows.iter().map(|r| r.kind);
                    assert_eq!(
                        fold_from(None, kinds()),
                        fold_from(None, seen.iter().copied())
                    );
                    let retried = fold_from(None, in_flight.iter().copied());
                    assert_eq!(
                        fold_from(retried, kinds()),
                        fold_from(retried, seen.iter().copied()),
                        "{in_flight:?} then {seen:?}"
                    );
                }
                assert!(rows.len() <= 2, "{in_flight:?} then {arrivals:?}: {rows:?}");
            }
        }
    }

    #[test]
    fn retained_rows_grow_by_at_most_one_row_per_path() {
        for retained in (1..=3).flat_map(sequences) {
            for arrivals in (1..=5).flat_map(sequences) {
                let mut rows: Vec<_> = retained
                    .iter()
                    .map(|&kind| PendingRow {
                        id: Some(1),
                        kind,
                        dirty: false,
                    })
                    .collect();
                for &kind in &arrivals {
                    admit(&[], &mut rows, kind);
                }
                let all = retained.iter().chain(&arrivals).copied();
                assert_eq!(
                    fold_from(None, rows.iter().map(|r| r.kind)),
                    fold_from(None, all)
                );
                assert!(rows.len() <= retained.len() + 1);
            }
        }
    }
}
