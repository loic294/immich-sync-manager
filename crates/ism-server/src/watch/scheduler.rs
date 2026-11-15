//! Debounced Immich library scans.
//!
//! A detected change in a library's folder, or an explicit ISM request (uploads and peer copies
//! that Immich must import), sets the library's scan due `scan_delay` (two minutes) from now;
//! every further request restarts the timer. A due scan starts only while no managed operation
//! holds or is about to hold the server's write barrier, then waits for the library queue to go
//! idle and announces [`ServerEvent::LibraryScanned`].

use std::time::Duration;

use ism_core::dto::ScanScheduleView;
use ism_core::maintenance::IdleConfirmation;

use super::selfwrite::now_ms;
use crate::error::{AppError, AppResult};
use crate::immich::registry;
use crate::state::{AppState, ServerEvent};

const MAX_REASONS: usize = 10;

fn delay_ms(state: &AppState) -> i64 {
    i64::try_from(state.config.timing.scan_delay.as_millis()).unwrap_or(i64::MAX / 4)
}

/// Schedules (or postpones) a scan of `library_id` on `server_id`.
pub async fn request_scan(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    reason: &str,
) -> AppResult<()> {
    request_scan_at(state, server_id, library_id, reason, now_ms()).await
}

pub(crate) async fn request_scan_at(
    state: &AppState,
    server_id: &str,
    library_id: &str,
    reason: &str,
    now_ms: i64,
) -> AppResult<()> {
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
    tx.commit().await?;
    state.scan_wake.notify_one();
    state.emit(ServerEvent::WatchChanged);
    Ok(())
}

pub async fn run(state: AppState) {
    if let Err(e) = sqlx::query(
        "UPDATE scan_schedule SET running = 0, due_at = COALESCE(due_at, ?) WHERE running = 1",
    )
    .bind(now_ms())
    .execute(&state.db)
    .await
    {
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

/// Starts every due scan whose server allows it. Returns the next due time still pending.
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
            "UPDATE scan_schedule SET due_at = NULL, running = 1, last_started_at = ?, last_error = NULL
             WHERE library_id = ? AND due_at = ? AND running = 0",
        )
        .bind(crate::db::now())
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

async fn scan(state: &AppState, server_id: &str, library_id: &str) -> AppResult<()> {
    let server = registry::server(state, server_id).await?;
    let api = registry::admin_api(state, &server).await?;
    let immich = |e: crate::immich::api::ApiError| {
        AppError::conflict("immich_error", crate::error::redact(&e.to_string()))
    };
    api.scan_library(library_id).await.map_err(immich)?;
    let timing = &state.config.timing;
    let mut deadline = tokio::time::Instant::now() + timing.scan_timeout;
    let mut idle = IdleConfirmation::default();
    loop {
        tokio::time::sleep(timing.poll).await;
        let queues = api.queues().await.map_err(immich)?;
        let library = queues
            .iter()
            .find(|q| q.name == crate::maintenance::LIBRARY_QUEUE);
        if idle.observe(library.is_some_and(crate::immich::api::Queue::is_idle)) {
            return Ok(());
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
    result: AppResult<()>,
) -> AppResult<()> {
    let error = result.as_ref().err().map(|e| e.message().to_owned());
    sqlx::query(
        "UPDATE scan_schedule SET running = 0, last_finished_at = ?, last_error = ? WHERE library_id = ?",
    )
    .bind(crate::db::now())
    .bind(&error)
    .bind(library_id)
    .execute(&state.db)
    .await?;
    match error {
        Some(e) => tracing::warn!(library = %library_id, "library scan failed: {e}"),
        None => library_scanned(state, server_id, library_id),
    }
    state.emit(ServerEvent::WatchChanged);
    state.scan_wake.notify_one();
    Ok(())
}

/// Hook after an ISM-requested scan finished: Immich has imported what it found. Published
/// uploads get their new Immich asset IDs here; other listeners use `LibraryScanned`.
fn library_scanned(state: &AppState, server_id: &str, library_id: &str) {
    tracing::info!(library = %library_id, "library scan finished");
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
