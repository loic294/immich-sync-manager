//! Group-wide transfer conditions without inferring file-specific eligibility.

use std::collections::HashMap;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use ism_core::mapping::RootMapping;
use ism_core::schedule::Schedule;
use sqlx::FromRow;

use crate::db;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

pub(crate) const ROOT_PROBE_LIMIT: usize = 4;
const ROOT_PROBE_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub(crate) enum ScheduleState {
    Allowed,
    Closed { next_at: Option<String> },
    Invalid,
}

pub(crate) struct QueueGroup {
    pub peer_id: String,
    pub peer_name: String,
    pub peer_status: String,
    pub share_id: String,
    pub share_name: String,
    pub root_configured: bool,
    pub root_available: Option<bool>,
    pub count: u64,
    pub size: u64,
    pub done: u64,
    pub first_at: String,
    pub last_at: String,
    pub backoff_count: u64,
    pub unclassified_backoff: u64,
    pub next_retry: Option<String>,
    pub recheck_at: Option<String>,
    pub schedule: ScheduleState,
}

pub(crate) struct FailedGroup {
    pub peer_id: String,
    pub peer_name: String,
    pub share_id: String,
    pub share_name: String,
    pub count: u64,
}

pub(crate) struct Snapshot {
    pub queued: Vec<QueueGroup>,
    pub failed: Vec<FailedGroup>,
    pub active_operation: bool,
    pub running: i64,
}

#[derive(FromRow)]
struct QueueRow {
    peer_id: String,
    peer_name: String,
    peer_status: String,
    share_id: String,
    share_name: String,
    root_id: String,
    root_path: Option<String>,
    count: i64,
    size: i64,
    done: i64,
    first_at: String,
    last_at: String,
    backoff_count: i64,
    unclassified_backoff: i64,
    next_retry: Option<String>,
}

#[derive(FromRow)]
struct FailedRow {
    peer_id: String,
    peer_name: String,
    share_id: String,
    share_name: String,
    count: i64,
}

#[derive(FromRow)]
struct Override {
    share_id: Option<String>,
    peer_id: Option<String>,
    until: String,
}

async fn root_available(
    state: &AppState,
    root_id: String,
    path: String,
    deadline: tokio::time::Instant,
) -> Option<bool> {
    if tokio::time::Instant::now() >= deadline {
        return None;
    }
    let permit = tokio::time::timeout_at(deadline, state.queue_root_probes.clone().acquire_owned())
        .await
        .ok()?
        .ok()?;
    if tokio::time::Instant::now() >= deadline {
        return None;
    }
    let root = crate::files::Root {
        mapping: RootMapping {
            root_id,
            container_path: path,
            immich_import_path: None,
            opm_prefix: None,
        },
        name: String::new(),
        server_id: None,
        library_id: None,
    };
    let probe = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        root.available()
    });
    match tokio::time::timeout_at(deadline, probe).await {
        Ok(Ok(available)) => Some(available),
        Ok(Err(error)) => {
            tracing::error!(
                error = %crate::error::redact(&error.to_string()),
                "root availability probe failed"
            );
            None
        }
        Err(_) => None,
    }
}

pub(crate) async fn root_availability(
    state: &AppState,
    roots: HashMap<String, Option<String>>,
) -> HashMap<String, Option<bool>> {
    let deadline = tokio::time::Instant::now() + ROOT_PROBE_TIMEOUT;
    stream::iter(roots.into_iter().map(|(root_id, path)| async move {
        let available = match path {
            Some(path) => root_available(state, root_id.clone(), path, deadline).await,
            None => Some(false),
        };
        (root_id, available)
    }))
    .buffer_unordered(ROOT_PROBE_LIMIT)
    .collect()
    .await
}

pub(crate) async fn snapshot(state: &AppState) -> AppResult<Snapshot> {
    let now = chrono::Utc::now();
    let now_string = db::ts(now);
    let mut tx = state.db.begin().await?;
    let raw: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key = ?")
        .bind(crate::peers::admin::SCHEDULE_KEY)
        .fetch_optional(&mut *tx)
        .await?;
    let (schedule, schedule_transition) = match crate::peers::admin::parse_schedule(raw) {
        Ok(view) => {
            let schedule =
                Schedule::new(&view.timezone, &view.windows).map_err(AppError::internal)?;
            let transition = schedule.next_transition(now).map(db::ts);
            let status = if schedule.is_allowed(now) {
                ScheduleState::Allowed
            } else {
                ScheduleState::Closed {
                    next_at: transition.clone(),
                }
            };
            (status, transition)
        }
        Err(error) if error.code() == "invalid_schedule" => (ScheduleState::Invalid, None),
        Err(error) => return Err(error),
    };
    let overrides: Vec<Override> = sqlx::query_as(
        "SELECT share_id,peer_id,until FROM overrides WHERE revoked_at IS NULL AND until > ?",
    )
    .bind(&now_string)
    .fetch_all(&mut *tx)
    .await?;
    let active_operation: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE status IN ('running','waiting_maintenance'))",
    )
    .fetch_one(&mut *tx)
    .await?;
    let running: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers WHERE status='running'")
        .fetch_one(&mut *tx)
        .await?;
    let rows: Vec<QueueRow> = sqlx::query_as(
        "WITH queued AS (
             SELECT t.peer_id,t.share_id,t.size,t.bytes_done,t.queued_at,t.updated_at,t.attempts,
                    CASE WHEN t.attempts > 0
                         AND strftime('%Y-%m-%dT%H:%M:%fZ',julianday(t.updated_at))=t.updated_at
                         THEN strftime('%Y-%m-%dT%H:%M:%fZ',
                                       julianday(t.updated_at)+t.attempts/1440.0)
                    END AS retry_at
             FROM transfers t WHERE t.status='queued'
         )
         SELECT p.id AS peer_id,p.name AS peer_name,p.status AS peer_status,
                s.id AS share_id,s.name AS share_name,s.root_id,m.container_path AS root_path,
                COUNT(*) AS count,COALESCE(SUM(t.size),0) AS size,
                COALESCE(SUM(t.bytes_done),0) AS done,
                MIN(t.queued_at) AS first_at,MAX(t.updated_at) AS last_at,
                SUM(CASE WHEN t.retry_at > ? THEN 1 ELSE 0 END) AS backoff_count,
                SUM(CASE WHEN t.attempts > 0 AND COALESCE(
                    strftime('%Y-%m-%dT%H:%M:%fZ',julianday(t.updated_at)),''
                ) != t.updated_at THEN 1 ELSE 0 END) AS unclassified_backoff,
                MIN(CASE WHEN t.retry_at > ? THEN t.retry_at END) AS next_retry
         FROM queued t JOIN peers p ON p.id=t.peer_id JOIN shares s ON s.id=t.share_id
         LEFT JOIN mappings m ON m.root_id=s.root_id
         GROUP BY p.id,p.name,p.status,s.id,s.name,s.root_id,m.container_path
         ORDER BY first_at,p.id,s.id",
    )
    .bind(&now_string)
    .bind(&now_string)
    .fetch_all(&mut *tx)
    .await?;
    let failures: Vec<FailedRow> = sqlx::query_as(
        "SELECT p.id AS peer_id,p.name AS peer_name,s.id AS share_id,s.name AS share_name,
                COUNT(*) AS count
         FROM transfers t JOIN peers p ON p.id=t.peer_id JOIN shares s ON s.id=t.share_id
         WHERE t.status='failed'
         GROUP BY p.id,p.name,s.id,s.name ORDER BY p.id,s.id",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    let roots: HashMap<String, Option<String>> = rows
        .iter()
        .map(|row| (row.root_id.clone(), row.root_path.clone()))
        .collect();
    let roots = root_availability(state, roots).await;
    let queued = rows
        .into_iter()
        .map(|row| {
            let root_configured = row.root_path.is_some();
            let root_available = roots.get(&row.root_id).copied().flatten();
            let matching_overrides: Vec<_> = overrides
                .iter()
                .filter(|entry| {
                    entry
                        .share_id
                        .as_deref()
                        .is_none_or(|id| id == row.share_id.as_str())
                        && entry
                            .peer_id
                            .as_deref()
                            .is_none_or(|id| id == row.peer_id.as_str())
                })
                .collect();
            let override_active = !matching_overrides.is_empty();
            let recheck_at = matching_overrides
                .iter()
                .filter_map(|entry| db::parse_ts(&entry.until).map(db::ts))
                .chain(schedule_transition.clone())
                .min();
            QueueGroup {
                peer_id: row.peer_id,
                peer_name: row.peer_name,
                peer_status: row.peer_status,
                share_id: row.share_id,
                share_name: row.share_name,
                root_configured,
                root_available,
                count: row.count.max(0) as u64,
                size: row.size.max(0) as u64,
                done: row.done.max(0) as u64,
                first_at: row.first_at,
                last_at: row.last_at,
                backoff_count: row.backoff_count.max(0) as u64,
                unclassified_backoff: row.unclassified_backoff.max(0) as u64,
                next_retry: row.next_retry,
                recheck_at,
                schedule: if override_active {
                    ScheduleState::Allowed
                } else {
                    schedule.clone()
                },
            }
        })
        .collect();
    let failed = failures
        .into_iter()
        .map(|row| FailedGroup {
            peer_id: row.peer_id,
            peer_name: row.peer_name,
            share_id: row.share_id,
            share_name: row.share_name,
            count: row.count.max(0) as u64,
        })
        .collect();
    Ok(Snapshot {
        queued,
        failed,
        active_operation,
        running,
    })
}
