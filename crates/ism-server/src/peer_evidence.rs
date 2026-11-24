//! Cached receiver activity, with freshness and malformed records kept explicit.

use std::collections::HashMap;

use futures_util::TryStreamExt;
use ism_core::dto::{ActivityExample, SyncActivity, SyncProgress};
use sqlx::{FromRow, Row};

use crate::db;
use crate::error::{AppResult, redact};
use crate::state::AppState;

const EXAMPLES: usize = 5;
const REASONS: usize = 32;

#[derive(FromRow)]
struct PeerRow {
    id: String,
    name: String,
    instance_id: Option<String>,
    status: String,
    progress_json: Option<String>,
    error: Option<String>,
    updated_at: Option<String>,
}

pub(crate) struct Peer {
    pub id: String,
    pub name: String,
    pub instance_id: Option<String>,
    pub observed_at: Option<String>,
    pub fresh: bool,
    pub cached_count: u64,
    pub invalid_count: u64,
    pub groups: Vec<Group>,
}

pub(crate) struct Snapshot {
    pub peers: Vec<Peer>,
    pub orphaned: u64,
}

pub(crate) struct Group {
    pub share_name: String,
    pub status: String,
    pub managed_operation: bool,
    pub count: u64,
    pub done: u64,
    pub size: u64,
    pub first_at: String,
    first_time: chrono::DateTime<chrono::Utc>,
    pub error: Option<String>,
    pub reasons: HashMap<String, u64>,
    pub examples: Vec<ActivityExample>,
}

fn valid_progress(raw: Option<&str>) -> bool {
    raw.and_then(|json| serde_json::from_str::<SyncProgress>(json).ok())
        .is_some_and(|progress| {
            progress.bytes_per_second.is_finite()
                && progress.bytes_per_second >= 0.0
                && progress.bytes_done <= progress.bytes_total
                && progress.files_completed <= progress.files_total
        })
}

fn decode(
    peer: &str,
    id: &str,
    json: &str,
) -> Option<(SyncActivity, chrono::DateTime<chrono::Utc>)> {
    let activity: SyncActivity = serde_json::from_str(json).ok()?;
    let queued_at = db::parse_ts(&activity.queued_at)?;
    if activity.peer_id != peer
        || activity.id != id
        || !activity.outgoing
        || activity.bytes_done > activity.size
        || ism_core::paths::RelPath::parse(&activity.relative_path).is_err()
        || !matches!(
            activity.status.as_str(),
            "queued" | "running" | "publishing" | "failed" | "completed" | "cancelled"
        )
    {
        return None;
    }
    Some((activity, queued_at))
}

pub(crate) async fn snapshot(state: &AppState) -> AppResult<Snapshot> {
    let mut tx = state.db.begin().await?;
    let rows: Vec<PeerRow> = sqlx::query_as(
        "SELECT p.id,p.name,p.instance_id,p.status,c.progress_json,c.error,c.updated_at
         FROM peers p LEFT JOIN peer_sync_state c ON c.peer_id=p.id ORDER BY p.name,p.id",
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut peers: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let fresh = row.status == "online"
                && row.error.is_none()
                && valid_progress(row.progress_json.as_deref())
                && crate::peers::notifications::snapshot_fresh(
                    state,
                    &row.id,
                    row.updated_at.as_deref(),
                );
            Peer {
                id: row.id,
                name: row.name,
                instance_id: row.instance_id,
                observed_at: row.updated_at,
                fresh,
                cached_count: 0,
                invalid_count: 0,
                groups: Vec::new(),
            }
        })
        .collect();
    let peer_indexes: HashMap<_, _> = peers
        .iter()
        .enumerate()
        .map(|(index, peer)| (peer.id.clone(), index))
        .collect();
    let mut groups: HashMap<(usize, String, String, bool), usize> = HashMap::new();
    let mut orphaned = 0;
    let mut records =
        sqlx::query("SELECT peer_id,id,activity_json FROM outgoing_sync ORDER BY peer_id,id")
            .fetch(&mut *tx);
    while let Some(record) = records.try_next().await? {
        let peer_id: String = record.try_get("peer_id")?;
        let Some(&peer_index) = peer_indexes.get(&peer_id) else {
            orphaned += 1;
            continue;
        };
        let peer = &mut peers[peer_index];
        peer.cached_count += 1;
        let id: String = record.try_get("id")?;
        let json: String = record.try_get("activity_json")?;
        let Some((activity, queued_at)) = decode(&peer_id, &id, &json) else {
            peer.invalid_count += 1;
            continue;
        };
        if matches!(activity.status.as_str(), "completed" | "cancelled") {
            continue;
        }
        let status = if peer.fresh {
            activity.status.clone()
        } else {
            "unavailable".into()
        };
        let managed_operation = activity.id.starts_with("operation:");
        let key = (
            peer_index,
            activity.share_name.clone(),
            status.clone(),
            managed_operation,
        );
        let group_index = *groups.entry(key).or_insert_with(|| {
            let index = peer.groups.len();
            peer.groups.push(Group {
                share_name: activity.share_name.clone(),
                status,
                managed_operation,
                count: 0,
                done: 0,
                size: 0,
                first_at: activity.queued_at.clone(),
                first_time: queued_at,
                error: None,
                reasons: HashMap::new(),
                examples: Vec::new(),
            });
            index
        });
        let group = &mut peer.groups[group_index];
        let (Some(size), Some(done)) = (
            group.size.checked_add(activity.size),
            group.done.checked_add(activity.bytes_done),
        ) else {
            peer.invalid_count += 1;
            continue;
        };
        group.count += 1;
        group.size = size;
        group.done = done;
        if queued_at < group.first_time {
            group.first_time = queued_at;
            group.first_at = activity.queued_at;
        }
        let error = peer
            .fresh
            .then(|| activity.error.as_deref().map(redact))
            .flatten();
        if group.examples.len() < EXAMPLES {
            group.examples.push(ActivityExample {
                resource: activity.relative_path.clone(),
                detail: error.clone(),
            });
        }
        if let Some(error) = error {
            if group.error.is_none() {
                group.error = Some(error.clone());
            }
            let reason = if activity.relative_path.is_empty() {
                error
            } else {
                error.replace(&activity.relative_path, "<item>")
            };
            if group.reasons.contains_key(&reason) || group.reasons.len() < REASONS {
                *group.reasons.entry(reason).or_default() += 1;
            } else {
                *group
                    .reasons
                    .entry("Other reported reasons".into())
                    .or_default() += 1;
            }
        }
    }
    drop(records);
    tx.commit().await?;
    Ok(Snapshot { peers, orphaned })
}
