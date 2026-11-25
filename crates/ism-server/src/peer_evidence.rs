//! Cached receiver activity, with freshness and malformed records kept explicit.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use ism_core::dto::{ActivityExample, SyncActivity, SyncProgress};
use sqlx::FromRow;

use crate::db;
use crate::error::{AppResult, redact};
use crate::state::AppState;

const EXAMPLES: usize = 5;
const REASONS: usize = 32;

type Records = Arc<BTreeMap<(String, String), Arc<str>>>;

#[derive(Default)]
pub(crate) struct Cache {
    revision: Option<i64>,
    records: Records,
}

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
    pub completed: HashMap<(String, bool), u64>,
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
    let mut cache = state.peer_evidence.lock().await;
    let mut tx = state.db.begin().await?;
    let (revision, deleted_revision): (i64, i64) = sqlx::query_as(
        "SELECT revision,deleted_revision FROM outgoing_sync_revision WHERE singleton=1",
    )
    .fetch_one(&mut *tx)
    .await?;
    let records =
        if cache.revision == Some(revision) {
            cache.records.clone()
        } else {
            let changes: Vec<(String, String, String)> = match cache.revision {
            Some(previous) => sqlx::query_as(
                "SELECT peer_id,id,activity_json FROM outgoing_sync WHERE evidence_revision > ?",
            )
            .bind(previous)
            .fetch_all(&mut *tx)
            .await?,
            None => sqlx::query_as(
                "SELECT peer_id,id,activity_json FROM outgoing_sync ORDER BY peer_id,id",
            )
            .fetch_all(&mut *tx)
            .await?,
        };
            let mut records = cache.records.clone();
            let updated = Arc::make_mut(&mut records);
            if cache
                .revision
                .is_some_and(|previous| deleted_revision > previous)
            {
                let keys: Vec<(String, String)> =
                    sqlx::query_as("SELECT peer_id,id FROM outgoing_sync")
                        .fetch_all(&mut *tx)
                        .await?;
                let keys: HashSet<_> = keys.into_iter().collect();
                updated.retain(|key, _| keys.contains(key));
            }
            for (peer_id, id, json) in changes {
                updated.insert((peer_id, id), Arc::from(json));
            }
            records
        };
    let rows: Vec<PeerRow> = sqlx::query_as(
        "SELECT p.id,p.name,p.instance_id,p.status,c.progress_json,c.error,c.updated_at
         FROM peers p LEFT JOIN peer_sync_state c ON c.peer_id=p.id ORDER BY p.name,p.id",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    cache.revision = Some(revision);
    cache.records = records.clone();
    drop(cache);
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
                completed: HashMap::new(),
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
    for ((peer_id, id), json) in records.iter() {
        let Some(&peer_index) = peer_indexes.get(peer_id) else {
            orphaned += 1;
            continue;
        };
        let peer = &mut peers[peer_index];
        peer.cached_count += 1;
        let Some((activity, queued_at)) = decode(peer_id, id, json) else {
            peer.invalid_count += 1;
            continue;
        };
        if activity.status == "completed" {
            *peer
                .completed
                .entry((activity.share_name, activity.id.starts_with("operation:")))
                .or_default() += 1;
            continue;
        }
        if activity.status == "cancelled" {
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
    Ok(Snapshot { peers, orphaned })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[tokio::test]
    async fn unchanged_records_are_reused_and_committed_changes_invalidate_them() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            crate::config::Config::for_tests(dir.path()),
            db::connect(dir.path()).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
             VALUES('peer','Example peer','https://peer.example','unused','offline',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        assert_eq!(snapshot(&state).await.unwrap().peers[0].cached_count, 0);
        let empty = state.peer_evidence.lock().await.records.clone();
        snapshot(&state).await.unwrap();
        assert!(Arc::ptr_eq(
            &empty,
            &state.peer_evidence.lock().await.records
        ));
        sqlx::query(
            "INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES('peer','item','{}')",
        )
        .execute(&state.db)
        .await
        .unwrap();
        let observed = snapshot(&state).await.unwrap();
        assert_eq!(observed.peers[0].cached_count, 1);
        assert_eq!(observed.peers[0].invalid_count, 1);
        let inserted = state.peer_evidence.lock().await.records.clone();
        assert!(!Arc::ptr_eq(&empty, &inserted));
        sqlx::query("UPDATE peers SET name='Renamed peer'")
            .execute(&state.db)
            .await
            .unwrap();
        assert_eq!(
            snapshot(&state).await.unwrap().peers[0].name,
            "Renamed peer"
        );
        assert!(Arc::ptr_eq(
            &inserted,
            &state.peer_evidence.lock().await.records
        ));
        sqlx::query("UPDATE outgoing_sync SET activity_json=activity_json")
            .execute(&state.db)
            .await
            .unwrap();
        snapshot(&state).await.unwrap();
        assert!(Arc::ptr_eq(
            &inserted,
            &state.peer_evidence.lock().await.records
        ));
        let mut tx = state.db.begin().await.unwrap();
        sqlx::query("DELETE FROM outgoing_sync")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        snapshot(&state).await.unwrap();
        assert!(Arc::ptr_eq(
            &inserted,
            &state.peer_evidence.lock().await.records
        ));
        sqlx::query("UPDATE outgoing_sync SET activity_json='{\"changed\":true}'")
            .execute(&state.db)
            .await
            .unwrap();
        snapshot(&state).await.unwrap();
        assert!(!Arc::ptr_eq(
            &inserted,
            &state.peer_evidence.lock().await.records
        ));
        assert_eq!(
            state
                .peer_evidence
                .lock()
                .await
                .records
                .get(&("peer".into(), "item".into()))
                .map(AsRef::as_ref),
            Some("{\"changed\":true}")
        );
        sqlx::query("DELETE FROM outgoing_sync")
            .execute(&state.db)
            .await
            .unwrap();
        let deleted = snapshot(&state).await.unwrap();
        assert_eq!(deleted.peers[0].cached_count, 0);
        assert_eq!(deleted.peers[0].invalid_count, 0);
    }

    #[tokio::test]
    async fn cache_tracks_deletes_reinsertions_and_key_changes() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            crate::config::Config::for_tests(dir.path()),
            db::connect(dir.path()).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
             VALUES('peer','Example peer','https://peer.example','unused','offline',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES('peer','first','{\"version\":1}'),('peer','second','{}')",
        )
        .execute(&state.db)
        .await
        .unwrap();
        assert_eq!(snapshot(&state).await.unwrap().peers[0].cached_count, 2);

        let mut tx = state.db.begin().await.unwrap();
        sqlx::query("DELETE FROM outgoing_sync WHERE peer_id='peer' AND id='first'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE outgoing_sync SET id='third' WHERE peer_id='peer' AND id='second'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES('peer','first','{\"version\":2}')",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let observed = snapshot(&state).await.unwrap();
        assert_eq!(observed.peers[0].cached_count, 2);
        assert_eq!(observed.peers[0].invalid_count, 2);
        let cache = state.peer_evidence.lock().await;
        assert!(cache.records.contains_key(&("peer".into(), "first".into())));
        assert!(cache.records.contains_key(&("peer".into(), "third".into())));
        assert_eq!(
            cache
                .records
                .get(&("peer".into(), "first".into()))
                .map(AsRef::as_ref),
            Some("{\"version\":2}")
        );
        assert!(
            !cache
                .records
                .contains_key(&("peer".into(), "second".into()))
        );
        drop(cache);
        sqlx::query("DELETE FROM peers WHERE id='peer'")
            .execute(&state.db)
            .await
            .unwrap();
        let observed = snapshot(&state).await.unwrap();
        assert!(observed.peers.is_empty());
        assert_eq!(observed.orphaned, 0);
    }
}
