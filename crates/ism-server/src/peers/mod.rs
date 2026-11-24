//! Peer-to-peer replication between ISM instances.
//!
//! A *share* is a folder replicated under the same name on several instances. Each instance keeps
//! a row per file with a version vector. Local changes become events in `event_log`; peers pull
//! them in order. Management events (moves, deletes, folders) are applied immediately through
//! managed operations so Immich keeps its asset IDs; file contents are copied by the transfer
//! worker during allowed hours. Concurrent changes become conflicts that an admin resolves.

pub mod activity;
pub mod admin;
pub mod inbox;
pub mod net;
pub(crate) mod notifications;
pub mod recovery;
pub mod retained;
pub mod transfer;
pub mod versions;

pub use inbox::{blocked_footprints, require_stable_share};

use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::Arc;

use ism_core::dto::{ConflictSide, Location, OperationAction, OperationRequest};
use ism_core::file_settings::{JunkRules, SharedSettings};
use ism_core::paths::RelPath;
use ism_core::version::{Causality, VersionVector};
use serde::{Deserialize, Serialize};

use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files::{self, HIDDEN, Root};
use crate::state::{AppState, ServerEvent};

/// Files modified more recently than this are left for the next scan (they may still be written).
/// Files must be unchanged this long before a share scan publishes them.
pub(crate) const SETTLE_SECS: i64 = 30;
const EVENT_BATCH: i64 = 500;
const SYNOLOGY_METADATA_DIR: &str = "@eaDir";

/// NAS-generated metadata and ignored names are not replicated content, at any depth.
fn excluded_path(path: &str, junk: &JunkRules) -> bool {
    path.split('/')
        .any(|name| name == SYNOLOGY_METADATA_DIR || junk.is_junk(name))
}

#[derive(Debug, Clone)]
pub struct Share {
    pub id: String,
    pub name: String,
    pub root_id: String,
    pub path: RelPath,
    pub peer_ids: Vec<String>,
    pub enabled: bool,
    /// The ignored names and prefixes when the share was loaded.
    pub junk: Arc<JunkRules>,
}

impl Share {
    /// The share-relative path of a location, if it lies inside the share.
    pub fn rel_of(&self, loc: &Location) -> Option<String> {
        if loc.root_id != self.root_id {
            return None;
        }
        let p = RelPath::parse(&loc.path).ok()?;
        if !p.starts_with(&self.path) || p == self.path || excluded_path(p.as_str(), &self.junk) {
            return None;
        }
        Some(if self.path.is_root() {
            p.as_str().to_owned()
        } else {
            p.as_str()[self.path.as_str().len() + 1..].to_owned()
        })
    }

    /// The root-relative path of a share-relative path.
    pub fn root_rel(&self, rel: &str) -> AppResult<RelPath> {
        let joined = match (self.path.is_root(), rel.is_empty()) {
            (true, _) => rel.to_owned(),
            (false, true) => self.path.as_str().to_owned(),
            (false, false) => format!("{}/{rel}", self.path.as_str()),
        };
        if excluded_path(&joined, &self.junk) {
            return Err(AppError::invalid_location(
                "NAS metadata and ignored names are excluded from peer replication",
            ));
        }
        files::parse_rel(&joined)
    }

    pub fn location(&self, rel: &str) -> AppResult<Location> {
        Ok(Location {
            root_id: self.root_id.clone(),
            path: self.root_rel(rel)?.as_str().to_owned(),
        })
    }

    pub fn includes(&self, peer_id: &str) -> bool {
        self.enabled
            && !excluded_path(self.path.as_str(), &self.junk)
            && self.peer_ids.iter().any(|p| p == peer_id)
    }
}

pub async fn shares(state: &AppState) -> AppResult<Vec<Share>> {
    let rows: Vec<(String, String, String, String, String, i64)> = sqlx::query_as(
        "SELECT id, name, root_id, path, peer_ids_json, enabled FROM shares ORDER BY name",
    )
    .fetch_all(&state.db)
    .await?;
    let junk = state.junk_rules();
    rows.into_iter()
        .map(|(id, name, root_id, path, peers, enabled)| {
            Ok(Share {
                id,
                name,
                root_id,
                path: files::parse_rel(&path)?,
                peer_ids: serde_json::from_str(&peers)?,
                enabled: enabled != 0,
                junk: junk.clone(),
            })
        })
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventData {
    /// The file or folder the event is about (the new path for moves).
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ns: Option<i64>,
    #[serde(default)]
    pub vv: VersionVector,
}

impl EventData {
    fn excluded(&self, junk: &JunkRules) -> bool {
        excluded_path(&self.path, junk)
            || self
                .from
                .as_deref()
                .is_some_and(|from| excluded_path(from, junk))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEvent {
    pub seq: i64,
    pub origin: String,
    pub origin_seq: i64,
    pub share: String,
    pub kind: String,
    pub data: EventData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPage {
    pub events: Vec<PeerEvent>,
    pub next: i64,
}

impl EventPage {
    fn validate(&self, after: i64) -> Result<(), String> {
        if self.next < after || self.events.len() > EVENT_BATCH as usize {
            return Err("invalid peer event page cursor or size".into());
        }
        let mut previous = after;
        for event in &self.events {
            if event.seq <= previous
                || event.seq > self.next
                || event.origin.is_empty()
                || event.origin_seq <= 0
            {
                return Err("invalid peer event sequence or identity".into());
            }
            previous = event.seq;
        }
        Ok(())
    }
}

/// Appends an event that originated here.
#[cfg(test)]
pub async fn append_local(
    state: &AppState,
    share: &str,
    kind: &str,
    data: &EventData,
) -> AppResult<()> {
    let mut tx = state.db.begin().await?;
    append_local_in(&mut tx, &state.instance_id, share, kind, data).await?;
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn append_local_in(
    conn: &mut sqlx::SqliteConnection,
    instance_id: &str,
    share: &str,
    kind: &str,
    data: &EventData,
) -> AppResult<()> {
    let next: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(origin_seq), 0) + 1 FROM event_log WHERE origin = ?",
    )
    .bind(instance_id)
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query("INSERT INTO event_log (origin, origin_seq, share_name, kind, data_json, at) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(instance_id)
        .bind(next)
        .bind(share)
        .bind(kind)
        .bind(serde_json::to_string(data)?)
        .bind(db::now())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Events `peer` has not seen: after `after`, in shares it takes part in, not its own.
pub async fn events_for(
    state: &AppState,
    peer_id: &str,
    peer_instance: &str,
    after: i64,
) -> AppResult<EventPage> {
    type Row = (i64, String, i64, String, String, String);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT seq, origin, origin_seq, share_name, kind, data_json FROM event_log WHERE seq > ? ORDER BY seq LIMIT ?",
    )
    .bind(after)
    .bind(EVENT_BATCH)
    .fetch_all(&state.db)
    .await?;
    let shares = shares(state).await?;
    let allowed: HashSet<&str> = shares
        .iter()
        .filter(|s| s.includes(peer_id))
        .map(|s| s.name.as_str())
        .collect();
    let next = rows.last().map_or(after, |r| r.0);
    let mut events = rows
        .into_iter()
        .filter(|r| r.1 != peer_instance && allowed.contains(r.3.as_str()))
        .map(|(seq, origin, origin_seq, share, kind, data)| {
            Ok(PeerEvent {
                seq,
                origin,
                origin_seq,
                share,
                kind,
                data: serde_json::from_str(&data)?,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
    // Keep the unfiltered cursor so historical metadata events do not stall pagination.
    let junk = state.junk_rules();
    events.retain(|e| e.kind == SHARE_ENABLED || !e.data.excluded(&junk));
    Ok(EventPage { events, next })
}

// ---------------------------------------------------------------------------------------------
// Replica rows

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub size: i64,
    pub mtime_ns: i64,
    pub sha256: Option<String>,
    pub deleted: bool,
    pub vv: VersionVector,
}

pub async fn row(state: &AppState, share_id: &str, rel: &str) -> AppResult<Option<Row>> {
    let mut conn = state.db.acquire().await?;
    row_in(&mut conn, share_id, rel).await
}

async fn row_in(
    conn: &mut sqlx::SqliteConnection,
    share_id: &str,
    rel: &str,
) -> AppResult<Option<Row>> {
    let r: Option<(i64, i64, Option<String>, i64, String)> = sqlx::query_as(
        "SELECT size, mtime_ns, sha256, deleted, vv_json FROM replica_files WHERE share_id = ? AND rel_path = ?",
    )
    .bind(share_id)
    .bind(rel)
    .fetch_optional(conn)
    .await?;
    r.map(|(size, mtime_ns, sha256, deleted, vv)| {
        Ok(Row {
            size,
            mtime_ns,
            sha256,
            deleted: deleted != 0,
            vv: serde_json::from_str(&vv)?,
        })
    })
    .transpose()
}

pub async fn put_row(state: &AppState, share_id: &str, rel: &str, r: &Row) -> AppResult<()> {
    let mut conn = state.db.acquire().await?;
    put_row_in(&mut conn, share_id, rel, r).await
}

async fn put_row_in(
    conn: &mut sqlx::SqliteConnection,
    share_id: &str,
    rel: &str,
    r: &Row,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO replica_files (share_id, rel_path, size, mtime_ns, sha256, deleted, vv_json) VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(share_id, rel_path) DO UPDATE SET size = excluded.size, mtime_ns = excluded.mtime_ns,
         sha256 = excluded.sha256, deleted = excluded.deleted, vv_json = excluded.vv_json",
    )
    .bind(share_id)
    .bind(rel)
    .bind(r.size)
    .bind(r.mtime_ns)
    .bind(&r.sha256)
    .bind(r.deleted as i64)
    .bind(serde_json::to_string(&r.vv)?)
    .execute(conn)
    .await?;
    Ok(())
}

async fn put_row_and_append_local(
    state: &AppState,
    share: &Share,
    rel: &str,
    r: &Row,
    kind: &str,
    data: &EventData,
) -> AppResult<()> {
    let mut tx = state.db.begin().await?;
    put_row_in(&mut tx, &share.id, rel, r).await?;
    append_local_in(&mut tx, &state.instance_id, &share.name, kind, data).await?;
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

/// Moves the rows of `from` (a file, or a folder prefix) to `to`, merging `vv` into each.
async fn rename_rows(
    state: &AppState,
    share_id: &str,
    from: &str,
    to: &str,
    vv: Option<&VersionVector>,
) -> AppResult<()> {
    let mut tx = state.db.begin().await?;
    rename_rows_in(&mut tx, share_id, from, to, vv).await?;
    tx.commit().await?;
    Ok(())
}

async fn rename_rows_in(
    conn: &mut sqlx::SqliteConnection,
    share_id: &str,
    from: &str,
    to: &str,
    vv: Option<&VersionVector>,
) -> AppResult<()> {
    let rows: Vec<(String, i64, i64, Option<String>, i64, String)> = sqlx::query_as(
        "SELECT rel_path, size, mtime_ns, sha256, deleted, vv_json FROM replica_files
         WHERE share_id = ? AND (rel_path = ? OR substr(rel_path, 1, ?) = ?)",
    )
    .bind(share_id)
    .bind(from)
    .bind(from.chars().count() as i64 + 1)
    .bind(format!("{from}/"))
    .fetch_all(&mut *conn)
    .await?;
    for (path, size, mtime_ns, sha256, deleted, vv_json) in rows {
        let new_path = format!("{to}{}", &path[from.len()..]);
        let mut v: VersionVector = serde_json::from_str(&vv_json)?;
        if let Some(extra) = vv {
            v.merge(extra);
        }
        sqlx::query("DELETE FROM replica_files WHERE share_id = ? AND rel_path = ?")
            .bind(share_id)
            .bind(&path)
            .execute(&mut *conn)
            .await?;
        put_row_in(
            conn,
            share_id,
            &new_path,
            &Row {
                size,
                mtime_ns,
                sha256,
                deleted: deleted != 0,
                vv: v,
            },
        )
        .await?;
    }
    Ok(())
}

fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}

fn reserved(name: &str) -> bool {
    HIDDEN.contains(&name) || name.starts_with(".ism")
}

fn hidden(name: &str, junk: &JunkRules) -> bool {
    reserved(name) || excluded_path(name, junk)
}

/// Hashes one file on disk and records it as a local change when it differs from its row.
async fn register_local(
    state: &AppState,
    share: &Share,
    root: &Root,
    rel: &str,
) -> AppResult<Option<Row>> {
    register_local_reporting(state, share, root, rel, None).await
}

async fn register_local_reporting(
    state: &AppState,
    share: &Share,
    root: &Root,
    rel: &str,
    task: Option<&crate::activity::Reporter>,
) -> AppResult<Option<Row>> {
    let root_rel = share.root_rel(rel)?;
    let abs = root.abs(&root_rel)?;
    let meta = match fs::metadata(&abs) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return row(state, &share.id, rel).await;
        }
        Err(e) => return Err(e.into()),
    };
    if !meta.is_file() {
        return row(state, &share.id, rel).await;
    }
    let existing = row(state, &share.id, rel).await?;
    let (size, mtime) = (meta.len() as i64, files::mtime_ns(&meta));
    let reporter = task.cloned();
    let total = meta.len();
    if let Some(task) = task {
        task.phase(&format!("Verifying checksum for {rel}"));
    }
    let fingerprint = tokio::task::spawn_blocking(move || {
        files::replacement_fingerprint_reporting(&abs, |done| {
            if let Some(task) = &reporter {
                task.progress(done, Some(total), "bytes hashed");
            }
        })
    })
    .await
    .map_err(AppError::internal)??;
    let Some((revision, sha)) = fingerprint else {
        return Err(AppError::conflict(
            "revision_changed",
            "file disappeared during verification",
        ));
    };
    if revision != files::revision(&meta) {
        return Err(AppError::conflict(
            "revision_changed",
            "file changed during verification",
        ));
    }
    let mut r = existing.unwrap_or(Row {
        size,
        mtime_ns: mtime,
        sha256: None,
        deleted: true,
        vv: VersionVector::default(),
    });
    let changed = r.deleted || r.sha256.as_deref() != Some(&sha);
    if !changed && r.size == size && r.mtime_ns == mtime {
        return Ok(Some(r));
    }
    r.size = size;
    r.mtime_ns = mtime;
    if changed {
        r.vv.increment(&state.instance_id);
        r.deleted = false;
        r.sha256 = Some(sha.clone());
    }
    if changed {
        put_row_and_append_local(
            state,
            share,
            rel,
            &r,
            "file_changed",
            &EventData {
                path: rel.to_owned(),
                sha256: Some(sha),
                size: Some(size as u64),
                mtime_ns: Some(mtime),
                vv: r.vv.clone(),
                ..Default::default()
            },
        )
        .await?;
    } else {
        put_row(state, &share.id, rel, &r).await?;
    }
    Ok(Some(r))
}

/// Compares a share with its rows and records local changes as events.
pub async fn scan_share(state: &AppState, share: &Share) -> AppResult<()> {
    crate::activity::observe(
        state,
        format!("share:{}", share.id),
        "Share inventory scan",
        &share.name,
        Some("/peers"),
        |task| async move { scan_share_reporting(state, share, &task).await },
    )
    .await
}

async fn scan_share_reporting(
    state: &AppState,
    share: &Share,
    task: &crate::activity::Reporter,
) -> AppResult<()> {
    task.waiting("Waiting for managed operations that are changing files");
    // Operations waiting to start (e.g. for the maintenance barrier) have changed nothing yet;
    // none can start changing files while this is held.
    let _operation_guard = state.replication_operation_lock.lock_scan().await;
    task.phase("Checking active operations and uncertain paths");
    inbox::require_scannable(state).await?;
    let mut blocked = blocked_footprints(state, share).await?;
    blocked.extend(admin::move_decision_footprints(state, share).await?);
    share.root_rel("")?;
    let root = files::root(&state.db, &share.root_id).await?;
    // An unmounted root must never look like "everything was deleted".
    root.ensure_available()?;
    let base = root.abs(&share.path)?;
    if !base.is_dir() {
        return Err(AppError::not_found(format!(
            "share folder '{}' does not exist",
            share.path
        )));
    }
    let junk = share.junk.clone();
    task.phase("Enumerating share files");
    let reporter = task.clone();
    let (found, blocked, incomplete) = tokio::task::spawn_blocking(move || {
        let mut found = Vec::new();
        let mut uncertain = blocked.clone();
        for entry in walkdir::WalkDir::new(&base)
            .min_depth(1)
            .into_iter()
            .filter_entry(|e| {
                let rel = e
                    .path()
                    .strip_prefix(&base)
                    .ok()
                    .and_then(|p| p.to_str())
                    .unwrap_or("");
                !hidden(&e.file_name().to_string_lossy(), &junk)
                    && !blocked
                        .iter()
                        .any(|p| p.is_empty() || rel == p || rel.starts_with(&format!("{p}/")))
            })
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    if let Some(rel) = error
                        .path()
                        .and_then(|p| p.strip_prefix(&base).ok())
                        .and_then(|p| p.to_str())
                    {
                        uncertain.push(rel.replace('\\', "/"));
                        continue;
                    }
                    return Err(std::io::Error::other(error));
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry
                .path()
                .strip_prefix(&base)
                .map_err(std::io::Error::other)?;
            let rel = path
                .to_str()
                .ok_or_else(|| std::io::Error::other("share contains a non-UTF-8 path"))?
                .replace('\\', "/");
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                Err(_) => {
                    uncertain.push(rel);
                    continue;
                }
            };
            found.push((rel, meta.len() as i64, files::mtime_ns(&meta)));
            reporter.progress(found.len() as u64, None, "files discovered");
        }
        let incomplete = uncertain.len() > blocked.len();
        Ok::<_, std::io::Error>((found, uncertain, incomplete))
    })
    .await
    .map_err(AppError::internal)??;
    let known: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT rel_path, size, mtime_ns, deleted FROM replica_files WHERE share_id = ?",
    )
    .bind(&share.id)
    .fetch_all(&state.db)
    .await?;
    let known: HashMap<String, (i64, i64, bool)> = known
        .into_iter()
        .map(|(p, s, m, d)| (p, (s, m, d != 0)))
        .collect();
    let settle_cutoff = now_ns() - SETTLE_SECS * 1_000_000_000;
    let mut seen = HashSet::new();
    task.phase("Hashing and reconciling settled files");
    let total = found.len() as u64;
    task.progress(0, Some(total), "files checked");
    for (index, (rel, _size, mtime)) in found.into_iter().enumerate() {
        task.progress(index as u64, Some(total), "files checked");
        seen.insert(rel.clone());
        if mtime > settle_cutoff || blocked.iter().any(|p| inbox::overlaps(&rel, p)) {
            continue;
        }
        if RelPath::parse(&rel).is_err() {
            continue;
        }
        register_local_reporting(state, share, &root, &rel, Some(task)).await?;
    }
    task.progress(total, Some(total), "files checked");
    task.phase("Reconciling removed files and publishing inventory");
    for (rel, (_, _, deleted)) in known {
        // A newly ignored row is not evidence that the file was deleted.
        if deleted
            || seen.contains(&rel)
            || rel.split('/').any(|name| hidden(name, &share.junk))
            || blocked.iter().any(|p| inbox::overlaps(&rel, p))
        {
            continue;
        }
        if let Some(mut r) = row(state, &share.id, &rel).await? {
            r.deleted = true;
            r.vv.increment(&state.instance_id);
            put_row_and_append_local(
                state,
                share,
                &rel,
                &r,
                "file_deleted",
                &EventData {
                    path: rel.clone(),
                    vv: r.vv.clone(),
                    ..Default::default()
                },
            )
            .await?;
        }
    }
    sqlx::query("UPDATE shares SET last_scan_at = ? WHERE id = ?")
        .bind(db::now())
        .bind(&share.id)
        .execute(&state.db)
        .await?;
    if incomplete {
        return Err(AppError::conflict(
            "scan_incomplete",
            "unreadable share paths were excluded from publication and deletion inference",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Local managed operations → events

/// What a finished local operation changed, for replication.
#[derive(Debug, Clone)]
pub enum LocalChange {
    FileMoved {
        from: Location,
        to: Location,
    },
    FileTrashed {
        at: Location,
    },
    FolderMoved {
        from: Location,
        to: Location,
    },
    FolderCreated {
        at: Location,
    },
    FolderTrashed {
        at: Location,
    },
    /// New or replaced content (uploads, restores): the share scan publishes it as
    /// `file_changed`, and peers fetch it within their allowed hours.
    FileWritten {
        at: Location,
    },
}

/// Asks the share scanner to rescan every enabled share containing one of `at` once the
/// write settled.
pub(crate) fn rescan_shares_soon(state: &AppState, shares: &[Share], at: &[&Location]) {
    let settle = std::time::Duration::from_secs(SETTLE_SECS.unsigned_abs() + 1);
    let due = std::time::Instant::now() + settle;
    let mut woke = false;
    if let Ok(mut map) = state.share_changes.lock() {
        for share in shares.iter().filter(|s| s.enabled) {
            if at.iter().any(|l| share.rel_of(l).is_some()) {
                map.insert(share.id.clone(), due);
                woke = true;
            }
        }
    }
    if woke {
        state.share_wake.notify_one();
    }
}

/// Called by the executor when an operation ends. Operations that applied peer events finish
/// their bookkeeping here; other operations become events for peers.
pub async fn operation_finished(
    state: &AppState,
    operation_id: &str,
    changes: &[LocalChange],
) -> AppResult<()> {
    if !finish_pending_event(state, operation_id).await?
        && !finish_pending(state, operation_id).await?
    {
        record_local(state, Some(operation_id), changes).await?;
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    inbox::finish_operation_in(&mut tx, operation_id).await?;
    sqlx::query(
        "UPDATE operations SET status='completed',recovery_resolved=1,finished_at=COALESCE(finished_at,?)
         WHERE id=? AND status='failed'
         AND EXISTS(SELECT 1 FROM operation_items WHERE operation_id=? AND status='done' AND json_extract(result_json,'$.recovery_verified')=1)
         AND NOT EXISTS(SELECT 1 FROM operation_items WHERE operation_id=? AND status!='done')",
    ).bind(db::now()).bind(operation_id).bind(operation_id).bind(operation_id).execute(&mut *tx).await?;
    tx.commit().await?;
    state.sync_wake.notify_one();
    Ok(())
}

async fn finish_pending_event(state: &AppState, operation_id: &str) -> AppResult<bool> {
    let pending: Option<(String, String, String)> = sqlx::query_as(
        "SELECT peer_id, share_id, event_json FROM pending_peer_events WHERE operation_id = ?",
    )
    .bind(operation_id)
    .fetch_optional(&state.db)
    .await?;
    let Some((peer_id, share_id, raw)) = pending else {
        return Ok(false);
    };
    let (status, recovery_resolved): (String, i64) = sqlx::query_as(
        "SELECT i.status, o.recovery_resolved
             FROM operation_items i JOIN operations o ON o.id = i.operation_id
             WHERE i.operation_id = ? AND i.idx = 0",
    )
    .bind(operation_id)
    .fetch_one(&state.db)
    .await?;
    if status != "done" && !(status == "recovery_required" && recovery_resolved != 0) {
        return Ok(true);
    }
    let event: PeerEvent = serde_json::from_str(&raw)?;
    let share = shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == share_id && share.includes(&peer_id))
        .ok_or_else(|| AppError::conflict("peer_share_changed", "the peer share changed"))?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let path = root.abs(&share.root_rel(&event.data.path)?)?;
    if !path.symlink_metadata()?.is_dir() {
        return Err(AppError::conflict(
            "peer_folder_missing",
            "the created peer folder could not be verified",
        ));
    }

    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let known: Option<(String, String, String)> = sqlx::query_as(
        "SELECT share_name, kind, data_json FROM event_log WHERE origin = ? AND origin_seq = ?",
    )
    .bind(&event.origin)
    .bind(event.origin_seq)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((share_name, kind, data)) = known {
        let data: EventData = serde_json::from_str(&data)?;
        if share_name != event.share || kind != event.kind || data != event.data {
            return Err(AppError::conflict(
                "peer_event_identity",
                "peer event identity was reused for different content",
            ));
        }
    } else {
        sqlx::query(
            "INSERT INTO event_log (origin, origin_seq, share_name, kind, data_json, at) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&event.origin)
        .bind(event.origin_seq)
        .bind(&event.share)
        .bind(&event.kind)
        .bind(serde_json::to_string(&event.data)?)
        .bind(db::now())
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM pending_peer_events WHERE operation_id = ?")
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
    inbox::finish_operation_in(&mut tx, operation_id).await?;
    if status == "recovery_required" {
        sqlx::query(
            "UPDATE operation_items SET status = 'done', error_code = NULL, error_message = NULL
             WHERE operation_id = ? AND idx = 0 AND status = 'recovery_required'",
        )
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE operations SET status = 'completed', finished_at = COALESCE(finished_at, ?)
             WHERE id = ? AND status = 'recovery_required'",
        )
        .bind(db::now())
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(true)
}

async fn record_local(
    state: &AppState,
    operation_id: Option<&str>,
    changes: &[LocalChange],
) -> AppResult<()> {
    let shares = shares(state).await?;
    let written: Vec<&Location> = changes
        .iter()
        .filter_map(|c| match c {
            LocalChange::FileWritten { at } => Some(at),
            _ => None,
        })
        .collect();
    rescan_shares_soon(state, &shares, &written);
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    if let Some(operation_id) = operation_id {
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO completed_peer_bookkeeping(operation_id) VALUES (?)",
        )
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            return Ok(());
        }
    }
    for change in changes {
        match change {
            LocalChange::FileWritten { .. } => {}
            LocalChange::FileTrashed { at } => {
                for share in &shares {
                    let Some(rel) = share.rel_of(at) else {
                        continue;
                    };
                    let mut r = row_in(&mut tx, &share.id, &rel).await?.unwrap_or(Row {
                        size: 0,
                        mtime_ns: 0,
                        sha256: None,
                        deleted: false,
                        vv: VersionVector::default(),
                    });
                    if r.deleted {
                        continue;
                    }
                    r.deleted = true;
                    r.vv.increment(&state.instance_id);
                    put_row_in(&mut tx, &share.id, &rel, &r).await?;
                    append_local_in(
                        &mut tx,
                        &state.instance_id,
                        &share.name,
                        "file_deleted",
                        &EventData {
                            path: rel,
                            vv: r.vv,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
            }
            LocalChange::FileMoved { from, to } | LocalChange::FolderMoved { from, to } => {
                let folder = matches!(change, LocalChange::FolderMoved { .. });
                for share in &shares {
                    let (Some(f), Some(t)) = (share.rel_of(from), share.rel_of(to)) else {
                        continue;
                    };
                    // Moves across shares are seen by the scanner as delete + create.
                    let r = row_in(&mut tx, &share.id, &f).await?;
                    if !folder && r.as_ref().is_none_or(|r| r.deleted) {
                        continue;
                    }
                    rename_rows_in(&mut tx, &share.id, &f, &t, None).await?;
                    let r = r.unwrap_or_else(|| Row {
                        size: 0,
                        mtime_ns: 0,
                        sha256: None,
                        deleted: false,
                        vv: VersionVector::default(),
                    });
                    append_local_in(
                        &mut tx,
                        &state.instance_id,
                        &share.name,
                        if folder { "folder_moved" } else { "file_moved" },
                        &EventData {
                            path: t,
                            from: Some(f),
                            sha256: r.sha256,
                            size: Some(r.size as u64),
                            vv: r.vv,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
            }
            LocalChange::FolderCreated { at } => {
                for share in &shares {
                    if let Some(p) = share.rel_of(at) {
                        append_local_in(
                            &mut tx,
                            &state.instance_id,
                            &share.name,
                            "folder_created",
                            &EventData {
                                path: p,
                                ..Default::default()
                            },
                        )
                        .await?;
                    }
                }
            }
            LocalChange::FolderTrashed { at } => {
                for share in &shares {
                    let Some(p) = share.rel_of(at) else { continue };
                    let rows: Vec<String> = sqlx::query_scalar(
                        "SELECT rel_path FROM replica_files WHERE share_id = ? AND deleted = 0 AND substr(rel_path, 1, ?) = ?",
                    )
                    .bind(&share.id)
                    .bind(p.chars().count() as i64 + 1)
                    .bind(format!("{p}/"))
                    .fetch_all(&mut *tx)
                    .await?;
                    let mut vv = VersionVector::default();
                    for rel in rows {
                        if let Some(mut r) = row_in(&mut tx, &share.id, &rel).await? {
                            r.deleted = true;
                            r.vv.increment(&state.instance_id);
                            vv.merge(&r.vv);
                            put_row_in(&mut tx, &share.id, &rel, &r).await?;
                        }
                    }
                    append_local_in(
                        &mut tx,
                        &state.instance_id,
                        &share.name,
                        "folder_trashed",
                        &EventData {
                            path: p,
                            vv,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
            }
        }
    }
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Applying peer events

#[allow(clippy::type_complexity)]
struct Batch {
    actions: Vec<OperationAction>,
    pending: Vec<(
        String,
        String,
        Option<String>,
        Option<String>,
        VersionVector,
    )>,
    paths: HashSet<(String, String)>,
}

impl Batch {
    /// Paths overlap when equal, nested, or in the same companion group: a managed move of
    /// `x.jpg` also moves `x.jpg.xmp` and `x.xmp`, so their own events must wait for it.
    fn touches(&self, share: &str, paths: &[&str]) -> bool {
        paths.iter().any(|p| {
            self.paths.iter().any(|(s, q)| {
                s == share
                    && (q == p
                        || q.starts_with(&format!("{p}/"))
                        || p.starts_with(&format!("{q}/"))
                        || group_key(q) == group_key(p))
            })
        })
    }

    fn push(
        &mut self,
        share: &Share,
        action: OperationAction,
        kind: &str,
        from: Option<&str>,
        to: Option<&str>,
        vv: &VersionVector,
    ) {
        for p in [from, to].into_iter().flatten() {
            self.paths.insert((share.id.clone(), p.to_owned()));
        }
        self.actions.push(action);
        self.pending.push((
            share.id.clone(),
            kind.to_owned(),
            from.map(Into::into),
            to.map(Into::into),
            vv.clone(),
        ));
    }
}

/// Folder plus the file name up to its first dot; shared by an original and its companions.
fn group_key(path: &str) -> (&str, &str) {
    let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
    (dir, name.split('.').next().unwrap_or(name))
}

fn side(instance: &str, r: Option<&Row>) -> ConflictSide {
    ConflictSide {
        instance: instance.to_owned(),
        sha256: r.and_then(|r| r.sha256.clone()),
        size: r.filter(|r| !r.deleted).map(|r| r.size as u64),
        modified_at: r.filter(|r| !r.deleted && r.mtime_ns > 0).and_then(|r| {
            chrono::DateTime::from_timestamp_nanos(r.mtime_ns)
                .to_rfc3339()
                .into()
        }),
        deleted: r.is_none_or(|r| r.deleted),
    }
}

pub struct ConflictInput<'a> {
    pub share: &'a Share,
    pub rel: &'a str,
    pub kind: &'a str,
    pub detail: String,
    pub remote: ConflictSide,
    pub remote_vv: &'a VersionVector,
    pub peer_id: &'a str,
    /// The source of a remote move.
    pub from: Option<&'a str>,
}

/// Records (or refreshes) an open conflict for one path.
pub async fn conflict(state: &AppState, c: ConflictInput<'_>) -> AppResult<()> {
    let local = row(state, &c.share.id, c.rel).await?;
    let local = side(&state.instance_name().await, local.as_ref());
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT id FROM conflicts WHERE share_id = ? AND rel_path = ? AND status = 'open'",
    )
    .bind(&c.share.id)
    .bind(c.rel)
    .fetch_optional(&state.db)
    .await?;
    let id = existing.unwrap_or_else(|| crate::crypto::new_id("cf"));
    sqlx::query(
        "INSERT INTO conflicts (id, share_id, rel_path, kind, detail, local_json, remote_json, remote_vv_json, peer_id, status, created_at, remote_from)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
         ON CONFLICT(id) DO UPDATE SET kind = excluded.kind, detail = excluded.detail, local_json = excluded.local_json,
         remote_json = excluded.remote_json, remote_vv_json = excluded.remote_vv_json, peer_id = excluded.peer_id,
         remote_from = excluded.remote_from",
    )
    .bind(&id)
    .bind(&c.share.id)
    .bind(c.rel)
    .bind(c.kind)
    .bind(&c.detail)
    .bind(serde_json::to_string(&local)?)
    .bind(serde_json::to_string(&c.remote)?)
    .bind(serde_json::to_string(c.remote_vv)?)
    .bind(c.peer_id)
    .bind(db::now())
    .bind(c.from)
    .execute(&state.db)
    .await?;
    // A conflicting path is not transferred until it is resolved.
    sqlx::query("DELETE FROM transfers WHERE share_id = ? AND rel_path = ? AND status = 'queued'")
        .bind(&c.share.id)
        .bind(c.rel)
        .execute(&state.db)
        .await?;
    state.emit(ServerEvent::ConflictsChanged);
    Ok(())
}

pub async fn has_open_conflict(state: &AppState, share_id: &str, rel: &str) -> AppResult<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM conflicts WHERE share_id = ? AND rel_path = ? AND status = 'open'",
    )
    .bind(share_id)
    .bind(rel)
    .fetch_one(&state.db)
    .await?
        > 0)
}

fn checked_transfer_size(sha256: &str, size: u64) -> AppResult<i64> {
    if sha256.len() != 64 || !sha256.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::bad_request(
            "invalid_peer_hash",
            "peer file checksum must be a SHA-256 digest",
        ));
    }
    i64::try_from(size).map_err(|_| {
        AppError::bad_request("invalid_peer_size", "peer file size is too large to track")
    })
}

fn file_event_metadata(data: &EventData) -> AppResult<(&str, u64)> {
    let path = RelPath::parse(&data.path)
        .map_err(|_| AppError::bad_request("invalid_peer_path", "invalid peer file path"))?;
    if path.is_root()
        || data
            .from
            .as_deref()
            .is_some_and(|from| !matches!(RelPath::parse(from), Ok(source) if !source.is_root()))
    {
        return Err(AppError::bad_request(
            "invalid_peer_path",
            "peer file paths must be valid and must not be the share root",
        ));
    }
    let sha256 = data.sha256.as_deref().ok_or_else(|| {
        AppError::bad_request("invalid_peer_hash", "peer file checksum is missing")
    })?;
    let size = data
        .size
        .ok_or_else(|| AppError::bad_request("invalid_peer_size", "peer file size is missing"))?;
    checked_transfer_size(sha256, size)?;
    Ok((sha256, size))
}

/// Checks the shape of a remote delete, move or trash before it can become a managed operation.
fn management_event_valid(kind: &str, data: &EventData) -> Result<(), String> {
    let valid = |p: &str| {
        RelPath::parse(p).is_ok_and(|rel| !rel.is_root() && !rel.segments().any(reserved))
    };
    let needs_from = matches!(kind, "file_moved" | "folder_moved");
    if !valid(&data.path)
        || data.from.as_deref().is_some_and(|f| !valid(f))
        || needs_from != data.from.is_some()
        || data.from.as_deref() == Some(data.path.as_str())
    {
        return Err(format!("invalid peer {kind} event"));
    }
    Ok(())
}

/// Queues the download of a remote version, replacing older queued versions of the same path.
pub async fn queue_transfer(
    state: &AppState,
    peer_id: &str,
    share: &Share,
    rel: &str,
    sha256: &str,
    size: u64,
    vv: &VersionVector,
) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    queue_transfer_in(&mut tx, peer_id, share, rel, sha256, size, vv).await?;
    tx.commit().await?;
    state.sync_wake.notify_one();
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

pub(super) async fn queue_transfer_in(
    conn: &mut sqlx::SqliteConnection,
    peer_id: &str,
    share: &Share,
    rel: &str,
    sha256: &str,
    size: u64,
    vv: &VersionVector,
) -> AppResult<()> {
    share.root_rel(rel)?;
    let size = checked_transfer_size(sha256, size)?;
    let sha256 = sha256.to_ascii_lowercase();
    let tx = conn;
    let queued: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT sha256, size, vv_json FROM transfers
         WHERE share_id = ? AND rel_path = ? AND status IN ('queued', 'running', 'failed')",
    )
    .bind(&share.id)
    .bind(rel)
    .fetch_all(&mut *tx)
    .await?;
    let mut existing = Vec::with_capacity(queued.len());
    let mut merged = vv.clone();
    for (hash, old_size, raw) in queued {
        let version: VersionVector = serde_json::from_str(&raw)?;
        if hash.eq_ignore_ascii_case(&sha256) && old_size == size {
            merged.merge(&version);
        } else {
            existing.push(version);
        }
    }
    let mut stale = false;
    for version in existing {
        match merged.compare(&version) {
            Causality::Dominates => {}
            Causality::DominatedBy => stale = true,
            Causality::Equal | Causality::Concurrent => {
                return Err(AppError::conflict(
                    "peer_versions_diverged",
                    "pending peer versions have conflicting contents; inspect the transfer queue before retrying",
                ));
            }
        }
    }
    if stale {
        return Ok(());
    }
    let run_id = activity::ensure_run(tx, peer_id).await?;
    let transfer_id = crate::crypto::new_id("tr");
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id)
         SELECT r.id,'transfer',? FROM sync_recovery r JOIN transfers t ON r.kind='transfer' AND r.target_id=t.id
         WHERE t.share_id=? AND t.rel_path=? AND t.status IN ('queued','failed')",
    ).bind(&transfer_id).bind(&share.id).bind(rel).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM transfers WHERE share_id = ? AND rel_path = ? AND status IN ('queued', 'failed')")
        .bind(&share.id)
        .bind(rel)
        .execute(&mut *tx)
        .await?;
    let now = db::now();
    sqlx::query(
        "INSERT INTO transfers (id, peer_id, share_id, rel_path, sha256, size, vv_json, status, queued_at, updated_at, run_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)",
    )
    .bind(&transfer_id)
    .bind(peer_id)
    .bind(&share.id)
    .bind(rel)
    .bind(sha256)
    .bind(size)
    .bind(serde_json::to_string(&merged)?)
    .bind(&now)
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

fn remote_side(peer_name: &str, d: &EventData, deleted: bool) -> ConflictSide {
    ConflictSide {
        instance: peer_name.to_owned(),
        sha256: d.sha256.clone(),
        size: d.size,
        modified_at: d
            .mtime_ns
            .map(|n| chrono::DateTime::from_timestamp_nanos(n).to_rfc3339()),
        deleted,
    }
}

enum Applied {
    Done,
    /// Stop the batch here; the event needs the managed operations queued so far to finish.
    Stop,
}

async fn queue_folder_event(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    event: &PeerEvent,
    cursor: i64,
) -> AppResult<()> {
    let principal = Principal::Peer {
        peer_id: peer.id.clone(),
    };
    let request = OperationRequest {
        idempotency_key: crate::crypto::new_id("peer-folder"),
        label: Some(format!("Folder from {}", peer.name)),
        actions: vec![OperationAction::CreateFolder {
            target: share.location(&event.data.path)?,
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let duplicate: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pending_peer_events WHERE origin = ? AND origin_seq = ?)",
    )
    .bind(&event.origin)
    .bind(event.origin_seq)
    .fetch_one(&mut *tx)
    .await?;
    if duplicate {
        return Err(AppError::conflict(
            "peer_event_pending",
            "this peer event is already being applied",
        ));
    }
    let accepted = crate::ops::insert_in(&mut tx, &principal, &request, None).await?;
    sqlx::query(
        "INSERT INTO pending_peer_events (operation_id, peer_id, share_id, origin, origin_seq, expected_cursor, event_json)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&accepted.operation_id)
    .bind(&peer.id)
    .bind(&share.id)
    .bind(&event.origin)
    .bind(event.origin_seq)
    .bind(cursor)
    .bind(serde_json::to_string(event)?)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE peer_inbox SET status = 'applying', operation_id = ?, error = NULL WHERE peer_id = ? AND seq = ?",
    )
    .bind(&accepted.operation_id)
    .bind(&peer.id)
    .bind(event.seq)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::ops::announce_queued(state, &accepted);
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn apply_event(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    root: &Root,
    e: &PeerEvent,
    batch: &mut Batch,
) -> AppResult<Applied> {
    let d = &e.data;
    let decided_move = admin::move_decision_footprints(state, share)
        .await?
        .iter()
        .any(|path| {
            inbox::overlaps(path, &d.path)
                || d.from
                    .as_ref()
                    .is_some_and(|from| inbox::overlaps(path, from))
        });
    if decided_move {
        return Err(AppError::conflict(
            "move_decision_pending",
            "move waits for verified conflict publication",
        ));
    }
    let rel = d.path.as_str();
    if RelPath::parse(rel).is_err()
        || d.from
            .as_deref()
            .is_some_and(|f| RelPath::parse(f).is_err())
    {
        return Err(AppError::bad_request(
            "invalid_peer_path",
            "invalid peer event path",
        ));
    }
    let paths: Vec<&str> = [Some(rel), d.from.as_deref()]
        .into_iter()
        .flatten()
        .collect();
    if batch.touches(&share.id, &paths) {
        return Ok(Applied::Stop);
    }
    let abs = root.abs(&share.root_rel(rel)?)?;
    match e.kind.as_str() {
        "file_changed" => {
            let (sha, size) = file_event_metadata(d)?;
            let sha = sha.to_ascii_lowercase();
            let local = register_local(state, share, root, rel).await?;
            match local {
                None => queue_transfer(state, &peer.id, share, rel, &sha, size, &d.vv).await?,
                Some(mut r) => match d.vv.compare(&r.vv) {
                    Causality::Equal => {
                        if r.deleted
                            || r.sha256.as_deref() != Some(sha.as_str())
                            || r.size != size as i64
                        {
                            return Err(AppError::conflict(
                                "peer_versions_diverged",
                                "equal peer file versions have conflicting contents",
                            ));
                        }
                    }
                    Causality::DominatedBy => {}
                    _ if !r.deleted && r.sha256.as_deref() == Some(sha.as_str()) => {
                        if r.size != size as i64 {
                            return Err(AppError::conflict(
                                "peer_versions_diverged",
                                "matching peer file checksums have different sizes",
                            ));
                        }
                        r.vv.merge(&d.vv);
                        put_row(state, &share.id, rel, &r).await?;
                    }
                    Causality::Dominates => {
                        queue_transfer(state, &peer.id, share, rel, &sha, size, &d.vv).await?;
                    }
                    Causality::Concurrent => {
                        let kind = if r.deleted {
                            "deleted_here_changed_there"
                        } else {
                            "changed_on_both"
                        };
                        conflict(
                            state,
                            ConflictInput {
                                share,
                                rel,
                                kind,
                                detail: format!(
                                    "{rel} changed on {} while it was {} here",
                                    peer.name,
                                    if r.deleted { "deleted" } else { "changed" }
                                ),
                                remote: remote_side(&peer.name, d, false),
                                remote_vv: &d.vv,
                                peer_id: &peer.id,
                                from: None,
                            },
                        )
                        .await?;
                    }
                },
            }
        }
        "file_deleted" => {
            let local = register_local(state, share, root, rel).await?;
            match local {
                None => {
                    put_row(
                        state,
                        &share.id,
                        rel,
                        &Row {
                            size: 0,
                            mtime_ns: 0,
                            sha256: None,
                            deleted: true,
                            vv: d.vv.clone(),
                        },
                    )
                    .await?;
                }
                Some(mut r) => match d.vv.compare(&r.vv) {
                    Causality::Equal | Causality::DominatedBy => {}
                    _ if r.deleted => {
                        r.vv.merge(&d.vv);
                        put_row(state, &share.id, rel, &r).await?;
                    }
                    Causality::Dominates => {
                        sqlx::query("DELETE FROM transfers WHERE share_id = ? AND rel_path = ? AND status = 'queued'")
                            .bind(&share.id)
                            .bind(rel)
                            .execute(&state.db)
                            .await?;
                        if abs.is_file() {
                            batch.push(
                                share,
                                OperationAction::Trash {
                                    source: share.location(rel)?,
                                    expected_revision: None,
                                },
                                "delete",
                                None,
                                Some(rel),
                                &d.vv,
                            );
                        } else {
                            r.deleted = true;
                            r.vv.merge(&d.vv);
                            put_row(state, &share.id, rel, &r).await?;
                        }
                    }
                    Causality::Concurrent => {
                        conflict(
                            state,
                            ConflictInput {
                                share,
                                rel,
                                kind: "changed_here_deleted_there",
                                detail: format!(
                                    "{rel} was deleted on {} while it changed here",
                                    peer.name
                                ),
                                remote: remote_side(&peer.name, d, true),
                                remote_vv: &d.vv,
                                peer_id: &peer.id,
                                from: None,
                            },
                        )
                        .await?;
                    }
                },
            }
        }
        "file_moved" => {
            let from = d.from.as_deref().unwrap_or_default();
            let from_abs = root.abs(&share.root_rel(from)?)?;
            let from_row = register_local(state, share, root, from).await?;
            let to_exists = abs.symlink_metadata().is_ok();
            let from_matches = from_abs.is_file()
                && from_row
                    .as_ref()
                    .is_some_and(|r| !r.deleted && r.sha256 == d.sha256);
            if from_matches && !to_exists {
                batch.push(
                    share,
                    OperationAction::Move {
                        source: share.location(from)?,
                        target: share.location(rel)?,
                        expected_revision: None,
                    },
                    "move",
                    Some(from),
                    Some(rel),
                    &d.vv,
                );
            } else if !from_abs.exists() && to_exists {
                // Already moved here (e.g. as a companion of another file).
                let to_row = register_local(state, share, root, rel).await?;
                if let Some(mut r) = to_row.filter(|r| r.sha256 == d.sha256) {
                    r.vv.merge(&d.vv);
                    put_row(state, &share.id, rel, &r).await?;
                    sqlx::query("DELETE FROM replica_files WHERE share_id = ? AND rel_path = ?")
                        .bind(&share.id)
                        .bind(from)
                        .execute(&state.db)
                        .await?;
                } else {
                    move_conflict(state, peer, share, e).await?;
                }
            } else if from_row.is_none() && !to_exists {
                // Never had the file: fetch it at its new place.
                let sha = d.sha256.clone().unwrap_or_default();
                queue_transfer(
                    state,
                    &peer.id,
                    share,
                    rel,
                    &sha,
                    d.size.unwrap_or(0),
                    &d.vv,
                )
                .await?;
            } else {
                move_conflict(state, peer, share, e).await?;
            }
        }
        "folder_moved" => {
            let from = d.from.as_deref().unwrap_or_default();
            let from_abs = root.abs(&share.root_rel(from)?)?;
            if from_abs.is_dir() && !abs.exists() {
                batch.push(
                    share,
                    OperationAction::MoveFolder {
                        source: share.location(from)?,
                        target: share.location(rel)?,
                    },
                    "folder_move",
                    Some(from),
                    Some(rel),
                    &d.vv,
                );
            } else if !from_abs.exists() && abs.is_dir() {
                rename_rows(state, &share.id, from, rel, None).await?;
            } else if from_abs.exists() || abs.exists() {
                move_conflict(state, peer, share, e).await?;
            }
        }
        "folder_created" => {
            if !abs.exists() {
                batch.push(
                    share,
                    OperationAction::CreateFolder {
                        target: share.location(rel)?,
                    },
                    "folder_create",
                    None,
                    Some(rel),
                    &d.vv,
                );
            }
        }
        "folder_trashed" => {
            if abs.is_dir() {
                batch.push(
                    share,
                    OperationAction::TrashFolder {
                        source: share.location(rel)?,
                        expected_entries: None,
                    },
                    "folder_trash",
                    None,
                    Some(rel),
                    &d.vv,
                );
            }
        }
        other => {
            return Err(AppError::bad_request(
                "unsupported_peer_event",
                format!("unsupported peer event kind '{other}'"),
            ));
        }
    }
    Ok(Applied::Done)
}

async fn move_conflict(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    e: &PeerEvent,
) -> AppResult<()> {
    let from = e.data.from.clone().unwrap_or_default();
    conflict(
        state,
        ConflictInput {
            share,
            rel: &e.data.path,
            kind: if e.kind == "folder_moved" { "folder_move" } else { "move" },
            detail: format!(
                "{} moved {from} to {}, but here the source changed or the destination already exists",
                peer.name, e.data.path
            ),
            remote: remote_side(&peer.name, &e.data, false),
            remote_vv: &e.data.vv,
            peer_id: &peer.id,
            from: e.data.from.as_deref(),
        },
    )
    .await
}

/// Pulls and applies new events from one peer. Returns the number applied.
pub async fn pull(state: &AppState, peer: &net::Peer) -> Result<usize, String> {
    inbox::pull(state, peer).await
}

/// Turns the managed actions collected from a peer into one operation, recording what each
/// item means for the replica rows once it finishes.
async fn insert_batch_in(
    tx: &mut sqlx::SqliteConnection,
    peer: &net::Peer,
    idempotency_key: String,
    batch: Batch,
) -> AppResult<Option<ism_core::dto::OperationAccepted>> {
    if batch.actions.is_empty() {
        return Ok(None);
    }
    let principal = Principal::Peer {
        peer_id: peer.id.clone(),
    };
    let req = OperationRequest {
        idempotency_key,
        label: Some(format!("Changes from {}", peer.name)),
        actions: batch.actions,
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let op = crate::ops::insert_in(&mut *tx, &principal, &req, None).await?;
    for (idx, (share_id, kind, from, to, vv)) in batch.pending.into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&op.operation_id)
        .bind(idx as i64)
        .bind(share_id)
        .bind(kind)
        .bind(from)
        .bind(to)
        .bind(serde_json::to_string(&vv)?)
        .bind(&peer.id)
        .execute(&mut *tx)
        .await?;
    }
    Ok(Some(op))
}

// ---------------------------------------------------------------------------------------------
// Share backfill

/// Rows per inventory page.
const INVENTORY_BATCH: i64 = 500;

/// One replica row of a share, as a peer sees it during a backfill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryEntry {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    pub size: u64,
    pub mtime_ns: i64,
    pub deleted: bool,
    #[serde(default)]
    pub vv: VersionVector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryPage {
    pub entries: Vec<InventoryEntry>,
    /// The cursor of the next page; `None` once the inventory is complete.
    pub next: Option<String>,
}

/// Kind of the event that tells peers a share became active here, so they backfill from us.
pub(crate) const SHARE_ENABLED: &str = "share_enabled";
/// Accepted for receipt only; this version never produces it (see `inbox::folder_pruned`).
pub(crate) const FOLDER_PRUNED: &str = "folder_pruned";

/// Marks `share` for a backfill from each of `peer_ids` and announces it, so both sides catch
/// up with what happened while the share was not replicated between them.
pub(crate) async fn request_backfill_in(
    tx: &mut sqlx::SqliteConnection,
    instance_id: &str,
    share_id: &str,
    share_name: &str,
    peer_ids: &[String],
) -> AppResult<()> {
    if peer_ids.is_empty() {
        return Ok(());
    }
    for peer_id in peer_ids {
        mark_backfill_in(&mut *tx, share_id, peer_id).await?;
    }
    append_local_in(
        tx,
        instance_id,
        share_name,
        SHARE_ENABLED,
        &EventData::default(),
    )
    .await
}

async fn mark_backfill_in(
    conn: &mut sqlx::SqliteConnection,
    share_id: &str,
    peer_id: &str,
) -> AppResult<()> {
    let known: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM peers WHERE id = ?)")
        .bind(peer_id)
        .fetch_one(&mut *conn)
        .await?;
    if known {
        sqlx::query(
            "INSERT INTO share_backfills (share_id, peer_id, after_path, requested_at) VALUES (?, ?, '', ?)
             ON CONFLICT(share_id, peer_id) DO UPDATE SET after_path = '', requested_at = excluded.requested_at",
        )
        .bind(share_id)
        .bind(peer_id)
        .bind(db::now())
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// One page of the replica rows of `share_name` after `after`, for a peer in the share.
pub async fn inventory_for(
    state: &AppState,
    peer_id: &str,
    share_name: &str,
    after: &str,
) -> AppResult<InventoryPage> {
    let Some(share) = shares(state)
        .await?
        .into_iter()
        .find(|s| s.name == share_name && s.includes(peer_id))
    else {
        // Not shared with this peer (yet): it will announce the share once enabled here.
        return Ok(InventoryPage {
            entries: Vec::new(),
            next: None,
        });
    };
    type R = (String, i64, i64, Option<String>, i64, String);
    let rows: Vec<R> = sqlx::query_as(
        "SELECT rel_path, size, mtime_ns, sha256, deleted, vv_json FROM replica_files
         WHERE share_id = ? AND rel_path > ? AND (deleted = 1 OR sha256 IS NOT NULL)
         AND instr('/' || rel_path || '/', ?) = 0
         ORDER BY rel_path LIMIT ?",
    )
    .bind(&share.id)
    .bind(after)
    .bind(format!("/{SYNOLOGY_METADATA_DIR}/"))
    .bind(INVENTORY_BATCH)
    .fetch_all(&state.db)
    .await?;
    let next = (rows.len() as i64 == INVENTORY_BATCH)
        .then(|| rows.last().map(|r| r.0.clone()))
        .flatten();
    let entries = rows
        .into_iter()
        .map(|(path, size, mtime_ns, sha256, deleted, vv)| {
            Ok(InventoryEntry {
                path,
                sha256: if deleted != 0 { None } else { sha256 },
                size: size.max(0) as u64,
                mtime_ns,
                deleted: deleted != 0,
                vv: serde_json::from_str(&vv)?,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(InventoryPage { entries, next })
}

impl InventoryPage {
    fn validate(&self, after: &str) -> Result<(), String> {
        if self.entries.len() as i64 > INVENTORY_BATCH {
            return Err("invalid peer inventory page size".into());
        }
        let mut previous = after;
        for entry in &self.entries {
            let path = RelPath::parse(&entry.path)
                .map_err(|_| "invalid peer inventory path".to_owned())?;
            if path.is_root()
                || entry.path.as_str() <= previous
                || entry.deleted == entry.sha256.is_some()
            {
                return Err("invalid peer inventory entry".into());
            }
            if let Some(sha) = &entry.sha256 {
                checked_transfer_size(sha, entry.size).map_err(|e| e.message().to_owned())?;
            }
            previous = &entry.path;
        }
        if self
            .next
            .as_deref()
            .is_some_and(|n| n != previous || self.entries.is_empty())
        {
            return Err("invalid peer inventory cursor".into());
        }
        Ok(())
    }
}

/// Applies one inventory page of a pending share backfill from `peer`: each remote row is
/// handled like the event that produced it, so newer versions are fetched, remote deletions
/// go to the ISM trash, and concurrent changes become conflicts. Returns the rows applied.
pub async fn backfill(state: &AppState, peer: &net::Peer) -> Result<usize, String> {
    inbox::backfill(state, peer).await
}

/// Finishes the replica bookkeeping of an operation that applied peer events or resolved a
/// conflict. Returns false when the operation was a plain local one.
/// The checksum and size a finished peer publication wrote. The upload must be the one the
/// operation item published, belong to this operation only, and hold a single file at `to`.
async fn published_upload(
    state: &AppState,
    operation_id: &str,
    idx: i64,
    share: &Share,
    to: &str,
) -> AppResult<(String, u64)> {
    let unverifiable = |message: &str| AppError::conflict("recovery_required", message);
    let action: String = sqlx::query_scalar(
        "SELECT action_json FROM operation_items WHERE operation_id = ? AND idx = ?",
    )
    .bind(operation_id)
    .bind(idx)
    .fetch_one(&state.db)
    .await?;
    let OperationAction::PublishUpload { upload_id } = crate::ops::exec::parse_action(&action)?
    else {
        return Err(unverifiable(
            "the peer publication is not an upload publication",
        ));
    };
    let uploads: Vec<(String, String)> =
        sqlx::query_as("SELECT id, request_json FROM uploads WHERE operation_id = ?")
            .bind(operation_id)
            .fetch_all(&state.db)
            .await?;
    let [(id, raw)] = uploads.as_slice() else {
        return Err(unverifiable(
            "the peer publication is not tied to exactly one upload",
        ));
    };
    if *id != upload_id {
        return Err(unverifiable(
            "the peer publication upload does not match its operation",
        ));
    }
    let request: ism_core::dto::CreateUpload = serde_json::from_str(raw)?;
    let [spec] = request.files.as_slice() else {
        return Err(unverifiable(
            "the peer publication has no single verifiable file",
        ));
    };
    let uploaded = RelPath::parse(&request.target.path)
        .map_err(|e| AppError::invalid_location(e.to_string()))?
        .join(&spec.name)
        .map_err(|e| AppError::invalid_location(e.to_string()))?;
    let at = share.rel_of(&Location {
        root_id: request.target.root_id.clone(),
        path: uploaded.as_str().to_owned(),
    });
    if at.as_deref() != Some(to) {
        return Err(unverifiable(
            "the peer publication upload targets a different path",
        ));
    }
    Ok((spec.sha256.clone(), spec.size))
}

#[cfg(test)]
pub(super) struct PublicationPause {
    pub reached: tokio::sync::oneshot::Sender<()>,
    pub resume: tokio::sync::oneshot::Receiver<()>,
}

/// Test-only pause between a peer publication's file write and its replica bookkeeping, keyed by
/// share id so concurrently running tests do not interfere.
#[cfg(test)]
pub(super) static PUBLICATION_PAUSES: std::sync::Mutex<Vec<(String, PublicationPause)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn publication_bookkeeping_pause(share_id: &str) {
    let pause = {
        let mut pauses = PUBLICATION_PAUSES.lock().unwrap();
        pauses
            .iter()
            .position(|(id, _)| id == share_id)
            .map(|index| pauses.remove(index).1)
    };
    if let Some(pause) = pause {
        let _ = pause.reached.send(());
        let _ = pause.resume.await;
    }
}

async fn finish_pending(state: &AppState, operation_id: &str) -> AppResult<bool> {
    type Pending = (
        i64,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        String,
        Option<String>,
    );
    let rows: Vec<Pending> = sqlx::query_as(
        "SELECT idx, share_id, kind, from_path, to_path, vv_json, peer_id, conflict_id FROM pending_peer_ops WHERE operation_id = ?",
    )
    .bind(operation_id)
    .fetch_all(&state.db)
    .await?;
    if rows.is_empty() {
        return Ok(false);
    }
    let shares = shares(state).await?;
    for (idx, share_id, kind, from, to, vv_json, _peer_id, conflict_id) in rows {
        let Some(share) = shares.iter().find(|s| s.id == share_id) else {
            continue;
        };
        let (status, code, message): (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT status, error_code, error_message FROM operation_items WHERE operation_id = ? AND idx = ?",
        )
        .bind(operation_id)
        .bind(idx)
        .fetch_one(&state.db)
        .await?;
        let ok = status == "done";
        if kind == "move_decision_step" {
            admin::finish_step(
                state,
                operation_id,
                conflict_id
                    .as_deref()
                    .ok_or_else(|| AppError::internal("move choice has no conflict"))?,
                &vv_json,
                ok,
            )
            .await?;
            continue;
        }
        let vv: VersionVector = serde_json::from_str(&vv_json)?;
        let to = to.unwrap_or_default();
        let delete_pending = ok;
        match (kind.as_str(), ok) {
            ("delete", _) if ok || code.as_deref() == Some("not_found") => {
                let mut r = row(state, &share.id, &to).await?.unwrap_or(Row {
                    size: 0,
                    mtime_ns: 0,
                    sha256: None,
                    deleted: true,
                    vv: VersionVector::default(),
                });
                r.deleted = true;
                r.vv.merge(&vv);
                put_row(state, &share.id, &to, &r).await?;
                if let Some(cid) = &conflict_id {
                    // A conflict resolved by keeping the peer's deletion: announce it now that
                    // the local copy is in the ISM trash.
                    settle_conflict(state, cid, share, "file_deleted", &to, r.vv).await?;
                }
            }
            ("move" | "folder_move", true) => {
                rename_rows(
                    state,
                    &share.id,
                    from.as_deref().unwrap_or_default(),
                    &to,
                    Some(&vv),
                )
                .await?;
                if let Some(cid) = &conflict_id {
                    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
                    admin::resolved_in(&mut tx, cid, "keep_remote").await?;
                    tx.commit().await?;
                    state.emit(ServerEvent::ConflictsChanged);
                }
            }
            ("folder_trash", true) => {
                sqlx::query("UPDATE replica_files SET deleted = 1 WHERE share_id = ? AND substr(rel_path, 1, ?) = ?")
                    .bind(&share.id)
                    .bind(to.chars().count() as i64 + 1)
                    .bind(format!("{to}/"))
                    .execute(&state.db)
                    .await?;
                if let Some(cid) = &conflict_id {
                    settle_conflict(state, cid, share, "folder_trashed", &to, vv.clone()).await?;
                }
            }
            ("keep_both", true) => {
                // The local copy now lives under a new name: a new file for every peer.
                let from = from.unwrap_or_default();
                sqlx::query("DELETE FROM replica_files WHERE share_id = ? AND rel_path = ?")
                    .bind(&share.id)
                    .bind(&to)
                    .execute(&state.db)
                    .await?;
                if let Some(mut r) = row(state, &share.id, &from).await? {
                    r.deleted = true;
                    put_row(state, &share.id, &from, &r).await?;
                }
                if let Ok(root) = files::root(&state.db, &share.root_id).await {
                    register_local(state, share, &root, &to).await?;
                }
                if let Some(cid) = &conflict_id {
                    admin::take_remote(state, cid).await?;
                }
            }
            ("publish", true) => {
                #[cfg(test)]
                publication_bookkeeping_pause(&share.id).await;
                let (published_sha, published_size) =
                    published_upload(state, operation_id, idx, share, &to).await?;
                let root = files::root(&state.db, &share.root_id).await?;
                let path = root.abs(&share.root_rel(&to)?)?;
                let (meta, fingerprint) = tokio::task::spawn_blocking(move || {
                    std::io::Result::Ok((
                        std::fs::metadata(&path)?,
                        files::replacement_fingerprint(&path)?,
                    ))
                })
                .await
                .map_err(AppError::internal)??;
                // Only the published bytes carry the peer's version. A local write that landed
                // after publication stays unregistered here so the next scan records it as a
                // local change (and peers see it as concurrent) instead of absorbing it.
                let unchanged = fingerprint.as_ref().is_some_and(|(revision, sha)| {
                    *revision == files::revision(&meta)
                        && sha.eq_ignore_ascii_case(&published_sha)
                        && meta.len() == published_size
                });
                let sha256 = published_sha;
                let mut merged = row(state, &share.id, &to)
                    .await?
                    .map(|r| r.vv)
                    .unwrap_or_default();
                merged.merge(&vv);
                let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
                put_row_in(
                    &mut tx,
                    &share.id,
                    &to,
                    &Row {
                        size: published_size as i64,
                        mtime_ns: if unchanged { files::mtime_ns(&meta) } else { 0 },
                        sha256: Some(sha256.clone()),
                        deleted: false,
                        vv: merged.clone(),
                    },
                )
                .await?;
                let move_choice: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pending_peer_ops WHERE kind='move_decision_wait'
                     AND share_id=? AND peer_id=? AND
                     EXISTS(SELECT 1 FROM json_each(vv_json,'$.files') WHERE json_extract(value,'$.path')=?))",
                ).bind(&share.id).bind(&_peer_id).bind(&to).fetch_one(&mut *tx).await?;
                if move_choice {
                    sqlx::query(
                        "INSERT OR IGNORE INTO completed_peer_bookkeeping(operation_id) VALUES (?)",
                    )
                    .bind(operation_id)
                    .execute(&mut *tx)
                    .await?;
                }
                if let Some(cid) = &conflict_id
                    && !move_choice
                {
                    admin::resolved_in(&mut tx, cid, "keep_remote").await?;
                    append_local_in(
                        &mut tx,
                        &state.instance_id,
                        &share.name,
                        "file_changed",
                        &EventData {
                            path: to.clone(),
                            sha256: Some(sha256),
                            size: Some(published_size),
                            vv: merged,
                            ..Default::default()
                        },
                    )
                    .await?;
                    state.emit(ServerEvent::ConflictsChanged);
                }
                sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id=? AND idx=?")
                    .bind(operation_id)
                    .bind(idx)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                if !unchanged {
                    let location = share.location(&to)?;
                    rescan_shares_soon(state, std::slice::from_ref(share), &[&location]);
                }
            }
            (_, true) => {}
            (_, false) if conflict_id.is_some() => {
                let move_choice: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pending_peer_ops WHERE kind='move_decision_wait' AND conflict_id=?)",
                ).bind(conflict_id.as_deref()).fetch_one(&state.db).await?;
                if move_choice {
                    continue;
                }
                // The resolution did not apply: the conflict needs another decision.
                sqlx::query(
                    "UPDATE conflicts SET status = 'open', resolution = NULL,
                     detail = detail || ' (resolution failed: ' || ? || ')'
                     WHERE id = ? AND status = 'resolving'",
                )
                .bind(message.or(code.clone()).unwrap_or_default())
                .bind(conflict_id.as_deref())
                .execute(&state.db)
                .await?;
                state.emit(ServerEvent::ConflictsChanged);
            }
            (_, false) => {}
        }
        if delete_pending || (kind == "delete" && code.as_deref() == Some("not_found")) {
            sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id = ? AND idx = ?")
                .bind(operation_id)
                .bind(idx)
                .execute(&state.db)
                .await?;
        }
    }
    Ok(true)
}

/// Marks a conflict resolved and announces the local outcome to peers, atomically.
async fn settle_conflict(
    state: &AppState,
    conflict_id: &str,
    share: &Share,
    kind: &str,
    rel: &str,
    vv: VersionVector,
) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    admin::resolved_in(&mut tx, conflict_id, "keep_remote").await?;
    append_local_in(
        &mut tx,
        &state.instance_id,
        &share.name,
        kind,
        &EventData {
            path: rel.to_owned(),
            vv,
            ..Default::default()
        },
    )
    .await?;
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    state.emit(ServerEvent::ConflictsChanged);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Workers

async fn poll_peer(state: &AppState, peer: &net::Peer) {
    if let Err(e) = crate::activity::observe(
        state,
        format!("peer:{}", peer.id),
        "Peer synchronization",
        &peer.name,
        Some("/peers"),
        |task| async move { poll_peer_reporting(state, peer, &task).await },
    )
    .await
    {
        tracing::warn!(peer=%peer.id, "peer background task: {}", e.message());
    }
    // Always process decisions, including after failed peer connections.
    if let Err(error) = crate::activity::observe(
        state,
        format!("move-decisions:{}", peer.id),
        "Peer move decision verification",
        &peer.name,
        Some("/sync"),
        |task| async move {
            task.waiting("Waiting for safe decision verification");
            admin::process_move_decisions(state).await
        },
    )
    .await
    {
        tracing::warn!("move decision verification deferred: {}", error.message());
    }
}

async fn poll_peer_reporting(
    state: &AppState,
    peer: &net::Peer,
    task: &crate::activity::Reporter,
) -> AppResult<()> {
    task.phase("Connecting to peer and verifying its identity");
    let hello: Result<net::Hello, String> = net::get_json(state, peer, "/api/peer/v1/hello").await;
    match hello {
        Ok(h) if peer.instance_id.as_deref() == Some(h.instance_id.as_str()) => {
            net::mark(state, &peer.id, Ok(())).await;
            task.phase("Synchronizing shared settings");
            sync_settings(state, peer).await;
            task.phase("Pulling and applying peer events");
            let pulled = pull(state, peer).await;
            if let Ok(n) = &pulled {
                task.progress(*n as u64, None, "events applied");
            }
            task.phase("Fetching and applying share inventory backfill");
            let filled = backfill(state, peer).await;
            let outcome = pulled.and(filled);
            let retained_error = inbox::processing_error(state, &peer.id)
                .await
                .unwrap_or_else(|error| Some(error.message().to_owned()));
            net::mark_processing_result(
                state,
                &peer.id,
                outcome
                    .as_ref()
                    .err()
                    .map(String::as_str)
                    .or(retained_error.as_deref()),
            )
            .await;
            if let Err(error) = outcome {
                return Err(AppError::conflict(
                    "peer_processing",
                    crate::error::redact(&error),
                ));
            }
            if let Some(error) = retained_error {
                return Err(AppError::conflict(
                    "peer_processing",
                    crate::error::redact(&error),
                ));
            }
        }
        Ok(_) => {
            net::mark(
                state,
                &peer.id,
                Err("the peer answered with another instance id".into()),
            )
            .await;
            return Err(AppError::conflict(
                "peer_identity",
                "the peer answered with another instance id",
            ));
        }
        Err(e) => {
            net::mark(state, &peer.id, Err(e.clone())).await;
            return Err(AppError::conflict(
                "peer_unreachable",
                crate::error::redact(&e),
            ));
        }
    }
    Ok(())
}

/// Merges the peer's shared file settings. Peers from before shared settings answer 404.
async fn sync_settings(state: &AppState, peer: &net::Peer) {
    let remote: Result<SharedSettings, String> =
        net::get_json(state, peer, "/api/peer/v1/settings").await;
    match remote {
        Ok(remote) => {
            if let Err(error) = crate::settings::merge_remote(state, &remote).await {
                tracing::warn!(peer = %peer.name, "cannot merge shared settings: {}", error.message());
            }
        }
        Err(error) => tracing::debug!(peer = %peer.name, "no shared settings: {error}"),
    }
}

pub async fn run(state: AppState) {
    let mut workers = tokio::task::JoinSet::new();
    workers.spawn(replicate(state.clone()));
    workers.spawn(activity::run(state.clone()));
    workers.spawn(versions::run(state));
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(()) => tracing::error!("peer background worker stopped unexpectedly"),
            Err(error) => tracing::error!("peer background worker failed: {error}"),
        }
    }
}

/// Polls each peer independently so a slow connection does not delay the others.
async fn replicate(state: AppState) {
    let tick = std::time::Duration::from_secs(state.config.tick_secs.max(1));
    // Spawned workers keep progressing during peer-list queries; dropping the set aborts them.
    let mut active = tokio::task::JoinSet::new();
    let mut running = HashMap::new();
    let mut next_poll = HashMap::new();
    loop {
        let peers = match net::peers(&state).await {
            Ok(peers) => peers,
            Err(error) => {
                tracing::error!("cannot load peers: {}", error.message());
                Vec::new()
            }
        };
        let current: HashSet<_> = peers.iter().map(|peer| peer.id.clone()).collect();
        next_poll.retain(|id, _| current.contains(id));
        let now = std::time::Instant::now();
        for peer in peers {
            if !running.contains_key(&peer.id)
                && next_poll.get(&peer.id).is_none_or(|due| *due <= now)
            {
                let id = peer.id.clone();
                let state = state.clone();
                let worker = active.spawn(async move {
                    poll_peer(&state, &peer).await;
                    peer.id
                });
                running.insert(id, worker.id());
            }
        }
        tokio::select! {
            Some(result) = active.join_next(), if !active.is_empty() => {
                let id = match result {
                    Ok(id) => Some(id),
                    Err(error) => {
                        tracing::error!("peer synchronization worker failed: {error}");
                        running.iter().find_map(|(peer, id)| (*id == error.id()).then(|| peer.clone()))
                    }
                };
                if let Some(id) = id {
                    running.remove(&id);
                    next_poll.insert(id, std::time::Instant::now() + tick);
                }
            }
            () = tokio::time::sleep(tick) => {}
            () = state.sync_wake.notified() => { next_poll.clear(); }
        }
    }
}

/// Scans never run while a managed operation is changing files, so half-done moves are never
/// mistaken for deletes.
/// Rescans shares: soon after the file watcher saw an outside change below one (once the
/// change had time to settle), and every `scan_interval_secs` as a safety net.
pub async fn scan_worker(state: AppState) {
    let scan_every = std::time::Duration::from_secs(state.config.scan_interval_secs.max(5));
    let retry = std::time::Duration::from_secs(state.config.tick_secs.max(1));
    let mut next_full = tokio::time::Instant::now();
    loop {
        let now = std::time::Instant::now();
        let full_due = tokio::time::Instant::now() >= next_full;
        let full = if full_due {
            match full_scan_allowed(&state).await {
                Ok(allowed) => allowed,
                Err(error) => {
                    tracing::warn!("full share scan check skipped: {}", error.message());
                    false
                }
            }
        } else {
            false
        };
        let due: Vec<String> = match state.share_changes.lock() {
            Ok(mut map) => {
                let due: Vec<String> = map
                    .iter()
                    .filter(|(_, at)| full || **at <= now)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in &due {
                    map.remove(id);
                }
                due
            }
            Err(_) => Vec::new(),
        };
        if full || !due.is_empty() {
            match shares(&state).await {
                Ok(shares) => {
                    for share in shares
                        .iter()
                        .filter(|s| s.enabled && (full || due.contains(&s.id)))
                    {
                        if let Err(e) = scan_share(&state, share).await {
                            tracing::warn!(share = %share.name, "scan skipped: {}", e.message());
                        }
                    }
                }
                Err(error) => tracing::error!("cannot load shares: {}", error.message()),
            }
        }
        if full {
            next_full = tokio::time::Instant::now() + scan_every;
        } else if full_due {
            next_full = tokio::time::Instant::now() + retry;
        }
        let next_change = state
            .share_changes
            .lock()
            .ok()
            .and_then(|map| map.values().min().copied())
            .map(|at| tokio::time::Instant::from_std(at).max(tokio::time::Instant::now()));
        let wake_at = next_change.map_or(next_full, |at| at.min(next_full));
        tokio::select! {
            () = tokio::time::sleep_until(wake_at) => {}
            () = state.share_wake.notified() => {}
        }
    }
}

async fn full_scan_allowed(state: &AppState) -> AppResult<bool> {
    // Each share scan excludes uncertain footprints, including queued transfers.
    inbox::require_scannable(state).await?;
    Ok(true)
}

/// Paths relative to a share, for the admin UI.
pub fn display(share: &Share, rel: &str) -> Location {
    share.location(rel).unwrap_or(Location {
        root_id: share.root_id.clone(),
        path: rel.to_owned(),
    })
}

#[cfg(test)]
mod tests;
