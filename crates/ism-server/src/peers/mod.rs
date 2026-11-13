//! Peer-to-peer replication between ISM instances.
//!
//! A *share* is a folder replicated under the same name on several instances. Each instance keeps
//! a row per file with a version vector. Local changes become events in `event_log`; peers pull
//! them in order. Management events (moves, deletes, folders) are applied immediately through
//! managed operations so Immich keeps its asset IDs; file contents are copied by the transfer
//! worker during allowed hours. Concurrent changes become conflicts that an admin resolves.

pub mod admin;
pub mod net;
pub mod transfer;

use std::collections::{HashMap, HashSet};
use std::fs;

use ism_core::dto::{ConflictSide, Location, OperationAction, OperationRequest};
use ism_core::paths::RelPath;
use ism_core::version::{Causality, VersionVector};
use serde::{Deserialize, Serialize};

use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files::{self, HIDDEN, Root};
use crate::state::{AppState, ServerEvent};

/// Files modified more recently than this are left for the next scan (they may still be written).
const SETTLE_SECS: i64 = 30;
const EVENT_BATCH: i64 = 500;

#[derive(Debug, Clone)]
pub struct Share {
    pub id: String,
    pub name: String,
    pub root_id: String,
    pub path: RelPath,
    pub peer_ids: Vec<String>,
    pub enabled: bool,
}

impl Share {
    /// The share-relative path of a location, if it lies inside the share.
    pub fn rel_of(&self, loc: &Location) -> Option<String> {
        if loc.root_id != self.root_id {
            return None;
        }
        let p = RelPath::parse(&loc.path).ok()?;
        if !p.starts_with(&self.path) || p == self.path {
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
        files::parse_rel(&joined)
    }

    pub fn location(&self, rel: &str) -> AppResult<Location> {
        Ok(Location {
            root_id: self.root_id.clone(),
            path: self.root_rel(rel)?.as_str().to_owned(),
        })
    }

    pub fn includes(&self, peer_id: &str) -> bool {
        self.enabled && self.peer_ids.iter().any(|p| p == peer_id)
    }
}

pub async fn shares(state: &AppState) -> AppResult<Vec<Share>> {
    let rows: Vec<(String, String, String, String, String, i64)> = sqlx::query_as(
        "SELECT id, name, root_id, path, peer_ids_json, enabled FROM shares ORDER BY name",
    )
    .fetch_all(&state.db)
    .await?;
    rows.into_iter()
        .map(|(id, name, root_id, path, peers, enabled)| {
            Ok(Share {
                id,
                name,
                root_id,
                path: files::parse_rel(&path)?,
                peer_ids: db::json_vec(&peers),
                enabled: enabled != 0,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// Appends an event that originated here.
pub async fn append_local(
    state: &AppState,
    share: &str,
    kind: &str,
    data: &EventData,
) -> AppResult<()> {
    let mut tx = state.db.begin().await?;
    let next: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(origin_seq), 0) + 1 FROM event_log WHERE origin = ?",
    )
    .bind(&state.instance_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO event_log (origin, origin_seq, share_name, kind, data_json, at) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&state.instance_id)
        .bind(next)
        .bind(share)
        .bind(kind)
        .bind(serde_json::to_string(data)?)
        .bind(db::now())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn log_remote(state: &AppState, e: &PeerEvent) -> AppResult<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO event_log (origin, origin_seq, share_name, kind, data_json, at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&e.origin)
    .bind(e.origin_seq)
    .bind(&e.share)
    .bind(&e.kind)
    .bind(serde_json::to_string(&e.data)?)
    .bind(db::now())
    .execute(&state.db)
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
    let events = rows
        .into_iter()
        .filter(|r| r.1 != peer_instance && allowed.contains(r.3.as_str()))
        .filter_map(|(seq, origin, origin_seq, share, kind, data)| {
            Some(PeerEvent {
                seq,
                origin,
                origin_seq,
                share,
                kind,
                data: serde_json::from_str(&data).ok()?,
            })
        })
        .collect();
    Ok(EventPage { events, next })
}

// ---------------------------------------------------------------------------------------------
// Replica rows

#[derive(Debug, Clone)]
pub struct Row {
    pub size: i64,
    pub mtime_ns: i64,
    pub sha256: Option<String>,
    pub deleted: bool,
    pub vv: VersionVector,
}

pub async fn row(state: &AppState, share_id: &str, rel: &str) -> AppResult<Option<Row>> {
    let r: Option<(i64, i64, Option<String>, i64, String)> = sqlx::query_as(
        "SELECT size, mtime_ns, sha256, deleted, vv_json FROM replica_files WHERE share_id = ? AND rel_path = ?",
    )
    .bind(share_id)
    .bind(rel)
    .fetch_optional(&state.db)
    .await?;
    Ok(r.map(|(size, mtime_ns, sha256, deleted, vv)| Row {
        size,
        mtime_ns,
        sha256,
        deleted: deleted != 0,
        vv: serde_json::from_str(&vv).unwrap_or_default(),
    }))
}

pub async fn put_row(state: &AppState, share_id: &str, rel: &str, r: &Row) -> AppResult<()> {
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
    .execute(&state.db)
    .await?;
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
    let rows: Vec<(String, i64, i64, Option<String>, i64, String)> = sqlx::query_as(
        "SELECT rel_path, size, mtime_ns, sha256, deleted, vv_json FROM replica_files
         WHERE share_id = ? AND (rel_path = ? OR substr(rel_path, 1, ?) = ?)",
    )
    .bind(share_id)
    .bind(from)
    .bind(from.len() as i64 + 1)
    .bind(format!("{from}/"))
    .fetch_all(&state.db)
    .await?;
    for (path, size, mtime_ns, sha256, deleted, vv_json) in rows {
        let new_path = format!("{to}{}", &path[from.len()..]);
        let mut v: VersionVector = serde_json::from_str(&vv_json).unwrap_or_default();
        if let Some(extra) = vv {
            v.merge(extra);
        }
        sqlx::query("DELETE FROM replica_files WHERE share_id = ? AND rel_path = ?")
            .bind(share_id)
            .bind(&path)
            .execute(&state.db)
            .await?;
        put_row(
            state,
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

fn hidden(name: &str) -> bool {
    HIDDEN.contains(&name) || name.starts_with(".ism")
}

/// Hashes one file on disk and records it as a local change when it differs from its row.
async fn register_local(
    state: &AppState,
    share: &Share,
    root: &Root,
    rel: &str,
) -> AppResult<Option<Row>> {
    let root_rel = share.root_rel(rel)?;
    let abs = root.abs(&root_rel)?;
    let Ok(meta) = fs::metadata(&abs) else {
        return row(state, &share.id, rel).await;
    };
    if !meta.is_file() {
        return row(state, &share.id, rel).await;
    }
    let existing = row(state, &share.id, rel).await?;
    let (size, mtime) = (meta.len() as i64, files::mtime_ns(&meta));
    if let Some(r) = &existing
        && !r.deleted
        && r.size == size
        && r.mtime_ns == mtime
    {
        return Ok(existing);
    }
    let sha = files::cached_sha256(&state.db, &root.mapping.root_id, &root_rel, &abs).await?;
    let mut r = existing.unwrap_or(Row {
        size,
        mtime_ns: mtime,
        sha256: None,
        deleted: true,
        vv: VersionVector::default(),
    });
    let changed = r.deleted || r.sha256.as_deref() != Some(&sha);
    r.size = size;
    r.mtime_ns = mtime;
    if changed {
        r.vv.increment(&state.instance_id);
        r.deleted = false;
        r.sha256 = Some(sha.clone());
    }
    put_row(state, &share.id, rel, &r).await?;
    if changed {
        append_local(
            state,
            &share.name,
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
    }
    Ok(Some(r))
}

/// Compares a share with its rows and records local changes as events.
pub async fn scan_share(state: &AppState, share: &Share) -> AppResult<()> {
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
    let found: Vec<(String, i64, i64)> = tokio::task::spawn_blocking(move || {
        walkdir::WalkDir::new(&base)
            .min_depth(1)
            .into_iter()
            .filter_entry(|e| !hidden(&e.file_name().to_string_lossy()))
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter_map(|e| {
                let rel = e
                    .path()
                    .strip_prefix(&base)
                    .ok()?
                    .to_str()?
                    .replace('\\', "/");
                let meta = e.metadata().ok()?;
                Some((rel, meta.len() as i64, files::mtime_ns(&meta)))
            })
            .collect()
    })
    .await
    .map_err(AppError::internal)?;
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
    for (rel, size, mtime) in found {
        seen.insert(rel.clone());
        if known
            .get(&rel)
            .is_some_and(|k| !k.2 && k.0 == size && k.1 == mtime)
            || mtime > settle_cutoff
        {
            continue;
        }
        if RelPath::parse(&rel).is_err() {
            continue;
        }
        if let Err(e) = register_local(state, share, &root, &rel).await {
            tracing::warn!(share = %share.name, "scan {rel}: {}", e.message());
        }
    }
    for (rel, (_, _, deleted)) in known {
        if deleted || seen.contains(&rel) {
            continue;
        }
        if let Some(mut r) = row(state, &share.id, &rel).await? {
            r.deleted = true;
            r.vv.increment(&state.instance_id);
            put_row(state, &share.id, &rel, &r).await?;
            append_local(
                state,
                &share.name,
                "file_deleted",
                &EventData {
                    path: rel.clone(),
                    vv: r.vv,
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
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Local managed operations → events

/// What a finished local operation changed, for replication.
#[derive(Debug, Clone)]
pub enum LocalChange {
    FileMoved { from: Location, to: Location },
    FolderMoved { from: Location, to: Location },
    FolderCreated { at: Location },
    FolderTrashed { at: Location },
}

/// Called by the executor when an operation ends. Operations that applied peer events finish
/// their bookkeeping here; other operations become events for peers.
pub async fn operation_finished(state: &AppState, operation_id: &str, changes: &[LocalChange]) {
    match finish_pending(state, operation_id).await {
        Ok(true) => {}
        Ok(false) => {
            if let Err(e) = record_local(state, changes).await {
                tracing::error!(
                    "replication bookkeeping for {operation_id}: {}",
                    e.message()
                );
            }
        }
        Err(e) => tracing::error!("peer bookkeeping for {operation_id}: {}", e.message()),
    }
    state.sync_wake.notify_one();
}

async fn record_local(state: &AppState, changes: &[LocalChange]) -> AppResult<()> {
    let shares = shares(state).await?;
    for change in changes {
        match change {
            LocalChange::FileMoved { from, to } | LocalChange::FolderMoved { from, to } => {
                let folder = matches!(change, LocalChange::FolderMoved { .. });
                for share in &shares {
                    let (Some(f), Some(t)) = (share.rel_of(from), share.rel_of(to)) else {
                        continue;
                    };
                    // Moves across shares are seen by the scanner as delete + create.
                    let r = row(state, &share.id, &f).await?;
                    if !folder && r.as_ref().is_none_or(|r| r.deleted) {
                        continue;
                    }
                    rename_rows(state, &share.id, &f, &t, None).await?;
                    let r = r.unwrap_or_else(|| Row {
                        size: 0,
                        mtime_ns: 0,
                        sha256: None,
                        deleted: false,
                        vv: VersionVector::default(),
                    });
                    append_local(
                        state,
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
                        append_local(
                            state,
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
                    .bind(p.len() as i64 + 1)
                    .bind(format!("{p}/"))
                    .fetch_all(&state.db)
                    .await?;
                    let mut vv = VersionVector::default();
                    for rel in rows {
                        if let Some(mut r) = row(state, &share.id, &rel).await? {
                            r.deleted = true;
                            r.vv.increment(&state.instance_id);
                            vv.merge(&r.vv);
                            put_row(state, &share.id, &rel, &r).await?;
                        }
                    }
                    append_local(
                        state,
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
        "INSERT INTO conflicts (id, share_id, rel_path, kind, detail, local_json, remote_json, remote_vv_json, peer_id, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
         ON CONFLICT(id) DO UPDATE SET kind = excluded.kind, detail = excluded.detail, local_json = excluded.local_json,
         remote_json = excluded.remote_json, remote_vv_json = excluded.remote_vv_json, peer_id = excluded.peer_id",
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
    sqlx::query("DELETE FROM transfers WHERE share_id = ? AND rel_path = ? AND status IN ('queued', 'failed')")
        .bind(&share.id)
        .bind(rel)
        .execute(&state.db)
        .await?;
    let now = db::now();
    sqlx::query(
        "INSERT INTO transfers (id, peer_id, share_id, rel_path, sha256, size, vv_json, status, queued_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)",
    )
    .bind(crate::crypto::new_id("tr"))
    .bind(peer_id)
    .bind(&share.id)
    .bind(rel)
    .bind(sha256)
    .bind(size as i64)
    .bind(serde_json::to_string(vv)?)
    .bind(&now)
    .bind(&now)
    .execute(&state.db)
    .await?;
    state.sync_wake.notify_one();
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

async fn apply_event(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    root: &Root,
    e: &PeerEvent,
    batch: &mut Batch,
) -> AppResult<Applied> {
    let d = &e.data;
    let rel = d.path.as_str();
    if RelPath::parse(rel).is_err()
        || d.from
            .as_deref()
            .is_some_and(|f| RelPath::parse(f).is_err())
    {
        tracing::warn!(peer = %peer.name, "ignoring event with an invalid path");
        return Ok(Applied::Done);
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
            let local = register_local(state, share, root, rel).await?;
            let sha = d.sha256.clone().unwrap_or_default();
            match local {
                None => {
                    queue_transfer(
                        state,
                        &peer.id,
                        share,
                        rel,
                        &sha,
                        d.size.unwrap_or(0),
                        &d.vv,
                    )
                    .await?
                }
                Some(mut r) => match d.vv.compare(&r.vv) {
                    Causality::Equal | Causality::DominatedBy => {}
                    _ if !r.deleted && r.sha256.as_deref() == Some(sha.as_str()) => {
                        r.vv.merge(&d.vv);
                        put_row(state, &share.id, rel, &r).await?;
                    }
                    Causality::Dominates => {
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
                let p = abs.clone();
                tokio::task::spawn_blocking(move || fs::create_dir_all(p))
                    .await
                    .map_err(AppError::internal)?
                    .map_err(AppError::internal)?;
            }
        }
        "folder_trashed" => {
            if abs.is_dir() {
                batch.push(
                    share,
                    OperationAction::TrashFolder {
                        source: share.location(rel)?,
                    },
                    "folder_trash",
                    None,
                    Some(rel),
                    &d.vv,
                );
            }
        }
        other => tracing::warn!(peer = %peer.name, "ignoring unknown event kind '{other}'"),
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
        },
    )
    .await
}

/// Pulls and applies new events from one peer. Returns the number applied.
pub async fn pull(state: &AppState, peer: &net::Peer) -> Result<usize, String> {
    let Some(instance) = peer.instance_id.clone() else {
        return Ok(0);
    };
    let pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pending_peer_ops WHERE peer_id = ?")
            .bind(&peer.id)
            .fetch_one(&state.db)
            .await
            .map_err(|e| e.to_string())?;
    if pending > 0 {
        return Ok(0);
    }
    let page: EventPage = net::get_json(
        state,
        peer,
        &format!("/api/peer/v1/events?after={}", peer.cursor),
    )
    .await?;
    let shares = shares(state).await.map_err(|e| e.message().to_owned())?;
    let mut batch = Batch {
        actions: Vec::new(),
        pending: Vec::new(),
        paths: HashSet::new(),
    };
    let mut cursor = peer.cursor;
    let mut applied = 0;
    let mut stopped = false;
    let mut first_key: Option<String> = None;
    for e in &page.events {
        let known: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_log WHERE origin = ? AND origin_seq = ?",
        )
        .bind(&e.origin)
        .bind(e.origin_seq)
        .fetch_one(&state.db)
        .await
        .map_err(|e| e.to_string())?;
        if known > 0 || e.origin == state.instance_id {
            cursor = e.seq;
            continue;
        }
        let Some(share) = shares
            .iter()
            .find(|s| s.name == e.share && s.includes(&peer.id))
        else {
            cursor = e.seq;
            continue;
        };
        let root = match files::root(&state.db, &share.root_id).await {
            Ok(r) if r.available() => r,
            // Retry from this event once the root is back.
            _ => {
                stopped = true;
                break;
            }
        };
        if has_open_conflict(state, &share.id, &e.data.path)
            .await
            .unwrap_or(false)
        {
            // Newer remote state for a conflicting path refreshes the conflict instead.
            let deleted = e.kind == "file_deleted";
            if e.kind.starts_with("file_") {
                let _ = conflict(
                    state,
                    ConflictInput {
                        share,
                        rel: &e.data.path,
                        kind: "changed_on_both",
                        detail: format!("{} changed {} again", peer.name, e.data.path),
                        remote: remote_side(&peer.name, &e.data, deleted),
                        remote_vv: &e.data.vv,
                        peer_id: &peer.id,
                    },
                )
                .await;
            }
            let _ = log_remote(state, e).await;
            cursor = e.seq;
            continue;
        }
        let before = batch.actions.len();
        match apply_event(state, peer, share, &root, e, &mut batch).await {
            Ok(Applied::Done) => {
                if batch.actions.len() > before && first_key.is_none() {
                    first_key = Some(format!("{}:{}", e.origin, e.origin_seq));
                }
                log_remote(state, e)
                    .await
                    .map_err(|e| e.message().to_owned())?;
                cursor = e.seq;
                applied += 1;
            }
            Ok(Applied::Stop) => {
                stopped = true;
                break;
            }
            Err(err) => {
                tracing::warn!(peer = %peer.name, "applying {} {}: {}", e.kind, e.data.path, err.message());
                stopped = true;
                break;
            }
        }
    }
    if !stopped {
        cursor = cursor.max(page.next);
    }
    if !batch.actions.is_empty() {
        let principal = Principal::Peer {
            peer_id: peer.id.clone(),
        };
        let req = OperationRequest {
            idempotency_key: format!("peer:{}:{}", instance, first_key.unwrap_or_default()),
            label: Some(format!("Changes from {}", peer.name)),
            actions: batch.actions,
            allow_owner_transfer: false,
        };
        let accepted = crate::ops::insert(state, &principal, &req)
            .await
            .map_err(|e| e.message().to_owned())?;
        for (idx, (share_id, kind, from, to, vv)) in batch.pending.into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&accepted.operation_id)
            .bind(idx as i64)
            .bind(share_id)
            .bind(kind)
            .bind(from)
            .bind(to)
            .bind(serde_json::to_string(&vv).unwrap_or_default())
            .bind(&peer.id)
            .execute(&state.db)
            .await
            .map_err(|e| e.to_string())?;
        }
    }
    sqlx::query("UPDATE peers SET cursor = ?, events_applied = events_applied + ? WHERE id = ?")
        .bind(cursor)
        .bind(applied as i64)
        .bind(&peer.id)
        .execute(&state.db)
        .await
        .map_err(|e| e.to_string())?;
    if applied > 0 {
        state.emit(ServerEvent::SyncChanged);
    }
    Ok(applied)
}

/// Finishes the replica bookkeeping of an operation that applied peer events or resolved a
/// conflict. Returns false when the operation was a plain local one.
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
    for (idx, share_id, kind, from, to, vv_json, peer_id, conflict_id) in rows {
        let Some(share) = shares.iter().find(|s| s.id == share_id) else {
            continue;
        };
        let vv: VersionVector = serde_json::from_str(&vv_json).unwrap_or_default();
        let (status, code, message): (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT status, error_code, error_message FROM operation_items WHERE operation_id = ? AND idx = ?",
        )
        .bind(operation_id)
        .bind(idx)
        .fetch_one(&state.db)
        .await?;
        let ok = status == "done";
        let to = to.unwrap_or_default();
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
            }
            ("folder_trash", true) => {
                sqlx::query("UPDATE replica_files SET deleted = 1 WHERE share_id = ? AND substr(rel_path, 1, ?) = ?")
                    .bind(&share.id)
                    .bind(to.len() as i64 + 1)
                    .bind(format!("{to}/"))
                    .execute(&state.db)
                    .await?;
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
            (_, true) => {}
            (_, false) => {
                let detail = format!(
                    "applying a change from a peer failed: {}",
                    message.unwrap_or_else(|| code.clone().unwrap_or_default())
                );
                let path = if to.is_empty() {
                    from.clone().unwrap_or_default()
                } else {
                    to.clone()
                };
                conflict(
                    state,
                    ConflictInput {
                        share,
                        rel: &path,
                        kind: "apply_failed",
                        detail,
                        remote: ConflictSide {
                            instance: peer_id.clone(),
                            sha256: None,
                            size: None,
                            modified_at: None,
                            deleted: kind == "delete" || kind == "folder_trash",
                        },
                        remote_vv: &vv,
                        peer_id: &peer_id,
                    },
                )
                .await?;
            }
        }
    }
    sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id = ?")
        .bind(operation_id)
        .execute(&state.db)
        .await?;
    Ok(true)
}

// ---------------------------------------------------------------------------------------------
// Workers

/// Scans shares and pulls peer events. Skips scanning while operations run so half-done moves
/// are never mistaken for deletes.
pub async fn run(state: AppState) {
    let tick = std::time::Duration::from_secs(state.config.tick_secs.max(1));
    let scan_every = std::time::Duration::from_secs(state.config.scan_interval_secs.max(5));
    let mut last_scan = std::time::Instant::now() - scan_every;
    loop {
        let peers = net::peers(&state).await.unwrap_or_default();
        for peer in &peers {
            let hello: Result<net::Hello, String> =
                net::get_json(&state, peer, "/api/peer/v1/hello").await;
            match hello {
                Ok(h) if peer.instance_id.as_deref() == Some(h.instance_id.as_str()) => {
                    net::mark(&state, &peer.id, Ok(())).await;
                    if let Err(e) = pull(&state, peer).await {
                        net::mark(&state, &peer.id, Err(e)).await;
                    }
                }
                Ok(_) => {
                    net::mark(
                        &state,
                        &peer.id,
                        Err("the peer answered with another instance id".into()),
                    )
                    .await
                }
                Err(e) => net::mark(&state, &peer.id, Err(e)).await,
            }
        }
        if last_scan.elapsed() >= scan_every {
            last_scan = std::time::Instant::now();
            let running: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM operations WHERE status IN ('running', 'waiting_maintenance')")
                .fetch_one(&state.db)
                .await
                .unwrap_or(1);
            if running == 0 {
                for share in shares(&state)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|s| s.enabled)
                {
                    if let Err(e) = scan_share(&state, share).await {
                        tracing::debug!(share = %share.name, "scan skipped: {}", e.message());
                    }
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(tick) => {}
            () = state.sync_wake.notified() => {}
        }
    }
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
