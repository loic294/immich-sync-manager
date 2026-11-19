//! Copies file contents from peers during allowed hours (or under a manual override).

use std::time::{Duration, UNIX_EPOCH};

use futures_util::StreamExt;
use ism_core::dto::{
    CreateUpload, ExistingOriginal, FileRole, Location, OnConflict, OperationAction,
    OperationRequest, UploadFileSpec,
};
use ism_core::schedule::Schedule;
use ism_core::sidecar::{CompanionRole, companion_role, companions_of, is_companion_name};
use ism_core::version::{Causality, VersionVector};
use reqwest::Method;
use tokio::io::AsyncWriteExt;

use super::{ConflictInput, Row, Share, net};
use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files::{self, STAGING_DIR};
use crate::state::{AppState, ServerEvent};

const MAX_ATTEMPTS: i64 = 8;
const PROGRESS_EVERY: u64 = 4 * 1024 * 1024;

fn existing_original_for_companion(
    folder: &std::path::Path,
    name: &str,
    companion_extensions: &[String],
) -> AppResult<ExistingOriginal> {
    let mut names = std::fs::read_dir(folder)
        .map_err(AppError::internal)?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_file()))
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect::<Vec<_>>();
    if !names.iter().any(|candidate| candidate == name) {
        names.push(name.to_owned());
    }
    let owners = names
        .iter()
        .filter(|candidate| !is_companion_name(candidate, companion_extensions))
        .filter(|candidate| {
            companions_of(candidate, &names, companion_extensions)
                .iter()
                .any(|companion| companion.name == name)
        })
        .collect::<Vec<_>>();
    let [original] = owners.as_slice() else {
        return Err(if owners.is_empty() {
            AppError::conflict(
                "companion_original_missing",
                format!("no original matches companion '{name}'"),
            )
        } else {
            AppError::conflict(
                "companion_original_ambiguous",
                format!("companion '{name}' belongs to multiple originals"),
            )
        });
    };
    let Some((expected_revision, _)) = files::replacement_fingerprint(&folder.join(original))?
    else {
        return Err(AppError::conflict(
            "companion_original_missing",
            format!("original '{original}' disappeared"),
        ));
    };
    Ok(ExistingOriginal {
        name: (*original).clone(),
        expected_revision,
    })
}

fn peer_upload_file(
    folder: &std::path::Path,
    name: &str,
    companion_extensions: &[String],
) -> AppResult<(FileRole, Option<ExistingOriginal>)> {
    match companion_role(name, companion_extensions) {
        Some(CompanionRole::Sidecar) => Ok((
            FileRole::Sidecar,
            Some(existing_original_for_companion(
                folder,
                name,
                companion_extensions,
            )?),
        )),
        Some(CompanionRole::Companion) => Ok((
            FileRole::Companion,
            Some(existing_original_for_companion(
                folder,
                name,
                companion_extensions,
            )?),
        )),
        None => Ok((FileRole::Original, None)),
    }
}

/// Interrupted downloads go back to the queue and resume from their staging file. A finished
/// download is handed to a managed operation in the same transaction that removes its
/// transfer, so the executor's own recovery covers publication.
pub async fn recover_after_restart(state: &AppState) -> AppResult<()> {
    sqlx::query("UPDATE transfers SET status = 'queued', error = NULL, updated_at = ? WHERE status = 'running'")
        .bind(db::now())
        .execute(&state.db)
        .await?;
    Ok(())
}

pub fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Whether file contents may move now for this share and peer.
pub async fn allowed(
    state: &AppState,
    share_id: &str,
    peer_id: &str,
) -> AppResult<(bool, Option<String>)> {
    let now = chrono::Utc::now();
    let ov: Option<String> = sqlx::query_scalar(
        "SELECT id FROM overrides WHERE revoked_at IS NULL AND until > ?
         AND (share_id IS NULL OR share_id = ?) AND (peer_id IS NULL OR peer_id = ?) ORDER BY until DESC LIMIT 1",
    )
    .bind(db::ts(now))
    .bind(share_id)
    .bind(peer_id)
    .fetch_optional(&state.db)
    .await?;
    if ov.is_some() {
        return Ok((true, ov));
    }
    let view = super::admin::schedule(state).await?;
    let schedule = Schedule::new(&view.timezone, &view.windows)
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok((schedule.is_allowed(now), None))
}

#[derive(Debug, Clone)]
struct Transfer {
    id: String,
    peer_id: String,
    share_id: String,
    rel: String,
    sha256: String,
    size: i64,
    vv: VersionVector,
}

async fn next(state: &AppState) -> AppResult<Option<(Transfer, Option<String>)>> {
    type R = (
        String,
        String,
        String,
        String,
        String,
        i64,
        String,
        i64,
        String,
    );
    let rows: Vec<R> = sqlx::query_as(
        "SELECT t.id, t.peer_id, t.share_id, t.rel_path, t.sha256, t.size, t.vv_json, t.attempts, t.updated_at
         FROM transfers t JOIN peers p ON p.id = t.peer_id
         WHERE t.status = 'queued' AND p.status = 'online' ORDER BY t.queued_at, t.id LIMIT 200",
    )
    .fetch_all(&state.db)
    .await?;
    let now = chrono::Utc::now();
    for (id, peer_id, share_id, rel, sha256, size, vv, attempts, updated) in rows {
        // Back off after failures: one minute per attempt.
        if attempts > 0
            && db::parse_ts(&updated).is_some_and(|u| now - u < chrono::Duration::minutes(attempts))
        {
            continue;
        }
        let (ok, ov) = allowed(state, &share_id, &peer_id).await?;
        if ok {
            return Ok(Some((
                Transfer {
                    id,
                    peer_id,
                    share_id,
                    rel,
                    sha256,
                    size,
                    vv: serde_json::from_str(&vv)?,
                },
                ov,
            )));
        }
    }
    Ok(None)
}

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        match next(&state).await {
            Ok(Some((t, ov))) => {
                match claim(&state, &t.id, ov.as_deref()).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::error!(transfer = %t.id, "claim transfer: {}", e.message());
                        tokio::time::sleep(tick).await;
                        continue;
                    }
                }
                state.emit(ServerEvent::SyncChanged);
                let result = run_one(&state, &t).await;
                let deferred = matches!(result, Ok(Outcome::Deferred));
                let now = db::now();
                let updated = match result {
                    Ok(Outcome::Done) => {
                        sqlx::query("DELETE FROM transfers WHERE id = ? AND status = 'running'")
                            .bind(&t.id)
                            .execute(&state.db)
                            .await
                    }
                    Ok(Outcome::Dropped) => {
                        sqlx::query("DELETE FROM transfers WHERE id = ? AND status = 'running'")
                            .bind(&t.id)
                            .execute(&state.db)
                            .await
                    }
                    Ok(Outcome::Blocked(reason)) => {
                        sqlx::query(
                            "UPDATE transfers SET status = 'failed', error = ?, updated_at = ? WHERE id = ? AND status = 'running'",
                        )
                        .bind(reason)
                        .bind(&now)
                        .bind(&t.id)
                        .execute(&state.db)
                        .await
                    }
                    Ok(Outcome::Deferred) => {
                        sqlx::query(
                            "UPDATE transfers SET status = 'queued', updated_at = ? WHERE id = ? AND status = 'running'",
                        )
                        .bind(&now)
                        .bind(&t.id)
                        .execute(&state.db)
                        .await
                    }
                    Err(e) => {
                        tracing::warn!(transfer = %t.id, "transfer failed: {e}");
                        sqlx::query(
                            "UPDATE transfers SET attempts = attempts + 1, error = ?, updated_at = ?,
                             status = CASE WHEN status = 'cancelled' THEN 'cancelled'
                                           WHEN attempts + 1 >= ? THEN 'failed' ELSE 'queued' END
                             WHERE id = ?",
                        )
                        .bind(&e)
                        .bind(&now)
                        .bind(MAX_ATTEMPTS)
                        .bind(&t.id)
                        .execute(&state.db)
                        .await
                    }
                };
                if let Err(e) = updated {
                    tracing::error!(transfer = %t.id, "record transfer outcome: {e}");
                }
                state.emit(ServerEvent::SyncChanged);
                if deferred {
                    tokio::time::sleep(tick).await;
                }
            }
            Ok(None) => {
                tokio::select! {
                    () = tokio::time::sleep(tick * 6) => {}
                    () = state.sync_wake.notified() => {}
                }
            }
            Err(e) => {
                tracing::error!("transfer queue: {}", e.message());
                tokio::time::sleep(tick * 6).await;
            }
        }
    }
}

pub(super) async fn claim(
    state: &AppState,
    id: &str,
    override_id: Option<&str>,
) -> AppResult<bool> {
    let updated = sqlx::query(
        "UPDATE transfers SET status = 'running', override_id = ?, updated_at = ?
         WHERE id = ? AND status = 'queued'",
    )
    .bind(override_id)
    .bind(db::now())
    .bind(id)
    .execute(&state.db)
    .await?;
    Ok(updated.rows_affected() == 1)
}

enum Outcome {
    Done,
    /// The remote version is gone or superseded; a newer event will follow.
    Dropped,
    Blocked(&'static str),
    Deferred,
}

async fn run_one(state: &AppState, t: &Transfer) -> Result<Outcome, String> {
    // Retain legacy queue entries for inspection, but never adopt or publish their content.
    if super::excluded_path(&t.rel) {
        return Ok(Outcome::Blocked(
            "Synology metadata is excluded from peer replication",
        ));
    }
    let _operation_guard = state.replication_operation_lock.lock().await;
    if let Err(e) = super::require_stable_operations(state).await {
        return if e.code() == "recovery_required" {
            Ok(Outcome::Deferred)
        } else {
            Err(e.message().to_owned())
        };
    }
    let m = |e: AppError| e.message().to_owned();
    let peer = net::peer(state, &t.peer_id).await.map_err(m)?;
    let shares = super::shares(state).await.map_err(m)?;
    let Some(share) = shares.into_iter().find(|s| s.id == t.share_id) else {
        return Ok(Outcome::Dropped);
    };
    if !share.includes(&peer.id) {
        return Ok(Outcome::Dropped);
    }
    let root = files::root(&state.db, &share.root_id).await.map_err(m)?;
    root.ensure_available().map_err(m)?;
    let target_rel = share.root_rel(&t.rel).map_err(m)?;
    let target = root.abs(&target_rel).map_err(m)?;

    // Adopt an identical local copy without downloading.
    let fingerprint_path = target.clone();
    let fingerprint =
        tokio::task::spawn_blocking(move || files::replacement_fingerprint(&fingerprint_path))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
    if fingerprint
        .as_ref()
        .is_some_and(|(_, hash)| hash == &t.sha256)
    {
        let meta = std::fs::metadata(&target).map_err(|e| e.to_string())?;
        if i64::try_from(meta.len()).ok() != Some(t.size) {
            return Ok(Outcome::Blocked(
                "matching peer file checksums have different sizes",
            ));
        }
        if fingerprint.as_ref().map(|(revision, _)| revision.as_str())
            != Some(files::revision(&meta).as_str())
        {
            return Err("target changed while verifying the local copy".into());
        }
        let mut tx = state
            .db
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| e.to_string())?;
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
                .bind(&t.id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
        if status.as_deref() != Some("running") {
            tx.commit().await.map_err(|e| e.to_string())?;
            return Ok(Outcome::Dropped);
        }
        let conflict: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM conflicts WHERE share_id = ? AND rel_path = ? AND status = 'open')",
        )
        .bind(&share.id)
        .bind(&t.rel)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        if conflict {
            tx.commit().await.map_err(|e| e.to_string())?;
            return Ok(Outcome::Blocked("the file has an unresolved conflict"));
        }
        let mut r = super::row_in(&mut tx, &share.id, &t.rel)
            .await
            .map_err(m)?
            .unwrap_or(Row {
                size: 0,
                mtime_ns: 0,
                sha256: None,
                deleted: false,
                vv: VersionVector::default(),
            });
        r.vv.merge(&t.vv);
        r.size = meta.len() as i64;
        r.mtime_ns = files::mtime_ns(&meta);
        r.sha256 = Some(t.sha256.clone());
        r.deleted = false;
        super::put_row_in(&mut tx, &share.id, &t.rel, &r)
            .await
            .map_err(m)?;
        archive_transfer(&mut tx, &t.id, None).await.map_err(m)?;
        sqlx::query("DELETE FROM transfers WHERE id = ? AND status = 'running'")
            .bind(&t.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        return Ok(Outcome::Done);
    }

    let staging_dir = root.base().join(STAGING_DIR).join("peer");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|e| e.to_string())?;
    let staging = staging_dir.join(&t.id);
    let offset = tokio::fs::metadata(&staging)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let path = format!(
        "/api/peer/v1/content?share={}&path={}&sha256={}",
        encode(&share.name),
        encode(&t.rel),
        t.sha256
    );
    let mut req = net::request(state, &peer.url, &peer.secret, Method::GET, &path, None)
        .timeout(Duration::from_secs(24 * 3600));
    if offset > 0 && (offset as i64) < t.size {
        req = req.header("range", format!("bytes={offset}-"));
    }
    let resp = req.send().await.map_err(|e| net::net_error(&e))?;
    let status = resp.status().as_u16();
    if status == 404 || status == 409 || status == 410 {
        let _ = tokio::fs::remove_file(&staging).await;
        return Ok(Outcome::Dropped);
    }
    if status != 200 && status != 206 {
        return Err(format!("HTTP {status} from {}", peer.name));
    }
    let mtime_ns: Option<i64> = resp
        .headers()
        .get("x-ism-mtime-ns")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(status == 206)
        .write(true)
        .truncate(status == 200)
        .open(&staging)
        .await
        .map_err(|e| e.to_string())?;
    let mut done = if status == 206 { offset } else { 0 };
    let mut reported = done;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| net::net_error(&e))?;
        file.write_all(&chunk).await.map_err(|e| e.to_string())?;
        done += chunk.len() as u64;
        if done - reported >= PROGRESS_EVERY {
            reported = done;
            let cancelled: Option<String> =
                sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
                    .bind(&t.id)
                    .fetch_optional(&state.db)
                    .await
                    .ok()
                    .flatten();
            if cancelled.as_deref() != Some("running") {
                let _ = tokio::fs::remove_file(&staging).await;
                return Ok(Outcome::Dropped);
            }
            let _ = sqlx::query("UPDATE transfers SET bytes_done = ?, updated_at = ? WHERE id = ?")
                .bind(done as i64)
                .bind(db::now())
                .bind(&t.id)
                .execute(&state.db)
                .await;
            state.emit(ServerEvent::SyncChanged);
        }
    }
    file.sync_all().await.map_err(|e| e.to_string())?;
    drop(file);
    let check = staging.clone();
    let hash = tokio::task::spawn_blocking(move || files::sha256_file(&check))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    if hash != t.sha256 {
        let _ = tokio::fs::remove_file(&staging).await;
        return Err("downloaded data does not match its checksum".into());
    }

    // Publish only if nothing changed here since the transfer was queued.
    let local = super::row(state, &share.id, &t.rel).await.map_err(m)?;
    let disk = std::fs::metadata(&target).ok();
    let consistent = match (&local, &disk) {
        (None, None) => true,
        (Some(r), None) => r.deleted,
        (Some(r), Some(meta)) => {
            !r.deleted && r.size == meta.len() as i64 && r.mtime_ns == files::mtime_ns(meta)
        }
        (None, Some(_)) => false,
    };
    let newer = local
        .as_ref()
        .is_none_or(|r| matches!(t.vv.compare(&r.vv), Causality::Dominates | Causality::Equal));
    if !consistent
        || !newer
        || super::has_open_conflict(state, &share.id, &t.rel)
            .await
            .map_err(m)?
    {
        let _ = tokio::fs::remove_file(&staging).await;
        conflict_here(state, &peer, &share, t).await.map_err(m)?;
        return Ok(Outcome::Dropped);
    }
    if let Some(ns) = mtime_ns {
        let src = staging.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let when = UNIX_EPOCH + Duration::from_nanos(ns.max(0) as u64);
            std::fs::File::options()
                .write(true)
                .open(&src)?
                .set_modified(when)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    }
    publish(state, &peer, &share, &root, t, &staging, &target_rel)
        .await
        .map_err(m)?;
    Ok(Outcome::Done)
}

/// Publishes a verified download through a managed upload operation: it runs under the
/// Immich write barrier, registers self-writes, keeps the replaced version in ISM trash and
/// requests a library scan. The replica row is updated when the operation finishes.
async fn publish(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    root: &files::Root,
    t: &Transfer,
    staging: &std::path::Path,
    target_rel: &ism_core::paths::RelPath,
) -> AppResult<()> {
    let name = target_rel
        .name()
        .ok_or_else(|| AppError::invalid_location("peer file has no name"))?
        .to_owned();
    let folder = target_rel
        .parent()
        .unwrap_or_else(ism_core::paths::RelPath::root);
    let folder_path = root.abs(&folder)?;
    let target = root.abs(target_rel)?;
    let current = {
        let path = target.clone();
        tokio::task::spawn_blocking(move || files::replacement_fingerprint(&path))
            .await
            .map_err(AppError::internal)??
    };
    let companion_extensions = files::companion_extensions(&state.db).await;
    let (role, existing_original) = peer_upload_file(&folder_path, &name, &companion_extensions)?;
    let upload_id = crate::crypto::new_id("upl");
    let principal = Principal::Peer {
        peer_id: peer.id.clone(),
    };
    let request = CreateUpload {
        idempotency_key: Some(format!("peer-transfer:{}", t.id)),
        target: Location {
            root_id: root.mapping.root_id.clone(),
            path: folder.as_str().to_owned(),
        },
        provenance: None,
        files: vec![UploadFileSpec {
            name,
            role,
            size: t.size as u64,
            sha256: t.sha256.clone(),
            expected_revision: current.as_ref().map(|(revision, _)| revision.clone()),
            expected_sha256: current.as_ref().map(|(_, sha)| sha.clone()),
        }],
        on_conflict: if existing_original.is_some() || current.is_some() {
            OnConflict::ReplaceIfExpected
        } else {
            OnConflict::Fail
        },
        existing_original,
    };
    let dir = crate::ops::plan::staging_dir(root, &upload_id)?;
    std::fs::create_dir_all(&dir)?;
    std::fs::rename(staging, dir.join("0"))?;
    let op = OperationRequest {
        idempotency_key: format!("upload:{upload_id}"),
        label: Some(format!("{} from {}", t.rel, peer.name)),
        actions: vec![OperationAction::PublishUpload {
            upload_id: upload_id.clone(),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let running: Option<String> = sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
        .bind(&t.id)
        .fetch_optional(&mut *tx)
        .await?;
    if running.as_deref() != Some("running") {
        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
        return Ok(());
    }
    sqlx::query("INSERT INTO uploads (id, principal, request_json, status, created_at, idempotency_key) VALUES (?, ?, ?, 'receiving', ?, ?)")
        .bind(&upload_id)
        .bind(principal.key())
        .bind(serde_json::to_string(&request)?)
        .bind(db::now())
        .bind(&request.idempotency_key)
        .execute(&mut *tx)
        .await?;
    let accepted = crate::ops::insert_in(&mut tx, &principal, &op, Some(&upload_id)).await?;
    sqlx::query(
        "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id)
         VALUES (?, 0, ?, 'publish', NULL, ?, ?, ?)",
    )
    .bind(&accepted.operation_id)
    .bind(&share.id)
    .bind(&t.rel)
    .bind(serde_json::to_string(&t.vv)?)
    .bind(&peer.id)
    .execute(&mut *tx)
    .await?;
    archive_transfer(&mut tx, &t.id, Some(&accepted.operation_id)).await?;
    sqlx::query("DELETE FROM transfers WHERE id = ?")
        .bind(&t.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    crate::ops::announce_queued(state, &accepted);
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn archive_transfer(
    conn: &mut sqlx::SqliteConnection,
    id: &str,
    operation_id: Option<&str>,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO transfer_history (id, peer_id, share_id, rel_path, sha256, size, queued_at, finished_at, operation_id)
         SELECT id, peer_id, share_id, rel_path, sha256, size, queued_at, ?, ? FROM transfers WHERE id = ?",
    )
    .bind(db::now()).bind(operation_id).bind(id).execute(conn).await?;
    Ok(())
}

async fn conflict_here(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    t: &Transfer,
) -> AppResult<()> {
    super::conflict(
        state,
        ConflictInput {
            share,
            rel: &t.rel,
            kind: "changed_on_both",
            detail: format!(
                "{} changed here while a newer version from {} was downloading",
                t.rel, peer.name
            ),
            remote: ism_core::dto::ConflictSide {
                instance: peer.name.clone(),
                sha256: Some(t.sha256.clone()),
                size: Some(t.size as u64),
                modified_at: None,
                deleted: false,
            },
            remote_vv: &t.vv,
            peer_id: &peer.id,
            from: None,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{existing_original_for_companion, peer_upload_file};
    use crate::ops::plan::upload_file_needs_sync_add;
    use ism_core::dto::FileRole;
    use ism_core::sidecar::DEFAULT_COMPANION_EXTENSIONS;

    #[test]
    fn classifies_peer_xmp_as_attached_to_existing_original() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("retry-move.png"), b"original").unwrap();
        std::fs::write(dir.path().join("retry-move.xmp"), b"metadata").unwrap();
        let extensions = DEFAULT_COMPANION_EXTENSIONS
            .iter()
            .map(|extension| extension.to_string())
            .collect::<Vec<_>>();

        let existing =
            existing_original_for_companion(dir.path(), "retry-move.xmp", &extensions).unwrap();
        assert_eq!(existing.name, "retry-move.png");
        assert!(!existing.expected_revision.is_empty());

        let (role, existing_original) =
            peer_upload_file(dir.path(), "retry-move.xmp", &extensions).unwrap();
        assert_eq!(role, FileRole::Sidecar);
        assert!(existing_original.is_some());
        assert!(!upload_file_needs_sync_add(role));

        let (role, existing_original) =
            peer_upload_file(dir.path(), "new-photo.png", &extensions).unwrap();
        assert_eq!(role, FileRole::Original);
        assert!(existing_original.is_none());
        assert!(upload_file_needs_sync_add(role));
    }

    #[test]
    fn refuses_ambiguous_shared_peer_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("retry.jpg"), b"jpg").unwrap();
        std::fs::write(dir.path().join("retry.png"), b"png").unwrap();
        std::fs::write(dir.path().join("retry.xmp"), b"metadata").unwrap();
        let extensions = DEFAULT_COMPANION_EXTENSIONS
            .iter()
            .map(|extension| extension.to_string())
            .collect::<Vec<_>>();

        let error =
            existing_original_for_companion(dir.path(), "retry.xmp", &extensions).unwrap_err();
        assert_eq!(error.code(), "companion_original_ambiguous");
    }
}
