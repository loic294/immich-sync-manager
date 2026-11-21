//! Copies file contents from peers during allowed hours (or under a manual override).

use std::time::{Duration, UNIX_EPOCH};

use futures_util::StreamExt;
use ism_core::dto::{
    CreateUpload, ExistingOriginal, FileRole, Location, OnConflict, OperationAction,
    OperationRequest, PlannedFile, UploadFileSpec,
};
use ism_core::paths::RelPath;
use ism_core::schedule::Schedule;
use ism_core::sidecar::{
    CompanionRole, companion_role, companions_of, is_companion_name, preferred_paired_original,
};
use ism_core::version::{Causality, VersionVector};
use reqwest::Method;
use tokio::io::{AsyncWriteExt, BufWriter};

use super::{ConflictInput, Row, Share, net};
use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files::{self, STAGING_DIR};
use crate::state::{AppState, ServerEvent};

const MAX_ATTEMPTS: i64 = 8;
const PROGRESS_EVERY: u64 = 4 * 1024 * 1024;
const STAGING_WRITE_BUFFER: usize = 256 * 1024;
pub(crate) const DOWNLOAD_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

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
    let selected = preferred_paired_original(
        &owners
            .iter()
            .map(|original| original.as_str())
            .collect::<Vec<_>>(),
    );
    let Some(original) = selected else {
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
        name: original.to_owned(),
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
    sqlx::query(
        "UPDATE transfers
         SET status = 'queued', attempts = 0, error = NULL, updated_at = ?
         WHERE status = 'running'
            OR (status = 'failed' AND (
                 error GLOB 'HTTP 504 from *'
                 OR error LIKE 'companion % belongs to multiple originals'
            ))",
    )
    .bind(db::now())
    .execute(&state.db)
    .await?;
    Ok(())
}

fn recovery_check_failed(message: impl Into<String>) -> AppError {
    AppError::conflict("recovery_recheck_failed", message.into())
}

/// Rechecks a peer upload whose file checks finished but replica/event bookkeeping did not.
///
/// This never moves, replaces or deletes a library file. It only verifies the final content and
/// finishes the persisted peer bookkeeping when every expected file still matches exactly.
pub async fn recheck_publish_recovery(state: &AppState, operation_id: &str) -> AppResult<()> {
    let _operation_guard = state.replication_operation_lock.try_lock().map_err(|_| {
        AppError::conflict(
            "recovery_busy",
            "peer replication is busy finishing another scan or operation; retry in a moment",
        )
    })?;
    let operation: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT status, admitted_server_ids_json, recovery_resolved
         FROM operations WHERE id = ?",
    )
    .bind(operation_id)
    .fetch_optional(&state.db)
    .await?;
    let Some((status, admitted, _resolved)) = operation else {
        return Err(AppError::not_found("operation not found"));
    };
    if status != "recovery_required" {
        return Err(recovery_check_failed(
            "only recovery-required operations can be rechecked",
        ));
    }
    let admitted: Vec<String> = serde_json::from_str(&admitted)?;
    if !admitted.is_empty() {
        return Err(recovery_check_failed(
            "this recovery includes Immich maintenance work; inspect it manually instead",
        ));
    }

    let pending: Vec<(i64, String, String, String, String, String)> = sqlx::query_as(
        "SELECT idx, share_id, kind, to_path, vv_json, peer_id
         FROM pending_peer_ops WHERE operation_id = ? ORDER BY idx",
    )
    .bind(operation_id)
    .fetch_all(&state.db)
    .await?;
    let [(idx, share_id, kind, to_path, vv_json, _peer_id)] = pending.as_slice() else {
        return Err(recovery_check_failed(
            "this operation does not have exactly one interrupted peer publication to recheck",
        ));
    };
    if kind != "publish" {
        return Err(recovery_check_failed(
            "this operation is not an interrupted peer publication",
        ));
    }

    type ItemRecovery = (i64, String, String, Option<String>, Option<String>);
    let items: Vec<ItemRecovery> = sqlx::query_as(
        "SELECT idx, kind, status, error_code, progress_json
         FROM operation_items WHERE operation_id = ? ORDER BY idx",
    )
    .bind(operation_id)
    .fetch_all(&state.db)
    .await?;
    let [(item_idx, item_kind, item_status, error_code, progress)] = items.as_slice() else {
        return Err(recovery_check_failed(
            "this peer publication does not have exactly one checkable action",
        ));
    };
    if *idx != *item_idx || item_kind != "publish_upload" || item_status != "recovery_required" {
        return Err(recovery_check_failed(
            "the operation item is not the interrupted upload action",
        ));
    }
    if error_code
        .as_deref()
        .is_some_and(|code| code != "peer_bookkeeping_failed")
    {
        return Err(recovery_check_failed(format!(
            "the item failed for '{code}', which cannot be resolved by rechecking the file",
            code = error_code.as_deref().unwrap_or_default()
        )));
    }
    let Some(progress) = progress.as_deref() else {
        return Err(recovery_check_failed(
            "the operation did not retain its completed file checks",
        ));
    };
    let progress: serde_json::Value = serde_json::from_str(progress)?;
    let Some(steps) = progress["steps"].as_array() else {
        return Err(recovery_check_failed(
            "the operation did not retain its completed file checks",
        ));
    };
    if steps.len() < 2
        || steps[..steps.len() - 1]
            .iter()
            .any(|step| step["status"] != "done")
        || steps.last().and_then(|step| step["label"].as_str()) != Some("Record changes")
        || !matches!(
            steps.last().and_then(|step| step["status"].as_str()),
            Some("running" | "done")
        )
    {
        return Err(recovery_check_failed(
            "the saved checks did not finish before the interrupted bookkeeping step",
        ));
    }

    let upload: Option<(String, String, String)> =
        sqlx::query_as("SELECT id, request_json, status FROM uploads WHERE operation_id = ?")
            .bind(operation_id)
            .fetch_optional(&state.db)
            .await?;
    let Some((upload_id, request_json, _upload_status)) = upload else {
        return Err(recovery_check_failed(
            "the interrupted upload record is missing",
        ));
    };
    let request: CreateUpload = serde_json::from_str(&request_json)?;
    let [spec] = request.files.as_slice() else {
        return Err(recovery_check_failed(
            "this action published more than one file; inspect it manually",
        ));
    };
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == *share_id)
        .ok_or_else(|| recovery_check_failed("the peer share no longer exists"))?;
    if request.target.root_id != share.root_id {
        return Err(recovery_check_failed(
            "the upload root does not match the interrupted peer share",
        ));
    }
    let target_folder = RelPath::parse(&request.target.path)
        .map_err(|e| AppError::invalid_location(e.to_string()))?;
    let target_rel = target_folder
        .join(&spec.name)
        .map_err(|e| AppError::invalid_location(e.to_string()))?;
    if share.rel_of(&Location {
        root_id: request.target.root_id.clone(),
        path: target_rel.as_str().to_owned(),
    }) != Some(to_path.clone())
    {
        return Err(recovery_check_failed(
            "the final file path does not match the interrupted peer publication",
        ));
    }
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let path = root.abs(&target_rel)?;
    let checked_path = path.clone();
    let fingerprint =
        tokio::task::spawn_blocking(move || files::replacement_fingerprint(&checked_path))
            .await
            .map_err(AppError::internal)?
            .map_err(|e| {
                if matches!(e.kind(), std::io::ErrorKind::NotFound) {
                    recovery_check_failed("the expected destination file is missing")
                } else {
                    recovery_check_failed(format!("the destination file cannot be verified: {e}"))
                }
            })?;
    let Some((revision, sha256)) = fingerprint else {
        return Err(recovery_check_failed(
            "the expected destination file is missing",
        ));
    };
    let meta = std::fs::metadata(&path).map_err(AppError::internal)?;
    if !meta.is_file() || meta.len() != spec.size || !sha256.eq_ignore_ascii_case(&spec.sha256) {
        return Err(recovery_check_failed(
            "the destination file no longer matches the upload size and checksum",
        ));
    }

    let vv: VersionVector = serde_json::from_str(vv_json)?;
    let result = serde_json::json!({
        "asset_id": null,
        "files": [PlannedFile {
            from: None,
            to: Some(Location {
                root_id: request.target.root_id.clone(),
                path: target_rel.as_str().to_owned(),
            }),
            role: spec.role,
            copy: false,
        }],
        "warnings": [],
    });
    let mut completed_progress = progress;
    if let Some(last) = completed_progress
        .get_mut("steps")
        .and_then(|steps| steps.as_array_mut())
        .and_then(|steps| steps.last_mut())
    {
        last["status"] = "done".into();
    }

    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let unchanged: Option<String> =
        sqlx::query_scalar("SELECT status FROM operations WHERE id = ?")
            .bind(operation_id)
            .fetch_optional(&mut *tx)
            .await?;
    if unchanged.as_deref() != Some("recovery_required") {
        return Err(recovery_check_failed(
            "the operation changed while it was being rechecked",
        ));
    }
    let pending_now: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pending_peer_ops WHERE operation_id = ? AND idx = ? AND kind = 'publish' AND to_path = ?",
    )
    .bind(operation_id)
    .bind(idx)
    .bind(to_path)
    .fetch_one(&mut *tx)
    .await?;
    if pending_now != 1 {
        return Err(recovery_check_failed(
            "the interrupted peer bookkeeping changed while it was being rechecked",
        ));
    }
    if files::revision(&std::fs::metadata(&path).map_err(AppError::internal)?) != revision {
        return Err(recovery_check_failed(
            "the destination file changed while it was being rechecked",
        ));
    }
    let mut row = super::row_in(&mut tx, &share.id, to_path)
        .await?
        .unwrap_or(Row {
            size: 0,
            mtime_ns: 0,
            sha256: None,
            deleted: false,
            vv: VersionVector::default(),
        });
    row.vv.merge(&vv);
    row.size = meta.len() as i64;
    row.mtime_ns = files::mtime_ns(&meta);
    row.sha256 = Some(sha256);
    row.deleted = false;
    super::put_row_in(&mut tx, &share.id, to_path, &row).await?;
    sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id = ? AND idx = ?")
        .bind(operation_id)
        .bind(idx)
        .execute(&mut *tx)
        .await?;
    let updated_item = sqlx::query(
        "UPDATE operation_items
         SET status = 'done', error_code = NULL, error_message = NULL,
             result_json = ?, progress_json = ?
         WHERE operation_id = ? AND idx = ? AND status = 'recovery_required'",
    )
    .bind(result.to_string())
    .bind(completed_progress.to_string())
    .bind(operation_id)
    .bind(idx)
    .execute(&mut *tx)
    .await?;
    if updated_item.rows_affected() != 1 {
        return Err(recovery_check_failed(
            "the interrupted operation item changed while it was being rechecked",
        ));
    }
    let updated_upload =
        sqlx::query("UPDATE uploads SET status = 'committed' WHERE id = ? AND operation_id = ?")
            .bind(&upload_id)
            .bind(operation_id)
            .execute(&mut *tx)
            .await?;
    if updated_upload.rows_affected() != 1 {
        return Err(recovery_check_failed(
            "the upload record changed while it was being rechecked",
        ));
    }
    let updated_operation = sqlx::query(
        "UPDATE operations
         SET status = 'completed', recovery_resolved = 1, finished_at = COALESCE(finished_at, ?)
         WHERE id = ? AND status = 'recovery_required'",
    )
    .bind(db::now())
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    if updated_operation.rows_affected() != 1 {
        return Err(recovery_check_failed(
            "the operation changed while it was being rechecked",
        ));
    }
    tx.commit().await?;

    let (operation, _) = crate::ops::load(state, operation_id).await?;
    crate::ops::record(
        state,
        operation_id,
        "finished",
        &serde_json::to_value(operation)?,
    )
    .await?;
    state.sync_wake.notify_waiters();
    state.emit(ServerEvent::OperationUpdated {
        operation_id: operation_id.to_owned(),
    });
    state.emit(ServerEvent::SyncChanged);
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

fn parse_content_range(raw: &str) -> Option<(u64, u64, u64)> {
    let (range, total) = raw.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let parsed = (start.parse().ok()?, end.parse().ok()?, total.parse().ok()?);
    (parsed.0 <= parsed.1 && parsed.1 < parsed.2).then_some(parsed)
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

async fn original_pending(state: &AppState, share_id: &str, rel: &str) -> AppResult<bool> {
    let extensions = state.sidecar_extensions();
    if !is_companion_name(rel, &extensions) {
        return Ok(false);
    }
    let (stem, _) = ism_core::paths::split_ext(rel);
    let paths: Vec<String> = sqlx::query_scalar(
        "SELECT rel_path FROM transfers
         WHERE share_id = ?1 AND status IN ('queued','running')
           AND (rel_path = ?2 OR substr(rel_path, 1, length(?2) + 1) = ?2 || '.')
         UNION ALL
         SELECT h.rel_path FROM transfer_history h JOIN operations o ON o.id = h.operation_id
         WHERE h.share_id = ?1 AND o.status IN ('queued','waiting_maintenance','running')
           AND (h.rel_path = ?2 OR substr(h.rel_path, 1, length(?2) + 1) = ?2 || '.')",
    )
    .bind(share_id)
    .bind(stem)
    .fetch_all(&state.db)
    .await?;
    Ok(paths.iter().any(|path| {
        !is_companion_name(path, &extensions)
            && std::path::Path::new(path).parent() == std::path::Path::new(rel).parent()
            && !companions_of(path, &[rel.to_owned()], &extensions).is_empty()
    }))
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
    let now = chrono::Utc::now();
    let mut offset = 0_i64;
    loop {
        let rows: Vec<R> = sqlx::query_as(
            "SELECT t.id, t.peer_id, t.share_id, t.rel_path, t.sha256, t.size, t.vv_json, t.attempts, t.updated_at
             FROM transfers t JOIN peers p ON p.id = t.peer_id
             WHERE t.status = 'queued' AND p.status = 'online' ORDER BY t.queued_at, t.id LIMIT 200 OFFSET ?",
        )
        .bind(offset)
        .fetch_all(&state.db)
        .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        offset += rows.len() as i64;
        for (id, peer_id, share_id, rel, sha256, size, vv, attempts, updated) in rows {
            // Back off after failures: one minute per attempt.
            if attempts > 0
                && db::parse_ts(&updated)
                    .is_some_and(|u| now - u < chrono::Duration::minutes(attempts))
            {
                continue;
            }
            let (ok, ov) = allowed(state, &share_id, &peer_id).await?;
            if ok && !original_pending(state, &share_id, &rel).await? {
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
    }
}

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        if tasks.len() < state.config.peer_transfer_concurrency {
            match super::require_recovery_resolved(&state).await {
                Ok(()) => match next(&state).await {
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
                        let state = state.clone();
                        tasks.spawn(async move {
                            run_claimed(&state, &t).await;
                        });
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!("transfer queue: {}", e.message());
                    }
                },
                Err(e) if e.code() != "recovery_required" => {
                    tracing::error!("transfer recovery check: {}", e.message());
                }
                Err(_) => {}
            }
        }
        tokio::select! {
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(e) = result {
                    tracing::error!("transfer task: {e}");
                }
            }
            () = tokio::time::sleep(tick * 6) => {}
            () = state.sync_wake.notified() => {}
        }
    }
}

async fn run_claimed(state: &AppState, t: &Transfer) {
    let result = run_one(state, t).await;
    let deferred = matches!(result, Ok(Outcome::Deferred));
    let now = db::now();
    let updated = match result {
        Ok(Outcome::Done | Outcome::Dropped) => {
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
        tokio::time::sleep(Duration::from_secs(state.config.tick_secs.max(1))).await;
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
    if super::excluded_path(&t.rel, &state.junk_rules()) {
        return Ok(Outcome::Blocked(
            "NAS metadata and ignored names are excluded from peer replication",
        ));
    }
    let operation_guard = state.replication_operation_lock.lock().await;
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
    // Only staging I/O runs concurrently; adoption and publication remain serialized with
    // managed operations, scans and peer bookkeeping.
    drop(operation_guard);

    let staging_dir = root.base().join(STAGING_DIR).join("peer");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|e| e.to_string())?;
    let staging = staging_dir.join(&t.id);
    let mut offset = tokio::fs::metadata(&staging)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let expected_size = t.size as u64;
    if offset > expected_size {
        tokio::fs::remove_file(&staging)
            .await
            .map_err(|e| e.to_string())?;
        offset = 0;
    }
    let path = format!(
        "/api/peer/v1/content?share={}&path={}&sha256={}",
        encode(&share.name),
        encode(&t.rel),
        t.sha256
    );
    let mut file = BufWriter::with_capacity(
        STAGING_WRITE_BUFFER,
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(offset > 0)
            .write(true)
            .truncate(offset == 0)
            .open(&staging)
            .await
            .map_err(|e| e.to_string())?,
    );
    let mut done = offset;
    let mut reported = done;
    let mut last_report = tokio::time::Instant::now();
    let mut mtime_ns: Option<i64> = None;
    while done < expected_size {
        let end = (done + DOWNLOAD_CHUNK_SIZE - 1).min(expected_size - 1);
        let resp = net::request(state, &peer.url, &peer.secret, Method::GET, &path, None)
            .timeout(Duration::from_secs(24 * 3600))
            .header("range", format!("bytes={done}-{end}"))
            .header("if-range", format!("\"{}\"", t.sha256))
            .send()
            .await
            .map_err(|e| net::net_error(&e))?;
        let status = resp.status().as_u16();
        if status == 404 || status == 409 || status == 410 {
            drop(file);
            let _ = tokio::fs::remove_file(&staging).await;
            return Ok(Outcome::Dropped);
        }
        if status != 206 {
            return Err(format!("HTTP {status} from {}", peer.name));
        }
        let content_range = resp
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range)
            .ok_or_else(|| "peer returned an invalid Content-Range".to_owned())?;
        if content_range != (done, end, expected_size) {
            return Err(format!(
                "peer returned unexpected Content-Range {content_range:?}"
            ));
        }
        if let Some(current_mtime) = resp
            .headers()
            .get("x-ism-mtime-ns")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
        {
            if mtime_ns.is_some_and(|previous| previous != current_mtime) {
                return Err("peer file changed during the transfer".into());
            }
            mtime_ns = Some(current_mtime);
        }
        let expected_bytes = end - done + 1;
        let mut received = 0_u64;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| net::net_error(&e))?;
            if received + chunk.len() as u64 > expected_bytes {
                return Err("peer sent more data than the requested range".into());
            }
            file.write_all(&chunk).await.map_err(|e| e.to_string())?;
            received += chunk.len() as u64;
            done += chunk.len() as u64;
            if done - reported >= PROGRESS_EVERY || last_report.elapsed() >= Duration::from_secs(1)
            {
                if !super::activity::record_progress(
                    state,
                    &t.id,
                    &t.peer_id,
                    done,
                    done - reported,
                )
                .await
                .map_err(m)?
                {
                    drop(file);
                    let _ = tokio::fs::remove_file(&staging).await;
                    return Ok(Outcome::Dropped);
                }
                reported = done;
                last_report = tokio::time::Instant::now();
            }
        }
        if received != expected_bytes {
            return Err(format!(
                "peer returned {received} of {expected_bytes} requested bytes"
            ));
        }
    }
    if !super::activity::record_progress(state, &t.id, &t.peer_id, done, done - reported)
        .await
        .map_err(m)?
    {
        let _ = tokio::fs::remove_file(&staging).await;
        return Ok(Outcome::Dropped);
    }
    file.flush().await.map_err(|e| e.to_string())?;
    let file = file.into_inner();
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

    let _operation_guard = state.replication_operation_lock.lock().await;
    if let Err(e) = super::require_stable_operations(state).await {
        return if e.code() == "recovery_required" {
            Ok(Outcome::Deferred)
        } else {
            Err(e.message().to_owned())
        };
    }
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
        .bind(&t.id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| e.to_string())?;
    if status.as_deref() != Some("running") {
        return Ok(Outcome::Dropped);
    }
    let Some(current_share) = super::shares(state)
        .await
        .map_err(m)?
        .into_iter()
        .find(|s| s.id == t.share_id && s.includes(&peer.id))
    else {
        return Ok(Outcome::Dropped);
    };
    if current_share.root_id != share.root_id
        || current_share.root_rel(&t.rel).map_err(m)? != target_rel
    {
        return Err("share location changed while downloading".into());
    }
    let root = files::root(&state.db, &share.root_id).await.map_err(m)?;
    root.ensure_available().map_err(m)?;
    if root.abs(&target_rel).map_err(m)? != target {
        return Err("root location changed while downloading".into());
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
    let companion_extensions = state.sidecar_extensions();
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
        "INSERT INTO transfer_history (id, peer_id, share_id, rel_path, sha256, size, queued_at, finished_at, operation_id, run_id)
         SELECT id, peer_id, share_id, rel_path, sha256, size, queued_at, ?, ?, run_id FROM transfers WHERE id = ?",
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
    use ism_core::file_settings::DEFAULT_SIDECAR_EXTENSIONS;

    #[test]
    fn classifies_peer_xmp_as_attached_to_existing_original() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("retry-move.png"), b"original").unwrap();
        std::fs::write(dir.path().join("retry-move.xmp"), b"metadata").unwrap();
        let extensions = DEFAULT_SIDECAR_EXTENSIONS
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
        let extensions = DEFAULT_SIDECAR_EXTENSIONS
            .iter()
            .map(|extension| extension.to_string())
            .collect::<Vec<_>>();

        let error =
            existing_original_for_companion(dir.path(), "retry.xmp", &extensions).unwrap_err();
        assert_eq!(error.code(), "companion_original_ambiguous");
    }

    #[test]
    fn accepts_known_paired_originals_for_peer_sidecars() {
        let extensions = DEFAULT_SIDECAR_EXTENSIONS
            .iter()
            .map(|extension| extension.to_string())
            .collect::<Vec<_>>();
        for (originals, expected) in [
            (["retry.JPG", "retry.MOV"], "retry.JPG"),
            (["retry.jpg", "retry.CR3"], "retry.jpg"),
            (["retry.jpg", "retry.edited.1730000000.jpg"], "retry.jpg"),
            (["retry.jpg", "retry-edited-1730000000.jpg"], "retry.jpg"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            for original in originals {
                std::fs::write(dir.path().join(original), original).unwrap();
            }
            std::fs::write(dir.path().join("retry.xmp"), b"metadata").unwrap();

            let existing =
                existing_original_for_companion(dir.path(), "retry.xmp", &extensions).unwrap();
            assert_eq!(existing.name, expected);
        }
    }
}
