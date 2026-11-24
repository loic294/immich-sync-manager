//! Copies file contents from peers during allowed hours (or under a manual override).

use std::collections::HashMap;
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
) -> AppResult<Option<ExistingOriginal>> {
    let entries = match std::fs::read_dir(folder) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(AppError::internal(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(AppError::internal)?;
        if entry.file_type().map_err(AppError::internal)?.is_file()
            && let Ok(name) = entry.file_name().into_string()
        {
            names.push(name);
        }
    }
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
        return Ok(None);
    };
    let Some((expected_revision, _)) = files::replacement_fingerprint(&folder.join(original))?
    else {
        return Ok(None);
    };
    Ok(Some(ExistingOriginal {
        name: original.to_owned(),
        expected_revision,
    }))
}

pub(crate) fn retry_at(attempts: i64, updated: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if attempts <= 0 {
        return None;
    }
    db::parse_ts(updated)
        .and_then(|at| at.checked_add_signed(chrono::Duration::minutes(attempts)))
        .filter(|at| *at > chrono::Utc::now())
}

fn peer_upload_file(
    folder: &std::path::Path,
    name: &str,
    companion_extensions: &[String],
) -> AppResult<(FileRole, Option<ExistingOriginal>)> {
    match companion_role(name, companion_extensions) {
        Some(CompanionRole::Sidecar) => Ok((
            FileRole::Sidecar,
            existing_original_for_companion(folder, name, companion_extensions)?,
        )),
        Some(CompanionRole::Companion) => Ok((
            FileRole::Companion,
            existing_original_for_companion(folder, name, companion_extensions)?,
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
    let _operation_guard = state.replication_operation_lock.lock().await;
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
            "this publication requires group verification",
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
    let uploaded_rel = target_folder
        .join(&spec.name)
        .map_err(|e| AppError::invalid_location(e.to_string()))?;
    let vv: VersionVector = serde_json::from_str(vv_json)?;
    let uploaded_share_path = share
        .rel_of(&Location {
            root_id: request.target.root_id.clone(),
            path: uploaded_rel.as_str().to_owned(),
        })
        .ok_or_else(|| recovery_check_failed("upload destination is outside the share"))?;
    let current_path = super::recovery::current_path(state, &share, to_path, &vv).await?;
    if super::recovery::current_path(state, &share, &uploaded_share_path, &vv).await?
        != current_path
    {
        return Err(recovery_check_failed(
            "the final file path does not match the interrupted peer publication",
        ));
    }
    let target_rel = share.root_rel(&current_path)?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    if admitted
        .iter()
        .any(|server| root.server_id.as_ref() != Some(server))
    {
        return Err(recovery_check_failed(
            "this publication includes unverified Immich effects",
        ));
    }
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
    let asset_id =
        crate::ops::exec::verified_asset_at_root(state, &root, &target_rel, spec.role).await?;

    let result = serde_json::json!({
        "asset_id": asset_id,
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

    let _maintenance_guard = state.maintenance_lock.lock().await;
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
    let mut row = super::row_in(&mut tx, &share.id, &current_path)
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
    super::put_row_in(&mut tx, &share.id, &current_path, &row).await?;
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
    let resumed = crate::maintenance::resolve_operation_recovery_in(&mut tx, operation_id).await?;
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
    for server_id in resumed {
        state.emit(ServerEvent::MaintenanceChanged { server_id });
    }
    if !admitted.is_empty() {
        state.maintenance_wake.notify_waiters();
    }

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
    source_rel: Option<String>,
    source_vv: Option<VersionVector>,
    retained_token: Option<String>,
    restore_parent: Option<String>,
}

pub(crate) async fn original_pending(
    state: &AppState,
    share_id: &str,
    rel: &str,
) -> AppResult<bool> {
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

pub(crate) enum Eligibility {
    Ready(Option<String>),
    Waiting {
        reason: String,
        next_at: Option<String>,
        blocked: bool,
    },
}

pub(crate) async fn eligibility(
    state: &AppState,
    share: &super::Share,
    peer: &str,
    id: &str,
    rel: &str,
    attempts: i64,
    updated: &str,
) -> AppResult<Eligibility> {
    match super::inbox::require_stable_transfer(state, share, &[rel], id).await {
        Ok(()) => {}
        Err(e) if matches!(e.code(), "recovery_required" | "root_unavailable") => {
            return Ok(Eligibility::Waiting {
                reason: crate::error::redact(e.message()),
                next_at: None,
                blocked: true,
            });
        }
        Err(e) => return Err(e),
    }
    if let Some(at) = retry_at(attempts, updated) {
        return Ok(Eligibility::Waiting {
            reason: "Retry backoff after a failed transfer".into(),
            next_at: Some(db::ts(at)),
            blocked: false,
        });
    }
    let (ok, override_id) = allowed(state, &share.id, peer).await?;
    if !ok {
        let view = super::admin::schedule(state).await?;
        let schedule = Schedule::new(&view.timezone, &view.windows).map_err(AppError::internal)?;
        return Ok(Eligibility::Waiting {
            reason: "Waiting for allowed file-transfer hours".into(),
            next_at: schedule.next_allowed(chrono::Utc::now()).map(db::ts),
            blocked: false,
        });
    }
    if original_pending(state, &share.id, rel).await? {
        return Ok(Eligibility::Waiting {
            reason: "Waiting for the original file before transferring its companion".into(),
            next_at: None,
            blocked: false,
        });
    }
    Ok(Eligibility::Ready(override_id))
}

#[cfg(test)]
pub(super) struct EligibilityPause {
    pub reached: tokio::sync::oneshot::Sender<()>,
    pub resume: tokio::sync::oneshot::Receiver<()>,
}

async fn next(
    state: &AppState,
    #[cfg(test)] pause: &mut Option<EligibilityPause>,
) -> AppResult<Option<(Transfer, Option<String>)>> {
    let shares = super::shares(state).await?;
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
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let mut offset = 0_i64;
    loop {
        let rows: Vec<R> = sqlx::query_as(
            "SELECT t.id, t.peer_id, t.share_id, t.rel_path, t.sha256, t.size, t.vv_json, t.attempts, t.updated_at,
                    t.source_rel_path,t.source_vv_json,t.restore_parent_operation_id,t.source_retained_token
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
        for (
            id,
            peer_id,
            share_id,
            rel,
            sha256,
            size,
            vv,
            attempts,
            updated,
            source_rel,
            source_vv,
            restore_parent,
            retained_token,
        ) in rows
        {
            let Some(share) = shares.iter().find(|share| share.id == share_id) else {
                continue;
            };
            let eligible =
                eligibility(state, share, &peer_id, &id, &rel, attempts, &updated).await?;
            #[cfg(test)]
            if matches!(eligible, Eligibility::Waiting { blocked: true, .. })
                && let Some(pause) = pause.take()
            {
                let _ = pause.reached.send(());
                let _ = pause.resume.await;
            }
            if let Eligibility::Waiting {
                reason,
                next_at,
                blocked,
            } = &eligible
            {
                tracing::trace!(
                    transfer = %id,
                    reason = %reason,
                    next_at = ?next_at,
                    blocked = *blocked,
                    "transfer remains queued"
                );
            }
            if let Eligibility::Ready(ov) = eligible {
                return Ok(Some((
                    Transfer {
                        id,
                        peer_id,
                        share_id,
                        rel,
                        sha256,
                        size,
                        vv: serde_json::from_str(&vv)?,
                        source_rel,
                        source_vv: source_vv
                            .map(|raw| serde_json::from_str(&raw))
                            .transpose()?,
                        restore_parent,
                        retained_token,
                    },
                    ov,
                )));
            }
        }
    }
}

pub async fn run(state: AppState) {
    run_inner(
        state,
        #[cfg(test)]
        None,
    )
    .await;
}

#[cfg(test)]
pub(super) async fn run_paused(state: AppState, pause: EligibilityPause) {
    run_inner(state, Some(pause)).await;
}

/// One scheduler pass: the transfer it would claim next, if any.
#[cfg(test)]
pub(super) async fn next_for_test(
    state: &AppState,
    mut pause: Option<EligibilityPause>,
) -> AppResult<Option<String>> {
    Ok(next(state, &mut pause).await?.map(|(t, _)| t.id))
}

async fn run_inner(state: AppState, #[cfg(test)] mut pause: Option<EligibilityPause>) {
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    let mut tasks = tokio::task::JoinSet::new();
    let mut transfers = HashMap::new();
    loop {
        // A managed operation can finish while eligibility is being checked.
        let wake = state.sync_wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if tasks.len() < state.config.peer_transfer_concurrency {
            match next(
                &state,
                #[cfg(test)]
                &mut pause,
            )
            .await
            {
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
                    state.worker_evidence.transfer_started(&t.id);
                    state.emit(ServerEvent::SyncChanged);
                    let state = state.clone();
                    let id = t.id.clone();
                    let task = tasks.spawn(async move {
                        run_claimed(&state, &t).await;
                    });
                    transfers.insert(task.id(), id);
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("transfer queue: {}", e.message());
                }
            }
        }
        tokio::select! {
            Some(result) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                record_transfer_exit(&state, &mut transfers, result);
            }
            () = tokio::time::sleep(tick * 6) => {}
            () = &mut wake => {}
        }
    }
}

fn record_transfer_exit(
    state: &AppState,
    transfers: &mut HashMap<tokio::task::Id, String>,
    result: Result<(tokio::task::Id, ()), tokio::task::JoinError>,
) {
    match result {
        Ok((task, ())) => {
            transfers.remove(&task);
        }
        Err(error) => {
            tracing::error!("transfer task: {error}");
            if let Some(id) = transfers.remove(&error.id()) {
                crate::activity::transfer_stopped(
                    state,
                    &id,
                    crate::worker_evidence::TransferStop::TaskStopped,
                );
            } else {
                tracing::error!("an unidentified transfer task stopped");
            }
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
        crate::activity::transfer_stopped(
            state,
            &t.id,
            crate::worker_evidence::TransferStop::OutcomeNotSaved,
        );
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
    let task = crate::activity::Task::start(
        state,
        format!("transfer:{}", t.id),
        "Peer file transfer",
        &t.rel,
        Some("/sync"),
    )
    .await
    .map_err(|e| e.message().to_owned())?;
    let result = run_one_reporting(state, t, &task).await;
    let report = match &result {
        Err(e) => Err(AppError::conflict(
            "transfer_failed",
            crate::error::redact(e),
        )),
        Ok(Outcome::Blocked(reason)) => Err(AppError::conflict("transfer_blocked", *reason)),
        _ => Ok(()),
    };
    let phase = match &result {
        Ok(Outcome::Deferred) => "Attempt deferred; file remains queued for its prerequisites",
        Ok(Outcome::Dropped) => "Attempt ended; transfer is no longer applicable",
        _ => "Transfer attempt finished",
    };
    task.finish_with_phase(&report, phase)
        .await
        .map_err(|e| e.message().to_owned())?;
    result
}

async fn run_one_reporting(
    state: &AppState,
    t: &Transfer,
    task: &crate::activity::Reporter,
) -> Result<Outcome, String> {
    task.phase("Verifying peer source evidence");
    // Retain legacy queue entries for inspection, but never adopt or publish their content.
    if super::excluded_path(&t.rel, &state.junk_rules()) {
        return Ok(Outcome::Blocked(
            "NAS metadata and ignored names are excluded from peer replication",
        ));
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
    if let Some(parent) = &t.restore_parent {
        match super::admin::validate_folder_restore(state, parent).await {
            Ok(()) => {}
            Err(error) if error.code() == "choose_version" => {
                return Ok(Outcome::Blocked(
                    "peer source tree changed; a new choice is required",
                ));
            }
            Err(_) => return Ok(Outcome::Deferred),
        }
    }
    // Remote-source adapters authenticate fresh version/hash evidence before local locking.
    if let Some(token) = &t.retained_token {
        let wanted = super::retained::Wanted {
            share: share.name.clone(),
            path: t.source_rel.clone().unwrap_or_else(|| t.rel.clone()),
            sha256: t.sha256.clone(),
            size: t.size as u64,
        };
        if super::retained::remote_check(state, &peer, token, &wanted)
            .await
            .is_err()
        {
            return Ok(Outcome::Deferred);
        }
    } else if let Some(source) = &t.source_rel {
        share.root_rel(source).map_err(m)?;
        let version = t
            .source_vv
            .as_ref()
            .ok_or("verified source version is missing")?;
        let evidence =
            match super::recovery::remote_evidence(state, &peer, &share, source, version).await {
                Ok(evidence) => evidence,
                Err(_) => return Ok(Outcome::Deferred),
            };
        if evidence.path != *source
            || evidence.deleted
            || evidence.sha256.as_deref() != Some(&t.sha256)
            || evidence.size != Some(t.size as u64)
            || !matches!(
                evidence.vv.compare(version),
                Causality::Equal | Causality::Dominates
            )
        {
            return Ok(Outcome::Deferred);
        }
    }
    task.waiting("Waiting for the replication-operation lock");
    let operation_guard = state.replication_operation_lock.lock().await;
    task.phase("Verifying local file and transfer prerequisites");
    if let Err(e) = super::inbox::require_stable_transfer(state, &share, &[&t.rel], &t.id).await {
        return if matches!(e.code(), "recovery_required" | "root_unavailable") {
            Ok(Outcome::Deferred)
        } else {
            Err(e.message().to_owned())
        };
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
    if t.restore_parent.is_none()
        && fingerprint
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
        if t.retained_token.is_some() {
            super::retained::preserve_staging(&root, &t.id).map_err(m)?;
        } else {
            tokio::fs::remove_file(&staging)
                .await
                .map_err(|e| e.to_string())?;
        }
        offset = 0;
    }
    let path = if let Some(token) = &t.retained_token {
        format!(
            "/api/peer/v1/recovery/retained/content?token={}",
            encode(token)
        )
    } else {
        format!(
            "/api/peer/v1/content?share={}&path={}&sha256={}",
            encode(&share.name),
            encode(t.source_rel.as_deref().unwrap_or(&t.rel)),
            t.sha256
        )
    };
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
            if t.source_rel.is_some() || t.retained_token.is_some() {
                return Ok(Outcome::Deferred);
            }
            let current: Option<String> =
                sqlx::query_scalar("SELECT rel_path FROM transfers WHERE id=?")
                    .bind(&t.id)
                    .fetch_optional(&state.db)
                    .await
                    .map_err(|e| e.to_string())?;
            if current.as_deref() != Some(&t.rel) {
                return Ok(Outcome::Deferred);
            }
            return Ok(Outcome::Blocked(
                "source version is no longer at the queued path; recovery will reconcile its current location",
            ));
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
        task.phase("Downloading file contents");
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| net::net_error(&e))?;
            if received + chunk.len() as u64 > expected_bytes {
                return Err("peer sent more data than the requested range".into());
            }
            file.write_all(&chunk).await.map_err(|e| e.to_string())?;
            received += chunk.len() as u64;
            done += chunk.len() as u64;
            task.progress(done, Some(expected_size), "bytes transferred");
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
                    if t.retained_token.is_some() {
                        super::retained::preserve_staging(&root, &t.id).map_err(m)?;
                    } else {
                        let _ = tokio::fs::remove_file(&staging).await;
                    }
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
        drop(file);
        if t.retained_token.is_some() {
            super::retained::preserve_staging(&root, &t.id).map_err(m)?;
        } else {
            let _ = tokio::fs::remove_file(&staging).await;
        }
        return Ok(Outcome::Dropped);
    }
    file.flush().await.map_err(|e| e.to_string())?;
    let file = file.into_inner();
    file.sync_all().await.map_err(|e| e.to_string())?;
    drop(file);
    let check = staging.clone();
    task.phase("Verifying downloaded checksum");
    let reporter = task.clone();
    let hash = tokio::task::spawn_blocking(move || {
        files::sha256_reporting(&check, |done| {
            reporter.progress(done, Some(expected_size), "bytes hashed")
        })
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;
    if hash != t.sha256 {
        if t.retained_token.is_some() {
            super::retained::preserve_staging(&root, &t.id).map_err(m)?;
        } else {
            let _ = tokio::fs::remove_file(&staging).await;
        }
        return Err("downloaded data does not match its checksum".into());
    }

    task.waiting("Waiting for the replication-operation lock before publication");
    let _operation_guard = state.replication_operation_lock.lock().await;
    task.phase("Verifying destination and submitting safe publication");
    if let Err(e) = super::inbox::require_stable_transfer(state, &share, &[&t.rel], &t.id).await {
        return if matches!(e.code(), "recovery_required" | "root_unavailable") {
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
    let source: (Option<String>, Option<String>, String, Option<String>) = sqlx::query_as(
        "SELECT source_rel_path,restore_parent_operation_id,rel_path,source_retained_token FROM transfers WHERE id=?",
    )
    .bind(&t.id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| e.to_string())?;
    if source.0 != t.source_rel
        || source.1 != t.restore_parent
        || source.2 != t.rel
        || source.3 != t.retained_token
    {
        return Ok(Outcome::Deferred);
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
    if t.restore_parent.is_none()
        && (!consistent
            || !newer
            || super::has_open_conflict(state, &share.id, &t.rel)
                .await
                .map_err(m)?)
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
    let (role, existing_original) = if t.restore_parent.is_some() {
        (
            match companion_role(&name, &companion_extensions) {
                Some(CompanionRole::Sidecar) => FileRole::Sidecar,
                Some(CompanionRole::Companion) => FileRole::Companion,
                None => FileRole::Original,
            },
            None,
        )
    } else {
        peer_upload_file(&folder_path, &name, &companion_extensions)?
    };
    if t.restore_parent.is_some() && current.is_some() {
        return Err(AppError::conflict(
            "peer_versions_diverged",
            "restoration target contains a valid competing file",
        ));
    }
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
    if let Some(parent) = &t.restore_parent {
        super::admin::record_restore_upload_in(&mut tx, parent, &t.rel, &upload_id).await?;
        archive_transfer(&mut tx, &t.id, Some(parent)).await?;
        sqlx::query("DELETE FROM transfers WHERE id=? AND status='running'")
            .bind(&t.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        state.sync_wake.notify_waiters();
        state.emit(ServerEvent::SyncChanged);
        return Ok(());
    }
    let accepted = crate::ops::insert_in(&mut tx, &principal, &op, Some(&upload_id)).await?;
    sqlx::query(
        "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id, conflict_id)
         VALUES (?, 0, ?, 'publish', NULL, ?, ?, ?,
            (SELECT id FROM conflicts WHERE share_id=? AND rel_path=? AND peer_id=? AND status='resolving' LIMIT 1))",
    )
    .bind(&accepted.operation_id)
    .bind(&share.id)
    .bind(&t.rel)
    .bind(serde_json::to_string(&t.vv)?)
    .bind(&peer.id)
    .bind(&share.id)
    .bind(&t.rel)
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

pub(super) async fn archive_transfer(
    conn: &mut sqlx::SqliteConnection,
    id: &str,
    operation_id: Option<&str>,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO transfer_history (id, peer_id, share_id, rel_path, sha256, size, queued_at, finished_at, operation_id, run_id, vv_json)
         SELECT id, peer_id, share_id, rel_path, sha256, size, queued_at, ?, ?, run_id, vv_json FROM transfers WHERE id = ?",
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

    #[tokio::test]
    async fn archive_preserves_the_final_merged_queued_version() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::AppState::new(
            crate::config::Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO peers(id,name,url,secret_enc,status,created_at) VALUES('peer','Peer','https://peer.example','unused','online',?)")
            .bind(crate::db::now()).execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled) VALUES('share','Archive','photos','','[\"peer\"]',1)")
            .execute(&state.db).await.unwrap();
        let share = super::super::shares(&state).await.unwrap().remove(0);
        let first = serde_json::from_str(r#"{"one":2}"#).unwrap();
        let second = serde_json::from_str(r#"{"two":3}"#).unwrap();
        let hash = "a".repeat(64);
        super::super::queue_transfer(&state, "peer", &share, "photo.jpg", &hash, 12, &first)
            .await
            .unwrap();
        super::super::queue_transfer(&state, "peer", &share, "photo.jpg", &hash, 12, &second)
            .await
            .unwrap();
        let (id, queued): (String, String) =
            sqlx::query_as("SELECT id,vv_json FROM transfers WHERE share_id='share'")
                .fetch_one(&state.db)
                .await
                .unwrap();
        let queued: ism_core::version::VersionVector = serde_json::from_str(&queued).unwrap();
        assert_eq!(queued.get("one"), 2);
        assert_eq!(queued.get("two"), 3);
        let mut tx = state.db.begin().await.unwrap();
        super::archive_transfer(&mut tx, &id, None).await.unwrap();
        tx.commit().await.unwrap();
        let archived: String =
            sqlx::query_scalar("SELECT vv_json FROM transfer_history WHERE id=?")
                .bind(id)
                .fetch_one(&state.db)
                .await
                .unwrap();
        let archived: ism_core::version::VersionVector = serde_json::from_str(&archived).unwrap();
        assert_eq!(archived, queued);
    }

    #[tokio::test]
    async fn panicked_transfer_task_reports_only_its_running_row() {
        let dir = tempfile::tempdir().unwrap();
        let pool = crate::db::connect(dir.path()).await.unwrap();
        let state = crate::state::AppState::new(
            crate::config::Config::for_tests(dir.path()),
            pool,
            crate::crypto::SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
             VALUES ('peer','Peer example','https://peer.example','unused','online',?)",
        )
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
             VALUES ('share','Archive','photos','','[\"peer\"]',1)",
        )
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
             VALUES ('transfer','peer','share','photo.example.jpg','example',10,'{}','running',?,?)",
        )
        .bind(crate::db::now())
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();

        let mut tasks = tokio::task::JoinSet::new();
        let mut transfers = std::collections::HashMap::new();
        let failed = tasks.spawn(async { panic!("synthetic transfer task failure") });
        transfers.insert(failed.id(), "transfer".to_owned());
        let sibling = tasks.spawn(std::future::pending());
        transfers.insert(sibling.id(), "other".to_owned());
        let result = tasks.join_next_with_id().await.unwrap();
        super::record_transfer_exit(&state, &mut transfers, result);
        assert!(transfers.contains_key(&sibling.id()));
        let snapshot = crate::attention::snapshot(&state).await.unwrap();
        let failure = snapshot
            .items
            .iter()
            .find(|item| item.id == "transfer-worker:peer:share:task-stopped")
            .unwrap();
        assert!(failure.blocks_work);
        assert_eq!(failure.count, 1);
        sqlx::query("UPDATE transfers SET status='queued' WHERE id='transfer'")
            .execute(&state.db)
            .await
            .unwrap();
        assert!(
            !crate::attention::snapshot(&state)
                .await
                .unwrap()
                .items
                .iter()
                .any(|item| item.id == "transfer-worker:peer:share:task-stopped")
        );
        tasks.shutdown().await;
    }

    #[test]
    fn classifies_peer_xmp_as_attached_to_existing_original() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("retry-move.png"), b"original").unwrap();
        std::fs::write(dir.path().join("retry-move.xmp"), b"metadata").unwrap();
        let extensions = DEFAULT_SIDECAR_EXTENSIONS
            .iter()
            .map(|extension| extension.to_string())
            .collect::<Vec<_>>();

        let existing = existing_original_for_companion(dir.path(), "retry-move.xmp", &extensions)
            .unwrap()
            .unwrap();
        assert_eq!(existing.name, "retry-move.png");
        assert!(!existing.expected_revision.is_empty());

        let (role, existing_original) =
            peer_upload_file(dir.path(), "retry-move.xmp", &extensions).unwrap();
        assert_eq!(role, FileRole::Sidecar);
        assert!(existing_original.is_some());
        assert!(!upload_file_needs_sync_add(role, "retry-move.xmp"));

        let (role, existing_original) =
            peer_upload_file(dir.path(), "new-photo.png", &extensions).unwrap();
        assert_eq!(role, FileRole::Original);
        assert!(existing_original.is_none());
        assert!(upload_file_needs_sync_add(role, "new-photo.png"));
    }

    #[test]
    fn leaves_ambiguous_shared_peer_sidecar_unanchored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("retry.jpg"), b"jpg").unwrap();
        std::fs::write(dir.path().join("retry.png"), b"png").unwrap();
        std::fs::write(dir.path().join("retry.xmp"), b"metadata").unwrap();
        let extensions = DEFAULT_SIDECAR_EXTENSIONS
            .iter()
            .map(|extension| extension.to_string())
            .collect::<Vec<_>>();

        let (role, existing) = peer_upload_file(dir.path(), "retry.xmp", &extensions).unwrap();
        assert_eq!(role, FileRole::Sidecar);
        assert!(existing.is_none());
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

            let existing = existing_original_for_companion(dir.path(), "retry.xmp", &extensions)
                .unwrap()
                .unwrap();
            assert_eq!(existing.name, expected);
        }
    }
}
