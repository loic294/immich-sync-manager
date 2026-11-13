//! Resumable uploads that are published into the original folder by a managed operation.

use std::io::{Seek, SeekFrom, Write};

use axum::http::HeaderMap;
use ism_core::dto::{
    ClientAction, CreateUpload, FileRole, OperationAccepted, OperationAction, OperationRequest,
    UploadFileState, UploadState,
};

use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::AppState;

pub const MAX_FILES: usize = 32;

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains(['/', '\\', '\0'])
        && !name.starts_with(".ism")
        && name.len() <= 255
}

async fn load(
    state: &AppState,
    principal: &Principal,
    id: &str,
) -> AppResult<(CreateUpload, String)> {
    let row: Option<(String, String, String)> =
        sqlx::query_as("SELECT principal, request_json, status FROM uploads WHERE id = ?")
            .bind(id)
            .fetch_optional(&state.db)
            .await?;
    let Some((owner, req, status)) = row else {
        return Err(AppError::not_found("upload not found"));
    };
    if owner != principal.key() {
        return Err(AppError::not_found("upload not found"));
    }
    Ok((serde_json::from_str(&req)?, status))
}

async fn staging(state: &AppState, req: &CreateUpload, id: &str) -> AppResult<std::path::PathBuf> {
    let root = files::root(&state.db, &req.target.root_id).await?;
    root.ensure_available()?;
    Ok(super::plan::staging_dir(&root, id))
}

pub async fn create(
    state: &AppState,
    principal: &Principal,
    req: CreateUpload,
) -> AppResult<UploadState> {
    principal.require(ClientAction::Upload, &req.target.root_id)?;
    if req.files.is_empty() || req.files.len() > MAX_FILES {
        return Err(AppError::bad_request(
            "bad_upload",
            format!("an upload holds 1-{MAX_FILES} files"),
        ));
    }
    if req
        .files
        .iter()
        .filter(|f| f.role == FileRole::Original)
        .count()
        != 1
    {
        return Err(AppError::bad_request(
            "bad_upload",
            "exactly one file must have role 'original'",
        ));
    }
    for f in &req.files {
        if !valid_name(&f.name) {
            return Err(AppError::bad_request(
                "bad_upload",
                format!("invalid file name '{}'", f.name),
            ));
        }
        if f.sha256.len() != 64 || !f.sha256.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(AppError::bad_request(
                "bad_upload",
                "sha256 must be 64 hex characters",
            ));
        }
    }
    let mut names: Vec<&str> = req.files.iter().map(|f| f.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    if names.len() != req.files.len() {
        return Err(AppError::bad_request(
            "bad_upload",
            "file names must be unique",
        ));
    }
    let (_, rel, abs) = files::locate(&state.db, &req.target).await?;
    // A missing folder is created when the upload is published, so uploads keep the original folder.
    if abs.exists() && !abs.is_dir() {
        return Err(AppError::conflict(
            "not_a_folder",
            format!("'{rel}' is a file, not a folder"),
        ));
    }
    let id = crate::crypto::new_id("up");
    let dir = staging(state, &req, &id).await?;
    std::fs::create_dir_all(&dir).map_err(AppError::internal)?;
    for i in 0..req.files.len() {
        std::fs::File::create(dir.join(i.to_string())).map_err(AppError::internal)?;
    }
    sqlx::query("INSERT INTO uploads (id, principal, request_json, status, created_at) VALUES (?, ?, ?, 'receiving', ?)")
        .bind(&id)
        .bind(principal.key())
        .bind(serde_json::to_string(&req)?)
        .bind(db::now())
        .execute(&state.db)
        .await?;
    get(state, principal, &id).await
}

pub async fn get(state: &AppState, principal: &Principal, id: &str) -> AppResult<UploadState> {
    let (req, status) = load(state, principal, id).await?;
    let dir = staging(state, &req, id).await.ok();
    let files = (0..req.files.len())
        .map(|i| UploadFileState {
            index: i as u32,
            received: dir
                .as_ref()
                .and_then(|d| std::fs::metadata(d.join(i.to_string())).ok())
                .map_or(0, |m| m.len()),
        })
        .collect();
    Ok(UploadState {
        upload_id: id.to_owned(),
        status,
        files,
    })
}

/// Parses `bytes start-end/total`.
fn content_range(headers: &HeaderMap) -> AppResult<Option<(u64, u64, u64)>> {
    let Some(raw) = headers.get("content-range") else {
        return Ok(None);
    };
    let bad =
        || AppError::bad_request("bad_range", "Content-Range must be 'bytes start-end/total'");
    let raw = raw.to_str().map_err(|_| bad())?;
    let rest = raw.strip_prefix("bytes ").ok_or_else(bad)?;
    let (range, total) = rest.split_once('/').ok_or_else(bad)?;
    let (start, end) = range.split_once('-').ok_or_else(bad)?;
    let parse = |v: &str| v.trim().parse::<u64>().map_err(|_| bad());
    let (start, end, total) = (parse(start)?, parse(end)?, parse(total)?);
    if end < start || end >= total {
        return Err(bad());
    }
    Ok(Some((start, end, total)))
}

pub async fn put_chunk(
    state: &AppState,
    principal: &Principal,
    id: &str,
    index: usize,
    headers: &HeaderMap,
    body: bytes::Bytes,
) -> AppResult<UploadState> {
    let (req, status) = load(state, principal, id).await?;
    if status != "receiving" {
        return Err(AppError::conflict(
            "upload_closed",
            "the upload was already committed",
        ));
    }
    let spec = req
        .files
        .get(index)
        .ok_or_else(|| AppError::not_found("no such file index"))?;
    let (start, total) = match content_range(headers)? {
        Some((start, end, total)) => {
            if end - start + 1 != body.len() as u64 {
                return Err(AppError::bad_request(
                    "bad_range",
                    "Content-Range does not match the body length",
                ));
            }
            (start, total)
        }
        None => (0, body.len() as u64),
    };
    if total != spec.size || start + body.len() as u64 > spec.size {
        return Err(AppError::bad_request(
            "bad_range",
            "the data exceeds the declared file size",
        ));
    }
    let path = staging(state, &req, id).await?.join(index.to_string());
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let len = f.metadata()?.len();
        // Chunks must continue the received data (or overwrite it); no holes.
        if start > len {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "gap"));
        }
        f.seek(SeekFrom::Start(start))?;
        f.write_all(&body)?;
        f.sync_data()
    })
    .await
    .map_err(AppError::internal)?
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::InvalidInput {
            AppError::new(
                axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
                "bad_range",
                "chunk starts after the received data",
            )
        } else {
            AppError::internal(e)
        }
    })?;
    get(state, principal, id).await
}

pub async fn commit(
    state: &AppState,
    principal: &Principal,
    id: &str,
) -> AppResult<OperationAccepted> {
    let (req, status) = load(state, principal, id).await?;
    if status == "committed" || status == "publishing" {
        let op: Option<String> =
            sqlx::query_scalar("SELECT operation_id FROM uploads WHERE id = ?")
                .bind(id)
                .fetch_one(&state.db)
                .await?;
        if let Some(op) = op {
            let (o, _) = super::load(state, &op).await?;
            return Ok(OperationAccepted {
                operation_id: op,
                status: o.status,
            });
        }
    }
    let dir = staging(state, &req, id).await?;
    for (i, spec) in req.files.iter().enumerate() {
        let path = dir.join(i.to_string());
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if len != spec.size {
            return Err(AppError::conflict(
                "upload_incomplete",
                format!("{}: received {len} of {} bytes", spec.name, spec.size),
            ));
        }
        let hash = tokio::task::spawn_blocking(move || files::sha256_file(&path))
            .await
            .map_err(AppError::internal)?
            .map_err(AppError::internal)?;
        if !hash.eq_ignore_ascii_case(&spec.sha256) {
            return Err(AppError::conflict(
                "checksum_mismatch",
                format!("{}: the data does not match sha256", spec.name),
            ));
        }
    }
    let request = OperationRequest {
        idempotency_key: format!("upload:{id}"),
        label: Some(format!(
            "Upload {}",
            req.files
                .iter()
                .find(|f| f.role == FileRole::Original)
                .map_or("", |f| &f.name)
        )),
        actions: vec![OperationAction::PublishUpload {
            upload_id: id.to_owned(),
        }],
        allow_owner_transfer: false,
    };
    let accepted = super::insert(state, principal, &request).await?;
    sqlx::query("UPDATE uploads SET status = 'publishing', operation_id = ? WHERE id = ?")
        .bind(&accepted.operation_id)
        .bind(id)
        .execute(&state.db)
        .await?;
    Ok(accepted)
}

/// Removes the staging folder of a published or abandoned upload.
pub async fn cleanup(state: &AppState, id: &str) {
    let req: Option<String> = sqlx::query_scalar("SELECT request_json FROM uploads WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    if let Some(req) = req.and_then(|r| serde_json::from_str::<CreateUpload>(&r).ok())
        && let Ok(dir) = staging(state, &req, id).await
    {
        let _ = tokio::fs::remove_dir_all(dir).await;
    }
}

/// Uploads left in `receiving` for more than a week are discarded.
pub async fn expire(state: &AppState) {
    let cutoff = db::ts(chrono::Utc::now() - chrono::Duration::days(7));
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM uploads WHERE status = 'receiving' AND created_at < ?")
            .bind(cutoff)
            .fetch_all(&state.db)
            .await
            .unwrap_or_default();
    for id in ids {
        cleanup(state, &id).await;
        let _ = sqlx::query("UPDATE uploads SET status = 'failed' WHERE id = ?")
            .bind(&id)
            .execute(&state.db)
            .await;
    }
}
