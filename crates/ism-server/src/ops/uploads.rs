//! Resumable uploads that are published into the original folder by a managed operation.

use std::io::{Seek, SeekFrom, Write};

use axum::http::HeaderMap;
use ism_core::dto::{
    ClientAction, CreateUpload, FileRole, OnConflict, OperationAccepted, OperationAction,
    OperationRequest, UploadFileState, UploadState,
};

use ism_core::paths::RelPath;

use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::AppState;

pub const MAX_FILES: usize = 32;

fn missing_staging() -> AppError {
    AppError::conflict(
        "upload_staging_missing",
        "upload staging file is missing; start a new upload",
    )
}

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
    super::plan::staging_dir(&root, id)
}

pub async fn create(
    state: &AppState,
    principal: &Principal,
    req: CreateUpload,
) -> AppResult<UploadState> {
    principal.require(ClientAction::Upload, &req.target.root_id)?;
    if req
        .idempotency_key
        .as_ref()
        .is_some_and(|key| key.trim().is_empty() || key.len() > 200)
    {
        return Err(AppError::bad_request(
            "bad_idempotency_key",
            "idempotency_key must be 1-200 characters",
        ));
    }
    let guard = if let Some(key) = &req.idempotency_key {
        let lock_key = crate::crypto::sha256_hex(format!("{}\0{key}", principal.key()).as_bytes());
        Some(state.lock_upload(&format!("create:{lock_key}")).await)
    } else {
        None
    };
    let (state, principal) = (state.clone(), principal.clone());
    tokio::spawn(async move {
        let _guard = guard;
        create_locked(&state, &principal, req).await
    })
    .await
    .map_err(AppError::internal)?
}

async fn create_locked(
    state: &AppState,
    principal: &Principal,
    req: CreateUpload,
) -> AppResult<UploadState> {
    if req.files.is_empty() || req.files.len() > MAX_FILES {
        return Err(AppError::bad_request(
            "bad_upload",
            format!("an upload holds 1-{MAX_FILES} files"),
        ));
    }
    let originals = req
        .files
        .iter()
        .filter(|f| f.role == FileRole::Original)
        .count();
    match &req.existing_original {
        None if originals != 1 => {
            return Err(AppError::bad_request(
                "bad_upload",
                "exactly one file must have role 'original', or name the existing original",
            ));
        }
        None => {}
        Some(_) if originals != 0 => {
            return Err(AppError::bad_request(
                "bad_upload",
                "a sidecar-only upload must not include an 'original' file",
            ));
        }
        Some(existing) => {
            if !valid_name(&existing.name) || existing.expected_revision.trim().is_empty() {
                return Err(AppError::bad_request(
                    "bad_upload",
                    "existing_original needs a valid name and expected_revision",
                ));
            }
            if req.files.iter().any(|f| f.name == existing.name) {
                return Err(AppError::bad_request(
                    "bad_upload",
                    "a sidecar cannot have the name of the original",
                ));
            }
            if req.on_conflict != OnConflict::ReplaceIfExpected {
                return Err(AppError::bad_request(
                    "bad_upload",
                    "sidecar-only uploads use on_conflict 'replace_if_expected'",
                ));
            }
        }
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
        if f.expected_sha256
            .as_ref()
            .is_some_and(|hash| hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()))
        {
            return Err(AppError::bad_request(
                "bad_upload",
                "expected_sha256 must be 64 hex characters",
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
    if let Some(origin) = req
        .provenance
        .as_ref()
        .and_then(|p| p.original_location.as_ref())
    {
        // ILA uploads go back to the folder the original came from, never to an upload library.
        let origin_folder = RelPath::parse(&origin.path)
            .ok()
            .and_then(|rel| rel.parent())
            .map(|rel| rel.to_string())
            .unwrap_or_default();
        let target_folder = RelPath::parse(&req.target.path)
            .map(|rel| rel.to_string())
            .unwrap_or_default();
        if origin.root_id != req.target.root_id || origin_folder != target_folder {
            return Err(AppError::bad_request(
                "upload_target_mismatch",
                "the target must be the folder of provenance.original_location",
            ));
        }
    }
    let request_json = serde_json::to_string(&req)?;
    if let Some(key) = &req.idempotency_key {
        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT id, request_json FROM uploads WHERE principal = ? AND idempotency_key = ?",
        )
        .bind(principal.key())
        .bind(key)
        .fetch_optional(&state.db)
        .await?;
        if let Some((id, existing_request)) = existing {
            let existing_request: CreateUpload = serde_json::from_str(&existing_request)?;
            if serde_json::to_value(&existing_request)? != serde_json::to_value(&req)? {
                return Err(AppError::conflict(
                    "idempotency_key_conflict",
                    "idempotency key was already used for a different upload",
                ));
            }
            return get(state, principal, &id).await;
        }
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
    let parent = dir
        .parent()
        .ok_or_else(|| AppError::internal("upload staging has no parent"))?;
    std::fs::create_dir_all(parent).map_err(AppError::internal)?;
    std::fs::create_dir(&dir).map_err(AppError::internal)?;
    for i in 0..req.files.len() {
        if let Err(error) = std::fs::File::create(dir.join(i.to_string())) {
            if let Err(cleanup_error) = std::fs::remove_dir_all(&dir) {
                tracing::error!(upload_id = %id, %cleanup_error, "could not clean up incomplete upload staging");
            }
            return Err(AppError::internal(error));
        }
    }
    sqlx::query("INSERT INTO uploads (id, principal, request_json, status, created_at, idempotency_key) VALUES (?, ?, ?, 'receiving', ?, ?)")
        .bind(&id)
        .bind(principal.key())
        .bind(request_json)
        .bind(db::now())
        .bind(&req.idempotency_key)
        .execute(&state.db)
        .await?;
    get(state, principal, &id).await
}

pub async fn get(state: &AppState, principal: &Principal, id: &str) -> AppResult<UploadState> {
    let _guard = state.lock_upload(id).await;
    get_locked(state, principal, id).await
}

async fn get_locked(state: &AppState, principal: &Principal, id: &str) -> AppResult<UploadState> {
    let (req, status) = load(state, principal, id).await?;
    let dir = if status == "receiving" {
        Some(staging(state, &req, id).await?)
    } else {
        staging(state, &req, id).await.ok()
    };
    let files = (0..req.files.len())
        .map(|i| {
            let metadata = dir
                .as_ref()
                .map(|dir| std::fs::metadata(dir.join(i.to_string())));
            let received = match metadata {
                Some(Ok(metadata)) if metadata.is_file() => metadata.len(),
                Some(Err(error))
                    if status == "receiving" && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Err(missing_staging());
                }
                Some(Err(error)) if status == "receiving" => {
                    return Err(AppError::internal(error));
                }
                Some(Ok(_)) if status == "receiving" => {
                    return Err(AppError::conflict(
                        "upload_staging_invalid",
                        "upload staging file is not a regular file",
                    ));
                }
                _ => 0,
            };
            Ok(UploadFileState {
                index: i as u32,
                received,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
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
    let guard = state.lock_upload(id).await;
    let (state, principal, id, headers) = (
        state.clone(),
        principal.clone(),
        id.to_owned(),
        headers.clone(),
    );
    // Keep the lock until I/O finishes even if the request is cancelled.
    tokio::spawn(async move {
        let _guard = guard;
        put_chunk_locked(&state, &principal, &id, index, &headers, body).await
    })
    .await
    .map_err(AppError::internal)?
}

async fn put_chunk_locked(
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
        let mut f = std::fs::OpenOptions::new().write(true).open(&path)?;
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
    .map_err(|e| match e.kind() {
        std::io::ErrorKind::InvalidInput => AppError::new(
            axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
            "bad_range",
            "chunk starts after the received data",
        ),
        std::io::ErrorKind::NotFound => missing_staging(),
        _ => AppError::internal(e),
    })?;
    get_locked(state, principal, id).await
}

pub async fn commit(
    state: &AppState,
    principal: &Principal,
    id: &str,
) -> AppResult<OperationAccepted> {
    let guard = state.lock_upload(id).await;
    let (state, principal, id) = (state.clone(), principal.clone(), id.to_owned());
    // A disconnected client must not interrupt checksum verification and publication.
    tokio::spawn(async move {
        let _guard = guard;
        commit_locked(&state, &principal, &id).await
    })
    .await
    .map_err(AppError::internal)?
}

async fn commit_locked(
    state: &AppState,
    principal: &Principal,
    id: &str,
) -> AppResult<OperationAccepted> {
    let (req, status) = load(state, principal, id).await?;
    if status != "receiving" {
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
        return Err(AppError::conflict(
            "upload_closed",
            "the upload is no longer receiving data",
        ));
    }
    let dir = staging(state, &req, id).await?;
    for (i, spec) in req.files.iter().enumerate() {
        let path = dir.join(i.to_string());
        let metadata = std::fs::metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                missing_staging()
            } else {
                AppError::internal(error)
            }
        })?;
        if !metadata.is_file() {
            return Err(AppError::conflict(
                "upload_staging_invalid",
                "upload staging file is not a regular file",
            ));
        }
        let len = metadata.len();
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
                .map(|f| f.name.as_str())
                .or(req.existing_original.as_ref().map(|o| o.name.as_str()))
                .unwrap_or("")
        )),
        actions: vec![OperationAction::PublishUpload {
            upload_id: id.to_owned(),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let _operation_guard = state
        .lock_operation_submission(&principal.key(), &request.idempotency_key)
        .await;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE principal = ? AND idempotency_key = ?)",
    )
    .bind(principal.key())
    .bind(&request.idempotency_key)
    .fetch_one(&state.db)
    .await?;
    if exists {
        return Err(AppError::conflict(
            "idempotency_key_conflict",
            "upload operation key was already used for a different operation",
        ));
    }
    super::insert_inner(state, principal, &request, Some(id)).await
}

async fn cleanup_checked(state: &AppState, req: &CreateUpload, id: &str) -> AppResult<()> {
    let dir = staging(state, req, id).await?;
    if let Err(error) = tokio::fs::remove_dir_all(dir).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(AppError::internal(error));
    }
    Ok(())
}

/// Removes the staging folder of a published or abandoned upload.
pub async fn cleanup(state: &AppState, id: &str) {
    let result = async {
        let req: String = sqlx::query_scalar("SELECT request_json FROM uploads WHERE id = ?")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
        let req: CreateUpload = serde_json::from_str(&req)?;
        cleanup_checked(state, &req, id).await
    }
    .await;
    if let Err(error) = result {
        tracing::error!(upload_id = %id, code = %error.code(), "could not clean up upload staging");
    }
}

/// Cancels only a receiving upload; published or recovery-required data is never removed.
pub async fn cancel(state: &AppState, principal: &Principal, id: &str) -> AppResult<()> {
    let guard = state.lock_upload(id).await;
    let (state, principal, id) = (state.clone(), principal.clone(), id.to_owned());
    tokio::spawn(async move {
        let _guard = guard;
        let (req, status) = load(&state, &principal, &id).await?;
        principal.require(ClientAction::Upload, &req.target.root_id)?;
        match status.as_str() {
            "receiving" => {
                let updated = sqlx::query(
                    "UPDATE uploads SET status = 'cancelled' WHERE id = ? AND status = 'receiving'",
                )
                .bind(&id)
                .execute(&state.db)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(AppError::conflict(
                        "upload_closed",
                        "the upload is no longer receiving data",
                    ));
                }
            }
            "cancelled" => {}
            _ => {
                return Err(AppError::conflict(
                    "upload_closed",
                    "only receiving uploads can be cancelled",
                ));
            }
        }
        cleanup_checked(&state, &req, &id).await
    })
    .await
    .map_err(AppError::internal)?
}

/// Uploads left in `receiving` for more than a week are discarded.
pub async fn expire(state: &AppState) {
    let cutoff = db::ts(chrono::Utc::now() - chrono::Duration::days(7));
    let ids: Vec<String> = match sqlx::query_scalar(
        "SELECT id FROM uploads WHERE status = 'receiving' AND created_at < ?",
    )
    .bind(&cutoff)
    .fetch_all(&state.db)
    .await
    {
        Ok(ids) => ids,
        Err(error) => {
            tracing::error!(%error, "could not list expired uploads");
            return;
        }
    };
    for id in ids {
        let _guard = state.lock_upload(&id).await;
        let result = sqlx::query("UPDATE uploads SET status = 'failed' WHERE id = ? AND status = 'receiving' AND created_at < ?")
            .bind(&id)
            .bind(&cutoff)
            .execute(&state.db)
            .await;
        match result {
            Ok(result) if result.rows_affected() == 1 => cleanup(state, &id).await,
            Ok(_) => {}
            Err(error) => tracing::error!(upload_id = %id, %error, "could not expire upload"),
        }
    }
}

/// After Immich scanned `library_id`, records the asset id of published uploads (and of the
/// originals that sidecar-only uploads belong to) that were still waiting for the import, and
/// announces the updated operation items. Returns how many items were filled.
pub async fn fill_asset_ids(
    state: &AppState,
    server_id: &str,
    library_id: &str,
) -> AppResult<usize> {
    let cutoff = db::ts(chrono::Utc::now() - chrono::Duration::days(14));
    let waiting: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT u.request_json, i.operation_id, i.idx FROM uploads u
         JOIN operation_items i ON i.operation_id = u.operation_id
         WHERE i.kind = 'publish_upload' AND i.status = 'done' AND i.result_json IS NOT NULL
           AND json_extract(i.result_json, '$.asset_id') IS NULL AND u.created_at >= ?
         ORDER BY u.created_at LIMIT 500",
    )
    .bind(&cutoff)
    .fetch_all(&state.db)
    .await?;
    if waiting.is_empty() {
        return Ok(0);
    }
    let server = crate::immich::registry::server(state, server_id).await?;
    let mut lookup = AssetLookup::new(state, &server, library_id).await;
    let mut filled = 0;
    for (request_json, operation_id, index) in waiting {
        let Ok(req) = serde_json::from_str::<CreateUpload>(&request_json) else {
            continue;
        };
        let Ok((root, rel, _)) = files::locate(&state.db, &req.target).await else {
            continue;
        };
        if root.server_id.as_deref() != Some(server_id)
            || root.library_id.as_deref() != Some(library_id)
        {
            continue;
        }
        let original = req
            .files
            .iter()
            .find(|f| f.role == FileRole::Original)
            .map(|f| f.name.as_str())
            .or(req.existing_original.as_ref().map(|o| o.name.as_str()));
        let Some(path) = original
            .and_then(|name| rel.join(name).ok())
            .and_then(|rel| root.immich_path(&rel))
        else {
            continue;
        };
        let Some(asset_id) = lookup.find(&path).await else {
            continue;
        };
        let updated = sqlx::query(
            "UPDATE operation_items SET result_json = json_set(result_json, '$.asset_id', ?)
             WHERE operation_id = ? AND idx = ? AND json_extract(result_json, '$.asset_id') IS NULL",
        )
        .bind(&asset_id)
        .bind(&operation_id)
        .bind(index)
        .execute(&state.db)
        .await?
        .rows_affected();
        if updated == 1 {
            filled += 1;
            let (op, _) = super::load(state, &operation_id).await?;
            if let Some(item) = op.items.get(usize::try_from(index).unwrap_or(usize::MAX)) {
                super::record(state, &operation_id, "item", &serde_json::to_value(item)?).await?;
            }
        }
    }
    Ok(filled)
}

/// Finds assets by original path: in Immich's database when ISM can read it, otherwise through
/// the API as the library owner (searches only return the caller's own assets).
struct AssetLookup {
    pg: Option<sqlx::PgPool>,
    api: Option<crate::immich::api::Api>,
}

impl AssetLookup {
    async fn new(
        state: &AppState,
        server: &crate::immich::registry::Server,
        library_id: &str,
    ) -> Self {
        use crate::immich::registry;
        let pg = if server.database_url_enc.is_some() {
            registry::pg_pool(state, server).await.ok()
        } else {
            None
        };
        let api = match registry::library_owner(state, library_id).await {
            Ok(Some(owner)) => registry::api_as_user(state, server, &owner).await.ok(),
            _ => None,
        };
        Self { pg, api }
    }

    async fn find(&mut self, path: &str) -> Option<String> {
        if let Some(pool) = &self.pg
            && let Ok(mut conn) = pool.acquire().await
            && let Ok(found) = crate::immich::pg::asset_by_path(&mut conn, path).await
        {
            return found.map(|asset| asset.id);
        }
        match &self.api {
            Some(api) => api.asset_id_by_path(path).await.ok().flatten(),
            None => None,
        }
    }
}
