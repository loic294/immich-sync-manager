//! Client API used by OPM, ILA and the web UI (`/api/v1`). See `docs/client-api.md`.

use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use futures_util::{StreamExt, stream};
use ism_core::dto::{
    CancelOperationByKey, Change, ClientAction, ClientAsset, ClientAssetPage, ClientInfo,
    ClientRoot, ClientSettings, CreateUpload, DirectWrites, DirectWritesReason, FileRole,
    FolderEntry, FolderListing, Location, Manifest, ManifestFile, OperationRequest,
};
use ism_core::paths::{RelPath, split_ext, strip_abs_prefix};
use ism_core::sidecar::{CompanionRole, companions_of};
use serde::Deserialize;
use tokio::sync::broadcast;

use super::{last_event_id, serve_file, sse};
use crate::auth::{Client, Principal};
use crate::error::{AppError, AppResult};
use crate::files::{self, Root};
use crate::immich::{pg, registry};
use crate::ops::{self, uploads};
use crate::state::{AppState, ServerEvent};

pub const API_VERSION: u32 = 1;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/client/info", get(info))
        .route("/client/settings", get(client_settings))
        .route("/assets", get(assets))
        .route("/locations/resolve", get(resolve_location))
        .route("/folders", get(folders))
        .route("/assets/{id}/manifest", get(asset_manifest))
        .route("/manifest", get(path_manifest))
        .route("/content", get(content))
        .route("/uploads", post(create_upload))
        .route("/uploads/{id}", get(get_upload).delete(cancel_upload))
        .route("/uploads/{id}/files/{index}", put(put_upload_chunk))
        .route("/uploads/{id}/commit", post(commit_upload))
        .route("/operations/preview", post(preview))
        .route("/operations", get(find_operation).post(submit))
        .route("/operations/cancel-by-key", post(cancel_by_key))
        .route(
            "/instances/{instance_id}/operations",
            get(find_operation_for_instance).post(submit_for_instance),
        )
        .route(
            "/instances/{instance_id}/operations/{id}",
            get(get_operation_for_instance),
        )
        .route(
            "/instances/{instance_id}/operations/cancel-by-key",
            post(cancel_by_key_for_instance),
        )
        .route("/operations/{id}", get(get_operation))
        .route("/operations/{id}/cancel", post(cancel_operation))
        .route("/operations/{id}/events", get(operation_events))
        .route("/events", get(events))
        .route("/trash", get(trash))
}

fn actions_of(p: &Principal) -> Vec<ClientAction> {
    match p {
        Principal::Client(g) => g.actions.clone(),
        _ => ClientAction::ALL.to_vec(),
    }
}

async fn info(State(state): State<AppState>, Client(p): Client) -> AppResult<Json<ClientInfo>> {
    let servers = registry::servers(&state).await?;
    let roots = files::load_roots(&state.db)
        .await?
        .into_iter()
        .filter(|r| p.sees_root(&r.mapping.root_id))
        .map(|r| {
            let (direct_writes, reason) = match r.server_id.as_deref() {
                None => (DirectWrites::NotLinked, None),
                Some(id) => match servers.iter().find(|s| s.id == id) {
                    None => (
                        DirectWrites::Blocked,
                        Some((
                            DirectWritesReason::UnknownServer,
                            "the root points to an unknown Immich server".to_owned(),
                        )),
                    ),
                    Some(server) => match registry::write_block(server) {
                        None => (DirectWrites::Enabled, None),
                        block => (DirectWrites::Blocked, block),
                    },
                },
            };
            let (direct_writes_reason, direct_writes_detail) = reason.unzip();
            ClientRoot {
                available: r.available(),
                root_id: r.mapping.root_id.clone(),
                name: r.name.clone(),
                immich_import_path: r.mapping.immich_import_path.clone(),
                opm_prefix: r.mapping.opm_prefix.clone(),
                direct_writes,
                direct_writes_reason,
                direct_writes_detail,
            }
        })
        .collect();
    Ok(Json(ClientInfo {
        instance_id: state.instance_id.clone(),
        instance_name: state.instance_name().await,
        version: env!("CARGO_PKG_VERSION").to_owned(),
        api_version: API_VERSION,
        token_kind: p.source(),
        actions: actions_of(&p),
        roots,
        capabilities: vec![
            "trash_if_asset_matches".into(),
            "cancel_receiving_upload".into(),
            "cancel_operation_by_key".into(),
            "instance_scoped_operations".into(),
            "find_operation_by_key".into(),
            "trash_expectations".into(),
            "partial_submit".into(),
            "shared_file_settings".into(),
            "immich_assets".into(),
        ],
    }))
}

async fn client_settings(State(state): State<AppState>, Client(_): Client) -> Json<ClientSettings> {
    Json(crate::settings::client_view(&state).await)
}

const ASSET_PAGE_DEFAULT: u32 = 1000;
const ASSET_PAGE_MAX: u32 = 5000;

#[derive(Deserialize)]
struct AssetsQuery {
    root_id: String,
    #[serde(default)]
    path: String,
    after: Option<String>,
    limit: Option<u32>,
}

fn immich_unavailable(detail: impl std::fmt::Display) -> AppError {
    AppError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "immich_unavailable",
        format!("the Immich database cannot be read: {detail}"),
    )
}

/// Live Immich assets below a root folder, one page at a time.
async fn assets(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<AssetsQuery>,
) -> AppResult<Json<ClientAssetPage>> {
    p.require(ClientAction::Read, &q.root_id)?;
    let limit = q.limit.unwrap_or(ASSET_PAGE_DEFAULT);
    if !(1..=ASSET_PAGE_MAX).contains(&limit) {
        return Err(AppError::bad_request(
            "invalid_request",
            format!("limit must be between 1 and {ASSET_PAGE_MAX}"),
        ));
    }
    let folder = files::parse_rel(&q.path)?;
    let after = q.after.as_deref().map(files::parse_rel).transpose()?;
    let root = files::root(&state.db, &q.root_id).await?;
    let linked = root
        .server_id
        .as_deref()
        .zip(root.immich_path(&RelPath::root()));
    let Some((server_id, base)) = linked else {
        return Ok(Json(ClientAssetPage {
            root_id: q.root_id,
            linked: false,
            assets: Vec::new(),
            next: None,
        }));
    };
    let folder_abs = root
        .immich_path(&folder)
        .ok_or_else(|| AppError::internal("linked root without an Immich path"))?;
    let after_abs = after.as_ref().and_then(|a| root.immich_path(a));
    let server = registry::server(&state, server_id).await?;
    if !server.compatible || server.database_url_enc.is_none() {
        return Err(immich_unavailable(
            "the server is incompatible or has no database connection",
        ));
    }
    let pool = registry::pg_pool(&state, &server)
        .await
        .map_err(|e| immich_unavailable(e.message()))?;
    let mut conn = pool.acquire().await.map_err(immich_unavailable)?;
    let mut rows = pg::live_assets_page(
        &mut conn,
        &folder_abs,
        after_abs.as_deref(),
        i64::from(limit) + 1,
    )
    .await
    .map_err(immich_unavailable)?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let assets: Vec<ClientAsset> = rows
        .into_iter()
        .filter_map(|(asset_id, original)| {
            strip_abs_prefix(&original, &base).map(|path| ClientAsset { asset_id, path })
        })
        .collect();
    let next = if more {
        assets.last().map(|a| a.path.clone())
    } else {
        None
    };
    Ok(Json(ClientAssetPage {
        root_id: q.root_id,
        linked: true,
        assets,
        next,
    }))
}

#[derive(Deserialize)]
struct ResolveQuery {
    immich_path: String,
}

async fn resolve_location(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<ResolveQuery>,
) -> AppResult<Json<Location>> {
    let roots = files::load_roots(&state.db).await?;
    let (root, rel) = files::resolve_immich_path(&roots, &q.immich_path)
        .filter(|(r, _)| p.sees_root(&r.mapping.root_id))
        .ok_or_else(|| AppError::not_found("no mapped root contains this path"))?;
    Ok(Json(root.location(&rel)))
}

#[derive(Deserialize)]
struct LocQuery {
    root_id: String,
    #[serde(default)]
    path: String,
}

async fn located(
    state: &AppState,
    p: &Principal,
    action: ClientAction,
    root_id: &str,
    path: &str,
) -> AppResult<(Root, RelPath, std::path::PathBuf)> {
    p.require(action, root_id)?;
    let loc = Location {
        root_id: root_id.to_owned(),
        path: path.to_owned(),
    };
    let (root, rel, abs) = files::locate(&state.db, &loc).await?;
    root.ensure_available()?;
    Ok((root, rel, abs))
}

async fn folders(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<LocQuery>,
) -> AppResult<Json<FolderListing>> {
    let (_, rel, abs) = located(&state, &p, ClientAction::Read, &q.root_id, &q.path).await?;
    let rel2 = rel.clone();
    let (dirs, entries) = tokio::task::spawn_blocking(move || files::list_dir(&abs, &rel2))
        .await
        .map_err(AppError::internal)?
        .map_err(|e| {
            if e.code() == "internal" {
                AppError::not_found("folder not found")
            } else {
                e
            }
        })?;
    let folders = dirs
        .into_iter()
        .map(|name| {
            let path = rel
                .join(&name)
                .map_err(|e| AppError::invalid_location(e.to_string()))?;
            Ok(FolderEntry {
                name,
                path: path.as_str().to_owned(),
            })
        })
        .collect::<AppResult<_>>()?;
    let files = entries
        .iter()
        .map(|(name, meta)| files::file_entry(&rel, name, meta))
        .collect::<AppResult<_>>()?;
    Ok(Json(FolderListing {
        root_id: q.root_id,
        path: rel.as_str().to_owned(),
        folders,
        files,
    }))
}

/// Builds the manifest of the original at `rel`: the original plus sidecars and companions.
pub async fn build_manifest(
    state: &AppState,
    root: &Root,
    rel: &RelPath,
    asset_id: Option<String>,
) -> AppResult<Manifest> {
    let name = rel
        .name()
        .ok_or_else(|| AppError::invalid_location("a manifest needs a file path"))?
        .to_owned();
    let folder = rel.parent().unwrap_or_else(RelPath::root);
    let abs = root.abs(rel)?;
    let meta = tokio::fs::metadata(&abs).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::not_found("file not found")
        } else {
            e.into()
        }
    })?;
    if !meta.is_file() {
        return Err(AppError::invalid_location("not a file"));
    }
    let folder_abs = root.abs(&folder)?;
    let folder2 = folder.clone();
    let (_, siblings) = tokio::task::spawn_blocking(move || files::list_dir(&folder_abs, &folder2))
        .await
        .map_err(AppError::internal)??;
    let names: Vec<String> = siblings.iter().map(|(n, _)| n.clone()).collect();
    let companions = companions_of(&name, &names, &state.sidecar_extensions());

    // Immich links (best effort; only when the database is readable).
    let mut conn = None;
    if let Some(server_id) = &root.server_id
        && let Ok(server) = registry::server(state, server_id).await
        && server.compatible
        && server.database_url_enc.is_some()
        && let Ok(pool) = registry::pg_pool(state, &server).await
    {
        conn = pool.acquire().await.ok();
    }
    let mut asset_id = asset_id;
    if asset_id.is_none()
        && let (Some(c), Some(path)) = (conn.as_mut(), root.immich_path(rel))
    {
        asset_id = pg::asset_by_path(c, &path)
            .await
            .ok()
            .flatten()
            .map(|a| a.id);
    }

    let mut out = Vec::new();
    let mut entries: Vec<(FileRole, String, bool)> =
        vec![(FileRole::Original, name.clone(), false)];
    entries.extend(companions.into_iter().map(|c| {
        let role = match c.role {
            CompanionRole::Sidecar => FileRole::Sidecar,
            CompanionRole::Companion => FileRole::Companion,
        };
        (role, c.name, c.shared)
    }));
    for (role, file_name, shared) in entries {
        let (_, meta) = siblings
            .iter()
            .find(|(n, _)| *n == file_name)
            .ok_or_else(|| {
                AppError::new(
                    StatusCode::PRECONDITION_FAILED,
                    "revision_changed",
                    "a file disappeared while building its manifest",
                )
            })?;
        let file_rel = folder
            .join(&file_name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?;
        let file_abs = root.abs(&file_rel)?;
        let (revision, sha) =
            tokio::task::spawn_blocking(move || files::replacement_fingerprint(&file_abs))
                .await
                .map_err(AppError::internal)?
                .map_err(|e| {
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                    ) {
                        AppError::new(
                            StatusCode::PRECONDITION_FAILED,
                            "revision_changed",
                            "a file changed while building its manifest",
                        )
                    } else {
                        e.into()
                    }
                })?
                .ok_or_else(|| {
                    AppError::new(
                        StatusCode::PRECONDITION_FAILED,
                        "revision_changed",
                        "a file disappeared while building its manifest",
                    )
                })?;
        if revision != files::revision(meta) {
            return Err(AppError::new(
                StatusCode::PRECONDITION_FAILED,
                "revision_changed",
                "a file changed while building its manifest",
            ));
        }
        let immich_linked = match (role, conn.as_mut(), root.immich_path(&file_rel)) {
            (FileRole::Sidecar, Some(c), Some(path)) => {
                pg::sidecar_by_path(c, &path).await.ok().flatten().is_some()
            }
            _ => false,
        };
        out.push(ManifestFile {
            role,
            name: file_name,
            path: file_rel.as_str().to_owned(),
            size: meta.len(),
            revision,
            sha256: Some(sha),
            immich_linked,
            shared,
        });
    }
    let stem = split_ext(&name).0;
    let group_path = if folder.is_root() {
        stem.to_owned()
    } else {
        format!("{}/{stem}", folder.as_str())
    };
    Ok(Manifest {
        group_id: format!("{}:{group_path}", root.mapping.root_id),
        asset_id,
        location: root.location(rel),
        folder: folder.as_str().to_owned(),
        files: out,
    })
}

async fn path_manifest(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<LocQuery>,
) -> AppResult<Json<Manifest>> {
    let (root, rel, _) = located(&state, &p, ClientAction::Download, &q.root_id, &q.path).await?;
    Ok(Json(build_manifest(&state, &root, &rel, None).await?))
}

/// Finds the original path of an Immich asset: database first, then the owner's API.
async fn asset_original(state: &AppState, asset_id: &str) -> AppResult<Option<String>> {
    for server in registry::servers(state).await? {
        if server.database_url_enc.is_some()
            && server.compatible
            && let Ok(pool) = registry::pg_pool(state, &server).await
            && let Ok(mut conn) = pool.acquire().await
            && let Ok(Some(asset)) = pg::asset_by_id(&mut conn, asset_id).await
        {
            return Ok(Some(asset.original_path));
        }
        let users: Vec<String> = sqlx::query_scalar(
            "SELECT immich_user_id FROM immich_accounts WHERE server_id = ? AND status = 'ok'",
        )
        .bind(&server.id)
        .fetch_all(&state.db)
        .await?;
        for user in users {
            let Ok(api) = registry::api_as_user(state, &server, &user).await else {
                continue;
            };
            if let Ok(v) = api.asset(asset_id).await
                && let Some(path) = v["originalPath"].as_str()
            {
                return Ok(Some(path.to_owned()));
            }
        }
    }
    Ok(None)
}

async fn asset_manifest(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Json<Manifest>> {
    if id.len() > 64 || !id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err(AppError::bad_request("bad_request", "invalid asset id"));
    }
    let original = asset_original(&state, &id)
        .await?
        .ok_or_else(|| AppError::not_found("asset not found"))?;
    let roots = files::load_roots(&state.db).await?;
    let (root, rel) = files::resolve_immich_path(&roots, &original)
        .ok_or_else(|| AppError::not_found("the asset is not inside a mapped root"))?;
    p.require(ClientAction::Download, &root.mapping.root_id)?;
    root.ensure_available()?;
    Ok(Json(build_manifest(&state, &root, &rel, Some(id)).await?))
}

#[derive(Deserialize)]
struct ContentQuery {
    root_id: String,
    path: String,
    revision: Option<String>,
}

async fn content(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<ContentQuery>,
    headers: HeaderMap,
) -> AppResult<Response> {
    let (_root, rel, abs) =
        located(&state, &p, ClientAction::Download, &q.root_id, &q.path).await?;
    if rel.is_root() {
        return Err(AppError::invalid_location("not a file"));
    }
    let meta = tokio::fs::metadata(&abs).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::not_found("file not found")
        } else {
            e.into()
        }
    })?;
    if !meta.is_file() {
        return Err(AppError::invalid_location("not a file"));
    }
    let revision = files::revision(&meta);
    if q.revision.as_deref().is_some_and(|r| r != revision) {
        return Err(AppError::new(
            StatusCode::PRECONDITION_FAILED,
            "revision_changed",
            "the file changed; fetch a fresh manifest",
        ));
    }
    let resp = serve_file(&abs, &headers, &revision, &[]).await?;
    // Detect ordinary metadata changes while opening; clients verify the manifest hash as well.
    let after = tokio::fs::metadata(&abs).await?;
    if files::revision(&after) != revision {
        return Err(AppError::new(
            StatusCode::PRECONDITION_FAILED,
            "revision_changed",
            "the file changed; fetch a fresh manifest",
        ));
    }
    Ok(resp)
}

async fn create_upload(
    State(state): State<AppState>,
    Client(p): Client,
    Json(req): Json<CreateUpload>,
) -> AppResult<Response> {
    let up = uploads::create(&state, &p, req).await?;
    Ok((StatusCode::CREATED, Json(up)).into_response())
}

async fn get_upload(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Response> {
    Ok(Json(uploads::get(&state, &p, &id).await?).into_response())
}

async fn cancel_upload(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Response> {
    uploads::cancel(&state, &p, &id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn put_upload_chunk(
    State(state): State<AppState>,
    Client(p): Client,
    Path((id, index)): Path<(String, usize)>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response> {
    Ok(Json(uploads::put_chunk(&state, &p, &id, index, &headers, body).await?).into_response())
}

async fn commit_upload(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Response> {
    let accepted = uploads::commit(&state, &p, &id).await?;
    Ok((StatusCode::ACCEPTED, Json(accepted)).into_response())
}

async fn preview(
    State(state): State<AppState>,
    Client(p): Client,
    Json(req): Json<OperationRequest>,
) -> AppResult<Response> {
    Ok(Json(ops::preview(&state, &p, &req).await?).into_response())
}

async fn submit(
    State(state): State<AppState>,
    Client(p): Client,
    Json(req): Json<OperationRequest>,
) -> AppResult<Response> {
    let accepted = ops::submit(&state, &p, req).await?;
    Ok((StatusCode::ACCEPTED, Json(accepted)).into_response())
}

fn check_instance(state: &AppState, instance_id: &str) -> AppResult<()> {
    if instance_id != state.instance_id {
        return Err(AppError::conflict(
            "instance_mismatch",
            "request is addressed to a different ISM instance",
        ));
    }
    Ok(())
}

async fn submit_for_instance(
    State(state): State<AppState>,
    Client(p): Client,
    Path(instance_id): Path<String>,
    Json(req): Json<OperationRequest>,
) -> AppResult<Response> {
    check_instance(&state, &instance_id)?;
    let accepted = ops::submit(&state, &p, req).await?;
    Ok((StatusCode::ACCEPTED, Json(accepted)).into_response())
}

#[derive(Deserialize)]
struct OperationKeyQuery {
    idempotency_key: String,
}

async fn find_operation(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<OperationKeyQuery>,
) -> AppResult<Response> {
    Ok(Json(ops::find_by_key(&state, &p, &q.idempotency_key).await?).into_response())
}

async fn find_operation_for_instance(
    State(state): State<AppState>,
    Client(p): Client,
    Path(instance_id): Path<String>,
    Query(q): Query<OperationKeyQuery>,
) -> AppResult<Response> {
    check_instance(&state, &instance_id)?;
    Ok(Json(ops::find_by_key(&state, &p, &q.idempotency_key).await?).into_response())
}

async fn get_operation(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Response> {
    Ok(Json(ops::get(&state, &p, &id).await?).into_response())
}

async fn get_operation_for_instance(
    State(state): State<AppState>,
    Client(p): Client,
    Path((instance_id, id)): Path<(String, String)>,
) -> AppResult<Response> {
    check_instance(&state, &instance_id)?;
    Ok(Json(ops::get(&state, &p, &id).await?).into_response())
}

async fn cancel_by_key(
    State(state): State<AppState>,
    Client(p): Client,
    Json(req): Json<CancelOperationByKey>,
) -> AppResult<Response> {
    Ok(Json(ops::cancel_by_key(&state, &p, req).await?).into_response())
}

async fn cancel_by_key_for_instance(
    State(state): State<AppState>,
    Client(p): Client,
    Path(instance_id): Path<String>,
    Json(req): Json<CancelOperationByKey>,
) -> AppResult<Response> {
    check_instance(&state, &instance_id)?;
    Ok(Json(ops::cancel_by_key(&state, &p, req).await?).into_response())
}

async fn cancel_operation(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Response> {
    Ok(Json(ops::cancel(&state, &p, &id).await?).into_response())
}

struct OpStream {
    state: AppState,
    id: String,
    cursor: i64,
    rx: broadcast::Receiver<ServerEvent>,
    resync: bool,
    done: bool,
}

/// How long after the library scan delay a finished upload's stream waits for Immich's import.
const ASSET_ID_WAIT_SECS: i64 = 30 * 60;

/// True while a finished operation has a published upload whose asset id is not known yet: its
/// stream stays open (for a bounded time) so the `item` carrying the asset id reaches listeners.
async fn awaiting_asset_id(state: &AppState, id: &str) -> bool {
    let wait = state.config.timing.scan_delay.as_secs().cast_signed() + ASSET_ID_WAIT_SECS;
    let cutoff = crate::db::ts(chrono::Utc::now() - chrono::Duration::seconds(wait));
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM operation_items i JOIN operations o ON o.id = i.operation_id
         WHERE i.operation_id = ? AND i.kind = 'publish_upload' AND i.status = 'done'
           AND i.result_json IS NOT NULL AND json_extract(i.result_json, '$.asset_id') IS NULL
           AND (o.finished_at IS NULL OR o.finished_at >= ?))",
    )
    .bind(id)
    .bind(cutoff)
    .fetch_one(&state.db)
    .await
    .unwrap_or(false)
}

async fn next_op_events(mut s: OpStream) -> Option<(Vec<Event>, OpStream)> {
    if s.done {
        return None;
    }
    loop {
        if s.resync {
            s.resync = false;
            return Some((vec![Event::default().event("resync").data("{}")], s));
        }
        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT seq, event, data FROM operation_events WHERE operation_id = ? AND seq > ? ORDER BY seq LIMIT 200",
        )
        .bind(&s.id)
        .bind(s.cursor)
        .fetch_all(&s.state.db)
        .await
        .ok()?;
        if !rows.is_empty() {
            let mut out = Vec::with_capacity(rows.len());
            for (seq, event, data) in rows {
                s.cursor = seq;
                if event == "finished" && !awaiting_asset_id(&s.state, &s.id).await {
                    s.done = true;
                }
                out.push(Event::default().id(seq.to_string()).event(event).data(data));
            }
            return Some((out, s));
        }
        // Nothing new: stop when the operation already finished, else wait for a wakeup.
        let finished = ops::load(&s.state, &s.id)
            .await
            .ok()?
            .0
            .status
            .is_terminal();
        if finished && !awaiting_asset_id(&s.state, &s.id).await {
            return None;
        }
        loop {
            match tokio::time::timeout(Duration::from_secs(15), s.rx.recv()).await {
                Ok(Ok(ServerEvent::OperationUpdated { operation_id })) if operation_id == s.id => {
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(broadcast::error::RecvError::Lagged(_))) | Err(_) => break,
                Ok(Err(broadcast::error::RecvError::Closed)) => return None,
            }
        }
    }
}

async fn operation_events(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> AppResult<Response> {
    ops::get(&state, &p, &id).await?;
    let rx = state.events.subscribe();
    let cursor = last_event_id(&headers).unwrap_or(0);
    let oldest: Option<i64> =
        sqlx::query_scalar("SELECT MIN(seq) FROM operation_events WHERE operation_id = ?")
            .bind(&id)
            .fetch_one(&state.db)
            .await?;
    let resync = cursor > 0 && oldest.is_some_and(|o| o > cursor + 1);
    let start = OpStream {
        state,
        id,
        cursor: if resync {
            oldest.unwrap_or(1) - 1
        } else {
            cursor
        },
        rx,
        resync,
        done: false,
    };
    let s = stream::unfold(start, next_op_events)
        .flat_map(|events| stream::iter(events.into_iter().map(Ok::<_, Infallible>)));
    Ok(sse(s))
}

fn visible_change(p: &Principal, c: &Change) -> bool {
    match c {
        Change::Moved { from, to } => p.sees_root(&from.root_id) || p.sees_root(&to.root_id),
        Change::Trashed { location, .. }
        | Change::Restored { location, .. }
        | Change::Created { location } => p.sees_root(&location.root_id),
        Change::Purged { .. } => true,
    }
}

async fn events(State(state): State<AppState>, Client(p): Client) -> AppResult<Response> {
    let rx = state.events.subscribe();
    let s = stream::unfold((rx, p, state), |(mut rx, p, state)| async move {
        loop {
            let ev = match rx.recv().await {
                Ok(ServerEvent::OperationCommitted(mut c)) => {
                    c.changes.retain(|ch| visible_change(&p, ch));
                    if c.changes.is_empty() {
                        continue;
                    }
                    let data = serde_json::to_string(&c).unwrap_or_default();
                    Event::default().event("operation_committed").data(data)
                }
                Ok(ServerEvent::SettingsChanged) => {
                    let settings = crate::settings::client_view(&state).await;
                    let data = serde_json::to_string(&settings).unwrap_or_default();
                    Event::default().event("settings_changed").data(data)
                }
                Ok(ServerEvent::LibraryScanned {
                    server_id,
                    library_id,
                }) => {
                    let Ok(roots) = files::load_roots(&state.db).await else {
                        continue;
                    };
                    let root_ids: Vec<String> = roots
                        .into_iter()
                        .filter(|r| {
                            r.server_id.as_deref() == Some(server_id.as_str())
                                && r.library_id.as_deref() == Some(library_id.as_str())
                                && p.sees_root(&r.mapping.root_id)
                        })
                        .map(|r| r.mapping.root_id)
                        .collect();
                    if root_ids.is_empty() {
                        continue;
                    }
                    let data = serde_json::json!({ "root_ids": root_ids }).to_string();
                    Event::default().event("assets_changed").data(data)
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    Event::default().event("resync").data("{}")
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            };
            return Some((Ok::<_, Infallible>(ev), (rx, p, state)));
        }
    });
    Ok(sse(s))
}

#[derive(Deserialize)]
struct TrashQuery {
    root_id: Option<String>,
}

async fn trash(
    State(state): State<AppState>,
    Client(p): Client,
    Query(q): Query<TrashQuery>,
) -> AppResult<Response> {
    Ok(Json(ops::trash_list(&state, &p, q.root_id.as_deref()).await?).into_response())
}
