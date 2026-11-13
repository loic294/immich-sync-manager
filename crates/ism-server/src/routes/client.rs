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
    Change, ClientAction, ClientInfo, ClientRoot, CreateUpload, FileRole, FolderEntry,
    FolderListing, Location, Manifest, ManifestFile, OperationRequest,
};
use ism_core::paths::{RelPath, split_ext};
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
        .route("/locations/resolve", get(resolve_location))
        .route("/folders", get(folders))
        .route("/assets/{id}/manifest", get(asset_manifest))
        .route("/manifest", get(path_manifest))
        .route("/content", get(content))
        .route("/uploads", post(create_upload))
        .route("/uploads/{id}", get(get_upload))
        .route("/uploads/{id}/files/{index}", put(put_upload_chunk))
        .route("/uploads/{id}/commit", post(commit_upload))
        .route("/operations/preview", post(preview))
        .route("/operations", post(submit))
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
            let direct_writes = r
                .server_id
                .as_deref()
                .and_then(|id| servers.iter().find(|s| s.id == id))
                .map(registry::direct_writes)
                .unwrap_or(ism_core::dto::DirectWrites::Disabled);
            ClientRoot {
                available: r.available(),
                root_id: r.mapping.root_id.clone(),
                name: r.name.clone(),
                immich_import_path: r.mapping.immich_import_path.clone(),
                opm_prefix: r.mapping.opm_prefix.clone(),
                direct_writes,
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
    let meta = tokio::fs::metadata(&abs)
        .await
        .map_err(|_| AppError::not_found("file not found"))?;
    if !meta.is_file() {
        return Err(AppError::invalid_location("not a file"));
    }
    let folder_abs = root.abs(&folder)?;
    let folder2 = folder.clone();
    let (_, siblings) = tokio::task::spawn_blocking(move || files::list_dir(&folder_abs, &folder2))
        .await
        .map_err(AppError::internal)??;
    let names: Vec<String> = siblings.iter().map(|(n, _)| n.clone()).collect();
    let companions = companions_of(&name, &names, &files::companion_extensions(&state.db).await);

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
        let Some((_, meta)) = siblings.iter().find(|(n, _)| *n == file_name) else {
            continue;
        };
        let file_rel = folder
            .join(&file_name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?;
        let file_abs = root.abs(&file_rel)?;
        let sha =
            files::cached_sha256(&state.db, &root.mapping.root_id, &file_rel, &file_abs).await?;
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
            revision: files::revision(meta),
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
    let (root, rel, abs) = located(&state, &p, ClientAction::Download, &q.root_id, &q.path).await?;
    if rel.is_root() {
        return Err(AppError::invalid_location("not a file"));
    }
    let meta = tokio::fs::metadata(&abs)
        .await
        .map_err(|_| AppError::not_found("file not found"))?;
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
    let known: Option<String> = sqlx::query_scalar(
        "SELECT sha256 FROM file_hashes WHERE root_id = ? AND path = ? AND size = ? AND mtime_ns = ?",
    )
    .bind(&root.mapping.root_id)
    .bind(rel.as_str())
    .bind(meta.len() as i64)
    .bind(files::mtime_ns(&meta))
    .fetch_optional(&state.db)
    .await?;
    let extra: Vec<(&'static str, String)> =
        known.map(|s| ("x-ism-sha256", s)).into_iter().collect();
    let resp = serve_file(&abs, &headers, &revision, &extra).await?;
    // The file may have changed while we opened it; never serve a mix of revisions.
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

async fn get_operation(
    State(state): State<AppState>,
    Client(p): Client,
    Path(id): Path<String>,
) -> AppResult<Response> {
    Ok(Json(ops::get(&state, &p, &id).await?).into_response())
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
                if event == "finished" {
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
        if finished {
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
    let s = stream::unfold((rx, p), |(mut rx, p)| async move {
        loop {
            match rx.recv().await {
                Ok(ServerEvent::OperationCommitted(mut c)) => {
                    c.changes.retain(|ch| visible_change(&p, ch));
                    if c.changes.is_empty() {
                        continue;
                    }
                    let data = serde_json::to_string(&c).unwrap_or_default();
                    let ev = Event::default().event("operation_committed").data(data);
                    return Some((Ok::<_, Infallible>(ev), (rx, p)));
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Some((Ok(Event::default().event("resync").data("{}")), (rx, p)));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
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
