//! Bounded activity feeds for the admin UI. The client API keeps its existing shapes.

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use ism_core::dto::{
    Location, OperationActivity, OperationDetail, OperationPage, OperationStatus,
    OperationTaskProgress, Progress, SyncFileDetails, SyncFileInfo, SyncPage,
};
use serde::Deserialize;
use sqlx::Row;
use tokio::sync::broadcast::error::RecvError;

use crate::auth::{Admin, session_user};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::immich::registry;
use crate::ops;
use crate::peers::net;
use crate::state::{AppState, ServerEvent};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/live", get(live))
        .route("/operations/page", get(operations))
        .route("/operations/{id}/detail", get(operation_detail))
        .route("/operations/{id}/handled", post(mark_handled))
        .route("/sync/page", get(sync))
        .route("/sync/files", get(sync_files))
        .route("/sync/thumbnail", get(sync_thumbnail))
}

#[derive(Default, Deserialize)]
pub struct PageQuery {
    pub id: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl PageQuery {
    fn bounds(&self) -> AppResult<(i64, i64)> {
        let limit = self.limit.unwrap_or(250);
        let offset = self.offset.unwrap_or(0);
        if !(1..=5000).contains(&limit) || offset < 0 {
            return Err(AppError::bad_request(
                "invalid_page",
                "limit must be 1-5000 and offset must be nonnegative",
            ));
        }
        Ok((limit, offset))
    }
}

fn operation_filter(status: Option<&str>) -> AppResult<&'static str> {
    Ok(match status {
        None | Some("") => "1",
        Some("recent") => "o.status IN ('queued','waiting_maintenance','running','completed')",
        Some("attention") => {
            "o.handled_at IS NULL AND (o.status IN ('failed','partial') OR (o.status = 'recovery_required' AND o.recovery_resolved = 0))"
        }
        Some("handled") => "o.handled_at IS NOT NULL",
        Some(
            "queued"
            | "waiting_maintenance"
            | "running"
            | "completed"
            | "partial"
            | "failed"
            | "cancelled"
            | "recovery_required",
        ) => "o.status = ?",
        _ => {
            return Err(AppError::bad_request(
                "invalid_status",
                "unknown operation status",
            ));
        }
    })
}

async fn operation_page(
    state: &AppState,
    q: &PageQuery,
    id: Option<&str>,
) -> AppResult<OperationPage> {
    let (limit, offset) = q.bounds()?;
    let filter = operation_filter(q.status.as_deref())?;
    let filter = if id.is_some() { "o.id = ?" } else { filter };
    let argument = id.or(q.status.as_deref()).unwrap_or_default();
    let mut count = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM operations o WHERE {filter}"
    )));
    if filter.contains('?') {
        count = count.bind(argument);
    }
    let total = count.fetch_one(&state.db).await?;
    let order = if q.status.as_deref() == Some("recent") {
        "CASE WHEN o.status IN ('queued','waiting_maintenance','running') THEN 0 ELSE 1 END,"
    } else {
        ""
    };
    let sql = format!(
        "SELECT o.id, o.label, o.status, o.created_at, o.finished_at, o.handled_at,
            CASE WHEN o.status = 'partial' AND o.admitted_server_ids_json = '[]' THEN 1 ELSE o.recovery_resolved END AS recovery_resolved,
            CASE WHEN o.source = 'peer' THEN 'ISM: ' || COALESCE(p.name, 'removed peer')
                 WHEN o.source = 'admin' THEN 'ISM: ' || ?
                 ELSE upper(o.source) || CASE WHEN c.name IS NULL THEN '' ELSE ': ' || c.name END END AS source,
            COUNT(i.idx) AS total,
            COALESCE(SUM(i.status NOT IN ('pending','running')),0) AS done,
            group_concat(DISTINCT CASE
                WHEN i.kind IN ('move','move_folder','rename') THEN 'Move'
                WHEN i.kind IN ('publish_upload','create_folder') THEN 'Add'
                WHEN i.kind IN ('trash','trash_folder','trash_if_asset_matches') THEN 'Trash'
                WHEN i.kind = 'restore' THEN 'Restore'
                WHEN i.kind = 'purge' THEN 'Purge' ELSE 'Update' END) AS title,
            group_concat(DISTINCT CASE WHEN s.name IS NOT NULL THEN 'Immich - ' || s.name ELSE 'ISM: ' || ? END) AS destination
         FROM (SELECT * FROM operations o WHERE {filter}
               ORDER BY {order} o.created_at DESC, o.id DESC LIMIT ? OFFSET ?) o
         LEFT JOIN operation_items i ON i.operation_id = o.id
         LEFT JOIN client_tokens c ON o.principal = 'token:' || c.id
         LEFT JOIN peers p ON o.principal = 'peer:' || p.id
         LEFT JOIN uploads u ON u.id = json_extract(i.action_json, '$.upload_id')
         LEFT JOIN trash tr ON tr.id = json_extract(i.action_json, '$.trash_id')
         LEFT JOIN mappings m ON m.root_id = COALESCE(
             json_extract(i.action_json, '$.target.root_id'),
             json_extract(u.request_json, '$.target.root_id'),
             json_extract(i.action_json, '$.source.root_id'), tr.root_id)
         LEFT JOIN immich_servers s ON s.id = m.server_id
         GROUP BY o.id ORDER BY {order} o.created_at DESC, o.id DESC"
    );
    let name = state.instance_name().await;
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(&name)
        .bind(&name);
    if filter.contains('?') {
        query = query.bind(argument);
    }
    let rows = query.bind(limit).bind(offset).fetch_all(&state.db).await?;
    let rows = rows
        .into_iter()
        .map(|r| {
            Ok(OperationActivity {
                operation_id: r.try_get("id")?,
                title: r
                    .try_get::<Option<String>, _>("title")?
                    .unwrap_or_else(|| "Update".into()),
                label: r.try_get("label")?,
                source: r.try_get("source")?,
                destination: r
                    .try_get::<Option<String>, _>("destination")?
                    .unwrap_or_else(|| format!("ISM: {name}")),
                status: OperationStatus::parse(&r.try_get::<String, _>("status")?),
                progress: Progress {
                    done: r.try_get::<i64, _>("done")? as u32,
                    total: r.try_get::<i64, _>("total")? as u32,
                },
                created_at: r.try_get("created_at")?,
                finished_at: r.try_get("finished_at")?,
                handled_at: r.try_get("handled_at")?,
                recovery_resolved: r.try_get::<i64, _>("recovery_resolved")? != 0,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok(OperationPage { rows, total })
}

async fn operations(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<PageQuery>,
) -> AppResult<Json<OperationPage>> {
    Ok(Json(operation_page(&state, &q, None).await?))
}

async fn operation_detail(
    State(state): State<AppState>,
    Admin(principal): Admin,
    Path(id): Path<String>,
) -> AppResult<Json<OperationDetail>> {
    let operation = ops::get(&state, &principal, &id).await?;
    let activity = operation_page(&state, &PageQuery::default(), Some(&id))
        .await?
        .rows
        .into_iter()
        .next()
        .ok_or_else(|| AppError::not_found("operation not found"))?;
    let rows: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT progress_json FROM operation_items WHERE operation_id = ? ORDER BY idx",
    )
    .bind(&id)
    .fetch_all(&state.db)
    .await?;
    let tasks = rows
        .into_iter()
        .map(|r| {
            r.map(|json| serde_json::from_str::<OperationTaskProgress>(&json))
                .transpose()
                .map(Option::unwrap_or_default)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(OperationDetail {
        operation,
        activity,
        tasks,
    }))
}

async fn mark_handled(
    State(state): State<AppState>,
    Admin(_): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    let op = ops::load(&state, &id).await?.0;
    if !matches!(
        op.status,
        OperationStatus::Failed | OperationStatus::Partial | OperationStatus::RecoveryRequired
    ) {
        return Err(AppError::conflict(
            "not_attention",
            "only operations needing attention can be marked handled",
        ));
    }
    sqlx::query("UPDATE operations SET handled_at = COALESCE(handled_at, ?) WHERE id = ?")
        .bind(db::now())
        .bind(&id)
        .execute(&state.db)
        .await?;
    db::audit(&state.db, "admin", "operation.handled", &id).await;
    state.emit(ServerEvent::OperationUpdated { operation_id: id });
    Ok(axum::http::StatusCode::NO_CONTENT.into_response())
}

async fn sync(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<PageQuery>,
) -> AppResult<Json<SyncPage>> {
    let (limit, offset) = q.bounds()?;
    let status = match q.status.as_deref() {
        None | Some("active") => "active",
        Some("completed") => "completed",
        _ => {
            return Err(AppError::bad_request(
                "invalid_status",
                "use active or completed",
            ));
        }
    };
    Ok(Json(
        crate::peers::activity::page(
            &state,
            if q.id.is_some() { "all" } else { status },
            limit,
            offset,
            q.id.as_deref(),
        )
        .await?,
    ))
}

#[derive(Deserialize)]
pub struct SyncFileQuery {
    pub share: String,
    pub path: String,
    pub peer_id: Option<String>,
}

pub async fn shared_location(
    state: &AppState,
    peer_id: Option<&str>,
    q: &SyncFileQuery,
) -> AppResult<Location> {
    if q.path.is_empty() {
        return Err(AppError::not_found("file not found"));
    }
    let share = crate::peers::shares(state)
        .await?
        .into_iter()
        .find(|s| s.name == q.share && peer_id.is_none_or(|id| s.includes(id)))
        .ok_or_else(|| AppError::not_found("share not found"))?;
    let rel = share.root_rel(&q.path)?;
    Ok(Location {
        root_id: share.root_id,
        path: rel.as_str().to_owned(),
    })
}

pub async fn file_details(state: &AppState, location: &Location) -> AppResult<SyncFileDetails> {
    let (root, rel, abs) = files::locate(&state.db, location).await?;
    if !tokio::fs::metadata(&abs)
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AppError::not_found("file not found")
            } else {
                e.into()
            }
        })?
        .is_file()
    {
        return Err(AppError::invalid_location("not a file"));
    }
    let folder = rel.parent().unwrap_or_else(ism_core::paths::RelPath::root);
    let folder_abs = root.abs(&folder)?;
    let folder2 = folder.clone();
    let (_, entries) = tokio::task::spawn_blocking(move || files::list_dir(&folder_abs, &folder2))
        .await
        .map_err(AppError::internal)??;
    let junk = state.junk_rules();
    let names: Vec<String> = entries
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| !junk.is_junk(name))
        .collect();
    let name = rel
        .name()
        .ok_or_else(|| AppError::invalid_location("not a file"))?;
    let extensions = state.sidecar_extensions();
    let mut selected = vec![(
        name.to_owned(),
        ism_core::sidecar::companion_role(name, &extensions)
            .map_or("original".to_owned(), |r| r.as_str().to_owned()),
    )];
    for original in names
        .iter()
        .filter(|n| !ism_core::sidecar::is_companion_name(n, &extensions))
    {
        let companions = ism_core::sidecar::companions_of(original, &names, &extensions);
        if original == name || companions.iter().any(|c| c.name == name) {
            selected.push((original.clone(), "original".into()));
            selected.extend(
                companions
                    .into_iter()
                    .map(|c| (c.name, c.role.as_str().to_owned())),
            );
        }
    }
    selected.sort();
    selected.dedup();
    Ok(SyncFileDetails {
        folder: root.abs(&folder)?.to_string_lossy().into_owned(),
        files: selected
            .into_iter()
            .map(|(name, role)| {
                let path = folder
                    .join(&name)
                    .map_err(|e| AppError::invalid_location(e.to_string()))?;
                Ok(SyncFileInfo {
                    path: path.as_str().to_owned(),
                    role,
                })
            })
            .collect::<AppResult<Vec<_>>>()?,
    })
}

pub async fn thumbnail(state: &AppState, location: &Location) -> AppResult<Response> {
    let (root, rel, _) = files::locate(&state.db, location).await?;
    let path = root
        .immich_path(&rel)
        .ok_or_else(|| AppError::not_found("no Immich thumbnail for this file"))?;
    let server = registry::server(
        state,
        root.server_id
            .as_deref()
            .ok_or_else(|| AppError::not_found("no Immich server for this file"))?,
    )
    .await?;
    let owner = registry::library_owner(
        state,
        root.library_id
            .as_deref()
            .ok_or_else(|| AppError::not_found("no Immich library for this file"))?,
    )
    .await?
    .ok_or_else(|| AppError::not_found("library owner not found"))?;
    let api = registry::api_as_user(state, &server, &owner).await?;
    let id = api
        .asset_id_by_path(&path)
        .await
        .map_err(|e| AppError::conflict("thumbnail_unavailable", e.to_string()))?
        .ok_or_else(|| AppError::not_found("Immich has not indexed this file yet"))?;
    let response = api
        .thumbnail(&id)
        .await
        .map_err(|e| AppError::conflict("thumbnail_unavailable", e.to_string()))?;
    image_response(response).await
}

async fn image_response(mut response: reqwest::Response) -> AppResult<Response> {
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if !matches!(
        content_type.as_str(),
        "image/jpeg" | "image/webp" | "image/png" | "image/avif"
    ) {
        return Err(AppError::conflict(
            "thumbnail_unavailable",
            "the preview was not a supported image",
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(AppError::internal)? {
        if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
            return Err(AppError::conflict(
                "thumbnail_unavailable",
                "the preview exceeds 4 MiB",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(([(header::CONTENT_TYPE, content_type)], bytes).into_response())
}

async fn remote_file(
    state: &AppState,
    peer_id: &str,
    endpoint: &str,
    q: &SyncFileQuery,
) -> AppResult<reqwest::Response> {
    let peer = net::peer(state, peer_id).await?;
    // Verify that this peer is allowed to see the same share before proxying.
    shared_location(state, Some(peer_id), q).await?;
    let query = reqwest::Url::parse_with_params(
        "http://ism.example",
        [("share", &q.share), ("path", &q.path)],
    )
    .map_err(AppError::internal)?;
    let path = format!(
        "/api/peer/v1/{endpoint}?{}",
        query.query().unwrap_or_default()
    );
    let response = net::request(
        state,
        &peer.url,
        &peer.secret,
        reqwest::Method::GET,
        &path,
        None,
    )
    .send()
    .await
    .map_err(|e| AppError::conflict("peer_unreachable", net::net_error(&e)))?;
    if !response.status().is_success() {
        return Err(AppError::conflict(
            "peer_preview_unavailable",
            format!(
                "The peer could not provide {endpoint} (HTTP {}). Both peers must support activity previews.",
                response.status().as_u16()
            ),
        ));
    }
    Ok(response)
}

async fn sync_files(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<SyncFileQuery>,
) -> AppResult<Json<SyncFileDetails>> {
    if let Some(peer_id) = &q.peer_id {
        let details = remote_file(&state, peer_id, "file-info", &q)
            .await?
            .json()
            .await
            .map_err(AppError::internal)?;
        return Ok(Json(details));
    }
    let location = shared_location(&state, None, &q).await?;
    Ok(Json(file_details(&state, &location).await?))
}

async fn sync_thumbnail(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<SyncFileQuery>,
) -> AppResult<Response> {
    if let Some(peer_id) = &q.peer_id {
        let response = remote_file(&state, peer_id, "thumbnail", &q).await?;
        return image_response(response).await;
    }
    let location = shared_location(&state, None, &q).await?;
    thumbnail(&state, &location).await
}

async fn live(
    State(state): State<AppState>,
    Admin(_): Admin,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> AppResult<Response> {
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin
            .to_str()
            .ok()
            .and_then(|s| reqwest::Url::parse(s).ok())
            .ok_or_else(|| AppError::forbidden("invalid WebSocket origin"))?;
        let host = headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default();
        let origin_host = origin.host().map(|h| h.to_string()).unwrap_or_default();
        let authority = origin.port().map_or_else(
            || origin_host.clone(),
            |port| format!("{origin_host}:{port}"),
        );
        if !authority.eq_ignore_ascii_case(host) {
            return Err(AppError::forbidden(
                "cross-origin WebSocket connection refused",
            ));
        }
    }
    let mut rx = state.events.subscribe();
    Ok(ws.on_upgrade(move |mut socket| async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                event = rx.recv() => {
                    let value = match event {
                        Ok(event) => match serde_json::to_string(&event) {
                            Ok(value) => value,
                            Err(error) => { tracing::error!("serialize live event: {error}"); break; }
                        },
                        Err(RecvError::Lagged(_)) => r#"{"type":"resync"}"#.into(),
                        Err(RecvError::Closed) => break,
                    };
                    if socket.send(Message::Text(value.into())).await.is_err() { break; }
                }
                message = socket.recv() => match message {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                },
                _ = tick.tick() => {
                    if !matches!(session_user(&state, &headers).await, Ok(Some(_))) { break; }
                    if socket.send(Message::Ping(Vec::new().into())).await.is_err() { break; }
                }
            }
        }
    }).into_response())
}
