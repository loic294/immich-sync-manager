//! Peer API (`/api/peer/v1`): signed requests between paired instances.

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{FromRequestParts, OriginalUri, Query, State};
use axum::http::HeaderMap;
use axum::http::request::Parts;
use axum::response::Response;
use axum::routing::{get, post};
use serde::Deserialize;

use super::serve_file;
use crate::auth::{PEER_HEADER, verify_peer_signature};
use crate::error::{AppError, AppResult};
use crate::files;
use crate::peers::{self, EventPage, InventoryPage, admin, net};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/pair", post(pair))
        .route("/hello", get(hello))
        .route("/events", get(events))
        .route("/inventory", get(inventory))
        .route("/content", get(content))
        .route("/settings", get(settings))
}

/// A paired peer whose GET request carries a valid signature.
pub struct PeerAuth(pub net::Peer);

impl FromRequestParts<AppState> for PeerAuth {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, AppError> {
        let sender = parts
            .headers
            .get(PEER_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| AppError::unauthorized("missing peer id"))?;
        let peer = net::peers(state)
            .await?
            .into_iter()
            .find(|p| p.instance_id.as_deref() == Some(sender))
            .ok_or_else(|| AppError::unauthorized("unknown peer"))?;
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        // Routes are nested; the signature covers the full original path.
        let full = parts
            .extensions
            .get::<OriginalUri>()
            .and_then(|u| u.0.path_and_query().map(|p| p.as_str().to_owned()))
            .unwrap_or_else(|| path_and_query.to_owned());
        verify_peer_signature(
            &peer.secret,
            parts.method.as_str(),
            &full,
            &parts.headers,
            &[],
        )?;
        Ok(Self(peer))
    }
}

async fn pair(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Json<net::Hello>> {
    let pq = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    Ok(Json(admin::pair(&state, &headers, pq, &body).await?))
}

async fn hello(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
) -> AppResult<Json<net::Hello>> {
    let shares = peers::shares(&state)
        .await?
        .into_iter()
        .filter(|s| s.includes(&peer.id))
        .map(|s| s.name)
        .collect();
    Ok(Json(net::Hello {
        instance_id: state.instance_id.clone(),
        name: state.instance_name().await,
        shares,
    }))
}

/// The shared file settings and primary designation, for the peer to merge.
async fn settings(
    State(state): State<AppState>,
    PeerAuth(_): PeerAuth,
) -> Json<ism_core::file_settings::SharedSettings> {
    Json(state.shared_settings().settings.clone())
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after: i64,
}

async fn events(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<EventsQuery>,
) -> AppResult<Json<EventPage>> {
    let instance = peer.instance_id.clone().unwrap_or_default();
    Ok(Json(
        peers::events_for(&state, &peer.id, &instance, q.after).await?,
    ))
}

#[derive(Deserialize)]
struct InventoryQuery {
    share: String,
    #[serde(default)]
    after: String,
}

async fn inventory(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<InventoryQuery>,
) -> AppResult<Json<InventoryPage>> {
    Ok(Json(
        peers::inventory_for(&state, &peer.id, &q.share, &q.after).await?,
    ))
}

#[derive(Deserialize)]
struct ContentQuery {
    share: String,
    path: String,
    sha256: String,
}

async fn content(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<ContentQuery>,
    headers: HeaderMap,
) -> AppResult<Response> {
    let share = peers::shares(&state)
        .await?
        .into_iter()
        .find(|s| s.name == q.share && s.includes(&peer.id))
        .ok_or_else(|| AppError::not_found("share not found"))?;
    if q.path.is_empty() {
        return Err(AppError::invalid_location("not a file"));
    }
    let rel = share.root_rel(&q.path)?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&rel)?;
    let meta = tokio::fs::metadata(&abs).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::not_found("file not found")
        } else {
            e.into()
        }
    })?;
    if !meta.is_file() {
        return Err(AppError::not_found("file not found"));
    }
    let path = abs.clone();
    let (revision, sha) =
        tokio::task::spawn_blocking(move || files::replacement_fingerprint(&path))
            .await
            .map_err(AppError::internal)?
            .map_err(|e| {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                ) {
                    AppError::conflict("content_changed", "the file changed during the request")
                } else {
                    e.into()
                }
            })?
            .ok_or_else(|| AppError::not_found("file not found"))?;
    if revision != files::revision(&meta) {
        return Err(AppError::conflict(
            "content_changed",
            "the file changed during the request",
        ));
    }
    if !sha.eq_ignore_ascii_case(&q.sha256) {
        return Err(AppError::conflict(
            "content_changed",
            "the file changed since the event",
        ));
    }
    let mtime = files::mtime_ns(&meta).to_string();
    let resp = serve_file(&abs, &headers, &sha, &[("x-ism-mtime-ns", mtime)]).await?;
    if files::revision(&tokio::fs::metadata(&abs).await?) != files::revision(&meta) {
        return Err(AppError::conflict(
            "content_changed",
            "the file changed during the request",
        ));
    }
    Ok(resp)
}
