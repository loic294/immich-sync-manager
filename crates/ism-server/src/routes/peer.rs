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
        .route("/notifications", get(super::peer_events::notifications))
        .route("/inventory", get(inventory))
        .route("/content", get(content))
        .route("/recovery/retained", get(retained_discovery))
        .route("/recovery/retained/check", get(retained_check))
        .route("/recovery/retained/content", get(retained_content))
        .route("/file-info", get(file_info))
        .route("/thumbnail", get(thumbnail))
        .route("/settings", get(settings))
        .route("/sync-activity", get(sync_activity))
        .route("/recovery", get(recovery_page).post(recovery))
        .route("/recovery/file", get(recovery_file))
        .route("/recovery/versions", get(recovery_versions))
        .route("/recovery/preview", get(recovery_preview))
        .route("/recovery/decisions", post(recovery_decisions))
}

async fn recovery_versions(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(query): Query<peers::versions::Query>,
) -> AppResult<Json<ism_core::dto::RecoveryVersions>> {
    let target = query.target();
    peers::recovery::authorize_peer(
        &state,
        &peer,
        &ism_core::dto::RecoveryBatchInput {
            targets: vec![target.clone()],
            all: false,
            resolution: None,
        },
    )
    .await?;
    Ok(Json(peers::versions::get(&state, &target).await?))
}

async fn recovery_preview(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(query): Query<peers::versions::PreviewQuery>,
) -> AppResult<Response> {
    peers::recovery::authorize_peer(
        &state,
        &peer,
        &ism_core::dto::RecoveryBatchInput {
            targets: vec![query.target.target()],
            all: false,
            resolution: None,
        },
    )
    .await?;
    peers::versions::preview(&state, &query).await
}

async fn recovery_decisions(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Json<ism_core::dto::RecoveryBatchResult>> {
    let sender = headers
        .get(PEER_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| AppError::unauthorized("missing peer id"))?;
    let peer = net::peers(&state)
        .await?
        .into_iter()
        .find(|peer| peer.instance_id.as_deref() == Some(sender))
        .ok_or_else(|| AppError::unauthorized("unknown peer"))?;
    verify_peer_signature(
        &peer.secret,
        "POST",
        uri.path_and_query().map_or(uri.path(), |pq| pq.as_str()),
        &headers,
        &body,
    )?;
    let input: ism_core::dto::RecoveryDecisionInput = serde_json::from_slice(&body)?;
    peers::recovery::authorize_peer(
        &state,
        &peer,
        &ism_core::dto::RecoveryBatchInput {
            targets: input
                .decisions
                .iter()
                .map(|decision| decision.target.clone())
                .collect(),
            all: false,
            resolution: Some(input.resolution),
        },
    )
    .await?;
    Ok(Json(
        peers::recovery::submit_decisions(&state, input).await?,
    ))
}

async fn recovery_page(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
) -> AppResult<Json<ism_core::dto::RecoveryPage>> {
    Ok(Json(peers::recovery::peer_page(&state, &peer).await?))
}

#[derive(Deserialize)]
struct RecoveryFileQuery {
    share: String,
    path: String,
    vv: String,
    origin: Option<String>,
    origin_seq: Option<i64>,
}

async fn recovery_file(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<RecoveryFileQuery>,
) -> AppResult<Json<peers::recovery::FileEvidence>> {
    let vv = serde_json::from_str(&q.vv)?;
    let event = match (&q.origin, q.origin_seq) {
        (Some(origin), Some(seq)) => Some((origin.as_str(), seq)),
        (None, None) => None,
        _ => {
            return Err(AppError::bad_request(
                "invalid_recovery_evidence",
                "event identity must include origin and sequence",
            ));
        }
    };
    // Evidence is observational and checks file revisions. Taking the mutation lock here
    // blocks comparisons behind entire scans; decisions revalidate under that lock later.
    Ok(Json(
        peers::recovery::evidence_at_event(&state, Some(&peer.id), &q.share, &q.path, &vv, event)
            .await?,
    ))
}

async fn recovery(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Json<ism_core::dto::RecoveryBatchResult>> {
    let sender = headers
        .get(PEER_HEADER)
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| AppError::unauthorized("missing peer id"))?;
    let peer = net::peers(&state)
        .await?
        .into_iter()
        .find(|peer| peer.instance_id.as_deref() == Some(sender))
        .ok_or_else(|| AppError::unauthorized("unknown peer"))?;
    verify_peer_signature(
        &peer.secret,
        "POST",
        uri.path_and_query().map_or(uri.path(), |pq| pq.as_str()),
        &headers,
        &body,
    )?;
    let input: ism_core::dto::RecoveryBatchInput = serde_json::from_slice(&body)?;
    peers::recovery::authorize_peer(&state, &peer, &input).await?;
    Ok(Json(peers::recovery::submit(&state, input).await?))
}

#[derive(Deserialize)]
struct ActivityQuery {
    #[serde(default)]
    after: i64,
}

async fn sync_activity(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<ActivityQuery>,
) -> AppResult<Json<peers::activity::Changes>> {
    Ok(Json(
        peers::activity::changes(&state, &peer.id, q.after).await?,
    ))
}

async fn file_info(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<super::activity::SyncFileQuery>,
) -> AppResult<Json<ism_core::dto::SyncFileDetails>> {
    let location = super::activity::shared_location(&state, Some(&peer.id), &q).await?;
    Ok(Json(
        super::activity::file_details(&state, &location).await?,
    ))
}

async fn thumbnail(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(q): Query<super::activity::SyncFileQuery>,
) -> AppResult<Response> {
    let location = super::activity::shared_location(&state, Some(&peer.id), &q).await?;
    super::activity::thumbnail(&state, &location).await
}

/// A paired peer whose GET request carries a valid signature.
pub struct PeerAuth(pub net::Peer);

async fn retained_discovery(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(wanted): Query<peers::retained::Wanted>,
) -> AppResult<Json<peers::retained::Discovery>> {
    let _guard = state.replication_operation_lock.lock().await;
    Ok(Json(
        peers::retained::discover(&state, Some(&peer.id), &wanted).await?,
    ))
}

#[derive(Deserialize)]
struct RetainedToken {
    token: String,
}

async fn retained_check(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(query): Query<RetainedToken>,
) -> AppResult<Json<peers::retained::Wanted>> {
    let _guard = state.replication_operation_lock.lock().await;
    let (_, wanted) = peers::retained::resolve(&state, Some(&peer.id), &query.token).await?;
    Ok(Json(wanted))
}

async fn retained_content(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
    Query(query): Query<RetainedToken>,
    headers: HeaderMap,
) -> AppResult<Response> {
    let _guard = state.replication_operation_lock.lock().await;
    let (path, wanted) = peers::retained::resolve(&state, Some(&peer.id), &query.token).await?;
    let meta = tokio::fs::metadata(&path).await?;
    serve_file(
        &path,
        &headers,
        &wanted.sha256,
        &[("x-ism-mtime-ns", files::mtime_ns(&meta).to_string())],
    )
    .await
}

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
    let tracked = peers::row(&state, &share.id, &q.path).await?;
    if !tracked.is_some_and(|row| {
        !row.deleted
            && row.size == meta.len() as i64
            && row.mtime_ns == files::mtime_ns(&meta)
            && row
                .sha256
                .is_some_and(|sha| sha.eq_ignore_ascii_case(&q.sha256))
    }) {
        return Err(AppError::conflict(
            "content_changed",
            "the file changed since the event",
        ));
    }
    if meta.len() <= crate::peers::transfer::DOWNLOAD_CHUNK_SIZE {
        let path = abs.clone();
        let fingerprint =
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
                })?;
        let (revision, sha) = fingerprint.ok_or_else(|| AppError::not_found("file not found"))?;
        if revision != files::revision(&meta) || !sha.eq_ignore_ascii_case(&q.sha256) {
            return Err(AppError::conflict(
                "content_changed",
                "the file changed since the event",
            ));
        }
    }
    let mtime = files::mtime_ns(&meta).to_string();
    let resp = serve_file(&abs, &headers, &q.sha256, &[("x-ism-mtime-ns", mtime)]).await?;
    if files::revision(&tokio::fs::metadata(&abs).await?) != files::revision(&meta) {
        return Err(AppError::conflict(
            "content_changed",
            "the file changed during the request",
        ));
    }
    Ok(resp)
}
