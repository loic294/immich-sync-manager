//! Lifecycle-scoped JSON snapshots over the authenticated admin WebSocket.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, Request, header};
use axum::response::{IntoResponse, Response};
use futures_util::SinkExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;
use tower::ServiceExt;

use crate::auth::{Admin, session_user};
use crate::error::{AppError, AppResult};
use crate::state::{AppState, ServerEvent};

const MAX_SUBSCRIPTIONS: usize = 128;
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Subscribe { id: u32, url: String },
    Refresh { id: u32 },
    Unsubscribe { id: u32 },
}

struct Subscription {
    url: String,
    last: Option<Value>,
    dirty: bool,
    force: bool,
    recheck: Option<Instant>,
}

fn affected(url: &str, event: &ServerEvent) -> bool {
    let path = url.split('?').next().unwrap_or(url);
    match path {
        "/overview" => !matches!(event, ServerEvent::ActivityChanged),
        "/operations/background" | "/attention" => true,
        "/operations/page" => matches!(
            event,
            ServerEvent::OperationUpdated { .. } | ServerEvent::OperationCommitted(_)
        ),
        path if path.starts_with("/operations/") => {
            matches!(
                event,
                ServerEvent::OperationUpdated { operation_id }
                    if path == format!("/operations/{operation_id}/detail")
            ) || matches!(
                event,
                ServerEvent::OperationCommitted(_) | ServerEvent::SyncChanged
            )
        }
        "/settings" => matches!(event, ServerEvent::SettingsChanged),
        "/servers" | "/accounts" | "/libraries" => matches!(
            event,
            ServerEvent::MaintenanceChanged { .. } | ServerEvent::SettingsChanged
        ),
        "/mappings" => matches!(
            event,
            ServerEvent::WatchChanged | ServerEvent::SettingsChanged
        ),
        "/trash" => matches!(event, ServerEvent::OperationCommitted(_)),
        _ => !matches!(event, ServerEvent::ActivityChanged),
    }
}

fn recheck(url: &str, data: &Value) -> Option<Instant> {
    if url != "/attention" {
        return None;
    }
    let now = chrono::Utc::now();
    let next = data["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            item["recheck_at"]
                .as_str()
                .or_else(|| item["next_at"].as_str())
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                .and_then(|at| (at.with_timezone(&chrono::Utc) - now).to_std().ok())
        })
        .min();
    let incomplete = data["completeness"] != "complete"
        || data["warnings"]
            .as_array()
            .is_some_and(|warnings| !warnings.is_empty());
    let next = if incomplete {
        Some(
            next.unwrap_or(Duration::from_secs(60))
                .min(Duration::from_secs(60)),
        )
    } else {
        next
    };
    next.map(|delay| Instant::now() + delay.max(Duration::from_secs(1)))
}

async fn snapshot(router: &Router, headers: &HeaderMap, url: &str) -> AppResult<Value> {
    if !url.starts_with('/') || url.starts_with("//") || url.len() > 4096 {
        return Err(AppError::bad_request(
            "invalid_subscription",
            "invalid snapshot URL",
        ));
    }
    let mut request = Request::builder()
        .uri(url)
        .body(Body::empty())
        .map_err(|_| AppError::bad_request("invalid_subscription", "invalid snapshot URL"))?;
    // Dispatch only through the read-only router, with the original session checked on every read.
    *request.headers_mut() = headers.clone();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), MAX_SNAPSHOT_BYTES)
        .await
        .map_err(AppError::internal)?;
    let data = if body.is_empty() {
        json!({ "error": { "code": "invalid_subscription", "message": "This resource is not available over WebSocket." } })
    } else {
        serde_json::from_slice::<Value>(&body).map_err(AppError::internal)?
    };
    Ok(json!({ "status": status, "data": data }))
}

async fn send(socket: &mut WebSocket, value: &Value) -> bool {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .is_ok()
}

pub(super) async fn live(
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
    let router = super::admin::live_reads().with_state(state.clone());
    let mut rx = state.events.subscribe();
    Ok(ws.max_message_size(8192).on_upgrade(move |mut socket| async move {
        if !send(&mut socket, &json!({ "type": "ready", "protocol": 1 })).await { return; }
        let mut subscriptions = BTreeMap::<u32, Subscription>::new();
        let mut refresh = tokio::time::interval(REFRESH_INTERVAL);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                event = rx.recv() => {
                    let value = match event {
                        Ok(event) => {
                            for subscription in subscriptions.values_mut() {
                                subscription.dirty |= affected(&subscription.url, &event);
                            }
                            match serde_json::to_value(&event) {
                                Ok(value) => value,
                                Err(error) => { tracing::error!("serialize live event: {error}"); break; }
                            }
                        }
                        Err(RecvError::Lagged(_)) => {
                            for subscription in subscriptions.values_mut() { subscription.dirty = true; }
                            json!({ "type": "resync" })
                        }
                        Err(RecvError::Closed) => break,
                    };
                    if !send(&mut socket, &value).await { break; }
                }
                message = socket.recv() => match message {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<Command>(&text) {
                            Ok(Command::Subscribe { id, url }) => {
                                if subscriptions.len() >= MAX_SUBSCRIPTIONS && !subscriptions.contains_key(&id) {
                                    if !send(&mut socket, &json!({
                                        "type": "snapshot", "id": id, "status": 429,
                                        "data": { "error": { "code": "subscription_limit", "message": "Too many live subscriptions." } }
                                    })).await { break; }
                                } else {
                                    subscriptions.insert(id, Subscription {
                                        url, last: None, dirty: true, force: true, recheck: None,
                                    });
                                }
                            }
                            Ok(Command::Refresh { id }) => {
                                if let Some(subscription) = subscriptions.get_mut(&id) {
                                    subscription.dirty = true;
                                    subscription.force = true;
                                }
                            }
                            Ok(Command::Unsubscribe { id }) => { subscriptions.remove(&id); }
                            Err(error) => {
                                tracing::warn!(%error, "invalid live command");
                                if !send(&mut socket, &json!({
                                    "type": "protocol_error", "message": "Invalid live subscription command."
                                })).await { break; }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                },
                _ = refresh.tick() => {
                    for (id, subscription) in &mut subscriptions {
                        if !subscription.dirty && subscription.recheck.is_none_or(|at| at > Instant::now()) {
                            continue;
                        }
                        subscription.dirty = false;
                        let result = tokio::time::timeout(
                            Duration::from_secs(15), snapshot(&router, &headers, &subscription.url),
                        ).await.unwrap_or_else(|_| Err(AppError::new(
                            axum::http::StatusCode::GATEWAY_TIMEOUT, "snapshot_timeout", "Live snapshot timed out.",
                        )));
                        let value = match result {
                            Ok(value) => value,
                            Err(error) => json!({
                                "status": error.status().as_u16(),
                                "data": { "error": { "code": error.code(), "message": error.message() } }
                            }),
                        };
                        subscription.recheck = recheck(&subscription.url, &value["data"]);
                        let mut comparable = value.clone();
                        if let Some(data) = comparable["data"].as_object_mut() { data.remove("observed_at"); }
                        if subscription.force || subscription.last.as_ref() != Some(&comparable) {
                            if !send(&mut socket, &json!({
                                "type": "snapshot", "id": id, "status": value["status"], "data": value["data"],
                            })).await { return; }
                            subscription.last = Some(comparable);
                        }
                        subscription.force = false;
                        if value["status"] == 401 { let _ = socket.close().await; return; }
                    }
                }
                _ = heartbeat.tick() => {
                    if !matches!(session_user(&state, &headers).await, Ok(Some(_))) { break; }
                    if socket.send(Message::Ping(Vec::new().into())).await.is_err() { break; }
                }
            }
        }
    }).into_response())
}
