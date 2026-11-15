//! Signed HTTP between paired instances.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::auth::{PEER_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER};
use crate::error::{AppError, AppResult};
use crate::state::{AppState, ServerEvent};

pub const SECRET_PURPOSE: &str = "peer-secret";

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub url: String,
    pub instance_id: Option<String>,
    pub secret: String,
    pub cursor: i64,
}

pub async fn peers(state: &AppState) -> AppResult<Vec<Peer>> {
    let rows: Vec<(String, String, String, Option<String>, String, i64)> = sqlx::query_as(
        "SELECT id, name, url, instance_id, secret_enc, cursor FROM peers ORDER BY name",
    )
    .fetch_all(&state.db)
    .await?;
    rows.into_iter()
        .map(|(id, name, url, instance_id, secret, cursor)| {
            Ok(Peer {
                secret: state
                    .secrets
                    .decrypt(SECRET_PURPOSE, &secret)
                    .map_err(AppError::internal)?,
                id,
                name,
                url,
                instance_id,
                cursor,
            })
        })
        .collect()
}

pub async fn peer(state: &AppState, id: &str) -> AppResult<Peer> {
    peers(state)
        .await?
        .into_iter()
        .find(|p| p.id == id)
        .ok_or_else(|| AppError::not_found("peer not found"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub instance_id: String,
    pub name: String,
    pub shares: Vec<String>,
}

/// Sends a signed request; the path must start with `/api/peer/v1/`.
pub fn request(
    state: &AppState,
    peer_url: &str,
    secret: &str,
    method: Method,
    path_and_query: &str,
    body: Option<Vec<u8>>,
) -> reqwest::RequestBuilder {
    let ts = chrono::Utc::now().timestamp();
    let body = body.unwrap_or_default();
    let sig = crate::crypto::sign_request(secret, method.as_str(), path_and_query, ts, &body);
    let url = format!("{}{}", peer_url.trim_end_matches('/'), path_and_query);
    let mut req = state
        .http
        .request(method, url)
        .header(PEER_HEADER, &state.instance_id)
        .header(TIMESTAMP_HEADER, ts.to_string())
        .header(SIGNATURE_HEADER, sig);
    if !body.is_empty() {
        req = req.header("content-type", "application/json").body(body);
    }
    req
}

pub fn net_error(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "timed out".into()
    } else if e.is_connect() {
        "unreachable".into()
    } else {
        let mut msg = e.to_string();
        if let Some(url) = e.url() {
            msg = msg.replace(url.as_str(), "<peer>");
        }
        crate::error::redact(&msg)
    }
}

pub async fn get_json<T: serde::de::DeserializeOwned>(
    state: &AppState,
    peer: &Peer,
    path: &str,
) -> Result<T, String> {
    let resp = request(state, &peer.url, &peer.secret, Method::GET, path, None)
        .send()
        .await
        .map_err(|e| net_error(&e))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "HTTP {}: {}",
            status.as_u16(),
            crate::error::redact(&body.chars().take(300).collect::<String>())
        ));
    }
    resp.json().await.map_err(|e| net_error(&e))
}

pub async fn mark(state: &AppState, peer_id: &str, result: Result<(), String>) {
    let now = crate::db::now();
    let updated = match result {
        Ok(()) => {
            let changed = sqlx::query(
                "UPDATE peers SET status = 'online', last_seen_at = ? WHERE id = ? AND status != 'online'",
            )
            .bind(&now)
            .bind(peer_id)
            .execute(&state.db)
            .await;
            if changed
                .as_ref()
                .is_ok_and(|result| result.rows_affected() == 0)
                && let Err(error) = sqlx::query(
                    "UPDATE peers SET last_seen_at = ? WHERE id = ? AND status = 'online'",
                )
                .bind(now)
                .bind(peer_id)
                .execute(&state.db)
                .await
            {
                tracing::error!(peer = %peer_id, "could not record peer last-seen time: {error}");
            }
            changed
        }
        Err(e) => {
            sqlx::query(
                "UPDATE peers SET status = 'offline', last_error = ?
                 WHERE id = ? AND (status != 'offline' OR last_error IS NOT ?)",
            )
            .bind(&e)
            .bind(peer_id)
            .bind(&e)
            .execute(&state.db)
            .await
        }
    };
    match updated {
        Ok(result) if result.rows_affected() > 0 => state.emit(ServerEvent::SyncChanged),
        Ok(_) => {}
        Err(error) => {
            tracing::error!(peer = %peer_id, "could not record peer connection status: {error}");
        }
    }
}

pub async fn mark_processing_result(state: &AppState, peer_id: &str, error: Option<&str>) {
    let updated =
        sqlx::query("UPDATE peers SET last_error = ? WHERE id = ? AND last_error IS NOT ?")
            .bind(error)
            .bind(peer_id)
            .bind(error)
            .execute(&state.db)
            .await;
    match updated {
        Ok(result) if result.rows_affected() > 0 => state.emit(ServerEvent::SyncChanged),
        Ok(_) => {}
        Err(error) => {
            tracing::error!(peer = %peer_id, "could not record peer processing status: {error}");
        }
    }
}
