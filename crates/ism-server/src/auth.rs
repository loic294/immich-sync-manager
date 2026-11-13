//! Authentication: admin sessions (cookie + CSRF header), client bearer tokens, peer signatures.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, Method};
use chrono::{Duration, Utc};
use ism_core::dto::ClientAction;

use crate::crypto::{constant_time_eq, sha256_hex};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

pub const SESSION_COOKIE: &str = "ism_session";
pub const CSRF_HEADER: &str = "x-ism-request";
pub const SESSION_DAYS: i64 = 14;

/// Who performs a request. Every managed operation records its principal.
#[derive(Debug, Clone)]
pub enum Principal {
    Admin { username: String },
    Client(ClientGrant),
    Peer { peer_id: String },
}

#[derive(Debug, Clone)]
pub struct ClientGrant {
    pub token_id: String,
    pub kind: String,
    /// Empty means every root.
    pub roots: Vec<String>,
    pub actions: Vec<ClientAction>,
}

impl Principal {
    pub fn key(&self) -> String {
        match self {
            Self::Admin { username } => format!("admin:{username}"),
            Self::Client(grant) => format!("token:{}", grant.token_id),
            Self::Peer { peer_id } => format!("peer:{peer_id}"),
        }
    }

    pub fn source(&self) -> String {
        match self {
            Self::Admin { .. } => "admin".into(),
            Self::Client(grant) => grant.kind.clone(),
            Self::Peer { .. } => "peer".into(),
        }
    }

    pub fn can(&self, action: ClientAction, root_id: &str) -> bool {
        match self {
            Self::Admin { .. } | Self::Peer { .. } => true,
            Self::Client(grant) => {
                grant.actions.contains(&action)
                    && (grant.roots.is_empty() || grant.roots.iter().any(|r| r == root_id))
            }
        }
    }

    pub fn require(&self, action: ClientAction, root_id: &str) -> AppResult<()> {
        if self.can(action, root_id) {
            Ok(())
        } else {
            Err(AppError::forbidden(format!(
                "this credential may not '{}' on root '{root_id}'",
                action.as_str()
            )))
        }
    }

    pub fn sees_root(&self, root_id: &str) -> bool {
        match self {
            Self::Client(grant) => {
                grant.roots.is_empty() || grant.roots.iter().any(|r| r == root_id)
            }
            _ => true,
        }
    }
}

pub fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(axum::http::header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

pub async fn session_user(state: &AppState, headers: &HeaderMap) -> AppResult<Option<String>> {
    let Some(token) = cookie(headers, SESSION_COOKIE) else {
        return Ok(None);
    };
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT u.username, s.expires_at FROM sessions s JOIN admin_users u ON u.id = s.user_id
         WHERE s.token_hash = ?",
    )
    .bind(sha256_hex(token.as_bytes()))
    .fetch_optional(&state.db)
    .await?;
    Ok(row
        .filter(|(_, expires)| db::parse_ts(expires).is_some_and(|t| t > Utc::now()))
        .map(|(user, _)| user))
}

pub async fn create_session(state: &AppState, user_id: i64) -> AppResult<String> {
    let token = crate::crypto::random_secret(32);
    sqlx::query(
        "INSERT INTO sessions (token_hash, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
    )
    .bind(sha256_hex(token.as_bytes()))
    .bind(user_id)
    .bind(db::now())
    .bind(db::ts(Utc::now() + Duration::days(SESSION_DAYS)))
    .execute(&state.db)
    .await?;
    Ok(token)
}

pub fn session_cookie(state: &AppState, token: &str, max_age: i64) -> String {
    let secure = if state.config.secure_cookies {
        "; Secure"
    } else {
        ""
    };
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}{secure}"
    )
}

fn csrf_ok(parts: &Parts) -> bool {
    matches!(parts.method, Method::GET | Method::HEAD | Method::OPTIONS)
        || parts.headers.get(CSRF_HEADER).is_some_and(|v| v == "1")
}

/// Extractor: a logged-in administrator. Non-GET requests also need `x-ism-request: 1`.
pub struct Admin(pub Principal);

impl FromRequestParts<AppState> for Admin {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, AppError> {
        let user = session_user(state, &parts.headers)
            .await?
            .ok_or_else(|| AppError::unauthorized("log in first"))?;
        if !csrf_ok(parts) {
            return Err(AppError::forbidden("missing x-ism-request header"));
        }
        Ok(Self(Principal::Admin { username: user }))
    }
}

/// Parses `ism_<id>_<secret>`.
pub fn split_token(raw: &str) -> Option<(&str, &str)> {
    let rest = raw.strip_prefix("ism_")?;
    let (id, secret) = rest.split_once('_')?;
    (!id.is_empty() && !secret.is_empty()).then_some((id, secret))
}

/// Extractor for the client API: a bearer token, or an admin session (so the UI can use the
/// same endpoints).
pub struct Client(pub Principal);

impl FromRequestParts<AppState> for Client {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, AppError> {
        let bearer = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        let Some(raw) = bearer else {
            return Admin::from_request_parts(parts, state)
                .await
                .map(|Admin(p)| Self(p));
        };
        let (id, secret) =
            split_token(raw.trim()).ok_or_else(|| AppError::unauthorized("malformed token"))?;
        let row: Option<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT kind, secret_hash, roots_json, actions_json, id FROM client_tokens
             WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&state.db)
        .await?;
        let Some((kind, hash, roots, actions, token_id)) = row else {
            return Err(AppError::unauthorized("unknown or revoked token"));
        };
        if !constant_time_eq(sha256_hex(secret.as_bytes()).as_bytes(), hash.as_bytes()) {
            return Err(AppError::unauthorized("unknown or revoked token"));
        }
        let _ = sqlx::query("UPDATE client_tokens SET last_used_at = ? WHERE id = ?")
            .bind(db::now())
            .bind(&token_id)
            .execute(&state.db)
            .await;
        let actions: Vec<ClientAction> = serde_json::from_str(&actions).unwrap_or_default();
        Ok(Self(Principal::Client(ClientGrant {
            token_id,
            kind,
            roots: db::json_vec(&roots),
            actions,
        })))
    }
}

/// Peer request headers.
pub const PEER_HEADER: &str = "x-ism-peer";
pub const TIMESTAMP_HEADER: &str = "x-ism-timestamp";
pub const SIGNATURE_HEADER: &str = "x-ism-signature";
pub const MAX_CLOCK_SKEW_SECS: i64 = 300;

/// Verifies a signed peer request against `secret`.
pub fn verify_peer_signature(
    secret: &str,
    method: &str,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> AppResult<()> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let timestamp: i64 = header(TIMESTAMP_HEADER)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| AppError::unauthorized("missing timestamp"))?;
    if (Utc::now().timestamp() - timestamp).abs() > MAX_CLOCK_SKEW_SECS {
        return Err(AppError::unauthorized(
            "request timestamp too far from server clock",
        ));
    }
    let signature =
        header(SIGNATURE_HEADER).ok_or_else(|| AppError::unauthorized("missing signature"))?;
    let expected = crate::crypto::sign_request(secret, method, path_and_query, timestamp, body);
    if constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
        Ok(())
    } else {
        Err(AppError::unauthorized("bad signature"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tokens_and_cookies() {
        assert_eq!(split_token("ism_abc_def_ghi"), Some(("abc", "def_ghi")));
        assert_eq!(split_token("abc"), None);
        let mut headers = HeaderMap::new();
        headers.insert("cookie", "a=1; ism_session=xyz".parse().unwrap());
        assert_eq!(cookie(&headers, SESSION_COOKIE).as_deref(), Some("xyz"));
    }

    #[test]
    fn client_grants_are_scoped() {
        let p = Principal::Client(ClientGrant {
            token_id: "t".into(),
            kind: "ila".into(),
            roots: vec!["photos".into()],
            actions: vec![ClientAction::Read, ClientAction::Download],
        });
        assert!(p.can(ClientAction::Download, "photos"));
        assert!(!p.can(ClientAction::Download, "other"));
        assert!(!p.can(ClientAction::Manage, "photos"));
    }

    #[test]
    fn peer_signatures_verify() {
        let ts = Utc::now().timestamp();
        let sig = crate::crypto::sign_request("secret", "GET", "/x", ts, b"");
        let mut headers = HeaderMap::new();
        headers.insert(TIMESTAMP_HEADER, ts.to_string().parse().unwrap());
        headers.insert(SIGNATURE_HEADER, sig.parse().unwrap());
        assert!(verify_peer_signature("secret", "GET", "/x", &headers, b"").is_ok());
        assert!(verify_peer_signature("secret", "GET", "/y", &headers, b"").is_err());
        assert!(verify_peer_signature("other", "GET", "/x", &headers, b"").is_err());
    }
}
