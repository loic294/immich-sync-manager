//! The single error type of the HTTP layer: `{"error":{"code","message"}}` with a status.

use std::borrow::Cow;
use std::fmt;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
pub struct AppError {
    status: StatusCode,
    code: Cow<'static, str>,
    message: String,
}

impl AppError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: Cow::Borrowed(code),
            message: message.into(),
        }
    }

    pub fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    /// A conflict whose code comes from a plan blocker.
    pub fn conflict_dynamic(code: String, message: String) -> Self {
        let status = match code.as_str() {
            "forbidden" => StatusCode::FORBIDDEN,
            "invalid_location" => StatusCode::BAD_REQUEST,
            "root_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
            "not_found" => StatusCode::NOT_FOUND,
            _ => StatusCode::CONFLICT,
        };
        Self {
            status,
            code: Cow::Owned(code),
            message,
        }
    }

    pub fn invalid_location(message: impl Into<String>) -> Self {
        Self::bad_request("invalid_location", message)
    }

    pub fn root_unavailable(root_id: &str) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "root_unavailable",
            format!(
                "root '{root_id}' is not mounted or its .ism-root marker is missing or invalid"
            ),
        )
    }

    /// 500. The cause is logged, never sent to the client.
    pub fn internal(cause: impl fmt::Display) -> Self {
        tracing::error!(error = %redact(&cause.to_string()), "internal error");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal server error",
        )
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Removes credentials from URLs in log text (`scheme://user:pass@host` -> `scheme://***@host`).
pub fn redact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find("://") {
        let (head, tail) = rest.split_at(pos + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '/' || c == '"' || c == '\'')
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rfind('@') {
            Some(at) => {
                out.push_str("***");
                out.push_str(&authority[at..]);
            }
            None => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    redact_password_params(&out)
}

/// Masks `password=` query or key/value parameters, e.g. in libpq connection strings.
fn redact_password_params(text: &str) -> String {
    const KEY: &str = "password=";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.to_ascii_lowercase().find(KEY) {
        let at_boundary = pos == 0
            || matches!(
                rest.as_bytes()[pos - 1],
                b'?' | b'&' | b' ' | b'\t' | b'\n' | b';' | b'"' | b'\''
            );
        let (head, tail) = rest.split_at(pos + KEY.len());
        out.push_str(head);
        if !at_boundary {
            rest = tail;
            continue;
        }
        let end = tail
            .find(|c: char| c.is_whitespace() || matches!(c, '&' | ';' | '"' | '\''))
            .unwrap_or(tail.len());
        out.push_str("***");
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            self.status.as_u16(),
            self.code,
            self.message
        )
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = json!({ "error": { "code": self.code, "message": self.message } });
        (self.status, Json(body)).into_response()
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        Self::internal(e)
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self::internal(e)
    }
}

#[cfg(test)]
mod tests {
    use super::redact;

    #[test]
    fn redacts_url_credentials() {
        assert_eq!(
            redact("connect postgres://user:secret@db.example:5432/immich failed"),
            "connect postgres://***@db.example:5432/immich failed"
        );
        assert_eq!(redact("http://ism.example/x"), "http://ism.example/x");
        assert_eq!(
            redact("postgres://db.example/immich?user=u&password=secret&sslmode=require"),
            "postgres://db.example/immich?user=u&password=***&sslmode=require"
        );
        assert_eq!(
            redact("host=db.example Password=secret dbname=immich"),
            "host=db.example Password=*** dbname=immich"
        );
        assert_eq!(redact("old_password=x"), "old_password=x");
    }
}
