//! HTTP routing: client API, admin API, peer API and the single-page app.

pub mod admin;
pub mod client;
pub mod peer;

use std::convert::Infallible;
use std::path::Path;

use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::Stream;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use tower_http::services::{ServeDir, ServeFile};

use crate::error::{AppError, AppResult};
use crate::state::AppState;

/// Largest request body: one upload chunk.
pub const MAX_BODY: usize = 64 * 1024 * 1024;

pub fn router(state: AppState) -> Router {
    let web = state.config.web_dir.clone();
    let spa = ServeDir::new(&web).fallback(ServeFile::new(web.join("index.html")));
    Router::new()
        .nest("/api/v1", client::router())
        .nest("/api/admin", admin::router())
        .nest("/api/peer/v1", peer::router())
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .fallback_service(spa)
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(axum::middleware::from_fn(security_headers))
        .with_state(state)
}

async fn security_headers(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let api = req.uri().path().starts_with("/api/");
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    h.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    h.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    h.insert("x-frame-options", HeaderValue::from_static("DENY"));
    if api {
        h.entry(header::CACHE_CONTROL)
            .or_insert(HeaderValue::from_static("no-store"));
    }
    resp
}

/// Parses a single `bytes=start-end` range against `len`.
fn parse_range(raw: &str, len: u64) -> Option<(u64, u64)> {
    let spec = raw.trim().strip_prefix("bytes=")?;
    if spec.contains(',') || len == 0 {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let (start, end) = if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        (len.saturating_sub(n), len - 1)
    } else {
        let start: u64 = a.parse().ok()?;
        let end = if b.is_empty() {
            len - 1
        } else {
            b.parse::<u64>().ok()?.min(len - 1)
        };
        (start, end)
    };
    (start <= end && start < len).then_some((start, end))
}

/// Streams a file with single-range support. `etag` is quoted into `ETag`.
pub async fn serve_file(
    path: &Path,
    headers: &HeaderMap,
    etag: &str,
    extra: &[(&'static str, String)],
) -> AppResult<Response> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => AppError::not_found("file not found"),
            _ => AppError::from(e),
        })?;
    let len = file.metadata().await?.len();
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let if_range_ok = headers
        .get(header::IF_RANGE)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|v| v.trim_matches('"') == etag);
    let mut builder = Response::builder()
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, format!("\"{etag}\""))
        .header(header::CONTENT_TYPE, "application/octet-stream");
    for (k, v) in extra {
        builder = builder.header(*k, v);
    }
    let resp = match range.filter(|_| if_range_ok) {
        Some(raw) => {
            let Some((start, end)) = parse_range(raw, len) else {
                return Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, format!("bytes */{len}"))
                    .body(Body::empty())
                    .map_err(AppError::internal);
            };
            file.seek(std::io::SeekFrom::Start(start)).await?;
            let take = end - start + 1;
            builder
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{len}"))
                .header(header::CONTENT_LENGTH, take)
                .body(Body::from_stream(ReaderStream::new(file.take(take))))
        }
        None => builder
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, len)
            .body(Body::from_stream(ReaderStream::new(file))),
    };
    resp.map_err(AppError::internal)
}

pub fn sse<S>(stream: S) -> Response
where
    S: Stream<Item = Result<Event, Infallible>> + Send + 'static,
{
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

pub fn last_event_id(headers: &HeaderMap) -> Option<i64> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::parse_range;

    #[test]
    fn parses_single_ranges() {
        assert_eq!(parse_range("bytes=0-9", 100), Some((0, 9)));
        assert_eq!(parse_range("bytes=90-", 100), Some((90, 99)));
        assert_eq!(parse_range("bytes=-10", 100), Some((90, 99)));
        assert_eq!(parse_range("bytes=50-500", 100), Some((50, 99)));
        assert_eq!(parse_range("bytes=100-", 100), None);
        assert_eq!(parse_range("bytes=0-1,5-6", 100), None);
        assert_eq!(parse_range("items=0-1", 100), None);
    }
}

#[cfg(test)]
mod flow_tests;
