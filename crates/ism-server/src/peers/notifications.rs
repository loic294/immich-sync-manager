//! Optional peer invalidations. Reading stays independent of authoritative refreshes.

use std::time::Duration;

use tokio::sync::watch;

use super::{activity, net, recovery};
use crate::error::AppError;
use crate::state::AppState;

const RETRY: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(45);
const COALESCE: Duration = Duration::from_secs(1);
const PATH: &str = "/api/peer/v1/notifications";

#[derive(Clone)]
enum Hint {
    Waiting,
    Refresh,
    Error(String),
}

pub(super) async fn run(state: &AppState, peer: &net::Peer) {
    let (sender, mut hints) = watch::channel(Hint::Waiting);
    let reader = async move {
        let client = match reqwest::Client::builder().connect_timeout(RETRY).build() {
            Ok(client) => client,
            Err(error) => {
                sender.send_replace(Hint::Error(net::net_error(&error)));
                return;
            }
        };
        loop {
            match subscribe(&client, state, peer, &sender).await {
                Ok(()) => {
                    // Older peers poll as before, but periodically probe for an upgrade.
                    for _ in 0..12 {
                        sender.send_replace(Hint::Refresh);
                        tokio::time::sleep(RETRY).await;
                    }
                }
                Err(error) => {
                    sender.send_replace(Hint::Error(error));
                    tokio::time::sleep(RETRY).await;
                }
            }
        }
    };
    let consumer = async {
        let mut retry = false;
        loop {
            let hint = tokio::select! {
                biased;
                result = hints.changed() => {
                    if result.is_err() { break; }
                    hints.borrow_and_update().clone()
                }
                _ = tokio::time::sleep(RETRY), if retry => Hint::Refresh,
            };
            retry = false;
            match hint {
                Hint::Waiting => {}
                Hint::Refresh => {
                    retry = !activity::refresh_peer(state, peer).await;
                    // Hints received during a slow fetch collapse to one further refresh.
                    tokio::time::sleep(COALESCE).await;
                }
                Hint::Error(error) => {
                    tracing::warn!(peer=%peer.id, "peer notifications unavailable: {error}");
                    let error = AppError::conflict("peer_notifications_unavailable", error);
                    if let Err(error) = recovery::unavailable(state, &peer.id, &error).await {
                        tracing::error!(
                            "cannot persist recovery availability: {}",
                            error.message()
                        );
                    }
                    activity::unavailable(state, &peer.id, error.message()).await;
                }
            }
        }
    };
    tokio::join!(reader, consumer);
}

// Only an explicitly unsupported endpoint selects legacy polling.
async fn subscribe(
    client: &reqwest::Client,
    state: &AppState,
    peer: &net::Peer,
    hints: &watch::Sender<Hint>,
) -> Result<(), String> {
    let request = net::request_with_client(
        client,
        state,
        &peer.url,
        &peer.secret,
        reqwest::Method::GET,
        PATH,
        None,
    );
    let mut response = tokio::time::timeout(Duration::from_secs(10), request.send())
        .await
        .map_err(|_| "notification connection timed out".to_owned())?
        .map_err(|e| net::net_error(&e))?;
    let status = response.status();
    if matches!(status.as_u16(), 404 | 405 | 501) {
        return Ok(());
    }
    if !status.is_success() {
        return Err(net::http_failure(status, ""));
    }
    if response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.split(';').next().map(str::trim) != Some("text/event-stream"))
    {
        return Err("peer notifications returned an invalid content type".into());
    }
    hints.send_replace(Hint::Refresh);
    let mut parser = Frames::default();
    loop {
        let chunk = tokio::time::timeout(READ_TIMEOUT, response.chunk())
            .await
            .map_err(|_| "peer notification keepalive timed out".to_owned())?
            .map_err(|e| net::net_error(&e))?
            .ok_or_else(|| "peer notification connection closed".to_owned())?;
        if parser.push(&chunk)? {
            hints.send_replace(Hint::Refresh);
        }
    }
}

#[derive(Default)]
struct Frames {
    line: Vec<u8>,
    event: String,
    size: usize,
}

impl Frames {
    fn push(&mut self, chunk: &[u8]) -> Result<bool, String> {
        let mut changed = false;
        for &byte in chunk {
            self.size += 1;
            if self.size > 16 * 1024 {
                return Err("peer notification frame is too large".into());
            }
            if byte != b'\n' {
                self.line.push(byte);
                continue;
            }
            let line = std::str::from_utf8(&self.line)
                .map_err(|_| "invalid peer notification encoding".to_owned())?
                .trim_end_matches('\r');
            if line.is_empty() {
                changed |= matches!(self.event.as_str(), "changed" | "catch_up");
                self.event.clear();
                self.size = 0;
            } else if let Some(value) = line.strip_prefix("event:") {
                self.event = value.strip_prefix(' ').unwrap_or(value).to_owned();
            }
            self.line.clear();
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn fragmented_coalesced_frames_ignore_keepalives() {
        let mut frames = Frames::default();
        assert!(!frames.push(b": keepalive\r\n\r\nevent: cha").unwrap());
        assert!(
            frames
                .push(b"nged\r\ndata: {}\r\n\r\nevent: catch_up\n\n")
                .unwrap()
        );
        assert!(!frames.push(b"event: unknown\n\n: keepalive\n\n").unwrap());
        assert!(frames.push(b"event: catch_up\n\n").unwrap());
        assert!(frames.push(&[b'x'; 16 * 1024 + 1]).is_err());
    }
}
