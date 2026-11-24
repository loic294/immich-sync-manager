//! Content-free, coalesced hints for paired peers. Consumers fetch authoritative state.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::State;
use axum::response::Sse;
use axum::response::sse::Event;
use futures_util::{Stream, stream};
use tokio::sync::{broadcast, watch};

use super::peer::PeerAuth;
use crate::error::{AppError, AppResult};
use crate::peers::net;
use crate::state::{AppState, PeerEvent};

const COALESCE: Duration = Duration::from_millis(250);
const KEEPALIVE: Duration = Duration::from_secs(15);

struct Subscription {
    state: AppState,
    peer: net::Peer,
    changes: broadcast::Receiver<PeerEvent>,
    configuration: watch::Receiver<()>,
}

impl Subscription {
    async fn authorized(&self) -> AppResult<bool> {
        Ok(net::peers(&self.state).await?.iter().any(|peer| {
            peer.id == self.peer.id
                && peer.instance_id == self.peer.instance_id
                && peer.secret == self.peer.secret
                && peer.url == self.peer.url
                && peer.name == self.peer.name
        }))
    }
}

pub(super) async fn notifications(
    State(state): State<AppState>,
    PeerAuth(peer): PeerAuth,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let subscription = Subscription {
        changes: state.peer_events.subscribe(),
        configuration: state.peer_configuration.subscribe(),
        state,
        peer,
    };
    // Recheck after subscribing: configuration may have changed during GET authentication.
    if !subscription.authorized().await? {
        return Err(AppError::unauthorized("peer configuration changed"));
    }
    let events = stream::unfold(subscription, |mut sub| async move {
        let (mut catch_up, mut changed, heartbeat) = tokio::select! {
            biased;
            _ = sub.configuration.changed() => return None,
            event = sub.changes.recv() => match event {
                Ok(PeerEvent::Changed) => (false, true, false),
                Ok(PeerEvent::Attention) => (false, false, false),
                Err(broadcast::error::RecvError::Lagged(_)) => (true, true, false),
                Err(broadcast::error::RecvError::Closed) => return None,
            },
            () = tokio::time::sleep(KEEPALIVE) => (false, false, true),
        };
        if !heartbeat {
            tokio::select! {
                biased;
                _ = sub.configuration.changed() => return None,
                () = tokio::time::sleep(COALESCE) => {}
            }
            // Bound draining even when producers on other threads keep publishing.
            for _ in 0..sub.changes.len() {
                match sub.changes.try_recv() {
                    Ok(PeerEvent::Changed) => changed = true,
                    Ok(PeerEvent::Attention) => {}
                    Err(broadcast::error::TryRecvError::Lagged(_)) => catch_up = true,
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Closed) => return None,
                }
            }
        }
        match sub.authorized().await {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => {
                tracing::error!(peer = %sub.peer.id, "peer notification authorization failed: {}", error.message());
                return None;
            }
        }
        // Do not send even a buffered hint after configuration changed during the DB read.
        if sub.configuration.has_changed().unwrap_or(true) {
            return None;
        }
        let event = if heartbeat {
            Event::default().comment("keepalive")
        } else {
            Event::default()
                .event(if catch_up {
                    "catch_up"
                } else if changed {
                    "changed"
                } else {
                    "attention_changed"
                })
                .data("{}")
        };
        Some((Ok(event), sub))
    });
    Ok(Sse::new(events))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::auth::{PEER_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER};
    use crate::config::Config;
    use crate::crypto::SecretBox;
    use crate::peers::{self, admin};
    use crate::state::ServerEvent;

    const PATH: &str = "/api/peer/v1/notifications";
    const SECRET: &str = "synthetic-peer-secret";

    struct Instance {
        state: AppState,
        url: String,
        server: tokio::task::JoinHandle<()>,
        _dir: tempfile::TempDir,
    }

    impl Drop for Instance {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    impl Instance {
        async fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db = crate::db::connect(dir.path()).await.unwrap();
            let state = AppState::new(
                Config::for_tests(dir.path()),
                db,
                SecretBox::open_or_create(dir.path()).unwrap(),
            )
            .await
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let app = crate::routes::router(state.clone());
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                state,
                url,
                server,
                _dir: dir,
            }
        }

        async fn add_peer(&self, sender: &str, url: &str) {
            sqlx::query(
                "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
                 VALUES('peer','Peer example',?,?,?,'online',?)",
            )
            .bind(url)
            .bind(sender)
            .bind(
                self.state
                    .secrets
                    .encrypt(net::SECRET_PURPOSE, SECRET)
                    .unwrap(),
            )
            .bind(crate::db::now())
            .execute(&self.state.db)
            .await
            .unwrap();
        }

        fn signed(
            &self,
            sender: &str,
            secret: &str,
            signed_path: &str,
            ts: i64,
        ) -> reqwest::RequestBuilder {
            self.state
                .http
                .get(format!("{}{PATH}", self.url))
                .header(PEER_HEADER, sender)
                .header(TIMESTAMP_HEADER, ts)
                .header(
                    SIGNATURE_HEADER,
                    crate::crypto::sign_request(secret, "GET", signed_path, ts, &[]),
                )
        }

        async fn subscribe(&self) -> reqwest::Response {
            let response = tokio::time::timeout(
                Duration::from_secs(2),
                self.signed(
                    "remote-example",
                    SECRET,
                    PATH,
                    chrono::Utc::now().timestamp(),
                )
                .send(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.headers()["content-type"], "text/event-stream");
            response
        }
    }

    async fn frame(response: &mut reqwest::Response) -> Option<String> {
        let mut data = String::new();
        while let Some(chunk) = response.chunk().await.unwrap() {
            data.push_str(std::str::from_utf8(&chunk).unwrap());
            if data.ends_with("\n\n") {
                return Some(data);
            }
        }
        assert!(data.is_empty(), "partial SSE frame: {data:?}");
        None
    }

    async fn next(response: &mut reqwest::Response) -> Option<String> {
        tokio::time::timeout(Duration::from_secs(2), frame(response))
            .await
            .unwrap()
    }

    async fn quiet(response: &mut reqwest::Response) {
        assert!(
            tokio::time::timeout(Duration::from_millis(400), frame(response))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn notification_http_authentication() {
        let instance = Instance::new().await;
        instance
            .add_peer("remote-example", "http://peer.example")
            .await;
        let unsigned = instance
            .state
            .http
            .get(format!("{}{PATH}", instance.url))
            .send()
            .await
            .unwrap();
        assert_eq!(unsigned.status(), reqwest::StatusCode::UNAUTHORIZED);
        let now = chrono::Utc::now().timestamp();
        for (sender, secret, path, ts) in [
            ("unknown-example", SECRET, PATH, now),
            ("remote-example", "wrong-secret", PATH, now),
            ("remote-example", SECRET, "/api/peer/v1/events", now),
            ("remote-example", SECRET, PATH, now - 600),
        ] {
            let response = instance
                .signed(sender, secret, path, ts)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        }
        let mut response = instance.subscribe().await;
        quiet(&mut response).await;
    }

    #[tokio::test]
    async fn notification_http_coalesces_local_changes_and_signals_lag() {
        let instance = Instance::new().await;
        instance
            .add_peer("remote-example", "http://peer.example")
            .await;
        let mut response = instance.subscribe().await;
        let started = tokio::time::Instant::now();
        peers::append_local(
            &instance.state,
            "Example",
            "file",
            &peers::EventData::default(),
        )
        .await
        .unwrap();
        for _ in 0..20 {
            instance.state.emit(ServerEvent::ConflictsChanged);
        }
        assert_eq!(
            next(&mut response).await.as_deref(),
            Some("event: changed\ndata: {}\n\n")
        );
        assert!(started.elapsed() >= COALESCE);
        quiet(&mut response).await;
        for _ in 0..1000 {
            instance.state.emit(ServerEvent::SyncChanged);
        }
        assert_eq!(
            next(&mut response).await.as_deref(),
            Some("event: catch_up\ndata: {}\n\n")
        );
        quiet(&mut response).await;
        sqlx::query(
            "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES('operation-example','example','peer:peer','peer','{}','failed',?)",
        )
        .bind(crate::db::now())
        .execute(&instance.state.db)
        .await
        .unwrap();
        peers::recovery::discover(&instance.state).await.unwrap();
        assert_eq!(
            next(&mut response).await.as_deref(),
            Some("event: changed\ndata: {}\n\n")
        );
        peers::recovery::discover(&instance.state).await.unwrap();
        quiet(&mut response).await;
    }

    #[tokio::test]
    async fn notification_http_distinguishes_attention_only_hints() {
        let instance = Instance::new().await;
        instance
            .add_peer("remote-example", "http://peer.example")
            .await;
        let mut response = instance.subscribe().await;
        instance.state.emit(ServerEvent::WatchChanged);
        assert_eq!(
            next(&mut response).await.as_deref(),
            Some("event: attention_changed\ndata: {}\n\n")
        );
        instance.state.emit(ServerEvent::MaintenanceChanged {
            server_id: "example".into(),
        });
        instance.state.emit(ServerEvent::SyncChanged);
        assert_eq!(
            next(&mut response).await.as_deref(),
            Some("event: changed\ndata: {}\n\n")
        );
    }

    #[tokio::test]
    async fn notification_http_revokes_removed_peer_with_pending_hint() {
        let instance = Instance::new().await;
        instance
            .add_peer("remote-example", "http://peer.example")
            .await;
        let mut response = instance.subscribe().await;
        instance.state.emit(ServerEvent::SyncChanged);
        admin::delete_peer(&instance.state, "peer").await.unwrap();
        assert_eq!(next(&mut response).await, None);
        let rejected = instance
            .signed(
                "remote-example",
                SECRET,
                PATH,
                chrono::Utc::now().timestamp(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn notification_http_revokes_configuration_and_credentials() {
        let instance = Instance::new().await;
        instance
            .add_peer("remote-example", "http://peer.example")
            .await;
        sqlx::query("INSERT INTO mappings(root_id,name,container_path,created_at) VALUES('photos','Photos example',?,?)")
            .bind(instance._dir.path().to_string_lossy().as_ref())
            .bind(crate::db::now()).execute(&instance.state.db).await.unwrap();
        sqlx::query("INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled) VALUES('share','Example','photos','','[\"peer\"]',1)")
            .execute(&instance.state.db).await.unwrap();
        let mut response = instance.subscribe().await;
        admin::delete_share(&instance.state, "share").await.unwrap();
        assert_eq!(next(&mut response).await, None);

        let mut response = instance.subscribe().await;
        sqlx::query("UPDATE peers SET secret_enc=? WHERE id='peer'")
            .bind(
                instance
                    .state
                    .secrets
                    .encrypt(net::SECRET_PURPOSE, "rotated-example")
                    .unwrap(),
            )
            .execute(&instance.state.db)
            .await
            .unwrap();
        instance.state.peer_configuration_changed();
        assert_eq!(next(&mut response).await, None);
        let rejected = instance
            .signed(
                "remote-example",
                SECRET,
                PATH,
                chrono::Utc::now().timestamp(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn notification_http_does_not_mirror_remote_status() {
        let instance = Instance::new().await;
        let remote = Instance::new().await;
        instance.add_peer("remote-example", &remote.url).await;
        remote
            .add_peer(&instance.state.instance_id, &instance.url)
            .await;
        let peer = net::peer(&instance.state, "peer").await.unwrap();
        let mut response = instance.subscribe().await;
        let mut ui = instance.state.events.subscribe();
        peers::activity::refresh(&instance.state, &peer)
            .await
            .unwrap();
        peers::activity::unavailable(&instance.state, "peer", "synthetic connection error").await;
        peers::recovery::refresh(&instance.state, &peer)
            .await
            .unwrap();
        net::mark(
            &instance.state,
            "peer",
            Err("synthetic connection error".into()),
        )
        .await;
        net::mark(&instance.state, "peer", Ok(())).await;
        net::mark_processing_result(&instance.state, "peer", Some("synthetic processing error"))
            .await;
        assert!(matches!(ui.recv().await.unwrap(), ServerEvent::SyncChanged));
        quiet(&mut response).await;
        instance.state.emit(ServerEvent::SyncChanged);
        assert_eq!(
            next(&mut response).await.as_deref(),
            Some("event: changed\ndata: {}\n\n")
        );
    }
}
