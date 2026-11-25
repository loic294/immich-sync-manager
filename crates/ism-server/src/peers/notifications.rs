//! Optional peer invalidations. Reading stays independent of authoritative refreshes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use super::{activity, net, recovery};
use crate::error::AppError;
use crate::state::AppState;

const RETRY: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(45);
const FRESHNESS: Duration = Duration::from_secs(30);
const SNAPSHOT_WRITE_INTERVAL: chrono::Duration = chrono::Duration::seconds(20);
const COALESCE: Duration = Duration::from_secs(1);
/// Activity may trail newer hints by at most this long; hints can outpace any single fetch.
const HINT_LAG: Duration = Duration::from_secs(10);
const PATH: &str = "/api/peer/v1/notifications";

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
pub(crate) struct Streams(Mutex<HashMap<String, Arc<Mutex<Health>>>>);

struct Health {
    connected: bool,
    revision: u64,
    /// Verified revision and when its hint arrived; later hints arrived no earlier than that.
    verified: [Option<(u64, tokio::time::Instant)>; 2],
    heartbeat: tokio::time::Instant,
}

#[derive(Clone, Copy)]
pub(super) enum Snapshot {
    Activity,
    Recovery,
}

impl Streams {
    fn connect(&self, peer: &str) -> Connection {
        let health = Arc::new(Mutex::new(Health {
            connected: true,
            revision: 0,
            verified: [None; 2],
            heartbeat: tokio::time::Instant::now(),
        }));
        lock(&self.0).insert(peer.into(), health.clone());
        Connection(health)
    }

    pub(super) fn fresh(&self, peer: &str, snapshot: Snapshot) -> Option<bool> {
        lock(&self.0).get(peer).map(|health| {
            let health = lock(health);
            let current = health.verified[snapshot as usize].is_some_and(|(revision, issued)| {
                revision == health.revision
                    || matches!(snapshot, Snapshot::Activity) && issued.elapsed() <= HINT_LAG
            });
            health.connected && current && health.heartbeat.elapsed() <= FRESHNESS
        })
    }

    pub(crate) fn invalidate(&self) {
        for health in lock(&self.0).values() {
            lock(health).connected = false;
        }
    }

    pub(crate) fn retain(&self, peers: &[net::Peer]) {
        lock(&self.0).retain(|id, _| peers.iter().any(|peer| peer.id == *id));
    }

    fn legacy(&self, peer: &str) {
        lock(&self.0).remove(peer);
    }

    /// Hints newer than the verified snapshot, for regressions.
    #[cfg(test)]
    pub(super) fn pending_hints(&self, peer: &str, snapshot: Snapshot) -> Option<u64> {
        lock(&self.0).get(peer).map(|health| {
            let health = lock(health);
            health.revision - health.verified[snapshot as usize].map_or(0, |(revision, _)| revision)
        })
    }
}

/// Activity uses only its own stream verification; unsupported peers keep timestamp freshness.
pub(crate) fn snapshot_fresh(state: &AppState, peer: &str, updated: Option<&str>) -> bool {
    let Some(at) = updated.and_then(crate::db::parse_ts) else {
        return false;
    };
    let age = chrono::Utc::now() - at;
    if age < chrono::Duration::zero() {
        return false;
    }
    state
        .peer_streams
        .fresh(peer, Snapshot::Activity)
        .unwrap_or(age <= chrono::Duration::seconds(30))
}

/// Unchanged snapshots skip timestamp writes for 20s, leaving a 10s freshness margin.
pub(super) fn snapshot_recent(updated: Option<&str>) -> bool {
    updated.and_then(crate::db::parse_ts).is_some_and(|at| {
        let age = chrono::Utc::now() - at;
        age >= chrono::Duration::zero() && age < SNAPSHOT_WRITE_INTERVAL
    })
}

struct Connection(Arc<Mutex<Health>>);

impl Drop for Connection {
    fn drop(&mut self) {
        lock(&self.0).connected = false;
    }
}

impl Connection {
    fn refresh(&self) -> Refresh {
        let mut health = lock(&self.0);
        health.revision += 1;
        // Activity ages out through HINT_LAG; recovery requires the exact latest revision.
        health.verified[Snapshot::Recovery as usize] = None;
        let issued = tokio::time::Instant::now();
        health.heartbeat = issued;
        Refresh {
            health: self.0.clone(),
            revision: health.revision,
            issued,
        }
    }

    fn heartbeat(&self) {
        lock(&self.0).heartbeat = tokio::time::Instant::now();
    }
}

#[derive(Clone)]
pub(super) struct Refresh {
    health: Arc<Mutex<Health>>,
    revision: u64,
    issued: tokio::time::Instant,
}

impl Refresh {
    pub(super) fn complete(&self, snapshot: Snapshot, success: bool) -> bool {
        let mut health = lock(&self.health);
        if !health.connected {
            return false;
        }
        let verified = &mut health.verified[snapshot as usize];
        if success
            && matches!(snapshot, Snapshot::Activity)
            && verified.is_none_or(|(revision, _)| revision < self.revision)
        {
            *verified = Some((self.revision, self.issued));
            return true;
        }
        if health.revision != self.revision {
            return false;
        }
        health.verified[snapshot as usize] = success.then_some((self.revision, self.issued));
        true
    }
}

#[derive(Clone)]
enum Hint {
    Waiting,
    Refresh(Option<Refresh>),
    Error(String),
}

struct AttentionLease {
    state: AppState,
    peer_id: String,
    owner: u64,
}

impl Drop for AttentionLease {
    fn drop(&mut self) {
        match self
            .state
            .peer_attention
            .change(&self.peer_id, self.owner, Some(false))
        {
            Ok(true) => self.state.emit_ui(crate::state::ServerEvent::SyncChanged),
            Ok(false) => {}
            Err(error) => {
                tracing::error!(peer=%self.peer_id, code=%error.code(), "cannot invalidate stopped peer attention")
            }
        }
    }
}

pub(super) async fn run(state: &AppState, peer: &net::Peer) {
    let owner = match state.peer_attention.begin(&peer.id) {
        Ok(owner) => owner,
        Err(error) => {
            tracing::error!(peer=%peer.id, code=%error.code(), "cannot start peer attention");
            return;
        }
    };
    let _lease = AttentionLease {
        state: state.clone(),
        peer_id: peer.id.clone(),
        owner,
    };
    let (sender, hints) = watch::channel(Hint::Waiting);
    let (attention_sender, mut attention_hints) = watch::channel(());
    let reader = async move {
        let client = match reqwest::Client::builder().connect_timeout(RETRY).build() {
            Ok(client) => client,
            Err(error) => {
                sender.send_replace(Hint::Error(net::net_error(&error)));
                return;
            }
        };
        loop {
            match subscribe(&client, state, peer, &sender, &attention_sender, owner).await {
                Ok(()) => {
                    state.peer_streams.legacy(&peer.id);
                    if let Err(error) =
                        attention_changed(state, peer, owner, Some(true), &attention_sender)
                    {
                        tracing::error!(peer=%peer.id, code=%error.code(), "cannot enable peer attention");
                    }
                    // Older peers poll as before, but periodically probe for an upgrade.
                    for _ in 0..12 {
                        sender.send_replace(Hint::Refresh(None));
                        tokio::time::sleep(RETRY).await;
                    }
                }
                Err(error) => {
                    if let Err(error) =
                        attention_changed(state, peer, owner, Some(false), &attention_sender)
                    {
                        tracing::error!(peer=%peer.id, code=%error.code(), "cannot invalidate peer attention");
                    }
                    sender.send_replace(Hint::Error(error));
                    tokio::time::sleep(RETRY).await;
                }
            }
        }
    };
    let attention = async {
        let mut poll = tokio::time::interval(super::attention::POLL);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = attention_hints.changed() => {
                    if result.is_err() { break; }
                    attention_hints.borrow_and_update();
                }
                _ = poll.tick() => {}
            }
            if let Err(error) = super::attention::refresh(state, peer, owner).await {
                tracing::warn!(peer=%peer.id, code=%error.code(), "cannot refresh peer attention");
            }
            tokio::time::sleep(COALESCE).await;
        }
    };
    tokio::join!(
        reader,
        consume(state, peer, Snapshot::Activity, hints.clone()),
        consume(state, peer, Snapshot::Recovery, hints),
        attention
    );
}

async fn consume(
    state: &AppState,
    peer: &net::Peer,
    snapshot: Snapshot,
    mut hints: watch::Receiver<Hint>,
) {
    let mut retry: Option<Option<Refresh>> = None;
    loop {
        let hint = tokio::select! {
            result = hints.changed() => {
                if result.is_err() { break; }
                hints.borrow_and_update().clone()
            }
            _ = tokio::time::sleep(RETRY), if retry.is_some() => Hint::Refresh(retry.take().flatten()),
        };
        retry = None;
        match hint {
            Hint::Waiting => {}
            Hint::Refresh(refresh) => {
                if !activity::refresh_snapshot(state, peer, snapshot, refresh.as_ref()).await {
                    retry = Some(refresh);
                }
                // Hints received during a slow fetch collapse to one further refresh.
                tokio::time::sleep(COALESCE).await;
            }
            Hint::Error(error) => {
                tracing::warn!(peer=%peer.id, "peer notifications unavailable: {error}");
                let error = AppError::conflict("peer_notifications_unavailable", error);
                match snapshot {
                    Snapshot::Recovery => {
                        if let Err(error) = recovery::unavailable(state, &peer.id, &error).await {
                            tracing::error!(
                                "cannot persist recovery availability: {}",
                                error.message()
                            );
                        }
                    }
                    Snapshot::Activity => {
                        activity::unavailable(state, &peer.id, error.message()).await;
                    }
                }
            }
        }
    }
}

fn attention_changed(
    state: &AppState,
    peer: &net::Peer,
    owner: u64,
    available: Option<bool>,
    hints: &watch::Sender<()>,
) -> crate::error::AppResult<()> {
    if state.peer_attention.change(&peer.id, owner, available)? {
        state.emit_ui(crate::state::ServerEvent::SyncChanged);
        hints.send_replace(());
    }
    Ok(())
}

// Only an explicitly unsupported endpoint selects legacy polling.
async fn subscribe(
    client: &reqwest::Client,
    state: &AppState,
    peer: &net::Peer,
    hints: &watch::Sender<Hint>,
    attention_hints: &watch::Sender<()>,
    owner: u64,
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
    let connection = state.peer_streams.connect(&peer.id);
    attention_changed(state, peer, owner, Some(true), attention_hints)
        .map_err(|error| error.message().to_owned())?;
    hints.send_replace(Hint::Refresh(Some(connection.refresh())));
    let mut parser = Frames::default();
    loop {
        let chunk = tokio::time::timeout(READ_TIMEOUT, response.chunk())
            .await
            .map_err(|_| "peer notification keepalive timed out".to_owned())?
            .map_err(|e| net::net_error(&e))?
            .ok_or_else(|| "peer notification connection closed".to_owned())?;
        let (changed, attention, heartbeat) = parser.push(&chunk)?;
        if changed || attention {
            attention_changed(state, peer, owner, None, attention_hints)
                .map_err(|error| error.message().to_owned())?;
        }
        if changed {
            hints.send_replace(Hint::Refresh(Some(connection.refresh())));
        } else if attention || heartbeat {
            connection.heartbeat();
        }
    }
}

#[derive(Default)]
struct Frames {
    line: Vec<u8>,
    event: String,
    size: usize,
    keepalive: bool,
}

impl Frames {
    fn push(&mut self, chunk: &[u8]) -> Result<(bool, bool, bool), String> {
        let mut changed = false;
        let mut attention = false;
        let mut heartbeat = false;
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
                attention |= self.event == "attention_changed";
                heartbeat |= self.keepalive;
                self.event.clear();
                self.keepalive = false;
                self.size = 0;
            } else if line == ": keepalive" {
                self.keepalive = true;
            } else if let Some(value) = line.strip_prefix("event:") {
                self.event = value.strip_prefix(' ').unwrap_or(value).to_owned();
            }
            self.line.clear();
        }
        Ok((changed, attention, heartbeat))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn unchanged_snapshot_write_interval_stays_inside_freshness_window() {
        let nineteen_seconds_ago =
            crate::db::ts(chrono::Utc::now() - chrono::Duration::seconds(19));
        let twenty_one_seconds_ago =
            crate::db::ts(chrono::Utc::now() - chrono::Duration::seconds(21));

        assert!(snapshot_recent(Some(&nineteen_seconds_ago)));
        assert!(!snapshot_recent(Some(&twenty_one_seconds_ago)));
    }

    #[test]
    fn stale_or_failed_refresh_cannot_verify_a_new_connection() {
        for snapshot in [Snapshot::Activity, Snapshot::Recovery] {
            let streams = Streams::default();
            let first = streams.connect("peer-example");
            let old = first.refresh();
            assert!(old.complete(snapshot, true));
            assert_eq!(streams.fresh("peer-example", snapshot), Some(true));
            drop(first);
            let current = streams.connect("peer-example");
            current.heartbeat();
            assert!(!old.complete(snapshot, true));
            assert_eq!(streams.fresh("peer-example", snapshot), Some(false));
            let initial = current.refresh();
            assert!(initial.complete(snapshot, false));
            assert_eq!(streams.fresh("peer-example", snapshot), Some(false));
            let pending = current.refresh();
            assert!(pending.complete(snapshot, true));
            lock(&current.0).heartbeat =
                tokio::time::Instant::now() - FRESHNESS - Duration::from_secs(1);
            assert_eq!(streams.fresh("peer-example", snapshot), Some(false));
            current.heartbeat();
            assert_eq!(streams.fresh("peer-example", snapshot), Some(true));
            streams.invalidate();
            current.heartbeat();
            assert!(!pending.complete(snapshot, true));
            assert_eq!(streams.fresh("peer-example", snapshot), Some(false));
            streams.legacy("peer-example");
            assert_eq!(streams.fresh("peer-example", snapshot), None);
        }
    }

    fn age(connection: &Connection, snapshot: Snapshot, by: Duration) {
        let mut health = lock(&connection.0);
        let verified = health.verified[snapshot as usize].as_mut().unwrap();
        verified.1 = tokio::time::Instant::now() - by;
    }

    #[test]
    fn recovery_needs_the_latest_hint_but_activity_trails_it_by_a_bounded_lag() {
        let streams = Streams::default();
        let connection = streams.connect("peer-example");
        let fresh = |snapshot| streams.fresh("peer-example", snapshot);
        let first = connection.refresh();
        assert!(first.complete(Snapshot::Activity, true));
        assert!(first.complete(Snapshot::Recovery, true));
        let second = connection.refresh();
        assert_eq!(fresh(Snapshot::Recovery), Some(false));
        assert!(!first.complete(Snapshot::Recovery, true));
        assert_eq!(fresh(Snapshot::Recovery), Some(false));
        assert_eq!(
            fresh(Snapshot::Activity),
            Some(true),
            "hint within HINT_LAG"
        );
        age(
            &connection,
            Snapshot::Activity,
            HINT_LAG + Duration::from_secs(1),
        );
        connection.heartbeat();
        assert_eq!(
            fresh(Snapshot::Activity),
            Some(false),
            "older than HINT_LAG"
        );

        // Sustained hints: each completion verifies its own revision even though newer ones exist.
        let third = connection.refresh();
        assert!(second.complete(Snapshot::Activity, true));
        assert_eq!(fresh(Snapshot::Activity), Some(true));
        let fourth = connection.refresh();
        assert!(third.complete(Snapshot::Activity, true));
        assert_eq!(fresh(Snapshot::Activity), Some(true));
        assert!(
            !second.complete(Snapshot::Activity, true),
            "never regresses"
        );
        assert!(!second.complete(Snapshot::Activity, false));
        assert_eq!(fresh(Snapshot::Activity), Some(true));
        age(
            &connection,
            Snapshot::Activity,
            HINT_LAG + Duration::from_secs(1),
        );
        assert_eq!(fresh(Snapshot::Activity), Some(false));
        assert!(
            fourth.complete(Snapshot::Activity, false),
            "latest failure is authoritative"
        );
        assert_eq!(fresh(Snapshot::Activity), Some(false));
        assert!(fourth.complete(Snapshot::Activity, true));
        assert_eq!(fresh(Snapshot::Activity), Some(true));
        assert_eq!(
            fresh(Snapshot::Recovery),
            Some(false),
            "independent snapshots"
        );
        assert!(fourth.complete(Snapshot::Recovery, true));
        assert_eq!(fresh(Snapshot::Recovery), Some(true));
        assert!(fourth.complete(Snapshot::Activity, false));
        assert_eq!(fresh(Snapshot::Activity), Some(false));
        assert_eq!(fresh(Snapshot::Recovery), Some(true));
    }

    #[test]
    fn fragmented_coalesced_frames_ignore_keepalives() {
        let mut frames = Frames::default();
        assert_eq!(
            frames.push(b": keepalive\r\n\r\nevent: cha").unwrap(),
            (false, false, true)
        );
        assert_eq!(
            frames
                .push(b"nged\r\ndata: {}\r\n\r\nevent: catch_up\n\n")
                .unwrap(),
            (true, false, false)
        );
        assert_eq!(
            frames.push(b"event: unknown\n\n: keepalive\n\n").unwrap(),
            (false, false, true)
        );
        assert_eq!(
            frames.push(b"event: catch_up\n\n").unwrap(),
            (true, false, false)
        );
        assert_eq!(
            frames.push(b"event: attention_changed\n\n").unwrap(),
            (false, true, false)
        );
        assert!(frames.push(&[b'x'; 16 * 1024 + 1]).is_err());
    }
}
