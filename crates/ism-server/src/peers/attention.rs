//! Minimal signed evidence for the shares a paired peer is authorized to access.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ism_core::dto::{
    AttentionCode, AttentionCompleteness, AttentionCoverage, AttentionItem, AttentionLevel,
    AttentionOwner, AttentionState,
};
use serde::{Deserialize, Serialize};

use super::{Share, net};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::state::{AppState, ServerEvent};

const PATH: &str = "/api/peer/v1/attention";
const MAX_ITEMS: usize = 500;
const MAX_BYTES: usize = 256 * 1024;
const FRESH_SECONDS: i64 = 40;
const MAX_CACHE_PEERS: usize = 64;
pub(super) const POLL: Duration = Duration::from_secs(30);
const CODES: &[AttentionCode] = &[
    AttentionCode::MaintenanceBlocked,
    AttentionCode::MaintenanceWaiting,
    AttentionCode::MappingUnavailable,
    AttentionCode::WatcherPaused,
    AttentionCode::LibraryScanFailed,
    AttentionCode::RecoveryDecision,
    AttentionCode::TransferDependency,
];

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Servers,
    Mappings,
    Conflicts,
    Operations,
}

impl Action {
    fn path(self) -> &'static str {
        match self {
            Self::Servers => "/servers",
            Self::Mappings => "/mappings",
            Self::Conflicts => "/conflicts",
            Self::Operations => "/operations",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Servers => "Servers",
            Self::Mappings => "Mappings",
            Self::Conflicts => "Conflicts",
            Self::Operations => "Operations",
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Item {
    id: String,
    code: AttentionCode,
    state: AttentionState,
    attention: AttentionLevel,
    blocks_work: bool,
    share_name: String,
    count: u64,
    action: Action,
    next_at: Option<String>,
    recheck_at: Option<String>,
    observed_at: String,
    completeness: AttentionCompleteness,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Report {
    version: u32,
    instance_id: String,
    observed_at: String,
    shares: Vec<String>,
    coverage: Vec<AttentionCoverage>,
    items: Vec<Item>,
}

#[derive(Clone)]
struct Observation {
    key: String,
    report: Option<Report>,
    checked: Instant,
    error: Option<String>,
}

impl Observation {
    fn remaining(&self) -> Option<Duration> {
        Duration::from_secs(FRESH_SECONDS as u64)
            .checked_sub(self.checked.elapsed())
            .filter(|remaining| !remaining.is_zero())
    }
}

#[derive(Default)]
pub(crate) struct Cache(Mutex<HashMap<String, Observation>>);

impl Cache {
    fn read(&self, peer: &str) -> AppResult<Option<Observation>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| AppError::internal("peer attention cache lock poisoned"))?
            .get(peer)
            .cloned())
    }

    fn record(&self, peer: &str, mut observation: Observation) -> AppResult<()> {
        let mut cache = self
            .0
            .lock()
            .map_err(|_| AppError::internal("peer attention cache lock poisoned"))?;
        if observation.report.is_none() {
            observation.report = cache
                .get(peer)
                .filter(|old| old.key == observation.key)
                .and_then(|old| old.report.clone());
        }
        if !cache.contains_key(peer) && cache.len() >= MAX_CACHE_PEERS {
            let oldest = cache
                .iter()
                .min_by_key(|(_, value)| value.checked)
                .map(|(id, _)| id.clone());
            if let Some(id) = oldest {
                cache.remove(&id);
                tracing::warn!(
                    "peer attention cache capacity reached; oldest evidence is unavailable"
                );
            }
        }
        cache.insert(peer.into(), observation);
        Ok(())
    }

    fn retain(&self, peers: &[CachedPeer]) -> AppResult<()> {
        self.0
            .lock()
            .map_err(|_| AppError::internal("peer attention cache lock poisoned"))?
            .retain(|id, _| peers.iter().any(|peer| peer.id == *id));
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct CachedPeer {
    id: String,
    name: String,
    url: String,
    instance_id: Option<String>,
    secret_enc: String,
    status: String,
}

async fn allowed_shares(state: &AppState, peer: &net::Peer) -> AppResult<Vec<Share>> {
    Ok(super::shares(state)
        .await?
        .into_iter()
        .filter(|share| share.includes(&peer.id))
        .collect())
}

fn cache_key(
    instance: Option<&str>,
    url: &str,
    secret_enc: &str,
    shares: &[Share],
) -> AppResult<String> {
    let scope: Vec<_> = shares
        .iter()
        .map(|s| (&s.id, &s.name, &s.root_id, s.path.as_str()))
        .collect();
    Ok(crate::crypto::sha256_hex(&serde_json::to_vec(&(
        instance, url, secret_enc, scope,
    ))?))
}

async fn configuration_key(
    state: &AppState,
    peer: &net::Peer,
    shares: &[Share],
) -> AppResult<String> {
    let encrypted: String = sqlx::query_scalar("SELECT secret_enc FROM peers WHERE id=?")
        .bind(&peer.id)
        .fetch_one(&state.db)
        .await?;
    cache_key(peer.instance_id.as_deref(), &peer.url, &encrypted, shares)
}

async fn still_authorized(state: &AppState, peer: &net::Peer, key: &str) -> AppResult<bool> {
    let current = net::peer(state, &peer.id).await?;
    Ok(current.instance_id == peer.instance_id
        && current.secret == peer.secret
        && current.url == peer.url
        && configuration_key(state, &current, &allowed_shares(state, &current).await?).await?
            == key)
}

pub(crate) async fn report(state: &AppState, peer: &net::Peer) -> AppResult<Report> {
    let shares = allowed_shares(state, peer).await?;
    let key = configuration_key(state, peer, &shares).await?;
    if shares.len() > MAX_ITEMS {
        return Err(AppError::conflict(
            "peer_attention_scope_too_large",
            "too many shares for a bounded attention report",
        ));
    }
    let roots: Vec<_> = crate::files::load_roots(&state.db)
        .await?
        .into_iter()
        .filter(|root| {
            shares
                .iter()
                .any(|share| share.root_id == root.mapping.root_id)
        })
        .collect();
    let selection = crate::local_evidence::Selection {
        roots: shares.iter().map(|s| s.root_id.clone()).collect(),
        shares: shares.iter().map(|s| s.id.clone()).collect(),
        servers: roots.iter().filter_map(|r| r.server_id.clone()).collect(),
        libraries: roots.iter().filter_map(|r| r.library_id.clone()).collect(),
    };
    let observed_at = db::now();
    let (local, _) =
        crate::local_evidence::selected_snapshot(state, "", &observed_at, Some(&selection)).await?;
    let mut items = Vec::new();
    let mut incomplete = HashSet::new();
    for item in local {
        let scope: Vec<_> = shares
            .iter()
            .filter(|share| {
                let root = roots
                    .iter()
                    .find(|root| root.mapping.root_id == share.root_id);
                match item.code {
                    AttentionCode::MaintenanceBlocked | AttentionCode::MaintenanceWaiting => root
                        .and_then(|root| root.server_id.as_deref())
                        .is_some_and(|id| {
                            item.id.strip_prefix("maintenance:") == Some(id)
                                || item.id.strip_prefix("compatibility:") == Some(id)
                        }),
                    AttentionCode::MappingUnavailable => {
                        item.id.strip_prefix("mapping:") == Some(share.root_id.as_str())
                    }
                    AttentionCode::WatcherPaused => {
                        item.id.strip_prefix("watch:") == Some(share.root_id.as_str())
                    }
                    AttentionCode::LibraryScanFailed => root
                        .and_then(|root| root.library_id.as_deref())
                        .is_some_and(|id| item.id.strip_prefix("library:") == Some(id)),
                    AttentionCode::RecoveryDecision => {
                        item.share_id.as_deref() == Some(share.id.as_str())
                            && item.peer_id.as_deref() == Some(peer.id.as_str())
                    }
                    _ => false,
                }
            })
            .collect();
        for share in scope {
            let action = match item.href.as_str() {
                "/servers" => Action::Servers,
                "/mappings" => Action::Mappings,
                "/conflicts" => Action::Conflicts,
                "/operations" => Action::Operations,
                _ => return Err(AppError::internal("unsupported peer attention action")),
            };
            let unknown = item.state == AttentionState::Unavailable;
            if unknown {
                incomplete.insert(item.code);
                if item.code == AttentionCode::MaintenanceBlocked {
                    incomplete.insert(AttentionCode::MaintenanceWaiting);
                }
            }
            // A library-wide error does not prove a particular shared folder is blocked.
            let library = item.code == AttentionCode::LibraryScanFailed;
            items.push(Item {
                id: crate::crypto::sign_request(
                    &peer.secret,
                    "ATTENTION-ID",
                    PATH,
                    0,
                    format!("{}\0{}", share.name, item.id).as_bytes(),
                ),
                code: item.code,
                state: item.state,
                attention: item.attention,
                blocks_work: !library && item.blocks_work,
                share_name: share.name.clone(),
                count: if unknown {
                    0
                } else if item.code == AttentionCode::RecoveryDecision {
                    item.count
                } else {
                    1
                },
                action,
                next_at: item.next_at.clone(),
                recheck_at: item.recheck_at.clone(),
                observed_at: item.observed_at.clone(),
                completeness: if library {
                    AttentionCompleteness::Partial
                } else {
                    item.completeness
                },
            });
        }
    }
    for share in &shares {
        if !roots
            .iter()
            .any(|root| root.mapping.root_id == share.root_id)
        {
            items.push(Item {
                id: crate::crypto::sign_request(
                    &peer.secret,
                    "ATTENTION-ID",
                    PATH,
                    0,
                    format!("{}\0mapping:{}", share.name, share.root_id).as_bytes(),
                ),
                code: AttentionCode::MappingUnavailable,
                state: AttentionState::Blocked,
                attention: AttentionLevel::ActionRequired,
                blocks_work: true,
                share_name: share.name.clone(),
                count: 1,
                action: Action::Mappings,
                next_at: None,
                recheck_at: None,
                observed_at: observed_at.clone(),
                completeness: AttentionCompleteness::Complete,
            });
        }
    }
    let active: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM operations WHERE status IN ('running','waiting_maintenance')",
    )
    .fetch_all(&state.db)
    .await?;
    let observed_operation = state.replication_operation_lock.has_registered_operations();
    let unknown_operation = active
        .iter()
        .any(|id| !state.replication_operation_lock.is_registered(id));
    if unknown_operation {
        incomplete.insert(AttentionCode::TransferDependency);
    }
    for share in &shares {
        for (present, unknown, key) in [
            (observed_operation, false, "operation-wait"),
            (unknown_operation, true, "operation-unverified"),
        ] {
            if !present {
                continue;
            }
            items.push(Item {
                id: crate::crypto::sign_request(
                    &peer.secret,
                    "ATTENTION-ID",
                    PATH,
                    0,
                    format!("{}\0{key}", share.name).as_bytes(),
                ),
                code: AttentionCode::TransferDependency,
                state: if unknown {
                    AttentionState::Unavailable
                } else {
                    AttentionState::Waiting
                },
                attention: AttentionLevel::Notice,
                blocks_work: !unknown,
                share_name: share.name.clone(),
                count: if unknown { 0 } else { 1 },
                action: Action::Operations,
                next_at: None,
                recheck_at: None,
                observed_at: observed_at.clone(),
                completeness: if unknown {
                    AttentionCompleteness::Unavailable
                } else {
                    AttentionCompleteness::Complete
                },
            });
        }
    }
    if items.len() > MAX_ITEMS {
        incomplete.extend(CODES.iter().copied());
        items.truncate(MAX_ITEMS);
    }
    if !still_authorized(state, peer, &key).await? {
        return Err(AppError::unauthorized(
            "peer attention authorization changed",
        ));
    }
    let report = Report {
        version: 1,
        instance_id: state.instance_id.clone(),
        observed_at,
        shares: shares.iter().map(|s| s.name.clone()).collect(),
        coverage: CODES
            .iter()
            .map(|code| AttentionCoverage {
                code: *code,
                owner: AttentionOwner::Local,
                peer_id: None,
                completeness: if incomplete.contains(code) {
                    AttentionCompleteness::Partial
                } else {
                    AttentionCompleteness::Complete
                },
            })
            .collect(),
        items,
    };
    if serde_json::to_vec(&report)?.len() > MAX_BYTES {
        return Err(AppError::conflict(
            "peer_attention_scope_too_large",
            "peer attention report exceeds its bounded size",
        ));
    }
    Ok(report)
}

fn validate(report: &Report, instance: Option<&str>, shares: &[Share]) -> bool {
    let expected: BTreeSet<_> = shares.iter().map(|share| share.name.as_str()).collect();
    let received: BTreeSet<_> = report.shares.iter().map(String::as_str).collect();
    let mut ids = HashSet::new();
    let mut codes = HashSet::new();
    report.version == 1
        && instance == Some(report.instance_id.as_str())
        && db::parse_ts(&report.observed_at).is_some()
        && expected == received
        && report.shares.len() == received.len()
        && report.shares.len() <= MAX_ITEMS
        && report.items.len() <= MAX_ITEMS
        && report.coverage.len() == CODES.len()
        && report.coverage.iter().all(|source| {
            CODES.contains(&source.code)
                && source.owner == AttentionOwner::Local
                && source.peer_id.is_none()
                && codes.insert(source.code)
        })
        && report.items.iter().all(|item| {
            let action = match item.code {
                AttentionCode::MaintenanceBlocked => {
                    matches!(item.action, Action::Servers | Action::Operations)
                }
                AttentionCode::MaintenanceWaiting => matches!(item.action, Action::Servers),
                AttentionCode::MappingUnavailable
                | AttentionCode::WatcherPaused
                | AttentionCode::LibraryScanFailed => matches!(item.action, Action::Mappings),
                AttentionCode::RecoveryDecision => matches!(item.action, Action::Conflicts),
                AttentionCode::TransferDependency => matches!(item.action, Action::Operations),
                _ => false,
            };
            CODES.contains(&item.code)
                && action
                && received.contains(item.share_name.as_str())
                && item.id.len() == 64
                && item.id.bytes().all(|b| b.is_ascii_hexdigit())
                && ids.insert(&item.id)
                && db::parse_ts(&item.observed_at).is_some()
                && item
                    .next_at
                    .as_deref()
                    .is_none_or(|at| db::parse_ts(at).is_some())
                && item
                    .recheck_at
                    .as_deref()
                    .is_none_or(|at| db::parse_ts(at).is_some())
                && matches!(
                    item.state,
                    AttentionState::Blocked
                        | AttentionState::Waiting
                        | AttentionState::Failed
                        | AttentionState::Unavailable
                )
                && (item.state != AttentionState::Unavailable || !item.blocks_work)
                && (item.code != AttentionCode::LibraryScanFailed || !item.blocks_work)
                && (!item.blocks_work || item.count > 0)
                && (item.state != AttentionState::Unavailable
                    || (item.count == 0
                        && item.completeness != AttentionCompleteness::Complete
                        && report.coverage.iter().any(|source| {
                            source.code == item.code
                                && source.completeness != AttentionCompleteness::Complete
                        })))
        })
}

async fn fetch(state: &AppState, peer: &net::Peer, shares: &[Share]) -> AppResult<Option<Report>> {
    let mut response = net::request(
        state,
        &peer.url,
        &peer.secret,
        reqwest::Method::GET,
        PATH,
        None,
    )
    .timeout(Duration::from_secs(10))
    .send()
    .await
    .map_err(|error| AppError::conflict("peer_attention_unavailable", net::net_error(&error)))?;
    if matches!(response.status().as_u16(), 404 | 405 | 501) {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(AppError::conflict(
            "peer_attention_unavailable",
            "peer attention request failed",
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BYTES as u64)
    {
        return Err(AppError::conflict(
            "invalid_peer_attention",
            "peer attention response is too large",
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| AppError::conflict("peer_attention_unavailable", net::net_error(&error)))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_BYTES {
            return Err(AppError::conflict(
                "invalid_peer_attention",
                "peer attention response is too large",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let report: Report = serde_json::from_slice(&bytes).map_err(|_| {
        AppError::conflict(
            "invalid_peer_attention",
            "peer attention response is unreadable",
        )
    })?;
    if !validate(&report, peer.instance_id.as_deref(), shares) {
        return Err(AppError::conflict(
            "invalid_peer_attention",
            "peer attention identity or scope could not be verified",
        ));
    }
    Ok(Some(report))
}

pub(super) async fn refresh(state: &AppState, peer: &net::Peer) -> AppResult<()> {
    let shares = allowed_shares(state, peer).await?;
    let key = configuration_key(state, peer, &shares).await?;
    let result = fetch(state, peer, &shares).await;
    if !still_authorized(state, peer, &key).await? {
        return Err(AppError::unauthorized(
            "peer attention authorization changed",
        ));
    }
    let (report, error) = match result {
        Ok(Some(report)) => (Some(report), None),
        Ok(None) => (None, Some("unsupported_peer_attention".to_owned())),
        Err(ref error) => {
            tracing::warn!(peer = %peer.id, code = %error.code(), "peer attention unavailable");
            (None, Some(error.code().to_owned()))
        }
    };
    state.peer_attention.record(
        &peer.id,
        Observation {
            key,
            report,
            checked: Instant::now(),
            error,
        },
    )?;
    state.emit_ui(ServerEvent::SyncChanged);
    Ok(())
}

pub(crate) struct Snapshot {
    pub items: Vec<AttentionItem>,
    pub coverage: Vec<AttentionCoverage>,
    pub warnings: Vec<String>,
}

pub(crate) async fn snapshot(state: &AppState) -> AppResult<Snapshot> {
    let (shares, scope_readable) = match super::shares(state).await {
        Ok(shares) => (shares, true),
        Err(error) => {
            tracing::error!(code=%error.code(), "peer attention share scope is unreadable");
            (Vec::new(), false)
        }
    };
    let mut items = Vec::new();
    let mut coverage = Vec::new();
    let mut warnings = Vec::new();
    let peers: Vec<CachedPeer> = sqlx::query_as(
        "SELECT id,name,url,instance_id,secret_enc,status FROM peers ORDER BY name,id",
    )
    .fetch_all(&state.db)
    .await?;
    state.peer_attention.retain(&peers)?;
    if peers.is_empty() && scope_readable {
        coverage.extend(
            CODES
                .iter()
                .chain([&AttentionCode::PeerAttentionUnavailable])
                .map(|code| AttentionCoverage {
                    code: *code,
                    owner: AttentionOwner::Receiver,
                    peer_id: None,
                    completeness: AttentionCompleteness::Complete,
                }),
        );
    }
    for peer in peers {
        let allowed: Vec<_> = shares
            .iter()
            .filter(|share| share.includes(&peer.id))
            .cloned()
            .collect();
        let key = cache_key(
            peer.instance_id.as_deref(),
            &peer.url,
            &peer.secret_enc,
            &allowed,
        )?;
        let report = state
            .peer_attention
            .read(&peer.id)?
            .and_then(|observation| {
                let remaining = observation.remaining()?;
                let valid_url = reqwest::Url::parse(&peer.url).is_ok_and(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.query().is_none()
                        && url.fragment().is_none()
                });
                if !scope_readable
                    || observation.key != key
                    || observation.error.is_some()
                    || peer.status != "online"
                    || !valid_url
                {
                    return None;
                }
                let report = observation.report?;
                validate(&report, peer.instance_id.as_deref(), &allowed)
                    .then_some((report, remaining))
            });
        let Some((report, remaining)) = report else {
            warnings.push(format!(
                "Shared-folder attention on {} could not be verified.",
                peer.name
            ));
            coverage.extend(
                CODES
                    .iter()
                    .chain([&AttentionCode::PeerAttentionUnavailable])
                    .map(|code| AttentionCoverage {
                        code: *code,
                        owner: AttentionOwner::Receiver,
                        peer_id: Some(peer.id.clone()),
                        completeness: AttentionCompleteness::Unavailable,
                    }),
            );
            items.push(AttentionItem {
                id: format!("peer-attention:{}", peer.id),
                code: AttentionCode::PeerAttentionUnavailable,
                state: AttentionState::Unavailable,
                attention: AttentionLevel::Notice,
                blocks_work: false,
                owner: AttentionOwner::Receiver,
                instance_id: peer.instance_id.clone(),
                instance_name: Some(peer.name.clone()),
                peer_id: Some(peer.id.clone()),
                share_id: None, share_name: None, count: 0, total_count: None,
                next_at: None,
                recheck_at: Some(db::ts(chrono::Utc::now() + chrono::Duration::seconds(30))),
                observed_at: db::now(),
                title: "Remote folder and maintenance status is unavailable".into(),
                message: format!("Check {} in Peers. Older instances do not provide this report; unavailable status does not mean synchronization is ready.", peer.name),
                href: "/peers".into(), operation_id: None, acknowledged: None, unresolved: None,
                completeness: AttentionCompleteness::Unavailable, examples: Vec::new(),
            });
            continue;
        };
        coverage.extend(report.coverage.into_iter().map(|mut source| {
            source.owner = AttentionOwner::Receiver;
            source.peer_id = Some(peer.id.clone());
            source
        }));
        coverage.push(AttentionCoverage {
            code: AttentionCode::PeerAttentionUnavailable,
            owner: AttentionOwner::Receiver,
            peer_id: Some(peer.id.clone()),
            completeness: AttentionCompleteness::Complete,
        });
        for item in report.items {
            let title = if item.state == AttentionState::Unavailable {
                "Remote condition could not be verified"
            } else {
                match item.code {
                    AttentionCode::MaintenanceBlocked => "Remote maintenance needs attention",
                    AttentionCode::MaintenanceWaiting => "Remote library scans are paused",
                    AttentionCode::MappingUnavailable => "Remote mapped folder is unavailable",
                    AttentionCode::WatcherPaused => "Remote file detection is paused",
                    AttentionCode::LibraryScanFailed => "Remote library synchronization failed",
                    AttentionCode::RecoveryDecision => "Remote conflicts need decisions",
                    AttentionCode::TransferDependency => "Remote sync waits for a file operation",
                    _ => return Err(AppError::internal("unsupported cached peer attention code")),
                }
            };
            let recheck = item
                .recheck_at
                .as_deref()
                .and_then(db::parse_ts)
                .filter(|at| *at > chrono::Utc::now())
                .map(db::ts)
                .into_iter()
                .chain(Some(db::ts(
                    chrono::Utc::now()
                        + chrono::Duration::milliseconds(remaining.as_millis() as i64),
                )))
                .min();
            items.push(AttentionItem {
                id: format!("peer-attention:{}:{}", peer.id, item.id),
                code: item.code, state: item.state, attention: item.attention,
                blocks_work: item.blocks_work, owner: AttentionOwner::Receiver,
                instance_id: peer.instance_id.clone(), instance_name: Some(peer.name.clone()),
                peer_id: Some(peer.id.clone()),
                share_id: allowed.iter().find(|share| share.name == item.share_name).map(|share| share.id.clone()),
                share_name: Some(item.share_name), count: item.count, total_count: None,
                next_at: item.next_at, recheck_at: recheck, observed_at: item.observed_at,
                title: title.into(),
                message: if item.code == AttentionCode::LibraryScanFailed {
                    format!("Review the shared library on {}. This error does not confirm that these shared files are blocked.", peer.name)
                } else {
                    format!("Open {} on {} to review this condition.", item.action.label(), peer.name)
                },
                href: format!("{}{}", peer.url.trim_end_matches('/'), item.action.path()),
                operation_id: None, acknowledged: None,
                unresolved: item.blocks_work.then_some(true),
                completeness: item.completeness, examples: Vec::new(),
            });
        }

        warnings.push(format!("{} reports shared-folder conditions only; other remote operation and worker conditions are not included.", peer.name));
    }

    Ok(Snapshot {
        items,
        coverage,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn expired_snapshot_and_removed_peer_cannot_reuse_cache_authority() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(
            crate::config::Config::for_tests(directory.path()),
            db::connect(directory.path()).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(directory.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
                "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
                 VALUES ('peer','Example peer','https://peer.example','remote','ciphertext','online',?)",
            ).bind(db::now()).execute(&state.db).await.unwrap();
        let observation = Observation {
            key: cache_key(Some("remote"), "https://peer.example", "ciphertext", &[]).unwrap(),
            report: Some(Report {
                version: 1,
                instance_id: "remote".into(),
                observed_at: db::now(),
                shares: Vec::new(),
                items: Vec::new(),
                coverage: CODES
                    .iter()
                    .map(|code| AttentionCoverage {
                        code: *code,
                        owner: AttentionOwner::Local,
                        peer_id: None,
                        completeness: AttentionCompleteness::Complete,
                    })
                    .collect(),
            }),
            checked: Instant::now(),
            error: None,
        };
        state
            .peer_attention
            .record("peer", observation.clone())
            .unwrap();
        assert!(
            snapshot(&state)
                .await
                .unwrap()
                .coverage
                .iter()
                .all(|source| source.completeness == AttentionCompleteness::Complete)
        );
        state
            .peer_attention
            .record(
                "peer",
                Observation {
                    checked: Instant::now() - Duration::from_secs(FRESH_SECONDS as u64),
                    ..observation
                },
            )
            .unwrap();
        let expired = snapshot(&state).await.unwrap();
        assert!(
            expired
                .coverage
                .iter()
                .all(|source| source.completeness == AttentionCompleteness::Unavailable)
        );
        assert!(!expired.warnings.is_empty());
        assert!(
            expired
                .items
                .iter()
                .any(|item| item.code == AttentionCode::PeerAttentionUnavailable
                    && !item.blocks_work)
        );
        sqlx::query("DELETE FROM peers WHERE id='peer'")
            .execute(&state.db)
            .await
            .unwrap();
        let removed = snapshot(&state).await.unwrap();
        assert!(removed.items.is_empty());
        assert!(state.peer_attention.read("peer").unwrap().is_none());
        assert!(
            removed
                .coverage
                .iter()
                .all(|source| source.peer_id.is_none()
                    && source.completeness == AttentionCompleteness::Complete)
        );
    }

    #[test]
    fn cache_capacity_and_monotonic_expiry_fail_closed() {
        let cache = Cache::default();
        let start = Instant::now();
        for index in 0..=MAX_CACHE_PEERS {
            cache
                .record(
                    &format!("peer-{index}"),
                    Observation {
                        key: "configuration".into(),
                        report: None,
                        checked: start + Duration::from_nanos(index as u64),
                        error: None,
                    },
                )
                .unwrap();
        }
        assert!(cache.read("peer-0").unwrap().is_none());
        assert!(
            cache
                .read(&format!("peer-{MAX_CACHE_PEERS}"))
                .unwrap()
                .is_some()
        );
        assert_eq!(cache.0.lock().unwrap().len(), MAX_CACHE_PEERS);
        let observation = Observation {
            key: "configuration".into(),
            report: None,
            error: None,
            checked: Instant::now() - Duration::from_secs(FRESH_SECONDS as u64),
        };
        assert!(
            observation.remaining().is_none(),
            "Expired evidence must not remain authoritative"
        );
        let observation = Observation {
            checked: Instant::now(),
            ..observation
        };
        assert!(observation.remaining().is_some());
        assert!(
            Cache::default().read("peer-1").unwrap().is_none(),
            "Restart cannot restore freshness"
        );
    }
}
