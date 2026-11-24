//! Current, owner-scoped evidence for in-app version decisions.

use axum::http::header;
use axum::response::{IntoResponse, Response};
use ism_core::dto::{
    RecoveryFileVersion, RecoveryKind, RecoveryTarget, RecoveryVersion, RecoveryVersions,
};
use serde::Deserialize;

use super::{Share, admin, net, preview_image};
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::AppState;

struct Snapshot {
    share_id: String,
    peer_id: String,
    view: RecoveryVersions,
}

async fn remember(
    state: &AppState,
    target: &str,
    generation: i64,
    share_id: &str,
    peer_id: &str,
    view: &RecoveryVersions,
) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM conflicts c JOIN conflict_evidence_jobs j ON j.target_id=c.id
         WHERE c.id=? AND c.status='open' AND c.share_id=? AND c.peer_id=? AND j.generation=?)",
    )
    .bind(target)
    .bind(share_id)
    .bind(peer_id)
    .bind(generation)
    .fetch_one(&mut *tx)
    .await?;
    if !valid {
        return Err(AppError::conflict(
            "version_conflict",
            "the conflict changed while its versions were being prepared",
        ));
    }
    let saved = sqlx::query(
        "INSERT INTO recovery_version_snapshots(target_id,revision,share_id,peer_id,view_json,created_at)
         VALUES (?,?,?,?,?,?) ON CONFLICT(target_id,revision) DO UPDATE SET
         view_json=excluded.view_json, created_at=excluded.created_at
         WHERE recovery_version_snapshots.share_id=excluded.share_id
           AND recovery_version_snapshots.peer_id=excluded.peer_id",
    )
    .bind(target)
    .bind(&view.revision)
    .bind(share_id)
    .bind(peer_id)
    .bind(serde_json::to_string(view)?)
    .bind(crate::db::now())
    .execute(&mut *tx)
    .await?;
    if saved.rows_affected() != 1 {
        return Err(AppError::conflict(
            "peer_share_changed",
            "the reviewed share is unavailable",
        ));
    }
    sqlx::query(
        "DELETE FROM recovery_version_snapshots WHERE target_id=?
         AND revision NOT IN (SELECT revision FROM recovery_version_snapshots WHERE target_id=?
                              ORDER BY created_at DESC, rowid DESC LIMIT 4)
         AND NOT EXISTS (SELECT 1 FROM sync_recovery WHERE kind='conflict' AND target_id=?
                         AND decision_revision=recovery_version_snapshots.revision
                         AND state NOT IN ('fixed','dismissed'))",
    )
    .bind(target)
    .bind(target)
    .bind(target)
    .execute(&mut *tx)
    .await?;
    let prepared = sqlx::query(
        "UPDATE conflict_evidence_jobs SET state='ready',error=NULL,error_detail=NULL
         WHERE target_id=? AND generation=? AND state='queued'",
    )
    .bind(target)
    .bind(generation)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if prepared.rows_affected() != 0 {
        state.version_event_wake.notify_one();
    }
    Ok(())
}

async fn recall(state: &AppState, target: &str, revision: &str) -> AppResult<Option<Snapshot>> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT s.share_id,s.peer_id,s.view_json FROM recovery_version_snapshots s
         JOIN conflicts c ON c.id=s.target_id AND c.share_id=s.share_id AND c.peer_id=s.peer_id
         WHERE s.target_id=? AND s.revision=?",
    )
    .bind(target)
    .bind(revision)
    .fetch_optional(&state.db)
    .await?;
    row.map(|(share_id, peer_id, json)| {
        Ok(Snapshot {
            share_id,
            peer_id,
            view: serde_json::from_str(&json)?,
        })
    })
    .transpose()
}

#[cfg(test)]
pub(super) async fn expire(state: &AppState, target: &str) {
    sqlx::query("DELETE FROM recovery_version_snapshots WHERE target_id=?")
        .bind(target)
        .execute(&state.db)
        .await
        .unwrap();
}

#[derive(Deserialize)]
pub struct Query {
    pub kind: RecoveryKind,
    pub id: String,
    pub peer_id: Option<String>,
}

#[derive(Deserialize)]
pub struct PreviewQuery {
    #[serde(flatten)]
    pub target: Query,
    pub revision: String,
    pub path: String,
    pub source: bool,
}

impl Query {
    pub fn target(&self) -> RecoveryTarget {
        RecoveryTarget {
            kind: self.kind,
            id: self.id.clone(),
            peer_id: self.peer_id.clone(),
        }
    }
}

pub(super) struct Context {
    pub share: Share,
    pub peer_id: String,
    pub local_paths: Vec<String>,
    pub remote_path: String,
    pub remote_state: String,
    /// Path, checksum, size and modification time of each source file.
    pub remote_files: Vec<(String, String, u64, Option<String>)>,
    pub remote_directories: Vec<String>,
}

fn role(state: &AppState, path: &str) -> String {
    ism_core::sidecar::companion_role(
        path.rsplit('/').next().unwrap_or(path),
        &state.sidecar_extensions(),
    )
    .map_or_else(|| "original".into(), |role| role.as_str().into())
}

pub(super) async fn local(
    state: &AppState,
    share: &Share,
    path: &str,
) -> AppResult<RecoveryVersion> {
    type FileEntry = (String, String, u64, String);
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&share.root_rel(path)?)?;
    let base = path.to_owned();
    let permit = state
        .version_scan_limit
        .clone()
        .acquire_owned()
        .await
        .map_err(AppError::internal)?;
    let entries = tokio::task::spawn_blocking(
        move || -> AppResult<(String, Vec<FileEntry>, Vec<String>)> {
            let _permit = permit;
            let meta = match std::fs::symlink_metadata(&abs) {
                Ok(meta) => meta,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(("absent".into(), Vec::new(), Vec::new()));
                }
                Err(error) => return Err(error.into()),
            };
            let folder = meta.is_dir();
            if !folder && !meta.is_file() {
                return Err(AppError::invalid_location(
                    "version evidence is not a regular file or directory",
                ));
            }
            let mut out = Vec::new();
            let mut directories = Vec::new();
            let paths = if folder {
                let entries =
                    crate::ops::plan::folder_inventory_bounded(&abs, 10_000).map_err(|error| {
                        if error.kind() == std::io::ErrorKind::InvalidData {
                            AppError::conflict(
                                "version_evidence_too_large",
                                "folder exceeds the 10,000-entry version evidence limit",
                            )
                        } else {
                            error.into()
                        }
                    })?;
                directories.push(base.clone());
                directories.extend(
                    entries
                        .iter()
                        .filter(|entry| entry.sha256.is_none() && !entry.name.is_empty())
                        .map(|entry| format!("{base}/{}", entry.name)),
                );
                directories.sort();
                entries
                    .into_iter()
                    .filter_map(|entry| entry.sha256.map(|hash| (entry.name, hash)))
                    .collect()
            } else {
                let hash = files::replacement_fingerprint(&abs)?
                    .ok_or_else(|| AppError::conflict("revision_changed", "file disappeared"))?
                    .1;
                vec![(String::new(), hash)]
            };
            for (name, expected) in paths {
                let file = if name.is_empty() {
                    abs.clone()
                } else {
                    abs.join(&name)
                };
                let before = std::fs::symlink_metadata(&file)?;
                let (revision, hash) = files::replacement_fingerprint(&file)?
                    .ok_or_else(|| AppError::conflict("revision_changed", "file disappeared"))?;
                if !before.is_file() || revision != files::revision(&before) || hash != expected {
                    return Err(AppError::conflict(
                        "revision_changed",
                        "version changed during verification",
                    ));
                }
                let path = if name.is_empty() {
                    base.clone()
                } else {
                    format!("{base}/{name}")
                };
                let modified =
                    chrono::DateTime::from_timestamp_nanos(files::mtime_ns(&before)).to_rfc3339();
                out.push((path, hash, before.len(), modified));
            }
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok((
                if folder { "folder" } else { "file" }.into(),
                out,
                directories,
            ))
        },
    )
    .await
    .map_err(AppError::internal)??;
    Ok(RecoveryVersion {
        instance: state.instance_name().await,
        path: path.into(),
        state: entries.0,
        files: entries
            .1
            .into_iter()
            .map(|(path, sha256, size, modified_at)| RecoveryFileVersion {
                role: role(state, &path),
                path,
                sha256,
                size: Some(size),
                modified_at: Some(modified_at),
            })
            .collect(),
        directories: entries.2,
    })
}

/// The comparison must answer before a reverse proxy in front of this instance times out.
const COMPARISON_DEADLINE: std::time::Duration = std::time::Duration::from_secs(50);

/// Retry immediately when a disconnected peer becomes reachable again.
pub(super) async fn wake_peer(state: &AppState, peer_id: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE conflict_evidence_jobs SET next_attempt=''
         WHERE state='queued' AND error IS NOT NULL AND target_id IN
         (SELECT id FROM conflicts WHERE peer_id=? AND status='open')",
    )
    .bind(peer_id)
    .execute(&state.db)
    .await?;
    state.version_wake.notify_waiters();
    Ok(())
}

pub(super) async fn reprepare(state: &AppState, target: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE conflict_evidence_jobs SET generation=generation+1, state='queued',
         attempts=0,next_attempt='',error=NULL,error_detail=NULL WHERE target_id=?",
    )
    .bind(target)
    .execute(&state.db)
    .await?;
    state.version_wake.notify_waiters();
    Ok(())
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum EvidenceState {
    Queued,
    Retrying,
    Running,
    Unavailable,
    Stopped,
    Blocked,
}

pub(crate) struct EvidenceGroup {
    pub key: String,
    pub state: EvidenceState,
    pub count: u64,
    pub first_at: String,
    pub observed_at: Option<String>,
    pub next_at: Option<String>,
    pub example_error: Option<String>,
    pub examples: Vec<ism_core::dto::ActivityExample>,
}

/// Read durable job states alongside actual worker observations, without claiming stale rows run.
pub(crate) async fn evidence_groups(state: &AppState) -> AppResult<Vec<EvidenceGroup>> {
    use std::collections::HashMap;

    #[derive(sqlx::FromRow)]
    struct Group {
        state: String,
        error: Option<String>,
        example_error: Option<String>,
        count: i64,
        first_at: String,
        next_at: Option<String>,
        unclassified_retry: i64,
    }

    let observations = state.worker_evidence.conflict_observations();
    let workers_stopped = [EVIDENCE_WORKER_ONE, EVIDENCE_WORKER_TWO]
        .iter()
        .all(|name| observations.stopped_workers.contains(*name));
    let mut tx = state.db.begin().await?;
    let rows: Vec<Group> = sqlx::query_as(
        "SELECT j.state,j.error,MIN(j.error_detail) AS example_error,COUNT(*) AS count,
                MIN(c.created_at) AS first_at,MIN(NULLIF(j.next_attempt,'')) AS next_at,
                SUM(CASE WHEN j.next_attempt='' OR
                    strftime('%Y-%m-%dT%H:%M:%fZ',j.next_attempt) IS NOT j.next_attempt
                    THEN 1 ELSE 0 END) AS unclassified_retry
         FROM conflict_evidence_jobs j JOIN conflicts c ON c.id=j.target_id
         WHERE c.status='open' AND j.state NOT IN ('ready','running')
         GROUP BY j.state,j.error ORDER BY j.state,j.error",
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut groups: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let status = match (row.state.as_str(), row.error.as_ref()) {
                ("queued", _) if workers_stopped => EvidenceState::Stopped,
                ("queued", None) => EvidenceState::Queued,
                ("queued", Some(_)) => EvidenceState::Retrying,
                ("blocked", _) => EvidenceState::Blocked,
                _ => EvidenceState::Unavailable,
            };
            EvidenceGroup {
                key: format!(
                    "conflict-evidence:{}:{}",
                    row.state,
                    row.error.as_deref().unwrap_or("none")
                ),
                state: status,
                count: row.count.max(0) as u64,
                first_at: row.first_at,
                observed_at: None,
                next_at: if row.unclassified_retry == 0 {
                    row.next_at.filter(|at| crate::db::parse_ts(at).is_some())
                } else {
                    None
                },
                example_error: row.example_error.map(|error| crate::error::redact(&error)),
                examples: Vec::new(),
            }
        })
        .collect();

    let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
        "SELECT j.target_id,j.generation,c.created_at,c.rel_path
         FROM conflict_evidence_jobs j JOIN conflicts c ON c.id=j.target_id
         WHERE c.status='open' AND j.state='running' ORDER BY c.created_at,j.target_id",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let mut current: HashMap<EvidenceState, EvidenceGroup> = HashMap::new();
    for (target_id, generation, first_at, path) in rows {
        let key = (target_id, generation);
        let (status, observed_at) = if let Some(at) = observations.active.get(&key) {
            (EvidenceState::Running, Some(at.clone()))
        } else if let Some(failure) = observations.stopped.get(&key) {
            (EvidenceState::Stopped, Some(failure.stopped_at.clone()))
        } else {
            (EvidenceState::Unavailable, None)
        };
        let group = current.entry(status).or_insert_with(|| EvidenceGroup {
            key: match status {
                EvidenceState::Running => "conflict-evidence:running",
                EvidenceState::Stopped => "conflict-evidence:stopped",
                _ => "conflict-evidence:unverified",
            }
            .into(),
            state: status,
            count: 0,
            first_at: first_at.clone(),
            observed_at: None,
            next_at: None,
            example_error: None,
            examples: Vec::new(),
        });
        group.count += 1;
        if first_at < group.first_at {
            group.first_at = first_at;
        }
        if observed_at > group.observed_at {
            group.observed_at = observed_at;
        }
        if group.examples.len() < 5 {
            group.examples.push(ism_core::dto::ActivityExample {
                resource: path,
                detail: None,
            });
        }
    }
    groups.extend(current.into_values());
    groups.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(groups)
}

const EVIDENCE_WORKER_ONE: &str = "Conflict evidence worker 1";
const EVIDENCE_WORKER_TWO: &str = "Conflict evidence worker 2";
const EVIDENCE_NOTIFIER: &str = "Conflict evidence notifications";

struct EvidenceSupervisor(AppState);

impl Drop for EvidenceSupervisor {
    fn drop(&mut self) {
        self.0.worker_evidence.conflict_supervisor_stopped(&[
            EVIDENCE_WORKER_ONE,
            EVIDENCE_WORKER_TWO,
            EVIDENCE_NOTIFIER,
        ]);
        self.0.emit(crate::state::ServerEvent::ActivityChanged);
    }
}

async fn worker(state: AppState, name: &'static str) {
    loop {
        let notified = state.version_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let claim: Result<Option<(String, i64, i64)>, sqlx::Error> = sqlx::query_as(
            "UPDATE conflict_evidence_jobs SET state='running'
             WHERE target_id=(SELECT j.target_id FROM conflict_evidence_jobs j
                 JOIN conflicts c ON c.id=j.target_id
                 WHERE j.state='queued' AND j.next_attempt<=? AND c.status='open'
                 ORDER BY j.next_attempt,j.target_id LIMIT 1)
             AND state='queued' RETURNING target_id,generation,attempts",
        )
        .bind(crate::db::now())
        .fetch_optional(&state.db)
        .await;
        match claim {
            Ok(Some((id, generation, attempts))) => {
                state
                    .worker_evidence
                    .conflict_claimed(name, &id, generation);
                state.emit(crate::state::ServerEvent::ActivityChanged);
                let target = RecoveryTarget {
                    kind: RecoveryKind::Conflict,
                    id: id.clone(),
                    peer_id: None,
                };
                let result = within(COMPARISON_DEADLINE, get(&state, &target)).await;
                let notify = result.is_ok() || attempts == 0;
                let updated = match result {
                    Ok(_) => {
                        sqlx::query(
                            "UPDATE conflict_evidence_jobs SET state='ready',error=NULL,error_detail=NULL
                         WHERE target_id=? AND generation=? AND state='running'",
                        )
                        .bind(&id)
                        .bind(generation)
                        .execute(&state.db)
                        .await
                    }
                    Err(error) => {
                        if attempts == 0 {
                            tracing::warn!(conflict_id=%id, code=%error.code(), "conflict evidence preparation deferred: {}", error.message());
                        } else {
                            tracing::debug!(conflict_id=%id, code=%error.code(), "conflict evidence preparation still deferred: {}", error.message());
                        }
                        let seconds = 2_i64 << attempts.clamp(0, 7);
                        let next =
                            crate::db::ts(chrono::Utc::now() + chrono::Duration::seconds(seconds));
                        sqlx::query(
                            "UPDATE conflict_evidence_jobs SET state=?,attempts=attempts+1,
                             next_attempt=?,error=?,error_detail=? WHERE target_id=? AND generation=? AND state='running'",
                        )
                        .bind(if error.code() == "version_evidence_too_large" { "blocked" } else { "queued" })
                        .bind(next)
                        .bind(error.code())
                        .bind(crate::error::redact(error.message()))
                        .bind(&id)
                        .bind(generation)
                        .execute(&state.db)
                        .await
                    }
                };
                let retry_after_save_failure = match updated {
                    Ok(result) if result.rows_affected() > 0 && notify => {
                        state.version_event_wake.notify_one();
                        false
                    }
                    Ok(_) => false,
                    Err(error) => {
                        tracing::error!(conflict_id=%id, %error, "could not record conflict evidence preparation");
                        loop {
                            match sqlx::query(
                                "UPDATE conflict_evidence_jobs SET state='queued' WHERE target_id=? AND generation=? AND state='running'",
                            ).bind(&id).bind(generation).execute(&state.db).await {
                                Ok(_) => break,
                                Err(reset) => {
                                    tracing::error!(conflict_id=%id, %reset, "could not release conflict evidence claim; retrying");
                                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                }
                            }
                        }
                        true
                    }
                };
                state
                    .worker_evidence
                    .conflict_finished(name, &id, generation);
                state.emit(crate::state::ServerEvent::ActivityChanged);
                if retry_after_save_failure {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
            Ok(None) => {
                let next: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
                    "SELECT MIN(j.next_attempt) FROM conflict_evidence_jobs j
                     JOIN conflicts c ON c.id=j.target_id WHERE j.state='queued' AND c.status='open'",
                ).fetch_one(&state.db).await;
                let delay = match next {
                    Ok(Some(next)) => crate::db::parse_ts(&next)
                        .map(|at| (at - chrono::Utc::now()).to_std().unwrap_or_default())
                        .unwrap_or_default(),
                    Ok(None) => std::time::Duration::from_secs(300),
                    Err(error) => {
                        tracing::error!(%error, "could not schedule conflict evidence preparation");
                        std::time::Duration::from_secs(5)
                    }
                };
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = &mut notified => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "could not claim conflict evidence preparation");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

pub(super) async fn run(state: AppState) {
    loop {
        match sqlx::query("UPDATE conflict_evidence_jobs SET state='queued' WHERE state='running'")
            .execute(&state.db)
            .await
        {
            Ok(_) => break,
            Err(error) => {
                tracing::error!(%error, "could not restore conflict evidence jobs");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
    let first = worker(state.clone(), EVIDENCE_WORKER_ONE);
    let second = worker(state.clone(), EVIDENCE_WORKER_TWO);
    let notifier_state = state.clone();
    let notifier = async move {
        loop {
            notifier_state.version_event_wake.notified().await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            notifier_state.emit(crate::state::ServerEvent::ConflictsChanged);
        }
    };
    supervise_workers(state, first, second, notifier).await;
}

pub(crate) async fn supervise_workers(
    state: AppState,
    first: impl std::future::Future<Output = ()> + Send + 'static,
    second: impl std::future::Future<Output = ()> + Send + 'static,
    notifier: impl std::future::Future<Output = ()> + Send + 'static,
) {
    use std::collections::HashMap;

    for name in [EVIDENCE_WORKER_ONE, EVIDENCE_WORKER_TWO, EVIDENCE_NOTIFIER] {
        crate::activity::worker_started(&state, name, crate::worker_evidence::Kind::Other);
    }
    let _supervisor = EvidenceSupervisor(state.clone());
    let mut workers = tokio::task::JoinSet::new();
    let mut names = HashMap::new();
    names.insert(workers.spawn(first).id(), EVIDENCE_WORKER_ONE);
    names.insert(workers.spawn(second).id(), EVIDENCE_WORKER_TWO);
    names.insert(workers.spawn(notifier).id(), EVIDENCE_NOTIFIER);
    while let Some(result) = workers.join_next_with_id().await {
        let (id, message) = match result {
            Ok((id, ())) => (id, "Background worker stopped unexpectedly"),
            Err(error) => {
                tracing::error!("conflict evidence worker failed: {error}");
                (error.id(), "Background worker panicked or was cancelled")
            }
        };
        let Some(name) = names.remove(&id) else {
            tracing::error!("an unidentified conflict evidence worker stopped");
            continue;
        };
        if name != EVIDENCE_NOTIFIER {
            state.worker_evidence.conflict_worker_stopped(name);
        }
        tracing::error!(worker = name, "{message}");
        crate::activity::worker_stopped(&state, name, message).await;
    }
}

/// [`get`] bounded by a deadline; a slow peer becomes a retryable error instead of a proxy 504.
pub async fn get_bounded(state: &AppState, target: &RecoveryTarget) -> AppResult<RecoveryVersions> {
    if target.kind == RecoveryKind::Conflict && target.peer_id.is_none() {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT s.view_json FROM recovery_version_snapshots s
             JOIN conflicts c ON c.id=s.target_id AND c.share_id=s.share_id AND c.peer_id=s.peer_id
             JOIN conflict_evidence_jobs j ON j.target_id=c.id
             WHERE s.target_id=? AND c.status='open' AND j.state='ready'
             ORDER BY s.created_at DESC, s.rowid DESC LIMIT 1",
        )
        .bind(&target.id)
        .fetch_optional(&state.db)
        .await?;
        if let Some(json) = json {
            return Ok(serde_json::from_str(&json)?);
        }
        let job: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT j.state,j.error,j.error_detail FROM conflict_evidence_jobs j
             JOIN conflicts c ON c.id=j.target_id WHERE c.id=? AND c.status='open'",
        )
        .bind(&target.id)
        .fetch_optional(&state.db)
        .await?;
        let (status, error, detail) =
            job.ok_or_else(|| AppError::not_found("open conflict not found"))?;
        if status == "ready" {
            return Err(AppError::internal("prepared conflict evidence is missing"));
        }
        if status == "blocked" {
            return Err(AppError::conflict_dynamic(
                error.unwrap_or_else(|| "version_evidence_blocked".into()),
                detail.unwrap_or_else(|| "Conflict evidence cannot be prepared.".into()),
            ));
        }
        let message = match (error, detail) {
            (Some(code), Some(detail)) => {
                format!("Conflict evidence is waiting ({code}): {detail}")
            }
            _ => "Conflict evidence is still being prepared. Try again shortly.".into(),
        };
        return Err(AppError::new(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "peer_busy",
            message,
        ));
    }
    within(COMPARISON_DEADLINE, get(state, target)).await
}

async fn within<T>(
    deadline: std::time::Duration,
    work: impl std::future::Future<Output = AppResult<T>>,
) -> AppResult<T> {
    tokio::time::timeout(deadline, work)
        .await
        .unwrap_or_else(|_| {
            Err(AppError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "peer_busy",
                "The other instance is busy. Try again in a moment.",
            ))
        })
}

pub async fn get(state: &AppState, target: &RecoveryTarget) -> AppResult<RecoveryVersions> {
    if let Some(peer_id) = &target.peer_id {
        // Only known work owned by this receiver can be forwarded.
        if super::recovery::outgoing(state, peer_id, target.kind, &target.id)
            .await?
            .is_none()
        {
            return Err(AppError::not_found("receiver recovery target not found"));
        }
        let peer = net::peer(state, peer_id).await?;
        let kind = serde_json::to_value(target.kind)?;
        let kind = kind
            .as_str()
            .ok_or_else(|| AppError::internal("invalid recovery kind"))?;
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [("kind", kind), ("id", target.id.as_str())],
        )
        .map_err(AppError::internal)?;
        return net::get_json(
            state,
            &peer,
            &format!(
                "/api/peer/v1/recovery/versions?{}",
                url.query().unwrap_or_default()
            ),
        )
        .await
        .map_err(|error| AppError::conflict("peer_evidence_unavailable", error));
    }
    if target.kind != RecoveryKind::Conflict {
        return Err(AppError::bad_request(
            "version_decision_required",
            "version evidence belongs to a conflict",
        ));
    }
    let generation: Option<i64> = sqlx::query_scalar(
        "SELECT j.generation FROM conflict_evidence_jobs j JOIN conflicts c ON c.id=j.target_id
         WHERE j.target_id=? AND c.status='open'",
    )
    .bind(&target.id)
    .fetch_optional(&state.db)
    .await?;
    let generation = generation.ok_or_else(|| {
        AppError::conflict("not_open", "the version decision is already executing")
    })?;
    let context = admin::version_context(state, &target.id).await?;
    let peer = net::peer(state, &context.peer_id).await?;
    let mut receiving = Vec::new();
    for path in context.local_paths {
        receiving.push(local(state, &context.share, &path).await?);
    }
    let mut directories = context.remote_directories;
    directories.sort();
    directories.dedup();
    let source = RecoveryVersion {
        instance: peer.name,
        path: context.remote_path,
        state: context.remote_state,
        files: context
            .remote_files
            .into_iter()
            .map(|(path, sha256, size, modified_at)| RecoveryFileVersion {
                role: role(state, &path),
                path,
                sha256,
                size: Some(size),
                modified_at,
            })
            .collect(),
        directories,
    };
    let revision = crate::crypto::sha256_hex(&serde_json::to_vec(&(
        &context.share.id,
        &context.peer_id,
        &receiving,
        &source,
    ))?);
    let view = RecoveryVersions {
        share: context.share.name.clone(),
        revision,
        receiving,
        source,
    };
    remember(
        state,
        &target.id,
        generation,
        &context.share.id,
        &context.peer_id,
        &view,
    )
    .await?;
    Ok(view)
}

type MovedVersion<'a> = (&'a str, &'a str, &'a [(String, String, u64)], &'a [String]);

pub(super) async fn validate_reviewed(
    state: &AppState,
    id: &str,
    revision: &str,
    remote: Option<&super::recovery::FileEvidence>,
    moved: Option<MovedVersion<'_>>,
) -> AppResult<Vec<RecoveryVersion>> {
    let Snapshot {
        share_id,
        peer_id,
        view,
        ..
    } = recall(state, id, revision)
        .await?
        .ok_or_else(|| AppError::conflict("version_conflict", "review current versions again"))?;
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == share_id && share.includes(&peer_id))
        .ok_or_else(|| {
            AppError::conflict("peer_share_changed", "the reviewed share is unavailable")
        })?;
    for expected in &view.receiving {
        if serde_json::to_value(local(state, &share, &expected.path).await?)?
            != serde_json::to_value(expected)?
        {
            return Err(AppError::conflict(
                "version_conflict",
                "receiving files changed after review",
            ));
        }
    }
    if let Some(remote) = remote {
        let status = if remote.deleted {
            "deleted"
        } else if remote.sha256.is_some() {
            "file"
        } else {
            "absent"
        };
        let matches = remote.inventory.is_none()
            && view.source.path == remote.path
            && view.source.state == status
            && match (&remote.sha256, remote.size, view.source.files.as_slice()) {
                (Some(hash), Some(size), [file]) => {
                    file.path == remote.path && file.sha256 == *hash && file.size == Some(size)
                }
                (None, None, []) => true,
                _ => false,
            };
        if !matches {
            return Err(AppError::conflict(
                "version_conflict",
                "source files changed after review",
            ));
        }
    }
    if let Some((location, state, files, directories)) = moved {
        let mut directories = directories.to_vec();
        directories.sort();
        directories.dedup();
        let matches = view.source.path == location
            && view.source.state == state
            && view.source.directories == directories
            && view.source.files.len() == files.len()
            && files.iter().all(|(path, hash, size)| {
                view.source.files.iter().any(|reviewed| {
                    reviewed.path == *path
                        && reviewed.sha256 == *hash
                        && reviewed.size == Some(*size)
                })
            });
        if !matches {
            return Err(AppError::conflict(
                "version_conflict",
                "source files changed after review",
            ));
        }
    }
    Ok(view.receiving)
}

async fn require_preview_transfer(state: &AppState, share: &Share, peer_id: &str) -> AppResult<()> {
    if !super::transfer::allowed(state, &share.id, peer_id).await?.0 {
        return Err(AppError::conflict(
            "preview_outside_transfer_hours",
            "Peer previews are unavailable outside allowed file-transfer hours.",
        ));
    }
    Ok(())
}

pub(crate) async fn authorize_peer_preview(
    state: &AppState,
    query: &PreviewQuery,
    peer_id: &str,
) -> AppResult<()> {
    let snapshot = recall(state, &query.target.id, &query.revision)
        .await?
        .ok_or_else(|| AppError::conflict("preview_expired", "refresh current versions"))?;
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == snapshot.share_id && share.includes(peer_id))
        .filter(|_| snapshot.peer_id == peer_id)
        .ok_or_else(|| {
            AppError::conflict("peer_share_changed", "the preview share is unavailable")
        })?;
    require_preview_transfer(state, &share, peer_id).await
}

pub async fn preview(state: &AppState, query: &PreviewQuery) -> AppResult<Response> {
    if let Some(peer_id) = &query.target.peer_id {
        if super::recovery::outgoing(state, peer_id, query.target.kind, &query.target.id)
            .await?
            .is_none()
        {
            return Err(AppError::not_found("receiver recovery target not found"));
        }
        let reviewed = get_bounded(state, &query.target.target()).await?;
        if reviewed.revision != query.revision {
            return Err(AppError::conflict(
                "preview_changed",
                "refresh current versions",
            ));
        }
        let share = super::shares(state)
            .await?
            .into_iter()
            .find(|share| share.name == reviewed.share && share.includes(peer_id))
            .ok_or_else(|| {
                AppError::conflict("peer_share_changed", "the preview share is unavailable")
            })?;
        require_preview_transfer(state, &share, peer_id).await?;
        let peer = net::peer(state, peer_id).await?;
        let source = query.source.to_string();
        let kind = serde_json::to_value(query.target.kind)?;
        let kind = kind
            .as_str()
            .ok_or_else(|| AppError::internal("invalid recovery kind"))?;
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [
                ("kind", kind),
                ("id", query.target.id.as_str()),
                ("revision", query.revision.as_str()),
                ("path", query.path.as_str()),
                ("source", source.as_str()),
            ],
        )
        .map_err(AppError::internal)?;
        let mut response = net::request(
            state,
            &peer.url,
            &peer.secret,
            reqwest::Method::GET,
            &format!(
                "/api/peer/v1/recovery/preview?{}",
                url.query().unwrap_or_default()
            ),
            None,
        )
        .timeout(std::time::Duration::from_secs(50))
        .send()
        .await
        .map_err(|error| {
            if error.is_timeout() {
                preview_image::busy()
            } else {
                AppError::conflict("preview_unavailable", net::net_error(&error))
            }
        })?;
        if !response.status().is_success() {
            let status = response.status();
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(AppError::internal)? {
                if body.len() + chunk.len() > 4096 {
                    return Err(AppError::conflict(
                        "preview_unavailable",
                        format!("Peer returned HTTP {status}"),
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            let code = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|value| value["error"]["code"].as_str().map(str::to_owned))
                .unwrap_or_default();
            if code == "preview_busy" {
                return Err(preview_image::busy());
            }
            if code == "preview_outside_transfer_hours" {
                return Err(AppError::conflict(
                    "preview_outside_transfer_hours",
                    "Peer previews are unavailable outside allowed file-transfer hours.",
                ));
            }
            return Err(AppError::conflict(
                "preview_unavailable",
                format!("Peer returned HTTP {status}"),
            ));
        }
        let mut response = crate::routes::activity::image_response(response).await?;
        require_preview_transfer(state, &share, peer_id).await?;
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("private, no-store"),
        );
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            header::HeaderValue::from_static("nosniff"),
        );
        return Ok(response);
    }
    preview_image::within(
        std::time::Duration::from_secs(40),
        preview_local(state, query, None),
    )
    .await
}

pub(crate) async fn peer_preview(
    state: &AppState,
    query: &PreviewQuery,
    requester: &str,
) -> AppResult<Response> {
    preview_image::within(
        std::time::Duration::from_secs(40),
        preview_local(state, query, Some(requester)),
    )
    .await
}

async fn preview_local(
    state: &AppState,
    query: &PreviewQuery,
    requester: Option<&str>,
) -> AppResult<Response> {
    if query.target.kind != RecoveryKind::Conflict {
        return Err(AppError::not_found("version preview not found"));
    }
    let Snapshot {
        share_id,
        peer_id,
        view,
        ..
    } = recall(state, &query.target.id, &query.revision)
        .await?
        .ok_or_else(|| AppError::conflict("preview_expired", "refresh current versions"))?;
    let selected = if query.source {
        view.source
            .files
            .iter()
            .find(|file| file.path == query.path)
    } else {
        view.receiving
            .iter()
            .flat_map(|version| &version.files)
            .find(|file| file.path == query.path)
    }
    .ok_or_else(|| AppError::not_found("file is outside this version decision"))?;
    let avif = match query
        .path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "jpg" | "jpeg" | "png" | "webp" => false,
        "avif" => true,
        _ => return Err(AppError::not_found("this file has no image preview")),
    };
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == share_id && share.includes(&peer_id))
        .ok_or_else(|| {
            AppError::conflict("peer_share_changed", "the preview share is unavailable")
        })?;
    let max_input = if avif {
        preview_image::MAX_AVIF_INPUT
    } else {
        preview_image::MAX_INPUT
    };
    let size_error = || {
        AppError::conflict(
            "preview_unavailable",
            if avif {
                "AVIF previews larger than 4 MiB are not supported."
            } else {
                "Image exceeds the 64 MiB preview input limit."
            },
        )
    };
    if selected.size.is_some_and(|size| size > max_input as u64) {
        return Err(size_error());
    }
    if query.source {
        require_preview_transfer(state, &share, &peer_id).await?;
    }
    let permit = preview_image::acquire().await?;
    if let Some(requester) = requester {
        require_preview_transfer(state, &share, requester).await?;
    }
    let bytes = if query.source {
        require_preview_transfer(state, &share, &peer_id).await?;
        let peer = net::peer(state, &peer_id).await?;
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [
                ("share", share.name.as_str()),
                ("path", query.path.as_str()),
                ("sha256", selected.sha256.as_str()),
            ],
        )
        .map_err(AppError::internal)?;
        let mut response = net::request(
            state,
            &peer.url,
            &peer.secret,
            reqwest::Method::GET,
            &format!("/api/peer/v1/content?{}", url.query().unwrap_or_default()),
            None,
        )
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|error| {
            if error.is_timeout() {
                preview_image::busy()
            } else {
                AppError::conflict("preview_unavailable", net::net_error(&error))
            }
        })?;
        if !response.status().is_success() {
            if matches!(
                response.status(),
                reqwest::StatusCode::CONFLICT | reqwest::StatusCode::NOT_FOUND
            ) {
                reprepare(state, &query.target.id).await?;
            }
            return Err(AppError::conflict(
                "preview_unavailable",
                format!("Peer returned HTTP {}", response.status()),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(AppError::internal)? {
            if chunk.len() > max_input - bytes.len() {
                return Err(size_error());
            }
            bytes.extend_from_slice(&chunk);
        }
        bytes
    } else {
        let root = files::root(&state.db, &share.root_id).await?;
        root.ensure_available()?;
        let path = root.abs(&share.root_rel(&query.path)?)?;
        let file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                reprepare(state, &query.target.id).await?;
                return Err(AppError::conflict(
                    "preview_changed",
                    "refresh current versions",
                ));
            }
            Err(error) => return Err(error.into()),
        };
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        file.take(max_input as u64 + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > max_input {
            return Err(size_error());
        }
        bytes
    };
    let result = preview_image::render(bytes, selected.sha256.clone(), permit, avif).await;
    if result
        .as_ref()
        .is_err_and(|error| error.code() == "preview_changed")
    {
        reprepare(state, &query.target.id).await?;
    }
    let (mime, bytes) = result?;
    if let Some(requester) = requester {
        require_preview_transfer(state, &share, requester).await?;
    }
    Ok((
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, "private, no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        bytes,
    )
        .into_response())
}

#[cfg(test)]
mod deadline_tests {
    #[tokio::test]
    async fn slow_comparison_becomes_retryable_instead_of_hanging() {
        let error = super::within(
            std::time::Duration::from_millis(20),
            std::future::pending::<crate::error::AppResult<()>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code(), "peer_busy");
    }
}
