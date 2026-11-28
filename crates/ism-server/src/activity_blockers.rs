//! Current waiting evidence, read from the queues that actually govern execution.

use ism_core::dto::{AttentionCode, AttentionState, BackgroundActivity, BackgroundStatus};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

use crate::db;
use crate::error::{AppResult, redact};
use crate::state::AppState;

pub(crate) fn entry(
    key: String,
    title: &str,
    resource: String,
    phase: String,
    page: &str,
) -> BackgroundActivity {
    let now = db::now();
    BackgroundActivity {
        id: format!("state:{key}"),
        task_key: key,
        title: title.into(),
        resource,
        status: BackgroundStatus::Waiting,
        phase,
        done: 0,
        total: None,
        unit: "items".into(),
        started_at: now.clone(),
        updated_at: now,
        finished_at: None,
        next_at: None,
        error: None,
        operation_id: None,
        page: Some(page.into()),
        count: 1,
        reasons: Vec::new(),
        examples: Vec::new(),
    }
}

const EXAMPLES: usize = 5;
const REASONS: usize = 5;

// Keep the full count and oldest timestamp, but only materialize the displayed examples.
// NOT MATERIALIZED avoids storing the entire backlog in a temporary CTE table.
// Seek to the first matching rowid so sampling does not reread an unrelated upload prefix.
const UPLOAD_ASSET_QUERY: &str = "WITH matched AS NOT MATERIALIZED (
    SELECT u.id,u.operation_id,u.created_at,u.rowid AS upload_rowid FROM uploads u
    JOIN operation_items i ON i.operation_id=u.operation_id
    WHERE i.kind='publish_upload' AND i.status='done' AND i.result_json IS NOT NULL
      AND json_extract(i.result_json,'$.asset_id') IS NULL
      AND u.has_original IS NOT 0
      AND (u.has_original=1
           OR json_extract(u.request_json,'$.existing_original.name') IS NOT NULL
           OR EXISTS(SELECT 1 FROM json_each(u.request_json,'$.files') f
                     WHERE json_extract(f.value,'$.role')='original'))
), summary AS (
    SELECT COUNT(*) AS count, MIN(created_at) AS first_at, MIN(upload_rowid) AS first_rowid
    FROM matched
)
SELECT matched.id,matched.operation_id,summary.count,summary.first_at
FROM summary CROSS JOIN matched
WHERE matched.upload_rowid >= summary.first_rowid
LIMIT ?";

async fn upload_asset_activity(pool: &sqlx::SqlitePool) -> AppResult<Option<BackgroundActivity>> {
    let started = std::time::Instant::now();
    let examples = sqlx::query(UPLOAD_ASSET_QUERY)
        .bind(EXAMPLES as i64)
        .fetch_all(pool)
        .await?;
    let count = examples
        .first()
        .map(|first| first.try_get::<i64, _>("count"))
        .transpose()?
        .unwrap_or(0);
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        count,
        returned_rows = examples.len(),
        "collected imported upload asset activity"
    );
    let Some(first) = examples.first() else {
        return Ok(None);
    };
    let mut row = entry(
        "upload-asset".into(),
        "Imported upload asset ID",
        "Uploads".into(),
        "Published file is waiting for Immich import and asset lookup".into(),
        "/operations",
    );
    row.count = count as u64;
    row.started_at = first.try_get("first_at")?;
    if row.count == 1 {
        row.operation_id = first.try_get("operation_id")?;
        row.resource = format!("Uploads / {}", first.try_get::<String, _>("id")?);
    } else {
        for example in &examples {
            row.examples.push(ism_core::dto::ActivityExample {
                resource: example.try_get("id")?,
                detail: None,
            });
        }
    }
    Ok(Some(row))
}

/// Folds per-item evidence into one row per group so thousands of files stay readable and cheap.
#[derive(Default)]
struct Groups {
    order: Vec<String>,
    rows: HashMap<String, (BackgroundActivity, HashMap<String, u64>)>,
}

impl Groups {
    /// `row.resource` names the group scope; `item` is the individual file or target.
    fn add(
        &mut self,
        key: String,
        mut row: BackgroundActivity,
        item: &str,
        reason: Option<String>,
    ) {
        let reason = reason.map(|r| r.replace(item, "<item>"));
        let example = ism_core::dto::ActivityExample {
            resource: item.into(),
            detail: row.error.clone(),
        };
        match self.rows.get_mut(&key) {
            Some((group, reasons)) => {
                group.count += 1;
                group.done += row.done;
                group.total = group.total.zip(row.total).map(|(a, b)| a + b);
                if row.started_at < group.started_at {
                    group.started_at = row.started_at;
                }
                if row.updated_at > group.updated_at {
                    group.updated_at = row.updated_at;
                }
                group.next_at = match (group.next_at.take(), row.next_at) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
                if group.examples.len() < EXAMPLES {
                    group.examples.push(example);
                }
                tally(reasons, reason);
            }
            None => {
                let mut reasons = HashMap::new();
                tally(&mut reasons, reason);
                row.id = format!("state:{key}");
                row.task_key = key.clone();
                row.examples = vec![example];
                self.order.push(key.clone());
                self.rows.insert(key, (row, reasons));
            }
        }
    }

    fn finish(mut self, out: &mut Vec<BackgroundActivity>) {
        for key in self.order {
            let Some((mut row, reasons)) = self.rows.remove(&key) else {
                continue;
            };
            if row.count == 1 {
                let item = row.examples.pop().map(|e| e.resource).unwrap_or_default();
                row.resource = format!("{} / {item}", row.resource);
                row.examples.clear();
            } else {
                let mut reasons: Vec<_> = reasons.into_iter().collect();
                reasons.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                row.reasons = reasons
                    .into_iter()
                    .take(REASONS)
                    .map(|(reason, count)| ism_core::dto::ActivityReason { reason, count })
                    .collect();
                if !row.reasons.is_empty() {
                    row.error = None;
                }
                row.operation_id = None;
            }
            out.push(row);
        }
    }
}

fn tally(reasons: &mut HashMap<String, u64>, reason: Option<String>) {
    if let Some(reason) = reason {
        *reasons.entry(reason).or_default() += 1;
    }
}

pub async fn get(state: &AppState) -> AppResult<(Vec<BackgroundActivity>, Vec<String>)> {
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    let (local, local_warnings) =
        crate::local_evidence::snapshot(state, &state.instance_name().await, &db::now()).await?;
    warnings.extend(local_warnings);
    for item in local
        .into_iter()
        .filter(|item| item.code != AttentionCode::LibraryScanFailed)
    {
        let mut row = entry(
            item.id,
            &item.title,
            item.share_name.unwrap_or_default(),
            item.message,
            &item.href,
        );
        row.status = match item.state {
            AttentionState::Blocked => BackgroundStatus::Blocked,
            AttentionState::Failed => BackgroundStatus::Failed,
            _ => BackgroundStatus::Waiting,
        };
        row.count = item.count;
        row.examples = item.examples;
        row.updated_at = item.observed_at;
        row.next_at = item.next_at;
        out.push(row);
    }
    let remote = crate::peers::attention::snapshot(state).await?;
    warnings.extend(remote.warnings);
    for item in remote.items {
        let mut row = entry(
            item.id,
            &item.title,
            item.share_name.map_or_else(
                || item.instance_name.clone().unwrap_or_default(),
                |share| {
                    format!(
                        "{} / {share}",
                        item.instance_name.clone().unwrap_or_default()
                    )
                },
            ),
            item.message,
            &item.href,
        );
        row.status = match item.state {
            AttentionState::Blocked => BackgroundStatus::Blocked,
            AttentionState::Failed => BackgroundStatus::Failed,
            _ => BackgroundStatus::Waiting,
        };
        row.count = item.count;
        row.updated_at = item.observed_at;
        row.next_at = item.next_at;
        out.push(row);
    }
    for view in crate::watch::scheduler::views(state).await? {
        if view.due_at.is_none() && view.last_error.is_none() && !view.running {
            continue;
        }
        let mut row = entry(
            format!("library:{}", view.library_id),
            "Library synchronization",
            view.library_name.unwrap_or(view.library_id),
            if view.running {
                "Synchronizing scoped paths or waiting for the Immich library queue".into()
            } else if view.waiting_for_barrier {
                "Waiting for the Immich maintenance barrier".into()
            } else if view.due_in_secs.is_some_and(|secs| secs > 0) {
                "Waiting for file changes to settle (debounce)".into()
            } else if view.last_error.is_some() {
                "Scoped synchronization failed; retained work waits for a new request".into()
            } else {
                "Waiting for the library scheduler".into()
            },
            "/mappings",
        );
        row.started_at = view
            .last_started_at
            .unwrap_or_else(|| row.started_at.clone());
        row.updated_at = view
            .last_finished_at
            .unwrap_or_else(|| row.updated_at.clone());
        row.next_at = view.due_at;
        row.error = view.last_error.map(|e| redact(&e));
        if view.running {
            row.status = BackgroundStatus::Running;
        } else if row.error.is_some() && row.next_at.is_none() {
            row.status = BackgroundStatus::Blocked;
        }
        out.push(row);
    }
    let shares = crate::peers::shares(state).await?;
    let pending_shares = state
        .share_changes
        .lock()
        .map_err(|_| crate::error::AppError::internal("share scan schedule lock poisoned"))?
        .clone();
    for share in shares.iter().filter(|s| s.enabled) {
        let Some(due) = pending_shares.get(&share.id) else {
            continue;
        };
        let mut row = entry(
            format!("share:{}", share.id),
            "Share inventory scan",
            share.name.clone(),
            if *due > std::time::Instant::now() {
                "Waiting for file changes to settle; scan readiness has not been checked".into()
            } else {
                "Share inventory rescan is queued; scan readiness has not been checked".into()
            },
            "/peers",
        );
        let delay = due.saturating_duration_since(std::time::Instant::now());
        row.next_at = chrono::Duration::from_std(delay)
            .ok()
            .and_then(|d| chrono::Utc::now().checked_add_signed(d))
            .map(db::ts);
        out.push(row);
    }
    for r in sqlx::query(
        "SELECT id,name,status,last_error,last_seen_at FROM peers WHERE status!='online'",
    )
    .fetch_all(&state.db)
    .await?
    {
        let id: String = r.try_get("id")?;
        let status: String = r.try_get("status")?;
        let phase = if status == "offline" {
            "Waiting for the peer to come online; event exchange and queued transfers cannot advance"
        } else {
            "Waiting for the peer connection to be verified; queued transfers cannot advance"
        };
        let mut row = entry(
            format!("peer:{id}"),
            "Peer connection",
            r.try_get("name")?,
            phase.into(),
            "/peers",
        );
        row.error = r
            .try_get::<Option<String>, _>("last_error")?
            .map(|e| redact(&e));
        if let Some(at) = r.try_get::<Option<String>, _>("last_seen_at")? {
            row.updated_at = at;
        }
        out.push(row);
    }
    for r in sqlx::query(
        "SELECT b.share_id,b.peer_id,b.requested_at,b.after_path,s.name AS share_name,p.name AS peer_name
         FROM share_backfills b JOIN shares s ON s.id=b.share_id JOIN peers p ON p.id=b.peer_id",
    ).fetch_all(&state.db).await? {
        let peer:String=r.try_get("peer_id")?;
        let after:String=r.try_get("after_path")?;
        let mut row=entry(format!("backfill:{peer}:{}",r.try_get::<String,_>("share_id")?),
            "Pending peer inventory",format!("{} / {}",r.try_get::<String,_>("peer_name")?,r.try_get::<String,_>("share_name")?),
            if after.is_empty() {"Waiting to receive the peer's inventory".into()}
            else {format!("Inventory pagination will continue after {after}")},"/peers");
        row.started_at=r.try_get("requested_at")?;
        out.push(row);
    }
    let receiver = crate::peer_evidence::snapshot(state).await?;
    if receiver.orphaned > 0 {
        warnings.push(
            "Some saved receiver activity refers to a missing peer; its status is unavailable."
                .into(),
        );
    }
    for peer in receiver.peers {
        if !peer.fresh {
            warnings.push(format!(
                "Receiver status for {} is not current; cached activity may be out of date.",
                peer.name
            ));
        }
        if peer.invalid_count > 0 {
            warnings.push(format!(
                "Some receiver activity for {} could not be read; displayed details are incomplete.",
                peer.name
            ));
        }
        let mut totals = peer.completed.clone();
        for group in &peer.groups {
            *totals
                .entry((group.share_name.clone(), group.managed_operation))
                .or_default() += group.count;
        }
        let mut active = HashMap::<(String, bool), BackgroundActivity>::new();
        for group in peer.groups {
            let (phase, status) = match group.status.as_str() {
                "queued" => ("Queued on the receiving ISM", BackgroundStatus::Waiting),
                "running" => (
                    "The receiving ISM is downloading",
                    BackgroundStatus::Running,
                ),
                "publishing" => (
                    "The receiving ISM is waiting for safe publication",
                    BackgroundStatus::Running,
                ),
                "failed" if group.managed_operation => (
                    "The receiving ISM reported a failed operation",
                    BackgroundStatus::Failed,
                ),
                "failed" => (
                    "The receiving ISM reported a failure",
                    BackgroundStatus::Blocked,
                ),
                _ => ("Receiver status is not current", BackgroundStatus::Waiting),
            };
            let mut row = entry(
                format!(
                    "outgoing:{}:{}:{}:{}",
                    peer.id,
                    group.share_name,
                    group.status,
                    if group.managed_operation {
                        "operation"
                    } else {
                        "transfer"
                    }
                ),
                "Outgoing peer synchronization",
                format!("{} / {}", peer.name, group.share_name),
                phase.into(),
                "/sync",
            );
            row.count = group.count;
            row.done = group.done;
            row.total = Some(group.size);
            row.unit = "bytes".into();
            row.started_at = group.first_at;
            if let Some(at) = &peer.observed_at {
                row.updated_at = at.clone();
            }
            row.error = group.error;
            row.status = status;
            if group.count == 1 {
                if let Some(example) = group.examples.into_iter().next() {
                    row.resource = format!("{} / {}", row.resource, example.resource);
                }
            } else {
                row.examples = group.examples;
                let mut reasons: Vec<_> = group.reasons.into_iter().collect();
                reasons.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                row.reasons = reasons
                    .into_iter()
                    .take(REASONS)
                    .map(|(reason, count)| ism_core::dto::ActivityReason { reason, count })
                    .collect();
                if !row.reasons.is_empty() {
                    row.error = None;
                }
            }
            if matches!(group.status.as_str(), "queued" | "running" | "publishing") {
                let key = (group.share_name.clone(), group.managed_operation);
                let completed = peer.completed.get(&key).copied().unwrap_or(0);
                match active.get_mut(&key) {
                    Some(batch) => {
                        batch.examples.extend(row.examples);
                        batch.examples.truncate(EXAMPLES);
                        if row.started_at < batch.started_at {
                            batch.started_at = row.started_at;
                        }
                        if row.status == BackgroundStatus::Running {
                            batch.status = row.status;
                            batch.phase = row.phase;
                        }
                    }
                    None => {
                        row.task_key = format!(
                            "outgoing:{}:{}:{}",
                            peer.id,
                            group.share_name,
                            if group.managed_operation {
                                "operation"
                            } else {
                                "transfer"
                            }
                        );
                        row.id = format!("state:{}", row.task_key);
                        row.resource = format!("{} / {}", peer.name, group.share_name);
                        row.done = completed;
                        row.total = totals.get(&key).copied();
                        row.count = row.total.unwrap_or(0);
                        row.unit = if group.managed_operation {
                            "steps completed"
                        } else {
                            "files completed"
                        }
                        .into();
                        active.insert(key, row);
                    }
                }
            } else {
                out.push(row);
            }
        }
        out.extend(active.into_values());
    }
    let share_name = |id: &str| {
        shares
            .iter()
            .find(|s| s.id == id)
            .map_or_else(|| id.to_owned(), |s| s.name.clone())
    };
    let queues = crate::queue_evidence::snapshot(state).await?;
    let running = queues.running;
    let transfer_failures = state.worker_evidence.transfer_failures();
    let mut running_ids = HashSet::new();
    // Recorded running transfers can include tasks that stopped before saving their outcome.
    for r in sqlx::query(
        "SELECT t.id,t.share_id,t.rel_path,t.size,t.bytes_done,t.queued_at,t.updated_at,p.name AS peer_name
         FROM transfers t JOIN peers p ON p.id=t.peer_id WHERE t.status='running' ORDER BY t.queued_at,t.id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let id: String = r.try_get("id")?;
        running_ids.insert(id.clone());
        let share_id: String = r.try_get("share_id")?;
        let mut row = entry(
            format!("transfer:{id}"),
            "Peer file transfer",
            format!(
                "{} / {} / {}",
                r.try_get::<String, _>("peer_name")?,
                share_name(&share_id),
                r.try_get::<String, _>("rel_path")?
            ),
            "Recorded as running; worker liveness is not yet verified".into(),
            "/sync",
        );
        row.status = BackgroundStatus::Running;
        row.unit = "bytes".into();
        row.done = r.try_get::<i64, _>("bytes_done")?.max(0) as u64;
        row.total = Some(r.try_get::<i64, _>("size")?.max(0) as u64);
        row.started_at = r.try_get("queued_at")?;
        row.updated_at = r.try_get("updated_at")?;
        if let Some(failure) = transfer_failures.get(&id) {
            row.status = BackgroundStatus::Blocked;
            row.phase = failure.reason.message().into();
            row.updated_at = failure.observed_at.clone();
        }
        out.push(row);
    }
    state
        .worker_evidence
        .discard_finished_transfers(&transfer_failures, &running_ids);
    let mut failed = Groups::default();
    for r in sqlx::query(
        "SELECT t.peer_id,t.share_id,t.rel_path,t.size,t.bytes_done,t.queued_at,t.updated_at,t.error,
                p.name AS peer_name
         FROM transfers t JOIN peers p ON p.id=t.peer_id WHERE t.status='failed' ORDER BY t.queued_at,t.id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let peer_id: String = r.try_get("peer_id")?;
        let share_id: String = r.try_get("share_id")?;
        let path: String = r.try_get("rel_path")?;
        let mut row = entry(
            String::new(),
            "Peer file transfer",
            format!("{} / {}", r.try_get::<String, _>("peer_name")?, share_name(&share_id)),
            "Transfer needs recovery".into(),
            "/sync",
        );
        row.status = BackgroundStatus::Blocked;
        row.unit = "bytes".into();
        row.done = r.try_get::<i64, _>("bytes_done")?.max(0) as u64;
        row.total = Some(r.try_get::<i64, _>("size")?.max(0) as u64);
        row.started_at = r.try_get("queued_at")?;
        row.updated_at = r.try_get("updated_at")?;
        row.error = r.try_get::<Option<String>, _>("error")?.map(|e| redact(&e));
        let reason = row.error.clone();
        failed.add(format!("transfer-failed:{peer_id}:{share_id}"), row, &path, reason);
    }
    failed.finish(&mut out);
    for group in queues.queued {
        let mut row = entry(
            format!("transfer-queue:{}:{}", group.peer_id, group.share_id),
            "Peer file transfer queue",
            format!("{} / {}", group.peer_name, group.share_name),
            String::new(),
            "/sync",
        );
        row.count = group.count;
        row.unit = "bytes".into();
        row.done = group.done;
        row.total = Some(group.size);
        row.started_at = group.first_at;
        row.updated_at = group.last_at;
        if group.backoff_count > 0 {
            row.reasons.push(ism_core::dto::ActivityReason {
                reason: if group.unclassified_backoff > 0 {
                    "At least these files are in retry backoff".into()
                } else {
                    "Retry backoff after a failed transfer".into()
                },
                count: group.backoff_count,
            });
        }
        let limit = state.config.peer_transfer_concurrency as i64;
        row.phase = if group.peer_status == "offline" {
            "Waiting for the peer to come online".into()
        } else if group.peer_status != "online" {
            "Waiting for the peer connection to be verified".into()
        } else if group.root_available == Some(false) {
            row.status = BackgroundStatus::Blocked;
            if group.root_configured {
                "The mapped root is not mounted or its marker is missing".into()
            } else {
                "The transfer share's mapped root is no longer configured".into()
            }
        } else if group.backoff_count == group.count {
            row.next_at = group.next_retry.clone();
            "Retry backoff after a failed transfer".into()
        } else {
            match group.schedule {
                crate::queue_evidence::ScheduleState::Invalid => {
                    row.status = BackgroundStatus::Blocked;
                    "The transfer schedule is invalid; save valid hours in Sync settings".into()
                }
                crate::queue_evidence::ScheduleState::Closed { next_at } => {
                    row.next_at = next_at;
                    "Waiting for allowed file-transfer hours".into()
                }
                crate::queue_evidence::ScheduleState::Allowed if group.root_available.is_none() => {
                    "Mapped folder availability could not be checked; transfer readiness is unknown"
                        .into()
                }
                crate::queue_evidence::ScheduleState::Allowed if running >= limit => {
                    format!(
                        "ISM records {running} running transfers (limit {limit}); queue readiness is not fully known"
                    )
                }
                crate::queue_evidence::ScheduleState::Allowed => {
                    if group.unclassified_backoff == 0 {
                        row.next_at = group.next_retry;
                    }
                    "Queued; file-specific eligibility has not been checked".into()
                }
            }
        };
        out.push(row);
    }
    for group in crate::peers::versions::evidence_groups(state).await? {
        use crate::peers::versions::EvidenceState;
        let (phase, status) = match group.state {
            EvidenceState::Queued => (
                "Waiting for a conflict comparison worker",
                BackgroundStatus::Waiting,
            ),
            EvidenceState::Retrying => (
                "The last comparison attempt failed; it is queued for retry",
                BackgroundStatus::Waiting,
            ),
            EvidenceState::Running => (
                "Comparing versions for these conflicts",
                BackgroundStatus::Running,
            ),
            EvidenceState::Stopped => (
                "A comparison worker stopped; restart ISM when safe",
                BackgroundStatus::Blocked,
            ),
            EvidenceState::Blocked => (
                "The comparison cannot continue automatically",
                BackgroundStatus::Blocked,
            ),
            EvidenceState::Unavailable => (
                "The saved comparison state or worker could not be verified",
                BackgroundStatus::Waiting,
            ),
        };
        let mut row = entry(
            group.key,
            "Conflict evidence preparation",
            "Conflicts".into(),
            phase.into(),
            "/conflicts",
        );
        row.status = status;
        row.count = group.count;
        row.unit = "conflicts".into();
        row.started_at = group.first_at;
        row.updated_at = group.observed_at.unwrap_or_default();
        if group.state == EvidenceState::Retrying {
            row.next_at = group
                .next_at
                .filter(|at| db::parse_ts(at).is_some_and(|time| time > chrono::Utc::now()));
        }
        row.error = group.example_error.map(|error| {
            if group.count > 1 {
                format!("Example last error: {error}")
            } else {
                error
            }
        });
        row.examples = group.examples;
        if group.state == EvidenceState::Unavailable {
            warnings.push(
                "Some conflict comparison work has no verifiable worker or saved state.".into(),
            );
        }
        out.push(row);
    }
    let mut recovery = Groups::default();
    for r in sqlx::query(
        "SELECT * FROM sync_recovery WHERE state NOT IN ('fixed') ORDER BY created_at,id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let kind: String = r.try_get("kind")?;
        let target: String = r.try_get("target_id")?;
        let status: String = r.try_get("state")?;
        let detail: Option<String> = r.try_get("detail")?;
        let mut row = entry(
            String::new(),
            "Recovery verification",
            kind.clone(),
            match status.as_str() {
                "queued" => "Queued for recovery verification",
                "fixing" => "Verifying evidence and applying the recovery plan",
                "needs_decision" => "Waiting for an administrator to choose a version",
                "not_retryable" => "Recovery cannot continue automatically",
                _ => "Waiting for dependent work or new evidence",
            }
            .into(),
            "/sync",
        );
        row.started_at = r.try_get("created_at")?;
        row.updated_at = r.try_get("updated_at")?;
        if kind == "operation" {
            row.operation_id = Some(target.clone());
        }
        row.error = detail.map(|d| redact(&d));
        row.status = match status.as_str() {
            "fixing" => BackgroundStatus::Running,
            "needs_decision" | "not_retryable" => BackgroundStatus::Blocked,
            _ => BackgroundStatus::Waiting,
        };
        let reason = row.error.clone();
        recovery.add(format!("recovery:{kind}:{status}"), row, &target, reason);
    }
    recovery.finish(&mut out);
    let inbox = crate::inbox_evidence::snapshot(state).await?;
    if inbox.unreadable > 0 {
        warnings.push(
            "Some saved peer events or inventory entries could not be verified; their status is incomplete."
                .into(),
        );
    }
    for group in inbox.groups {
        use crate::inbox_evidence::{Kind, Status};
        let (title, phase) = match (group.kind, group.status) {
            (Kind::Inventory, Status::Unavailable) => (
                "Peer inventory application",
                "Saved inventory could not be read; its current status is unknown",
            ),
            (_, Status::Unavailable) => (
                "Peer event application",
                "A saved peer event could not be read; its current status is unknown",
            ),
            (Kind::FolderRemoval, Status::Failed) => (
                "Peer event application",
                "This folder removal needs ISM support; other folders can still sync",
            ),
            (Kind::Inventory, Status::Failed) => (
                "Peer inventory application",
                "The last attempt to apply this inventory failed",
            ),
            (_, Status::Failed) => (
                "Peer event application",
                "The last attempt to apply these changes failed",
            ),
            (Kind::Inventory, Status::Waiting) => (
                "Peer inventory application",
                "Received inventory awaits safe dependency verification",
            ),
            (_, Status::Waiting) if group.event_status == "applying" => (
                "Peer event application",
                "Waiting for the related managed operation to finish",
            ),
            (_, Status::Waiting) if group.event_status == "awaiting_content" => (
                "Peer event application",
                "Waiting for the related file content",
            ),
            (_, Status::Waiting) => (
                "Peer event application",
                "Received changes await dependency verification and application",
            ),
        };
        let mut row = entry(
            group.key(),
            title,
            format!("{} / {}", group.peer_name, group.share_name),
            phase.into(),
            "/peers",
        );
        row.count = group.count;
        row.started_at.clear();
        row.updated_at = group.updated_at.unwrap_or_default();
        row.operation_id = group.operation_id;
        if group.status == Status::Failed {
            row.status = if group.kind == Kind::FolderRemoval {
                BackgroundStatus::Blocked
            } else {
                BackgroundStatus::Failed
            };
            row.error = group.error;
        }
        if group.count == 1 {
            if let Some(example) = group.examples.into_iter().next()
                && !example.resource.is_empty()
            {
                row.resource = format!("{} / {}", row.resource, example.resource);
            }
        } else {
            row.examples = group.examples;
            if group.status == Status::Unavailable {
                for example in &mut row.examples {
                    example.detail = None;
                }
            } else {
                let mut reasons: Vec<_> = group.reasons.into_iter().collect();
                reasons.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                row.reasons = reasons
                    .into_iter()
                    .take(REASONS)
                    .map(|(reason, count)| ism_core::dto::ActivityReason { reason, count })
                    .collect();
                if !row.reasons.is_empty() {
                    row.error = None;
                }
            }
        }
        out.push(row);
    }
    for r in sqlx::query(
        "SELECT peer_id,kind,target_id,state,updated_at FROM outgoing_recovery_requests WHERE state IN ('queued','forwarding','waiting')",
    ).fetch_all(&state.db).await? {
        let peer:String=r.try_get("peer_id")?;
        let target:String=r.try_get("target_id")?;
        let kind:String=r.try_get("kind")?;
        let status:String=r.try_get("state")?;
        let mut row=entry(format!("forward:{peer}:{kind}:{target}"),"Peer recovery request",
            format!("{peer} / {kind}: {target}"),format!("Recovery request is {status} on the receiver"),"/sync");
        row.updated_at=r.try_get("updated_at")?;
        out.push(row);
    }
    if let Some(row) = upload_asset_activity(&state.db).await? {
        out.push(row);
    }
    Ok((out, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn upload(pool: &sqlx::SqlitePool, id: &str, at: &str, request: &str) {
        sqlx::query(
            "INSERT INTO operations
             (id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES (?,?,'synthetic','client','{}','done',?)",
        )
        .bind(id)
        .bind(id)
        .bind(at)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO uploads (id,principal,request_json,status,operation_id,created_at)
             VALUES (?,'synthetic',?,'receiving',?,?)",
        )
        .bind(id)
        .bind(request)
        .bind(id)
        .bind(at)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO operation_items
             (operation_id,idx,kind,action_json,status,result_json)
             VALUES (?,0,'publish_upload','{}','done','{}')",
        )
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    }

    const ORIGINAL: &str = r#"{"files":[{"role":"original","name":"photo.jpg"}]}"#;

    #[tokio::test]
    async fn upload_summary_bounds_examples_without_truncating_count_or_oldest_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::connect(dir.path()).await.unwrap();
        for i in 0..16 {
            upload(
                &pool,
                &format!("sidecar-{i:02}"),
                "2019-01-01",
                r#"{"files":[{"role":"sidecar"}]}"#,
            )
            .await;
        }
        for i in 0..12 {
            upload(
                &pool,
                &format!("upload-{i:02}"),
                if i == 11 { "2020-01-01" } else { "2026-01-01" },
                ORIGINAL,
            )
            .await;
        }
        // Multiple matching items in one operation retain the original join's multiplicity.
        sqlx::query(
            "INSERT INTO operation_items
             (operation_id,idx,kind,action_json,status,result_json)
             VALUES ('upload-00',1,'publish_upload','{}','done','{}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let raw = sqlx::query(UPLOAD_ASSET_QUERY)
            .bind(EXAMPLES as i64)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(raw.len(), EXAMPLES);
        let before = db::now();
        let row = upload_asset_activity(&pool).await.unwrap().unwrap();
        assert_eq!(row.count, 13);
        assert_eq!(row.started_at, "2020-01-01");
        assert!(row.updated_at >= before && row.updated_at <= db::now());
        assert_eq!(row.resource, "Uploads");
        assert_eq!(row.examples.len(), EXAMPLES);
        assert!(row.examples.iter().all(|e| e.detail.is_none()));
        assert!(row.operation_id.is_none());
        assert!(row.reasons.is_empty());
        assert_eq!(row.id, "state:upload-asset");
        assert_eq!(row.task_key, "upload-asset");
        assert_eq!(row.page.as_deref(), Some("/operations"));
    }

    #[tokio::test]
    async fn upload_summary_preserves_singleton_action_and_refreshes_after_writes() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::connect(dir.path()).await.unwrap();
        assert!(upload_asset_activity(&pool).await.unwrap().is_none());
        upload(
            &pool,
            "existing",
            "2025-01-01",
            r#"{"existing_original":{"name":"photo.jpg"}}"#,
        )
        .await;
        let row = upload_asset_activity(&pool).await.unwrap().unwrap();
        assert_eq!(row.count, 1);
        assert_eq!(row.resource, "Uploads / existing");
        assert_eq!(row.operation_id.as_deref(), Some("existing"));
        assert_eq!(row.started_at, "2025-01-01");
        assert!(row.examples.is_empty());
        sqlx::query("UPDATE operation_items SET result_json='{\"asset_id\":\"asset\"}'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(upload_asset_activity(&pool).await.unwrap().is_none());
        sqlx::query("UPDATE operation_items SET result_json='{\"asset_id\":null}'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            upload_asset_activity(&pool).await.unwrap().unwrap().count,
            1
        );
        sqlx::query("UPDATE uploads SET request_json='{\"files\":[{\"role\":\"sidecar\"}]}'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(upload_asset_activity(&pool).await.unwrap().is_none());
        sqlx::query("UPDATE uploads SET request_json=?")
            .bind(r#"{"files":[{"role":"sidecar"},{"role":"original"}]}"#)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            upload_asset_activity(&pool).await.unwrap().unwrap().count,
            1
        );
        sqlx::query("DELETE FROM uploads")
            .execute(&pool)
            .await
            .unwrap();
        assert!(upload_asset_activity(&pool).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn upload_summary_retains_original_eligibility_predicates() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::connect(dir.path()).await.unwrap();
        for id in [
            "original",
            "sidecar",
            "pending",
            "no-result",
            "resolved",
            "move",
            "unlinked",
        ] {
            upload(&pool, id, "2025-01-01", ORIGINAL).await;
        }
        sqlx::raw_sql(
            "UPDATE uploads SET request_json='{\"files\":[{\"role\":\"sidecar\"}]}' WHERE id='sidecar';
             UPDATE operation_items SET status='running' WHERE operation_id='pending';
             UPDATE operation_items SET result_json=NULL WHERE operation_id='no-result';
             UPDATE operation_items SET result_json='{\"asset_id\":\"asset\"}' WHERE operation_id='resolved';
             UPDATE operation_items SET kind='move' WHERE operation_id='move';
             UPDATE uploads SET operation_id=NULL WHERE id='unlinked';",
        )
        .execute(&pool)
        .await
        .unwrap();
        let row = upload_asset_activity(&pool).await.unwrap().unwrap();
        assert_eq!(row.count, 1);
        // Upload status is intentionally not an eligibility predicate.
        assert_eq!(row.operation_id.as_deref(), Some("original"));
    }

    #[tokio::test]
    async fn upload_summary_does_not_hide_unreadable_evidence_beyond_example_limit() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::connect(dir.path()).await.unwrap();
        for i in 0..8 {
            upload(&pool, &format!("upload-{i}"), "2025-01-01", ORIGINAL).await;
        }
        sqlx::query(
            "UPDATE operation_items SET result_json='invalid' WHERE operation_id='upload-7'",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert!(upload_asset_activity(&pool).await.is_err());
        sqlx::query("UPDATE operation_items SET result_json='{}'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE uploads SET request_json='invalid' WHERE id='upload-7'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(upload_asset_activity(&pool).await.is_err());
        sqlx::query("UPDATE uploads SET request_json=? WHERE id='upload-7'")
            .bind(r#"{"files":["not-an-object"]}"#)
            .execute(&pool)
            .await
            .unwrap();
        assert!(upload_asset_activity(&pool).await.is_err());
    }
}
