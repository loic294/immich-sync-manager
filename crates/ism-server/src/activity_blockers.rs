//! Current waiting evidence, read from the queues that actually govern execution.

use ism_core::dto::{BackgroundActivity, BackgroundStatus};
use ism_core::maintenance::Phase;
use sqlx::Row;
use std::collections::HashMap;

use crate::db;
use crate::error::{AppResult, redact};
use crate::state::AppState;

fn entry(
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
/// Queued transfers whose full eligibility is evaluated: the head of the queue is what the
/// transfer worker examines next, so it explains why the queue is or is not moving.
const QUEUE_HEAD: i64 = 10;

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

pub async fn get(state: &AppState) -> AppResult<Vec<BackgroundActivity>> {
    let mut out = Vec::new();
    for server in crate::immich::registry::servers(state).await? {
        let m = &server.maintenance;
        if m.phase != Phase::Idle || m.drift.is_some() {
            let mut row = entry(
                format!("maintenance:{}", server.id), "Immich maintenance", server.name,
                m.message.clone().or_else(|| m.drift.clone()).unwrap_or_else(|| match m.phase {
                    Phase::Preparing => "Pausing the library queue and confirming it is idle".into(),
                    Phase::Active => format!("Library scans paused while {} managed operations hold the write barrier", m.active_ops),
                    Phase::Resuming => "Resuming the Immich library queue".into(),
                    Phase::RecoveryRequired => "Unverified file changes require recovery before writes can resume".into(),
                    Phase::Blocked => "Maintenance needs attention before writes can resume".into(),
                    Phase::Idle => "Immich settings drift prevents safe writes".into(),
                }), "/servers",
            );
            if matches!(m.phase, Phase::Blocked | Phase::RecoveryRequired) || m.drift.is_some() {
                row.status = BackgroundStatus::Blocked;
            }
            out.push(row);
        }
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
    for view in crate::watch::status_views(state)
        .await?
        .into_values()
        .filter(|v| v.paused)
    {
        let mut row = entry(
            format!("watch:{}", view.root_id),
            "File detection paused",
            view.name,
            view.warnings.join("; "),
            "/mappings",
        );
        row.status = BackgroundStatus::Blocked;
        if let Some(at) = view.last_pass_at {
            row.started_at = at.clone();
            row.updated_at = at;
        }
        out.push(row);
    }
    let shares = crate::peers::shares(state).await?;
    let pending_shares = state
        .share_changes
        .lock()
        .map_err(|_| crate::error::AppError::internal("share scan schedule lock poisoned"))?
        .clone();
    let share_gate = crate::peers::inbox::require_no_active_operations(state).await;
    let gate_reason = match share_gate {
        Ok(()) => None,
        Err(e) if e.code() == "recovery_required" => Some(redact(e.message())),
        Err(e) => return Err(e),
    };
    for share in shares.iter().filter(|s| s.enabled) {
        let due = pending_shares.get(&share.id);
        if due.is_none() && gate_reason.is_none() {
            continue;
        }
        let mut row = entry(
            format!("share:{}", share.id),
            "Share inventory scan",
            share.name.clone(),
            gate_reason.clone().unwrap_or_else(|| {
                if due.is_some_and(|at| *at > std::time::Instant::now()) {
                    "Waiting for file changes to settle before scanning the share".into()
                } else {
                    "Share inventory rescan is queued".into()
                }
            }),
            "/peers",
        );
        if let Some(at) = due {
            let delay = at.saturating_duration_since(std::time::Instant::now());
            row.next_at = chrono::Duration::from_std(delay)
                .ok()
                .and_then(|d| chrono::Utc::now().checked_add_signed(d))
                .map(db::ts);
        }
        out.push(row);
    }
    for r in sqlx::query("SELECT id,name,last_error,last_seen_at FROM peers WHERE status!='online'")
        .fetch_all(&state.db)
        .await?
    {
        let id: String = r.try_get("id")?;
        let mut row=entry(format!("peer:{id}"),"Peer connection",r.try_get("name")?,
            "Waiting for the peer to come online; event exchange and queued transfers cannot advance".into(),"/peers");
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
    let mut outgoing = Groups::default();
    for json in sqlx::query_scalar::<_, String>("SELECT activity_json FROM outgoing_sync")
        .fetch_all(&state.db)
        .await?
    {
        let transfer: ism_core::dto::SyncActivity = serde_json::from_str(&json)?;
        let (phase, status) = match transfer.status.as_str() {
            "queued" => ("Queued on the receiving ISM", BackgroundStatus::Waiting),
            "running" => (
                "The receiving ISM is downloading",
                BackgroundStatus::Running,
            ),
            "publishing" => (
                "The receiving ISM is waiting for safe publication",
                BackgroundStatus::Running,
            ),
            "failed" => (
                "The receiving ISM reported a failure",
                BackgroundStatus::Blocked,
            ),
            _ => continue,
        };
        let mut row = entry(
            String::new(),
            "Outgoing peer synchronization",
            format!("{} / {}", transfer.destination, transfer.share_name),
            phase.into(),
            "/sync",
        );
        row.done = transfer.bytes_done;
        row.total = Some(transfer.size);
        row.unit = "bytes".into();
        row.started_at = transfer.queued_at;
        row.error = transfer.error.map(|e| redact(&e));
        row.status = status;
        let reason = row.error.clone();
        outgoing.add(
            format!(
                "outgoing:{}:{}:{}",
                transfer.peer_id, transfer.share_name, transfer.status
            ),
            row,
            &transfer.relative_path,
            reason,
        );
    }
    outgoing.finish(&mut out);
    let share_name = |id: &str| {
        shares
            .iter()
            .find(|s| s.id == id)
            .map_or_else(|| id.to_owned(), |s| s.name.clone())
    };
    let running: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers WHERE status='running'")
        .fetch_one(&state.db)
        .await?;
    // In-flight transfers are few (bounded by the concurrency limit) and each reports progress.
    for r in sqlx::query(
        "SELECT t.id,t.share_id,t.rel_path,t.size,t.bytes_done,t.queued_at,t.updated_at,p.name AS peer_name
         FROM transfers t JOIN peers p ON p.id=t.peer_id WHERE t.status='running' ORDER BY t.queued_at,t.id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let share_id: String = r.try_get("share_id")?;
        let mut row = entry(
            format!("transfer:{}", r.try_get::<String, _>("id")?),
            "Peer file transfer",
            format!(
                "{} / {} / {}",
                r.try_get::<String, _>("peer_name")?,
                share_name(&share_id),
                r.try_get::<String, _>("rel_path")?
            ),
            "Downloading, verifying, or preparing safe publication".into(),
            "/sync",
        );
        row.status = BackgroundStatus::Running;
        row.unit = "bytes".into();
        row.done = r.try_get::<i64, _>("bytes_done")?.max(0) as u64;
        row.total = Some(r.try_get::<i64, _>("size")?.max(0) as u64);
        row.started_at = r.try_get("queued_at")?;
        row.updated_at = r.try_get("updated_at")?;
        out.push(row);
    }
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
    for r in sqlx::query(
        "SELECT t.peer_id,t.share_id,p.name AS peer_name,p.status AS peer_status,COUNT(*) AS n,
                COALESCE(SUM(t.size),0) AS size,COALESCE(SUM(t.bytes_done),0) AS done,
                MIN(t.queued_at) AS first_at,MAX(t.updated_at) AS last_at
         FROM transfers t JOIN peers p ON p.id=t.peer_id WHERE t.status='queued'
         GROUP BY t.peer_id,t.share_id ORDER BY MIN(t.queued_at)",
    )
    .fetch_all(&state.db)
    .await?
    {
        let peer_id: String = r.try_get("peer_id")?;
        let share_id: String = r.try_get("share_id")?;
        let n = r.try_get::<i64, _>("n")?.max(0) as u64;
        let mut row = entry(
            format!("transfer-queue:{peer_id}:{share_id}"),
            "Peer file transfer queue",
            format!(
                "{} / {}",
                r.try_get::<String, _>("peer_name")?,
                share_name(&share_id)
            ),
            String::new(),
            "/sync",
        );
        row.count = n;
        row.unit = "bytes".into();
        row.done = r.try_get::<i64, _>("done")?.max(0) as u64;
        row.total = Some(r.try_get::<i64, _>("size")?.max(0) as u64);
        row.started_at = r.try_get("first_at")?;
        row.updated_at = r.try_get("last_at")?;
        let Some(share) = shares.iter().find(|s| s.id == share_id) else {
            row.status = BackgroundStatus::Blocked;
            row.phase = "The transfer share is no longer available".into();
            out.push(row);
            continue;
        };
        if r.try_get::<String, _>("peer_status")? != "online" {
            row.phase = "Waiting for the peer to come online".into();
            out.push(row);
            continue;
        }
        use crate::peers::transfer::Eligibility;
        let mut reasons: HashMap<String, u64> = HashMap::new();
        let mut ready = false;
        let mut blocked = false;
        for h in sqlx::query(
            "SELECT id,rel_path,attempts,updated_at FROM transfers
             WHERE status='queued' AND peer_id=? AND share_id=? ORDER BY queued_at,id LIMIT ?",
        )
        .bind(&peer_id)
        .bind(&share_id)
        .bind(QUEUE_HEAD)
        .fetch_all(&state.db)
        .await?
        {
            let id: String = h.try_get("id")?;
            let path: String = h.try_get("rel_path")?;
            let updated: String = h.try_get("updated_at")?;
            let detail = match crate::peers::transfer::eligibility(
                state,
                share,
                &peer_id,
                &id,
                &path,
                h.try_get("attempts")?,
                &updated,
            )
            .await?
            {
                Eligibility::Ready(_) => {
                    ready = true;
                    "Ready to transfer".to_owned()
                }
                Eligibility::Waiting {
                    reason,
                    next_at,
                    blocked: b,
                } => {
                    blocked |= b;
                    row.next_at = match (row.next_at.take(), next_at) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    reason
                }
            };
            *reasons.entry(detail.clone()).or_default() += 1;
            if row.examples.len() < EXAMPLES {
                row.examples.push(ism_core::dto::ActivityExample {
                    resource: path,
                    detail: Some(detail),
                });
            }
        }
        let mut reasons: Vec<_> = reasons.into_iter().collect();
        reasons.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let limit = state.config.peer_transfer_concurrency as i64;
        row.phase = if ready && running >= limit {
            format!("Waiting for a free transfer slot ({running} of {limit} in use)")
        } else if ready {
            "Next file starts as soon as the transfer worker picks it up".into()
        } else {
            format!(
                "Next files in the queue are waiting: {}",
                reasons.first().map_or("", |r| r.0.as_str())
            )
        };
        if !ready && blocked {
            row.status = BackgroundStatus::Blocked;
        }
        row.reasons = reasons
            .into_iter()
            .take(REASONS)
            .map(|(reason, count)| ism_core::dto::ActivityReason { reason, count })
            .collect();
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
    for r in sqlx::query(
        "SELECT i.peer_id,p.name,i.share_name,i.status,i.error,i.operation_id,COUNT(*) AS n
         FROM peer_inbox i JOIN peers p ON p.id=i.peer_id WHERE i.status!='done'
         GROUP BY i.peer_id,i.share_name,i.status,i.error,i.operation_id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let peer: String = r.try_get("peer_id")?;
        let share: String = r.try_get("share_name")?;
        let status: String = r.try_get("status")?;
        let error: Option<String> = r.try_get("error")?;
        let op: Option<String> = r.try_get("operation_id")?;
        let mut row = entry(
            format!(
                "inbox:{peer}:{share}:{status}:{}",
                op.as_deref().unwrap_or("")
            ),
            "Peer event application",
            format!("{} / {share}", r.try_get::<String, _>("name")?),
            error.as_deref().map(redact).unwrap_or_else(|| {
                if op.is_some() {
                    "Waiting for the related managed operation to finish".into()
                } else {
                    "Received events awaiting dependency verification and application".into()
                }
            }),
            "/peers",
        );
        row.done = r.try_get::<i64, _>("n")? as u64;
        row.unit = "pending events".into();
        row.operation_id = op;
        if error.is_some() {
            row.status = BackgroundStatus::Blocked;
        }
        out.push(row);
    }
    for r in sqlx::query(
        "SELECT b.peer_id,p.name,s.name AS share_name,b.error,b.operation_id,COUNT(*) AS n
         FROM peer_backfill_inbox b JOIN peers p ON p.id=b.peer_id JOIN shares s ON s.id=b.share_id
         GROUP BY b.peer_id,s.name,b.error,b.operation_id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let peer: String = r.try_get("peer_id")?;
        let share: String = r.try_get("share_name")?;
        let op: Option<String> = r.try_get("operation_id")?;
        let error: Option<String> = r.try_get("error")?;
        let mut row = entry(
            format!("inventory:{peer}:{share}:{}", op.as_deref().unwrap_or("")),
            "Peer inventory application",
            format!("{} / {share}", r.try_get::<String, _>("name")?),
            error.as_deref().map(redact).unwrap_or_else(|| {
                "Received inventory awaiting safe dependency verification".into()
            }),
            "/peers",
        );
        row.done = r.try_get::<i64, _>("n")? as u64;
        row.unit = "pending inventory entries".into();
        row.operation_id = op;
        if error.is_some() {
            row.status = BackgroundStatus::Blocked;
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
    let mut uploads = Groups::default();
    for r in sqlx::query(
        "SELECT u.id,u.operation_id,u.created_at FROM uploads u JOIN operation_items i ON i.operation_id=u.operation_id
         WHERE i.kind='publish_upload' AND i.status='done' AND i.result_json IS NOT NULL
           AND json_extract(i.result_json,'$.asset_id') IS NULL
           AND (json_extract(u.request_json,'$.existing_original.name') IS NOT NULL
                OR EXISTS(SELECT 1 FROM json_each(u.request_json,'$.files') f WHERE json_extract(f.value,'$.role')='original'))",
    ).fetch_all(&state.db).await? {
        let id:String=r.try_get("id")?;
        let mut row=entry(String::new(),"Imported upload asset ID","Uploads".into(),
            "Published file is waiting for Immich import and asset lookup".into(),"/operations");
        row.operation_id=r.try_get("operation_id")?;
        row.started_at=r.try_get("created_at")?;
        uploads.add("upload-asset".into(),row,&id,None);
    }
    uploads.finish(&mut out);
    Ok(out)
}
