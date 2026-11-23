//! Current waiting evidence, read from the queues that actually govern execution.

use ism_core::dto::{BackgroundActivity, BackgroundStatus};
use ism_core::maintenance::Phase;
use sqlx::Row;

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
    for json in sqlx::query_scalar::<_, String>("SELECT activity_json FROM outgoing_sync")
        .fetch_all(&state.db)
        .await?
    {
        let transfer: ism_core::dto::SyncActivity = serde_json::from_str(&json)?;
        if !matches!(
            transfer.status.as_str(),
            "queued" | "running" | "publishing" | "failed"
        ) {
            continue;
        }
        let mut row = entry(
            format!("outgoing:{}:{}", transfer.peer_id, transfer.id),
            "Outgoing peer synchronization",
            format!(
                "{} / {} / {}",
                transfer.destination, transfer.share_name, transfer.relative_path
            ),
            format!(
                "Receiver reports {}{}",
                transfer.status,
                if transfer.status == "publishing" {
                    "; waiting for safe publication"
                } else {
                    ""
                }
            ),
            "/sync",
        );
        row.done = transfer.bytes_done;
        row.total = Some(transfer.size);
        row.unit = "bytes".into();
        row.started_at = transfer.queued_at;
        row.error = transfer.error.map(|e| redact(&e));
        row.status = match transfer.status.as_str() {
            "running" | "publishing" => BackgroundStatus::Running,
            "failed" => BackgroundStatus::Blocked,
            _ => BackgroundStatus::Waiting,
        };
        out.push(row);
    }
    let rows = sqlx::query(
        "SELECT t.*,p.name AS peer_name,p.status AS peer_status FROM transfers t
         JOIN peers p ON p.id=t.peer_id WHERE t.status IN ('queued','running','failed') ORDER BY t.queued_at,t.id",
    ).fetch_all(&state.db).await?;
    let running: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers WHERE status='running'")
        .fetch_one(&state.db)
        .await?;
    for r in rows {
        let id: String = r.try_get("id")?;
        let share_id: String = r.try_get("share_id")?;
        let peer_id: String = r.try_get("peer_id")?;
        let path: String = r.try_get("rel_path")?;
        let status: String = r.try_get("status")?;
        let mut row = entry(
            format!("transfer:{id}"),
            "Peer file transfer",
            format!(
                "{} / {} / {}",
                r.try_get::<String, _>("peer_name")?,
                shares
                    .iter()
                    .find(|s| s.id == share_id)
                    .map_or(share_id.as_str(), |s| &s.name),
                path
            ),
            "Queued for transfer".into(),
            "/sync",
        );
        row.unit = "bytes".into();
        row.done = r.try_get::<i64, _>("bytes_done")?.max(0) as u64;
        row.total = Some(r.try_get::<i64, _>("size")?.max(0) as u64);
        row.started_at = r.try_get("queued_at")?;
        row.updated_at = r.try_get("updated_at")?;
        row.error = r.try_get::<Option<String>, _>("error")?.map(|e| redact(&e));
        if status == "running" {
            row.status = BackgroundStatus::Running;
            row.phase = "Downloading, verifying, or preparing safe publication".into();
        } else if status == "failed" {
            row.status = BackgroundStatus::Blocked;
            row.phase = "Transfer needs recovery".into();
        } else if r.try_get::<String, _>("peer_status")? != "online" {
            row.phase = "Waiting for the peer to come online".into();
        } else if let Some(share) = shares.iter().find(|s| s.id == share_id) {
            use crate::peers::transfer::Eligibility;
            match crate::peers::transfer::eligibility(
                state,
                share,
                &peer_id,
                &id,
                &path,
                r.try_get("attempts")?,
                &row.updated_at,
            )
            .await?
            {
                Eligibility::Ready(_)
                    if running >= state.config.peer_transfer_concurrency as i64 =>
                {
                    row.phase = "Waiting for a free transfer slot".into();
                }
                Eligibility::Ready(_) => {}
                Eligibility::Waiting {
                    reason,
                    next_at,
                    blocked,
                } => {
                    row.status = if blocked {
                        BackgroundStatus::Blocked
                    } else {
                        BackgroundStatus::Waiting
                    };
                    row.phase = reason;
                    row.next_at = next_at;
                }
            }
        } else {
            row.status = BackgroundStatus::Blocked;
            row.phase = "The transfer share is no longer available".into();
        }
        out.push(row);
    }
    for r in sqlx::query(
        "SELECT * FROM sync_recovery WHERE state NOT IN ('fixed') ORDER BY created_at,id",
    )
    .fetch_all(&state.db)
    .await?
    {
        let id: String = r.try_get("id")?;
        let kind: String = r.try_get("kind")?;
        let target: String = r.try_get("target_id")?;
        let status: String = r.try_get("state")?;
        let detail: Option<String> = r.try_get("detail")?;
        let mut row = entry(
            format!("recovery:{id}"),
            "Recovery verification",
            format!("{kind}: {target}"),
            detail.map(|d| redact(&d)).unwrap_or_else(|| {
                match status.as_str() {
                    "queued" => "Queued for recovery verification",
                    "fixing" => "Verifying evidence and applying the recovery plan",
                    "needs_decision" => "Waiting for an administrator to choose a version",
                    "not_retryable" => "Recovery cannot continue automatically",
                    _ => "Waiting for dependent work or new evidence",
                }
                .into()
            }),
            "/sync",
        );
        row.started_at = r.try_get("created_at")?;
        row.updated_at = r.try_get("updated_at")?;
        if kind == "operation" {
            row.operation_id = Some(target);
        }
        row.status = match status.as_str() {
            "fixing" => BackgroundStatus::Running,
            "needs_decision" | "not_retryable" => BackgroundStatus::Blocked,
            _ => BackgroundStatus::Waiting,
        };
        out.push(row);
    }
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
    for r in sqlx::query(
        "SELECT u.id,u.operation_id,u.created_at FROM uploads u JOIN operation_items i ON i.operation_id=u.operation_id
         WHERE i.kind='publish_upload' AND i.status='done' AND i.result_json IS NOT NULL
           AND json_extract(i.result_json,'$.asset_id') IS NULL
           AND (json_extract(u.request_json,'$.existing_original.name') IS NOT NULL
                OR EXISTS(SELECT 1 FROM json_each(u.request_json,'$.files') f WHERE json_extract(f.value,'$.role')='original'))",
    ).fetch_all(&state.db).await? {
        let id:String=r.try_get("id")?;
        let mut row=entry(format!("upload-asset:{id}"),"Imported upload asset ID",id,
            "Published file is waiting for Immich import and asset lookup".into(),"/operations");
        row.operation_id=r.try_get("operation_id")?;
        row.started_at=r.try_get("created_at")?;
        out.push(row);
    }
    Ok(out)
}
