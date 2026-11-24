//! Structured, evidence-backed conditions for the administration UI.

use std::collections::{HashMap, HashSet};

use ism_core::dto::{
    AttentionCode, AttentionCompleteness, AttentionCoverage, AttentionItem, AttentionLevel,
    AttentionOwner, AttentionSnapshot, AttentionState,
};

use crate::db;
use crate::error::AppResult;
use crate::queue_evidence::ScheduleState;
use crate::state::AppState;

#[derive(sqlx::FromRow)]
struct Operation {
    id: String,
    status: String,
    handled_at: Option<String>,
    recovery_resolved: bool,
    admitted_server_ids_json: String,
}

pub async fn snapshot(state: &AppState) -> AppResult<AttentionSnapshot> {
    let observed_at = db::now();
    let instance_name = state.instance_name().await;
    let (mut items, mut warnings) =
        crate::local_evidence::snapshot(state, &instance_name, &observed_at).await?;
    let maintenance_readable = !items.iter().any(|item| {
        item.code == AttentionCode::MaintenanceBlocked && item.state == AttentionState::Unavailable
    });
    let mappings_readable = !items.iter().any(|item| {
        item.code == AttentionCode::MappingUnavailable && item.state == AttentionState::Unavailable
    });
    let operations: Vec<Operation> = sqlx::query_as(
        "SELECT id,status,handled_at,recovery_resolved,admitted_server_ids_json
         FROM operations
         WHERE (handled_at IS NULL AND status IN ('failed','partial','recovery_required'))
            OR (recovery_resolved=0 AND
                (status='recovery_required' OR (status='partial' AND admitted_server_ids_json!='[]')))
         ORDER BY created_at DESC,id",
    )
    .fetch_all(&state.db)
    .await?;
    items.extend(operations
        .into_iter()
        .map(|operation| {
            let unresolved = !operation.recovery_resolved
                && (operation.status == "recovery_required"
                    || (operation.status == "partial"
                        && operation.admitted_server_ids_json != "[]"));
            let (code, item_state, title, message) = if unresolved {
                (
                    AttentionCode::OperationRecovery,
                    AttentionState::Blocked,
                    "Operation recovery needs review",
                    "Review the operation and resolve its recovery before affected file changes can continue.",
                )
            } else {
                (
                    AttentionCode::OperationFailed,
                    AttentionState::Failed,
                    "An operation failed",
                    "Review the failed operation in Operations.",
                )
            };
            AttentionItem {
                id: format!("operation:{}", operation.id),
                code,
                state: item_state,
                attention: AttentionLevel::ActionRequired,
                blocks_work: unresolved,
                owner: AttentionOwner::Local,
                instance_id: Some(state.instance_id.clone()),
                instance_name: Some(instance_name.clone()),
                peer_id: None,
                share_id: None,
                share_name: None,
                count: 1,
                total_count: Some(1),
                next_at: None,
                recheck_at: None,
                observed_at: observed_at.clone(),
                title: title.into(),
                message: message.into(),
                href: "/operations".into(),
                operation_id: Some(operation.id),
                acknowledged: Some(operation.handled_at.is_some()),
                unresolved: Some(unresolved),
                completeness: AttentionCompleteness::Complete,
                examples: Vec::new(),
            }
        })
    );
    let queues = crate::queue_evidence::snapshot(state).await?;
    let pending_transfers =
        queues.queued.iter().map(|group| group.count).sum::<u64>() + queues.running.max(0) as u64;
    for group in &queues.queued {
        let evidence = AttentionItem {
            id: format!(
                "transfer-evidence:{}:{}",
                group.peer_id, group.share_id
            ),
            code: AttentionCode::TransferEvidenceUnavailable,
            state: AttentionState::Unavailable,
            attention: AttentionLevel::None,
            blocks_work: false,
            owner: AttentionOwner::Local,
            instance_id: Some(state.instance_id.clone()),
            instance_name: Some(instance_name.clone()),
            peer_id: Some(group.peer_id.clone()),
            share_id: Some(group.share_id.clone()),
            share_name: Some(group.share_name.clone()),
            count: group.count,
            total_count: Some(group.count),
            next_at: None,
            recheck_at: group.recheck_at.clone(),
            observed_at: observed_at.clone(),
            title: "Transfer readiness is not fully known".into(),
            message: "File-specific dependencies have not been checked; some files may be ready while others wait.".into(),
            href: "/sync".into(),
            operation_id: None,
            acknowledged: None,
            unresolved: None,
            completeness: AttentionCompleteness::Partial,
            examples: Vec::new(),
        };
        if group.peer_status == "offline" {
            let mut item = evidence.clone();
            item.id = format!("peer-offline:{}:{}", group.peer_id, group.share_id);
            item.code = AttentionCode::PeerOffline;
            item.state = AttentionState::Waiting;
            item.attention = AttentionLevel::Notice;
            item.blocks_work = true;
            item.title = "Peer is offline".into();
            item.message = format!(
                "ISM reports {} offline. Check its connection in Peers.",
                group.peer_name
            );
            item.href = "/peers".into();
            item.unresolved = Some(true);
            item.completeness = AttentionCompleteness::Complete;
            items.push(item);
        } else if group.peer_status != "online" {
            let mut item = evidence.clone();
            item.id = format!("peer-evidence:{}:{}", group.peer_id, group.share_id);
            item.code = AttentionCode::PeerEvidenceUnavailable;
            item.title = "Peer connection is not yet verified".into();
            item.message = format!("Check the connection to {} in Peers.", group.peer_name);
            item.href = "/peers".into();
            items.push(item);
        }
        if queues.active_operation {
            let mut item = evidence.clone();
            item.id = format!("transfer-operation:{}:{}", group.peer_id, group.share_id);
            item.code = AttentionCode::TransferDependency;
            item.state = AttentionState::Waiting;
            item.blocks_work = true;
            item.title = "Transfers wait for a managed operation".into();
            item.message =
                "Peer bookkeeping waits while a managed operation runs or enters maintenance."
                    .into();
            item.href = "/operations".into();
            item.unresolved = Some(true);
            item.completeness = AttentionCompleteness::Complete;
            items.push(item);
        }
        if group.root_available == Some(false) {
            let mut item = evidence.clone();
            item.id = format!("transfer-root:{}:{}", group.peer_id, group.share_id);
            item.code = AttentionCode::TransferDependency;
            item.state = AttentionState::Blocked;
            item.attention = AttentionLevel::ActionRequired;
            item.blocks_work = true;
            item.title = "Mapped folder is unavailable".into();
            item.message = if group.root_configured {
                "Restore the mapped folder and its marker before transfers can continue.".into()
            } else {
                "Restore the mapping for this share before transfers can continue.".into()
            };
            item.href = "/mappings".into();
            item.unresolved = Some(true);
            item.completeness = AttentionCompleteness::Complete;
            items.push(item);
        } else if group.root_available.is_none() {
            let mut item = evidence.clone();
            item.id = format!(
                "transfer-root-evidence:{}:{}",
                group.peer_id, group.share_id
            );
            item.title = "Mapped folder status is unavailable".into();
            item.message = "ISM could not check the mapped folder yet.".into();
            item.href = "/mappings".into();
            items.push(item);
        }
        match &group.schedule {
            ScheduleState::Invalid => {
                let mut item = evidence.clone();
                item.id = format!("transfer-schedule:{}:{}", group.peer_id, group.share_id);
                item.code = AttentionCode::TransferScheduleInvalid;
                item.state = AttentionState::Blocked;
                item.attention = AttentionLevel::ActionRequired;
                item.blocks_work = true;
                item.title = "Transfer schedule needs repair".into();
                item.message = "Save valid transfer hours in Sync settings.".into();
                item.unresolved = Some(true);
                item.completeness = AttentionCompleteness::Complete;
                items.push(item);
            }
            ScheduleState::Closed { next_at } => {
                let mut item = evidence.clone();
                item.id = format!("transfer-schedule:{}:{}", group.peer_id, group.share_id);
                item.code = AttentionCode::TransferScheduled;
                item.state = AttentionState::Waiting;
                item.attention = if next_at.is_some() {
                    AttentionLevel::Notice
                } else {
                    AttentionLevel::ActionRequired
                };
                item.blocks_work = true;
                item.next_at = next_at.clone();
                item.title = "Transfers are outside allowed hours".into();
                item.message = if next_at.is_some() {
                    "Wait for the next window or adjust transfer hours in Sync settings.".into()
                } else {
                    "No transfer window is scheduled. Update the hours in Sync settings.".into()
                };
                item.unresolved = Some(true);
                item.completeness = AttentionCompleteness::Complete;
                items.push(item);
            }
            ScheduleState::Allowed => {
                if group.peer_status == "online" {
                    items.push(evidence.clone());
                }
            }
        }
        if group.backoff_count > 0 {
            let mut item = evidence;
            item.id = format!("transfer-backoff:{}:{}", group.peer_id, group.share_id);
            item.code = AttentionCode::TransferBackoff;
            item.state = AttentionState::Waiting;
            item.blocks_work = true;
            item.count = group.backoff_count;
            if group.unclassified_backoff == 0 {
                item.next_at = group.next_retry.clone();
                item.recheck_at = item
                    .recheck_at
                    .into_iter()
                    .chain(item.next_at.clone())
                    .min();
                item.completeness = AttentionCompleteness::Complete;
            }
            item.title = "Transfers are waiting to retry".into();
            item.message =
                "Retry backoff is active; other transfer prerequisites may still apply.".into();
            item.unresolved = Some(true);
            items.push(item);
        }
    }
    for group in queues.failed {
        items.push(AttentionItem {
            id: format!("transfer-failed:{}:{}", group.peer_id, group.share_id),
            code: AttentionCode::TransferFailed,
            state: AttentionState::Failed,
            attention: AttentionLevel::ActionRequired,
            blocks_work: true,
            owner: AttentionOwner::Local,
            instance_id: Some(state.instance_id.clone()),
            instance_name: Some(instance_name.clone()),
            peer_id: Some(group.peer_id),
            share_id: Some(group.share_id),
            share_name: Some(group.share_name),
            count: group.count,
            total_count: Some(group.count),
            next_at: None,
            recheck_at: None,
            observed_at: observed_at.clone(),
            title: "Transfers need recovery".into(),
            message: format!("Review failed transfers for {} in Sync.", group.peer_name),
            href: "/sync?view=attention".into(),
            operation_id: None,
            acknowledged: None,
            unresolved: Some(true),
            completeness: AttentionCompleteness::Complete,
            examples: Vec::new(),
        });
    }
    let inbox = crate::inbox_evidence::snapshot(state).await?;
    let inbox_readable = inbox
        .groups
        .iter()
        .all(|group| group.status != crate::inbox_evidence::Status::Unavailable);
    let pending_inbox = inbox.replication_pending;
    for group in inbox.groups {
        use crate::inbox_evidence::{Kind, Status};
        if group.status == Status::Waiting {
            continue;
        }
        let unavailable = group.status == Status::Unavailable;
        let confirmed_blocker = !unavailable && group.kind == Kind::FolderRemoval;
        let (code, title, message) = if unavailable {
            (
                AttentionCode::PeerEvidenceUnavailable,
                "Saved peer work could not be verified",
                "The saved event or inventory entry is unreadable; check Operations for its scope.",
            )
        } else {
            match group.kind {
                Kind::FolderRemoval => (
                    AttentionCode::FolderRemovalFailed,
                    "A peer folder removal needs support",
                    "This folder change remains pending. Other folders can still sync.",
                ),
                Kind::Inventory => (
                    AttentionCode::PeerInventoryFailed,
                    "Peer inventory needs review",
                    "The last attempt failed for these pending inventory entries. Check Operations for details.",
                ),
                Kind::Event => (
                    AttentionCode::PeerInboxFailed,
                    "Peer changes need review",
                    "The last attempt failed for these pending changes. Check Operations for details.",
                ),
            }
        };
        items.push(AttentionItem {
            id: group.key(),
            code,
            state: if unavailable {
                AttentionState::Unavailable
            } else {
                AttentionState::Failed
            },
            attention: if unavailable {
                AttentionLevel::None
            } else {
                AttentionLevel::ActionRequired
            },
            blocks_work: confirmed_blocker,
            owner: AttentionOwner::Local,
            instance_id: Some(state.instance_id.clone()),
            instance_name: Some(instance_name.clone()),
            peer_id: Some(group.peer_id),
            share_id: group.share_id,
            share_name: Some(group.share_name),
            count: group.count,
            total_count: None,
            next_at: None,
            recheck_at: None,
            observed_at: group
                .updated_at
                .filter(|at| db::parse_ts(at).is_some())
                .unwrap_or_else(|| observed_at.clone()),
            title: title.into(),
            message: message.into(),
            href: "/operations".into(),
            operation_id: group.operation_id,
            acknowledged: None,
            unresolved: (!unavailable).then_some(true),
            completeness: if !confirmed_blocker {
                AttentionCompleteness::Partial
            } else {
                AttentionCompleteness::Complete
            },
            examples: group
                .examples
                .into_iter()
                .map(|example| ism_core::dto::ActivityExample {
                    resource: example.resource,
                    detail: None,
                })
                .collect(),
        });
    }
    let comparisons = crate::peers::versions::evidence_groups(state).await?;
    let comparisons_readable = comparisons
        .iter()
        .all(|group| group.state != crate::peers::versions::EvidenceState::Unavailable);
    for group in comparisons {
        use crate::peers::versions::EvidenceState;
        let (code, item_state, level, blocks_work, title, message) = match group.state {
            EvidenceState::Queued | EvidenceState::Running => continue,
            EvidenceState::Retrying => (
                AttentionCode::RecoveryFailed,
                AttentionState::Waiting,
                AttentionLevel::Notice,
                true,
                "Conflict comparison is waiting to retry",
                "The last attempt failed. Open Conflicts for details while ISM retries.",
            ),
            EvidenceState::Stopped => (
                AttentionCode::WorkerStopped,
                AttentionState::Failed,
                AttentionLevel::ActionRequired,
                true,
                "Conflict comparison worker stopped",
                "Review server logs and restart ISM when safe to resume these comparisons.",
            ),
            EvidenceState::Blocked => (
                AttentionCode::RecoveryFailed,
                AttentionState::Blocked,
                AttentionLevel::ActionRequired,
                true,
                "Conflict comparison needs attention",
                "The comparison cannot continue automatically. Open Conflicts for details.",
            ),
            EvidenceState::Unavailable => (
                AttentionCode::RecoveryFailed,
                AttentionState::Unavailable,
                AttentionLevel::None,
                false,
                "Conflict comparison status is unavailable",
                "ISM cannot verify the saved comparison state or its worker.",
            ),
        };
        items.push(AttentionItem {
            id: group.key,
            code,
            state: item_state,
            attention: level,
            blocks_work,
            owner: AttentionOwner::Local,
            instance_id: Some(state.instance_id.clone()),
            instance_name: Some(instance_name.clone()),
            peer_id: None,
            share_id: None,
            share_name: None,
            count: group.count,
            total_count: None,
            next_at: if group.state == EvidenceState::Retrying {
                group
                    .next_at
                    .filter(|at| db::parse_ts(at).is_some_and(|time| time > chrono::Utc::now()))
            } else {
                None
            },
            recheck_at: None,
            observed_at: group.observed_at.unwrap_or_else(|| observed_at.clone()),
            title: title.into(),
            message: message.into(),
            href: "/conflicts".into(),
            operation_id: None,
            acknowledged: None,
            unresolved: blocks_work.then_some(true),
            completeness: if group.state == EvidenceState::Unavailable {
                AttentionCompleteness::Unavailable
            } else {
                AttentionCompleteness::Complete
            },
            examples: group.examples,
        });
    }
    let workers = state.worker_evidence.stopped_workers();
    for worker in workers {
        use crate::worker_evidence::Kind;
        let pending: u64 = match worker.kind {
            Kind::Transfers => pending_transfers,
            Kind::PeerReplication | Kind::PeerWorkers => {
                pending_inbox
                    + sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM share_backfills")
                        .fetch_one(&state.db)
                        .await?
                        .max(0) as u64
            }
            Kind::Operations => sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM operations WHERE status IN ('queued','running','waiting_maintenance')",
            )
            .fetch_one(&state.db)
            .await?
            .max(0) as u64,
            Kind::Recovery => sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM sync_recovery WHERE state IN ('queued','fixing','waiting')",
            )
            .fetch_one(&state.db)
            .await?
            .max(0) as u64,
            Kind::Other => 0,
        };
        items.push(AttentionItem {
            id: format!("worker:{}", worker.name),
            code: AttentionCode::WorkerStopped,
            state: AttentionState::Failed,
            attention: AttentionLevel::ActionRequired,
            blocks_work: pending > 0,
            owner: AttentionOwner::Local,
            instance_id: Some(state.instance_id.clone()),
            instance_name: Some(instance_name.clone()),
            peer_id: None,
            share_id: None,
            share_name: None,
            count: pending.max(1),
            total_count: None,
            next_at: None,
            recheck_at: None,
            observed_at: worker.observed_at,
            title: format!("{} stopped", worker.name),
            message: format!(
                "{}. Review server logs and restart ISM when safe.",
                worker.message
            ),
            href: "/operations".into(),
            operation_id: None,
            acknowledged: None,
            unresolved: Some(true),
            completeness: if matches!(worker.kind, Kind::Other) {
                AttentionCompleteness::Partial
            } else {
                AttentionCompleteness::Complete
            },
            examples: Vec::new(),
        });
    }
    let stopped_transfers = state.worker_evidence.transfer_failures();
    if !stopped_transfers.is_empty() {
        let mut running = HashSet::new();
        let mut groups = HashMap::new();
        for row in sqlx::query_as::<_, (String, String, String, String, String, String)>(
            "SELECT t.id,t.peer_id,p.name,t.share_id,s.name,t.rel_path
             FROM transfers t JOIN peers p ON p.id=t.peer_id JOIN shares s ON s.id=t.share_id
             WHERE t.status='running' ORDER BY t.peer_id,t.share_id,t.queued_at,t.id",
        )
        .fetch_all(&state.db)
        .await?
        {
            let (id, peer_id, peer_name, share_id, share_name, path) = row;
            running.insert(id.clone());
            let Some(failure) = stopped_transfers.get(&id) else {
                continue;
            };
            let key = (peer_id.clone(), share_id.clone(), failure.reason);
            let item = groups.entry(key).or_insert_with(|| AttentionItem {
                id: format!(
                    "transfer-worker:{}:{}:{}",
                    peer_id,
                    share_id,
                    failure.reason.key()
                ),
                code: AttentionCode::WorkerStopped,
                state: AttentionState::Failed,
                attention: AttentionLevel::ActionRequired,
                blocks_work: true,
                owner: AttentionOwner::Local,
                instance_id: Some(state.instance_id.clone()),
                instance_name: Some(instance_name.clone()),
                peer_id: Some(peer_id),
                share_id: Some(share_id),
                share_name: Some(share_name),
                count: 0,
                total_count: None,
                next_at: None,
                recheck_at: None,
                observed_at: failure.observed_at.clone(),
                title: "Transfer tasks stopped".into(),
                message: format!(
                    "{}. Review the affected transfers from {} in Operations.",
                    failure.reason.message(),
                    peer_name
                ),
                href: "/operations".into(),
                operation_id: None,
                acknowledged: None,
                unresolved: Some(true),
                completeness: AttentionCompleteness::Complete,
                examples: Vec::new(),
            });
            item.count += 1;
            if item.observed_at < failure.observed_at {
                item.observed_at = failure.observed_at.clone();
            }
            if item.examples.len() < 5 {
                item.examples.push(ism_core::dto::ActivityExample {
                    resource: path,
                    detail: None,
                });
            }
        }
        state
            .worker_evidence
            .discard_finished_transfers(&stopped_transfers, &running);
        let mut groups: Vec<_> = groups.into_values().collect();
        groups.sort_by(|a, b| a.id.cmp(&b.id));
        items.extend(groups);
    }
    let shares: Vec<(String, String)> = sqlx::query_as("SELECT id,name FROM shares")
        .fetch_all(&state.db)
        .await?;
    let receiver = crate::peer_evidence::snapshot(state).await?;
    let receiver_readable = receiver.orphaned == 0
        && receiver
            .peers
            .iter()
            .all(|peer| peer.fresh && peer.invalid_count == 0);
    if receiver.orphaned > 0 {
        items.push(AttentionItem {
            id: "orphaned-peer-evidence".into(),
            code: AttentionCode::PeerEvidenceUnavailable,
            state: AttentionState::Unavailable,
            attention: AttentionLevel::None,
            blocks_work: false,
            owner: AttentionOwner::Local,
            instance_id: Some(state.instance_id.clone()),
            instance_name: Some(instance_name.clone()),
            peer_id: None,
            share_id: None,
            share_name: None,
            count: receiver.orphaned,
            total_count: None,
            next_at: None,
            recheck_at: None,
            observed_at: observed_at.clone(),
            title: "Some receiver activity has no matching peer".into(),
            message: "Review peer configuration; the saved receiver status is unavailable.".into(),
            href: "/peers".into(),
            operation_id: None,
            acknowledged: None,
            unresolved: None,
            completeness: AttentionCompleteness::Partial,
            examples: Vec::new(),
        });
    }
    for peer in receiver.peers {
        let peer_observed_at = peer
            .observed_at
            .as_ref()
            .filter(|at| db::parse_ts(at).is_some())
            .cloned()
            .unwrap_or_else(|| observed_at.clone());
        if !peer.fresh || peer.invalid_count > 0 {
            items.push(AttentionItem {
                id: format!("peer-evidence:{}", peer.id),
                code: AttentionCode::PeerEvidenceUnavailable,
                state: AttentionState::Unavailable,
                attention: AttentionLevel::None,
                blocks_work: false,
                owner: AttentionOwner::Receiver,
                instance_id: peer.instance_id.clone(),
                instance_name: Some(peer.name.clone()),
                peer_id: Some(peer.id.clone()),
                share_id: None,
                share_name: None,
                count: if peer.fresh {
                    peer.invalid_count
                } else {
                    peer.cached_count
                },
                total_count: None,
                next_at: None,
                recheck_at: None,
                observed_at: peer_observed_at.clone(),
                title: if peer.fresh {
                    "Receiver status is incomplete".into()
                } else {
                    "Receiver status is unavailable".into()
                },
                message: if peer.fresh {
                    format!(
                        "Some reports from {} are unreadable. Check the peer connection.",
                        peer.name
                    )
                } else {
                    format!("Check the connection to {} in Peers.", peer.name)
                },
                href: "/peers".into(),
                operation_id: None,
                acknowledged: None,
                unresolved: None,
                completeness: AttentionCompleteness::Partial,
                examples: Vec::new(),
            });
        }
        for group in peer
            .groups
            .into_iter()
            .filter(|group| peer.fresh && group.status == "failed")
        {
            let managed = group.managed_operation;
            items.push(AttentionItem {
                id: format!(
                    "receiver-{}-failed:{}:{}",
                    if managed { "operation" } else { "transfer" },
                    peer.id,
                    group.share_name
                ),
                code: if managed {
                    AttentionCode::OperationFailed
                } else {
                    AttentionCode::ReceiverTransferFailed
                },
                state: AttentionState::Failed,
                attention: if managed {
                    AttentionLevel::Notice
                } else {
                    AttentionLevel::ActionRequired
                },
                blocks_work: !managed,
                owner: AttentionOwner::Receiver,
                instance_id: peer.instance_id.clone(),
                instance_name: Some(peer.name.clone()),
                peer_id: Some(peer.id.clone()),
                share_id: shares
                    .iter()
                    .find(|(_, name)| name == &group.share_name)
                    .map(|(id, _)| id.clone()),
                share_name: Some(group.share_name),
                count: group.count,
                total_count: Some(group.count),
                next_at: None,
                recheck_at: None,
                observed_at: peer_observed_at.clone(),
                title: if managed {
                    "A receiver operation failed".into()
                } else {
                    "Receiver transfers need recovery".into()
                },
                message: format!("Review sync issues on {} in Sync.", peer.name),
                href: "/sync?view=attention".into(),
                operation_id: None,
                acknowledged: None,
                unresolved: (!managed).then_some(true),
                completeness: if managed || peer.invalid_count > 0 {
                    AttentionCompleteness::Partial
                } else {
                    AttentionCompleteness::Complete
                },
                examples: group
                    .examples
                    .into_iter()
                    .map(|example| ism_core::dto::ActivityExample {
                        resource: example.resource,
                        detail: None,
                    })
                    .collect(),
            });
        }
    }
    let remote = crate::peers::attention::snapshot(state).await?;
    items.extend(remote.items);
    warnings.extend(remote.warnings);
    let now = chrono::Utc::now();
    for item in &mut items {
        let next = item
            .next_at
            .as_deref()
            .and_then(db::parse_ts)
            .filter(|at| *at > now)
            .map(db::ts);
        item.recheck_at = item.recheck_at.take().into_iter().chain(next).min();
    }
    if items
        .iter()
        .any(|item| item.code == AttentionCode::TransferEvidenceUnavailable)
    {
        warnings.push("File-specific transfer dependencies have not all been checked.".into());
    }
    if items
        .iter()
        .any(|item| item.completeness != AttentionCompleteness::Complete)
        && warnings.is_empty()
    {
        warnings.push("Some conditions could not be fully verified; absence of a blocker does not confirm that work is ready.".into());
    }
    let completeness = if warnings.is_empty()
        && items
            .iter()
            .all(|item| item.completeness == AttentionCompleteness::Complete)
        && remote
            .coverage
            .iter()
            .all(|source| source.completeness == AttentionCompleteness::Complete)
    {
        AttentionCompleteness::Complete
    } else {
        AttentionCompleteness::Partial
    };
    let mut coverage = Vec::new();
    let mut cover = |codes: &[AttentionCode], owner, readable| {
        coverage.extend(codes.iter().map(|code| AttentionCoverage {
            code: *code,
            owner,
            peer_id: None,
            completeness: if readable {
                AttentionCompleteness::Complete
            } else {
                AttentionCompleteness::Partial
            },
        }));
    };
    cover(
        &[
            AttentionCode::OperationRecovery,
            AttentionCode::OperationFailed,
            AttentionCode::WatcherPaused,
            AttentionCode::LibraryScanFailed,
            AttentionCode::RecoveryDecision,
            AttentionCode::TransferFailed,
            AttentionCode::TransferScheduled,
            AttentionCode::TransferScheduleInvalid,
            AttentionCode::TransferEvidenceUnavailable,
            AttentionCode::WorkerStopped,
        ],
        AttentionOwner::Local,
        true,
    );
    cover(
        &[
            AttentionCode::MaintenanceBlocked,
            AttentionCode::MaintenanceWaiting,
        ],
        AttentionOwner::Local,
        maintenance_readable,
    );
    cover(
        &[AttentionCode::MappingUnavailable],
        AttentionOwner::Local,
        mappings_readable,
    );
    cover(
        &[AttentionCode::TransferDependency],
        AttentionOwner::Local,
        queues
            .queued
            .iter()
            .all(|group| group.root_available.is_some()),
    );
    cover(
        &[AttentionCode::TransferBackoff],
        AttentionOwner::Local,
        queues
            .queued
            .iter()
            .all(|group| group.unclassified_backoff == 0),
    );
    cover(
        &[AttentionCode::PeerOffline],
        AttentionOwner::Local,
        queues
            .queued
            .iter()
            .all(|group| matches!(group.peer_status.as_str(), "online" | "offline")),
    );
    cover(
        &[AttentionCode::PeerEvidenceUnavailable],
        AttentionOwner::Local,
        inbox_readable
            && !items.iter().any(|item| {
                item.owner == AttentionOwner::Local
                    && item.code == AttentionCode::PeerEvidenceUnavailable
                    && item.state == AttentionState::Unavailable
            }),
    );
    cover(
        &[
            AttentionCode::FolderRemovalFailed,
            AttentionCode::PeerInventoryFailed,
            AttentionCode::PeerInboxFailed,
        ],
        AttentionOwner::Local,
        inbox_readable,
    );
    cover(
        &[AttentionCode::RecoveryFailed],
        AttentionOwner::Local,
        comparisons_readable,
    );
    cover(
        &[
            AttentionCode::ReceiverTransferFailed,
            AttentionCode::OperationFailed,
            AttentionCode::PeerEvidenceUnavailable,
        ],
        AttentionOwner::Receiver,
        receiver_readable,
    );
    coverage.extend(remote.coverage);
    Ok(AttentionSnapshot {
        observed_at,
        completeness,
        warnings,
        coverage,
        items,
    })
}
