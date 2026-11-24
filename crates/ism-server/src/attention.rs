//! Structured, evidence-backed conditions for the administration UI.

use ism_core::dto::{
    AttentionCode, AttentionCompleteness, AttentionItem, AttentionLevel, AttentionOwner,
    AttentionSnapshot, AttentionState,
};

use crate::db;
use crate::error::AppResult;
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
    let items = operations
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
        .collect();
    Ok(AttentionSnapshot {
        observed_at,
        completeness: AttentionCompleteness::Partial,
        warnings: vec![
            "Some background and peer conditions are not yet included in this view.".into(),
        ],
        items,
    })
}
