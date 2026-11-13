//! Managed operations: submission, previews, status, progress events and execution.

pub mod exec;
pub mod plan;
pub mod uploads;

use ism_core::dto::{
    ClientAction, Issue, ItemStatus, Location, Operation, OperationAccepted, OperationAction,
    OperationItem, OperationRequest, OperationStatus, PreviewResponse, Progress, TrashEntry,
};

use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::state::{AppState, ServerEvent};

pub const MAX_ACTIONS: usize = 5000;

fn required_actions(action: &OperationAction) -> Vec<(ClientAction, Option<&Location>)> {
    match action {
        OperationAction::Move { source, target, .. }
        | OperationAction::Rename { source, target, .. }
        | OperationAction::MoveFolder { source, target } => {
            vec![
                (ClientAction::Manage, Some(source)),
                (ClientAction::Manage, Some(target)),
            ]
        }
        OperationAction::CreateFolder { target } => vec![(ClientAction::Manage, Some(target))],
        OperationAction::Trash { source } | OperationAction::TrashFolder { source } => {
            vec![(ClientAction::Trash, Some(source))]
        }
        OperationAction::Restore { .. } => vec![(ClientAction::Trash, None)],
        OperationAction::Purge { .. } => vec![(ClientAction::Purge, None)],
        OperationAction::PublishUpload { .. } => vec![(ClientAction::Upload, None)],
    }
}

fn check_grants(principal: &Principal, req: &OperationRequest) -> AppResult<()> {
    if req.actions.is_empty() {
        return Err(AppError::bad_request("empty_request", "no actions"));
    }
    if req.actions.len() > MAX_ACTIONS {
        return Err(AppError::bad_request(
            "too_many_actions",
            format!("at most {MAX_ACTIONS} actions per operation"),
        ));
    }
    for action in &req.actions {
        for (grant, loc) in required_actions(action) {
            match loc {
                Some(loc) => principal.require(grant, &loc.root_id)?,
                None => {
                    if let Principal::Client(g) = principal
                        && !g.actions.contains(&grant)
                    {
                        return Err(AppError::forbidden(format!(
                            "this credential may not '{}'",
                            grant.as_str()
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn preview(
    state: &AppState,
    principal: &Principal,
    req: &OperationRequest,
) -> AppResult<PreviewResponse> {
    check_grants(principal, req)?;
    let ctx = plan::Ctx::new(state, principal, req.allow_owner_transfer).await?;
    let mut items = Vec::with_capacity(req.actions.len());
    let mut blockers: Vec<Issue> = Vec::new();
    for (index, action) in req.actions.iter().enumerate() {
        let p = plan::plan(&ctx, action).await?;
        for b in &p.blockers {
            if !blockers.iter().any(|x| x.code == b.code) {
                blockers.push(b.clone());
            }
        }
        items.push(p.preview(index as u32, action.kind()));
    }
    Ok(PreviewResponse {
        ok: blockers.is_empty(),
        items,
        blockers,
    })
}

/// Validates, plans and queues an operation. The same idempotency key returns the first
/// operation. Requests with blockers are refused as a whole; nothing is queued.
pub async fn submit(
    state: &AppState,
    principal: &Principal,
    req: OperationRequest,
) -> AppResult<OperationAccepted> {
    if req.idempotency_key.trim().is_empty() || req.idempotency_key.len() > 200 {
        return Err(AppError::bad_request(
            "bad_idempotency_key",
            "idempotency_key must be 1-200 characters",
        ));
    }
    if let Some((id, status)) = sqlx::query_as::<_, (String, String)>(
        "SELECT id, status FROM operations WHERE principal = ? AND idempotency_key = ?",
    )
    .bind(principal.key())
    .bind(&req.idempotency_key)
    .fetch_optional(&state.db)
    .await?
    {
        return Ok(OperationAccepted {
            operation_id: id,
            status: OperationStatus::parse(&status),
        });
    }
    let preview = preview(state, principal, &req).await?;
    if let Some(first) = preview.blockers.first() {
        return Err(AppError::conflict_dynamic(
            first.code.clone(),
            first.message.clone(),
        ));
    }
    insert(state, principal, &req).await
}

/// Queues an operation without a preview (internal use, e.g. upload publishing).
pub async fn insert(
    state: &AppState,
    principal: &Principal,
    req: &OperationRequest,
) -> AppResult<OperationAccepted> {
    let id = crate::crypto::new_id("op");
    let now = db::now();
    let mut tx = state.db.begin().await?;
    sqlx::query(
        "INSERT INTO operations (id, idempotency_key, principal, source, label, request_json, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)",
    )
    .bind(&id)
    .bind(&req.idempotency_key)
    .bind(principal.key())
    .bind(principal.source())
    .bind(&req.label)
    .bind(serde_json::to_string(req)?)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    for (index, action) in req.actions.iter().enumerate() {
        sqlx::query(
            "INSERT INTO operation_items (operation_id, idx, kind, action_json, status) VALUES (?, ?, ?, ?, 'pending')",
        )
        .bind(&id)
        .bind(index as i64)
        .bind(action.kind())
        .bind(serde_json::to_string(action)?)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    state.ops_wake.notify_one();
    state.emit(ServerEvent::OperationUpdated {
        operation_id: id.clone(),
    });
    Ok(OperationAccepted {
        operation_id: id,
        status: OperationStatus::Queued,
    })
}

type OpRow = (
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    Option<String>,
);

pub async fn load(state: &AppState, id: &str) -> AppResult<(Operation, String)> {
    let row: Option<OpRow> = sqlx::query_as(
        "SELECT id, status, label, source, created_at, principal, finished_at FROM operations WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    let Some((operation_id, status, label, source, created_at, principal, finished_at)) = row
    else {
        return Err(AppError::not_found("operation not found"));
    };
    type ItemRow = (
        i64,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<ItemRow> = sqlx::query_as(
        "SELECT idx, kind, status, error_code, error_message, result_json FROM operation_items
         WHERE operation_id = ? ORDER BY idx",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    let items: Vec<OperationItem> = rows
        .into_iter()
        .map(|(idx, kind, status, code, message, result)| OperationItem {
            index: idx as u32,
            kind,
            status: ItemStatus::parse(&status),
            error: code.map(|code| Issue {
                code,
                message: message.unwrap_or_default(),
            }),
            result: result.and_then(|r| serde_json::from_str(&r).ok()),
        })
        .collect();
    let done = items
        .iter()
        .filter(|i| !matches!(i.status, ItemStatus::Pending | ItemStatus::Running))
        .count() as u32;
    Ok((
        Operation {
            operation_id,
            status: OperationStatus::parse(&status),
            label,
            source,
            created_at,
            finished_at,
            progress: Progress {
                done,
                total: items.len() as u32,
            },
            items,
        },
        principal,
    ))
}

/// Loads an operation visible to `principal` (clients only see their own).
pub async fn get(state: &AppState, principal: &Principal, id: &str) -> AppResult<Operation> {
    let (op, owner) = load(state, id).await?;
    if matches!(principal, Principal::Client(_)) && owner != principal.key() {
        return Err(AppError::not_found("operation not found"));
    }
    Ok(op)
}

pub async fn list(state: &AppState, status: Option<&str>, limit: i64) -> AppResult<Vec<Operation>> {
    let ids: Vec<String> = match status {
        Some(s) => {
            sqlx::query_scalar(
                "SELECT id FROM operations WHERE status = ? ORDER BY created_at DESC LIMIT ?",
            )
            .bind(s)
            .bind(limit)
            .fetch_all(&state.db)
            .await?
        }
        None => {
            sqlx::query_scalar("SELECT id FROM operations ORDER BY created_at DESC LIMIT ?")
                .bind(limit)
                .fetch_all(&state.db)
                .await?
        }
    };
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let (mut op, _) = load(state, &id).await?;
        op.items.truncate(200);
        out.push(op);
    }
    Ok(out)
}

pub async fn cancel(state: &AppState, principal: &Principal, id: &str) -> AppResult<Operation> {
    get(state, principal, id).await?;
    sqlx::query("UPDATE operations SET cancel_requested = 1 WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await?;
    sqlx::query("UPDATE operations SET status = 'cancelled', finished_at = ? WHERE id = ? AND status = 'queued'")
        .bind(db::now())
        .bind(id)
        .execute(&state.db)
        .await?;
    let (op, _) = load(state, id).await?;
    if op.status == OperationStatus::Cancelled {
        sqlx::query("UPDATE operation_items SET status = 'skipped' WHERE operation_id = ? AND status = 'pending'")
            .bind(id)
            .execute(&state.db)
            .await?;
        record(
            state,
            id,
            "finished",
            &serde_json::to_value(load(state, id).await?.0)?,
        )
        .await?;
    }
    state.ops_wake.notify_one();
    Ok(load(state, id).await?.0)
}

/// Admin: the files were checked by hand; release the server barrier.
pub async fn resolve_recovery(state: &AppState, id: &str) -> AppResult<()> {
    let row: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT status, admitted_server_ids_json, recovery_resolved FROM operations WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    let Some((status, servers, resolved)) = row else {
        return Err(AppError::not_found("operation not found"));
    };
    if status != "recovery_required"
        && !(status == "partial" && resolved == 0 && !db::json_vec(&servers).is_empty())
    {
        return Err(AppError::conflict(
            "not_recoverable",
            "this operation does not need recovery",
        ));
    }
    if resolved != 0 {
        return Ok(());
    }
    sqlx::query("UPDATE operations SET recovery_resolved = 1 WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await?;
    let recovery_servers: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT value FROM operations, json_each(operations.admitted_server_ids_json) WHERE id = ?",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    for server in recovery_servers {
        crate::maintenance::resolve_recovery(state, &server).await?;
    }
    db::audit(&state.db, "admin", "operation.resolve", id).await;
    Ok(())
}

/// Appends a progress event; SSE subscribers read them in order.
pub async fn record(
    state: &AppState,
    operation_id: &str,
    event: &str,
    data: &serde_json::Value,
) -> AppResult<i64> {
    let seq: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM operation_events WHERE operation_id = ?",
    )
    .bind(operation_id)
    .fetch_one(&state.db)
    .await?;
    sqlx::query(
        "INSERT INTO operation_events (operation_id, seq, event, data, at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(operation_id)
    .bind(seq)
    .bind(event)
    .bind(data.to_string())
    .bind(db::now())
    .execute(&state.db)
    .await?;
    // Keep the stream bounded; clients with older cursors resync.
    sqlx::query("DELETE FROM operation_events WHERE operation_id = ? AND seq <= ?")
        .bind(operation_id)
        .bind(seq - 2000)
        .execute(&state.db)
        .await?;
    state.emit(ServerEvent::OperationUpdated {
        operation_id: operation_id.to_owned(),
    });
    Ok(seq)
}

pub async fn trash_list(
    state: &AppState,
    principal: &Principal,
    root_id: Option<&str>,
) -> AppResult<Vec<TrashEntry>> {
    type Row = (
        String,
        String,
        String,
        i64,
        i64,
        String,
        Option<String>,
        String,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, root_id, path, is_folder, size, trashed_at, operation_id, asset_ids_json FROM trash
         WHERE restored_at IS NULL AND purged_at IS NULL ORDER BY trashed_at DESC",
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .filter(|r| root_id.is_none_or(|id| id == r.1) && principal.sees_root(&r.1))
        .map(
            |(id, root_id, path, is_folder, size, trashed_at, operation_id, ids)| TrashEntry {
                trash_id: id,
                location: Location { root_id, path },
                is_folder: is_folder != 0,
                size: size as u64,
                trashed_at,
                operation_id,
                asset_ids: db::json_vec(&ids),
            },
        )
        .collect())
}
