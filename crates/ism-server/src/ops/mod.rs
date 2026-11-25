//! Managed operations: submission, previews, status, progress events and execution.

pub mod exec;
pub mod plan;
#[allow(
    dead_code,
    reason = "The manifest protocol is staged until peer and reviewed-operation contracts are integrated."
)]
pub mod review;
mod tree;
pub mod uploads;

use ism_core::dto::{
    CancelOperationByKey, CancelOperationByKeyResult, ClientAction, Issue, ItemStatus, Location,
    Operation, OperationAccepted, OperationAction, OperationItem, OperationRequest,
    OperationStatus, PreviewResponse, Progress, TrashEntry,
};

use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::state::{AppState, ServerEvent};

pub const MAX_ACTIONS: usize = 5000;

fn validate_key(key: &str) -> AppResult<()> {
    if key.trim().is_empty() || key.len() > 200 {
        return Err(AppError::bad_request(
            "bad_idempotency_key",
            "idempotency_key must be 1-200 characters",
        ));
    }
    Ok(())
}

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
        OperationAction::Trash { source, .. }
        | OperationAction::TrashIfAssetMatches { source, .. }
        | OperationAction::TrashFolder { source, .. } => {
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
    let key = crate::crypto::sha256_hex(
        format!("{}\0{}", principal.key(), req.idempotency_key).as_bytes(),
    );
    crate::activity::observe(
        state,
        format!("preview:{key}"),
        "Operation preview",
        req.label.as_deref().unwrap_or("Managed file request"),
        Some("/operations"),
        |task| async move { preview_reporting(state, principal, req, &task).await },
    )
    .await
}

async fn preview_reporting(
    state: &AppState,
    principal: &Principal,
    req: &OperationRequest,
    task: &crate::activity::Reporter,
) -> AppResult<PreviewResponse> {
    task.phase("Planning and verifying requested file actions");
    let ctx = plan::Ctx::new(state, principal, req.allow_owner_transfer).await?;
    let mut items = Vec::with_capacity(req.actions.len());
    let mut blockers: Vec<Issue> = Vec::new();
    for (index, action) in req.actions.iter().enumerate() {
        task.progress(
            index as u64,
            Some(req.actions.len() as u64),
            "actions planned",
        );
        let p = plan::plan(&ctx, action).await?;
        for b in &p.blockers {
            if !blockers.iter().any(|x| x.code == b.code) {
                blockers.push(b.clone());
            }
            task.progress(
                req.actions.len() as u64,
                Some(req.actions.len() as u64),
                "actions planned",
            );
        }
        items.push(p.preview(index as u32, action.kind()));
    }
    Ok(PreviewResponse {
        ok: blockers.is_empty(),
        items,
        blockers,
    })
}

/// Validates, plans and queues an operation. Identical retries return the first operation.
/// Requests with blockers are refused as a whole and nothing is queued, unless
/// `allow_partial` is set: then blocked items are recorded as failed and the rest run.
pub async fn submit(
    state: &AppState,
    principal: &Principal,
    req: OperationRequest,
) -> AppResult<OperationAccepted> {
    validate_key(&req.idempotency_key)?;
    let key = crate::crypto::sha256_hex(
        format!("{}\0{}", principal.key(), req.idempotency_key).as_bytes(),
    );
    let task = crate::activity::Task::start(
        state,
        format!("submission:{key}"),
        "Operation admission",
        req.label.as_deref().unwrap_or("Managed file request"),
        Some("/operations"),
    )
    .await?;
    task.waiting("Waiting for the operation submission lock");
    let guard = state
        .lock_operation_submission(&principal.key(), &req.idempotency_key)
        .await;
    let (state, principal) = (state.clone(), principal.clone());
    tokio::spawn(async move {
        let _guard = guard;
        task.phase("Validating and admitting the managed operation");
        let result = submit_locked(&state, &principal, req).await;
        task.finish(&result).await?;
        result
    })
    .await
    .map_err(AppError::internal)?
}

async fn submit_locked(
    state: &AppState,
    principal: &Principal,
    req: OperationRequest,
) -> AppResult<OperationAccepted> {
    let cancelled: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operation_cancellations WHERE principal = ? AND idempotency_key = ?)",
    )
    .bind(principal.key())
    .bind(&req.idempotency_key)
    .fetch_one(&state.db)
    .await?;
    if cancelled {
        return Err(AppError::conflict(
            "operation_key_cancelled",
            "this operation key was cancelled before submission",
        ));
    }
    if let Some((id, status, stored)) = sqlx::query_as::<_, (String, String, String)>(
        "SELECT id, status, request_json FROM operations WHERE principal = ? AND idempotency_key = ?",
    )
    .bind(principal.key())
    .bind(&req.idempotency_key)
    .fetch_optional(&state.db)
    .await?
    {
        let value: serde_json::Value = serde_json::from_str(&stored)?;
        if value["actions"].as_array().is_some_and(|actions| {
            actions.iter().any(|action| action["kind"] == "publish_upload")
        }) {
            return Err(AppError::conflict(
                "idempotency_key_conflict",
                "idempotency key was already used for a different operation",
            ));
        }
        let stored: OperationRequest = serde_json::from_value(value)?;
        if serde_json::to_value(&stored)? != serde_json::to_value(&req)? {
            return Err(AppError::conflict(
                "idempotency_key_conflict",
                "idempotency key was already used for a different operation",
            ));
        }
        return Ok(OperationAccepted {
            operation_id: id,
            status: OperationStatus::parse(&status),
        });
    }
    let preview = preview(state, principal, &req).await?;
    if !req.allow_partial {
        if let Some(first) = preview.blockers.first() {
            return Err(AppError::conflict_dynamic(
                first.code.clone(),
                first.message.clone(),
            ));
        }
        return insert(state, principal, &req).await;
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let accepted = insert_in(&mut tx, principal, &req, None).await?;
    for item in &preview.items {
        if let Some(blocker) = item.blockers.first() {
            sqlx::query(
                "UPDATE operation_items SET status = 'failed', error_code = ?, error_message = ? WHERE operation_id = ? AND idx = ?",
            )
            .bind(&blocker.code)
            .bind(&blocker.message)
            .bind(&accepted.operation_id)
            .bind(item.index as i64)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    for item in &preview.items {
        if let Some(blocker) = item.blockers.first() {
            log_item_failure(
                &accepted.operation_id,
                i64::from(item.index),
                &blocker.code,
                &blocker.message,
            );
        }
    }
    announce_queued(state, &accepted);
    Ok(accepted)
}

fn log_item_failure(operation: &str, item: i64, code: &str, message: &str) {
    tracing::warn!(
        operation = %operation, item, code = %code,
        next_retry = "no automatic operation retry; review recovery or submit again",
        "operation item failed: {}", crate::error::redact(message)
    );
}

/// Queues an operation without a preview (internal use, e.g. upload publishing).
pub async fn insert(
    state: &AppState,
    principal: &Principal,
    req: &OperationRequest,
) -> AppResult<OperationAccepted> {
    insert_inner(state, principal, req, None).await
}

async fn insert_inner(
    state: &AppState,
    principal: &Principal,
    req: &OperationRequest,
    upload_id: Option<&str>,
) -> AppResult<OperationAccepted> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let accepted = insert_in(&mut tx, principal, req, upload_id).await?;
    tx.commit().await?;
    announce_queued(state, &accepted);
    Ok(accepted)
}

pub(crate) async fn insert_in(
    conn: &mut sqlx::SqliteConnection,
    principal: &Principal,
    req: &OperationRequest,
    upload_id: Option<&str>,
) -> AppResult<OperationAccepted> {
    let id = crate::crypto::new_id("op");
    let now = db::now();
    let cancelled: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operation_cancellations WHERE principal = ? AND idempotency_key = ?)",
    )
    .bind(principal.key())
    .bind(&req.idempotency_key)
    .fetch_one(&mut *conn)
    .await?;
    if cancelled {
        return Err(AppError::conflict(
            "operation_key_cancelled",
            "this operation key was cancelled before submission",
        ));
    }
    if let Some(upload_id) = upload_id {
        let updated = sqlx::query(
            "UPDATE uploads SET status = 'publishing', operation_id = ? WHERE id = ? AND principal = ? AND status = 'receiving'",
        )
        .bind(&id)
        .bind(upload_id)
        .bind(principal.key())
        .execute(&mut *conn)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(AppError::conflict(
                "upload_closed",
                "the upload is no longer receiving data",
            ));
        }
    }
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
    .execute(&mut *conn)
    .await?;
    for (index, action) in req.actions.iter().enumerate() {
        sqlx::query(
            "INSERT INTO operation_items (operation_id, idx, kind, action_json, status) VALUES (?, ?, ?, ?, 'pending')",
        )
        .bind(&id)
        .bind(index as i64)
        .bind(action.kind())
        .bind(serde_json::to_string(action)?)
        .execute(&mut *conn)
        .await?;
    }
    Ok(OperationAccepted {
        operation_id: id,
        status: OperationStatus::Queued,
    })
}

pub(crate) fn announce_queued(state: &AppState, accepted: &OperationAccepted) {
    state.ops_wake.notify_one();
    state.emit(ServerEvent::OperationUpdated {
        operation_id: accepted.operation_id.clone(),
    });
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

type ItemRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
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

pub(crate) async fn load_item(
    state: &AppState,
    operation_id: &str,
    index: i64,
) -> AppResult<OperationItem> {
    let row: Option<ItemRow> = sqlx::query_as(
        "SELECT idx, kind, status, error_code, error_message, result_json
             FROM operation_items WHERE operation_id = ? AND idx = ?",
    )
    .bind(operation_id)
    .bind(index)
    .fetch_optional(&state.db)
    .await?;
    let Some((idx, kind, status, code, message, result)) = row else {
        return Err(AppError::not_found("operation item not found"));
    };
    Ok(OperationItem {
        index: idx as u32,
        kind,
        status: ItemStatus::parse(&status),
        error: code.map(|code| Issue {
            code,
            message: message.unwrap_or_default(),
        }),
        result: result.and_then(|value| serde_json::from_str(&value).ok()),
    })
}

/// Loads an operation visible to `principal` (clients only see their own).
pub async fn get(state: &AppState, principal: &Principal, id: &str) -> AppResult<Operation> {
    let (op, owner) = load(state, id).await?;
    if matches!(principal, Principal::Client(_)) && owner != principal.key() {
        return Err(AppError::not_found("operation not found"));
    }
    Ok(op)
}

/// The caller's operation submitted under `key`, read without side effects.
pub async fn find_by_key(
    state: &AppState,
    principal: &Principal,
    key: &str,
) -> AppResult<Operation> {
    validate_key(key)?;
    let id: Option<String> =
        sqlx::query_scalar("SELECT id FROM operations WHERE principal = ? AND idempotency_key = ?")
            .bind(principal.key())
            .bind(key)
            .fetch_optional(&state.db)
            .await?;
    if let Some(id) = id {
        return Ok(load(state, &id).await?.0);
    }
    let cancelled: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operation_cancellations WHERE principal = ? AND idempotency_key = ?)",
    )
    .bind(principal.key())
    .bind(key)
    .fetch_one(&state.db)
    .await?;
    if cancelled {
        return Err(AppError::new(
            axum::http::StatusCode::NOT_FOUND,
            "cancelled_before_submission",
            "the key was cancelled before an operation was submitted",
        ));
    }
    Err(AppError::not_found(
        "no operation was submitted with this key",
    ))
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
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("UPDATE operations SET cancel_requested = 1 WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE operations SET status = 'cancelled', finished_at = ? WHERE id = ? AND status = 'queued'")
        .bind(db::now())
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id = ?")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    if status == "cancelled" {
        sqlx::query("UPDATE operation_items SET status = 'skipped' WHERE operation_id = ? AND status = 'pending'")
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    if status == "cancelled" {
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

pub async fn cancel_by_key(
    state: &AppState,
    principal: &Principal,
    request: CancelOperationByKey,
) -> AppResult<CancelOperationByKeyResult> {
    let key = request.idempotency_key;
    validate_key(&key)?;
    if request.expected_request.idempotency_key != key {
        return Err(AppError::bad_request(
            "bad_idempotency_key",
            "expected request must use the same idempotency key",
        ));
    }
    check_grants(principal, &request.expected_request)?;
    let guard = state
        .lock_operation_submission(&principal.key(), &key)
        .await;
    let (state, principal) = (state.clone(), principal.clone());
    tokio::spawn(async move {
        let _guard = guard;
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT id, request_json FROM operations WHERE principal = ? AND idempotency_key = ?",
        )
        .bind(principal.key())
        .bind(&key)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((id, stored)) = row else {
            sqlx::query(
                "INSERT OR IGNORE INTO operation_cancellations (principal, idempotency_key, created_at) VALUES (?, ?, ?)",
            )
            .bind(principal.key())
            .bind(&key)
            .bind(db::now())
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(CancelOperationByKeyResult::CancelledBeforeSubmission);
        };
        let stored: serde_json::Value = serde_json::from_str(&stored)?;
        let stored: OperationRequest = serde_json::from_value(stored).map_err(|_| {
            AppError::conflict(
                "idempotency_key_conflict",
                "idempotency key was already used for a different operation",
            )
        })?;
        if serde_json::to_value(stored)? != serde_json::to_value(request.expected_request)? {
            return Err(AppError::conflict(
                "idempotency_key_conflict",
                "idempotency key was already used for a different operation",
            ));
        }
        let updated = sqlx::query(
            "UPDATE operations SET status = 'cancelled', cancel_requested = 1, finished_at = ? WHERE id = ? AND status = 'queued'",
        )
        .bind(db::now())
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 0 {
            sqlx::query(
                "UPDATE operation_items SET status = 'skipped' WHERE operation_id = ? AND status = 'pending'",
            )
            .bind(&id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let (operation, _) = load(&state, &id).await?;
        if updated.rows_affected() != 0 {
            record(&state, &id, "finished", &serde_json::to_value(&operation)?).await?;
            state.ops_wake.notify_one();
        }
        Ok(CancelOperationByKeyResult::Operation { operation })
    })
    .await
    .map_err(AppError::internal)?
}

/// Sync recovery is verified by ISM, never by a manual acknowledgement.
pub async fn resolve_recovery(state: &AppState, id: &str) -> AppResult<()> {
    crate::peers::recovery::discover(state).await?;
    if crate::peers::recovery::get(state, ism_core::dto::RecoveryKind::Operation, id)
        .await?
        .is_some()
    {
        crate::peers::recovery::submit(
            state,
            ism_core::dto::RecoveryBatchInput {
                targets: vec![ism_core::dto::RecoveryTarget {
                    kind: ism_core::dto::RecoveryKind::Operation,
                    id: id.into(),
                    peer_id: None,
                }],
                all: false,
                resolution: None,
            },
        )
        .await?;
        return Ok(());
    }
    crate::maintenance::resolve_operation_recovery(state, id).await?;
    state.sync_wake.notify_waiters();
    state.emit(ServerEvent::OperationUpdated {
        operation_id: id.to_owned(),
    });
    state.emit(ServerEvent::SyncChanged);
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
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let seq: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM operation_events WHERE operation_id = ?",
    )
    .bind(operation_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO operation_events (operation_id, seq, event, data, at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(operation_id)
    .bind(seq)
    .bind(event)
    .bind(data.to_string())
    .bind(db::now())
    .execute(&mut *tx)
    .await?;
    // Keep the stream bounded; clients with older cursors resync.
    sqlx::query("DELETE FROM operation_events WHERE operation_id = ? AND seq <= ?")
        .bind(operation_id)
        .bind(seq - 2000)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
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
