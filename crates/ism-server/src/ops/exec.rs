//! Runs queued operations one at a time inside the maintenance window.
//!
//! Each item is re-planned against the current disk and database, then executed step by step.
//! A failing step undoes the completed ones in reverse order; the database transaction is the
//! last step, so a committed item never needs undoing. When undoing fails, the item and its
//! servers are marked `recovery_required` and Immich watchers stay off.

use std::collections::BTreeMap;
use std::fs;

use crate::peers::LocalChange;
use ism_core::dto::{
    Change, ClientAction, FileRole, ImmichMoveMode, ItemStatus, OperationAction,
    OperationCommitted, OperationStatus,
};
use serde_json::json;

use super::plan::{self, ApiKind, ApiStep, DbStep, Plan, Step};
use crate::auth::{ClientGrant, Principal};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::immich::{pg, registry};
use crate::state::{AppState, ServerEvent};

pub async fn run(state: AppState) {
    let tick = std::time::Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        loop {
            let next: Option<String> = match sqlx::query_scalar(
                "SELECT id FROM operations WHERE status = 'queued' ORDER BY created_at LIMIT 1",
            )
            .fetch_optional(&state.db)
            .await
            {
                Ok(next) => next,
                Err(e) => {
                    tracing::error!("executor: {e}");
                    None
                }
            };
            let Some(id) = next else { break };
            if let Err(e) = run_operation(&state, &id).await {
                tracing::error!(operation = %id, "operation failed: {}", e.message());
                let result = async {
                    let mut tx = state.db.begin().await?;
                    let updated = sqlx::query("UPDATE operations SET status = 'failed', finished_at = ? WHERE id = ? AND status IN ('queued', 'waiting_maintenance')")
                        .bind(db::now())
                        .bind(&id)
                        .execute(&mut *tx)
                        .await?;
                    if updated.rows_affected() != 0 {
                        sqlx::query("UPDATE uploads SET status = 'failed' WHERE operation_id = ? AND status = 'publishing'")
                            .bind(&id)
                            .execute(&mut *tx)
                            .await?;
                    }
                    tx.commit().await
                }.await;
                if let Err(error) = result {
                    tracing::error!(operation = %id, "could not record operation failure: {error}");
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(tick) => {}
            () = state.ops_wake.notified() => {}
        }
    }
}

/// Rebuilds the principal that submitted an operation; revoked tokens lose their rights.
pub async fn principal_from_key(state: &AppState, key: &str) -> AppResult<Principal> {
    if let Some(username) = key.strip_prefix("admin:") {
        return Ok(Principal::Admin {
            username: username.into(),
        });
    }
    if let Some(peer_id) = key.strip_prefix("peer:") {
        return Ok(Principal::Peer {
            peer_id: peer_id.into(),
        });
    }
    if let Some(token_id) = key.strip_prefix("token:") {
        let row: Option<(String, String, String)> = sqlx::query_as(
            "SELECT kind, roots_json, actions_json FROM client_tokens WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(token_id)
        .fetch_optional(&state.db)
        .await?;
        let (kind, roots, actions) =
            row.ok_or_else(|| AppError::unauthorized("the credential was revoked"))?;
        return Ok(Principal::Client(ClientGrant {
            token_id: token_id.into(),
            kind,
            roots: db::json_vec(&roots),
            actions: db::json_vec(&actions)
                .iter()
                .filter_map(|a| ClientAction::parse(a))
                .collect(),
        }));
    }
    Ok(Principal::Admin {
        username: key.into(),
    })
}

async fn set_status(state: &AppState, id: &str, status: OperationStatus) -> AppResult<()> {
    let finished = status.is_terminal().then(db::now);
    let mut tx = state.db.begin().await?;
    sqlx::query(
        "UPDATE operations SET status = ?, finished_at = COALESCE(?, finished_at) WHERE id = ?",
    )
    .bind(status.as_str())
    .bind(finished)
    .bind(id)
    .execute(&mut *tx)
    .await?;
    if matches!(
        status,
        OperationStatus::Partial
            | OperationStatus::Failed
            | OperationStatus::Cancelled
            | OperationStatus::RecoveryRequired
    ) {
        sqlx::query(
            "UPDATE uploads SET status = 'failed' WHERE operation_id = ? AND status = 'publishing'",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    let (op, _) = super::load(state, id).await?;
    let event = if status.is_terminal() {
        "finished"
    } else {
        "progress"
    };
    super::record(state, id, event, &serde_json::to_value(op)?).await?;
    Ok(())
}

async fn set_item(
    state: &AppState,
    id: &str,
    index: i64,
    status: ItemStatus,
    error: Option<(&str, &str)>,
    result: Option<&serde_json::Value>,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE operation_items SET status = ?, error_code = ?, error_message = ?, result_json = COALESCE(?, result_json)
         WHERE operation_id = ? AND idx = ?",
    )
    .bind(status.as_str())
    .bind(error.map(|e| e.0))
    .bind(error.map(|e| e.1))
    .bind(result.map(ToString::to_string))
    .bind(id)
    .bind(index)
    .execute(&state.db)
    .await?;
    let (op, _) = super::load(state, id).await?;
    if let Some(item) = op.items.get(index as usize) {
        super::record(state, id, "item", &serde_json::to_value(item)?).await?;
    }
    Ok(())
}

pub async fn run_operation(state: &AppState, id: &str) -> AppResult<()> {
    let _replication_guard = state.replication_operation_lock.lock().await;
    let claimed = sqlx::query(
        "UPDATE operations SET status = 'waiting_maintenance' WHERE id = ? AND status = 'queued'",
    )
    .bind(id)
    .execute(&state.db)
    .await?;
    if claimed.rows_affected() == 0 {
        return Ok(());
    }
    let (request_json, principal_key): (String, String) =
        sqlx::query_as("SELECT request_json, principal FROM operations WHERE id = ?")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    // Read only the flags: stored actions may include the internal `publish_upload`.
    let request: serde_json::Value = serde_json::from_str(&request_json)?;
    let allow_owner_transfer = request["allow_owner_transfer"].as_bool().unwrap_or(false);
    let actions: Vec<OperationAction> = {
        let raw: Vec<String> = sqlx::query_scalar(
            "SELECT action_json FROM operation_items WHERE operation_id = ? ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&state.db)
        .await?;
        raw.iter()
            .map(|a| parse_action(a))
            .collect::<Result<_, _>>()?
    };
    // Items refused at submission (`allow_partial`) stay failed and are not run.
    let rejected: Vec<i64> = sqlx::query_scalar(
        "SELECT idx FROM operation_items WHERE operation_id = ? AND status = 'failed'",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    let principal = match principal_from_key(state, &principal_key).await {
        Ok(p) => p,
        Err(e) => {
            fail_all(state, id, e.code(), e.message()).await?;
            return Ok(());
        }
    };
    let ctx = plan::Ctx::new(state, &principal, allow_owner_transfer).await?;

    // Maintenance admission is limited to servers whose current plan can need legacy DB or
    // queue-scan behavior. API-native external-library plans stay independent of unrelated scans.
    let mut servers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, action) in actions.iter().enumerate() {
        if rejected.contains(&(index as i64)) {
            continue;
        }
        let touched = ctx.touched_servers(action).await?;
        let preliminary_plan = plan::plan(&ctx, action).await;
        for (server_id, library) in touched {
            let requires_barrier = match &preliminary_plan {
                Ok(plan) => plan_requires_queue_barrier(state, &server_id, plan).await?,
                Err(_) => true,
            };
            if !requires_barrier {
                continue;
            }
            let libs = servers.entry(server_id).or_default();
            if let Some(l) = library
                && !libs.contains(&l)
            {
                libs.push(l);
            }
        }
    }
    if !servers.is_empty() {
        set_status(state, id, OperationStatus::WaitingMaintenance).await?;
    }
    let mut admitted: Vec<String> = Vec::new();
    for server in servers.keys() {
        if let Err(e) = crate::maintenance::acquire(state, server).await {
            for server in &admitted {
                crate::maintenance::release(state, server).await?;
            }
            fail_all(state, id, e.code(), e.message()).await?;
            return Ok(());
        }
        admitted.push(server.clone());
    }
    sqlx::query("UPDATE operations SET status = 'running', started_at = ?, admitted_server_ids_json = ? WHERE id = ?")
        .bind(db::now())
        .bind(serde_json::to_string(&admitted)?)
        .bind(id)
        .execute(&state.db)
        .await?;
    set_status(state, id, OperationStatus::Running).await?;
    // Re-read roots and servers now that the window is open.
    let ctx = plan::Ctx::new(state, &principal, allow_owner_transfer).await?;

    let mut changes: Vec<Change> = Vec::new();
    let mut local_changes: Vec<LocalChange> = Vec::new();
    let (mut done, mut failed, mut recovery, mut cancelled) = (0, 0, 0, false);
    for (index, action) in actions.iter().enumerate() {
        let index = index as i64;
        if rejected.contains(&index) {
            failed += 1;
            continue;
        }
        let cancel: i64 =
            sqlx::query_scalar("SELECT cancel_requested FROM operations WHERE id = ?")
                .bind(id)
                .fetch_one(&state.db)
                .await?;
        if cancel != 0 {
            cancelled = true;
            sqlx::query("UPDATE operation_items SET status = 'skipped' WHERE operation_id = ? AND status = 'pending'")
                .bind(id)
                .execute(&state.db)
                .await?;
            break;
        }
        set_item(state, id, index, ItemStatus::Running, None, None).await?;
        let plan = match plan::plan(&ctx, action).await {
            Ok(p) => p,
            Err(e) => {
                failed += 1;
                set_item(
                    state,
                    id,
                    index,
                    ItemStatus::Failed,
                    Some((e.code(), e.message())),
                    None,
                )
                .await?;
                continue;
            }
        };
        let mut barrier_failed = false;
        for (server_id, _) in ctx.touched_servers(action).await? {
            if admitted.contains(&server_id)
                || !plan_requires_queue_barrier(state, &server_id, &plan).await?
            {
                continue;
            }
            if let Err(e) = crate::maintenance::acquire(state, &server_id).await {
                failed += 1;
                set_item(
                    state,
                    id,
                    index,
                    ItemStatus::Failed,
                    Some((e.code(), e.message())),
                    None,
                )
                .await?;
                barrier_failed = true;
                break;
            }
            admitted.push(server_id);
            sqlx::query("UPDATE operations SET admitted_server_ids_json = ? WHERE id = ?")
                .bind(serde_json::to_string(&admitted)?)
                .bind(id)
                .execute(&state.db)
                .await?;
        }
        if barrier_failed {
            continue;
        }
        if let Some(b) = plan.blockers.first() {
            failed += 1;
            set_item(
                state,
                id,
                index,
                ItemStatus::Failed,
                Some((&b.code, &b.message)),
                None,
            )
            .await?;
            continue;
        }
        journal(state, id, index, &plan).await?;
        match execute(state, id, &plan).await {
            Ok(()) => {
                done += 1;
                if needs_scan(action, &plan) {
                    for (server, library) in ctx.touched_servers(action).await? {
                        if let Some(library) = library
                            && let Err(e) = crate::watch::scheduler::request_scan(
                                state,
                                &server,
                                &library,
                                &format!("operation {id}"),
                            )
                            .await
                        {
                            tracing::warn!(operation = %id, "cannot schedule a library scan: {}", e.message());
                        }
                    }
                }
                changes.extend(plan.changes.clone());
                local_changes.extend(local_changes_of(action, &plan));
                let result = json!({ "files": plan.files, "asset_id": plan.asset_id, "warnings": plan.warnings });
                set_item(state, id, index, ItemStatus::Done, None, Some(&result)).await?;
            }
            Err(Failure {
                code,
                message,
                recovery: needs,
            }) => {
                let status = if needs {
                    ItemStatus::RecoveryRequired
                } else {
                    ItemStatus::Failed
                };
                if needs {
                    if recovery == 0 {
                        for s in &admitted {
                            crate::maintenance::report_recovery(
                                state,
                                s,
                                &format!("operation {id}: {message}"),
                            )
                            .await?;
                        }
                    }
                    recovery += 1;
                } else {
                    failed += 1;
                }
                set_item(state, id, index, status, Some((&code, &message)), None).await?;
            }
        }
    }
    if let Err(error) = crate::peers::operation_finished(state, id, &local_changes).await {
        tracing::error!(operation = %id, "peer bookkeeping failed: {}", error.message());
        let message = "filesystem changes may have completed, but peer bookkeeping failed; reconcile replica state and events before resolving recovery";
        let index: Option<i64> = sqlx::query_scalar(
            "SELECT idx FROM operation_items WHERE operation_id = ? AND status != 'pending' ORDER BY CASE status WHEN 'done' THEN 0 ELSE 1 END, idx DESC LIMIT 1",
        )
        .bind(id)
        .fetch_optional(&state.db)
        .await?;
        if let Some(index) = index {
            set_item(
                state,
                id,
                index,
                ItemStatus::RecoveryRequired,
                Some(("peer_bookkeeping_failed", message)),
                None,
            )
            .await?;
        }
        if recovery == 0 {
            for server in &admitted {
                crate::maintenance::report_recovery(state, server, message).await?;
            }
        }
        recovery += 1;
    }
    for server in &admitted {
        crate::maintenance::release(state, server).await?;
    }
    let status = if recovery > 0 {
        OperationStatus::RecoveryRequired
    } else if cancelled {
        OperationStatus::Cancelled
    } else if failed == 0 {
        OperationStatus::Completed
    } else if done > 0 {
        OperationStatus::Partial
    } else {
        OperationStatus::Failed
    };
    set_status(state, id, status).await?;
    if !changes.is_empty() {
        state.emit(ServerEvent::OperationCommitted(OperationCommitted {
            operation_id: id.to_owned(),
            changes,
        }));
    }
    Ok(())
}

/// What a completed item changed, for replication to peers. New files (restores, uploads) are
/// found by the next share scan instead.
fn local_changes_of(action: &OperationAction, plan: &Plan) -> Vec<LocalChange> {
    match action {
        OperationAction::Move { .. } | OperationAction::Rename { .. } => plan
            .files
            .iter()
            .filter(|f| !f.copy && f.role == FileRole::Original)
            .filter_map(|f| {
                Some(LocalChange::FileMoved {
                    from: f.from.clone()?,
                    to: f.to.clone()?,
                })
            })
            .collect(),
        OperationAction::Trash { .. } | OperationAction::TrashIfAssetMatches { .. } => plan
            .files
            .iter()
            .filter(|f| !f.copy)
            .filter_map(|f| {
                Some(LocalChange::FileTrashed {
                    at: f.from.clone()?,
                })
            })
            .collect(),
        OperationAction::MoveFolder { source, target } => vec![LocalChange::FolderMoved {
            from: source.clone(),
            to: target.clone(),
        }],
        OperationAction::CreateFolder { target } => {
            vec![LocalChange::FolderCreated { at: target.clone() }]
        }
        OperationAction::TrashFolder { source, .. } => {
            vec![LocalChange::FolderTrashed { at: source.clone() }]
        }
        OperationAction::PublishUpload { .. } | OperationAction::Restore { .. } => plan
            .files
            .iter()
            .filter_map(|f| Some(LocalChange::FileWritten { at: f.to.clone()? }))
            .collect(),
        _ => Vec::new(),
    }
}

fn parse_action(raw: &str) -> AppResult<OperationAction> {
    // `PublishUpload` is internal and never deserialised from clients; stored items may hold it.
    let v: serde_json::Value = serde_json::from_str(raw)?;
    if v["kind"] == "publish_upload" {
        return Ok(OperationAction::PublishUpload {
            upload_id: v["upload_id"].as_str().unwrap_or_default().to_owned(),
        });
    }
    Ok(serde_json::from_value(v)?)
}

async fn fail_all(state: &AppState, id: &str, code: &str, message: &str) -> AppResult<()> {
    sqlx::query("UPDATE operation_items SET status = 'failed', error_code = ?, error_message = ? WHERE operation_id = ? AND status IN ('pending', 'running')")
        .bind(code)
        .bind(message)
        .bind(id)
        .execute(&state.db)
        .await?;
    set_status(state, id, OperationStatus::Failed).await
}

async fn journal(state: &AppState, id: &str, index: i64, plan: &Plan) -> AppResult<()> {
    let steps: Vec<String> = plan
        .steps
        .iter()
        .map(|s| match s {
            Step::Mkdir(p) => format!("mkdir {}", p.display()),
            Step::CheckRevision { path, .. } => format!("check revision {}", path.display()),
            Step::CheckTree { path, .. } => format!("check folder inventory {}", path.display()),
            Step::Fs(m) => format!(
                "{} {} -> {}",
                if m.copy { "copy" } else { "move" },
                m.from.display(),
                m.to.display()
            ),
            Step::Api(a) => format!("immich {:?} {} assets as {}", a.kind, a.ids.len(), a.owner),
            Step::Remove(p) => format!("remove {}", p.display()),
        })
        .collect();
    let db_steps: Vec<String> = plan
        .db
        .values()
        .flatten()
        .map(|d| format!("{d:?}"))
        .collect();
    sqlx::query("UPDATE operation_items SET journal_json = ? WHERE operation_id = ? AND idx = ?")
        .bind(json!({ "steps": steps, "database": db_steps }).to_string())
        .bind(id)
        .bind(index)
        .execute(&state.db)
        .await?;
    Ok(())
}

pub struct Failure {
    pub code: String,
    pub message: String,
    pub recovery: bool,
}

impl Failure {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            recovery: false,
        }
    }
}

enum Undo {
    Rmdir(std::path::PathBuf),
    MoveBack {
        from: std::path::PathBuf,
        to: std::path::PathBuf,
    },
    RemoveCopy(std::path::PathBuf),
    Api(ApiStep),
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())))
}

async fn plan_requires_queue_barrier(
    state: &AppState,
    server_id: &str,
    plan: &Plan,
) -> AppResult<bool> {
    let server = registry::server(state, server_id).await?;
    Ok(sync_plan_requires_queue_barrier(
        server.external_library_move_mode,
        server.external_library_api_capable,
        server_id,
        plan,
    ))
}

fn sync_plan_requires_queue_barrier(
    mode: ImmichMoveMode,
    api_capable: Option<bool>,
    server_id: &str,
    plan: &Plan,
) -> bool {
    if plan
        .db
        .get(server_id)
        .is_some_and(|steps| !steps.is_empty())
    {
        return true;
    }

    plan.steps.iter().any(|step| {
        let Step::Api(api) = step else {
            return false;
        };
        if api.server_id != server_id {
            return false;
        }
        match &api.kind {
            ApiKind::SyncAdd { .. } | ApiKind::SyncRemove { .. } | ApiKind::SyncMove { .. } => {
                !matches!(
                    (mode, api_capable),
                    (ImmichMoveMode::ApiOnly, _) | (ImmichMoveMode::ApiWithDbFallback, Some(true))
                )
            }
            ApiKind::Trash | ApiKind::Restore | ApiKind::ForceDelete => true,
        }
    })
}

fn forbidden(error: &crate::immich::api::ApiError) -> bool {
    matches!(
        error,
        crate::immich::api::ApiError::Status { status: 403, .. }
    )
}

fn all_admins_forbidden(attempted: &[(String, String, String)]) -> crate::immich::api::ApiError {
    crate::immich::api::ApiError::Status {
        status: 403,
        message: format!(
            "Forbidden for all healthy ISM admin accounts ({})",
            attempted
                .iter()
                .map(|(account, user, issue)| format!("{account}:{user} ({issue})"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

async fn admin_candidate_issue(candidate: &registry::AdminApiCandidate) -> Option<String> {
    let identity = match candidate.api.external_library_api_identity().await {
        Ok(identity) => identity,
        Err(error) => return Some(format!("identity check failed: {error}")),
    };
    let mut issues = Vec::new();
    if identity.user_id != candidate.immich_user_id {
        issues.push(format!(
            "key belongs to {}, stored account expects {}",
            identity.user_id, candidate.immich_user_id
        ));
    }
    if !identity.is_admin {
        issues.push("effective Immich user is no longer an administrator".to_owned());
    }
    if !identity
        .permissions
        .iter()
        .any(|permission| permission == "all" || permission == "library.update")
    {
        issues.push(format!("key {} lacks library.update", identity.key_id));
    }
    (!issues.is_empty()).then(|| issues.join("; "))
}

async fn sync_file_add_with_admins(
    state: &AppState,
    server: &registry::Server,
    library_id: &str,
    path: &str,
) -> AppResult<Result<(), crate::immich::api::ApiError>> {
    let candidates = registry::admin_api_candidates(state, server).await?;
    let mut attempted = Vec::new();
    for candidate in candidates {
        tracing::debug!(
            server_id = %server.id,
            account_id = %candidate.account_id,
            account_label = %candidate.label,
            immich_user_id = %candidate.immich_user_id,
            library_id,
            path,
            "trying external-library files/add with Immich admin"
        );
        if let Some(issue) = admin_candidate_issue(&candidate).await {
            attempted.push((
                candidate.account_id.clone(),
                candidate.immich_user_id.clone(),
                issue,
            ));
            continue;
        }
        match candidate.api.sync_library_file_add(library_id, path).await {
            Ok(()) => return Ok(Ok(())),
            Err(error) if forbidden(&error) => {
                attempted.push((
                    candidate.account_id.clone(),
                    candidate.immich_user_id.clone(),
                    "verified identity/permission, endpoint still returned 403".to_owned(),
                ));
                continue;
            }
            Err(error) => return Ok(Err(error)),
        }
    }
    Ok(Err(all_admins_forbidden(&attempted)))
}

async fn sync_file_remove_with_admins(
    state: &AppState,
    server: &registry::Server,
    library_id: &str,
    path: &str,
) -> AppResult<Result<(), crate::immich::api::ApiError>> {
    let candidates = registry::admin_api_candidates(state, server).await?;
    let mut attempted = Vec::new();
    for candidate in candidates {
        tracing::debug!(
            server_id = %server.id,
            account_id = %candidate.account_id,
            account_label = %candidate.label,
            immich_user_id = %candidate.immich_user_id,
            library_id,
            path,
            "trying external-library files/remove with Immich admin"
        );
        if let Some(issue) = admin_candidate_issue(&candidate).await {
            attempted.push((
                candidate.account_id.clone(),
                candidate.immich_user_id.clone(),
                issue,
            ));
            continue;
        }
        match candidate
            .api
            .sync_library_file_remove(library_id, path)
            .await
        {
            Ok(()) => return Ok(Ok(())),
            Err(error) if forbidden(&error) => {
                attempted.push((
                    candidate.account_id.clone(),
                    candidate.immich_user_id.clone(),
                    "verified identity/permission, endpoint still returned 403".to_owned(),
                ));
                continue;
            }
            Err(error) => return Ok(Err(error)),
        }
    }
    Ok(Err(all_admins_forbidden(&attempted)))
}

async fn sync_file_move_with_admins(
    state: &AppState,
    server: &registry::Server,
    source_library_id: &str,
    source_path: &str,
    destination_path: &str,
    destination_library_id: Option<&str>,
    asset_id: Option<&str>,
) -> AppResult<Result<(), crate::immich::api::ApiError>> {
    let candidates = registry::admin_api_candidates(state, server).await?;
    let mut attempted = Vec::new();
    for candidate in candidates {
        tracing::debug!(
            server_id = %server.id,
            account_id = %candidate.account_id,
            account_label = %candidate.label,
            immich_user_id = %candidate.immich_user_id,
            source_library_id,
            destination_library_id,
            source_path,
            destination_path,
            asset_id,
            "trying external-library files/move with Immich admin"
        );
        if let Some(issue) = admin_candidate_issue(&candidate).await {
            attempted.push((
                candidate.account_id.clone(),
                candidate.immich_user_id.clone(),
                issue,
            ));
            continue;
        }
        match candidate
            .api
            .sync_library_file_move(
                source_library_id,
                source_path,
                destination_path,
                destination_library_id,
                asset_id,
            )
            .await
        {
            Ok(()) => return Ok(Ok(())),
            Err(error) if forbidden(&error) => {
                attempted.push((
                    candidate.account_id.clone(),
                    candidate.immich_user_id.clone(),
                    "verified identity/permission, endpoint still returned 403".to_owned(),
                ));
                continue;
            }
            Err(error) => return Ok(Err(error)),
        }
    }
    Ok(Err(all_admins_forbidden(&attempted)))
}

async fn call_api(state: &AppState, step: &ApiStep) -> AppResult<()> {
    let server = registry::server(state, &step.server_id).await?;
    let result = match &step.kind {
        ApiKind::Trash => {
            let api = registry::api_as_user(state, &server, &step.owner).await?;
            api.delete_assets(&step.ids, false).await
        }
        ApiKind::Restore => {
            let api = registry::api_as_user(state, &server, &step.owner).await?;
            api.restore_assets(&step.ids).await
        }
        ApiKind::ForceDelete => {
            let api = registry::api_as_user(state, &server, &step.owner).await?;
            api.delete_assets(&step.ids, true).await
        }
        ApiKind::SyncAdd { library_id, path } => {
            if server.external_library_move_mode == ImmichMoveMode::DbOnly {
                schedule_add_fallback(state, &server, library_id).await
            } else {
                match sync_file_add_with_admins(state, &server, library_id, path).await? {
                    Ok(()) => Ok(()),
                    Err(error)
                        if server.external_library_move_mode
                            == ImmichMoveMode::ApiWithDbFallback
                            && server.external_library_api_capable != Some(true)
                            && error.is_external_library_api_unavailable() =>
                    {
                        schedule_add_fallback(state, &server, library_id).await
                    }
                    Err(error) => Err(error),
                }
            }
        }
        ApiKind::SyncRemove { library_id, path } => {
            if server.external_library_move_mode == ImmichMoveMode::DbOnly {
                Ok(())
            } else {
                match sync_file_remove_with_admins(state, &server, library_id, path).await? {
                    Ok(()) => Ok(()),
                    Err(error)
                        if server.external_library_move_mode
                            == ImmichMoveMode::ApiWithDbFallback
                            && server.external_library_api_capable != Some(true)
                            && error.is_external_library_api_unavailable() =>
                    {
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
        }
        ApiKind::SyncMove {
            source_library_id,
            destination_library_id,
            source_path,
            destination_path,
            asset_id,
            from_owner,
            to_owner,
        } => {
            let fallback = || {
                fallback_sync_move(
                    state,
                    &server,
                    destination_library_id,
                    source_path,
                    destination_path,
                    asset_id,
                    from_owner,
                    to_owner,
                )
            };
            match server.external_library_move_mode {
                ImmichMoveMode::DbOnly => fallback().await,
                ImmichMoveMode::ApiOnly | ImmichMoveMode::ApiWithDbFallback => {
                    match sync_file_move_with_admins(
                        state,
                        &server,
                        source_library_id,
                        source_path,
                        destination_path,
                        destination_library_id.as_deref(),
                        asset_id.as_deref(),
                    )
                    .await?
                    {
                        Ok(()) => Ok(()),
                        Err(error)
                            if should_use_db_fallback(
                                server.external_library_move_mode,
                                server.external_library_api_capable,
                                &error,
                            ) =>
                        {
                            fallback().await
                        }
                        Err(error) => Err(error),
                    }
                }
            }
        }
    };
    result.map_err(|e| AppError::conflict("immich_error", crate::error::redact(&e.to_string())))
}

async fn schedule_add_fallback(
    state: &AppState,
    server: &registry::Server,
    library_id: &str,
) -> Result<(), crate::immich::api::ApiError> {
    crate::watch::scheduler::request_scan(
        state,
        &server.id,
        library_id,
        "external-library file add API unavailable",
    )
    .await
    .map_err(|error| crate::immich::api::ApiError::Decode(error.to_string()))
}

async fn fallback_sync_move(
    state: &AppState,
    server: &registry::Server,
    destination_library_id: &Option<String>,
    source_path: &str,
    destination_path: &str,
    asset_id: &Option<String>,
    from_owner: &Option<String>,
    to_owner: &Option<String>,
) -> Result<(), crate::immich::api::ApiError> {
    if !server.direct_writes_enabled {
        return Err(crate::immich::api::ApiError::Decode(
            "direct database fallback is disabled for this Immich server".to_owned(),
        ));
    }
    let pool = registry::pg_pool(state, server)
        .await
        .map_err(|error| crate::immich::api::ApiError::Decode(error.to_string()))?;
    let mut tx = pool.begin().await.map_err(|error| {
        crate::immich::api::ApiError::Decode(format!(
            "could not start compatibility transaction: {error}"
        ))
    })?;

    if let (Some(destination_library_id), Some(asset_id), Some(from_owner), Some(to_owner)) =
        (destination_library_id, asset_id, from_owner, to_owner)
    {
        pg::move_asset(&mut tx, asset_id, source_path, destination_path)
            .await
            .map_err(|error| {
                crate::immich::api::ApiError::Decode(format!(
                    "compatibility path move failed: {error}"
                ))
            })?;
        pg::transfer_owner(
            &mut tx,
            asset_id,
            from_owner,
            to_owner,
            destination_library_id,
        )
        .await
        .map_err(|error| {
            crate::immich::api::ApiError::Decode(format!(
                "compatibility owner transfer failed: {error}"
            ))
        })?;
    } else if let Some(asset_id) = asset_id {
        pg::move_asset(&mut tx, asset_id, source_path, destination_path)
            .await
            .map_err(|error| {
                crate::immich::api::ApiError::Decode(format!(
                    "compatibility path move failed: {error}"
                ))
            })?;
    } else {
        return Err(crate::immich::api::ApiError::Decode(
            "compatibility move is missing its asset ID".to_owned(),
        ));
    }

    tx.commit().await.map_err(|error| {
        crate::immich::api::ApiError::Decode(format!("compatibility transaction failed: {error}"))
    })?;
    Ok(())
}

fn reverse_api_kind(kind: &ApiKind) -> Option<ApiKind> {
    match kind {
        ApiKind::Trash => Some(ApiKind::Restore),
        ApiKind::Restore => Some(ApiKind::Trash),
        ApiKind::ForceDelete => None,
        ApiKind::SyncAdd { .. } | ApiKind::SyncRemove { .. } => None,
        ApiKind::SyncMove {
            source_library_id,
            destination_library_id,
            source_path,
            destination_path,
            asset_id,
            from_owner,
            to_owner,
        } => Some(ApiKind::SyncMove {
            source_library_id: destination_library_id
                .clone()
                .unwrap_or_else(|| source_library_id.clone()),
            destination_library_id: destination_library_id
                .as_ref()
                .map(|_| source_library_id.clone()),
            source_path: destination_path.clone(),
            destination_path: source_path.clone(),
            asset_id: asset_id.clone(),
            from_owner: to_owner.clone(),
            to_owner: from_owner.clone(),
        }),
    }
}

fn should_use_db_fallback(
    mode: ImmichMoveMode,
    api_capable: Option<bool>,
    error: &crate::immich::api::ApiError,
) -> bool {
    mode == ImmichMoveMode::ApiWithDbFallback
        && api_capable != Some(true)
        && error.is_external_library_api_unavailable()
}

async fn apply(state: &AppState, step: &Step) -> Result<Option<Undo>, Failure> {
    let io = |e: std::io::Error| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Failure::new("destination_exists", e.to_string())
        } else {
            Failure::new("io_error", e.to_string())
        }
    };
    match step {
        Step::CheckTree { path, expected } => {
            let path = path.clone();
            let actual = blocking(move || plan::folder_inventory(&path))
                .await
                .map_err(io)?;
            if &actual != expected {
                return Err(Failure::new(
                    "revision_changed",
                    "folder contents changed during the operation",
                ));
            }
            Ok(None)
        }
        Step::CheckRevision {
            path,
            expected,
            expected_sha256,
        } => {
            let path = path.clone();
            let actual = match blocking(move || files::replacement_fingerprint(&path)).await {
                Ok(actual) => actual,
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                    return Err(Failure::new(
                        "revision_changed",
                        "destination could not be verified",
                    ));
                }
                Err(e) => return Err(io(e)),
            };
            let hash_matches = match (&actual, expected_sha256) {
                (Some((_, actual)), Some(expected)) => actual.eq_ignore_ascii_case(expected),
                (None, None) => true,
                _ => false,
            };
            if actual.as_ref().map(|(revision, _)| revision) != expected.as_ref() || !hash_matches {
                return Err(Failure::new(
                    "revision_changed",
                    "file changed before the operation",
                ));
            }
            Ok(None)
        }
        Step::Mkdir(p) => {
            if p.exists() {
                return Ok(None);
            }
            // Remember the topmost directory we create so undo removes all of them.
            let mut top = p.clone();
            while let Some(parent) = top.parent() {
                if parent.exists() {
                    break;
                }
                top = parent.to_path_buf();
            }
            let path = p.clone();
            blocking(move || fs::create_dir_all(path))
                .await
                .map_err(io)?;
            Ok(Some(Undo::Rmdir(top)))
        }
        Step::Fs(m) => {
            let (from, to) = (m.from.clone(), m.to.clone());
            if m.copy {
                blocking(move || {
                    if to.symlink_metadata().is_ok() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            "destination exists",
                        ));
                    }
                    files::copy_verified(&from, &to)
                })
                .await
                .map_err(io)?;
                Ok(Some(Undo::RemoveCopy(m.to.clone())))
            } else {
                blocking(move || files::move_no_replace(&from, &to))
                    .await
                    .map_err(io)?;
                Ok(Some(Undo::MoveBack {
                    from: m.to.clone(),
                    to: m.from.clone(),
                }))
            }
        }
        Step::Api(a) => {
            call_api(state, a)
                .await
                .map_err(|e| Failure::new(e.code(), e.message().to_owned()))?;
            let undo = reverse_api_kind(&a.kind);
            Ok(undo.map(|kind| Undo::Api(ApiStep { kind, ..a.clone() })))
        }
        Step::Remove(p) => {
            let path = p.clone();
            blocking(move || {
                if path.is_dir() {
                    fs::remove_dir_all(&path)
                } else if path.exists() {
                    fs::remove_file(&path)
                } else {
                    Ok(())
                }
            })
            .await
            .map_err(io)?;
            Ok(None)
        }
    }
}

async fn undo(state: &AppState, undo: Undo) -> Result<(), String> {
    match undo {
        Undo::Rmdir(p) => {
            // Only empty directories we created; anything else stays.
            let _ = blocking(move || {
                for entry in walkdir::WalkDir::new(&p).contents_first(true) {
                    let entry = entry.map_err(std::io::Error::other)?;
                    if entry.file_type().is_dir() {
                        let _ = fs::remove_dir(entry.path());
                    }
                }
                Ok(())
            })
            .await;
            Ok(())
        }
        Undo::MoveBack { from, to } => {
            let label = format!("{} -> {}", from.display(), to.display());
            blocking(move || files::move_no_replace(&from, &to))
                .await
                .map_err(|e| format!("could not move back {label}: {e}"))
        }
        Undo::RemoveCopy(p) => blocking(move || fs::remove_file(p))
            .await
            .map_err(|e| e.to_string()),
        Undo::Api(step) => call_api(state, &step)
            .await
            .map_err(|e| e.message().to_owned()),
    }
}

async fn rollback(state: &AppState, done: Vec<Undo>, failure: Failure) -> Failure {
    let mut problems = Vec::new();
    for u in done.into_iter().rev() {
        if let Err(e) = undo(state, u).await {
            problems.push(e);
        }
    }
    if problems.is_empty() {
        failure
    } else {
        Failure {
            code: "recovery_required".into(),
            message: format!("{}; undo failed: {}", failure.message, problems.join("; ")),
            recovery: true,
        }
    }
}

async fn apply_db(state: &AppState, server_id: &str, steps: &[DbStep]) -> Result<(), Failure> {
    let server = registry::server(state, server_id)
        .await
        .map_err(|e| Failure::new(e.code(), e.message().to_owned()))?;
    let pool = registry::pg_pool(state, &server)
        .await
        .map_err(|e| Failure::new(e.code(), e.message().to_owned()))?;
    let db_err = |e: pg::PgError| {
        let code = match e {
            pg::PgError::Stale(_) => "revision_changed",
            pg::PgError::PathTaken(_) => "destination_exists",
            pg::PgError::TransferBlocked(_) => "owner_transfer_blocked",
            pg::PgError::Db(_) => "database_error",
        };
        Failure::new(code, crate::error::redact(&e.to_string()))
    };
    let mut tx = pool.begin().await.map_err(|e| db_err(e.into()))?;
    for step in steps {
        match step {
            DbStep::MoveSidecar { file_id, from, to } => {
                pg::move_sidecar(&mut tx, file_id, from, to).await
            }
        }
        .map_err(db_err)?;
    }
    tx.commit().await.map_err(|e| db_err(e.into()))?;
    Ok(())
}

/// What the plan's file steps will change, announced to the watcher before touching anything.
fn expected_writes(plan: &Plan) -> Vec<crate::watch::Expected> {
    use crate::watch::Expected;
    let mut out = Vec::new();
    for step in &plan.steps {
        match step {
            Step::Mkdir(p) | Step::Remove(p) => out.push(Expected::tree(p.clone())),
            Step::Fs(m) => out.extend(Expected::moved(&m.from, &m.to)),
            Step::CheckRevision { .. } | Step::CheckTree { .. } | Step::Api(_) => {}
        }
    }
    out
}

/// Whether Immich must scan to see the result: new or restored files, or files moved without
/// a direct database update (no asset yet). Moves of known assets update Immich directly.
fn needs_scan(action: &OperationAction, plan: &Plan) -> bool {
    if plan.steps.iter().any(|step| {
        matches!(
            step,
            Step::Api(ApiStep {
                kind: ApiKind::SyncAdd { .. },
                ..
            })
        )
    }) {
        return false;
    }
    let touches_files = plan
        .steps
        .iter()
        .any(|s| matches!(s, Step::Fs(_) | Step::Mkdir(_)));
    match action {
        OperationAction::PublishUpload { .. } | OperationAction::Restore { .. } => true,
        OperationAction::Move { .. }
        | OperationAction::Rename { .. }
        | OperationAction::MoveFolder { .. } => touches_files && plan.db.is_empty(),
        _ => false,
    }
}

async fn execute(state: &AppState, op_id: &str, plan: &Plan) -> Result<(), Failure> {
    if let Err(e) = crate::watch::expect_writes(state, &expected_writes(plan)).await {
        return Err(Failure::new(
            "internal",
            format!("cannot record the expected file changes: {}", e.message()),
        ));
    }
    let mut done = Vec::new();
    for step in &plan.steps {
        match apply(state, step).await {
            Ok(Some(u)) => done.push(u),
            Ok(None) => {}
            Err(f) if matches!(step, Step::Remove(_)) => return Err(f),
            Err(f) => return Err(rollback(state, done, f).await),
        }
    }
    for (server_id, steps) in &plan.db {
        if let Err(f) = apply_db(state, server_id, steps).await {
            return Err(rollback(state, done, f).await);
        }
    }
    if let Err(e) = bookkeeping(state, op_id, plan).await {
        return Err(Failure {
            code: "recovery_required".into(),
            message: format!("operation bookkeeping failed: {}", e.message()),
            recovery: true,
        });
    }
    Ok(())
}

async fn bookkeeping(state: &AppState, op_id: &str, plan: &Plan) -> AppResult<()> {
    let now = db::now();
    if let Some(t) = &plan.trash_new {
        sqlx::query(
            "INSERT INTO trash (id, root_id, path, quarantine_rel, is_folder, size, trashed_at, operation_id, asset_ids_json, server_id, file_inventory_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&t.id)
        .bind(&t.root_id)
        .bind(&t.path)
        .bind(&t.quarantine_rel)
        .bind(t.is_folder as i64)
        .bind(t.size as i64)
        .bind(&now)
        .bind(op_id)
        .bind(serde_json::to_string(&t.asset_ids)?)
        .bind(&t.server_id)
        .bind(
            t.file_inventory
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        )
        .execute(&state.db)
        .await?;
    }
    if let Some((trash_id, what)) = &plan.trash_done {
        let column = if *what == "restored" {
            "restored_at"
        } else {
            "purged_at"
        };
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE trash SET {column} = ? WHERE id = ?"
        )))
        .bind(&now)
        .bind(trash_id)
        .execute(&state.db)
        .await?;
        if *what == "restored" {
            // Remove the now-empty quarantine folder.
            let row: Option<(String, String)> =
                sqlx::query_as("SELECT root_id, quarantine_rel FROM trash WHERE id = ?")
                    .bind(trash_id)
                    .fetch_optional(&state.db)
                    .await?;
            if let Some((root_id, q)) = row
                && let Ok(root) = files::root(&state.db, &root_id).await
            {
                let path = root.base().join(q);
                let _ = blocking(move || {
                    for entry in walkdir::WalkDir::new(&path).contents_first(true) {
                        let entry = entry.map_err(std::io::Error::other)?;
                        if entry.file_type().is_dir() {
                            let _ = fs::remove_dir(entry.path());
                        }
                    }
                    Ok(())
                })
                .await;
            }
        }
    }
    if let Some(upload_id) = &plan.upload_done {
        sqlx::query("UPDATE uploads SET status = 'committed' WHERE id = ?")
            .bind(upload_id)
            .execute(&state.db)
            .await?;
        super::uploads::cleanup(state, upload_id).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use ism_core::dto::{Location, PlannedFile};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path},
    };

    #[tokio::test]
    async fn sync_move_retries_a_forbidden_admin_key_without_changing_asset_identity() {
        let server = MockServer::start().await;
        for (key, user_id, status) in [("stale", "user-1", 403), ("valid", "user-2", 204)] {
            Mock::given(method("GET"))
                .and(path("/api/api-keys/me"))
                .and(header("x-api-key", key))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "id": format!("key-{key}"),
                    "permissions": ["library.update"]
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/users/me"))
                .and(header("x-api-key", key))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "id": user_id,
                    "isAdmin": true
                })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/libraries/source-library/files/move"))
                .and(header("x-api-key", key))
                .and(body_json(json!({
                    "sourcePath": "/library/source/photo.jpg",
                    "destinationPath": "/library/destination/photo.jpg",
                    "destinationLibraryId": "destination-library",
                    "assetId": "asset-1"
                })))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
        }

        let dir = tempfile::tempdir().unwrap();
        let pool = crate::db::connect(dir.path()).await.unwrap();
        let secrets = crate::crypto::SecretBox::open_or_create(dir.path()).unwrap();
        let state = AppState::new(crate::config::Config::for_tests(dir.path()), pool, secrets)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO immich_servers (id, name, url, maintenance_json, created_at) VALUES \
             ('server-1', 'Immich', ?, ?, ?)",
        )
        .bind(server.uri())
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
        for (account_id, user_id, key) in [
            ("account-1", "user-1", "stale"),
            ("account-2", "user-2", "valid"),
        ] {
            sqlx::query(
                "INSERT INTO immich_accounts (id, server_id, label, email, immich_user_id, \
                 is_admin, api_key_enc, status, created_at) VALUES (?, 'server-1', ?, ?, ?, 1, ?, 'ok', ?)",
            )
            .bind(account_id)
            .bind(account_id)
            .bind(format!("{account_id}@example.test"))
            .bind(user_id)
            .bind(
                state
                    .secrets
                    .encrypt(crate::immich::registry::API_KEY_PURPOSE, key)
                    .unwrap(),
            )
            .bind(crate::db::now())
            .execute(&state.db)
            .await
            .unwrap();
        }
        let immich_server = registry::server(&state, "server-1").await.unwrap();

        let result = sync_file_move_with_admins(
            &state,
            &immich_server,
            "source-library",
            "/library/source/photo.jpg",
            "/library/destination/photo.jpg",
            Some("destination-library"),
            Some("asset-1"),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap(), ());

        let move_requests = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/libraries/source-library/files/move")
            .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(move_requests.len(), 2);
        assert!(
            move_requests
                .iter()
                .all(|body| body["assetId"] == "asset-1")
        );
        assert!(
            move_requests
                .iter()
                .all(|body| body["destinationLibraryId"] == "destination-library")
        );
    }

    fn api_sync_plan(kind: ApiKind) -> Plan {
        Plan {
            steps: vec![Step::Api(ApiStep {
                kind,
                server_id: "server-1".to_owned(),
                owner: "owner-1".to_owned(),
                ids: Vec::new(),
            })],
            ..Plan::default()
        }
    }

    #[test]
    fn queue_barrier_classification_keeps_legacy_and_database_work_conservative() {
        let add = api_sync_plan(ApiKind::SyncAdd {
            library_id: "library-1".to_owned(),
            path: "/library/photo.jpg".to_owned(),
        });
        assert!(!sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiOnly,
            None,
            "server-1",
            &add
        ));
        assert!(!sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiWithDbFallback,
            Some(true),
            "server-1",
            &add
        ));
        assert!(sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiWithDbFallback,
            Some(false),
            "server-1",
            &add
        ));
        assert!(sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiWithDbFallback,
            None,
            "server-1",
            &add
        ));
        assert!(sync_plan_requires_queue_barrier(
            ImmichMoveMode::DbOnly,
            Some(true),
            "server-1",
            &add
        ));

        let legacy = api_sync_plan(ApiKind::Trash);
        assert!(sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiOnly,
            Some(true),
            "server-1",
            &legacy
        ));

        let mut with_sidecar_db = add;
        with_sidecar_db.db.insert(
            "server-1".to_owned(),
            vec![DbStep::MoveSidecar {
                file_id: "sidecar-1".to_owned(),
                from: "/library/photo.xmp".to_owned(),
                to: "/library/moved/photo.xmp".to_owned(),
            }],
        );
        assert!(sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiOnly,
            Some(true),
            "server-1",
            &with_sidecar_db
        ));
    }

    #[test]
    fn remove_sync_only_skips_barrier_when_api_native_is_authoritative() {
        let remove = api_sync_plan(ApiKind::SyncRemove {
            library_id: "library-1".to_owned(),
            path: "/library/photo.jpg".to_owned(),
        });
        assert!(!sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiOnly,
            Some(false),
            "server-1",
            &remove
        ));
        assert!(!sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiWithDbFallback,
            Some(true),
            "server-1",
            &remove
        ));
        assert!(sync_plan_requires_queue_barrier(
            ImmichMoveMode::ApiWithDbFallback,
            Some(false),
            "server-1",
            &remove
        ));
    }

    #[test]
    fn move_mode_controls_database_fallback() {
        let unavailable = crate::immich::api::ApiError::Status {
            status: 404,
            message: "Cannot POST /api/libraries/id/files/move".to_owned(),
        };
        let conflict = crate::immich::api::ApiError::Status {
            status: 409,
            message: "Destination already has an asset".to_owned(),
        };
        assert!(!should_use_db_fallback(
            ImmichMoveMode::ApiOnly,
            Some(false),
            &unavailable
        ));
        assert!(should_use_db_fallback(
            ImmichMoveMode::ApiWithDbFallback,
            Some(false),
            &unavailable
        ));
        assert!(!should_use_db_fallback(
            ImmichMoveMode::ApiWithDbFallback,
            Some(true),
            &unavailable
        ));
        assert!(!should_use_db_fallback(
            ImmichMoveMode::ApiWithDbFallback,
            None,
            &conflict
        ));
    }

    #[test]
    fn explicit_add_sync_replaces_the_publish_full_scan() {
        let action = OperationAction::PublishUpload {
            upload_id: "upload-1".to_owned(),
        };
        let mut plan = Plan {
            steps: vec![Step::Fs(plan::FsMove {
                from: "staging".into(),
                to: "photo.jpg".into(),
                copy: false,
            })],
            ..Plan::default()
        };
        assert!(needs_scan(&action, &plan));

        plan.steps.push(Step::Api(ApiStep {
            kind: ApiKind::SyncAdd {
                library_id: "library-1".to_owned(),
                path: "/library/photo.jpg".to_owned(),
            },
            server_id: "server-1".to_owned(),
            owner: "owner-1".to_owned(),
            ids: Vec::new(),
        }));
        assert!(!needs_scan(&action, &plan));
    }

    #[test]
    fn file_sync_steps_are_terminal_and_not_reversed() {
        for kind in [
            ApiKind::SyncAdd {
                library_id: "library-1".to_owned(),
                path: "/library/photo.jpg".to_owned(),
            },
            ApiKind::SyncRemove {
                library_id: "library-1".to_owned(),
                path: "/library/photo.jpg".to_owned(),
            },
        ] {
            assert_eq!(reverse_api_kind(&kind), None);
        }
    }

    #[test]
    fn managed_move_emits_only_the_original_peer_event() {
        let original_from = Location {
            root_id: "root-1".to_owned(),
            path: "photo.jpg".to_owned(),
        };
        let original_to = Location {
            root_id: "root-1".to_owned(),
            path: "edited/photo.jpg".to_owned(),
        };
        let plan = Plan {
            files: vec![
                PlannedFile {
                    from: Some(original_from.clone()),
                    to: Some(original_to.clone()),
                    role: FileRole::Original,
                    copy: false,
                },
                PlannedFile {
                    from: Some(Location {
                        root_id: "root-1".to_owned(),
                        path: "photo.jpg.xmp".to_owned(),
                    }),
                    to: Some(Location {
                        root_id: "root-1".to_owned(),
                        path: "edited/photo.jpg.xmp".to_owned(),
                    }),
                    role: FileRole::Sidecar,
                    copy: false,
                },
            ],
            ..Plan::default()
        };
        let action = OperationAction::Move {
            source: original_from.clone(),
            target: original_to.clone(),
            expected_revision: None,
        };

        let changes = local_changes_of(&action, &plan);
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], LocalChange::FileMoved { from, to } if from == &original_from && to == &original_to)
        );
    }

    #[test]
    fn sync_move_undo_reverses_paths_libraries_and_owners() {
        let forward = ApiKind::SyncMove {
            source_library_id: "library-a".to_owned(),
            destination_library_id: Some("library-b".to_owned()),
            source_path: "/mnt/a/photo.jpg".to_owned(),
            destination_path: "/mnt/b/photo.jpg".to_owned(),
            asset_id: Some("asset-1".to_owned()),
            from_owner: Some("owner-a".to_owned()),
            to_owner: Some("owner-b".to_owned()),
        };
        let Some(ApiKind::SyncMove {
            source_library_id,
            destination_library_id,
            source_path,
            destination_path,
            asset_id,
            from_owner,
            to_owner,
        }) = reverse_api_kind(&forward)
        else {
            panic!("sync move should create undo");
        };
        assert_eq!(source_library_id, "library-b");
        assert_eq!(destination_library_id.as_deref(), Some("library-a"));
        assert_eq!(source_path, "/mnt/b/photo.jpg");
        assert_eq!(destination_path, "/mnt/a/photo.jpg");
        assert_eq!(asset_id.as_deref(), Some("asset-1"));
        assert_eq!(from_owner.as_deref(), Some("owner-b"));
        assert_eq!(to_owner.as_deref(), Some("owner-a"));
    }

    #[tokio::test]
    async fn folder_inventory_failure_rolls_back_the_move() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let target = dir.path().join("quarantine");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("photo.jpg"), b"original").unwrap();
        let expected = plan::folder_inventory(&source).unwrap();
        fs::write(source.join("photo.jpg"), b"changed").unwrap();
        let state = AppState::new(
            crate::config::Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let plan = Plan {
            steps: vec![
                Step::Fs(plan::FsMove {
                    from: source.clone(),
                    to: target.clone(),
                    copy: false,
                }),
                Step::CheckTree {
                    path: target.clone(),
                    expected,
                },
            ],
            ..Plan::default()
        };
        let Err(failure) = execute(&state, "unused", &plan).await else {
            panic!("changed folder should fail verification");
        };
        assert_eq!(failure.code, "revision_changed");
        assert!(!failure.recovery);
        assert_eq!(fs::read(source.join("photo.jpg")).unwrap(), b"changed");
        assert!(!target.exists());
    }
}
