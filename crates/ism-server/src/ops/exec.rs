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
                "SELECT id FROM operations WHERE status = 'queued'
                 ORDER BY CASE WHEN source = 'opm' THEN 0 ELSE 1 END, created_at, rowid LIMIT 1",
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
    if status.is_terminal() {
        state.sync_wake.notify_waiters();
    }
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
    let item = super::load_item(state, id, index).await?;
    super::record(state, id, "item", &serde_json::to_value(item)?).await?;
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
    state.emit(ServerEvent::OperationUpdated {
        operation_id: id.to_owned(),
    });
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
    let ctx = plan::Ctx::for_operation(state, &principal, allow_owner_transfer, id).await?;

    // Peer and conflict-recovery mutations must use the library barrier, including API-native
    // plans. Other operations retain their existing queue-scan admission policy.
    let sync_managed = matches!(principal, Principal::Peer { .. })
        || sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pending_peer_ops WHERE operation_id=?)",
        )
        .bind(id)
        .fetch_one(&state.db)
        .await?;
    let mut servers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, action) in actions.iter().enumerate() {
        if rejected.contains(&(index as i64)) {
            continue;
        }
        let touched = ctx.touched_servers(action).await?;
        let preliminary_plan = plan::plan(&ctx, action).await;
        for (server_id, library) in touched {
            let requires_barrier = sync_managed
                || match &preliminary_plan {
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
    let ctx = plan::Ctx::for_operation(state, &principal, allow_owner_transfer, id).await?;

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
                || !sync_managed && !plan_requires_queue_barrier(state, &server_id, &plan).await?
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
        let result = match execute(state, id, index, &plan).await {
            Ok(()) if needs_scan(action, &plan) => schedule_imports(state, id, &plan)
                .await
                .map_err(|error| Failure {
                    code: "recovery_required".into(),
                    message: format!(
                        "cannot schedule Immich synchronization: {}",
                        error.message()
                    ),
                    recovery: true,
                }),
            result => result,
        };
        match result {
            Ok(()) => {
                done += 1;
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

pub(crate) fn parse_action(raw: &str) -> AppResult<OperationAction> {
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
    let mut verification = Vec::new();
    for file in &plan.files {
        let Some(to) = &file.to else { continue };
        let root = files::root(&state.db, &to.root_id).await?;
        root.ensure_available()?;
        let destination = root.abs(&files::parse_rel(&to.path)?)?;
        let path = if let Some(from) = &file.from {
            let source = files::root(&state.db, &from.root_id).await?;
            source.ensure_available()?;
            source.abs(&files::parse_rel(&from.path)?)?
        } else if let Some(source) = plan.steps.iter().find_map(|step| match step {
            Step::Fs(m) if m.to == destination => Some(m.from.clone()),
            _ => None,
        }) {
            source
        } else {
            continue;
        };
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_file() {
            verification.push(json!({
                "location": to,
                "size": metadata.len(),
                "sha256": files::sha256_file(&path)?,
            }));
        } else if metadata.is_dir() {
            verification.push(json!({
                "location": to,
                "inventory": plan::folder_inventory(&path)?,
            }));
        } else {
            return Err(AppError::invalid_location(
                "cannot journal an unsafe file type",
            ));
        }
    }
    let steps: Vec<String> = plan
        .steps
        .iter()
        .map(|s| match s {
            Step::Mkdir(p) => format!("mkdir {}", p.display()),
            Step::CheckRevision { path, .. } => format!("check revision {}", path.display()),
            Step::CheckTree { path, .. } => format!("check folder inventory {}", path.display()),
            Step::CheckHash { path, .. } => format!("check sha256 {}", path.display()),
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
    sqlx::query(
        "UPDATE operation_items SET journal_json = json_patch(?,json_object(
            'folder_restore_manifest',json(json_extract(journal_json,'$.folder_restore_manifest')),
            'folder_prerequisites',json(json_extract(journal_json,'$.folder_prerequisites')),
            'recovery_execution',json(json_extract(journal_json,'$.recovery_execution'))))
         WHERE operation_id = ? AND idx = ?",
    )
    .bind(
        json!({
            "steps": steps, "database": db_steps, "verification": verification,
            "api_effects": plan.steps.iter().enumerate().filter_map(|(index,step)| {
                if let Step::Api(api)=step {Some((index,api))} else {None}
            }).collect::<Vec<_>>(),
            "db_effects": plan.db.iter().enumerate().map(|(index,(server,steps))|
                (plan.steps.len()+index,server,steps)).collect::<Vec<_>>(),
            "trash_new":plan.trash_new,"trash_done":plan.trash_done,
            "upload_done":plan.upload_done,"asset_id":plan.asset_id,
            "execution_steps": plan.steps,
        })
        .to_string(),
    )
    .bind(id)
    .bind(index)
    .execute(&state.db)
    .await?;
    let progress = ism_core::dto::OperationTaskProgress {
        files: plan.files.clone(),
        steps: steps
            .into_iter()
            .chain(
                plan.db
                    .keys()
                    .map(|server| format!("Update Immich database: {server}")),
            )
            .chain(std::iter::once("Record changes".into()))
            .map(|label| ism_core::dto::OperationStep {
                label,
                status: "pending".into(),
            })
            .collect(),
    };
    sqlx::query("UPDATE operation_items SET progress_json = ? WHERE operation_id = ? AND idx = ?")
        .bind(serde_json::to_string(&progress)?)
        .bind(id)
        .bind(index)
        .execute(&state.db)
        .await?;
    Ok(())
}

/// Finishes the original operation using retained content evidence, without re-planning it.
pub async fn recover_bookkeeping(state: &AppState, id: &str) -> AppResult<bool> {
    let _guard = state.replication_operation_lock.lock().await;
    recover_bookkeeping_locked(state, id).await
}

async fn recover_bookkeeping_locked(state: &AppState, id: &str) -> AppResult<bool> {
    let (operation, _) = super::load(state, id).await?;
    if operation.status == OperationStatus::Completed {
        return Ok(true);
    }
    if operation.status != OperationStatus::RecoveryRequired {
        return Err(AppError::conflict(
            "recovery_changed",
            "operation no longer needs recovery",
        ));
    }
    type RecoveryItem = (i64, String, String, Option<String>, Option<String>);
    let rows: Vec<RecoveryItem> = sqlx::query_as(
        "SELECT idx,action_json,status,journal_json,progress_json FROM operation_items
         WHERE operation_id=? ORDER BY idx",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    let mut changes = Vec::new();
    for (item, action, status, journal, progress) in rows {
        if !matches!(status.as_str(), "done" | "recovery_required") {
            return Err(AppError::conflict(
                "recovery_evidence_missing",
                "an item did not complete",
            ));
        }
        let progress: ism_core::dto::OperationTaskProgress =
            serde_json::from_str(progress.as_deref().ok_or_else(|| {
                AppError::conflict(
                    "recovery_evidence_missing",
                    "completed-step evidence is missing",
                )
            })?)?;
        let journal: serde_json::Value =
            serde_json::from_str(journal.as_deref().ok_or_else(|| {
                AppError::conflict(
                    "recovery_evidence_missing",
                    "verification journal is missing",
                )
            })?)?;
        let api_effects: Vec<(usize, ApiStep)> = journal
            .get("api_effects")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        let db_effects: Vec<(usize, String, Vec<DbStep>)> = journal
            .get("db_effects")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        let action = parse_action(&action)?;
        if journal.get("api_effects").is_none()
            && (journal["database"]
                .as_array()
                .is_none_or(|steps| !steps.is_empty())
                || journal["steps"].as_array().is_none_or(|steps| {
                    steps
                        .iter()
                        .any(|step| step.as_str().is_none_or(|step| step.starts_with("immich ")))
                }))
        {
            return Err(AppError::conflict(
                "recovery_immich_verification",
                "Immich effects must be verified before completing recovery",
            ));
        }
        let checks = journal["verification"].as_array().ok_or_else(|| {
            AppError::conflict(
                "recovery_evidence_missing",
                "trusted file inventory is missing",
            )
        })?;
        if checks.is_empty() {
            match &action {
                OperationAction::CreateFolder { target } => {
                    let (root, rel, _) = files::locate(&state.db, target).await?;
                    if !fs::symlink_metadata(root.abs(&rel)?)?.is_dir() {
                        return Err(AppError::conflict(
                            "recovery_changed",
                            "folder is not verified",
                        ));
                    }
                }
                _ => {
                    return Err(AppError::conflict(
                        "recovery_evidence_missing",
                        "no completed content inventory was retained",
                    ));
                }
            }
        }
        verify_folder_prerequisites(state, id, &journal, false).await?;
        let recovered_steps = recover_moves(state, id, item, &action, &journal, &progress).await?;
        if progress.steps.is_empty()
            || progress.steps[..progress.steps.len() - 1]
                .iter()
                .enumerate()
                .any(|(index, step)| {
                    step.status != "done"
                        && !api_effects.iter().any(|(i, _)| *i == index)
                        && !db_effects.iter().any(|(i, ..)| *i == index)
                        && !recovered_steps.contains(&index)
                })
        {
            return Err(AppError::conflict(
                "recovery_evidence_missing",
                "interrupted execution requires more than bookkeeping recovery",
            ));
        }
        for check in checks {
            let location: ism_core::dto::Location =
                serde_json::from_value(check["location"].clone())?;
            let root = files::root(&state.db, &location.root_id).await?;
            root.ensure_available()?;
            let path = root.abs(&files::parse_rel(&location.path)?)?;
            if let Some(expected) = check.get("inventory") {
                let expected: Vec<plan::TrashEntry> = serde_json::from_value(expected.clone())?;
                if plan::folder_inventory(&path)? != expected {
                    return Err(AppError::conflict(
                        "recovery_changed",
                        "folder inventory changed",
                    ));
                }
            } else {
                let fingerprint = files::replacement_fingerprint(&path)?;
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file()
                    || Some(metadata.len()) != check["size"].as_u64()
                    || fingerprint.as_ref().map(|(_, sha)| sha.as_str()) != check["sha256"].as_str()
                {
                    return Err(AppError::conflict(
                        "recovery_changed",
                        "file content changed",
                    ));
                }
            }
        }
        for (index, step) in &api_effects {
            if !verify_api_effect(state, step).await? {
                call_api(state, step).await?;
                if !verify_api_effect(state, step).await? {
                    return Err(AppError::conflict(
                        "recovery_immich_verification",
                        "Immich is still applying the recovered change",
                    ));
                }
            }
            step_progress(state, id, item, *index, "done").await;
        }
        for (index, server, steps) in &db_effects {
            if !verify_db_effects(state, server, steps).await? {
                apply_db(state, server, steps).await.map_err(|e| {
                    AppError::conflict(
                        "recovery_immich_failed",
                        format!("{}: {}", e.code, e.message),
                    )
                })?;
                if !verify_db_effects(state, server, steps).await? {
                    return Err(AppError::conflict(
                        "recovery_immich_verification",
                        "Immich database effects are not verified",
                    ));
                }
            }
            step_progress(state, id, item, *index, "done").await;
        }
        let trash_done: Option<(String, String)> = journal
            .get("trash_done")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .flatten();
        let trash_done = trash_done
            .map(|(id, kind)| match kind.as_str() {
                "restored" => Ok((id, "restored")),
                "purged" => Ok((id, "purged")),
                _ => Err(AppError::internal("invalid journal trash outcome")),
            })
            .transpose()?;
        let publication = if let OperationAction::PublishUpload { upload_id } = &action {
            Some(authenticated_publish(state, id, item, upload_id, &journal, &progress).await?)
        } else {
            None
        };
        let mut plan = Plan {
            files: progress.files,
            trash_new: journal
                .get("trash_new")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .flatten(),
            trash_done,
            upload_done: journal
                .get("upload_done")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .flatten(),
            asset_id: journal
                .get("asset_id")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .flatten(),
            ..Default::default()
        };
        if publication.is_some() {
            plan.upload_done = None;
        }
        bookkeeping(state, id, &plan).await?;
        if let Some(publication) = &publication {
            finalize_recovered_publish(state, id, item, publication).await?;
        }
        step_progress(state, id, item, progress.steps.len() - 1, "done").await;
        changes.extend(local_changes_of(&action, &plan));
    }
    verify_recovery_completion(state, id).await?;
    // Replays are idempotent at the peer-bookkeeping transaction boundary.
    sqlx::query("UPDATE operation_items SET status='done' WHERE operation_id=? AND status='recovery_required'")
        .bind(id).execute(&state.db).await?;
    if let Err(e) = crate::peers::operation_finished(state, id, &changes).await {
        sqlx::query("UPDATE operation_items SET status='recovery_required' WHERE operation_id=? AND error_code='peer_bookkeeping_failed'")
            .bind(id).execute(&state.db).await?;
        return Err(e);
    }
    verify_recovery_completion(state, id).await?;
    crate::maintenance::resolve_operation_recovery(state, id).await?;
    sqlx::query(
        "UPDATE operation_items SET error_code=NULL,error_message=NULL WHERE operation_id=?",
    )
    .bind(id)
    .execute(&state.db)
    .await?;
    set_status(state, id, OperationStatus::Completed).await?;
    Ok(true)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum RecoveryContent {
    File { size: u64, sha256: String },
    Folder(Vec<plan::TrashEntry>),
}

impl RecoveryContent {
    fn read(path: &std::path::Path) -> AppResult<Option<Self>> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if metadata.is_dir() {
            return Ok(Some(Self::Folder(plan::folder_inventory(path)?)));
        }
        if !metadata.is_file() {
            return Err(recovery_changed(
                "symlinks and special files are not recoverable",
            ));
        }
        let (revision, sha256) = files::replacement_fingerprint(path)?
            .ok_or_else(|| recovery_changed("file disappeared during recovery"))?;
        if revision != files::revision(&metadata)
            || files::revision(&fs::symlink_metadata(path)?) != revision
        {
            return Err(recovery_changed(
                "file changed during recovery verification",
            ));
        }
        Ok(Some(Self::File {
            size: metadata.len(),
            sha256,
        }))
    }

    fn from_check(check: &serde_json::Value) -> AppResult<Self> {
        if let Some(inventory) = check.get("inventory") {
            return Ok(Self::Folder(serde_json::from_value(inventory.clone())?));
        }
        Ok(Self::File {
            size: check["size"]
                .as_u64()
                .ok_or_else(|| recovery_changed("journal file size is missing"))?,
            sha256: check["sha256"]
                .as_str()
                .ok_or_else(|| recovery_changed("journal checksum is missing"))?
                .to_owned(),
        })
    }

    fn subset_of(&self, complete: &Self) -> bool {
        match (self, complete) {
            (Self::Folder(actual), Self::Folder(expected)) => {
                actual.iter().all(|entry| expected.contains(entry))
            }
            _ => self == complete,
        }
    }

    fn trash_inventory(&self, name: &str) -> Vec<plan::TrashEntry> {
        match self {
            Self::Folder(entries) => entries.clone(),
            Self::File { sha256, .. } => vec![plan::TrashEntry {
                name: name.to_owned(),
                sha256: Some(sha256.clone()),
            }],
        }
    }
}

fn recovery_changed(message: &str) -> AppError {
    AppError::conflict("recovery_changed", message)
}

async fn verify_recovery_completion(state: &AppState, id: &str) -> AppResult<()> {
    let items: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT idx,action_json,journal_json,progress_json FROM operation_items WHERE operation_id=?",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    for (item, action, journal, progress) in items {
        let journal: serde_json::Value = serde_json::from_str(&journal)?;
        let progress: ism_core::dto::OperationTaskProgress = serde_json::from_str(&progress)?;
        let action = parse_action(&action)?;
        verify_folder_prerequisites(state, id, &journal, true).await?;
        if let OperationAction::PublishUpload { upload_id } = &action {
            let publication =
                authenticated_publish(state, id, item, upload_id, &journal, &progress).await?;
            verify_recovered_publish(state, id, item, &publication).await?;
        }
        for check in journal["verification"]
            .as_array()
            .ok_or_else(|| recovery_changed("final inventory is missing"))?
        {
            let location = serde_json::from_value(check["location"].clone())?;
            let (_, _, path) = files::locate(&state.db, &location).await?;
            if RecoveryContent::read(&path)?.as_ref() != Some(&RecoveryContent::from_check(check)?)
            {
                return Err(recovery_changed(
                    "final destination content is not verified",
                ));
            }
        }
        if matches!(
            action,
            OperationAction::Move { .. }
                | OperationAction::Rename { .. }
                | OperationAction::MoveFolder { .. }
        ) {
            for file in progress.files {
                if let Some(from) = file.from {
                    let (_, _, path) = files::locate(&state.db, &from).await?;
                    let content = RecoveryContent::read(&path)?;
                    if !file.copy && content.is_some() {
                        return Err(recovery_changed("final move source is not absent"));
                    }
                    if file.copy {
                        let check = journal["verification"]
                            .as_array()
                            .and_then(|checks| {
                                checks.iter().find(|check| {
                                    serde_json::from_value::<ism_core::dto::Location>(
                                        check["location"].clone(),
                                    )
                                    .is_ok_and(|location| file.to.as_ref() == Some(&location))
                                })
                            })
                            .ok_or_else(|| recovery_changed("shared companion proof is missing"))?;
                        if content.as_ref() != Some(&RecoveryContent::from_check(check)?) {
                            return Err(recovery_changed("shared companion source changed"));
                        }
                    }
                }
            }
        }
        let archives: Vec<RecoveryArchive> = journal
            .get("recovery_execution")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        for archive in archives {
            if !archive.done {
                return Err(recovery_changed("recovery trash was not finalized"));
            }
            recover_archive(state, id, &archive).await?;
        }
        let api_effects: Vec<(usize, ApiStep)> = journal
            .get("api_effects")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        for (_, step) in api_effects {
            if !verify_api_effect(state, &step).await? {
                return Err(recovery_changed("final Immich effect is not verified"));
            }
        }
        let db_effects: Vec<(usize, String, Vec<DbStep>)> = journal
            .get("db_effects")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        for (_, server, steps) in db_effects {
            if !verify_db_effects(state, &server, &steps).await? {
                return Err(recovery_changed(
                    "final Immich database effect is not verified",
                ));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RecoveryArchive {
    movement: plan::FsMove,
    record: plan::TrashRecord,
    content: RecoveryContent,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    source_upload: Option<String>,
    #[serde(default)]
    source_folder_prerequisite: bool,
}

struct RecoveryMove {
    index: usize,
    movement: plan::FsMove,
    expected: RecoveryContent,
    source: files::Root,
    destination: files::Root,
    upload_id: Option<String>,
}

struct AuthenticatedPublish {
    upload_id: String,
    principal: String,
    root: files::Root,
    target: ism_core::dto::Location,
    staged: std::path::PathBuf,
    expected: RecoveryContent,
    role: FileRole,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct FrozenFolderRestore {
    root_id: String,
    source: String,
    target: String,
    directories: Vec<String>,
    files: Vec<(String, String, u64)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FolderPrerequisite {
    upload_id: String,
    target: ism_core::dto::Location,
    staged: std::path::PathBuf,
    expected: RecoveryContent,
    role: FileRole,
    asset_id: Option<String>,
    done: bool,
}

async fn folder_parent(
    state: &AppState,
    id: &str,
) -> AppResult<(
    Principal,
    OperationAction,
    OperationStatus,
    serde_json::Value,
)> {
    let (operation, key) = super::load(state, id).await?;
    if operation.items.len() != 1 || operation.source != "peer" || !key.starts_with("peer:") {
        return Err(recovery_changed(
            "folder continuation requires one authenticated peer action",
        ));
    }
    let principal = principal_from_key(state, &key).await?;
    let Principal::Peer { peer_id } = &principal else {
        return Err(recovery_changed(
            "folder continuation has no peer principal",
        ));
    };
    let known_peer: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM peers WHERE id=?)")
        .bind(peer_id)
        .fetch_one(&state.db)
        .await?;
    if !known_peer {
        return Err(recovery_changed("folder continuation peer is unavailable"));
    }
    let action: String = sqlx::query_scalar(
        "SELECT action_json FROM operation_items WHERE operation_id=? AND idx=0",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await?;
    let action = parse_action(&action)?;
    if !matches!(action, OperationAction::MoveFolder { .. })
        || !matches!(
            operation.status,
            OperationStatus::Failed
                | OperationStatus::RecoveryRequired
                | OperationStatus::Completed
        )
    {
        return Err(recovery_changed(
            "original folder operation is not recoverable",
        ));
    }
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT journal_json FROM operation_items WHERE operation_id=? AND idx=0",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await?;
    Ok((
        principal,
        action,
        operation.status,
        raw.map(|raw| serde_json::from_str(&raw))
            .transpose()?
            .unwrap_or_else(|| json!({})),
    ))
}

impl FrozenFolderRestore {
    fn inventory(&self) -> AppResult<Vec<plan::TrashEntry>> {
        let source = files::parse_rel(&self.source)?;
        let target = files::parse_rel(&self.target)?;
        if source.is_root() || target.is_root() || target.starts_with(&source) {
            return Err(recovery_changed(
                "folder restoration has an unsafe original scope",
            ));
        }
        let mut entries = BTreeMap::from([(String::new(), None)]);
        for directory in &self.directories {
            let mut directory = Some(files::parse_rel(directory)?);
            while let Some(rel) = directory {
                let name = rel
                    .rebase(&source, &ism_core::paths::RelPath::root())
                    .ok_or_else(|| {
                        recovery_changed("restoration directory is outside the original source")
                    })?;
                entries.insert(name.to_string(), None);
                if rel == source {
                    break;
                }
                directory = rel.parent();
            }
        }
        for (path, hash, _) in &self.files {
            let rel = files::parse_rel(path)?;
            let name = rel
                .rebase(&source, &ism_core::paths::RelPath::root())
                .filter(|rel| !rel.is_root())
                .ok_or_else(|| {
                    recovery_changed("restoration file is outside the original source")
                })?;
            if hash.len() != 64
                || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                || entries
                    .insert(name.to_string(), Some(hash.to_ascii_lowercase()))
                    .is_some()
            {
                return Err(recovery_changed(
                    "restoration inventory is ambiguous or lacks a valid checksum",
                ));
            }
            let mut parent = rel.parent();
            while let Some(directory) = parent {
                let name = directory
                    .rebase(&source, &ism_core::paths::RelPath::root())
                    .ok_or_else(|| {
                        recovery_changed("restoration parent is outside the original source")
                    })?;
                if entries.entry(name.to_string()).or_insert(None).is_some() {
                    return Err(recovery_changed("restoration file overlaps a directory"));
                }
                if directory == source {
                    break;
                }
                parent = directory.parent();
            }
        }
        Ok(entries
            .into_iter()
            .map(|(name, sha256)| plan::TrashEntry { name, sha256 })
            .collect())
    }
}

async fn freeze_folder_restore(state: &AppState, id: &str) -> AppResult<FrozenFolderRestore> {
    let remote: crate::peers::admin::VerifiedFolderRestore =
        crate::peers::admin::verified_folder_restore(state, id).await?;
    let mut frozen = FrozenFolderRestore {
        root_id: remote.root_id,
        source: remote.source.to_string(),
        target: remote.target.to_string(),
        directories: remote.directories.iter().map(ToString::to_string).collect(),
        files: remote
            .files
            .iter()
            .map(|(path, hash, size)| (path.to_string(), hash.to_ascii_lowercase(), *size))
            .collect(),
    };
    frozen.directories.sort();
    frozen.files.sort();
    let (_, action, _, journal) = folder_parent(state, id).await?;
    let OperationAction::MoveFolder { source, target } = action else {
        return Err(recovery_changed("original action is not a folder move"));
    };
    if source.root_id != frozen.root_id
        || target.root_id != frozen.root_id
        || source.path != frozen.source
        || target.path != frozen.target
    {
        return Err(recovery_changed(
            "authenticated restoration manifest differs from the immutable folder action",
        ));
    }
    let inventory = frozen.inventory()?;
    if let Some(checks) = journal["verification"].as_array() {
        let expected = checks
            .iter()
            .find(|check| check["location"] == json!(target))
            .ok_or_else(|| {
                recovery_changed("original folder journal has no destination inventory")
            })?;
        if RecoveryContent::from_check(expected)? != RecoveryContent::Folder(inventory) {
            return Err(recovery_changed(
                "authenticated restoration differs from the original folder inventory",
            ));
        }
    }
    if let Some(previous) = journal.get("folder_restore_manifest") {
        if previous != &serde_json::to_value(&frozen)? {
            return Err(recovery_changed("frozen restoration manifest changed"));
        }
    } else {
        sqlx::query(
            "UPDATE operation_items SET journal_json=json_set(COALESCE(journal_json,'{}'),'$.folder_restore_manifest',json(?))
             WHERE operation_id=? AND idx=0",
        ).bind(serde_json::to_string(&frozen)?).bind(id).execute(&state.db).await?;
    }
    Ok(frozen)
}

async fn prepare_folder_recovery(state: &AppState, id: &str, root: &files::Root) -> AppResult<()> {
    let (_, _, status, _) = folder_parent(state, id).await?;
    if status == OperationStatus::Failed {
        let admitted: String =
            sqlx::query_scalar("SELECT admitted_server_ids_json FROM operations WHERE id=?")
                .bind(id)
                .fetch_one(&state.db)
                .await?;
        let admitted: Vec<String> = serde_json::from_str(&admitted)?;
        if !admitted.is_empty() && admitted != root.server_id.iter().cloned().collect::<Vec<_>>() {
            return Err(recovery_changed(
                "original folder maintenance mapping changed",
            ));
        }
        let mut tx = state.db.begin().await?;
        sqlx::query(
            "UPDATE operations SET status='recovery_required',recovery_resolved=0,admitted_server_ids_json='[]'
             WHERE id=? AND status='failed'",
        ).bind(id).execute(&mut *tx).await?;
        sqlx::query(
            "UPDATE operation_items SET status='recovery_required' WHERE operation_id=? AND idx=0",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        state.sync_wake.notify_waiters();
    } else if status != OperationStatus::RecoveryRequired {
        return Err(recovery_changed("folder continuation is no longer waiting"));
    }
    recovery_window(state, id, root).await?;
    verify_publish_hold(state, id, root).await
}

async fn recorded_folder_prerequisite(
    state: &AppState,
    id: &str,
    upload_id: &str,
) -> AppResult<FolderPrerequisite> {
    let (principal, action, _, journal) = folder_parent(state, id).await?;
    let frozen: FrozenFolderRestore =
        serde_json::from_value(journal["folder_restore_manifest"].clone())?;
    let prerequisites: Vec<FolderPrerequisite> =
        serde_json::from_value(journal["folder_prerequisites"].clone())?;
    let prerequisite = prerequisites
        .into_iter()
        .find(|entry| entry.upload_id == upload_id)
        .ok_or_else(|| {
            recovery_changed("folder upload has no authenticated supplemental execution evidence")
        })?;
    let OperationAction::MoveFolder { source, target } = action else {
        return Err(recovery_changed(
            "folder prerequisite lost its original action",
        ));
    };
    if source.root_id != frozen.root_id
        || target.root_id != frozen.root_id
        || source.path != frozen.source
        || target.path != frozen.target
    {
        return Err(recovery_changed(
            "recorded restoration scope differs from its original action",
        ));
    }
    let (request, owner, status, operation): (String, String, String, Option<String>) =
        sqlx::query_as("SELECT request_json,principal,status,operation_id FROM uploads WHERE id=?")
            .bind(upload_id)
            .fetch_one(&state.db)
            .await?;
    let request: ism_core::dto::CreateUpload = serde_json::from_str(&request)?;
    let (location, expected, role) = folder_upload_spec(&request, &frozen)?;
    let root = files::root(&state.db, &frozen.root_id).await?;
    if owner != principal.key()
        || location != prerequisite.target
        || expected != prerequisite.expected
        || role != prerequisite.role
        || verified_upload_stage(&root, upload_id)? != prerequisite.staged
        || !(status == "receiving" && operation.is_none()
            || status == "committed" && operation.as_deref() == Some(id))
    {
        return Err(recovery_changed(
            "recorded folder upload association or staging changed",
        ));
    }
    Ok(prerequisite)
}

fn folder_upload_spec(
    request: &ism_core::dto::CreateUpload,
    frozen: &FrozenFolderRestore,
) -> AppResult<(ism_core::dto::Location, RecoveryContent, FileRole)> {
    let [file] = request.files.as_slice() else {
        return Err(recovery_changed(
            "folder prerequisite must contain one file",
        ));
    };
    let folder = files::parse_rel(&request.target.path)?;
    let rel = folder.join(&file.name).map_err(AppError::internal)?;
    files::parse_rel(rel.as_str())?;
    if request.target.root_id != frozen.root_id
        || request.on_conflict != ism_core::dto::OnConflict::Fail
        || request.provenance.is_some()
        || request.existing_original.is_some()
        || file.expected_revision.is_some()
        || file.expected_sha256.is_some()
        || !frozen.files.iter().any(|(path, hash, size)| {
            path == rel.as_str() && hash.eq_ignore_ascii_case(&file.sha256) && *size == file.size
        })
    {
        return Err(recovery_changed(
            "folder prerequisite differs from the authenticated frozen file",
        ));
    }
    Ok((
        ism_core::dto::Location {
            root_id: frozen.root_id.clone(),
            path: rel.to_string(),
        },
        RecoveryContent::File {
            size: file.size,
            sha256: file.sha256.to_ascii_lowercase(),
        },
        file.role,
    ))
}

async fn persist_folder_prerequisites(
    state: &AppState,
    id: &str,
    prerequisites: &[FolderPrerequisite],
) -> AppResult<()> {
    sqlx::query(
        "UPDATE operation_items SET journal_json=json_set(journal_json,'$.folder_prerequisites',json(?))
         WHERE operation_id=? AND idx=0",
    ).bind(serde_json::to_string(prerequisites)?).bind(id).execute(&state.db).await?;
    Ok(())
}

async fn verify_folder_prerequisites(
    state: &AppState,
    id: &str,
    journal: &serde_json::Value,
    at_destination: bool,
) -> AppResult<()> {
    let Some(manifest) = journal.get("folder_restore_manifest") else {
        return Ok(());
    };
    let frozen: FrozenFolderRestore = serde_json::from_value(manifest.clone())?;
    frozen.inventory()?;
    let prerequisites: Vec<FolderPrerequisite> = journal
        .get("folder_prerequisites")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    let root = files::root(&state.db, &frozen.root_id).await?;
    for entry in prerequisites {
        let prerequisite = recorded_folder_prerequisite(state, id, &entry.upload_id).await?;
        let committed: bool = sqlx::query_scalar(
            "SELECT status='committed' AND operation_id=? FROM uploads WHERE id=?",
        )
        .bind(id)
        .bind(&entry.upload_id)
        .fetch_one(&state.db)
        .await?;
        if !prerequisite.done
            || !committed
            || RecoveryContent::read(&prerequisite.staged)?.is_some()
        {
            return Err(recovery_changed(
                "folder prerequisite has not finished safely",
            ));
        }
        let asset = if at_destination {
            let rel = files::parse_rel(&prerequisite.target.path)?
                .rebase(
                    &files::parse_rel(&frozen.source)?,
                    &files::parse_rel(&frozen.target)?,
                )
                .ok_or_else(|| recovery_changed("prerequisite escaped the original folder"))?;
            if RecoveryContent::read(&root.abs(&rel)?)?.as_ref() != Some(&prerequisite.expected) {
                return Err(recovery_changed("final prerequisite content changed"));
            }
            verified_asset_at_root(state, &root, &rel, prerequisite.role).await?
        } else {
            verify_prerequisite_asset(state, id, &root, &prerequisite.target, prerequisite.role)
                .await?
        };
        if asset != prerequisite.asset_id {
            return Err(recovery_changed("restored asset identity changed"));
        }
    }
    Ok(())
}

async fn verify_prerequisite_asset(
    state: &AppState,
    id: &str,
    root: &files::Root,
    target: &ism_core::dto::Location,
    role: FileRole,
) -> AppResult<Option<String>> {
    if root.server_id.is_none() || role != FileRole::Original {
        return Ok(None);
    }
    let rel = files::parse_rel(&target.path)?;
    let (_, _, _, journal) = folder_parent(state, id).await?;
    let effects: Vec<(usize, ApiStep)> = journal
        .get("api_effects")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    let original = effects
        .iter()
        .find(|(_, effect)| match &effect.kind {
            ApiKind::SyncMove { source_path, .. } => {
                Some(source_path) == root.immich_path(&rel).as_ref()
            }
            _ => false,
        })
        .map(|(_, effect)| effect);
    if let Some(effect) = original
        && verify_api_effect(state, effect).await?
        && let ApiKind::SyncMove {
            asset_id: Some(id), ..
        } = &effect.kind
    {
        return Ok(Some(id.clone()));
    }
    let asset = verified_asset_at_root(state, root, &rel, role).await?;
    if let Some(ApiStep {
        kind: ApiKind::SyncMove {
            asset_id: Some(id), ..
        },
        ..
    }) = original
        && asset.as_ref() != Some(id)
    {
        return Err(recovery_changed(
            "original folder asset identity changed before restoration",
        ));
    }
    Ok(asset)
}

fn verify_folder_subset(root: &files::Root, frozen: &FrozenFolderRestore) -> AppResult<()> {
    let path = root.abs(&files::parse_rel(&frozen.source)?)?;
    if let Some(content) = RecoveryContent::read(&path)?
        && !content.subset_of(&RecoveryContent::Folder(frozen.inventory()?))
    {
        return Err(recovery_changed(
            "old folder contains unknown or changed restoration entries",
        ));
    }
    Ok(())
}

async fn create_folder_restore_directories(
    state: &AppState,
    root: &files::Root,
    frozen: &FrozenFolderRestore,
) -> AppResult<()> {
    verify_folder_subset(root, frozen)?;
    let source = files::parse_rel(&frozen.source)?;
    let mut steps = Vec::new();
    for entry in frozen
        .inventory()?
        .into_iter()
        .filter(|entry| entry.sha256.is_none())
    {
        let rel = files::parse_rel(&entry.name)?
            .rebase(&ism_core::paths::RelPath::root(), &source)
            .ok_or_else(|| recovery_changed("restoration directory cannot be scoped"))?;
        steps.push(Step::Mkdir(root.abs(&rel)?));
    }
    let plan = Plan {
        steps,
        ..Default::default()
    };
    crate::watch::expect_writes(state, &expected_writes(&plan)).await?;
    for step in &plan.steps {
        apply(state, step)
            .await
            .map_err(|failure| AppError::conflict("recovery_filesystem", failure.message))?;
    }
    Ok(())
}

/// Restores one authenticated prerequisite inside its original operation's maintenance window.
pub(crate) async fn recover_folder_prerequisite_upload(
    state: &AppState,
    id: &str,
    upload_id: &str,
) -> AppResult<bool> {
    let _replication_guard = state.replication_operation_lock.lock().await;
    let _upload_guard = state.lock_upload(upload_id).await;
    if upload_id.is_empty()
        || !upload_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    {
        return Err(recovery_changed("invalid prerequisite upload identity"));
    }
    let frozen = freeze_folder_restore(state, id).await?;
    let root = files::root(&state.db, &frozen.root_id).await?;
    root.ensure_available()?;
    let (_, _, _, journal) = folder_parent(state, id).await?;
    let mut prerequisites: Vec<FolderPrerequisite> = journal
        .get("folder_prerequisites")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    if !prerequisites
        .iter()
        .any(|entry| entry.upload_id == upload_id)
    {
        let request = crate::peers::admin::verified_restore_upload(state, id, upload_id).await?;
        let (target, expected, role) = folder_upload_spec(&request, &frozen)?;
        let (owner, operation, status): (String, Option<String>, String) =
            sqlx::query_as("SELECT principal,operation_id,status FROM uploads WHERE id=?")
                .bind(upload_id)
                .fetch_one(&state.db)
                .await?;
        let (principal, _, _, _) = folder_parent(state, id).await?;
        if owner != principal.key() || operation.is_some() || status != "receiving" {
            return Err(recovery_changed(
                "prerequisite upload has a foreign principal or child operation",
            ));
        }
        let staged = verified_upload_stage(&root, upload_id)?;
        if RecoveryContent::read(&staged)?.as_ref() != Some(&expected) {
            return Err(recovery_changed(
                "prerequisite staging lacks the complete authenticated bytes",
            ));
        }
        prepare_folder_recovery(state, id, &root).await?;
        verify_folder_subset(&root, &frozen)?;
        let asset_id = verify_prerequisite_asset(state, id, &root, &target, role).await?;
        prerequisites.push(FolderPrerequisite {
            upload_id: upload_id.to_owned(),
            target,
            staged,
            expected,
            role,
            asset_id,
            done: false,
        });
        persist_folder_prerequisites(state, id, &prerequisites).await?;
    }
    let prerequisite = recorded_folder_prerequisite(state, id, upload_id).await?;
    prepare_folder_recovery(state, id, &root).await?;
    let asset =
        verify_prerequisite_asset(state, id, &root, &prerequisite.target, prerequisite.role)
            .await?;
    if asset != prerequisite.asset_id {
        return Err(recovery_changed(
            "folder prerequisite asset identity changed",
        ));
    }
    verify_publish_hold(state, id, &root).await?;
    create_folder_restore_directories(state, &root, &frozen).await?;
    let target = root.abs(&files::parse_rel(&prerequisite.target.path)?)?;
    let actual = RecoveryContent::read(&target)?;
    let staged = RecoveryContent::read(&prerequisite.staged)?;
    if actual
        .as_ref()
        .is_some_and(|actual| actual != &prerequisite.expected)
        || staged
            .as_ref()
            .is_some_and(|actual| actual != &prerequisite.expected)
        || actual.is_none() && staged.is_none()
    {
        return Err(recovery_changed(
            "folder prerequisite endpoints are changed or incomplete",
        ));
    }
    let mut archives: Vec<RecoveryArchive> = journal
        .get("recovery_execution")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    for archive in &mut archives {
        recover_archive(state, id, archive).await?;
        archive.done = true;
    }
    persist_recovery_archives(state, id, 0, &archives).await?;
    if RecoveryContent::read(&target)?.as_ref() == Some(&prerequisite.expected) {
        if let Some(content) = RecoveryContent::read(&prerequisite.staged)? {
            retain_recovery_copy(
                state,
                id,
                0,
                RecoveryRetention {
                    root: &root,
                    path: &prerequisite.staged,
                    logical_path: &target,
                    source_upload: Some(upload_id),
                    folder_prerequisite: true,
                },
                &content,
                &mut archives,
            )
            .await?;
        }
    } else {
        let plan = Plan {
            steps: vec![Step::Fs(plan::FsMove {
                from: prerequisite.staged.clone(),
                to: target.clone(),
                copy: false,
            })],
            ..Default::default()
        };
        crate::watch::expect_writes(state, &expected_writes(&plan)).await?;
        apply(state, &plan.steps[0])
            .await
            .map_err(|failure| AppError::conflict("recovery_filesystem", failure.message))?;
    }
    if RecoveryContent::read(&target)?.as_ref() != Some(&prerequisite.expected)
        || RecoveryContent::read(&prerequisite.staged)?.is_some()
    {
        return Err(recovery_changed(
            "restored prerequisite content is not verified",
        ));
    }
    let current = prerequisites
        .iter_mut()
        .find(|entry| entry.upload_id == upload_id)
        .ok_or_else(|| recovery_changed("prerequisite evidence disappeared"))?;
    current.done = true;
    let directory = plan::staging_dir(&root, upload_id)?;
    match fs::remove_dir(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let principal = folder_parent(state, id).await?.0.key();
    let mut tx = state.db.begin().await?;
    sqlx::query(
        "UPDATE operation_items SET journal_json=json_set(journal_json,'$.folder_prerequisites',json(?))
         WHERE operation_id=? AND idx=0",
    ).bind(serde_json::to_string(&prerequisites)?).bind(id).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE uploads SET status='committed',operation_id=? WHERE id=? AND principal=?
         AND (status='receiving' AND operation_id IS NULL OR status='committed' AND operation_id=?)",
    ).bind(id).bind(upload_id).bind(principal).bind(id)
        .execute(&mut *tx).await?.rows_affected().eq(&1).then_some(())
        .ok_or_else(|| recovery_changed("prerequisite upload changed before finalization"))?;
    tx.commit().await?;
    Ok(true)
}

/// Completes the immutable original folder action, never a child borrowing its barrier.
pub(crate) async fn resume_recovered_folder_operation(
    state: &AppState,
    id: &str,
) -> AppResult<bool> {
    let _replication_guard = state.replication_operation_lock.lock().await;
    let (principal, action, status, _) = folder_parent(state, id).await?;
    if status == OperationStatus::Completed {
        return Ok(true);
    }
    let frozen = freeze_folder_restore(state, id).await?;
    let root = files::root(&state.db, &frozen.root_id).await?;
    prepare_folder_recovery(state, id, &root).await?;
    let (_, _, _, journal) = folder_parent(state, id).await?;
    verify_folder_prerequisites(state, id, &journal, false).await?;
    if journal.get("steps").is_some() {
        // A published original destination is enough; do not recreate an already removed source.
        let destination = root.abs(&files::parse_rel(&frozen.target)?)?;
        if RecoveryContent::read(&destination)?.as_ref()
            == Some(&RecoveryContent::Folder(frozen.inventory()?))
        {
            return recover_bookkeeping_locked(state, id).await;
        }
    }
    create_folder_restore_directories(state, &root, &frozen).await?;
    crate::peers::admin::verify_restored_folder_source(state, id).await?;
    if journal.get("steps").is_some() {
        return recover_bookkeeping_locked(state, id).await;
    }
    let request: String = sqlx::query_scalar("SELECT request_json FROM operations WHERE id=?")
        .bind(id)
        .fetch_one(&state.db)
        .await?;
    let request: serde_json::Value = serde_json::from_str(&request)?;
    let ctx = plan::Ctx::new(
        state,
        &principal,
        request["allow_owner_transfer"].as_bool().unwrap_or(false),
    )
    .await?;
    let plan = plan::plan(&ctx, &action).await?;
    if let Some(blocker) = plan.blockers.first() {
        return Err(AppError::conflict(
            "recovery_plan_blocked",
            blocker.message.clone(),
        ));
    }
    self::journal(state, id, 0, &plan).await?;
    execute(state, id, 0, &plan)
        .await
        .map_err(|failure| AppError::conflict("recovery_execution", failure.message))?;
    recover_bookkeeping_locked(state, id).await
}

async fn authenticated_publish(
    state: &AppState,
    id: &str,
    item: i64,
    upload_id: &str,
    journal: &serde_json::Value,
    progress: &ism_core::dto::OperationTaskProgress,
) -> AppResult<AuthenticatedPublish> {
    if upload_id.is_empty()
        || !upload_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        || item != 0
        || journal["upload_done"].as_str() != Some(upload_id)
        || !journal["trash_new"].is_null()
        || !journal["trash_done"].is_null()
    {
        return Err(recovery_changed(
            "publication lacks an unambiguous non-replacement upload association",
        ));
    }
    let (principal, source, key, request, resolved): (String, String, String, String, i64) =
        sqlx::query_as("SELECT principal,source,idempotency_key,request_json,recovery_resolved FROM operations WHERE id=?")
            .bind(id).fetch_one(&state.db).await?;
    let peer_id = principal
        .strip_prefix("peer:")
        .filter(|peer| !peer.is_empty())
        .ok_or_else(|| {
            recovery_changed("publication was not submitted by an authenticated peer")
        })?;
    let request: serde_json::Value = serde_json::from_str(&request)?;
    let actions = request["actions"]
        .as_array()
        .ok_or_else(|| recovery_changed("original publication request is missing"))?;
    if source != "peer"
        || resolved != 0
        || key != format!("upload:{upload_id}")
        || actions.len() != 1
        || actions[0]["kind"] != "publish_upload"
        || actions[0]["upload_id"].as_str() != Some(upload_id)
    {
        return Err(recovery_changed(
            "original operation does not authenticate this publication",
        ));
    }
    let peer_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM peers WHERE id=?)")
        .bind(peer_id)
        .fetch_one(&state.db)
        .await?;
    let item_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM operation_items WHERE operation_id=?")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    let uploads: Vec<(String, String, String, String)> =
        sqlx::query_as("SELECT id,principal,request_json,status FROM uploads WHERE operation_id=?")
            .bind(id)
            .fetch_all(&state.db)
            .await?;
    let [(stored_id, owner, request, status)] = uploads.as_slice() else {
        return Err(recovery_changed(
            "operation does not own exactly one upload",
        ));
    };
    if !peer_exists
        || item_count != 1
        || stored_id != upload_id
        || owner != &principal
        || !matches!(status.as_str(), "publishing" | "failed" | "committed")
    {
        return Err(recovery_changed(
            "upload owner, operation association or lifecycle changed",
        ));
    }
    let request: ism_core::dto::CreateUpload = serde_json::from_str(request)?;
    let [spec] = request.files.as_slice() else {
        return Err(recovery_changed(
            "publication recovery requires one authenticated upload file",
        ));
    };
    if request.on_conflict != ism_core::dto::OnConflict::Fail
        || request.existing_original.is_some()
        || request.provenance.is_some()
        || spec.expected_revision.is_some()
        || spec.expected_sha256.is_some()
        || spec.name.is_empty()
        || spec.name == "."
        || spec.name == ".."
        || spec.name.contains(['/', '\\', '\0'])
        || spec.name.starts_with(".ism")
        || spec.name.len() > 255
        || spec.sha256.len() != 64
        || !spec.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || (spec.role != FileRole::Original
            && !super::uploads::standalone_peer_upload(
                &Principal::Peer {
                    peer_id: peer_id.to_owned(),
                },
                &request,
                &state.sidecar_extensions(),
            ))
    {
        return Err(recovery_changed(
            "replacement, group or uncertain companion publication is not recoverable",
        ));
    }
    let (root, folder, _) = files::locate(&state.db, &request.target).await?;
    let rel = folder.join(&spec.name).map_err(AppError::internal)?;
    files::parse_rel(rel.as_str())?;
    let target = root.location(&rel);
    let [file] = progress.files.as_slice() else {
        return Err(recovery_changed(
            "journal does not describe a single publication",
        ));
    };
    let checks = journal["verification"]
        .as_array()
        .ok_or_else(|| recovery_changed("publication content journal is missing"))?;
    let expected = RecoveryContent::File {
        size: spec.size,
        sha256: spec.sha256.to_ascii_lowercase(),
    };
    if file.from.is_some()
        || file.to.as_ref() != Some(&target)
        || file.copy
        || file.role != spec.role
        || checks.len() != 1
        || serde_json::from_value::<ism_core::dto::Location>(checks[0]["location"].clone())?
            != target
        || RecoveryContent::from_check(&checks[0])? != expected
    {
        return Err(recovery_changed(
            "journal destination or content differs from the authenticated upload",
        ));
    }
    let api_effects: Vec<(usize, ApiStep)> =
        serde_json::from_value(journal["api_effects"].clone())?;
    let db_effects: Vec<(usize, String, Vec<DbStep>)> =
        serde_json::from_value(journal["db_effects"].clone())?;
    if !db_effects.is_empty() {
        return Err(recovery_changed(
            "publication contains unexpected database effects",
        ));
    }
    if let Some(server) = &root.server_id
        && spec.role == FileRole::Original
    {
        let library = root
            .library_id
            .as_ref()
            .ok_or_else(|| recovery_changed("publication library is missing"))?;
        let owner = registry::library_owner(state, library)
            .await?
            .ok_or_else(|| recovery_changed("publication library owner is missing"))?;
        let path = root
            .immich_path(&rel)
            .ok_or_else(|| recovery_changed("publication Immich path is missing"))?;
        if api_effects.len() != 1
            || api_effects[0].1.server_id != *server
            || api_effects[0].1.owner != owner
            || !api_effects[0].1.ids.is_empty()
            || api_effects[0].1.kind
                != (ApiKind::SyncAdd {
                    library_id: library.clone(),
                    path,
                })
        {
            return Err(recovery_changed(
                "publication Immich effect differs from the original mapped library",
            ));
        }
    } else if !api_effects.is_empty() {
        return Err(recovery_changed(
            "publication contains unexpected Immich effects",
        ));
    }
    let pending: Vec<(i64, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT idx,share_id,kind,to_path,peer_id FROM pending_peer_ops WHERE operation_id=?",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    if !pending.is_empty() {
        let [(index, share_id, kind, to, pending_peer)] = pending.as_slice() else {
            return Err(recovery_changed(
                "publication has ambiguous peer bookkeeping",
            ));
        };
        let shares = crate::peers::shares(state).await?;
        let share = shares
            .iter()
            .find(|share| &share.id == share_id)
            .ok_or_else(|| recovery_changed("publication share is missing"))?;
        if *index != item
            || kind != "publish"
            || pending_peer != peer_id
            || !share.enabled
            || !share.peer_ids.iter().any(|peer| peer == peer_id)
            || share.rel_of(&target).is_none()
            || share.rel_of(&target).as_ref() != to.as_ref()
        {
            return Err(recovery_changed(
                "publication peer bookkeeping refers to a foreign destination",
            ));
        }
    }
    verify_publish_hold(state, id, &root).await?;
    let staged = verified_upload_stage(&root, upload_id)?;
    Ok(AuthenticatedPublish {
        upload_id: upload_id.to_owned(),
        principal,
        root,
        target,
        staged,
        expected,
        role: spec.role,
    })
}

async fn verify_publish_hold(state: &AppState, id: &str, root: &files::Root) -> AppResult<()> {
    let (raw, resolved): (String, i64) = sqlx::query_as(
        "SELECT admitted_server_ids_json,recovery_resolved FROM operations WHERE id=?",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await?;
    let admitted: Vec<String> = serde_json::from_str(&raw)?;
    if resolved != 0 || admitted != root.server_id.iter().cloned().collect::<Vec<_>>() {
        return Err(recovery_changed(
            "publication no longer owns its original maintenance hold",
        ));
    }
    if let Some(server) = &root.server_id {
        let maintenance = registry::server(state, server).await?.maintenance;
        if maintenance.phase != ism_core::maintenance::Phase::RecoveryRequired
            || !maintenance.queue_paused_by_ism
            || maintenance.unresolved_recoveries == 0
            || maintenance.drift.is_some()
        {
            return Err(recovery_changed(
                "publication maintenance hold is not verified",
            ));
        }
    }
    Ok(())
}

fn verified_upload_stage(root: &files::Root, upload_id: &str) -> AppResult<std::path::PathBuf> {
    root.ensure_available()?;
    let directory = plan::staging_dir(root, upload_id)?;
    match fs::symlink_metadata(&directory) {
        Ok(metadata) if metadata.is_dir() => {
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                if entry.file_name() != "0" || !fs::symlink_metadata(entry.path())?.is_file() {
                    return Err(recovery_changed(
                        "upload staging contains unknown or unsafe entries",
                    ));
                }
            }
        }
        Ok(_) => return Err(recovery_changed("upload staging is not a directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let rel =
        ism_core::paths::RelPath::parse(&format!("{}/uploads/{upload_id}/0", files::STAGING_DIR))
            .map_err(AppError::internal)?;
    root.abs(&rel)
}

async fn finalize_recovered_publish(
    state: &AppState,
    id: &str,
    item: i64,
    publication: &AuthenticatedPublish,
) -> AppResult<()> {
    let asset = verify_recovered_publish(state, id, item, publication).await?;
    // Only remove an empty, authenticated stage directory; never recursively clean recovery bytes.
    let directory = plan::staging_dir(&publication.root, &publication.upload_id)?;
    match fs::remove_dir(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut tx = state.db.begin().await?;
    let changed = sqlx::query(
        "UPDATE uploads SET status='committed' WHERE id=? AND operation_id=? AND principal=?
         AND status IN ('publishing','failed','committed')",
    )
    .bind(&publication.upload_id)
    .bind(id)
    .bind(&publication.principal)
    .execute(&mut *tx)
    .await?;
    if changed.rows_affected() != 1 {
        return Err(recovery_changed(
            "upload association changed before finalization",
        ));
    }
    sqlx::query(
        "UPDATE operation_items SET result_json=json_set(COALESCE(result_json,'{}'),'$.asset_id',?,'$.recovery_verified',json('true'))
         WHERE operation_id=? AND idx=?",
    ).bind(&asset).bind(id).bind(item).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

async fn verify_recovered_publish(
    state: &AppState,
    id: &str,
    item: i64,
    publication: &AuthenticatedPublish,
) -> AppResult<Option<String>> {
    verify_publish_hold(state, id, &publication.root).await?;
    let staged = verified_upload_stage(&publication.root, &publication.upload_id)?;
    let rel = files::parse_rel(&publication.target.path)?;
    let final_path = publication.root.abs(&rel)?;
    if staged != publication.staged
        || RecoveryContent::read(&staged)?.is_some()
        || RecoveryContent::read(&final_path)?.as_ref() != Some(&publication.expected)
    {
        return Err(recovery_changed(
            "publication endpoints are not ready for finalization",
        ));
    }
    let asset = verified_asset_at_root(state, &publication.root, &rel, publication.role).await?;
    let previous: Option<String> = sqlx::query_scalar(
        "SELECT json_extract(result_json,'$.asset_id') FROM operation_items WHERE operation_id=? AND idx=?",
    ).bind(id).bind(item).fetch_one(&state.db).await?;
    if previous.is_some() && previous != asset {
        return Err(recovery_changed(
            "recovered publication asset identity changed",
        ));
    }
    verify_publish_hold(state, id, &publication.root).await?;
    if verified_upload_stage(&publication.root, &publication.upload_id)? != staged
        || RecoveryContent::read(&staged)?.is_some()
        || RecoveryContent::read(&publication.root.abs(&rel)?)?.as_ref()
            != Some(&publication.expected)
    {
        return Err(recovery_changed(
            "publication changed during Immich verification",
        ));
    }
    Ok(asset)
}

// A short file is salvageable only when its entire byte sequence matches the trusted full copy.
fn matching_partial(
    partial_path: &std::path::Path,
    partial: &RecoveryContent,
    full_path: &std::path::Path,
    full: &RecoveryContent,
) -> AppResult<bool> {
    if partial.subset_of(full) {
        return Ok(true);
    }
    if let (RecoveryContent::Folder(actual), RecoveryContent::Folder(expected)) = (partial, full) {
        for entry in actual {
            let Some(original) = expected.iter().find(|original| original.name == entry.name)
            else {
                return Ok(false);
            };
            if entry == original {
                continue;
            }
            if entry.sha256.is_none() || original.sha256.is_none() {
                return Ok(false);
            }
            let short_path = partial_path.join(&entry.name);
            let long_path = full_path.join(&entry.name);
            let short = RecoveryContent::read(&short_path)?
                .ok_or_else(|| recovery_changed("partial folder file disappeared"))?;
            let long = RecoveryContent::read(&long_path)?
                .ok_or_else(|| recovery_changed("complete folder file disappeared"))?;
            if !matching_partial(&short_path, &short, &long_path, &long)? {
                return Ok(false);
            }
        }
        return Ok(
            RecoveryContent::read(partial_path)?.as_ref() == Some(partial)
                && RecoveryContent::read(full_path)?.as_ref() == Some(full),
        );
    }
    if let (RecoveryContent::File { size: short, .. }, RecoveryContent::File { size: long, .. }) =
        (partial, full)
        && short < long
    {
        use std::io::Read;
        let mut partial_file = fs::File::open(partial_path)?;
        let mut full_file = fs::File::open(full_path)?;
        let mut left = [0_u8; 64 * 1024];
        let mut right = [0_u8; 64 * 1024];
        let mut remaining = *short;
        while remaining > 0 {
            let count = remaining.min(left.len() as u64) as usize;
            partial_file.read_exact(&mut left[..count])?;
            full_file.read_exact(&mut right[..count])?;
            if left[..count] != right[..count] {
                return Ok(false);
            }
            remaining -= count as u64;
        }
        return Ok(
            RecoveryContent::read(partial_path)?.as_ref() == Some(partial)
                && RecoveryContent::read(full_path)?.as_ref() == Some(full),
        );
    }
    Ok(false)
}

async fn recovery_window(state: &AppState, id: &str, root: &files::Root) -> AppResult<()> {
    let Some(server) = &root.server_id else {
        return Ok(());
    };
    let raw: String =
        sqlx::query_scalar("SELECT admitted_server_ids_json FROM operations WHERE id=?")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    let mut admitted: Vec<String> = serde_json::from_str(&raw)?;
    if !admitted.contains(server) {
        crate::maintenance::acquire(state, server).await?;
        let _maintenance_guard = state.maintenance_lock.lock().await;
        let mut tx = state.db.begin().await?;
        let raw: String =
            sqlx::query_scalar("SELECT maintenance_json FROM immich_servers WHERE id=?")
                .bind(server)
                .fetch_one(&mut *tx)
                .await?;
        let mut maintenance: ism_core::maintenance::MaintenanceState = serde_json::from_str(&raw)?;
        if maintenance.phase != ism_core::maintenance::Phase::Active
            || !maintenance.queue_paused_by_ism
            || maintenance.drift.is_some()
        {
            return Err(recovery_changed(
                "maintenance changed during recovery admission",
            ));
        }
        maintenance.unresolved_recoveries += 1;
        maintenance.require_recovery("finishing interrupted filesystem move");
        maintenance.settle();
        admitted.push(server.clone());
        sqlx::query("UPDATE operations SET admitted_server_ids_json=? WHERE id=? AND status='recovery_required' AND recovery_resolved=0")
            .bind(serde_json::to_string(&admitted)?)
            .bind(id)
            .execute(&mut *tx)
            .await?.rows_affected().eq(&1).then_some(())
            .ok_or_else(|| recovery_changed("operation changed during recovery admission"))?;
        sqlx::query("UPDATE immich_servers SET maintenance_json=? WHERE id=?")
            .bind(serde_json::to_string(&maintenance)?)
            .bind(server)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        state.emit(ServerEvent::MaintenanceChanged {
            server_id: server.clone(),
        });
        state.maintenance_wake.notify_waiters();
    }
    let maintenance = registry::server(state, server).await?.maintenance;
    if !maintenance.queue_paused_by_ism
        || maintenance.drift.is_some()
        || !matches!(
            maintenance.phase,
            ism_core::maintenance::Phase::RecoveryRequired
        )
    {
        return Err(AppError::conflict(
            "recovery_maintenance",
            "the original operation's maintenance barrier is not held",
        ));
    }
    Ok(())
}

async fn recover_archive(state: &AppState, id: &str, archive: &RecoveryArchive) -> AppResult<()> {
    let root = files::root(&state.db, &archive.record.root_id).await?;
    root.ensure_available()?;
    let source = if let Some(upload_id) = &archive.source_upload {
        if archive.source_folder_prerequisite {
            let prerequisite = recorded_folder_prerequisite(state, id, upload_id).await?;
            if prerequisite.target.root_id != archive.record.root_id
                || prerequisite.target.path != archive.record.path
                || archive.content != prerequisite.expected
            {
                return Err(recovery_changed(
                    "retained stage differs from its authenticated folder prerequisite",
                ));
            }
            prerequisite.staged
        } else {
            let (journal, progress): (String, String) = sqlx::query_as(
            "SELECT journal_json,progress_json FROM operation_items WHERE operation_id=? AND idx=0",
        ).bind(id).fetch_one(&state.db).await?;
            let publication = authenticated_publish(
                state,
                id,
                0,
                upload_id,
                &serde_json::from_str(&journal)?,
                &serde_json::from_str(&progress)?,
            )
            .await?;
            if publication.target.root_id != archive.record.root_id
                || publication.target.path != archive.record.path
                || archive.content != publication.expected
            {
                return Err(recovery_changed(
                    "retained stage does not belong to the authenticated publication",
                ));
            }
            publication.staged
        }
    } else {
        root.abs(&files::parse_rel(&archive.record.path)?)?
    };
    let quarantine = ism_core::paths::RelPath::parse(&format!(
        "{}/{}",
        archive.record.quarantine_rel, archive.record.path
    ))
    .map_err(AppError::internal)?;
    if source != archive.movement.from
        || root.abs(&quarantine)? != archive.movement.to
        || archive.movement.copy
        || !archive.record.asset_ids.is_empty()
        || archive.record.server_id.is_some()
    {
        return Err(recovery_changed(
            "recovery trash evidence differs from its mapped paths",
        ));
    }
    recovery_window(state, id, &root).await?;
    if archive.done {
        if RecoveryContent::read(&archive.movement.to)?.as_ref() != Some(&archive.content) {
            return Err(recovery_changed("retained recovery trash changed"));
        }
        return bookkeeping(
            state,
            id,
            &Plan {
                trash_new: Some(archive.record.clone()),
                ..Default::default()
            },
        )
        .await;
    }
    let source = RecoveryContent::read(&archive.movement.from)?;
    let destination = RecoveryContent::read(&archive.movement.to)?;
    match (source, destination) {
        (Some(source), None) if source == archive.content => {
            let plan = Plan {
                steps: vec![Step::Fs(archive.movement.clone())],
                ..Default::default()
            };
            crate::watch::expect_writes(state, &expected_writes(&plan)).await?;
            apply(state, &plan.steps[0])
                .await
                .map_err(|failure| AppError::conflict("recovery_filesystem", failure.message))?;
        }
        (None, Some(destination)) if destination == archive.content => {}
        _ => return Err(recovery_changed("recovery trash paths are not verified")),
    }
    if RecoveryContent::read(&archive.movement.from)?.is_some()
        || RecoveryContent::read(&archive.movement.to)?.as_ref() != Some(&archive.content)
    {
        return Err(recovery_changed("retained recovery trash is not verified"));
    }
    bookkeeping(
        state,
        id,
        &Plan {
            trash_new: Some(archive.record.clone()),
            ..Default::default()
        },
    )
    .await
}

async fn retain_recovery_copy(
    state: &AppState,
    id: &str,
    item: i64,
    retention: RecoveryRetention<'_>,
    content: &RecoveryContent,
    archives: &mut Vec<RecoveryArchive>,
) -> AppResult<()> {
    let RecoveryRetention {
        root,
        path,
        logical_path,
        source_upload,
        folder_prerequisite,
    } = retention;
    let rel = logical_path
        .strip_prefix(root.base())
        .map_err(AppError::internal)?;
    let rel = rel
        .to_str()
        .ok_or_else(|| recovery_changed("non-UTF-8 recovery path"))?;
    files::parse_rel(rel)?;
    let trash_id = crate::crypto::new_id("trash");
    let quarantine_rel = format!("{}/{trash_id}", files::TRASH_DIR);
    let quarantine = ism_core::paths::RelPath::parse(&format!("{quarantine_rel}/{rel}"))
        .map_err(AppError::internal)?;
    let archive = RecoveryArchive {
        movement: plan::FsMove {
            from: path.to_owned(),
            to: root.abs(&quarantine)?,
            copy: false,
        },
        record: plan::TrashRecord {
            id: trash_id,
            root_id: root.mapping.root_id.clone(),
            path: rel.to_owned(),
            quarantine_rel,
            is_folder: matches!(content, RecoveryContent::Folder(_)),
            size: files::tree_size(path),
            // Redundant bytes are not the canonical Immich asset.
            asset_ids: Vec::new(),
            server_id: None,
            file_inventory: Some(
                content.trash_inventory(
                    logical_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or_else(|| recovery_changed("recovery file has no name"))?,
                ),
            ),
        },
        content: content.clone(),
        done: false,
        source_upload: source_upload.map(str::to_owned),
        source_folder_prerequisite: folder_prerequisite,
    };
    archives.push(archive.clone());
    // Persist intent before moving bytes. Original action, plan and checks remain immutable.
    sqlx::query(
        "UPDATE operation_items SET journal_json=json_set(journal_json,'$.recovery_execution',json(?))
         WHERE operation_id=? AND idx=?",
    ).bind(serde_json::to_string(archives)?).bind(id).bind(item)
        .execute(&state.db).await?;
    recover_archive(state, id, &archive).await?;
    archives
        .last_mut()
        .ok_or_else(|| AppError::internal("recovery archive missing"))?
        .done = true;
    persist_recovery_archives(state, id, item, archives).await
}

struct RecoveryRetention<'a> {
    root: &'a files::Root,
    path: &'a std::path::Path,
    logical_path: &'a std::path::Path,
    source_upload: Option<&'a str>,
    folder_prerequisite: bool,
}

async fn persist_recovery_archives(
    state: &AppState,
    id: &str,
    item: i64,
    archives: &[RecoveryArchive],
) -> AppResult<()> {
    sqlx::query(
        "UPDATE operation_items SET journal_json=json_set(journal_json,'$.recovery_execution',json(?))
         WHERE operation_id=? AND idx=?",
    ).bind(serde_json::to_string(archives)?).bind(id).bind(item)
        .execute(&state.db).await?;
    Ok(())
}

async fn recover_moves(
    state: &AppState,
    id: &str,
    item: i64,
    action: &OperationAction,
    journal: &serde_json::Value,
    progress: &ism_core::dto::OperationTaskProgress,
) -> AppResult<Vec<usize>> {
    if !matches!(
        action,
        OperationAction::Move { .. }
            | OperationAction::Rename { .. }
            | OperationAction::MoveFolder { .. }
            | OperationAction::PublishUpload { .. }
    ) {
        return Ok(Vec::new());
    }
    let labels = journal["steps"]
        .as_array()
        .ok_or_else(|| recovery_changed("journal steps are missing"))?;
    let checks = journal["verification"]
        .as_array()
        .ok_or_else(|| recovery_changed("journal content proof is missing"))?;
    let typed: Option<Vec<Step>> = journal
        .get("execution_steps")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?;
    let api_effects: Vec<(usize, ApiStep)> = journal
        .get("api_effects")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    let db_effects: Vec<(usize, String, Vec<DbStep>)> = journal
        .get("db_effects")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    if progress.steps.len() != labels.len() + db_effects.len() + 1 {
        return Err(recovery_changed("journal and progress step counts differ"));
    }
    if api_effects
        .iter()
        .any(|(_, step)| matches!(&step.kind, ApiKind::SyncMove { asset_id: None, .. }))
    {
        return Err(recovery_changed(
            "original Immich asset identity was not retained",
        ));
    }
    let mut moves = Vec::new();
    let publication = if let OperationAction::PublishUpload { upload_id } = action {
        Some(authenticated_publish(state, id, item, upload_id, journal, progress).await?)
    } else {
        None
    };
    for file in &progress.files {
        let to = file
            .to
            .as_ref()
            .ok_or_else(|| recovery_changed("filesystem destination lacks recovery proof"))?;
        let (source, from_path) = if let Some(publication) = &publication {
            (publication.root.clone(), publication.staged.clone())
        } else {
            let from = file
                .from
                .as_ref()
                .ok_or_else(|| recovery_changed("filesystem source lacks recovery proof"))?;
            let (root, _, path) = files::locate(&state.db, from).await?;
            (root, path)
        };
        let (destination, _, to_path) = files::locate(&state.db, to).await?;
        let label = format!(
            "{} {} -> {}",
            if file.copy { "copy" } else { "move" },
            from_path.display(),
            to_path.display()
        );
        let indices: Vec<_> = labels
            .iter()
            .enumerate()
            .filter_map(|(index, value)| (value.as_str() == Some(&label)).then_some(index))
            .collect();
        if indices.len() != 1 || from_path == to_path {
            return Err(recovery_changed(
                "move journal does not uniquely match its mapped paths",
            ));
        }
        let index = indices[0];
        if let Some(steps) = &typed {
            match steps.get(index) {
                Some(Step::Fs(movement))
                    if movement.from == from_path
                        && movement.to == to_path
                        && movement.copy == file.copy => {}
                _ => {
                    return Err(recovery_changed(
                        "serialized filesystem effect differs from its authenticated endpoints",
                    ));
                }
            }
        }
        let check = checks
            .iter()
            .find(|check| {
                serde_json::from_value::<ism_core::dto::Location>(check["location"].clone())
                    .is_ok_and(|location| location == *to)
            })
            .ok_or_else(|| recovery_changed("move destination lacks trusted original content"))?;
        moves.push(RecoveryMove {
            index,
            movement: plan::FsMove {
                from: from_path,
                to: to_path,
                copy: file.copy,
            },
            expected: RecoveryContent::from_check(check)?,
            source,
            destination,
            upload_id: publication
                .as_ref()
                .map(|publication| publication.upload_id.clone()),
        });
    }
    if publication.is_none()
        && api_effects.is_empty()
        && db_effects.is_empty()
        && moves.iter().any(|movement| {
            (movement.source.server_id.is_some() || movement.destination.server_id.is_some())
                && match &movement.expected {
                    RecoveryContent::File { .. } => true,
                    RecoveryContent::Folder(entries) => {
                        entries.iter().any(|entry| entry.sha256.is_some())
                    }
                }
        })
    {
        return Err(AppError::conflict(
            "recovery_immich_verification",
            "journal has no independently verifiable Immich effect for library content",
        ));
    }
    if publication.is_none()
        && moves
            .iter()
            .flat_map(|movement| [&movement.source, &movement.destination])
            .filter_map(|root| root.server_id.as_ref())
            .any(|server| {
                !api_effects
                    .iter()
                    .any(|(_, step)| &step.server_id == server)
                    && !db_effects
                        .iter()
                        .any(|(_, planned_server, _)| planned_server == server)
            })
        && moves.iter().any(|movement| match &movement.expected {
            RecoveryContent::File { .. } => true,
            RecoveryContent::Folder(entries) => entries.iter().any(|entry| entry.sha256.is_some()),
        })
    {
        return Err(recovery_changed(
            "mapped Immich server differs from the retained effects",
        ));
    }
    if moves.is_empty() {
        return Err(recovery_changed("no journaled filesystem moves"));
    }
    let mut recovered: Vec<_> = moves.iter().map(|movement| movement.index).collect();
    if let Some(steps) = &typed {
        if steps.len() != labels.len() {
            return Err(recovery_changed(
                "serialized execution and journal step counts differ",
            ));
        }
        for (index, effect) in &api_effects {
            match steps.get(*index) {
                Some(Step::Api(original))
                    if original.kind == effect.kind
                        && original.server_id == effect.server_id
                        && original.owner == effect.owner
                        && original.ids == effect.ids => {}
                _ => {
                    return Err(recovery_changed(
                        "serialized Immich effect differs from its original journal",
                    ));
                }
            }
        }
        if let Some(publication) = &publication {
            for step in steps {
                if let Step::CheckHash {
                    expected_size,
                    expected_sha256,
                    ..
                } = step
                    && (RecoveryContent::File {
                        size: *expected_size,
                        sha256: expected_sha256.to_ascii_lowercase(),
                    }) != publication.expected
                {
                    return Err(recovery_changed(
                        "serialized hash check differs from the authenticated upload",
                    ));
                }
            }
        }
    }
    // Reject extra/intermediate mutations, even if their progress says done.
    for (index, label) in labels.iter().enumerate() {
        if recovered.contains(&index) {
            continue;
        }
        if label
            .as_str()
            .is_some_and(|label| label.starts_with("immich "))
        {
            if !api_effects.iter().any(|(i, _)| *i == index) {
                return Err(recovery_changed(
                    "Immich step has no retained executable effect",
                ));
            }
            continue;
        }
        let covered_check = typed
            .as_ref()
            .and_then(|steps| steps.get(index))
            .is_some_and(|step| {
                let path = match step {
                    Step::CheckTree { path, .. }
                    | Step::CheckHash { path, .. }
                    | Step::CheckRevision { path, .. } => path,
                    Step::Mkdir(path) => {
                        return publication.is_some()
                            && moves.iter().any(|movement| {
                                movement.movement.to.parent() == Some(path.as_path())
                            });
                    }
                    _ => return false,
                };
                moves.iter().any(|movement| {
                    path == &movement.movement.from || path == &movement.movement.to
                })
            })
            || (typed.is_none()
                && moves.iter().any(|movement| {
                    [&movement.movement.from, &movement.movement.to]
                        .iter()
                        .any(|path| {
                            ["check revision", "check folder inventory", "check sha256"]
                                .iter()
                                .any(|prefix| {
                                    label.as_str() == Some(&format!("{prefix} {}", path.display()))
                                })
                        })
                }))
            || (typed.is_none()
                && publication.is_some()
                && moves.iter().any(|movement| {
                    movement.movement.to.parent().is_some_and(|parent| {
                        label.as_str() == Some(&format!("mkdir {}", parent.display()))
                    })
                }));
        if !covered_check {
            return Err(recovery_changed(
                "an intermediate filesystem effect lacks recovery proof",
            ));
        }
        recovered.push(index);
    }
    let mut archives: Vec<RecoveryArchive> = journal
        .get("recovery_execution")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    for archive in &mut archives {
        recover_archive(state, id, archive).await?;
        archive.done = true;
    }
    persist_recovery_archives(state, id, item, &archives).await?;
    // Preflight the entire file group before moving any of its original bytes.
    for movement in &moves {
        verify_recoverable_move(movement)?;
        if publication.is_some() {
            verify_publish_hold(state, id, &movement.destination).await?;
        } else {
            recovery_window(state, id, &movement.source).await?;
            recovery_window(state, id, &movement.destination).await?;
        }
    }
    for movement in &moves {
        verify_recoverable_move(movement)?;
        let source = RecoveryContent::read(&movement.movement.from)?;
        let destination = RecoveryContent::read(&movement.movement.to)?;
        if destination.as_ref() == Some(&movement.expected) {
            if !movement.movement.copy
                && let Some(source) = source
            {
                retain_recovery_copy(
                    state,
                    id,
                    item,
                    RecoveryRetention {
                        root: &movement.source,
                        path: &movement.movement.from,
                        logical_path: if publication.is_some() {
                            &movement.movement.to
                        } else {
                            &movement.movement.from
                        },
                        source_upload: movement.upload_id.as_deref(),
                        folder_prerequisite: false,
                    },
                    &source,
                    &mut archives,
                )
                .await?;
            }
        } else {
            if let Some(destination) = destination {
                retain_recovery_copy(
                    state,
                    id,
                    item,
                    RecoveryRetention {
                        root: &movement.destination,
                        path: &movement.movement.to,
                        logical_path: &movement.movement.to,
                        source_upload: None,
                        folder_prerequisite: false,
                    },
                    &destination,
                    &mut archives,
                )
                .await?;
            }
            let plan = Plan {
                steps: vec![Step::Fs(movement.movement.clone())],
                ..Default::default()
            };
            crate::watch::expect_writes(state, &expected_writes(&plan)).await?;
            if RecoveryContent::read(&movement.movement.from)?.as_ref() != Some(&movement.expected)
            {
                return Err(recovery_changed("source changed before recovery execution"));
            }
            apply(state, &plan.steps[0])
                .await
                .map_err(|failure| AppError::conflict("recovery_filesystem", failure.message))?;
        }
        if RecoveryContent::read(&movement.movement.to)?.as_ref() != Some(&movement.expected)
            || (!movement.movement.copy
                && RecoveryContent::read(&movement.movement.from)?.is_some())
        {
            return Err(recovery_changed(
                "recovered move endpoints are not verified",
            ));
        }
        step_progress(state, id, item, movement.index, "done").await;
    }
    for archive in &archives {
        recover_archive(state, id, archive).await?;
    }
    for index in &recovered {
        step_progress(state, id, item, *index, "done").await;
    }
    Ok(recovered)
}

fn verify_recoverable_move(movement: &RecoveryMove) -> AppResult<()> {
    for (root, path) in [
        (&movement.source, &movement.movement.from),
        (&movement.destination, &movement.movement.to),
    ] {
        root.ensure_available()?;
        let rel = path.strip_prefix(root.base()).map_err(AppError::internal)?;
        let verified = if movement.upload_id.is_some() && path == &movement.movement.from {
            verified_upload_stage(root, movement.upload_id.as_deref().unwrap_or_default())?
        } else {
            let rel = files::parse_rel(
                rel.to_str()
                    .ok_or_else(|| recovery_changed("non-UTF-8 path"))?,
            )?;
            root.abs(&rel)?
        };
        if verified != *path {
            return Err(recovery_changed("recovery path changed"));
        }
    }
    let source = RecoveryContent::read(&movement.movement.from)?;
    let destination = RecoveryContent::read(&movement.movement.to)?;
    let source_full = source.as_ref() == Some(&movement.expected);
    let destination_full = destination.as_ref() == Some(&movement.expected);
    if movement.upload_id.is_some() && source.is_some() && !source_full {
        return Err(recovery_changed(
            "authenticated upload staging content changed",
        ));
    }
    if source_full
        && destination_full
        && fs::canonicalize(&movement.movement.from)? == fs::canonicalize(&movement.movement.to)?
    {
        return Err(recovery_changed(
            "aliased move endpoints cannot be recovered as redundant copies",
        ));
    }
    if movement.movement.copy && !source_full {
        return Err(recovery_changed("shared companion source is not complete"));
    }
    if !source_full && !destination_full {
        return Err(recovery_changed(
            "neither move endpoint proves complete original content",
        ));
    }
    for (path, content, other) in [
        (
            &movement.movement.from,
            source.as_ref(),
            &movement.movement.to,
        ),
        (
            &movement.movement.to,
            destination.as_ref(),
            &movement.movement.from,
        ),
    ] {
        if let Some(content) = content
            && content != &movement.expected
            && !matching_partial(path, content, other, &movement.expected)?
        {
            return Err(recovery_changed(
                "move endpoint contains changed or unexpected entries",
            ));
        }
    }
    Ok(())
}

async fn verify_api_effect(state: &AppState, step: &ApiStep) -> AppResult<bool> {
    let immich = |error: crate::immich::api::ApiError| {
        AppError::conflict("recovery_immich_unavailable", error.to_string())
    };
    let server = registry::server(state, &step.server_id).await?;
    let owner = match &step.kind {
        ApiKind::SyncMove {
            to_owner: Some(owner),
            ..
        } => owner,
        _ => &step.owner,
    };
    let api = registry::api_as_user(state, &server, owner).await?;
    match &step.kind {
        ApiKind::SyncAdd { path, .. } => {
            Ok(api.asset_id_by_path(path).await.map_err(immich)?.is_some())
        }
        ApiKind::SyncRemove { path, .. } => {
            Ok(api.asset_id_by_path(path).await.map_err(immich)?.is_none())
        }
        ApiKind::SyncMove {
            source_library_id,
            destination_library_id,
            destination_path,
            asset_id,
            to_owner,
            ..
        } => {
            let id = match asset_id {
                Some(id) => id.clone(),
                None => match api
                    .asset_id_by_path(destination_path)
                    .await
                    .map_err(immich)?
                {
                    Some(id) => id,
                    None => return Ok(false),
                },
            };
            let asset = match api.asset(&id).await {
                Ok(asset) => asset,
                Err(crate::immich::api::ApiError::Status { status: 404, .. }) => return Ok(false),
                Err(error) => return Err(immich(error)),
            };
            Ok(asset["originalPath"].as_str() == Some(destination_path)
                && asset["libraryId"].as_str()
                    == Some(
                        destination_library_id
                            .as_deref()
                            .unwrap_or(source_library_id),
                    )
                && to_owner
                    .as_ref()
                    .is_none_or(|owner| asset["ownerId"].as_str() == Some(owner)))
        }
        kind => {
            for id in &step.ids {
                match api.asset(id).await {
                    Ok(asset) => {
                        let valid = match kind {
                            ApiKind::Trash => asset["isTrashed"].as_bool() == Some(true),
                            ApiKind::Restore => asset["isTrashed"].as_bool() == Some(false),
                            ApiKind::ForceDelete => false,
                            _ => {
                                return Err(AppError::internal("unexpected asset recovery effect"));
                            }
                        };
                        if !valid {
                            return Ok(false);
                        }
                    }
                    Err(crate::immich::api::ApiError::Status { status: 404, .. })
                        if *kind == ApiKind::ForceDelete => {}
                    Err(error) => return Err(immich(error)),
                }
            }
            Ok(true)
        }
    }
}

pub(crate) async fn verified_asset_at_root(
    state: &AppState,
    root: &files::Root,
    path: &ism_core::paths::RelPath,
    role: ism_core::dto::FileRole,
) -> AppResult<Option<String>> {
    let Some(server_id) = &root.server_id else {
        return Ok(None);
    };
    if role != ism_core::dto::FileRole::Original {
        return Ok(None);
    }
    let library = root.library_id.as_ref().ok_or_else(|| {
        AppError::conflict(
            "recovery_immich_verification",
            "root has no verified Immich library",
        )
    })?;
    let owner = registry::library_owner(state, library)
        .await?
        .ok_or_else(|| {
            AppError::conflict(
                "recovery_immich_verification",
                "library owner is unavailable",
            )
        })?;
    let server = registry::server(state, server_id).await?;
    let api = registry::api_as_user(state, &server, &owner).await?;
    let expected = root.immich_path(path).ok_or_else(|| {
        AppError::conflict(
            "recovery_immich_verification",
            "Immich path mapping is unavailable",
        )
    })?;
    let unavailable = |e: crate::immich::api::ApiError| {
        AppError::conflict("recovery_immich_unavailable", e.to_string())
    };
    let id = api
        .asset_id_by_path(&expected)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| {
            AppError::conflict(
                "recovery_immich_verification",
                "published file is awaiting verified Immich indexing",
            )
        })?;
    let asset = api.asset(&id).await.map_err(unavailable)?;
    if asset["originalPath"].as_str() != Some(&expected)
        || asset["libraryId"].as_str() != Some(library)
        || asset["ownerId"].as_str() != Some(&owner)
        || asset["isTrashed"].as_bool() != Some(false)
    {
        return Err(AppError::conflict(
            "recovery_immich_verification",
            "Immich asset does not match the recovered file",
        ));
    }
    Ok(Some(id))
}

/// Read-only proof of both disk absence and absence of live Immich original/sidecar references.
pub(crate) async fn verified_file_absence_at_root(
    state: &AppState,
    root: &files::Root,
    rel: &ism_core::paths::RelPath,
) -> AppResult<()> {
    root.ensure_available()?;
    files::parse_rel(rel.as_str())?;
    let path = root.abs(rel)?;
    if rel.is_root() || fs::symlink_metadata(&path).is_ok() {
        return Err(recovery_changed("expected absent file still exists"));
    }
    if RecoveryContent::read(&path)?.is_some() {
        return Err(recovery_changed("file absence is not verified"));
    }
    if let Some(server_id) = &root.server_id {
        let library = root
            .library_id
            .as_ref()
            .ok_or_else(|| recovery_changed("absence verification has no mapped Immich library"))?;
        let owner = registry::library_owner(state, library)
            .await?
            .ok_or_else(|| {
                recovery_changed("absence verification has no verified library owner")
            })?;
        let expected = root
            .immich_path(rel)
            .ok_or_else(|| recovery_changed("absence verification has no Immich path mapping"))?;
        let parent = rel.parent().unwrap_or_else(ism_core::paths::RelPath::root);
        let folder = root
            .immich_path(&parent)
            .ok_or_else(|| recovery_changed("absence verification has no Immich parent mapping"))?;
        let server = registry::server(state, server_id).await?;
        // Owner-scoped API search cannot prove absence of foreign-owner or sidecar references.
        let pool = registry::pg_pool(state, &server).await?;
        let mut tx = pool.begin().await?;
        for asset in pg::assets_under(&mut tx, &folder)
            .await?
            .into_iter()
            .filter(|asset| asset.original_path == expected)
        {
            verify_absent_asset(&asset, library, &owner)?;
        }
        for sidecar in pg::sidecars_under(&mut tx, &folder)
            .await?
            .into_iter()
            .filter(|sidecar| sidecar.path == expected)
        {
            let asset = pg::asset_by_id(&mut tx, &sidecar.asset_id)
                .await?
                .ok_or_else(|| {
                    recovery_changed("sidecar reference has no verifiable owning asset")
                })?;
            verify_absent_asset(&asset, library, &owner)?;
        }
        tx.commit().await?;
    }
    root.ensure_available()?;
    if root.abs(rel)? != path || RecoveryContent::read(&path)?.is_some() {
        return Err(recovery_changed(
            "file absence changed during Immich verification",
        ));
    }
    Ok(())
}

fn verify_absent_asset(asset: &pg::AssetRow, library: &str, owner: &str) -> AppResult<()> {
    if asset.library_id.as_deref() != Some(library) || asset.owner_id != owner || !asset.trashed {
        return Err(recovery_changed(
            "absent file has a live or foreign Immich reference",
        ));
    }
    Ok(())
}

async fn verify_db_effects(state: &AppState, server_id: &str, steps: &[DbStep]) -> AppResult<bool> {
    let server = registry::server(state, server_id).await?;
    let pool = registry::pg_pool(state, &server).await?;
    let mut tx = pool.begin().await?;
    for step in steps {
        match step {
            DbStep::MoveSidecar { file_id, from, to } => {
                let destination = pg::sidecar_by_path(&mut tx, to).await?;
                if destination.is_none_or(|row| row.id != *file_id)
                    || pg::sidecar_by_path(&mut tx, from).await?.is_some()
                {
                    return Ok(false);
                }
            }
            DbStep::RemoveSidecar { sidecar, asset } => {
                if !pg::sidecars_at_path(&mut tx, &sidecar.path)
                    .await?
                    .is_empty()
                    || pg::asset_by_id(&mut tx, &asset.id).await?.as_ref() != Some(asset)
                {
                    return Ok(false);
                }
            }
        }
    }
    tx.commit().await?;
    Ok(true)
}

async fn step_progress(state: &AppState, id: &str, item: i64, step: usize, status: &str) {
    let result: AppResult<()> = async {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT progress_json FROM operation_items WHERE operation_id = ? AND idx = ?",
        )
        .bind(id)
        .bind(item)
        .fetch_optional(&state.db)
        .await?
        .flatten();
        let Some(json) = json else {
            return Ok(());
        };
        let mut progress: ism_core::dto::OperationTaskProgress = serde_json::from_str(&json)?;
        if let Some(current) = progress.steps.get_mut(step) {
            current.status = status.to_owned();
        }
        sqlx::query(
            "UPDATE operation_items SET progress_json = ? WHERE operation_id = ? AND idx = ?",
        )
        .bind(serde_json::to_string(&progress)?)
        .bind(id)
        .bind(item)
        .execute(&state.db)
        .await?;
        state.emit(crate::state::ServerEvent::OperationUpdated {
            operation_id: id.to_owned(),
        });
        Ok(())
    }
    .await;
    if let Err(error) = result {
        tracing::error!(
            operation = id,
            item,
            step,
            "cannot persist live step progress: {}",
            error.message()
        );
    }
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
    let mut server = registry::server(state, server_id).await?;
    if server.external_library_move_mode == ImmichMoveMode::ApiWithDbFallback
        && server.external_library_api_capable != Some(true)
        && let Some(capable) = probe_external_library_capability(state, &server).await?
    {
        sqlx::query("UPDATE immich_servers SET external_library_api_capable = ? WHERE id = ?")
            .bind(capable as i64)
            .bind(server_id)
            .execute(&state.db)
            .await?;
        server.external_library_api_capable = Some(capable);
    }
    Ok(sync_plan_requires_queue_barrier(
        server.external_library_move_mode,
        server.external_library_api_capable,
        server_id,
        plan,
    ))
}

async fn probe_external_library_capability(
    state: &AppState,
    server: &registry::Server,
) -> AppResult<Option<bool>> {
    let candidates = match registry::admin_api_candidates(state, server).await {
        Ok(candidates) => candidates,
        Err(_) => return Ok(None),
    };
    for candidate in candidates {
        if admin_candidate_issue(&candidate).await.is_some() {
            continue;
        }
        match candidate.api.external_library_api_capable().await {
            Ok(capable) => return Ok(Some(capable)),
            Err(crate::immich::api::ApiError::Status { status: 403, .. }) => continue,
            Err(_) => return Ok(None),
        }
    }
    Ok(None)
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
                schedule_add_fallback(state, &server, library_id, path).await
            } else {
                match sync_file_add_with_admins(state, &server, library_id, path).await? {
                    Ok(()) => Ok(()),
                    Err(error)
                        if server.external_library_move_mode
                            == ImmichMoveMode::ApiWithDbFallback
                            && server.external_library_api_capable != Some(true)
                            && error.is_external_library_api_unavailable() =>
                    {
                        schedule_add_fallback(state, &server, library_id, path).await
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
                    (source_path, destination_path),
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
    path: &str,
) -> Result<(), crate::immich::api::ApiError> {
    let work = crate::watch::scheduler::Work::folder_for_file(path)
        .map_err(|error| crate::immich::api::ApiError::Decode(error.to_string()))?;
    crate::watch::scheduler::request_work(
        state,
        &server.id,
        library_id,
        "external-library file add API unavailable",
        &[work],
    )
    .await
    .map_err(|error| crate::immich::api::ApiError::Decode(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
async fn fallback_sync_move(
    state: &AppState,
    server: &registry::Server,
    destination_library_id: &Option<String>,
    (source_path, destination_path): (&str, &str),
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
        Step::CheckHash {
            path,
            expected_size,
            expected_sha256,
        } => {
            let path = path.clone();
            let actual = blocking(move || {
                let Some((revision, sha256)) = files::replacement_fingerprint(&path)? else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "file disappeared before hash verification",
                    ));
                };
                let metadata = std::fs::metadata(&path)?;
                if files::revision(&metadata) != revision {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "file changed while verifying its hash",
                    ));
                }
                Ok((metadata.len(), sha256))
            })
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::InvalidInput => {
                    Failure::new("revision_changed", "file changed while verifying its hash")
                }
                _ => io(e),
            })?;
            if actual.0 != *expected_size
                || !actual.1.eq_ignore_ascii_case(expected_sha256.as_str())
            {
                return Err(Failure::new(
                    "checksum_mismatch",
                    "file content does not match its expected SHA-256 checksum",
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
                blocking(move || Ok(files::move_verified(&from, &to)))
                    .await
                    .map_err(io)?
                    .map_err(|error| {
                        if error.recovery_required {
                            Failure {
                                code: "recovery_required".into(),
                                message: error.to_string(),
                                recovery: true,
                            }
                        } else {
                            io(error.error)
                        }
                    })?;
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
            blocking(move || Ok(files::move_verified(&from, &to)))
                .await
                .map_err(|e| format!("could not move back {label}: {e}"))?
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
            DbStep::RemoveSidecar { sidecar, asset } => {
                pg::remove_sidecar(&mut tx, sidecar, asset).await
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
            Step::CheckRevision { .. }
            | Step::CheckTree { .. }
            | Step::CheckHash { .. }
            | Step::Api(_) => {}
        }
    }
    out
}

async fn schedule_imports(state: &AppState, id: &str, plan: &Plan) -> AppResult<()> {
    use crate::watch::scheduler::{self, Work, WorkKind};

    let has_original = plan
        .files
        .iter()
        .any(|file| file.role == FileRole::Original);
    for file in &plan.files {
        if has_original && file.role != FileRole::Original {
            continue;
        }
        let Some(location) = &file.to else { continue };
        let root = files::root(&state.db, &location.root_id).await?;
        if let (Some(server), Some(library)) = (&root.server_id, &root.library_id) {
            let rel = files::parse_rel(&location.path)?;
            let path = root
                .immich_path(&rel)
                .ok_or_else(|| AppError::invalid_location("destination has no Immich path"))?;
            let metadata = fs::metadata(root.abs(&rel)?)?;
            let kind = if metadata.is_dir() {
                WorkKind::Folder
            } else if !has_original {
                WorkKind::Changed
            } else {
                WorkKind::Added
            };
            scheduler::request_work(
                state,
                server,
                library,
                &format!("operation {id}"),
                &[Work { path, kind }],
            )
            .await?;
        }
    }
    Ok(())
}

/// Whether file imports remain after the plan's direct API/database updates.
fn needs_scan(action: &OperationAction, plan: &Plan) -> bool {
    if plan.steps.iter().any(|step| {
        matches!(
            step,
            Step::Api(ApiStep {
                kind: ApiKind::SyncAdd { .. } | ApiKind::SyncMove { .. },
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

async fn execute(state: &AppState, op_id: &str, item: i64, plan: &Plan) -> Result<(), Failure> {
    if let Err(e) = crate::watch::expect_writes(state, &expected_writes(plan)).await {
        return Err(Failure::new(
            "internal",
            format!("cannot record the expected file changes: {}", e.message()),
        ));
    }
    let mut done = Vec::new();
    for (index, step) in plan.steps.iter().enumerate() {
        step_progress(state, op_id, item, index, "running").await;
        match apply(state, step).await {
            Ok(Some(u)) => done.push(u),
            Ok(None) => {}
            Err(f) if matches!(step, Step::Remove(_)) => {
                step_progress(state, op_id, item, index, "failed").await;
                return Err(f);
            }
            Err(f) => {
                step_progress(state, op_id, item, index, "failed").await;
                return Err(rollback(state, done, f).await);
            }
        }
        step_progress(state, op_id, item, index, "done").await;
    }
    for (index, (server_id, steps)) in plan.db.iter().enumerate() {
        let index = plan.steps.len() + index;
        step_progress(state, op_id, item, index, "running").await;
        if let Err(f) = apply_db(state, server_id, steps).await {
            step_progress(state, op_id, item, index, "failed").await;
            return Err(rollback(state, done, f).await);
        }
        step_progress(state, op_id, item, index, "done").await;
    }
    let index = plan.steps.len() + plan.db.len();
    step_progress(state, op_id, item, index, "running").await;
    if let Err(e) = bookkeeping(state, op_id, plan).await {
        step_progress(state, op_id, item, index, "failed").await;
        return Err(Failure {
            code: "recovery_required".into(),
            message: format!("operation bookkeeping failed: {}", e.message()),
            recovery: true,
        });
    }
    step_progress(state, op_id, item, index, "done").await;
    Ok(())
}

async fn bookkeeping(state: &AppState, op_id: &str, plan: &Plan) -> AppResult<()> {
    let now = db::now();
    if let Some(t) = &plan.trash_new {
        sqlx::query(
            "INSERT INTO trash (id, root_id, path, quarantine_rel, is_folder, size, trashed_at, operation_id, asset_ids_json, server_id, file_inventory_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET id=excluded.id
             WHERE root_id=excluded.root_id AND path=excluded.path
               AND quarantine_rel=excluded.quarantine_rel AND is_folder=excluded.is_folder
               AND size=excluded.size AND asset_ids_json=excluded.asset_ids_json
               AND server_id IS excluded.server_id AND file_inventory_json IS excluded.file_inventory_json",
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
        .await?
        .rows_affected().eq(&1).then_some(()).ok_or_else(
            ||AppError::conflict("recovery_changed","trash bookkeeping differs from the retained plan"),
        )?;
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
#[path = "recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use ism_core::dto::{Location, PlannedFile};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path},
    };

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    #[cfg_attr(
        target_os = "macos",
        ignore = "requires a separate filesystem in ISM_TEST_CROSS_DEVICE_DIR"
    )]
    async fn cross_device_step_rolls_back_when_a_later_step_fails() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(target_os = "linux")]
        let destination = tempfile::tempdir_in("/dev/shm").unwrap();
        #[cfg(target_os = "macos")]
        let destination =
            tempfile::tempdir_in(std::env::var_os("ISM_TEST_CROSS_DEVICE_DIR").unwrap()).unwrap();
        let pool = crate::db::connect(dir.path()).await.unwrap();
        let secrets = crate::crypto::SecretBox::open_or_create(dir.path()).unwrap();
        let state = AppState::new(crate::config::Config::for_tests(dir.path()), pool, secrets)
            .await
            .unwrap();
        let from = dir.path().join("album");
        let to = destination.path().join("album");
        fs::create_dir_all(from.join("empty")).unwrap();
        fs::write(from.join("photo.jpg"), b"synthetic photo").unwrap();
        fs::write(from.join("photo.jpg.xmp"), b"synthetic sidecar").unwrap();
        let moved = match apply(
            &state,
            &Step::Fs(plan::FsMove {
                from: from.clone(),
                to: to.clone(),
                copy: false,
            }),
        )
        .await
        {
            Ok(Some(undo)) => undo,
            _ => panic!("cross-device step failed"),
        };
        assert!(!from.exists());
        assert_eq!(fs::read(to.join("photo.jpg")).unwrap(), b"synthetic photo");
        let failure = rollback(
            &state,
            vec![moved],
            Failure::new("test_failure", "later step failed"),
        )
        .await;
        assert!(!failure.recovery);
        assert_eq!(failure.code, "test_failure");
        assert!(!to.exists());
        assert!(from.join("empty").is_dir());
        assert_eq!(
            fs::read(from.join("photo.jpg.xmp")).unwrap(),
            b"synthetic sidecar"
        );
    }

    #[tokio::test]
    async fn upload_publication_checks_hash_before_and_after_the_rename() {
        let dir = tempfile::tempdir().unwrap();
        let pool = crate::db::connect(dir.path()).await.unwrap();
        let secrets = crate::crypto::SecretBox::open_or_create(dir.path()).unwrap();
        let state = AppState::new(crate::config::Config::for_tests(dir.path()), pool, secrets)
            .await
            .unwrap();
        let staging = dir.path().join("staging-photo");
        let target = dir.path().join("published-photo");
        let contents = b"verified photo contents";
        std::fs::write(&staging, contents).unwrap();
        let expected_sha256 = crate::crypto::sha256_hex(contents);
        let check_staging = Step::CheckHash {
            path: staging.clone(),
            expected_size: contents.len() as u64,
            expected_sha256: expected_sha256.clone(),
        };

        let before = Step::CheckHash {
            path: staging.clone(),
            expected_size: contents.len() as u64,
            expected_sha256: crate::crypto::sha256_hex(b"different photo"),
        };
        let error = match apply(&state, &before).await {
            Ok(_) => panic!("mismatched staging hash was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, "checksum_mismatch");
        assert_eq!(std::fs::read(&staging).unwrap(), contents);

        assert!(apply(&state, &check_staging).await.is_ok());
        let move_file = match apply(
            &state,
            &Step::Fs(plan::FsMove {
                from: staging.clone(),
                to: target.clone(),
                copy: false,
            }),
        )
        .await
        {
            Ok(Some(undo)) => undo,
            Ok(None) => panic!("move did not return rollback information"),
            Err(_) => panic!("move failed"),
        };
        std::fs::write(&target, b"tampered photo contents").unwrap();
        let check_target = Step::CheckHash {
            path: target.clone(),
            expected_size: contents.len() as u64,
            expected_sha256,
        };
        let error = match apply(&state, &check_target).await {
            Ok(_) => panic!("mismatched published hash was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, "checksum_mismatch");

        undo(&state, move_file).await.unwrap();
        assert!(!target.exists());
        assert_eq!(std::fs::read(&staging).unwrap(), b"tampered photo contents");
    }

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

    #[tokio::test]
    async fn publish_upload_reprobes_unknown_capability_before_waiting_for_queue() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/api-keys/me"))
            .and(header("x-api-key", "valid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "key-1",
                "permissions": ["library.update"]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/users/me"))
            .and(header("x-api-key", "valid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "user-1",
                "isAdmin": true
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/libraries/invalid/files/move"))
            .and(header("x-api-key", "valid"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "message": ["Invalid UUID"]
            })))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let pool = crate::db::connect(dir.path()).await.unwrap();
        let secrets = crate::crypto::SecretBox::open_or_create(dir.path()).unwrap();
        let state = AppState::new(crate::config::Config::for_tests(dir.path()), pool, secrets)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO immich_servers (id, name, url, external_library_move_mode, \
             maintenance_json, created_at) VALUES ('server-1', 'Immich', ?, 'api_with_db_fallback', ?, ?)",
        )
        .bind(server.uri())
        .bind(
            serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap(),
        )
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO immich_accounts (id, server_id, label, email, immich_user_id, \
             is_admin, api_key_enc, status, created_at) \
             VALUES ('account-1', 'server-1', 'Admin', 'admin@example.test', 'user-1', 1, ?, 'ok', ?)",
        )
        .bind(
            state
                .secrets
                .encrypt(crate::immich::registry::API_KEY_PURPOSE, "valid")
                .unwrap(),
        )
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();

        let plan = api_sync_plan(ApiKind::SyncAdd {
            library_id: "library-1".to_owned(),
            path: "/library/retry-move.png".to_owned(),
        });
        assert!(
            !plan_requires_queue_barrier(&state, "server-1", &plan)
                .await
                .unwrap()
        );
        let capable: Option<i64> = sqlx::query_scalar(
            "SELECT external_library_api_capable FROM immich_servers WHERE id = 'server-1'",
        )
        .fetch_one(&state.db)
        .await
        .unwrap();
        assert_eq!(capable, Some(1));
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
        let Err(failure) = execute(&state, "unused", 0, &plan).await else {
            panic!("changed folder should fail verification");
        };
        assert_eq!(failure.code, "revision_changed");
        assert!(!failure.recovery);
        assert_eq!(fs::read(source.join("photo.jpg")).unwrap(), b"changed");
        assert!(!target.exists());
    }
}
