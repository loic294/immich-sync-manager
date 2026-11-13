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
    Change, ClientAction, ItemStatus, OperationAction, OperationCommitted, OperationStatus,
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
                let _ = sqlx::query("UPDATE operations SET status = 'failed', finished_at = ? WHERE id = ? AND status IN ('queued', 'waiting_maintenance')")
                    .bind(db::now())
                    .bind(&id)
                    .execute(&state.db)
                    .await;
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
    sqlx::query(
        "UPDATE operations SET status = ?, finished_at = COALESCE(?, finished_at) WHERE id = ?",
    )
    .bind(status.as_str())
    .bind(finished)
    .bind(id)
    .execute(&state.db)
    .await?;
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
    let principal = match principal_from_key(state, &principal_key).await {
        Ok(p) => p,
        Err(e) => {
            fail_all(state, id, e.code(), e.message()).await?;
            return Ok(());
        }
    };
    let ctx = plan::Ctx::new(state, &principal, allow_owner_transfer).await?;

    // Maintenance admission for every Immich server the operation touches.
    let mut servers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for action in &actions {
        for (server, library) in ctx.touched_servers(action).await? {
            let libs = servers.entry(server).or_default();
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
    for (server, libs) in &servers {
        if let Err(e) = crate::maintenance::acquire(state, server, libs).await {
            for s in &admitted {
                crate::maintenance::release(state, s).await?;
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
                    recovery += 1;
                    for s in &admitted {
                        crate::maintenance::report_recovery(
                            state,
                            s,
                            &format!("operation {id}: {message}"),
                        )
                        .await?;
                    }
                } else {
                    failed += 1;
                }
                set_item(state, id, index, status, Some((&code, &message)), None).await?;
            }
        }
    }
    for s in &admitted {
        crate::maintenance::release(state, s).await?;
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
    crate::peers::operation_finished(state, id, &local_changes).await;
    set_status(state, id, status).await?;
    if !changes.is_empty() {
        state.emit(ServerEvent::OperationCommitted(OperationCommitted {
            operation_id: id.to_owned(),
            changes,
        }));
    }
    Ok(())
}

/// What a completed item changed, for replication to peers. New files (restores, uploads) and
/// trashed files are found by the next share scan instead.
fn local_changes_of(action: &OperationAction, plan: &Plan) -> Vec<LocalChange> {
    match action {
        OperationAction::Move { .. } | OperationAction::Rename { .. } => plan
            .files
            .iter()
            .filter(|f| !f.copy)
            .filter_map(|f| {
                Some(LocalChange::FileMoved {
                    from: f.from.clone()?,
                    to: f.to.clone()?,
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
        OperationAction::TrashFolder { source } => {
            vec![LocalChange::FolderTrashed { at: source.clone() }]
        }
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

async fn call_api(state: &AppState, step: &ApiStep) -> AppResult<()> {
    let server = registry::server(state, &step.server_id).await?;
    let api = registry::api_as_user(state, &server, &step.owner).await?;
    let result = match step.kind {
        ApiKind::Trash => api.delete_assets(&step.ids, false).await,
        ApiKind::Restore => api.restore_assets(&step.ids).await,
        ApiKind::ForceDelete => api.delete_assets(&step.ids, true).await,
    };
    result.map_err(|e| AppError::conflict("immich_error", crate::error::redact(&e.to_string())))
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
            let undo = match a.kind {
                ApiKind::Trash => Some(ApiKind::Restore),
                ApiKind::Restore => Some(ApiKind::Trash),
                ApiKind::ForceDelete => None,
            };
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
            pg::PgError::Db(_) => "database_error",
        };
        Failure::new(code, crate::error::redact(&e.to_string()))
    };
    let mut tx = pool.begin().await.map_err(|e| db_err(e.into()))?;
    for step in steps {
        match step {
            DbStep::MoveAsset { asset_id, from, to } => {
                pg::move_asset(&mut tx, asset_id, from, to).await
            }
            DbStep::MoveSidecar { file_id, from, to } => {
                pg::move_sidecar(&mut tx, file_id, from, to).await
            }
            DbStep::ChangeLibrary {
                asset_id,
                from_owner,
                to_owner,
                to_library,
            } => pg::transfer_owner(&mut tx, asset_id, from_owner, to_owner, to_library).await,
        }
        .map_err(db_err)?;
    }
    tx.commit().await.map_err(|e| db_err(e.into()))?;
    Ok(())
}

async fn execute(state: &AppState, op_id: &str, plan: &Plan) -> Result<(), Failure> {
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
        // Files and Immich are consistent; only ISM's own records lag behind.
        tracing::error!("bookkeeping after operation {op_id}: {}", e.message());
    }
    Ok(())
}

async fn bookkeeping(state: &AppState, op_id: &str, plan: &Plan) -> AppResult<()> {
    let now = db::now();
    if let Some(t) = &plan.trash_new {
        sqlx::query(
            "INSERT INTO trash (id, root_id, path, quarantine_rel, is_folder, size, trashed_at, operation_id, asset_ids_json, server_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
