//! Old-source restoration is a continuation of the failed folder operation, not a new upload
//! operation. Its immutable authenticated inventory is independent of scheduler evidence.

use std::collections::BTreeMap;

use ism_core::dto::{
    ConflictResolution, ConflictSide, CreateUpload, FileRole, OperationAction, OperationRequest,
};
use ism_core::paths::RelPath;
use serde::{Deserialize, Serialize};

use super::move_decisions::{self, Evidence};
use crate::error::{AppError, AppResult};
use crate::files;
use crate::peers::{self, Share};
use crate::state::AppState;

#[derive(Serialize, Deserialize)]
struct Restoration {
    operation_id: String,
    share_id: String,
    peer_id: String,
    original_request: String,
    source: String,
    target: String,
    remote: Evidence,
    uploads: BTreeMap<String, String>,
    #[serde(default)]
    conflict_id: Option<String>,
    #[serde(default)]
    generations: Vec<Generation>,
    #[serde(default)]
    evidence_generation: u64,
}

#[derive(Serialize, Deserialize)]
struct Generation {
    resolution: ConflictResolution,
    remote: Evidence,
    #[serde(default)]
    local_inventory: Option<BTreeMap<String, (Option<String>, u64)>>,
    #[serde(default)]
    children: Vec<String>,
    #[serde(default)]
    goals: BTreeMap<String, String>,
}

fn inventory(evidence: &Evidence) -> BTreeMap<String, (Option<String>, u64)> {
    let relative = |path: &str| {
        path.strip_prefix(&evidence.location)
            .unwrap_or(path)
            .to_owned()
    };
    evidence
        .directories
        .iter()
        .map(|path| (relative(path), (None, 0)))
        .chain(
            evidence
                .files
                .iter()
                .map(|file| (relative(&file.path), (Some(file.sha256.clone()), file.size))),
        )
        .collect()
}

async fn local_inventory(
    state: &AppState,
    share: &Share,
    path: &str,
) -> AppResult<BTreeMap<String, (Option<String>, u64)>> {
    let version = peers::versions::local(state, share, path).await?;
    if version.state != "folder" {
        return Err(unverified("the reviewed receiving folder is unavailable"));
    }
    let relative = |value: &str| {
        value
            .strip_prefix(path)
            .map(|part| part.trim_end_matches('/').to_owned())
            .ok_or_else(|| unverified("receiving inventory escaped its folder"))
    };
    version
        .directories
        .iter()
        .map(|path| Ok((relative(path)?, (None, 0))))
        .chain(version.files.iter().map(|file| {
            Ok((
                relative(&file.path)?,
                (
                    Some(file.sha256.clone()),
                    file.size
                        .ok_or_else(|| unverified("receiving file size is unavailable"))?,
                ),
            ))
        }))
        .collect()
}

async fn save(state: &AppState, plan: &Restoration) -> AppResult<()> {
    sqlx::query(
        "UPDATE pending_peer_ops SET vv_json=?,conflict_id=? WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(serde_json::to_string(plan)?)
    .bind(&plan.conflict_id)
    .bind(format!("folder-restore:{}", plan.operation_id))
    .execute(&state.db)
    .await?;
    Ok(())
}

fn context(plan: &Restoration, share: Share) -> super::Conflict {
    super::Conflict {
        id: plan.operation_id.clone(),
        share,
        rel: plan.target.clone(),
        kind: "folder_move".into(),
        remote: ConflictSide {
            instance: String::new(),
            sha256: None,
            size: None,
            deleted: false,
            modified_at: None,
        },
        remote_vv: plan.remote.version.clone(),
        peer_id: plan.peer_id.clone(),
        status: "resolving".into(),
        remote_from: Some(plan.source.clone()),
    }
}

async fn request_choice(
    state: &AppState,
    plan: &mut Restoration,
    current: &Evidence,
    force: bool,
) -> AppResult<()> {
    let share = bound_share(state, plan).await?;
    // Materialize authenticated retained bytes only at the missing original source.
    // This preserves a version for comparison; the final move still requires approval.
    let completed: bool =
        sqlx::query_scalar("SELECT status='completed' FROM operations WHERE id=?")
            .bind(&plan.operation_id)
            .fetch_one(&state.db)
            .await?;
    if !completed {
        let expected_plan = serde_json::to_string(&plan)?;
        for upload in plan.uploads.values() {
            crate::ops::exec::recover_folder_prerequisite_upload_for_plan(
                state,
                &plan.operation_id,
                upload,
                &expected_plan,
            )
            .await?;
        }
    }
    let _guard = state.replication_operation_lock.lock().await;
    let latest = load(state, &plan.operation_id)
        .await?
        .ok_or_else(|| unverified("restoration disappeared while refreshing evidence"))?;
    if serde_json::to_value(&latest)? != serde_json::to_value(&*plan)? {
        return Err(AppError::conflict(
            "recovery_evidence_changed",
            "the restoration changed while evidence was being refreshed",
        ));
    }
    *plan = latest;
    if !force
        && plan.generations.last().is_some_and(|generation| {
            generation.remote.location == current.location
                && inventory(&generation.remote) == inventory(current)
        })
    {
        return Ok(());
    }
    plan.evidence_generation = plan
        .evidence_generation
        .checked_add(1)
        .ok_or_else(|| unverified("restoration evidence generation exhausted"))?;
    if let Some(id) = &plan.conflict_id {
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        let (status, recovery_state): (String, Option<String>) = sqlx::query_as(
            "SELECT c.status,r.state FROM conflicts c
             LEFT JOIN sync_recovery r ON r.kind='conflict' AND r.target_id=c.id WHERE c.id=?",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        if status != "open" && recovery_state.as_deref() == Some("fixing") {
            return Err(AppError::conflict(
                "recovery_decision_in_progress",
                "the previous version decision is still executing",
            ));
        }
        sqlx::query(
            "UPDATE conflicts SET status='open',resolution=NULL,resolved_at=NULL,remote_vv_json=? WHERE id=?",
        )
        .bind(serde_json::to_string(&current.version)?)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if status != "open" {
            sqlx::query(
                "UPDATE sync_recovery SET state='needs_decision',resolution=NULL,decision_revision=NULL,
                 code='choose_version',detail='Review the new version evidence in ISM',updated_at=?
                 WHERE kind='conflict' AND target_id=?",
            )
            .bind(crate::db::now())
            .bind(id)
            .execute(&mut *tx)
            .await?;
        }
        let updated = sqlx::query(
            "UPDATE pending_peer_ops SET vv_json=? WHERE operation_id=? AND kind='folder_restore'",
        )
        .bind(serde_json::to_string(&plan)?)
        .bind(format!("folder-restore:{}", plan.operation_id))
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(unverified("restoration disappeared while reopening"));
        }
        tx.commit().await?;
    } else {
        peers::conflict(
            state,
            peers::ConflictInput {
                share: &share,
                rel: &plan.target,
                kind: "folder_restore",
                detail: "Choose between the verified retained restoration and current peer files"
                    .into(),
                remote: context(plan, share.clone()).remote,
                remote_vv: &current.version,
                peer_id: &plan.peer_id,
                from: Some(&plan.source),
            },
        )
        .await?;
        plan.conflict_id = sqlx::query_scalar(
            "SELECT id FROM conflicts WHERE share_id=? AND rel_path=? AND kind='folder_restore' AND status='open'",
        )
        .bind(&plan.share_id)
        .bind(&plan.target)
        .fetch_optional(&state.db)
        .await?;
        save(state, plan).await?;
    }
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery_links(recovery_id,kind,target_id)
         SELECT id,'conflict',? FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(&plan.conflict_id)
    .bind(&plan.operation_id)
    .execute(&state.db)
    .await?;
    sqlx::query(
        "UPDATE sync_recovery SET state='needs_decision',code='choose_version',
         detail='Peer tree changed; choose the retained version, current peer version, or both in ISM',updated_at=?
         WHERE kind='operation' AND target_id=? AND state!='fixed'",
    )
    .bind(crate::db::now())
    .bind(&plan.operation_id)
    .execute(&state.db)
    .await?;
    state.emit(crate::state::ServerEvent::ConflictsChanged);
    Ok(())
}

pub(in crate::peers) async fn approve(
    state: &AppState,
    actor: &str,
    conflict: &super::Conflict,
    resolution: ConflictResolution,
    revision: Option<&str>,
) -> AppResult<()> {
    if resolution == ConflictResolution::Skip {
        return Err(unverified(
            "a retained restoration still has executable version choices",
        ));
    }
    let raw: String = sqlx::query_scalar(
        "SELECT vv_json FROM pending_peer_ops WHERE kind='folder_restore' AND conflict_id=?",
    )
    .bind(&conflict.id)
    .fetch_one(&state.db)
    .await?;
    let plan: Restoration = serde_json::from_str(&raw)?;
    let share = bound_share(state, &plan).await?;
    let current =
        move_decisions::evidence_at(state, &context(&plan, share.clone()), Some(&plan.remote))
            .await?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let _guard = state.replication_operation_lock.lock().await;
    if let Some(revision) = revision {
        let files: Vec<_> = current
            .files
            .iter()
            .map(|file| (file.path.clone(), file.sha256.clone(), file.size))
            .collect();
        peers::versions::validate_reviewed(
            state,
            &conflict.id,
            revision,
            None,
            Some((&current.location, "folder", &files, &current.directories)),
        )
        .await?;
    }
    let latest = load(state, &plan.operation_id)
        .await?
        .ok_or_else(|| unverified("restoration disappeared during approval"))?;
    if serde_json::to_value(&latest)? != serde_json::to_value(&plan)? {
        return Err(AppError::conflict(
            "recovery_evidence_changed",
            "the restoration changed while the decision was being verified",
        ));
    }
    let mut plan = latest;
    let completed: bool =
        sqlx::query_scalar("SELECT status='completed' FROM operations WHERE id=?")
            .bind(&plan.operation_id)
            .fetch_one(&state.db)
            .await?;
    if !completed {
        for old in &plan.remote.files {
            let target = move_decisions::rebase(&old.path, &plan.remote.location, &plan.source)?;
            let restored = root.abs(&share.root_rel(&target)?)?;
            let staged = plan
                .uploads
                .get(&target)
                .map(|id| crate::ops::plan::staging_dir(&root, id).map(|path| path.join("0")))
                .transpose()?;
            let retained = [Some(restored), staged].into_iter().flatten().any(|path| {
                files::replacement_fingerprint(&path)
                    .is_ok_and(|value| value.is_some_and(|(_, sha)| sha == old.sha256))
            });
            if !retained
                && !current.files.iter().any(|file| {
                    file.path == old.path && file.sha256 == old.sha256 && file.size == old.size
                })
            {
                return Err(unverified(
                    "the original version has no complete verified retained bytes or matching peer source",
                ));
            }
        }
    }
    if conflict.status != "open" {
        return Err(unverified("restoration choice is already being applied"));
    }
    if let Some(previous) = plan.generations.last() {
        for child in &previous.children {
            let done: bool =
                sqlx::query_scalar("SELECT status='resolved' FROM conflicts WHERE id=?")
                    .bind(child)
                    .fetch_one(&state.db)
                    .await?;
            if !done {
                return Err(unverified(
                    "previous managed file choice must finish or recover first",
                ));
            }
        }
    }
    let local_inventory = local_inventory(
        state,
        &share,
        if completed {
            &plan.target
        } else {
            &plan.source
        },
    )
    .await?;
    plan.generations.push(Generation {
        resolution,
        remote: current,
        local_inventory: Some(local_inventory),
        children: Vec::new(),
        goals: BTreeMap::new(),
    });
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let updated = sqlx::query(
        "UPDATE conflicts SET status='resolving',resolution=? WHERE id=? AND status='open'",
    )
    .bind(match resolution {
        ConflictResolution::KeepLocal => "keep_local",
        ConflictResolution::KeepRemote => "keep_remote",
        ConflictResolution::KeepBoth => "keep_both",
        ConflictResolution::Skip => unreachable!(),
    })
    .bind(&conflict.id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(unverified("restoration choice changed during admission"));
    }
    sqlx::query(
        "UPDATE pending_peer_ops SET vv_json=? WHERE kind='folder_restore' AND conflict_id=?",
    )
    .bind(serde_json::to_string(&plan)?)
    .bind(&conflict.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::db::audit(&state.db, actor, "folder_restore.approve", &conflict.id).await;
    state.sync_wake.notify_waiters();
    Ok(())
}

pub(crate) struct VerifiedFolderRestore {
    pub root_id: String,
    pub source: RelPath,
    pub target: RelPath,
    pub directories: Vec<RelPath>,
    pub files: Vec<(RelPath, String, u64)>,
}

fn unverified(message: &str) -> AppError {
    AppError::conflict("recovery_evidence_missing", message)
}

async fn load(state: &AppState, id: &str) -> AppResult<Option<Restoration>> {
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT vv_json FROM pending_peer_ops WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(format!("folder-restore:{id}"))
    .fetch_optional(&state.db)
    .await?;
    raw.map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
}

pub(crate) async fn restoration_plan_matches_locked(
    state: &AppState,
    operation_id: &str,
    expected: &str,
) -> AppResult<bool> {
    let expected: Restoration = serde_json::from_str(expected)?;
    let Some(current) = load(state, operation_id).await? else {
        return Ok(false);
    };
    Ok(serde_json::to_value(current)? == serde_json::to_value(expected)?)
}

pub(crate) async fn restoration_resume_allowed_locked(
    state: &AppState,
    operation_id: &str,
    expected: &str,
) -> AppResult<bool> {
    let plan: Restoration = serde_json::from_str(expected)?;
    if plan.operation_id != operation_id
        || !restoration_plan_matches_locked(state, operation_id, expected).await?
    {
        return Ok(false);
    }
    match &plan.conflict_id {
        None => Ok(plan.generations.is_empty()),
        Some(id) if !plan.generations.is_empty() => Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM conflicts WHERE id=? AND status='resolving')",
        )
        .bind(id)
        .fetch_one(&state.db)
        .await?),
        Some(_) => Ok(false),
    }
}

async fn bound_share(state: &AppState, plan: &Restoration) -> AppResult<Share> {
    let (principal, request): (String, String) =
        sqlx::query_as("SELECT principal,request_json FROM operations WHERE id=?")
            .bind(&plan.operation_id)
            .fetch_one(&state.db)
            .await?;
    if principal != format!("peer:{}", plan.peer_id) || request != plan.original_request {
        return Err(unverified("original folder operation changed"));
    }
    let share = peers::shares(state)
        .await?
        .into_iter()
        .find(|s| s.id == plan.share_id && s.includes(&plan.peer_id))
        .ok_or_else(|| unverified("restoration share is unavailable"))?;
    let request: OperationRequest = serde_json::from_str(&request)?;
    if !matches!(
        request.actions.as_slice(),
        [OperationAction::MoveFolder { source, target }]
            if share.rel_of(source).as_deref() == Some(&plan.source)
                && share.rel_of(target).as_deref() == Some(&plan.target)
    ) || plan.remote.event_identity.is_none()
    {
        return Err(unverified(
            "restoration is not bound to the original folder move",
        ));
    }
    Ok(share)
}

pub(crate) async fn verified_folder_restore(
    state: &AppState,
    operation_id: &str,
) -> AppResult<VerifiedFolderRestore> {
    let plan = load(state, operation_id)
        .await?
        .ok_or_else(|| unverified("folder restoration manifest is missing"))?;
    let share = bound_share(state, &plan).await?;
    let local = |path: &str| {
        share.root_rel(&move_decisions::rebase(
            path,
            &plan.remote.location,
            &plan.source,
        )?)
    };
    Ok(VerifiedFolderRestore {
        root_id: share.root_id.clone(),
        source: share.root_rel(&plan.source)?,
        target: share.root_rel(&plan.target)?,
        directories: plan
            .remote
            .directories
            .iter()
            .map(|p| local(p))
            .collect::<AppResult<_>>()?,
        files: plan
            .remote
            .files
            .iter()
            .map(|f| Ok((local(&f.path)?, f.sha256.clone(), f.size)))
            .collect::<AppResult<_>>()?,
    })
}

pub(crate) async fn verified_restore_upload(
    state: &AppState,
    operation_id: &str,
    upload_id: &str,
) -> AppResult<CreateUpload> {
    let plan = load(state, operation_id)
        .await?
        .ok_or_else(|| unverified("folder restoration manifest is missing"))?;
    let share = bound_share(state, &plan).await?;
    let (principal, raw, status): (String, String, String) =
        sqlx::query_as("SELECT principal,request_json,status FROM uploads WHERE id=?")
            .bind(upload_id)
            .fetch_one(&state.db)
            .await?;
    let request: CreateUpload = serde_json::from_str(&raw)?;
    let [file] = request.files.as_slice() else {
        return Err(unverified(
            "restoration upload must contain exactly one file",
        ));
    };
    let rel = files::parse_rel(&request.target.path)?
        .join(&file.name)
        .map_err(|e| AppError::invalid_location(e.to_string()))?;
    let manifest = verified_folder_restore(state, operation_id).await?;
    let role = match ism_core::sidecar::companion_role(&file.name, &state.sidecar_extensions()) {
        Some(ism_core::sidecar::CompanionRole::Sidecar) => FileRole::Sidecar,
        Some(ism_core::sidecar::CompanionRole::Companion) => FileRole::Companion,
        None => FileRole::Original,
    };
    if principal != format!("peer:{}", plan.peer_id)
        || !matches!(status.as_str(), "receiving" | "committed")
        || request.target.root_id != share.root_id
        || !plan.uploads.values().any(|id| id == upload_id)
        || file.expected_revision.is_some()
        || file.expected_sha256.is_some()
        || request.existing_original.is_some()
        || file.role != role
        || !manifest
            .files
            .iter()
            .any(|(path, sha, size)| *path == rel && *sha == file.sha256 && *size == file.size)
    {
        return Err(unverified(
            "upload disagrees with the authenticated source restoration",
        ));
    }
    Ok(request)
}

pub(crate) async fn verify_restored_folder_source(
    state: &AppState,
    operation_id: &str,
) -> AppResult<()> {
    let manifest = verified_folder_restore(state, operation_id).await?;
    let root = files::root(&state.db, &manifest.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&manifest.source)?;
    let relative = |path: &RelPath| {
        if *path == manifest.source {
            Ok(String::new())
        } else {
            path.as_str()
                .strip_prefix(&format!("{}/", manifest.source))
                .map(str::to_owned)
                .ok_or_else(|| unverified("restoration file escaped its source"))
        }
    };
    let expected: BTreeMap<String, Option<String>> = manifest
        .directories
        .iter()
        .map(|p| Ok((relative(p)?, None)))
        .chain(
            manifest
                .files
                .iter()
                .map(|(p, sha, _)| Ok((relative(p)?, Some(sha.clone())))),
        )
        .collect::<AppResult<_>>()?;
    let actual: BTreeMap<String, Option<String>> = crate::ops::plan::folder_inventory(&abs)?
        .into_iter()
        .map(|entry| (entry.name, entry.sha256))
        .collect();
    if actual != expected {
        return Err(unverified(
            "restored folder does not match the complete frozen inventory",
        ));
    }
    for (path, _, size) in manifest.files {
        if root.abs(&path)?.symlink_metadata()?.len() != size {
            return Err(unverified("restored file size changed"));
        }
    }
    Ok(())
}

pub(in crate::peers) async fn transfer_parent(
    state: &AppState,
    transfer_id: &str,
) -> AppResult<Option<String>> {
    type R = (String, String, String, String, String, i64);
    let r: Option<R> = sqlx::query_as(
        "SELECT restore_parent_operation_id,peer_id,share_id,rel_path,sha256,size
         FROM transfers WHERE id=? AND restore_parent_operation_id IS NOT NULL",
    )
    .bind(transfer_id)
    .fetch_optional(&state.db)
    .await?;
    let Some((parent, peer, share, target, sha, size)) = r else {
        return Ok(None);
    };
    let plan = load(state, &parent)
        .await?
        .ok_or_else(|| unverified("restoration transfer has no manifest"))?;
    bound_share(state, &plan).await?;
    if plan.peer_id != peer
        || plan.share_id != share
        || !plan.remote.files.iter().any(|f| {
            f.sha256 == sha
                && i64::try_from(f.size).ok() == Some(size)
                && move_decisions::rebase(&f.path, &plan.remote.location, &plan.source)
                    .is_ok_and(|path| path == target)
        })
    {
        return Err(unverified(
            "transfer disagrees with the frozen restoration manifest",
        ));
    }
    Ok(Some(parent))
}

pub(in crate::peers) async fn restore_missing_folder(
    state: &AppState,
    operation_id: &str,
) -> AppResult<bool> {
    if load(state, operation_id).await?.is_some() {
        return Ok(true);
    }
    type R = (String, String, String, String, String);
    let pending: Option<R> = sqlx::query_as(
        "SELECT share_id,peer_id,from_path,to_path,vv_json FROM pending_peer_ops
         WHERE operation_id=? AND kind='folder_move' LIMIT 1",
    )
    .bind(operation_id)
    .fetch_optional(&state.db)
    .await?;
    let Some((share_id, peer_id, source, target, vv)) = pending else {
        return Ok(false);
    };
    let share = peers::shares(state)
        .await?
        .into_iter()
        .find(|s| s.id == share_id)
        .ok_or_else(|| unverified("folder recovery share is missing"))?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let missing = |path: &str| -> AppResult<bool> {
        match root.abs(&share.root_rel(path)?)?.symlink_metadata() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e.into()),
            Ok(_) => Ok(false),
        }
    };
    if !missing(&source)? || !missing(&target)? {
        return Ok(false);
    }
    let original_request: String =
        sqlx::query_scalar("SELECT request_json FROM operations WHERE id=?")
            .bind(operation_id)
            .fetch_one(&state.db)
            .await?;
    let c = super::Conflict {
        id: operation_id.into(),
        share: share.clone(),
        rel: target.clone(),
        kind: "folder_move".into(),
        remote: ConflictSide {
            instance: String::new(),
            sha256: None,
            size: None,
            deleted: false,
            modified_at: None,
        },
        remote_vv: serde_json::from_str(&vv)?,
        peer_id: peer_id.clone(),
        status: "resolving".into(),
        remote_from: Some(source.clone()),
    };
    // Authenticate the whole current tree before taking the local replication lock.
    let remote = move_decisions::evidence(state, &c).await?;
    let journaled: bool = sqlx::query_scalar(
        "SELECT COALESCE(json_type(journal_json,'$.execution_steps')='array',0)
         FROM operation_items WHERE operation_id=? AND idx=0",
    )
    .bind(operation_id)
    .fetch_one(&state.db)
    .await?;
    if !journaled {
        for file in &remote.files {
            let final_path = move_decisions::rebase(&file.path, &remote.location, &target)?;
            crate::ops::exec::verified_file_absence_at_root(
                state,
                &root,
                &c.share.root_rel(&final_path)?,
            )
            .await?;
        }
    }
    let plan = Restoration {
        operation_id: operation_id.into(),
        share_id,
        peer_id,
        original_request,
        source,
        target,
        remote,
        uploads: BTreeMap::new(),
        conflict_id: None,
        generations: Vec::new(),
        evidence_generation: 0,
    };
    bound_share(state, &plan).await?;
    let _guard = state.replication_operation_lock.lock().await;
    if !missing(&plan.source)? || !missing(&plan.target)? {
        return Err(unverified(
            "local folder changed during evidence collection",
        ));
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let sentinel = format!("folder-restore:{operation_id}");
    sqlx::query(
        "INSERT OR IGNORE INTO pending_peer_ops
         (operation_id,idx,share_id,kind,from_path,to_path,vv_json,peer_id)
         VALUES (?,0,?,'folder_restore',?,?,?,?)",
    )
    .bind(&sentinel)
    .bind(&plan.share_id)
    .bind(&plan.source)
    .bind(&plan.target)
    .bind(serde_json::to_string(&plan)?)
    .bind(&plan.peer_id)
    .execute(&mut *tx)
    .await?;
    // A missing-operation sentinel prevents prerequisite downloads being mistaken for final
    // move completion. The authoritative original operation remains the final target.
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery_links(recovery_id,kind,target_id)
         SELECT id,'operation',? FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(&sentinel)
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    for file in &plan.remote.files {
        let target = move_decisions::rebase(&file.path, &plan.remote.location, &plan.source)?;
        let mut vv = file.vv.clone();
        if let Some(local) = peers::row_in(&mut tx, &plan.share_id, &target).await? {
            vv.merge(&local.vv);
        }
        peers::queue_transfer_in(
            &mut tx,
            &plan.peer_id,
            &c.share,
            &target,
            &file.sha256,
            file.size,
            &vv,
        )
        .await?;
        sqlx::query(
            "UPDATE transfers SET source_rel_path=?,source_vv_json=?,restore_parent_operation_id=?
             WHERE share_id=? AND peer_id=? AND rel_path=? AND status='queued'",
        )
        .bind(&file.path)
        .bind(serde_json::to_string(&file.vv)?)
        .bind(operation_id)
        .bind(&plan.share_id)
        .bind(&plan.peer_id)
        .bind(&target)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO sync_recovery_links(recovery_id,kind,target_id)
             SELECT r.id,'transfer',t.id FROM sync_recovery r JOIN transfers t ON t.restore_parent_operation_id=r.target_id
             WHERE r.kind='operation' AND r.target_id=?",
        ).bind(operation_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    state.sync_wake.notify_waiters();
    Ok(true)
}

pub(in crate::peers) async fn record_upload_in(
    conn: &mut sqlx::SqliteConnection,
    operation_id: &str,
    target: &str,
    upload_id: &str,
) -> AppResult<()> {
    let sentinel = format!("folder-restore:{operation_id}");
    let raw: String = sqlx::query_scalar(
        "SELECT vv_json FROM pending_peer_ops WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(&sentinel)
    .fetch_one(&mut *conn)
    .await?;
    let mut plan: Restoration = serde_json::from_str(&raw)?;
    plan.uploads.insert(target.into(), upload_id.into());
    sqlx::query(
        "UPDATE pending_peer_ops SET vv_json=? WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(serde_json::to_string(&plan)?)
    .bind(sentinel)
    .execute(conn)
    .await?;
    Ok(())
}

pub(in crate::peers) async fn process(state: &AppState) -> AppResult<()> {
    let raw: Vec<String> =
        sqlx::query_scalar("SELECT vv_json FROM pending_peer_ops WHERE kind='folder_restore'")
            .fetch_all(&state.db)
            .await?;
    for raw in raw {
        let plan: Restoration = serde_json::from_str(&raw)?;
        if let Err(error) = process_one(state, &plan).await {
            tracing::debug!(operation=%plan.operation_id, "folder restoration waits: {}", error.message());
        }
    }
    Ok(())
}

pub(in crate::peers) async fn validate_current_manifest(
    state: &AppState,
    operation_id: &str,
) -> AppResult<()> {
    validate_manifest_for_resume(state, operation_id).await?;
    Ok(())
}

async fn validate_manifest_for_resume(
    state: &AppState,
    operation_id: &str,
) -> AppResult<Restoration> {
    let mut plan = load(state, operation_id)
        .await?
        .ok_or_else(|| unverified("folder restoration manifest is missing"))?;
    let share = bound_share(state, &plan).await?;
    let c = super::Conflict {
        id: operation_id.into(),
        share,
        rel: plan.target.clone(),
        kind: "folder_move".into(),
        remote: ConflictSide {
            instance: String::new(),
            sha256: None,
            size: None,
            deleted: false,
            modified_at: None,
        },
        remote_vv: plan.remote.version.clone(),
        peer_id: plan.peer_id.clone(),
        status: "resolving".into(),
        remote_from: Some(plan.source.clone()),
    };
    let current = move_decisions::evidence_at(state, &c, Some(&plan.remote)).await?;
    let relative = |evidence: &Evidence, path: &str| -> String {
        path.strip_prefix(&evidence.location)
            .unwrap_or(path)
            .to_owned()
    };
    let files = |evidence: &Evidence| -> BTreeMap<String, (String, u64)> {
        evidence
            .files
            .iter()
            .map(|f| (relative(evidence, &f.path), (f.sha256.clone(), f.size)))
            .collect()
    };
    let dirs = |evidence: &Evidence| -> std::collections::BTreeSet<String> {
        evidence
            .directories
            .iter()
            .map(|path| relative(evidence, path))
            .collect()
    };
    let expected = plan
        .generations
        .last()
        .map(|generation| &generation.remote)
        .unwrap_or(&plan.remote);
    if files(&current) != files(expected)
        || dirs(&current) != dirs(expected)
        || !plan.generations.is_empty() && current.location != expected.location
    {
        request_choice(state, &mut plan, &current, false).await?;
        return Err(AppError::conflict(
            "choose_version",
            "peer tree changed during source restoration",
        ));
    }
    if !plan.generations.is_empty() {
        return Ok(plan);
    }
    if current.location != plan.remote.location {
        // Only authenticated movement changes transport sources; frozen receiver targets and
        // expected bytes stay exactly the same.
        let _guard = state.replication_operation_lock.lock().await;
        let latest = load(state, &plan.operation_id)
            .await?
            .ok_or_else(|| unverified("restoration disappeared while refreshing its source"))?;
        if serde_json::to_value(&latest)? != serde_json::to_value(&plan)? {
            return Err(AppError::conflict(
                "recovery_evidence_changed",
                "the restoration changed while its source was being verified",
            ));
        }
        plan.remote = current;
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        for file in &plan.remote.files {
            let target = move_decisions::rebase(&file.path, &plan.remote.location, &plan.source)?;
            sqlx::query(
                "UPDATE transfers SET source_rel_path=?,source_vv_json=?,
                 status=CASE WHEN status='failed' THEN 'queued' ELSE status END
                 WHERE restore_parent_operation_id=? AND rel_path=? AND sha256=? AND size=?",
            )
            .bind(&file.path)
            .bind(serde_json::to_string(&file.vv)?)
            .bind(operation_id)
            .bind(&target)
            .bind(&file.sha256)
            .bind(i64::try_from(file.size).map_err(AppError::internal)?)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE pending_peer_ops SET vv_json=? WHERE operation_id=? AND kind='folder_restore'",
        )
        .bind(serde_json::to_string(&plan)?)
        .bind(format!("folder-restore:{operation_id}"))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(plan)
}

async fn process_one(state: &AppState, plan: &Restoration) -> AppResult<()> {
    let completed: bool =
        sqlx::query_scalar("SELECT status='completed' FROM operations WHERE id=?")
            .bind(&plan.operation_id)
            .fetch_one(&state.db)
            .await?;
    if !completed {
        // No authenticated peer I/O may run under the local operation lock.
        let verified_plan = validate_manifest_for_resume(state, &plan.operation_id).await?;
        let plan = &verified_plan;
        let share = bound_share(state, plan).await?;
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM transfers WHERE restore_parent_operation_id=?)",
        )
        .bind(&plan.operation_id)
        .fetch_one(&state.db)
        .await?;
        let expected_plan = serde_json::to_string(plan)?;
        for upload in plan.uploads.values() {
            if crate::ops::exec::recover_folder_prerequisite_upload_for_plan(
                state,
                &plan.operation_id,
                upload,
                &expected_plan,
            )
            .await?
            {
                let _guard = state.replication_operation_lock.lock().await;
                if !restoration_plan_matches_locked(state, &plan.operation_id, &expected_plan)
                    .await?
                {
                    return Err(AppError::conflict(
                        "recovery_evidence_changed",
                        "the restoration changed before recording its prerequisite",
                    ));
                }
                let root = files::root(&state.db, &share.root_id).await?;
                let request = verified_restore_upload(state, &plan.operation_id, upload).await?;
                let target = request.target.path.clone();
                let spec = &request.files[0];
                let root_rel = files::parse_rel(&target)?
                    .join(&spec.name)
                    .map_err(|e| AppError::invalid_location(e.to_string()))?;
                let rel = share
                    .rel_of(&root.location(&root_rel))
                    .ok_or_else(|| unverified("restored file escaped the share"))?;
                let remote = plan
                    .remote
                    .files
                    .iter()
                    .find(|f| {
                        move_decisions::rebase(&f.path, &plan.remote.location, &plan.source)
                            .is_ok_and(|path| path == rel)
                    })
                    .ok_or_else(|| unverified("restored file is absent from the manifest"))?;
                let path = root.abs(&root_rel)?;
                if files::replacement_fingerprint(&path)?
                    .is_none_or(|(_, hash)| hash != remote.sha256)
                {
                    return Err(unverified("restored source bytes changed"));
                }
                let meta = path.symlink_metadata()?;
                peers::put_row(
                    state,
                    &share.id,
                    &rel,
                    &peers::Row {
                        size: i64::try_from(meta.len()).map_err(AppError::internal)?,
                        mtime_ns: files::mtime_ns(&meta),
                        sha256: Some(remote.sha256.clone()),
                        deleted: false,
                        vv: remote.vv.clone(),
                    },
                )
                .await?;
            }
        }
        if pending {
            return Ok(());
        }
        if plan.uploads.len() != plan.remote.files.len() {
            return Err(unverified("folder prerequisite transfers are incomplete"));
        }
        if !crate::ops::exec::resume_recovered_folder_operation_for_plan(
            state,
            &plan.operation_id,
            &expected_plan,
        )
        .await?
        {
            return Ok(());
        }
    }
    let mut finished_plan = None;
    if !finish_generation(state, &plan.operation_id, &mut finished_plan).await? {
        return Ok(());
    }
    let finished_plan =
        finished_plan.ok_or_else(|| unverified("finished restoration has no plan"))?;
    cleanup_finished_plan(state, &plan.operation_id, &finished_plan).await
}

async fn cleanup_finished_plan(
    state: &AppState,
    operation_id: &str,
    finished_plan: &str,
) -> AppResult<()> {
    let sentinel = format!("folder-restore:{operation_id}");
    let _guard = state.replication_operation_lock.lock().await;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT p.vv_json FROM pending_peer_ops p
         LEFT JOIN conflicts c ON c.id=p.conflict_id
         WHERE p.operation_id=? AND p.kind='folder_restore'
         AND (p.conflict_id IS NULL OR c.status='resolved')",
    )
    .bind(&sentinel)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(raw) = raw else {
        return Ok(());
    };
    let current: Restoration = serde_json::from_str(&raw)?;
    if serde_json::to_string(&current)? != finished_plan {
        return Ok(());
    }
    sqlx::query("DELETE FROM sync_recovery_links WHERE kind='operation' AND target_id=?")
        .bind(&sentinel)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id=? AND kind='folder_restore'")
        .bind(&sentinel)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    state.sync_wake.notify_waiters();
    Ok(())
}

async fn finish_generation(
    state: &AppState,
    operation_id: &str,
    finished_plan: &mut Option<String>,
) -> AppResult<bool> {
    let mut plan = load(state, operation_id)
        .await?
        .ok_or_else(|| unverified("restoration disappeared before convergence"))?;
    let token = serde_json::to_string(&plan)?;
    let Some(generation) = plan.generations.last() else {
        if plan.conflict_id.is_none() {
            *finished_plan = Some(token);
            return Ok(true);
        }
        return Ok(false);
    };
    let conflict_id = plan
        .conflict_id
        .clone()
        .ok_or_else(|| unverified("approved generation has no conflict"))?;
    let status: String = sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
        .bind(&conflict_id)
        .fetch_one(&state.db)
        .await?;
    if status == "open" {
        return Ok(false);
    }
    let share = bound_share(state, &plan).await?;
    if generation.children.is_empty()
        && !reviewed_local_tree_is_current(state, &mut plan, &share, None).await?
    {
        return Ok(false);
    }
    let generation = plan
        .generations
        .last()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    let whole_tree = generation.remote.location != plan.target
        || inventory(&generation.remote)
            .keys()
            .ne(inventory(&plan.remote).keys());
    if whole_tree {
        if finish_tree_generation(state, plan, share).await? {
            *finished_plan = Some(token);
            return Ok(true);
        }
        return Ok(false);
    }
    let current =
        move_decisions::evidence_at(state, &context(&plan, share.clone()), Some(&plan.remote))
            .await?;
    let approved = inventory(&generation.remote);
    for file in &current.files {
        if approved.get(
            file.path
                .strip_prefix(&current.location)
                .unwrap_or(&file.path),
        ) == Some(&(Some(file.sha256.clone()), file.size))
        {
            continue;
        }
        let row = peers::row(state, &share.id, &file.path).await?;
        let published = row.is_some_and(|row| {
            row.sha256.as_ref() == Some(&file.sha256)
                && row.vv.get(&state.instance_id) > 0
                && matches!(
                    file.vv.compare(&row.vv),
                    ism_core::version::Causality::Equal | ism_core::version::Causality::Dominates
                )
        });
        if !published {
            request_choice(state, &mut plan, &current, false).await?;
            return Ok(false);
        }
    }
    if current.directories != generation.remote.directories
        || generation
            .remote
            .files
            .iter()
            .any(|file| !current.files.iter().any(|actual| actual.path == file.path))
    {
        request_choice(state, &mut plan, &current, false).await?;
        return Ok(false);
    }
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    if let Some(child) = generation.children.last() {
        let child_status: String = sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
            .bind(child)
            .fetch_one(&state.db)
            .await?;
        if child_status != "resolved" {
            let resolution: Option<String> =
                sqlx::query_scalar("SELECT resolution FROM conflicts WHERE id=?")
                    .bind(child)
                    .fetch_one(&state.db)
                    .await?;
            if child_status == "open" {
                let label = resolution
                    .ok_or_else(|| unverified("file decision lost its approved choice"))?;
                let resolution = match label.as_str() {
                    "keep_local" => ConflictResolution::KeepLocal,
                    "keep_remote" => ConflictResolution::KeepRemote,
                    "keep_both" => ConflictResolution::KeepBoth,
                    _ => return Err(unverified("unknown file decision")),
                };
                super::resolve_restoration_child(state, child, resolution, &token).await?;
            }
            return Ok(false);
        }
    }
    if generation.children.len() < generation.remote.files.len() {
        create_file_child(state, plan, &share, &root).await?;
        return Ok(false);
    }
    for (path, hash) in &generation.goals {
        let actual = files::replacement_fingerprint(&root.abs(&share.root_rel(path)?)?)?;
        if actual.is_none_or(|(_, sha)| sha != *hash) {
            return Err(unverified(
                "approved publication no longer matches its selected bytes",
            ));
        }
    }
    if finish_parent(state, &plan, &conflict_id).await? {
        *finished_plan = Some(token);
        return Ok(true);
    }
    Ok(false)
}

async fn create_file_child(
    state: &AppState,
    mut plan: Restoration,
    share: &Share,
    root: &files::Root,
) -> AppResult<()> {
    let generation = plan
        .generations
        .last()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    let remote = generation
        .remote
        .files
        .get(generation.children.len())
        .cloned()
        .ok_or_else(|| unverified("approved file decision disappeared"))?;
    let _guard = state.replication_operation_lock.lock().await;
    if !plan_is_current_resolving(state, &plan).await? {
        return Ok(());
    }
    let local = files::replacement_fingerprint(&root.abs(&share.root_rel(&remote.path)?)?)?;
    let chosen = if local.as_ref().is_some_and(|(_, sha)| *sha == remote.sha256) {
        ConflictResolution::KeepLocal
    } else if local.is_none() {
        ConflictResolution::KeepRemote
    } else {
        generation.resolution
    };
    let goal = if chosen == ConflictResolution::KeepLocal {
        local
            .as_ref()
            .map(|(_, sha)| sha.clone())
            .ok_or_else(|| unverified("retained selected file disappeared"))?
    } else {
        remote.sha256.clone()
    };
    let child = crate::crypto::new_id("cf");
    let local_row = peers::row(state, &share.id, &remote.path).await?;
    let local_side = peers::side(&state.instance_name().await, local_row.as_ref());
    let remote_side = ConflictSide {
        instance: String::new(),
        sha256: Some(remote.sha256),
        size: Some(remote.size),
        deleted: false,
        modified_at: None,
    };
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM conflicts WHERE share_id=? AND rel_path=? AND status IN ('open','resolving'))",
    ).bind(&share.id).bind(&remote.path).fetch_one(&state.db).await?;
    if active {
        return Err(unverified("another file conflict must be resolved first"));
    }
    let generation = plan
        .generations
        .last_mut()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    generation.goals.insert(remote.path.clone(), goal);
    generation.children.push(child.clone());
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(
        "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,remote_vv_json,peer_id,status,resolution,created_at)
         VALUES(?,?,?,'content','Explicit approved folder restoration generation',?,?,?,?,'open',?,?)",
    )
    .bind(&child).bind(&share.id).bind(&remote.path)
    .bind(serde_json::to_string(&local_side)?).bind(serde_json::to_string(&remote_side)?)
    .bind(serde_json::to_string(&remote.vv)?).bind(&plan.peer_id)
    .bind(match chosen {
        ConflictResolution::KeepLocal => "keep_local",
        ConflictResolution::KeepRemote => "keep_remote",
        ConflictResolution::KeepBoth => "keep_both",
        ConflictResolution::Skip => unreachable!(),
    })
    .bind(crate::db::now()).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE pending_peer_ops SET vv_json=? WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(serde_json::to_string(&plan)?)
    .bind(format!("folder-restore:{}", plan.operation_id))
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery_links(recovery_id,kind,target_id)
         SELECT id,'conflict',? FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(&child)
    .bind(&plan.operation_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    state.emit(crate::state::ServerEvent::ConflictsChanged);
    state.sync_wake.notify_waiters();
    Ok(())
}

async fn finish_parent(state: &AppState, plan: &Restoration, id: &str) -> AppResult<bool> {
    let _guard = state.replication_operation_lock.lock().await;
    let Some(current) = load(state, &plan.operation_id).await? else {
        return Ok(false);
    };
    if serde_json::to_value(current)? != serde_json::to_value(plan)? {
        return Ok(false);
    }
    let status: String = sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
        .bind(id)
        .fetch_one(&state.db)
        .await?;
    if status == "resolved" {
        return Ok(true);
    }
    let updated = sqlx::query(
        "UPDATE conflicts SET status='resolved',resolved_at=? WHERE id=? AND status='resolving'",
    )
    .bind(crate::db::now())
    .bind(id)
    .execute(&state.db)
    .await?;
    if updated.rows_affected() == 1 {
        state.emit(crate::state::ServerEvent::ConflictsChanged);
        return Ok(true);
    }
    Ok(false)
}

async fn plan_is_current_resolving(state: &AppState, plan: &Restoration) -> AppResult<bool> {
    let Some(current) = load(state, &plan.operation_id).await? else {
        return Ok(false);
    };
    if serde_json::to_value(current)? != serde_json::to_value(plan)? {
        return Ok(false);
    }
    let Some(id) = &plan.conflict_id else {
        return Ok(false);
    };
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM conflicts WHERE id=? AND status='resolving')",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await?)
}

pub(super) async fn child_is_current_locked(
    state: &AppState,
    expected_plan: &str,
    child: &str,
) -> AppResult<bool> {
    let plan: Restoration = serde_json::from_str(expected_plan)?;
    if !plan
        .generations
        .last()
        .is_some_and(|generation| generation.children.iter().any(|id| id == child))
    {
        return Ok(false);
    }
    plan_is_current_resolving(state, &plan).await
}

#[cfg(test)]
pub(in crate::peers) async fn finish_generation_before_cleanup(
    state: &AppState,
    operation_id: &str,
) -> AppResult<bool> {
    finish_generation(state, operation_id, &mut None).await
}

#[cfg(test)]
pub(in crate::peers) async fn snapshot_restoration(
    state: &AppState,
    operation_id: &str,
) -> AppResult<String> {
    let plan = load(state, operation_id)
        .await?
        .ok_or_else(|| unverified("restoration disappeared"))?;
    Ok(serde_json::to_string(&plan)?)
}

#[cfg(test)]
pub(in crate::peers) async fn finish_snapshot(state: &AppState, snapshot: &str) -> AppResult<bool> {
    let plan: Restoration = serde_json::from_str(snapshot)?;
    let id = plan
        .conflict_id
        .as_deref()
        .ok_or_else(|| unverified("restoration has no conflict"))?;
    finish_parent(state, &plan, id).await
}

#[cfg(test)]
pub(in crate::peers) async fn cleanup_snapshot(
    state: &AppState,
    operation_id: &str,
    snapshot: &str,
) -> AppResult<()> {
    cleanup_finished_plan(state, operation_id, snapshot).await
}

#[cfg(test)]
pub(in crate::peers) async fn stale_generation_probes(
    state: &AppState,
    snapshot: &str,
) -> AppResult<()> {
    let mut plan: Restoration = serde_json::from_str(snapshot)?;
    let share = bound_share(state, &plan).await?;
    let root = files::root(&state.db, &share.root_id).await?;
    create_file_child(state, serde_json::from_str(snapshot)?, &share, &root).await?;
    if reviewed_local_tree_is_current(state, &mut plan, &share, None).await? {
        return Err(unverified("superseded local tree was accepted"));
    }
    stale_reopen(state, snapshot).await
}

#[cfg(test)]
pub(in crate::peers) async fn stale_reopen(state: &AppState, snapshot: &str) -> AppResult<()> {
    let mut plan: Restoration = serde_json::from_str(snapshot)?;
    let share = bound_share(state, &plan).await?;
    let current =
        move_decisions::evidence_at(state, &context(&plan, share), Some(&plan.remote)).await?;
    match request_choice(state, &mut plan, &current, true).await {
        Err(error) if error.code() == "recovery_evidence_changed" => Ok(()),
        Err(error) => Err(error),
        Ok(()) => Err(unverified("superseded evidence reopened the restoration")),
    }
}

async fn finish_tree_generation(
    state: &AppState,
    mut plan: Restoration,
    share: Share,
) -> AppResult<bool> {
    let generation = plan
        .generations
        .last()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    let parent = plan
        .conflict_id
        .clone()
        .ok_or_else(|| unverified("approved generation has no conflict"))?;
    let child = generation.children.first().cloned();
    if let Some(child) = child {
        let status: String = sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
            .bind(&child)
            .fetch_one(&state.db)
            .await?;
        if status == "resolved" {
            return finish_parent(state, &plan, &parent).await;
        }
        if status != "open" {
            return Ok(false);
        }
        let refreshed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pending_peer_ops WHERE kind='move_decision_open' AND conflict_id=?)",
        )
        .bind(&child)
        .fetch_one(&state.db)
        .await?;
        if refreshed {
            return Ok(false);
        }
        if !reviewed_local_tree_is_current(state, &mut plan, &share, Some(&child)).await? {
            return Ok(false);
        }
        let generation = plan
            .generations
            .last()
            .ok_or_else(|| unverified("approved generation disappeared"))?;
        let c = super::load_conflict(state, &child).await?;
        let current = move_decisions::evidence_at(state, &c, Some(&plan.remote)).await?;
        if current.location != generation.remote.location
            || inventory(&current) != inventory(&generation.remote)
        {
            let guard = state.replication_operation_lock.lock().await;
            if !plan_is_current_resolving(state, &plan).await? {
                return Ok(false);
            }
            let mut latest = load(state, &plan.operation_id)
                .await?
                .ok_or_else(|| unverified("restoration disappeared during decision"))?;
            discard_unstarted_tree_child(state, &mut latest, &child).await?;
            plan = latest;
            drop(guard);
            request_choice(state, &mut plan, &current, false).await?;
            return Ok(false);
        }
        let root = files::root(&state.db, &share.root_id).await?;
        root.ensure_available()?;
        let _guard = state.replication_operation_lock.lock().await;
        if !plan_is_current_resolving(state, &plan).await? {
            return Ok(false);
        }
        peers::inbox::require_move_choice_stable(
            state,
            &share,
            &[&plan.source, &plan.target, &current.location],
            &child,
        )
        .await?;
        if super::load_conflict(state, &child).await?.status != "open" {
            return Ok(false);
        }
        let label = match generation.resolution {
            ConflictResolution::KeepLocal => "keep_local",
            ConflictResolution::KeepRemote => "keep_remote",
            ConflictResolution::KeepBoth => "keep_both",
            ConflictResolution::Skip => return Err(unverified("invalid restoration choice")),
        };
        move_decisions::submit(
            state,
            "restoration",
            &c,
            &root,
            generation.resolution,
            label,
            current,
            None,
        )
        .await?;
        return Ok(false);
    }

    if !reviewed_local_tree_is_current(state, &mut plan, &share, None).await? {
        return Ok(false);
    }
    let generation = plan
        .generations
        .last()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    let current =
        move_decisions::evidence_at(state, &context(&plan, share.clone()), Some(&plan.remote))
            .await?;
    if current.location != generation.remote.location
        || inventory(&current) != inventory(&generation.remote)
    {
        request_choice(state, &mut plan, &current, false).await?;
        return Ok(false);
    }
    let child = crate::crypto::new_id("cf");
    let resolution = match generation.resolution {
        ConflictResolution::KeepLocal => "keep_local",
        ConflictResolution::KeepRemote => "keep_remote",
        ConflictResolution::KeepBoth => "keep_both",
        ConflictResolution::Skip => return Err(unverified("invalid restoration choice")),
    };
    let local = peers::side(&state.instance_name().await, None);
    let remote = context(&plan, share.clone()).remote;
    let _guard = state.replication_operation_lock.lock().await;
    if !plan_is_current_resolving(state, &plan).await? {
        return Ok(false);
    }
    plan.generations
        .last_mut()
        .ok_or_else(|| unverified("approved generation disappeared"))?
        .children
        .push(child.clone());
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(
        "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,remote_vv_json,peer_id,status,resolution,created_at,remote_from)
         VALUES(?,?,?,'folder_move','Approved folder restoration tree choice',?,?,?,?,'open',?,?,?)",
    )
    .bind(&child)
    .bind(&share.id)
    .bind(&plan.target)
    .bind(serde_json::to_string(&local)?)
    .bind(serde_json::to_string(&remote)?)
    .bind(serde_json::to_string(&plan.remote.version)?)
    .bind(&plan.peer_id)
    .bind(resolution)
    .bind(crate::db::now())
    .bind(&plan.source)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE pending_peer_ops SET vv_json=? WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(serde_json::to_string(&plan)?)
    .bind(format!("folder-restore:{}", plan.operation_id))
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery_links(recovery_id,kind,target_id)
         SELECT id,'conflict',? FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(&child)
    .bind(&plan.operation_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    state.emit(crate::state::ServerEvent::ConflictsChanged);
    state.sync_wake.notify_waiters();
    Ok(false)
}

async fn reviewed_local_tree_is_current(
    state: &AppState,
    plan: &mut Restoration,
    share: &Share,
    child: Option<&str>,
) -> AppResult<bool> {
    let _guard = state.replication_operation_lock.lock().await;
    let mut latest = load(state, &plan.operation_id)
        .await?
        .ok_or_else(|| unverified("restoration disappeared during decision"))?;
    if serde_json::to_value(&latest)? != serde_json::to_value(&*plan)? {
        return Ok(false);
    }
    let parent = plan
        .conflict_id
        .as_deref()
        .ok_or_else(|| unverified("approved generation has no conflict"))?;
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM conflicts WHERE id=? AND status IN ('resolving','resolved'))",
    )
    .bind(parent)
    .fetch_one(&state.db)
    .await?;
    if !active {
        return Ok(false);
    }
    let generation = latest
        .generations
        .last()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    let expected = generation
        .local_inventory
        .clone()
        .unwrap_or_else(|| inventory(&latest.remote));
    if local_inventory(state, share, &latest.target).await? == expected {
        return Ok(true);
    }
    if let Some(child) = child {
        discard_unstarted_tree_child(state, &mut latest, child).await?;
    }
    *plan = latest;
    drop(_guard);
    let current =
        move_decisions::evidence_at(state, &context(plan, share.clone()), Some(&plan.remote))
            .await?;
    request_choice(state, plan, &current, true).await?;
    Ok(false)
}

async fn discard_unstarted_tree_child(
    state: &AppState,
    plan: &mut Restoration,
    child: &str,
) -> AppResult<()> {
    let status: String = sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
        .bind(child)
        .fetch_one(&state.db)
        .await?;
    let pending: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_peer_ops WHERE conflict_id=?)")
            .bind(child)
            .fetch_one(&state.db)
            .await?;
    let generation = plan
        .generations
        .last_mut()
        .ok_or_else(|| unverified("approved generation disappeared"))?;
    if status != "open" || pending || !generation.children.iter().any(|id| id == child) {
        return Err(unverified("tree changed during its managed decision"));
    }
    generation.children.retain(|id| id != child);
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("DELETE FROM sync_recovery_links WHERE kind='conflict' AND target_id=?")
        .bind(child)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM conflicts WHERE id=? AND status='open'")
        .bind(child)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE pending_peer_ops SET vv_json=? WHERE operation_id=? AND kind='folder_restore'",
    )
    .bind(serde_json::to_string(plan)?)
    .bind(format!("folder-restore:{}", plan.operation_id))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn footprints(state: &AppState, share: &Share) -> AppResult<Vec<String>> {
    let paths: Vec<(String, String)> = sqlx::query_as(
        "SELECT from_path,to_path FROM pending_peer_ops WHERE kind='folder_restore' AND share_id=?",
    )
    .bind(&share.id)
    .fetch_all(&state.db)
    .await?;
    Ok(paths
        .into_iter()
        .flat_map(|(source, target)| [source, target])
        .collect())
}

pub(super) async fn version_context(
    state: &AppState,
    id: &str,
) -> AppResult<peers::versions::Context> {
    let raw: String = sqlx::query_scalar(
        "SELECT vv_json FROM pending_peer_ops WHERE kind='folder_restore' AND conflict_id=?",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await?;
    let plan: Restoration = serde_json::from_str(&raw)?;
    let share = bound_share(state, &plan).await?;
    let current =
        move_decisions::evidence_at(state, &context(&plan, share.clone()), Some(&plan.remote))
            .await?;
    Ok(peers::versions::Context {
        share,
        peer_id: plan.peer_id,
        local_paths: vec![plan.source, plan.target],
        remote_path: current.location,
        remote_state: "folder".into(),
        remote_files: current
            .files
            .into_iter()
            .map(|file| (file.path, file.sha256, file.size, file.modified_at))
            .collect(),
        remote_directories: current.directories,
    })
}
