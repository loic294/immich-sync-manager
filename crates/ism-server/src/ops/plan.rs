//! Turns one requested action into a checked plan of compensable steps.
//!
//! Plans are computed for previews and again right before execution (inside the maintenance
//! window), so execution always works from the current state of disk and database.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;

use ism_core::dto::DirectWrites;
use ism_core::dto::{
    Change, ClientAction, CreateUpload, FileRole, Issue, Location, OnConflict, OperationAction,
    PlannedAsset, PlannedFile, PreviewItem,
};
use ism_core::paths::{RelPath, split_ext};
use ism_core::sidecar::{CompanionRole, companions_of, is_companion_name, renamed_companion};
use sqlx::PgConnection;

use crate::auth::Principal;
use crate::error::{AppError, AppResult};
use crate::files::{self, Root, STAGING_DIR, TRASH_DIR};
use crate::immich::pg::{self, AssetRow};
use crate::immich::registry::{self, Server};
use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct FsMove {
    pub from: PathBuf,
    pub to: PathBuf,
    /// Copy instead of move (a companion shared with other originals stays in place).
    pub copy: bool,
}

#[derive(Debug, Clone)]
pub enum DbStep {
    MoveAsset {
        asset_id: String,
        from: String,
        to: String,
    },
    MoveSidecar {
        file_id: String,
        from: String,
        to: String,
    },
    ChangeLibrary {
        asset_id: String,
        from_owner: String,
        to_owner: String,
        to_library: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKind {
    Trash,
    Restore,
    ForceDelete,
}

#[derive(Debug, Clone)]
pub struct ApiStep {
    pub kind: ApiKind,
    pub server_id: String,
    pub owner: String,
    pub ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Step {
    Mkdir(PathBuf),
    Fs(FsMove),
    Api(ApiStep),
    /// Irreversible; always last.
    Remove(PathBuf),
}

#[derive(Debug, Clone)]
pub struct TrashRecord {
    pub id: String,
    pub root_id: String,
    pub path: String,
    pub quarantine_rel: String,
    pub is_folder: bool,
    pub size: u64,
    pub asset_ids: Vec<String>,
    pub server_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub files: Vec<PlannedFile>,
    pub assets: Vec<PlannedAsset>,
    pub blockers: Vec<Issue>,
    pub warnings: Vec<Issue>,
    /// File-system and API steps, in order. Each completed step can be undone except `Remove`.
    pub steps: Vec<Step>,
    /// One database transaction per server, committed after all steps succeeded.
    pub db: BTreeMap<String, Vec<DbStep>>,
    pub changes: Vec<Change>,
    pub trash_new: Option<TrashRecord>,
    /// `(trash_id, "restored" | "purged")`.
    pub trash_done: Option<(String, &'static str)>,
    pub upload_done: Option<String>,
    pub asset_id: Option<String>,
}

impl Plan {
    fn block(&mut self, code: &str, message: impl Into<String>) {
        self.blockers.push(Issue {
            code: code.into(),
            message: message.into(),
        });
    }

    fn warn(&mut self, code: &str, message: impl Into<String>) {
        self.warnings.push(Issue {
            code: code.into(),
            message: message.into(),
        });
    }

    pub fn preview(&self, index: u32, kind: &str) -> PreviewItem {
        PreviewItem {
            index,
            kind: kind.into(),
            files: self.files.clone(),
            assets: self.assets.clone(),
            blockers: self.blockers.clone(),
            warnings: self.warnings.clone(),
        }
    }
}

/// Context shared by all items of one request.
pub struct Ctx<'a> {
    pub state: &'a AppState,
    pub principal: &'a Principal,
    pub allow_owner_transfer: bool,
    pub roots: Vec<Root>,
    pub companions: Vec<String>,
    pub servers: Vec<Server>,
}

impl<'a> Ctx<'a> {
    pub async fn new(
        state: &'a AppState,
        principal: &'a Principal,
        allow_owner_transfer: bool,
    ) -> AppResult<Self> {
        Ok(Self {
            state,
            principal,
            allow_owner_transfer,
            roots: files::load_roots(&state.db).await?,
            companions: files::companion_extensions(&state.db).await,
            servers: registry::servers(state).await?,
        })
    }

    fn root(&self, root_id: &str) -> AppResult<&Root> {
        self.roots
            .iter()
            .find(|r| r.mapping.root_id == root_id)
            .ok_or_else(|| AppError::invalid_location(format!("unknown root '{root_id}'")))
    }

    fn server(&self, id: &str) -> Option<&Server> {
        self.servers.iter().find(|s| s.id == id)
    }

    /// Resolves a location, checking the grant, availability and path safety.
    fn locate(&self, loc: &Location, action: ClientAction) -> AppResult<(&Root, RelPath, PathBuf)> {
        let root = self.root(&loc.root_id)?;
        self.principal.require(action, &loc.root_id)?;
        root.ensure_available()?;
        let rel = files::parse_rel(&loc.path)?;
        if rel.is_root() {
            return Err(AppError::invalid_location(
                "the root itself cannot be changed",
            ));
        }
        let abs = root.abs(&rel)?;
        Ok((root, rel, abs))
    }

    /// Servers whose libraries are touched by this action, with the library ids to rescan.
    pub async fn touched_servers(
        &self,
        action: &OperationAction,
    ) -> AppResult<Vec<(String, Option<String>)>> {
        let mut roots: Vec<String> = Vec::new();
        match action {
            OperationAction::Move { source, target, .. }
            | OperationAction::Rename { source, target, .. }
            | OperationAction::MoveFolder { source, target } => {
                roots.push(source.root_id.clone());
                roots.push(target.root_id.clone());
            }
            OperationAction::Trash { source } | OperationAction::TrashFolder { source } => {
                roots.push(source.root_id.clone());
            }
            OperationAction::CreateFolder { .. } => {}
            OperationAction::Restore { trash_id } | OperationAction::Purge { trash_id } => {
                if let Some(root) =
                    sqlx::query_scalar::<_, String>("SELECT root_id FROM trash WHERE id = ?")
                        .bind(trash_id)
                        .fetch_optional(&self.state.db)
                        .await?
                {
                    roots.push(root);
                }
            }
            OperationAction::PublishUpload { upload_id } => {
                if let Some(raw) =
                    sqlx::query_scalar::<_, String>("SELECT request_json FROM uploads WHERE id = ?")
                        .bind(upload_id)
                        .fetch_optional(&self.state.db)
                        .await?
                {
                    let req: CreateUpload = serde_json::from_str(&raw)?;
                    roots.push(req.target.root_id);
                }
            }
        }
        let mut out = Vec::new();
        for root_id in roots {
            if let Ok(root) = self.root(&root_id)
                && let Some(server) = &root.server_id
            {
                let entry = (server.clone(), root.library_id.clone());
                if !out.contains(&entry) {
                    out.push(entry);
                }
            }
        }
        Ok(out)
    }

    async fn conn(&self, server: &Server) -> AppResult<sqlx::pool::PoolConnection<sqlx::Postgres>> {
        let pool = registry::pg_pool(self.state, server).await?;
        Ok(pool.acquire().await?)
    }
}

/// The Immich side of a root: its server, when direct writes are possible.
fn immich_side<'c>(ctx: &'c Ctx<'_>, root: &Root, plan: &mut Plan) -> Option<&'c Server> {
    let server_id = root.server_id.as_ref()?;
    let Some(server) = ctx.server(server_id) else {
        plan.block(
            "direct_writes_unavailable",
            format!(
                "root '{}' points to an unknown Immich server",
                root.mapping.root_id
            ),
        );
        return None;
    };
    match registry::direct_writes(server) {
        DirectWrites::Enabled => Some(server),
        DirectWrites::Disabled => {
            plan.block(
                "direct_writes_unavailable",
                format!(
                    "changes to '{}' need the Immich compatibility check and direct writes",
                    root.name
                ),
            );
            None
        }
        DirectWrites::Blocked => {
            plan.block(
                "maintenance_blocked",
                server
                    .maintenance
                    .message
                    .clone()
                    .unwrap_or_else(|| "Immich maintenance needs attention".into()),
            );
            None
        }
    }
}

fn sibling_names(dir: &std::path::Path) -> Vec<String> {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn role_of(role: &CompanionRole) -> FileRole {
    match role {
        CompanionRole::Sidecar => FileRole::Sidecar,
        CompanionRole::Companion => FileRole::Companion,
    }
}

/// Whether `name` would collide with an existing entry of `dir` on a case-insensitive or
/// normalising file system.
fn collides(dir: &std::path::Path, name: &str, ignore: &HashSet<String>) -> bool {
    let Ok(key) = RelPath::parse(name).map(|r| r.collision_key()) else {
        return true;
    };
    fs::read_dir(dir)
        .map(|entries| {
            entries.filter_map(Result::ok).any(|e| {
                e.file_name().into_string().is_ok_and(|n| {
                    !ignore.contains(&n)
                        && RelPath::parse(&n).is_ok_and(|r| r.collision_key() == key)
                })
            })
        })
        .unwrap_or(false)
}

pub async fn plan(ctx: &Ctx<'_>, action: &OperationAction) -> AppResult<Plan> {
    let mut plan = Plan::default();
    let result = match action {
        OperationAction::Move {
            source,
            target,
            expected_revision,
        }
        | OperationAction::Rename {
            source,
            target,
            expected_revision,
        } => plan_move_file(ctx, &mut plan, source, target, expected_revision.as_deref()).await,
        OperationAction::CreateFolder { target } => plan_create_folder(ctx, &mut plan, target),
        OperationAction::MoveFolder { source, target } => {
            plan_move_folder(ctx, &mut plan, source, target).await
        }
        OperationAction::Trash { source } => plan_trash(ctx, &mut plan, source, false).await,
        OperationAction::TrashFolder { source } => plan_trash(ctx, &mut plan, source, true).await,
        OperationAction::Restore { trash_id } => plan_restore(ctx, &mut plan, trash_id).await,
        OperationAction::Purge { trash_id } => plan_purge(ctx, &mut plan, trash_id).await,
        OperationAction::PublishUpload { upload_id } => {
            plan_publish(ctx, &mut plan, upload_id).await
        }
    };
    match result {
        Ok(()) => Ok(plan),
        // Expected problems with the request become blockers of this item.
        Err(e) if e.status().is_client_error() || e.status().as_u16() == 503 => {
            plan.block(e.code(), e.message());
            Ok(plan)
        }
        Err(e) => Err(e),
    }
}

/// Moves and renames of one original with its whole sidecar/companion group.
async fn plan_move_file(
    ctx: &Ctx<'_>,
    plan: &mut Plan,
    source: &Location,
    target: &Location,
    expected_revision: Option<&str>,
) -> AppResult<()> {
    let (src_root, src_rel, src_abs) = ctx.locate(source, ClientAction::Manage)?;
    let (dst_root, dst_rel, dst_abs) = ctx.locate(target, ClientAction::Manage)?;
    let meta = match fs::metadata(&src_abs) {
        Ok(m) if m.is_file() => m,
        Ok(_) => {
            return Err(AppError::bad_request(
                "not_a_file",
                "source is a folder; use move_folder",
            ));
        }
        Err(_) => {
            return Err(AppError::not_found(format!(
                "{} does not exist",
                source.path
            )));
        }
    };
    if let Some(expected) = expected_revision
        && files::revision(&meta) != expected
    {
        plan.block(
            "revision_changed",
            format!("{} changed since it was listed", source.path),
        );
    }
    let src_name = src_rel.name().unwrap_or_default().to_owned();
    let dst_name = dst_rel.name().unwrap_or_default().to_owned();
    if is_companion_name(&src_name, &ctx.companions) {
        plan.block(
            "not_an_original",
            "move the original file; its sidecars follow automatically",
        );
    }
    let src_dir = src_abs.parent().map(ToOwned::to_owned).unwrap_or_default();
    let dst_dir = dst_abs.parent().map(ToOwned::to_owned).unwrap_or_default();
    let src_parent = src_rel.parent().unwrap_or_else(RelPath::root);
    let dst_parent = dst_rel.parent().unwrap_or_else(RelPath::root);
    let same_dir = src_root.mapping.root_id == dst_root.mapping.root_id && src_parent == dst_parent;
    if src_root.mapping.root_id == dst_root.mapping.root_id && src_rel == dst_rel {
        plan.block("no_change", "source and target are the same");
        return Ok(());
    }

    // The group: original first, then companions.
    let mut group: Vec<(String, String, FileRole, bool)> = vec![(
        src_name.clone(),
        dst_name.clone(),
        FileRole::Original,
        false,
    )];
    for c in companions_of(&src_name, &sibling_names(&src_dir), &ctx.companions) {
        let new_name = renamed_companion(&c.name, &src_name, &dst_name);
        group.push((c.name.clone(), new_name, role_of(&c.role), c.shared));
    }
    // Names leaving the source folder don't block a rename within it (case-only renames).
    let leaving: HashSet<String> = if same_dir {
        group.iter().filter(|g| !g.3).map(|g| g.0.clone()).collect()
    } else {
        HashSet::new()
    };
    if dst_dir.exists() && !dst_dir.is_dir() {
        plan.block("destination_exists", "the target folder is a file");
    }
    for (from_name, to_name, role, shared) in &group {
        let from_rel = src_parent
            .join(from_name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?;
        let to_rel = dst_parent
            .join(to_name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?;
        if dst_dir.is_dir() && collides(&dst_dir, to_name, &leaving) {
            plan.block("destination_exists", format!("{to_rel} already exists"));
        }
        plan.files.push(PlannedFile {
            from: Some(src_root.location(&from_rel)),
            to: Some(dst_root.location(&to_rel)),
            role: *role,
            copy: *shared,
        });
        if *shared {
            plan.warn(
                "shared_companion",
                format!("{from_name} is shared with other originals; it is copied, not moved"),
            );
        }
        plan.steps.push(Step::Fs(FsMove {
            from: src_abs.with_file_name(from_name),
            to: dst_abs.with_file_name(to_name),
            copy: *shared,
        }));
    }
    if !dst_dir.exists() {
        plan.steps.insert(0, Step::Mkdir(dst_dir.clone()));
    }
    plan.changes.push(Change::Moved {
        from: source.clone(),
        to: target.clone(),
    });

    // Immich side.
    let src_server = if src_root.server_id.is_some() {
        immich_side(ctx, src_root, plan)
    } else {
        None
    };
    let dst_server = if dst_root.server_id.is_some() {
        immich_side(ctx, dst_root, plan)
    } else {
        None
    };
    match (&src_root.server_id, &dst_root.server_id) {
        (Some(a), Some(b)) if a != b => {
            plan.block(
                "cross_server_move",
                "moving between two Immich servers is not supported",
            );
            return Ok(());
        }
        (Some(_), None) => {
            plan.block(
                "leaves_immich",
                "the target root is not an Immich library; trash the asset or map the target to a library",
            );
            return Ok(());
        }
        _ => {}
    }
    let (Some(server), Some(_)) = (src_server, dst_server) else {
        // Only the target is in Immich (or neither): the next library scan imports the file.
        return Ok(());
    };
    let (Some(from_path), Some(to_path)) = (
        src_root.immich_path(&src_rel),
        dst_root.immich_path(&dst_rel),
    ) else {
        plan.block(
            "direct_writes_unavailable",
            "root mapping has no Immich import path",
        );
        return Ok(());
    };
    let mut conn = ctx.conn(server).await?;
    let Some(asset) = pg::asset_by_path(&mut conn, &from_path).await? else {
        plan.warn(
            "not_in_immich",
            "Immich has not imported this file yet; the next scan picks it up",
        );
        return Ok(());
    };
    if pg::asset_by_path(&mut conn, &to_path).await?.is_some() {
        plan.block(
            "destination_exists",
            "Immich already has an asset at the target path",
        );
    }
    plan.asset_id = Some(asset.id.clone());
    let mut steps = vec![DbStep::MoveAsset {
        asset_id: asset.id.clone(),
        from: from_path.clone(),
        to: to_path.clone(),
    }];
    for (i, (from_name, to_name, _, shared)) in group.iter().enumerate().skip(1) {
        let (Some(cf), Some(ct)) = (
            src_root.immich_path(
                &src_parent
                    .join(from_name)
                    .unwrap_or_else(|_| src_rel.clone()),
            ),
            dst_root.immich_path(&dst_parent.join(to_name).unwrap_or_else(|_| dst_rel.clone())),
        ) else {
            continue;
        };
        if let Some(sidecar) = pg::sidecar_by_path(&mut conn, &cf).await?
            && sidecar.asset_id == asset.id
        {
            if *shared {
                plan.warn(
                    "shared_sidecar",
                    format!("{from_name} stays linked to its original location"),
                );
            } else {
                steps.push(DbStep::MoveSidecar {
                    file_id: sidecar.id,
                    from: cf,
                    to: ct,
                });
                plan.files[i].role = FileRole::Sidecar;
            }
        }
    }
    owner_change(ctx, plan, &mut conn, server, &asset, dst_root, &mut steps).await?;
    plan.db.insert(server.id.clone(), steps);
    Ok(())
}

/// Adds library/owner changes when the target belongs to another Immich library.
#[allow(clippy::too_many_arguments)]
async fn owner_change(
    ctx: &Ctx<'_>,
    plan: &mut Plan,
    conn: &mut PgConnection,
    server: &Server,
    asset: &AssetRow,
    dst_root: &Root,
    steps: &mut Vec<DbStep>,
) -> AppResult<()> {
    let target_library = dst_root.library_id.clone();
    let owner_label = registry::account_label(ctx.state, &server.id, &asset.owner_id).await;
    let same_library = target_library.is_none() || target_library == asset.library_id;
    if same_library {
        plan.assets.push(PlannedAsset {
            asset_id: asset.id.clone(),
            library_id: asset.library_id.clone(),
            owner: owner_label,
            owner_change: false,
        });
        return Ok(());
    }
    let to_library = target_library.unwrap_or_default();
    let Some(to_owner) = registry::library_owner(ctx.state, &to_library).await? else {
        plan.block(
            "library_unknown",
            "refresh the Immich libraries of the target root",
        );
        return Ok(());
    };
    let owner_differs = to_owner != asset.owner_id;
    plan.assets.push(PlannedAsset {
        asset_id: asset.id.clone(),
        library_id: Some(to_library.clone()),
        owner: registry::account_label(ctx.state, &server.id, &to_owner).await,
        owner_change: owner_differs,
    });
    if owner_differs {
        if !ctx.allow_owner_transfer {
            plan.block(
                "owner_transfer_required",
                format!("the target library belongs to another user (now owned by {owner_label}); approve the ownership transfer"),
            );
        }
        if !ctx
            .principal
            .can(ClientAction::TransferOwner, &dst_root.mapping.root_id)
        {
            plan.block(
                "forbidden",
                "this credential may not transfer asset ownership",
            );
        }
        for reason in pg::transfer_blockers(conn, asset).await? {
            plan.block("owner_transfer_blocked", reason);
        }
    }
    steps.push(DbStep::ChangeLibrary {
        asset_id: asset.id.clone(),
        from_owner: asset.owner_id.clone(),
        to_owner,
        to_library,
    });
    Ok(())
}

fn plan_create_folder(ctx: &Ctx<'_>, plan: &mut Plan, target: &Location) -> AppResult<()> {
    let (_, rel, abs) = ctx.locate(target, ClientAction::Manage)?;
    let parent = abs.parent().map(ToOwned::to_owned).unwrap_or_default();
    let name = rel.name().unwrap_or_default();
    if abs.exists() || (parent.is_dir() && collides(&parent, name, &HashSet::new())) {
        plan.block("destination_exists", format!("{rel} already exists"));
    }
    plan.steps.push(Step::Mkdir(abs));
    plan.changes.push(Change::Created {
        location: target.clone(),
    });
    Ok(())
}

async fn plan_move_folder(
    ctx: &Ctx<'_>,
    plan: &mut Plan,
    source: &Location,
    target: &Location,
) -> AppResult<()> {
    let (src_root, src_rel, src_abs) = ctx.locate(source, ClientAction::Manage)?;
    let (dst_root, dst_rel, dst_abs) = ctx.locate(target, ClientAction::Manage)?;
    if !src_abs.is_dir() {
        return Err(AppError::not_found(format!(
            "folder {} does not exist",
            source.path
        )));
    }
    let same_root = src_root.mapping.root_id == dst_root.mapping.root_id;
    if same_root && dst_rel.starts_with(&src_rel) {
        plan.block("invalid_target", "a folder cannot move into itself");
        return Ok(());
    }
    let parent = dst_abs.parent().map(ToOwned::to_owned).unwrap_or_default();
    let leaving: HashSet<String> = if same_root && src_rel.parent() == dst_rel.parent() {
        src_rel
            .name()
            .map(|n| HashSet::from([n.to_owned()]))
            .unwrap_or_default()
    } else {
        HashSet::new()
    };
    if dst_abs.exists() && !leaving.contains(dst_rel.name().unwrap_or_default())
        || parent.is_dir() && collides(&parent, dst_rel.name().unwrap_or_default(), &leaving)
    {
        plan.block("destination_exists", format!("{dst_rel} already exists"));
    }
    plan.files.push(PlannedFile {
        from: Some(source.clone()),
        to: Some(target.clone()),
        role: FileRole::Original,
        copy: false,
    });
    plan.steps.push(Step::Fs(FsMove {
        from: src_abs.clone(),
        to: dst_abs.clone(),
        copy: false,
    }));
    plan.changes.push(Change::Moved {
        from: source.clone(),
        to: target.clone(),
    });
    let src_server = if src_root.server_id.is_some() {
        immich_side(ctx, src_root, plan)
    } else {
        None
    };
    let dst_server = if dst_root.server_id.is_some() {
        immich_side(ctx, dst_root, plan)
    } else {
        None
    };
    match (&src_root.server_id, &dst_root.server_id) {
        (Some(a), Some(b)) if a != b => {
            plan.block(
                "cross_server_move",
                "moving between two Immich servers is not supported",
            );
            return Ok(());
        }
        (Some(_), None) => {
            plan.block("leaves_immich", "the target root is not an Immich library");
            return Ok(());
        }
        _ => {}
    }
    let (Some(server), Some(_)) = (src_server, dst_server) else {
        return Ok(());
    };
    let (Some(from_prefix), Some(to_prefix)) = (
        src_root.immich_path(&src_rel),
        dst_root.immich_path(&dst_rel),
    ) else {
        return Ok(());
    };
    let mut conn = ctx.conn(server).await?;
    let rebase = |path: &str| format!("{to_prefix}{}", &path[from_prefix.len()..]);
    let mut steps = Vec::new();
    let assets = pg::assets_under(&mut conn, &from_prefix).await?;
    if !pg::assets_under(&mut conn, &to_prefix).await?.is_empty() {
        plan.block(
            "destination_exists",
            "Immich already has assets under the target folder",
        );
    }
    for asset in &assets {
        steps.push(DbStep::MoveAsset {
            asset_id: asset.id.clone(),
            from: asset.original_path.clone(),
            to: rebase(&asset.original_path),
        });
    }
    for sidecar in pg::sidecars_under(&mut conn, &from_prefix).await? {
        steps.push(DbStep::MoveSidecar {
            to: rebase(&sidecar.path),
            from: sidecar.path,
            file_id: sidecar.id,
        });
    }
    for asset in &assets {
        owner_change(ctx, plan, &mut conn, server, asset, dst_root, &mut steps).await?;
    }
    if plan.assets.len() > 50 {
        let changed = plan.assets.iter().filter(|a| a.owner_change).count();
        plan.assets.truncate(50);
        plan.warn(
            "many_assets",
            format!("{} assets move ({changed} change owner)", assets.len()),
        );
    }
    plan.db.insert(server.id.clone(), steps);
    Ok(())
}

/// Groups asset ids by owner, skipping assets already in Immich's trash.
fn by_owner(assets: &[AssetRow], want_trashed: bool) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for a in assets.iter().filter(|a| a.trashed == want_trashed) {
        out.entry(a.owner_id.clone())
            .or_default()
            .push(a.id.clone());
    }
    out
}

async fn plan_trash(
    ctx: &Ctx<'_>,
    plan: &mut Plan,
    source: &Location,
    folder: bool,
) -> AppResult<()> {
    let (root, rel, abs) = ctx.locate(source, ClientAction::Trash)?;
    let meta = fs::metadata(&abs)
        .map_err(|_| AppError::not_found(format!("{} does not exist", source.path)))?;
    if meta.is_dir() != folder {
        return Err(AppError::bad_request(
            "wrong_kind",
            if folder {
                "source is a file; use trash"
            } else {
                "source is a folder; use trash_folder"
            },
        ));
    }
    let trash_id = crate::crypto::new_id("trash");
    let quarantine = |r: &RelPath| format!("{TRASH_DIR}/{trash_id}/{r}");
    let base = root.base();
    let mut moved: Vec<(RelPath, FileRole)> = vec![(rel.clone(), FileRole::Original)];
    if !folder {
        let name = rel.name().unwrap_or_default();
        if is_companion_name(name, &ctx.companions) {
            plan.block(
                "not_an_original",
                "trash the original file; its sidecars follow automatically",
            );
        }
        let parent = rel.parent().unwrap_or_else(RelPath::root);
        let dir = abs.parent().map(ToOwned::to_owned).unwrap_or_default();
        for c in companions_of(name, &sibling_names(&dir), &ctx.companions) {
            if c.shared {
                plan.warn(
                    "shared_companion",
                    format!("{} is shared with other originals and stays", c.name),
                );
                continue;
            }
            if let Ok(r) = parent.join(&c.name) {
                moved.push((r, role_of(&c.role)));
            }
        }
    }
    let mut size = 0;
    for (r, role) in &moved {
        let from = root.abs(r)?;
        size += files::tree_size(&from);
        plan.files.push(PlannedFile {
            from: Some(root.location(r)),
            to: None,
            role: *role,
            copy: false,
        });
        plan.steps.push(Step::Fs(FsMove {
            from,
            to: base.join(quarantine(r)),
            copy: false,
        }));
    }
    let mut asset_ids = Vec::new();
    if root.server_id.is_some()
        && let Some(server) = immich_side(ctx, root, plan)
        && let Some(immich_path) = root.immich_path(&rel)
    {
        let mut conn = ctx.conn(server).await?;
        let assets = if folder {
            pg::assets_under(&mut conn, &immich_path).await?
        } else {
            pg::asset_by_path(&mut conn, &immich_path)
                .await?
                .into_iter()
                .collect()
        };
        asset_ids = assets.iter().map(|a| a.id.clone()).collect();
        plan.asset_id = (!folder).then(|| asset_ids.first().cloned()).flatten();
        // Immich trash first: if moving the files then fails, the assets are restored.
        let mut api_steps: Vec<Step> = by_owner(&assets, false)
            .into_iter()
            .map(|(owner, ids)| {
                Step::Api(ApiStep {
                    kind: ApiKind::Trash,
                    server_id: server.id.clone(),
                    owner,
                    ids,
                })
            })
            .collect();
        for a in assets.iter().take(50) {
            plan.assets.push(PlannedAsset {
                asset_id: a.id.clone(),
                library_id: a.library_id.clone(),
                owner: registry::account_label(ctx.state, &server.id, &a.owner_id).await,
                owner_change: false,
            });
        }
        api_steps.append(&mut plan.steps);
        plan.steps = api_steps;
    }
    plan.trash_new = Some(TrashRecord {
        id: trash_id.clone(),
        root_id: root.mapping.root_id.clone(),
        path: rel.as_str().to_owned(),
        quarantine_rel: quarantine(&RelPath::root())
            .trim_end_matches('/')
            .to_owned(),
        is_folder: folder,
        size,
        asset_ids,
        server_id: root.server_id.clone(),
    });
    plan.changes.push(Change::Trashed {
        location: source.clone(),
        trash_id,
    });
    Ok(())
}

struct TrashRow {
    root_id: String,
    path: String,
    quarantine_rel: String,
    asset_ids: Vec<String>,
    server_id: Option<String>,
}

#[allow(clippy::type_complexity)]
async fn trash_row(ctx: &Ctx<'_>, trash_id: &str) -> AppResult<TrashRow> {
    let row: Option<(String, String, String, String, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT root_id, path, quarantine_rel, asset_ids_json, server_id, restored_at, purged_at FROM trash WHERE id = ?",
    )
    .bind(trash_id)
    .fetch_optional(&ctx.state.db)
    .await?;
    let Some((root_id, path, quarantine_rel, ids, server_id, restored, purged)) = row else {
        return Err(AppError::not_found("trash item not found"));
    };
    if restored.is_some() || purged.is_some() {
        return Err(AppError::conflict(
            "trash_gone",
            "this trash item was already restored or purged",
        ));
    }
    Ok(TrashRow {
        root_id,
        path,
        quarantine_rel,
        asset_ids: crate::db::json_vec(&ids),
        server_id,
    })
}

/// Assets of a trash item that still exist in Immich.
async fn trash_assets(
    ctx: &Ctx<'_>,
    plan: &mut Plan,
    root: &Root,
    row: &TrashRow,
) -> AppResult<(Option<String>, Vec<AssetRow>)> {
    if row.asset_ids.is_empty() || row.server_id.is_none() {
        return Ok((None, Vec::new()));
    }
    let Some(server) = immich_side(ctx, root, plan) else {
        return Ok((None, Vec::new()));
    };
    let mut conn = ctx.conn(server).await?;
    let mut found = Vec::new();
    for id in &row.asset_ids {
        if let Some(a) = pg::asset_by_id(&mut conn, id).await? {
            found.push(a);
        }
    }
    if found.len() < row.asset_ids.len() {
        plan.warn(
            "asset_expired",
            "some assets left Immich's trash meanwhile; the files come back as new assets after the next scan",
        );
    }
    Ok((Some(server.id.clone()), found))
}

async fn plan_restore(ctx: &Ctx<'_>, plan: &mut Plan, trash_id: &str) -> AppResult<()> {
    let row = trash_row(ctx, trash_id).await?;
    let location = Location {
        root_id: row.root_id.clone(),
        path: row.path.clone(),
    };
    let (root, rel, target) = ctx.locate(&location, ClientAction::Trash)?;
    let quarantine = root.base().join(&row.quarantine_rel);
    if !quarantine.exists() {
        plan.block("trash_missing", "the quarantined files are gone");
        return Ok(());
    }
    // The quarantine keeps the root-relative layout: `<quarantine>/<parent>/<names>`.
    let parent = rel.parent().unwrap_or_else(RelPath::root);
    let held = quarantine.join(parent.as_str());
    let dest_dir = target.parent().map(ToOwned::to_owned).unwrap_or_default();
    let mut names: Vec<String> = fs::read_dir(&held)
        .map(|e| {
            e.filter_map(Result::ok)
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    for name in names {
        let back = parent
            .join(&name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?;
        if dest_dir.join(&name).exists()
            || (dest_dir.is_dir() && collides(&dest_dir, &name, &HashSet::new()))
        {
            plan.block("destination_exists", format!("{back} already exists"));
        }
        plan.files.push(PlannedFile {
            from: None,
            to: Some(root.location(&back)),
            role: if back == rel {
                FileRole::Original
            } else {
                FileRole::Companion
            },
            copy: false,
        });
        plan.steps.push(Step::Fs(FsMove {
            from: held.join(&name),
            to: dest_dir.join(&name),
            copy: false,
        }));
    }
    let (server_id, assets) = trash_assets(ctx, plan, root, &row).await?;
    if let Some(server_id) = server_id {
        for (owner, ids) in by_owner(&assets, true) {
            plan.steps.push(Step::Api(ApiStep {
                kind: ApiKind::Restore,
                server_id: server_id.clone(),
                owner,
                ids,
            }));
        }
    }
    plan.trash_done = Some((trash_id.to_owned(), "restored"));
    plan.changes.push(Change::Restored {
        location,
        trash_id: trash_id.to_owned(),
    });
    Ok(())
}

async fn plan_purge(ctx: &Ctx<'_>, plan: &mut Plan, trash_id: &str) -> AppResult<()> {
    let row = trash_row(ctx, trash_id).await?;
    ctx.principal.require(ClientAction::Purge, &row.root_id)?;
    let root = ctx.root(&row.root_id)?;
    root.ensure_available()?;
    let quarantine = root.base().join(&row.quarantine_rel);
    let (server_id, assets) = trash_assets(ctx, plan, root, &row).await?;
    if let Some(server_id) = server_id {
        for (owner, ids) in by_owner(&assets, true) {
            plan.steps.push(Step::Api(ApiStep {
                kind: ApiKind::ForceDelete,
                server_id: server_id.clone(),
                owner,
                ids,
            }));
        }
        let live = by_owner(&assets, false);
        if !live.is_empty() {
            plan.warn(
                "asset_restored_in_immich",
                "an asset was restored in Immich meanwhile; it is left alone",
            );
        }
    }
    plan.files.push(PlannedFile {
        from: Some(Location {
            root_id: row.root_id.clone(),
            path: row.path.clone(),
        }),
        to: None,
        role: FileRole::Original,
        copy: false,
    });
    plan.steps.push(Step::Remove(quarantine));
    plan.trash_done = Some((trash_id.to_owned(), "purged"));
    plan.changes.push(Change::Purged {
        trash_id: trash_id.to_owned(),
    });
    Ok(())
}

pub fn staging_dir(root: &Root, upload_id: &str) -> PathBuf {
    root.base()
        .join(STAGING_DIR)
        .join("uploads")
        .join(upload_id)
}

/// Picks `stem (n).ext` names for the original so the whole group lands without collisions.
fn free_name(dir: &std::path::Path, names: &[String], original: &str) -> String {
    let (stem, ext) = split_ext(original);
    for n in 1..1000 {
        let candidate = match ext {
            Some(ext) => format!("{stem} ({n}).{ext}"),
            None => format!("{stem} ({n})"),
        };
        let clash = names
            .iter()
            .map(|name| renamed_companion(name, original, &candidate))
            .any(|name| dir.join(&name).exists() || collides(dir, &name, &HashSet::new()));
        if !clash {
            return candidate;
        }
    }
    original.to_owned()
}

async fn plan_publish(ctx: &Ctx<'_>, plan: &mut Plan, upload_id: &str) -> AppResult<()> {
    let raw: Option<String> = sqlx::query_scalar("SELECT request_json FROM uploads WHERE id = ?")
        .bind(upload_id)
        .fetch_optional(&ctx.state.db)
        .await?;
    let req: CreateUpload =
        serde_json::from_str(&raw.ok_or_else(|| AppError::not_found("upload not found"))?)?;
    let root = ctx.root(&req.target.root_id)?;
    ctx.principal
        .require(ClientAction::Upload, &req.target.root_id)?;
    root.ensure_available()?;
    let folder = files::parse_rel(&req.target.path)?;
    let dir = root.abs(&folder)?;
    let staging = staging_dir(root, upload_id);
    let names: Vec<String> = req.files.iter().map(|f| f.name.clone()).collect();
    let original = req
        .files
        .iter()
        .find(|f| f.role == FileRole::Original)
        .map(|f| f.name.clone())
        .unwrap_or_else(|| names[0].clone());
    let exists = names
        .iter()
        .any(|n| dir.join(n).exists() || collides(&dir, n, &HashSet::new()));
    let mut final_original = original.clone();
    if exists {
        match req.on_conflict {
            OnConflict::Fail => plan.block(
                "destination_exists",
                format!("{original} already exists in {folder}"),
            ),
            OnConflict::Rename => final_original = free_name(&dir, &names, &original),
            OnConflict::ReplaceIfExpected => {
                let expected = req
                    .provenance
                    .as_ref()
                    .and_then(|p| p.expected_revision.clone());
                let current = fs::metadata(dir.join(&original))
                    .ok()
                    .map(|m| files::revision(&m));
                if expected.is_none() || expected != current {
                    plan.block(
                        "revision_changed",
                        format!("{original} changed on the server; not replacing it"),
                    );
                } else {
                    // The replaced versions are kept as a recoverable trash item.
                    let trash_id = crate::crypto::new_id("trash");
                    let q = format!("{TRASH_DIR}/{trash_id}");
                    let mut size = 0;
                    for name in &names {
                        let existing = dir.join(name);
                        if existing.exists() {
                            size += files::tree_size(&existing);
                            let r = folder
                                .join(name)
                                .map_err(|e| AppError::invalid_location(e.to_string()))?;
                            plan.steps.push(Step::Fs(FsMove {
                                from: existing,
                                to: root.base().join(&q).join(r.as_str()),
                                copy: false,
                            }));
                        }
                    }
                    let path = folder
                        .join(&original)
                        .map_err(|e| AppError::invalid_location(e.to_string()))?;
                    plan.warn(
                        "replaces_existing",
                        format!("the current {original} is kept in trash"),
                    );
                    plan.trash_new = Some(TrashRecord {
                        id: trash_id,
                        root_id: root.mapping.root_id.clone(),
                        path: path.as_str().to_owned(),
                        quarantine_rel: q,
                        is_folder: false,
                        size,
                        asset_ids: Vec::new(),
                        server_id: None,
                    });
                }
            }
        }
    }
    if !dir.exists() {
        plan.steps.insert(0, Step::Mkdir(dir.clone()));
    }
    for (index, f) in req.files.iter().enumerate() {
        let name = renamed_companion(&f.name, &original, &final_original);
        let r = folder
            .join(&name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?;
        plan.files.push(PlannedFile {
            from: None,
            to: Some(root.location(&r)),
            role: f.role,
            copy: false,
        });
        plan.steps.push(Step::Fs(FsMove {
            from: staging.join(index.to_string()),
            to: dir.join(&name),
            copy: false,
        }));
        if f.name == original {
            plan.changes.push(Change::Created {
                location: root.location(&r),
            });
        }
    }
    if root.server_id.is_some() {
        immich_side(ctx, root, plan);
    }
    plan.upload_done = Some(upload_id.to_owned());
    Ok(())
}
