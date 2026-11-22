//! Read-only, path-scoped retained content evidence. Publication remains a normal peer transfer.

use std::path::PathBuf;

use ism_core::dto::CreateUpload;
use ism_core::paths::RelPath;
use reqwest::Method;
use serde::{Deserialize, Serialize};

use super::transfer::encode;
use super::{Share, net};
use crate::error::{AppError, AppResult};
use crate::files::{self, Root};
use crate::ops::plan::{TrashEntry, folder_inventory};
use crate::state::AppState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wanted {
    pub share: String,
    /// Exact share-relative content path, not a hash-based move identity.
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub token: String,
    pub wanted: Wanted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discovery {
    pub sources: Vec<Source>,
    /// False means waiting/unverified, even when sources is empty.
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Locator {
    Trash {
        id: String,
        inventory: String,
    },
    Stage {
        transfer: String,
        name: String,
    },
    Upload {
        id: String,
        index: usize,
        request: String,
    },
}

fn unverified() -> AppError {
    AppError::conflict(
        "retained_unverified",
        "retained content evidence is incomplete or changed",
    )
}

fn not_wanted() -> AppError {
    AppError::conflict(
        "retained_not_wanted",
        "verified inventory has no matching exact content path",
    )
}

async fn share(state: &AppState, peer: Option<&str>, wanted: &Wanted) -> AppResult<Share> {
    if wanted.sha256.len() != 64
        || !wanted.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || wanted.size > i64::MAX as u64
        || files::parse_rel(&wanted.path)?.is_root()
    {
        return Err(AppError::bad_request(
            "invalid_retained_request",
            "invalid content identity",
        ));
    }
    if let Some(peer) = peer {
        net::peer(state, peer).await?;
    }
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|s| s.name == wanted.share && s.enabled && peer.is_none_or(|p| s.includes(p)))
        .ok_or_else(|| AppError::not_found("enabled peer share not found"))?;
    share.root_rel(&wanted.path)?;
    Ok(share)
}

async fn stable(state: &AppState, share: &Share, wanted: &Wanted) -> AppResult<Root> {
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    if let Some(server) = &root.server_id {
        let maintenance = crate::immich::registry::server(state, server)
            .await?
            .maintenance;
        if matches!(
            maintenance.phase,
            ism_core::maintenance::Phase::Blocked | ism_core::maintenance::Phase::RecoveryRequired
        ) || maintenance.drift.is_some()
        {
            return Err(unverified());
        }
    }
    // The lock excludes an executing write step; do not make unrelated roots wait.
    let active: Vec<String> = sqlx::query_scalar(
        "SELECT request_json FROM operations WHERE status IN ('queued','running','waiting_maintenance','partial','recovery_required')
         OR (recovery_resolved=0 AND status IN ('failed','cancelled'))")
        .fetch_all(&state.db).await?;
    let target = share.root_rel(&wanted.path)?;
    for raw in active {
        let request: serde_json::Value = serde_json::from_str(&raw)?;
        if overlaps_location(&request, &share.root_id, target.as_str()) {
            return Err(unverified());
        }
        for action in request["actions"].as_array().ok_or_else(unverified)? {
            if let Some(id) = action["upload_id"].as_str() {
                let raw: Option<String> =
                    sqlx::query_scalar("SELECT request_json FROM uploads WHERE id=?")
                        .bind(id)
                        .fetch_optional(&state.db)
                        .await?;
                let upload: CreateUpload =
                    serde_json::from_str(raw.as_deref().ok_or_else(unverified)?)?;
                if upload.target.root_id == share.root_id {
                    let folder = files::parse_rel(&upload.target.path)?;
                    for file in upload.files {
                        let rel = folder.join(&file.name).map_err(AppError::internal)?;
                        if super::inbox::overlaps(rel.as_str(), target.as_str()) {
                            return Err(unverified());
                        }
                    }
                }
            }
            if let Some(id) = action["trash_id"].as_str() {
                let location: Option<(String, String)> =
                    sqlx::query_as("SELECT root_id,path FROM trash WHERE id=?")
                        .bind(id)
                        .fetch_optional(&state.db)
                        .await?;
                let (root_id, path) = location.ok_or_else(unverified)?;
                if root_id == share.root_id && super::inbox::overlaps(&path, target.as_str()) {
                    return Err(unverified());
                }
            }
        }
    }
    super::inbox::require_stable_share(state, share, &[&wanted.path]).await?;
    Ok(root)
}

fn overlaps_location(value: &serde_json::Value, root: &str, target: &str) -> bool {
    if value["root_id"].as_str() == Some(root)
        && value["path"]
            .as_str()
            .is_none_or(|path| super::inbox::overlaps(path, target))
    {
        return true;
    }
    match value {
        serde_json::Value::Object(values) => {
            values.values().any(|v| overlaps_location(v, root, target))
        }
        serde_json::Value::Array(values) => {
            values.iter().any(|v| overlaps_location(v, root, target))
        }
        _ => false,
    }
}

fn verify(root: &Root, rel: &str, wanted: &Wanted) -> AppResult<PathBuf> {
    let path = held_path(root, rel)?;
    let before = std::fs::symlink_metadata(&path).map_err(|_| unverified())?;
    if !before.is_file() || before.len() != wanted.size {
        return Err(unverified());
    }

    let (revision, hash) = files::replacement_fingerprint(&path)?.ok_or_else(unverified)?;
    let after = std::fs::symlink_metadata(&path)?;
    if revision != files::revision(&before)
        || revision != files::revision(&after)
        || hash != wanted.sha256
    {
        return Err(unverified());
    }
    Ok(path)
}

fn held_path(root: &Root, rel: &str) -> AppResult<PathBuf> {
    // Internal retention namespaces are intentionally not accepted by files::parse_rel.
    root.abs(&RelPath::parse(rel).map_err(AppError::internal)?)
}

pub(crate) fn preserve_staging(root: &Root, id: &str) -> AppResult<()> {
    let rel = format!("{}/peer/{id}", files::STAGING_DIR);
    let stage = held_path(root, &rel)?;
    let hash = files::sha256_file(&stage)?;
    let retained = held_path(
        root,
        &format!("{rel}.{hash}.{}", crate::crypto::new_id("retained")),
    )?;
    std::fs::rename(stage, retained)?;
    Ok(())
}

async fn operation_done(state: &AppState, id: Option<&str>) -> AppResult<bool> {
    let Some(id) = id else { return Ok(false) };
    Ok(
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE id=?")
            .bind(id)
            .fetch_optional(&state.db)
            .await?
            .as_deref()
            == Some("completed"),
    )
}

async fn upload_done(state: &AppState, id: &str, operation: Option<&str>) -> AppResult<bool> {
    let Some(operation) = operation else {
        return Ok(false);
    };
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT status,request_json FROM operations WHERE id=?")
            .bind(operation)
            .fetch_optional(&state.db)
            .await?;
    let Some((status, request)) = row else {
        return Ok(false);
    };
    let request: serde_json::Value = serde_json::from_str(&request)?;
    Ok(status == "completed"
        && request["actions"].as_array().is_some_and(|actions| {
            actions
                .iter()
                .any(|action| action["upload_id"].as_str() == Some(id))
        }))
}

async fn locate(
    state: &AppState,
    root: &Root,
    share: &Share,
    wanted: &Wanted,
    locator: &Locator,
) -> AppResult<PathBuf> {
    let wanted_rel = share.root_rel(&wanted.path)?;
    match locator {
        Locator::Trash { id, inventory } => {
            type Trash = (String, String, String, i64, Option<String>, Option<String>);
            let row: Option<Trash> = sqlx::query_as(
                "SELECT path,quarantine_rel,root_id,is_folder,file_inventory_json,operation_id FROM trash
                 WHERE id=? AND purged_at IS NULL AND restored_at IS NULL")
                .bind(id).fetch_optional(&state.db).await?;
            let Some((original, quarantine, root_id, folder, saved, operation)) = row else {
                return Err(unverified());
            };
            if root_id != share.root_id
                || quarantine != format!("{}/{id}", files::TRASH_DIR)
                || saved.as_deref() != Some(inventory)
                || !operation_done(state, operation.as_deref()).await?
            {
                return Err(unverified());
            }
            let original = files::parse_rel(&original)?;
            let parent = original
                .parent()
                .unwrap_or_else(ism_core::paths::RelPath::root);
            let entries: Vec<TrashEntry> =
                serde_json::from_str(inventory).map_err(|_| unverified())?;
            let (tree_rel, entry_name) = if folder != 0 {
                if !wanted_rel.starts_with(&original) || wanted_rel == original {
                    return Err(unverified());
                }
                (
                    format!("{quarantine}/{original}"),
                    wanted_rel.as_str()[original.as_str().len() + 1..].to_owned(),
                )
            } else {
                if wanted_rel
                    .parent()
                    .unwrap_or_else(ism_core::paths::RelPath::root)
                    != parent
                {
                    return Err(unverified());
                }
                (
                    if parent.is_root() {
                        quarantine
                    } else {
                        format!("{quarantine}/{parent}")
                    },
                    wanted_rel.name().ok_or_else(unverified)?.to_owned(),
                )
            };
            // Validate the entire frozen inventory before trusting its exact path entry.
            let tree = held_path(root, &tree_rel)?;
            let mut actual = folder_inventory(&tree).map_err(|_| unverified())?;
            if folder == 0 {
                actual.retain(|entry| !entry.name.is_empty());
            }
            if actual != entries {
                return Err(unverified());
            }
            if entries
                .iter()
                .filter(|e| e.name == entry_name && e.sha256.as_deref() == Some(&wanted.sha256))
                .count()
                != 1
            {
                return Err(not_wanted());
            }
            verify(root, &format!("{tree_rel}/{entry_name}"), wanted)
        }
        Locator::Stage { transfer, name } => {
            let applicable: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM transfers WHERE id=?1 AND share_id=?2 AND rel_path=?3
                 UNION ALL SELECT 1 FROM transfer_history WHERE id=?1 AND share_id=?2 AND rel_path=?3)")
                .bind(transfer).bind(&share.id).bind(&wanted.path).fetch_one(&state.db).await?;
            if !applicable
                || !(name == transfer || name.starts_with(&format!("{transfer}.")))
                || name.contains(['/', '\\'])
            {
                return Err(unverified());
            }
            verify(root, &format!("{}/peer/{name}", files::STAGING_DIR), wanted)
        }
        Locator::Upload { id, index, request } => {
            let saved: Option<(String, String, Option<String>)> =
                sqlx::query_as("SELECT request_json,status,operation_id FROM uploads WHERE id=?")
                    .bind(id)
                    .fetch_optional(&state.db)
                    .await?;
            let Some((saved, status, operation)) = saved else {
                return Err(unverified());
            };
            if saved != *request
                || (status == "committed" && !upload_done(state, id, operation.as_deref()).await?)
            {
                return Err(unverified());
            }
            let req: CreateUpload = serde_json::from_str(request).map_err(|_| unverified())?;
            let spec = req.files.get(*index).ok_or_else(unverified)?;
            let rel = files::parse_rel(&req.target.path)?
                .join(&spec.name)
                .map_err(AppError::internal)?;
            if req.target.root_id != share.root_id
                || rel != wanted_rel
                || spec.sha256 != wanted.sha256
                || spec.size != wanted.size
            {
                return Err(unverified());
            }
            verify(
                root,
                &format!("{}/uploads/{id}/{index}", files::STAGING_DIR),
                wanted,
            )
        }
    }
}

/// Caller holds the replication operation lock. This never restores a folder or changes originals.
/// Errors and complete=false are waiting evidence, never proof that all copies were exhausted.
pub async fn discover(
    state: &AppState,
    peer: Option<&str>,
    wanted: &Wanted,
) -> AppResult<Discovery> {
    let share = share(state, peer, wanted).await?;
    let root = stable(state, &share, wanted).await?;
    let wanted_rel = share.root_rel(&wanted.path)?;
    let parent = wanted_rel
        .parent()
        .unwrap_or_else(ism_core::paths::RelPath::root);
    let mut locators = Vec::new();
    let mut complete = true;
    type Trash = (String, String, i64, Option<String>);
    let trash: Vec<Trash> = sqlx::query_as(
        "SELECT id,path,is_folder,file_inventory_json FROM trash WHERE root_id=? AND purged_at IS NULL AND restored_at IS NULL")
        .bind(&share.root_id).fetch_all(&state.db).await?;
    for (id, path, folder, inventory) in trash {
        let path = files::parse_rel(&path)?;
        let applicable = if folder != 0 {
            wanted_rel.starts_with(&path)
        } else {
            path.parent().unwrap_or_else(ism_core::paths::RelPath::root) == parent
        };
        if !applicable {
            continue;
        }
        let potentially_grouped = folder != 0
            || path == wanted_rel
            || path.name().is_some_and(|original| {
                wanted_rel.name().is_some_and(|wanted_name| {
                    ism_core::sidecar::companions_of(
                        original,
                        &[wanted_name.to_owned()],
                        &state.sidecar_extensions(),
                    )
                    .iter()
                    .any(|companion| companion.name == wanted_name)
                })
            });
        let Some(inventory) = inventory else {
            if potentially_grouped {
                complete = false;
            }
            continue;
        };
        let entries: Vec<TrashEntry> = match serde_json::from_str(&inventory) {
            Ok(entries) => entries,
            Err(_) => {
                if potentially_grouped {
                    complete = false;
                }
                continue;
            }
        };
        let name = if folder != 0 && wanted_rel != path {
            wanted_rel.as_str()[path.as_str().len() + 1..].to_owned()
        } else {
            wanted_rel.name().unwrap_or_default().to_owned()
        };
        if folder == 0 && path != wanted_rel && !entries.iter().any(|e| e.name == name) {
            continue;
        }
        locators.push(Locator::Trash { id, inventory });
    }
    let transfers: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM transfers WHERE share_id=?1 AND rel_path=?2
         UNION SELECT id FROM transfer_history WHERE share_id=?1 AND rel_path=?2",
    )
    .bind(&share.id)
    .bind(&wanted.path)
    .fetch_all(&state.db)
    .await?;
    let stage = held_path(&root, &format!("{}/peer", files::STAGING_DIR))?;
    let stage_names = match std::fs::read_dir(stage) {
        Ok(entries) => entries
            .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    for transfer in transfers {
        for name in stage_names
            .iter()
            .filter(|name| **name == transfer || name.starts_with(&format!("{transfer}.")))
        {
            locators.push(Locator::Stage {
                transfer: transfer.clone(),
                name: name.clone(),
            });
        }
    }
    let uploads: Vec<(String, String, String, Option<String>)> =
        sqlx::query_as("SELECT id,request_json,status,operation_id FROM uploads WHERE json_extract(request_json,'$.target.root_id')=?")
            .bind(&share.root_id).fetch_all(&state.db).await?;
    for (id, request, status, operation) in uploads {
        let req: CreateUpload = serde_json::from_str(&request)?;
        for (index, spec) in req.files.iter().enumerate() {
            let rel = files::parse_rel(&req.target.path)?
                .join(&spec.name)
                .map_err(AppError::internal)?;
            if rel != wanted_rel {
                continue;
            }
            let stage = held_path(
                &root,
                &format!("{}/uploads/{id}/{index}", files::STAGING_DIR),
            )?;
            let absent = match std::fs::symlink_metadata(&stage) {
                Ok(_) => false,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(_) => return Err(unverified()),
            };
            if absent
                && status == "committed"
                && upload_done(state, &id, operation.as_deref()).await?
            {
                continue;
            }
            if spec.sha256 != wanted.sha256 || spec.size != wanted.size {
                // Other upload versions are not evidence for this exact content.
                continue;
            }
            locators.push(Locator::Upload {
                id: id.clone(),
                index,
                request: request.clone(),
            });
        }
    }
    let mut sources = Vec::new();
    for locator in locators {
        match locate(state, &root, &share, wanted, &locator).await {
            Ok(_) => {}
            Err(error) => {
                if error.code() != "retained_not_wanted" {
                    complete = false;
                }
                continue;
            }
        }
        let wanted_json = serde_json::to_string(wanted)?;
        let locator_json = serde_json::to_string(&locator)?;
        let token = crate::crypto::sha256_hex(
            format!(
                "retained\0{}\0{}\0{wanted_json}\0{locator_json}",
                peer.unwrap_or("local"),
                share.id
            )
            .as_bytes(),
        );
        sqlx::query("INSERT OR IGNORE INTO retained_sources(token,peer_id,share_id,wanted_json,locator_json) VALUES(?,?,?,?,?)")
            .bind(&token).bind(peer).bind(&share.id).bind(wanted_json).bind(locator_json).execute(&state.db).await?;
        sources.push(Source {
            token,
            wanted: wanted.clone(),
        });
    }
    sources.sort_by(|a, b| a.token.cmp(&b.token));
    Ok(Discovery { sources, complete })
}

/// Re-authorizes the peer/share and uncached content/inventory on every read, including after restart.
pub async fn resolve(
    state: &AppState,
    peer: Option<&str>,
    token: &str,
) -> AppResult<(PathBuf, Wanted)> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT share_id,wanted_json,locator_json FROM retained_sources WHERE token=? AND peer_id IS ?")
        .bind(token).bind(peer).fetch_optional(&state.db).await?;
    let Some((share_id, wanted, locator)) = row else {
        return Err(AppError::not_found("retained source not found"));
    };
    let wanted: Wanted = serde_json::from_str(&wanted)?;
    let share = share(state, peer, &wanted).await?;
    if share.id != share_id {
        return Err(unverified());
    }
    let root = stable(state, &share, &wanted).await?;
    let path = locate(
        state,
        &root,
        &share,
        &wanted,
        &serde_json::from_str(&locator)?,
    )
    .await?;
    Ok((path, wanted))
}

pub async fn remote_discover(
    state: &AppState,
    peer: &net::Peer,
    wanted: &Wanted,
) -> AppResult<Discovery> {
    let path = format!(
        "/api/peer/v1/recovery/retained?share={}&path={}&sha256={}&size={}",
        encode(&wanted.share),
        encode(&wanted.path),
        wanted.sha256,
        wanted.size
    );
    let response = net::request(state, &peer.url, &peer.secret, Method::GET, &path, None)
        .send()
        .await
        .map_err(AppError::internal)?;
    if !response.status().is_success() {
        return Err(unverified());
    }
    let discovery: Discovery = response.json().await.map_err(AppError::internal)?;
    if discovery.sources.iter().any(|s| {
        s.wanted != *wanted
            || s.token.len() != 64
            || !s.token.bytes().all(|b| b.is_ascii_hexdigit())
    }) {
        return Err(unverified());
    }
    Ok(discovery)
}

pub async fn remote_check(
    state: &AppState,
    peer: &net::Peer,
    token: &str,
    wanted: &Wanted,
) -> AppResult<()> {
    let path = format!(
        "/api/peer/v1/recovery/retained/check?token={}",
        encode(token)
    );
    let response = net::request(state, &peer.url, &peer.secret, Method::GET, &path, None)
        .send()
        .await
        .map_err(AppError::internal)?;
    if !response.status().is_success()
        || response
            .json::<Wanted>()
            .await
            .map_err(AppError::internal)?
            != *wanted
    {
        return Err(unverified());
    }
    Ok(())
}

/// After the coordinator admits a stable repair plan, attach the source to the existing transfer.
/// Target path/version are deliberately unchanged. Caller holds the replication operation lock.
pub async fn queue_remote(
    state: &AppState,
    id: &str,
    peer: &net::Peer,
    source: &Source,
) -> AppResult<bool> {
    // No network call under the local operation lock: simultaneous peer repairs must not
    // deadlock. The ordinary worker re-authenticates and rechecks before any content request.
    let share = share(state, Some(&peer.id), &source.wanted).await?;
    let changed = sqlx::query(
        "UPDATE transfers SET peer_id=?,source_rel_path=?,source_vv_json=NULL,source_retained_token=?,
            status='queued',error=NULL,attempts=0,updated_at=?
         WHERE id=? AND share_id=? AND sha256=? AND size=? AND status='failed'")
        .bind(&peer.id).bind(&source.wanted.path).bind(&source.token).bind(crate::db::now())
        .bind(id).bind(&share.id).bind(&source.wanted.sha256).bind(source.wanted.size as i64)
        .execute(&state.db).await?.rows_affected() == 1;
    if changed {
        state.sync_wake.notify_one();
    }
    Ok(changed)
}

/// Copy a local retained source into the existing peer staging identity, preserving its source
/// and any different partial staging. Caller holds the replication operation lock.
pub async fn stage_local(state: &AppState, id: &str, source: &Source) -> AppResult<bool> {
    let (path, wanted) = resolve(state, None, &source.token).await?;
    if wanted != source.wanted {
        return Err(unverified());
    }
    let share = share(state, None, &wanted).await?;
    let applicable: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM transfers WHERE id=? AND share_id=? AND sha256=? AND size=? AND status='failed')")
        .bind(id).bind(&share.id).bind(&wanted.sha256).bind(wanted.size as i64).fetch_one(&state.db).await?;
    if !applicable {
        return Ok(false);
    }
    let root = files::root(&state.db, &share.root_id).await?;
    let rel = format!("{}/peer/{id}", files::STAGING_DIR);
    let staging = held_path(&root, &rel)?;
    if path != staging && verify(&root, &rel, &wanted).is_err() {
        if std::fs::symlink_metadata(&staging).is_ok() {
            preserve_staging(&root, id)?;
        }
        std::fs::create_dir_all(staging.parent().ok_or_else(unverified)?)?;
        let mut input = std::fs::File::open(&path)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?;
        std::io::copy(&mut input, &mut output)?;
        output.sync_all()?;
        verify(&root, &rel, &wanted)?;
        resolve(state, None, &source.token).await?;
    }
    let changed = sqlx::query(
        "UPDATE transfers SET source_rel_path=NULL,source_vv_json=NULL,source_retained_token=NULL,
            status='queued',error=NULL,attempts=0,updated_at=? WHERE id=? AND status='failed'",
    )
    .bind(crate::db::now())
    .bind(id)
    .execute(&state.db)
    .await?
    .rows_affected()
        == 1;
    if changed {
        state.sync_wake.notify_one();
    }
    Ok(changed)
}

#[cfg(test)]
#[path = "retained_tests.rs"]
mod tests;
