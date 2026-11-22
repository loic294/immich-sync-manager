//! Durable move choices. Preparations are managed operations; content is published by the
//! ordinary transfer worker. A non-operation pending row retains the final verification plan.

use ism_core::dto::{ConflictResolution, OperationAction, OperationRequest};
use ism_core::paths::RelPath;
use ism_core::version::VersionVector;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{Conflict, conflict_name};
use crate::auth::Principal;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::peers::{self, EventData, Row, recovery};
use crate::state::{AppState, ServerEvent};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Evidence {
    pub(super) destination: String,
    #[serde(default)]
    pub(super) location: String,
    pub(super) version: VersionVector,
    pub(super) source: Option<String>,
    pub(super) event_identity: Option<(String, i64)>,
    #[serde(default)]
    event_sequence: Option<i64>,
    pub(super) files: Vec<Content>,
    pub(super) directories: Vec<String>,
    #[serde(default)]
    pub(super) lineage: Vec<peers::PeerEvent>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Content {
    pub(super) path: String,
    pub(super) sha256: String,
    pub(super) size: u64,
    pub(super) vv: VersionVector,
    pub(super) download: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) modified_at: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Decision {
    #[serde(default)]
    recovery_ids: Vec<String>,
    #[serde(default)]
    retained_transfers: Vec<String>,
    event_identity: Option<(String, i64)>,
    current: Option<OperationAction>,
    steps: Vec<OperationAction>,
    files: Vec<Content>,
    directories: Vec<String>,
    absent: Vec<String>,
    events: Vec<(String, EventData)>,
    #[serde(default)]
    remote_evidence: Option<Evidence>,
    #[serde(default)]
    lineage: Vec<peers::PeerEvent>,
    #[serde(default)]
    displaced_files: Vec<Content>,
    #[serde(default)]
    displaced_directories: Vec<String>,
}

fn invalid(message: &str) -> AppError {
    AppError::conflict("recovery_evidence_missing", message)
}

fn nested(base: &str, name: &str) -> AppResult<String> {
    let name = RelPath::parse(name).map_err(|e| AppError::invalid_location(e.to_string()))?;
    if name.is_root() {
        return Err(invalid("empty inventory path"));
    }
    let path = format!("{base}/{name}");
    RelPath::parse(&path).map_err(|e| AppError::invalid_location(e.to_string()))?;
    Ok(path)
}

pub(super) async fn evidence(state: &AppState, c: &Conflict) -> AppResult<Evidence> {
    let saved: Option<String> = sqlx::query_scalar(
        "SELECT vv_json FROM pending_peer_ops WHERE kind='move_decision_open' AND conflict_id=?",
    )
    .bind(&c.id)
    .fetch_optional(&state.db)
    .await?;
    let saved: Option<Decision> = saved.map(|raw| serde_json::from_str(&raw)).transpose()?;
    evidence_at(
        state,
        c,
        saved
            .as_ref()
            .and_then(|plan| plan.remote_evidence.as_ref()),
    )
    .await
}

pub(super) async fn evidence_at(
    state: &AppState,
    c: &Conflict,
    anchor: Option<&Evidence>,
) -> AppResult<Evidence> {
    let peer = peers::net::peer(state, &c.peer_id).await?;
    let receipts: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM peer_inbox WHERE peer_id=? AND share_name=?
         AND ((? IS NULL AND status!='done') OR (origin=? AND origin_seq=?)) ORDER BY seq DESC",
    )
    .bind(&c.peer_id)
    .bind(&c.share.name)
    .bind(anchor.and_then(|a| a.event_identity.as_ref().map(|id| id.0.as_str())))
    .bind(anchor.and_then(|a| a.event_identity.as_ref().map(|id| id.0.as_str())))
    .bind(anchor.and_then(|a| a.event_identity.as_ref().map(|id| id.1)))
    .fetch_all(&state.db)
    .await?;
    // Completed inbox receipts are pruned. The immutable captured identity/cursor outlive them.
    let mut event_identity = anchor.and_then(|e| e.event_identity.clone());
    let mut event_sequence = anchor.and_then(|e| e.event_sequence);
    for raw in receipts {
        let event: peers::PeerEvent = serde_json::from_str(&raw)?;
        if event.data.path == c.rel
            && event.data.from == c.remote_from
            && event.data.vv == c.remote_vv
            && ((c.kind == "folder_move" && event.kind == "folder_moved")
                || (c.kind == "move"
                    && event.kind == "file_moved"
                    && event.data.sha256 == c.remote.sha256))
        {
            event_identity = Some((event.origin, event.origin_seq));
            event_sequence = Some(event.seq);
            break;
        }
    }
    let remote = if let Some((origin, seq)) = &event_identity {
        let version = serde_json::to_string(&c.remote_vv)?;
        let sequence = seq.to_string();
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [
                ("share", c.share.name.as_str()),
                ("path", c.rel.as_str()),
                ("vv", version.as_str()),
                ("origin", origin.as_str()),
                ("origin_seq", sequence.as_str()),
            ],
        )
        .map_err(AppError::internal)?;
        let path = format!(
            "/api/peer/v1/recovery/file?{}",
            url.query().unwrap_or_default()
        );
        peers::net::get_json::<recovery::FileEvidence>(state, &peer, &path)
            .await
            .map_err(|error| AppError::conflict("peer_evidence_unavailable", error))?
    } else {
        recovery::remote_evidence(state, &peer, &c.share, &c.rel, &c.remote_vv).await?
    };
    // Without an event anchor, folder hashes or empty vectors cannot establish move lineage.
    if remote.deleted || (remote.path != c.rel && event_identity.is_none()) {
        return Err(invalid(
            "the chosen move destination has changed; refresh its evidence",
        ));
    }
    let mut result = Evidence {
        destination: c.rel.clone(),
        location: remote.path.clone(),
        version: c.remote_vv.clone(),
        source: c.remote_from.clone(),
        event_identity,
        event_sequence,
        files: Vec::new(),
        directories: Vec::new(),
        lineage: Vec::new(),
    };
    let mut intermediate = Vec::new();
    if result.event_identity.is_some() {
        let mut current = c.rel.clone();
        let mut after =
            event_sequence.ok_or_else(|| invalid("move event cursor is unavailable"))?;
        let last = remote
            .last_move_seq
            .ok_or_else(|| invalid("peer cannot prove the complete move lineage"))?;
        if last < after {
            return Err(invalid("peer move lineage cursor is invalid"));
        }
        while after < last {
            let page: peers::EventPage =
                peers::net::get_json(state, &peer, &format!("/api/peer/v1/events?after={after}"))
                    .await
                    .map_err(|error| AppError::conflict("peer_evidence_unavailable", error))?;
            page.validate(after).map_err(AppError::internal)?;
            for event in page.events {
                if event.seq > last || event.share != c.share.name {
                    continue;
                }
                if matches!(event.kind.as_str(), "folder_moved" | "file_moved")
                    && let Some(from) = event.data.from.as_deref()
                    && (from == current
                        || event.kind == "folder_moved" && current.starts_with(&format!("{from}/")))
                {
                    current = rebase(&current, from, &event.data.path)?;
                    result.lineage.push(event);
                } else if matches!(
                    event.kind.as_str(),
                    "file_changed" | "file_deleted" | "folder_created" | "folder_trashed"
                ) {
                    intermediate.push(event);
                }
            }
            if page.next == after {
                return Err(invalid(
                    "current location has no complete authenticated move lineage",
                ));
            }
            after = page.next;
        }
        if current != remote.path {
            return Err(invalid(
                "current location has no complete authenticated move lineage",
            ));
        }
    }
    let mut decision_scope = if c.kind == "folder_move" {
        let mut scope = remote.path.clone();
        for path in [c.remote_from.as_deref(), Some(c.rel.as_str())]
            .into_iter()
            .flatten()
        {
            if scope.starts_with(&format!("{path}/")) {
                scope = path.to_owned();
            }
        }
        Some(scope)
    } else {
        None
    };
    if let Some(scope) = &mut decision_scope {
        for event in &intermediate {
            if event.kind == "folder_trashed" && scope.starts_with(&format!("{}/", event.data.path))
            {
                RelPath::parse(&event.data.path)
                    .map_err(|e| AppError::invalid_location(e.to_string()))?;
                *scope = event.data.path.clone();
            }
        }
        result
            .lineage
            .extend(intermediate.into_iter().filter(|event| {
                event.data.path == *scope || event.data.path.starts_with(&format!("{scope}/"))
            }));
        result.lineage.sort_by_key(|event| event.seq);
    }
    let remote = if let Some(scope) = decision_scope.as_deref()
        && scope != remote.path
    {
        let scoped =
            recovery::remote_evidence(state, &peer, &c.share, scope, &VersionVector::default())
                .await?;
        if scoped.path != scope || scoped.deleted || scoped.inventory.is_none() {
            return Err(invalid("the overlapping folder scope is unavailable"));
        }
        {
            let prefix = remote
                .path
                .strip_prefix(&format!("{scope}/"))
                .ok_or_else(|| invalid("moved folder escaped its decision scope"))?;
            let expected = remote
                .inventory
                .as_ref()
                .ok_or_else(|| invalid("moved folder inventory is unavailable"))?;
            let child_prefix = format!("{prefix}/");
            let actual: BTreeMap<_, _> = scoped
                .inventory
                .as_ref()
                .ok_or_else(|| invalid("overlapping folder inventory is unavailable"))?
                .iter()
                .filter_map(|entry| {
                    if entry.name == prefix {
                        Some((String::new(), entry.sha256.clone()))
                    } else {
                        entry
                            .name
                            .strip_prefix(&child_prefix)
                            .map(|name| (name.to_owned(), entry.sha256.clone()))
                    }
                })
                .collect();
            let expected_map: BTreeMap<_, _> = expected
                .iter()
                .map(|entry| (entry.name.clone(), entry.sha256.clone()))
                .collect();
            if expected_map.len() != expected.len() || actual != expected_map {
                return Err(invalid(
                    "the moved folder changed while collecting its larger scope",
                ));
            }
        }
        result.location = scope.to_owned();
        scoped
    } else {
        remote
    };
    if c.kind == "folder_move" {
        let inventory = remote
            .inventory
            .ok_or_else(|| invalid("peer folder is unavailable"))?;
        if inventory
            .first()
            .is_none_or(|entry| !entry.name.is_empty() || entry.sha256.is_some())
        {
            return Err(invalid("peer inventory has no directory root"));
        }
        result.directories.push(remote.path.clone());
        let mut seen = std::collections::HashSet::new();
        for entry in inventory {
            if entry.name.is_empty() {
                continue;
            }
            let path = nested(&remote.path, &entry.name)?;
            c.share.root_rel(&path)?;
            if !seen.insert(path.clone()) {
                return Err(invalid("duplicate peer inventory path"));
            }
            if let Some(sha256) = entry.sha256 {
                let file = recovery::remote_evidence(
                    state,
                    &peer,
                    &c.share,
                    &path,
                    &VersionVector::default(),
                )
                .await?;
                if file.path != path || file.deleted || file.sha256.as_deref() != Some(&sha256) {
                    return Err(invalid("peer tree changed while collecting file evidence"));
                }
                let size = file.size.ok_or_else(|| invalid("missing peer file size"))?;
                peers::checked_transfer_size(&sha256, size)?;
                result.files.push(Content {
                    path,
                    sha256,
                    size,
                    vv: file.vv,
                    download: true,
                    modified_at: file.modified_at,
                });
            } else {
                result.directories.push(path);
            }
        }
    } else {
        let sha256 = remote
            .sha256
            .ok_or_else(|| invalid("peer file is unavailable"))?;
        let size = remote
            .size
            .ok_or_else(|| invalid("missing peer file size"))?;
        peers::checked_transfer_size(&sha256, size)?;
        result.files.push(Content {
            path: remote.path,
            sha256,
            size,
            vv: remote.vv,
            download: true,
            modified_at: remote.modified_at,
        });
    }
    Ok(result)
}

async fn local_content(
    state: &AppState,
    c: &Conflict,
    root: &files::Root,
    path: &str,
) -> AppResult<(Vec<Content>, Vec<String>)> {
    let abs = root.abs(&c.share.root_rel(path)?)?;
    let metadata = match abs.symlink_metadata() {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(e) => return Err(e.into()),
    };
    let (entries, directories) = if metadata.is_dir() {
        let mut files = Vec::new();
        let mut dirs = vec![path.to_owned()];
        for entry in crate::ops::plan::folder_inventory(&abs)? {
            if entry.name.is_empty() {
                continue;
            }
            let rel = nested(path, &entry.name)?;
            if entry.sha256.is_some() {
                files.push(rel);
            } else {
                dirs.push(rel);
            }
        }
        (files, dirs)
    } else if metadata.is_file() {
        (vec![path.to_owned()], Vec::new())
    } else {
        return Err(invalid(
            "move source or destination is not a regular file or directory",
        ));
    };
    let mut files = Vec::new();
    for path in entries {
        let file = root.abs(&c.share.root_rel(&path)?)?;
        let (revision, sha256) = files::replacement_fingerprint(&file)?
            .ok_or_else(|| invalid("local content disappeared"))?;
        let meta = file.symlink_metadata()?;
        if files::revision(&meta) != revision {
            return Err(invalid("local content changed during verification"));
        }
        let mut vv = peers::row(state, &c.share.id, &path)
            .await?
            .map(|r| r.vv)
            .unwrap_or_default();
        vv.merge(&c.remote_vv);
        files.push(Content {
            path,
            sha256,
            size: meta.len(),
            vv,
            download: false,
            modified_at: Some(
                chrono::DateTime::from_timestamp_nanos(files::mtime_ns(&meta)).to_rfc3339(),
            ),
        });
    }
    Ok((files, directories))
}

fn trash(c: &Conflict, root: &files::Root, path: &str) -> AppResult<Option<OperationAction>> {
    let abs = root.abs(&c.share.root_rel(path)?)?;
    let meta = match abs.symlink_metadata() {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let source = c.share.location(path)?;
    if meta.is_dir() {
        // Inventory validation also rejects symlinks anywhere below the folder.
        crate::ops::plan::folder_inventory(&abs)?;
        Ok(Some(OperationAction::TrashFolder {
            source,
            expected_entries: None,
        }))
    } else if meta.is_file() {
        Ok(Some(OperationAction::Trash {
            source,
            expected_revision: Some(files::revision(&meta)),
        }))
    } else {
        Err(invalid("move target is not a regular file or directory"))
    }
}

pub(super) fn rebase(path: &str, from: &str, to: &str) -> AppResult<String> {
    if path == from {
        Ok(to.to_owned())
    } else {
        nested(
            to,
            path.strip_prefix(&format!("{from}/"))
                .ok_or_else(|| invalid("inventory escaped its folder"))?,
        )
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn submit(
    state: &AppState,
    _actor: &str,
    c: &Conflict,
    root: &files::Root,
    resolution: ConflictResolution,
    label: &str,
    mut remote: Evidence,
) -> AppResult<()> {
    if remote.destination != c.rel
        || remote.version != c.remote_vv
        || remote.source != c.remote_from
    {
        return Err(invalid("conflict changed while fetching evidence"));
    }
    let original_from = c
        .remote_from
        .as_deref()
        .ok_or_else(|| invalid("move has no source"))?;
    if original_from.is_empty()
        || original_from == c.rel
        || original_from.starts_with(&format!("{}/", c.rel))
        || c.rel.starts_with(&format!("{original_from}/"))
    {
        return Err(invalid("move paths overlap"));
    }
    let from = if c.kind == "folder_move" && peers::inbox::overlaps(&remote.location, original_from)
    {
        remote.location.as_str()
    } else {
        original_from
    };
    let (mut source_files, source_dirs) = local_content(state, c, root, from).await?;
    let mut destinations = Vec::new();
    if !peers::inbox::overlaps(&c.rel, from)
        && (c.rel == remote.location || !c.rel.starts_with(&format!("{}/", remote.location)))
    {
        let (files, dirs) = local_content(state, c, root, &c.rel).await?;
        destinations.push((c.rel.clone(), files, dirs));
    }
    if remote.location != c.rel
        && (remote.location != from || resolution == ConflictResolution::KeepBoth)
    {
        let (files, dirs) = local_content(state, c, root, &remote.location).await?;
        destinations.push((remote.location.clone(), files, dirs));
    }
    let mut decision = Decision {
        recovery_ids: sqlx::query_scalar(
            "SELECT id FROM sync_recovery WHERE kind='conflict' AND target_id=?
             UNION SELECT recovery_id FROM sync_recovery_links WHERE kind='conflict' AND target_id=?",
        ).bind(&c.id).bind(&c.id).fetch_all(&state.db).await?,
        retained_transfers: peers::inbox::retained_choice_transfers(state, &c.share, &c.id).await?,
        event_identity: remote.event_identity.clone(),
        current: None,
        steps: Vec::new(),
        files: Vec::new(),
        directories: Vec::new(),
        absent: Vec::new(),
        events: Vec::new(),
        remote_evidence: Some(remote.clone()),
        lineage: remote.lineage.clone(),
        displaced_files: Vec::new(),
        displaced_directories: Vec::new(),
    };
    let saved: Option<String> = sqlx::query_scalar(
        "SELECT vv_json FROM pending_peer_ops WHERE kind='move_decision_open' AND conflict_id=?",
    )
    .bind(&c.id)
    .fetch_optional(&state.db)
    .await?;
    if let Some(saved) = saved {
        let previous: Decision = serde_json::from_str(&saved)?;
        let copies: Vec<String> = previous
            .directories
            .iter()
            .filter(|path| {
                !peers::inbox::overlaps(path, from)
                    && !peers::inbox::overlaps(path, &c.rel)
                    && !previous
                        .directories
                        .iter()
                        .any(|parent| *path != parent && path.starts_with(&format!("{parent}/")))
            })
            .cloned()
            .collect();
        // Previously preserved copies still need announcements after refreshing the choice.
        for path in copies.iter().chain(
            previous
                .files
                .iter()
                .filter(|file| {
                    !file.download
                        && !peers::inbox::overlaps(&file.path, from)
                        && !peers::inbox::overlaps(&file.path, &c.rel)
                        && !copies
                            .iter()
                            .any(|parent| file.path.starts_with(&format!("{parent}/")))
                })
                .map(|file| &file.path),
        ) {
            peers::inbox::require_operations_stable(state, &c.share, &[path]).await?;
            let (files, dirs) = local_content(state, c, root, path).await?;
            decision.files.extend(files);
            decision.directories.extend(dirs);
        }
    }
    // The selected contents must dominate all known versions, not only the move's vector.
    for file in &mut remote.files {
        if let Some(local) = peers::row(state, &c.share.id, &file.path).await? {
            file.vv.merge(&local.vv);
        }
        let old = rebase(&file.path, &remote.location, from)?;
        if let Some(local) = peers::row(state, &c.share.id, &old).await? {
            file.vv.merge(&local.vv);
        }
        file.vv.merge(&c.remote_vv);
        if let Some(source) = source_files.iter_mut().find(|f| f.path == old) {
            source.vv.merge(&file.vv);
        }
    }
    decision
        .displaced_files
        .extend(source_files.iter().cloned());
    decision
        .displaced_directories
        .extend(source_dirs.iter().cloned());
    for (_, files, dirs) in &destinations {
        decision.displaced_files.extend(files.iter().cloned());
        decision.displaced_directories.extend(dirs.iter().cloned());
    }
    if from == remote.location && resolution != ConflictResolution::KeepLocal {
        let mut removed_directories: Vec<String> = Vec::new();
        let mut local_directories = source_dirs.clone();
        local_directories.sort_by_key(|path| path.split('/').count());
        for directory in &local_directories {
            if remote.directories.contains(directory)
                || remote.files.iter().any(|file| {
                    directory == &file.path || directory.starts_with(&format!("{}/", file.path))
                })
                || removed_directories
                    .iter()
                    .any(|parent| directory.starts_with(&format!("{parent}/")))
            {
                continue;
            }
            removed_directories.push(directory.clone());
            decision.absent.push(directory.clone());
            decision.events.push((
                "folder_trashed".into(),
                EventData {
                    path: directory.clone(),
                    ..Default::default()
                },
            ));
        }
        for file in &source_files {
            if remote.directories.contains(&file.path)
                || remote.files.iter().any(|remote| {
                    file.path == remote.path || file.path.starts_with(&format!("{}/", remote.path))
                })
                || removed_directories
                    .iter()
                    .any(|directory| file.path.starts_with(&format!("{directory}/")))
            {
                continue;
            }
            decision.absent.push(file.path.clone());
            decision.events.push((
                "file_deleted".into(),
                EventData {
                    path: file.path.clone(),
                    vv: file.vv.clone(),
                    ..Default::default()
                },
            ));
        }
    }
    match resolution {
        ConflictResolution::KeepLocal => {
            decision.files.extend(source_files);
            decision.directories.extend(source_dirs);
            for (_, files, dirs) in destinations {
                decision.files.extend(files);
                decision.directories.extend(dirs);
            }
            let mut removed_directories: Vec<String> = Vec::new();
            remote
                .directories
                .sort_by_key(|path| path.split('/').count());
            for directory in &remote.directories {
                if !decision.directories.contains(directory)
                    && !removed_directories
                        .iter()
                        .any(|parent| directory.starts_with(&format!("{parent}/")))
                {
                    removed_directories.push(directory.clone());
                    if !decision.files.iter().any(|file| file.path == *directory) {
                        decision.absent.push(directory.clone());
                    }
                    decision.events.push((
                        "folder_trashed".into(),
                        EventData {
                            path: directory.clone(),
                            ..Default::default()
                        },
                    ));
                }
            }
            for remote_file in &remote.files {
                if let Some(local) = decision
                    .files
                    .iter_mut()
                    .find(|f| f.path == remote_file.path)
                {
                    local.vv.merge(&remote_file.vv);
                } else {
                    if removed_directories
                        .iter()
                        .any(|directory| remote_file.path.starts_with(&format!("{directory}/")))
                    {
                        continue;
                    }
                    if !decision.directories.contains(&remote_file.path) {
                        decision.absent.push(remote_file.path.clone());
                    }
                    decision.events.push((
                        "file_deleted".into(),
                        EventData {
                            path: remote_file.path.clone(),
                            vv: remote_file.vv.clone(),
                            ..Default::default()
                        },
                    ));
                }
            }
        }
        ConflictResolution::KeepRemote | ConflictResolution::KeepBoth => {
            if resolution == ConflictResolution::KeepRemote {
                if let Some(action) = trash(c, root, from)? {
                    decision.steps.push(action);
                }
                if remote.location != from {
                    decision.absent.push(from.to_owned());
                }
                if c.kind == "folder_move" && remote.location != from {
                    decision.events.push((
                        "folder_trashed".into(),
                        EventData {
                            path: from.to_owned(),
                            ..Default::default()
                        },
                    ));
                } else if c.kind != "folder_move" && remote.location != from {
                    let mut vv = source_files
                        .first()
                        .map(|f| f.vv.clone())
                        .unwrap_or_default();
                    for file in &remote.files {
                        vv.merge(&file.vv);
                    }
                    decision.events.push((
                        "file_deleted".into(),
                        EventData {
                            path: from.to_owned(),
                            vv,
                            ..Default::default()
                        },
                    ));
                }
            } else if remote.location != from {
                decision.files.extend(source_files);
                decision.directories.extend(source_dirs);
            }
            for (destination, destination_files, destination_dirs) in destinations {
                if !destination_files.is_empty() || !destination_dirs.is_empty() {
                    if resolution == ConflictResolution::KeepBoth {
                        let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
                        let mut target = None;
                        for n in 1..=100 {
                            let candidate = conflict_name(&destination, &date, n);
                            let abs = root.abs(&c.share.root_rel(&candidate)?)?;
                            match abs.symlink_metadata() {
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                    target = Some(candidate);
                                    break;
                                }
                                Ok(_) => {}
                                Err(e) => return Err(e.into()),
                            }
                        }
                        let target = target.ok_or_else(|| invalid("no free conflict copy name"))?;
                        decision.steps.push(if destination_dirs.is_empty() {
                            OperationAction::Rename {
                                source: c.share.location(&destination)?,
                                target: c.share.location(&target)?,
                                expected_revision: Some(files::revision(
                                    &root
                                        .abs(&c.share.root_rel(&destination)?)?
                                        .symlink_metadata()?,
                                )),
                            }
                        } else {
                            OperationAction::MoveFolder {
                                source: c.share.location(&destination)?,
                                target: c.share.location(&target)?,
                            }
                        });
                        for mut file in destination_files {
                            file.path = rebase(&file.path, &destination, &target)?;
                            decision.files.push(file);
                        }
                        for dir in destination_dirs {
                            decision
                                .directories
                                .push(rebase(&dir, &destination, &target)?);
                        }
                    } else if let Some(action) = trash(c, root, &destination)? {
                        // Do not announce this local preparation: the peer still serves its tree.
                        decision.steps.push(action);
                    }
                }
                if remote.location != c.rel && !peers::inbox::overlaps(&c.rel, &remote.location) {
                    decision.absent.push(c.rel.clone());
                }
            }
            remote.directories.sort_by_key(|p| p.split('/').count());
            for dir in &remote.directories {
                decision.steps.push(OperationAction::CreateFolder {
                    target: c.share.location(dir)?,
                });
            }
            decision.directories.extend(remote.directories);
            decision.files.extend(remote.files);
        }
        ConflictResolution::Skip => return Err(invalid("choose a version before dismissing")),
    }
    for file in &mut decision.files {
        file.vv.increment(&state.instance_id);
    }
    for (_, event) in &mut decision.events {
        event.vv.increment(&state.instance_id);
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let marked = sqlx::query(
        "UPDATE conflicts SET status='resolving',resolution=? WHERE id=? AND status='open'",
    )
    .bind(label)
    .bind(&c.id)
    .execute(&mut *tx)
    .await?;
    if marked.rows_affected() != 1 {
        return Err(AppError::conflict(
            "not_open",
            "conflict is already being resolved",
        ));
    }
    sqlx::query("DELETE FROM pending_peer_ops WHERE kind='move_decision_open' AND conflict_id=?")
        .bind(&c.id)
        .execute(&mut *tx)
        .await?;
    for recovery_id in &decision.recovery_ids {
        sqlx::query(
            "INSERT OR IGNORE INTO sync_recovery_links(recovery_id,kind,target_id) VALUES (?,'operation',?)",
        ).bind(recovery_id).bind(format!("move-decision:{}", c.id)).execute(&mut *tx).await?;
    }
    advance_in(&mut tx, c, &mut decision).await?;
    tx.commit().await?;
    state.ops_wake.notify_one();
    state.sync_wake.notify_one();
    state.emit(ServerEvent::ConflictsChanged);
    Ok(())
}

async fn advance_in(
    tx: &mut sqlx::SqliteConnection,
    c: &Conflict,
    decision: &mut Decision,
) -> AppResult<()> {
    let (operation, kind) = if decision.steps.is_empty() {
        decision.current = None;
        for file in decision.files.iter().filter(|f| f.download) {
            let existing: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM transfers WHERE share_id=? AND peer_id=? AND rel_path=?
                 AND sha256=? AND size=? AND status IN ('queued','running'))",
            ).bind(&c.share.id).bind(&c.peer_id).bind(&file.path).bind(&file.sha256)
                .bind(i64::try_from(file.size).map_err(AppError::internal)?).fetch_one(&mut *tx).await?;
            if !existing {
                peers::queue_transfer_in(
                    tx,
                    &c.peer_id,
                    &c.share,
                    &file.path,
                    &file.sha256,
                    file.size,
                    &file.vv,
                )
                .await?;
            }
            for recovery_id in &decision.recovery_ids {
                sqlx::query(
                    "INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id)
                     SELECT ?,'transfer',id FROM transfers WHERE share_id=? AND rel_path=? AND peer_id=?
                     AND status IN ('queued','running')",
                ).bind(recovery_id).bind(&c.share.id).bind(&file.path).bind(&c.peer_id)
                    .execute(&mut *tx).await?;
            }
        }
        // This is a decision, not an executable operation. It intentionally has no operations
        // row: completed preparations must not block their own content publications.
        (format!("move-decision:{}", c.id), "move_decision_wait")
    } else {
        let action = decision.steps.remove(0);
        decision.current = Some(action.clone());
        let request = OperationRequest {
            idempotency_key: crate::crypto::new_id("move-choice"),
            label: Some(format!("Resolve move conflict on {}", c.rel)),
            actions: vec![action],
            allow_owner_transfer: false,
            allow_partial: false,
        };
        let principal = Principal::Peer {
            peer_id: c.peer_id.clone(),
        };
        let accepted = crate::ops::insert_in(tx, &principal, &request, None).await?;
        for recovery_id in &decision.recovery_ids {
            sqlx::query(
                "INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id) VALUES (?,'operation',?)",
            ).bind(recovery_id).bind(&accepted.operation_id).execute(&mut *tx).await?;
        }
        (accepted.operation_id, "move_decision_step")
    };
    sqlx::query(
        "INSERT INTO pending_peer_ops
         (operation_id,idx,share_id,kind,from_path,to_path,vv_json,peer_id,conflict_id)
         VALUES (?,0,?,?,?, ?,?,?,?)",
    )
    .bind(operation)
    .bind(&c.share.id)
    .bind(kind)
    .bind(&c.remote_from)
    .bind(&c.rel)
    .bind(serde_json::to_string(decision)?)
    .bind(&c.peer_id)
    .bind(&c.id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

pub(in crate::peers) async fn finish_step(
    state: &AppState,
    operation_id: &str,
    conflict_id: &str,
    raw: &str,
    ok: bool,
) -> AppResult<()> {
    if !ok {
        // Keep the pending preparation and its footprint. Recovery may retry it; never turn a
        // partially applied choice into a successful resolution or unblock another choice.
        return Ok(());
    }
    let c = super::load_conflict(state, conflict_id).await?;
    let mut decision: Decision = serde_json::from_str(raw)?;
    let owners: Vec<String> = sqlx::query_scalar(
        "SELECT recovery_id FROM sync_recovery_links WHERE kind='operation' AND target_id=?
         UNION SELECT id FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(operation_id)
    .bind(operation_id)
    .fetch_all(&state.db)
    .await?;
    for owner in owners {
        if !decision.recovery_ids.contains(&owner) {
            decision.recovery_ids.push(owner);
        }
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let removed = sqlx::query(
        "DELETE FROM pending_peer_ops WHERE operation_id=? AND kind='move_decision_step'",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    if removed.rows_affected() == 1 {
        sqlx::query("INSERT OR IGNORE INTO completed_peer_bookkeeping(operation_id) VALUES (?)")
            .bind(operation_id)
            .execute(&mut *tx)
            .await?;
        match &decision.current {
            Some(
                OperationAction::Trash { source, .. } | OperationAction::TrashFolder { source, .. },
            ) => {
                let rel = c
                    .share
                    .rel_of(source)
                    .ok_or_else(|| invalid("trash escaped the share"))?;
                sqlx::query(
                    "UPDATE replica_files SET deleted=1 WHERE share_id=?
                     AND (rel_path=? OR substr(rel_path,1,length(?)+1)=?||'/')",
                )
                .bind(&c.share.id)
                .bind(&rel)
                .bind(&rel)
                .bind(&rel)
                .execute(&mut *tx)
                .await?;
            }
            Some(
                OperationAction::Rename { source, target, .. }
                | OperationAction::MoveFolder { source, target },
            ) => {
                let from = c
                    .share
                    .rel_of(source)
                    .ok_or_else(|| invalid("move escaped the share"))?;
                let to = c
                    .share
                    .rel_of(target)
                    .ok_or_else(|| invalid("move escaped the share"))?;
                peers::rename_rows_in(&mut tx, &c.share.id, &from, &to, None).await?;
            }
            Some(OperationAction::CreateFolder { .. }) => {}
            _ => return Err(invalid("move preparation has no verified action")),
        }
        advance_in(&mut tx, &c, &mut decision).await?;
    }
    tx.commit().await?;
    state.ops_wake.notify_one();
    state.sync_wake.notify_one();
    Ok(())
}

pub(in crate::peers) async fn content_decision(
    state: &AppState,
    share: &peers::Share,
    event: &peers::PeerEvent,
) -> AppResult<Option<String>> {
    if event.kind != "file_changed" {
        return Ok(None);
    }
    let plans: Vec<String> = sqlx::query_scalar(
        "SELECT p.vv_json FROM pending_peer_ops p JOIN conflicts c ON c.id=p.conflict_id
         WHERE p.share_id=? AND p.kind IN ('move_decision_step','move_decision_wait')
         AND c.status='resolving'",
    )
    .bind(&share.id)
    .fetch_all(&state.db)
    .await?;
    for raw in plans {
        let plan: Decision = serde_json::from_str(&raw)?;
        if let Some(file) = plan.files.iter().find(|f| {
            f.path == event.data.path
                && matches!(
                    event.data.vv.compare(&f.vv),
                    ism_core::version::Causality::Equal | ism_core::version::Causality::DominatedBy
                )
        }) {
            if !file.download {
                return Ok(Some("keep_local".into()));
            }
            if event.data.sha256.as_deref() == Some(&file.sha256)
                && event.data.size == Some(file.size)
            {
                return Ok(Some("keep_remote".into()));
            }
        }
    }
    Ok(None)
}

pub(in crate::peers) async fn event_decision(
    state: &AppState,
    share: &peers::Share,
    event: &peers::PeerEvent,
) -> AppResult<Option<String>> {
    if event.kind != "folder_moved" {
        return Ok(None);
    }
    sqlx::query_scalar(
        "SELECT c.resolution FROM pending_peer_ops p JOIN conflicts c ON c.id=p.conflict_id
         WHERE p.share_id=? AND p.kind IN ('move_decision_step','move_decision_wait')
         AND c.status='resolving' AND (
           (json_extract(p.vv_json,'$.event_identity[0]')=? AND json_extract(p.vv_json,'$.event_identity[1]')=?)
           OR EXISTS(SELECT 1 FROM json_each(p.vv_json,'$.lineage') l
             WHERE json_extract(l.value,'$.origin')=? AND json_extract(l.value,'$.origin_seq')=?)
         ) LIMIT 1",
    )
    .bind(&share.id)
    .bind(&event.origin)
    .bind(event.origin_seq)
    .bind(&event.origin)
    .bind(event.origin_seq)
    .fetch_optional(&state.db)
    .await
    .map_err(Into::into)
}

pub(in crate::peers) async fn process(state: &AppState) -> AppResult<()> {
    super::process_folder_restorations(state).await?;
    let plans: Vec<(String, String)> = sqlx::query_as(
        "SELECT conflict_id,vv_json FROM pending_peer_ops WHERE kind='move_decision_wait'",
    )
    .fetch_all(&state.db)
    .await?;
    for (id, raw) in plans {
        let c = match super::load_conflict(state, &id).await {
            Ok(c) => c,
            Err(error) => {
                tracing::warn!(conflict=%id, "move choice context waits: {}", error.message());
                continue;
            }
        };
        let mut decision: Decision = match serde_json::from_str(&raw) {
            Ok(decision) => decision,
            Err(error) => {
                tracing::warn!(conflict=%id, "move choice journal is unverifiable: {error}");
                continue;
            }
        };
        let fresh = if decision.remote_evidence.is_some() {
            // Evidence is read before locking: another peer may be processing its own choice.
            match evidence_at(state, &c, decision.remote_evidence.as_ref()).await {
                Ok(evidence) => Some(evidence),
                Err(error) => {
                    tracing::debug!(conflict=%id, "move choice evidence waits: {}", error.message());
                    continue;
                }
            }
        } else {
            None
        };
        let _guard = state.replication_operation_lock.lock().await;
        if let Some(fresh) = fresh {
            match refresh_choice(state, &c, &mut decision, fresh).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(conflict=%id, "move choice refresh waits: {}", error.message());
                    continue;
                }
            }
        }
        if let Err(error) = finish_choice(state, &id, &serde_json::to_string(&decision)?).await {
            tracing::warn!(conflict = %id, "move decision verification deferred: {}", error.message());
        }
    }
    Ok(())
}
fn contents(evidence: &Evidence) -> BTreeMap<String, (Option<String>, u64)> {
    evidence
        .directories
        .iter()
        .map(|path| {
            (
                path.strip_prefix(&evidence.location)
                    .unwrap_or(path)
                    .to_owned(),
                (None, 0),
            )
        })
        .chain(evidence.files.iter().map(|file| {
            (
                file.path
                    .strip_prefix(&evidence.location)
                    .unwrap_or(&file.path)
                    .to_owned(),
                (Some(file.sha256.clone()), file.size),
            )
        }))
        .collect()
}

fn previously_displaced(decision: &Decision, previous: &Evidence, fresh: &Evidence) -> bool {
    if previous.location != fresh.location {
        return false;
    }
    let before = contents(previous);
    let after = contents(fresh);
    if before
        .iter()
        .any(|(path, content)| after.get(path) != Some(content))
    {
        return false;
    }
    let covered = |path: &str| {
        decision.events.iter().any(|(kind, event)| {
            kind == "file_deleted" && event.path == path
                || kind == "folder_trashed"
                    && (event.path == path || path.starts_with(&format!("{}/", event.path)))
        })
    };
    for directory in &fresh.directories {
        if !previous.directories.contains(directory)
            && (!covered(directory) || !decision.displaced_directories.contains(directory))
        {
            return false;
        }
    }
    for file in &fresh.files {
        if previous.files.iter().any(|prior| prior.path == file.path) {
            continue;
        }
        if !covered(&file.path)
            || !decision.displaced_files.iter().any(|displaced| {
                displaced.path == file.path
                    && displaced.sha256 == file.sha256
                    && displaced.size == file.size
                    && matches!(
                        file.vv.compare(&displaced.vv),
                        ism_core::version::Causality::Equal
                            | ism_core::version::Causality::DominatedBy
                    )
            })
        {
            return false;
        }
    }
    true
}

async fn reopen_choice(state: &AppState, c: &Conflict, decision: &Decision) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(
        "UPDATE conflicts SET status='open',resolution=NULL WHERE id=? AND status='resolving'",
    )
    .bind(&c.id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE pending_peer_ops SET kind='move_decision_open' WHERE kind='move_decision_wait' AND conflict_id=?")
                .bind(&c.id).execute(&mut *tx).await?;
    for file in decision.files.iter().filter(|file| file.download) {
        sqlx::query(
                    "DELETE FROM sync_recovery_links WHERE kind='transfer' AND target_id IN
                     (SELECT id FROM transfers WHERE share_id=? AND peer_id=? AND rel_path=? AND status IN ('queued','running'))",
                ).bind(&c.share.id).bind(&c.peer_id).bind(&file.path).execute(&mut *tx).await?;
        sqlx::query(
                    "UPDATE transfers SET status='cancelled',error='choice evidence changed; bytes retained'
                     WHERE share_id=? AND peer_id=? AND rel_path=? AND status IN ('queued','running')",
                ).bind(&c.share.id).bind(&c.peer_id).bind(&file.path).execute(&mut *tx).await?;
    }
    sqlx::query("DELETE FROM sync_recovery_links WHERE recovery_id=?")
        .bind(format!("conflict:{}", c.id))
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM sync_recovery_links WHERE kind='operation' AND target_id=?")
        .bind(format!("move-decision:{}", c.id))
        .execute(&mut *tx)
        .await?;
    for owner in &decision.recovery_ids {
        sqlx::query(
                    "UPDATE sync_recovery SET state='needs_decision',resolution=NULL,code='choose_version',
                     detail='The competing contents or layout changed; preserved versions require a new choice',updated_at=? WHERE id=?",
                ).bind(crate::db::now()).bind(owner).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    state.emit(ServerEvent::ConflictsChanged);
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn refresh_choice(
    state: &AppState,
    c: &Conflict,
    decision: &mut Decision,
    fresh: Evidence,
) -> AppResult<bool> {
    let Some(previous) = &decision.remote_evidence else {
        return Ok(false);
    };
    if contents(previous) != contents(&fresh) && !previously_displaced(decision, previous, &fresh) {
        reopen_choice(state, c, decision).await?;
        return Ok(true);
    }
    if previous.location == fresh.location {
        return Ok(false);
    }
    let old = previous.location.clone();
    let new = fresh.location.clone();
    let resolution: String = sqlx::query_scalar("SELECT resolution FROM conflicts WHERE id=?")
        .bind(&c.id)
        .fetch_one(&state.db)
        .await?;
    if let Some(from) = &c.remote_from
        && peers::inbox::overlaps(&new, from)
    {
        if new != *from || resolution != "keep_remote" {
            reopen_choice(state, c, decision).await?;
            return Ok(true);
        }
        decision
            .absent
            .retain(|path| !peers::inbox::overlaps(path, &new));
        decision.events.retain(|(kind, event)| {
            !matches!(kind.as_str(), "folder_trashed" | "file_deleted")
                || !peers::inbox::overlaps(&event.path, &new)
        });
    }
    let root = files::root(&state.db, &c.share.root_id).await?;
    root.ensure_available()?;
    if resolution == "keep_local" {
        decision
            .events
            .retain(|(_, event)| !peers::inbox::overlaps(&event.path, &old));
        decision
            .absent
            .retain(|path| !peers::inbox::overlaps(path, &old));
        if !decision.directories.contains(&new) && !decision.files.iter().any(|f| f.path == new) {
            decision.events.push((
                "folder_trashed".into(),
                EventData {
                    path: new.clone(),
                    ..Default::default()
                },
            ));
            decision.absent.push(new.clone());
        } else {
            return Err(invalid(
                "relocated peer tree overlaps the selected local layout",
            ));
        }
    } else {
        let old_path = root.abs(&c.share.root_rel(&old)?)?;
        let new_path = root.abs(&c.share.root_rel(&new)?)?;
        let allowed = contents(previous);
        for abs in [&old_path, &new_path] {
            match abs.symlink_metadata() {
                Ok(meta) if meta.is_dir() => {
                    for entry in crate::ops::plan::folder_inventory(abs)? {
                        let name = if entry.name.is_empty() {
                            String::new()
                        } else {
                            format!("/{}", entry.name)
                        };
                        if allowed.get(&name).map(|(sha, _)| sha) != Some(&entry.sha256) {
                            reopen_choice(state, c, decision).await?;
                            return Ok(true);
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
                _ => {
                    reopen_choice(state, c, decision).await?;
                    return Ok(true);
                }
            }
        }
        if old_path.is_dir() {
            decision.steps.push(if new_path.exists() {
                trash(c, &root, &old)?.ok_or_else(|| invalid("choice source disappeared"))?
            } else {
                OperationAction::MoveFolder {
                    source: c.share.location(&old)?,
                    target: c.share.location(&new)?,
                }
            });
        } else if !new_path.exists() {
            for dir in &fresh.directories {
                decision.steps.push(OperationAction::CreateFolder {
                    target: c.share.location(dir)?,
                });
            }
        }
        for file in decision.files.iter_mut().filter(|file| file.download) {
            file.path = rebase(&file.path, &old, &new)?;
            if let Some(remote) = fresh.files.iter().find(|r| r.path == file.path) {
                file.vv.merge(&remote.vv);
                file.vv.increment(&state.instance_id);
            }
        }
        for dir in &mut decision.directories {
            if peers::inbox::overlaps(dir, &old) {
                *dir = rebase(dir, &old, &new)?;
            }
        }
        decision.absent.push(old.clone());
    }
    decision.lineage = fresh.lineage.clone();
    decision.remote_evidence = Some(fresh);
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    for file in decision.files.iter().filter(|f| f.download) {
        let old_file = rebase(&file.path, &new, &old)?;
        sqlx::query(
                    "UPDATE transfers SET rel_path=?,vv_json=? WHERE share_id=? AND peer_id=? AND rel_path=?
                     AND sha256=? AND status IN ('queued','running','failed')",
                ).bind(&file.path).bind(serde_json::to_string(&file.vv)?).bind(&c.share.id).bind(&c.peer_id)
                    .bind(&old_file).bind(&file.sha256).execute(&mut *tx).await?;
    }
    sqlx::query("DELETE FROM pending_peer_ops WHERE kind='move_decision_wait' AND conflict_id=?")
        .bind(&c.id)
        .execute(&mut *tx)
        .await?;
    advance_in(&mut tx, c, decision).await?;
    tx.commit().await?;
    state.ops_wake.notify_waiters();
    state.sync_wake.notify_waiters();
    Ok(true)
}

pub(in crate::peers) async fn footprints(
    state: &AppState,
    share: &peers::Share,
) -> AppResult<Vec<String>> {
    let plans: Vec<(Option<String>, Option<String>, String)> = sqlx::query_as(
        "SELECT from_path,to_path,vv_json FROM pending_peer_ops
         WHERE share_id=? AND kind IN ('move_decision_step','move_decision_wait','move_decision_open')",
    )
    .bind(&share.id)
    .fetch_all(&state.db)
    .await?;
    let mut paths = Vec::new();
    for (from, to, raw) in plans {
        paths.extend([from, to].into_iter().flatten());
        let decision: Decision = serde_json::from_str(&raw)?;
        paths.extend(decision.directories);
        paths.extend(decision.files.into_iter().map(|file| file.path));
        paths.extend(decision.absent);
    }
    let restores: Vec<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT from_path,to_path FROM pending_peer_ops WHERE share_id=? AND kind='folder_restore'",
    )
    .bind(&share.id)
    .fetch_all(&state.db)
    .await?;
    for (from, to) in restores {
        paths.extend([from, to].into_iter().flatten());
    }
    paths.extend(super::move_restore::footprints(state, share).await?);
    Ok(paths)
}

async fn finish_choice(state: &AppState, id: &str, raw: &str) -> AppResult<()> {
    let c = super::load_conflict(state, id).await?;
    if c.status != "resolving" {
        return Ok(());
    }
    let decision: Decision = serde_json::from_str(raw)?;
    let root = files::root(&state.db, &c.share.root_id).await?;
    root.ensure_available()?;
    let paths: Vec<&str> = decision
        .files
        .iter()
        .map(|f| f.path.as_str())
        .chain(decision.directories.iter().map(String::as_str))
        .chain(decision.absent.iter().map(String::as_str))
        .collect();
    if peers::inbox::require_operations_stable_retained(
        state,
        &c.share,
        &paths,
        &decision.retained_transfers,
    )
    .await
    .is_err()
    {
        return Ok(());
    }
    let mut ready = true;
    for directory in &decision.directories {
        if decision
            .directories
            .iter()
            .any(|parent| directory.starts_with(&format!("{parent}/")))
        {
            continue;
        }
        let abs = root.abs(&c.share.root_rel(directory)?)?;
        if !abs
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.is_dir())
        {
            ready = false;
            break;
        }
        let expected: std::collections::BTreeMap<String, Option<String>> = decision
            .directories
            .iter()
            .filter_map(|path| {
                if path == directory {
                    Some((String::new(), None))
                } else {
                    path.strip_prefix(&format!("{directory}/"))
                        .map(|name| (name.to_owned(), None))
                }
            })
            .chain(decision.files.iter().filter_map(|file| {
                file.path
                    .strip_prefix(&format!("{directory}/"))
                    .map(|name| (name.to_owned(), Some(file.sha256.clone())))
            }))
            .collect();
        let actual: std::collections::BTreeMap<String, Option<String>> =
            crate::ops::plan::folder_inventory(&abs)?
                .into_iter()
                .map(|entry| (entry.name, entry.sha256))
                .collect();
        if actual != expected {
            ready = false;
            break;
        }
    }
    for file in &decision.files {
        let busy: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM transfers WHERE share_id=? AND rel_path=?
             AND NOT(status='cancelled' AND error='choice evidence changed; bytes retained'
                 AND id IN (SELECT value FROM json_each(?))))",
        )
        .bind(&c.share.id)
        .bind(&file.path)
        .bind(serde_json::to_string(&decision.retained_transfers)?)
        .fetch_one(&state.db)
        .await?;
        if busy {
            ready = false;
            break;
        }
        let abs = root.abs(&c.share.root_rel(&file.path)?)?;
        if files::replacement_fingerprint(&abs)?.is_none_or(|(_, hash)| hash != file.sha256) {
            ready = false;
            break;
        }
        if file.download {
            // Bytes alone are not publication: a download may have been staged but its
            // managed replacement/bookkeeping has not committed.
            let published = peers::row(state, &c.share.id, &file.path).await?;
            if published.is_none_or(|r| {
                r.deleted
                    || r.sha256.as_deref() != Some(&file.sha256)
                    || !matches!(
                        r.vv.compare(&file.vv),
                        ism_core::version::Causality::Equal
                            | ism_core::version::Causality::Dominates
                    )
            }) {
                ready = false;
                break;
            }
        }
    }
    for dir in &decision.directories {
        let abs = root.abs(&c.share.root_rel(dir)?)?;
        if !abs.symlink_metadata().is_ok_and(|m| m.is_dir()) {
            ready = false;
        }
    }
    for path in &decision.absent {
        let abs = root.abs(&c.share.root_rel(path)?)?;
        match abs.symlink_metadata() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                ready = false;
            }
            Err(e) => return Err(e.into()),
        }
    }
    if !ready {
        return Ok(());
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let removed = sqlx::query(
        "DELETE FROM pending_peer_ops WHERE kind='move_decision_wait' AND conflict_id=?",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    if removed.rows_affected() != 1 {
        return Ok(());
    }
    for (kind, event) in &decision.events {
        if kind == "file_deleted" {
            let mut row = peers::row_in(&mut tx, &c.share.id, &event.path)
                .await?
                .unwrap_or(Row {
                    size: 0,
                    mtime_ns: 0,
                    sha256: None,
                    deleted: true,
                    vv: VersionVector::default(),
                });
            row.deleted = true;
            row.vv.merge(&event.vv);
            peers::put_row_in(&mut tx, &c.share.id, &event.path, &row).await?;
        }
        peers::append_local_in(&mut tx, &state.instance_id, &c.share.name, kind, event).await?;
    }
    for dir in &decision.directories {
        peers::append_local_in(
            &mut tx,
            &state.instance_id,
            &c.share.name,
            "folder_created",
            &EventData {
                path: dir.clone(),
                ..Default::default()
            },
        )
        .await?;
    }
    for file in &decision.files {
        let abs = root.abs(&c.share.root_rel(&file.path)?)?;
        let meta = abs.symlink_metadata()?;
        peers::put_row_in(
            &mut tx,
            &c.share.id,
            &file.path,
            &Row {
                size: meta.len() as i64,
                mtime_ns: files::mtime_ns(&meta),
                sha256: Some(file.sha256.clone()),
                deleted: false,
                vv: file.vv.clone(),
            },
        )
        .await?;
        peers::append_local_in(
            &mut tx,
            &state.instance_id,
            &c.share.name,
            "file_changed",
            &EventData {
                path: file.path.clone(),
                sha256: Some(file.sha256.clone()),
                size: Some(file.size),
                vv: file.vv.clone(),
                ..Default::default()
            },
        )
        .await?;
    }
    super::resolved_in(&mut tx, id, "keep_remote").await?;
    sqlx::query("DELETE FROM sync_recovery_links WHERE kind='operation' AND target_id=?")
        .bind(format!("move-decision:{id}"))
        .execute(&mut *tx)
        .await?;
    let receipts: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM peer_inbox WHERE peer_id=? AND share_name=? AND status!='done'
             AND json_extract(event_json,'$.kind') IN ('file_moved','folder_moved')",
    )
    .bind(&c.peer_id)
    .bind(&c.share.name)
    .fetch_all(&mut *tx)
    .await?;
    for raw in receipts {
        let event: peers::PeerEvent = serde_json::from_str(&raw)?;
        if decision.event_identity.as_ref() == Some(&(event.origin.clone(), event.origin_seq)) {
            peers::inbox::done_in(&mut tx, &c.peer_id, &event, false).await?;
        }
    }
    for event in &decision.lineage {
        sqlx::query(
            "INSERT OR IGNORE INTO peer_decision_receipts(origin,origin_seq,share_name,evidence_peer_id)
             VALUES (?,?,?,?)",
        ).bind(&event.origin).bind(event.origin_seq).bind(&c.share.name).bind(&c.peer_id)
            .execute(&mut *tx).await?;
        peers::inbox::done_in(&mut tx, &c.peer_id, event, false).await?;
    }
    if let Some((origin, sequence)) = &decision.event_identity {
        sqlx::query(
            "INSERT OR IGNORE INTO peer_decision_receipts(origin,origin_seq,share_name,evidence_peer_id)
             VALUES (?,?,?,?)",
        ).bind(origin).bind(sequence).bind(&c.share.name).bind(&c.peer_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    state.emit(ServerEvent::ConflictsChanged);
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}
