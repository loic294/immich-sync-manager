use std::collections::{BTreeMap, BTreeSet, HashMap};

use ism_core::dto::{
    ConflictFilter, ConflictScope, Location, RecoveryKind, RecoveryState, RecoveryTarget,
    RecoveryView,
};
use serde::Deserialize;
use serde_json::Value;

use super::{inbox, net};
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::AppState;

#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct RecoveryFilter {
    pub share: String,
    pub folder: String,
    pub issue_type: String,
    pub status: String,
    pub kind: String,
}

impl RecoveryFilter {
    pub fn validate(&self) -> AppResult<()> {
        if !self.folder.is_empty() && self.share.is_empty() {
            return Err(AppError::bad_request(
                "invalid_recovery_filter",
                "a folder filter requires a share",
            ));
        }
        if !self.issue_type.is_empty()
            && self.issue_type != "none"
            && !self.issue_type.starts_with("code:")
        {
            return Err(AppError::bad_request(
                "invalid_recovery_filter",
                "unknown issue type filter",
            ));
        }
        if ![
            "",
            "queued",
            "fixing",
            "waiting",
            "needs_decision",
            "fixed",
            "not_retryable",
        ]
        .contains(&self.status.as_str())
            || !["", "operation", "transfer", "conflict"].contains(&self.kind.as_str())
        {
            return Err(AppError::bad_request(
                "invalid_recovery_filter",
                "unknown recovery status or item kind",
            ));
        }
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        !self.share.is_empty()
            || !self.folder.is_empty()
            || !self.issue_type.is_empty()
            || !self.status.is_empty()
            || !self.kind.is_empty()
    }

    pub fn matches(&self, row: &RecoveryView) -> bool {
        let issue_type_matches = match self.issue_type.as_str() {
            "" => true,
            "none" => row.code.is_none(),
            value => value.strip_prefix("code:") == row.code.as_deref(),
        };
        let status = match row.state {
            RecoveryState::Queued => "queued",
            RecoveryState::Fixing => "fixing",
            RecoveryState::Waiting => "waiting",
            RecoveryState::NeedsDecision => "needs_decision",
            RecoveryState::Fixed => "fixed",
            RecoveryState::NotRetryable => "not_retryable",
        };
        issue_type_matches
            && (self.status.is_empty() || self.status == status)
            && (self.kind.is_empty() || self.kind == kind_name(row.target.kind))
            && (self.share.is_empty()
                || scopes(row).any(|scope| {
                    scope.share_name == self.share
                        && (self.folder.is_empty()
                            || scope.path == self.folder
                            || scope
                                .path
                                .strip_prefix(&self.folder)
                                .is_some_and(|suffix| suffix.starts_with('/')))
                }))
    }
}

fn kind_name(kind: RecoveryKind) -> &'static str {
    match kind {
        RecoveryKind::Operation => "operation",
        RecoveryKind::Transfer => "transfer",
        RecoveryKind::Conflict => "conflict",
    }
}

fn scopes(row: &RecoveryView) -> impl Iterator<Item = &ConflictScope> {
    row.scopes.iter().chain(
        row.scopes
            .is_empty()
            .then_some(row.conflict_scope.as_ref())
            .flatten(),
    )
}

pub(super) fn facets<'a>(
    rows: impl Iterator<Item = &'a RecoveryView>,
) -> (Vec<ConflictFilter>, Vec<Option<String>>) {
    let mut folders: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut issue_types = BTreeSet::new();
    for row in rows {
        issue_types.insert(row.code.clone());
        for scope in scopes(row) {
            let entry = folders.entry(scope.share_name.clone()).or_default();
            for (index, _) in scope.path.match_indices('/') {
                entry.insert(scope.path[..index].to_owned());
            }
        }
    }
    (
        folders
            .into_iter()
            .map(|(share_name, folders)| ConflictFilter {
                share_name,
                folders: folders.into_iter().collect(),
            })
            .collect(),
        issue_types.into_iter().collect(),
    )
}

type ScopeKey = (String, String);
type ScopeSets = HashMap<ScopeKey, BTreeSet<(String, String)>>;
type ScopeMap = HashMap<ScopeKey, Vec<ConflictScope>>;

fn target_key(target: &RecoveryTarget) -> ScopeKey {
    (kind_name(target.kind).to_owned(), target.id.clone())
}

fn add_scope(scopes: &mut ScopeSets, kind: &str, id: &str, share: &str, path: &str) {
    scopes
        .entry((kind.to_owned(), id.to_owned()))
        .or_default()
        .insert((share.to_owned(), path.to_owned()));
}

fn ids(targets: &[RecoveryTarget], kind: RecoveryKind) -> AppResult<String> {
    Ok(serde_json::to_string(
        &targets
            .iter()
            .filter(|target| target.kind == kind && target.peer_id.is_none())
            .map(|target| &target.id)
            .collect::<Vec<_>>(),
    )?)
}

fn collect_upload_locations(raw: &str) -> AppResult<Vec<Location>> {
    let value: Value = serde_json::from_str(raw)?;
    let mut locations = Vec::new();
    inbox::collect_locations(&value, &mut locations)?;
    if let Some(target) = value.get("target")
        && let (Some(root_id), Some(folder)) = (
            target.get("root_id").and_then(Value::as_str),
            target.get("path").and_then(Value::as_str),
        )
    {
        for spec in value
            .get("files")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(value.get("existing_original"))
        {
            if let Some(name) = spec.get("name").and_then(Value::as_str) {
                let path = if folder.is_empty() {
                    name.to_owned()
                } else {
                    format!("{folder}/{name}")
                };
                files::parse_rel(&path)?;
                locations.push(Location {
                    root_id: root_id.to_owned(),
                    path,
                });
            }
        }
    }
    Ok(locations)
}

async fn local_scopes(state: &AppState, targets: &[RecoveryTarget]) -> AppResult<ScopeMap> {
    let conflict_ids = ids(targets, RecoveryKind::Conflict)?;
    let transfer_ids = ids(targets, RecoveryKind::Transfer)?;
    let operation_ids = ids(targets, RecoveryKind::Operation)?;
    let mut scopes = ScopeSets::new();
    let direct: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT 'conflict',c.id,s.name,c.rel_path FROM conflicts c
         JOIN shares s ON s.id=c.share_id WHERE c.id IN (SELECT value FROM json_each(?))
         UNION ALL SELECT 'transfer',t.id,s.name,t.rel_path FROM transfers t
         JOIN shares s ON s.id=t.share_id WHERE t.id IN (SELECT value FROM json_each(?))
         UNION ALL SELECT 'transfer',h.id,s.name,h.rel_path FROM transfer_history h
         JOIN shares s ON s.id=h.share_id WHERE h.id IN (SELECT value FROM json_each(?))
         AND NOT EXISTS(SELECT 1 FROM transfers t WHERE t.id=h.id)",
    )
    .bind(&conflict_ids)
    .bind(&transfer_ids)
    .bind(&transfer_ids)
    .fetch_all(&state.db)
    .await?;
    for (kind, id, share, path) in direct {
        add_scope(&mut scopes, &kind, &id, &share, &path);
    }

    let operations: Vec<(String, String)> = sqlx::query_as(
        "SELECT id,request_json FROM operations WHERE id IN (SELECT value FROM json_each(?))",
    )
    .bind(&operation_ids)
    .fetch_all(&state.db)
    .await?;
    if !operations.is_empty() {
        let shares = super::shares(state).await?;
        let mut locations: HashMap<String, Vec<Location>> = HashMap::new();
        let mut uploads: HashMap<String, Vec<String>> = HashMap::new();
        let mut trash: HashMap<String, Vec<String>> = HashMap::new();
        for (id, raw) in operations {
            let request: Value = match serde_json::from_str(&raw) {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(operation=%id, %error, "cannot determine recovery location");
                    continue;
                }
            };
            let mut found = Vec::new();
            if let Err(error) = inbox::collect_locations(&request, &mut found) {
                tracing::warn!(operation=%id, %error, "cannot determine recovery location");
                found.clear();
            }
            locations.insert(id.clone(), found);
            for action in request
                .get("actions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(upload) = action.get("upload_id").and_then(Value::as_str) {
                    uploads
                        .entry(upload.to_owned())
                        .or_default()
                        .push(id.clone());
                }
                if let Some(item) = action.get("trash_id").and_then(Value::as_str) {
                    trash.entry(item.to_owned()).or_default().push(id.clone());
                }
            }
        }

        let upload_ids = serde_json::to_string(&uploads.keys().collect::<Vec<_>>())?;
        let saved_uploads: Vec<(String, String)> = sqlx::query_as(
            "SELECT id,request_json FROM uploads WHERE id IN (SELECT value FROM json_each(?))",
        )
        .bind(upload_ids)
        .fetch_all(&state.db)
        .await?;
        for (id, raw) in saved_uploads {
            let found = match collect_upload_locations(&raw) {
                Ok(found) => found,
                Err(error) => {
                    tracing::warn!(upload=%id, %error, "cannot determine recovery location");
                    continue;
                }
            };
            if let Some(owners) = uploads.get(&id) {
                for owner in owners {
                    locations
                        .entry(owner.clone())
                        .or_default()
                        .extend(found.clone());
                }
            }
        }

        let trash_ids = serde_json::to_string(&trash.keys().collect::<Vec<_>>())?;
        let saved_trash: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT id,root_id,path FROM trash WHERE id IN (SELECT value FROM json_each(?))",
        )
        .bind(trash_ids)
        .fetch_all(&state.db)
        .await?;
        for (id, root_id, path) in saved_trash {
            if let Some(owners) = trash.get(&id) {
                for owner in owners {
                    locations.entry(owner.clone()).or_default().push(Location {
                        root_id: root_id.clone(),
                        path: path.clone(),
                    });
                }
            }
        }
        for (id, paths) in locations {
            for location in paths {
                for share in &shares {
                    if let Some(path) = inbox::share_path(share, &location) {
                        add_scope(&mut scopes, "operation", &id, &share.name, &path);
                    }
                }
            }
        }
    }

    type PendingOperation = (String, String, Option<String>, Option<String>);
    let pending: Vec<PendingOperation> = sqlx::query_as(
        "SELECT p.operation_id,s.name,p.from_path,p.to_path FROM pending_peer_ops p
         JOIN shares s ON s.id=p.share_id
         WHERE p.operation_id IN (SELECT value FROM json_each(?))",
    )
    .bind(&operation_ids)
    .fetch_all(&state.db)
    .await?;
    for (id, share, from, to) in pending {
        if from.is_none() && to.is_none() {
            add_scope(&mut scopes, "operation", &id, &share, "");
        }
        for path in from.into_iter().chain(to) {
            add_scope(&mut scopes, "operation", &id, &share, &path);
        }
    }

    let events: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT p.operation_id,s.name,p.event_json FROM pending_peer_events p
         JOIN shares s ON s.id=p.share_id
         WHERE p.operation_id IN (SELECT value FROM json_each(?))",
    )
    .bind(&operation_ids)
    .fetch_all(&state.db)
    .await?;
    for (id, share, raw) in events {
        let event: Value = match serde_json::from_str(&raw) {
            Ok(event) => event,
            Err(error) => {
                tracing::warn!(operation=%id, %error, "cannot determine recovery event location");
                continue;
            }
        };
        let mut found = false;
        for path in [
            event.pointer("/data/path").and_then(Value::as_str),
            event.pointer("/data/from").and_then(Value::as_str),
        ]
        .into_iter()
        .flatten()
        {
            files::parse_rel(path)?;
            add_scope(&mut scopes, "operation", &id, &share, path);
            found = true;
        }
        if !found {
            add_scope(&mut scopes, "operation", &id, &share, "");
        }
    }
    Ok(scopes
        .into_iter()
        .map(|(key, locations)| {
            (
                key,
                locations
                    .into_iter()
                    .map(|(share_name, path)| ConflictScope { share_name, path })
                    .collect(),
            )
        })
        .collect())
}

pub(super) async fn attach_local_scopes(
    state: &AppState,
    rows: &mut [RecoveryView],
) -> AppResult<()> {
    let targets: Vec<_> = rows.iter().map(|row| row.target.clone()).collect();
    let mut scopes = local_scopes(state, &targets).await?;
    for row in rows {
        row.scopes = scopes.remove(&target_key(&row.target)).unwrap_or_default();
    }
    Ok(())
}

pub(crate) async fn attach_peer_scopes(
    state: &AppState,
    peer: &net::Peer,
    rows: &mut [RecoveryView],
) -> AppResult<()> {
    attach_local_scopes(state, rows).await?;
    let allowed: BTreeSet<String> = super::shares(state)
        .await?
        .into_iter()
        .filter(|share| share.includes(&peer.id))
        .map(|share| share.name)
        .collect();
    for row in rows {
        row.scopes
            .retain(|scope| allowed.contains(&scope.share_name));
        if row
            .conflict_scope
            .as_ref()
            .is_some_and(|scope| !allowed.contains(&scope.share_name))
        {
            row.conflict_scope = None;
        }
    }
    Ok(())
}
