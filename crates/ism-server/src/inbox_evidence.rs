//! Retained peer effects grouped by their actual scope and last recorded outcome.

use std::collections::HashMap;

use futures_util::TryStreamExt;
use ism_core::dto::ActivityExample;
use sqlx::Row;

use crate::error::{AppResult, redact};
use crate::peers::{InventoryEntry, InventoryPage, PeerEvent};
use crate::state::AppState;

const EXAMPLES: usize = 5;
const REASONS: usize = 32;

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum Kind {
    Event,
    FolderRemoval,
    Inventory,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::FolderRemoval => "folder-removal",
            Self::Inventory => "inventory",
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum Status {
    Waiting,
    Failed,
    Unavailable,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
        }
    }
}

pub(crate) struct Group {
    pub peer_id: String,
    pub peer_name: String,
    pub share_id: Option<String>,
    pub share_name: String,
    pub kind: Kind,
    pub status: Status,
    pub event_status: String,
    pub count: u64,
    pub updated_at: Option<String>,
    pub operation_id: Option<String>,
    pub error: Option<String>,
    pub reasons: HashMap<String, u64>,
    pub examples: Vec<ActivityExample>,
}

pub(crate) struct Snapshot {
    pub groups: Vec<Group>,
    pub unreadable: u64,
    pub replication_pending: u64,
}

impl Group {
    pub fn key(&self) -> String {
        format!(
            "peer-inbox:{}:{}:{}:{}:{}",
            self.peer_id,
            self.share_name,
            self.kind.as_str(),
            self.status.as_str(),
            self.event_status
        )
    }
}

struct RowEvidence {
    peer_id: String,
    peer_name: String,
    share_id: Option<String>,
    share_name: String,
    kind: Kind,
    status: Status,
    event_status: String,
    path: String,
    error: Option<String>,
    updated_at: Option<String>,
    operation_id: Option<String>,
}

fn add(
    groups: &mut Vec<Group>,
    indexes: &mut HashMap<(String, String, Kind, Status, String), usize>,
    row: RowEvidence,
) {
    let key = (
        row.peer_id.clone(),
        row.share_name.clone(),
        row.kind,
        row.status,
        row.event_status.clone(),
    );
    let index = *indexes.entry(key).or_insert_with(|| {
        let index = groups.len();
        groups.push(Group {
            peer_id: row.peer_id,
            peer_name: row.peer_name,
            share_id: row.share_id,
            share_name: row.share_name,
            kind: row.kind,
            status: row.status,
            event_status: row.event_status,
            count: 0,
            updated_at: None,
            operation_id: row.operation_id.clone(),
            error: None,
            reasons: HashMap::new(),
            examples: Vec::new(),
        });
        index
    });
    let group = &mut groups[index];
    group.count += 1;
    if group.count > 1 {
        group.operation_id = None;
    }
    if row.updated_at > group.updated_at {
        group.updated_at = row.updated_at;
    }
    let error = row.error.map(|value| redact(&value));
    if group.examples.len() < EXAMPLES {
        group.examples.push(ActivityExample {
            resource: row.path.clone(),
            detail: error.clone(),
        });
    }
    if let Some(error) = error {
        if group.error.is_none() {
            group.error = Some(error.clone());
        }
        let reason = if row.path.is_empty() {
            error
        } else {
            error.replace(&row.path, "<item>")
        };
        if group.reasons.contains_key(&reason) || group.reasons.len() < REASONS {
            *group.reasons.entry(reason).or_default() += 1;
        } else {
            *group
                .reasons
                .entry("Other reported reasons".into())
                .or_default() += 1;
        }
    }
}

fn event(raw: &str, seq: i64, origin: &str, origin_seq: i64, share: &str) -> Option<PeerEvent> {
    let event: PeerEvent = serde_json::from_str(raw).ok()?;
    if seq <= 0
        || origin.is_empty()
        || origin_seq <= 0
        || share.is_empty()
        || event.seq != seq
        || event.origin != origin
        || event.origin_seq != origin_seq
        || event.share != share
        || crate::peers::inbox::event_valid(&event).is_err()
    {
        return None;
    }
    Some(event)
}

pub(crate) async fn snapshot(state: &AppState) -> AppResult<Snapshot> {
    let mut tx = state.db.begin().await?;
    let mut groups = Vec::new();
    let mut indexes = HashMap::new();
    let mut unreadable = 0;
    let mut replication_pending = 0;
    let mut rows = sqlx::query(
        "SELECT i.peer_id,p.name AS peer_name,i.share_name,s.id AS share_id,i.status,
                i.error,i.operation_id,i.attempted_at,i.seq,i.origin,i.origin_seq,i.event_json
         FROM peer_inbox i JOIN peers p ON p.id=i.peer_id
         LEFT JOIN shares s ON s.name=i.share_name
         WHERE i.status!='done' ORDER BY i.peer_id,i.share_name,i.seq",
    )
    .fetch(&mut *tx);
    while let Some(row) = rows.try_next().await? {
        let peer_name: String = row.try_get("peer_name")?;
        let share_name: String = row.try_get("share_name")?;
        let seq: i64 = row.try_get("seq")?;
        let origin: String = row.try_get("origin")?;
        let origin_seq: i64 = row.try_get("origin_seq")?;
        let raw: String = row.try_get("event_json")?;
        let error: Option<String> = row.try_get("error")?;
        let event = event(&raw, seq, &origin, origin_seq, &share_name);
        let event_status: String = row.try_get("status")?;
        let operation_id: Option<String> = row.try_get("operation_id")?;
        let known_status = matches!(
            event_status.as_str(),
            "received" | "applying" | "awaiting_content"
        );
        if known_status && operation_id.is_none() {
            replication_pending += 1;
        }
        if event.is_none() || !known_status {
            unreadable += 1;
        }
        let kind = if event.as_ref().is_some_and(|event| {
            event.kind == crate::peers::FOLDER_PRUNED
                && event_status == "received"
                && operation_id.is_none()
                && error.as_deref()
                    == Some(
                        crate::peers::inbox::folder_pruned_message(
                            &peer_name,
                            &share_name,
                            &event.data.path,
                        )
                        .as_str(),
                    )
        }) {
            Kind::FolderRemoval
        } else {
            Kind::Event
        };
        let status = if event.is_none() || !known_status {
            Status::Unavailable
        } else if error.is_some() {
            Status::Failed
        } else {
            Status::Waiting
        };
        add(
            &mut groups,
            &mut indexes,
            RowEvidence {
                peer_id: row.try_get("peer_id")?,
                peer_name,
                share_id: row.try_get("share_id")?,
                share_name,
                kind,
                status,
                event_status: match event_status.as_str() {
                    "received" | "applying" | "awaiting_content" => event_status,
                    _ => "other".into(),
                },
                path: event.map_or_else(|| format!("Event {seq}"), |event| event.data.path),
                error,
                updated_at: row.try_get("attempted_at")?,
                operation_id,
            },
        );
    }
    drop(rows);

    let mut rows = sqlx::query(
        "SELECT b.peer_id,p.name AS peer_name,s.id AS share_id,s.name AS share_name,
                b.path,b.entry_json,b.error,b.operation_id,b.attempted_at
         FROM peer_backfill_inbox b JOIN peers p ON p.id=b.peer_id
         JOIN shares s ON s.id=b.share_id ORDER BY b.peer_id,s.name,b.path",
    )
    .fetch(&mut *tx);
    while let Some(row) = rows.try_next().await? {
        let path: String = row.try_get("path")?;
        let raw: String = row.try_get("entry_json")?;
        let error: Option<String> = row.try_get("error")?;
        let operation_id: Option<String> = row.try_get("operation_id")?;
        if operation_id.is_none() {
            replication_pending += 1;
        }
        let valid = serde_json::from_str::<InventoryEntry>(&raw)
            .ok()
            .is_some_and(|entry| {
                entry.path == path
                    && InventoryPage {
                        entries: vec![entry],
                        next: None,
                    }
                    .validate("")
                    .is_ok()
            });
        let status = if !valid {
            unreadable += 1;
            Status::Unavailable
        } else if error.is_some() {
            Status::Failed
        } else {
            Status::Waiting
        };
        add(
            &mut groups,
            &mut indexes,
            RowEvidence {
                peer_id: row.try_get("peer_id")?,
                peer_name: row.try_get("peer_name")?,
                share_id: row.try_get("share_id")?,
                share_name: row.try_get("share_name")?,
                kind: Kind::Inventory,
                status,
                event_status: String::new(),
                path,
                error,
                updated_at: row.try_get("attempted_at")?,
                operation_id,
            },
        );
    }
    drop(rows);
    tx.commit().await?;
    Ok(Snapshot {
        groups,
        unreadable,
        replication_pending,
    })
}
