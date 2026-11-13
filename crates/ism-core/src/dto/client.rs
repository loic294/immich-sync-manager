//! Types shared with OPM, ILA and peers.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Location {
    pub root_id: String,
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ClientAction {
    Read,
    Download,
    Upload,
    Manage,
    Trash,
    Purge,
    TransferOwner,
}

impl ClientAction {
    pub const ALL: [ClientAction; 7] = [
        Self::Read,
        Self::Download,
        Self::Upload,
        Self::Manage,
        Self::Trash,
        Self::Purge,
        Self::TransferOwner,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Download => "download",
            Self::Upload => "upload",
            Self::Manage => "manage",
            Self::Trash => "trash",
            Self::Purge => "purge",
            Self::TransferOwner => "transfer_owner",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.as_str() == raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DirectWrites {
    Enabled,
    Disabled,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ClientRoot {
    pub root_id: String,
    pub name: String,
    pub immich_import_path: Option<String>,
    pub opm_prefix: Option<String>,
    pub available: bool,
    pub direct_writes: DirectWrites,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ClientInfo {
    pub instance_id: String,
    pub instance_name: String,
    pub version: String,
    pub api_version: u32,
    pub token_kind: String,
    pub actions: Vec<ClientAction>,
    pub roots: Vec<ClientRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FolderEntry {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FileEntry {
    pub name: String,
    pub path: String,
    pub size: u64,
    pub revision: String,
    pub modified_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FolderListing {
    pub root_id: String,
    pub path: String,
    pub folders: Vec<FolderEntry>,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum FileRole {
    Original,
    Sidecar,
    Companion,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ManifestFile {
    pub role: FileRole,
    pub name: String,
    pub path: String,
    pub size: u64,
    pub revision: String,
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub immich_linked: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub shared: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Manifest {
    pub group_id: String,
    pub asset_id: Option<String>,
    pub location: Location,
    pub folder: String,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OnConflict {
    Fail,
    Rename,
    ReplaceIfExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UploadProvenance {
    pub source: String,
    pub original_location: Option<Location>,
    pub expected_revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UploadFileSpec {
    pub name: String,
    pub role: FileRole,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateUpload {
    pub target: Location,
    pub provenance: Option<UploadProvenance>,
    pub files: Vec<UploadFileSpec>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UploadFileState {
    pub index: u32,
    pub received: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UploadState {
    pub upload_id: String,
    pub status: String,
    pub files: Vec<UploadFileState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum OperationAction {
    Move {
        source: Location,
        target: Location,
        #[serde(default)]
        expected_revision: Option<String>,
    },
    Rename {
        source: Location,
        target: Location,
        #[serde(default)]
        expected_revision: Option<String>,
    },
    CreateFolder {
        target: Location,
    },
    MoveFolder {
        source: Location,
        target: Location,
    },
    Trash {
        source: Location,
    },
    TrashFolder {
        source: Location,
    },
    Restore {
        trash_id: String,
    },
    Purge {
        trash_id: String,
    },
    /// Internal: publish a committed upload. Not accepted from clients.
    #[serde(skip_deserializing)]
    #[ts(skip)]
    PublishUpload {
        upload_id: String,
    },
}

impl OperationAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Move { .. } => "move",
            Self::Rename { .. } => "rename",
            Self::CreateFolder { .. } => "create_folder",
            Self::MoveFolder { .. } => "move_folder",
            Self::Trash { .. } => "trash",
            Self::TrashFolder { .. } => "trash_folder",
            Self::Restore { .. } => "restore",
            Self::Purge { .. } => "purge",
            Self::PublishUpload { .. } => "publish_upload",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationRequest {
    pub idempotency_key: String,
    #[serde(default)]
    pub label: Option<String>,
    pub actions: Vec<OperationAction>,
    #[serde(default)]
    pub allow_owner_transfer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PlannedFile {
    pub from: Option<Location>,
    pub to: Option<Location>,
    pub role: FileRole,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub copy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PlannedAsset {
    pub asset_id: String,
    pub library_id: Option<String>,
    pub owner: String,
    pub owner_change: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Issue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PreviewItem {
    pub index: u32,
    pub kind: String,
    pub files: Vec<PlannedFile>,
    pub assets: Vec<PlannedAsset>,
    pub blockers: Vec<Issue>,
    pub warnings: Vec<Issue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PreviewResponse {
    pub ok: bool,
    pub items: Vec<PreviewItem>,
    pub blockers: Vec<Issue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OperationStatus {
    Queued,
    WaitingMaintenance,
    Running,
    Completed,
    Partial,
    Failed,
    Cancelled,
    RecoveryRequired,
}

impl OperationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::WaitingMaintenance => "waiting_maintenance",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::RecoveryRequired => "recovery_required",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw {
            "waiting_maintenance" => Self::WaitingMaintenance,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "partial" => Self::Partial,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "recovery_required" => Self::RecoveryRequired,
            _ => Self::Queued,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Partial
                | Self::Failed
                | Self::Cancelled
                | Self::RecoveryRequired
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ItemStatus {
    Pending,
    Running,
    Done,
    Skipped,
    Failed,
    RecoveryRequired,
}

impl ItemStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::RecoveryRequired => "recovery_required",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw {
            "running" => Self::Running,
            "done" => Self::Done,
            "skipped" => Self::Skipped,
            "failed" => Self::Failed,
            "recovery_required" => Self::RecoveryRequired,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Progress {
    pub done: u32,
    pub total: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationItem {
    pub index: u32,
    pub kind: String,
    pub status: ItemStatus,
    pub error: Option<Issue>,
    #[ts(type = "unknown")]
    pub result: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Operation {
    pub operation_id: String,
    pub status: OperationStatus,
    pub label: Option<String>,
    pub source: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub progress: Progress,
    pub items: Vec<OperationItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationAccepted {
    pub operation_id: String,
    pub status: OperationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum Change {
    Moved {
        from: Location,
        to: Location,
    },
    Trashed {
        location: Location,
        trash_id: String,
    },
    Restored {
        location: Location,
        trash_id: String,
    },
    Purged {
        trash_id: String,
    },
    Created {
        location: Location,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationCommitted {
    pub operation_id: String,
    pub changes: Vec<Change>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TrashEntry {
    pub trash_id: String,
    pub location: Location,
    pub is_folder: bool,
    pub size: u64,
    pub trashed_at: String,
    pub operation_id: Option<String>,
    pub asset_ids: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_use_the_documented_shape() {
        let raw = r#"{"kind":"move","source":{"root_id":"r","path":"a.jpg"},"target":{"root_id":"r","path":"b/a.jpg"}}"#;
        let action: OperationAction = serde_json::from_str(raw).unwrap();
        assert_eq!(action.kind(), "move");
        assert!(
            serde_json::from_str::<OperationAction>(r#"{"kind":"publish_upload","upload_id":"x"}"#)
                .is_err()
        );
        assert_eq!(
            ClientAction::parse("transfer_owner"),
            Some(ClientAction::TransferOwner)
        );
    }
}
