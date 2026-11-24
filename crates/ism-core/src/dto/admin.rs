//! Types used by the administration UI.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use super::client::{
    ClientAction, FileSettings, InstanceRef, Location, Operation, OperationStatus, PlannedFile,
    Progress,
};
use crate::maintenance::Phase;
use crate::schedule::Window;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionInfo {
    pub needs_setup: bool,
    pub authenticated: bool,
    pub username: Option<String>,
    pub instance_id: String,
    pub instance_name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SetupRequest {
    pub username: String,
    pub password: String,
    pub instance_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MaintenanceView {
    pub phase: Phase,
    pub epoch: u64,
    pub active_ops: u32,
    /// ISM holds Immich's `library` queue paused.
    pub queue_paused: bool,
    /// Immich's file watcher or scheduled scan was found on; writes are refused until an admin
    /// re-applies ISM's settings.
    pub drift: Option<String>,
    pub message: Option<String>,
}

/// How ISM detects changes in a mapped folder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WatchMode {
    /// Polling on network file systems, native change events otherwise.
    Auto,
    Native,
    Poll,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WatchSettingsInput {
    pub mode: WatchMode,
    /// Seconds between polling passes (60 to 86400).
    pub poll_interval_secs: u32,
}

/// Change detection of one mapped folder.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WatchStatusView {
    pub root_id: String,
    pub name: String,
    /// The folder belongs to an Immich library or a peer share and is watched.
    pub watched: bool,
    pub mode: WatchMode,
    pub poll_interval_secs: u32,
    /// `native` or `poll` once the watcher started.
    pub effective_mode: Option<String>,
    pub fs_type: Option<String>,
    pub last_pass_at: Option<String>,
    pub last_change_at: Option<String>,
    /// Detection is paused because the folder is unreachable or suddenly empty.
    pub paused: bool,
    pub warnings: Vec<String>,
    pub files: i64,
}

/// A pending or running Immich library scan requested by ISM.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ScanScheduleView {
    pub library_id: String,
    pub library_name: Option<String>,
    pub server_id: String,
    pub due_at: Option<String>,
    /// Seconds until the scan is due; 0 when due (it may wait for the write barrier).
    pub due_in_secs: Option<i64>,
    pub reasons: Vec<String>,
    pub running: bool,
    /// Due, but a managed operation holds the server's write barrier.
    pub waiting_for_barrier: bool,
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum ImmichMoveMode {
    ApiOnly,
    #[default]
    ApiWithDbFallback,
    DbOnly,
}

impl ImmichMoveMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ApiOnly => "api_only",
            Self::ApiWithDbFallback => "api_with_db_fallback",
            Self::DbOnly => "db_only",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "api_only" => Some(Self::ApiOnly),
            "api_with_db_fallback" => Some(Self::ApiWithDbFallback),
            "db_only" => Some(Self::DbOnly),
            _ => None,
        }
    }
}

#[cfg(test)]
mod move_mode_tests {
    use super::ImmichMoveMode;

    #[test]
    fn supports_the_persisted_move_modes() {
        assert_eq!(ImmichMoveMode::default().as_str(), "api_with_db_fallback");
        for mode in [
            ImmichMoveMode::ApiOnly,
            ImmichMoveMode::ApiWithDbFallback,
            ImmichMoveMode::DbOnly,
        ] {
            assert_eq!(ImmichMoveMode::parse(mode.as_str()), Some(mode));
        }
        assert_eq!(ImmichMoveMode::parse("unknown"), None);
    }
}

/// An instance the admin may make primary: this one or a paired peer.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PrimaryCandidate {
    pub instance_id: String,
    pub name: String,
    pub this_instance: bool,
}

/// `GET /api/admin/settings`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SettingsView {
    pub instance_id: String,
    pub instance_name: String,
    pub files: FileSettings,
    pub files_revision: i64,
    /// The primary instance, or null when every instance keeps its own file settings.
    pub primary: Option<InstanceRef>,
    /// False while another instance is primary: its values are shown read-only.
    pub files_editable: bool,
    pub primary_candidates: Vec<PrimaryCandidate>,
}

/// `PUT /api/admin/settings`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SettingsInput {
    pub instance_name: String,
    /// The instance to make primary, or null for none.
    #[serde(default)]
    pub primary_instance_id: Option<String>,
    /// New file settings; omit to keep them. Refused while another instance is primary.
    #[serde(default)]
    pub files: Option<FileSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ImmichServerView {
    pub id: String,
    pub name: String,
    pub url: String,
    pub version: Option<String>,
    /// Version is v3.4.1 and the database schema matches the verified migration.
    pub compatible: bool,
    pub compatibility_detail: Option<String>,
    pub database_configured: bool,
    pub direct_writes_enabled: bool,
    #[serde(default)]
    pub external_library_move_mode: ImmichMoveMode,
    pub external_library_api_capable: Option<bool>,
    pub admin_account_id: Option<String>,
    pub maintenance: MaintenanceView,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ImmichServerInput {
    pub name: String,
    pub url: String,
    /// `postgres://...` connection string for direct writes. Stored encrypted; never returned.
    #[serde(default)]
    pub database_url: Option<String>,
    #[serde(default)]
    pub direct_writes_enabled: bool,
    #[serde(default)]
    pub external_library_move_mode: ImmichMoveMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AccountView {
    pub id: String,
    pub server_id: String,
    pub label: String,
    pub email: String,
    pub immich_user_id: String,
    pub is_admin: bool,
    pub status: String,
    pub last_error: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AccountInput {
    pub server_id: String,
    pub label: String,
    pub email: String,
    /// Used once to create a scoped API key; never stored.
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LibraryView {
    pub id: String,
    pub server_id: String,
    pub name: String,
    pub owner_user_id: String,
    pub owner_account_id: Option<String>,
    pub import_paths: Vec<String>,
    pub asset_count: Option<i64>,
    /// An account that owns this library is connected, so changes can be routed to it.
    pub covered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MountView {
    pub path: String,
    pub writable: bool,
    pub mapped_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MappingView {
    pub root_id: String,
    pub name: String,
    pub container_path: String,
    pub server_id: Option<String>,
    pub library_id: Option<String>,
    pub immich_import_path: Option<String>,
    pub opm_prefix: Option<String>,
    pub available: bool,
    pub availability_detail: Option<String>,
    pub watch: WatchStatusView,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MappingInput {
    pub root_id: String,
    pub name: String,
    pub container_path: String,
    #[serde(default)]
    pub server_id: Option<String>,
    #[serde(default)]
    pub library_id: Option<String>,
    #[serde(default)]
    pub immich_import_path: Option<String>,
    #[serde(default)]
    pub opm_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ClientTokenView {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub roots: Vec<String>,
    pub actions: Vec<ClientAction>,
    pub created_at: String,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ClientTokenInput {
    pub name: String,
    pub kind: String,
    pub roots: Vec<String>,
    pub actions: Vec<ClientAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ClientTokenCreated {
    pub token: ClientTokenView,
    /// Shown once.
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PeerView {
    pub id: String,
    pub name: String,
    pub url: String,
    pub instance_id: Option<String>,
    pub status: String,
    pub last_seen_at: Option<String>,
    pub last_error: Option<String>,
    pub events_applied: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PeerInvite {
    /// Paste into the other instance. Contains a one-time pairing secret.
    pub invite: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PeerAcceptInput {
    pub name: String,
    /// Address of the other instance as reachable from here, e.g. `http://192.0.2.2:8080`.
    pub url: String,
    pub invite: String,
    /// Address of this instance as reachable from the other one, sent while pairing.
    pub callback_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ShareView {
    pub id: String,
    /// Shares with the same name on two instances replicate into each other.
    pub name: String,
    pub location: Location,
    pub peer_ids: Vec<String>,
    pub enabled: bool,
    pub last_scan_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ShareInput {
    pub name: String,
    pub location: Location,
    pub peer_ids: Vec<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TransferView {
    pub id: String,
    pub peer_id: String,
    pub peer_name: String,
    pub share_name: String,
    pub location: Location,
    pub status: String,
    pub size: u64,
    pub bytes_done: u64,
    pub attempts: u32,
    pub error: Option<String>,
    pub queued_at: String,
    pub override_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationActivity {
    pub operation_id: String,
    pub title: String,
    pub label: Option<String>,
    pub source: String,
    pub destination: String,
    pub status: OperationStatus,
    pub progress: Progress,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub handled_at: Option<String>,
    pub recovery_resolved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub phase: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationPage {
    pub rows: Vec<OperationActivity>,
    pub total: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct BackgroundActivity {
    pub id: String,
    pub task_key: String,
    pub title: String,
    pub resource: String,
    pub status: BackgroundStatus,
    pub phase: String,
    pub done: u64,
    pub total: Option<u64>,
    pub unit: String,
    pub started_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
    pub next_at: Option<String>,
    pub error: Option<String>,
    pub operation_id: Option<String>,
    pub page: Option<String>,
    /// Number of queued items this row summarizes; bulk work is grouped rather than listed.
    #[serde(default = "one")]
    pub count: u64,
    /// Most frequent reasons among the grouped items, largest first.
    #[serde(default)]
    pub reasons: Vec<ActivityReason>,
    /// A few representative items from the group.
    #[serde(default)]
    pub examples: Vec<ActivityExample>,
}

fn one() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ActivityReason {
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ActivityExample {
    pub resource: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum BackgroundStatus {
    Running,
    Waiting,
    Blocked,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct BackgroundActivityPage {
    // Current work and unresolved issues are never hidden by history pagination.
    pub current: Vec<BackgroundActivity>,
    pub history: Vec<BackgroundActivity>,
    pub total: i64,
    pub observed_at: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionCompleteness {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionState {
    Ready,
    Running,
    Waiting,
    Blocked,
    Failed,
    Stalled,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionLevel {
    None,
    Notice,
    ActionRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionOwner {
    Local,
    Receiver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionCode {
    OperationRecovery,
    OperationFailed,
    MaintenanceBlocked,
    MaintenanceWaiting,
    WatcherPaused,
    LibraryScanFailed,
    PeerOffline,
    PeerEvidenceUnavailable,
    TransferFailed,
    ReceiverTransferFailed,
    TransferScheduled,
    TransferScheduleInvalid,
    TransferBackoff,
    TransferDependency,
    TransferReady,
    TransferRunning,
    TransferEvidenceUnavailable,
    PeerInboxFailed,
    FolderRemovalFailed,
    PeerInventoryFailed,
    RecoveryDecision,
    RecoveryFailed,
    WorkerStopped,
    WorkerStalled,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AttentionItem {
    pub id: String,
    pub code: AttentionCode,
    pub state: AttentionState,
    pub attention: AttentionLevel,
    /// True only when this condition is confirmed to prevent the counted work from advancing.
    pub blocks_work: bool,
    pub owner: AttentionOwner,
    pub instance_id: Option<String>,
    pub instance_name: Option<String>,
    pub peer_id: Option<String>,
    pub share_id: Option<String>,
    pub share_name: Option<String>,
    /// Confirmed affected work in this group; different groups can overlap.
    pub count: u64,
    /// Total work in the underlying scope, when known.
    pub total_count: Option<u64>,
    pub next_at: Option<String>,
    pub recheck_at: Option<String>,
    pub observed_at: String,
    pub title: String,
    pub message: String,
    pub href: String,
    pub operation_id: Option<String>,
    pub acknowledged: Option<bool>,
    pub unresolved: Option<bool>,
    pub completeness: AttentionCompleteness,
    pub examples: Vec<ActivityExample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AttentionSnapshot {
    pub observed_at: String,
    pub completeness: AttentionCompleteness,
    pub warnings: Vec<String>,
    pub items: Vec<AttentionItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationStep {
    pub label: String,
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationTaskProgress {
    pub files: Vec<PlannedFile>,
    pub steps: Vec<OperationStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationDetail {
    pub operation: Operation,
    pub activity: OperationActivity,
    pub tasks: Vec<OperationTaskProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub recovery: Option<RecoveryView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RecoveryKind {
    Operation,
    Transfer,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryTarget {
    pub kind: RecoveryKind,
    pub id: String,
    #[serde(default)]
    pub peer_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RecoveryState {
    Queued,
    Fixing,
    Waiting,
    NeedsDecision,
    Fixed,
    NotRetryable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryView {
    pub target: RecoveryTarget,
    pub title: String,
    pub state: RecoveryState,
    pub code: Option<String>,
    pub detail: Option<String>,
    pub can_fix: bool,
    pub can_dismiss: bool,
    pub replacement_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub local_instance: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub peer_instance: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryBatchInput {
    #[serde(default)]
    pub targets: Vec<RecoveryTarget>,
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub resolution: Option<ConflictResolution>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryBatchResult {
    pub items: Vec<RecoveryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryPage {
    pub rows: Vec<RecoveryView>,
    pub total: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryFileVersion {
    pub path: String,
    pub role: String,
    pub sha256: String,
    pub size: Option<u64>,
    pub modified_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryVersion {
    pub instance: String,
    pub path: String,
    pub state: String,
    pub files: Vec<RecoveryFileVersion>,
    #[serde(default)]
    pub directories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryVersions {
    pub share: String,
    pub revision: String,
    pub receiving: Vec<RecoveryVersion>,
    pub source: RecoveryVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryDecision {
    pub target: RecoveryTarget,
    pub revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoveryDecisionInput {
    pub decisions: Vec<RecoveryDecision>,
    pub resolution: ConflictResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncActivity {
    pub id: String,
    pub outgoing: bool,
    pub peer_id: String,
    pub share_name: String,
    pub relative_path: String,
    pub location: Location,
    pub source: String,
    pub destination: String,
    pub destination_path: String,
    pub status: String,
    pub size: u64,
    pub bytes_done: u64,
    pub error: Option<String>,
    pub queued_at: String,
    pub finished_at: Option<String>,
    pub operation_id: Option<String>,
    #[serde(default)]
    pub readd_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub recovery: Option<RecoveryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncPage {
    pub rows: Vec<SyncActivity>,
    pub total: i64,
    pub progress: SyncProgress,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncProgress {
    pub files_total: u64,
    pub files_completed: u64,
    pub bytes_total: u64,
    pub bytes_done: u64,
    pub bytes_per_second: f64,
    pub eta_seconds: Option<u64>,
    pub queued: u64,
    pub running: u64,
    pub publishing: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncFileInfo {
    pub path: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncFileDetails {
    pub folder: String,
    pub files: Vec<SyncFileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SyncStatus {
    pub allowed_now: bool,
    pub next_allowed_at: Option<String>,
    pub override_active: Option<OverrideView>,
    pub running: Vec<TransferView>,
    pub queued: Vec<TransferView>,
    pub queued_total: i64,
    pub failed_total: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ScheduleView {
    pub timezone: String,
    pub windows: Vec<Window>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OverrideView {
    pub id: String,
    pub share_id: Option<String>,
    pub peer_id: Option<String>,
    pub until: String,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OverrideInput {
    #[serde(default)]
    pub share_id: Option<String>,
    #[serde(default)]
    pub peer_id: Option<String>,
    pub minutes: u32,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConflictSide {
    pub instance: String,
    pub sha256: Option<String>,
    pub size: Option<u64>,
    pub modified_at: Option<String>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConflictView {
    pub id: String,
    pub share_name: String,
    pub location: Location,
    pub kind: String,
    pub detail: String,
    pub local: ConflictSide,
    pub remote: ConflictSide,
    pub peer_id: String,
    pub status: String,
    pub created_at: String,
    /// For a move that conflicted: where the peer moved the file or folder from.
    pub moved_from: Option<Location>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub recovery: Option<RecoveryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "owner", content = "conflict", rename_all = "snake_case")]
#[ts(export)]
pub enum ConflictPageRow {
    Local(Box<ConflictView>),
    Peer(RecoveryView),
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConflictPage {
    pub rows: Vec<ConflictPageRow>,
    pub total: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConflictResolution {
    KeepLocal,
    KeepRemote,
    KeepBoth,
    /// Dismisses the conflict without changing either side.
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ResolveConflictInput {
    pub resolution: ConflictResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Overview {
    pub instance_name: String,
    pub servers: Vec<ImmichServerView>,
    pub roots_total: i64,
    pub roots_unavailable: i64,
    pub operations_running: i64,
    pub operations_attention: i64,
    pub transfers_queued: i64,
    pub conflicts_open: i64,
    pub peers_online: i64,
    pub peers_total: i64,
    pub uncovered_libraries: i64,
    pub watchers: Vec<WatchStatusView>,
    pub scans: Vec<ScanScheduleView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AuditEntry {
    pub id: i64,
    pub at: String,
    pub actor: String,
    pub action: String,
    pub detail: String,
}
