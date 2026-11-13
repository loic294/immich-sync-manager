//! Types used by the administration UI.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use super::client::{ClientAction, Location};
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
    pub seconds_until_reconcile: Option<i64>,
    pub affected_libraries: Vec<String>,
    pub message: Option<String>,
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
    /// Address of the other instance as reachable from here, e.g. `http://100.64.0.2:8080`.
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConflictResolution {
    KeepLocal,
    KeepRemote,
    KeepBoth,
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
