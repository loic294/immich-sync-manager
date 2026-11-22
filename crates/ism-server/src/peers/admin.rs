//! Administration of peers, shares, the transfer schedule and conflicts.

use axum::http::HeaderMap;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ism_core::dto::{
    ConflictResolution, ConflictSide, ConflictView, CreateUpload, Location, OperationAction,
    OperationRequest, OverrideInput, OverrideView, PeerAcceptInput, PeerInvite, PeerView,
    ScheduleView, ShareInput, ShareView, SyncStatus, TransferView,
};
use ism_core::schedule::Schedule;
use ism_core::version::VersionVector;
use reqwest::Method;
use serde::{Deserialize, Serialize};

use super::{EventData, Row, Share, net};
use crate::auth::Principal;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::{AppState, ServerEvent};

const INVITE_PURPOSE: &str = "peer-invite";
const INVITE_HOURS: i64 = 24;
const SCHEDULE_KEY: &str = "sync_schedule";

#[path = "move_decisions.rs"]
mod move_decisions;
#[path = "move_restore.rs"]
mod move_restore;
pub(super) use move_decisions::{
    content_decision, event_decision, finish_step, footprints as move_decision_footprints,
    process as process_move_decisions,
};
pub(super) use move_restore::validate_current_manifest as validate_folder_restore;
pub(crate) use move_restore::{
    VerifiedFolderRestore, verified_folder_restore, verified_restore_upload,
    verify_restored_folder_source,
};
pub(super) use move_restore::{process as process_folder_restorations, restore_missing_folder};
pub(super) use move_restore::{
    record_upload_in as record_restore_upload_in, transfer_parent as restore_transfer_parent,
};

// ---------------------------------------------------------------------------------------------
// Pairing

#[derive(Serialize, Deserialize)]
struct InviteToken {
    instance_id: String,
    name: String,
    invite_id: String,
    secret: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PairRequest {
    pub invite_id: String,
    pub instance_id: String,
    pub name: String,
    pub url: String,
}

fn check_url(url: &str) -> AppResult<String> {
    let url = url.trim().trim_end_matches('/');
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| AppError::bad_request("invalid_url", "enter a full http(s) URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(AppError::bad_request(
            "invalid_url",
            "enter a full http(s) URL",
        ));
    }
    Ok(url.to_owned())
}

fn check_name(name: &str, what: &str) -> AppResult<String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 100 || name.chars().any(char::is_control) {
        return Err(AppError::bad_request(
            "invalid_name",
            format!("{what} must be 1-100 printable characters"),
        ));
    }
    Ok(name.to_owned())
}

pub async fn create_invite(state: &AppState) -> AppResult<PeerInvite> {
    let id = crate::crypto::new_id("inv");
    let secret = crate::crypto::random_secret(32);
    let expires = chrono::Utc::now() + chrono::Duration::hours(INVITE_HOURS);
    sqlx::query("INSERT INTO peer_invites (id, secret_enc, expires_at) VALUES (?, ?, ?)")
        .bind(&id)
        .bind(
            state
                .secrets
                .encrypt(INVITE_PURPOSE, &secret)
                .map_err(AppError::internal)?,
        )
        .bind(db::ts(expires))
        .execute(&state.db)
        .await?;
    let token = InviteToken {
        instance_id: state.instance_id.clone(),
        name: state.instance_name().await,
        invite_id: id,
        secret,
    };
    Ok(PeerInvite {
        invite: URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token)?),
        expires_at: db::ts(expires),
    })
}

async fn insert_peer(
    state: &AppState,
    name: &str,
    url: &str,
    instance_id: &str,
    secret: &str,
) -> AppResult<String> {
    if instance_id == state.instance_id {
        return Err(AppError::bad_request(
            "self_pairing",
            "an instance cannot pair with itself",
        ));
    }
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM peers WHERE instance_id = ?")
        .bind(instance_id)
        .fetch_optional(&state.db)
        .await?;
    if exists.is_some() {
        return Err(AppError::conflict(
            "already_paired",
            "this instance is already paired",
        ));
    }
    let id = crate::crypto::new_id("peer");
    sqlx::query(
        "INSERT INTO peers (id, name, url, instance_id, secret_enc, status, created_at) VALUES (?, ?, ?, ?, ?, 'unknown', ?)",
    )
    .bind(&id)
    .bind(name)
    .bind(url)
    .bind(instance_id)
    .bind(state.secrets.encrypt(net::SECRET_PURPOSE, secret).map_err(AppError::internal)?)
    .bind(db::now())
    .execute(&state.db)
    .await?;
    state.sync_wake.notify_one();
    Ok(id)
}

/// Handles the signed pairing request a peer sends after accepting our invite.
pub async fn pair(
    state: &AppState,
    headers: &HeaderMap,
    path_and_query: &str,
    body: &[u8],
) -> AppResult<net::Hello> {
    let req: PairRequest = serde_json::from_slice(body)
        .map_err(|_| AppError::bad_request("invalid_request", "malformed pairing request"))?;
    let invite: Option<(String, String, Option<String>)> =
        sqlx::query_as("SELECT secret_enc, expires_at, used_at FROM peer_invites WHERE id = ?")
            .bind(&req.invite_id)
            .fetch_optional(&state.db)
            .await?;
    let Some((secret_enc, expires, used)) = invite else {
        return Err(AppError::unauthorized("unknown invite"));
    };
    if used.is_some() || db::parse_ts(&expires).is_none_or(|e| e < chrono::Utc::now()) {
        return Err(AppError::unauthorized(
            "the invite expired or was already used",
        ));
    }
    let secret = state
        .secrets
        .decrypt(INVITE_PURPOSE, &secret_enc)
        .map_err(AppError::internal)?;
    crate::auth::verify_peer_signature(&secret, "POST", path_and_query, headers, body)?;
    let sender = headers
        .get(crate::auth::PEER_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if sender != req.instance_id {
        return Err(AppError::unauthorized("instance id mismatch"));
    }
    let name = check_name(&req.name, "peer name")?;
    let url = check_url(&req.url)?;
    insert_peer(state, &name, &url, &req.instance_id, &secret).await?;
    sqlx::query("UPDATE peer_invites SET used_at = ? WHERE id = ?")
        .bind(db::now())
        .bind(&req.invite_id)
        .execute(&state.db)
        .await?;
    db::audit(
        &state.db,
        &format!("peer:{}", req.instance_id),
        "peer.paired",
        &name,
    )
    .await;
    Ok(net::Hello {
        instance_id: state.instance_id.clone(),
        name: state.instance_name().await,
        shares: Vec::new(),
    })
}

/// Accepts an invite created on another instance.
pub async fn accept(state: &AppState, input: PeerAcceptInput) -> AppResult<PeerView> {
    let raw = URL_SAFE_NO_PAD
        .decode(input.invite.trim())
        .ok()
        .and_then(|b| serde_json::from_slice::<InviteToken>(&b).ok())
        .ok_or_else(|| AppError::bad_request("invalid_invite", "the invite code is not valid"))?;
    let url = check_url(&input.url)?;
    let callback = check_url(&input.callback_url)?;
    let name = if input.name.trim().is_empty() {
        check_name(&raw.name, "peer name")?
    } else {
        check_name(&input.name, "peer name")?
    };
    let body = serde_json::to_vec(&PairRequest {
        invite_id: raw.invite_id.clone(),
        instance_id: state.instance_id.clone(),
        name: state.instance_name().await,
        url: callback,
    })?;
    let resp = net::request(
        state,
        &url,
        &raw.secret,
        Method::POST,
        "/api/peer/v1/pair",
        Some(body),
    )
    .send()
    .await
    .map_err(|e| {
        AppError::conflict_dynamic(
            "peer_unreachable".into(),
            format!("could not reach the peer: {}", net::net_error(&e)),
        )
    })?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::conflict_dynamic(
            "pairing_rejected".into(),
            format!(
                "the peer rejected pairing (HTTP {status}): {}",
                crate::error::redact(&text.chars().take(300).collect::<String>())
            ),
        ));
    }
    let hello: net::Hello = resp
        .json()
        .await
        .map_err(|e| AppError::internal(net::net_error(&e)))?;
    if hello.instance_id != raw.instance_id {
        return Err(AppError::conflict(
            "pairing_rejected",
            "the peer answered with another instance id",
        ));
    }
    let id = insert_peer(state, &name, &url, &raw.instance_id, &raw.secret).await?;
    db::audit(&state.db, "admin", "peer.paired", &name).await;
    peer_view(state, &id).await
}

type PeerRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    i64,
);

fn to_peer_view(r: PeerRow) -> PeerView {
    PeerView {
        id: r.0,
        name: r.1,
        url: r.2,
        instance_id: r.3,
        status: r.4,
        last_seen_at: r.5,
        last_error: r.6,
        events_applied: r.7,
    }
}

const PEER_COLS: &str =
    "id, name, url, instance_id, status, last_seen_at, last_error, events_applied";

pub async fn list_peers(state: &AppState) -> AppResult<Vec<PeerView>> {
    let rows: Vec<PeerRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PEER_COLS} FROM peers ORDER BY name"
    )))
    .fetch_all(&state.db)
    .await?;
    Ok(rows.into_iter().map(to_peer_view).collect())
}

pub async fn peer_view(state: &AppState, id: &str) -> AppResult<PeerView> {
    let r: Option<PeerRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PEER_COLS} FROM peers WHERE id = ?"
    )))
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    r.map(to_peer_view)
        .ok_or_else(|| AppError::not_found("peer not found"))
}

pub async fn delete_peer(state: &AppState, id: &str) -> AppResult<()> {
    let _guard = state.replication_operation_lock.lock().await;
    let peer = peer_view(state, id).await?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    discard_finished_pending(&mut tx, "peer_id", id).await?;
    sqlx::query("DELETE FROM peers WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM conflicts WHERE peer_id = ? AND status = 'open'")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM overrides WHERE peer_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let shares: Vec<(String, String)> = sqlx::query_as("SELECT id, peer_ids_json FROM shares")
        .fetch_all(&mut *tx)
        .await?;
    for (share_id, json) in shares {
        let ids: Vec<String> = db::json_vec(&json)
            .into_iter()
            .filter(|p| p != id)
            .collect();
        sqlx::query("UPDATE shares SET peer_ids_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&ids)?)
            .bind(share_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    db::audit(&state.db, "admin", "peer.removed", &peer.name).await;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

pub async fn retry_peer(state: &AppState, id: &str) -> AppResult<()> {
    let _guard = state.replication_operation_lock.lock().await;
    peer_view(state, id).await?;
    let pending: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pending_peer_events WHERE peer_id = ?1)
         OR EXISTS(SELECT 1 FROM pending_peer_ops WHERE peer_id = ?1)",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await?;
    if pending != 0 {
        return Err(AppError::conflict(
            "peer_operation_pending",
            "re-add the failed operation from the sync queue before retrying this peer",
        ));
    }
    state.sync_wake.notify_waiters();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Shares

pub async fn list_shares(state: &AppState) -> AppResult<Vec<ShareView>> {
    let scans: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT id, last_scan_at FROM shares")
            .fetch_all(&state.db)
            .await?;
    Ok(super::shares(state)
        .await?
        .into_iter()
        .map(|s| ShareView {
            last_scan_at: scans
                .iter()
                .find(|(id, _)| *id == s.id)
                .and_then(|(_, at)| at.clone()),
            location: Location {
                root_id: s.root_id,
                path: s.path.as_str().to_owned(),
            },
            id: s.id,
            name: s.name,
            peer_ids: s.peer_ids,
            enabled: s.enabled,
        })
        .collect())
}

async fn validate_share(
    state: &AppState,
    id: Option<&str>,
    input: &ShareInput,
) -> AppResult<(String, String)> {
    let name = input.name.trim();
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ' '))
    {
        return Err(AppError::bad_request(
            "invalid_name",
            "share names use 1-64 letters, digits, spaces, '-', '_' or '.'",
        ));
    }
    let root = files::root(&state.db, &input.location.root_id).await?;
    let path = files::parse_rel(&input.location.path)?;
    if root.available() && !root.abs(&path)?.is_dir() {
        return Err(AppError::bad_request(
            "invalid_location",
            "the shared folder does not exist",
        ));
    }
    for peer in &input.peer_ids {
        peer_view(state, peer).await?;
    }
    for other in super::shares(state).await? {
        if Some(other.id.as_str()) == id {
            continue;
        }
        if other.name == name {
            return Err(AppError::conflict(
                "name_taken",
                "another share already uses this name",
            ));
        }
        if other.root_id == root.mapping.root_id
            && (path.starts_with(&other.path) || other.path.starts_with(&path))
        {
            return Err(AppError::conflict(
                "share_overlap",
                format!("this folder overlaps the share '{}'", other.name),
            ));
        }
    }
    Ok((name.to_owned(), path.as_str().to_owned()))
}

pub async fn create_share(state: &AppState, input: ShareInput) -> AppResult<ShareView> {
    let _guard = state.replication_operation_lock.lock().await;
    let (name, path) = validate_share(state, None, &input).await?;
    let id = crate::crypto::new_id("share");
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("INSERT INTO shares (id, name, root_id, path, peer_ids_json, enabled) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&id)
        .bind(&name)
        .bind(&input.location.root_id)
        .bind(&path)
        .bind(serde_json::to_string(&input.peer_ids)?)
        .bind(input.enabled as i64)
        .execute(&mut *tx)
        .await?;
    if input.enabled {
        super::request_backfill_in(&mut tx, &state.instance_id, &id, &name, &input.peer_ids)
            .await?;
    }
    tx.commit().await?;
    db::audit(&state.db, "admin", "share.created", &name).await;
    state.sync_wake.notify_one();
    share_view(state, &id).await
}

async fn share_view(state: &AppState, id: &str) -> AppResult<ShareView> {
    list_shares(state)
        .await?
        .into_iter()
        .find(|s| s.id == id)
        .ok_or_else(|| AppError::not_found("share not found"))
}

/// Updates peers and the enabled flag. The name and folder identify the share on every
/// instance, so they cannot change once created.
pub async fn update_share(state: &AppState, id: &str, input: ShareInput) -> AppResult<ShareView> {
    let _guard = state.replication_operation_lock.lock().await;
    let pending: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_peer_events WHERE share_id = ?)")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    if pending {
        return Err(AppError::conflict(
            "peer_operation_pending",
            "wait for the pending peer operation before changing this share",
        ));
    }
    let current = share_view(state, id).await?;
    if current.name != input.name.trim() || current.location != input.location {
        return Err(AppError::bad_request(
            "share_locked",
            "a share's name and folder cannot change; remove it and create a new one",
        ));
    }
    validate_share(state, Some(id), &input).await?;
    // Peers for which the share becomes active catch up with what they missed.
    let activated: Vec<String> = if input.enabled {
        input
            .peer_ids
            .iter()
            .filter(|p| !current.enabled || !current.peer_ids.contains(p))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("UPDATE shares SET peer_ids_json = ?, enabled = ? WHERE id = ?")
        .bind(serde_json::to_string(&input.peer_ids)?)
        .bind(input.enabled as i64)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    super::request_backfill_in(&mut tx, &state.instance_id, id, &current.name, &activated).await?;
    tx.commit().await?;
    db::audit(&state.db, "admin", "share.updated", &current.name).await;
    state.sync_wake.notify_one();
    share_view(state, id).await
}

pub async fn delete_share(state: &AppState, id: &str) -> AppResult<()> {
    let _guard = state.replication_operation_lock.lock().await;
    let share = share_view(state, id).await?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    discard_finished_pending(&mut tx, "share_id", id).await?;
    sqlx::query("DELETE FROM shares WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM overrides WHERE share_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    db::audit(&state.db, "admin", "share.removed", &share.name).await;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn discard_finished_pending(
    conn: &mut sqlx::SqliteConnection,
    scope: &str,
    id: &str,
) -> AppResult<()> {
    let sql = match scope {
        "peer_id" => {
            "SELECT p.operation_id, o.status, o.recovery_resolved FROM pending_peer_events p JOIN operations o ON o.id = p.operation_id WHERE p.peer_id = ?"
        }
        "share_id" => {
            "SELECT p.operation_id, o.status, o.recovery_resolved FROM pending_peer_events p JOIN operations o ON o.id = p.operation_id WHERE p.share_id = ?"
        }
        _ => return Err(AppError::internal("invalid pending event scope")),
    };
    let rows: Vec<(String, String, i64)> =
        sqlx::query_as(sql).bind(id).fetch_all(&mut *conn).await?;
    for (operation_id, status, resolved) in rows {
        if !(matches!(status.as_str(), "failed" | "cancelled")
            || status == "recovery_required" && resolved != 0)
        {
            return Err(AppError::conflict(
                "peer_operation_pending",
                "finish or cancel the pending peer operation before removing its share or peer",
            ));
        }
        sqlx::query("DELETE FROM pending_peer_events WHERE operation_id = ?")
            .bind(operation_id)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Schedule, overrides and transfers

pub async fn schedule(state: &AppState) -> AppResult<ScheduleView> {
    let Some(raw) = db::get_setting(&state.db, SCHEDULE_KEY).await? else {
        return Ok(ScheduleView {
            timezone: "UTC".into(),
            windows: Vec::new(),
        });
    };
    let invalid = || {
        AppError::conflict(
            "invalid_schedule",
            "the stored sync schedule is invalid; save a valid schedule before transfers can resume",
        )
    };
    let view: ScheduleView = serde_json::from_str(&raw).map_err(|_| invalid())?;
    Schedule::new(&view.timezone, &view.windows).map_err(|_| invalid())?;
    Ok(view)
}

pub async fn set_schedule(state: &AppState, view: ScheduleView) -> AppResult<ScheduleView> {
    Schedule::new(&view.timezone, &view.windows)
        .map_err(|e| AppError::bad_request("invalid_schedule", e.to_string()))?;
    db::set_setting(&state.db, SCHEDULE_KEY, &serde_json::to_string(&view)?).await?;
    db::audit(&state.db, "admin", "sync.schedule", &view.timezone).await;
    state.sync_wake.notify_one();
    state.emit(ServerEvent::SyncChanged);
    Ok(view)
}

type OverrideRow = (
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);

fn to_override(r: OverrideRow) -> OverrideView {
    OverrideView {
        id: r.0,
        share_id: r.1,
        peer_id: r.2,
        until: r.3,
        reason: r.4,
        created_at: r.5,
    }
}

pub async fn active_overrides(state: &AppState) -> AppResult<Vec<OverrideView>> {
    let rows: Vec<OverrideRow> = sqlx::query_as(
        "SELECT id, share_id, peer_id, until, reason, created_at FROM overrides
         WHERE revoked_at IS NULL AND until > ? ORDER BY until DESC",
    )
    .bind(db::now())
    .fetch_all(&state.db)
    .await?;
    Ok(rows.into_iter().map(to_override).collect())
}

pub async fn create_override(state: &AppState, input: OverrideInput) -> AppResult<OverrideView> {
    if input.minutes == 0 || input.minutes > 7 * 24 * 60 {
        return Err(AppError::bad_request(
            "invalid_duration",
            "an override lasts between 1 minute and 7 days",
        ));
    }
    if let Some(share) = &input.share_id {
        share_view(state, share).await?;
    }
    if let Some(peer) = &input.peer_id {
        peer_view(state, peer).await?;
    }
    let reason = input.reason.trim();
    if reason.chars().count() > 200 {
        return Err(AppError::bad_request(
            "invalid_reason",
            "keep the reason under 200 characters",
        ));
    }
    let id = crate::crypto::new_id("ovr");
    let until = chrono::Utc::now() + chrono::Duration::minutes(i64::from(input.minutes));
    sqlx::query("INSERT INTO overrides (id, share_id, peer_id, until, reason, created_at) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&id)
        .bind(&input.share_id)
        .bind(&input.peer_id)
        .bind(db::ts(until))
        .bind(reason)
        .bind(db::now())
        .execute(&state.db)
        .await?;
    db::audit(
        &state.db,
        "admin",
        "sync.override",
        &format!("{} minutes", input.minutes),
    )
    .await;
    state.sync_wake.notify_one();
    state.emit(ServerEvent::SyncChanged);
    active_overrides(state)
        .await?
        .into_iter()
        .find(|o| o.id == id)
        .ok_or_else(|| AppError::internal("override vanished"))
}

pub async fn revoke_override(state: &AppState, id: &str) -> AppResult<()> {
    let done =
        sqlx::query("UPDATE overrides SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL")
            .bind(db::now())
            .bind(id)
            .execute(&state.db)
            .await?;
    if done.rows_affected() == 0 {
        return Err(AppError::not_found("override not found"));
    }
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

async fn transfers(state: &AppState, statuses: &str, limit: i64) -> AppResult<Vec<TransferView>> {
    type R = (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        i64,
        i64,
        Option<String>,
        String,
        Option<String>,
    );
    let rows: Vec<R> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT t.id, t.peer_id, p.name, s.name, s.root_id, s.path, t.rel_path, t.size, t.bytes_done, t.attempts, t.error,
                t.queued_at, t.override_id
         FROM transfers t JOIN peers p ON p.id = t.peer_id JOIN shares s ON s.id = t.share_id
         WHERE t.status IN ({statuses}) ORDER BY t.queued_at LIMIT ?"
    )))
    .bind(limit)
    .fetch_all(&state.db)
    .await?;
    let status: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, status FROM transfers WHERE status IN ({statuses})"
    )))
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                peer_id,
                peer_name,
                share_name,
                root_id,
                share_path,
                rel,
                size,
                done,
                attempts,
                error,
                queued_at,
                ov,
            )| {
                let path = if share_path.is_empty() {
                    rel
                } else {
                    format!("{share_path}/{rel}")
                };
                TransferView {
                    status: status
                        .iter()
                        .find(|(i, _)| *i == id)
                        .map(|(_, s)| s.clone())
                        .unwrap_or_default(),
                    id,
                    peer_id,
                    peer_name,
                    share_name,
                    location: Location { root_id, path },
                    size: size.max(0) as u64,
                    bytes_done: done.max(0) as u64,
                    attempts: attempts.max(0) as u32,
                    error,
                    queued_at,
                    override_id: ov,
                }
            },
        )
        .collect())
}

pub async fn sync_status(state: &AppState) -> AppResult<SyncStatus> {
    sync_status_with_rows(state, true).await
}

pub async fn sync_status_with_rows(state: &AppState, include_rows: bool) -> AppResult<SyncStatus> {
    let view = schedule(state).await?;
    let schedule = Schedule::new(&view.timezone, &view.windows)
        .map_err(|e| AppError::internal(e.to_string()))?;
    let now = chrono::Utc::now();
    let allowed_now = schedule.is_allowed(now);
    let count = |sql: &'static str| {
        let pool = state.db.clone();
        async move { sqlx::query_scalar::<_, i64>(sql).fetch_one(&pool).await }
    };
    Ok(SyncStatus {
        allowed_now,
        next_allowed_at: if allowed_now {
            None
        } else {
            schedule.next_allowed(now).map(db::ts)
        },
        override_active: active_overrides(state).await?.into_iter().next(),
        running: if include_rows {
            transfers(state, "'running'", 50).await?
        } else {
            Vec::new()
        },
        queued: if include_rows {
            transfers(state, "'queued', 'failed', 'cancelled'", 500).await?
        } else {
            Vec::new()
        },
        queued_total: count("SELECT COUNT(*) FROM transfers WHERE status = 'queued'").await?,
        failed_total: count("SELECT COUNT(*) FROM transfers WHERE status = 'failed'").await?,
    })
}

pub async fn retry_transfer(state: &AppState, id: &str) -> AppResult<()> {
    let done = sqlx::query(
        "UPDATE transfers SET status = 'queued', attempts = 0, error = NULL, updated_at = ?
         WHERE id = ? AND status IN ('failed', 'cancelled')",
    )
    .bind(db::now())
    .bind(id)
    .execute(&state.db)
    .await?;
    if done.rows_affected() == 0 {
        return Err(AppError::not_found(
            "no failed or cancelled transfer with this id",
        ));
    }
    state.sync_wake.notify_one();
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

pub async fn cancel_transfer(state: &AppState, id: &str) -> AppResult<()> {
    let failed: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfers WHERE id=? AND status='failed')")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    if failed
        && !super::recovery::get(state, ism_core::dto::RecoveryKind::Transfer, id)
            .await?
            .is_some_and(|r| r.can_dismiss)
    {
        return Err(AppError::conflict(
            "recovery_not_exhausted",
            "recover this issue before dismissing it",
        ));
    }
    let done = sqlx::query("UPDATE transfers SET status = 'cancelled', updated_at = ? WHERE id = ? AND status IN ('queued', 'running', 'failed')")
        .bind(db::now())
        .bind(id)
        .execute(&state.db)
        .await?;
    if done.rows_affected() == 0 {
        return Err(AppError::not_found("no active transfer with this id"));
    }
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

/// Requeues an item while retaining its original recovery until the replacement is verified.
pub async fn readd_sync_operation(state: &AppState, operation_id: &str, idx: i64) -> AppResult<()> {
    super::recovery::discover(state).await?;
    let _operation_guard = state.replication_operation_lock.lock().await;
    let operation: Option<(String, String, i64)> =
        sqlx::query_as("SELECT status, principal, recovery_resolved FROM operations WHERE id = ?")
            .bind(operation_id)
            .fetch_optional(&state.db)
            .await?;
    let Some((status, principal_key, recovery_resolved)) = operation else {
        return Err(AppError::not_found("sync operation not found"));
    };
    if !(matches!(status.as_str(), "failed" | "partial" | "cancelled")
        || status == "recovery_required" && recovery_resolved != 0)
    {
        return Err(AppError::conflict(
            "sync_operation_not_retryable",
            "only failed, cancelled or resolved peer operations can be re-added",
        ));
    }
    let Some(peer_id) = principal_key.strip_prefix("peer:") else {
        return Err(AppError::conflict(
            "sync_operation_not_peer",
            "this operation did not come from a peer",
        ));
    };
    let action_json: Option<String> = sqlx::query_scalar(
        "SELECT action_json FROM operation_items
         WHERE operation_id = ? AND idx = ? AND status IN ('failed','skipped','recovery_required')",
    )
    .bind(operation_id)
    .bind(idx)
    .fetch_optional(&state.db)
    .await?;
    let Some(action_json) = action_json else {
        return Err(AppError::not_found(
            "no failed sync operation item with this index",
        ));
    };
    let action = crate::ops::exec::parse_action(&action_json)?;
    sqlx::query("INSERT OR IGNORE INTO sync_recovery (id,kind,target_id,created_at,updated_at) VALUES (?,'operation',?,?,?)")
        .bind(format!("operation:{operation_id}")).bind(operation_id).bind(db::now()).bind(db::now())
        .execute(&state.db).await?;

    type PendingOperation = (
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    );
    let pending: Option<PendingOperation> = sqlx::query_as(
        "SELECT share_id, kind, from_path, to_path, vv_json, conflict_id
         FROM pending_peer_ops WHERE operation_id = ? AND idx = ? AND peer_id = ?",
    )
    .bind(operation_id)
    .bind(idx)
    .bind(peer_id)
    .fetch_optional(&state.db)
    .await?;
    if let Some((share_id, kind, from_path, to_path, vv_json, conflict_id)) = pending {
        if kind == "publish" {
            let vv: VersionVector = serde_json::from_str(&vv_json)?;
            let upload_request: Option<String> =
                sqlx::query_scalar("SELECT request_json FROM uploads WHERE operation_id = ?")
                    .bind(operation_id)
                    .fetch_optional(&state.db)
                    .await?;
            let upload_request = upload_request.ok_or_else(|| {
                AppError::conflict(
                    "sync_publication_missing",
                    "the failed publication metadata is missing",
                )
            })?;
            let upload: CreateUpload = serde_json::from_str(&upload_request)?;
            let file = upload.files.first().ok_or_else(|| {
                AppError::conflict(
                    "sync_publication_missing",
                    "the failed publication has no file metadata",
                )
            })?;
            let share = super::shares(state)
                .await?
                .into_iter()
                .find(|share| share.id == share_id)
                .ok_or_else(|| {
                    AppError::conflict(
                        "peer_share_changed",
                        "the peer share changed while this operation was pending",
                    )
                })?;
            let rel = to_path.as_deref().ok_or_else(|| {
                AppError::conflict(
                    "sync_publication_missing",
                    "the failed publication has no destination",
                )
            })?;
            let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
            super::queue_transfer_in(&mut tx, peer_id, &share, rel, &file.sha256, file.size, &vv)
                .await?;
            sqlx::query(
                "INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id)
                 SELECT ?,'transfer',id FROM transfers WHERE share_id=? AND rel_path=? AND peer_id=? AND status IN ('queued','running')",
            ).bind(format!("operation:{operation_id}")).bind(&share.id).bind(rel).bind(peer_id)
                .execute(&mut *tx).await?;
            sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id = ? AND idx = ?")
                .bind(operation_id)
                .bind(idx)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            state.sync_wake.notify_one();
        } else {
            let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
            let request = retry_request(action);
            let principal = Principal::Peer {
                peer_id: peer_id.to_owned(),
            };
            let accepted = crate::ops::insert_in(&mut tx, &principal, &request, None).await?;
            sqlx::query("INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id) VALUES (?,'operation',?)")
                .bind(format!("operation:{operation_id}")).bind(&accepted.operation_id).execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE sync_recovery SET replacement_operation_id=?
                 WHERE kind='operation' AND target_id=?",
            )
            .bind(&accepted.operation_id)
            .bind(operation_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id, conflict_id)
                 VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&accepted.operation_id)
            .bind(&share_id)
            .bind(&kind)
            .bind(&from_path)
            .bind(&to_path)
            .bind(&vv_json)
            .bind(peer_id)
            .bind(&conflict_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM pending_peer_ops WHERE operation_id = ? AND idx = ?")
                .bind(operation_id)
                .bind(idx)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            crate::ops::announce_queued(state, &accepted);
        }
        state.emit(ServerEvent::SyncChanged);
        return Ok(());
    }

    type PendingEvent = (String, String, i64, String);
    let pending_event: Option<PendingEvent> = sqlx::query_as(
        "SELECT share_id, event_json, expected_cursor, peer_id
         FROM pending_peer_events WHERE operation_id = ? AND peer_id = ?",
    )
    .bind(operation_id)
    .bind(peer_id)
    .fetch_optional(&state.db)
    .await?;
    let Some((share_id, event_json, expected_cursor, peer_id)) = pending_event else {
        return Err(AppError::not_found(
            "the failed item is no longer pending in the sync queue",
        ));
    };
    if status == "recovery_required" {
        super::finish_pending_event(state, operation_id).await?;
        state.emit(ServerEvent::OperationUpdated {
            operation_id: operation_id.to_owned(),
        });
        state.emit(ServerEvent::SyncChanged);
        return Ok(());
    }
    let event: super::PeerEvent = serde_json::from_str(&event_json)?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let principal = Principal::Peer {
        peer_id: peer_id.clone(),
    };
    let request = retry_request(action);
    let accepted = crate::ops::insert_in(&mut tx, &principal, &request, None).await?;
    sqlx::query("INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id) VALUES (?,'operation',?)")
        .bind(format!("operation:{operation_id}")).bind(&accepted.operation_id).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE sync_recovery SET replacement_operation_id=?
         WHERE kind='operation' AND target_id=?",
    )
    .bind(&accepted.operation_id)
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM pending_peer_events WHERE operation_id = ?")
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO pending_peer_events (operation_id, peer_id, share_id, origin, origin_seq, expected_cursor, event_json)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&accepted.operation_id)
    .bind(&peer_id)
    .bind(&share_id)
    .bind(&event.origin)
    .bind(event.origin_seq)
    .bind(expected_cursor)
    .bind(&event_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::ops::announce_queued(state, &accepted);
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

fn retry_request(action: OperationAction) -> OperationRequest {
    OperationRequest {
        idempotency_key: crate::crypto::new_id("sync-readd"),
        label: Some("Re-added peer change".into()),
        actions: vec![action],
        allow_owner_transfer: false,
        allow_partial: false,
    }
}

// ---------------------------------------------------------------------------------------------
// Conflicts

struct Conflict {
    id: String,
    share: Share,
    rel: String,
    kind: String,
    remote: ConflictSide,
    remote_vv: VersionVector,
    peer_id: String,
    status: String,
    remote_from: Option<String>,
}

async fn load_conflict(state: &AppState, id: &str) -> AppResult<Conflict> {
    type R = (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
    );
    let r: Option<R> = sqlx::query_as(
        "SELECT share_id, rel_path, kind, remote_json, remote_vv_json, peer_id, status, remote_from FROM conflicts WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    let (share_id, rel, kind, remote, vv, peer_id, status, remote_from) =
        r.ok_or_else(|| AppError::not_found("conflict not found"))?;
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|s| s.id == share_id)
        .ok_or_else(|| AppError::not_found("share not found"))?;
    Ok(Conflict {
        id: id.to_owned(),
        share,
        rel,
        kind,
        remote: serde_json::from_str(&remote)?,
        remote_vv: serde_json::from_str(&vv)?,
        peer_id,
        status,
        remote_from,
    })
}

pub(super) async fn version_context(
    state: &AppState,
    id: &str,
) -> AppResult<super::versions::Context> {
    let c = load_conflict(state, id).await?;
    if c.status != "open" {
        return Err(AppError::conflict(
            "not_open",
            "the version decision is already executing",
        ));
    }
    if c.kind == "folder_restore" {
        return move_restore::version_context(state, id).await;
    }
    let mut local_paths = vec![c.rel.clone()];
    if let Some(from) = &c.remote_from
        && *from != c.rel
    {
        local_paths.insert(0, from.clone());
    }
    let (remote_path, remote_state, remote_files, remote_directories) =
        if matches!(c.kind.as_str(), "move" | "folder_move") {
            let evidence = move_decisions::evidence(state, &c).await?;
            (
                evidence.location,
                if c.kind == "folder_move" {
                    "folder"
                } else {
                    "file"
                }
                .into(),
                evidence
                    .files
                    .into_iter()
                    .map(|file| (file.path, file.sha256, file.size, file.modified_at))
                    .collect(),
                evidence.directories,
            )
        } else {
            let peer = super::net::peer(state, &c.peer_id).await?;
            let evidence =
                super::recovery::remote_evidence(state, &peer, &c.share, &c.rel, &c.remote_vv)
                    .await?;
            let files = evidence
                .sha256
                .zip(evidence.size)
                .map(|(hash, size)| vec![(evidence.path.clone(), hash, size, evidence.modified_at)])
                .unwrap_or_default();
            let status = if evidence.deleted {
                "deleted"
            } else if files.is_empty() {
                "absent"
            } else {
                "file"
            };
            (evidence.path, status.into(), files, Vec::new())
        };
    if matches!(c.kind.as_str(), "move" | "folder_move") && !local_paths.contains(&remote_path) {
        local_paths.push(remote_path.clone());
    }
    Ok(super::versions::Context {
        share: c.share,
        peer_id: c.peer_id,
        local_paths,
        remote_path,
        remote_state,
        remote_files,
        remote_directories,
    })
}

pub async fn list_conflicts(
    state: &AppState,
    include_resolved: bool,
) -> AppResult<Vec<ConflictView>> {
    type R = (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
    );
    let rows: Vec<R> = sqlx::query_as(
        "SELECT id, share_id, rel_path, kind, detail, local_json, remote_json, peer_id, status, created_at, remote_from FROM conflicts
         WHERE status != 'resolved' OR ? ORDER BY created_at DESC LIMIT 500",
    )
    .bind(include_resolved)
    .fetch_all(&state.db)
    .await?;
    let shares = super::shares(state).await?;
    rows.into_iter()
        .map(
            |(
                id,
                share_id,
                rel,
                kind,
                detail,
                local,
                remote,
                peer_id,
                status,
                created_at,
                from,
            )| {
                let share = shares
                    .iter()
                    .find(|s| s.id == share_id)
                    .ok_or_else(|| AppError::internal("conflict references a missing share"))?;
                Ok(ConflictView {
                    id,
                    share_name: share.name.clone(),
                    location: super::display(share, &rel),
                    kind,
                    detail,
                    local: serde_json::from_str(&local)?,
                    remote: serde_json::from_str(&remote)?,
                    peer_id,
                    status,
                    created_at,
                    moved_from: from.map(|f| super::display(share, &f)),
                })
            },
        )
        .collect()
}

async fn mark_resolved(state: &AppState, id: &str, resolution: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE conflicts SET status = 'resolved', resolution = ?, resolved_at = ? WHERE id = ?",
    )
    .bind(resolution)
    .bind(db::now())
    .bind(id)
    .execute(&state.db)
    .await?;
    state.emit(ServerEvent::ConflictsChanged);
    Ok(())
}

/// Submits a managed operation whose replica bookkeeping runs when it finishes, and marks the
/// conflict as resolving in the same transaction. The operation's outcome settles the conflict:
/// resolved when it succeeds, open again when it fails.
#[allow(clippy::too_many_arguments)]
async fn submit_pending(
    state: &AppState,
    actor: &str,
    c: &Conflict,
    resolution: &str,
    action: OperationAction,
    kind: &str,
    from: Option<&str>,
    to: &str,
    vv: &VersionVector,
) -> AppResult<()> {
    let req = OperationRequest {
        idempotency_key: crate::crypto::new_id(&format!("conflict-{kind}")),
        label: Some(format!("Resolve conflict on {}", c.rel)),
        actions: vec![action],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let principal = Principal::Admin {
        username: actor.to_owned(),
    };
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let marked = sqlx::query(
        "UPDATE conflicts SET status = 'resolving', resolution = COALESCE(resolution, ?) WHERE id = ? AND status IN ('open', 'resolving')",
    )
    .bind(resolution)
    .bind(&c.id)
    .execute(&mut *tx)
    .await?;
    if marked.rows_affected() != 1 {
        return Err(AppError::conflict(
            "not_open",
            "this conflict is already resolved",
        ));
    }
    let accepted = crate::ops::insert_in(&mut tx, &principal, &req, None).await?;
    sqlx::query(
        "INSERT INTO pending_peer_ops (operation_id, idx, share_id, kind, from_path, to_path, vv_json, peer_id, conflict_id)
         VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&accepted.operation_id)
    .bind(&c.share.id)
    .bind(kind)
    .bind(from)
    .bind(to)
    .bind(serde_json::to_string(vv)?)
    .bind(&c.peer_id)
    .bind(&c.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::ops::announce_queued(state, &accepted);
    state.emit(ServerEvent::ConflictsChanged);
    Ok(())
}

fn conflict_name(rel: &str, date: &str, n: u32) -> String {
    let (dir, name) = rel.rsplit_once('/').map_or(("", rel), |(d, n)| (d, n));
    let (stem, ext) = match name.rfind('.') {
        Some(i) if i > 0 => (&name[..i], &name[i..]),
        _ => (name, ""),
    };
    let suffix = if n == 1 {
        format!(" (conflict {date})")
    } else {
        format!(" (conflict {date} {n})")
    };
    let file = format!("{stem}{suffix}{ext}");
    if dir.is_empty() {
        file
    } else {
        format!("{dir}/{file}")
    }
}

fn merged(local: Option<&Row>, remote: &VersionVector) -> VersionVector {
    let mut vv = local.map(|r| r.vv.clone()).unwrap_or_default();
    vv.merge(remote);
    vv
}

/// A later, causally conclusive file announcement can carry the other peer's explicit choice.
/// It cannot settle concurrent histories or overwrite an unregistered local edit.
pub(super) async fn accept_dominating_decision(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    event: &super::PeerEvent,
) -> AppResult<()> {
    if event.kind != "file_changed" {
        return Ok(());
    }
    let conflicts: Vec<(String, String)> = sqlx::query_as(
        "SELECT id,remote_vv_json FROM conflicts WHERE share_id=? AND rel_path=? AND status='open'
         AND kind NOT IN ('move','folder_move')",
    )
    .bind(&share.id)
    .bind(&event.data.path)
    .fetch_all(&state.db)
    .await?;
    if conflicts.is_empty() {
        return Ok(());
    }
    super::inbox::require_operations_stable(state, share, &[&event.data.path]).await?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&share.root_rel(&event.data.path)?)?;
    let present = match abs.symlink_metadata() {
        Ok(meta) if meta.is_file() => true,
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let local = super::register_local(state, share, &root, &event.data.path).await?;
    if !present && local.as_ref().is_some_and(|row| !row.deleted) {
        return Ok(());
    }
    for (id, raw) in conflicts {
        let remote: VersionVector = serde_json::from_str(&raw)?;
        if event.data.vv.compare(&remote) != ism_core::version::Causality::Dominates
            || local.as_ref().is_some_and(|row| {
                event.data.vv.compare(&row.vv) != ism_core::version::Causality::Dominates
            })
        {
            continue;
        }
        let Some(hash) = &event.data.sha256 else {
            continue;
        };
        let Some(size) = event.data.size else {
            continue;
        };
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        let changed = sqlx::query(
            "UPDATE conflicts SET status='resolving',resolution='keep_remote',
             remote_vv_json=?,remote_json=?,peer_id=? WHERE id=? AND status='open' AND remote_vv_json=?",
        ).bind(serde_json::to_string(&event.data.vv)?)
            .bind(serde_json::to_string(&super::remote_side(&peer.name, &event.data, false))?)
            .bind(&peer.id).bind(&id).bind(&raw).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            continue;
        }
        let receipts: Vec<String> = sqlx::query_scalar(
            "SELECT event_json FROM peer_inbox WHERE peer_id=? AND share_name=? AND status!='done'",
        )
        .bind(&peer.id)
        .bind(&share.name)
        .fetch_all(&mut *tx)
        .await?;
        for raw in receipts {
            let old: super::PeerEvent = serde_json::from_str(&raw)?;
            if old.kind == "file_changed"
                && old.data.path == event.data.path
                && old.data.vv.compare(&event.data.vv) == ism_core::version::Causality::DominatedBy
            {
                super::inbox::done_in(&mut tx, &peer.id, &old, false).await?;
            }
        }
        if let Some(local) = local.as_ref().filter(|row| {
            !row.deleted && row.sha256.as_ref() == Some(hash) && row.size as u64 == size
        }) {
            let mut row = local.clone();
            row.vv.merge(&event.data.vv);
            super::put_row_in(&mut tx, &share.id, &event.data.path, &row).await?;
            resolved_in(&mut tx, &id, "keep_remote").await?;
        } else {
            super::queue_transfer_in(
                &mut tx,
                &peer.id,
                share,
                &event.data.path,
                hash,
                size,
                &event.data.vv,
            )
            .await?;
        }
        tx.commit().await?;
        state.emit(ServerEvent::ConflictsChanged);
        state.sync_wake.notify_one();
    }
    Ok(())
}

pub async fn resolve(
    state: &AppState,
    actor: &str,
    id: &str,
    resolution: ConflictResolution,
) -> AppResult<()> {
    resolve_reviewed(state, actor, id, resolution, None).await
}

pub(super) async fn resolve_reviewed(
    state: &AppState,
    actor: &str,
    id: &str,
    resolution: ConflictResolution,
    revision: Option<&str>,
) -> AppResult<()> {
    if resolution == ConflictResolution::Skip {
        return Err(AppError::conflict(
            "recovery_not_exhausted",
            "choose a version before dismissing this conflict",
        ));
    }
    let initial = load_conflict(state, id).await?;
    if initial.kind == "folder_restore" {
        return move_restore::approve(state, actor, &initial, resolution, revision).await;
    }
    let evidence = if matches!(initial.kind.as_str(), "move" | "folder_move") {
        Some(move_decisions::evidence(state, &initial).await?)
    } else {
        None
    };
    let current_remote = if evidence.is_none() {
        let peer = super::net::peer(state, &initial.peer_id).await?;
        Some(
            super::recovery::remote_evidence(
                state,
                &peer,
                &initial.share,
                &initial.rel,
                &initial.remote_vv,
            )
            .await?,
        )
    } else {
        None
    };
    let _operation_guard = state.replication_operation_lock.lock().await;
    let mut c = load_conflict(state, id).await?;
    if let Some(revision) = revision {
        let moved_files: Vec<_> = evidence
            .as_ref()
            .map(|value| {
                value
                    .files
                    .iter()
                    .map(|file| (file.path.clone(), file.sha256.clone(), file.size))
                    .collect()
            })
            .unwrap_or_default();
        super::versions::validate_reviewed(
            state,
            id,
            revision,
            current_remote.as_ref(),
            evidence.as_ref().map(|value| {
                (
                    value.location.as_str(),
                    if c.kind == "folder_move" {
                        "folder"
                    } else {
                        "file"
                    },
                    moved_files.as_slice(),
                    value.directories.as_slice(),
                )
            }),
        )
        .await?;
    }
    let paths = [c.rel.as_str(), c.remote_from.as_deref().unwrap_or(&c.rel)];
    super::inbox::require_move_choice_stable(state, &c.share, &paths, &c.id).await?;
    if c.status != "open" {
        return Err(AppError::conflict(
            "not_open",
            "this conflict is already being resolved",
        ));
    }
    if let Some(remote) = current_remote {
        if remote.inventory.is_some()
            || !matches!(
                remote.vv.compare(&c.remote_vv),
                ism_core::version::Causality::Equal | ism_core::version::Causality::Dominates
            )
            || remote.sha256.is_none() && !remote.deleted
        {
            return Err(AppError::conflict(
                "recovery_evidence_missing",
                "the current peer version cannot be verified",
            ));
        }
        let side = ConflictSide {
            instance: c.remote.instance.clone(),
            sha256: remote.sha256.clone(),
            size: remote.size,
            modified_at: None,
            deleted: remote.deleted,
        };
        if remote.path != c.rel {
            sqlx::query(
                "UPDATE conflicts SET kind='move',remote_from=rel_path,rel_path=?,remote_json=?,remote_vv_json=?
                 WHERE id=? AND status='open'",
            ).bind(remote.path).bind(serde_json::to_string(&side)?)
            .bind(serde_json::to_string(&remote.vv)?).bind(id).execute(&state.db).await?;
            state.emit(ServerEvent::ConflictsChanged);
            return Err(AppError::conflict(
                "version_conflict",
                "the peer location changed; review its current move",
            ));
        }
        c.remote = side;
        c.remote_vv = remote.vv;
        sqlx::query(
            "UPDATE conflicts SET remote_json=?,remote_vv_json=? WHERE id=? AND status='open'",
        )
        .bind(serde_json::to_string(&c.remote)?)
        .bind(serde_json::to_string(&c.remote_vv)?)
        .bind(id)
        .execute(&state.db)
        .await?;
    }
    let is_move = matches!(c.kind.as_str(), "move" | "folder_move");
    let root = files::root(&state.db, &c.share.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&c.share.root_rel(&c.rel)?)?;
    let label = match resolution {
        ConflictResolution::KeepLocal => "keep_local",
        ConflictResolution::KeepRemote => "keep_remote",
        ConflictResolution::KeepBoth => "keep_both",
        ConflictResolution::Skip => "skip",
    };
    db::audit(
        &state.db,
        actor,
        "conflict.resolve",
        &format!("{} {label}", c.rel),
    )
    .await;

    if resolution == ConflictResolution::Skip {
        return Err(AppError::conflict(
            "recovery_not_exhausted",
            "choose a version before dismissing this conflict",
        ));
    }
    if is_move {
        return move_decisions::submit(
            state,
            actor,
            &c,
            &root,
            resolution,
            label,
            evidence.ok_or_else(|| AppError::internal("missing move evidence"))?,
        )
        .await;
    }

    let local_present = match std::fs::metadata(&abs) {
        Ok(meta) if meta.is_file() => true,
        Ok(_) => {
            return Err(AppError::conflict(
                "wrong_kind",
                "conflict target is not a regular file",
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(e.into()),
    };
    let local = if local_present {
        super::register_local(state, &c.share, &root, &c.rel).await?
    } else {
        super::row(state, &c.share.id, &c.rel).await?
    };
    let resolution = match resolution {
        ConflictResolution::KeepBoth if !local_present || c.remote.deleted => {
            if local_present {
                ConflictResolution::KeepLocal
            } else {
                ConflictResolution::KeepRemote
            }
        }
        other => other,
    };
    match resolution {
        ConflictResolution::KeepLocal => {
            let mut vv = merged(local.as_ref(), &c.remote_vv);
            vv.increment(&state.instance_id);
            let mut r = local.unwrap_or(Row {
                size: 0,
                mtime_ns: 0,
                sha256: None,
                deleted: true,
                vv: VersionVector::default(),
            });
            r.vv = vv.clone();
            r.deleted = !local_present;
            let (kind, data) = if local_present {
                (
                    "file_changed",
                    EventData {
                        path: c.rel.clone(),
                        sha256: r.sha256.clone(),
                        size: Some(r.size.max(0) as u64),
                        mtime_ns: Some(r.mtime_ns),
                        vv,
                        ..Default::default()
                    },
                )
            } else {
                (
                    "file_deleted",
                    EventData {
                        path: c.rel.clone(),
                        vv,
                        ..Default::default()
                    },
                )
            };
            let mut tx = state.db.begin().await?;
            let updated = sqlx::query(
                "UPDATE conflicts SET status = 'resolved', resolution = ?, resolved_at = ? WHERE id = ? AND status = 'open'",
            )
            .bind(label)
            .bind(db::now())
            .bind(id)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(AppError::conflict(
                    "not_open",
                    "this conflict is already being resolved",
                ));
            }
            super::put_row_in(&mut tx, &c.share.id, &c.rel, &r).await?;
            super::append_local_in(&mut tx, &state.instance_id, &c.share.name, kind, &data).await?;
            tx.commit().await?;
            state.emit(ServerEvent::SyncChanged);
            state.emit(ServerEvent::ConflictsChanged);
            Ok(())
        }
        ConflictResolution::KeepRemote => take_remote(state, id).await,
        ConflictResolution::Skip => mark_resolved(state, id, label).await,
        ConflictResolution::KeepBoth => {
            let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
            let mut n = 1;
            let target = loop {
                let candidate = conflict_name(&c.rel, &date, n);
                let exists = root
                    .abs(&c.share.root_rel(&candidate)?)?
                    .symlink_metadata()
                    .is_ok();
                if !exists {
                    break candidate;
                }
                n += 1;
                if n > 100 {
                    return Err(AppError::conflict(
                        "destination_exists",
                        "no free name for the local copy",
                    ));
                }
            };
            let vv = merged(local.as_ref(), &c.remote_vv);
            submit_pending(
                state,
                actor,
                &c,
                label,
                OperationAction::Rename {
                    source: c.share.location(&c.rel)?,
                    target: c.share.location(&target)?,
                    expected_revision: None,
                },
                "keep_both",
                Some(&c.rel),
                &target,
                &vv,
            )
            .await
        }
    }
}

/// Makes the remote side win: trashes the local copy, or downloads the remote version.
pub async fn take_remote(state: &AppState, id: &str) -> AppResult<()> {
    let c = load_conflict(state, id).await?;
    let root = files::root(&state.db, &c.share.root_id).await?;
    let abs = root.abs(&c.share.root_rel(&c.rel)?)?;
    let local = super::row(state, &c.share.id, &c.rel).await?;
    let vv = merged(local.as_ref(), &c.remote_vv);
    if c.remote.deleted {
        // The deletion is announced once the trash operation succeeded.
        if abs.is_dir() {
            return submit_pending(
                state,
                "conflict",
                &c,
                "keep_remote",
                OperationAction::TrashFolder {
                    source: c.share.location(&c.rel)?,
                    expected_entries: None,
                },
                "folder_trash",
                None,
                &c.rel,
                &vv,
            )
            .await;
        }
        if abs.is_file() {
            return submit_pending(
                state,
                "conflict",
                &c,
                "keep_remote",
                OperationAction::Trash {
                    source: c.share.location(&c.rel)?,
                    expected_revision: None,
                },
                "delete",
                None,
                &c.rel,
                &vv,
            )
            .await;
        }
        let mut r = local.unwrap_or(Row {
            size: 0,
            mtime_ns: 0,
            sha256: None,
            deleted: true,
            vv: VersionVector::default(),
        });
        r.deleted = true;
        r.vv = vv.clone();
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        resolved_in(&mut tx, id, "keep_remote").await?;
        super::put_row_in(&mut tx, &c.share.id, &c.rel, &r).await?;
        super::append_local_in(
            &mut tx,
            &state.instance_id,
            &c.share.name,
            "file_deleted",
            &EventData {
                path: c.rel.clone(),
                vv,
                ..Default::default()
            },
        )
        .await?;
        tx.commit().await?;
    } else if let Some(sha) = &c.remote.sha256 {
        // The download replaces the local copy through a managed upload (old version to the
        // ISM trash) once transfers are allowed.
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        super::queue_transfer_in(
            &mut tx,
            &c.peer_id,
            &c.share,
            &c.rel,
            sha,
            c.remote.size.unwrap_or(0),
            &vv,
        )
        .await?;
        sqlx::query("UPDATE conflicts SET status='resolving',resolution=COALESCE(resolution,'keep_remote') WHERE id=? AND status IN ('open','resolving')")
            .bind(id).execute(&mut *tx).await?;
        tx.commit().await?;
    } else {
        return Err(AppError::conflict(
            "recovery_evidence_missing",
            "the chosen peer version has no verified content",
        ));
    }
    state.sync_wake.notify_one();
    state.emit(ServerEvent::SyncChanged);
    state.emit(ServerEvent::ConflictsChanged);
    Ok(())
}

/// Marks a conflict resolved unless another resolution already finished it.
pub(super) async fn resolved_in(
    conn: &mut sqlx::SqliteConnection,
    id: &str,
    resolution: &str,
) -> AppResult<()> {
    let updated = sqlx::query(
        "UPDATE conflicts SET status = 'resolved', resolution = COALESCE(resolution, ?), resolved_at = ?
         WHERE id = ? AND status IN ('open', 'resolving')",
    )
    .bind(resolution)
    .bind(db::now())
    .bind(id)
    .execute(conn)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(AppError::conflict(
            "not_open",
            "this conflict is already resolved",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::conflict_name;

    #[test]
    fn names_conflict_copies() {
        assert_eq!(
            conflict_name("a/b/IMG_1.jpg", "2024-05-01", 1),
            "a/b/IMG_1 (conflict 2024-05-01).jpg"
        );
        assert_eq!(
            conflict_name("notes", "2024-05-01", 2),
            "notes (conflict 2024-05-01 2)"
        );
        assert_eq!(
            conflict_name(".hidden", "2024-05-01", 1),
            ".hidden (conflict 2024-05-01)"
        );
    }
}
