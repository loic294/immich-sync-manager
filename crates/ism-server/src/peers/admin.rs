//! Administration of peers, shares, the transfer schedule and conflicts.

use axum::http::HeaderMap;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ism_core::dto::{
    ConflictResolution, ConflictSide, ConflictView, Location, OperationAction, OperationRequest,
    OverrideInput, OverrideView, PeerAcceptInput, PeerInvite, PeerView, ScheduleView, ShareInput,
    ShareView, SyncStatus, TransferView,
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
    let peer = peer_view(state, id).await?;
    let mut tx = state.db.begin().await?;
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
    let (name, path) = validate_share(state, None, &input).await?;
    let id = crate::crypto::new_id("share");
    sqlx::query("INSERT INTO shares (id, name, root_id, path, peer_ids_json, enabled) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&id)
        .bind(&name)
        .bind(&input.location.root_id)
        .bind(&path)
        .bind(serde_json::to_string(&input.peer_ids)?)
        .bind(input.enabled as i64)
        .execute(&state.db)
        .await?;
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
    let current = share_view(state, id).await?;
    if current.name != input.name.trim() || current.location != input.location {
        return Err(AppError::bad_request(
            "share_locked",
            "a share's name and folder cannot change; remove it and create a new one",
        ));
    }
    validate_share(state, Some(id), &input).await?;
    sqlx::query("UPDATE shares SET peer_ids_json = ?, enabled = ? WHERE id = ?")
        .bind(serde_json::to_string(&input.peer_ids)?)
        .bind(input.enabled as i64)
        .bind(id)
        .execute(&state.db)
        .await?;
    db::audit(&state.db, "admin", "share.updated", &current.name).await;
    state.sync_wake.notify_one();
    share_view(state, id).await
}

pub async fn delete_share(state: &AppState, id: &str) -> AppResult<()> {
    let share = share_view(state, id).await?;
    sqlx::query("DELETE FROM shares WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await?;
    sqlx::query("DELETE FROM overrides WHERE share_id = ?")
        .bind(id)
        .execute(&state.db)
        .await?;
    db::audit(&state.db, "admin", "share.removed", &share.name).await;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Schedule, overrides and transfers

pub async fn schedule(state: &AppState) -> AppResult<ScheduleView> {
    Ok(db::get_setting(&state.db, SCHEDULE_KEY)
        .await?
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or(ScheduleView {
            timezone: "UTC".into(),
            windows: Vec::new(),
        }))
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
        running: transfers(state, "'running'", 50).await?,
        queued: transfers(state, "'queued', 'failed', 'cancelled'", 500).await?,
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
}

async fn load_conflict(state: &AppState, id: &str) -> AppResult<Conflict> {
    let r: Option<(String, String, String, String, String, String, String)> = sqlx::query_as(
        "SELECT share_id, rel_path, kind, remote_json, remote_vv_json, peer_id, status FROM conflicts WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    let (share_id, rel, kind, remote, vv, peer_id, status) =
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
        remote_vv: serde_json::from_str(&vv).unwrap_or_default(),
        peer_id,
        status,
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
    );
    let rows: Vec<R> = sqlx::query_as(
        "SELECT id, share_id, rel_path, kind, detail, local_json, remote_json, peer_id, status, created_at FROM conflicts
         WHERE status != 'resolved' OR ? ORDER BY created_at DESC LIMIT 500",
    )
    .bind(include_resolved)
    .fetch_all(&state.db)
    .await?;
    let shares = super::shares(state).await?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(id, share_id, rel, kind, detail, local, remote, peer_id, status, created_at)| {
                let share = shares.iter().find(|s| s.id == share_id)?;
                Some(ConflictView {
                    id,
                    share_name: share.name.clone(),
                    location: super::display(share, &rel),
                    kind,
                    detail,
                    local: serde_json::from_str(&local).ok()?,
                    remote: serde_json::from_str(&remote).ok()?,
                    peer_id,
                    status,
                    created_at,
                })
            },
        )
        .collect())
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

/// Submits a managed operation whose replica bookkeeping runs when it finishes.
#[allow(clippy::too_many_arguments)]
async fn submit_pending(
    state: &AppState,
    actor: &str,
    c: &Conflict,
    action: OperationAction,
    kind: &str,
    from: Option<&str>,
    to: &str,
    vv: &VersionVector,
) -> AppResult<()> {
    let req = OperationRequest {
        idempotency_key: format!("conflict:{}:{kind}", c.id),
        label: Some(format!("Resolve conflict on {}", c.rel)),
        actions: vec![action],
        allow_owner_transfer: false,
    };
    let principal = Principal::Admin {
        username: actor.to_owned(),
    };
    let accepted = crate::ops::insert(state, &principal, &req).await?;
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
    .execute(&state.db)
    .await?;
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

pub async fn resolve(
    state: &AppState,
    actor: &str,
    id: &str,
    resolution: ConflictResolution,
) -> AppResult<()> {
    let c = load_conflict(state, id).await?;
    if c.status != "open" {
        return Err(AppError::conflict(
            "not_open",
            "this conflict is already being resolved",
        ));
    }
    let root = files::root(&state.db, &c.share.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&c.share.root_rel(&c.rel)?)?;
    let is_move = matches!(c.kind.as_str(), "move" | "folder_move");
    let label = match resolution {
        ConflictResolution::KeepLocal => "keep_local",
        ConflictResolution::KeepRemote => "keep_remote",
        ConflictResolution::KeepBoth => "keep_both",
    };
    db::audit(
        &state.db,
        actor,
        "conflict.resolve",
        &format!("{} {label}", c.rel),
    )
    .await;

    if is_move {
        // The local layout stays; keeping the remote side fetches its copy at the new path.
        if matches!(resolution, ConflictResolution::KeepRemote)
            && c.kind == "move"
            && !abs.exists()
            && let Some(sha) = &c.remote.sha256
        {
            super::queue_transfer(
                state,
                &c.peer_id,
                &c.share,
                &c.rel,
                sha,
                c.remote.size.unwrap_or(0),
                &c.remote_vv,
            )
            .await?;
        }
        return mark_resolved(state, id, label).await;
    }

    let local = if abs.is_file() {
        super::register_local(state, &c.share, &root, &c.rel).await?
    } else {
        super::row(state, &c.share.id, &c.rel).await?
    };
    let local_present = abs.is_file();
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
            super::put_row(state, &c.share.id, &c.rel, &r).await?;
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
            super::append_local(state, &c.share.name, kind, &data).await?;
            mark_resolved(state, id, label).await
        }
        ConflictResolution::KeepRemote => take_remote(state, id).await,
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
            .await?;
            sqlx::query("UPDATE conflicts SET status = 'resolving', resolution = ? WHERE id = ?")
                .bind(label)
                .bind(id)
                .execute(&state.db)
                .await?;
            state.emit(ServerEvent::ConflictsChanged);
            Ok(())
        }
    }
}

/// Makes the remote side win: deletes the local copy, or downloads the remote version.
pub async fn take_remote(state: &AppState, id: &str) -> AppResult<()> {
    let c = load_conflict(state, id).await?;
    let root = files::root(&state.db, &c.share.root_id).await?;
    let abs = root.abs(&c.share.root_rel(&c.rel)?)?;
    let local = super::row(state, &c.share.id, &c.rel).await?;
    let vv = merged(local.as_ref(), &c.remote_vv);
    if c.remote.deleted {
        if abs.is_dir() {
            submit_pending(
                state,
                "conflict",
                &c,
                OperationAction::TrashFolder {
                    source: c.share.location(&c.rel)?,
                },
                "folder_trash",
                None,
                &c.rel,
                &vv,
            )
            .await?;
        } else if abs.is_file() {
            submit_pending(
                state,
                "conflict",
                &c,
                OperationAction::Trash {
                    source: c.share.location(&c.rel)?,
                },
                "delete",
                None,
                &c.rel,
                &vv,
            )
            .await?;
        } else {
            let mut r = local.unwrap_or(Row {
                size: 0,
                mtime_ns: 0,
                sha256: None,
                deleted: true,
                vv: VersionVector::default(),
            });
            r.deleted = true;
            r.vv = vv.clone();
            super::put_row(state, &c.share.id, &c.rel, &r).await?;
        }
        super::append_local(
            state,
            &c.share.name,
            "file_deleted",
            &EventData {
                path: c.rel.clone(),
                vv,
                ..Default::default()
            },
        )
        .await?;
    } else if let Some(sha) = &c.remote.sha256 {
        sqlx::query("UPDATE conflicts SET status = 'resolved', resolution = COALESCE(resolution, 'keep_remote'), resolved_at = ? WHERE id = ?")
            .bind(db::now())
            .bind(id)
            .execute(&state.db)
            .await?;
        super::queue_transfer(
            state,
            &c.peer_id,
            &c.share,
            &c.rel,
            sha,
            c.remote.size.unwrap_or(0),
            &vv,
        )
        .await?;
        super::append_local(
            state,
            &c.share.name,
            "file_changed",
            &EventData {
                path: c.rel.clone(),
                sha256: Some(sha.clone()),
                size: c.remote.size,
                vv,
                ..Default::default()
            },
        )
        .await?;
        state.emit(ServerEvent::ConflictsChanged);
        return Ok(());
    }
    sqlx::query("UPDATE conflicts SET status = 'resolved', resolution = COALESCE(resolution, 'keep_remote'), resolved_at = ? WHERE id = ?")
        .bind(db::now())
        .bind(id)
        .execute(&state.db)
        .await?;
    state.emit(ServerEvent::ConflictsChanged);
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
