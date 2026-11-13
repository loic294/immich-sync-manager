//! Copies file contents from peers during allowed hours (or under a manual override).

use std::time::{Duration, UNIX_EPOCH};

use futures_util::StreamExt;
use ism_core::schedule::Schedule;
use ism_core::version::{Causality, VersionVector};
use reqwest::Method;
use tokio::io::AsyncWriteExt;

use super::{ConflictInput, Row, Share, net};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files::{self, STAGING_DIR};
use crate::state::{AppState, ServerEvent};

const MAX_ATTEMPTS: i64 = 8;
const PROGRESS_EVERY: u64 = 4 * 1024 * 1024;

pub fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Whether file contents may move now for this share and peer.
pub async fn allowed(
    state: &AppState,
    share_id: &str,
    peer_id: &str,
) -> AppResult<(bool, Option<String>)> {
    let now = chrono::Utc::now();
    let ov: Option<String> = sqlx::query_scalar(
        "SELECT id FROM overrides WHERE revoked_at IS NULL AND until > ?
         AND (share_id IS NULL OR share_id = ?) AND (peer_id IS NULL OR peer_id = ?) ORDER BY until DESC LIMIT 1",
    )
    .bind(db::ts(now))
    .bind(share_id)
    .bind(peer_id)
    .fetch_optional(&state.db)
    .await?;
    if ov.is_some() {
        return Ok((true, ov));
    }
    let view = super::admin::schedule(state).await?;
    let schedule = Schedule::new(&view.timezone, &view.windows)
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok((schedule.is_allowed(now), None))
}

#[derive(Debug, Clone)]
struct Transfer {
    id: String,
    peer_id: String,
    share_id: String,
    rel: String,
    sha256: String,
    size: i64,
    vv: VersionVector,
}

async fn next(state: &AppState) -> AppResult<Option<(Transfer, Option<String>)>> {
    type R = (
        String,
        String,
        String,
        String,
        String,
        i64,
        String,
        i64,
        String,
    );
    let rows: Vec<R> = sqlx::query_as(
        "SELECT t.id, t.peer_id, t.share_id, t.rel_path, t.sha256, t.size, t.vv_json, t.attempts, t.updated_at
         FROM transfers t JOIN peers p ON p.id = t.peer_id
         WHERE t.status = 'queued' AND p.status = 'online' ORDER BY t.queued_at LIMIT 200",
    )
    .fetch_all(&state.db)
    .await?;
    let now = chrono::Utc::now();
    for (id, peer_id, share_id, rel, sha256, size, vv, attempts, updated) in rows {
        // Back off after failures: one minute per attempt.
        if attempts > 0
            && db::parse_ts(&updated).is_some_and(|u| now - u < chrono::Duration::minutes(attempts))
        {
            continue;
        }
        let (ok, ov) = allowed(state, &share_id, &peer_id).await?;
        if ok {
            return Ok(Some((
                Transfer {
                    id,
                    peer_id,
                    share_id,
                    rel,
                    sha256,
                    size,
                    vv: serde_json::from_str(&vv).unwrap_or_default(),
                },
                ov,
            )));
        }
    }
    Ok(None)
}

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        match next(&state).await {
            Ok(Some((t, ov))) => {
                let _ = sqlx::query("UPDATE transfers SET status = 'running', override_id = ?, updated_at = ? WHERE id = ?")
                    .bind(&ov)
                    .bind(db::now())
                    .bind(&t.id)
                    .execute(&state.db)
                    .await;
                state.emit(ServerEvent::SyncChanged);
                let result = run_one(&state, &t).await;
                let now = db::now();
                let _ = match result {
                    Ok(Outcome::Done) => {
                        sqlx::query("DELETE FROM transfers WHERE id = ?")
                            .bind(&t.id)
                            .execute(&state.db)
                            .await
                    }
                    Ok(Outcome::Dropped) => {
                        sqlx::query("DELETE FROM transfers WHERE id = ?")
                            .bind(&t.id)
                            .execute(&state.db)
                            .await
                    }
                    Err(e) => {
                        tracing::warn!(transfer = %t.id, "transfer failed: {e}");
                        sqlx::query(
                            "UPDATE transfers SET attempts = attempts + 1, error = ?, updated_at = ?,
                             status = CASE WHEN status = 'cancelled' THEN 'cancelled'
                                           WHEN attempts + 1 >= ? THEN 'failed' ELSE 'queued' END
                             WHERE id = ?",
                        )
                        .bind(&e)
                        .bind(&now)
                        .bind(MAX_ATTEMPTS)
                        .bind(&t.id)
                        .execute(&state.db)
                        .await
                    }
                };
                state.emit(ServerEvent::SyncChanged);
            }
            Ok(None) => {
                tokio::select! {
                    () = tokio::time::sleep(tick * 6) => {}
                    () = state.sync_wake.notified() => {}
                }
            }
            Err(e) => {
                tracing::error!("transfer queue: {}", e.message());
                tokio::time::sleep(tick * 6).await;
            }
        }
    }
}

enum Outcome {
    Done,
    /// The remote version is gone or superseded; a newer event will follow.
    Dropped,
}

async fn run_one(state: &AppState, t: &Transfer) -> Result<Outcome, String> {
    let m = |e: AppError| e.message().to_owned();
    let peer = net::peer(state, &t.peer_id).await.map_err(m)?;
    let shares = super::shares(state).await.map_err(m)?;
    let Some(share) = shares.into_iter().find(|s| s.id == t.share_id) else {
        return Ok(Outcome::Dropped);
    };
    if !share.includes(&peer.id) {
        return Ok(Outcome::Dropped);
    }
    let root = files::root(&state.db, &share.root_id).await.map_err(m)?;
    root.ensure_available().map_err(m)?;
    let target_rel = share.root_rel(&t.rel).map_err(m)?;
    let target = root.abs(&target_rel).map_err(m)?;

    // Adopt an identical local copy without downloading.
    if target.is_file()
        && files::cached_sha256(&state.db, &root.mapping.root_id, &target_rel, &target)
            .await
            .map_err(m)?
            == t.sha256
    {
        let meta = std::fs::metadata(&target).map_err(|e| e.to_string())?;
        let mut r = super::row(state, &share.id, &t.rel)
            .await
            .map_err(m)?
            .unwrap_or(Row {
                size: 0,
                mtime_ns: 0,
                sha256: None,
                deleted: false,
                vv: VersionVector::default(),
            });
        r.vv.merge(&t.vv);
        r.size = meta.len() as i64;
        r.mtime_ns = files::mtime_ns(&meta);
        r.sha256 = Some(t.sha256.clone());
        r.deleted = false;
        super::put_row(state, &share.id, &t.rel, &r)
            .await
            .map_err(m)?;
        return Ok(Outcome::Done);
    }

    let staging_dir = root.base().join(STAGING_DIR).join("peer");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|e| e.to_string())?;
    let staging = staging_dir.join(&t.id);
    let offset = tokio::fs::metadata(&staging)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let path = format!(
        "/api/peer/v1/content?share={}&path={}&sha256={}",
        encode(&share.name),
        encode(&t.rel),
        t.sha256
    );
    let mut req = net::request(state, &peer.url, &peer.secret, Method::GET, &path, None)
        .timeout(Duration::from_secs(24 * 3600));
    if offset > 0 && (offset as i64) < t.size {
        req = req.header("range", format!("bytes={offset}-"));
    }
    let resp = req.send().await.map_err(|e| net::net_error(&e))?;
    let status = resp.status().as_u16();
    if status == 404 || status == 409 || status == 410 {
        let _ = tokio::fs::remove_file(&staging).await;
        return Ok(Outcome::Dropped);
    }
    if status != 200 && status != 206 {
        return Err(format!("HTTP {status} from {}", peer.name));
    }
    let mtime_ns: Option<i64> = resp
        .headers()
        .get("x-ism-mtime-ns")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(status == 206)
        .write(true)
        .truncate(status == 200)
        .open(&staging)
        .await
        .map_err(|e| e.to_string())?;
    let mut done = if status == 206 { offset } else { 0 };
    let mut reported = done;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| net::net_error(&e))?;
        file.write_all(&chunk).await.map_err(|e| e.to_string())?;
        done += chunk.len() as u64;
        if done - reported >= PROGRESS_EVERY {
            reported = done;
            let cancelled: Option<String> =
                sqlx::query_scalar("SELECT status FROM transfers WHERE id = ?")
                    .bind(&t.id)
                    .fetch_optional(&state.db)
                    .await
                    .ok()
                    .flatten();
            if cancelled.as_deref() != Some("running") {
                let _ = tokio::fs::remove_file(&staging).await;
                return Ok(Outcome::Dropped);
            }
            let _ = sqlx::query("UPDATE transfers SET bytes_done = ?, updated_at = ? WHERE id = ?")
                .bind(done as i64)
                .bind(db::now())
                .bind(&t.id)
                .execute(&state.db)
                .await;
            state.emit(ServerEvent::SyncChanged);
        }
    }
    file.sync_all().await.map_err(|e| e.to_string())?;
    drop(file);
    let check = staging.clone();
    let hash = tokio::task::spawn_blocking(move || files::sha256_file(&check))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    if hash != t.sha256 {
        let _ = tokio::fs::remove_file(&staging).await;
        return Err("downloaded data does not match its checksum".into());
    }

    // Publish only if nothing changed here since the transfer was queued.
    let local = super::row(state, &share.id, &t.rel).await.map_err(m)?;
    let disk = std::fs::metadata(&target).ok();
    let consistent = match (&local, &disk) {
        (None, None) => true,
        (Some(r), None) => r.deleted,
        (Some(r), Some(meta)) => {
            !r.deleted && r.size == meta.len() as i64 && r.mtime_ns == files::mtime_ns(meta)
        }
        (None, Some(_)) => false,
    };
    let newer = local
        .as_ref()
        .is_none_or(|r| matches!(t.vv.compare(&r.vv), Causality::Dominates | Causality::Equal));
    if !consistent
        || !newer
        || super::has_open_conflict(state, &share.id, &t.rel)
            .await
            .map_err(m)?
    {
        let _ = tokio::fs::remove_file(&staging).await;
        conflict_here(state, &peer, &share, t).await.map_err(m)?;
        return Ok(Outcome::Dropped);
    }
    let (src, dst) = (staging.clone(), target.clone());
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(ns) = mtime_ns {
            let when = UNIX_EPOCH + Duration::from_nanos(ns.max(0) as u64);
            std::fs::File::options()
                .write(true)
                .open(&src)?
                .set_modified(when)?;
        }
        std::fs::rename(&src, &dst)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;
    let meta = std::fs::metadata(&target).map_err(|e| e.to_string())?;
    let mut vv = local.map(|r| r.vv).unwrap_or_default();
    vv.merge(&t.vv);
    super::put_row(
        state,
        &share.id,
        &t.rel,
        &Row {
            size: meta.len() as i64,
            mtime_ns: files::mtime_ns(&meta),
            sha256: Some(t.sha256.clone()),
            deleted: false,
            vv,
        },
    )
    .await
    .map_err(m)?;
    Ok(Outcome::Done)
}

async fn conflict_here(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    t: &Transfer,
) -> AppResult<()> {
    super::conflict(
        state,
        ConflictInput {
            share,
            rel: &t.rel,
            kind: "changed_on_both",
            detail: format!(
                "{} changed here while a newer version from {} was downloading",
                t.rel, peer.name
            ),
            remote: ism_core::dto::ConflictSide {
                instance: peer.name.clone(),
                sha256: Some(t.sha256.clone()),
                size: Some(t.size as u64),
                modified_at: None,
                deleted: false,
            },
            remote_vv: &t.vv,
            peer_id: &peer.id,
        },
    )
    .await
}
