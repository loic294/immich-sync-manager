//! Receiver-owned transfer activity, mirrored incrementally to the source.

use std::collections::HashMap;
use std::time::Duration;

use ism_core::dto::{Location, SyncActivity, SyncPage, SyncProgress};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

use super::net;
use crate::db;
use crate::error::{AppError, AppResult};
use crate::state::{AppState, ServerEvent};

const CHANGE_LIMIT: i64 = 500;

pub async fn run(state: AppState) {
    // Spawned workers keep progressing during peer-list queries; dropping the set aborts them.
    let mut active = tokio::task::JoinSet::new();
    let mut running: HashMap<String, (net::Peer, tokio::task::AbortHandle)> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut configuration = state.peer_configuration.subscribe();
    loop {
        tokio::select! {
            reconfigured = async {
                tokio::select! {
                    _ = interval.tick() => false,
                    _ = configuration.changed() => true,
                }
            } => {
                match net::peers(&state).await {
                    Ok(peers) => {
                        state.peer_streams.retain(&peers);
                        running.retain(|id, (old, handle)| {
                            let keep = !reconfigured && peers.iter().any(|peer| peer.id == *id
                                && peer.url == old.url && peer.secret == old.secret
                                && peer.instance_id == old.instance_id && peer.name == old.name);
                            if !keep { handle.abort(); }
                            keep
                        });
                        for peer in peers {
                            if !running.contains_key(&peer.id) {
                                let id = peer.id.clone();
                                let state = state.clone();
                                let current = peer.clone();
                                let mut configuration = configuration.clone();
                                let worker = active.spawn(async move {
                                    tokio::select! {
                                        biased;
                                        _ = configuration.changed() => {}
                                        _ = super::notifications::run(&state, &peer) => {}
                                    }
                                });
                                running.insert(id, (current, worker));
                            }
                        }
                    }
                    Err(error) => tracing::error!("cannot load peers for sync activity: {}", error.message()),
                }
            }
            Some(result) = active.join_next_with_id(), if !active.is_empty() => {
                match result {
                    Ok((id, ())) => {
                        running.retain(|_, (_, handle)| handle.id() != id);
                    }
                    Err(error) => {
                        running.retain(|_, (_, handle)| handle.id() != error.id());
                        if !error.is_cancelled() {
                            tracing::error!("peer activity worker failed: {error}");
                        }
                    }
                }
            }
        }
    }
}

pub(super) async fn refresh_snapshot(
    state: &AppState,
    peer: &net::Peer,
    snapshot: super::notifications::Snapshot,
    verification: Option<&super::notifications::Refresh>,
) -> bool {
    let success = match snapshot {
        super::notifications::Snapshot::Recovery => {
            match super::recovery::refresh(state, peer).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(peer=%peer.id,"cannot refresh recovery: {}",error.message());
                    if let Err(error) = super::recovery::unavailable(state, &peer.id, &error).await
                    {
                        tracing::error!(
                            "cannot persist recovery availability: {}",
                            error.message()
                        );
                    }
                    false
                }
            }
        }
        super::notifications::Snapshot::Activity => match refresh(state, peer).await {
            Ok(()) => true,
            Err(error) => {
                let error = crate::error::redact(&error);
                tracing::warn!(peer=%peer.id, "cannot refresh activity: {error}");
                unavailable(state, &peer.id, &error).await;
                false
            }
        },
    };
    if let Some(verification) = verification
        && verification.complete(snapshot, success)
        && success
    {
        state.emit_ui(ServerEvent::SyncChanged);
    }
    success
}

const CTE: &str = "
WITH received AS (
    SELECT id,peer_id,share_id,rel_path,size,bytes_done,error,queued_at,
           NULL AS finished_at,NULL AS operation_id,status,run_id,0 AS readd_available
    FROM transfers
    UNION ALL
    SELECT h.id,h.peer_id,h.share_id,h.rel_path,h.size,h.size,
           CASE WHEN o.status IN ('failed','partial','recovery_required')
                THEN 'Publishing needs attention; open the related operation.' ELSE NULL END,
           h.queued_at,CASE WHEN h.operation_id IS NULL THEN h.finished_at ELSE o.finished_at END,
           h.operation_id,
           CASE WHEN h.operation_id IS NULL OR o.status = 'completed' THEN 'completed'
                WHEN o.status IN ('failed','partial','recovery_required') THEN 'failed'
                WHEN o.status = 'cancelled' THEN 'cancelled' ELSE 'publishing' END,
           h.run_id,0
    FROM transfer_history h LEFT JOIN operations o ON o.id = h.operation_id
    UNION ALL
    SELECT 'operation:' || p.operation_id || ':' || p.idx,p.peer_id,p.share_id,
           COALESCE(p.to_path,p.from_path),0,0,i.error_message,o.created_at,o.finished_at,
           p.operation_id,
           CASE WHEN o.status IN ('queued','waiting_maintenance') THEN 'queued'
                WHEN o.status = 'running' THEN 'publishing'
                WHEN o.status IN ('failed','partial','recovery_required','cancelled') THEN 'failed'
                ELSE 'completed' END,'',
           CASE WHEN o.status IN ('failed','partial','cancelled')
                OR (o.status = 'recovery_required' AND o.recovery_resolved != 0)
                THEN 1 ELSE 0 END
    FROM pending_peer_ops p JOIN operations o ON o.id = p.operation_id
    JOIN operation_items i ON i.operation_id = p.operation_id AND i.idx = p.idx
    UNION ALL
    SELECT 'operation:' || p.operation_id || ':0',p.peer_id,p.share_id,
           json_extract(p.event_json,'$.data.path'),0,0,i.error_message,
           o.created_at,o.finished_at,p.operation_id,
           CASE WHEN o.status IN ('queued','waiting_maintenance') THEN 'queued'
                WHEN o.status = 'running' THEN 'publishing'
                WHEN o.status IN ('failed','partial','recovery_required','cancelled') THEN 'failed'
                ELSE 'completed' END,'',
           CASE WHEN o.status IN ('failed','partial','cancelled')
                OR (o.status = 'recovery_required' AND o.recovery_resolved != 0)
                THEN 1 ELSE 0 END
    FROM pending_peer_events p JOIN operations o ON o.id = p.operation_id
    JOIN operation_items i ON i.operation_id = p.operation_id AND i.idx = 0
), activity AS (
    SELECT received.*,0 AS outgoing,NULL AS remote_destination FROM received
    UNION ALL
    SELECT c.id,c.peer_id,s.id,json_extract(c.activity_json,'$.relative_path'),
           json_extract(c.activity_json,'$.size'),json_extract(c.activity_json,'$.bytes_done'),
           json_extract(c.activity_json,'$.error'),json_extract(c.activity_json,'$.queued_at'),
           json_extract(c.activity_json,'$.finished_at'),json_extract(c.activity_json,'$.operation_id'),
           json_extract(c.activity_json,'$.status'),'',0,1,
           json_extract(c.activity_json,'$.destination_path')
    FROM outgoing_sync c JOIN shares s ON s.name = json_extract(c.activity_json,'$.share_name')
)";

struct Selection<'a> {
    status: &'a str,
    limit: i64,
    offset: i64,
    ids: Option<&'a str>,
    peer: Option<&'a str>,
    outgoing: bool,
}

async fn select(
    conn: &mut SqliteConnection,
    name: &str,
    q: Selection<'_>,
) -> AppResult<(Vec<SyncActivity>, i64)> {
    let filter = "t.outgoing <= ?1 AND (?2 IS NULL OR t.peer_id = ?2)
        AND (?3 IS NULL OR t.id IN (SELECT value FROM json_each(?3)))
        AND (?4 = 'all' OR (?4 = 'completed') = (t.status = 'completed'))";
    let total = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "{CTE} SELECT COUNT(*) FROM activity t
         JOIN shares s ON s.id = t.share_id JOIN mappings m ON m.root_id = s.root_id
         WHERE {filter}"
    )))
    .bind(q.outgoing)
    .bind(q.peer)
    .bind(q.ids)
    .bind(q.status)
    .fetch_one(&mut *conn)
    .await?;
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{CTE} SELECT t.*,p.name AS peer_name,s.name AS share_name,s.root_id,
            CASE WHEN s.path = '' THEN t.rel_path ELSE s.path || '/' || t.rel_path END AS path,
            m.container_path
         FROM activity t JOIN peers p ON p.id = t.peer_id JOIN shares s ON s.id = t.share_id
         JOIN mappings m ON m.root_id = s.root_id WHERE {filter}
         ORDER BY CASE t.status WHEN 'running' THEN 0 WHEN 'publishing' THEN 1 WHEN 'queued' THEN 2 ELSE 3 END,
            CASE WHEN t.status = 'completed' THEN t.finished_at END DESC,t.queued_at,t.id,t.peer_id
         LIMIT ?5 OFFSET ?6"
    )))
    .bind(q.outgoing)
    .bind(q.peer)
    .bind(q.ids)
    .bind(q.status)
    .bind(q.limit)
    .bind(q.offset)
    .fetch_all(&mut *conn)
    .await?;
    let rows = rows
        .into_iter()
        .map(|r| {
            let path: String = r.try_get("path")?;
            let outgoing: bool = r.try_get("outgoing")?;
            let peer: String = r.try_get("peer_name")?;
            Ok(SyncActivity {
                recovery: None,
                id: r.try_get("id")?,
                outgoing,
                peer_id: r.try_get("peer_id")?,
                share_name: r.try_get("share_name")?,
                relative_path: r.try_get("rel_path")?,
                location: Location {
                    root_id: r.try_get("root_id")?,
                    path: path.clone(),
                },
                source: format!("ISM: {}", if outgoing { name } else { &peer }),
                destination: format!("ISM: {}", if outgoing { &peer } else { name }),
                destination_path: if outgoing {
                    r.try_get("remote_destination")?
                } else {
                    format!(
                        "{}/{path}",
                        r.try_get::<String, _>("container_path")?
                            .trim_end_matches('/')
                    )
                },
                status: r.try_get("status")?,
                size: r.try_get::<i64, _>("size")?.max(0) as u64,
                bytes_done: r.try_get::<i64, _>("bytes_done")?.max(0) as u64,
                error: r.try_get("error")?,
                queued_at: r.try_get("queued_at")?,
                finished_at: r.try_get("finished_at")?,
                operation_id: r.try_get("operation_id")?,
                readd_available: r.try_get::<i64, _>("readd_available")? != 0,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok((rows, total))
}

fn estimate(progress: &mut SyncProgress) {
    let remaining = progress.bytes_total.saturating_sub(progress.bytes_done);
    progress.eta_seconds = if remaining > 0 && progress.bytes_per_second > 0.0 {
        Some((remaining as f64 / progress.bytes_per_second).ceil() as u64)
    } else {
        None
    };
}

async fn progress(conn: &mut SqliteConnection, peer: Option<&str>) -> AppResult<SyncProgress> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{CTE} SELECT COUNT(*) AS files_total,
            COALESCE(SUM(t.status = 'completed'),0) AS files_completed,
            COALESCE(SUM(t.size),0) AS bytes_total,
            COALESCE(SUM(min(t.size,max(t.bytes_done,0))),0) AS bytes_done,
            COALESCE(SUM(t.status = 'queued'),0) AS queued,
            COALESCE(SUM(t.status = 'running'),0) AS running,
            COALESCE(SUM(t.status = 'publishing'),0) AS publishing,
            COALESCE(SUM(t.status IN ('failed','cancelled')),0) AS failed
         FROM received t LEFT JOIN sync_runs r ON r.peer_id = t.peer_id
         WHERE (t.run_id = '' OR r.run_id = t.run_id)
           AND (?1 IS NULL OR t.peer_id = ?1)"
    )))
    .bind(peer)
    .fetch_one(&mut *conn)
    .await?;
    let mut result = SyncProgress {
        files_total: row.try_get::<i64, _>("files_total")?.max(0) as u64,
        files_completed: row.try_get::<i64, _>("files_completed")?.max(0) as u64,
        bytes_total: row.try_get::<i64, _>("bytes_total")?.max(0) as u64,
        bytes_done: row.try_get::<i64, _>("bytes_done")?.max(0) as u64,
        queued: row.try_get::<i64, _>("queued")?.max(0) as u64,
        running: row.try_get::<i64, _>("running")?.max(0) as u64,
        publishing: row.try_get::<i64, _>("publishing")?.max(0) as u64,
        failed: row.try_get::<i64, _>("failed")?.max(0) as u64,
        ..Default::default()
    };
    let now = chrono::Utc::now();
    let rates: Vec<(i64, String)> = sqlx::query_as(
        "SELECT SUM(s.bytes),r.started_at FROM sync_byte_samples s
         JOIN sync_runs r ON r.peer_id = s.peer_id
         WHERE s.at > ? AND (? IS NULL OR s.peer_id = ?) GROUP BY s.peer_id",
    )
    .bind(now.timestamp_millis() - 60_000)
    .bind(peer)
    .bind(peer)
    .fetch_all(&mut *conn)
    .await?;
    for (bytes, started_at) in rates {
        let started = db::parse_ts(&started_at)
            .ok_or_else(|| AppError::internal("invalid sync run start time"))?;
        let seconds = (now - started).num_milliseconds().clamp(1000, 60_000) as f64 / 1000.0;
        result.bytes_per_second += bytes as f64 / seconds;
    }
    estimate(&mut result);
    Ok(result)
}

pub async fn page(
    state: &AppState,
    status: &str,
    limit: i64,
    offset: i64,
    id: Option<&str>,
) -> AppResult<SyncPage> {
    super::recovery::discover(state).await?;
    let name = state.instance_name().await;
    let ids = id.map(|id| serde_json::to_string(&[id])).transpose()?;
    let mut tx = state.db.begin().await?;
    let (mut rows, total) = select(
        &mut tx,
        &name,
        Selection {
            status,
            limit,
            offset,
            ids: ids.as_deref(),
            peer: None,
            outgoing: true,
        },
    )
    .await?;
    let mut progress = progress(&mut tx, None).await?;
    type CachedProgress = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let cached: Vec<CachedProgress> = sqlx::query_as(
        "SELECT p.id,p.name,c.progress_json,c.error,c.updated_at FROM peers p
         LEFT JOIN peer_sync_state c ON c.peer_id = p.id ORDER BY p.name",
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut warnings = Vec::new();
    for (id, peer, json, error, updated) in cached {
        let fresh = super::notifications::snapshot_fresh(state, &id, updated.as_deref());
        let error =
            error.or_else(|| (!fresh).then(|| "No recent status from the receiver.".into()));
        if let Some(json) = json {
            let remote: SyncProgress = serde_json::from_str(&json)?;
            progress.files_total += remote.files_total;
            progress.files_completed += remote.files_completed;
            progress.bytes_total += remote.bytes_total;
            progress.bytes_done += remote.bytes_done;
            progress.queued += remote.queued;
            progress.running += remote.running;
            progress.publishing += remote.publishing;
            progress.failed += remote.failed;
            if error.is_none() {
                progress.bytes_per_second += remote.bytes_per_second;
            }
        }
        if let Some(error) = error {
            warnings.push(format!(
                "Outgoing sync with {peer} is not up to date (last update: {}). {error}",
                updated.as_deref().unwrap_or("never")
            ));
        }
    }
    let recovering: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM operations WHERE status='recovery_required' AND recovery_resolved=0",
    )
    .fetch_one(&mut *tx)
    .await?;
    if recovering > 0 {
        warnings.push(format!(
            "{recovering} sync issues are awaiting recovery. Independent sync continues."
        ));
    }
    if warnings.is_empty() {
        estimate(&mut progress);
    } else {
        progress.eta_seconds = None;
    }
    tx.commit().await?;
    for row in &mut rows {
        let (kind, id) = row.operation_id.as_ref().map_or(
            (ism_core::dto::RecoveryKind::Transfer, row.id.as_str()),
            |id| (ism_core::dto::RecoveryKind::Operation, id.as_str()),
        );
        row.recovery = if row.outgoing {
            super::recovery::outgoing(state, &row.peer_id, kind, id).await?
        } else {
            super::recovery::get(state, kind, id).await?
        };
    }
    Ok(SyncPage {
        rows,
        total,
        progress,
        warnings,
    })
}

pub async fn ensure_run(conn: &mut SqliteConnection, peer: &str) -> AppResult<String> {
    let current: Option<String> =
        sqlx::query_scalar("SELECT run_id FROM sync_runs WHERE peer_id = ?")
            .bind(peer)
            .fetch_optional(&mut *conn)
            .await?;
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM transfers WHERE peer_id = ?1 AND status IN ('queued','running','failed'))
         OR EXISTS(SELECT 1 FROM transfer_history h JOIN operations o ON o.id = h.operation_id
                   WHERE h.peer_id = ?1 AND o.status NOT IN ('completed','cancelled'))",
    )
    .bind(peer)
    .fetch_one(&mut *conn)
    .await?;
    if let Some(current) = current
        && pending
    {
        return Ok(current);
    }
    let id = crate::crypto::new_id("sync");
    sqlx::query(
        "INSERT INTO sync_runs (peer_id,run_id,started_at) VALUES (?,?,?)
         ON CONFLICT(peer_id) DO UPDATE SET run_id = excluded.run_id,started_at = excluded.started_at",
    )
    .bind(peer)
    .bind(&id)
    .bind(db::now())
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM sync_byte_samples WHERE peer_id = ?")
        .bind(peer)
        .execute(&mut *conn)
        .await?;
    Ok(id)
}

pub async fn record_progress(
    state: &AppState,
    id: &str,
    peer: &str,
    done: u64,
    delta: u64,
) -> AppResult<bool> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let updated = sqlx::query(
        "UPDATE transfers SET bytes_done = ?,updated_at = ? WHERE id = ? AND status = 'running'",
    )
    .bind(i64::try_from(done).map_err(AppError::internal)?)
    .bind(db::now())
    .bind(id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() == 0 {
        return Ok(false);
    }
    if delta > 0 {
        let now = chrono::Utc::now().timestamp_millis();
        sqlx::query("INSERT INTO sync_byte_samples (peer_id,at,bytes) VALUES (?,?,?)")
            .bind(peer)
            .bind(now)
            .bind(i64::try_from(delta).map_err(AppError::internal)?)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM sync_byte_samples WHERE at <= ?")
            .bind(now - 60_000)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(true)
}

#[derive(Serialize, Deserialize)]
pub struct Change {
    id: String,
    activity: Option<SyncActivity>,
}

#[derive(Serialize, Deserialize)]
pub struct Changes {
    cursor: i64,
    more: bool,
    changes: Vec<Change>,
    progress: SyncProgress,
}

pub async fn changes(state: &AppState, peer: &str, after: i64) -> AppResult<Changes> {
    if after < 0 {
        return Err(AppError::bad_request(
            "invalid_cursor",
            "cursor must be nonnegative",
        ));
    }
    let name = state.instance_name().await;
    let mut tx = state.db.begin().await?;
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT seq,transfer_id FROM sync_changes WHERE peer_id = ? AND seq > ? ORDER BY seq LIMIT ?",
    )
    .bind(peer)
    .bind(after)
    .bind(CHANGE_LIMIT)
    .fetch_all(&mut *tx)
    .await?;
    let ids = serde_json::to_string(&events.iter().map(|(_, id)| id).collect::<Vec<_>>())?;
    let (rows, _) = select(
        &mut tx,
        &name,
        Selection {
            status: "all",
            limit: CHANGE_LIMIT,
            offset: 0,
            ids: Some(&ids),
            peer: Some(peer),
            outgoing: false,
        },
    )
    .await?;
    let progress = progress(&mut tx, Some(peer)).await?;
    tx.commit().await?;
    let target = |row: &SyncActivity| {
        row.operation_id.as_ref().map_or(
            (ism_core::dto::RecoveryKind::Transfer, row.id.clone()),
            |id| (ism_core::dto::RecoveryKind::Operation, id.clone()),
        )
    };
    let targets: Vec<_> = rows.iter().map(target).collect();
    // Returned rows must carry their recovery, so discovery for them fails closed; it is bounded
    // to this page so unrelated backlog never holds the response behind the SQLite writer.
    super::recovery::discover_targets(
        state,
        &targets
            .iter()
            .map(|(kind, id)| (*kind, id.as_str()))
            .collect::<Vec<_>>(),
    )
    .await?;
    let mut decorated = Vec::new();
    for (mut row, (kind, id)) in rows.into_iter().zip(targets) {
        row.recovery = super::recovery::get(state, kind, &id).await?;
        decorated.push(row);
    }
    let mut rows: HashMap<_, _> = decorated
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect();
    Ok(Changes {
        cursor: events.last().map_or(after, |(seq, _)| *seq),
        more: events.len() == CHANGE_LIMIT as usize,
        changes: events
            .into_iter()
            .map(|(_, id)| Change {
                activity: rows.remove(&id),
                id,
            })
            .collect(),
        progress,
    })
}

type CachedProgress = (i64, Option<String>, Option<String>, Option<String>);

pub async fn refresh(state: &AppState, peer: &net::Peer) -> Result<(), String> {
    let mut cursor: i64 =
        sqlx::query_scalar("SELECT cursor FROM peer_sync_state WHERE peer_id = ?")
            .bind(&peer.id)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or(0);
    loop {
        let response = net::request(
            state,
            &peer.url,
            &peer.secret,
            reqwest::Method::GET,
            &format!("/api/peer/v1/sync-activity?after={cursor}"),
            None,
        )
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| net::net_error(&e))?
        .error_for_status()
        .map_err(|e| net::net_error(&e))?;
        let mut batch: Changes = response.json().await.map_err(|e| net::net_error(&e))?;
        if batch.cursor < cursor
            || (batch.cursor == cursor && (!batch.changes.is_empty() || batch.more))
            || batch.changes.len() > CHANGE_LIMIT as usize
            || !batch.progress.bytes_per_second.is_finite()
            || batch.progress.bytes_per_second < 0.0
            || batch.progress.bytes_done > batch.progress.bytes_total
            || batch.progress.files_completed > batch.progress.files_total
        {
            return Err("invalid peer sync activity".into());
        }
        let json = serde_json::to_string(&batch.progress).map_err(|e| e.to_string())?;
        if batch.changes.is_empty() && !batch.more {
            // Unchanged recent snapshots skip the writer lock; the write path rechecks.
            let old: Option<CachedProgress> = sqlx::query_as(
                "SELECT cursor,progress_json,error,updated_at FROM peer_sync_state WHERE peer_id = ?",
            )
            .bind(&peer.id)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| e.to_string())?;
            if old.is_some_and(|(old_cursor, progress, error, updated)| {
                old_cursor == batch.cursor
                    && progress.as_deref() == Some(&json)
                    && error.is_none()
                    && super::notifications::snapshot_recent(updated.as_deref())
            }) {
                return Ok(());
            }
        }
        let shares = super::shares(state)
            .await
            .map_err(|e| e.message().to_owned())?;
        let mut tx = state
            .db
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| e.to_string())?;
        for change in &mut batch.changes {
            if let Some(row) = &mut change.activity {
                if row.id != change.id
                    || row.outgoing
                    || row.bytes_done > row.size
                    || !matches!(
                        row.status.as_str(),
                        "queued" | "running" | "publishing" | "completed" | "failed" | "cancelled"
                    )
                {
                    return Err("invalid peer transfer activity".into());
                }
                row.readd_available = false;
                if let Some(share) = shares
                    .iter()
                    .find(|s| s.name == row.share_name && s.includes(&peer.id))
                {
                    row.location = share
                        .location(&row.relative_path)
                        .map_err(|e| e.message().to_owned())?;
                    row.outgoing = true;
                    row.peer_id = peer.id.clone();
                    sqlx::query(
                        "INSERT INTO outgoing_sync (peer_id,id,activity_json) VALUES (?,?,?)
                         ON CONFLICT(peer_id,id) DO UPDATE SET activity_json = excluded.activity_json",
                    )
                    .bind(&peer.id).bind(&change.id)
                    .bind(serde_json::to_string(row).map_err(|e| e.to_string())?)
                    .execute(&mut *tx).await.map_err(|e| e.to_string())?;
                    continue;
                }
            }
            sqlx::query("DELETE FROM outgoing_sync WHERE peer_id = ? AND id = ?")
                .bind(&peer.id)
                .bind(&change.id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
        }
        let old: Option<CachedProgress> = sqlx::query_as(
            "SELECT cursor,progress_json,error,updated_at FROM peer_sync_state WHERE peer_id = ?",
        )
        .bind(&peer.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        let changed = old
            .as_ref()
            .is_none_or(|(old_cursor, progress, error, updated_at)| {
                *old_cursor != batch.cursor
                    || progress.as_deref() != Some(&json)
                    || error.is_some()
                    || updated_at.is_none() != batch.more
            });
        let updated_at = (!batch.more).then(db::now);
        let recent = old.as_ref().is_some_and(|(_, _, _, updated)| {
            super::notifications::snapshot_recent(updated.as_deref())
        });
        if changed || !recent {
            sqlx::query(
                "INSERT INTO peer_sync_state (peer_id,cursor,progress_json,updated_at,error) VALUES (?,?,?,?,NULL)
                 ON CONFLICT(peer_id) DO UPDATE SET cursor = excluded.cursor,progress_json = excluded.progress_json,
                     updated_at = excluded.updated_at,error = NULL",
            ).bind(&peer.id).bind(batch.cursor).bind(json).bind(updated_at)
            .execute(&mut *tx).await.map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        if changed {
            state.emit_ui(ServerEvent::SyncChanged);
        }
        cursor = batch.cursor;
        if !batch.more {
            return Ok(());
        }
    }
}

pub async fn unavailable(state: &AppState, peer: &str, error: &str) {
    let updated = sqlx::query(
        "INSERT INTO peer_sync_state (peer_id,error) VALUES (?,?)
         ON CONFLICT(peer_id) DO UPDATE SET error = excluded.error
         WHERE peer_sync_state.error IS NOT excluded.error",
    )
    .bind(peer)
    .bind(error)
    .execute(&state.db)
    .await;
    match updated {
        Ok(result) if result.rows_affected() > 0 => state.emit_ui(ServerEvent::SyncChanged),
        Ok(_) => {}
        Err(e) => tracing::error!(peer, "record sync activity error: {e}"),
    }
}
