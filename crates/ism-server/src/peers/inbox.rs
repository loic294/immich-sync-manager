//! Durable receipt and dependency-aware application. Only verified effects enter `event_log`.

use super::*;
use serde_json::Value;

const INBOX_LIMIT: i64 = 10_000;

pub(super) async fn backfill(state: &AppState, peer: &net::Peer) -> Result<usize, String> {
    crate::activity::observe(
        state,
        format!("backfill:{}", peer.id),
        "Peer inventory backfill",
        &peer.name,
        Some("/peers"),
        |task| async move {
            backfill_reporting(state, peer, &task)
                .await
                .map_err(|e| AppError::conflict("peer_backfill", crate::error::redact(&e)))
        },
    )
    .await
    .map_err(|e| e.message().to_owned())
}

async fn backfill_reporting(
    state: &AppState,
    peer: &net::Peer,
    task: &crate::activity::Reporter,
) -> Result<usize, String> {
    task.phase("Fetching inventory pages and saving durable receipts");
    let all = shares(state).await.map_err(|e| e.message().to_owned())?;
    let pending: Option<(String, String)> = sqlx::query_as(
        "SELECT share_id, after_path FROM share_backfills WHERE peer_id = ?
         ORDER BY attempted_at IS NOT NULL, attempted_at, requested_at, share_id LIMIT 1",
    )
    .bind(&peer.id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| e.to_string())?;
    let mut receipt = Ok(());
    if let Some((share_id, after)) = pending {
        sqlx::query(
            "UPDATE share_backfills SET attempted_at = ? WHERE peer_id = ? AND share_id = ?",
        )
        .bind(db::now())
        .bind(&peer.id)
        .bind(&share_id)
        .execute(&state.db)
        .await
        .map_err(|e| e.to_string())?;
        if let Some(share) = all
            .iter()
            .find(|s| s.id == share_id && s.includes(&peer.id))
        {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM peer_backfill_inbox WHERE peer_id = ?")
                    .bind(&peer.id)
                    .fetch_one(&state.db)
                    .await
                    .map_err(|e| e.to_string())?;
            if count <= INBOX_LIMIT - INVENTORY_BATCH {
                receipt = match net::get_json::<InventoryPage>(
                    state,
                    peer,
                    &format!(
                        "/api/peer/v1/inventory?share={}&after={}",
                        transfer::encode(&share.name),
                        transfer::encode(&after)
                    ),
                )
                .await
                {
                    Ok(page) => {
                        page.validate(&after)?;
                        admit_inventory(state, peer, share, &after, &page)
                            .await
                            .map_err(|e| e.message().to_owned())
                    }
                    Err(error) => Err(error),
                };
            }
        } else {
            sqlx::query("DELETE FROM share_backfills WHERE peer_id = ? AND share_id = ?")
                .bind(&peer.id)
                .bind(&share_id)
                .execute(&state.db)
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    task.waiting("Waiting for the replication-operation lock before applying inventory");
    let _guard = state.replication_operation_lock.lock().await;
    task.phase("Applying inventory entries");
    let applied = replay_inventory(state, peer, &all, task)
        .await
        .map_err(|e| e.message().to_owned())?;
    if applied > 0 {
        state.emit(ServerEvent::SyncChanged);
        Ok(applied)
    } else {
        receipt.map(|_| 0)
    }
}

async fn admit_inventory(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    after: &str,
    page: &InventoryPage,
) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM peer_backfill_inbox WHERE peer_id = ?")
            .bind(&peer.id)
            .fetch_one(&mut *tx)
            .await?;
    if count + page.entries.len() as i64 > INBOX_LIMIT {
        return Err(AppError::conflict(
            "peer_inbox_full",
            "inventory inbox admission limit reached",
        ));
    }
    for entry in &page.entries {
        sqlx::query(
            "INSERT INTO peer_backfill_inbox(peer_id, share_id, path, entry_json) VALUES (?, ?, ?, ?)
             ON CONFLICT(peer_id, share_id, path) DO UPDATE SET
                 next_entry_json = CASE WHEN operation_id IS NOT NULL THEN excluded.entry_json ELSE NULL END,
                 entry_json = CASE WHEN operation_id IS NULL THEN excluded.entry_json ELSE entry_json END",
        )
        .bind(&peer.id).bind(&share.id).bind(&entry.path).bind(serde_json::to_string(entry)?)
        .execute(&mut *tx).await?;
    }
    let changed = if let Some(next) = &page.next {
        sqlx::query("UPDATE share_backfills SET after_path = ? WHERE share_id = ? AND peer_id = ? AND after_path = ?")
            .bind(next).bind(&share.id).bind(&peer.id).bind(after).execute(&mut *tx).await?
    } else {
        sqlx::query(
            "DELETE FROM share_backfills WHERE share_id = ? AND peer_id = ? AND after_path = ?",
        )
        .bind(&share.id)
        .bind(&peer.id)
        .bind(after)
        .execute(&mut *tx)
        .await?
    };
    if changed.rows_affected() != 1 {
        return Err(AppError::conflict(
            "peer_backfill_changed",
            "share backfill restarted while receiving a page; retrying",
        ));
    }
    tx.commit().await?;
    Ok(())
}

async fn replay_inventory(
    state: &AppState,
    peer: &net::Peer,
    all: &[Share],
    task: &crate::activity::Reporter,
) -> AppResult<usize> {
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT share_id, path, entry_json, operation_id FROM peer_backfill_inbox WHERE peer_id = ?
         ORDER BY attempted_at IS NOT NULL, attempted_at, share_id, path LIMIT ?",
    )
    .bind(&peer.id)
    .bind(INVENTORY_BATCH)
    .fetch_all(&state.db)
    .await?;
    let mut applied = 0;
    let total = rows.len() as u64;
    for (done, (share_id, path, raw, operation)) in rows.into_iter().enumerate() {
        task.progress(done as u64, Some(total), "inventory entries checked");
        sqlx::query("UPDATE peer_backfill_inbox SET attempted_at = ? WHERE peer_id = ? AND share_id = ? AND path = ?")
            .bind(db::now()).bind(&peer.id).bind(&share_id).bind(&path).execute(&state.db).await?;
        let result = if let Some(operation) = operation {
            operation_finished(state, &operation, &[])
                .await
                .map(|_| false)
        } else if let Some(share) = all
            .iter()
            .find(|s| s.id == share_id && s.includes(&peer.id))
        {
            let entry: InventoryEntry = serde_json::from_str(&raw)?;
            apply_inventory(state, peer, share, &entry).await
        } else {
            sqlx::query(
                "DELETE FROM peer_backfill_inbox WHERE peer_id = ? AND share_id = ? AND path = ?",
            )
            .bind(&peer.id)
            .bind(&share_id)
            .bind(&path)
            .execute(&state.db)
            .await?;
            Ok(false)
        };
        match result {
            Ok(done) => applied += usize::from(done),
            Err(error) => {
                sqlx::query("UPDATE peer_backfill_inbox SET error = ? WHERE peer_id = ? AND share_id = ? AND path = ?")
                    .bind(error.message()).bind(&peer.id).bind(&share_id).bind(&path).execute(&state.db).await?;
            }
        }
    }
    Ok(applied)
}

async fn apply_inventory(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    entry: &InventoryEntry,
) -> AppResult<bool> {
    let ignored = entry.path.split('/').any(|name| hidden(name, &share.junk));
    let mut batch = Batch {
        actions: Vec::new(),
        pending: Vec::new(),
        paths: HashSet::new(),
    };
    if !ignored {
        require_operations_stable(state, share, &[&entry.path]).await?;
        let events: Vec<String> = sqlx::query_scalar(
            "SELECT event_json FROM peer_inbox WHERE share_name = ? AND status != 'done'",
        )
        .bind(&share.name)
        .fetch_all(&state.db)
        .await?;
        for raw in events {
            let event: PeerEvent = serde_json::from_str(&raw)?;
            if event.kind != SHARE_ENABLED && paths(&event).iter().any(|p| overlaps(p, &entry.path))
            {
                return Err(AppError::conflict(
                    "peer_dependency",
                    "inventory waits for an overlapping peer event",
                ));
            }
        }
        if has_open_conflict(state, &share.id, &entry.path).await? {
            return Err(AppError::conflict(
                "peer_conflict",
                "inventory waits for conflict resolution",
            ));
        }
        let root = files::root(&state.db, &share.root_id).await?;
        root.ensure_available()?;
        let event = PeerEvent {
            seq: 0,
            origin: peer.instance_id.clone().unwrap_or_default(),
            origin_seq: 0,
            share: share.name.clone(),
            kind: if entry.deleted {
                "file_deleted"
            } else {
                "file_changed"
            }
            .into(),
            data: EventData {
                path: entry.path.clone(),
                from: None,
                sha256: entry.sha256.clone(),
                size: Some(entry.size),
                mtime_ns: Some(entry.mtime_ns),
                vv: entry.vv.clone(),
            },
        };
        if matches!(
            apply_event(state, peer, share, &root, &event, &mut batch).await?,
            Applied::Stop
        ) {
            return Ok(false);
        }
        if has_open_conflict(state, &share.id, &entry.path).await? {
            return Err(AppError::conflict(
                "peer_conflict",
                "inventory waits for conflict resolution",
            ));
        }
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let accepted =
        insert_batch_in(&mut tx, peer, crate::crypto::new_id("peer-backfill"), batch).await?;
    if let Some(accepted) = &accepted {
        sqlx::query("UPDATE peer_backfill_inbox SET operation_id = ?, error = NULL WHERE peer_id = ? AND share_id = ? AND path = ?")
            .bind(&accepted.operation_id).bind(&peer.id).bind(&share.id).bind(&entry.path).execute(&mut *tx).await?;
    } else {
        sqlx::query(
            "DELETE FROM peer_backfill_inbox WHERE peer_id = ? AND share_id = ? AND path = ?",
        )
        .bind(&peer.id)
        .bind(&share.id)
        .bind(&entry.path)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    if let Some(accepted) = &accepted {
        crate::ops::announce_queued(state, accepted);
    }
    Ok(accepted.is_none() && !ignored)
}

pub(super) fn overlaps(a: &str, b: &str) -> bool {
    a.is_empty()
        || b.is_empty()
        || a == b
        || a.starts_with(&format!("{b}/"))
        || b.starts_with(&format!("{a}/"))
        || group_key(a) == group_key(b)
}

fn paths(event: &PeerEvent) -> Vec<&str> {
    [Some(event.data.path.as_str()), event.data.from.as_deref()]
        .into_iter()
        .flatten()
        .collect()
}

fn event_valid(event: &PeerEvent) -> AppResult<()> {
    let result = match event.kind.as_str() {
        SHARE_ENABLED => {
            if event.data != EventData::default() {
                Err("invalid peer share-enabled event".to_owned())
            } else {
                Ok(())
            }
        }
        "file_changed" => file_event_metadata(&event.data)
            .map(|_| ())
            .map_err(|e| e.message().to_owned()),
        "folder_created" => management_event_valid("folder_created", &event.data),
        "file_deleted" | "file_moved" | "folder_moved" | "folder_trashed" | FOLDER_PRUNED => {
            management_event_valid(&event.kind, &event.data)
        }
        other => Err(format!("unsupported peer event kind '{other}'")),
    };
    result.map_err(|message| AppError::bad_request("invalid_peer_event", message))
}

async fn validate_identity(
    tx: &mut sqlx::SqliteConnection,
    state: &AppState,
    event: &PeerEvent,
) -> AppResult<()> {
    let known: Option<(String, String, String)> = sqlx::query_as(
        "SELECT share_name, kind, data_json FROM peer_event_receipts WHERE origin = ? AND origin_seq = ?
         UNION ALL SELECT share_name, kind, data_json FROM event_log WHERE origin = ? AND origin_seq = ? LIMIT 1",
    )
    .bind(&event.origin)
    .bind(event.origin_seq)
    .bind(&event.origin)
    .bind(event.origin_seq)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((share, kind, data)) = known {
        let data: EventData = serde_json::from_str(&data)?;
        if share != event.share || kind != event.kind || data != event.data {
            return Err(AppError::conflict(
                "peer_event_identity",
                "peer event identity was reused for different content",
            ));
        }
    } else if event.origin == state.instance_id {
        return Err(AppError::conflict(
            "peer_event_identity",
            "peer event claims an unknown local event identity",
        ));
    }
    Ok(())
}

/// Validate the entire page and atomically persist it with its receipt watermark.
async fn admit(state: &AppState, peer: &net::Peer, after: i64, page: &EventPage) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_inbox WHERE peer_id = ?")
        .bind(&peer.id)
        .fetch_one(&mut *tx)
        .await?;
    if count + page.events.len() as i64 > INBOX_LIMIT {
        return Err(AppError::conflict(
            "peer_inbox_full",
            "peer inbox admission limit reached",
        ));
    }
    for event in &page.events {
        event_valid(event)?;
        validate_identity(&mut tx, state, event).await?;
        if serde_json::to_vec(event)?.len() > 65_536 {
            return Err(AppError::bad_request(
                "invalid_peer_event",
                "peer event exceeds the receipt size limit",
            ));
        }
        sqlx::query(
            "INSERT OR IGNORE INTO peer_event_receipts(origin, origin_seq, share_name, kind, data_json)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&event.origin)
        .bind(event.origin_seq)
        .bind(&event.share)
        .bind(&event.kind)
        .bind(serde_json::to_string(&event.data)?)
        .execute(&mut *tx)
        .await?;
        let older: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT peer_id, seq, event_json FROM peer_inbox
             WHERE share_name = ? AND status != 'done' ORDER BY peer_id, seq",
        )
        .bind(&event.share)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO peer_inbox(peer_id, seq, origin, origin_seq, share_name, event_json)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&peer.id)
        .bind(event.seq)
        .bind(&event.origin)
        .bind(event.origin_seq)
        .bind(&event.share)
        .bind(serde_json::to_string(event)?)
        .execute(&mut *tx)
        .await?;
        for (other_peer, seq, raw) in older {
            let previous: PeerEvent = serde_json::from_str(&raw)?;
            if previous.kind == SHARE_ENABLED || event.kind == SHARE_ENABLED {
                continue;
            }
            if previous.kind == "file_changed"
                && event.kind == "file_changed"
                && previous.data.path != event.data.path
            {
                continue;
            }
            if paths(event)
                .iter()
                .any(|a| paths(&previous).iter().any(|b| overlaps(a, b)))
            {
                sqlx::query(
                    "INSERT INTO peer_inbox_dependencies(peer_id, seq, depends_peer_id, depends_seq)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(&peer.id)
                .bind(event.seq)
                .bind(other_peer)
                .bind(seq)
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    let updated =
        sqlx::query("UPDATE peers SET received_cursor = ? WHERE id = ? AND received_cursor = ?")
            .bind(page.next)
            .bind(&peer.id)
            .bind(after)
            .execute(&mut *tx)
            .await?;
    if updated.rows_affected() != 1 {
        return Err(AppError::conflict(
            "peer_cursor_changed",
            "peer receipt cursor changed; retrying",
        ));
    }
    advance_in(&mut tx, &peer.id).await?;
    tx.commit().await?;
    Ok(())
}

/// Advance only the contiguous application watermark; receipt may be much further ahead.
async fn advance_in(tx: &mut sqlx::SqliteConnection, peer_id: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE peers SET cursor = MAX(cursor, MIN(received_cursor,
             COALESCE((SELECT MIN(seq) - 1 FROM peer_inbox WHERE peer_id = ? AND status != 'done'), received_cursor)))
         WHERE id = ?",
    )
    .bind(peer_id)
    .bind(peer_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM peer_inbox WHERE peer_id = ? AND status = 'done'
         AND seq <= (SELECT cursor FROM peers WHERE id = ?)",
    )
    .bind(peer_id)
    .bind(peer_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

pub(super) async fn done_in(
    tx: &mut sqlx::SqliteConnection,
    peer_id: &str,
    event: &PeerEvent,
    advertise: bool,
) -> AppResult<()> {
    if advertise {
        sqlx::query(
            "INSERT OR IGNORE INTO event_log(origin, origin_seq, share_name, kind, data_json, at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&event.origin)
        .bind(event.origin_seq)
        .bind(&event.share)
        .bind(&event.kind)
        .bind(serde_json::to_string(&event.data)?)
        .bind(db::now())
        .execute(&mut *tx)
        .await?;
    }
    let changed = sqlx::query(
        "UPDATE peer_inbox SET status = 'done', error = NULL WHERE peer_id = ? AND seq = ? AND status != 'done'",
    )
    .bind(peer_id)
    .bind(event.seq)
    .execute(&mut *tx)
    .await?;
    if advertise && changed.rows_affected() != 0 {
        sqlx::query("UPDATE peers SET events_applied = events_applied + 1 WHERE id = ?")
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
    }
    advance_in(tx, peer_id).await
}

pub(super) async fn finish_operation_in(
    tx: &mut sqlx::SqliteConnection,
    operation_id: &str,
) -> AppResult<()> {
    let verified: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM operation_items i JOIN operations o ON o.id = i.operation_id
             WHERE i.operation_id = ? AND i.status != 'done'
               AND NOT (i.status = 'recovery_required' AND o.recovery_resolved != 0))",
    ).bind(operation_id).fetch_one(&mut *tx).await?;
    if !verified {
        return Ok(());
    }
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pending_peer_ops WHERE operation_id = ?)
         OR EXISTS(SELECT 1 FROM pending_peer_events WHERE operation_id = ?)",
    )
    .bind(operation_id)
    .bind(operation_id)
    .fetch_one(&mut *tx)
    .await?;
    if pending {
        return Ok(());
    }
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT peer_id, event_json FROM peer_inbox WHERE operation_id = ? AND status != 'done'",
    )
    .bind(operation_id)
    .fetch_all(&mut *tx)
    .await?;
    for (peer_id, raw) in rows {
        let event: PeerEvent = serde_json::from_str(&raw)?;
        done_in(tx, &peer_id, &event, true).await?;
    }
    sqlx::query(
        "DELETE FROM peer_backfill_inbox WHERE operation_id = ? AND next_entry_json IS NULL",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE peer_backfill_inbox SET entry_json = next_entry_json, next_entry_json = NULL,
             operation_id = NULL, error = NULL WHERE operation_id = ?",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// Replay validated input on every poll, even when reception is unavailable or bounded.
pub(super) async fn pull(state: &AppState, peer: &net::Peer) -> Result<usize, String> {
    crate::activity::observe(
        state,
        format!("pull:{}", peer.id),
        "Peer event processing",
        &peer.name,
        Some("/peers"),
        |task| async move {
            pull_reporting(state, peer, &task)
                .await
                .map_err(|e| AppError::conflict("peer_events", crate::error::redact(&e)))
        },
    )
    .await
    .map_err(|e| e.message().to_owned())
}

async fn pull_reporting(
    state: &AppState,
    peer: &net::Peer,
    task: &crate::activity::Reporter,
) -> Result<usize, String> {
    task.phase("Fetching peer events and saving durable receipts");
    if peer.instance_id.is_none() {
        return Ok(0);
    }
    let after: i64 = sqlx::query_scalar("SELECT received_cursor FROM peers WHERE id = ?")
        .bind(&peer.id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| e.to_string())?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_inbox WHERE peer_id = ?")
        .bind(&peer.id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| e.to_string())?;
    let receipt = if count <= INBOX_LIMIT - EVENT_BATCH {
        match net::get_json::<EventPage>(state, peer, &format!("/api/peer/v1/events?after={after}"))
            .await
        {
            Ok(page) => match page.validate(after) {
                Ok(()) => admit(state, peer, after, &page)
                    .await
                    .map_err(|e| e.message().to_owned()),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    } else {
        Ok(())
    };
    task.waiting("Waiting for the replication-operation lock before applying events");
    let _guard = state.replication_operation_lock.lock().await;
    task.phase("Verifying and applying received events");
    let applied = replay(state, peer, task)
        .await
        .map_err(|e| e.message().to_owned())?;
    if applied > 0 {
        task.progress(applied as u64, None, "events applied");
        state.emit(ServerEvent::SyncChanged);
        Ok(applied)
    } else {
        receipt.map(|_| 0)
    }
}

async fn record_error(state: &AppState, peer_id: &str, seq: i64, error: &str) -> AppResult<bool> {
    let updated = sqlx::query(
        "UPDATE peer_inbox SET error = ? WHERE peer_id = ? AND seq = ? AND error IS NOT ?",
    )
    .bind(error)
    .bind(peer_id)
    .bind(seq)
    .bind(error)
    .execute(&state.db)
    .await?;
    Ok(updated.rows_affected() != 0)
}

async fn replay(
    state: &AppState,
    peer: &net::Peer,
    task: &crate::activity::Reporter,
) -> AppResult<usize> {
    // Rotate blocked rows fairly, but preserve dependencies explicitly rather than by cursor.
    let mut rows: Vec<(i64, String, Option<String>, String)> = sqlx::query_as(
        "SELECT seq, event_json, operation_id, status FROM peer_inbox
         WHERE peer_id = ? AND status != 'done'
         ORDER BY attempted_at IS NOT NULL, attempted_at, seq LIMIT ?",
    )
    .bind(&peer.id)
    .bind(EVENT_BATCH)
    .fetch_all(&state.db)
    .await?;
    rows.sort_by_key(|row| row.0);
    let all = shares(state).await?;
    let mut applied = 0;
    let mut first_error = None;
    let total = rows.len() as u64;
    for (done, (seq, raw, operation_id, status)) in rows.into_iter().enumerate() {
        task.progress(done as u64, Some(total), "events checked");
        sqlx::query("UPDATE peer_inbox SET attempted_at = ? WHERE peer_id = ? AND seq = ?")
            .bind(db::now())
            .bind(&peer.id)
            .bind(seq)
            .execute(&state.db)
            .await?;
        if let Some(operation_id) = operation_id {
            if let Err(error) = operation_finished(state, &operation_id, &[]).await {
                record_error(state, &peer.id, seq, error.message()).await?;
                if first_error.is_none() {
                    first_error = Some(error);
                }
            } else {
                let failed: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM operations WHERE id = ? AND status IN ('failed','partial','cancelled','recovery_required')",
                ).bind(&operation_id).fetch_optional(&state.db).await?;
                if let Some(status) = failed {
                    let error = AppError::conflict(
                        "recovery_required",
                        format!("peer operation {operation_id} needs attention ({status})"),
                    );
                    record_error(state, &peer.id, seq, error.message()).await?;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
            continue;
        }
        let event: PeerEvent = serde_json::from_str(&raw)?;
        if let Some(share) = all
            .iter()
            .find(|share| share.name == event.share && share.includes(&peer.id))
            && let Err(error) =
                super::admin::accept_dominating_decision(state, peer, share, &event).await
        {
            record_error(state, &peer.id, seq, error.message()).await?;
            if first_error.is_none() {
                first_error = Some(error);
            }
            continue;
        }
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM peer_inbox_dependencies d
             JOIN peer_inbox b ON b.peer_id = d.depends_peer_id AND b.seq = d.depends_seq
             WHERE d.peer_id = ? AND d.seq = ? AND b.status != 'done')",
        )
        .bind(&peer.id)
        .bind(seq)
        .fetch_one(&state.db)
        .await?;
        if blocked {
            continue;
        }
        let result = if status == "awaiting_content" {
            verify_content(state, peer, &all, &event).await
        } else {
            apply_received(state, peer, &all, &event).await
        };
        match result {
            Ok(done) => applied += usize::from(done),
            Err(error) => {
                if record_error(state, &peer.id, seq, error.message()).await? {
                    tracing::warn!(peer = %peer.name, path = %event.data.path, "peer effect deferred: {}", error.message());
                }
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if applied == 0
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(applied)
}

async fn apply_received(
    state: &AppState,
    peer: &net::Peer,
    all: &[Share],
    event: &PeerEvent,
) -> AppResult<bool> {
    let known: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM event_log WHERE origin = ? AND origin_seq = ?)
         OR EXISTS(SELECT 1 FROM peer_decision_receipts WHERE origin=? AND origin_seq=? AND share_name=?)",
    )
    .bind(&event.origin)
    .bind(event.origin_seq)
    .bind(&event.origin)
    .bind(event.origin_seq)
    .bind(&event.share)
    .fetch_one(&state.db)
    .await?;
    let share = all
        .iter()
        .find(|s| s.name == event.share && s.includes(&peer.id));
    let advertise = !known && share.is_some_and(|s| !event.data.excluded(&s.junk));
    if !advertise {
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        done_in(&mut tx, &peer.id, event, false).await?;
        tx.commit().await?;
        return Ok(false);
    }
    let share = share.ok_or_else(|| AppError::not_found("peer share changed"))?;
    require_operations_stable(state, share, &paths(event)).await?;
    if event.kind == FOLDER_PRUNED {
        return Err(folder_pruned(peer, share, event));
    }
    if let Some(resolution) = conflict_decision(state, share, event).await? {
        if matches!(resolution.as_str(), "keep_local" | "skip") {
            let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
            done_in(&mut tx, &peer.id, event, false).await?;
            tx.commit().await?;
            return Ok(true);
        }
        if event.kind == "file_changed" {
            sqlx::query("UPDATE peer_inbox SET status = 'awaiting_content', error = NULL WHERE peer_id = ? AND seq = ?")
                .bind(&peer.id).bind(event.seq).execute(&state.db).await?;
            return verify_content(state, peer, all, event).await;
        }
    }
    if event.kind == SHARE_ENABLED {
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        mark_backfill_in(&mut tx, &share.id, &peer.id).await?;
        done_in(&mut tx, &peer.id, event, true).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let mut batch = Batch {
        actions: Vec::new(),
        pending: Vec::new(),
        paths: HashSet::new(),
    };
    if event.kind == "folder_created" {
        let abs = root.abs(&share.root_rel(&event.data.path)?)?;
        match abs.symlink_metadata() {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(AppError::conflict(
                    "peer_folder_exists",
                    "peer folder destination is not a directory",
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                queue_folder_event(state, peer, share, event, peer.cursor).await?;
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
    } else if has_open_conflict(state, &share.id, &event.data.path).await? {
        if event.kind.starts_with("file_") {
            conflict(
                state,
                ConflictInput {
                    share,
                    rel: &event.data.path,
                    kind: "changed_on_both",
                    detail: format!("{} changed {} again", peer.name, event.data.path),
                    remote: remote_side(&peer.name, &event.data, event.kind == "file_deleted"),
                    remote_vv: &event.data.vv,
                    peer_id: &peer.id,
                    from: None,
                },
            )
            .await?;
        }
        // A conflict is not an applied filesystem effect and must not be advertised as one.
        return Err(AppError::conflict(
            "peer_conflict",
            "peer effect waits for conflict resolution",
        ));
    } else {
        if matches!(
            apply_event(state, peer, share, &root, event, &mut batch).await?,
            Applied::Stop
        ) {
            return Ok(false);
        }
        if has_open_conflict(state, &share.id, &event.data.path).await? {
            return Err(AppError::conflict(
                "peer_conflict",
                "peer effect waits for conflict resolution",
            ));
        }
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let accepted = insert_batch_in(
        &mut tx,
        peer,
        format!("peer:{}:{}", event.origin, event.origin_seq),
        batch,
    )
    .await?;
    if let Some(accepted) = &accepted {
        sqlx::query("UPDATE peer_inbox SET status = 'applying', operation_id = ?, error = NULL WHERE peer_id = ? AND seq = ?")
            .bind(&accepted.operation_id).bind(&peer.id).bind(event.seq).execute(&mut *tx).await?;
    } else if matches!(event.kind.as_str(), "file_changed" | "file_moved")
        && sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM transfers WHERE share_id = ? AND rel_path = ?)",
        )
        .bind(&share.id)
        .bind(&event.data.path)
        .fetch_one(&mut *tx)
        .await?
    {
        sqlx::query(
            "UPDATE peer_inbox SET status = 'awaiting_content', error = NULL WHERE peer_id = ? AND seq = ?",
        ).bind(&peer.id).bind(event.seq).execute(&mut *tx).await?;
    } else {
        done_in(&mut tx, &peer.id, event, true).await?;
    }
    tx.commit().await?;
    if let Some(accepted) = &accepted {
        crate::ops::announce_queued(state, accepted);
    }
    Ok(accepted.is_none())
}

/// Receiver-only compatibility: a peer prune is retained, never applied or acknowledged.
/// Nothing here deletes or advertises it, so it stays pending (path-scoped, holding later
/// overlapping events) until a version that can handle and propagate prunes replays it,
/// even when the folder is already absent here and only transitive peers still have it.
fn folder_pruned(peer: &net::Peer, share: &Share, event: &PeerEvent) -> AppError {
    AppError::conflict(
        "peer_folder_prune_unsupported",
        format!(
            "{} removed folder '{}' in share '{}'. This folder change needs support in a future \
             ISM update. Other folders can still sync.",
            peer.name, event.data.path, share.name
        ),
    )
}

async fn conflict_decision(
    state: &AppState,
    share: &Share,
    event: &PeerEvent,
) -> AppResult<Option<String>> {
    if event.kind.starts_with("folder_") {
        return super::admin::event_decision(state, share, event).await;
    }
    if let Some(decision) = super::admin::content_decision(state, share, event).await? {
        return Ok(Some(decision));
    }
    let rows: Vec<(Option<String>, String)> = sqlx::query_as(
        "SELECT resolution, remote_vv_json FROM conflicts WHERE share_id = ? AND rel_path = ? AND status != 'open' ORDER BY created_at DESC",
    ).bind(&share.id).bind(&event.data.path).fetch_all(&state.db).await?;
    for (resolution, raw) in rows {
        let vv: VersionVector = serde_json::from_str(&raw)?;
        if matches!(
            event.data.vv.compare(&vv),
            Causality::Equal | Causality::DominatedBy
        ) {
            return Ok(resolution);
        }
    }
    Ok(None)
}

async fn verify_content(
    state: &AppState,
    peer: &net::Peer,
    all: &[Share],
    event: &PeerEvent,
) -> AppResult<bool> {
    let Some(share) = all
        .iter()
        .find(|s| s.name == event.share && s.includes(&peer.id))
    else {
        return Ok(false);
    };
    let mut current = event.data.path.clone();
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let original_present = match root.abs(&share.root_rel(&current)?)?.symlink_metadata() {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let later: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM peer_inbox WHERE peer_id = ? AND seq > ? AND share_name = ? ORDER BY seq",
    ).bind(&peer.id).bind(event.seq).bind(&event.share).fetch_all(&state.db).await?;
    for raw in later {
        if original_present {
            break;
        }
        let movement: PeerEvent = serde_json::from_str(&raw)?;
        if let Some(from) = &movement.data.from {
            if movement.kind == "file_moved" && current == *from {
                current = movement.data.path;
            } else if movement.kind == "folder_moved" && current.starts_with(&format!("{from}/")) {
                current = format!("{}{}", movement.data.path, &current[from.len()..]);
            }
        }
    }
    require_operations_stable(state, share, &[&current]).await?;
    match root.abs(&share.root_rel(&current)?)?.metadata() {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Err(AppError::conflict(
                "wrong_kind",
                "peer content is not a regular file",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let Some(row) = super::row(state, &share.id, &current).await? else {
                return Ok(false);
            };
            if !row.deleted || event.data.vv.compare(&row.vv) != Causality::DominatedBy {
                return Ok(false);
            }
            let rel = share.root_rel(&current)?;
            crate::ops::exec::verified_file_absence_at_root(state, &root, &rel).await?;
            let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
            if super::row_in(&mut tx, &share.id, &current).await?.as_ref() != Some(&row)
                || root.abs(&rel)?.symlink_metadata().is_ok()
            {
                return Err(AppError::conflict(
                    "recovery_changed",
                    "superseding deletion changed during acknowledgment",
                ));
            }
            done_in(&mut tx, &peer.id, event, false).await?;
            tx.commit().await?;
            return Ok(true);
        }
        Err(error) => return Err(error.into()),
    }
    let Some(row) = register_local(state, share, &root, &current).await? else {
        return Ok(false);
    };
    if event.data.vv.compare(&row.vv) == Causality::DominatedBy
        && (row.deleted || row.sha256 != event.data.sha256)
    {
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        done_in(&mut tx, &peer.id, event, false).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let verified = !row.deleted
        && row.sha256 == event.data.sha256
        && event.data.size.and_then(|s| i64::try_from(s).ok()) == Some(row.size)
        && matches!(
            event.data.vv.compare(&row.vv),
            Causality::Equal | Causality::DominatedBy
        );
    if !verified {
        return Ok(false);
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    done_in(&mut tx, &peer.id, event, true).await?;
    tx.commit().await?;
    Ok(true)
}

fn collect_locations(value: &Value, out: &mut Vec<Location>) -> AppResult<()> {
    if let Some(root_id) = value.get("root_id").and_then(Value::as_str)
        && let Some(path) = value.get("path").and_then(Value::as_str)
    {
        files::parse_rel(path)?;
        out.push(Location {
            root_id: root_id.to_owned(),
            path: path.to_owned(),
        });
    }
    match value {
        Value::Object(fields) => {
            for child in fields.values() {
                collect_locations(child, out)?;
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_locations(child, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn share_path(share: &Share, location: &Location) -> Option<String> {
    if location.root_id != share.root_id {
        return None;
    }
    let base = share.path.as_str();
    if location.path == base
        || base.starts_with(&format!("{}/", location.path))
        || location.path.is_empty()
    {
        Some(String::new())
    } else if base.is_empty() {
        Some(location.path.clone())
    } else {
        location
            .path
            .strip_prefix(&format!("{base}/"))
            .map(str::to_owned)
    }
}

/// Failed operations retain only their source/target/companion footprint. An unresolved
/// admitted maintenance barrier additionally protects its server, never every server.
async fn operation_footprints(
    state: &AppState,
    share: &Share,
    recovery_parents: &[String],
) -> AppResult<Vec<String>> {
    operation_footprints_for(state, share, recovery_parents, None).await
}

async fn operation_footprints_for(
    state: &AppState,
    share: &Share,
    recovery_parents: &[String],
    staging_parent: Option<&str>,
) -> AppResult<Vec<String>> {
    let server: Option<String> =
        sqlx::query_scalar("SELECT server_id FROM mappings WHERE root_id = ?")
            .bind(&share.root_id)
            .fetch_one(&state.db)
            .await?;
    let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "SELECT id, request_json, admitted_server_ids_json, server_ids_json,
                CASE WHEN recovery_resolved = 0 AND status IN ('partial','recovery_required') THEN 1 ELSE 0 END
         FROM operations o WHERE
             (recovery_resolved = 0 AND status IN ('queued','failed','cancelled','partial','recovery_required'))
             OR EXISTS(SELECT 1 FROM pending_peer_ops WHERE operation_id = o.id)
             OR EXISTS(SELECT 1 FROM pending_peer_events WHERE operation_id = o.id)",
    ).fetch_all(&state.db).await?;
    let mut blocked = Vec::new();
    for (id, request, admitted, planned_servers, barrier) in rows {
        // This exception only admits transport/staging for a manifest-verified in-parent
        // restoration. The executor still owns and verifies the original held write barrier.
        if staging_parent == Some(id.as_str()) {
            continue;
        }
        if barrier == 0 && recovery_parents.contains(&id) {
            continue;
        }
        let request: Value = serde_json::from_str(&request)?;
        let mut locations = Vec::new();
        collect_locations(&request, &mut locations)?;
        if let Some(actions) = request.get("actions").and_then(Value::as_array) {
            for action in actions {
                if let Some(upload_id) = action.get("upload_id").and_then(Value::as_str) {
                    let raw: String =
                        sqlx::query_scalar("SELECT request_json FROM uploads WHERE id = ?")
                            .bind(upload_id)
                            .fetch_one(&state.db)
                            .await?;
                    let upload: Value = serde_json::from_str(&raw)?;
                    let target: Location = serde_json::from_value(
                        upload.get("target").cloned().ok_or_else(|| {
                            AppError::conflict(
                                "recovery_required",
                                "upload has no verifiable target",
                            )
                        })?,
                    )?;
                    let specs = upload
                        .get("files")
                        .and_then(Value::as_array)
                        .ok_or_else(|| {
                            AppError::conflict(
                                "recovery_required",
                                "upload has no verifiable files",
                            )
                        })?;
                    for spec in specs.iter().chain(upload.get("existing_original")) {
                        if let Some(name) = spec.get("name").and_then(Value::as_str) {
                            let path = if target.path.is_empty() {
                                name.to_owned()
                            } else {
                                format!("{}/{name}", target.path)
                            };
                            files::parse_rel(&path)?;
                            locations.push(Location {
                                root_id: target.root_id.clone(),
                                path,
                            });
                        }
                    }
                }
                if let Some(trash_id) = action.get("trash_id").and_then(Value::as_str) {
                    let (root_id, path): (String, String) =
                        sqlx::query_as("SELECT root_id, path FROM trash WHERE id = ?")
                            .bind(trash_id)
                            .fetch_one(&state.db)
                            .await?;
                    locations.push(Location { root_id, path });
                }
            }
        }
        // Malformed persisted requests must not silently lose their safety footprint.
        if locations.is_empty() {
            return Err(AppError::conflict(
                "recovery_required",
                format!("operation {id} has an unverifiable footprint"),
            ));
        }
        let servers: Vec<String> = match serde_json::from_str(&admitted) {
            Ok(servers) => servers,
            Err(_) => {
                let planned: Vec<String> = serde_json::from_str(&planned_servers)?;
                let mut affected = locations.iter().any(|loc| loc.root_id == share.root_id)
                    || server.as_ref().is_some_and(|s| planned.contains(s));
                for location in &locations {
                    let linked: Option<String> =
                        sqlx::query_scalar("SELECT server_id FROM mappings WHERE root_id = ?")
                            .bind(&location.root_id)
                            .fetch_optional(&state.db)
                            .await?
                            .flatten();
                    affected |= server.is_some() && linked == server;
                }
                if affected {
                    return Err(AppError::conflict(
                        "recovery_required",
                        format!("operation {id} has unverifiable admitted servers"),
                    ));
                }
                continue;
            }
        };
        if barrier != 0 && server.as_ref().is_some_and(|s| servers.contains(s)) {
            blocked.push(String::new());
            continue;
        }
        blocked.extend(locations.iter().filter_map(|loc| share_path(share, loc)));
    }
    Ok(blocked)
}

pub(crate) async fn require_no_active_operations(state: &AppState) -> AppResult<()> {
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE status IN ('running','waiting_maintenance'))",
    )
    .fetch_one(&state.db)
    .await?;
    if active {
        return Err(AppError::conflict(
            "recovery_required",
            "peer bookkeeping waits for active managed operations",
        ));
    }
    Ok(())
}

pub(super) async fn require_operations_stable(
    state: &AppState,
    share: &Share,
    paths: &[&str],
) -> AppResult<()> {
    require_operations_stable_for(state, share, paths, &[]).await
}

async fn require_operations_stable_for(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    recovery_parents: &[String],
) -> AppResult<()> {
    require_operations_stable_except(state, share, paths, recovery_parents, &[]).await
}

pub(super) async fn require_move_choice_stable(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    conflict_id: &str,
) -> AppResult<()> {
    let retained = retained_choice_transfers(state, share, conflict_id).await?;
    require_operations_stable_except(state, share, paths, &[], &retained).await
}

pub(super) async fn retained_choice_transfers(
    state: &AppState,
    share: &Share,
    conflict_id: &str,
) -> AppResult<Vec<String>> {
    sqlx::query_scalar(
        "SELECT t.id FROM transfers t JOIN pending_peer_ops p ON p.share_id=t.share_id AND p.peer_id=t.peer_id
         WHERE p.kind='move_decision_open' AND p.conflict_id=? AND p.share_id=?
         AND t.status='cancelled' AND t.error='choice evidence changed; bytes retained'
         AND (EXISTS(SELECT 1 FROM json_each(p.vv_json,'$.files') f
           WHERE json_extract(f.value,'$.download')=1 AND json_extract(f.value,'$.path')=t.rel_path
           AND json_extract(f.value,'$.sha256')=t.sha256 AND json_extract(f.value,'$.size')=t.size)
           OR EXISTS(SELECT 1 FROM json_each(p.vv_json,'$.retained_transfers') r WHERE r.value=t.id))",
    ).bind(conflict_id).bind(&share.id).fetch_all(&state.db).await.map_err(Into::into)
}

pub(super) async fn require_operations_stable_retained(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    retained: &[String],
) -> AppResult<()> {
    require_operations_stable_except(state, share, paths, &[], retained).await
}

async fn require_operations_stable_except(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    recovery_parents: &[String],
    retained_transfers: &[String],
) -> AppResult<()> {
    require_no_active_operations(state).await?;
    let blocked = operation_footprints(state, share, recovery_parents).await?;
    if blocked
        .iter()
        .any(|b| paths.is_empty() || paths.iter().any(|p| overlaps(p, b)))
    {
        return Err(AppError::conflict(
            "recovery_required",
            "peer footprint waits for unresolved managed operations",
        ));
    }
    require_unresolved_transfers_except(state, share, paths, retained_transfers).await
}

async fn require_unresolved_transfers(
    state: &AppState,
    share: &Share,
    paths: &[&str],
) -> AppResult<()> {
    require_unresolved_transfers_except(state, share, paths, &[]).await
}

async fn require_unresolved_transfers_except(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    retained: &[String],
) -> AppResult<()> {
    let failed: Vec<(String, String)> = sqlx::query_as(
        "SELECT id,rel_path FROM transfers WHERE share_id = ? AND status IN ('failed','cancelled')",
    )
    .bind(&share.id)
    .fetch_all(&state.db)
    .await?;
    if failed.iter().any(|(id, b)| {
        !retained.contains(id)
            && (paths.is_empty() || paths.iter().any(|p| *p != b && overlaps(p, b)))
    }) {
        return Err(AppError::conflict(
            "recovery_required",
            "peer footprint waits for a failed companion transfer",
        ));
    }
    Ok(())
}

/// Uncertain paths for scans, including inbox effects and queued downloads. An empty path
/// means the whole share is protected. Scanners must skip deletion inference there too.
pub async fn blocked_footprints(state: &AppState, share: &Share) -> AppResult<Vec<String>> {
    let mut blocked = operation_footprints(state, share, &[]).await?;
    let events: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM peer_inbox WHERE share_name = ? AND status != 'done'",
    )
    .bind(&share.name)
    .fetch_all(&state.db)
    .await?;
    for raw in events {
        let event: PeerEvent = serde_json::from_str(&raw)?;
        if conflict_decision(state, share, &event).await?.is_some() {
            continue;
        }
        if event.kind != SHARE_ENABLED {
            blocked.extend(paths(&event).into_iter().map(str::to_owned));
        }
    }
    let transfers: Vec<String> = sqlx::query_scalar(
        "SELECT rel_path FROM transfers WHERE share_id = ? AND status IN ('queued','running','failed','cancelled')",
    ).bind(&share.id).fetch_all(&state.db).await?;
    blocked.extend(transfers);
    let inventory: Vec<String> =
        sqlx::query_scalar("SELECT path FROM peer_backfill_inbox WHERE share_id = ?")
            .bind(&share.id)
            .fetch_all(&state.db)
            .await?;
    blocked.extend(inventory);
    Ok(blocked)
}

/// Scoped counterpart to the legacy global failure gate, for transfer publication.
pub async fn require_stable_share(
    state: &AppState,
    share: &Share,
    paths: &[&str],
) -> AppResult<()> {
    require_stable_share_for(state, share, paths, &[]).await
}

pub(crate) async fn require_stable_transfer(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    transfer_id: &str,
) -> AppResult<()> {
    if let Some(parent) = super::admin::restore_transfer_parent(state, transfer_id).await? {
        require_no_active_operations(state).await?;
        let blocked = operation_footprints_for(state, share, &[], Some(&parent)).await?;
        if blocked
            .iter()
            .any(|b| paths.is_empty() || paths.iter().any(|p| overlaps(p, b)))
        {
            return Err(AppError::conflict(
                "recovery_required",
                "restoration transport waits for another operation",
            ));
        }
        return require_stable_share_effects(state, share, paths, &[parent]).await;
    }
    let parents: Vec<String> = sqlx::query_scalar(
        "WITH RECURSIVE ancestors(kind,target_id) AS (
            SELECT r.kind,r.target_id FROM sync_recovery_links l
            JOIN sync_recovery r ON r.id=l.recovery_id WHERE l.kind='transfer' AND l.target_id=?
            UNION
            SELECT r.kind,r.target_id FROM sync_recovery_links l
            JOIN sync_recovery r ON r.id=l.recovery_id
            JOIN ancestors a ON a.kind=l.kind AND a.target_id=l.target_id
        ) SELECT target_id FROM ancestors WHERE kind='operation'",
    )
    .bind(transfer_id)
    .fetch_all(&state.db)
    .await?;
    if parents.is_empty() {
        return super::require_stable_share(state, share, paths).await;
    }
    require_stable_share_for(state, share, paths, &parents).await
}

async fn require_stable_share_for(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    recovery_parents: &[String],
) -> AppResult<()> {
    require_operations_stable_for(state, share, paths, recovery_parents).await?;
    require_stable_share_effects(state, share, paths, recovery_parents).await
}

async fn require_stable_share_effects(
    state: &AppState,
    share: &Share,
    paths: &[&str],
    recovery_parents: &[String],
) -> AppResult<()> {
    files::root(&state.db, &share.root_id)
        .await?
        .ensure_available()?;
    let events: Vec<(String,Option<String>)> = sqlx::query_as(
        "SELECT event_json,operation_id FROM peer_inbox WHERE share_name = ? AND status NOT IN ('done','awaiting_content')
         AND (error IS NOT NULL OR operation_id IS NOT NULL)
         AND NOT EXISTS(SELECT 1 FROM peer_inbox_dependencies d JOIN peer_inbox b
             ON b.peer_id = d.depends_peer_id AND b.seq = d.depends_seq
             WHERE d.peer_id = peer_inbox.peer_id AND d.seq = peer_inbox.seq AND b.status = 'awaiting_content')",
    )
    .bind(&share.name)
    .fetch_all(&state.db)
    .await?;
    for (raw, operation) in events {
        if operation
            .as_ref()
            .is_some_and(|id| recovery_parents.contains(id))
        {
            continue;
        }
        require_unresolved_transfers(state, share, paths).await?;
        let event: PeerEvent = serde_json::from_str(&raw)?;
        if conflict_decision(state, share, &event).await?.is_some() {
            continue;
        }
        if event.kind != SHARE_ENABLED
            && (paths.is_empty()
                || paths
                    .iter()
                    .any(|p| self::paths(&event).iter().any(|b| overlaps(p, b))))
        {
            return Err(AppError::conflict(
                "recovery_required",
                "peer publication waits for a dependent inbox effect",
            ));
        }
    }
    Ok(())
}

pub(super) async fn processing_error(state: &AppState, peer_id: &str) -> AppResult<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT error FROM peer_inbox WHERE peer_id = ? AND status != 'done' AND error IS NOT NULL
         UNION ALL SELECT error FROM peer_backfill_inbox WHERE peer_id = ? AND error IS NOT NULL LIMIT 1",
    ).bind(peer_id).bind(peer_id).fetch_optional(&state.db).await?)
}
