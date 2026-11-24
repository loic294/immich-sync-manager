//! Paired real-HTTP notification clients, with controllable endpoint compatibility/failure.

use super::*;
use crate::peers::notifications::Snapshot;
use crate::peers::{activity, net};
use std::sync::atomic::Ordering;

const NOTIFICATIONS: &str = "/api/peer/v1/notifications";
const RECOVERY: &str = "/api/peer/v1/recovery";
const PROGRESS: &str = "/api/peer/v1/sync-activity";
const ATTENTION: &str = "/api/peer/v1/attention";

fn count(instance: &Instance, path: &str) -> usize {
    instance
        .requests
        .lock()
        .unwrap()
        .get(path)
        .copied()
        .unwrap_or(0)
}

async fn wait<F: std::future::Future<Output = bool>>(what: &str, mut check: impl FnMut() -> F) {
    tokio::time::timeout(Duration::from_secs(9), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {what}"));
}

async fn fixture(status: u16) -> (Instance, Instance, net::Peer) {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    b.notification_status.store(status, Ordering::SeqCst);
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    for table in ["peer_sync_state", "peer_recovery_state"] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
            .execute(&a.state.db)
            .await
            .unwrap();
    }
    a.requests.lock().unwrap().clear();
    b.requests.lock().unwrap().clear();
    a.tasks.push(tokio::spawn(activity::run(a.state.clone())));
    a.tasks
        .push(tokio::spawn(crate::activity::run(a.state.clone())));
    (a, b, peer)
}

async fn ready(a: &Instance, peer: &net::Peer) -> bool {
    let snapshots = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE peer_id=? AND error IS NULL)
         AND EXISTS(SELECT 1 FROM peer_recovery_state WHERE peer_id=? AND error IS NULL)",
    )
    .bind(&peer.id)
    .bind(&peer.id)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    snapshots
        && [Snapshot::Activity, Snapshot::Recovery]
            .into_iter()
            .all(|snapshot| {
                a.state
                    .peer_streams
                    .fresh(&peer.id, snapshot)
                    .unwrap_or(true)
            })
}

async fn age_snapshots(a: &Instance) {
    for table in ["peer_sync_state", "peer_recovery_state"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {table} SET updated_at='2000-01-01T00:00:00Z'"
        )))
        .execute(&a.state.db)
        .await
        .unwrap();
    }
}

async fn failed_remote_operation(b: &Instance) {
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES('heartbeat-example','heartbeat',?,'peer','{}','failed',?)",
    )
    .bind(format!("peer:{}", peer.id))
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    crate::peers::recovery::discover(&b.state).await.unwrap();
}

async fn remote_mapping(a: &Instance, peer: &net::Peer) -> (bool, bool) {
    let snapshot = crate::peers::attention::snapshot(&a.state).await.unwrap();
    let blocked = snapshot.items.iter().any(|item| {
        item.code == ism_core::dto::AttentionCode::MappingUnavailable
            && item.peer_id.as_deref() == Some(&peer.id)
            && item.blocks_work
    });
    let complete = snapshot.coverage.iter().any(|source| {
        source.code == ism_core::dto::AttentionCode::MappingUnavailable
            && source.peer_id.as_deref() == Some(&peer.id)
            && source.completeness == ism_core::dto::AttentionCompleteness::Complete
    });
    (blocked, complete)
}

#[tokio::test]
async fn notification_client_attention_hold_does_not_block_updates_or_publish_stale_evidence() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    std::fs::write(b.shared.parent().unwrap().join(".ism-root"), "wrong-root").unwrap();
    b.state.emit(crate::state::ServerEvent::WatchChanged);
    wait("remote blocker", || async {
        remote_mapping(&a, &peer).await == (true, true)
    })
    .await;

    std::fs::write(b.shared.parent().unwrap().join(".ism-root"), "photos").unwrap();
    b.attention_hold.armed.store(true, Ordering::SeqCst);
    b.state.emit(crate::state::ServerEvent::WatchChanged);
    tokio::time::timeout(Duration::from_secs(9), b.attention_hold.entered.notified())
        .await
        .expect("first attention response is held");
    std::fs::write(b.shared.parent().unwrap().join(".ism-root"), "wrong-root").unwrap();
    let progress = count(&b, PROGRESS);
    let recovery = count(&b, RECOVERY);
    b.state.emit(crate::state::ServerEvent::WatchChanged);
    failed_remote_operation(&b).await;
    wait("activity and recovery while attention is held", || async {
        count(&b, PROGRESS) > progress
            && count(&b, RECOVERY) > recovery
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM outgoing_recovery WHERE target_id='heartbeat-example')",
            )
            .fetch_one(&a.state.db)
            .await
            .unwrap()
    })
    .await;
    assert_eq!(remote_mapping(&a, &peer).await, (false, false));

    b.attention_hold.armed.store(true, Ordering::SeqCst);
    b.attention_hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(9), b.attention_hold.entered.notified())
        .await
        .expect("new hint schedules a fresh response");
    assert_eq!(
        remote_mapping(&a, &peer).await,
        (false, false),
        "the old clear response must not become authoritative"
    );
    b.attention_hold.release.notify_one();
    wait("fresh blocker", || async {
        remote_mapping(&a, &peer).await == (true, true)
    })
    .await;
}

#[tokio::test]
async fn notification_client_attention_only_traffic_keeps_snapshots_fresh_without_writes() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    failed_remote_operation(&b).await;
    wait("recovery snapshot", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM outgoing_recovery WHERE target_id='heartbeat-example')",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    wait("receiver snapshots verified", || ready(&a, &peer)).await;
    wait("initial attention", || async {
        remote_mapping(&a, &peer).await.1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    age_snapshots(&a).await;
    audit_writes(&a).await;
    let before = (
        count(&b, ATTENTION),
        count(&b, PROGRESS),
        count(&b, RECOVERY),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(31);
    while tokio::time::Instant::now() < deadline {
        b.state.emit(crate::state::ServerEvent::WatchChanged);
        b.state.emit(crate::state::ServerEvent::MaintenanceChanged {
            server_id: "example".into(),
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            cache_available(&a, &peer).await,
            (true, true),
            "sustained attention-only frames must keep activity and recovery fresh"
        );
    }
    assert!(count(&b, ATTENTION) > before.0);
    assert_eq!(count(&b, NOTIFICATIONS), 1, "stream remains connected");
    wait("attention current", || async {
        remote_mapping(&a, &peer).await.1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        (count(&b, PROGRESS), count(&b, RECOVERY)),
        (before.1, before.2)
    );
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM write_audit")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(
        writes, 0,
        "attention-only changes must not rewrite sync or recovery"
    );
    b.state.emit(crate::state::ServerEvent::SyncChanged);
    wait("ordinary refresh still works", || async {
        count(&b, PROGRESS) > before.1 && count(&b, RECOVERY) > before.2
    })
    .await;
}

async fn audit_writes(a: &Instance) {
    sqlx::query("CREATE TABLE write_audit (table_name TEXT NOT NULL, action TEXT NOT NULL)")
        .execute(&a.state.db)
        .await
        .unwrap();
    for table in [
        "peer_sync_state",
        "peer_recovery_state",
        "outgoing_sync",
        "outgoing_recovery",
    ] {
        for action in ["INSERT", "UPDATE", "DELETE"] {
            let statement = format!(
                "CREATE TRIGGER audit_{table}_{action} AFTER {action} ON {table} \
                 BEGIN INSERT INTO write_audit VALUES ('{table}','{action}'); END"
            );
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&a.state.db)
                .await
                .unwrap();
        }
    }
}

async fn take_writes(a: &Instance) -> Vec<(String, String)> {
    let writes = sqlx::query_as("SELECT table_name,action FROM write_audit ORDER BY rowid")
        .fetch_all(&a.state.db)
        .await
        .unwrap();
    sqlx::query("DELETE FROM write_audit")
        .execute(&a.state.db)
        .await
        .unwrap();
    writes
}

async fn cache_available(a: &Instance, peer: &net::Peer) -> (bool, bool) {
    let progress = activity::page(&a.state, "active", 100, 0, None)
        .await
        .unwrap();
    let recovery = crate::peers::recovery::outgoing(
        &a.state,
        &peer.id,
        ism_core::dto::RecoveryKind::Operation,
        "heartbeat-example",
    )
    .await
    .unwrap()
    .unwrap();
    let list = crate::peers::recovery::page(&a.state, 100, 0)
        .await
        .unwrap();
    (
        progress.warnings.is_empty(),
        recovery.code.as_deref() != Some("peer_recovery_unavailable")
            && list.rows.iter().any(|row| {
                row.target.id == "heartbeat-example"
                    && row.code.as_deref() != Some("peer_recovery_unavailable")
            }),
    )
}

#[tokio::test]
async fn notification_client_healthy_stream_keeps_old_snapshots_available() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    failed_remote_operation(&b).await;
    wait("recovery snapshot", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM outgoing_recovery WHERE target_id='heartbeat-example')",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    age_snapshots(&a).await;
    let conflict = serde_json::json!({
        "target": { "kind": "conflict", "id": "heartbeat-conflict", "peer_id": null },
        "title": "example.jpg", "state": "needs_decision", "code": "choose_version",
        "detail": null, "can_fix": false, "can_dismiss": false, "replacement_operation_id": null
    });
    sqlx::query("INSERT INTO outgoing_recovery(peer_id,kind,target_id,view_json) VALUES (?, 'conflict', 'heartbeat-conflict', ?)")
        .bind(&peer.id)
        .bind(conflict.to_string())
        .execute(&a.state.db)
        .await
        .unwrap();
    let conflicts = crate::peers::admin::page_conflicts(&a.state, false, 50, 0)
        .await
        .unwrap();
    assert!(matches!(
        conflicts.rows.first(),
        Some(ism_core::dto::ConflictPageRow::Peer(item))
            if item.target.id == "heartbeat-conflict"
                && item.code.as_deref() == Some("choose_version")
    ));
    assert_eq!(
        count(&b, NOTIFICATIONS),
        1,
        "authenticated stream is still connected"
    );
    assert_eq!(
        cache_available(&a, &peer).await,
        (true, true),
        "healthy idle stream must not expire progress or recovery solely by snapshot age"
    );
    let baseline = (count(&b, RECOVERY), count(&b, PROGRESS));
    let history = crate::activity::page(&a.state, 100, 0)
        .await
        .unwrap()
        .history
        .len();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(
        cache_available(&a, &peer).await,
        (true, true),
        "live keepalives extend freshness"
    );
    assert_eq!((count(&b, RECOVERY), count(&b, PROGRESS)), baseline);
    assert_eq!(
        crate::activity::page(&a.state, 100, 0)
            .await
            .unwrap()
            .history
            .len(),
        history
    );
    for table in ["peer_sync_state", "peer_recovery_state"] {
        let timestamp: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT updated_at FROM {table} WHERE peer_id=?"
        )))
        .bind(&peer.id)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        assert_eq!(
            timestamp, "2000-01-01T00:00:00Z",
            "keepalives must not write heartbeat timestamps"
        );
    }
}

#[tokio::test]
async fn notification_client_failed_fetch_or_connection_never_extends_freshness() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    failed_remote_operation(&b).await;
    wait("receiver evidence", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM outgoing_recovery WHERE target_id='heartbeat-example')",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    wait("receiver snapshots verified", || ready(&a, &peer)).await;
    age_snapshots(&a).await;
    assert_eq!(cache_available(&a, &peer).await, (true, true));

    b.snapshot_status.store(500, Ordering::SeqCst);
    b.state.emit(crate::state::ServerEvent::SyncChanged);
    wait("failed re-fetch", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE error IS NOT NULL)",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    assert_eq!(cache_available(&a, &peer).await, (false, false));
    tokio::time::sleep(Duration::from_secs(16)).await;
    assert_eq!(
        a.state.peer_streams.fresh(&peer.id, Snapshot::Recovery),
        Some(false),
        "keepalives cannot authorize failed snapshots"
    );
    b.snapshot_status.store(0, Ordering::SeqCst);
    wait("successful retry", || ready(&a, &peer)).await;
    age_snapshots(&a).await;
    assert_eq!(cache_available(&a, &peer).await, (true, true));

    // A new stream must not reuse verification from the previous connection.
    b.recovery_status.store(500, Ordering::SeqCst);
    a.state.peer_configuration_changed();
    assert_eq!(
        a.state.peer_streams.fresh(&peer.id, Snapshot::Recovery),
        Some(false),
        "configuration invalidation is immediate"
    );
    wait("new stream with failed initial recovery fetch", || async {
        count(&b, NOTIFICATIONS) >= 2
            && a.state.peer_streams.fresh(&peer.id, Snapshot::Activity) == Some(true)
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE error IS NULL)
                 AND EXISTS(SELECT 1 FROM peer_recovery_state WHERE error IS NOT NULL)",
            )
            .fetch_one(&a.state.db)
            .await
            .unwrap()
    })
    .await;
    age_snapshots(&a).await;
    assert_eq!(
        cache_available(&a, &peer).await,
        (true, false),
        "activity verifies independently while recovery remains unavailable"
    );
    b.recovery_status.store(0, Ordering::SeqCst);
    wait("both new snapshots verified", || ready(&a, &peer)).await;
    age_snapshots(&a).await;
    assert_eq!(cache_available(&a, &peer).await, (true, true));

    b.notification_status.store(500, Ordering::SeqCst);
    b.state.peer_configuration_changed();
    wait("disconnection", || async {
        a.state.peer_streams.fresh(&peer.id, Snapshot::Activity) == Some(false)
    })
    .await;
    assert_eq!(cache_available(&a, &peer).await, (false, false));
    wait("attention unavailable during disconnection", || async {
        !remote_mapping(&a, &peer).await.1
    })
    .await;
    admin::delete_peer(&a.state, &peer.id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        a.state.peer_streams.fresh(&peer.id, Snapshot::Activity),
        None,
        "removed connection is pruned"
    );
}

#[tokio::test]
async fn notification_client_idle_keepalive_does_not_refresh_or_create_history() {
    let (a, b, peer) = fixture(0).await;
    wait("initial authoritative catch-up", || ready(&a, &peer)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let baseline = (
        count(&b, NOTIFICATIONS),
        count(&b, RECOVERY),
        count(&b, PROGRESS),
    );
    let history = crate::activity::page(&a.state, 100, 0)
        .await
        .unwrap()
        .history
        .len();
    assert_eq!(baseline, (1, 1, 1));
    // Cross both three former polling ticks and the first SSE keepalive.
    tokio::time::sleep(Duration::from_secs(16)).await;
    assert_eq!(
        (
            count(&b, NOTIFICATIONS),
            count(&b, RECOVERY),
            count(&b, PROGRESS)
        ),
        baseline
    );
    assert_eq!(
        crate::activity::page(&a.state, 100, 0)
            .await
            .unwrap()
            .history
            .len(),
        history
    );
}

#[tokio::test]
async fn notification_client_unchanged_hint_does_not_create_success_history() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let baseline = crate::activity::page(&a.state, 100, 0)
        .await
        .unwrap()
        .history
        .len();
    let before = count(&b, PROGRESS);
    b.state.emit(crate::state::ServerEvent::SyncChanged);
    wait("unchanged notification fetched", || async {
        count(&b, PROGRESS) > before
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        crate::activity::page(&a.state, 100, 0)
            .await
            .unwrap()
            .history
            .len(),
        baseline,
        "routine successful refreshes must not create background history"
    );
}

#[tokio::test]
async fn notification_client_active_progress_coalesces_and_reconnects_without_replication() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    let _barrier = a.state.replication_operation_lock.lock().await;
    let remote_peer = net::peers(&b.state).await.unwrap().remove(0);
    let share = crate::peers::shares(&b.state).await.unwrap().remove(0);
    crate::peers::queue_transfer(
        &b.state,
        &remote_peer.id,
        &share,
        "progress.jpg",
        &crate::crypto::sha256_hex(b"synthetic"),
        1000,
        &VersionVector::default(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE transfers SET status='running'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM transfers")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let before = count(&b, PROGRESS);
    for done in 1..=100 {
        activity::record_progress(&b.state, &id, &remote_peer.id, done, 1)
            .await
            .unwrap();
    }
    wait(
        "useful transfer progress while replication is blocked",
        || async {
            let page = activity::page(&a.state, "active", 100, 0, None)
                .await
                .unwrap();
            page.rows
                .iter()
                .any(|row| row.outgoing && row.bytes_done == 100)
        },
    )
    .await;
    assert!(
        count(&b, PROGRESS) - before <= 2,
        "burst must not cause a request per byte update"
    );

    // Closing an authenticated stream forces reconnect; mutations in the gap must catch up.
    b.state.peer_configuration_changed();
    wait("stream closure visible", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE error LIKE '%connection closed%')",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    activity::record_progress(&b.state, &id, &remote_peer.id, 900, 800)
        .await
        .unwrap();
    wait("reconnect catches up without another hint", || async {
        let page = activity::page(&a.state, "active", 100, 0, None)
            .await
            .unwrap();
        ready(&a, &peer).await && page.rows.iter().any(|row| row.bytes_done == 900)
    })
    .await;
    assert_eq!(count(&b, NOTIFICATIONS), 2);
    drop(_barrier);
    admin::delete_peer(&a.state, &peer.id).await.unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        count(&b, NOTIFICATIONS),
        2,
        "removed peers must not reconnect"
    );
    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM peer_sync_state")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(
        persisted, 0,
        "removed peer must not be resurrected by a stale worker"
    );
}

#[tokio::test]
async fn notification_client_legacy_fallback_and_configuration_reprobe() {
    for status in [404, 405, 501] {
        let (a, b, peer) = fixture(status).await;
        wait("legacy initial refresh", || ready(&a, &peer)).await;
        wait("legacy attention remains available", || async {
            remote_mapping(&a, &peer).await.1
        })
        .await;
        wait("legacy periodic refresh", || async {
            count(&b, PROGRESS) >= 2
        })
        .await;
        assert_eq!(count(&b, NOTIFICATIONS), 1);
        b.notification_status.store(0, Ordering::SeqCst);
        a.state.peer_configuration_changed();
        wait("configuration reprobes upgraded peer", || async {
            count(&b, NOTIFICATIONS) == 2 && ready(&a, &peer).await
        })
        .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let requests = count(&b, PROGRESS);
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(count(&b, PROGRESS), requests, "upgrade must stop polling");
    }
}

#[tokio::test]
async fn notification_client_errors_are_actionable_not_legacy_fallback() {
    for status in [401, 403, 500, 200] {
        let (a, b, peer) = fixture(status).await;
        wait("notification error persisted", || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE error IS NOT NULL)
                 AND EXISTS(SELECT 1 FROM peer_recovery_state WHERE error LIKE 'peer_notifications_unavailable:%')",
            ).fetch_one(&a.state.db).await.unwrap()
        }).await;
        assert_eq!(count(&b, RECOVERY), 0);
        assert_eq!(count(&b, PROGRESS), 0);
        b.notification_status.store(0, Ordering::SeqCst);
        wait("automatic retry clears availability errors", || {
            ready(&a, &peer)
        })
        .await;
        assert_eq!(count(&b, NOTIFICATIONS), 2);
    }
}

#[tokio::test]
async fn notification_client_retries_failed_snapshots_and_fetches_recovery_hints() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    b.snapshot_status.store(500, Ordering::SeqCst);
    b.state.emit(crate::state::ServerEvent::SyncChanged);
    wait("snapshot errors visible", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE error IS NOT NULL)
                 AND EXISTS(SELECT 1 FROM peer_recovery_state WHERE error IS NOT NULL)",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    b.snapshot_status.store(0, Ordering::SeqCst);
    wait("snapshot retry without new notification", || {
        ready(&a, &peer)
    })
    .await;
    assert_eq!(count(&b, NOTIFICATIONS), 1);

    let remote_peer = net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES('failed-example','example',?,'peer','{}','failed',?)",
    )
    .bind(format!("peer:{}", remote_peer.id))
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    crate::peers::recovery::discover(&b.state).await.unwrap();
    wait("recovery hint fetches receiver-owned evidence", || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM outgoing_recovery WHERE peer_id=? AND target_id='failed-example')",
            ).bind(&peer.id).fetch_one(&a.state.db).await.unwrap()
        }).await;
}

#[tokio::test]
async fn notification_client_stalled_recovery_keeps_sync_page_fresh_without_verifying_recovery() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    failed_remote_operation(&b).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    let remote_peer = net::peers(&b.state).await.unwrap().remove(0);
    let share = crate::peers::shares(&b.state).await.unwrap().remove(0);
    crate::peers::queue_transfer(
        &b.state,
        &remote_peer.id,
        &share,
        "progress.jpg",
        &crate::crypto::sha256_hex(b"synthetic"),
        1000,
        &VersionVector::default(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE transfers SET status='running'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let transfer: String = sqlx::query_scalar("SELECT id FROM transfers")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    activity::record_progress(&b.state, &transfer, &remote_peer.id, 100, 100)
        .await
        .unwrap();
    for table in ["peer_sync_state", "peer_recovery_state"] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
            .execute(&a.state.db)
            .await
            .unwrap();
    }
    b.recovery_hold.armed.store(true, Ordering::SeqCst);
    a.tasks.push(tokio::spawn(activity::run(a.state.clone())));
    tokio::time::timeout(Duration::from_secs(9), b.recovery_hold.entered.notified())
        .await
        .expect("recovery response was not held");
    wait("activity persisted while recovery is stalled", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM peer_sync_state
             WHERE peer_id=? AND error IS NULL AND updated_at IS NOT NULL)",
        )
        .bind(&peer.id)
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    })
    .await;
    wait("activity independently verified", || async {
        a.state.peer_streams.fresh(&peer.id, Snapshot::Activity) == Some(true)
    })
    .await;
    assert!(count(&b, PROGRESS) >= 1);
    let page = activity::page(&a.state, "active", 100, 0, None)
        .await
        .unwrap();
    assert!(page.warnings.is_empty(), "{:?}", page.warnings);
    assert_eq!(
        (page.progress.bytes_total, page.progress.bytes_done),
        (1000, 100)
    );
    assert!(page.progress.bytes_per_second > 0.0);
    assert!(page.progress.eta_seconds.is_some());
    assert!(
        page.rows
            .iter()
            .any(|row| row.outgoing && row.bytes_done == 100)
    );
    assert_eq!(cache_available(&a, &peer).await, (true, false));
    let recovered: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM peer_recovery_state WHERE updated_at IS NOT NULL)",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert!(!recovered, "the recovery snapshot is still in flight");
    assert_eq!(
        a.state.peer_streams.fresh(&peer.id, Snapshot::Recovery),
        Some(false),
        "recovery stays unverified while its snapshot is held"
    );
    let held_requests = count(&b, RECOVERY);
    activity::record_progress(&b.state, &transfer, &remote_peer.id, 200, 100)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = activity::page(&a.state, "active", 100, 0, None)
                .await
                .unwrap();
            if a.state.peer_streams.fresh(&peer.id, Snapshot::Activity) == Some(true)
                && page.warnings.is_empty()
                && page.progress.bytes_done == 200
                && page.progress.bytes_per_second > 0.0
                && page.progress.eta_seconds.is_some()
                && page
                    .rows
                    .iter()
                    .any(|row| row.outgoing && row.bytes_done == 200)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a new activity hint must refresh the Sync page while recovery remains held");
    assert_eq!(
        count(&b, RECOVERY),
        held_requests,
        "recovery stays single-flight while its previous response is held"
    );
    assert_eq!(cache_available(&a, &peer).await, (true, false));
    b.recovery_hold.armed.store(true, Ordering::SeqCst);
    b.recovery_hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), b.recovery_hold.entered.notified())
        .await
        .expect("the newer recovery snapshot should be requested");
    assert_eq!(
        a.state.peer_streams.fresh(&peer.id, Snapshot::Recovery),
        Some(false),
        "the old response must not verify recovery for the newer hint"
    );
    assert_eq!(cache_available(&a, &peer).await, (true, false));
    b.recovery_hold.release.notify_one();
    wait("verified after the stalled recovery completes", || {
        ready(&a, &peer)
    })
    .await;
    assert_eq!(
        a.state.peer_streams.fresh(&peer.id, Snapshot::Recovery),
        Some(true)
    );
    assert_eq!(cache_available(&a, &peer).await, (true, true));
}

async fn running_transfer(b: &Instance, size: u64) -> (String, String) {
    let remote_peer = net::peers(&b.state).await.unwrap().remove(0);
    let share = crate::peers::shares(&b.state).await.unwrap().remove(0);
    crate::peers::queue_transfer(
        &b.state,
        &remote_peer.id,
        &share,
        "progress.jpg",
        &crate::crypto::sha256_hex(b"synthetic"),
        size,
        &VersionVector::default(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE transfers SET status='running'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let transfer = sqlx::query_scalar("SELECT id FROM transfers")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    (transfer, remote_peer.id)
}

#[tokio::test]
async fn activity_repeated_updates_coalesce_and_preserve_paging_and_deletion() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    let before: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq),0) FROM sync_changes")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    let (transfer, remote_peer) = running_transfer(&b, 1000).await;
    for done in [100, 200] {
        activity::record_progress(&b.state, &transfer, &remote_peer, done, 100)
            .await
            .unwrap();
    }
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT seq,transfer_id FROM sync_changes WHERE peer_id=? AND seq>? ORDER BY seq",
    )
    .bind(&remote_peer)
    .bind(before)
    .fetch_all(&b.state.db)
    .await
    .unwrap();
    assert_eq!(
        events.len(),
        1,
        "triggers coalesce repeated transfer updates"
    );
    let cursor = events[0].0;
    let page = serde_json::to_value(
        activity::changes(&b.state, &remote_peer, before)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(page["cursor"], cursor);
    assert_eq!(page["changes"].as_array().unwrap().len(), 1);
    assert_eq!(page["changes"][0]["id"], transfer);
    assert_eq!(page["changes"][0]["activity"]["bytes_done"], 200);
    activity::refresh(&a.state, &peer).await.unwrap();
    let mirrored: String =
        sqlx::query_scalar("SELECT activity_json FROM outgoing_sync WHERE peer_id=? AND id=?")
            .bind(&peer.id)
            .bind(&transfer)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&mirrored).unwrap()["bytes_done"],
        200
    );

    // Exactly one page of genuine tombstones, then a later update to the live transfer.
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<499)
         INSERT INTO sync_changes(transfer_id,peer_id) SELECT 'deleted-example-' || i,? FROM n",
    )
    .bind(&remote_peer)
    .execute(&b.state.db)
    .await
    .unwrap();
    let boundary: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM sync_changes WHERE peer_id=?")
        .bind(&remote_peer)
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    for done in [300, 400] {
        activity::record_progress(&b.state, &transfer, &remote_peer, done, 100)
            .await
            .unwrap();
    }
    let page = serde_json::to_value(
        activity::changes(&b.state, &remote_peer, cursor)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(page["cursor"], boundary);
    assert_eq!(page["more"], true);
    let changes = page["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 500);
    for (i, change) in changes.iter().enumerate() {
        assert_eq!(change["id"], format!("deleted-example-{i}"));
        assert!(change["activity"].is_null());
    }
    let page = serde_json::to_value(
        activity::changes(&b.state, &remote_peer, boundary)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(page["more"], false);
    assert_eq!(page["changes"].as_array().unwrap().len(), 1);
    assert_eq!(page["changes"][0]["id"], transfer);
    assert_eq!(page["changes"][0]["activity"]["bytes_done"], 400);
    activity::refresh(&a.state, &peer).await.unwrap();
    let mirrored: String =
        sqlx::query_scalar("SELECT activity_json FROM outgoing_sync WHERE peer_id=? AND id=?")
            .bind(&peer.id)
            .bind(&transfer)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&mirrored).unwrap()["bytes_done"],
        400
    );

    let before_delete = page["cursor"].as_i64().unwrap();
    sqlx::query("DELETE FROM transfers WHERE id=?")
        .bind(&transfer)
        .execute(&b.state.db)
        .await
        .unwrap();
    let page = serde_json::to_value(
        activity::changes(&b.state, &remote_peer, before_delete)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(page["cursor"].as_i64().unwrap() > before_delete);
    assert_eq!(page["changes"].as_array().unwrap().len(), 1);
    assert_eq!(page["changes"][0]["id"], transfer);
    assert!(page["changes"][0]["activity"].is_null());
    activity::refresh(&a.state, &peer).await.unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outgoing_sync WHERE peer_id=? AND id=?")
            .bind(&peer.id)
            .bind(&transfer)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    assert_eq!(remaining, 0, "a genuine tombstone removes the mirror");
}

#[tokio::test]
async fn sustained_progress_hints_faster_than_activity_fetches_keep_sync_fresh() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    let (transfer, remote_peer) = running_transfer(&b, 100_000_000).await;
    // Hints coalesce every 250ms on the receiver; each activity response takes longer.
    b.activity_delay.store(700, Ordering::SeqCst);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let progress = tokio::spawn({
        let state = b.state.clone();
        let stop = stop.clone();
        async move {
            let mut done = 0;
            while !stop.load(Ordering::SeqCst) {
                done += 10_000;
                activity::record_progress(&state, &transfer, &remote_peer, done, 10_000)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });
    a.tasks.push(tokio::spawn(activity::run(a.state.clone())));
    wait("first activity verified", || async {
        a.state.peer_streams.fresh(&peer.id, Snapshot::Activity) == Some(true)
    })
    .await;
    let fetched = count(&b, PROGRESS);
    let (mut first, mut last, mut outpaced) = (None, 0, false);
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let pending = a
            .state
            .peer_streams
            .pending_hints(&peer.id, Snapshot::Activity)
            .unwrap();
        outpaced |= pending > 0;
        let page = activity::page(&a.state, "active", 100, 0, None)
            .await
            .unwrap();
        assert!(
            page.warnings.is_empty(),
            "{pending} pending hints: {:?}",
            page.warnings
        );
        assert!(page.progress.bytes_per_second > 0.0);
        assert!(page.progress.eta_seconds.is_some());
        assert!(page.progress.bytes_done >= last, "progress never regresses");
        last = page.progress.bytes_done;
        first.get_or_insert(last);
    }
    stop.store(true, Ordering::SeqCst);
    progress.await.unwrap();
    assert!(
        outpaced,
        "hints must arrive while an activity fetch is in flight"
    );
    assert!(
        last > first.unwrap(),
        "persisted snapshots advance under sustained hints"
    );
    assert!(count(&b, PROGRESS) > fetched + 1);
}

#[tokio::test]
async fn receiver_activity_does_not_wait_for_unrelated_recovery_discovery() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    let (transfer, remote_peer) = running_transfer(&b, 1000).await;
    activity::record_progress(&b.state, &transfer, &remote_peer, 100, 100)
        .await
        .unwrap();
    // Undiscovered recovery unrelated to the activity rows, then an unrelated SQLite writer.
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES('unrelated-example','unrelated',?,'peer','{}','failed',?)",
    )
    .bind(format!("peer:{remote_peer}"))
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    // Recently seen peers need no last-seen write, isolating discovery as the only writer.
    sqlx::query("UPDATE peers SET status='online',last_seen_at=?")
        .bind(crate::db::now())
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut writer = b.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE peers SET last_error='held' WHERE id=?")
        .bind(&remote_peer)
        .execute(&mut *writer)
        .await
        .unwrap();
    let changes = tokio::time::timeout(
        Duration::from_secs(3),
        activity::changes(&b.state, &remote_peer, 0),
    )
    .await
    .expect("unrelated discovery must not hold activity behind the writer")
    .unwrap();
    let encoded = serde_json::to_value(&changes).unwrap();
    assert_eq!(encoded["progress"]["bytes_done"], 100, "{encoded}");
    activity::refresh(&a.state, &peer).await.unwrap();
    assert!(
        activity::page(&a.state, "active", 100, 0, None)
            .await
            .unwrap()
            .rows
            .iter()
            .any(|row| row.outgoing && row.bytes_done == 100),
        "real HTTP activity completes while the receiver writer is held"
    );
    writer.rollback().await.unwrap();

    // A selected row that needs discovery waits for the writer instead of omitting recovery.
    sqlx::query("UPDATE transfers SET status='failed',error='synthetic' WHERE id=?")
        .bind(&transfer)
        .execute(&b.state.db)
        .await
        .unwrap();
    let mut writer = b.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE peers SET last_error='held again' WHERE id=?")
        .bind(&remote_peer)
        .execute(&mut *writer)
        .await
        .unwrap();
    let pending = tokio::spawn({
        let state = b.state.clone();
        async move { activity::changes(&state, &remote_peer, 0).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!pending.is_finished(), "required recovery is not omitted");
    writer.rollback().await.unwrap();
    let changes = serde_json::to_value(pending.await.unwrap().unwrap()).unwrap();
    let row = changes["changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["id"] == transfer.as_str())
        .unwrap();
    assert_eq!(
        row["activity"]["recovery"]["target"]["id"],
        transfer.as_str(),
        "{row}"
    );
    let unrelated: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sync_recovery WHERE target_id='unrelated-example')",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert!(
        !unrelated,
        "page-scoped discovery leaves unrelated backlog alone"
    );
    crate::peers::recovery::discover(&b.state).await.unwrap();
    let unrelated: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sync_recovery WHERE target_id='unrelated-example')",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert!(unrelated, "global discovery shares the same eligibility");
}

async fn competing_writer(
    state: AppState,
    peer: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Duration {
    let mut worst = Duration::ZERO;
    while !stop.load(Ordering::SeqCst) {
        let started = std::time::Instant::now();
        sqlx::query("UPDATE peers SET last_error=? WHERE id=?")
            .bind(crate::db::now())
            .bind(&peer)
            .execute(&state.db)
            .await
            .unwrap();
        worst = worst.max(started.elapsed());
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    worst
}

/// Prints SQLx timings for the receiver's 500-change page; run at the serialized Cargo gate with
/// `cargo test -p ism-server measure_activity_page_discovery -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "manual SQLx contention measurement"]
async fn measure_activity_page_discovery() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let remote_peer = net::peers(&b.state).await.unwrap().remove(0);
    let share = crate::peers::shares(&b.state).await.unwrap().remove(0);
    for i in 0..500 {
        crate::peers::queue_transfer(
            &b.state,
            &remote_peer.id,
            &share,
            &format!("measure-{i}.jpg"),
            &crate::crypto::sha256_hex(format!("synthetic-{i}").as_bytes()),
            1000,
            &VersionVector::default(),
        )
        .await
        .unwrap();
    }
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i < 4999)
         INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         SELECT 'backlog-' || i,'backlog-' || i,?,'peer','{}','failed',? FROM n",
    )
    .bind(format!("peer:{}", remote_peer.id))
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE peers SET status='online',last_seen_at=?")
        .bind(crate::db::now())
        .execute(&b.state.db)
        .await
        .unwrap();
    let hold = Duration::from_millis(250);
    for scenario in ["500 healthy rows", "500 failed rows, undiscovered"] {
        if scenario.starts_with("500 failed") {
            sqlx::query("UPDATE transfers SET status='failed',error='synthetic'")
                .execute(&b.state.db)
                .await
                .unwrap();
        }
        let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_recovery")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
        // Competing writer committing every 2 ms while the page is produced.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let idle_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let idle = tokio::spawn(competing_writer(
            b.state.clone(),
            remote_peer.id.clone(),
            idle_stop.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        idle_stop.store(true, Ordering::SeqCst);
        let idle = idle.await.unwrap();
        let competitor = tokio::spawn(competing_writer(
            b.state.clone(),
            remote_peer.id.clone(),
            stop.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = std::time::Instant::now();
        let page = activity::changes(&b.state, &remote_peer.id, 0)
            .await
            .unwrap();
        let elapsed = started.elapsed();
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop.store(true, Ordering::SeqCst);
        let worst = competitor.await.unwrap();
        let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_recovery")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&page).unwrap()["changes"]
                .as_array()
                .unwrap()
                .len(),
            500
        );
        println!(
            "{scenario}: page={elapsed:?} competitor-max={worst:?} (idle {idle:?}) inserted={}",
            after - before
        );
    }
    // Held writer: a page whose rows are all discovered needs no writer; fresh failures wait.
    for scenario in [
        "discovered page, held writer",
        "newly failed page, held writer",
    ] {
        if scenario.starts_with("newly") {
            sqlx::query("DELETE FROM sync_recovery WHERE kind='transfer'")
                .execute(&b.state.db)
                .await
                .unwrap();
        }
        let mut writer = b.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("UPDATE peers SET last_error='held' WHERE id=?")
            .bind(&remote_peer.id)
            .execute(&mut *writer)
            .await
            .unwrap();
        let pending = tokio::spawn({
            let state = b.state.clone();
            let peer = remote_peer.id.clone();
            async move {
                let started = std::time::Instant::now();
                activity::changes(&state, &peer, 0).await.unwrap();
                started.elapsed()
            }
        });
        tokio::time::sleep(hold).await;
        writer.rollback().await.unwrap();
        let elapsed = pending.await.unwrap();
        let backlog: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sync_recovery WHERE target_id LIKE 'backlog-%'",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap();
        println!("{scenario} {hold:?}: page={elapsed:?} unrelated-backlog-inserted={backlog}");
    }
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort();
    values[values.len() / 2]
}

/// Prints SQLx timings for an actual paired recovery refresh at the 5,000-row snapshot limit;
/// run at the serialized Cargo gate with
/// `cargo test -p ism-server measure_recovery_refresh_contention -- --ignored --nocapture`.
/// Synthetic local evidence only: it does not characterize production load or lift the limit.
#[tokio::test]
#[ignore = "manual SQLx contention measurement"]
async fn measure_recovery_refresh_contention() {
    const ROWS: usize = 5000;
    const RUNS: usize = 5;
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    let remote_peer = net::peers(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i < 4999)
         INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         SELECT printf('measure-%04d',i),'measure-' || i,?,'peer','{}','failed',? FROM n",
    )
    .bind(format!("peer:{}", remote_peer.id))
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    crate::peers::recovery::discover(&b.state).await.unwrap();

    // The GET alone, outside any transaction; refresh itself must finish it within 10 s.
    let started = std::time::Instant::now();
    let page: ism_core::dto::RecoveryPage =
        net::get_json_within(&a.state, &peer, RECOVERY, Some(Duration::from_secs(60)))
            .await
            .unwrap();
    let get = started.elapsed();
    assert_eq!(page.rows.len(), ROWS, "receiver snapshot is at the limit");
    assert!(
        get < Duration::from_secs(10),
        "GET took {get:?}; refresh would time out before reaching SQLite"
    );
    let mut expected: Vec<(String, String)> = page
        .rows
        .iter()
        .map(|row| (row.target.id.clone(), serde_json::to_string(row).unwrap()))
        .collect();
    expected.sort();
    println!("receiver GET of {ROWS} rows: {get:?}");

    let state = a.state.clone();
    let peer_id = peer.id.clone();
    let mirrored = move || {
        let state = state.clone();
        let peer_id = peer_id.clone();
        async move {
            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT kind,target_id,view_json FROM outgoing_recovery WHERE peer_id=?
                 ORDER BY target_id",
            )
            .bind(&peer_id)
            .fetch_all(&state.db)
            .await
            .unwrap();
            assert!(rows.iter().all(|(kind, _, _)| kind == "operation"));
            rows.into_iter()
                .map(|(_, id, json)| (id, json))
                .collect::<Vec<_>>()
        }
    };

    for contended in [false, true] {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let baseline = tokio::spawn(competing_writer(
            a.state.clone(),
            peer.id.clone(),
            stop.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.store(true, Ordering::SeqCst);
        let baseline = baseline.await.unwrap();
        let label = if contended {
            "competing writer every 2 ms"
        } else {
            "idle"
        };
        println!("[{label}] competing writer alone, no refresh: max wait {baseline:?}");
        for scenario in ["5,000 inserted", "5,000 changed", "unchanged recent"] {
            let mut totals = Vec::new();
            let mut fetches = Vec::new();
            let mut checks = Vec::new();
            let mut waits = Vec::new();
            let mut holds = Vec::new();
            let mut competitor = Vec::new();
            for _ in 0..RUNS {
                match scenario {
                    "5,000 inserted" => {
                        sqlx::query("DELETE FROM outgoing_recovery WHERE peer_id=?")
                            .bind(&peer.id)
                            .execute(&a.state.db)
                            .await
                            .unwrap();
                    }
                    "5,000 changed" => {
                        sqlx::query(
                            "UPDATE outgoing_recovery SET view_json=json_object('stale',rowid)
                             WHERE peer_id=?",
                        )
                        .bind(&peer.id)
                        .execute(&a.state.db)
                        .await
                        .unwrap();
                    }
                    _ => {
                        // Keep the 10 s recency window open so this measures the read-only path.
                        sqlx::query("UPDATE peer_recovery_state SET updated_at=? WHERE peer_id=?")
                            .bind(crate::db::now())
                            .bind(&peer.id)
                            .execute(&a.state.db)
                            .await
                            .unwrap();
                    }
                }
                let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let writer = contended.then(|| {
                    tokio::spawn(competing_writer(
                        a.state.clone(),
                        peer.id.clone(),
                        stop.clone(),
                    ))
                });
                if contended {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let started = std::time::Instant::now();
                crate::peers::recovery::refresh(&a.state, &peer)
                    .await
                    .unwrap();
                totals.push(started.elapsed());
                if let Some(writer) = writer {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    stop.store(true, Ordering::SeqCst);
                    competitor.push(writer.await.unwrap());
                }
                let phases = crate::peers::recovery::refresh_timing::take(&peer.id).unwrap();
                fetches.push(phases.fetch);
                checks.push(phases.check);
                assert_eq!(
                    phases.hold.is_some(),
                    scenario != "unchanged recent",
                    "{scenario}: only changed snapshots take the writer lock"
                );
                if let (Some(wait), Some(hold)) = (phases.lock_wait, phases.hold) {
                    waits.push(wait);
                    holds.push(hold);
                }
                assert_eq!(mirrored().await, expected, "{scenario}: mirror matches GET");
                let error: Option<String> =
                    sqlx::query_scalar("SELECT error FROM peer_recovery_state WHERE peer_id=?")
                        .bind(&peer.id)
                        .fetch_one(&a.state.db)
                        .await
                        .unwrap();
                assert_eq!(error, None);
            }
            let held = if holds.is_empty() {
                "no writer lock".to_owned()
            } else {
                format!(
                    "lock-wait={:?} write-tx={:?} (max {:?})",
                    median(waits),
                    median(holds.clone()),
                    holds.iter().max().unwrap()
                )
            };
            let competing = if competitor.is_empty() {
                String::new()
            } else {
                format!(
                    " competitor max-wait median={:?} worst={:?}",
                    median(competitor.clone()),
                    competitor.iter().max().unwrap()
                )
            };
            println!(
                "[{label}] {scenario}, median of {RUNS}: refresh={:?} get={:?} \
                 prepare+read-check={:?} {held}{competing}",
                median(totals),
                median(fetches),
                median(checks),
            );
        }
    }

    // Semantic write counts, measured after timing so audit triggers do not inflate it.
    audit_writes(&a).await;
    sqlx::query(
        "UPDATE outgoing_recovery SET view_json=json_object('stale',rowid) WHERE peer_id=?",
    )
    .bind(&peer.id)
    .execute(&a.state.db)
    .await
    .unwrap();
    take_writes(&a).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    let writes = take_writes(&a).await;
    assert_eq!(
        writes
            .iter()
            .filter(|(table, action)| table == "outgoing_recovery" && action == "UPDATE")
            .count(),
        ROWS
    );
    assert_eq!(
        writes.len(),
        ROWS + 1,
        "only changed mirrors and the freshness row are written"
    );
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    assert!(
        take_writes(&a).await.is_empty(),
        "unchanged recent refresh writes nothing"
    );
    assert_eq!(mirrored().await, expected);
}

#[tokio::test]
async fn unchanged_peer_snapshots_do_not_rewrite_mirrors() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    failed_remote_operation(&b).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    activity::refresh(&a.state, &peer).await.unwrap();
    audit_writes(&a).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    activity::refresh(&a.state, &peer).await.unwrap();
    assert!(
        take_writes(&a).await.is_empty(),
        "unchanged recent snapshots must not write"
    );
    let stale = serde_json::json!({
        "target": { "kind": "conflict", "id": "stale-example", "peer_id": null },
        "title": "stale.jpg", "state": "needs_decision", "code": "choose_version",
        "detail": null, "can_fix": false, "can_dismiss": false, "replacement_operation_id": null
    });
    sqlx::query("INSERT INTO outgoing_recovery(peer_id,kind,target_id,view_json) VALUES (?, 'conflict', 'stale-example', ?)")
        .bind(&peer.id)
        .bind(stale.to_string())
        .execute(&a.state.db)
        .await
        .unwrap();
    take_writes(&a).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    assert_eq!(
        take_writes(&a).await,
        [
            ("outgoing_recovery".to_owned(), "DELETE".to_owned()),
            ("peer_recovery_state".to_owned(), "UPDATE".to_owned()),
        ],
        "only the removed row changes; unchanged rows are not reinserted"
    );
    age_snapshots(&a).await;
    take_writes(&a).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    activity::refresh(&a.state, &peer).await.unwrap();
    assert_eq!(
        take_writes(&a).await,
        [
            ("peer_recovery_state".to_owned(), "UPDATE".to_owned()),
            ("peer_sync_state".to_owned(), "UPDATE".to_owned()),
        ],
        "old unchanged snapshots refresh only their timestamps"
    );
    let mirrored: Vec<String> =
        sqlx::query_scalar("SELECT target_id FROM outgoing_recovery WHERE peer_id=?")
            .bind(&peer.id)
            .fetch_all(&a.state.db)
            .await
            .unwrap();
    assert_eq!(mirrored, ["heartbeat-example"]);
}

#[tokio::test]
async fn unchanged_recent_refresh_skips_the_writer_lock_and_changes_still_persist() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    failed_remote_operation(&b).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    activity::refresh(&a.state, &peer).await.unwrap();
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(mode, "wal");

    let writer = a.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        crate::peers::recovery::refresh(&a.state, &peer)
            .await
            .unwrap();
        activity::refresh(&a.state, &peer).await.unwrap();
    })
    .await
    .expect("an unchanged recent refresh must not wait for an unrelated writer");
    writer.rollback().await.unwrap();

    let stale = serde_json::json!({
        "target": { "kind": "conflict", "id": "stale-example", "peer_id": null },
        "title": "stale.jpg", "state": "needs_decision", "code": "choose_version",
        "detail": null, "can_fix": false, "can_dismiss": false, "replacement_operation_id": null
    });
    sqlx::query("INSERT INTO outgoing_recovery(peer_id,kind,target_id,view_json) VALUES (?, 'conflict', 'stale-example', ?)")
        .bind(&peer.id)
        .bind(stale.to_string())
        .execute(&a.state.db)
        .await
        .unwrap();
    let before: Option<String> =
        sqlx::query_scalar("SELECT updated_at FROM peer_recovery_state WHERE peer_id=?")
            .bind(&peer.id)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    let writer = a.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let changed = tokio::spawn({
        let state = a.state.clone();
        let peer = peer.clone();
        async move { crate::peers::recovery::refresh(&state, &peer).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !changed.is_finished(),
        "a changed snapshot must wait for the writer lock"
    );
    writer.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(9), changed)
        .await
        .expect("changed snapshot did not persist after the writer released")
        .unwrap()
        .unwrap();
    let mirrored: Vec<String> =
        sqlx::query_scalar("SELECT target_id FROM outgoing_recovery WHERE peer_id=?")
            .bind(&peer.id)
            .fetch_all(&a.state.db)
            .await
            .unwrap();
    assert_eq!(mirrored, ["heartbeat-example"]);
    let after: Option<String> =
        sqlx::query_scalar("SELECT updated_at FROM peer_recovery_state WHERE peer_id=?")
            .bind(&peer.id)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    assert_ne!(after, before, "a changed snapshot records its verification");
}

#[tokio::test]
async fn recovery_snapshot_started_before_acceptance_keeps_it_until_a_later_snapshot() {
    use ism_core::dto::{RecoveryBatchInput, RecoveryKind, RecoveryTarget};

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let owner = net::peers(&b.state).await.unwrap().remove(0);
    let share = crate::peers::shares(&b.state).await.unwrap().remove(0);
    sqlx::query(
        "INSERT INTO transfers (id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at,error)
         VALUES ('late-accept',?,?, 'photo.xmp',?,3,'{}','failed',?,?, 'peer unavailable')",
    )
    .bind(&owner.id)
    .bind(&share.id)
    .bind(crate::crypto::sha256_hex(b"xmp"))
    .bind(crate::db::now())
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    let receiver = net::peers(&a.state).await.unwrap().remove(0);
    crate::peers::recovery::refresh(&a.state, &receiver)
        .await
        .unwrap();
    crate::peers::recovery::submit(
        &a.state,
        RecoveryBatchInput {
            targets: vec![RecoveryTarget {
                kind: RecoveryKind::Transfer,
                id: "late-accept".into(),
                peer_id: Some(receiver.id.clone()),
            }],
            all: false,
            resolution: None,
        },
    )
    .await
    .unwrap();
    // Recreate the pre-upgrade schema, seed a legacy acceptance, then reopen normally.
    let migration: i64 = sqlx::query_scalar(
        "SELECT version FROM _sqlx_migrations WHERE description='recovery acceptance generation'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    for statement in [
        "ALTER TABLE outgoing_recovery_requests DROP COLUMN accepted_generation",
        "ALTER TABLE peer_recovery_state DROP COLUMN acceptance_generation",
    ] {
        sqlx::query(statement).execute(&a.state.db).await.unwrap();
    }
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version=?")
        .bind(migration)
        .execute(&a.state.db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO outgoing_recovery_requests
         (peer_id,kind,target_id,state,view_json,updated_at)
         SELECT peer_id,kind,'legacy-accept','accepted',view_json,updated_at
         FROM outgoing_recovery_requests WHERE target_id='late-accept'",
    )
    .execute(&a.state.db)
    .await
    .unwrap();
    sqlx::query("DELETE FROM peer_recovery_state WHERE peer_id=?")
        .bind(&receiver.id)
        .execute(&a.state.db)
        .await
        .unwrap();
    a.state.db.close().await;
    let db = crate::db::connect(a._dir.path()).await.unwrap();
    a.state = AppState::new(
        Config::for_tests(a._dir.path()),
        db,
        SecretBox::open_or_create(a._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let legacy: (String, i64) = sqlx::query_as(
        "SELECT state,accepted_generation FROM outgoing_recovery_requests
         WHERE peer_id=? AND target_id='legacy-accept'",
    )
    .bind(&receiver.id)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(legacy, ("accepted".to_owned(), 0));
    let state_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM peer_recovery_state WHERE peer_id=?)")
            .bind(&receiver.id)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    assert!(!state_exists, "the legacy peer had no recovery state");
    b.recovery_hold.armed.store(true, Ordering::SeqCst);
    let held = tokio::spawn({
        let state = a.state.clone();
        let receiver = receiver.clone();
        async move { crate::peers::recovery::refresh(&state, &receiver).await }
    });
    tokio::time::timeout(Duration::from_secs(9), b.recovery_hold.entered.notified())
        .await
        .expect("recovery response was not held");
    crate::peers::recovery::forward_peer(&a.state, &receiver.id)
        .await
        .unwrap();
    let accepted = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM outgoing_recovery_requests
             WHERE target_id='late-accept' AND state='accepted'",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap()
    };
    assert_eq!(accepted().await, 1);
    let generation: (i64, i64) = sqlx::query_as(
        "SELECT s.acceptance_generation,r.accepted_generation
         FROM peer_recovery_state s JOIN outgoing_recovery_requests r ON r.peer_id=s.peer_id
         WHERE s.peer_id=? AND r.target_id='late-accept'",
    )
    .bind(&receiver.id)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(
        generation,
        (1, 1),
        "missing counters are created atomically"
    );
    b.recovery_hold.release.notify_one();
    held.await.unwrap().unwrap();
    let legacy: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outgoing_recovery_requests WHERE target_id='legacy-accept'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(legacy, 0, "a verified snapshot clears legacy acceptances");
    assert_eq!(
        accepted().await,
        1,
        "a snapshot requested before acceptance must not clear it"
    );
    crate::peers::recovery::refresh(&a.state, &receiver)
        .await
        .unwrap();
    assert_eq!(
        accepted().await,
        0,
        "a later snapshot reflects and clears the acceptance"
    );
}

#[tokio::test]
async fn recovery_acceptance_counter_exhaustion_fails_closed() {
    use ism_core::dto::{RecoveryBatchInput, RecoveryKind, RecoveryTarget};

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    failed_remote_operation(&b).await;
    crate::peers::recovery::refresh(&a.state, &peer)
        .await
        .unwrap();
    crate::peers::recovery::submit(
        &a.state,
        RecoveryBatchInput {
            targets: vec![RecoveryTarget {
                kind: RecoveryKind::Operation,
                id: "heartbeat-example".into(),
                peer_id: Some(peer.id.clone()),
            }],
            all: false,
            resolution: None,
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE peer_recovery_state SET acceptance_generation=9223372036854775807 WHERE peer_id=?",
    )
    .bind(&peer.id)
    .execute(&a.state.db)
    .await
    .unwrap();
    let error = crate::peers::recovery::forward_peer(&a.state, &peer.id)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "peer_recovery_generation_exhausted");
    let counter: (i64, String) = sqlx::query_as(
        "SELECT acceptance_generation,typeof(acceptance_generation)
         FROM peer_recovery_state WHERE peer_id=?",
    )
    .bind(&peer.id)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(counter, (i64::MAX, "integer".to_owned()));
    let accepted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outgoing_recovery_requests WHERE peer_id=? AND state='accepted'",
    )
    .bind(&peer.id)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(
        accepted, 0,
        "an exhausted counter cannot save an acceptance"
    );
}
