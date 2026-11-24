//! Paired real-HTTP notification clients, with controllable endpoint compatibility/failure.

use super::*;
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
    snapshots && a.state.peer_streams.fresh(&peer.id).unwrap_or(true)
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
async fn notification_client_attention_only_changes_do_not_rewrite_sync_or_recovery() {
    let (a, b, peer) = fixture(0).await;
    wait("connected", || ready(&a, &peer)).await;
    wait("initial attention", || async {
        remote_mapping(&a, &peer).await.1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    sqlx::query("CREATE TABLE write_audit (table_name TEXT NOT NULL)")
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
                 BEGIN INSERT INTO write_audit VALUES ('{table}'); END"
            );
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&a.state.db)
                .await
                .unwrap();
        }
    }
    let before = (
        count(&b, ATTENTION),
        count(&b, PROGRESS),
        count(&b, RECOVERY),
    );
    b.state.emit(crate::state::ServerEvent::WatchChanged);
    b.state.emit(crate::state::ServerEvent::MaintenanceChanged {
        server_id: "example".into(),
    });
    wait("attention-only refresh", || async {
        count(&b, ATTENTION) > before.0
    })
    .await;
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
        a.state.peer_streams.fresh(&peer.id),
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
        a.state.peer_streams.fresh(&peer.id),
        Some(false),
        "configuration invalidation is immediate"
    );
    wait("new stream with failed initial recovery fetch", || async {
        count(&b, NOTIFICATIONS) >= 2
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
        (false, false),
        "both initial snapshots are required"
    );
    b.recovery_status.store(0, Ordering::SeqCst);
    wait("both new snapshots verified", || ready(&a, &peer)).await;
    age_snapshots(&a).await;
    assert_eq!(cache_available(&a, &peer).await, (true, true));

    b.notification_status.store(500, Ordering::SeqCst);
    b.state.peer_configuration_changed();
    wait("disconnection", || async {
        a.state.peer_streams.fresh(&peer.id) == Some(false)
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
        a.state.peer_streams.fresh(&peer.id),
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
