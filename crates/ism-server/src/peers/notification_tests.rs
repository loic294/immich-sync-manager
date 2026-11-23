//! Paired real-HTTP notification clients, with controllable endpoint compatibility/failure.

use super::*;
use crate::peers::{activity, net};
use std::sync::atomic::Ordering;

const NOTIFICATIONS: &str = "/api/peer/v1/notifications";
const RECOVERY: &str = "/api/peer/v1/recovery";
const PROGRESS: &str = "/api/peer/v1/sync-activity";

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
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM peer_sync_state WHERE peer_id=? AND error IS NULL)
         AND EXISTS(SELECT 1 FROM peer_recovery_state WHERE peer_id=? AND error IS NULL)",
    )
    .bind(&peer.id)
    .bind(&peer.id)
    .fetch_one(&a.state.db)
    .await
    .unwrap()
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
