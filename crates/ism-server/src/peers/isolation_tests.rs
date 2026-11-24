use super::*;
use crate::peers::{self, EventData};

#[tokio::test]
async fn share_scan_yields_to_operations_and_rechecks_missing_files() {
    let mut b = instance("Beta").await;
    stop_workers(&mut b).await;
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = peers::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("late.jpg"), b"original");
    peers::scan_share(&b.state, &share).await.unwrap();
    std::fs::remove_file(b.shared.join("late.jpg")).unwrap();
    write_old(
        &b._dir.path().join("photos/replacement.jpg"),
        b"replacement",
    );
    for index in 0..1024 {
        write_old(&b.shared.join(format!("photo-{index:04}.jpg")), b"photo");
    }

    let scan = peers::scan_share(&b.state, &share);
    tokio::pin!(scan);
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut scan => panic!("scan finished before publishing its first file: {result:?}"),
            () = async {
                loop {
                    let published: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM replica_files WHERE share_id=? AND rel_path LIKE 'photo-%')",
                    )
                    .bind(&share.id)
                    .fetch_one(&b.state.db)
                    .await
                    .unwrap();
                    if published {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            } => {}
        }
        // Admission must become available before the whole inventory is reconciled.
        let gate = b.state.replication_operation_lock.lock();
        tokio::pin!(gate);
        let guard = tokio::select! {
            biased;
            guard = &mut gate => guard,
            result = &mut scan => panic!("scan held admission until completion: {result:?}"),
        };
        let accepted = crate::ops::submit(
            &b.state,
            &Principal::Admin {
                username: "test".into(),
            },
            OperationRequest {
                idempotency_key: "during-share-scan".into(),
                label: None,
                actions: vec![OperationAction::Move {
                    source: loc("replacement.jpg"),
                    target: loc("shared/late.jpg"),
                    expected_revision: None,
                }],
                allow_owner_transfer: false,
                allow_partial: false,
            },
        )
        .await
        .unwrap();
        let operation = crate::ops::exec::run_operation(&b.state, &accepted.operation_id);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            result = &mut operation => panic!("operation bypassed admission: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        drop(guard);
        tokio::select! {
            biased;
            result = &mut operation => result.unwrap(),
            result = &mut scan => panic!("operation waited for the whole scan: {result:?}"),
        }
        assert_eq!(
            crate::ops::load(&b.state, &accepted.operation_id)
                .await
                .unwrap()
                .0
                .status,
            ism_core::dto::OperationStatus::Completed
        );
        scan.await.unwrap();
    })
    .await
    .expect("scan and operation must both make progress");

    assert_eq!(
        std::fs::read(b.shared.join("late.jpg")).unwrap(),
        b"replacement"
    );
    assert!(
        !peers::row(&b.state, &share.id, "late.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted,
        "an enumeration taken before the move is not evidence of deletion"
    );
    let checked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM replica_files WHERE share_id=? AND rel_path LIKE 'photo-%' AND deleted=0",
    )
    .bind(&share.id)
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    assert_eq!(checked, 1024);
}

#[tokio::test]
async fn cancelled_folder_does_not_prevent_another_folder_operation_for_the_same_peer() {
    let (a, b, peer, share_name) = pending_folder_fixture().await;
    peers::pull(&b.state, &peer).await.unwrap();
    let first = peers::activity::page(&b.state, "active", 50, 0, None)
        .await
        .unwrap()
        .rows
        .remove(0)
        .operation_id
        .unwrap();
    crate::ops::cancel(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        &first,
    )
    .await
    .unwrap();
    peers::append_local(
        &a.state,
        &share_name,
        "folder_created",
        &EventData {
            path: "independent-folder".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(peers::pull(&b.state, &peer).await.is_err());
    let pending = peers::activity::page(&b.state, "active", 50, 0, None)
        .await
        .unwrap();
    assert_eq!(pending.rows.len(), 2);
    let second = pending
        .rows
        .iter()
        .find(|row| row.relative_path == "independent-folder")
        .unwrap()
        .operation_id
        .as_ref()
        .unwrap();
    crate::ops::exec::run_operation(&b.state, second)
        .await
        .unwrap();
    assert!(b.shared.join("independent-folder").is_dir());
    assert!(!b.shared.join("new-folder").exists());
    // Originating peers are filtered out; inspect what a different replica could receive.
    let advertised = peers::events_for(&b.state, &peer.id, "observer-instance", 0)
        .await
        .unwrap();
    assert!(
        advertised
            .events
            .iter()
            .any(|event| event.data.path == "independent-folder")
    );
    assert!(
        !advertised
            .events
            .iter()
            .any(|event| event.data.path == "new-folder")
    );
    assert_eq!(
        peers::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
}

#[tokio::test]
async fn unresolved_server_barrier_does_not_protect_an_independent_server() {
    let (_a, b, _peer, _) = pending_folder_fixture().await;
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "scoped-barrier".into(),
            label: None,
            actions: vec![OperationAction::CreateFolder {
                target: loc("shared/uncertain"),
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    for (id, url) in [
        ("one", "https://one.example"),
        ("two", "https://two.example"),
    ] {
        sqlx::query("INSERT INTO immich_servers(id,name,url,maintenance_json,created_at) VALUES(?,?,?,'{}',?)")
            .bind(id).bind(id).bind(url).bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    }
    sqlx::query("UPDATE mappings SET server_id='one' WHERE root_id='photos'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let other = b._dir.path().join("other");
    std::fs::create_dir(&other).unwrap();
    std::fs::write(other.join(".ism-root"), "other\n").unwrap();
    sqlx::query("INSERT INTO mappings(root_id,name,container_path,server_id,created_at) VALUES('other','Other',?,'two',?)")
        .bind(other.to_string_lossy().as_ref()).bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    let family = peers::shares(&b.state).await.unwrap().remove(0);
    let independent = peers::Share {
        id: "independent".into(),
        name: "Independent".into(),
        root_id: "other".into(),
        path: ism_core::paths::RelPath::root(),
        peer_ids: vec![],
        enabled: true,
        junk: b.state.junk_rules(),
    };
    for admitted in ["[\"one\"]", "not-json", "{}"] {
        sqlx::query("UPDATE operations SET status='recovery_required',server_ids_json='[\"one\"]',admitted_server_ids_json=? WHERE id=?")
            .bind(admitted).bind(&accepted.operation_id).execute(&b.state.db).await.unwrap();
        assert_eq!(
            peers::require_stable_share(&b.state, &family, &["unrelated.jpg"])
                .await
                .unwrap_err()
                .code(),
            "recovery_required"
        );
        peers::require_stable_share(&b.state, &independent, &["available.jpg"])
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn unavailable_root_does_not_stop_receiving_another_share_from_the_same_peer() {
    let (a, b, peer, family) = pending_folder_fixture().await;
    let other = b._dir.path().join("other");
    std::fs::create_dir_all(&other).unwrap();
    std::fs::write(other.join(".ism-root"), "other\n").unwrap();
    sqlx::query(
        "INSERT INTO mappings(root_id,name,container_path,created_at) VALUES('other','Other',?,?)",
    )
    .bind(other.to_string_lossy().as_ref())
    .bind(crate::db::now())
    .execute(&b.state.db)
    .await
    .unwrap();
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Independent".into(),
            location: Location {
                root_id: "other".into(),
                path: String::new(),
            },
            peer_ids: vec![peer.id.clone()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    std::fs::create_dir(a.shared.parent().unwrap().join("independent")).unwrap();
    let remote_peer = peers::net::peers(&a.state).await.unwrap().remove(0);
    admin::create_share(
        &a.state,
        ShareInput {
            name: "Independent".into(),
            location: loc("independent"),
            peer_ids: vec![remote_peer.id],
            enabled: true,
        },
    )
    .await
    .unwrap();
    for (share, path) in [
        (family.as_str(), "blocked.jpg"),
        ("Independent", "available.jpg"),
    ] {
        peers::append_local(
            &a.state,
            share,
            "file_changed",
            &EventData {
                path: path.into(),
                sha256: Some("a".repeat(64)),
                size: Some(7),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    std::fs::remove_file(b.shared.parent().unwrap().join(".ism-root")).unwrap();
    peers::pull(&b.state, &peer).await.unwrap();
    let queued = admin::sync_status(&b.state).await.unwrap().queued;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].share_name, "Independent");
    assert_eq!(queued[0].location.path, "available.jpg");
    assert_eq!(
        peers::net::peer(&b.state, &peer.id).await.unwrap().cursor,
        peer.cursor
    );
}

#[tokio::test]
async fn backfill_defers_pending_folder_entries_without_losing_independent_inventory() {
    let (a, b, peer, _) = pending_folder_fixture().await;
    peers::pull(&b.state, &peer).await.unwrap();
    write_old(&a.shared.join("new-folder/child.jpg"), b"child");
    write_old(&a.shared.join("z-independent.jpg"), b"independent");
    scan(&a).await;
    let share = peers::shares(&b.state).await.unwrap().remove(0);
    let mut tx = b.state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
    peers::request_backfill_in(
        &mut tx,
        &b.state.instance_id,
        &share.id,
        &share.name,
        std::slice::from_ref(&peer.id),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    peers::backfill(&b.state, &peer).await.unwrap();
    let queued = admin::sync_status(&b.state).await.unwrap().queued;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].location.path, "shared/z-independent.jpg");
    let operation: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&b.state, &operation)
        .await
        .unwrap();
    peers::backfill(&b.state, &peer).await.unwrap();
    assert_eq!(admin::sync_status(&b.state).await.unwrap().queued.len(), 2);
}

#[tokio::test]
async fn pending_folder_defers_descendants_but_receives_independent_same_share_files() {
    let (a, b, peer, share_name) = pending_folder_fixture().await;
    peers::pull(&b.state, &peer).await.unwrap();
    for path in ["new-folder/child.jpg", "independent.jpg"] {
        peers::append_local(
            &a.state,
            &share_name,
            "file_changed",
            &EventData {
                path: path.into(),
                sha256: Some("a".repeat(64)),
                size: Some(7),
                vv: VersionVector::default(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    peers::pull(&b.state, &peer).await.unwrap();
    let status = admin::sync_status(&b.state).await.unwrap();
    assert_eq!(status.queued.len(), 1);
    assert_eq!(status.queued[0].location.path, "shared/independent.jpg");
    let advertised = peers::events_for(&b.state, &peer.id, "observer-instance", 0)
        .await
        .unwrap();
    assert!(advertised.events.iter().all(|event| {
        event.data.path != "new-folder"
            && event.data.path != "new-folder/child.jpg"
            && event.data.path != "independent.jpg"
    }));
    // A replay of the same receipt must neither lose the blocked child nor duplicate work.
    let restarted = AppState::new(
        Config::for_tests(b._dir.path()),
        crate::db::connect(b._dir.path()).await.unwrap(),
        SecretBox::open_or_create(b._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    peers::pull(&restarted, &peer).await.unwrap();
    assert_eq!(admin::sync_status(&b.state).await.unwrap().queued.len(), 1);
    let operation: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&b.state, &operation)
        .await
        .unwrap();
    let peer = peers::net::peer(&b.state, &peer.id).await.unwrap();
    peers::pull(&b.state, &peer).await.unwrap();
    assert_eq!(admin::sync_status(&b.state).await.unwrap().queued.len(), 2);
}

#[tokio::test]
async fn failed_folder_scan_keeps_uncertain_rows_but_publishes_independent_changes() {
    let (a, b, peer, _) = pending_folder_fixture().await;
    let share = peers::shares(&b.state).await.unwrap().remove(0);
    write_old(&b.shared.join("new-folder/tracked.jpg"), b"tracked");
    peers::scan_share(&b.state, &share).await.unwrap();
    std::fs::remove_dir_all(b.shared.join("new-folder")).unwrap();
    peers::pull(&b.state, &peer).await.unwrap();
    let operation: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&b.state.db)
        .await
        .unwrap();
    crate::ops::cancel(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        &operation,
    )
    .await
    .unwrap();
    write_old(&b.shared.join("independent.jpg"), b"independent");
    peers::scan_share(&b.state, &share).await.unwrap();
    assert!(
        !peers::row(&b.state, &share.id, "new-folder/tracked.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
    assert!(
        peers::row(&b.state, &share.id, "independent.jpg")
            .await
            .unwrap()
            .is_some()
    );
    let events = peers::events_for(&b.state, &peer.id, &a.state.instance_id, 0)
        .await
        .unwrap();
    assert!(
        events
            .events
            .iter()
            .any(|e| e.data.path == "independent.jpg")
    );
    assert!(
        !events
            .events
            .iter()
            .any(|e| e.kind == "file_deleted" && e.data.path == "new-folder/tracked.jpg")
    );
}

#[tokio::test]
async fn operation_waiting_for_maintenance_does_not_hold_share_scans() {
    use ism_core::maintenance::MaintenanceState;
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    let other = b._dir.path().join("other");
    std::fs::create_dir(&other).unwrap();
    std::fs::write(other.join(".ism-root"), "other\n").unwrap();
    // Server two's barrier is still being prepared, so admission keeps the operation waiting.
    let mut preparing = MaintenanceState::default();
    preparing.begin_prepare();
    sqlx::query("INSERT INTO immich_servers(id,name,url,database_url_enc,direct_writes_enabled,version,compatible,maintenance_json,created_at) VALUES('two','two','https://two.example','unused',1,'3.4.1',1,?,?)")
        .bind(serde_json::to_string(&preparing).unwrap()).bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    sqlx::query("INSERT INTO mappings(root_id,name,container_path,server_id,created_at) VALUES('other','Other',?,'two',?)")
        .bind(other.to_string_lossy().as_ref()).bind(crate::db::now()).execute(&b.state.db).await.unwrap();
    for (name, location) in [
        ("Family", loc("shared")),
        (
            "Independent",
            Location {
                root_id: "other".into(),
                path: String::new(),
            },
        ),
    ] {
        admin::create_share(
            &b.state,
            ShareInput {
                name: name.into(),
                location,
                peer_ids: vec![],
                enabled: true,
            },
        )
        .await
        .unwrap();
    }
    let shares = peers::shares(&b.state).await.unwrap();
    let family = shares.iter().find(|s| s.name == "Family").unwrap().clone();
    let independent = shares
        .iter()
        .find(|s| s.name == "Independent")
        .unwrap()
        .clone();
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "held-by-barrier".into(),
            label: None,
            actions: vec![OperationAction::CreateFolder {
                target: Location {
                    root_id: "other".into(),
                    path: "held".into(),
                },
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    // Peer bookkeeping for the independent share makes it sync-managed: it needs the barrier.
    sqlx::query("INSERT INTO pending_peer_ops(operation_id,idx,share_id,kind,vv_json,peer_id) VALUES(?,0,?,'folder_created','{}','peer-example')")
        .bind(&accepted.operation_id).bind(&independent.id).execute(&b.state.db).await.unwrap();
    write_old(&b.shared.join("independent.jpg"), b"photo");

    let operation = crate::ops::exec::run_operation(&b.state, &accepted.operation_id);
    tokio::pin!(operation);
    let gate = &b.state.replication_operation_lock;
    tokio::select! {
        result = &mut operation => panic!("operation was not held by the barrier: {result:?}"),
        () = async {
            loop {
                let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id = ?")
                    .bind(&accepted.operation_id).fetch_one(&b.state.db).await.unwrap();
                if status == "waiting_maintenance" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        } => {}
    }
    // Keep driving the held operation while scans run beside it; it has changed nothing yet.
    for share in [&family, &independent] {
        tokio::select! {
            result = &mut operation => panic!("operation was not held by the barrier: {result:?}"),
            scanned = tokio::time::timeout(Duration::from_secs(2), peers::scan_share(&b.state, share)) => {
                scanned.expect("a share scan waited for an operation that changed nothing").unwrap();
            }
        }
    }
    assert!(
        peers::row(&b.state, &family.id, "independent.jpg")
            .await
            .unwrap()
            .is_some()
    );
    tokio::select! {
        result = &mut operation => panic!("operation was not held by the barrier: {result:?}"),
        waited = tokio::time::timeout(Duration::from_millis(100), gate.lock()) => {
            assert!(waited.is_err(), "other bookkeeping must still wait");
        }
    }

    let mut blocked = MaintenanceState::default();
    blocked.block("test barrier");
    crate::immich::registry::save_maintenance(&b.state, "two", &blocked)
        .await
        .unwrap();
    operation.await.unwrap();
    let _admitted = tokio::time::timeout(Duration::from_secs(1), gate.lock())
        .await
        .unwrap();
}

#[tokio::test]
async fn active_operation_without_a_registration_fails_scans_closed() {
    let b = instance("Beta").await;
    for task in &b.tasks {
        task.abort();
    }
    admin::create_share(
        &b.state,
        ShareInput {
            name: "Family".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let family = peers::shares(&b.state).await.unwrap().remove(0);
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "orphaned".into(),
            label: None,
            actions: vec![OperationAction::CreateFolder {
                target: loc("elsewhere"),
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    let id = accepted.operation_id;
    sqlx::query("UPDATE operations SET status = 'running' WHERE id = ?")
        .bind(&id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let task_key = format!("share:{}", family.id);
    let (rows, _) = crate::activity_blockers::get(&b.state).await.unwrap();
    assert!(!rows.iter().any(|row| row.task_key == task_key));
    b.state
        .share_changes
        .lock()
        .unwrap()
        .insert(family.id.clone(), std::time::Instant::now());
    let (rows, _) = crate::activity_blockers::get(&b.state).await.unwrap();
    let queued = rows.iter().find(|row| row.task_key == task_key).unwrap();
    assert_eq!(
        queued.phase,
        "Share inventory rescan is queued; scan readiness has not been checked"
    );
    b.state.share_changes.lock().unwrap().remove(&family.id);

    // Either active phase left behind by a restart has no registration in this process.
    for status in ["waiting_maintenance", "running"] {
        sqlx::query("UPDATE operations SET status = ? WHERE id = ?")
            .bind(status)
            .bind(&id)
            .execute(&b.state.db)
            .await
            .unwrap();
        let scan =
            tokio::time::timeout(Duration::from_secs(2), peers::scan_share(&b.state, &family));
        let error = scan.await.expect("scan must not hang").unwrap_err();
        assert_eq!(error.code(), "recovery_required", "{status}: {error:?}");
    }

    // A panic while running drops the registration but leaves the row active.
    let state = b.state.clone();
    let task_id = id.clone();
    let panicked = tokio::spawn(async move {
        let gate = &state.replication_operation_lock;
        let admitted = gate.lock().await;
        let registration = gate.register(&admitted, &task_id);
        drop(admitted);
        registration.begin_mutating().await;
        panic!("synthetic operation panic");
    })
    .await;
    assert!(panicked.unwrap_err().is_panic());
    assert!(!b.state.replication_operation_lock.is_registered(&id));
    let scan = tokio::time::timeout(Duration::from_secs(2), peers::scan_share(&b.state, &family));
    let error = scan.await.expect("scan must not hang").unwrap_err();
    assert_eq!(error.code(), "recovery_required", "{error:?}");
}
