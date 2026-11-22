use super::*;
use crate::peers::{self, EventData};

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
