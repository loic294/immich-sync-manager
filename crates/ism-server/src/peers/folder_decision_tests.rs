//! Paired move decisions, including durable preparations and content publication.

use super::*;

async fn setup(folder: bool, destination: bool) -> (Instance, Instance, String) {
    setup_at(folder, destination, if folder { "old" } else { "old.jpg" }).await
}

async fn setup_at(folder: bool, destination: bool, from: &str) -> (Instance, Instance, String) {
    setup_paths(
        folder,
        destination,
        from,
        if folder { "new" } else { "new.jpg" },
    )
    .await
}

async fn setup_paths(
    folder: bool,
    destination: bool,
    from: &str,
    to: &str,
) -> (Instance, Instance, String) {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    if folder {
        write_old(
            &a.shared.join(format!("{from}/nested/photo.jpg")),
            b"local source",
        );
        write_old(
            &b.shared.join(format!("{to}/nested/photo.jpg")),
            b"remote source",
        );
        std::fs::create_dir_all(b.shared.join(format!("{to}/empty"))).unwrap();
        if destination {
            write_old(
                &a.shared.join(format!("{to}/occupied.jpg")),
                b"local destination",
            );
        }
    } else {
        write_old(&a.shared.join(from), b"local source");
        write_old(&b.shared.join(to), b"remote source");
        if destination {
            write_old(&a.shared.join(to), b"local destination");
        }
    }
    scan(&a).await;
    scan(&b).await;
    let share = super::super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::super::net::peers(&a.state).await.unwrap().remove(0);
    let remote = super::super::shares(&b.state).await.unwrap().remove(0);
    let row = super::super::row(&b.state, &remote.id, to).await.unwrap();
    let vv = row.as_ref().map(|r| r.vv.clone()).unwrap_or_default();
    super::super::conflict(
        &a.state,
        super::super::ConflictInput {
            share: &share,
            rel: to,
            kind: if folder { "folder_move" } else { "move" },
            detail: "Synthetic divergent move".into(),
            remote: ConflictSide {
                instance: "Beta".into(),
                sha256: row.as_ref().and_then(|r| r.sha256.clone()),
                size: row.as_ref().map(|r| r.size as u64),
                deleted: false,
                modified_at: None,
            },
            remote_vv: &vv,
            peer_id: &peer.id,
            from: Some(from),
        },
    )
    .await
    .unwrap();
    let id = sqlx::query_scalar("SELECT id FROM conflicts WHERE status='open'")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    let data = super::super::EventData {
        path: to.into(),
        from: Some(from.into()),
        vv,
        sha256: row.as_ref().and_then(|r| r.sha256.clone()),
        size: row.as_ref().map(|r| r.size as u64),
        ..Default::default()
    };
    super::super::append_local(
        &b.state,
        &remote.name,
        if folder { "folder_moved" } else { "file_moved" },
        &data,
    )
    .await
    .unwrap();
    let (seq, origin_seq): (i64, i64) =
        sqlx::query_as("SELECT seq,origin_seq FROM event_log ORDER BY seq DESC LIMIT 1")
            .fetch_one(&b.state.db)
            .await
            .unwrap();
    let event = super::super::PeerEvent {
        seq,
        origin: b.state.instance_id.clone(),
        origin_seq,
        share: remote.name,
        kind: if folder {
            "folder_moved".into()
        } else {
            "file_moved".into()
        },
        data,
    };
    sqlx::query(
        "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json,error)
         VALUES (?,?,?,?,?,?,'synthetic move conflict')",
    )
    .bind(&peer.id)
    .bind(seq)
    .bind(&event.origin)
    .bind(origin_seq)
    .bind(&event.share)
    .bind(serde_json::to_string(&event).unwrap())
    .execute(&a.state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE peers SET cursor=?,received_cursor=? WHERE id=?")
        .bind(seq - 1)
        .bind(seq)
        .bind(&peer.id)
        .execute(&a.state.db)
        .await
        .unwrap();
    (a, b, id)
}

async fn missing_folder() -> (Instance, Instance, String) {
    let (a, b, conflict) = setup(true, false).await;
    sqlx::query("DELETE FROM conflicts WHERE id=?")
        .bind(conflict)
        .execute(&a.state.db)
        .await
        .unwrap();
    let share = super::super::shares(&a.state).await.unwrap().remove(0);
    let peer = super::super::net::peers(&a.state).await.unwrap().remove(0);
    write_old(&a.shared.join("old/nested/photo.jpg"), b"remote source");
    std::fs::create_dir_all(a.shared.join("old/empty")).unwrap();
    let request = OperationRequest {
        idempotency_key: "synthetic-missing-folder".into(),
        label: None,
        actions: vec![OperationAction::MoveFolder {
            source: share.location("old").unwrap(),
            target: share.location("new").unwrap(),
        }],
        allow_owner_transfer: false,
        allow_partial: false,
    };
    let accepted = crate::ops::submit(
        &a.state,
        &Principal::Peer {
            peer_id: peer.id.clone(),
        },
        request,
    )
    .await
    .unwrap();
    std::fs::remove_dir_all(a.shared.join("old")).unwrap();
    sqlx::query(
            "INSERT INTO pending_peer_ops(operation_id,idx,share_id,kind,from_path,to_path,vv_json,peer_id)
             VALUES (?,0,?,'folder_move','old','new','{}',?)",
        )
        .bind(&accepted.operation_id)
        .bind(&share.id)
        .bind(&peer.id)
        .execute(&a.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE peer_inbox SET operation_id=?,status='applying',error=NULL")
        .bind(&accepted.operation_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&a.state, &accepted.operation_id)
        .await
        .unwrap();
    super::super::recovery::discover(&a.state).await.unwrap();
    (a, b, accepted.operation_id)
}

#[tokio::test]
async fn folder_decisions_reject_stale_empty_directory_evidence() {
    let (a, b, conflict) = setup(true, false).await;
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: conflict,
        peer_id: None,
    };
    for (instance, path) in [(&a, "old/new-empty"), (&b, "new/new-empty")] {
        let before = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        std::fs::create_dir_all(instance.shared.join(path)).unwrap();
        let after = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        assert_ne!(before.revision, after.revision);
        assert!(
            after
                .receiving
                .iter()
                .chain(std::iter::once(&after.source))
                .flat_map(|version| &version.directories)
                .any(|directory| directory == path)
        );
        let result = super::super::recovery::submit_decisions(
            &a.state,
            ism_core::dto::RecoveryDecisionInput {
                decisions: vec![ism_core::dto::RecoveryDecision {
                    target: target.clone(),
                    revision: before.revision,
                }],
                resolution: ConflictResolution::KeepRemote,
            },
        )
        .await
        .unwrap();
        assert_eq!(result.items[0].state, ism_core::dto::RecoveryState::Queued);
        super::super::recovery::tick(&a.state).await.unwrap();
        assert_eq!(
            super::super::recovery::get(&a.state, target.kind, &target.id)
                .await
                .unwrap()
                .unwrap()
                .code
                .as_deref(),
            Some("version_conflict")
        );
    }
}

#[tokio::test]
async fn recovery_missing_folder_restores_source_before_final_move() {
    let (mut a, mut b, operation) = missing_folder().await;
    write_old(
        &b.shared.join("new/nested/photo.jpg.xmp"),
        b"synthetic metadata",
    );
    write_old(&b.shared.join("independent.jpg"), b"independent transfer");
    scan(&b).await;
    assert!(
        super::super::admin::restore_missing_folder(&a.state, &operation)
            .await
            .unwrap(),
        "authenticated complete peer tree should restore the missing source"
    );
    assert!(!a.shared.join("new").exists());
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    for _ in 0..200 {
        let received: bool =
            sqlx::query_scalar("SELECT COUNT(*)=2 FROM uploads WHERE status='receiving'")
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if received {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let received: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM uploads WHERE status='receiving'")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(
        received, 2,
        "restoration must persist receiving uploads without child operations"
    );
    assert!(!a.shared.join("old").exists());
    assert!(!a.shared.join("new").exists());
    let early: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=?")
        .bind(&operation)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_ne!(
        early, "completed",
        "restoration prerequisites cannot complete the original move"
    );
    reboot(&mut a).await;
    restart(&mut a);
    restart(&mut b);
    for _ in 0..400 {
        let completed: bool =
            sqlx::query_scalar("SELECT status='completed' FROM operations WHERE id=?")
                .bind(&operation)
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if completed {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        std::fs::read(a.shared.join("new/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
    assert_eq!(
        std::fs::read(b.shared.join("new/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
    assert!(!a.shared.join("old").exists());
    assert!(a.shared.join("new/empty").is_dir());
    assert_eq!(
        std::fs::read(a.shared.join("new/nested/photo.jpg.xmp")).unwrap(),
        b"synthetic metadata"
    );
    assert_eq!(
        std::fs::read(b.shared.join("new/nested/photo.jpg.xmp")).unwrap(),
        b"synthetic metadata"
    );
    let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=?")
        .bind(&operation)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(status, "completed");
    eventually(
        "unrelated transfer progresses beside restoration",
        &[&a, &b],
        || {
            std::fs::read(a.shared.join("independent.jpg"))
                .is_ok_and(|bytes| bytes == b"independent transfer")
        },
    )
    .await;
}

#[tokio::test]
async fn recovery_missing_folder_unavailable_evidence_preserves_work_and_retries() {
    let (mut a, mut b, operation) = missing_folder().await;
    assert!(
        super::super::admin::restore_missing_folder(&a.state, &operation)
            .await
            .unwrap()
    );
    for task in b.tasks.drain(..) {
        task.abort();
        let _ = task.await;
    }
    let error = super::super::admin::validate_folder_restore(&a.state, &operation)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "peer_evidence_unavailable");
    assert!(!a.shared.join("old").exists());
    assert!(!a.shared.join("new").exists());
    let attempts: i64 = sqlx::query_scalar(
        "SELECT attempts FROM sync_recovery WHERE target_id=? AND kind='operation'",
    )
    .bind(&operation)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(attempts, 0);
    reboot(&mut b).await;
    restart(&mut a);
    for _ in 0..400 {
        if a.shared.join("new/nested/photo.jpg").is_file() {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        std::fs::read(a.shared.join("new/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
    assert_eq!(
        std::fs::read(b.shared.join("new/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
}

#[tokio::test]
async fn recovery_missing_folder_changed_peer_retains_received_version_for_new_decision() {
    let (mut a, b, operation) = missing_folder().await;
    assert!(
        super::super::admin::restore_missing_folder(&a.state, &operation)
            .await
            .unwrap()
    );
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    for _ in 0..200 {
        let received: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE status='receiving')")
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if received {
            break;
        }

        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    verify_changed_retained(&mut a, &b, &operation).await;
}

#[tokio::test]
async fn recovery_missing_folder_changed_version_is_executable_after_explicit_generation_choice() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let linked = choice != ConflictResolution::KeepBoth;
        changed_version_choice(choice, linked, None).await;
    }
}

#[tokio::test]
async fn recovery_missing_folder_accepts_changed_version_after_peer_relocation() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        changed_tree_choice(choice, true, false, None).await;
    }
}

#[tokio::test]
async fn recovery_missing_folder_accepts_changed_directory_structure() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        changed_tree_choice(choice, false, false, None).await;
    }
}

#[tokio::test]
async fn recovery_missing_folder_reopens_choice_after_another_peer_move() {
    changed_tree_choice(ConflictResolution::KeepBoth, true, true, None).await;
}

#[tokio::test]
async fn recovery_missing_folder_reopens_choice_if_restored_tree_changes() {
    for child_created in [false, true] {
        restored_tree_change_choice(child_created).await;
    }
}

async fn restored_tree_change_choice(child_created: bool) {
    let (mut a, mut b, operation) = missing_folder().await;
    admin::restore_missing_folder(&a.state, &operation)
        .await
        .unwrap();
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    for _ in 0..200 {
        let received: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE status='receiving')")
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if received {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop_workers(&mut a).await;
    write_old(&b.shared.join("new/added.jpg"), b"peer-only file");
    scan(&b).await;
    assert_eq!(
        admin::validate_folder_restore(&a.state, &operation)
            .await
            .unwrap_err()
            .code(),
        "choose_version"
    );
    let conflict: String = sqlx::query_scalar(
        "SELECT id FROM conflicts WHERE kind='folder_restore' AND status='open'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: conflict.clone(),
        peer_id: None,
    };
    let review = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    admin::resolve_reviewed(
        &a.state,
        "admin",
        &conflict,
        ConflictResolution::KeepBoth,
        Some(&review.revision),
    )
    .await
    .unwrap();
    assert!(
        crate::ops::exec::resume_recovered_folder_operation(&a.state, &operation)
            .await
            .unwrap()
    );
    if child_created {
        admin::process_folder_restorations(&a.state).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM conflicts WHERE kind='folder_move'")
                .fetch_one(&a.state.db)
                .await
                .unwrap(),
            1
        );
    }
    write_old(
        &a.shared.join("new/nested/photo.jpg"),
        b"changed receiving bytes",
    );
    admin::process_folder_restorations(&a.state).await.unwrap();
    assert_eq!(status(&a, &conflict).await, "open");
    let updated = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    assert_ne!(review.revision, updated.revision);
    super::super::recovery::submit_decisions(
        &a.state,
        ism_core::dto::RecoveryDecisionInput {
            decisions: vec![ism_core::dto::RecoveryDecision {
                target,
                revision: updated.revision,
            }],
            resolution: ConflictResolution::KeepBoth,
        },
    )
    .await
    .unwrap();
    restart(&mut a);
    restart(&mut b);
    a.tasks
        .push(tokio::spawn(super::super::recovery::run(a.state.clone())));
    resolved(&a, &conflict).await;
    eventually("re-reviewed versions converge", &[&a, &b], || {
        [&a, &b].iter().all(|instance| {
            instance.shared.join("new/added.jpg").is_file()
                && contains_bytes(instance, b"changed receiving bytes")
        })
    })
    .await;
}

#[tokio::test]
async fn recovery_missing_folder_reopens_choice_when_peer_moves_after_tree_child_is_created() {
    let (mut a, mut b, operation) = missing_folder().await;
    admin::restore_missing_folder(&a.state, &operation)
        .await
        .unwrap();
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    for _ in 0..200 {
        let received: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE status='receiving')")
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if received {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop_workers(&mut a).await;
    write_old(&b.shared.join("new/added.jpg"), b"peer-only file");
    scan(&b).await;
    relocate(&b).await;
    assert_eq!(
        admin::validate_folder_restore(&a.state, &operation)
            .await
            .unwrap_err()
            .code(),
        "choose_version"
    );
    let conflict: String = sqlx::query_scalar(
        "SELECT id FROM conflicts WHERE kind='folder_restore' AND status='open'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: conflict.clone(),
        peer_id: None,
    };
    let review = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    admin::resolve_reviewed(
        &a.state,
        "admin",
        &conflict,
        ConflictResolution::KeepBoth,
        Some(&review.revision),
    )
    .await
    .unwrap();
    assert!(
        crate::ops::exec::resume_recovered_folder_operation(&a.state, &operation)
            .await
            .unwrap()
    );
    admin::process_folder_restorations(&a.state).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM conflicts WHERE kind='folder_move'")
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
        1
    );
    move_folder(&b, "later", "final").await;
    admin::process_folder_restorations(&a.state).await.unwrap();
    assert_eq!(status(&a, &conflict).await, "open");
    let updated = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    admin::resolve_reviewed(
        &a.state,
        "admin",
        &conflict,
        ConflictResolution::KeepBoth,
        Some(&updated.revision),
    )
    .await
    .unwrap();
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &conflict).await;
    eventually("refreshed relocated tree converges", &[&a, &b], || {
        [&a, &b].iter().all(|instance| {
            instance.shared.join("final/added.jpg").is_file()
                && contains_bytes(instance, b"remote source")
        })
    })
    .await;
}

async fn changed_tree_choice(
    choice: ConflictResolution,
    relocate_peer: bool,
    relocate_again: bool,
    database_url: Option<&str>,
) {
    let (mut a, mut b, operation) = missing_folder().await;
    let immich = if let Some(url) = database_url {
        Some(linked_restore(&a, &b, &operation, Some(url)).await)
    } else {
        None
    };
    admin::restore_missing_folder(&a.state, &operation)
        .await
        .unwrap();
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    for _ in 0..200 {
        let received: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE status='receiving')")
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if received {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop_workers(&mut a).await;
    write_old(
        &b.shared.join("new/nested/photo.jpg"),
        b"changed current bytes",
    );
    write_old(&b.shared.join("new/added.jpg"), b"added current file");
    std::fs::create_dir_all(b.shared.join("new/added-empty")).unwrap();
    scan(&b).await;
    if relocate_peer {
        relocate(&b).await;
    }
    assert_eq!(
        admin::validate_folder_restore(&a.state, &operation)
            .await
            .unwrap_err()
            .code(),
        "choose_version"
    );
    let conflict: String = sqlx::query_scalar(
        "SELECT id FROM conflicts WHERE kind='folder_restore' AND status='open'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: conflict.clone(),
        peer_id: None,
    };
    let versions = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    let remote_path = if relocate_peer { "later" } else { "new" };
    assert_eq!(versions.source.path, remote_path);
    let result = super::super::recovery::submit_decisions(
        &a.state,
        ism_core::dto::RecoveryDecisionInput {
            decisions: vec![ism_core::dto::RecoveryDecision {
                target: target.clone(),
                revision: versions.revision.clone(),
            }],
            resolution: choice,
        },
    )
    .await
    .unwrap();
    assert_ne!(
        result.items[0].state,
        ism_core::dto::RecoveryState::Waiting,
        "{:?}",
        result.items[0]
    );
    if relocate_again {
        admin::resolve_reviewed(
            &a.state,
            "admin",
            &conflict,
            choice,
            Some(&versions.revision),
        )
        .await
        .unwrap();
        move_folder(&b, "later", "final").await;
        assert_eq!(
            admin::validate_folder_restore(&a.state, &operation)
                .await
                .unwrap_err()
                .code(),
            "choose_version"
        );
        assert_eq!(status(&a, &conflict).await, "open");
        let updated = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        assert_eq!(updated.source.path, "final");
        assert_ne!(updated.revision, versions.revision);
        super::super::recovery::submit_decisions(
            &a.state,
            ism_core::dto::RecoveryDecisionInput {
                decisions: vec![ism_core::dto::RecoveryDecision {
                    target,
                    revision: updated.revision,
                }],
                resolution: choice,
            },
        )
        .await
        .unwrap();
    }
    let remote_path = if relocate_again { "final" } else { remote_path };
    reboot(&mut a).await;
    restart(&mut a);
    restart(&mut b);
    a.tasks
        .push(tokio::spawn(super::super::recovery::run(a.state.clone())));
    resolved(&a, &conflict).await;
    eventually("approved tree versions converge", &[&a, &b], || {
        [&a, &b].iter().all(|instance| {
            let selected_path = if choice == ConflictResolution::KeepLocal {
                "new"
            } else {
                remote_path
            };
            let expected = if choice == ConflictResolution::KeepLocal {
                b"remote source".as_slice()
            } else {
                b"changed current bytes".as_slice()
            };
            std::fs::read(
                instance
                    .shared
                    .join(format!("{selected_path}/nested/photo.jpg")),
            )
            .is_ok_and(|bytes| bytes == expected)
                && (choice == ConflictResolution::KeepLocal
                    || instance
                        .shared
                        .join(format!("{selected_path}/added.jpg"))
                        .is_file()
                        && instance
                            .shared
                            .join(format!("{selected_path}/added-empty"))
                            .is_dir())
                && (choice != ConflictResolution::KeepBoth
                    || contains_bytes(instance, b"remote source"))
        })
    })
    .await;
    if let Some((server, assets)) = immich {
        let retained = assets.lock().unwrap()["asset-1"].clone();
        assert!(retained.contains("conflict"));
        let requests = server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.url.path() == "/api/queues/library"
                && request.method.as_str() == "PUT"
                && serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["isPaused"]
                    == true
        }));
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database named ismtest"]
async fn disposable_linked_relocated_restoration_preserves_both_asset_versions() {
    let url = std::env::var("ISM_DISPOSABLE_IMMICH_DATABASE_URL")
        .expect("set the disposable proof database URL");
    assert!(url.ends_with("/ismtest"), "use only the proof database");
    let pool = crate::immich::pg::connect(&url).await.unwrap();
    sqlx::query(
        r#"CREATE TABLE asset (
            id text PRIMARY KEY, "ownerId" text NOT NULL, "libraryId" text,
            "originalPath" text NOT NULL, "livePhotoVideoId" text, "stackId" text,
            "duplicateId" text, "deletedAt" timestamptz
        )"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"CREATE TABLE asset_file (id text PRIMARY KEY,"assetId" text,path text,type text)"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO asset(id,"ownerId","libraryId","originalPath")
           VALUES('asset-1','owner-1','library-1','/library/shared/new/nested/photo.jpg')"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    changed_tree_choice(ConflictResolution::KeepBoth, true, false, Some(&url)).await;
    sqlx::query("DROP TABLE asset_file,asset")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database named ismtest"]
async fn disposable_linked_folder_restoration_keep_both_preserves_asset() {
    let url = std::env::var("ISM_DISPOSABLE_IMMICH_DATABASE_URL")
        .expect("set the disposable proof database URL");
    assert!(url.ends_with("/ismtest"), "use only the proof database");
    let pool = crate::immich::pg::connect(&url).await.unwrap();
    sqlx::query(
        r#"CREATE TABLE asset (
            id text PRIMARY KEY, "ownerId" text NOT NULL, "libraryId" text,
            "originalPath" text NOT NULL, "livePhotoVideoId" text, "stackId" text,
            "duplicateId" text, "deletedAt" timestamptz
        )"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO asset(id,"ownerId","libraryId","originalPath")
           VALUES('asset-1','owner-1','library-1','/library/shared/new/nested/photo.jpg')"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    changed_version_choice(ConflictResolution::KeepBoth, true, Some(&url)).await;
    sqlx::query("DROP TABLE asset")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database named ismtest"]
async fn disposable_linked_overlapping_keep_both_moves_existing_asset_under_barrier() {
    let url = std::env::var("ISM_DISPOSABLE_IMMICH_DATABASE_URL")
        .expect("set the disposable proof database URL");
    assert!(url.ends_with("/ismtest"), "use only the proof database");
    let pool = crate::immich::pg::connect(&url).await.unwrap();
    sqlx::query(
        r#"CREATE TABLE asset (
            id text PRIMARY KEY, "ownerId" text NOT NULL, "libraryId" text,
            "originalPath" text NOT NULL, "livePhotoVideoId" text, "stackId" text,
            "duplicateId" text, "deletedAt" timestamptz
        )"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"CREATE TABLE asset_file (id text PRIMARY KEY,"assetId" text,path text,type text)"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO asset(id,"ownerId","libraryId","originalPath")
           VALUES('asset-1','owner-1','library-1','/library/shared/old/nested/photo.jpg')"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let (a, b, id) = setup(true, false).await;
    let (server, assets) = linked_restore(&a, &b, "no-existing-operation", Some(&url)).await;
    sqlx::query("UPDATE immich_servers SET version='3.4.1',maintenance_json=? WHERE id='server-1'")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .execute(&a.state.db)
        .await
        .unwrap();
    std::fs::create_dir_all(b.shared.join("old")).unwrap();
    relocate_to(&b, "old/inside").await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
        .await
        .unwrap();
    preparations(&a).await;
    let asset_path = assets.lock().unwrap().get("asset-1").cloned().unwrap();
    assert!(asset_path.starts_with("/library/shared/old (conflict "));
    assert!(asset_path.ends_with("/nested/photo.jpg"));
    let requests = server.received_requests().await.unwrap();
    let move_index = requests
        .iter()
        .position(|request| request.url.path() == "/api/libraries/library-1/files/move")
        .unwrap();
    assert!(requests[..move_index].iter().any(|request| {
        request.url.path() == "/api/queues/library"
            && request.method.as_str() == "PUT"
            && serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["isPaused"]
                == true
    }));
    assert!(requests[move_index + 1..].iter().any(|request| {
        request.url.path() == "/api/queues/library"
            && request.method.as_str() == "PUT"
            && serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["isPaused"]
                == false
    }));
    assert_eq!(
        crate::immich::registry::server(&a.state, "server-1")
            .await
            .unwrap()
            .maintenance
            .phase,
        ism_core::maintenance::Phase::Idle
    );
    assert!(contains_bytes(&a, b"local source"));
    sqlx::query("DROP TABLE asset_file,asset")
        .execute(&pool)
        .await
        .unwrap();
}

async fn changed_version_choice(
    choice: ConflictResolution,
    linked: bool,
    database_url: Option<&str>,
) {
    let (mut a, mut b, operation) = missing_folder().await;
    let immich = if linked {
        Some(linked_restore(&a, &b, &operation, database_url).await)
    } else {
        None
    };
    admin::restore_missing_folder(&a.state, &operation)
        .await
        .unwrap();
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    for _ in 0..200 {
        let received: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM uploads WHERE status='receiving')")
                .fetch_one(&a.state.db)
                .await
                .unwrap();
        if received {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop_workers(&mut a).await;
    write_old(
        &b.shared.join("new/nested/photo.jpg"),
        b"changed current bytes",
    );
    scan(&b).await;
    assert_eq!(
        admin::validate_folder_restore(&a.state, &operation)
            .await
            .unwrap_err()
            .code(),
        "choose_version"
    );
    let conflict: String = sqlx::query_scalar(
        "SELECT id FROM conflicts WHERE kind='folder_restore' AND status='open'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: conflict.clone(),
        peer_id: None,
    };
    let versions = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    assert_eq!(versions.source.state, "folder");
    assert_eq!(versions.source.path, "new");
    assert_eq!(versions.receiving[0].state, "folder");
    assert_eq!(versions.receiving[0].files[0].path, "old/nested/photo.jpg");
    assert_eq!(versions.source.files[0].path, "new/nested/photo.jpg");
    assert!(versions.source.files[0].modified_at.is_some());
    assert_ne!(
        versions.source.files[0].sha256,
        versions.receiving[0].files[0].sha256
    );
    super::super::recovery::submit_decisions(
        &a.state,
        ism_core::dto::RecoveryDecisionInput {
            decisions: vec![ism_core::dto::RecoveryDecision {
                target,
                revision: versions.revision,
            }],
            resolution: choice,
        },
    )
    .await
    .unwrap();
    let original_status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=?")
        .bind(&operation)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_ne!(original_status, "completed");
    assert!(!a.shared.join("new").exists());
    let recovery_status: String = sqlx::query_scalar(
        "SELECT state FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(&operation)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_ne!(recovery_status, "fixed");
    reboot(&mut a).await;
    restart(&mut a);
    a.tasks
        .push(tokio::spawn(super::super::recovery::run(a.state.clone())));
    restart(&mut b);
    resolved(&a, &conflict).await;
    eventually(
        "approved restoration bytes converge on both peers",
        &[&a, &b],
        || {
            [&a, &b].iter().all(|instance| {
                !instance.shared.join("old").exists()
                    && instance.shared.join("new/empty").is_dir()
                    && std::fs::read(instance.shared.join("new/nested/photo.jpg")).is_ok_and(
                        |bytes| {
                            bytes
                                == if choice == ConflictResolution::KeepLocal {
                                    b"remote source".as_slice()
                                } else {
                                    b"changed current bytes".as_slice()
                                }
                        },
                    )
                    && (choice != ConflictResolution::KeepBoth
                        || contains_bytes(instance, b"remote source"))
            })
        },
    )
    .await;
    assert!(contains_bytes(&a, b"remote source"));
    assert!(contains_bytes(&b, b"changed current bytes"));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE id=?")
            .bind(&operation)
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
        "completed"
    );
    if let Some((immich, assets)) = immich {
        let retained_original = assets.lock().unwrap().get("asset-1").cloned().unwrap();
        if choice == ConflictResolution::KeepBoth {
            assert!(retained_original.contains("conflict"));
        } else {
            assert_eq!(retained_original, "/library/shared/new/nested/photo.jpg");
            assert_eq!(assets.lock().unwrap().len(), 1);
        }
        assert!(
            immich
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| !request.url.path().ends_with("/files/remove"))
        );
    }
    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    stop_workers(&mut a).await;
    reboot(&mut a).await;
    for _ in 0..3 {
        admin::process_folder_restorations(&a.state).await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_log")
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
        events
    );
}

async fn verify_changed_retained(a: &mut Instance, b: &Instance, operation: &str) {
    stop_workers(a).await;
    let upload: String = sqlx::query_scalar("SELECT id FROM uploads WHERE status='receiving'")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    write_old(
        &b.shared.join("new/nested/photo.jpg"),
        b"changed current bytes",
    );
    scan(b).await;
    let error = super::super::admin::validate_folder_restore(&a.state, operation)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "choose_version");
    let share = super::super::shares(&a.state).await.unwrap().remove(0);
    let root = crate::files::root(&a.state.db, &share.root_id)
        .await
        .unwrap();
    let _ = crate::ops::plan::staging_dir(&root, &upload).unwrap();
    assert_eq!(
        std::fs::read(a.shared.join("old/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
    assert_eq!(
        std::fs::read(b.shared.join("new/nested/photo.jpg")).unwrap(),
        b"changed current bytes"
    );
    assert!(a.shared.join("old").is_dir());
    assert!(!a.shared.join("new").exists());
    let state: String = sqlx::query_scalar(
        "SELECT state FROM sync_recovery WHERE target_id=? AND kind='operation'",
    )
    .bind(operation)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(state, "needs_decision");
    let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=?")
        .bind(operation)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_ne!(status, "completed");
}
type SyntheticAssets = std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, String>>>;

async fn linked_restore(
    instance: &Instance,
    peer: &Instance,
    operation: &str,
    database_url: Option<&str>,
) -> (wiremock::MockServer, SyntheticAssets) {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };
    let server = MockServer::start().await;
    let queue_paused = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    Mock::given(method("GET"))
        .and(path("/api/admin/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "library":{"watch":{"enabled":false},"scan":{"enabled":false}}
        })))
        .mount(&server)
        .await;
    let get_queue = queue_paused.clone();
    Mock::given(method("GET"))
        .and(path("/api/queues"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "name":"library",
                "isPaused":get_queue.load(std::sync::atomic::Ordering::SeqCst),
                "statistics":{"active":0,"waiting":0,"delayed":0,"paused":0}
            }]))
        })
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/queues/library"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            queue_paused.store(
                body["isPaused"].as_bool().unwrap(),
                std::sync::atomic::Ordering::SeqCst,
            );
            ResponseTemplate::new(200).set_body_json(serde_json::json!({}))
        })
        .mount(&server)
        .await;
    let assets: SyntheticAssets =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::from([(
            "asset-1".into(),
            "/library/shared/old/nested/photo.jpg".into(),
        )])));
    let search_assets = assets.clone();
    Mock::given(method("POST"))
        .and(path("/api/search/metadata"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let entries: Vec<serde_json::Value> = search_assets
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, source)| Some(source.as_str()) == body["originalPath"].as_str())
                .map(|(id, source)| serde_json::json!({"id":id,"originalPath":source}))
                .collect();
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"assets":{"items":entries}}))
        })
        .mount(&server)
        .await;
    for id in ["asset-1", "asset-2"] {
        let get_assets = assets.clone();
        Mock::given(method("GET"))
            .and(path(format!("/api/assets/{id}")))
            .respond_with(move |_: &wiremock::Request| {
                let assets = get_assets.lock().unwrap();
                if let Some(source) = assets.get(id) {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "id":id,"originalPath":source,"ownerId":"owner-1",
                        "libraryId":"library-1","isTrashed":false,
                    }))
                } else {
                    ResponseTemplate::new(404)
                }
            })
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/api/users/me"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id":"owner-1","isAdmin":true})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/api-keys/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"id":"synthetic-key","permissions":["library.update"]}),
        ))
        .mount(&server)
        .await;
    let moved_assets = assets.clone();
    Mock::given(method("POST"))
        .and(path("/api/libraries/library-1/files/move"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let mut assets = moved_assets.lock().unwrap();
            let id = body["assetId"].as_str().unwrap();
            assert_eq!(
                assets.get(id).map(String::as_str),
                body["sourcePath"].as_str()
            );
            assets.insert(id.into(), body["destinationPath"].as_str().unwrap().into());
            ResponseTemplate::new(200).set_body_json(serde_json::json!({}))
        })
        .mount(&server)
        .await;
    let added_assets = assets.clone();
    Mock::given(method("POST"))
        .and(path("/api/libraries/library-1/files/add"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let source = body["path"].as_str().unwrap();
            let mut assets = added_assets.lock().unwrap();
            if !assets.values().any(|path| path == source) {
                let id = format!("asset-{}", assets.len() + 1);
                assets.insert(id, source.into());
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({}))
        })
        .mount(&server)
        .await;
    let maintenance = ism_core::maintenance::MaintenanceState {
        phase: ism_core::maintenance::Phase::RecoveryRequired,
        unresolved_recoveries: 1,
        queue_paused_by_ism: true,
        ..Default::default()
    };
    sqlx::query(
        "INSERT INTO immich_servers(id,name,url,database_url_enc,compatible,external_library_move_mode,maintenance_json,created_at)
         VALUES('server-1','Synthetic Immich',?,?,1,'api_only',?,?)",
    ).bind(server.uri())
        .bind(instance.state.secrets.encrypt(crate::immich::registry::DATABASE_URL_PURPOSE,
            database_url.unwrap_or("postgres://test:test@db.example/immich")).unwrap())
        .bind(serde_json::to_string(&maintenance).unwrap())
        .bind(crate::db::now()).execute(&instance.state.db).await.unwrap();
    sqlx::query(
        "INSERT INTO immich_accounts(id,server_id,label,email,immich_user_id,is_admin,api_key_enc,status,created_at)
         VALUES('account-1','server-1','Synthetic user','synthetic@account.example','owner-1',1,?,'ok',?)",
    ).bind(instance.state.secrets.encrypt(crate::immich::registry::API_KEY_PURPOSE, "synthetic-key").unwrap())
        .bind(crate::db::now()).execute(&instance.state.db).await.unwrap();
    sqlx::query(
        "INSERT INTO immich_libraries(id,server_id,name,owner_user_id,import_paths_json,refreshed_at)
         VALUES('library-1','server-1','Synthetic library','owner-1','[\"/library\"]',?)",
    ).bind(crate::db::now()).execute(&instance.state.db).await.unwrap();
    sqlx::query(
        "UPDATE mappings SET server_id='server-1',library_id='library-1',immich_import_path='/library' WHERE root_id='photos'",
    ).execute(&instance.state.db).await.unwrap();
    sqlx::query("UPDATE operations SET admitted_server_ids_json='[\"server-1\"]',status='recovery_required' WHERE id=?")
        .bind(operation)
        .execute(&instance.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE operation_items SET status='recovery_required' WHERE operation_id=?")
        .bind(operation)
        .execute(&instance.state.db)
        .await
        .unwrap();
    let share = super::super::shares(&instance.state)
        .await
        .unwrap()
        .remove(0);
    let root = crate::files::root(&instance.state.db, &share.root_id)
        .await
        .unwrap();
    let source = share.location("old").unwrap();
    let target = share.location("new").unwrap();
    let movement = crate::ops::plan::ApiStep {
        server_id: "server-1".into(),
        owner: "owner-1".into(),
        ids: vec!["asset-1".into()],
        kind: crate::ops::plan::ApiKind::SyncMove {
            source_library_id: "library-1".into(),
            destination_library_id: None,
            source_path: "/library/shared/old/nested/photo.jpg".into(),
            destination_path: "/library/shared/new/nested/photo.jpg".into(),
            asset_id: Some("asset-1".into()),
            from_owner: None,
            to_owner: None,
        },
    };
    let source_abs = root
        .abs(&crate::files::parse_rel(&source.path).unwrap())
        .unwrap();
    let target_abs = root
        .abs(&crate::files::parse_rel(&target.path).unwrap())
        .unwrap();
    let journal = serde_json::json!({
        "steps":[format!("move {} -> {}",source_abs.display(),target_abs.display()),"immich original asset"],
        "execution_steps":[
            crate::ops::plan::Step::Fs(crate::ops::plan::FsMove {
                from:root.abs(&crate::files::parse_rel(&source.path).unwrap()).unwrap(),
                to:root.abs(&crate::files::parse_rel(&target.path).unwrap()).unwrap(),
                copy:false,
            }),
            crate::ops::plan::Step::Api(movement.clone()),
        ],
        "verification":[{
            "location":target,
            "inventory":crate::ops::plan::folder_inventory(&peer.shared.join("new")).unwrap(),
        }],
        "api_effects":[[1,movement]],
        "db_effects":[],
    });
    let progress = ism_core::dto::OperationTaskProgress {
        files: vec![ism_core::dto::PlannedFile {
            from: Some(source),
            to: Some(target),
            role: ism_core::dto::FileRole::Original,
            copy: false,
        }],
        steps: vec![
            ism_core::dto::OperationStep {
                label: "Move original folder".into(),
                status: "failed".into(),
            },
            ism_core::dto::OperationStep {
                label: "Move original asset".into(),
                status: "pending".into(),
            },
            ism_core::dto::OperationStep {
                label: "Record changes".into(),
                status: "pending".into(),
            },
        ],
    };
    sqlx::query("UPDATE operation_items SET journal_json=?,progress_json=? WHERE operation_id=?")
        .bind(journal.to_string())
        .bind(serde_json::to_string(&progress).unwrap())
        .bind(operation)
        .execute(&instance.state.db)
        .await
        .unwrap();
    (server, assets)
}

async fn reboot(instance: &mut Instance) {
    for task in instance.tasks.drain(..) {
        task.abort();
        let _ = task.await;
    }
    let state = AppState::new(
        Config::for_tests(instance._dir.path()),
        crate::db::connect(instance._dir.path()).await.unwrap(),
        SecretBox::open_or_create(instance._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind(instance.url.trim_start_matches("http://"))
        .await
        .unwrap();
    let app = crate::routes::router(state.clone());
    instance.tasks.push(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    instance.state = state;
}

async fn preparations(instance: &Instance) {
    for _ in 0..100 {
        let next: Option<String> = sqlx::query_scalar(
            "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step' LIMIT 1",
        )
        .fetch_optional(&instance.state.db)
        .await
        .unwrap();
        let Some(operation) = next else { return };
        crate::ops::exec::run_operation(&instance.state, &operation)
            .await
            .unwrap();
        let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=?")
            .bind(&operation)
            .fetch_one(&instance.state.db)
            .await
            .unwrap();
        assert_eq!(status, "completed", "{operation}");
    }
    panic!("preparations did not finish");
}

async fn status(instance: &Instance, id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
        .bind(id)
        .fetch_one(&instance.state.db)
        .await
        .unwrap()
}

fn restart(instance: &mut Instance) {
    instance.tasks.extend([
        tokio::spawn(crate::maintenance::run(instance.state.clone())),
        tokio::spawn(crate::ops::exec::run(instance.state.clone())),
        tokio::spawn(super::super::run(instance.state.clone())),
        tokio::spawn(super::super::transfer::run(instance.state.clone())),
        tokio::spawn(super::super::scan_worker(instance.state.clone())),
    ]);
}

async fn resolved(instance: &Instance, id: &str) {
    for _ in 0..400 {
        if status(instance, id).await == "resolved" {
            return;
        }
        instance.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let ops: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT o.id,o.status,i.error_message FROM operations o LEFT JOIN operation_items i ON i.operation_id=o.id")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let transfers: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT rel_path,status,error FROM transfers")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let pending: Vec<(String, String)> =
        sqlx::query_as("SELECT kind,vv_json FROM pending_peer_ops")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let rows: Vec<(String, Option<String>, i64, String)> =
        sqlx::query_as("SELECT rel_path,sha256,deleted,vv_json FROM replica_files")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let result = admin::process_move_decisions(&instance.state).await;
    let conflicts: Vec<(String, String, String)> =
        sqlx::query_as("SELECT rel_path,status,detail FROM conflicts")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let history: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT rel_path,operation_id FROM transfer_history")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let mut restoration = Vec::new();
    for (kind, raw) in &pending {
        if kind == "folder_restore" {
            let plan: serde_json::Value = serde_json::from_str(raw).unwrap();
            restoration.push(
                crate::ops::exec::resume_recovered_folder_operation(
                    &instance.state,
                    plan["operation_id"].as_str().unwrap(),
                )
                .await,
            );
        }
    }
    panic!(
        "decision did not finish: operations={ops:?} transfers={transfers:?} pending={pending:?} rows={rows:?} result={result:?} conflicts={conflicts:?} history={history:?} restoration={restoration:?}"
    );
}

fn contains_bytes(instance: &Instance, bytes: &[u8]) -> bool {
    walkdir::WalkDir::new(instance._dir.path())
        .into_iter()
        .filter_map(Result::ok)
        .any(|entry| {
            entry.file_type().is_file()
                && std::fs::read(entry.path()).is_ok_and(|actual| actual == bytes)
        })
}

#[tokio::test]
async fn folder_keep_remote_preserves_additions_after_review() {
    preserves_addition(false, false).await;
    preserves_addition(false, true).await;
}

#[tokio::test]
async fn folder_keep_remote_preserves_empty_directories_after_review() {
    preserves_addition(true, false).await;
    preserves_addition(true, true).await;
}

async fn preserves_addition(directory: bool, destination: bool) {
    let (mut a, _b, id) = setup(true, destination).await;
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: id.clone(),
        peer_id: None,
    };
    let review = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    admin::resolve_reviewed(
        &a.state,
        "admin",
        &id,
        ConflictResolution::KeepRemote,
        Some(&review.revision),
    )
    .await
    .unwrap();
    if destination {
        let operation: String = sqlx::query_scalar(
            "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step'",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        crate::ops::exec::run_operation(&a.state, &operation)
            .await
            .unwrap();
    }
    let parent = if destination { "new" } else { "old" };
    let addition = a.shared.join(format!(
        "{parent}/{}",
        if directory { "new-empty" } else { "new.jpg" }
    ));
    if directory {
        std::fs::create_dir_all(&addition).unwrap();
    } else {
        write_old(&addition, b"unreviewed addition");
    }
    reboot(&mut a).await;
    let operation: String = sqlx::query_scalar(
        "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    crate::ops::exec::run_operation(&a.state, &operation)
        .await
        .unwrap();
    assert!(
        addition.exists(),
        "unreviewed addition moved to trash: {directory}"
    );
    assert_eq!(status(&a, &id).await, "open");
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT recovery_resolved FROM operations WHERE id=?")
            .bind(&operation)
            .fetch_one(&a.state.db)
            .await
            .unwrap()
    );
    if destination {
        assert!(a.shared.join("new/occupied.jpg").is_file());
        assert!(contains_bytes(&a, b"local source"));
    } else {
        assert!(a.shared.join("old/nested/photo.jpg").is_file());
    }
    let updated = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    assert_ne!(review.revision, updated.revision);
    admin::resolve_reviewed(
        &a.state,
        "admin",
        &id,
        ConflictResolution::KeepRemote,
        Some(&updated.revision),
    )
    .await
    .unwrap();
    preparations(&a).await;
    assert!(contains_bytes(&a, b"local source"));
    if !directory {
        assert!(contains_bytes(&a, b"unreviewed addition"));
    }
}

#[tokio::test]
async fn folder_keep_remote_rejects_additions_before_queuing() {
    for directory in [false, true] {
        let (a, _b, id) = setup(true, false).await;
        let target = ism_core::dto::RecoveryTarget {
            kind: ism_core::dto::RecoveryKind::Conflict,
            id: id.clone(),
            peer_id: None,
        };
        let review = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        let addition = a.shared.join(if directory {
            "old/added-empty"
        } else {
            "old/added.jpg"
        });
        if directory {
            std::fs::create_dir(&addition).unwrap();
        } else {
            write_old(&addition, b"new content");
        }
        let error = admin::resolve_reviewed(
            &a.state,
            "admin",
            &id,
            ConflictResolution::KeepRemote,
            Some(&review.revision),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "version_conflict");
        assert!(addition.exists());
        assert!(a.shared.join("old/nested/photo.jpg").is_file());
        assert_eq!(status(&a, &id).await, "open");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pending_peer_ops WHERE kind='move_decision_step'"
            )
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn folder_keep_remote_reopens_for_changed_reviewed_contents() {
    for remove_directory in [false, true] {
        let (a, _b, id) = setup(true, false).await;
        std::fs::create_dir(a.shared.join("old/reviewed-empty")).unwrap();
        admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
            .await
            .unwrap();
        if remove_directory {
            std::fs::remove_dir(a.shared.join("old/reviewed-empty")).unwrap();
        } else {
            // Same size and write_old's fixed mtime: content hashes must also be pinned.
            write_old(&a.shared.join("old/nested/photo.jpg"), b"other source");
        }
        let operation: String = sqlx::query_scalar(
            "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step'",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        crate::ops::exec::run_operation(&a.state, &operation)
            .await
            .unwrap();
        assert_eq!(status(&a, &id).await, "open");
        assert!(a.shared.join("old/nested").is_dir());
        assert_eq!(
            std::fs::read(a.shared.join("old/nested/photo.jpg")).unwrap(),
            if remove_directory {
                b"local source"
            } else {
                b"other source"
            }
        );
    }
}

#[tokio::test]
async fn folder_keep_remote_rejects_legacy_or_incomplete_queued_evidence() {
    for field in ["$.reviewed_trees", "$.reviewed_trees.\"shared/old\""] {
        let (a, _b, id) = setup(true, false).await;
        admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
            .await
            .unwrap();
        let operation: String = sqlx::query_scalar(
            "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step'",
        )
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE pending_peer_ops SET vv_json=json_remove(vv_json,?) WHERE operation_id=?",
        )
        .bind(field)
        .bind(&operation)
        .execute(&a.state.db)
        .await
        .unwrap();
        crate::ops::exec::run_operation(&a.state, &operation)
            .await
            .unwrap();
        assert!(a.shared.join("old/nested/photo.jpg").is_file());
        assert_eq!(status(&a, &id).await, "open");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT error_code FROM operation_items WHERE operation_id=?"
            )
            .bind(&operation)
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
            "recovery_evidence_missing"
        );
    }
}

#[tokio::test]
async fn folder_keep_remote_rebuilds_nested_tree_and_retains_trash_after_restart() {
    let (mut a, mut b, id) = setup(true, true).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    assert_eq!(status(&a, &id).await, "resolving");
    reboot(&mut a).await;
    preparations(&a).await;
    assert_eq!(status(&a, &id).await, "resolving");
    assert!(!a.shared.join("old").exists());
    assert!(!a.shared.join("new/nested/photo.jpg").exists());
    assert!(contains_bytes(&a, b"local source"));
    assert!(contains_bytes(&a, b"local destination"));
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually("same chosen tree on both peers", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("new/nested/photo.jpg"))
                .is_ok_and(|b| b == b"remote source")
                && i.shared.join("new/empty").is_dir()
                && !i.shared.join("old").exists()
        })
    })
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn folder_streaming_verification_rejects_changes_after_managed_planning() {
    use std::os::unix::fs::PermissionsExt;

    for change in [
        "content",
        "added_directory",
        "removed_directory",
        "symlink",
        "unreadable",
        "unreadable_addition",
    ] {
        let (mut a, b, id) = setup(true, true).await;
        let original = a.shared.join("old/nested/photo.jpg");
        let permissions = std::fs::metadata(&original).unwrap().permissions();
        if change == "removed_directory" {
            std::fs::create_dir(a.shared.join("old/nested/empty")).unwrap();
        }
        if change.starts_with("unreadable") {
            std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o000)).unwrap();
            let inaccessible = std::fs::File::open(&original).is_err();
            std::fs::set_permissions(&original, permissions.clone()).unwrap();
            if !inaccessible {
                eprintln!(
                    "Skipping unreadable-file case: filesystem permits reads without mode bits"
                );
                continue;
            }
        }
        admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
            .await
            .unwrap();
        let operation: String = sqlx::query_scalar(
            "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step' AND conflict_id=?",
        )
        .bind(&id)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        let permit = a
            .state
            .version_scan_limit
            .clone()
            .acquire_many_owned(2)
            .await
            .unwrap();
        let state = a.state.clone();
        let operation_id = operation.clone();
        a.tasks.push(tokio::spawn(async move {
            crate::ops::exec::run_operation(&state, &operation_id)
                .await
                .unwrap();
        }));
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let checking: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM operation_items WHERE operation_id=?
                     AND json_extract(progress_json,'$.steps[0].status')='running'
                     AND json_type(journal_json,'$.execution_steps[0].CheckTree.expected')='array')",
                )
                .bind(&operation)
                .fetch_one(&a.state.db)
                .await
                .unwrap();
                if checking {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the managed operation must reach its journaled tree check");
        assert!(a.shared.join("old").is_dir());
        match change {
            "content" => std::fs::write(&original, b"changed during execution").unwrap(),
            "added_directory" => {
                std::fs::create_dir(a.shared.join("old/nested/added-empty")).unwrap()
            }
            "removed_directory" => std::fs::remove_dir(a.shared.join("old/nested/empty")).unwrap(),
            "symlink" => {
                std::os::unix::fs::symlink("photo.jpg", a.shared.join("old/nested/unexpected-link"))
                    .unwrap()
            }
            "unreadable" => {
                std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o000))
                    .unwrap();
            }
            "unreadable_addition" => {
                let addition = a.shared.join("old/unreviewed");
                std::fs::create_dir(&addition).unwrap();
                let file = addition.join("photo.jpg");
                std::fs::write(&file, b"unreviewed addition").unwrap();
                std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
            }
            _ => unreachable!(),
        }
        drop(permit);
        a.tasks.pop().unwrap().await.unwrap();
        std::fs::set_permissions(&original, permissions).unwrap();
        let result: (String, Option<String>) = sqlx::query_as(
            "SELECT status,error_code FROM operation_items WHERE operation_id=? AND idx=0",
        )
        .bind(&operation)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
        assert_eq!(result.0, "failed", "{change}");
        assert_eq!(
            result.1.as_deref(),
            Some(if change.starts_with("unreadable") {
                "io_error"
            } else {
                "revision_changed"
            }),
            "{change}",
        );
        let trashed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trash WHERE operation_id=?")
            .bind(&operation)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
        assert_eq!(trashed, 0);
        assert!(a.shared.join("old").is_dir());
        assert_eq!(
            std::fs::read(a.shared.join("new/occupied.jpg")).unwrap(),
            b"local destination"
        );
        assert_eq!(
            std::fs::read(b.shared.join("new/nested/photo.jpg")).unwrap(),
            b"remote source"
        );
        assert_ne!(status(&a, &id).await, "resolved");
    }
}

#[tokio::test]
async fn folder_keep_both_retains_source_and_occupied_destination_on_both_peers() {
    let (mut a, mut b, id) = setup(true, true).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
        .await
        .unwrap();
    let first: String = sqlx::query_scalar(
        "SELECT operation_id FROM pending_peer_ops WHERE kind='move_decision_step'",
    )
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    crate::ops::exec::run_operation(&a.state, &first)
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    super::super::operation_finished(
        &a.state,
        &first,
        &[super::super::LocalChange::FolderMoved {
            from: Location {
                root_id: "photos".into(),
                path: "shared/new".into(),
            },
            to: Location {
                root_id: "photos".into(),
                path: "shared/replay-must-not-move".into(),
            },
        }],
    )
    .await
    .unwrap();
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(before, after);
    reboot(&mut a).await;
    preparations(&a).await;
    assert_eq!(status(&a, &id).await, "resolving");
    let copy = std::fs::read_dir(&a.shared)
        .unwrap()
        .filter_map(Result::ok)
        .find(|e| e.file_name().to_string_lossy().starts_with("new (conflict"))
        .unwrap()
        .file_name();
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually("all three versions on both peers", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("old/nested/photo.jpg")).is_ok_and(|b| b == b"local source")
                && std::fs::read(i.shared.join("new/nested/photo.jpg"))
                    .is_ok_and(|b| b == b"remote source")
                && std::fs::read(i.shared.join(&copy).join("occupied.jpg"))
                    .is_ok_and(|b| b == b"local destination")
        })
    })
    .await;
    stop_workers(&mut a).await;
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    admin::process_move_decisions(&a.state).await.unwrap();
    admin::process_move_decisions(&a.state).await.unwrap();
    assert!(
        admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
            .await
            .is_err()
    );
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn folder_keep_local_publishes_layout_and_remote_bytes_go_to_trash() {
    let (mut a, mut b, id) = setup(true, false).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepLocal)
        .await
        .unwrap();
    assert_eq!(status(&a, &id).await, "resolving");
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "resolved");
    restart(&mut a);
    restart(&mut b);
    eventually("local layout on both peers", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("old/nested/photo.jpg")).is_ok_and(|b| b == b"local source")
                && !i.shared.join("new").exists()
        })
    })
    .await;
    assert!(contains_bytes(&b, b"remote source"));
}

#[tokio::test]
async fn file_move_waits_for_publish_and_unrelated_work_continues() {
    let (mut a, mut b, id) = setup(false, true).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    assert_eq!(status(&a, &id).await, "resolving");
    sqlx::query(
        "UPDATE transfers SET status='failed',error='synthetic pause' WHERE rel_path='new.jpg'",
    )
    .execute(&a.state.db)
    .await
    .unwrap();
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "resolving");
    write_old(&b.shared.join("unrelated.jpg"), b"independent");
    scan(&b).await;
    restart(&mut a);
    restart(&mut b);
    eventually(
        "unrelated work while move publication is paused",
        &[&a, &b],
        || std::fs::read(a.shared.join("unrelated.jpg")).is_ok_and(|b| b == b"independent"),
    )
    .await;
    assert_eq!(status(&a, &id).await, "resolving");
    sqlx::query("UPDATE transfers SET status='queued',error=NULL WHERE rel_path='new.jpg'")
        .execute(&a.state.db)
        .await
        .unwrap();
    a.state.sync_wake.notify_one();
    resolved(&a, &id).await;
    eventually("published move and unrelated file", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("new.jpg")).is_ok_and(|b| b == b"remote source")
                && !i.shared.join("old.jpg").exists()
                && std::fs::read(i.shared.join("unrelated.jpg")).is_ok_and(|b| b == b"independent")
        })
    })
    .await;
    assert!(contains_bytes(&a, b"local source"));
    assert!(contains_bytes(&a, b"local destination"));
}

#[tokio::test]
async fn folder_keep_local_prunes_peer_only_empty_directories() {
    let (mut a, mut b, id) = setup(true, true).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepLocal)
        .await
        .unwrap();
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "resolved");
    restart(&mut a);
    restart(&mut b);
    eventually("exact local tree on both peers", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("new/occupied.jpg"))
                .is_ok_and(|b| b == b"local destination")
                && !i.shared.join("new/empty").exists()
                && !i.shared.join("new/nested").exists()
                && std::fs::read(i.shared.join("old/nested/photo.jpg"))
                    .is_ok_and(|b| b == b"local source")
        })
    })
    .await;
    assert!(contains_bytes(&b, b"remote source"));
}

#[tokio::test]
async fn file_keep_local_and_keep_both_converge_occupied_destinations() {
    for choice in [ConflictResolution::KeepLocal, ConflictResolution::KeepBoth] {
        let (mut a, mut b, id) = setup(false, true).await;
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        let check = || {
            [&a, &b].iter().all(|i| {
                std::fs::read(i.shared.join("old.jpg")).is_ok_and(|b| b == b"local source")
                    && std::fs::read(i.shared.join("new.jpg")).is_ok_and(|b| {
                        b == if choice == ConflictResolution::KeepLocal {
                            b"local destination".as_slice()
                        } else {
                            b"remote source".as_slice()
                        }
                    })
                    && contains_bytes(i, b"local destination")
            })
        };
        for _ in 0..300 {
            if check() {
                break;
            }
            a.state.sync_wake.notify_waiters();
            b.state.sync_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let snapshots: Vec<_> = [&a, &b]
            .iter()
            .map(|i| {
                (
                    std::fs::read(i.shared.join("old.jpg")),
                    std::fs::read(i.shared.join("new.jpg")),
                    std::fs::read_dir(&i.shared)
                        .unwrap()
                        .filter_map(Result::ok)
                        .map(|e| e.file_name())
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        let conflicts: Vec<(String, String, String, String)> =
            sqlx::query_as("SELECT rel_path,status,detail,remote_vv_json FROM conflicts")
                .fetch_all(&b.state.db)
                .await
                .unwrap();
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT rel_path,vv_json FROM replica_files")
                .fetch_all(&b.state.db)
                .await
                .unwrap();
        let events: Vec<String> =
            sqlx::query_scalar("SELECT data_json FROM event_log WHERE kind='file_changed'")
                .fetch_all(&a.state.db)
                .await
                .unwrap();
        assert!(
            check(),
            "{choice:?}: {snapshots:?} {conflicts:?} rows={rows:?} events={events:?}"
        );
        assert!(contains_bytes(&b, b"remote source"));
    }
}

#[tokio::test]
async fn publication_must_complete_before_move_choice_settles() {
    let (mut a, b, id) = setup(false, false).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    a.tasks
        .push(tokio::spawn(super::super::transfer::run(a.state.clone())));
    let mut publication = None;
    for _ in 0..400 {
        publication =
            sqlx::query_scalar("SELECT operation_id FROM pending_peer_ops WHERE kind='publish'")
                .fetch_optional(&a.state.db)
                .await
                .unwrap();
        if publication.is_some() {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let publication: String = publication.expect("managed publication was not queued");
    assert!(!a.shared.join("new.jpg").exists());
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "resolving");
    crate::ops::exec::run_operation(&a.state, &publication)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(a.shared.join("new.jpg")).unwrap(),
        b"remote source"
    );
    assert_eq!(status(&a, &id).await, "resolving");
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "resolved");
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    super::super::operation_finished(&a.state, &publication, &[])
        .await
        .unwrap();
    admin::process_move_decisions(&a.state).await.unwrap();
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(before, after);
    assert!(contains_bytes(&a, b"local source"));
    assert_eq!(
        std::fs::read(b.shared.join("new.jpg")).unwrap(),
        b"remote source"
    );
}

#[tokio::test]
async fn folder_symlinks_fail_closed_without_admitting_a_choice() {
    let (a, b, id) = setup(true, false).await;
    std::os::unix::fs::symlink("../empty", b.shared.join("new/nested/link")).unwrap();
    assert!(
        admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
            .await
            .is_err()
    );
    assert_eq!(status(&a, &id).await, "open");
    assert_eq!(
        std::fs::read(a.shared.join("old/nested/photo.jpg")).unwrap(),
        b"local source"
    );
}

#[tokio::test]
async fn folder_keep_remote_recovers_when_original_source_is_missing() {
    let (mut a, mut b, id) = setup(true, true).await;
    std::fs::rename(a.shared.join("old"), a.shared.join("retained")).unwrap();
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    reboot(&mut a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually("missing-source decision on both peers", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            !i.shared.join("old").exists()
                && std::fs::read(i.shared.join("new/nested/photo.jpg"))
                    .is_ok_and(|b| b == b"remote source")
                && i.shared.join("new/empty").is_dir()
        })
    })
    .await;
    assert!(contains_bytes(&a, b"local source"));
    assert!(contains_bytes(&a, b"local destination"));
}

#[tokio::test]
async fn folder_keep_local_can_replace_remote_tree_with_local_file() {
    let (mut a, mut b, id) = setup(true, false).await;
    write_old(&a.shared.join("new"), b"chosen local file");
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepLocal)
        .await
        .unwrap();
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    let check = || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("new")).is_ok_and(|b| b == b"chosen local file")
                && std::fs::read(i.shared.join("old/nested/photo.jpg"))
                    .is_ok_and(|b| b == b"local source")
        })
    };
    for _ in 0..300 {
        if check() {
            break;
        }
        a.state.sync_wake.notify_waiters();
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let items: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT kind,status,error_message FROM operation_items")
            .fetch_all(&b.state.db)
            .await
            .unwrap();
    let inbox: Vec<(String, Option<String>, String)> =
        sqlx::query_as("SELECT status,error,event_json FROM peer_inbox WHERE status!='done'")
            .fetch_all(&b.state.db)
            .await
            .unwrap();
    let transfers: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT rel_path,status,error FROM transfers")
            .fetch_all(&b.state.db)
            .await
            .unwrap();
    assert!(
        check(),
        "items={items:?} inbox={inbox:?} transfers={transfers:?}"
    );
    assert!(contains_bytes(&b, b"remote source"));
}

#[tokio::test]
async fn empty_folder_vectors_do_not_suppress_new_management_events() {
    let (a, b, id) = setup(true, false).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepLocal)
        .await
        .unwrap();
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "resolved");
    super::super::append_local(
        &b.state,
        "Family",
        "folder_created",
        &super::super::EventData {
            path: "new".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let peer = super::super::net::peers(&a.state).await.unwrap().remove(0);
    super::super::pull(&a.state, &peer).await.unwrap();
    let operation: String = sqlx::query_scalar("SELECT operation_id FROM pending_peer_events")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&a.state, &operation)
        .await
        .unwrap();
    assert!(a.shared.join("new").is_dir());
    assert_eq!(status(&a, &id).await, "resolved");
}

async fn relocate(instance: &Instance) {
    relocate_to(instance, "later").await;
}

async fn relocate_to(instance: &Instance, target: &str) {
    move_folder(instance, "new", target).await;
}

async fn move_folder(instance: &Instance, source: &str, target: &str) {
    let share = super::super::shares(&instance.state)
        .await
        .unwrap()
        .remove(0);
    let accepted = crate::ops::submit(
        &instance.state,
        &Principal::Admin {
            username: "synthetic".into(),
        },
        OperationRequest {
            idempotency_key: crate::crypto::new_id("synthetic-relocation"),
            label: None,
            actions: vec![OperationAction::MoveFolder {
                source: share.location(source).unwrap(),
                target: share.location(target).unwrap(),
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&instance.state, &accepted.operation_id)
        .await
        .unwrap();
    assert!(instance.shared.join(target).is_dir());
}

#[tokio::test]
async fn relocated_folder_destination_is_included_in_review() {
    let (a, b, id) = setup(true, false).await;
    relocate(&b).await;
    write_old(&a.shared.join("later/occupied.jpg"), b"local destination");
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id,
        peer_id: None,
    };
    let before = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    assert!(before.receiving.iter().any(|side| {
        side.path == "later"
            && side
                .files
                .iter()
                .any(|file| file.path == "later/occupied.jpg")
    }));
    write_old(&a.shared.join("later/occupied.jpg"), b"changed destination");
    let result = super::super::recovery::submit_decisions(
        &a.state,
        ism_core::dto::RecoveryDecisionInput {
            decisions: vec![ism_core::dto::RecoveryDecision {
                target: target.clone(),
                revision: before.revision,
            }],
            resolution: ConflictResolution::KeepRemote,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.items[0].state, ism_core::dto::RecoveryState::Queued);
    super::super::recovery::tick(&a.state).await.unwrap();
    assert_eq!(
        super::super::recovery::get(&a.state, target.kind, &target.id)
            .await
            .unwrap()
            .unwrap()
            .code
            .as_deref(),
        Some("version_conflict")
    );
}

#[tokio::test]
async fn relocated_folder_inside_source_resolves_each_choice() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let (mut a, mut b, id) = setup(true, false).await;
        std::fs::create_dir_all(b.shared.join("old")).unwrap();
        relocate_to(&b, "old/inside").await;
        write_old(&b.shared.join("old/peer-only.jpg"), b"peer-only sibling");
        scan(&b).await;
        let target = ism_core::dto::RecoveryTarget {
            kind: ism_core::dto::RecoveryKind::Conflict,
            id: id.clone(),
            peer_id: None,
        };
        let versions = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        assert_eq!(versions.source.path, "old");
        assert!(
            versions
                .source
                .files
                .iter()
                .any(|f| f.path == "old/peer-only.jpg")
        );
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        eventually(
            "nested relocated folder choice converges",
            &[&a, &b],
            || {
                [&a, &b].iter().all(|i| {
                    let local_copy = std::fs::read_dir(&i.shared)
                        .unwrap()
                        .flatten()
                        .any(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with("old (conflict ")
                                && std::fs::read(entry.path().join("nested/photo.jpg"))
                                    .is_ok_and(|bytes| bytes == b"local source")
                        });
                    !i.shared.join("new").exists()
                        && match choice {
                            ConflictResolution::KeepLocal => {
                                std::fs::read(i.shared.join("old/nested/photo.jpg"))
                                    .is_ok_and(|bytes| bytes == b"local source")
                            }
                            ConflictResolution::KeepRemote => {
                                !i.shared.join("old/nested/photo.jpg").exists()
                            }
                            ConflictResolution::KeepBoth => local_copy,
                            _ => false,
                        }
                        && if choice == ConflictResolution::KeepLocal {
                            !i.shared.join("old/inside").exists()
                        } else {
                            std::fs::read(i.shared.join("old/inside/nested/photo.jpg"))
                                .is_ok_and(|bytes| bytes == b"remote source")
                        }
                        && if choice == ConflictResolution::KeepLocal {
                            !i.shared.join("old/peer-only.jpg").exists()
                        } else {
                            std::fs::read(i.shared.join("old/peer-only.jpg"))
                                .is_ok_and(|bytes| bytes == b"peer-only sibling")
                        }
                })
            },
        )
        .await;
        assert!(contains_bytes(&a, b"local source"));
        assert!(contains_bytes(&b, b"remote source"));
        assert!(contains_bytes(&b, b"peer-only sibling"));
    }
}

#[tokio::test]
async fn relocated_folder_above_source_resolves_each_choice() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let (mut a, mut b, id) = setup_at(true, false, "old/inside").await;
        write_old(&a.shared.join("old/local-only.jpg"), b"local-only sibling");
        scan(&a).await;
        relocate_to(&b, "old").await;
        let target = ism_core::dto::RecoveryTarget {
            kind: ism_core::dto::RecoveryKind::Conflict,
            id: id.clone(),
            peer_id: None,
        };
        let versions = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        assert_eq!(versions.source.path, "old");
        assert!(
            versions.receiving.iter().any(|side| side.path == "old"
                && side.files.iter().any(|f| f.path == "old/local-only.jpg"))
        );
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        eventually(
            "parent relocated folder choice converges",
            &[&a, &b],
            || {
                [&a, &b].iter().all(|i| {
                    let local_copy = std::fs::read_dir(&i.shared)
                        .unwrap()
                        .flatten()
                        .any(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with("old (conflict ")
                                && std::fs::read(entry.path().join("inside/nested/photo.jpg"))
                                    .is_ok_and(|bytes| bytes == b"local source")
                                && std::fs::read(entry.path().join("local-only.jpg"))
                                    .is_ok_and(|bytes| bytes == b"local-only sibling")
                        });
                    !i.shared.join("new").exists()
                        && match choice {
                            ConflictResolution::KeepLocal => {
                                std::fs::read(i.shared.join("old/inside/nested/photo.jpg"))
                                    .is_ok_and(|bytes| bytes == b"local source")
                                    && std::fs::read(i.shared.join("old/local-only.jpg"))
                                        .is_ok_and(|bytes| bytes == b"local-only sibling")
                                    && !i.shared.join("old/nested/photo.jpg").exists()
                            }
                            ConflictResolution::KeepRemote | ConflictResolution::KeepBoth => {
                                std::fs::read(i.shared.join("old/nested/photo.jpg"))
                                    .is_ok_and(|bytes| bytes == b"remote source")
                                    && !i.shared.join("old/local-only.jpg").exists()
                                    && (choice != ConflictResolution::KeepBoth || local_copy)
                            }
                            _ => false,
                        }
                })
            },
        )
        .await;
        assert!(contains_bytes(&a, b"local source"));
        assert!(contains_bytes(&a, b"local-only sibling"));
        assert!(contains_bytes(&b, b"remote source"));
    }
}

#[tokio::test]
async fn ancestor_decision_supersedes_intermediate_folder_removal() {
    let (mut a, mut b, id) = setup_paths(true, false, "old/inside", "old/new").await;
    move_folder(&b, "old/new", "temporary").await;
    let share = super::super::shares(&b.state).await.unwrap().remove(0);
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "synthetic".into(),
        },
        OperationRequest {
            idempotency_key: "synthetic-remove-empty-parent".into(),
            label: None,
            actions: vec![OperationAction::TrashFolder {
                source: share.location("old").unwrap(),
                expected_entries: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    let removed_seq: i64 = sqlx::query_scalar(
        "SELECT seq FROM event_log WHERE kind='folder_trashed'
         AND json_extract(data_json,'$.path')='old' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    move_folder(&b, "temporary", "old").await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually(
        "ancestor decision preserves the selected peer folder",
        &[&a, &b],
        || {
            [&a, &b].iter().all(|i| {
                std::fs::read(i.shared.join("old/nested/photo.jpg"))
                    .is_ok_and(|bytes| bytes == b"remote source")
                    && !i.shared.join("old/inside/nested/photo.jpg").exists()
            })
        },
    )
    .await;
    for _ in 0..400 {
        let applied: bool = sqlx::query_scalar("SELECT cursor>=? FROM peers LIMIT 1")
            .bind(removed_seq)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
        if applied {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let applied: bool = sqlx::query_scalar("SELECT cursor>=? FROM peers LIMIT 1")
        .bind(removed_seq)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert!(
        applied,
        "the intermediate removal must be applied or superseded"
    );
    assert!(a.shared.join("old/nested/photo.jpg").is_file());
    assert!(b.shared.join("old/nested/photo.jpg").is_file());
}

#[tokio::test]
async fn folder_choice_follows_relocated_destination_ancestor() {
    let (mut a, mut b, id) = setup_paths(true, false, "old", "parent/new").await;
    move_folder(&b, "parent", "later").await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually("ancestor relocation converges", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("later/new/nested/photo.jpg"))
                .is_ok_and(|bytes| bytes == b"remote source")
                && !i.shared.join("old").exists()
                && !i.shared.join("parent/new").exists()
        })
    })
    .await;
}

#[tokio::test]
async fn folder_returning_to_destination_supersedes_intermediate_removal() {
    let (mut a, mut b, id) = setup(true, false).await;
    move_folder(&b, "new", "temporary").await;
    std::fs::create_dir_all(b.shared.join("new")).unwrap();
    let share = super::super::shares(&b.state).await.unwrap().remove(0);
    let accepted = crate::ops::submit(
        &b.state,
        &Principal::Admin {
            username: "synthetic".into(),
        },
        OperationRequest {
            idempotency_key: "synthetic-remove-old-destination".into(),
            label: None,
            actions: vec![OperationAction::TrashFolder {
                source: share.location("new").unwrap(),
                expected_entries: None,
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
        .await
        .unwrap();
    let removed_seq: i64 = sqlx::query_scalar(
        "SELECT seq FROM event_log WHERE kind='folder_trashed'
         AND json_extract(data_json,'$.path')='new' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(&b.state.db)
    .await
    .unwrap();
    move_folder(&b, "temporary", "new").await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    for _ in 0..400 {
        let applied: bool = sqlx::query_scalar("SELECT cursor>=? FROM peers LIMIT 1")
            .bind(removed_seq)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
        if applied {
            break;
        }
        a.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let applied: bool = sqlx::query_scalar("SELECT cursor>=? FROM peers LIMIT 1")
        .bind(removed_seq)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert!(applied);
    assert_eq!(
        std::fs::read(a.shared.join("new/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
    assert_eq!(
        std::fs::read(b.shared.join("new/nested/photo.jpg")).unwrap(),
        b"remote source"
    );
}

#[tokio::test]
async fn parent_removal_expands_decision_to_preserve_siblings() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let (mut a, mut b, id) = setup_paths(true, false, "old", "parent/new").await;
        write_old(&a.shared.join("parent/local-only.jpg"), b"local sibling");
        scan(&a).await;
        move_folder(&b, "parent/new", "temporary").await;
        let share = super::super::shares(&b.state).await.unwrap().remove(0);
        let accepted = crate::ops::submit(
            &b.state,
            &Principal::Admin {
                username: "synthetic".into(),
            },
            OperationRequest {
                idempotency_key: "synthetic-remove-parent".into(),
                label: None,
                actions: vec![OperationAction::TrashFolder {
                    source: share.location("parent").unwrap(),
                    expected_entries: None,
                }],
                allow_owner_transfer: false,
                allow_partial: false,
            },
        )
        .await
        .unwrap();
        crate::ops::exec::run_operation(&b.state, &accepted.operation_id)
            .await
            .unwrap();
        let removed_seq: i64 = sqlx::query_scalar(
            "SELECT seq FROM event_log WHERE kind='folder_trashed'
             AND json_extract(data_json,'$.path')='parent' ORDER BY seq DESC LIMIT 1",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap();
        std::fs::create_dir_all(b.shared.join("parent")).unwrap();
        move_folder(&b, "temporary", "parent/new").await;
        let target = ism_core::dto::RecoveryTarget {
            kind: ism_core::dto::RecoveryKind::Conflict,
            id: id.clone(),
            peer_id: None,
        };
        let review = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        assert_eq!(review.source.path, "parent");
        assert!(review.receiving.iter().any(|side| {
            side.path == "parent"
                && side
                    .files
                    .iter()
                    .any(|file| file.path == "parent/local-only.jpg")
        }));
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        for _ in 0..400 {
            let applied: bool = sqlx::query_scalar("SELECT cursor>=? FROM peers LIMIT 1")
                .bind(removed_seq)
                .fetch_one(&a.state.db)
                .await
                .unwrap();
            if applied {
                break;
            }
            a.state.sync_wake.notify_waiters();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let applied: bool = sqlx::query_scalar("SELECT cursor>=? FROM peers LIMIT 1")
            .bind(removed_seq)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
        assert!(applied);
        eventually("expanded parent choice converges", &[&a, &b], || {
            [&a, &b].iter().all(|i| {
                let sibling_copy = std::fs::read_dir(&i.shared)
                    .unwrap()
                    .flatten()
                    .any(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("parent (conflict ")
                            && std::fs::read(entry.path().join("local-only.jpg"))
                                .is_ok_and(|bytes| bytes == b"local sibling")
                    });
                match choice {
                    ConflictResolution::KeepLocal => {
                        std::fs::read(i.shared.join("old/nested/photo.jpg"))
                            .is_ok_and(|bytes| bytes == b"local source")
                            && std::fs::read(i.shared.join("parent/local-only.jpg"))
                                .is_ok_and(|bytes| bytes == b"local sibling")
                            && !i.shared.join("parent/new").exists()
                    }
                    ConflictResolution::KeepRemote | ConflictResolution::KeepBoth => {
                        std::fs::read(i.shared.join("parent/new/nested/photo.jpg"))
                            .is_ok_and(|bytes| bytes == b"remote source")
                            && !i.shared.join("parent/local-only.jpg").exists()
                            && (choice != ConflictResolution::KeepBoth || sibling_copy)
                    }
                    _ => false,
                }
            })
        })
        .await;
        assert!(contains_bytes(&a, b"local sibling"));
        assert!(contains_bytes(&b, b"remote source"));
    }
}

#[tokio::test]
async fn overlapping_folder_review_rejects_changes_outside_moved_subtree() {
    let (a, b, id) = setup(true, false).await;
    std::fs::create_dir_all(b.shared.join("old")).unwrap();
    relocate_to(&b, "old/inside").await;
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: id.clone(),
        peer_id: None,
    };
    for (instance, path) in [(&a, "old/local-sibling"), (&b, "old/peer-sibling")] {
        let before = super::super::versions::get(&a.state, &target)
            .await
            .unwrap();
        std::fs::create_dir_all(instance.shared.join(path)).unwrap();
        let result = super::super::recovery::submit_decisions(
            &a.state,
            ism_core::dto::RecoveryDecisionInput {
                decisions: vec![ism_core::dto::RecoveryDecision {
                    target: target.clone(),
                    revision: before.revision,
                }],
                resolution: ConflictResolution::KeepRemote,
            },
        )
        .await
        .unwrap();
        assert_eq!(result.items[0].state, ism_core::dto::RecoveryState::Queued);
        super::super::recovery::tick(&a.state).await.unwrap();
        assert_eq!(
            super::super::recovery::get(&a.state, target.kind, &target.id)
                .await
                .unwrap()
                .unwrap()
                .code
                .as_deref(),
            Some("version_conflict")
        );
        assert_eq!(status(&a, &id).await, "open");
    }
}

#[tokio::test]
async fn overlapping_folder_choice_runs_through_recovery_coordinator() {
    let (mut a, mut b, id) = setup(true, false).await;
    std::fs::create_dir_all(b.shared.join("old")).unwrap();
    relocate_to(&b, "old/inside").await;
    let target = ism_core::dto::RecoveryTarget {
        kind: ism_core::dto::RecoveryKind::Conflict,
        id: id.clone(),
        peer_id: None,
    };
    let review = super::super::versions::get(&a.state, &target)
        .await
        .unwrap();
    let result = super::super::recovery::submit_decisions(
        &a.state,
        ism_core::dto::RecoveryDecisionInput {
            decisions: vec![ism_core::dto::RecoveryDecision {
                target,
                revision: review.revision,
            }],
            resolution: ConflictResolution::KeepRemote,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.items[0].state, ism_core::dto::RecoveryState::Queued);
    restart(&mut a);
    restart(&mut b);
    a.tasks
        .push(tokio::spawn(super::super::recovery::run(a.state.clone())));
    resolved(&a, &id).await;
    eventually("reviewed overlapping choice converges", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("old/inside/nested/photo.jpg"))
                .is_ok_and(|bytes| bytes == b"remote source")
                && !i.shared.join("old/nested/photo.jpg").exists()
        })
    })
    .await;
    assert!(contains_bytes(&a, b"local source"));
}

#[tokio::test]
async fn overlapping_remote_choice_handles_late_replication_of_displaced_local_files() {
    let (mut a, mut b, id) = setup(true, false).await;
    std::fs::create_dir_all(b.shared.join("old")).unwrap();
    relocate_to(&b, "old/inside").await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    restart(&mut b);
    eventually("earlier local version reaches the peer", &[&a, &b], || {
        std::fs::read(b.shared.join("old/nested/photo.jpg"))
            .is_ok_and(|bytes| bytes == b"local source")
    })
    .await;
    preparations(&a).await;
    restart(&mut a);
    resolved(&a, &id).await;
    eventually("selected tree converges", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("old/inside/nested/photo.jpg"))
                .is_ok_and(|bytes| bytes == b"remote source")
                && !i.shared.join("old/nested/photo.jpg").exists()
        })
    })
    .await;
    assert!(contains_bytes(&a, b"local source"));
}

#[tokio::test]
async fn overlapping_remote_choice_reopens_for_new_peer_changes_in_displaced_tree() {
    for changed in ["bytes", "version", "directory"] {
        let (a, mut b, id) = setup(true, false).await;
        std::fs::create_dir_all(b.shared.join("old")).unwrap();
        relocate_to(&b, "old/inside").await;
        admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
            .await
            .unwrap();
        restart(&mut b);
        eventually("earlier local version reaches the peer", &[&a, &b], || {
            std::fs::read(b.shared.join("old/nested/photo.jpg"))
                .is_ok_and(|bytes| bytes == b"local source")
        })
        .await;
        // Until the copy's content is verified, b's scans skip the path, so a change made there
        // would not be versioned yet.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM peer_inbox WHERE status != 'done')",
        )
        .fetch_one(&b.state.db)
        .await
        .unwrap()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "copy content is verified"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let state = b.state.clone();
        let guard = state.replication_operation_lock.lock().await;
        stop_workers(&mut b).await;
        drop(guard);
        match changed {
            "bytes" => write_old(&b.shared.join("old/nested/photo.jpg"), b"new peer bytes"),
            "version" => {
                write_old(&b.shared.join("old/nested/photo.jpg"), b"temporary change");
                scan(&b).await;
                write_old(&b.shared.join("old/nested/photo.jpg"), b"local source");
            }
            "directory" => std::fs::create_dir_all(b.shared.join("old/nested/new-empty")).unwrap(),
            _ => unreachable!(),
        }
        scan(&b).await;
        preparations(&a).await;
        admin::process_move_decisions(&a.state).await.unwrap();
        assert_eq!(status(&a, &id).await, "open", "{changed}");
    }
}

#[tokio::test]
async fn overlapping_remote_choice_removes_displaced_files_on_third_peer() {
    for choice in [ConflictResolution::KeepRemote, ConflictResolution::KeepBoth] {
        third_peer_overlapping_choice(choice).await;
    }
}

async fn third_peer_overlapping_choice(choice: ConflictResolution) {
    let (mut a, mut b, id) = setup(true, false).await;
    let mut c = instance("Gamma").await;
    stop_workers(&mut c).await;
    let invite = admin::create_invite(&a.state).await.unwrap();
    admin::accept(
        &c.state,
        PeerAcceptInput {
            name: String::new(),
            url: a.url.clone(),
            invite: invite.invite,
            callback_url: c.url.clone(),
        },
    )
    .await
    .unwrap();
    let existing = super::super::shares(&a.state).await.unwrap().remove(0);
    let a_peer_ids: Vec<String> = admin::list_peers(&a.state)
        .await
        .unwrap()
        .into_iter()
        .map(|peer| peer.id)
        .collect();
    admin::update_share(
        &a.state,
        &existing.id,
        ShareInput {
            name: existing.name.clone(),
            location: Location {
                root_id: "photos".into(),
                path: "shared".into(),
            },
            peer_ids: a_peer_ids,
            enabled: true,
        },
    )
    .await
    .unwrap();
    let c_peer = admin::list_peers(&c.state).await.unwrap().remove(0);
    let c_share = admin::create_share(
        &c.state,
        ShareInput {
            name: "Family".into(),
            location: Location {
                root_id: "photos".into(),
                path: "shared".into(),
            },
            peer_ids: vec![c_peer.id],
            enabled: true,
        },
    )
    .await
    .unwrap();
    write_old(&c.shared.join("old/nested/photo.jpg"), b"local source");
    let mut row = super::super::row(&a.state, &existing.id, "old/nested/photo.jpg")
        .await
        .unwrap()
        .unwrap();
    row.mtime_ns =
        crate::files::mtime_ns(&std::fs::metadata(c.shared.join("old/nested/photo.jpg")).unwrap());
    let mut tx = c.state.db.begin().await.unwrap();
    super::super::put_row_in(&mut tx, &c_share.id, "old/nested/photo.jpg", &row)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    std::fs::create_dir_all(b.shared.join("old")).unwrap();
    relocate_to(&b, "old/inside").await;
    admin::resolve(&a.state, "admin", &id, choice)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    restart(&mut c);
    resolved(&a, &id).await;
    eventually(
        "third peer adopts selected folder and trashes displaced file",
        &[&a, &b, &c],
        || {
            std::fs::read(c.shared.join("old/inside/nested/photo.jpg"))
                .is_ok_and(|bytes| bytes == b"remote source")
                && !c.shared.join("old/nested/photo.jpg").exists()
                && (choice != ConflictResolution::KeepBoth
                    || std::fs::read_dir(&c.shared)
                        .unwrap()
                        .flatten()
                        .any(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with("old (conflict ")
                                && std::fs::read(entry.path().join("nested/photo.jpg"))
                                    .is_ok_and(|bytes| bytes == b"local source")
                        }))
        },
    )
    .await;
    assert!(contains_bytes(&c, b"local source"));
}

#[tokio::test]
async fn waiting_folder_choice_reopens_for_overlapping_relocation() {
    let (mut a, mut b, id) = setup(true, false).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
        .await
        .unwrap();
    preparations(&a).await;
    std::fs::create_dir_all(b.shared.join("old")).unwrap();
    relocate_to(&b, "old/inside").await;
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "open");
    reboot(&mut a).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually("reopened overlapping choice converges", &[&a, &b], || {
        [&a, &b].iter().all(|i| {
            std::fs::read(i.shared.join("old/inside/nested/photo.jpg"))
                .is_ok_and(|bytes| bytes == b"remote source")
                && std::fs::read_dir(&i.shared)
                    .unwrap()
                    .flatten()
                    .any(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("old (conflict ")
                            && std::fs::read(entry.path().join("nested/photo.jpg"))
                                .is_ok_and(|bytes| bytes == b"local source")
                    })
        })
    })
    .await;
}

#[tokio::test]
async fn peer_folder_returned_to_source_preserves_both_versions_for_each_choice() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let (mut a, mut b, id) = setup(true, false).await;
        relocate_to(&b, "old").await;
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        eventually("returned folder choice converges", &[&a, &b], || {
            (choice != ConflictResolution::KeepBoth || contains_bytes(&b, b"local source"))
                && [&a, &b].iter().all(|i| {
                    !i.shared.join("new").exists()
                        && std::fs::read(i.shared.join("old/nested/photo.jpg")).is_ok_and(|bytes| {
                            bytes
                                == if choice == ConflictResolution::KeepLocal {
                                    b"local source".as_slice()
                                } else {
                                    b"remote source".as_slice()
                                }
                        })
                })
        })
        .await;
        assert!(contains_bytes(&a, b"local source"));
        assert!(contains_bytes(&b, b"remote source"));
    }
}

#[tokio::test]
async fn relocated_peer_folder_choices_follow_authenticated_current_lineage() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let (mut a, mut b, id) = setup(true, false).await;
        relocate(&b).await;
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        eventually(
            "authenticated relocated choice converges",
            &[&a, &b],
            || {
                [&a, &b].iter().all(|i| {
                    !i.shared.join("new").exists()
                        && if choice == ConflictResolution::KeepRemote {
                            !i.shared.join("old").exists()
                        } else {
                            std::fs::read(i.shared.join("old/nested/photo.jpg"))
                                .is_ok_and(|bytes| bytes == b"local source")
                        }
                        && if choice == ConflictResolution::KeepLocal {
                            !i.shared.join("later").exists()
                        } else {
                            std::fs::read(i.shared.join("later/nested/photo.jpg"))
                                .is_ok_and(|bytes| bytes == b"remote source")
                        }
                })
            },
        )
        .await;
        assert!(contains_bytes(&a, b"local source"));
        assert!(contains_bytes(&b, b"remote source"));
    }
}

#[tokio::test]
async fn waiting_choice_can_finish_after_peer_folder_returns_to_occupied_source() {
    for choice in [
        ConflictResolution::KeepLocal,
        ConflictResolution::KeepRemote,
        ConflictResolution::KeepBoth,
    ] {
        let (mut a, mut b, id) = setup(true, false).await;
        admin::resolve(&a.state, "admin", &id, choice)
            .await
            .unwrap();
        preparations(&a).await;
        relocate_to(&b, "old").await;
        admin::process_move_decisions(&a.state).await.unwrap();
        if choice != ConflictResolution::KeepRemote {
            assert_eq!(status(&a, &id).await, "open");
            reboot(&mut a).await;
            admin::resolve(&a.state, "admin", &id, choice)
                .await
                .unwrap();
        } else {
            assert_eq!(status(&a, &id).await, "resolving");
        }
        preparations(&a).await;
        restart(&mut a);
        restart(&mut b);
        resolved(&a, &id).await;
        eventually(
            "returned waiting choices converge without deadlock",
            &[&a, &b],
            || {
                (choice != ConflictResolution::KeepBoth || contains_bytes(&b, b"local source"))
                    && [&a, &b].iter().all(|i| {
                        !i.shared.join("new").exists()
                            && std::fs::read(i.shared.join("old/nested/photo.jpg")).is_ok_and(
                                |bytes| {
                                    bytes
                                        == if choice == ConflictResolution::KeepLocal {
                                            b"local source".as_slice()
                                        } else {
                                            b"remote source".as_slice()
                                        }
                                },
                            )
                    })
            },
        )
        .await;
        assert!(contains_bytes(&a, b"local source"));
        assert!(contains_bytes(&b, b"remote source"));
        if choice == ConflictResolution::KeepBoth {
            assert!(contains_bytes(&b, b"local source"));
        }
    }
}

#[tokio::test]
async fn relocated_peer_folder_during_wait_rebases_durable_choice_without_duplicates() {
    let (mut a, mut b, id) = setup(true, true).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    let transfer: String =
        sqlx::query_scalar("SELECT id FROM transfers WHERE rel_path='new/nested/photo.jpg'")
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    relocate(&b).await;
    admin::process_move_decisions(&a.state).await.unwrap();
    preparations(&a).await;
    let updated: (String, String) = sqlx::query_as("SELECT id,rel_path FROM transfers WHERE id=?")
        .bind(&transfer)
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    assert_eq!(updated, (transfer, "later/nested/photo.jpg".into()));
    assert_eq!(status(&a, &id).await, "resolving");
    reboot(&mut a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually(
        "relocated waiting choice settles both instances",
        &[&a, &b],
        || {
            [&a, &b].iter().all(|i| {
                !i.shared.join("old").exists()
                    && !i.shared.join("new").exists()
                    && std::fs::read(i.shared.join("later/nested/photo.jpg"))
                        .is_ok_and(|bytes| bytes == b"remote source")
            })
        },
    )
    .await;
    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    stop_workers(&mut a).await;
    reboot(&mut a).await;
    for _ in 0..3 {
        admin::process_move_decisions(&a.state).await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_log")
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
        events
    );
    assert!(contains_bytes(&a, b"local source"));
    assert!(contains_bytes(&a, b"local destination"));
}

#[tokio::test]
async fn changed_peer_folder_choice_refreshes_needs_decision_without_selecting_new_bytes() {
    let (mut a, mut b, id) = setup(true, true).await;
    super::super::recovery::discover(&a.state).await.unwrap();
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    let raw: String = sqlx::query_scalar(
        "SELECT event_json FROM peer_inbox WHERE json_extract(event_json,'$.kind')='folder_moved' LIMIT 1",
    ).fetch_one(&a.state.db).await.unwrap();
    let event: super::super::PeerEvent = serde_json::from_str(&raw).unwrap();
    let peer = super::super::net::peers(&a.state).await.unwrap().remove(0);
    let mut tx = a.state.db.begin().await.unwrap();
    super::super::inbox::done_in(&mut tx, &peer.id, &event, false)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    write_old(
        &b.shared.join("new/nested/photo.jpg"),
        b"changed competing bytes",
    );
    scan(&b).await;
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "open");
    assert!(!a.shared.join("new/nested/photo.jpg").exists());
    let recovery: (String, Option<String>) = sqlx::query_as(
        "SELECT state,resolution FROM sync_recovery WHERE kind='conflict' AND target_id=?",
    )
    .bind(&id)
    .fetch_one(&a.state.db)
    .await
    .unwrap();
    assert_eq!(recovery, ("needs_decision".into(), None));
    assert_eq!(
        std::fs::read(b.shared.join("new/nested/photo.jpg")).unwrap(),
        b"changed competing bytes"
    );
    assert!(contains_bytes(&a, b"local source"));
    assert!(contains_bytes(&a, b"local destination"));
    reboot(&mut a).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually(
        "refreshed explicit choice converges after receipt pruning",
        &[&a, &b],
        || {
            [&a, &b].iter().all(|i| {
                std::fs::read(i.shared.join("new/nested/photo.jpg"))
                    .is_ok_and(|bytes| bytes == b"changed competing bytes")
            })
        },
    )
    .await;
}

#[tokio::test]
async fn refreshed_folder_choice_announces_copies_preserved_by_the_previous_choice() {
    let (mut a, mut b, id) = setup(true, true).await;
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepBoth)
        .await
        .unwrap();
    preparations(&a).await;
    write_old(
        &b.shared.join("new/nested/photo.jpg"),
        b"changed competing bytes",
    );
    scan(&b).await;
    admin::process_move_decisions(&a.state).await.unwrap();
    assert_eq!(status(&a, &id).await, "open");
    admin::resolve(&a.state, "admin", &id, ConflictResolution::KeepRemote)
        .await
        .unwrap();
    preparations(&a).await;
    restart(&mut a);
    restart(&mut b);
    resolved(&a, &id).await;
    eventually(
        "retained copies survive refreshed decisions on both peers",
        &[&a, &b],
        || {
            contains_bytes(&a, b"local destination")
                && contains_bytes(&b, b"local destination")
                && [&a, &b].iter().all(|i| {
                    std::fs::read(i.shared.join("new/nested/photo.jpg"))
                        .is_ok_and(|bytes| bytes == b"changed competing bytes")
                })
        },
    )
    .await;
}
