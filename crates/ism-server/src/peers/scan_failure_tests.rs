use super::*;

async fn fixture() -> (Instance, super::super::Share) {
    let mut local = instance("Scan fixture").await;
    stop_workers(&mut local).await;
    admin::create_share(
        &local.state,
        ShareInput {
            name: "Archive".into(),
            location: loc("shared"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = super::super::shares(&local.state).await.unwrap().remove(0);
    (local, share)
}

async fn failure(state: &AppState, id: &str) -> Option<serde_json::Value> {
    let snapshot = crate::attention::snapshot(state).await.unwrap();
    serde_json::to_value(snapshot).unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "share_scan_failed" && item["share_id"] == id)
        .cloned()
}

#[cfg(unix)]
struct Denied(std::path::PathBuf, std::fs::Permissions);

#[cfg(unix)]
impl Denied {
    fn new(path: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::metadata(path).unwrap().permissions();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0)).unwrap();
        Self(path.to_owned(), permissions)
    }
}

#[cfg(unix)]
impl Drop for Denied {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.0, self.1.clone()).unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn share_scan_failure_preserves_unreadable_paths_and_independent_changes() {
    let (local, share) = fixture().await;
    let locked = local.shared.join("locked");
    write_old(&locked.join("photo.jpg"), b"original");
    super::super::scan_share(&local.state, &share)
        .await
        .unwrap();
    let denied = Denied::new(&locked);
    assert_eq!(
        std::fs::read_dir(&locked).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied,
        "this regression requires an unprivileged test process"
    );
    write_old(&local.shared.join("independent.jpg"), b"independent");
    assert!(
        super::super::scan_share(&local.state, &share)
            .await
            .is_err()
    );
    assert!(
        !super::super::row(&local.state, &share.id, "locked/photo.jpg")
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
    assert!(
        super::super::row(&local.state, &share.id, "independent.jpg")
            .await
            .unwrap()
            .is_some()
    );
    let item = failure(&local.state, &share.id).await.unwrap();
    assert_eq!(item["blocks_work"], true);
    assert_eq!(item["href"], "/peers");
    assert!(item["message"].as_str().unwrap().contains("permission"));
    assert!(!item.to_string().contains("os error"));
    assert!(
        !item
            .to_string()
            .contains(&*local._dir.path().to_string_lossy())
    );
    assert!(
        super::super::scan_share(&local.state, &share)
            .await
            .is_err()
    );
    assert!(failure(&local.state, &share.id).await.is_some());
    drop(denied);

    let accepted = crate::ops::submit(
        &local.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "scan-failure-partial".into(),
            label: None,
            actions: vec![OperationAction::CreateFolder {
                target: loc("shared/pending"),
            }],
            allow_owner_transfer: false,
            allow_partial: false,
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE operations SET status='recovery_required' WHERE id=?")
        .bind(&accepted.operation_id)
        .execute(&local.state.db)
        .await
        .unwrap();
    super::super::scan_share(&local.state, &share)
        .await
        .unwrap();
    assert!(
        failure(&local.state, &share.id).await.is_some(),
        "excluded paths are not proof of recovery"
    );
    crate::ops::resolve_recovery(&local.state, &accepted.operation_id)
        .await
        .unwrap();
    super::super::scan_share(&local.state, &share)
        .await
        .unwrap();
    assert!(failure(&local.state, &share.id).await.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn share_scan_failure_hash_permission_does_not_stop_independent_files() {
    let (local, share) = fixture().await;
    let file = local.shared.join("denied.jpg");
    write_old(&file, b"private");
    let denied = Denied::new(&file);
    assert_eq!(
        std::fs::File::open(&file).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    write_old(&local.shared.join("independent.jpg"), b"independent");
    assert!(
        super::super::scan_share(&local.state, &share)
            .await
            .is_err()
    );
    assert!(
        failure(&local.state, &share.id).await.unwrap()["message"]
            .as_str()
            .unwrap()
            .contains("permission")
    );
    assert!(
        super::super::row(&local.state, &share.id, "independent.jpg")
            .await
            .unwrap()
            .is_some()
    );
    drop(denied);
    super::super::scan_share(&local.state, &share)
        .await
        .unwrap();
    assert!(failure(&local.state, &share.id).await.is_none());
}

#[tokio::test]
async fn share_scan_failure_survives_restart_and_share_lifecycle() {
    let (local, share) = fixture().await;
    std::fs::remove_dir(&local.shared).unwrap();
    assert!(
        super::super::scan_share(&local.state, &share)
            .await
            .is_err()
    );
    let before = failure(&local.state, &share.id).await.unwrap();
    assert!(!before["message"].as_str().unwrap().contains("permission"));
    let restarted = AppState::new(
        Config::for_tests(local._dir.path()),
        local.state.db.clone(),
        SecretBox::open_or_create(local._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    crate::activity::recover(&restarted).await.unwrap();
    assert_eq!(failure(&restarted, &share.id).await.unwrap(), before);
    std::fs::create_dir(&local.shared).unwrap();
    let independent = local._dir.path().join("photos/other");
    std::fs::create_dir(&independent).unwrap();
    admin::create_share(
        &restarted,
        ShareInput {
            name: "Other".into(),
            location: loc("other"),
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let other = super::super::shares(&restarted)
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.name == "Other")
        .unwrap();
    super::super::scan_share(&restarted, &other).await.unwrap();
    assert!(failure(&restarted, &share.id).await.is_some());
    let input = |enabled| ShareInput {
        name: share.name.clone(),
        location: loc("shared"),
        peer_ids: vec![],
        enabled,
    };
    let mut changes = restarted.events.subscribe();
    admin::update_share(&restarted, &share.id, input(false))
        .await
        .unwrap();
    assert!(matches!(
        changes.try_recv().unwrap(),
        crate::state::ServerEvent::SyncChanged
    ));
    assert!(failure(&restarted, &share.id).await.is_none());
    super::super::scan_share(&restarted, &share).await.unwrap();
    admin::update_share(&restarted, &share.id, input(true))
        .await
        .unwrap();
    assert!(failure(&restarted, &share.id).await.is_some());
    admin::delete_share(&restarted, &share.id).await.unwrap();
    assert!(failure(&restarted, &share.id).await.is_none());
    let retained: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM share_scan_failures WHERE share_id=?")
            .bind(&share.id)
            .fetch_one(&restarted.db)
            .await
            .unwrap();
    assert_eq!(retained, 0);
}

#[tokio::test]
async fn share_scan_failure_reports_persistence_failure_and_retries_saving() {
    let (local, share) = fixture().await;
    sqlx::query(
        "CREATE TRIGGER reject_scan_failure BEFORE INSERT ON share_scan_failures
         BEGIN SELECT RAISE(ABORT, 'synthetic evidence write failure'); END",
    )
    .execute(&local.state.db)
    .await
    .unwrap();
    std::fs::remove_dir(&local.shared).unwrap();
    let result = super::super::scan_share(&local.state, &share).await;
    assert_eq!(result.unwrap_err().code(), "share_scan_evidence_failed");
    let activity = crate::activity::page(&local.state, 10, 0).await.unwrap();
    assert!(activity.current.iter().any(|item| {
        item.task_key == format!("share:{}", share.id)
            && item.status == ism_core::dto::BackgroundStatus::Failed
            && item
                .error
                .as_deref()
                .is_some_and(|error| error.contains("could not be saved"))
    }));
    sqlx::query("DROP TRIGGER reject_scan_failure")
        .execute(&local.state.db)
        .await
        .unwrap();
    assert!(
        super::super::scan_share(&local.state, &share)
            .await
            .is_err()
    );
    assert!(failure(&local.state, &share.id).await.is_some());
}
