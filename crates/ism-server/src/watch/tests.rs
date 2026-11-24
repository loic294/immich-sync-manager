#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use ism_core::maintenance::{MaintenanceState, Phase};
use serde_json::json;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{body_json, method, path},
};

use super::*;
use crate::immich::registry;
use crate::maintenance::tests::{FakeImmich, setup as setup_base};

async fn setup() -> (
    tempfile::TempDir,
    AppState,
    wiremock::MockServer,
    FakeImmich,
) {
    let setup = setup_base().await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/invalid/files/move"))
        .respond_with(ResponseTemplate::new(400))
        .with_priority(1)
        .mount(&setup.2)
        .await;
    setup
}

/// Maps `<tmp>/photos` as root 'photos' of library 'lib1' on server 's1'.
async fn map_root(state: &AppState, dir: &Path) -> (Root, PathBuf) {
    let base = dir.join("photos");
    std::fs::create_dir_all(base.join("trip")).unwrap();
    std::fs::write(base.join(files::SENTINEL), "photos").unwrap();
    sqlx::query(
        "INSERT INTO immich_libraries (id, server_id, name, owner_user_id, import_paths_json, exclusion_patterns_json, refreshed_at)
         VALUES ('lib1', 's1', 'Family', 'u1', '[\"/library\"]', '[\"**/@eaDir/**\"]', ?)",
    )
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO mappings (root_id, name, container_path, server_id, library_id, immich_import_path, created_at)
         VALUES ('photos', 'Photos', ?, 's1', 'lib1', '/library', ?)",
    )
    .bind(base.to_str().unwrap())
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();
    (files::root(&state.db, "photos").await.unwrap(), base)
}

async fn due_at(state: &AppState) -> Option<i64> {
    sqlx::query_scalar("SELECT due_at FROM scan_schedule WHERE library_id = 'lib1'")
        .fetch_optional(&state.db)
        .await
        .unwrap()
        .flatten()
}

async fn full_pass(state: &AppState, root: &Root) -> PassOutcome {
    let outcome = pass(state, root, None).await.unwrap();
    // Mark the pass as done the way the watcher task does.
    let mut status = load_status(state, &root.mapping.root_id).await.unwrap();
    if !matches!(outcome, PassOutcome::Paused(_)) {
        status.last_pass_at = Some(crate::db::now());
    }
    save_status(state, &root.mapping.root_id, &status)
        .await
        .unwrap();
    outcome
}

fn changes(outcome: PassOutcome) -> Vec<(String, ChangeKind)> {
    match outcome {
        PassOutcome::Changes(c) => {
            let mut c: Vec<_> = c.into_iter().map(|c| (c.rel, c.kind)).collect();
            c.sort();
            c
        }
        PassOutcome::Baseline => panic!("baseline"),
        PassOutcome::Paused(reason) => panic!("paused: {reason}"),
    }
}

#[tokio::test]
async fn first_pass_is_a_silent_baseline_then_changes_are_diffed() {
    let (dir, state, _server, _fake) = setup().await;
    let (root, base) = map_root(&state, dir.path()).await;
    std::fs::write(base.join("trip/a.jpg"), "a").unwrap();
    assert!(matches!(
        full_pass(&state, &root).await,
        PassOutcome::Baseline
    ));

    std::fs::write(base.join("trip/b.jpg"), "b").unwrap();
    std::fs::remove_file(base.join("trip/a.jpg")).unwrap();
    assert_eq!(
        changes(full_pass(&state, &root).await),
        [
            ("trip/a.jpg".to_owned(), ChangeKind::Removed),
            ("trip/b.jpg".to_owned(), ChangeKind::Added)
        ]
    );
    // A scoped pass only looks below its folder.
    std::fs::write(base.join("trip/c.jpg"), "c").unwrap();
    std::fs::write(base.join("other.jpg"), "o").unwrap();
    assert_eq!(
        changes(pass(&state, &root, Some("trip")).await.unwrap()),
        [("trip/c.jpg".to_owned(), ChangeKind::Added)]
    );
    assert_eq!(
        changes(full_pass(&state, &root).await),
        [("other.jpg".to_owned(), ChangeKind::Added)]
    );
}

#[tokio::test]
async fn an_unreachable_or_suddenly_empty_folder_pauses_instead_of_deleting() {
    let (dir, state, _server, _fake) = setup().await;
    let (root, base) = map_root(&state, dir.path()).await;
    std::fs::write(base.join("trip/a.jpg"), "a").unwrap();
    full_pass(&state, &root).await;

    std::fs::remove_dir_all(base.join("trip")).unwrap();
    assert!(
        matches!(full_pass(&state, &root).await, PassOutcome::Paused(r) if r.contains("suddenly empty"))
    );
    std::fs::remove_file(base.join(files::SENTINEL)).unwrap();
    assert!(
        matches!(full_pass(&state, &root).await, PassOutcome::Paused(r) if r.contains("unreachable"))
    );
    let kept: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM watch_snapshot WHERE root_id = 'photos'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(kept, 1, "the snapshot is kept while paused");
    assert_eq!(due_at(&state).await, None, "nothing was reported");

    // The administrator confirms the folder is empty on purpose.
    std::fs::write(base.join(files::SENTINEL), "photos").unwrap();
    reset_baseline(&state, "photos").await.unwrap();
    assert!(matches!(
        full_pass(&state, &root).await,
        PassOutcome::Baseline
    ));
}

#[tokio::test]
async fn own_writes_are_ignored_and_outside_changes_schedule_a_scan() {
    let (dir, state, _server, _fake) = setup().await;
    let (root, base) = map_root(&state, dir.path()).await;
    full_pass(&state, &root).await;

    std::fs::write(base.join("trip/mine.jpg"), "mine").unwrap();
    let meta = std::fs::metadata(base.join("trip/mine.jpg")).unwrap();
    expect_writes(
        &state,
        &[Expected::file(
            base.join("trip/mine.jpg"),
            Some(4),
            Some(files::mtime_ns(&meta)),
        )],
    )
    .await
    .unwrap();
    std::fs::create_dir_all(base.join("@eaDir")).unwrap();
    std::fs::write(base.join("@eaDir/thumb.jpg"), "t").unwrap();
    let PassOutcome::Changes(found) = full_pass(&state, &root).await else {
        panic!("changes expected");
    };
    // `@eaDir` is an ignored name, so the walk never reports the thumbnail.
    assert_eq!(found.len(), 1);
    let outside = handle(&state, &root, found).await.unwrap();
    assert!(outside.is_empty(), "own writes are not outside changes");
    assert_eq!(
        due_at(&state).await,
        None,
        "own writes and ignored names do not scan"
    );

    std::fs::write(base.join("trip/theirs.jpg"), "theirs").unwrap();
    let PassOutcome::Changes(found) = full_pass(&state, &root).await else {
        panic!("changes expected");
    };
    let before = selfwrite::now_ms();
    let outside = handle(&state, &root, found).await.unwrap();
    assert_eq!(outside.len(), 1);
    let due = due_at(&state).await.expect("scan scheduled");
    let delay = i64::try_from(state.config.timing.scan_delay.as_millis()).unwrap();
    assert!(due >= before + delay, "due {due} before {before}+{delay}");
}

#[tokio::test]
async fn expired_self_writes_are_pruned() {
    let (_dir, state, _server, _fake) = setup().await;
    expect_writes(&state, &[Expected::tree("/m/a")])
        .await
        .unwrap();
    assert_eq!(
        selfwrite::load(&state, selfwrite::now_ms())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(selfwrite::load(&state, i64::MAX).await.unwrap().is_empty());
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM self_writes")
        .fetch_one(&state.db)
        .await
        .unwrap();
    assert_eq!(left, 0);
}

#[tokio::test]
async fn each_change_restarts_the_scan_timer() {
    let (_dir, state, _server, _fake) = setup().await;
    let delay = i64::try_from(state.config.timing.scan_delay.as_millis()).unwrap();
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "first",
        &[folder_work("/library/trip")],
        1_000,
    )
    .await
    .unwrap();
    assert_eq!(due_at(&state).await, Some(1_000 + delay));
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "second",
        &[folder_work("/library/other")],
        5_000,
    )
    .await
    .unwrap();
    assert_eq!(due_at(&state).await, Some(5_000 + delay));
    assert_eq!(
        scheduler::step(&state, 5_000 + delay - 1).await.unwrap(),
        Some(5_000 + delay),
        "not due yet"
    );
    let reasons: String = sqlx::query_scalar("SELECT reasons_json FROM scan_schedule")
        .fetch_one(&state.db)
        .await
        .unwrap();
    assert_eq!(reasons, r#"["first","second"]"#);
}

async fn scan_requests(server: &wiremock::MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/api/libraries/lib1/scan")
        .count()
}

fn folder_work(path: &str) -> scheduler::Work {
    scheduler::Work {
        path: path.into(),
        kind: scheduler::WorkKind::Folder,
    }
}

#[tokio::test]
async fn watcher_uses_file_api_without_scanning_on_success() {
    let (dir, state, server, _fake) = setup().await;
    let (root, _) = map_root(&state, dir.path()).await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/add"))
        .and(body_json(json!({ "path": "/library/trip/new.jpg" })))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/remove"))
        .and(body_json(json!({ "path": "/library/trip/old.jpg" })))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    handle(
        &state,
        &root,
        vec![
            outside_change("trip/new.jpg", ChangeKind::Added),
            outside_change("trip/new.jpg", ChangeKind::Changed),
            outside_change("trip/old.jpg", ChangeKind::Removed),
        ],
    )
    .await
    .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_requests(&server).await,
        0,
        "successful file sync must not scan"
    );
}

fn outside_change(rel: &str, kind: ChangeKind) -> Change {
    Change {
        rel: rel.into(),
        kind,
        size: Some(1),
        mtime_ns: Some(1),
    }
}

async fn scan_bodies(server: &wiremock::MockServer) -> Vec<serde_json::Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/api/libraries/lib1/scan")
        .map(|r| r.body_json().unwrap())
        .collect()
}

#[tokio::test]
async fn only_unhandled_folders_are_scanned_and_scopes_are_coalesced() {
    let (dir, state, server, _fake) = setup().await;
    let (root, _) = map_root(&state, dir.path()).await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/add"))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/add"))
        .and(body_json(json!({ "path": "/library/trip/new.jpg" })))
        .respond_with(ResponseTemplate::new(405))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    handle(
        &state,
        &root,
        vec![
            outside_change("handled/new.jpg", ChangeKind::Added),
            outside_change("trip/new.jpg", ChangeKind::Added),
            outside_change("trip/a.jpg", ChangeKind::Changed),
            outside_change("trip/sub/b.jpg", ChangeKind::Changed),
            outside_change("trips/c.jpg", ChangeKind::Changed),
            outside_change("@eaDir/thumb.jpg", ChangeKind::Added),
        ],
    )
    .await
    .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [
            json!({ "path": "/library/trip" }),
            json!({ "path": "/library/trips" }),
        ]
    );
    assert_eq!(scheduler::views(&state).await.unwrap()[0].last_error, None);
}

#[tokio::test]
async fn targeted_scan_errors_are_visible_and_never_broadened() {
    let (dir, state, server, _fake) = setup().await;
    let (root, _) = map_root(&state, dir.path()).await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/scan"))
        .and(body_json(json!({ "path": "/library/trip" })))
        .respond_with(ResponseTemplate::new(500))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    let mut events = state.events.subscribe();
    handle(
        &state,
        &root,
        vec![outside_change("trip/a.jpg", ChangeKind::Changed)],
    )
    .await
    .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
    assert!(
        scheduler::views(&state).await.unwrap()[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("500")
    );
    assert_eq!(retained(&state).await, 1, "failed work remains durable");
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(event, ServerEvent::LibraryScanned { .. }));
    }
}

#[tokio::test]
async fn unsupported_servers_never_receive_a_scope_they_might_ignore() {
    let (dir, state, server, _fake) = setup().await;
    let (root, _) = map_root(&state, dir.path()).await;
    sqlx::query("UPDATE immich_servers SET external_library_api_capable = 0 WHERE id = 's1'")
        .execute(&state.db)
        .await
        .unwrap();
    handle(
        &state,
        &root,
        vec![outside_change("trip/a.jpg", ChangeKind::Changed)],
    )
    .await
    .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(scan_requests(&server).await, 0);
    assert!(
        scheduler::views(&state).await.unwrap()[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("refusing a potentially full-library scan")
    );
}

#[tokio::test]
async fn file_api_errors_do_not_mask_authentication_or_claim_removals_were_scanned() {
    for (kind, endpoint, status, capable) in [
        (ChangeKind::Added, "add", 403, None),
        (ChangeKind::Added, "add", 404, None),
        (ChangeKind::Added, "add", 405, Some(true)),
        (ChangeKind::Removed, "remove", 405, None),
    ] {
        let (dir, state, server, _fake) = setup().await;
        let (root, _) = map_root(&state, dir.path()).await;
        sqlx::query("UPDATE immich_servers SET external_library_api_capable = ? WHERE id = 's1'")
            .bind(capable)
            .execute(&state.db)
            .await
            .unwrap();
        Mock::given(method("POST"))
            .and(path(format!("/api/libraries/lib1/files/{endpoint}")))
            .respond_with(ResponseTemplate::new(status))
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        handle(&state, &root, vec![outside_change("trip/a.jpg", kind)])
            .await
            .unwrap();
        scheduler::step(&state, i64::MAX / 2).await.unwrap();
        wait_for_sync(&state).await;
        assert_eq!(scan_requests(&server).await, 0);
        assert!(
            scheduler::views(&state).await.unwrap()[0]
                .last_error
                .is_some()
        );
    }
}

#[tokio::test]
async fn restart_retries_stale_removals_without_blocking_other_work() {
    let (dir, state, server, _fake) = setup().await;
    let (root, base) = map_root(&state, dir.path()).await;
    std::fs::write(base.join("trip/old.jpg"), "recreated").unwrap();
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/remove"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({ "message": "Removed file still exists" })),
        )
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/add"))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(1)
        .expect(2)
        .mount(&server)
        .await;
    handle(
        &state,
        &root,
        vec![
            outside_change("trip/old.jpg", ChangeKind::Removed),
            outside_change("trip/new.jpg", ChangeKind::Added),
        ],
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE scan_schedule SET due_at = NULL, last_error = 'Immich answered 400: Removed file still exists'",
    )
    .execute(&state.db)
    .await
    .unwrap();

    scheduler::recover(&state).await.unwrap();
    assert!(due_at(&state).await.is_some(), "failed work is rescheduled");
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;

    assert_eq!(scan_requests(&server).await, 0);
    let additions: Vec<serde_json::Value> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/api/libraries/lib1/files/add")
        .map(|request| request.body_json().unwrap())
        .collect();
    assert_eq!(
        additions,
        [
            json!({ "path": "/library/trip/new.jpg" }),
            json!({ "path": "/library/trip/old.jpg" }),
        ]
    );
    assert_eq!(scheduler::views(&state).await.unwrap()[0].last_error, None);
    let remaining: i64 = retained(&state).await;
    assert_eq!(remaining, 0);
    assert_eq!(
        std::fs::read(base.join("trip/old.jpg")).unwrap(),
        b"recreated"
    );
}

#[tokio::test]
async fn removal_rejection_stays_queued_if_the_file_cannot_be_verified() {
    for remove_marker in [false, true] {
        let (dir, state, server, _fake) = setup().await;
        let (root, base) = map_root(&state, dir.path()).await;
        if remove_marker {
            std::fs::write(base.join("trip/old.jpg"), "recreated").unwrap();
            std::fs::remove_file(base.join(files::SENTINEL)).unwrap();
        }
        Mock::given(method("POST"))
            .and(path("/api/libraries/lib1/files/remove"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({ "message": "Removed file still exists" })),
            )
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        handle(
            &state,
            &root,
            vec![outside_change("trip/old.jpg", ChangeKind::Removed)],
        )
        .await
        .unwrap();
        scheduler::step(&state, i64::MAX / 2).await.unwrap();
        wait_for_sync(&state).await;

        assert!(
            scheduler::views(&state).await.unwrap()[0]
                .last_error
                .is_some()
        );
        assert_eq!(scan_requests(&server).await, 0);
        let remaining: i64 = retained(&state).await;
        assert_eq!(remaining, 1);
    }
}

#[tokio::test]
async fn recreated_file_disappearing_during_scan_keeps_the_removal_queued() {
    let (dir, state, server, _fake) = setup().await;
    let (root, base) = map_root(&state, dir.path()).await;
    let recreated = base.join("trip/old.jpg");
    std::fs::write(&recreated, "recreated").unwrap();
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/remove"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({ "message": "Removed file still exists" })),
        )
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/add"))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/scan"))
        .respond_with(move |_request: &wiremock::Request| {
            std::fs::remove_file(&recreated).unwrap();
            ResponseTemplate::new(204)
        })
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    handle(
        &state,
        &root,
        vec![
            outside_change("trip/old.jpg", ChangeKind::Removed),
            outside_change("trip/other.jpg", ChangeKind::Changed),
        ],
    )
    .await
    .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;

    assert!(
        scheduler::views(&state).await.unwrap()[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("changed during synchronization")
    );
    let remaining: i64 = retained(&state).await;
    assert_eq!(remaining, 2);
}

#[tokio::test]
#[ignore = "requires a disposable custom Immich runtime and synthetic library"]
async fn real_immich_recreated_file_recovery_preserves_asset_identity() {
    let url = std::env::var("ISM_PROOF_URL").unwrap();
    let key = std::env::var("ISM_PROOF_KEY").unwrap();
    let user = std::env::var("ISM_PROOF_USER").unwrap();
    let library = std::env::var("ISM_PROOF_LIBRARY").unwrap();
    let root = PathBuf::from(std::env::var("ISM_PROOF_ROOT").unwrap());
    let run = std::env::var("ISM_PROOF_RUN").unwrap();
    std::fs::write(root.join(files::SENTINEL), "photos").unwrap();
    let (_dir, state, _server, _fake) = setup().await;
    let encrypted = state
        .secrets
        .encrypt(registry::API_KEY_PURPOSE, &key)
        .unwrap();
    sqlx::query(
        "UPDATE immich_servers SET url = ?, external_library_api_capable = 1 WHERE id = 's1'",
    )
    .bind(&url)
    .execute(&state.db)
    .await
    .unwrap();
    sqlx::query("UPDATE immich_accounts SET api_key_enc = ?, immich_user_id = ? WHERE id = 'a1'")
        .bind(encrypted)
        .bind(user)
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO immich_libraries (id,server_id,name,owner_user_id,import_paths_json,exclusion_patterns_json,refreshed_at)
         VALUES (?,'s1','Synthetic','proof','[\"/mnt/ext\"]','[]',?)",
    )
    .bind(&library)
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO mappings (root_id,name,container_path,server_id,library_id,immich_import_path,created_at)
         VALUES ('photos','Synthetic',?,'s1',?,'/mnt/ext',?)",
    )
    .bind(root.to_str().unwrap())
    .bind(&library)
    .bind(crate::db::now())
    .execute(&state.db)
    .await
    .unwrap();
    let api = crate::immich::api::Api::new(state.http.clone(), &url, key);
    let old = format!("/mnt/ext/{run}/recreated.png");
    let new = format!("/mnt/ext/{run}/other.png");
    api.sync_library_file_add(&library, &old).await.unwrap();
    let before = api.asset_id_by_path(&old).await.unwrap().unwrap();
    assert_eq!(api.asset_id_by_path(&new).await.unwrap(), None);
    let rejected = api
        .sync_library_file_remove(&library, &old)
        .await
        .unwrap_err();
    assert!(matches!(
        rejected,
        crate::immich::api::ApiError::Status { status: 400, ref message }
            if message == "Removed file still exists"
    ));
    scheduler::request_work_at(
        &state,
        "s1",
        &library,
        "synthetic runtime proof",
        &[
            scheduler::Work {
                path: old.clone(),
                kind: scheduler::WorkKind::Removed,
            },
            scheduler::Work {
                path: new.clone(),
                kind: scheduler::WorkKind::Added,
            },
        ],
        0,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE scan_schedule SET due_at = NULL, last_error = 'Immich answered 400: Removed file still exists' WHERE library_id = ?")
        .bind(&library)
        .execute(&state.db)
        .await
        .unwrap();
    scheduler::recover(&state).await.unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let running: bool =
                sqlx::query_scalar("SELECT running FROM scan_schedule WHERE library_id = ?")
                    .bind(&library)
                    .fetch_one(&state.db)
                    .await
                    .unwrap();
            if !running {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM scan_schedule WHERE library_id = ?")
            .bind(&library)
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(error, None);
    assert_eq!(api.asset_id_by_path(&old).await.unwrap(), Some(before));
    assert!(api.asset_id_by_path(&new).await.unwrap().is_some());
    assert!(root.join(run).join("recreated.png").is_file());
    let remaining: i64 = retained(&state).await;
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn requests_during_a_run_and_restart_keep_their_scopes() {
    let (_dir, state, server, _fake) = setup().await;
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "first",
        &[folder_work("/library/trip")],
        0,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE scan_schedule SET running = 1, due_at = NULL, running_through = 1")
        .execute(&state.db)
        .await
        .unwrap();
    scheduler::recover(&state).await.unwrap();
    // Block dispatch so a new request deterministically arrives after the batch is claimed.
    let guard = state.maintenance_lock.lock().await;
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "second",
        &[folder_work("/library/other")],
        1_000,
    )
    .await
    .unwrap();
    drop(guard);
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
    assert!(due_at(&state).await.is_some());
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [
            json!({ "path": "/library/trip" }),
            json!({ "path": "/library/other" }),
        ]
    );
    assert_eq!(due_at(&state).await, None);
    let remaining: i64 = retained(&state).await;
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn barrier_admission_between_claim_and_dispatch_defers_the_batch() {
    let (_dir, state, server, _fake) = setup().await;
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "change",
        &[folder_work("/library/trip")],
        0,
    )
    .await
    .unwrap();
    let guard = state.maintenance_lock.lock().await;
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    let mut held = MaintenanceState::default();
    held.begin_prepare();
    registry::save_maintenance(&state, "s1", &held)
        .await
        .unwrap();
    drop(guard);
    wait_for_sync(&state).await;
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(due_at(&state).await.is_some());
    registry::save_maintenance(&state, "s1", &MaintenanceState::default())
        .await
        .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
}

/// Scoped work not yet acknowledged by a successful run.
async fn retained(state: &AppState) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM scan_work w JOIN scan_schedule s ON s.library_id = w.library_id
         WHERE w.id > s.acked_through",
    )
    .fetch_one(&state.db)
    .await
    .unwrap()
}

async fn wait_for_sync(state: &AppState) {
    for _ in 0..100 {
        let running: bool =
            sqlx::query_scalar("SELECT running FROM scan_schedule WHERE library_id = 'lib1'")
                .fetch_one(&state.db)
                .await
                .unwrap();
        if !running {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("synchronization did not finish");
}

#[tokio::test]
async fn due_scans_wait_for_the_write_barrier() {
    let (_dir, state, server, _fake) = setup().await;
    let mut events = state.events.subscribe();
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "upload",
        &[scheduler::Work {
            path: "/library/trip/new.jpg".into(),
            kind: scheduler::WorkKind::Added,
        }],
        0,
    )
    .await
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/add"))
        .respond_with(ResponseTemplate::new(405))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    let mut held = MaintenanceState::default();
    held.begin_prepare();
    held.prepared();
    registry::save_maintenance(&state, "s1", &held)
        .await
        .unwrap();

    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    assert_eq!(
        scan_requests(&server).await,
        0,
        "deferred while the barrier is held"
    );
    let view = scheduler::views(&state).await.unwrap();
    assert!(view[0].waiting_for_barrier);
    assert_eq!(view[0].due_in_secs, Some(0));
    let activity = crate::activity::page(&state, 25, 0).await.unwrap();
    assert!(
        activity
            .current
            .iter()
            .any(|a| a.phase == "Waiting for the Immich maintenance barrier")
    );
    assert!(activity.current.iter().any(|a| {
        a.task_key == "maintenance:s1"
            && a.status == ism_core::dto::BackgroundStatus::Waiting
            && a.phase
                .contains("Library scans are paused for file operations")
    }));
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "direct file APIs must also wait for the barrier"
    );

    registry::save_maintenance(&state, "s1", &MaintenanceState::default())
        .await
        .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    for _ in 0..100 {
        let running: bool = sqlx::query_scalar("SELECT running FROM scan_schedule")
            .fetch_one(&state.db)
            .await
            .unwrap();
        if !running {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(scan_requests(&server).await, 1);
    assert_eq!(due_at(&state).await, None);
    let finished: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT last_finished_at, last_error FROM scan_schedule")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert!(finished.0.is_some());
    assert_eq!(finished.1, None);
    let mut scanned = false;
    while let Ok(event) = events.try_recv() {
        scanned |= matches!(event, ServerEvent::LibraryScanned { ref library_id, .. } if library_id == "lib1");
    }
    assert!(scanned);
    assert_eq!(
        registry::server(&state, "s1")
            .await
            .unwrap()
            .maintenance
            .phase,
        Phase::Idle
    );
}

#[tokio::test]
async fn native_watcher_reports_outside_changes() {
    let (dir, state, _server, _fake) = setup().await;
    let (_root, base) = map_root(&state, dir.path()).await;
    sqlx::query("INSERT INTO watch_settings (root_id, mode, poll_interval_secs) VALUES ('photos', 'native', 60)")
        .execute(&state.db)
        .await
        .unwrap();
    let task = tokio::spawn(root_task(
        state.clone(),
        "photos".into(),
        Spec {
            container_path: base.to_str().unwrap().to_owned(),
            mode: WatchMode::Native,
            interval: 60,
        },
    ));
    let mut ready = false;
    for _ in 0..200 {
        if load_status(&state, "photos")
            .await
            .unwrap()
            .last_pass_at
            .is_some()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(ready, "baseline pass");
    // Native watchers may need a moment before they deliver events.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    std::fs::write(base.join("trip/new.jpg"), "n").unwrap();
    let mut scheduled = false;
    for _ in 0..250 {
        if due_at(&state).await.is_some() {
            scheduled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    task.abort();
    let status = load_status(&state, "photos").await.unwrap();
    assert!(scheduled, "{status:?}");
    assert_eq!(status.effective_mode.as_deref(), Some("native"));
}

#[test]
fn scopes_collapse_to_parents() {
    let paths: BTreeSet<String> = ["a", "a/b", "a/b/c.jpg", "ab/x.jpg", "c/d.jpg"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(collapse(&paths), ["a", "ab/x.jpg", "c/d.jpg"]);
}

fn file_work(path: &str, kind: scheduler::WorkKind) -> scheduler::Work {
    scheduler::Work {
        path: path.into(),
        kind,
    }
}

async fn stored_rows(state: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM scan_work")
        .fetch_one(&state.db)
        .await
        .unwrap()
}

async fn wait_for_cleanup(state: &AppState) {
    for _ in 0..250 {
        if stored_rows(state).await == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("acknowledged scan work was not cleaned up");
}

#[tokio::test]
async fn repeated_changes_to_one_path_keep_the_backlog_bounded_and_exact() {
    use scheduler::WorkKind::{Added, Removed};
    let (_dir, state, server, _fake) = setup().await;
    let path = "/library/trip/a.jpg";
    for i in 0..500 {
        let batch = [file_work(path, Added), file_work(path, Removed)];
        scheduler::request_work_at(&state, "s1", "lib1", "change", &batch, i)
            .await
            .unwrap();
        scheduler::request_work_at(&state, "s1", "lib1", "dup", &[folder_work("/library/x")], i)
            .await
            .unwrap();
    }
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "change",
        &[file_work(path, Added)],
        500,
    )
    .await
    .unwrap();
    assert!(
        stored_rows(&state).await <= 3,
        "one file and one folder key"
    );
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    // Removed then added folds to a change, which requests the parent folder's scan.
    let mut bodies = scan_bodies(&server).await;
    bodies.sort_by_key(ToString::to_string);
    assert_eq!(
        bodies,
        [
            json!({ "path": "/library/trip" }),
            json!({ "path": "/library/x" })
        ]
    );
    assert_eq!(retained(&state).await, 0);
    wait_for_cleanup(&state).await;
}

#[tokio::test]
async fn large_batches_are_read_in_pages_acknowledged_once_and_cleaned_in_chunks() {
    let (_dir, state, server, _fake) = setup().await;
    let work: Vec<_> = (0..2_500)
        .map(|i| {
            file_work(
                &format!("/library/trip/{i}.jpg"),
                scheduler::WorkKind::Changed,
            )
        })
        .collect();
    scheduler::request_work_at(&state, "s1", "lib1", "import", &work, 0)
        .await
        .unwrap();
    assert_eq!(stored_rows(&state).await, 2_500);
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
    assert_eq!(
        retained(&state).await,
        0,
        "one acknowledgement covers the batch"
    );
    assert_eq!(due_at(&state).await, None);
    wait_for_cleanup(&state).await;
}

#[tokio::test]
async fn a_failed_claim_retries_with_later_arrivals_folded_exactly() {
    use scheduler::WorkKind::{Added, Changed, Removed};
    let (_dir, state, server, _fake) = setup().await;
    let path = "/library/trip/a.jpg";
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/api/libraries/lib1/files/add"))
        .respond_with(ResponseTemplate::new(500))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    scheduler::request_work_at(&state, "s1", "lib1", "a", &[file_work(path, Added)], 0)
        .await
        .unwrap();
    let guard = state.maintenance_lock.lock().await;
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    for (i, kind) in [Removed, Changed].into_iter().enumerate() {
        scheduler::request_work_at(
            &state,
            "s1",
            "lib1",
            "b",
            &[file_work(path, kind)],
            i as i64,
        )
        .await
        .unwrap();
    }
    drop(guard);
    wait_for_sync(&state).await;
    assert!(
        scheduler::views(&state).await.unwrap()[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("500")
    );
    // Added, removed, changed amounts to a change: a folder scan, never another add.
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
    assert_eq!(scheduler::views(&state).await.unwrap()[0].last_error, None);
    assert_eq!(retained(&state).await, 0);
}

async fn file_api_requests(server: &wiremock::MockServer, action: &str) -> usize {
    let path = format!("/api/libraries/lib1/files/{action}");
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == path)
        .count()
}

/// A request stored before keys existed, exactly as the previous release wrote it.
async fn store_pre_migration_removal(state: &AppState, path: &str) {
    sqlx::query("INSERT INTO scan_schedule (library_id, server_id) VALUES ('lib1', 's1')")
        .execute(&state.db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO scan_work (library_id, work_json) VALUES ('lib1', ?)")
        .bind(json!({ "path": path, "kind": "Removed" }).to_string())
        .execute(&state.db)
        .await
        .unwrap();
}

#[tokio::test]
async fn stored_requests_from_before_the_migration_coalesce_with_new_ones() {
    let (dir, state, server, _fake) = setup().await;
    let (_root, base) = map_root(&state, dir.path()).await;
    std::fs::write(base.join("trip/old.jpg"), "recreated").unwrap();
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/remove"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({ "message": "Removed file still exists" })),
        )
        .with_priority(1)
        .mount(&server)
        .await;
    let file = "/library/trip/old.jpg";
    store_pre_migration_removal(&state, file).await;
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "recreated",
        &[file_work(file, scheduler::WorkKind::Added)],
        0,
    )
    .await
    .unwrap();
    assert_eq!(stored_rows(&state).await, 1, "rewritten in place");
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    // As in one combined read, removed then added folds to a change, which requests the
    // parent folder's scan instead of a removal and a (no-op) add of the existing asset.
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
    assert_eq!(file_api_requests(&server, "remove").await, 0);
    assert_eq!(file_api_requests(&server, "add").await, 0);
    assert_eq!(scheduler::views(&state).await.unwrap()[0].last_error, None);
    assert_eq!(retained(&state).await, 0);
    wait_for_cleanup(&state).await;
}

#[tokio::test]
async fn stored_requests_without_a_path_keep_the_whole_batch_unacknowledged() {
    let (_dir, state, server, _fake) = setup().await;
    let file = "/library/trip/old.jpg";
    store_pre_migration_removal(&state, file).await;
    for malformed in [
        json!({ "kind": "Changed" }),
        json!({ "path": null, "kind": "Added" }),
    ] {
        sqlx::query("INSERT INTO scan_work (library_id, work_json) VALUES ('lib1', ?)")
            .bind(malformed.to_string())
            .execute(&state.db)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE scan_schedule SET due_at = 0")
        .execute(&state.db)
        .await
        .unwrap();
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    let view = &scheduler::views(&state).await.unwrap()[0];
    let error = view.last_error.as_deref().unwrap();
    assert!(
        error.contains("2 stored synchronization request(s) are unreadable"),
        "{error}"
    );
    // Kept and reported, but not retried automatically: retrying cannot repair the evidence.
    // A later request for the library makes it due again and reports the same error.
    assert_eq!(view.due_at, None);
    assert!(!view.running);
    let acked: i64 =
        sqlx::query_scalar("SELECT acked_through FROM scan_schedule WHERE library_id = 'lib1'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(acked, 0);
    assert_eq!(retained(&state).await, 3);
    assert_eq!(stored_rows(&state).await, 3);
    assert_eq!(file_api_requests(&server, "remove").await, 0);
    assert!(scan_bodies(&server).await.is_empty());
    let (items, _) = crate::local_evidence::snapshot(&state, "synthetic", &crate::db::now())
        .await
        .unwrap();
    let item = items
        .iter()
        .find(|item| item.code == ism_core::dto::AttentionCode::LibraryScanFailed)
        .unwrap();
    assert!(item.blocks_work);
    assert_eq!((item.count, item.next_at.as_deref()), (3, None));
    assert!(
        item.message
            .starts_with("ISM kept 3 synchronization request(s); no retry is scheduled"),
        "{}",
        item.message
    );
    assert!(!item.message.contains("Mappings"));
    assert_eq!(scheduler::step(&state, i64::MAX / 2).await.unwrap(), None);
    assert!(
        !scheduler::views(&state).await.unwrap()[0].running,
        "no retry loop"
    );
    assert_eq!(retained(&state).await, 3);
}

#[tokio::test]
async fn migrating_corrupt_stored_requests_keeps_them_unacknowledged() {
    let (dir, state, server, _fake) = setup().await;
    // Revert migration 0019 to recreate a database from before it, holding a valid request
    // next to corrupt ones that its generated key previously could not index.
    for statement in [
        "DROP INDEX scan_work_key",
        "ALTER TABLE scan_work DROP COLUMN work_key",
        "ALTER TABLE scan_schedule DROP COLUMN acked_through",
        "DELETE FROM _sqlx_migrations WHERE version = 19",
        "INSERT INTO scan_schedule (library_id, server_id, due_at) VALUES ('lib1', 's1', 0)",
    ] {
        sqlx::query(statement).execute(&state.db).await.unwrap();
    }
    for work in [
        r#"{"path":"/library/trip/old.jpg","kind":"Removed"}"#,
        "{",
        r#"{"path":5,"kind":"Added"}"#,
    ] {
        sqlx::query("INSERT INTO scan_work (library_id, work_json) VALUES ('lib1', ?)")
            .bind(work)
            .execute(&state.db)
            .await
            .unwrap();
    }
    let migrated = crate::db::connect(dir.path()).await.unwrap();
    let keys: Vec<Option<String>> =
        sqlx::query_scalar("SELECT work_key FROM scan_work ORDER BY id")
            .fetch_all(&migrated)
            .await
            .unwrap();
    assert_eq!(
        keys,
        [Some("file:/library/trip/old.jpg".into()), None, None]
    );
    migrated.close().await;

    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    let view = &scheduler::views(&state).await.unwrap()[0];
    let error = view.last_error.as_deref().unwrap();
    assert!(
        error.contains("2 stored synchronization request(s) are unreadable (invalid JSON, a missing or non-absolute path, or an unknown kind); the batch of 3 is kept"),
        "{error}"
    );
    let acked: i64 =
        sqlx::query_scalar("SELECT acked_through FROM scan_schedule WHERE library_id = 'lib1'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(acked, 0);
    assert_eq!(retained(&state).await, 3);
    assert_eq!(stored_rows(&state).await, 3);
    assert_eq!(file_api_requests(&server, "remove").await, 0);
    assert_eq!(file_api_requests(&server, "add").await, 0);
    assert!(scan_bodies(&server).await.is_empty());
}

#[tokio::test]
async fn an_unreadable_request_after_a_full_page_stops_the_batch_before_dispatch() {
    let (_dir, state, server, _fake) = setup().await;
    sqlx::query(
        "INSERT INTO scan_schedule (library_id, server_id, due_at) VALUES ('lib1', 's1', 0)",
    )
    .execute(&state.db)
    .await
    .unwrap();
    // More valid removals than one read page, then requests that have a key but do not
    // deserialize or name a valid scope: an unknown kind, a duplicate field, a relative path
    // and an empty one. Streaming dispatch would reach
    // them only after sending the earlier paths to Immich.
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
         INSERT INTO scan_work (library_id, work_json)
         SELECT 'lib1', json_object('path', printf('/library/trip/%05d.jpg', i), 'kind', 'Removed')
         FROM n ORDER BY i",
    )
    .execute(&state.db)
    .await
    .unwrap();
    for work in [
        r#"{"path":"/library/z.jpg","kind":"NotAWorkKind"}"#,
        r#"{"path":"/library/z.jpg","path":"/library/y.jpg","kind":"Removed"}"#,
        r#"{"path":"library/relative.jpg","kind":"Removed"}"#,
        r#"{"path":"","kind":"Changed"}"#,
    ] {
        sqlx::query("INSERT INTO scan_work (library_id, work_json) VALUES ('lib1', ?)")
            .bind(work)
            .execute(&state.db)
            .await
            .unwrap();
    }
    let null_keys: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM scan_work WHERE work_key IS NULL")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(
        null_keys, 0,
        "these requests are only detectable by reading them"
    );

    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    let view = &scheduler::views(&state).await.unwrap()[0];
    let error = view.last_error.as_deref().unwrap();
    assert!(
        error.contains("4 stored synchronization request(s) are unreadable (invalid JSON, a missing or non-absolute path, or an unknown kind); the batch of 2005 is kept"),
        "{error}"
    );
    assert_eq!(view.due_at, None);
    let acked: i64 =
        sqlx::query_scalar("SELECT acked_through FROM scan_schedule WHERE library_id = 'lib1'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(acked, 0);
    assert_eq!(stored_rows(&state).await, 2005);
    assert_eq!(file_api_requests(&server, "remove").await, 0);
    assert_eq!(file_api_requests(&server, "add").await, 0);
    assert!(scan_bodies(&server).await.is_empty());
}

#[tokio::test]
async fn admission_appends_after_unreadable_stored_requests_and_keeps_other_paths() {
    use scheduler::WorkKind::{Added, Changed, Removed};
    let (_dir, state, server, _fake) = setup().await;
    sqlx::query("INSERT INTO scan_schedule (library_id, server_id) VALUES ('lib1', 's1')")
        .execute(&state.db)
        .await
        .unwrap();
    // Keyed rows that do not deserialize: an unknown kind and a duplicate field.
    let corrupt = [
        r#"{"path":"/library/a.jpg","kind":"NotAWorkKind"}"#,
        r#"{"path":"/library/b.jpg","path":"/library/x.jpg","kind":"Removed"}"#,
    ];
    for work in corrupt {
        sqlx::query("INSERT INTO scan_work (library_id, work_json) VALUES ('lib1', ?)")
            .bind(work)
            .execute(&state.db)
            .await
            .unwrap();
    }
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "changed",
        &[
            file_work("/library/a.jpg", Removed),
            file_work("/library/c.jpg", Added),
            file_work("/library/b.jpg", Changed),
            file_work("/library/a.jpg", Added),
            file_work("/library/c.jpg", Changed),
        ],
        0,
    )
    .await
    .unwrap();
    let rows: Vec<String> = sqlx::query_scalar("SELECT work_json FROM scan_work ORDER BY id")
        .fetch_all(&state.db)
        .await
        .unwrap();
    assert_eq!(rows[..2], corrupt, "unreadable rows are kept unchanged");
    let mut appended: Vec<(String, String)> = rows[2..]
        .iter()
        .map(|json| {
            let work: serde_json::Value = serde_json::from_str(json).unwrap();
            (
                work["path"].as_str().unwrap().to_owned(),
                work["kind"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    // Affected paths keep every new request in arrival order; others still coalesce.
    let a: Vec<_> = appended
        .iter()
        .filter(|(p, _)| p == "/library/a.jpg")
        .collect();
    assert_eq!(a.len(), 2);
    assert_eq!((a[0].1.as_str(), a[1].1.as_str()), ("Removed", "Added"));
    appended.sort();
    assert_eq!(
        appended,
        [
            ("/library/a.jpg".into(), "Added".into()),
            ("/library/a.jpg".into(), "Removed".into()),
            ("/library/b.jpg".into(), "Changed".into()),
            ("/library/c.jpg".into(), "Added".into()),
        ]
    );

    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    let view = &scheduler::views(&state).await.unwrap()[0];
    let error = view.last_error.as_deref().unwrap();
    assert!(
        error.contains("2 stored synchronization request(s) are unreadable (invalid JSON, a missing or non-absolute path, or an unknown kind); the batch of 6 is kept"),
        "{error}"
    );
    let acked: i64 =
        sqlx::query_scalar("SELECT acked_through FROM scan_schedule WHERE library_id = 'lib1'")
            .fetch_one(&state.db)
            .await
            .unwrap();
    assert_eq!(acked, 0);
    assert_eq!(stored_rows(&state).await, 6);
    assert_eq!(file_api_requests(&server, "remove").await, 0);
    assert_eq!(file_api_requests(&server, "add").await, 0);
    assert!(scan_bodies(&server).await.is_empty());
}

#[tokio::test]
async fn admission_reads_a_bounded_window_of_a_large_legacy_backlog() {
    let (_dir, state, _server, _fake) = setup().await;
    let file = "/library/trip/hot.jpg";
    sqlx::query("INSERT INTO scan_schedule (library_id, server_id) VALUES ('lib1', 's1')")
        .execute(&state.db)
        .await
        .unwrap();
    // Uncoalesced rows from before the migration: added, then many changes, so the oldest
    // row decides the result and lies far outside the admission window.
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
         INSERT INTO scan_work (library_id, work_json)
         SELECT 'lib1', json_object('path', ?, 'kind', CASE i WHEN 0 THEN 'Added' ELSE 'Changed' END)
         FROM n ORDER BY i",
    )
    .bind(file)
    .execute(&state.db)
    .await
    .unwrap();
    for kind in [
        scheduler::WorkKind::Removed,
        scheduler::WorkKind::Added,
        scheduler::WorkKind::Changed,
        scheduler::WorkKind::Changed,
    ] {
        scheduler::request_work_at(&state, "s1", "lib1", "changed", &[file_work(file, kind)], 0)
            .await
            .unwrap();
    }
    let kinds: Vec<String> =
        sqlx::query_scalar("SELECT json_extract(work_json, '$.kind') FROM scan_work ORDER BY id")
            .fetch_all(&state.db)
            .await
            .unwrap();
    // Added, changes, removed, added folds to a change exactly as the full history does; the
    // unread rows are untouched and only one row is appended.
    assert_eq!(kinds.len(), 2002);
    assert_eq!(kinds[0], "Added");
    assert!(kinds[1..1999].iter().all(|k| k == "Changed"));
    assert_eq!(kinds[2000..], ["Removed", "Added"]);
}

#[tokio::test]
async fn a_failed_pre_migration_batch_retries_with_new_requests_folded_in() {
    let (_dir, state, server, _fake) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/lib1/files/remove"))
        .respond_with(ResponseTemplate::new(500))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    let file = "/library/trip/old.jpg";
    store_pre_migration_removal(&state, file).await;
    sqlx::query("UPDATE scan_schedule SET due_at = 0")
        .execute(&state.db)
        .await
        .unwrap();
    let guard = state.maintenance_lock.lock().await;
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    scheduler::request_work_at(
        &state,
        "s1",
        "lib1",
        "recreated",
        &[file_work(file, scheduler::WorkKind::Added)],
        0,
    )
    .await
    .unwrap();
    assert_eq!(stored_rows(&state).await, 2, "claimed rows never change");
    drop(guard);
    wait_for_sync(&state).await;
    assert!(
        scheduler::views(&state).await.unwrap()[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("500")
    );
    assert_eq!(retained(&state).await, 2);
    scheduler::step(&state, i64::MAX / 2).await.unwrap();
    wait_for_sync(&state).await;
    assert_eq!(
        scan_bodies(&server).await,
        [json!({ "path": "/library/trip" })]
    );
    assert_eq!(file_api_requests(&server, "remove").await, 1);
    assert_eq!(file_api_requests(&server, "add").await, 0);
    assert_eq!(scheduler::views(&state).await.unwrap()[0].last_error, None);
    assert_eq!(retained(&state).await, 0);
    wait_for_cleanup(&state).await;
}
