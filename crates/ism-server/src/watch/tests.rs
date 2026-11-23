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
    let retained: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_work")
        .fetch_one(&state.db)
        .await
        .unwrap();
    assert_eq!(retained, 1, "failed work remains durable");
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
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_work")
        .fetch_one(&state.db)
        .await
        .unwrap();
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
        a.phase
            .contains("managed operations hold the write barrier")
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
