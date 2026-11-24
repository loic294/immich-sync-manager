#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

const OWNER: &str = "admin:example";

async fn setup() -> (tempfile::TempDir, SqlitePool, Location, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let pool = db::connect(dir.path()).await.unwrap();
    let folder = dir.path().join("photos");
    std::fs::create_dir(&folder).unwrap();
    sqlx::query(
        "INSERT INTO mappings(root_id,name,container_path,created_at)
         VALUES ('photos','Photos',?,'2026-01-01')",
    )
    .bind(dir.path().to_str().unwrap())
    .execute(&pool)
    .await
    .unwrap();
    (
        dir,
        pool,
        Location {
            root_id: "photos".into(),
            path: "photos".into(),
        },
        folder,
    )
}

fn limit() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(1))
}

#[tokio::test]
async fn public_review_api_checks_mount_and_owner_before_serving_or_verifying() {
    let (dir, pool, location, path) = setup().await;
    std::fs::write(dir.path().join(files::SENTINEL), "photos").unwrap();
    std::fs::write(path.join("a.jpg"), b"photo").unwrap();
    let state = AppState::new(
        crate::config::Config::for_tests(dir.path()),
        pool,
        crypto::SecretBox::open_or_create(dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let owner = Principal::Admin {
        username: "example".into(),
    };
    let manifest = capture(&state, &owner, &location).await.unwrap();
    let first = page(
        &state,
        &owner,
        &location,
        &manifest.id,
        &manifest.revision,
        None,
        1,
    )
    .await
    .unwrap();
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.next_after.as_deref(), Some(""));
    verify(&state, &owner, &location, &manifest.id, &manifest.revision)
        .await
        .unwrap();
    let sender = Principal::Peer {
        peer_id: "sender".into(),
    };
    assert!(capture(&state, &sender, &location).await.is_err());
    assert!(
        verify(&state, &sender, &location, &manifest.id, &manifest.revision)
            .await
            .is_err()
    );
    std::fs::remove_file(dir.path().join(files::SENTINEL)).unwrap();
    assert!(
        page(
            &state,
            &owner,
            &location,
            &manifest.id,
            &manifest.revision,
            first.next_after.as_deref(),
            1,
        )
        .await
        .is_err()
    );
    assert!(
        verify(&state, &owner, &location, &manifest.id, &manifest.revision)
            .await
            .is_err()
    );
    state.db.close().await;
}

#[tokio::test]
async fn large_tree_pages_survive_restart_and_stay_bound_to_one_revision() {
    let (dir, pool, location, path) = setup().await;
    std::fs::create_dir(path.join("empty")).unwrap();
    std::fs::create_dir(path.join("nested")).unwrap();
    for index in 0..10_001 {
        std::fs::write(path.join(format!("nested/{index:05}.jpg")), b"photo").unwrap();
    }
    let manifest = build(&pool, OWNER, &location, path.clone(), limit())
        .await
        .unwrap();
    assert_eq!(manifest.entry_count, 10_004);
    let first = read_page(
        &pool,
        OWNER,
        &location,
        &manifest.id,
        &manifest.revision,
        None,
        500,
    )
    .await
    .unwrap();
    assert_eq!(first.entries.len(), 500);
    assert_eq!(first.entries[0].name, "");
    assert_eq!(first.entries[1].name, "empty");
    assert_eq!(first.entries[1].sha256, None);
    assert_eq!(first.entries[1].size, None);
    pool.close().await;
    let pool = db::connect(dir.path()).await.unwrap();
    let mut seen = first.entries.len();
    let mut after = first.next_after;
    let mut previous = first.entries.last().unwrap().name.clone();
    while let Some(cursor) = after {
        let page = read_page(
            &pool,
            OWNER,
            &location,
            &manifest.id,
            &manifest.revision,
            Some(&cursor),
            500,
        )
        .await
        .unwrap();
        assert_eq!(page.manifest.revision, manifest.revision);
        assert_eq!(page.manifest.entry_count, manifest.entry_count);
        assert!(page.entries.len() <= 500);
        for item in &page.entries {
            assert!(item.name > previous);
            previous = item.name.clone();
        }
        seen += page.entries.len();
        after = page.next_after;
    }
    assert_eq!(seen as i64, manifest.entry_count);
    verify_tree(&pool, &manifest.id, manifest.entry_count, path, limit())
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test]
async fn live_content_and_empty_directory_changes_invalidate_review() {
    let (_dir, pool, location, path) = setup().await;
    std::fs::write(path.join("a.jpg"), b"before").unwrap();
    std::fs::create_dir(path.join("empty")).unwrap();
    let manifest = build(&pool, OWNER, &location, path.clone(), limit())
        .await
        .unwrap();
    std::fs::write(path.join("a.jpg"), b"after!").unwrap();
    assert!(
        verify_tree(
            &pool,
            &manifest.id,
            manifest.entry_count,
            path.clone(),
            limit()
        )
        .await
        .is_err()
    );
    std::fs::write(path.join("a.jpg"), b"before").unwrap();
    std::fs::remove_dir(path.join("empty")).unwrap();
    assert!(
        verify_tree(
            &pool,
            &manifest.id,
            manifest.entry_count,
            path.clone(),
            limit()
        )
        .await
        .is_err()
    );
    std::fs::create_dir(path.join("empty")).unwrap();
    std::fs::create_dir(path.join("added")).unwrap();
    assert!(
        verify_tree(
            &pool,
            &manifest.id,
            manifest.entry_count,
            path.clone(),
            limit()
        )
        .await
        .is_err()
    );
    std::fs::remove_dir(path.join("added")).unwrap();
    verify_tree(
        &pool,
        &manifest.id,
        manifest.entry_count,
        path.clone(),
        limit(),
    )
    .await
    .unwrap();
    let repeated = build(&pool, OWNER, &location, path, limit()).await.unwrap();
    assert_eq!(repeated.revision, manifest.revision);
    pool.close().await;
}

#[tokio::test]
async fn wrong_owner_location_revision_and_incomplete_reviews_fail_closed() {
    let (_dir, pool, location, path) = setup().await;
    let manifest = build(&pool, OWNER, &location, path, limit()).await.unwrap();
    for (owner, location, revision) in [
        (
            "admin:another",
            location.clone(),
            manifest.revision.as_str(),
        ),
        (
            OWNER,
            Location {
                root_id: location.root_id.clone(),
                path: "elsewhere".into(),
            },
            manifest.revision.as_str(),
        ),
        (OWNER, location.clone(), "wrong"),
    ] {
        assert!(
            recall(&pool, owner, &location, &manifest.id, revision)
                .await
                .is_err()
        );
    }
    assert!(
        authorize(
            &Principal::Peer {
                peer_id: "sender".into()
            },
            &location
        )
        .is_err()
    );
    for page_size in [0, MAX_PAGE + 1] {
        assert!(
            read_page(
                &pool,
                OWNER,
                &location,
                &manifest.id,
                &manifest.revision,
                None,
                page_size
            )
            .await
            .is_err()
        );
    }
    let bad_cursor = read_page(
        &pool,
        OWNER,
        &location,
        &manifest.id,
        &manifest.revision,
        Some("not-in-this-review"),
        500,
    )
    .await
    .unwrap_err();
    assert_eq!(bad_cursor.code(), "bad_review_cursor");
    sqlx::query(
        "INSERT INTO folder_review_manifests(id,owner,root_id,path,state,expires_at)
         VALUES ('unfinished',?,'photos','photos','building','2099-01-01')",
    )
    .bind(OWNER)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        recall(&pool, OWNER, &location, "unfinished", "")
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM folder_review_entries WHERE manifest_id=?")
            .bind(&manifest.id)
            .execute(&pool)
            .await
            .is_err()
    );
    // Removing the whole manifest must still cascade its rows.
    sqlx::query("DELETE FROM folder_review_manifests WHERE id=?")
        .bind(&manifest.id)
        .execute(&pool)
        .await
        .unwrap();
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM folder_review_entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
    pool.close().await;
}

#[tokio::test]
async fn reconnect_preserves_active_staging_and_restart_cleans_expired_staging() {
    let (dir, pool, location, path) = setup().await;
    let manifest = build(&pool, OWNER, &location, path, limit()).await.unwrap();
    sqlx::query(
        "INSERT INTO folder_review_manifests(id,owner,root_id,path,state,expires_at)
         VALUES ('unfinished',?,'photos','photos','building','2099-01-01')",
    )
    .bind(OWNER)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO folder_review_entries(manifest_id,name) VALUES ('unfinished','')")
        .execute(&pool)
        .await
        .unwrap();
    let other = db::connect(dir.path()).await.unwrap();
    let building: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM folder_review_manifests WHERE state='building'")
            .fetch_one(&other)
            .await
            .unwrap();
    assert_eq!(building, 1);
    assert!(
        recall(&other, OWNER, &location, "unfinished", "")
            .await
            .is_err()
    );
    sqlx::query("UPDATE folder_review_manifests SET expires_at='2000-01-01' WHERE id='unfinished'")
        .execute(&pool)
        .await
        .unwrap();
    other.close().await;
    pool.close().await;
    let restarted = db::connect(dir.path()).await.unwrap();
    recall(
        &restarted,
        OWNER,
        &location,
        &manifest.id,
        &manifest.revision,
    )
    .await
    .unwrap();
    let building: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM folder_review_manifests WHERE state='building'")
            .fetch_one(&restarted)
            .await
            .unwrap();
    assert_eq!(building, 0);
    let entries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM folder_review_entries")
        .fetch_one(&restarted)
        .await
        .unwrap();
    assert_eq!(entries, manifest.entry_count);
    restarted.close().await;
}

#[test]
fn streaming_traversal_rejects_concurrent_changes() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.jpg"), b"before").unwrap();
    let result = tree::walk(dir.path(), None, |item| {
        if item.name == "a.jpg" {
            std::fs::write(dir.path().join("a.jpg"), b"changed during scan")?;
        }
        Ok(())
    });
    assert!(result.is_err());
}

#[tokio::test]
async fn expired_preparation_cannot_publish_or_be_used_as_evidence() {
    let (_dir, pool, location, path) = setup().await;
    sqlx::query(
        "INSERT INTO folder_review_manifests(id,owner,root_id,path,state,expires_at)
         VALUES ('expired',?,'photos','photos','building','2000-01-01')",
    )
    .bind(OWNER)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        build_entries(&pool, "expired", &location, path, limit())
            .await
            .is_err()
    );
    let state: (String, Option<String>) =
        sqlx::query_as("SELECT state,revision FROM folder_review_manifests WHERE id='expired'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, ("building".into(), None));
    assert!(
        read_page(&pool, OWNER, &location, "expired", "", None, 500)
            .await
            .is_err()
    );
    pool.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn unsupported_entries_never_publish_partial_evidence() {
    let (_dir, pool, location, path) = setup().await;
    std::fs::write(path.join("a.jpg"), b"photo").unwrap();
    std::os::unix::fs::symlink("a.jpg", path.join("link")).unwrap();
    assert!(build(&pool, OWNER, &location, path, limit()).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM folder_review_manifests")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
}
