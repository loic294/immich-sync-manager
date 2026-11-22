#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::time::Duration;

use ism_core::dto::{Location, OperationAction, OperationRequest, PeerAcceptInput, ShareInput};

use super::*;
use crate::auth::Principal;
use crate::config::Config;
use crate::crypto::SecretBox;

struct Instance {
    dir: tempfile::TempDir,
    state: AppState,
    root: PathBuf,
    url: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Instance {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Instance {
    async fn new() -> Self {
        let dir = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        let db = crate::db::connect(dir.path()).await.unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            db,
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let root = dir.path().join("photos");
        std::fs::create_dir_all(root.join("shared")).unwrap();
        std::fs::write(root.join(".ism-root"), "photos\n").unwrap();
        sqlx::query("INSERT INTO mappings(root_id,name,container_path,created_at) VALUES('photos','Photos',?,?)")
            .bind(root.to_string_lossy().as_ref()).bind(crate::db::now()).execute(&state.db).await.unwrap();
        let mut instance = Self {
            dir,
            state,
            root,
            url: String::new(),
            tasks: Vec::new(),
        };
        instance.listen().await;
        instance
    }

    async fn listen(&mut self) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::routes::router(self.state.clone());
        self.tasks.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
    }

    async fn restart(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        self.state = AppState::new(
            Config::for_tests(self.dir.path()),
            self.state.db.clone(),
            SecretBox::open_or_create(self.dir.path()).unwrap(),
        )
        .await
        .unwrap();
        super::super::transfer::recover_after_restart(&self.state)
            .await
            .unwrap();
        self.listen().await;
    }

    fn workers(&mut self) {
        self.tasks
            .push(tokio::spawn(crate::ops::exec::run(self.state.clone())));
        self.tasks.push(tokio::spawn(super::super::transfer::run(
            self.state.clone(),
        )));
    }
}

async fn pair() -> (Instance, Instance) {
    let a = Instance::new().await;
    let b = Instance::new().await;
    let invite = super::super::admin::create_invite(&a.state).await.unwrap();
    super::super::admin::accept(
        &b.state,
        PeerAcceptInput {
            name: "Alpha".into(),
            url: a.url.clone(),
            invite: invite.invite,
            callback_url: b.url.clone(),
        },
    )
    .await
    .unwrap();
    for instance in [&a, &b] {
        let peer = net::peers(&instance.state).await.unwrap().remove(0);
        super::super::admin::create_share(
            &instance.state,
            ShareInput {
                name: "Family".into(),
                location: Location {
                    root_id: "photos".into(),
                    path: "shared".into(),
                },
                peer_ids: vec![peer.id],
                enabled: true,
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE peers SET status='online'")
            .execute(&instance.state.db)
            .await
            .unwrap();
    }
    (a, b)
}

fn want(path: &str, bytes: &[u8]) -> Wanted {
    Wanted {
        share: "Family".into(),
        path: path.into(),
        sha256: crate::crypto::sha256_hex(bytes),
        size: bytes.len() as u64,
    }
}

async fn transfer(instance: &Instance, id: &str, path: &str, wanted: &Wanted) -> net::Peer {
    let peer = net::peers(&instance.state).await.unwrap().remove(0);
    let share = super::super::shares(&instance.state)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at,error)
        VALUES(?,?,?,?,?,?,'{}','failed',?,?,'live source gone')")
        .bind(id).bind(&peer.id).bind(&share.id).bind(path).bind(&wanted.sha256).bind(wanted.size as i64)
        .bind(crate::db::now()).bind(crate::db::now()).execute(&instance.state.db).await.unwrap();
    peer
}

async fn finished(instance: &Instance, id: &str) {
    for _ in 0..300 {
        let status: Option<String> = sqlx::query_scalar("SELECT o.status FROM transfer_history h JOIN operations o ON o.id=h.operation_id WHERE h.id=?")
            .bind(id).fetch_optional(&instance.state.db).await.unwrap();
        if status.as_deref() == Some("completed") {
            return;
        }
        instance.state.sync_wake.notify_waiters();
        instance.state.ops_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let transfers: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT id,status,error FROM transfers")
            .fetch_all(&instance.state.db)
            .await
            .unwrap();
    let operations: Vec<(String, String)> = sqlx::query_as("SELECT id,status FROM operations")
        .fetch_all(&instance.state.db)
        .await
        .unwrap();
    panic!("managed publication did not finish: {transfers:?} {operations:?}");
}

async fn retain_stage(instance: &Instance, id: &str, wanted: &Wanted, bytes: &[u8]) -> PathBuf {
    let share = super::super::shares(&instance.state)
        .await
        .unwrap()
        .remove(0);
    let peer = net::peers(&instance.state).await.unwrap().remove(0);
    sqlx::query("INSERT INTO transfer_history(id,peer_id,share_id,rel_path,sha256,size,queued_at,finished_at)
        VALUES(?,?,?,?,?,?,?,?)")
        .bind(id).bind(peer.id).bind(share.id).bind(&wanted.path).bind(&wanted.sha256).bind(wanted.size as i64)
        .bind(crate::db::now()).bind(crate::db::now()).execute(&instance.state.db).await.unwrap();
    let live = instance.root.join("shared").join(&wanted.path);
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    std::fs::write(&live, bytes).unwrap();
    let path = instance.root.join(files::STAGING_DIR).join("peer").join(id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::rename(live, &path).unwrap();
    path
}

#[tokio::test]
async fn paired_trash_exact_file_download_uses_managed_publication_without_folder_resurrection() {
    let (mut a, mut b) = pair().await;
    let folder = a.root.join("shared/retired");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("photo.jpg"), b"retained photo").unwrap();
    std::fs::write(folder.join("unrelated.jpg"), b"unrelated deletion").unwrap();
    a.tasks
        .push(tokio::spawn(crate::ops::exec::run(a.state.clone())));
    let accepted = crate::ops::submit(
        &a.state,
        &Principal::Admin {
            username: "test".into(),
        },
        OperationRequest {
            idempotency_key: "retained-folder-trash".into(),
            label: None,
            actions: vec![OperationAction::TrashFolder {
                source: Location {
                    root_id: "photos".into(),
                    path: "shared/retired".into(),
                },
                expected_entries: None,
            }],
            allow_partial: false,
            allow_owner_transfer: false,
        },
    )
    .await
    .unwrap();
    for _ in 0..200 {
        let status: String = sqlx::query_scalar("SELECT status FROM operations WHERE id=?")
            .bind(&accepted.operation_id)
            .fetch_one(&a.state.db)
            .await
            .unwrap();
        if status == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!folder.exists());
    let wanted = want("retired/photo.jpg", b"retained photo");
    let peer = transfer(&b, "trash-repair", "photo.jpg", &wanted).await;
    let found = remote_discover(&b.state, &peer, &wanted).await.unwrap();
    assert!(found.complete);
    assert_eq!(found.sources.len(), 1);
    let absent = want("retired/not-the-photo.jpg", b"retained photo");
    let wrong_path = remote_discover(&b.state, &peer, &absent).await.unwrap();
    assert!(wrong_path.complete);
    assert!(
        wrong_path.sources.is_empty(),
        "a duplicate hash is not an exact path"
    );
    let (trash_id, inventory): (String, String) =
        sqlx::query_as("SELECT id,file_inventory_json FROM trash")
            .fetch_one(&a.state.db)
            .await
            .unwrap();
    for bad in [
        None,
        Some("not-json"),
        Some(r#"[{"name":"unrelated.jpg","sha256":null}]"#),
    ] {
        sqlx::query("UPDATE trash SET file_inventory_json=? WHERE id=?")
            .bind(bad)
            .bind(&trash_id)
            .execute(&a.state.db)
            .await
            .unwrap();
        let rejected = remote_discover(&b.state, &peer, &wanted).await.unwrap();
        assert!(!rejected.complete);
        assert!(rejected.sources.is_empty());
    }
    sqlx::query("UPDATE trash SET file_inventory_json=? WHERE id=?")
        .bind(&inventory)
        .bind(&trash_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE trash SET quarantine_rel='.ism-trash/foreign' WHERE id=?")
        .bind(&trash_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    assert!(
        !remote_discover(&b.state, &peer, &wanted)
            .await
            .unwrap()
            .complete
    );
    sqlx::query("UPDATE trash SET quarantine_rel=? WHERE id=?")
        .bind(format!(".ism-trash/{trash_id}"))
        .bind(&trash_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    let source_path = {
        let source_peer = net::peers(&a.state).await.unwrap().remove(0);
        resolve(&a.state, Some(&source_peer.id), &found.sources[0].token)
            .await
            .unwrap()
            .0
    };
    {
        let _guard = b.state.replication_operation_lock.lock().await;
        assert!(
            queue_remote(&b.state, "trash-repair", &peer, &found.sources[0])
                .await
                .unwrap()
        );
    }
    b.workers();
    finished(&b, "trash-repair").await;
    assert_eq!(
        std::fs::read(b.root.join("shared/photo.jpg")).unwrap(),
        b"retained photo"
    );
    assert_eq!(std::fs::read(&source_path).unwrap(), b"retained photo");
    assert!(!b.root.join("shared/unrelated.jpg").exists());
    assert!(!b.root.join("shared/retired").exists());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM trash WHERE restored_at IS NOT NULL")
            .fetch_one(&a.state.db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn paired_retained_orphan_xmp_survives_restart_offline_and_allowed_hours_wait() {
    let (mut a, mut b) = pair().await;
    let wanted = want("orphan.xmp", b"standalone xmp");
    let held = retain_stage(&a, "held-xmp", &wanted, b"standalone xmp").await;
    let peer = transfer(&b, "xmp-repair", "orphan.xmp", &wanted).await;
    super::super::admin::set_schedule(
        &b.state,
        ism_core::dto::ScheduleView {
            timezone: "UTC".into(),
            windows: vec![ism_core::schedule::Window {
                days: Vec::new(),
                start: "00:00".into(),
                end: "00:00".into(),
            }],
        },
    )
    .await
    .unwrap();
    let found = remote_discover(&b.state, &peer, &wanted).await.unwrap();
    assert!(found.complete);
    assert_eq!(found.sources.len(), 1);
    assert_eq!(
        found.sources[0].token,
        remote_discover(&b.state, &peer, &wanted)
            .await
            .unwrap()
            .sources[0]
            .token
    );
    {
        let _guard = b.state.replication_operation_lock.lock().await;
        assert!(
            queue_remote(&b.state, "xmp-repair", &peer, &found.sources[0])
                .await
                .unwrap()
        );
    }
    a.restart().await;
    b.restart().await;
    sqlx::query("UPDATE peers SET url=?,status='offline' WHERE id=?")
        .bind(&a.url)
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    b.workers();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!b.root.join("shared/orphan.xmp").exists());
    sqlx::query("UPDATE peers SET status='online' WHERE id=?")
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!b.root.join("shared/orphan.xmp").exists());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT attempts FROM transfers WHERE id='xmp-repair'")
            .fetch_one(&b.state.db)
            .await
            .unwrap(),
        0
    );
    let peer = net::peer(&b.state, &peer.id).await.unwrap();
    assert_eq!(
        remote_discover(&b.state, &peer, &wanted)
            .await
            .unwrap()
            .sources[0]
            .token,
        found.sources[0].token
    );
    super::super::admin::set_schedule(
        &b.state,
        ism_core::dto::ScheduleView {
            timezone: "UTC".into(),
            windows: Vec::new(),
        },
    )
    .await
    .unwrap();
    finished(&b, "xmp-repair").await;
    assert_eq!(
        std::fs::read(b.root.join("shared/orphan.xmp")).unwrap(),
        b"standalone xmp"
    );
    assert_eq!(std::fs::read(held).unwrap(), b"standalone xmp");
    assert!(!b.root.join("shared/orphan.jpg").exists());
}

#[tokio::test]
async fn retained_sources_reject_tampering_foreign_peer_disabled_share_and_missing_mount() {
    let (a, b) = pair().await;
    let wanted = want("orphan.xmp", b"standalone xmp");
    let held = retain_stage(&a, "held-xmp", &wanted, b"standalone xmp").await;
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    let source = remote_discover(&b.state, &peer, &wanted)
        .await
        .unwrap()
        .sources
        .remove(0);
    let other = Instance::new().await;
    let invite = super::super::admin::create_invite(&a.state).await.unwrap();
    super::super::admin::accept(
        &other.state,
        PeerAcceptInput {
            name: "Foreign".into(),
            url: a.url.clone(),
            invite: invite.invite,
            callback_url: other.url.clone(),
        },
    )
    .await
    .unwrap();
    let foreign = net::peers(&other.state).await.unwrap().remove(0);
    assert!(
        remote_discover(&other.state, &foreign, &wanted)
            .await
            .is_err()
    );
    assert!(
        remote_check(&other.state, &foreign, &source.token, &wanted)
            .await
            .is_err()
    );
    assert!(
        resolve(&a.state, Some(&other.state.instance_id), &source.token)
            .await
            .is_err()
    );
    assert!(resolve(&a.state, None, &source.token).await.is_err());
    assert!(
        remote_check(&b.state, &peer, &"a".repeat(64), &wanted)
            .await
            .is_err()
    );
    std::fs::write(&held, b"tampered").unwrap();
    let found = remote_discover(&b.state, &peer, &wanted).await.unwrap();
    assert!(!found.complete);
    assert!(found.sources.is_empty());
    assert!(
        remote_check(&b.state, &peer, &source.token, &wanted)
            .await
            .is_err()
    );
    std::fs::write(&held, b"standalone xmp").unwrap();
    sqlx::query("UPDATE shares SET enabled=0")
        .execute(&a.state.db)
        .await
        .unwrap();
    assert!(
        remote_check(&b.state, &peer, &source.token, &wanted)
            .await
            .is_err()
    );
    sqlx::query("UPDATE shares SET enabled=1")
        .execute(&a.state.db)
        .await
        .unwrap();
    std::fs::remove_file(a.root.join(".ism-root")).unwrap();
    assert!(remote_discover(&b.state, &peer, &wanted).await.is_err());
}

#[tokio::test]
async fn local_retained_upload_is_copied_without_consuming_original_or_redundant_staging() {
    use ism_core::dto::{FileRole, OnConflict, UploadFileSpec};
    let (mut a, b) = pair().await;
    let wanted = want("photo.jpg", b"upload retained original");
    let principal = Principal::Admin {
        username: "test".into(),
    };
    let upload = crate::ops::uploads::create(
        &a.state,
        &principal,
        CreateUpload {
            idempotency_key: None,
            target: Location {
                root_id: "photos".into(),
                path: "shared".into(),
            },
            provenance: None,
            files: vec![UploadFileSpec {
                name: "photo.jpg".into(),
                role: FileRole::Original,
                size: wanted.size,
                sha256: wanted.sha256.clone(),
                expected_revision: None,
                expected_sha256: None,
            }],
            on_conflict: OnConflict::Fail,
            existing_original: None,
        },
    )
    .await
    .unwrap();
    crate::ops::uploads::put_chunk(
        &a.state,
        &principal,
        &upload.upload_id,
        0,
        &axum::http::HeaderMap::new(),
        bytes::Bytes::from_static(b"upload retained original"),
    )
    .await
    .unwrap();
    // A complete received upload is a retained source even though it was never published live.
    let discovery = discover(&a.state, None, &wanted).await.unwrap();
    assert!(discovery.complete);
    assert_eq!(discovery.sources.len(), 1);
    let accepted = crate::ops::uploads::commit(&a.state, &principal, &upload.upload_id)
        .await
        .unwrap();
    // Reproduce a completed upload whose cleanup retained the stage; no library repair is used.
    sqlx::query("UPDATE operations SET status='completed' WHERE id=?")
        .bind(&accepted.operation_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE uploads SET status='committed' WHERE id=?")
        .bind(&upload.upload_id)
        .execute(&a.state.db)
        .await
        .unwrap();
    let completed = discover(&a.state, None, &wanted).await.unwrap();
    assert!(completed.complete);
    assert_eq!(completed.sources.len(), 1);
    assert_eq!(completed.sources[0].token, discovery.sources[0].token);
    transfer(&a, "local-repair", "photo.jpg", &wanted).await;
    let stage = a.root.join(files::STAGING_DIR).join("peer/local-repair");
    std::fs::create_dir_all(stage.parent().unwrap()).unwrap();
    std::fs::write(&stage, b"redundant partial download").unwrap();
    {
        let _guard = a.state.replication_operation_lock.lock().await;
        assert!(
            stage_local(&a.state, "local-repair", &discovery.sources[0])
                .await
                .unwrap()
        );
    }
    a.workers();
    finished(&a, "local-repair").await;
    assert_eq!(
        std::fs::read(a.root.join("shared/photo.jpg")).unwrap(),
        b"upload retained original"
    );
    let (_, still_wanted) = resolve(&a.state, None, &discovery.sources[0].token)
        .await
        .unwrap();
    assert_eq!(still_wanted, wanted);
    let entries = std::fs::read_dir(stage.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect::<Vec<_>>();
    assert!(
        entries
            .iter()
            .any(|p| std::fs::read(p).unwrap() == b"redundant partial download")
    );
    let remote = net::peers(&b.state).await.unwrap().remove(0);
    assert!(
        !remote_discover(&b.state, &remote, &wanted)
            .await
            .unwrap()
            .sources
            .is_empty()
    );
}
