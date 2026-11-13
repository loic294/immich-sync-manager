//! Two real instances talking over HTTP on localhost.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ism_core::dto::{
    ConflictResolution, Location, OperationAction, OperationRequest, PeerAcceptInput, ShareInput,
};

use super::admin;
use crate::auth::Principal;
use crate::config::Config;
use crate::crypto::SecretBox;
use crate::state::AppState;

struct Instance {
    _dir: tempfile::TempDir,
    state: AppState,
    url: String,
    shared: PathBuf,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Instance {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

async fn instance(name: &str) -> Instance {
    let dir = tempfile::tempdir().unwrap();
    let pool = crate::db::connect(dir.path()).await.unwrap();
    let secrets = SecretBox::open_or_create(dir.path()).unwrap();
    let state = AppState::new(Config::for_tests(dir.path()), pool, secrets)
        .await
        .unwrap();
    crate::db::set_setting(&state.db, "instance_name", name)
        .await
        .unwrap();
    let root = dir.path().join("photos");
    let shared = root.join("shared");
    std::fs::create_dir_all(&shared).unwrap();
    std::fs::write(root.join(".ism-root"), "photos\n").unwrap();
    sqlx::query("INSERT INTO mappings (root_id, name, container_path, created_at) VALUES ('photos', 'Photos', ?, ?)")
        .bind(root.to_string_lossy().to_string())
        .bind(crate::db::now())
        .execute(&state.db)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = crate::routes::router(state.clone());
    let tasks = vec![
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }),
        tokio::spawn(crate::ops::exec::run(state.clone())),
        tokio::spawn(super::run(state.clone())),
        tokio::spawn(super::transfer::run(state.clone())),
    ];
    Instance {
        _dir: dir,
        state,
        url,
        shared,
        tasks,
    }
}

/// Writes a file that looks settled to the scanner.
fn write_old(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
    let f = std::fs::File::options().write(true).open(path).unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(120))
        .unwrap();
}

async fn scan(i: &Instance) {
    for share in super::shares(&i.state).await.unwrap() {
        super::scan_share(&i.state, &share).await.unwrap();
    }
    i.state.sync_wake.notify_waiters();
}

async fn eventually(what: &str, peers: &[&Instance], mut check: impl FnMut() -> bool) {
    for _ in 0..300 {
        if check() {
            return;
        }
        for p in peers {
            p.state.sync_wake.notify_waiters();
            p.state.ops_wake.notify_waiters();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

async fn pair() -> (Instance, Instance) {
    let a = instance("Alpha").await;
    let b = instance("Beta").await;
    let invite = admin::create_invite(&a.state).await.unwrap();
    admin::accept(
        &b.state,
        PeerAcceptInput {
            name: String::new(),
            url: a.url.clone(),
            invite: invite.invite,
            callback_url: b.url.clone(),
        },
    )
    .await
    .unwrap();
    for (me, _) in [(&a, &b), (&b, &a)] {
        let peers = admin::list_peers(&me.state).await.unwrap();
        assert_eq!(peers.len(), 1);
        admin::create_share(
            &me.state,
            ShareInput {
                name: "Family".into(),
                location: Location {
                    root_id: "photos".into(),
                    path: "shared".into(),
                },
                peer_ids: vec![peers[0].id.clone()],
                enabled: true,
            },
        )
        .await
        .unwrap();
    }
    (a, b)
}

async fn run_op(i: &Instance, key: &str, actions: Vec<OperationAction>) {
    let principal = Principal::Admin {
        username: "test".into(),
    };
    let req = OperationRequest {
        idempotency_key: key.into(),
        label: Some(key.into()),
        actions,
        allow_owner_transfer: false,
    };
    let accepted = crate::ops::submit(&i.state, &principal, req).await.unwrap();
    i.state.ops_wake.notify_waiters();
    for _ in 0..200 {
        let (op, _) = crate::ops::load(&i.state, &accepted.operation_id)
            .await
            .unwrap();
        let s = format!("{:?}", op.status).to_lowercase();
        if s == "completed" {
            return;
        }
        assert!(
            !matches!(
                s.as_str(),
                "failed" | "partial" | "cancelled" | "recoveryrequired"
            ),
            "{op:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("operation {key} did not finish");
}

fn loc(path: &str) -> Location {
    Location {
        root_id: "photos".into(),
        path: path.into(),
    }
}

#[tokio::test]
async fn replicates_files_moves_and_conflicts() {
    let (a, b) = pair().await;

    // A new photo and its sidecar travel to the peer.
    write_old(&a.shared.join("2024/x.jpg"), b"jpeg-v1");
    write_old(&a.shared.join("2024/x.jpg.xmp"), b"<xmp/>");
    scan(&a).await;
    eventually("new files reach B", &[&a, &b], || {
        std::fs::read(b.shared.join("2024/x.jpg")).ok().as_deref() == Some(b"jpeg-v1")
            && b.shared.join("2024/x.jpg.xmp").is_file()
    })
    .await;

    // A managed move on A is replayed on B as a move, sidecar included.
    run_op(
        &a,
        "move-x",
        vec![
            OperationAction::CreateFolder {
                target: loc("shared/2024/Trip"),
            },
            OperationAction::Move {
                source: loc("shared/2024/x.jpg"),
                target: loc("shared/2024/Trip/x.jpg"),
                expected_revision: None,
            },
        ],
    )
    .await;
    eventually("move reaches B", &[&a, &b], || {
        b.shared.join("2024/Trip/x.jpg").is_file()
            && b.shared.join("2024/Trip/x.jpg.xmp").is_file()
            && !b.shared.join("2024/x.jpg").exists()
    })
    .await;

    // Both sides edit the same file before syncing: a conflict, not a silent overwrite.
    write_old(&a.shared.join("2024/Trip/x.jpg"), b"edited on alpha");
    write_old(&b.shared.join("2024/Trip/x.jpg"), b"edited on beta!!");
    scan(&a).await;
    scan(&b).await;
    let mut conflict = None;
    for _ in 0..300 {
        for i in [&a, &b] {
            let open = admin::list_conflicts(&i.state, false).await.unwrap();
            if let Some(c) = open.into_iter().next() {
                conflict = Some((i, c));
            }
        }
        if conflict.is_some() {
            break;
        }
        a.state.sync_wake.notify_waiters();
        b.state.sync_wake.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (side, c) = conflict.expect("a conflict is reported");
    assert_eq!(c.location.path, "shared/2024/Trip/x.jpg");
    assert_eq!(
        std::fs::read(a.shared.join("2024/Trip/x.jpg")).unwrap(),
        b"edited on alpha"
    );
    assert_eq!(
        std::fs::read(b.shared.join("2024/Trip/x.jpg")).unwrap(),
        b"edited on beta!!"
    );

    // Keeping both versions converges: each instance ends up with both files.
    admin::resolve(
        &side.state,
        "admin:test",
        &c.id,
        ConflictResolution::KeepBoth,
    )
    .await
    .unwrap();
    eventually("both versions on both sides", &[&a, &b], || {
        let names = |dir: &Path| -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".jpg"))
                .collect();
            v.sort();
            v
        };
        let (na, nb) = (
            names(&a.shared.join("2024/Trip")),
            names(&b.shared.join("2024/Trip")),
        );
        na.len() == 2 && na == nb
    })
    .await;
    let copy = std::fs::read_dir(a.shared.join("2024/Trip"))
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().contains("conflict"))
        .unwrap()
        .file_name();
    let ca = std::fs::read(a.shared.join("2024/Trip").join(&copy)).unwrap();
    let cb = std::fs::read(b.shared.join("2024/Trip").join(&copy)).unwrap();
    assert_eq!(
        ca, cb,
        "the conflict copy has the same content on both sides"
    );
    let xa = std::fs::read(a.shared.join("2024/Trip/x.jpg")).unwrap();
    let xb = std::fs::read(b.shared.join("2024/Trip/x.jpg")).unwrap();
    assert_eq!(xa, xb, "the original name converges too");
    assert_ne!(xa, ca);
}
