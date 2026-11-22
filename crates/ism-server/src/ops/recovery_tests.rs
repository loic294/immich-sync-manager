#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use ism_core::dto::{Location, PlannedFile};
use std::path::PathBuf;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

struct Fixture {
    _directory: tempfile::TempDir,
    state: AppState,
    base: PathBuf,
    action: OperationAction,
    plan: Plan,
}

impl Fixture {
    async fn new(folder: bool, group: bool) -> Self {
        let directory = tempfile::tempdir_in(".").unwrap();
        let data = directory.path().join("state");
        fs::create_dir(&data).unwrap();
        let base = directory.path().canonicalize().unwrap().join("library");
        fs::create_dir(&base).unwrap();
        fs::write(base.join(files::SENTINEL), "root-1").unwrap();
        let state = AppState::new(
            crate::config::Config::for_tests(&data),
            crate::db::connect(&data).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(&data).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO mappings(root_id,name,container_path,created_at)
             VALUES('root-1','Synthetic library',?,?)",
        )
        .bind(base.to_str().unwrap())
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let source = if folder { "album" } else { "photo.jpg" };
        let target = if folder {
            "moved/album"
        } else {
            "moved/photo.jpg"
        };
        if folder {
            fs::create_dir_all(base.join("album/nested/empty")).unwrap();
            fs::write(base.join("album/nested/photo.jpg"), b"synthetic photo").unwrap();
            fs::write(
                base.join("album/nested/photo.jpg.xmp"),
                b"synthetic sidecar",
            )
            .unwrap();
        } else {
            fs::write(base.join(source), b"synthetic photo").unwrap();
        }
        let location = |path: &str| Location {
            root_id: "root-1".into(),
            path: path.into(),
        };
        let action = if folder {
            OperationAction::MoveFolder {
                source: location(source),
                target: location(target),
            }
        } else {
            OperationAction::Move {
                source: location(source),
                target: location(target),
                expected_revision: None,
            }
        };
        let mut plan = Plan::default();
        for (from, to, role) in
            std::iter::once((source.to_owned(), target.to_owned(), FileRole::Original)).chain(
                group.then_some((
                    "photo.jpg.xmp".into(),
                    "moved/photo.jpg.xmp".into(),
                    FileRole::Sidecar,
                )),
            )
        {
            if role == FileRole::Sidecar {
                fs::write(base.join(&from), b"synthetic sidecar").unwrap();
            }
            plan.files.push(PlannedFile {
                from: Some(location(&from)),
                to: Some(location(&to)),
                role,
                copy: false,
            });
            plan.steps.push(Step::Fs(plan::FsMove {
                from: base.join(from),
                to: base.join(to),
                copy: false,
            }));
        }
        sqlx::query(
            "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES('op-1','recovery-test','admin:synthetic','admin','{}','recovery_required',?)",
        ).bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query(
            "INSERT INTO operation_items(operation_id,idx,kind,action_json,status)
             VALUES('op-1',0,?,?, 'recovery_required')",
        )
        .bind(if folder { "move_folder" } else { "move" })
        .bind(serde_json::to_string(&action).unwrap())
        .execute(&state.db)
        .await
        .unwrap();
        journal(&state, "op-1", 0, &plan).await.unwrap();
        step_progress(&state, "op-1", 0, 0, "failed").await;
        Self {
            _directory: directory,
            state,
            base,
            action,
            plan,
        }
    }

    async fn evidence(&self) -> (serde_json::Value, ism_core::dto::OperationTaskProgress) {
        let (journal, progress): (String, String) = sqlx::query_as(
            "SELECT journal_json,progress_json FROM operation_items WHERE operation_id='op-1'",
        )
        .fetch_one(&self.state.db)
        .await
        .unwrap();
        (
            serde_json::from_str(&journal).unwrap(),
            serde_json::from_str(&progress).unwrap(),
        )
    }

    async fn finish_files(&self) -> AppResult<Vec<usize>> {
        let (journal, progress) = self.evidence().await;
        recover_moves(&self.state, "op-1", 0, &self.action, &journal, &progress).await
    }

    async fn peer_upload(&mut self, server: Option<&MockServer>) -> PathBuf {
        use ism_core::dto::{CreateUpload, OnConflict, OperationRequest, UploadFileSpec};
        if let Some(server) = server {
            self.immich(server).await;
            sqlx::query(
                "INSERT INTO immich_libraries(id,server_id,name,owner_user_id,import_paths_json,refreshed_at)
                 VALUES('library-1','server-1','Synthetic library','owner-1','[\"/library\"]',?)",
            ).bind(db::now()).execute(&self.state.db).await.unwrap();
        }
        sqlx::query(
            "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
             VALUES('peer-1','Synthetic peer','https://peer.example','peer-instance',?,'offline',?)",
        ).bind(self.state.secrets.encrypt(crate::peers::net::SECRET_PURPOSE, "synthetic-peer-key").unwrap())
            .bind(db::now()).execute(&self.state.db).await.unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
             VALUES('share-1','Synthetic share','root-1','','[\"peer-1\"]',1)",
        )
        .execute(&self.state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO pending_peer_ops(operation_id,idx,share_id,kind,to_path,vv_json,peer_id)
             VALUES('op-1',0,'share-1','publish','moved/photo.jpg','{\"peer-instance\":1}','peer-1')",
        ).execute(&self.state.db).await.unwrap();
        let source = self.base.join("photo.jpg");
        let hash = files::sha256_file(&source).unwrap();
        let size = fs::metadata(&source).unwrap().len();
        let request = CreateUpload {
            idempotency_key: Some("synthetic-transfer".into()),
            target: Location {
                root_id: "root-1".into(),
                path: "moved".into(),
            },
            provenance: None,
            files: vec![UploadFileSpec {
                name: "photo.jpg".into(),
                role: FileRole::Original,
                size,
                sha256: hash.clone(),
                expected_revision: None,
                expected_sha256: None,
            }],
            on_conflict: OnConflict::Fail,
            existing_original: None,
        };
        sqlx::query(
            "INSERT INTO uploads(id,principal,request_json,status,operation_id,created_at)
             VALUES('up-1','peer:peer-1',?,'failed','op-1',?)",
        )
        .bind(serde_json::to_string(&request).unwrap())
        .bind(db::now())
        .execute(&self.state.db)
        .await
        .unwrap();
        let root = files::root(&self.state.db, "root-1").await.unwrap();
        let staged = plan::staging_dir(&root, "up-1").unwrap().join("0");
        fs::create_dir_all(staged.parent().unwrap()).unwrap();
        fs::rename(source, &staged).unwrap();
        self.action = OperationAction::PublishUpload {
            upload_id: "up-1".into(),
        };
        let operation_request = OperationRequest {
            idempotency_key: "upload:up-1".into(),
            label: None,
            actions: vec![self.action.clone()],
            allow_owner_transfer: false,
            allow_partial: false,
        };
        sqlx::query(
            "UPDATE operations SET principal='peer:peer-1',source='peer',idempotency_key='upload:up-1',request_json=?
             WHERE id='op-1'",
        ).bind(serde_json::to_string(&operation_request).unwrap()).execute(&self.state.db).await.unwrap();
        sqlx::query("UPDATE operation_items SET kind='publish_upload',action_json=? WHERE operation_id='op-1'")
            .bind(serde_json::to_string(&self.action).unwrap()).execute(&self.state.db).await.unwrap();
        let target = self.base.join("moved/photo.jpg");
        self.plan = Plan {
            files: vec![PlannedFile {
                from: None,
                to: Some(Location {
                    root_id: "root-1".into(),
                    path: "moved/photo.jpg".into(),
                }),
                role: FileRole::Original,
                copy: false,
            }],
            steps: vec![
                Step::Mkdir(self.base.join("moved")),
                Step::CheckHash {
                    path: staged.clone(),
                    expected_size: size,
                    expected_sha256: hash.clone(),
                },
                Step::Fs(plan::FsMove {
                    from: staged.clone(),
                    to: target.clone(),
                    copy: false,
                }),
                Step::CheckHash {
                    path: target,
                    expected_size: size,
                    expected_sha256: hash,
                },
            ],
            upload_done: Some("up-1".into()),
            ..Default::default()
        };
        if server.is_some() {
            self.plan.steps.push(Step::Api(ApiStep {
                server_id: "server-1".into(),
                owner: "owner-1".into(),
                ids: Vec::new(),
                kind: ApiKind::SyncAdd {
                    library_id: "library-1".into(),
                    path: "/library/moved/photo.jpg".into(),
                },
            }));
        }
        journal(&self.state, "op-1", 0, &self.plan).await.unwrap();
        staged
    }

    async fn restart(&mut self) {
        let config = self.state.config.clone();
        let data = config.data_dir.clone();
        self.state.db.close().await;
        self.state = AppState::new(
            config,
            crate::db::connect(&data).await.unwrap(),
            crate::crypto::SecretBox::open_or_create(&data).unwrap(),
        )
        .await
        .unwrap();
        crate::maintenance::recover_after_restart(&self.state)
            .await
            .unwrap();
    }

    async fn folder_restore(&self, retained_journal: bool) -> Vec<String> {
        use ism_core::dto::{CreateUpload, OnConflict, OperationRequest, UploadFileSpec};
        sqlx::query(
            "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
             VALUES('peer-1','Synthetic peer','https://peer.example','peer-instance',?,'offline',?)",
        )
        .bind(self.state.secrets.encrypt(crate::peers::net::SECRET_PURPOSE, "synthetic-key").unwrap())
        .bind(db::now()).execute(&self.state.db).await.unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
             VALUES('share-1','Synthetic share','root-1','','[\"peer-1\"]',1)",
        )
        .execute(&self.state.db)
        .await
        .unwrap();
        let request = serde_json::to_string(&OperationRequest {
            idempotency_key: "synthetic-folder".into(),
            label: None,
            actions: vec![self.action.clone()],
            allow_owner_transfer: false,
            allow_partial: false,
        })
        .unwrap();
        sqlx::query(
            "UPDATE operations SET principal='peer:peer-1',source='peer',request_json=? WHERE id='op-1'",
        ).bind(&request).execute(&self.state.db).await.unwrap();
        let root = files::root(&self.state.db, "root-1").await.unwrap();
        let mut directories = Vec::new();
        let mut contents = Vec::new();
        let mut uploads = BTreeMap::new();
        for entry in plan::folder_inventory(&self.base.join("album")).unwrap() {
            let remote = if entry.name.is_empty() {
                "moved/album".to_owned()
            } else {
                format!("moved/album/{}", entry.name)
            };
            let Some(hash) = entry.sha256 else {
                directories.push(remote);
                continue;
            };
            let old = self.base.join("album").join(&entry.name);
            let size = fs::metadata(&old).unwrap().len();
            let upload_id = format!("restore-{}", uploads.len());
            let rel = files::parse_rel(&format!("album/{}", entry.name)).unwrap();
            let spec = CreateUpload {
                idempotency_key: None,
                target: Location {
                    root_id: "root-1".into(),
                    path: rel.parent().unwrap().to_string(),
                },
                files: vec![UploadFileSpec {
                    name: entry.name.rsplit('/').next().unwrap().into(),
                    role: if entry.name.ends_with(".xmp") {
                        FileRole::Sidecar
                    } else {
                        FileRole::Original
                    },
                    size,
                    sha256: hash.clone(),
                    expected_revision: None,
                    expected_sha256: None,
                }],
                provenance: None,
                on_conflict: OnConflict::Fail,
                existing_original: None,
            };
            sqlx::query(
                "INSERT INTO uploads(id,principal,request_json,status,created_at)
                 VALUES(?,'peer:peer-1',?,'receiving',?)",
            )
            .bind(&upload_id)
            .bind(serde_json::to_string(&spec).unwrap())
            .bind(db::now())
            .execute(&self.state.db)
            .await
            .unwrap();
            let stage = plan::staging_dir(&root, &upload_id).unwrap().join("0");
            fs::create_dir_all(stage.parent().unwrap()).unwrap();
            fs::rename(old, stage).unwrap();
            contents.push(json!({
                "path":remote,"sha256":hash,"size":size,
                "vv":{"peer-instance":1},"download":true,
            }));
            uploads.insert(remote, upload_id);
        }
        fs::remove_dir_all(self.base.join("album")).unwrap();
        let manifest = json!({
            "operation_id":"op-1","share_id":"share-1","peer_id":"peer-1",
            "original_request":request,"source":"album","target":"moved/album",
            "remote":{
                "destination":"moved/album","location":"moved/album",
                "version":{"peer-instance":1},"source":"album",
                "event_identity":["peer-instance",1],"files":contents,
                "directories":directories,
            },
            "uploads":uploads,
        });
        sqlx::query(
            "INSERT INTO pending_peer_ops(operation_id,idx,share_id,kind,vv_json,peer_id)
             VALUES('folder-restore:op-1',0,'share-1','folder_restore',?,'peer-1')",
        )
        .bind(manifest.to_string())
        .execute(&self.state.db)
        .await
        .unwrap();
        if !retained_journal {
            sqlx::query("UPDATE operation_items SET journal_json=NULL,progress_json=NULL,status='failed' WHERE operation_id='op-1'")
                .execute(&self.state.db).await.unwrap();
            sqlx::query("UPDATE operations SET status='failed' WHERE id='op-1'")
                .execute(&self.state.db)
                .await
                .unwrap();
        }
        uploads.into_values().collect()
    }
}

#[tokio::test]
async fn authenticated_folder_prerequisites_resume_the_original_after_restart() {
    for retained_journal in [false, true] {
        let mut fixture = Fixture::new(true, false).await;
        let uploads = fixture.folder_restore(retained_journal).await;
        for upload in &uploads {
            assert!(
                recover_folder_prerequisite_upload(&fixture.state, "op-1", upload)
                    .await
                    .unwrap()
            );
            fixture.assert_waiting().await;
        }
        fixture.restart().await;
        for upload in &uploads {
            assert!(
                recover_folder_prerequisite_upload(&fixture.state, "op-1", upload)
                    .await
                    .unwrap()
            );
        }
        assert!(
            resume_recovered_folder_operation(&fixture.state, "op-1")
                .await
                .unwrap()
        );
        assert!(!fixture.base.join("album").exists());
        assert_eq!(
            fs::read(fixture.base.join("moved/album/nested/photo.jpg")).unwrap(),
            b"synthetic photo"
        );
        assert!(fixture.base.join("moved/album/nested/empty").is_dir());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM operations")
                .fetch_one(&fixture.state.db)
                .await
                .unwrap(),
            1
        );
        assert!(
            resume_recovered_folder_operation(&fixture.state, "op-1")
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn folder_prerequisite_restarts_between_filesystem_and_atomic_finalization() {
    let mut fixture = Fixture::new(true, false).await;
    let uploads = fixture.folder_restore(false).await;
    sqlx::query(
        "CREATE TRIGGER fail_restore_finalization BEFORE UPDATE OF status ON uploads
         WHEN NEW.status='committed'
         BEGIN SELECT RAISE(ABORT,'synthetic restoration interruption'); END",
    )
    .execute(&fixture.state.db)
    .await
    .unwrap();
    assert!(
        recover_folder_prerequisite_upload(&fixture.state, "op-1", &uploads[0])
            .await
            .is_err()
    );
    let (_, _, _, evidence) = folder_parent(&fixture.state, "op-1").await.unwrap();
    assert_eq!(evidence["folder_prerequisites"][0]["done"], false);
    sqlx::query("DROP TRIGGER fail_restore_finalization")
        .execute(&fixture.state.db)
        .await
        .unwrap();
    fixture.restart().await;
    for upload in uploads {
        assert!(
            recover_folder_prerequisite_upload(&fixture.state, "op-1", &upload)
                .await
                .unwrap()
        );
    }
    assert!(
        resume_recovered_folder_operation(&fixture.state, "op-1")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn folder_restoration_refuses_tampering_foreign_references_and_unfinished_prerequisites() {
    for corruption in ["stage", "foreign", "extra", "done"] {
        let fixture = Fixture::new(true, false).await;
        let uploads = fixture.folder_restore(true).await;
        match corruption {
            "stage" => {
                let root = files::root(&fixture.state.db, "root-1").await.unwrap();
                fs::write(
                    plan::staging_dir(&root, &uploads[0]).unwrap().join("0"),
                    b"changed",
                )
                .unwrap();
            }
            "foreign" => {
                sqlx::query("UPDATE uploads SET principal='peer:foreign' WHERE id=?")
                    .bind(&uploads[0])
                    .execute(&fixture.state.db)
                    .await
                    .unwrap();
            }
            "extra" => {
                fs::create_dir(fixture.base.join("album")).unwrap();
                fs::write(fixture.base.join("album/unknown.jpg"), b"unknown").unwrap();
            }
            "done" => {
                for upload in &uploads {
                    recover_folder_prerequisite_upload(&fixture.state, "op-1", upload)
                        .await
                        .unwrap();
                }
                fs::write(fixture.base.join("album/nested/photo.jpg"), b"changed").unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            recover_folder_prerequisite_upload(&fixture.state, "op-1", &uploads[0])
                .await
                .is_err(),
            "{corruption}"
        );
        assert!(
            resume_recovered_folder_operation(&fixture.state, "op-1")
                .await
                .is_err(),
            "{corruption}"
        );
        assert!(!fixture.base.join("moved/album").exists());
        fixture.assert_waiting().await;
    }
}

#[tokio::test]
async fn empty_authenticated_folder_restoration_needs_no_upload_or_child() {
    let fixture = Fixture::new(true, false).await;
    fs::remove_file(fixture.base.join("album/nested/photo.jpg")).unwrap();
    fs::remove_file(fixture.base.join("album/nested/photo.jpg.xmp")).unwrap();
    assert!(fixture.folder_restore(false).await.is_empty());
    assert!(
        resume_recovered_folder_operation(&fixture.state, "op-1")
            .await
            .unwrap()
    );
    assert!(fixture.base.join("moved/album/nested/empty").is_dir());
    assert!(!fixture.base.join("album").exists());
}

#[tokio::test]
async fn redundant_folder_prerequisite_bytes_are_retained_without_trashing_assets() {
    let mut fixture = Fixture::new(true, false).await;
    let uploads = fixture.folder_restore(true).await;
    let root = files::root(&fixture.state.db, "root-1").await.unwrap();
    fs::create_dir_all(fixture.base.join("album/nested")).unwrap();
    fs::copy(
        plan::staging_dir(&root, &uploads[0]).unwrap().join("0"),
        fixture.base.join("album/nested/photo.jpg"),
    )
    .unwrap();
    for upload in &uploads {
        recover_folder_prerequisite_upload(&fixture.state, "op-1", upload)
            .await
            .unwrap();
    }
    fixture.restart().await;
    assert!(
        resume_recovered_folder_operation(&fixture.state, "op-1")
            .await
            .unwrap()
    );
    let trash = fixture.trash().await;
    assert_eq!(trash.len(), 1);
    assert_eq!(fs::read(&trash[0].0).unwrap(), b"synthetic photo");
    assert_eq!(trash[0].1, "[]");
    assert!(trash[0].2.is_none());
}

#[tokio::test]
async fn linked_folder_prerequisites_preserve_the_journaled_asset_and_original_hold() {
    for valid_owner in [false, true] {
        let server = MockServer::start().await;
        let mut fixture = Fixture::new(true, false).await;
        fixture.immich(&server).await;
        sqlx::query(
            "INSERT INTO immich_libraries(id,server_id,name,owner_user_id,import_paths_json,refreshed_at)
             VALUES('library-1','server-1','Synthetic library','owner-1','[\"/library\"]',?)",
        ).bind(db::now()).execute(&fixture.state.db).await.unwrap();
        let Some(Step::Api(effect)) = fixture.plan.steps.last_mut() else {
            panic!("missing API effect")
        };
        let ApiKind::SyncMove {
            source_path,
            destination_path,
            ..
        } = &mut effect.kind
        else {
            panic!("wrong API effect")
        };
        *source_path = "/library/album/nested/photo.jpg".into();
        *destination_path = "/library/moved/album/nested/photo.jpg".into();
        journal(&fixture.state, "op-1", 0, &fixture.plan)
            .await
            .unwrap();
        let uploads = fixture.folder_restore(true).await;
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let asset_published = published.clone();
        Mock::given(method("GET"))
            .and(path("/api/assets/asset-1"))
            .respond_with(move |_: &wiremock::Request| {
                let asset_path = if asset_published.load(std::sync::atomic::Ordering::SeqCst) {
                    "/library/moved/album/nested/photo.jpg"
                } else {
                    "/library/album/nested/photo.jpg"
                };
                ResponseTemplate::new(200).set_body_json(json!({
                    "id":"asset-1","originalPath":asset_path,"libraryId":"library-1",
                    "ownerId":if valid_owner {"owner-1"} else {"foreign-owner"},"isTrashed":false,
                }))
            })
            .mount(&server)
            .await;
        let search_published = published.clone();
        Mock::given(method("POST"))
            .and(path("/api/search/metadata"))
            .respond_with(move |_: &wiremock::Request| {
                let asset_path = if search_published.load(std::sync::atomic::Ordering::SeqCst) {
                    "/library/moved/album/nested/photo.jpg"
                } else {
                    "/library/album/nested/photo.jpg"
                };
                ResponseTemplate::new(200).set_body_json(json!({
                    "assets":{"items":[{"id":"asset-1","originalPath":asset_path}]},
                }))
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/users/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"id":"owner-1","isAdmin":true})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/api-keys/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id":"synthetic-key","permissions":["library.update"],
            })))
            .mount(&server)
            .await;
        let move_published = published.clone();
        Mock::given(method("POST"))
            .and(path("/api/libraries/library-1/files/move"))
            .and(wiremock::matchers::body_json(json!({
                "sourcePath":"/library/album/nested/photo.jpg",
                "destinationPath":"/library/moved/album/nested/photo.jpg","assetId":"asset-1",
            })))
            .respond_with(move |_: &wiremock::Request| {
                move_published.store(true, std::sync::atomic::Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({}))
            })
            .expect(if valid_owner { 1 } else { 0 })
            .mount(&server)
            .await;
        if valid_owner {
            for upload in uploads {
                recover_folder_prerequisite_upload(&fixture.state, "op-1", &upload)
                    .await
                    .unwrap();
            }
            fixture.assert_waiting().await;
            fixture.restart().await;
            assert!(
                resume_recovered_folder_operation(&fixture.state, "op-1")
                    .await
                    .unwrap()
            );
            let maintenance = registry::server(&fixture.state, "server-1")
                .await
                .unwrap()
                .maintenance;
            assert_eq!(maintenance.unresolved_recoveries, 0);
            assert!(published.load(std::sync::atomic::Ordering::SeqCst));
            assert!(
                server
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .all(|request| !request.url.path().ends_with("/files/add"))
            );
        } else {
            assert!(
                recover_folder_prerequisite_upload(&fixture.state, "op-1", &uploads[0])
                    .await
                    .is_err()
            );
            fixture.assert_waiting().await;
            let maintenance = registry::server(&fixture.state, "server-1")
                .await
                .unwrap()
                .maintenance;
            assert_eq!(maintenance.unresolved_recoveries, 1);
            assert!(maintenance.queue_paused_by_ism);
            assert!(!fixture.base.join("album").exists());
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn absence_rejects_symlink_components_and_hidden_paths() {
    let fixture = Fixture::new(false, false).await;
    let root = files::root(&fixture.state.db, "root-1").await.unwrap();
    std::os::unix::fs::symlink("missing", fixture.base.join("link")).unwrap();
    for path in ["link/photo.jpg", "link", ".ism-staging/uploads/foreign/0"] {
        let rel = ism_core::paths::RelPath::parse(path).unwrap();
        assert!(
            verified_file_absence_at_root(&fixture.state, &root, &rel)
                .await
                .is_err(),
            "{path}"
        );
    }
}

#[tokio::test]
async fn absence_verification_is_read_only_and_rejects_missing_immich_proof() {
    let mut fixture = Fixture::new(false, false).await;
    let root = files::root(&fixture.state.db, "root-1").await.unwrap();
    assert!(
        verified_file_absence_at_root(
            &fixture.state,
            &root,
            &files::parse_rel("missing.jpg").unwrap()
        )
        .await
        .is_ok()
    );
    assert!(
        verified_file_absence_at_root(
            &fixture.state,
            &root,
            &files::parse_rel("photo.jpg").unwrap()
        )
        .await
        .is_err()
    );
    let server = MockServer::start().await;
    fixture.immich(&server).await;
    let root = files::root(&fixture.state.db, "root-1").await.unwrap();
    assert!(
        verified_file_absence_at_root(
            &fixture.state,
            &root,
            &files::parse_rel("missing.jpg").unwrap()
        )
        .await
        .is_err()
    );
    fixture.assert_waiting().await;
    assert_eq!(
        fs::read(fixture.base.join("photo.jpg")).unwrap(),
        b"synthetic photo"
    );
}

#[test]
fn absence_accepts_only_trashed_references_in_the_exact_library_and_owner() {
    let mut asset = pg::AssetRow {
        id: "asset-1".into(),
        owner_id: "owner-1".into(),
        library_id: Some("library-1".into()),
        original_path: "/library/missing.jpg".into(),
        live_photo_video_id: None,
        stack_id: None,
        duplicate_id: None,
        trashed: true,
    };
    assert!(verify_absent_asset(&asset, "library-1", "owner-1").is_ok());
    asset.trashed = false;
    assert!(verify_absent_asset(&asset, "library-1", "owner-1").is_err());
    asset.trashed = true;
    assert!(verify_absent_asset(&asset, "foreign-library", "owner-1").is_err());
    assert!(verify_absent_asset(&asset, "library-1", "foreign-owner").is_err());
}

async fn published_asset(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/search/metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "assets":{"items":[{"id":"asset-1","originalPath":"/library/moved/photo.jpg"}]},
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/assets/asset-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"asset-1","originalPath":"/library/moved/photo.jpg","libraryId":"library-1",
            "ownerId":"owner-1","isTrashed":false,
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn terminal_status_wakes_sync_after_the_terminal_state_is_persisted() {
    let fixture = Fixture::new(false, false).await;
    let wake = fixture.state.sync_wake.notified();
    tokio::pin!(wake);
    wake.as_mut().enable();
    let waiting_peer = async {
        wake.await;
        sqlx::query_scalar::<_, String>("SELECT status FROM operations WHERE id='op-1'")
            .fetch_one(&fixture.state.db)
            .await
            .unwrap()
    };
    let (observed, transition) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(
            waiting_peer,
            set_status(&fixture.state, "op-1", OperationStatus::Completed)
        )
    })
    .await
    .expect("terminal persistence did not wake the waiting peer");
    transition.unwrap();
    assert_eq!(observed, "completed");
}

#[tokio::test]
async fn peer_publication_restarts_after_peer_bookkeeping_failure() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    let stage = fixture.peer_upload(Some(&server)).await;
    published_asset(&server).await;
    sqlx::query(
        "CREATE TRIGGER fail_publication_bookkeeping BEFORE DELETE ON pending_peer_ops
         BEGIN SELECT RAISE(ABORT,'synthetic bookkeeping interruption'); END",
    )
    .execute(&fixture.state.db)
    .await
    .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    assert!(!stage.exists());
    assert_eq!(
        registry::server(&fixture.state, "server-1")
            .await
            .unwrap()
            .maintenance
            .unresolved_recoveries,
        1
    );
    let status: String = sqlx::query_scalar("SELECT status FROM uploads WHERE id='up-1'")
        .fetch_one(&fixture.state.db)
        .await
        .unwrap();
    assert_eq!(status, "committed");
    fixture.restart().await;
    sqlx::query("DROP TRIGGER fail_publication_bookkeeping")
        .execute(&fixture.state.db)
        .await
        .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pending_peer_ops WHERE operation_id='op-1'")
            .fetch_one(&fixture.state.db)
            .await
            .unwrap();
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn indexed_but_invalid_peer_asset_does_not_finalize_or_release_hold() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    fixture.peer_upload(Some(&server)).await;
    Mock::given(method("POST"))
        .and(path("/api/search/metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "assets":{"items":[{"id":"asset-1","originalPath":"/library/moved/photo.jpg"}]},
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/assets/asset-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"asset-1","originalPath":"/library/moved/photo.jpg","libraryId":"library-1",
            "ownerId":"foreign-owner","isTrashed":false,
        })))
        .mount(&server)
        .await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    let status: String = sqlx::query_scalar("SELECT status FROM uploads WHERE id='up-1'")
        .fetch_one(&fixture.state.db)
        .await
        .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(
        registry::server(&fixture.state, "server-1")
            .await
            .unwrap()
            .maintenance
            .unresolved_recoveries,
        1
    );
}

#[tokio::test]
async fn held_peer_publication_finishes_trusted_stage_after_restart() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    let stage = fixture.peer_upload(Some(&server)).await;
    let (original_journal, _) = fixture.evidence().await;
    published_asset(&server).await;
    fixture.restart().await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert!(!stage.parent().unwrap().exists());
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg")).unwrap(),
        b"synthetic photo"
    );
    let (status, asset): (String, Option<String>) = sqlx::query_as(
        "SELECT u.status,json_extract(i.result_json,'$.asset_id') FROM uploads u
             JOIN operation_items i ON i.operation_id=u.operation_id WHERE u.id='up-1'",
    )
    .fetch_one(&fixture.state.db)
    .await
    .unwrap();
    assert_eq!(status, "committed");
    assert_eq!(asset.as_deref(), Some("asset-1"));
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pending_peer_ops WHERE operation_id='op-1'")
            .fetch_one(&fixture.state.db)
            .await
            .unwrap();
    assert_eq!(pending, 0);
    let recorded: String = sqlx::query_scalar(
        "SELECT sha256 FROM replica_files WHERE share_id='share-1' AND rel_path='moved/photo.jpg'",
    )
    .fetch_one(&fixture.state.db)
    .await
    .unwrap();
    assert_eq!(
        recorded,
        files::sha256_file(&fixture.base.join("moved/photo.jpg")).unwrap()
    );
    let maintenance = registry::server(&fixture.state, "server-1")
        .await
        .unwrap()
        .maintenance;
    assert_eq!(maintenance.unresolved_recoveries, 0);
    assert_eq!(maintenance.phase, ism_core::maintenance::Phase::Resuming);
    let (mut final_journal, progress) = fixture.evidence().await;
    final_journal
        .as_object_mut()
        .unwrap()
        .remove("recovery_execution");
    assert_eq!(final_journal, original_journal);
    assert!(progress.steps.iter().all(|step| step.status == "done"));
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
}

#[tokio::test]
async fn already_published_peer_upload_retains_verified_redundant_stage() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    let stage = fixture.peer_upload(Some(&server)).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::copy(&stage, fixture.base.join("moved/photo.jpg")).unwrap();
    published_asset(&server).await;
    fixture.finish_files().await.unwrap();
    fixture.restart().await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let trash = fixture.trash().await;
    assert_eq!(trash.len(), 1);
    assert_eq!(fs::read(&trash[0].0).unwrap(), b"synthetic photo");
    assert_eq!(trash[0].1, "[]");
    assert!(trash[0].2.is_none());
    assert!(!stage.exists());
}

#[tokio::test]
async fn published_peer_upload_without_stage_finishes_after_restart() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    let stage = fixture.peer_upload(Some(&server)).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::rename(&stage, fixture.base.join("moved/photo.jpg")).unwrap();
    published_asset(&server).await;
    fixture.restart().await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert!(fixture.trash().await.is_empty());
}

#[tokio::test]
async fn held_peer_publication_retains_barrier_until_immich_effect_is_verified() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    fixture.peer_upload(Some(&server)).await;
    Mock::given(method("POST"))
        .and(path("/api/search/metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"assets":{"items":[]}})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/api-keys/me"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id":"key-1","permissions":["all"]})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/users/me"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id":"owner-1","isAdmin":true})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/library-1/files/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    let maintenance = registry::server(&fixture.state, "server-1")
        .await
        .unwrap()
        .maintenance;
    assert!(maintenance.queue_paused_by_ism);
    assert_eq!(maintenance.unresolved_recoveries, 1);
    let status: String = sqlx::query_scalar("SELECT status FROM uploads WHERE id='up-1'")
        .fetch_one(&fixture.state.db)
        .await
        .unwrap();
    assert_eq!(status, "failed");
    fixture.restart().await;
    server.reset().await;
    published_asset(&server).await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
}

#[tokio::test]
async fn changed_peer_stage_or_final_content_never_releases_original_hold() {
    for changed_stage in [true, false] {
        let server = MockServer::start().await;
        let mut fixture = Fixture::new(false, false).await;
        let stage = fixture.peer_upload(Some(&server)).await;
        fs::create_dir(fixture.base.join("moved")).unwrap();
        let final_path = fixture.base.join("moved/photo.jpg");
        fs::write(&final_path, b"synthetic photo").unwrap();
        if changed_stage {
            // Even a shorter matching prefix is not a complete authenticated staging file.
            fs::write(&stage, b"synthetic").unwrap();
        } else {
            fs::write(&final_path, b"changed final bytes").unwrap();
        }
        fixture.restart().await;
        assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
        fixture.assert_waiting().await;
        assert!(stage.exists());
        assert!(final_path.exists());
        assert!(fixture.trash().await.is_empty());
        assert_eq!(
            registry::server(&fixture.state, "server-1")
                .await
                .unwrap()
                .maintenance
                .unresolved_recoveries,
            1
        );
    }
}

#[tokio::test]
async fn foreign_upload_owner_association_root_and_stage_are_refused() {
    for corruption in [
        "owner",
        "association",
        "root",
        "stage",
        "typed-stage",
        "peer",
        "hold",
        "upload_done",
        "principal",
    ] {
        let server = MockServer::start().await;
        let mut fixture = Fixture::new(false, false).await;
        let stage = fixture.peer_upload(Some(&server)).await;
        match corruption {
            "owner" => {
                sqlx::query("UPDATE uploads SET principal='peer:foreign-peer' WHERE id='up-1'")
                    .execute(&fixture.state.db)
                    .await
                    .unwrap();
            }
            "association" => {
                sqlx::query("UPDATE uploads SET operation_id='foreign-operation' WHERE id='up-1'")
                    .execute(&fixture.state.db)
                    .await
                    .unwrap();
            }
            "root" => {
                sqlx::query("UPDATE uploads SET request_json=json_set(request_json,'$.target.root_id','foreign-root') WHERE id='up-1'")
                        .execute(&fixture.state.db).await.unwrap();
            }
            "stage" | "typed-stage" => {
                let foreign = stage
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("foreign-upload/0");
                fs::create_dir(foreign.parent().unwrap()).unwrap();
                fs::write(&foreign, b"synthetic photo").unwrap();
                let (mut journal, _) = fixture.evidence().await;
                if corruption == "stage" {
                    journal["steps"][2] = json!(format!(
                        "move {} -> {}",
                        foreign.display(),
                        fixture.base.join("moved/photo.jpg").display(),
                    ));
                }
                journal["execution_steps"][2]["Fs"]["from"] = json!(foreign);
                sqlx::query("UPDATE operation_items SET journal_json=? WHERE operation_id='op-1'")
                    .bind(journal.to_string())
                    .execute(&fixture.state.db)
                    .await
                    .unwrap();
            }
            "peer" => {
                sqlx::query(
                    "UPDATE pending_peer_ops SET peer_id='foreign-peer' WHERE operation_id='op-1'",
                )
                .execute(&fixture.state.db)
                .await
                .unwrap();
            }
            "hold" => {
                sqlx::query("UPDATE operations SET admitted_server_ids_json='[]' WHERE id='op-1'")
                    .execute(&fixture.state.db)
                    .await
                    .unwrap();
            }
            "upload_done" => {
                sqlx::query("UPDATE operation_items SET journal_json=json_set(journal_json,'$.upload_done','foreign-upload') WHERE operation_id='op-1'")
                    .execute(&fixture.state.db).await.unwrap();
            }
            "principal" => {
                sqlx::query("UPDATE operations SET principal='admin:synthetic',source='admin' WHERE id='op-1'")
                    .execute(&fixture.state.db).await.unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            recover_bookkeeping(&fixture.state, "op-1").await.is_err(),
            "{corruption}"
        );
        assert_eq!(fs::read(&stage).unwrap(), b"synthetic photo");
        assert!(!fixture.base.join("moved/photo.jpg").exists());
        fixture.assert_waiting().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_or_unknown_peer_stage_entries_are_not_cleaned_up() {
    for symlink in [true, false] {
        let server = MockServer::start().await;
        let mut fixture = Fixture::new(false, false).await;
        let stage = fixture.peer_upload(Some(&server)).await;
        if symlink {
            let foreign = fixture.base.join("foreign.jpg");
            fs::rename(&stage, &foreign).unwrap();
            std::os::unix::fs::symlink(&foreign, &stage).unwrap();
        } else {
            fs::write(
                stage.parent().unwrap().join("foreign.txt"),
                b"unknown bytes",
            )
            .unwrap();
        }
        assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
        assert!(!fixture.base.join("moved/photo.jpg").exists());
        assert!(stage.parent().unwrap().exists());
        fixture.assert_waiting().await;
    }
}

impl Fixture {
    async fn trash(&self) -> Vec<(PathBuf, String, Option<String>)> {
        let records: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT quarantine_rel,path,asset_ids_json,server_id FROM trash ORDER BY id",
        )
        .fetch_all(&self.state.db)
        .await
        .unwrap();
        records
            .into_iter()
            .map(|(quarantine, path, ids, server)| {
                (self.base.join(quarantine).join(path), ids, server)
            })
            .collect()
    }

    async fn assert_waiting(&self) {
        let (status, resolved): (String, i64) =
            sqlx::query_as("SELECT status,recovery_resolved FROM operations WHERE id='op-1'")
                .fetch_one(&self.state.db)
                .await
                .unwrap();
        assert_eq!((status.as_str(), resolved), ("recovery_required", 0));
    }

    async fn immich(&mut self, server: &MockServer) {
        let maintenance = ism_core::maintenance::MaintenanceState {
            phase: ism_core::maintenance::Phase::RecoveryRequired,
            unresolved_recoveries: 1,
            queue_paused_by_ism: true,
            ..Default::default()
        };
        sqlx::query(
            "INSERT INTO immich_servers(id,name,url,external_library_move_mode,maintenance_json,created_at)
             VALUES('server-1','Synthetic Immich',?,'api_only',?,?)",
        ).bind(server.uri()).bind(serde_json::to_string(&maintenance).unwrap())
            .bind(db::now()).execute(&self.state.db).await.unwrap();
        sqlx::query(
            "INSERT INTO immich_accounts(id,server_id,label,email,immich_user_id,is_admin,api_key_enc,status,created_at)
             VALUES('account-1','server-1','Synthetic user','synthetic@example.test','owner-1',1,?,'ok',?)",
        ).bind(self.state.secrets.encrypt(registry::API_KEY_PURPOSE, "synthetic-key").unwrap())
            .bind(db::now()).execute(&self.state.db).await.unwrap();
        sqlx::query(
            "UPDATE mappings SET server_id='server-1',library_id='library-1',immich_import_path='/library'
             WHERE root_id='root-1'",
        ).execute(&self.state.db).await.unwrap();
        sqlx::query(
            "UPDATE operations SET admitted_server_ids_json='[\"server-1\"]' WHERE id='op-1'",
        )
        .execute(&self.state.db)
        .await
        .unwrap();
        self.plan.steps.push(Step::Api(ApiStep {
            server_id: "server-1".into(),
            owner: "owner-1".into(),
            ids: vec!["asset-1".into()],
            kind: ApiKind::SyncMove {
                source_library_id: "library-1".into(),
                destination_library_id: None,
                source_path: "/library/photo.jpg".into(),
                destination_path: "/library/moved/photo.jpg".into(),
                asset_id: Some("asset-1".into()),
                from_owner: None,
                to_owner: None,
            },
        }));
        journal(&self.state, "op-1", 0, &self.plan).await.unwrap();
    }
}

#[tokio::test]
async fn interrupted_file_finish_and_repeated_recovery_are_idempotent() {
    let fixture = Fixture::new(false, false).await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert!(!fixture.base.join("photo.jpg").exists());
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg")).unwrap(),
        b"synthetic photo"
    );
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert!(fixture.trash().await.is_empty());
}

#[tokio::test]
async fn file_group_finishes_after_only_original_moved() {
    let fixture = Fixture::new(false, true).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::rename(
        fixture.base.join("photo.jpg"),
        fixture.base.join("moved/photo.jpg"),
    )
    .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg.xmp")).unwrap(),
        b"synthetic sidecar"
    );
    assert!(!fixture.base.join("photo.jpg.xmp").exists());
}

#[tokio::test]
async fn complete_destination_retains_partially_removed_source_folder() {
    let fixture = Fixture::new(true, false).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::rename(fixture.base.join("album"), fixture.base.join("moved/album")).unwrap();
    fs::create_dir_all(fixture.base.join("album/nested/empty")).unwrap();
    fs::write(
        fixture.base.join("album/nested/photo.jpg.xmp"),
        b"synthetic sidecar",
    )
    .unwrap();
    fixture.finish_files().await.unwrap();
    let trash = fixture.trash().await;
    assert_eq!(trash.len(), 1);
    assert_eq!(
        fs::read(trash[0].0.join("nested/photo.jpg.xmp")).unwrap(),
        b"synthetic sidecar"
    );
    assert!(trash[0].0.join("nested/empty").is_dir());
    assert_eq!(trash[0].1, "[]");
    assert!(trash[0].2.is_none());
    // The operation is still waiting for metadata/bookkeeping, as after a process restart.
    fixture.assert_waiting().await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert_eq!(fixture.trash().await.len(), 1);
}

#[tokio::test]
async fn full_source_retains_matching_incomplete_destination_folder() {
    let mut fixture = Fixture::new(true, false).await;
    let (original_journal, _) = fixture.evidence().await;
    fs::create_dir_all(fixture.base.join("moved/album/nested")).unwrap();
    fs::write(
        fixture.base.join("moved/album/nested/photo.jpg"),
        b"synthetic",
    )
    .unwrap();
    fixture.finish_files().await.unwrap();
    // Restart replays the completed archive without confusing the new canonical target with debris.
    fixture.restart().await;
    fixture.finish_files().await.unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let trash = fixture.trash().await;
    assert_eq!(trash.len(), 1);
    assert_eq!(
        fs::read(trash[0].0.join("nested/photo.jpg")).unwrap(),
        b"synthetic"
    );
    assert!(!trash[0].0.join("nested/photo.jpg.xmp").exists());
    assert_eq!(
        fs::read(fixture.base.join("moved/album/nested/photo.jpg.xmp")).unwrap(),
        b"synthetic sidecar"
    );
    let (mut final_journal, _) = fixture.evidence().await;
    final_journal
        .as_object_mut()
        .unwrap()
        .remove("recovery_execution");
    assert_eq!(final_journal, original_journal);
    let stored_action: String =
        sqlx::query_scalar("SELECT action_json FROM operation_items WHERE operation_id='op-1'")
            .fetch_one(&fixture.state.db)
            .await
            .unwrap();
    assert_eq!(
        stored_action,
        serde_json::to_string(&fixture.action).unwrap()
    );
}

#[tokio::test]
async fn truncated_file_destination_is_retained_only_when_all_bytes_match() {
    let fixture = Fixture::new(false, false).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::write(fixture.base.join("moved/photo.jpg"), b"synthetic").unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let trash = fixture.trash().await;
    assert_eq!(trash.len(), 1);
    assert_eq!(fs::read(&trash[0].0).unwrap(), b"synthetic");
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg")).unwrap(),
        b"synthetic photo"
    );
}

#[tokio::test]
async fn changed_destination_refuses_the_entire_file_group() {
    let fixture = Fixture::new(false, true).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::write(fixture.base.join("moved/photo.jpg.xmp"), b"tampered").unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    assert_eq!(
        fs::read(fixture.base.join("photo.jpg")).unwrap(),
        b"synthetic photo"
    );
    assert!(!fixture.base.join("moved/photo.jpg").exists());
    assert!(fixture.trash().await.is_empty());
}

#[tokio::test]
async fn unexpected_source_entries_are_never_moved_or_deleted() {
    let fixture = Fixture::new(true, false).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::rename(fixture.base.join("album"), fixture.base.join("moved/album")).unwrap();
    fs::create_dir_all(fixture.base.join("album/nested")).unwrap();
    fs::write(
        fixture.base.join("album/nested/unexpected.txt"),
        b"unknown bytes",
    )
    .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    assert_eq!(
        fs::read(fixture.base.join("album/nested/unexpected.txt")).unwrap(),
        b"unknown bytes"
    );
    fixture.assert_waiting().await;
    assert!(fixture.trash().await.is_empty());
}

#[tokio::test]
async fn recovery_archive_intent_survives_interruption_after_rename() {
    let fixture = Fixture::new(false, false).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::write(fixture.base.join("moved/photo.jpg"), b"synthetic photo").unwrap();
    fixture.finish_files().await.unwrap();
    let (mut journal, _) = fixture.evidence().await;
    // Simulate a crash after the archive move but before its completion flag was durable.
    journal["recovery_execution"][0]["done"] = json!(false);
    sqlx::query("DELETE FROM trash")
        .execute(&fixture.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE operation_items SET journal_json=? WHERE operation_id='op-1'")
        .bind(journal.to_string())
        .execute(&fixture.state.db)
        .await
        .unwrap();
    fixture.finish_files().await.unwrap();
    assert_eq!(fixture.trash().await.len(), 1);
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
}

#[tokio::test]
async fn missing_content_or_legacy_inventory_never_fabricates_recovery_proof() {
    let fixture = Fixture::new(false, false).await;
    let (mut journal, _) = fixture.evidence().await;
    journal["verification"] = json!([]);
    sqlx::query("UPDATE operation_items SET journal_json=? WHERE operation_id='op-1'")
        .bind(journal.to_string())
        .execute(&fixture.state.db)
        .await
        .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    assert!(fixture.base.join("photo.jpg").is_file());
    fixture.assert_waiting().await;
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_in_partial_folder_is_refused() {
    let fixture = Fixture::new(true, false).await;
    fs::create_dir_all(fixture.base.join("moved/album")).unwrap();
    std::os::unix::fs::symlink("nested", fixture.base.join("moved/album/link")).unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    assert!(fixture.base.join("album/nested/photo.jpg").is_file());
    fixture.assert_waiting().await;
    assert!(fixture.trash().await.is_empty());
}

#[tokio::test]
async fn invalid_immich_effect_keeps_barrier_and_valid_retry_preserves_asset_identity() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    fixture.immich(&server).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::write(fixture.base.join("moved/photo.jpg"), b"synthetic photo").unwrap();
    Mock::given(method("GET")).and(path("/api/assets/asset-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"asset-1","originalPath":"/library/photo.jpg","libraryId":"library-1","ownerId":"owner-1",
        }))).mount(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/libraries/library-1/files/move"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    let maintenance = registry::server(&fixture.state, "server-1")
        .await
        .unwrap()
        .maintenance;
    assert!(maintenance.queue_paused_by_ism);
    assert_eq!(maintenance.unresolved_recoveries, 1);
    assert_eq!(
        maintenance.phase,
        ism_core::maintenance::Phase::RecoveryRequired
    );
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg")).unwrap(),
        b"synthetic photo"
    );
    server.reset().await;
    Mock::given(method("GET")).and(path("/api/assets/asset-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"asset-1","originalPath":"/library/moved/photo.jpg","libraryId":"library-1","ownerId":"owner-1",
        }))).mount(&server).await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let maintenance = registry::server(&fixture.state, "server-1")
        .await
        .unwrap()
        .maintenance;
    assert_eq!(maintenance.unresolved_recoveries, 0);
    assert_eq!(maintenance.phase, ism_core::maintenance::Phase::Resuming);
    assert!(maintenance.queue_paused_by_ism);
    let trash = fixture.trash().await;
    assert_eq!(trash.len(), 1);
    assert_eq!(trash[0].1, "[]");
    assert!(trash[0].2.is_none());
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method.as_str() != "DELETE")
    );
}

#[tokio::test]
async fn legacy_journal_with_full_content_proof_still_finishes_file_moves() {
    let mut fixture = Fixture::new(false, false).await;
    let source = fixture.base.join("photo.jpg");
    fixture.plan.steps.insert(
        0,
        Step::CheckHash {
            path: source.clone(),
            expected_size: fs::metadata(&source).unwrap().len(),
            expected_sha256: files::sha256_file(&source).unwrap(),
        },
    );
    journal(&fixture.state, "op-1", 0, &fixture.plan)
        .await
        .unwrap();
    let (mut journal, _) = fixture.evidence().await;
    journal.as_object_mut().unwrap().remove("execution_steps");
    sqlx::query("UPDATE operation_items SET journal_json=? WHERE operation_id='op-1'")
        .bind(journal.to_string())
        .execute(&fixture.state.db)
        .await
        .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let (_, progress) = fixture.evidence().await;
    assert!(progress.steps.iter().all(|step| step.status == "done"));
}

#[tokio::test]
async fn missing_both_endpoints_and_tampered_retained_trash_stay_waiting() {
    let fixture = Fixture::new(false, false).await;
    fs::remove_file(fixture.base.join("photo.jpg")).unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    let fixture = Fixture::new(false, false).await;
    fs::create_dir(fixture.base.join("moved")).unwrap();
    fs::write(fixture.base.join("moved/photo.jpg"), b"synthetic").unwrap();
    fixture.finish_files().await.unwrap();
    fs::write(&fixture.trash().await[0].0, b"changed retained bytes").unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.is_err());
    fixture.assert_waiting().await;
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg")).unwrap(),
        b"synthetic photo"
    );
}

#[tokio::test]
async fn recovery_preserves_shared_companion_source() {
    let mut fixture = Fixture::new(false, true).await;
    fixture.plan.files[1].copy = true;
    let Step::Fs(companion) = &mut fixture.plan.steps[1] else {
        panic!("expected filesystem step")
    };
    companion.copy = true;
    journal(&fixture.state, "op-1", 0, &fixture.plan)
        .await
        .unwrap();
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    assert_eq!(
        fs::read(fixture.base.join("photo.jpg.xmp")).unwrap(),
        b"synthetic sidecar"
    );
    assert_eq!(
        fs::read(fixture.base.join("moved/photo.jpg.xmp")).unwrap(),
        b"synthetic sidecar"
    );
}

#[tokio::test]
async fn missing_original_maintenance_hold_refuses_filesystem_mutation() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    fixture.immich(&server).await;
    sqlx::query("UPDATE immich_servers SET maintenance_json=? WHERE id='server-1'")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .execute(&fixture.state.db)
        .await
        .unwrap();
    let error = recover_bookkeeping(&fixture.state, "op-1")
        .await
        .unwrap_err();
    assert_eq!(error.code(), "recovery_maintenance");
    assert!(fixture.base.join("photo.jpg").is_file());
    assert!(!fixture.base.join("moved/photo.jpg").exists());
    assert!(fixture.trash().await.is_empty());
    fixture.assert_waiting().await;
}

#[tokio::test]
async fn api_native_interruption_acquires_original_operation_barrier_without_child() {
    let server = MockServer::start().await;
    let mut fixture = Fixture::new(false, false).await;
    fixture.immich(&server).await;
    sqlx::query("UPDATE operations SET admitted_server_ids_json='[]' WHERE id='op-1'")
        .execute(&fixture.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE immich_servers SET maintenance_json=? WHERE id='server-1'")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .execute(&fixture.state.db)
        .await
        .unwrap();
    Mock::given(method("GET"))
        .and(path("/api/admin/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "library":{"watch":{"enabled":false},"scan":{"enabled":false}},
        })))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/queues/library"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/queues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
            "name":"library","isPaused":true,"statistics":{"active":0},
        }])))
        .mount(&server)
        .await;
    Mock::given(method("GET")).and(path("/api/assets/asset-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"asset-1","originalPath":"/library/moved/photo.jpg","libraryId":"library-1","ownerId":"owner-1",
        }))).mount(&server).await;
    fixture.finish_files().await.unwrap();
    let maintenance = registry::server(&fixture.state, "server-1")
        .await
        .unwrap()
        .maintenance;
    assert!(maintenance.queue_paused_by_ism);
    assert_eq!(maintenance.unresolved_recoveries, 1);
    assert_eq!(maintenance.active_ops, 0);
    let admitted: String =
        sqlx::query_scalar("SELECT admitted_server_ids_json FROM operations WHERE id='op-1'")
            .fetch_one(&fixture.state.db)
            .await
            .unwrap();
    assert_eq!(admitted, "[\"server-1\"]");
    fixture.restart().await;
    assert!(recover_bookkeeping(&fixture.state, "op-1").await.unwrap());
    let maintenance = registry::server(&fixture.state, "server-1")
        .await
        .unwrap()
        .maintenance;
    assert_eq!(maintenance.unresolved_recoveries, 0);
    assert_eq!(maintenance.phase, ism_core::maintenance::Phase::Resuming);
    let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM operations")
        .fetch_one(&fixture.state.db)
        .await
        .unwrap();
    assert_eq!(operations, 1);
}
