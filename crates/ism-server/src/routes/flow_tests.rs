//! End-to-end tests of the HTTP API on a root that is not linked to Immich.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::config::Config;
use crate::crypto::SecretBox;
use crate::state::AppState;

struct Harness {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    app: Router,
    state: AppState,
    cookie: String,
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: bytes::Bytes,
}

#[tokio::test]
async fn conflict_http_filters_preserve_include_resolved_and_support_resolved_only() {
    let h = harness().await;
    h.map_root().await;
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('share','Archive','photos','','[]',1)",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let side = json!({
        "instance": "Example", "sha256": null, "size": null,
        "modified_at": null, "deleted": false,
    })
    .to_string();
    for (id, path, status) in [
        ("open", "Trips/open.jpg", "open"),
        ("resolved", "Archive/resolved.jpg", "resolved"),
    ] {
        sqlx::query(
            "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,
             remote_vv_json,peer_id,status,created_at)
             VALUES (?,'share',?,'changed_on_both','synthetic conflict',?,?,'{}','former-peer',?,?)",
        )
        .bind(id)
        .bind(path)
        .bind(&side)
        .bind(&side)
        .bind(status)
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    }
    let auth = [("cookie", h.cookie.as_str())];
    for (query, expected, folders) in [
        ("", vec!["open"], vec!["Trips"]),
        ("?include_resolved=false", vec!["open"], vec!["Trips"]),
        (
            "?include_resolved=true",
            vec!["open", "resolved"],
            vec!["Archive", "Trips"],
        ),
        ("?resolved_only=true", vec!["resolved"], vec!["Archive"]),
        (
            "?include_resolved=true&resolved_only=true",
            vec!["resolved"],
            vec!["Archive"],
        ),
    ] {
        let reply = send(
            &h.app,
            get(&format!("/api/admin/conflicts/page{query}"), &auth),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{query}: {}", reply.json());
        let page = reply.json();
        let mut statuses = page["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["conflict"]["status"].as_str().unwrap())
            .collect::<Vec<_>>();
        statuses.sort_unstable();
        assert_eq!(statuses, expected, "{query}");
        assert_eq!(page["total"], expected.len(), "{query}");
        assert_eq!(page["filters"][0]["folders"], json!(folders), "{query}");
    }
    let list = send(
        &h.app,
        get("/api/admin/conflicts?include_resolved=true", &auth),
    )
    .await;
    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(list.json().as_array().unwrap().len(), 2);
    let last = send(
        &h.app,
        get(
            "/api/admin/conflicts/page?include_resolved=true&limit=1&offset=1",
            &auth,
        ),
    )
    .await;
    assert_eq!(last.status, StatusCode::OK);
    assert_eq!(last.json()["total"], 2);
    assert_eq!(last.json()["rows"][0]["conflict"]["status"], "open");
}

#[tokio::test]
async fn watch_snapshot_ranges_keep_exact_paths_without_adjacent_prefixes_or_duplicates() {
    let h = harness().await;
    sqlx::query(
        "INSERT INTO mappings(root_id,name,container_path,created_at) VALUES('photos','Archive','/photos',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    for path in [
        "trip",
        "trip/a.jpg",
        "trip/nested/b.jpg",
        "trip0/a.jpg",
        "trip2",
    ] {
        sqlx::query(
            "INSERT INTO watch_snapshot(root_id,rel_path,size,mtime_ns,inode) VALUES('photos',?,1,2,3)",
        )
        .bind(path)
        .execute(&h.state.db)
        .await
        .unwrap();
    }
    let scoped = crate::watch::poll::load(&h.state, "photos", Some("trip"))
        .await
        .unwrap();
    assert_eq!(
        scoped.keys().map(String::as_str).collect::<Vec<_>>(),
        ["trip", "trip/a.jpg", "trip/nested/b.jpg"]
    );
    let all = crate::watch::poll::load(&h.state, "photos", None)
        .await
        .unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(
        crate::watch::poll::load(&h.state, "photos", Some("trip/a.jpg"))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn background_activity_requires_admin_and_keeps_current_work_outside_history_pagination() {
    let h = harness().await;
    let uri = "/api/admin/operations/background?limit=1&offset=99";
    assert_eq!(
        send(&h.app, get(uri, &[])).await.status,
        StatusCode::UNAUTHORIZED
    );
    let task = crate::activity::Task::start(
        &h.state,
        "scan:example",
        "Share scan",
        "Archive",
        Some("/peers"),
    )
    .await
    .unwrap();
    task.waiting("Waiting for the replication-operation lock");
    task.progress(15, None, "files discovered");
    let auth = [("cookie", h.cookie.as_str())];
    let reply = send(&h.app, get(uri, &auth)).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    assert_eq!(
        reply.json()["current"][0]["phase"],
        "Waiting for the replication-operation lock"
    );
    assert_eq!(reply.json()["current"][0]["done"], 15);
    assert!(reply.json()["current"][0]["total"].is_null());
    assert_eq!(reply.json()["history"].as_array().unwrap().len(), 0);
    assert_eq!(
        send(
            &h.app,
            get("/api/admin/operations/background?limit=0", &auth)
        )
        .await
        .status,
        StatusCode::BAD_REQUEST
    );
    task.finish(&Ok(())).await.unwrap();
    let reply = send(
        &h.app,
        get("/api/admin/operations/background?limit=1", &auth),
    )
    .await;
    assert_eq!(reply.json()["current"][0]["status"], "completed");
    assert_eq!(reply.json()["history"].as_array().unwrap().len(), 0);
    assert!(!reply.json()["warnings"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn share_scan_failure_is_visible_in_attention_and_background_http() {
    let h = harness().await;
    h.map_root().await;
    crate::peers::admin::create_share(
        &h.state,
        ism_core::dto::ShareInput {
            name: "Archive".into(),
            location: ism_core::dto::Location {
                root_id: "photos".into(),
                path: "2024".into(),
            },
            peer_ids: vec![],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let share = crate::peers::shares(&h.state).await.unwrap().remove(0);
    std::fs::remove_dir(h.root.join("2024")).unwrap();
    assert!(crate::peers::scan_share(&h.state, &share).await.is_err());
    let auth = [("cookie", h.cookie.as_str())];
    let response = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(response.status, StatusCode::OK);
    let body = response.json();
    let item = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "share_scan_failed")
        .unwrap();
    assert_eq!(item["share_id"], share.id);
    assert_eq!(item["share_name"], "Archive");
    assert_eq!(item["blocks_work"], true);
    assert_eq!(item["attention"], "action_required");
    assert_eq!(item["href"], "/peers");
    assert!(!item["message"].as_str().unwrap().contains("permission"));
    assert!(!item.to_string().contains(&*h.root.to_string_lossy()));
    let background = send(&h.app, get("/api/admin/operations/background", &auth)).await;
    assert_eq!(background.status, StatusCode::OK);
    let rows = background.json();
    let rows = rows["current"].as_array().unwrap();
    assert!(
        rows.iter()
            .any(|item| item["task_key"] == format!("share:{}", share.id))
    );
    let evidence = rows
        .iter()
        .find(|item| item["task_key"] == format!("share-scan:{}", share.id))
        .unwrap();
    assert_eq!(evidence["phase"], item["message"]);
    assert_eq!(evidence["page"], "/peers");
    std::fs::create_dir(h.root.join("2024")).unwrap();
    crate::peers::scan_share(&h.state, &share).await.unwrap();
    let recovered = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(
        !recovered["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["code"] == "share_scan_failed")
    );
}

#[tokio::test]
async fn attention_covers_standalone_local_conditions_and_clears_resolved_sources() {
    let h = harness().await;
    let auth = [("cookie", h.cookie.as_str())];
    let empty = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert_eq!(empty["completeness"], "complete");
    assert!(empty["warnings"].as_array().unwrap().is_empty());
    h.map_root().await;
    let maintenance = ism_core::maintenance::MaintenanceState {
        drift: Some("synthetic settings drift".into()),
        ..Default::default()
    };
    sqlx::query(
        "INSERT INTO immich_servers(id,name,url,compatible,maintenance_json,created_at)
         VALUES ('server','Example','https://immich.example',0,?,?)",
    )
    .bind(serde_json::to_string(&maintenance).unwrap())
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('share','Archive','photos','','[]',1)",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query("INSERT INTO watch_status(root_id,paused,last_pass_at) VALUES ('photos',1,?)")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO scan_schedule(library_id,server_id,last_error,last_finished_at)
         VALUES ('library','server','synthetic scan failure',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO scan_work(library_id,work_json) VALUES ('library','{}'),('library','{}')",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let mut tx = h.state.db.begin().await.unwrap();
    for i in 0..1_000 {
        sqlx::query(
            "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,
                                   remote_vv_json,peer_id,status,created_at)
             VALUES (?,'share',?,'edit','synthetic conflict','{}','{}','{}','former-peer','open',?)",
        ).bind(format!("decision-{i}")).bind(format!("photo-{i}.jpg"))
            .bind(crate::db::now()).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    std::fs::write(h.root.join(".ism-root"), "wrong-root").unwrap();

    let reply = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    assert!(reply.body.len() < 20_000);
    let snapshot = reply.json();
    assert_eq!(snapshot["completeness"], "complete");
    let items = snapshot["items"].as_array().unwrap();
    for (id, code, href) in [
        ("maintenance:server", "maintenance_blocked", "/servers"),
        ("compatibility:server", "maintenance_blocked", "/servers"),
        ("mapping:photos", "mapping_unavailable", "/mappings"),
        ("watch:photos", "watcher_paused", "/mappings"),
        ("library:library", "library_scan_failed", "/mappings"),
        (
            "conflict-decisions:share:former-peer",
            "recovery_decision",
            "/conflicts",
        ),
    ] {
        let item = items.iter().find(|item| item["id"] == id).unwrap();
        assert_eq!(item["code"], code);
        assert_eq!(item["href"], href);
        assert_eq!(item["blocks_work"], true);
        assert_eq!(item["owner"], "local");
    }
    let decisions = items
        .iter()
        .find(|item| item["code"] == "recovery_decision")
        .unwrap();
    assert_eq!(decisions["count"], 1_000);
    assert_eq!(decisions["peer_id"], "former-peer");
    assert_eq!(decisions["share_id"], "share");
    assert_eq!(decisions["examples"].as_array().unwrap().len(), 5);
    assert_eq!(
        items
            .iter()
            .find(|item| item["code"] == "library_scan_failed")
            .unwrap()["count"],
        2
    );
    let background = send(&h.app, get("/api/admin/operations/background", &auth)).await;
    assert_eq!(background.status, StatusCode::OK, "{}", background.json());
    let background = background.json();
    for key in [
        "mapping:photos",
        "compatibility:server",
        "conflict-decisions:share:former-peer",
    ] {
        assert!(
            background["current"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["task_key"] == key)
        );
    }

    // Acknowledged rows awaiting cleanup are no longer retained work.
    sqlx::query("UPDATE scan_schedule SET acked_through=(SELECT MAX(id) FROM scan_work)")
        .execute(&h.state.db)
        .await
        .unwrap();
    let historic = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let failed_scan = historic["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "library_scan_failed")
        .unwrap();
    assert_eq!(failed_scan["count"], 0);
    assert_eq!(failed_scan["blocks_work"], false);
    sqlx::query("UPDATE immich_servers SET compatible=1,maintenance_json=?")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE watch_status SET paused=0")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE scan_schedule SET last_error=NULL")
        .execute(&h.state.db)
        .await
        .unwrap();
    // A chosen decision in progress must not ask for another decision.
    sqlx::query("UPDATE conflicts SET status='resolving'")
        .execute(&h.state.db)
        .await
        .unwrap();
    std::fs::write(h.root.join(".ism-root"), "photos").unwrap();
    let cleared = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(cleared["items"].as_array().unwrap().is_empty(), "{cleared}");
    assert_eq!(cleared["completeness"], "complete");

    sqlx::query(
        "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
         VALUES ('peer','Example','https://peer.example','unused','online',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    for (phase, code, state) in [
        (
            ism_core::maintenance::Phase::Preparing,
            "maintenance_waiting",
            "waiting",
        ),
        (
            ism_core::maintenance::Phase::Active,
            "maintenance_waiting",
            "waiting",
        ),
        (
            ism_core::maintenance::Phase::Resuming,
            "maintenance_waiting",
            "waiting",
        ),
        (
            ism_core::maintenance::Phase::Blocked,
            "maintenance_blocked",
            "blocked",
        ),
        (
            ism_core::maintenance::Phase::RecoveryRequired,
            "maintenance_blocked",
            "blocked",
        ),
    ] {
        sqlx::query("UPDATE immich_servers SET maintenance_json=?")
            .bind(
                serde_json::to_string(&ism_core::maintenance::MaintenanceState {
                    phase,
                    ..Default::default()
                })
                .unwrap(),
            )
            .execute(&h.state.db)
            .await
            .unwrap();
        let snapshot = send(&h.app, get("/api/admin/attention", &auth))
            .await
            .json();
        let item = snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == "maintenance:server")
            .unwrap();
        assert_eq!(item["code"], code);
        assert_eq!(item["state"], state);
        assert_eq!(
            snapshot["completeness"], "partial",
            "Remote coverage is unavailable"
        );
    }
    sqlx::query("UPDATE immich_servers SET maintenance_json=?")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .execute(&h.state.db)
        .await
        .unwrap();
    let cleared = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert_eq!(cleared["completeness"], "partial");
    assert!(
        !cleared["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "maintenance:server")
    );
    for code in [
        "maintenance_blocked",
        "maintenance_waiting",
        "recovery_decision",
    ] {
        let source = cleared["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .find(|source| source["code"] == code && source["owner"] == "local")
            .unwrap();
        assert_eq!(source["completeness"], "complete");
    }
    assert!(
        cleared["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .any(|source| {
                source["code"] == "maintenance_blocked"
                    && source["owner"] == "receiver"
                    && source["peer_id"] == "peer"
                    && source["completeness"] == "unavailable"
            })
    );
    sqlx::query("UPDATE immich_servers SET maintenance_json='unreadable'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let unknown = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(unknown.status, StatusCode::OK, "{}", unknown.json());
    let unknown = unknown.json();
    let item = unknown["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "maintenance:server")
        .unwrap();
    assert_eq!(item["state"], "unavailable");
    assert_eq!(item["completeness"], "unavailable");
    assert_eq!(item["blocks_work"], false);
    assert_eq!(unknown["completeness"], "partial");
    assert!(!unknown["warnings"].as_array().unwrap().is_empty());
    for code in ["maintenance_blocked", "maintenance_waiting"] {
        assert_eq!(
            unknown["coverage"]
                .as_array()
                .unwrap()
                .iter()
                .find(|source| source["code"] == code && source["owner"] == "local")
                .unwrap()["completeness"],
            "partial"
        );
    }
}

#[tokio::test]
async fn attention_keeps_timed_out_mapping_checks_unknown_and_bounded() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join(".ism-root"), "wrong-root").unwrap();
    let auth = [("cookie", h.cookie.as_str())];
    let blocked = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert_eq!(blocked["items"][0]["id"], "mapping:photos");
    assert_eq!(blocked["items"][0]["blocks_work"], true);
    assert!(blocked["items"][0]["recheck_at"].is_string());

    let permits = h
        .state
        .queue_root_probes
        .clone()
        .acquire_many_owned(crate::queue_evidence::ROOT_PROBE_LIMIT as u32)
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        send(&h.app, get("/api/admin/attention", &auth)),
    )
    .await
    .unwrap();
    assert_eq!(response.status, StatusCode::OK, "{}", response.json());
    let unknown = response.json();
    assert_eq!(unknown["items"][0]["id"], "mapping:photos");
    assert_eq!(unknown["items"][0]["state"], "unavailable");
    assert_eq!(unknown["items"][0]["blocks_work"], false);
    assert_eq!(unknown["items"][0]["completeness"], "unavailable");
    assert_eq!(unknown["completeness"], "partial");
    assert_eq!(
        unknown["coverage"]
            .as_array()
            .unwrap()
            .iter()
            .find(|source| source["code"] == "mapping_unavailable" && source["owner"] == "local")
            .unwrap()["completeness"],
        "partial"
    );
    drop(permits);
    std::fs::write(h.root.join(".ism-root"), "photos").unwrap();
    let cleared = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(cleared["items"].as_array().unwrap().is_empty());
    assert_eq!(cleared["completeness"], "complete");
}

#[tokio::test]
async fn attention_rechecks_override_expiry_allowed_hours_and_retry_eligibility() {
    let h = harness().await;
    h.map_root().await;
    sqlx::query(
        "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
         VALUES ('peer','Example','https://peer.example','unused','online',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('share','Archive','photos','','[]',1)",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
         VALUES ('transfer','peer','share','photo.jpg','synthetic',1,'{}','queued',?,?)",
    ).bind(crate::db::now()).bind(crate::db::now()).execute(&h.state.db).await.unwrap();
    crate::db::set_setting(
        &h.state.db,
        "sync_schedule",
        &json!({"timezone":"UTC","windows":[{"days":[],"start":"00:00","end":"00:00"}]})
            .to_string(),
    )
    .await
    .unwrap();
    let until = crate::db::ts(chrono::Utc::now() + chrono::Duration::minutes(5));
    sqlx::query(
        "INSERT INTO overrides(id,share_id,peer_id,until,reason,created_at)
         VALUES ('override','share','peer',?,'synthetic',?)",
    )
    .bind(&until)
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    let auth = [("cookie", h.cookie.as_str())];
    let allowed = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let item = allowed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_evidence_unavailable")
        .unwrap();
    assert_eq!(item["recheck_at"], until);
    assert!(
        !allowed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["code"] == "transfer_scheduled")
    );
    sqlx::query("UPDATE overrides SET revoked_at=?")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    let closed = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let item = closed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_scheduled")
        .unwrap();
    assert!(item["next_at"].is_null());
    assert_eq!(item["blocks_work"], true);

    let now = chrono::Utc::now();
    let start = (now - chrono::Duration::hours(1))
        .format("%H:%M")
        .to_string();
    let end = (now + chrono::Duration::hours(1))
        .format("%H:%M")
        .to_string();
    crate::db::set_setting(
        &h.state.db,
        "sync_schedule",
        &json!({"timezone":"UTC","windows":[{"days":[0,1,2,3,4,5,6],"start":start,"end":end}]})
            .to_string(),
    )
    .await
    .unwrap();
    let allowed = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let item = allowed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_evidence_unavailable")
        .unwrap();
    let transition = crate::db::parse_ts(item["recheck_at"].as_str().unwrap()).unwrap();
    assert!(transition > now && transition <= now + chrono::Duration::hours(1));
    assert_eq!(transition.format("%H:%M").to_string(), end);
    sqlx::query("UPDATE transfers SET attempts=1,updated_at=?")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    let retry = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let item = retry["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_backoff")
        .unwrap();
    assert!(item["next_at"].is_string());
    assert_eq!(item["recheck_at"], item["next_at"]);
    assert_eq!(retry["completeness"], "partial");
    assert!(retry["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("Shared-folder attention")
    }));
}

#[tokio::test]
async fn attention_requires_admin_and_distinguishes_recovery_from_historical_failure() {
    let h = harness().await;
    let uri = "/api/admin/attention";
    assert_eq!(
        send(&h.app, get(uri, &[])).await.status,
        StatusCode::UNAUTHORIZED
    );
    for (id, status, handled, resolved) in [
        (
            "recovery",
            "recovery_required",
            Some("2026-01-01T00:01:00Z"),
            0,
        ),
        ("failure", "failed", None, 0),
    ] {
        sqlx::query(
            "INSERT INTO operations
             (id,idempotency_key,principal,source,request_json,status,created_at,finished_at,handled_at,recovery_resolved)
             VALUES (?,?,'admin','admin','{}',?,'2026-01-01T00:00:00Z','2026-01-01T00:01:00Z',?,?)",
        )
        .bind(id)
        .bind(id)
        .bind(status)
        .bind(handled)
        .bind(resolved)
        .execute(&h.state.db)
        .await
        .unwrap();
    }

    let auth = [("cookie", h.cookie.as_str())];
    let reply = send(&h.app, get(uri, &auth)).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    let snapshot = reply.json();
    let items = snapshot["items"].as_array().unwrap();
    let recovery = items
        .iter()
        .find(|item| item["operation_id"] == "recovery")
        .unwrap();
    assert_eq!(recovery["code"], "operation_recovery");
    assert_eq!(recovery["attention"], "action_required");
    assert_eq!(recovery["acknowledged"], true);
    assert_eq!(recovery["unresolved"], true);
    assert_eq!(recovery["blocks_work"], true);
    assert_eq!(recovery["owner"], "local");
    let failure = items
        .iter()
        .find(|item| item["operation_id"] == "failure")
        .unwrap();
    assert_eq!(failure["code"], "operation_failed");
    assert_eq!(failure["attention"], "action_required");
    assert_eq!(failure["acknowledged"], false);
    assert_eq!(failure["blocks_work"], false);
    let failure_id = failure["id"].as_str().unwrap().to_owned();

    sqlx::query("UPDATE operations SET handled_at='2026-01-01T00:02:00Z' WHERE id='failure'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let again = send(&h.app, get(uri, &auth)).await.json();
    assert!(
        again["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["id"] != failure_id)
    );
    assert!(
        again["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == recovery["id"] && item["unresolved"] == true)
    );
}

#[tokio::test]
async fn attention_reports_queue_scope_and_invalid_schedule_without_guessing_stalls() {
    let h = harness().await;
    h.map_root().await;
    for (id, status) in [
        ("ordinary-peer", "online"),
        ("override-peer", "online"),
        ("offline-peer", "offline"),
        ("unknown-peer", "unknown"),
    ] {
        sqlx::query(
            "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
             VALUES (?,?,?,?,'unused',?,?)",
        )
        .bind(id)
        .bind(id)
        .bind(format!("https://{id}.example"))
        .bind(format!("instance-{id}"))
        .bind(status)
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('share','Archive','photos','','[]',1)",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let mut tx = h.state.db.begin().await.unwrap();
    for (peer, count) in [
        ("ordinary-peer", 15),
        ("override-peer", 4),
        ("offline-peer", 3),
        ("unknown-peer", 2),
    ] {
        for i in 0..count {
            sqlx::query(
                "INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
                 VALUES (?,?,'share',?,'example',10,'{}','queued',?,?)",
            )
            .bind(format!("{peer}-{i}"))
            .bind(peer)
            .bind(format!("photo-{i}.example.jpg"))
            .bind(crate::db::now())
            .bind(crate::db::now())
            .execute(&mut *tx)
            .await
            .unwrap();
        }
    }
    sqlx::query(
        "INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,error,queued_at,updated_at)
         VALUES ('failed-transfer','ordinary-peer','share','failed.example.jpg','example',10,'{}','failed',
                 'synthetic diagnostic detail',?,?)",
    )
    .bind(crate::db::now())
    .bind(crate::db::now())
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO overrides(id,peer_id,until,reason,created_at)
         VALUES ('override','override-peer',?,'test',?)",
    )
    .bind(crate::db::ts(
        chrono::Utc::now() + chrono::Duration::days(1),
    ))
    .bind(crate::db::now())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    crate::db::set_setting(&h.state.db, "sync_schedule", "not-json")
        .await
        .unwrap();

    let auth = [("cookie", h.cookie.as_str())];
    let response = send(
        &h.app,
        get("/api/admin/operations/background?limit=25", &auth),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.json());
    let background = response.json();
    let override_row = background["current"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_key"] == "transfer-queue:override-peer:share")
        .unwrap();
    assert_eq!(override_row["status"], "waiting");

    let response = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.json());
    let snapshot = response.json();
    let items = snapshot["items"].as_array().unwrap();
    let invalid = items
        .iter()
        .find(|item| {
            item["code"] == "transfer_schedule_invalid" && item["peer_id"] == "ordinary-peer"
        })
        .unwrap();
    assert_eq!(invalid["count"], 15);
    assert_eq!(invalid["blocks_work"], true);
    assert_eq!(invalid["attention"], "action_required");
    assert_eq!(invalid["href"], "/sync");
    assert!(!items.iter().any(|item| {
        item["code"] == "transfer_schedule_invalid" && item["peer_id"] == "override-peer"
    }));
    let offline = items
        .iter()
        .find(|item| item["code"] == "peer_offline" && item["peer_id"] == "offline-peer")
        .unwrap();
    assert_eq!(offline["count"], 3);
    assert_eq!(offline["state"], "waiting");
    assert_eq!(offline["attention"], "notice");
    assert_eq!(offline["blocks_work"], true);
    let failure = items
        .iter()
        .find(|item| item["code"] == "transfer_failed")
        .unwrap();
    assert_eq!(failure["count"], 1);
    assert_eq!(failure["blocks_work"], true);
    assert!(
        !failure["message"]
            .as_str()
            .unwrap()
            .contains("synthetic diagnostic detail")
    );
    let unknown = items
        .iter()
        .find(|item| {
            item["code"] == "transfer_evidence_unavailable" && item["peer_id"] == "override-peer"
        })
        .unwrap();
    assert_eq!(unknown["completeness"], "partial");
    assert_eq!(unknown["blocks_work"], false);
    let unknown_peer = items
        .iter()
        .find(|item| {
            item["code"] == "peer_evidence_unavailable" && item["peer_id"] == "unknown-peer"
        })
        .unwrap();
    assert_eq!(unknown_peer["blocks_work"], false);
    assert_eq!(unknown_peer["completeness"], "partial");
    assert!(
        !items
            .iter()
            .any(|item| item["code"] == "peer_offline" && item["peer_id"] == "unknown-peer")
    );
    assert!(items.iter().all(|item| item["state"] != "stalled"));

    sqlx::query("UPDATE overrides SET revoked_at=? WHERE id='override'")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(snapshot["items"].as_array().unwrap().iter().any(|item| {
        item["code"] == "transfer_schedule_invalid"
            && item["peer_id"] == "override-peer"
            && item["count"] == 4
    }));
    sqlx::query(
        "UPDATE overrides SET revoked_at=NULL,until='2000-01-01T00:00:00.000Z' WHERE id='override'",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(snapshot["items"].as_array().unwrap().iter().any(|item| {
        item["code"] == "transfer_schedule_invalid" && item["peer_id"] == "override-peer"
    }));

    for (id, updated_at) in [
        ("ordinary-peer-0", crate::db::now()),
        ("ordinary-peer-1", chrono::Utc::now().to_rfc3339()),
    ] {
        sqlx::query("UPDATE transfers SET attempts=2,updated_at=? WHERE id=?")
            .bind(updated_at)
            .bind(id)
            .execute(&h.state.db)
            .await
            .unwrap();
    }
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let backoff = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_backoff" && item["peer_id"] == "ordinary-peer")
        .unwrap();
    assert_eq!(backoff["count"], 1);
    assert_eq!(backoff["total_count"], 15);
    assert_eq!(backoff["completeness"], "partial");
    assert_eq!(backoff["blocks_work"], true);
    assert!(backoff["next_at"].is_null());

    let permits = tokio::time::timeout(
        Duration::from_secs(2),
        h.state
            .queue_root_probes
            .clone()
            .acquire_many_owned(crate::queue_evidence::ROOT_PROBE_LIMIT as u32),
    )
    .await
    .unwrap()
    .unwrap();
    let snapshot = tokio::time::timeout(
        Duration::from_secs(2),
        send(&h.app, get("/api/admin/attention", &auth)),
    )
    .await
    .unwrap()
    .json();
    let root = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "transfer-root-evidence:ordinary-peer:share")
        .unwrap();
    assert_eq!(root["completeness"], "partial");
    assert_eq!(root["blocks_work"], false);
    assert!(
        !snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "transfer-root:ordinary-peer:share")
    );
    drop(permits);

    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES ('active','active','admin','admin','{}','running',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let gate = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_dependency" && item["peer_id"] == "ordinary-peer")
        .unwrap();
    assert_eq!(gate["count"], 15);
    assert_eq!(gate["attention"], "none");
    assert_eq!(gate["blocks_work"], true);
    assert_eq!(gate["state"], "waiting");
    assert_eq!(gate["href"], "/operations");
    sqlx::query("UPDATE operations SET status='waiting_maintenance' WHERE id='active'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(snapshot["items"].as_array().unwrap().iter().any(|item| {
        item["code"] == "transfer_dependency"
            && item["peer_id"] == "ordinary-peer"
            && item["title"] == "Transfers wait for a managed operation"
    }));
    sqlx::query("UPDATE operations SET status='queued' WHERE id='active'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(!snapshot["items"].as_array().unwrap().iter().any(|item| {
        item["code"] == "transfer_dependency"
            && item["title"] == "Transfers wait for a managed operation"
    }));
}

#[tokio::test]
async fn attention_distinguishes_fresh_receiver_failures_from_stale_peer_evidence() {
    let h = harness().await;
    for (peer, updated) in [
        ("fresh-peer", crate::db::now()),
        ("stale-peer", "2000-01-01T00:00:00.000Z".into()),
        (
            "future-peer",
            crate::db::ts(chrono::Utc::now() + chrono::Duration::hours(1)),
        ),
        ("error-peer", crate::db::now()),
    ] {
        sqlx::query(
            "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
             VALUES (?,?,?,?,'unused','online',?)",
        )
        .bind(peer)
        .bind(peer)
        .bind(format!("https://{peer}.example"))
        .bind(format!("instance-{peer}"))
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
        let progress = serde_json::to_string(&ism_core::dto::SyncProgress::default()).unwrap();
        sqlx::query(
            "INSERT INTO peer_sync_state(peer_id,cursor,progress_json,updated_at)
             VALUES (?,0,?,?)",
        )
        .bind(peer)
        .bind(progress)
        .bind(updated)
        .execute(&h.state.db)
        .await
        .unwrap();
        let transfer = json!({
            "id": format!("{peer}-file"),
            "outgoing": true,
            "peer_id": peer,
            "share_name": "Archive",
            "relative_path": "photo.example.jpg",
            "location": { "root_id": "photos", "path": "photo.example.jpg" },
            "source": "ISM: Local",
            "destination": "ISM: Peer",
            "destination_path": "photo.example.jpg",
            "status": "failed",
            "size": 10,
            "bytes_done": 0,
            "error": "synthetic receiver detail",
            "queued_at": crate::db::now(),
            "finished_at": null,
            "operation_id": null
        });
        sqlx::query("INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES (?,?,?)")
            .bind(peer)
            .bind(format!("{peer}-file"))
            .bind(transfer.to_string())
            .execute(&h.state.db)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE peer_sync_state SET error='synthetic connection failure' WHERE peer_id='error-peer'")
            .execute(&h.state.db)
            .await
            .unwrap();
    sqlx::query(
            "INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES ('fresh-peer','bad-record','{}')",
    )
    .execute(&h.state.db)
    .await
    .unwrap();

    let auth = [("cookie", h.cookie.as_str())];
    let response = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.json());
    let snapshot = response.json();
    assert_eq!(snapshot["completeness"], "partial");
    let items = snapshot["items"].as_array().unwrap();
    let failure = items
        .iter()
        .find(|item| item["code"] == "receiver_transfer_failed" && item["peer_id"] == "fresh-peer")
        .unwrap();
    assert_eq!(failure["owner"], "receiver");
    assert_eq!(failure["state"], "failed");
    assert_eq!(failure["blocks_work"], true);
    assert_eq!(failure["completeness"], "partial");
    assert!(
        !failure["message"]
            .as_str()
            .unwrap()
            .contains("synthetic receiver detail")
    );
    let unknown = items
        .iter()
        .find(|item| item["code"] == "peer_evidence_unavailable" && item["peer_id"] == "stale-peer")
        .unwrap();
    assert_eq!(unknown["owner"], "receiver");
    assert_eq!(unknown["state"], "unavailable");
    assert_eq!(unknown["blocks_work"], false);
    for peer in ["future-peer", "error-peer"] {
        assert!(items.iter().any(|item| {
            item["code"] == "peer_evidence_unavailable"
                && item["peer_id"] == peer
                && item["blocks_work"] == false
        }));
        assert!(
            !items.iter().any(|item| {
                item["code"] == "receiver_transfer_failed" && item["peer_id"] == peer
            })
        );
    }
    assert!(items.iter().any(|item| {
        item["code"] == "peer_evidence_unavailable"
            && item["peer_id"] == "fresh-peer"
            && item["count"] == 1
    }));
    assert!(!items.iter().any(|item| {
        item["code"] == "receiver_transfer_failed" && item["peer_id"] == "stale-peer"
    }));
}

#[tokio::test]
async fn partially_received_peer_snapshot_is_not_reported_as_current() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    let h = harness().await;
    let second_page = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let peer_app = Router::new().route(
        "/api/peer/v1/sync-activity",
        axum::routing::get({
            let second_page = second_page.clone();
            let finish = finish.clone();
            move |axum::extract::Query(query): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| {
                let second_page = second_page.clone();
                let finish = finish.clone();
                async move {
                    if query.get("after").is_some_and(|after| after == "0") {
                        axum::Json(json!({
                            "cursor": 1,
                            "more": true,
                            "changes": [{ "id": "removed", "activity": null }],
                            "progress": ism_core::dto::SyncProgress::default()
                        }))
                    } else {
                        second_page.notify_one();
                        finish.notified().await;
                        axum::Json(json!({
                            "cursor": 1,
                            "more": false,
                            "changes": [],
                            "progress": ism_core::dto::SyncProgress::default()
                        }))
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, peer_app).await.unwrap();
    });
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         VALUES ('peer','Peer example',?,'remote','unused','online',?)",
    )
    .bind(&url)
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO peer_sync_state(peer_id,cursor,progress_json,updated_at)
         VALUES ('peer',0,?,?)",
    )
    .bind(serde_json::to_string(&ism_core::dto::SyncProgress::default()).unwrap())
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    let cached = json!({
        "id": "cached-file",
        "outgoing": true,
        "peer_id": "peer",
        "share_name": "Archive",
        "relative_path": "photo.example.jpg",
        "location": { "root_id": "photos", "path": "photo.example.jpg" },
        "source": "ISM: Local",
        "destination": "ISM: Peer example",
        "destination_path": "photo.example.jpg",
        "status": "failed",
        "size": 10,
        "bytes_done": 0,
        "error": "synthetic receiver detail",
        "queued_at": crate::db::now(),
        "finished_at": null,
        "operation_id": null
    });
    sqlx::query(
        "INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES ('peer','cached-file',?)",
    )
    .bind(cached.to_string())
    .execute(&h.state.db)
    .await
    .unwrap();
    let peer = crate::peers::net::Peer {
        id: "peer".into(),
        name: "Peer example".into(),
        url,
        instance_id: Some("remote".into()),
        secret: "synthetic-secret".into(),
        cursor: 0,
    };
    let state = h.state.clone();
    let refresh = tokio::spawn(async move { crate::peers::activity::refresh(&state, &peer).await });
    tokio::time::timeout(Duration::from_secs(3), second_page.notified())
        .await
        .unwrap();
    let auth = [("cookie", h.cookie.as_str())];
    let partial = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let items = partial["items"].as_array().unwrap();
    assert!(
        items.iter().any(|item| {
            item["code"] == "peer_evidence_unavailable" && item["peer_id"] == "peer"
        })
    );
    assert!(
        !items
            .iter()
            .any(|item| item["code"] == "receiver_transfer_failed")
    );
    finish.notify_one();
    refresh.await.unwrap().unwrap();
    let complete = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(
        complete["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["code"] == "receiver_transfer_failed")
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn malformed_peer_activity_does_not_hide_other_attention_or_background_work() {
    let h = harness().await;
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         VALUES ('peer','Peer example','https://peer.example','remote','unused','online',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES ('peer','bad-record','{}')",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES ('failure','failure','admin','admin','{}','failed',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    let auth = [("cookie", h.cookie.as_str())];
    let attention = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(attention.status, StatusCode::OK, "{}", attention.json());
    let snapshot = attention.json();
    assert_ne!(snapshot["completeness"], "complete");
    assert!(
        snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["operation_id"] == "failure")
    );
    let unknown = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "peer_evidence_unavailable")
        .unwrap();
    assert_eq!(unknown["blocks_work"], false);
    let background = send(
        &h.app,
        get("/api/admin/operations/background?limit=25", &auth),
    )
    .await;
    assert_eq!(background.status, StatusCode::OK, "{}", background.json());
    assert!(!background.json()["warnings"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn retained_peer_failures_are_grouped_and_scoped_to_pending_work() {
    let h = harness().await;
    h.map_root().await;
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         VALUES ('peer','Peer example','https://peer.example','remote','unused','online',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('share','Archive','photos','','[\"peer\"]',1)",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let mut tx = h.state.db.begin().await.unwrap();
    for seq in 1..=125 {
        let folder = seq <= 120;
        let path = if folder {
            format!("folder-{seq}")
        } else {
            format!("photo-{seq}.example.jpg")
        };
        let event = json!({
            "seq": seq,
            "origin": "remote",
            "origin_seq": seq,
            "share": "Archive",
            "kind": if folder { "folder_pruned" } else { "file_deleted" },
            "data": { "path": path, "vv": {} }
        });
        let error = if folder {
            Some(crate::peers::inbox::folder_pruned_message(
                "Peer example",
                "Archive",
                &path,
            ))
        } else if seq < 125 {
            Some(format!("Last attempt failed for {path}"))
        } else {
            None
        };
        sqlx::query(
            "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json,error,attempted_at)
             VALUES ('peer',?,'remote',?,'Archive',?,?,?)",
        )
        .bind(seq)
        .bind(seq)
        .bind(event.to_string())
        .bind(error)
        .bind(crate::db::now())
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json)
         VALUES ('peer',200,'remote',200,'Archive','not-json')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    let invalid_identity = json!({
        "seq": 201,
        "origin": "",
        "origin_seq": 201,
        "share": "Archive",
        "kind": "folder_pruned",
        "data": { "path": "unverifiable", "vv": {} }
    });
    sqlx::query(
        "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json)
         VALUES ('peer',201,'',201,'Archive',?)",
    )
    .bind(invalid_identity.to_string())
    .execute(&mut *tx)
    .await
    .unwrap();
    for path in ["inventory-1.example.jpg", "inventory-2.example.jpg"] {
        let inventory = json!({
            "path": path,
            "sha256": null,
            "size": 0,
            "mtime_ns": 0,
            "deleted": true,
            "vv": {}
        });
        sqlx::query(
            "INSERT INTO peer_backfill_inbox(peer_id,share_id,path,entry_json,error)
             VALUES ('peer','share',?,?,?)",
        )
        .bind(path)
        .bind(inventory.to_string())
        .bind(format!("Could not apply {path}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();

    let auth = [("cookie", h.cookie.as_str())];
    let attention = send(&h.app, get("/api/admin/attention", &auth)).await;
    assert_eq!(attention.status, StatusCode::OK, "{}", attention.json());
    let snapshot = attention.json();
    let items = snapshot["items"].as_array().unwrap();
    let folder = items
        .iter()
        .find(|item| item["code"] == "folder_removal_failed")
        .unwrap();
    assert_eq!(folder["count"], 120);
    assert_eq!(folder["peer_id"], "peer");
    assert_eq!(folder["share_id"], "share");
    assert_eq!(folder["blocks_work"], true);
    assert!(
        folder["message"]
            .as_str()
            .unwrap()
            .contains("Other folders")
    );
    let event = items
        .iter()
        .find(|item| item["code"] == "peer_inbox_failed")
        .unwrap();
    assert_eq!(event["count"], 4);
    assert_eq!(event["blocks_work"], false);
    assert_eq!(event["completeness"], "partial");
    let inventory = items
        .iter()
        .find(|item| item["code"] == "peer_inventory_failed")
        .unwrap();
    assert_eq!(inventory["count"], 2);
    assert_eq!(inventory["blocks_work"], false);
    assert_eq!(inventory["completeness"], "partial");
    let unknown = items
        .iter()
        .find(|item| {
            item["code"] == "peer_evidence_unavailable"
                && item["title"] == "Saved peer work could not be verified"
        })
        .unwrap();
    assert_eq!(unknown["count"], 2);
    assert_eq!(unknown["blocks_work"], false);

    let background = send(
        &h.app,
        get("/api/admin/operations/background?limit=25", &auth),
    )
    .await;
    assert_eq!(background.status, StatusCode::OK, "{}", background.json());
    let snapshot = background.json();
    let current = snapshot["current"].as_array().unwrap();
    let folder = current
        .iter()
        .find(|item| item["task_key"] == folder["id"])
        .unwrap();
    assert_eq!(folder["status"], "blocked");
    assert_eq!(folder["count"], 120);
    assert_eq!(folder["reasons"][0]["count"], 120);
    assert_eq!(folder["examples"].as_array().unwrap().len(), 5);
    let generic_event = current
        .iter()
        .find(|item| item["task_key"] == event["id"])
        .unwrap();
    assert_eq!(generic_event["status"], "failed");
    assert_eq!(
        current
            .iter()
            .filter(|item| item["title"] == "Peer event application")
            .count(),
        4
    );
    assert!(!snapshot["warnings"].as_array().unwrap().is_empty());

    sqlx::query("UPDATE peer_inbox SET status='done'")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("DELETE FROM peer_backfill_inbox")
        .execute(&h.state.db)
        .await
        .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(snapshot["items"].as_array().unwrap().iter().all(|item| {
        !matches!(
            item["code"].as_str(),
            Some("folder_removal_failed" | "peer_inbox_failed" | "peer_inventory_failed")
        )
    }));
}

#[tokio::test]
async fn unsaved_transfer_outcome_remains_visible_until_the_running_row_changes() {
    let h = harness().await;
    h.map_root().await;
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         VALUES ('peer','Peer example','https://peer.example','remote','invalid','online',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('share','Archive','photos','','[\"peer\"]',1)",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
         VALUES ('transfer','peer','share','photo.example.jpg','example',10,'{}','queued',?,?)",
    )
    .bind(crate::db::now())
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_transfer_outcome BEFORE UPDATE OF status ON transfers
         WHEN OLD.status='running'
         BEGIN SELECT RAISE(FAIL, 'synthetic outcome write failure'); END",
    )
    .execute(&h.state.db)
    .await
    .unwrap();

    let worker = tokio::spawn(crate::peers::transfer::run(h.state.clone()));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if h.state
                .worker_evidence
                .transfer_failures()
                .contains_key("transfer")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the dispatcher must attempt and fail to save the outcome");
    let status: String = sqlx::query_scalar("SELECT status FROM transfers WHERE id='transfer'")
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert_eq!(status, "running");

    let auth = [("cookie", h.cookie.as_str())];
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    let failure = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "transfer-worker:peer:share:outcome-not-saved")
        .unwrap();
    assert_eq!(failure["code"], "worker_stopped");
    assert_eq!(failure["blocks_work"], true);
    assert_eq!(failure["count"], 1);
    let background = send(
        &h.app,
        get("/api/admin/operations/background?limit=25", &auth),
    )
    .await
    .json();
    let transfer = background["current"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_key"] == "transfer:transfer" && item["status"] == "blocked")
        .unwrap();
    assert!(
        transfer["phase"]
            .as_str()
            .unwrap()
            .contains("outcome could not be saved")
    );

    worker.abort();
    let _ = worker.await;
    sqlx::query("DROP TRIGGER reject_transfer_outcome")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE transfers SET status='failed' WHERE id='transfer'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let snapshot = send(&h.app, get("/api/admin/attention", &auth))
        .await
        .json();
    assert!(
        snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["id"] != "transfer-worker:peer:share:outcome-not-saved")
    );
    assert!(
        !h.state
            .worker_evidence
            .transfer_failures()
            .contains_key("transfer")
    );
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
}

async fn send(app: &Router, req: Request<Body>) -> Reply {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    Reply {
        status,
        headers,
        body,
    }
}

fn json_req(method: &str, uri: &str, auth: &[(&str, &str)], body: &Value) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    for (k, v) in auth {
        b = b.header(*k, *v);
    }
    b.body(Body::from(body.to_string())).unwrap()
}

fn get(uri: &str, auth: &[(&str, &str)]) -> Request<Body> {
    let mut b = Request::builder().uri(uri);
    for (k, v) in auth {
        b = b.header(*k, *v);
    }
    b.body(Body::empty()).unwrap()
}

async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::for_tests(dir.path());
    let pool = crate::db::connect(dir.path()).await.unwrap();
    let secrets = SecretBox::open_or_create(dir.path()).unwrap();
    let state = AppState::new(config, pool, secrets).await.unwrap();
    let app = super::router(state.clone());
    let root = dir.path().join("photos");
    std::fs::create_dir_all(root.join("2024")).unwrap();

    let setup = json!({ "username": "admin", "password": "correct horse battery", "instance_name": "Test" });
    let r = send(&app, json_req("POST", "/api/admin/setup", &[], &setup)).await;
    assert_eq!(
        r.status,
        StatusCode::FORBIDDEN,
        "setup needs the CSRF header"
    );
    let r = send(
        &app,
        json_req(
            "POST",
            "/api/admin/setup",
            &[("x-ism-request", "1")],
            &setup,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT, "{:?}", r.json());
    let cookie = r.headers[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let again = send(
        &app,
        json_req(
            "POST",
            "/api/admin/setup",
            &[("x-ism-request", "1")],
            &setup,
        ),
    )
    .await;
    assert_eq!(again.status, StatusCode::CONFLICT);
    Harness {
        _dir: dir,
        root,
        app,
        state,
        cookie,
    }
}

impl Harness {
    fn admin(&self) -> Vec<(&str, &str)> {
        vec![("cookie", self.cookie.as_str()), ("x-ism-request", "1")]
    }
}

#[tokio::test]
async fn repeated_client_requests_do_not_write_token_usage_each_time() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read"]).await;
    sqlx::query("CREATE TABLE token_usage_writes (id INTEGER PRIMARY KEY)")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER count_token_usage AFTER UPDATE OF last_used_at ON client_tokens
         BEGIN INSERT INTO token_usage_writes VALUES (NULL); END",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    for _ in 0..25 {
        let response = send(
            &h.app,
            get("/api/v1/client/info", &[("authorization", &bearer)]),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK);
    }
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM token_usage_writes")
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert_eq!(writes, 1, "recent requests share one usage update");

    sqlx::query("UPDATE client_tokens SET last_used_at = '2024-01-01T00:00:00Z'")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("DELETE FROM token_usage_writes")
        .execute(&h.state.db)
        .await
        .unwrap();
    let responses = futures_util::future::join_all((0..12).map(|_| {
        send(
            &h.app,
            get("/api/v1/client/info", &[("authorization", &bearer)]),
        )
    }))
    .await;
    assert!(
        responses
            .iter()
            .all(|response| response.status == StatusCode::OK)
    );
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM token_usage_writes")
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert_eq!(
        writes, 1,
        "concurrent requests refresh an old timestamp once"
    );
    sqlx::query("UPDATE client_tokens SET revoked_at = ?")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    let response = send(
        &h.app,
        get("/api/v1/client/info", &[("authorization", &bearer)]),
    )
    .await;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn activity_websocket_requires_a_session_and_same_origin_and_streams_changes() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let h = harness().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = h.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("ws://{address}/api/admin/live");
    let unauthenticated = tokio_tungstenite::connect_async(&url).await;
    assert!(
        matches!(unauthenticated, Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status() == StatusCode::UNAUTHORIZED)
    );
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert(header::COOKIE, h.cookie.parse().unwrap());
    request
        .headers_mut()
        .insert(header::ORIGIN, "https://unrelated.example".parse().unwrap());
    let cross_origin = tokio_tungstenite::connect_async(request).await;
    assert!(
        matches!(cross_origin, Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status() == StatusCode::FORBIDDEN)
    );
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert(header::COOKIE, h.cookie.parse().unwrap());
    request
        .headers_mut()
        .insert(header::ORIGIN, format!("http://{address}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    h.state.emit(crate::state::ServerEvent::SyncChanged);
    let event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = socket.next().await.unwrap().unwrap();
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                break serde_json::from_str::<Value>(&text).unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(event["type"], "sync_changed");
    socket.close(None).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn activity_pages_are_bounded_stable_and_handling_does_not_resolve_recovery() {
    let h = harness().await;
    h.map_root().await;
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 525)
         INSERT INTO operations (id, idempotency_key, principal, source, request_json, status, created_at)
         SELECT printf('op-%05d', i), printf('key-%d',i), 'admin:admin', 'admin', '{}',
                CASE WHEN i = 525 THEN 'recovery_required' ELSE 'completed' END, '2026-01-01T00:00:00Z' FROM n",
    ).execute(&h.state.db).await.unwrap();
    sqlx::query(
        r#"INSERT INTO operation_items (operation_id, idx, kind, action_json, status)
           SELECT id, 0, 'move', '{"kind":"move","source":{"root_id":"photos","path":"a.jpg"},"target":{"root_id":"photos","path":"2024/a.jpg"}}', 'done' FROM operations"#,
    ).execute(&h.state.db).await.unwrap();
    let page = send(&h.app, get("/api/admin/operations/page", &h.admin())).await;
    assert_eq!(page.status, StatusCode::OK, "{:?}", page.json());
    assert_eq!(page.json()["total"], 525);
    assert_eq!(page.json()["rows"].as_array().unwrap().len(), 250);
    assert_eq!(page.json()["rows"][0]["operation_id"], "op-00525");
    assert_eq!(page.json()["rows"][0]["title"], "Move");
    assert_eq!(page.json()["rows"][0]["source"], "ISM: Test");
    assert_eq!(page.json()["rows"][0]["destination"], "ISM: Test");
    let page = send(
        &h.app,
        get(
            "/api/admin/operations/page?limit=5000&offset=250",
            &h.admin(),
        ),
    )
    .await;
    assert_eq!(page.json()["rows"].as_array().unwrap().len(), 275);
    assert_eq!(page.json()["rows"][0]["operation_id"], "op-00275");
    let invalid = send(
        &h.app,
        get("/api/admin/operations/page?limit=5001", &h.admin()),
    )
    .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
    let recent = send(
        &h.app,
        get(
            "/api/admin/operations/page?status=recent&limit=10",
            &h.admin(),
        ),
    )
    .await;
    assert_eq!(recent.json()["rows"].as_array().unwrap().len(), 10);
    let before = send(&h.app, get("/api/admin/overview", &h.admin())).await;
    assert_eq!(before.json()["operations_attention"], 1);
    let response = send(
        &h.app,
        json_req(
            "POST",
            "/api/admin/operations/op-00525/handled",
            &h.admin(),
            &json!({}),
        ),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::NO_CONTENT,
        "{:?}",
        response.json()
    );
    let (status, resolved, handled): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT status, recovery_resolved, handled_at FROM operations WHERE id = 'op-00525'",
    )
    .fetch_one(&h.state.db)
    .await
    .unwrap();
    assert_eq!(status, "recovery_required");
    assert_eq!(resolved, 0);
    assert!(handled.is_some());
    let attention = send(
        &h.app,
        get("/api/admin/operations/page?status=attention", &h.admin()),
    )
    .await;
    assert_eq!(attention.json()["total"], 0);
    let overview = send(&h.app, get("/api/admin/overview", &h.admin())).await;
    assert_eq!(overview.json()["operations_attention"], 0);
    let response = send(
        &h.app,
        json_req(
            "POST",
            "/api/admin/operations/op-00524/handled",
            &h.admin(),
            &json!({}),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn activity_details_include_original_sidecars_and_live_step_states() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.jpg"), b"original").unwrap();
    std::fs::write(h.root.join("2024/a.jpg.xmp"), b"metadata").unwrap();
    let accepted = send(&h.app, json_req("POST", "/api/v1/operations", &h.admin(), &json!({
        "idempotency_key": "live-step-details",
        "actions": [{"kind":"move","source":{"root_id":"photos","path":"2024/a.jpg"},"target":{"root_id":"photos","path":"2024/b.jpg"}}]
    }))).await;
    assert_eq!(
        accepted.status,
        StatusCode::ACCEPTED,
        "{:?}",
        accepted.json()
    );
    let id = accepted.json()["operation_id"].as_str().unwrap().to_owned();
    crate::ops::exec::run_operation(&h.state, &id)
        .await
        .unwrap();
    let detail = send(
        &h.app,
        get(&format!("/api/admin/operations/{id}/detail"), &h.admin()),
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK, "{:?}", detail.json());
    assert_eq!(detail.json()["operation"]["status"], "completed");
    let body = detail.json();
    let files = body["tasks"][0]["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .any(|file| file["from"]["path"] == "2024/a.jpg.xmp"
                && file["to"]["path"] == "2024/b.jpg.xmp")
    );
    let steps = body["tasks"][0]["steps"].as_array().unwrap();
    assert!(steps.len() >= 3);
    assert!(steps.iter().all(|step| step["status"] == "done"));
    sqlx::query("UPDATE operation_items SET status='running', progress_json = ? WHERE operation_id = ?")
        .bind(json!({"files": [], "steps": [{"label":"Move original", "status":"done"},{"label":"Update Immich","status":"running"},{"label":"Record changes","status":"pending"}]}).to_string())
        .bind(&id).execute(&h.state.db).await.unwrap();
    let detail = send(
        &h.app,
        get(&format!("/api/admin/operations/{id}/detail"), &h.admin()),
    )
    .await;
    assert_eq!(detail.json()["tasks"][0]["steps"][1]["status"], "running");
    assert_eq!(detail.json()["tasks"][0]["steps"][2]["status"], "pending");
}

#[tokio::test]
async fn sync_activity_keeps_running_first_and_pages_completed_publications_separately() {
    let h = harness().await;
    h.map_root().await;
    sqlx::query("INSERT INTO peers (id,name,url,secret_enc,status,created_at) VALUES ('peer1','Remote','https://peer.example','unused','online',?)")
        .bind(crate::db::now()).execute(&h.state.db).await.unwrap();
    sqlx::query("INSERT INTO shares (id,name,root_id,path,peer_ids_json,enabled) VALUES ('share1','Photos','photos','2024','[\"peer1\"]',1)")
        .execute(&h.state.db).await.unwrap();
    for (id, status) in [
        ("a-queued", "queued"),
        ("b-running", "running"),
        ("c-queued", "queued"),
        ("d-failed", "failed"),
    ] {
        sqlx::query(
            "INSERT INTO transfers (id,peer_id,share_id,rel_path,sha256,size,vv_json,status,bytes_done,queued_at,updated_at)
             VALUES (?, 'peer1','share1',?,'hash',100,'{}',?,50,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        ).bind(id).bind(format!("{id}.jpg")).bind(status).execute(&h.state.db).await.unwrap();
    }
    sqlx::query("INSERT INTO operations (id,idempotency_key,principal,source,request_json,status,created_at) VALUES ('publish1','publish-key','peer:peer1','peer','{}','running',?)")
        .bind(crate::db::now()).execute(&h.state.db).await.unwrap();
    for (id, operation) in [("history1", None), ("history2", Some("publish1"))] {
        sqlx::query(
            "INSERT INTO transfer_history (id,peer_id,share_id,rel_path,sha256,size,queued_at,finished_at,operation_id)
             VALUES (?,'peer1','share1',?,'hash',100,'2026-01-01T00:00:00Z','2026-01-02T00:00:00Z',?)",
        ).bind(id).bind(format!("{id}.jpg")).bind(operation).execute(&h.state.db).await.unwrap();
    }
    let response = send(&h.app, get("/api/admin/sync/page?limit=2", &h.admin())).await;
    assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
    assert_eq!(response.json()["total"], 5);
    assert_eq!(response.json()["rows"][0]["id"], "b-running");
    assert_eq!(response.json()["rows"][1]["id"], "history2");
    assert!(response.json()["rows"][1]["finished_at"].is_null());
    assert_eq!(response.json()["rows"][0]["source"], "ISM: Remote");
    assert_eq!(response.json()["rows"][0]["destination"], "ISM: Test");
    let response = send(
        &h.app,
        get("/api/admin/sync/page?limit=2&offset=2", &h.admin()),
    )
    .await;
    assert_eq!(response.json()["rows"][0]["id"], "a-queued");
    assert_eq!(response.json()["rows"][1]["id"], "c-queued");
    let response = send(
        &h.app,
        get("/api/admin/sync/page?status=completed", &h.admin()),
    )
    .await;
    assert_eq!(response.json()["total"], 1);
    sqlx::query("UPDATE operations SET status='completed',finished_at='2026-01-03T00:00:00Z' WHERE id='publish1'")
        .execute(&h.state.db).await.unwrap();
    let response = send(
        &h.app,
        get("/api/admin/sync/page?status=completed", &h.admin()),
    )
    .await;
    assert_eq!(response.json()["total"], 2);
    assert_eq!(response.json()["rows"][0]["id"], "history2");
    let response = send(&h.app, get("/api/admin/sync/page?id=history2", &h.admin())).await;
    assert_eq!(response.json()["rows"][0]["status"], "completed");
    let response = send(
        &h.app,
        get("/api/admin/sync/files?share=Photos&path=", &h.admin()),
    )
    .await;
    assert_eq!(response.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn mounted_folders_browse_deep_paths_one_level_at_a_time() {
    let h = harness().await;
    let photos = h.state.config.mounts_dir.join("homes/example-user/Photos");
    std::fs::create_dir_all(photos.join("2024/Trip")).unwrap();
    std::fs::create_dir_all(photos.join("2025")).unwrap();
    std::fs::create_dir_all(photos.join(".ism-trash")).unwrap();
    std::fs::write(photos.join("photo.jpg"), b"image").unwrap();
    let mapping = json!({
        "root_id": "archive",
        "name": "Archive",
        "container_path": photos.to_str().unwrap(),
    });
    let response = send(
        &h.app,
        json_req("POST", "/api/admin/mappings", &h.admin(), &mapping),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::CREATED,
        "{:?}",
        response.json()
    );

    let response = send(&h.app, get("/api/admin/mounts", &h.admin())).await;
    assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
    let body = response.json();
    let mounts = body.as_array().unwrap();
    assert_eq!(mounts.len(), 2);
    assert!(mounts[0]["path"].as_str().unwrap().ends_with("/homes"));
    assert!(mounts[1]["path"].as_str().unwrap().ends_with("/photos"));

    for (parent, children) in [
        ("homes", vec!["example-user"]),
        ("homes/example-user", vec!["Photos"]),
        ("homes/example-user/Photos", vec!["2024", "2025"]),
        ("homes/example-user/Photos/2024", vec!["Trip"]),
        ("homes/example-user/Photos/2024/Trip", vec![]),
    ] {
        let parent = h.state.config.mounts_dir.join(parent);
        let mut url = reqwest::Url::parse("http://ism.example/api/admin/mounts").unwrap();
        url.query_pairs_mut()
            .append_pair("path", parent.to_str().unwrap());
        let response = send(
            &h.app,
            get(
                &format!("{}?{}", url.path(), url.query().unwrap()),
                &h.admin(),
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
        let body = response.json();
        let mounts = body.as_array().unwrap();
        let expected: Vec<_> = children
            .iter()
            .map(|child| parent.join(child).to_str().unwrap().to_owned())
            .collect();
        let actual: Vec<_> = mounts
            .iter()
            .map(|mount| mount["path"].as_str().unwrap())
            .collect();
        assert_eq!(actual, expected, "listing {}", parent.display());
        if parent.ends_with("example-user") {
            assert_eq!(mounts[0]["mapped_root"], "archive");
        }
    }
}

#[tokio::test]
async fn mounted_folders_do_not_truncate_large_directories() {
    let h = harness().await;
    for index in 0..205 {
        std::fs::create_dir(h.root.join(format!("album-{index:03}"))).unwrap();
    }
    let mut url = reqwest::Url::parse("http://ism.example/api/admin/mounts").unwrap();
    url.query_pairs_mut()
        .append_pair("path", h.root.to_str().unwrap());
    let response = send(
        &h.app,
        get(
            &format!("{}?{}", url.path(), url.query().unwrap()),
            &h.admin(),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
    let body = response.json();
    // The existing 2024 folder is listed along with every new album.
    assert_eq!(body.as_array().unwrap().len(), 206);
    assert!(
        body.as_array().unwrap().last().unwrap()["path"]
            .as_str()
            .unwrap()
            .ends_with("/album-204")
    );
}

#[tokio::test]
async fn mounted_folders_reject_escape_paths_and_report_unavailable_folders() {
    let h = harness().await;
    let outside = tempfile::tempdir().unwrap();
    let traversal = h.state.config.mounts_dir.join("../");
    let missing = h.root.join("missing");
    let mut cases = vec![
        (outside.path(), StatusCode::FORBIDDEN),
        (traversal.as_path(), StatusCode::BAD_REQUEST),
        (std::path::Path::new("photos"), StatusCode::BAD_REQUEST),
        (missing.as_path(), StatusCode::SERVICE_UNAVAILABLE),
    ];
    #[cfg(unix)]
    let escape_link = h.root.join("escape");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(outside.path(), &escape_link).unwrap();
        std::os::unix::fs::symlink(&h.root, h.root.join("cycle")).unwrap();
        cases.push((escape_link.as_path(), StatusCode::FORBIDDEN));
    }
    for (path, status) in cases {
        let mut url = reqwest::Url::parse("http://ism.example/api/admin/mounts").unwrap();
        url.query_pairs_mut()
            .append_pair("path", path.to_str().unwrap());
        let response = send(
            &h.app,
            get(
                &format!("{}?{}", url.path(), url.query().unwrap()),
                &h.admin(),
            ),
        )
        .await;
        assert_eq!(response.status, status, "{:?}", response.json());
        assert!(response.json()["error"]["message"].is_string());
    }
    let mut url = reqwest::Url::parse("http://ism.example/api/admin/mounts").unwrap();
    url.query_pairs_mut()
        .append_pair("path", h.root.to_str().unwrap());
    let uri = format!("{}?{}", url.path(), url.query().unwrap());
    let response = send(&h.app, get(&uri, &h.admin())).await;
    assert_eq!(response.status, StatusCode::OK);
    let body = response.json();
    let mounts = body.as_array().unwrap();
    assert_eq!(mounts.len(), 1);
    assert!(mounts[0]["path"].as_str().unwrap().ends_with("/2024"));
    let response = send(&h.app, get(&uri, &[])).await;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cancellation_by_key_blocks_late_submission_and_is_principal_scoped() {
    let h = harness().await;
    h.map_root().await;
    let first = h.token(&["manage"]).await;
    let second = h.token(&["manage"]).await;
    let request = json!({
        "idempotency_key": "late-request",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    let cancel = json!({ "idempotency_key": "late-request", "expected_request": request });
    let response = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations/cancel-by-key",
            &[("authorization", &first)],
            &cancel,
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
    assert_eq!(response.json()["status"], "cancelled_before_submission");
    let reopened_pool = crate::db::connect(h._dir.path()).await.unwrap();
    let reopened_state = AppState::new(
        h.state.config.clone(),
        reopened_pool,
        SecretBox::open_or_create(h._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let reopened_app = super::router(reopened_state);
    let late = send(
        &reopened_app,
        json_req(
            "POST",
            "/api/v1/operations",
            &[("authorization", &first)],
            &request,
        ),
    )
    .await;
    assert_eq!(late.status, StatusCode::CONFLICT);
    assert_eq!(late.json()["error"]["code"], "operation_key_cancelled");
    let other = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations",
            &[("authorization", &second)],
            &request,
        ),
    )
    .await;
    assert_eq!(other.status, StatusCode::ACCEPTED, "{:?}", other.json());
}

#[tokio::test]
async fn instance_scoped_operations_refuse_a_different_instance() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["manage"]).await;
    let request = json!({
        "idempotency_key": "instance-check",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    for (url, body) in [
        ("/api/v1/instances/other/operations", request.clone()),
        (
            "/api/v1/instances/other/operations/cancel-by-key",
            json!({"idempotency_key": "instance-check", "expected_request": request}),
        ),
    ] {
        let response = send(
            &h.app,
            json_req("POST", url, &[("authorization", &bearer)], &body),
        )
        .await;
        assert_eq!(response.status, StatusCode::CONFLICT);
        assert_eq!(response.json()["error"]["code"], "instance_mismatch");
    }
    let response = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/instances/{}/operations", h.state.instance_id),
            &[("authorization", &bearer)],
            &request,
        ),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::ACCEPTED,
        "{:?}",
        response.json()
    );
}

#[tokio::test]
async fn attention_reports_only_sustained_gate_waits_for_queued_operations() {
    let h = harness().await;
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES ('gate-op','gate-key','admin:test','admin','{}','queued',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    let scan = h
        .state
        .replication_operation_lock
        .lock_share_scan("share-example")
        .await;
    let wait = h.state.replication_operation_lock.lock_operation("gate-op");
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), wait.as_mut())
            .await
            .is_err()
    );
    let snapshot = crate::attention::snapshot(&h.state).await.unwrap();
    let item = snapshot
        .items
        .iter()
        .find(|item| item.operation_id.as_deref() == Some("gate-op"))
        .unwrap();
    assert_eq!(item.code, ism_core::dto::AttentionCode::OperationGateWait);
    assert_eq!(item.attention, ism_core::dto::AttentionLevel::None);
    assert!(!item.blocks_work);
    assert!(item.recheck_at.is_some());
    h.state
        .replication_operation_lock
        .age_wait_for_test("gate-op", Duration::from_secs(31));
    let snapshot = crate::attention::snapshot(&h.state).await.unwrap();
    let item = snapshot
        .items
        .iter()
        .find(|item| item.operation_id.as_deref() == Some("gate-op"))
        .unwrap();
    assert_eq!(item.attention, ism_core::dto::AttentionLevel::Notice);
    assert!(item.blocks_work);
    assert_eq!(item.message, "Waiting for a share scan.");
    assert_eq!(item.href, "/operations");
    assert!(item.recheck_at.is_none());
    sqlx::query("UPDATE operations SET status='cancelled' WHERE id='gate-op'")
        .execute(&h.state.db)
        .await
        .unwrap();
    assert!(
        !crate::attention::snapshot(&h.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.code == ism_core::dto::AttentionCode::OperationGateWait)
    );
    drop(scan);
    drop(wait.await);
    assert!(h.state.replication_operation_lock.waiting().is_empty());
}

#[tokio::test]
async fn attention_wait_messages_follow_the_evidenced_holder() {
    let h = harness().await;
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES ('gate-op','gate-key','admin:test','admin','{}','queued',?)",
    )
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();
    let holder = h.state.replication_operation_lock.lock().await;
    let wait = h.state.replication_operation_lock.lock_operation("gate-op");
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), wait.as_mut())
            .await
            .is_err()
    );
    h.state
        .replication_operation_lock
        .age_wait_for_test("gate-op", Duration::from_secs(31));
    let snapshot = crate::attention::snapshot(&h.state).await.unwrap();
    let item = snapshot
        .items
        .iter()
        .find(|item| item.code == ism_core::dto::AttentionCode::OperationGateWait)
        .unwrap();
    assert_eq!(
        item.message,
        "Waiting for other work; the cause is unknown."
    );
    let registered = h
        .state
        .replication_operation_lock
        .register(&holder, "holding-operation");
    let snapshot = crate::attention::snapshot(&h.state).await.unwrap();
    let item = snapshot
        .items
        .iter()
        .find(|item| item.code == ism_core::dto::AttentionCode::OperationGateWait)
        .unwrap();
    assert_eq!(item.message, "Waiting for another operation.");
    drop(registered);
    let snapshot = crate::attention::snapshot(&h.state).await.unwrap();
    let item = snapshot
        .items
        .iter()
        .find(|item| item.code == ism_core::dto::AttentionCode::OperationGateWait)
        .unwrap();
    assert_eq!(
        item.message,
        "Waiting for other work; the cause is unknown."
    );
    drop(holder);
    drop(wait.await);
    assert!(
        !crate::attention::snapshot(&h.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.code == ism_core::dto::AttentionCode::OperationGateWait)
    );
}

#[tokio::test]
async fn cancellation_by_key_prevents_queued_operation_from_executing() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["manage"]).await;
    let request = json!({
        "idempotency_key": "cancel-queued",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    let accepted = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations",
            &[("authorization", &bearer)],
            &request,
        ),
    )
    .await;
    let id = accepted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_string();
    let gate = h.state.replication_operation_lock.lock().await;
    let state = h.state.clone();
    let worker_id = id.clone();
    let worker =
        tokio::spawn(async move { crate::ops::exec::run_operation(&state, &worker_id).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.state.replication_operation_lock.waiting().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.state
        .replication_operation_lock
        .age_wait_for_test(&id, Duration::from_secs(31));
    assert!(
        crate::attention::snapshot(&h.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.operation_id.as_deref() == Some(id.as_str())
                && item.code == ism_core::dto::AttentionCode::OperationGateWait
                && item.blocks_work)
    );
    let wrong = json!({
        "idempotency_key": "cancel-queued",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/other" } }]
    });
    let rejected = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations/cancel-by-key",
            &[("authorization", &bearer)],
            &json!({ "idempotency_key": "cancel-queued", "expected_request": wrong }),
        ),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::CONFLICT);
    assert_eq!(rejected.json()["error"]["code"], "idempotency_key_conflict");
    assert_eq!(
        crate::ops::load(&h.state, &id).await.unwrap().0.status,
        ism_core::dto::OperationStatus::Queued
    );
    let response = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations/cancel-by-key",
            &[("authorization", &bearer)],
            &json!({ "idempotency_key": "cancel-queued", "expected_request": request }),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
    assert_eq!(response.json()["operation"]["status"], "cancelled");
    assert!(
        !crate::attention::snapshot(&h.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.operation_id.as_deref() == Some(id.as_str())
                && item.code == ism_core::dto::AttentionCode::OperationGateWait)
    );
    assert_eq!(
        response.json()["operation"]["items"][0]["status"],
        "skipped"
    );
    drop(gate);
    worker.await.unwrap().unwrap();
    assert!(!h.root.join("2024/new").exists());
    assert_eq!(
        crate::ops::load(&h.state, &id).await.unwrap().0.status,
        ism_core::dto::OperationStatus::Cancelled
    );
}

#[tokio::test]
async fn cancellation_rolls_back_if_skipping_queued_items_fails() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["manage"]).await;
    let auth = [("authorization", bearer.as_str())];
    let request = json!({
        "idempotency_key": "cancel-rollback",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    let accepted = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &request),
    )
    .await;
    assert_eq!(accepted.status, StatusCode::ACCEPTED);
    let id = accepted.json()["operation_id"].as_str().unwrap().to_owned();
    sqlx::query(
        "CREATE TRIGGER fail_cancel_skip BEFORE UPDATE OF status ON operation_items
         WHEN NEW.status = 'skipped' BEGIN SELECT RAISE(FAIL, 'test failure'); END",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let response = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/operations/{id}/cancel"),
            &auth,
            &json!({}),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);
    let state: (String, i64, String) = sqlx::query_as(
        "SELECT o.status, o.cancel_requested, i.status FROM operations o
         JOIN operation_items i ON i.operation_id = o.id WHERE o.id = ?",
    )
    .bind(id)
    .fetch_one(&h.state.db)
    .await
    .unwrap();
    assert_eq!(state, ("queued".into(), 0, "pending".into()));
}

#[tokio::test]
async fn cancellation_by_key_does_not_claim_a_running_operation_was_cancelled() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["manage"]).await;
    let request = json!({
        "idempotency_key": "active",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    let accepted = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations",
            &[("authorization", &bearer)],
            &request,
        ),
    )
    .await;
    let id = accepted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_string();
    sqlx::query("UPDATE operations SET status = 'running' WHERE id = ?")
        .bind(&id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let response = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations/cancel-by-key",
            &[("authorization", &bearer)],
            &json!({ "idempotency_key": "active", "expected_request": request }),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.json()["operation"]["status"], "running");
    let flag: i64 = sqlx::query_scalar("SELECT cancel_requested FROM operations WHERE id = ?")
        .bind(&id)
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert_eq!(flag, 0);
}

impl Harness {
    async fn map_root(&self) {
        let body = json!({ "root_id": "photos", "name": "Photos", "container_path": self.root.to_string_lossy() });
        let r = send(
            &self.app,
            json_req("POST", "/api/admin/mappings", &self.admin(), &body),
        )
        .await;
        assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
        assert_eq!(
            std::fs::read_to_string(self.root.join(".ism-root"))
                .unwrap()
                .trim(),
            "photos"
        );
    }

    async fn token(&self, actions: &[&str]) -> String {
        self.token_for("opm", actions).await
    }

    async fn token_for(&self, kind: &str, actions: &[&str]) -> String {
        let body =
            json!({ "name": "client", "kind": kind, "roots": ["photos"], "actions": actions });
        let r = send(
            &self.app,
            json_req("POST", "/api/admin/tokens", &self.admin(), &body),
        )
        .await;
        assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
        format!("Bearer {}", r.json()["secret"].as_str().unwrap())
    }

    async fn wait_finished(&self, bearer: &str, op: &str) -> Value {
        let tick = tokio::spawn(crate::ops::exec::run(self.state.clone()));
        let mut last = Value::Null;
        for _ in 0..200 {
            let r = send(
                &self.app,
                get(
                    &format!("/api/v1/operations/{op}"),
                    &[("authorization", bearer)],
                ),
            )
            .await;
            last = r.json();
            let status = last["status"].as_str().unwrap_or_default();
            if !matches!(status, "queued" | "running" | "waiting_maintenance") {
                tick.abort();
                return last;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        tick.abort();
        panic!("operation did not finish: {last}");
    }
}

#[tokio::test]
async fn queued_opm_operations_run_before_sync_and_keep_arrival_order() {
    use crate::auth::Principal;
    use ism_core::dto::OperationRequest;

    let h = harness().await;
    h.map_root().await;
    let opm = h.token(&["manage"]).await;
    let second_opm = h.token(&["manage"]).await;
    let ila = h.token_for("ila", &["manage"]).await;
    let mut ids = Vec::new();
    for (name, bearer) in [
        ("peer-first", None),
        ("ila-first", Some(&ila)),
        ("opm-first", Some(&opm)),
        ("peer-second", None),
        ("opm-second", Some(&second_opm)),
    ] {
        let request = json!({
            "idempotency_key": name,
            "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": name } }]
        });
        let id = if let Some(bearer) = bearer {
            let response = send(
                &h.app,
                json_req(
                    "POST",
                    "/api/v1/operations",
                    &[("authorization", bearer)],
                    &request,
                ),
            )
            .await;
            assert_eq!(
                response.status,
                StatusCode::ACCEPTED,
                "{:?}",
                response.json()
            );
            response.json()["operation_id"].as_str().unwrap().to_owned()
        } else {
            crate::ops::insert(
                &h.state,
                &Principal::Peer {
                    peer_id: "example-peer".into(),
                },
                &serde_json::from_value::<OperationRequest>(request).unwrap(),
            )
            .await
            .unwrap()
            .operation_id
        };
        ids.push(id);
    }
    // Same-timestamp arrivals must retain their insertion order within each priority.
    sqlx::query("UPDATE operations SET created_at = '2024-01-01T00:00:00Z'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let worker = tokio::spawn(crate::ops::exec::run(h.state.clone()));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let completed: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM operations WHERE status = 'completed'")
                    .fetch_one(&h.state.db)
                    .await
                    .unwrap();
            let finished: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM operation_events WHERE event='finished'")
                    .fetch_one(&h.state.db)
                    .await
                    .unwrap();
            if completed == 5 && finished == 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    worker.abort();
    let _ = worker.await;
    result.expect("all queued operations must complete");
    let finished: Vec<String> = sqlx::query_scalar(
        "SELECT operation_id FROM operation_events WHERE event = 'finished' ORDER BY rowid",
    )
    .fetch_all(&h.state.db)
    .await
    .unwrap();
    assert_eq!(finished, [2, 4, 0, 1, 3].map(|index| ids[index].as_str()));
    assert!(h.root.join("opm-first").is_dir());
    assert!(h.root.join("opm-second").is_dir());
    assert!(h.root.join("peer-first").is_dir());
}

#[tokio::test]
async fn admin_session_and_login() {
    let h = harness().await;
    let r = send(&h.app, get("/api/admin/session", &[("cookie", &h.cookie)])).await;
    assert_eq!(r.json()["authenticated"], true);
    assert_eq!(r.json()["needs_setup"], false);
    let r = send(&h.app, get("/api/admin/overview", &[])).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);

    let bad = json!({ "username": "admin", "password": "wrong password!" });
    let r = send(
        &h.app,
        json_req("POST", "/api/admin/login", &[("x-ism-request", "1")], &bad),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let good = json!({ "username": "admin", "password": "correct horse battery" });
    let r = send(
        &h.app,
        json_req("POST", "/api/admin/login", &[("x-ism-request", "1")], &good),
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(r.headers["cache-control"], "no-store");
}

#[tokio::test]
async fn immich_writes_are_enabled_for_a_ready_server() {
    let mut maintenance = ism_core::maintenance::MaintenanceState::default();
    maintenance.phase = ism_core::maintenance::Phase::Active;
    let server = crate::immich::registry::Server {
        id: "server".into(),
        name: "Example".into(),
        url: "https://immich.example".into(),
        database_url_enc: Some("encrypted".into()),
        direct_writes_enabled: true,
        external_library_move_mode: ism_core::dto::ImmichMoveMode::ApiWithDbFallback,
        external_library_api_capable: Some(true),
        version: Some("3.4.1".into()),
        compatible: true,
        compatibility_detail: None,
        maintenance,
    };
    assert_eq!(crate::immich::registry::write_block(&server), None);
}

#[tokio::test]
async fn immich_move_mode_controls_direct_write_requirement() {
    let server = crate::immich::registry::Server {
        id: "server".into(),
        name: "Example".into(),
        url: "https://immich.example".into(),
        database_url_enc: Some("encrypted".into()),
        direct_writes_enabled: false,
        external_library_move_mode: ism_core::dto::ImmichMoveMode::ApiOnly,
        external_library_api_capable: Some(true),
        version: Some("3.4.1".into()),
        compatible: true,
        compatibility_detail: None,
        maintenance: ism_core::maintenance::MaintenanceState::default(),
    };
    assert_eq!(crate::immich::registry::write_block(&server), None);

    let mut db_only = server;
    db_only.external_library_move_mode = ism_core::dto::ImmichMoveMode::DbOnly;
    assert!(crate::immich::registry::write_block(&db_only).is_some());

    db_only.external_library_move_mode = ism_core::dto::ImmichMoveMode::ApiWithDbFallback;
    assert_eq!(crate::immich::registry::write_block(&db_only), None);
}

#[tokio::test]
async fn migrated_server_defaults_to_api_with_db_fallback() {
    let h = harness().await;
    sqlx::query(
        "INSERT INTO immich_servers (id, name, url, maintenance_json, created_at) \
         VALUES ('mode-default', 'Mode default', 'https://immich.example', ?, ?)",
    )
    .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
    .bind(crate::db::now())
    .execute(&h.state.db)
    .await
    .unwrap();

    let server = crate::immich::registry::server(&h.state, "mode-default")
        .await
        .unwrap();
    assert_eq!(
        server.external_library_move_mode,
        ism_core::dto::ImmichMoveMode::ApiWithDbFallback
    );
    assert_eq!(server.external_library_api_capable, None);
}

#[tokio::test]
async fn an_empty_library_folder_does_not_require_immich_queue_access() {
    let h = harness().await;
    h.map_root().await;
    // Empty folders do not change Immich assets or require a queue pause.
    sqlx::query("INSERT INTO immich_servers (id, name, url, database_url_enc, direct_writes_enabled, version, compatible, maintenance_json, created_at) VALUES ('server', 'Example', 'https://immich.example', 'encrypted', 1, '3.4.1', 1, ?, ?)")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE mappings SET server_id = 'server' WHERE root_id = 'photos'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let request = json!({
        "idempotency_key": "folder",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    let principal = crate::auth::Principal::Admin {
        username: "test".into(),
    };
    let request: ism_core::dto::OperationRequest = serde_json::from_value(request).unwrap();
    let accepted = crate::ops::insert(&h.state, &principal, &request)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&h.state, &accepted.operation_id)
        .await
        .unwrap();
    let (operation, _) = crate::ops::load(&h.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(operation.status, ism_core::dto::OperationStatus::Completed);
    assert!(operation.items[0].error.is_none());
    assert!(h.root.join("2024/new").is_dir());
}

#[tokio::test]
async fn a_move_into_an_immich_library_schedules_an_import_scan() {
    let h = harness().await;
    h.map_root().await;
    let inbox = h._dir.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::write(inbox.join("IMG_0001.jpg"), b"jpeg").unwrap();
    std::fs::write(inbox.join("IMG_0001.jpg.xmp"), b"xmp").unwrap();
    let body =
        json!({ "root_id": "inbox", "name": "Inbox", "container_path": inbox.to_string_lossy() });
    let r = send(
        &h.app,
        json_req("POST", "/api/admin/mappings", &h.admin(), &body),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    let (_server, fake) = crate::maintenance::tests::fake_server(&h.state).await;
    sqlx::query("INSERT INTO immich_libraries (id, server_id, name, owner_user_id, import_paths_json, refreshed_at) VALUES ('lib1', 's1', 'Photos', 'u1', '[\"/library/photos\"]', ?)")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE immich_servers SET direct_writes_enabled = 1, database_url_enc = 'unused' WHERE id = 's1'")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE mappings SET server_id = 's1', library_id = 'lib1', immich_import_path = '/library/photos' WHERE root_id = 'photos'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let bearer = h.token(&["read"]).await;
    let auth = [("authorization", bearer.as_str())];
    let states = |info: &serde_json::Value| -> Vec<(String, String)> {
        let mut v: Vec<_> = info["roots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r["root_id"].as_str().unwrap().to_owned(),
                    r["direct_writes"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        v.sort();
        v
    };
    let info = send(&h.app, get("/api/v1/client/info", &auth)).await.json();
    assert_eq!(states(&info), [("photos".to_owned(), "enabled".to_owned())]);
    let target = h.root.join("2024/IMG_0001.jpg");
    *fake.probe.lock().unwrap() = Some(target.clone());

    let request: ism_core::dto::OperationRequest = serde_json::from_value(json!({
        "idempotency_key": "move-in",
        "actions": [{ "kind": "move",
            "source": { "root_id": "inbox", "path": "IMG_0001.jpg" },
            "target": { "root_id": "photos", "path": "2024/IMG_0001.jpg" } }]
    }))
    .unwrap();
    let principal = crate::auth::Principal::Admin {
        username: "test".into(),
    };
    let accepted = crate::ops::insert(&h.state, &principal, &request)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&h.state, &accepted.operation_id)
        .await
        .unwrap();
    let (operation, _) = crate::ops::load(&h.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        operation.status,
        ism_core::dto::OperationStatus::Completed,
        "{operation:?}"
    );
    assert!(target.exists() && h.root.join("2024/IMG_0001.jpg.xmp").exists());
    assert!(!inbox.join("IMG_0001.jpg").exists());
    // No existing asset is mutated, so the import does not pause Immich's queues.
    assert!(fake.queue_log.lock().unwrap().is_empty());
    assert!(fake.paused.lock().unwrap().is_empty());
    let server = crate::immich::registry::server(&h.state, "s1")
        .await
        .unwrap();
    assert_eq!(server.maintenance.active_ops, 0);
    // The file has no asset yet: Immich imports it with the next scan of its library.
    let due: Option<i64> =
        sqlx::query_scalar("SELECT due_at FROM scan_schedule WHERE library_id = 'lib1'")
            .fetch_optional(&h.state.db)
            .await
            .unwrap()
            .flatten();
    assert!(due.is_some());
    // ISM's own writes are not reported as outside changes.
    let expected: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM self_writes")
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert!(expected > 0);

    // Settings drift blocks the linked root with a reason; the plain root is unaffected.
    let mut maintenance = server.maintenance.clone();
    maintenance.drift = Some("the library watcher was re-enabled".into());
    sqlx::query("UPDATE immich_servers SET maintenance_json = ? WHERE id = 's1'")
        .bind(serde_json::to_string(&maintenance).unwrap())
        .execute(&h.state.db)
        .await
        .unwrap();
    let info = send(&h.app, get("/api/v1/client/info", &auth)).await.json();
    let photos = info["roots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["root_id"] == "photos")
        .unwrap();
    assert_eq!(photos["direct_writes"], "blocked");
    assert_eq!(photos["direct_writes_reason"], "drift");
    assert_eq!(
        photos["direct_writes_detail"],
        "the library watcher was re-enabled"
    );
}

#[tokio::test]
async fn ila_trash_requires_asset_identity_and_rejects_legacy_retry() {
    let h = harness().await;
    h.map_root().await;
    let path = h.root.join("2024/a.jpg");
    std::fs::write(&path, b"photo").unwrap();
    let bearer = h.token_for("ila", &["read", "trash"]).await;
    let auth = [("authorization", bearer.as_str())];
    let info = send(&h.app, get("/api/v1/client/info", &auth)).await;
    assert!(
        info.json()["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("trash_if_asset_matches"))
    );
    let legacy = json!({
        "idempotency_key": "ila-local-deletion:1",
        "actions": [{ "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.jpg" } }]
    });
    let new = json!({
        "idempotency_key": "ila-local-deletion:1",
        "actions": [{ "kind": "trash_if_asset_matches", "source": { "root_id": "photos", "path": "2024/a.jpg" },
                      "expected_asset_id": "asset-1" }]
    });
    let rejected = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &new)).await;
    assert_eq!(rejected.status, StatusCode::CONFLICT);
    assert_eq!(
        rejected.json()["error"]["code"],
        "asset_identity_unverified"
    );
    assert!(path.exists());

    let token_id: String = sqlx::query_scalar("SELECT id FROM client_tokens WHERE kind = 'ila'")
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    let principal = crate::ops::exec::principal_from_key(&h.state, &format!("token:{token_id}"))
        .await
        .unwrap();
    let legacy_request: ism_core::dto::OperationRequest = serde_json::from_value(legacy).unwrap();
    let accepted = crate::ops::insert(&h.state, &principal, &legacy_request)
        .await
        .unwrap();
    crate::ops::exec::run_operation(&h.state, &accepted.operation_id)
        .await
        .unwrap();
    let (operation, _) = crate::ops::load(&h.state, &accepted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        operation.items[0].error.as_ref().unwrap().code,
        "asset_identity_required"
    );
    assert!(path.exists());
    let retry = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &new)).await;
    assert_eq!(retry.status, StatusCode::CONFLICT);
    assert_eq!(retry.json()["error"]["code"], "idempotency_key_conflict");

    sqlx::query("INSERT INTO immich_servers (id, name, url, direct_writes_enabled, maintenance_json, created_at) VALUES ('server', 'Example', 'https://immich.example', 0, ?, ?)")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE mappings SET server_id = 'server' WHERE root_id = 'photos'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let ctx = crate::ops::plan::Ctx::new(&h.state, &principal, false)
        .await
        .unwrap();
    let plan = crate::ops::plan::plan(&ctx, &legacy_request.actions[0])
        .await
        .unwrap();
    assert_eq!(plan.blockers[0].code, "asset_identity_required");
    assert!(path.exists());
}

#[tokio::test]
async fn browse_manifest_and_download() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.CR3"), b"0123456789").unwrap();
    std::fs::write(h.root.join("2024/a.CR3.xmp"), b"<xmp/>").unwrap();
    std::fs::create_dir(h.root.join("2024/Trip")).unwrap();
    let bearer = h.token(&["read", "download"]).await;
    let auth = [("authorization", bearer.as_str())];

    let r = send(
        &h.app,
        get("/api/v1/folders?root_id=photos&path=2024", &auth),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let folders = r.json();
    assert_eq!(folders["folders"][0]["name"], "Trip");
    assert_eq!(folders["files"].as_array().unwrap().len(), 2);

    let r = send(
        &h.app,
        get("/api/v1/manifest?root_id=photos&path=2024/a.CR3", &auth),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let manifest = r.json();
    assert_eq!(manifest["group_id"], "photos:2024/a");
    let files = manifest["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .any(|f| f["role"] == "sidecar" && f["name"] == "a.CR3.xmp")
    );
    let original = files.iter().find(|f| f["role"] == "original").unwrap();
    let revision = original["revision"].as_str().unwrap();

    let uri = format!("/api/v1/content?root_id=photos&path=2024/a.CR3&revision={revision}");
    let mut req = get(&uri, &auth);
    req.headers_mut()
        .insert(header::RANGE, "bytes=2-5".parse().unwrap());
    let r = send(&h.app, req).await;
    assert_eq!(r.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(&r.body[..], b"2345");
    assert_eq!(r.headers[header::CONTENT_RANGE], "bytes 2-5/10");

    let r = send(&h.app, get(&uri, &auth)).await;
    assert_eq!(&r.body[..], b"0123456789");
    assert!(!r.headers.contains_key("x-ism-sha256"));

    std::thread::sleep(Duration::from_millis(5));
    std::fs::write(h.root.join("2024/a.CR3"), b"changed!!!!").unwrap();
    let r = send(&h.app, get(&uri, &auth)).await;
    assert_eq!(r.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(r.json()["error"]["code"], "revision_changed");

    let r = send(
        &h.app,
        get("/api/v1/manifest?root_id=photos&path=../etc/passwd", &auth),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let mv = json!({ "idempotency_key": "k", "label": "x", "actions": [{ "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.CR3" } }] });
    let r = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &mv)).await;
    assert_eq!(
        r.status,
        StatusCode::FORBIDDEN,
        "read-only token cannot submit"
    );
}

#[tokio::test]
async fn manifest_rehashes_when_content_changes_without_metadata_change() {
    let h = harness().await;
    h.map_root().await;
    let path = h.root.join("2024/a.jpg");
    std::fs::write(&path, b"old-content").unwrap();
    let bearer = h.token(&["read", "download"]).await;
    let auth = [("authorization", bearer.as_str())];
    let uri = "/api/v1/manifest?root_id=photos&path=2024/a.jpg";
    let first = send(&h.app, get(uri, &auth)).await;
    assert_eq!(first.status, StatusCode::OK);
    let before = first.json()["files"][0].clone();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, b"new-content").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    let second = send(&h.app, get(uri, &auth)).await;
    assert_eq!(second.status, StatusCode::OK);
    let after = second.json()["files"][0].clone();
    assert_eq!(before["revision"], after["revision"]);
    assert_ne!(before["sha256"], after["sha256"]);
    assert_eq!(after["sha256"], crate::crypto::sha256_hex(b"new-content"));
}

#[tokio::test]
async fn operation_retries_reject_changed_requests_and_share_one_submission() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "manage"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "idempotency_key": "create-folder-1",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/Trip" } }]
    });
    let (first, retry) = tokio::join!(
        send(&h.app, json_req("POST", "/api/v1/operations", &auth, &body)),
        send(&h.app, json_req("POST", "/api/v1/operations", &auth, &body))
    );
    assert_eq!(first.status, StatusCode::ACCEPTED, "{:?}", first.json());
    assert_eq!(retry.status, StatusCode::ACCEPTED, "{:?}", retry.json());
    let id = first.json()["operation_id"].as_str().unwrap().to_owned();
    assert_eq!(retry.json()["operation_id"], id);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM operations WHERE idempotency_key = ?")
            .bind("create-folder-1")
            .fetch_one(&h.state.db)
            .await
            .unwrap();
    assert_eq!(count, 1);

    let mut changed = body.clone();
    changed["actions"][0]["target"]["path"] = json!("2024/Other");
    let conflict = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &changed),
    )
    .await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.json()["error"]["code"], "idempotency_key_conflict");
    assert_eq!(h.wait_finished(&bearer, &id).await["status"], "completed");
    assert!(!h.root.join("2024/Other").exists());
}

#[tokio::test]
async fn operation_submission_and_key_cancellation_wait_for_sqlite_writer() {
    let h = harness().await;
    let mut writer = h.state.db.begin().await.unwrap();
    sqlx::query("INSERT INTO settings (key, value) VALUES ('test-writer', 'held')")
        .execute(&mut *writer)
        .await
        .unwrap();
    let principal = crate::auth::Principal::Admin {
        username: "admin".into(),
    };
    let request: ism_core::dto::OperationRequest = serde_json::from_value(json!({
        "idempotency_key": "writer-test",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    }))
    .unwrap();
    let state = h.state.clone();
    let actor = principal.clone();
    let mut submission =
        tokio::spawn(async move { crate::ops::insert(&state, &actor, &request).await });

    let cancel_request: ism_core::dto::CancelOperationByKey = serde_json::from_value(json!({
        "idempotency_key": "cancel-writer-test",
        "expected_request": {
            "idempotency_key": "cancel-writer-test",
            "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
        }
    }))
    .unwrap();
    let state = h.state.clone();
    let mut cancellation =
        tokio::spawn(
            async move { crate::ops::cancel_by_key(&state, &principal, cancel_request).await },
        );

    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut submission)
            .await
            .is_err(),
        "submission must wait for the other writer rather than fail"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut cancellation)
            .await
            .is_err(),
        "cancellation must wait for the other writer rather than fail"
    );
    writer.commit().await.unwrap();
    submission.await.unwrap().unwrap();
    assert!(matches!(
        cancellation.await.unwrap().unwrap(),
        ism_core::dto::CancelOperationByKeyResult::CancelledBeforeSubmission
    ));
}

#[tokio::test]
async fn move_group_trash_and_restore() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/b.jpg"), b"jpeg").unwrap();
    std::fs::write(h.root.join("2024/b.jpg.xmp"), b"<xmp/>").unwrap();
    let bearer = h.token(&["read", "manage", "trash"]).await;
    let auth = [("authorization", bearer.as_str())];

    let body = json!({
        "idempotency_key": "move-1",
        "label": "Move b",
        "actions": [
            { "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/Trip" } },
            { "kind": "move", "source": { "root_id": "photos", "path": "2024/b.jpg" },
              "target": { "root_id": "photos", "path": "2024/Trip/b.jpg" } }
        ]
    });
    let r = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &body),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["ok"], true, "{:?}", r.json());
    let r = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &body)).await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let again = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &body)).await;
    assert_eq!(
        again.json()["operation_id"],
        op.as_str(),
        "idempotent submit"
    );

    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert!(h.root.join("2024/Trip/b.jpg").is_file());
    assert!(
        h.root.join("2024/Trip/b.jpg.xmp").is_file(),
        "sidecar follows the original"
    );
    assert!(!h.root.join("2024/b.jpg").exists());

    let body = json!({ "idempotency_key": "trash-1", "label": "Trash b",
        "actions": [{ "kind": "trash", "source": { "root_id": "photos", "path": "2024/Trip/b.jpg" } }] });
    let r = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &body)).await;
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert!(!h.root.join("2024/Trip/b.jpg").exists());
    assert!(!h.root.join("2024/Trip/b.jpg.xmp").exists());

    let r = send(&h.app, get("/api/v1/trash", &auth)).await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let trash = r.json();
    let items = trash
        .as_array()
        .or_else(|| trash["items"].as_array())
        .unwrap()
        .clone();
    assert_eq!(items.len(), 1, "{trash}");
    let trash_id = items[0]["trash_id"]
        .as_str()
        .or_else(|| items[0]["id"].as_str())
        .unwrap()
        .to_owned();

    let body = json!({ "idempotency_key": "restore-1", "label": "Restore b",
        "actions": [{ "kind": "restore", "trash_id": trash_id }] });
    let r = send(&h.app, json_req("POST", "/api/v1/operations", &auth, &body)).await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(
        std::fs::read(h.root.join("2024/Trip/b.jpg")).unwrap(),
        b"jpeg"
    );
    assert!(h.root.join("2024/Trip/b.jpg.xmp").is_file());
}

#[tokio::test]
async fn restore_rejects_missing_quarantined_original() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.jpg"), b"photo").unwrap();
    std::fs::write(h.root.join("2024/a.jpg.xmp"), b"sidecar").unwrap();
    let bearer = h.token(&["read", "trash", "purge"]).await;
    let auth = [("authorization", bearer.as_str())];
    let trash = json!({
        "idempotency_key": "trash-missing-original",
        "actions": [{ "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.jpg" } }]
    });
    let submitted = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &trash),
    )
    .await;
    let operation = submitted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        h.wait_finished(&bearer, &operation).await["status"],
        "completed"
    );
    let (trash_id, quarantine): (String, String) =
        sqlx::query_as("SELECT id, quarantine_rel FROM trash")
            .fetch_one(&h.state.db)
            .await
            .unwrap();
    sqlx::query("UPDATE trash SET quarantine_rel = '.ism-trash/unexpected' WHERE id = ?")
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let restore = json!({
        "idempotency_key": "restore-invalid-quarantine",
        "actions": [{ "kind": "restore", "trash_id": trash_id }]
    });
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(preview.status, StatusCode::OK);
    assert_eq!(preview.json()["ok"], false);
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_corrupt"
    );
    let rejected = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &restore),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::CONFLICT);
    assert_eq!(rejected.json()["error"]["code"], "trash_corrupt");
    let purge = json!({
        "idempotency_key": "purge-invalid-quarantine",
        "actions": [{ "kind": "purge", "trash_id": trash_id }]
    });
    let rejected = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &purge),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::CONFLICT);
    assert_eq!(rejected.json()["error"]["code"], "trash_corrupt");
    sqlx::query("UPDATE trash SET quarantine_rel = ? WHERE id = ?")
        .bind(format!(".ism-trash/{trash_id}"))
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    std::fs::remove_file(h.root.join(&quarantine).join("2024/a.jpg")).unwrap();
    let restore = json!({
        "idempotency_key": "restore-missing-original",
        "actions": [{ "kind": "restore", "trash_id": trash_id }]
    });
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(preview.status, StatusCode::OK);
    assert_eq!(preview.json()["ok"], false);
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_missing"
    );
    let restored: Option<String> = sqlx::query_scalar("SELECT restored_at FROM trash")
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert!(restored.is_none());
    assert!(h.root.join(&quarantine).join("2024/a.jpg.xmp").is_file());
}

#[tokio::test]
async fn restore_requires_complete_file_group_inventory() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.jpg"), b"photo").unwrap();
    std::fs::write(h.root.join("2024/a.jpg.xmp"), b"sidecar").unwrap();
    let bearer = h.token(&["read", "trash"]).await;
    let auth = [("authorization", bearer.as_str())];
    let trash = json!({
        "idempotency_key": "inventory-trash",
        "actions": [{ "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.jpg" } }]
    });
    let submitted = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &trash),
    )
    .await;
    let operation = submitted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        h.wait_finished(&bearer, &operation).await["status"],
        "completed"
    );
    let (trash_id, quarantine, inventory): (String, String, String) =
        sqlx::query_as("SELECT id, quarantine_rel, file_inventory_json FROM trash")
            .fetch_one(&h.state.db)
            .await
            .unwrap();
    let held = h.root.join(&quarantine).join("2024");
    let restore = json!({
        "idempotency_key": "inventory-restore",
        "actions": [{ "kind": "restore", "trash_id": trash_id }]
    });

    std::fs::remove_file(held.join("a.jpg.xmp")).unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    let refused = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &restore),
    )
    .await;
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert!(held.join("a.jpg").is_file());
    assert!(!h.root.join("2024/a.jpg").exists());
    std::fs::write(held.join("a.jpg.xmp"), b"sidecar").unwrap();

    std::fs::write(held.join("a.jpg.xmp"), b"changed").unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    std::fs::write(held.join("a.jpg.xmp"), b"sidecar").unwrap();

    std::fs::write(held.join("unexpected.txt"), b"unexpected").unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    std::fs::remove_file(held.join("unexpected.txt")).unwrap();

    sqlx::query("UPDATE trash SET file_inventory_json = NULL WHERE id = ?")
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_missing"
    );
    sqlx::query("UPDATE trash SET file_inventory_json = ? WHERE id = ?")
        .bind(inventory)
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE trash SET asset_ids_json = 'broken' WHERE id = ?")
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(preview.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(held.join("a.jpg").is_file());
    sqlx::query("UPDATE trash SET asset_ids_json = '[]' WHERE id = ?")
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let submitted = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &restore),
    )
    .await;
    assert_eq!(
        submitted.status,
        StatusCode::ACCEPTED,
        "{:?}",
        submitted.json()
    );
    let operation = submitted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        h.wait_finished(&bearer, &operation).await["status"],
        "completed"
    );
    assert_eq!(std::fs::read(h.root.join("2024/a.jpg")).unwrap(), b"photo");
    assert_eq!(
        std::fs::read(h.root.join("2024/a.jpg.xmp")).unwrap(),
        b"sidecar"
    );
}

#[tokio::test]
async fn folder_trash_restores_only_a_complete_unchanged_tree() {
    let h = harness().await;
    h.map_root().await;
    let folder = h.root.join("2024/trip");
    std::fs::create_dir_all(folder.join("nested/empty")).unwrap();
    std::fs::write(folder.join("nested/a.jpg"), b"photo").unwrap();
    std::fs::write(folder.join("nested/a.jpg.xmp"), b"sidecar").unwrap();
    let bearer = h.token(&["read", "trash"]).await;
    let auth = [("authorization", bearer.as_str())];
    let trash = json!({
        "idempotency_key": "folder-inventory-trash",
        "actions": [{ "kind": "trash_folder", "source": { "root_id": "photos", "path": "2024/trip" } }]
    });
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("a.jpg", folder.join("nested/link")).unwrap();
        let preview = send(
            &h.app,
            json_req("POST", "/api/v1/operations/preview", &auth, &trash),
        )
        .await;
        assert_eq!(
            preview.json()["items"][0]["blockers"][0]["code"],
            "trash_inventory_unverifiable"
        );
        assert!(folder.join("nested/a.jpg").exists());
        std::fs::remove_file(folder.join("nested/link")).unwrap();
    }
    let submitted = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &trash),
    )
    .await;
    assert_eq!(
        submitted.status,
        StatusCode::ACCEPTED,
        "{:?}",
        submitted.json()
    );
    let op = submitted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(h.wait_finished(&bearer, &op).await["status"], "completed");
    let (trash_id, quarantine, inventory): (String, String, String) =
        sqlx::query_as("SELECT id, quarantine_rel, file_inventory_json FROM trash")
            .fetch_one(&h.state.db)
            .await
            .unwrap();
    let held = h.root.join(&quarantine).join("2024");
    let tree = held.join("trip");
    let restore = json!({
        "idempotency_key": "folder-inventory-restore",
        "actions": [{ "kind": "restore", "trash_id": trash_id }]
    });

    std::fs::remove_file(tree.join("nested/a.jpg.xmp")).unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    assert_eq!(
        send(
            &h.app,
            json_req("POST", "/api/v1/operations", &auth, &restore)
        )
        .await
        .status,
        StatusCode::CONFLICT
    );
    std::fs::write(tree.join("nested/a.jpg.xmp"), b"sidecar").unwrap();

    std::fs::write(tree.join("nested/a.jpg"), b"edited").unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    std::fs::write(tree.join("nested/a.jpg"), b"photo").unwrap();

    std::fs::remove_dir(tree.join("nested/empty")).unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    std::fs::create_dir(tree.join("nested/empty")).unwrap();

    std::fs::write(held.join("unexpected.txt"), b"unexpected").unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_changed"
    );
    std::fs::remove_file(held.join("unexpected.txt")).unwrap();

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("a.jpg", tree.join("nested/link")).unwrap();
        let preview = send(
            &h.app,
            json_req("POST", "/api/v1/operations/preview", &auth, &restore),
        )
        .await;
        assert_eq!(
            preview.json()["items"][0]["blockers"][0]["code"],
            "trash_inventory_unverifiable"
        );
        std::fs::remove_file(tree.join("nested/link")).unwrap();
    }

    sqlx::query("UPDATE trash SET file_inventory_json = NULL WHERE id = ?")
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let preview = send(
        &h.app,
        json_req("POST", "/api/v1/operations/preview", &auth, &restore),
    )
    .await;
    assert_eq!(
        preview.json()["items"][0]["blockers"][0]["code"],
        "trash_inventory_missing"
    );
    sqlx::query("UPDATE trash SET file_inventory_json = ? WHERE id = ?")
        .bind(inventory)
        .bind(&trash_id)
        .execute(&h.state.db)
        .await
        .unwrap();

    let submitted = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &restore),
    )
    .await;
    assert_eq!(
        submitted.status,
        StatusCode::ACCEPTED,
        "{:?}",
        submitted.json()
    );
    let op = submitted.json()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(h.wait_finished(&bearer, &op).await["status"], "completed");
    assert_eq!(
        std::fs::read(folder.join("nested/a.jpg")).unwrap(),
        b"photo"
    );
    assert_eq!(
        std::fs::read(folder.join("nested/a.jpg.xmp")).unwrap(),
        b"sidecar"
    );
    assert!(folder.join("nested/empty").is_dir());
}

#[cfg(unix)]
#[tokio::test]
async fn client_requests_reject_symlinks_inside_a_mapped_root() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.jpg"), b"photo").unwrap();
    std::os::unix::fs::symlink(h.root.join("2024"), h.root.join("alias")).unwrap();
    let bearer = h.token(&["read", "download", "manage"]).await;
    let auth = [("authorization", bearer.as_str())];
    let content = send(
        &h.app,
        get("/api/v1/content?root_id=photos&path=alias/a.jpg", &auth),
    )
    .await;
    assert_eq!(content.status, StatusCode::BAD_REQUEST);
    assert_eq!(content.json()["error"]["code"], "invalid_location");

    let request = json!({
        "idempotency_key": "symlink-move",
        "actions": [{
            "kind": "move",
            "source": { "root_id": "photos", "path": "alias/a.jpg" },
            "target": { "root_id": "photos", "path": "2024/b.jpg" }
        }]
    });
    let response = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &request),
    )
    .await;
    assert_ne!(response.status, StatusCode::ACCEPTED);
    assert_eq!(std::fs::read(h.root.join("2024/a.jpg")).unwrap(), b"photo");
    assert!(!h.root.join("2024/b.jpg").exists());
}

#[tokio::test]
async fn receiving_upload_does_not_hide_missing_staging_files() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 5,
                    "sha256": crate::crypto::sha256_hex(b"photo") }],
        "on_conflict": "fail"
    });
    let response = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(response.status, StatusCode::CREATED);
    let id = response.json()["upload_id"].as_str().unwrap().to_owned();
    std::fs::remove_file(h.root.join(".ism-staging/uploads").join(&id).join("0")).unwrap();
    let response = send(&h.app, get(&format!("/api/v1/uploads/{id}"), &auth)).await;
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(response.json()["error"]["code"], "upload_staging_missing");
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/api/v1/uploads/{id}/files/0"))
        .header("authorization", &bearer)
        .body(Body::from("photo"))
        .unwrap();
    let response = send(&h.app, request).await;
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(response.json()["error"]["code"], "upload_staging_missing");
    let response = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/uploads/{id}/commit"),
            &auth,
            &json!({}),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(response.json()["error"]["code"], "upload_staging_missing");
    assert!(
        !h.root
            .join(".ism-staging/uploads")
            .join(&id)
            .join("0")
            .exists()
    );
}

#[tokio::test]
async fn cancelling_receiving_upload_is_idempotent_and_preserves_ownership() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "idempotency_key": "import-job",
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 5,
                    "sha256": crate::crypto::sha256_hex(b"photo") }],
        "on_conflict": "fail"
    });
    let created = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    let id = created.json()["upload_id"].as_str().unwrap().to_owned();
    let staged = h.root.join(".ism-staging/uploads").join(&id);
    std::fs::write(staged.join("0"), b"photo").unwrap();

    let other = h.token(&["read", "upload"]).await;
    let delete = |token: &str| {
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/uploads/{id}"))
            .header("authorization", token)
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        send(&h.app, delete(&other)).await.status,
        StatusCode::NOT_FOUND
    );
    assert!(staged.exists());

    let guard = h.state.lock_upload(&id).await;
    let app = h.app.clone();
    let request = delete(&bearer);
    let mut cancel = tokio::spawn(async move { send(&app, request).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut cancel)
            .await
            .is_err()
    );
    drop(guard);
    assert_eq!(cancel.await.unwrap().status, StatusCode::NO_CONTENT);
    assert!(!staged.exists());
    assert_eq!(
        send(&h.app, delete(&bearer)).await.status,
        StatusCode::NO_CONTENT
    );
    let retry = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(retry.json()["upload_id"], id);
    assert_eq!(retry.json()["status"], "cancelled");
    let commit = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/uploads/{id}/commit"),
            &auth,
            &json!({}),
        ),
    )
    .await;
    assert_eq!(commit.status, StatusCode::CONFLICT);
    assert_eq!(commit.json()["error"]["code"], "upload_closed");
}

#[tokio::test]
async fn cancelled_upload_cleanup_can_be_retried_after_root_recovers() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 0,
                    "sha256": crate::crypto::sha256_hex(b"") }],
        "on_conflict": "fail"
    });
    let created = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    let id = created.json()["upload_id"].as_str().unwrap().to_owned();
    let staged = h.root.join(".ism-staging/uploads").join(&id);
    let sentinel = h.root.join(".ism-root");
    std::fs::remove_file(&sentinel).unwrap();
    let delete = || {
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/uploads/{id}"))
            .header("authorization", &bearer)
            .body(Body::empty())
            .unwrap()
    };
    assert!(!send(&h.app, delete()).await.status.is_success());
    assert!(staged.exists());
    let status: String = sqlx::query_scalar("SELECT status FROM uploads WHERE id = ?")
        .bind(&id)
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert_eq!(status, "cancelled");
    std::fs::write(&sentinel, "photos\n").unwrap();
    assert_eq!(send(&h.app, delete()).await.status, StatusCode::NO_CONTENT);
    assert!(!staged.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn upload_cancellation_refuses_symlinked_staging_parent() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 0,
                    "sha256": crate::crypto::sha256_hex(b"") }],
        "on_conflict": "fail"
    });
    let created = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    let id = created.json()["upload_id"].as_str().unwrap().to_owned();
    let uploads = h.root.join(".ism-staging/uploads");
    let backup = h.root.join(".ism-staging/uploads-backup");
    let outside = h._dir.path().join("outside");
    std::fs::create_dir_all(outside.join(&id)).unwrap();
    std::fs::write(outside.join(&id).join("0"), b"preserve").unwrap();
    std::fs::rename(&uploads, &backup).unwrap();
    std::os::unix::fs::symlink(&outside, &uploads).unwrap();
    let delete = || {
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/uploads/{id}"))
            .header("authorization", &bearer)
            .body(Body::empty())
            .unwrap()
    };
    let rejected = send(&h.app, delete()).await;
    assert_eq!(rejected.json()["error"]["code"], "invalid_location");
    assert_eq!(
        std::fs::read(outside.join(&id).join("0")).unwrap(),
        b"preserve"
    );
    std::fs::remove_file(&uploads).unwrap();
    std::fs::rename(&backup, &uploads).unwrap();
    assert_eq!(send(&h.app, delete()).await.status, StatusCode::NO_CONTENT);
    assert!(!uploads.join(&id).exists());
}

#[tokio::test]
async fn upload_status_waits_for_a_complete_chunk() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 5,
                    "sha256": crate::crypto::sha256_hex(b"photo") }],
        "on_conflict": "fail"
    });
    let response = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    let id = response.json()["upload_id"].as_str().unwrap().to_owned();

    let guard = h.state.lock_upload(&id).await;
    let staged_file = h.root.join(".ism-staging/uploads").join(&id).join("0");
    std::fs::write(&staged_file, b"ph").unwrap();
    let request = get(&format!("/api/v1/uploads/{id}"), &auth);
    let app = h.app.clone();
    let mut status = tokio::spawn(async move { send(&app, request).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut status)
            .await
            .is_err()
    );
    std::fs::write(&staged_file, b"photo").unwrap();
    drop(guard);
    let response = status.await.unwrap();
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.json()["files"][0]["received"], 5);
}

#[tokio::test]
async fn upload_chunks_and_commit_are_serialized() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 5,
                    "sha256": crate::crypto::sha256_hex(b"photo") }],
        "on_conflict": "fail"
    });
    let response = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(response.status, StatusCode::CREATED);
    let id = response.json()["upload_id"].as_str().unwrap().to_owned();

    let guard = h.state.lock_upload(&id).await;
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/api/v1/uploads/{id}/files/0"))
        .header("authorization", &bearer)
        .body(Body::from("photo"))
        .unwrap();
    let app = h.app.clone();
    let mut put = tokio::spawn(async move { send(&app, request).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut put)
            .await
            .is_err()
    );
    drop(guard);
    assert!(put.await.unwrap().status.is_success());

    let guard = h.state.lock_upload(&id).await;
    let request = json_req(
        "POST",
        &format!("/api/v1/uploads/{id}/commit"),
        &auth,
        &json!({}),
    );
    let app = h.app.clone();
    let mut commit = tokio::spawn(async move { send(&app, request).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut commit)
            .await
            .is_err()
    );
    drop(guard);
    let response = commit.await.unwrap();
    assert_eq!(
        response.status,
        StatusCode::ACCEPTED,
        "{:?}",
        response.json()
    );
    let operation = response.json()["operation_id"].as_str().unwrap().to_owned();

    let cancel = Request::builder()
        .method("DELETE")
        .uri(format!("/api/v1/uploads/{id}"))
        .header("authorization", &bearer)
        .body(Body::empty())
        .unwrap();
    let refused = send(&h.app, cancel).await;
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert_eq!(refused.json()["error"]["code"], "upload_closed");
    assert_eq!(
        std::fs::read(h.root.join(".ism-staging/uploads").join(&id).join("0")).unwrap(),
        b"photo"
    );

    let late = Request::builder()
        .method("PUT")
        .uri(format!("/api/v1/uploads/{id}/files/0"))
        .header("authorization", &bearer)
        .body(Body::from("other"))
        .unwrap();
    assert_eq!(send(&h.app, late).await.status, StatusCode::CONFLICT);
    assert_eq!(
        h.wait_finished(&bearer, &operation).await["status"],
        "completed"
    );
    assert_eq!(std::fs::read(h.root.join("2024/a.jpg")).unwrap(), b"photo");
}

#[tokio::test]
async fn upload_expiry_rechecks_status_before_removing_staged_files() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 5,
                    "sha256": crate::crypto::sha256_hex(b"photo") }],
        "on_conflict": "fail"
    });
    let response = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    let id = response.json()["upload_id"].as_str().unwrap().to_owned();
    sqlx::query("UPDATE uploads SET created_at = '2000-01-01T00:00:00Z' WHERE id = ?")
        .bind(&id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let guard = h.state.lock_upload(&id).await;
    let state = h.state.clone();
    let mut expiry = tokio::spawn(async move { crate::ops::uploads::expire(&state).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut expiry)
            .await
            .is_err()
    );
    sqlx::query("UPDATE uploads SET status = 'publishing' WHERE id = ?")
        .bind(&id)
        .execute(&h.state.db)
        .await
        .unwrap();
    drop(guard);
    expiry.await.unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM uploads WHERE id = ?")
        .bind(&id)
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    assert_eq!(status, "publishing");
    assert!(
        h.root
            .join(".ism-staging/uploads")
            .join(&id)
            .join("0")
            .exists()
    );
}

#[tokio::test]
async fn upload_creation_retries_are_idempotent_per_credential() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "idempotency_key": "import-job-1",
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 5,
                    "sha256": crate::crypto::sha256_hex(b"photo") }],
        "on_conflict": "fail"
    });
    let (first, retry) = tokio::join!(
        send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)),
        send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body))
    );
    assert_eq!(first.status, StatusCode::CREATED);
    assert_eq!(retry.status, StatusCode::CREATED);
    let id = first.json()["upload_id"].as_str().unwrap().to_owned();
    assert_eq!(retry.json()["upload_id"], id);

    let put = Request::builder()
        .method("PUT")
        .uri(format!("/api/v1/uploads/{id}/files/0"))
        .header("authorization", &bearer)
        .body(Body::from("photo"))
        .unwrap();
    assert!(send(&h.app, put).await.status.is_success());
    let retry = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(retry.json()["upload_id"], id);
    assert_eq!(retry.json()["files"][0]["received"], 5);

    let old_request: String = sqlx::query_scalar("SELECT request_json FROM uploads WHERE id = ?")
        .bind(&id)
        .fetch_one(&h.state.db)
        .await
        .unwrap();
    let mut old_request: Value = serde_json::from_str(&old_request).unwrap();
    old_request["files"][0]
        .as_object_mut()
        .unwrap()
        .remove("expected_revision");
    sqlx::query("UPDATE uploads SET request_json = ? WHERE id = ?")
        .bind(old_request.to_string())
        .bind(&id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let retry = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(retry.status, StatusCode::CREATED);
    assert_eq!(retry.json()["upload_id"], id);

    let mut changed = body.clone();
    changed["files"][0]["name"] = json!("b.jpg");
    let conflict = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &changed)).await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.json()["error"]["code"], "idempotency_key_conflict");

    sqlx::query("UPDATE uploads SET status = 'failed' WHERE id = ?")
        .bind(&id)
        .execute(&h.state.db)
        .await
        .unwrap();
    let failed = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(failed.json()["upload_id"], id);
    assert_eq!(failed.json()["status"], "failed");

    let other_bearer = h.token(&["read", "upload"]).await;
    let other_auth = [("authorization", other_bearer.as_str())];
    let other = send(
        &h.app,
        json_req("POST", "/api/v1/uploads", &other_auth, &body),
    )
    .await;
    assert_eq!(other.status, StatusCode::CREATED);
    assert_ne!(other.json()["upload_id"], id);

    let mut legacy = body.clone();
    legacy.as_object_mut().unwrap().remove("idempotency_key");
    let first = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &legacy)).await;
    let second = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &legacy)).await;
    assert_eq!(first.status, StatusCode::CREATED);
    assert_eq!(second.status, StatusCode::CREATED);
    assert_ne!(first.json()["upload_id"], second.json()["upload_id"]);

    for key in ["", "   ", &"x".repeat(201)] {
        let mut invalid = body.clone();
        invalid["idempotency_key"] = json!(key);
        let response = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &invalid)).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert_eq!(response.json()["error"]["code"], "bad_idempotency_key");
    }
}

#[tokio::test]
async fn upload_commit_rejects_an_existing_operation_key() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload", "manage"]).await;
    let auth = [("authorization", bearer.as_str())];
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": "a.jpg", "role": "original", "size": 0,
                    "sha256": crate::crypto::sha256_hex(b"") }],
        "on_conflict": "fail"
    });
    let upload = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    let id = upload.json()["upload_id"].as_str().unwrap().to_owned();
    let operation = json!({
        "idempotency_key": format!("upload:{id}"),
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/Trip" } }]
    });
    let operation = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &operation),
    )
    .await;
    assert_eq!(operation.status, StatusCode::ACCEPTED);
    let commit = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/uploads/{id}/commit"),
            &auth,
            &json!({}),
        ),
    )
    .await;
    assert_eq!(commit.status, StatusCode::CONFLICT);
    assert_eq!(commit.json()["error"]["code"], "idempotency_key_conflict");
    let upload = send(&h.app, get(&format!("/api/v1/uploads/{id}"), &auth)).await;
    assert_eq!(upload.json()["status"], "receiving");
}

#[tokio::test]
async fn upload_keeps_the_folder() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload", "manage"]).await;
    let auth = [("authorization", bearer.as_str())];
    let photo = b"photo-bytes";
    let xmp = b"<x/>";
    let sha = |b: &[u8]| crate::crypto::sha256_hex(b);
    let body = json!({
        "target": { "root_id": "photos", "path": "2024/New" },
        "provenance": { "source": "ila" },
        "files": [
            { "name": "c.jpg", "role": "original", "size": photo.len(), "sha256": sha(photo) },
            { "name": "c.jpg.xmp", "role": "sidecar", "size": xmp.len(), "sha256": sha(xmp) }
        ],
        "on_conflict": "fail"
    });
    let r = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    let id = r.json()["upload_id"].as_str().unwrap().to_owned();

    let put = |index: usize, range: &str, bytes: &'static [u8]| {
        Request::builder()
            .method("PUT")
            .uri(format!("/api/v1/uploads/{id}/files/{index}"))
            .header("authorization", bearer.as_str())
            .header(header::CONTENT_RANGE, range)
            .body(Body::from(bytes))
            .unwrap()
    };
    let r = send(&h.app, put(0, "bytes 0-4/11", &photo[..5])).await;
    assert!(r.status.is_success(), "{:?}", r.json());
    let r = send(&h.app, get(&format!("/api/v1/uploads/{id}"), &auth)).await;
    assert_eq!(r.json()["files"][0]["received"], 5);
    let r = send(&h.app, put(0, "bytes 5-10/11", &photo[5..])).await;
    assert!(r.status.is_success(), "{:?}", r.json());
    let r = send(&h.app, put(1, "bytes 0-3/4", xmp)).await;
    assert!(r.status.is_success(), "{:?}", r.json());

    let r = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/uploads/{id}/commit"),
            &auth,
            &json!({}),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let different = json!({
        "idempotency_key": format!("upload:{id}"),
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/Other" } }]
    });
    let conflict = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &different),
    )
    .await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.json()["error"]["code"], "idempotency_key_conflict");
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(std::fs::read(h.root.join("2024/New/c.jpg")).unwrap(), photo);
    assert_eq!(
        std::fs::read(h.root.join("2024/New/c.jpg.xmp")).unwrap(),
        xmp
    );
}

#[tokio::test]
async fn replacement_checks_each_destination_revision() {
    for scenario in [
        "unchanged",
        "changed",
        "same_mtime",
        "omitted",
        "new_sidecar",
    ] {
        let h = harness().await;
        h.map_root().await;
        let original = h.root.join("2024/a.jpg");
        let sidecar = h.root.join("2024/a.jpg.xmp");
        std::fs::write(&original, b"old-original").unwrap();
        if scenario != "new_sidecar" {
            std::fs::write(&sidecar, b"old-sidecar").unwrap();
        }
        let revision = |p: &std::path::Path| crate::files::revision(&std::fs::metadata(p).unwrap());
        let original_revision = revision(&original);
        let sidecar_revision =
            (scenario != "new_sidecar" && scenario != "omitted").then(|| revision(&sidecar));
        if scenario == "changed" {
            std::fs::write(&sidecar, b"changed-on-server").unwrap();
        }
        if scenario == "same_mtime" {
            let modified = std::fs::metadata(&sidecar).unwrap().modified().unwrap();
            std::fs::write(&sidecar, b"bad-sidecar").unwrap();
            std::fs::File::open(&sidecar)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(modified))
                .unwrap();
        }
        let bearer = h.token(&["read", "upload"]).await;
        let auth = [("authorization", bearer.as_str())];
        let sha = |b: &[u8]| crate::crypto::sha256_hex(b);
        let body = json!({
            "target": { "root_id": "photos", "path": "2024" },
            "provenance": { "source": "ila", "expected_revision": original_revision },
            "files": [
                { "name": "a.jpg", "role": "original", "size": 3, "sha256": sha(b"new"),
                  "expected_sha256": sha(b"old-original") },
                { "name": "a.jpg.xmp", "role": "sidecar", "size": 3, "sha256": sha(b"xmp"),
                  "expected_revision": sidecar_revision,
                  "expected_sha256": if scenario == "new_sidecar" { None } else { Some(sha(b"old-sidecar")) } }
            ],
            "on_conflict": "replace_if_expected"
        });
        let r = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, &body)).await;
        assert_eq!(r.status, StatusCode::CREATED, "{scenario}: {:?}", r.json());
        let id = r.json()["upload_id"].as_str().unwrap().to_owned();
        for (index, bytes) in [b"new", b"xmp"].into_iter().enumerate() {
            let request = Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/uploads/{id}/files/{index}"))
                .header("authorization", bearer.as_str())
                .body(Body::from(bytes.as_slice()))
                .unwrap();
            let r = send(&h.app, request).await;
            assert!(r.status.is_success(), "{scenario}: {:?}", r.json());
        }
        let r = send(
            &h.app,
            json_req(
                "POST",
                &format!("/api/v1/uploads/{id}/commit"),
                &auth,
                &json!({}),
            ),
        )
        .await;
        assert_eq!(r.status, StatusCode::ACCEPTED, "{scenario}: {:?}", r.json());
        let op = r.json()["operation_id"].as_str().unwrap().to_owned();
        let done = h.wait_finished(&bearer, &op).await;
        if scenario == "changed" || scenario == "same_mtime" || scenario == "omitted" {
            assert_eq!(done["status"], "failed", "{scenario}: {done}");
            assert_eq!(done["items"][0]["error"]["code"], "revision_changed");
            let status = send(&h.app, get(&format!("/api/v1/uploads/{id}"), &auth)).await;
            assert_eq!(status.json()["status"], "failed", "{scenario}");
            let retry = send(
                &h.app,
                json_req(
                    "POST",
                    &format!("/api/v1/uploads/{id}/commit"),
                    &auth,
                    &json!({}),
                ),
            )
            .await;
            assert_eq!(retry.json()["operation_id"], op, "{scenario}");
            assert_eq!(retry.json()["status"], "failed", "{scenario}");
            assert_eq!(std::fs::read(&original).unwrap(), b"old-original");
            let expected: &[u8] = if scenario == "changed" {
                b"changed-on-server"
            } else if scenario == "same_mtime" {
                b"bad-sidecar"
            } else {
                b"old-sidecar"
            };
            assert_eq!(std::fs::read(&sidecar).unwrap(), expected);
        } else {
            assert_eq!(done["status"], "completed", "{scenario}: {done}");
            assert_eq!(std::fs::read(&original).unwrap(), b"new");
            assert_eq!(std::fs::read(&sidecar).unwrap(), b"xmp");
            let inventory: String = sqlx::query_scalar("SELECT file_inventory_json FROM trash")
                .fetch_one(&h.state.db)
                .await
                .unwrap();
            let files: Vec<Value> = serde_json::from_str(&inventory).unwrap();
            let mut names: Vec<&str> = files
                .iter()
                .map(|file| file["name"].as_str().unwrap())
                .collect();
            names.sort();
            let expected = if scenario == "new_sidecar" {
                vec!["a.jpg"]
            } else {
                vec!["a.jpg", "a.jpg.xmp"]
            };
            assert_eq!(names, expected, "{scenario}");
            for file in files {
                let expected_hash = if file["name"] == "a.jpg" {
                    sha(b"old-original")
                } else {
                    sha(b"old-sidecar")
                };
                assert_eq!(file["sha256"], expected_hash, "{scenario}");
            }
        }
    }
}

#[tokio::test]
async fn bookkeeping_failure_is_reported_as_recovery_required() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.jpg"), b"original").unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_trash BEFORE INSERT ON trash BEGIN SELECT RAISE(FAIL, 'test failure'); END",
    )
    .execute(&h.state.db)
    .await
    .unwrap();
    let bearer = h.token(&["read", "trash"]).await;
    let body = json!({
        "idempotency_key": "test-bookkeeping",
        "label": "Trash",
        "actions": [{ "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.jpg" } }]
    });
    let r = send(
        &h.app,
        json_req(
            "POST",
            "/api/v1/operations",
            &[("authorization", &bearer)],
            &body,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "recovery_required", "{done}");
    assert_eq!(done["items"][0]["error"]["code"], "recovery_required");
    assert!(!h.root.join("2024/a.jpg").exists());
}

#[tokio::test]
async fn independent_mappings_share_folders_and_keep_their_markers() {
    for (nested, opm_first) in [(false, false), (false, true), (true, false)] {
        let h = harness().await;
        let immich_dir = if nested {
            h.root.join("2024")
        } else {
            h.root.clone()
        };
        let immich = json!({
            "root_id": "immich", "name": "Immich", "container_path": immich_dir,
            "immich_import_path": "/external/photos"
        });
        let opm = json!({
            "root_id": "opm", "name": "OPM", "container_path": h.root,
            "opm_prefix": "/opm/photos"
        });
        let inputs = if opm_first {
            [&opm, &immich]
        } else {
            [&immich, &opm]
        };
        for input in inputs {
            let response = send(
                &h.app,
                json_req("POST", "/api/admin/mappings", &h.admin(), input),
            )
            .await;
            assert_eq!(
                response.status,
                StatusCode::CREATED,
                "{:?}",
                response.json()
            );
            assert_eq!(response.json()["available"], true);
        }
        let response = send(
            &h.app,
            json_req("PUT", "/api/admin/mappings/opm", &h.admin(), &opm),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{:?}", response.json());
        assert_eq!(response.json()["available"], true);
        let roots = crate::files::load_roots(&h.state.db).await.unwrap();
        assert!(roots.iter().all(|root| root.available()));
        let removed = if opm_first { "opm" } else { "immich" };
        let remaining = if opm_first { "immich" } else { "opm" };
        let response = send(
            &h.app,
            json_req(
                "DELETE",
                &format!("/api/admin/mappings/{removed}"),
                &h.admin(),
                &Value::Null,
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::NO_CONTENT,
            "{:?}",
            response.json()
        );
        assert!(
            crate::files::root(&h.state.db, remaining)
                .await
                .unwrap()
                .available()
        );
    }
}

#[tokio::test]
async fn mapping_rejects_claimed_folder() {
    let h = harness().await;
    std::fs::write(h.root.join(".ism-root"), "other\n").unwrap();
    let body = json!({ "root_id": "photos", "name": "Photos", "container_path": h.root.to_string_lossy() });
    let r = send(
        &h.app,
        json_req("POST", "/api/admin/mappings", &h.admin(), &body),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["error"]["code"], "folder_claimed");
}

#[tokio::test]
async fn opm_alias_cannot_bypass_immich_for_nested_file_changes() {
    let h = harness().await;
    for input in [
        json!({
            "root_id": "immich", "name": "Immich", "container_path": h.root.join("2024"),
            "immich_import_path": "/external/photos"
        }),
        json!({
            "root_id": "opm", "name": "OPM", "container_path": h.root,
            "opm_prefix": "/opm/photos"
        }),
    ] {
        let response = send(
            &h.app,
            json_req("POST", "/api/admin/mappings", &h.admin(), &input),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::CREATED,
            "{:?}",
            response.json()
        );
    }
    sqlx::query("INSERT INTO immich_servers (id, name, url, maintenance_json, created_at) VALUES ('server', 'Example', 'https://immich.example', ?, ?)")
        .bind(serde_json::to_string(&ism_core::maintenance::MaintenanceState::default()).unwrap())
        .bind(crate::db::now())
        .execute(&h.state.db).await.unwrap();
    sqlx::query("UPDATE mappings SET server_id = 'server' WHERE root_id = 'immich'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let request = serde_json::from_value(json!({
        "idempotency_key": "opm-alias",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "opm", "path": "2024/new" } }]
    }))
    .unwrap();
    let principal = crate::auth::Principal::Admin {
        username: "admin".into(),
    };
    let response = crate::ops::preview(&h.state, &principal, &request)
        .await
        .unwrap();
    assert!(!response.ok);
    assert_eq!(response.blockers[0].code, "immich_mapping_required");
    assert!(!h.root.join("2024/new").exists());
    let outside = serde_json::from_value(json!({
        "idempotency_key": "outside-import",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "opm", "path": "2025" } }]
    }))
    .unwrap();
    let response = crate::ops::preview(&h.state, &principal, &outside)
        .await
        .unwrap();
    assert!(response.ok, "{:?}", response.blockers);
}

#[tokio::test]
async fn mapping_does_not_overwrite_an_empty_existing_marker() {
    let h = harness().await;
    std::fs::write(h.root.join(".ism-root"), "").unwrap();
    let body = json!({ "root_id": "photos", "name": "Photos", "container_path": h.root.to_string_lossy() });
    let response = send(
        &h.app,
        json_req("POST", "/api/admin/mappings", &h.admin(), &body),
    )
    .await;
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(
        std::fs::read_to_string(h.root.join(".ism-root")).unwrap(),
        ""
    );
}

#[tokio::test]
async fn wrong_volume_marker_blocks_reads_and_managed_operations() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join(".ism-root"), "other\n").unwrap();
    let bearer = h.token(&["read", "manage"]).await;
    let auth = [("authorization", bearer.as_str())];
    let info = send(&h.app, get("/api/v1/client/info", &auth)).await;
    assert_eq!(info.status, StatusCode::OK);
    assert_eq!(info.json()["roots"][0]["available"], false);
    assert_eq!(info.json()["roots"][0]["direct_writes"], "not_linked");
    assert_eq!(info.json()["roots"][0]["direct_writes_reason"], Value::Null);
    let browse = send(
        &h.app,
        get("/api/v1/folders?root_id=photos&path=2024", &auth),
    )
    .await;
    assert_eq!(browse.status, StatusCode::SERVICE_UNAVAILABLE);
    let request = json!({
        "idempotency_key": "wrong-volume",
        "actions": [{ "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/new" } }]
    });
    let operation = send(
        &h.app,
        json_req("POST", "/api/v1/operations", &auth, &request),
    )
    .await;
    assert_eq!(operation.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!h.root.join("2024/new").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn mapping_rejects_symlinked_volume_marker() {
    let h = harness().await;
    let target = h._dir.path().join("marker");
    std::fs::write(&target, "photos\n").unwrap();
    std::os::unix::fs::symlink(&target, h.root.join(".ism-root")).unwrap();
    let body = json!({ "root_id": "photos", "name": "Photos", "container_path": h.root.to_string_lossy() });
    let response = send(
        &h.app,
        json_req("POST", "/api/admin/mappings", &h.admin(), &body),
    )
    .await;
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(std::fs::read_to_string(target).unwrap(), "photos\n");
}

/// Creates, fills and commits an upload; returns the finished operation (or the error response).
async fn run_upload(h: &Harness, bearer: &str, body: &Value, contents: &[&[u8]]) -> Value {
    let auth = [("authorization", bearer)];
    let r = send(&h.app, json_req("POST", "/api/v1/uploads", &auth, body)).await;
    if r.status != StatusCode::CREATED {
        return json!({ "http": r.status.as_u16(), "body": r.json() });
    }
    let id = r.json()["upload_id"].as_str().unwrap().to_owned();
    for (index, bytes) in contents.iter().enumerate() {
        let request = Request::builder()
            .method("PUT")
            .uri(format!("/api/v1/uploads/{id}/files/{index}"))
            .header("authorization", bearer)
            .body(Body::from(bytes.to_vec()))
            .unwrap();
        let r = send(&h.app, request).await;
        assert!(r.status.is_success(), "{:?}", r.json());
    }
    let r = send(
        &h.app,
        json_req(
            "POST",
            &format!("/api/v1/uploads/{id}/commit"),
            &auth,
            &json!({}),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    h.wait_finished(bearer, &op).await
}

#[tokio::test]
async fn sidecar_only_upload_replaces_just_the_sidecar() {
    let sha = |b: &[u8]| crate::crypto::sha256_hex(b);
    for scenario in [
        "replace",
        "new_sidecar",
        "original_changed",
        "unexpected_sidecar",
    ] {
        let h = harness().await;
        h.map_root().await;
        let original = h.root.join("2024/a.jpg");
        let sidecar = h.root.join("2024/a.jpg.xmp");
        std::fs::write(&original, b"original").unwrap();
        if scenario != "new_sidecar" {
            std::fs::write(&sidecar, b"old-xmp").unwrap();
        }
        let revision = |p: &std::path::Path| crate::files::revision(&std::fs::metadata(p).unwrap());
        let original_revision = revision(&original);
        let sidecar_revision =
            (scenario == "replace" || scenario == "original_changed").then(|| revision(&sidecar));
        if scenario == "original_changed" {
            std::fs::write(&original, b"edited-on-server").unwrap();
        }
        let bearer = h.token(&["read", "upload"]).await;
        let body = json!({
            "target": { "root_id": "photos", "path": "2024" },
            "existing_original": { "name": "a.jpg", "expected_revision": original_revision },
            "files": [
                { "name": "a.jpg.xmp", "role": "sidecar", "size": 7, "sha256": sha(b"new-xmp"),
                  "expected_revision": sidecar_revision,
                  "expected_sha256": sidecar_revision.as_ref().map(|_| sha(b"old-xmp")) }
            ],
            "on_conflict": "replace_if_expected"
        });
        let done = run_upload(&h, &bearer, &body, &[b"new-xmp"]).await;
        let trashed: Vec<(String, String)> =
            sqlx::query_as("SELECT path, file_inventory_json FROM trash")
                .fetch_all(&h.state.db)
                .await
                .unwrap();
        match scenario {
            "replace" | "new_sidecar" => {
                assert_eq!(done["status"], "completed", "{scenario}: {done}");
                assert_eq!(std::fs::read(&sidecar).unwrap(), b"new-xmp");
                assert_eq!(std::fs::read(&original).unwrap(), b"original");
                if scenario == "replace" {
                    // Only the replaced sidecar is kept in trash, never a copy of the original.
                    assert_eq!(trashed.len(), 1, "{scenario}");
                    assert_eq!(trashed[0].0, "2024/a.jpg.xmp");
                    let files: Vec<Value> = serde_json::from_str(&trashed[0].1).unwrap();
                    assert_eq!(files.len(), 1);
                    assert_eq!(files[0]["name"], "a.jpg.xmp");
                    assert_eq!(files[0]["sha256"], sha(b"old-xmp"));
                } else {
                    assert!(trashed.is_empty(), "{scenario}");
                }
            }
            _ => {
                assert_eq!(done["status"], "failed", "{scenario}: {done}");
                assert_eq!(
                    done["items"][0]["error"]["code"], "revision_changed",
                    "{scenario}"
                );
                assert_eq!(std::fs::read(&sidecar).unwrap(), b"old-xmp");
                assert!(trashed.is_empty(), "{scenario}");
            }
        }
    }
}

#[tokio::test]
async fn sidecar_only_and_provenance_uploads_are_validated() {
    let h = harness().await;
    h.map_root().await;
    std::fs::write(h.root.join("2024/a.jpg"), b"original").unwrap();
    let revision = crate::files::revision(&std::fs::metadata(h.root.join("2024/a.jpg")).unwrap());
    let bearer = h.token(&["read", "upload"]).await;
    let sha = crate::crypto::sha256_hex(b"xmp");
    let existing = json!({ "name": "a.jpg", "expected_revision": revision });
    let sidecar = json!({ "name": "a.jpg.xmp", "role": "sidecar", "size": 3, "sha256": sha });
    let original = json!({ "name": "a.jpg", "role": "original", "size": 3, "sha256": sha });
    let rejected = [
        json!({ "target": { "root_id": "photos", "path": "2024" }, "existing_original": existing,
                "files": [sidecar], "on_conflict": "fail" }),
        json!({ "target": { "root_id": "photos", "path": "2024" }, "existing_original": existing,
                "files": [original, sidecar], "on_conflict": "replace_if_expected" }),
        json!({ "target": { "root_id": "photos", "path": "2024" },
                "files": [sidecar], "on_conflict": "fail" }),
        json!({ "target": { "root_id": "photos", "path": "2025" },
                "provenance": { "source": "ila", "original_location": { "root_id": "photos", "path": "2024/a.jpg" } },
                "files": [original], "on_conflict": "fail" }),
    ];
    for body in rejected {
        let done = run_upload(&h, &bearer, &body, &[]).await;
        assert_eq!(done["http"], 400, "{body}: {done}");
    }
    // A file that does not belong to the original is refused before anything moves.
    let stranger = json!({
        "target": { "root_id": "photos", "path": "2024" }, "existing_original": existing,
        "files": [{ "name": "b.jpg.xmp", "role": "sidecar", "size": 3, "sha256": sha }],
        "on_conflict": "replace_if_expected"
    });
    let done = run_upload(&h, &bearer, &stranger, &[b"xmp"]).await;
    assert_eq!(done["status"], "failed", "{done}");
    assert_eq!(done["items"][0]["error"]["code"], "not_a_companion");
    assert!(!h.root.join("2024/b.jpg.xmp").exists());
}

#[tokio::test]
async fn an_upload_gets_its_asset_id_after_the_scoped_scan_fallback() {
    let h = harness().await;
    h.map_root().await;
    let (server, fake) = crate::maintenance::tests::fake_server(&h.state).await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/api/libraries/lib1/scan"))
        .and(wiremock::matchers::body_json(json!({
            "path": "/library/photos/2024/Trip"
        })))
        .respond_with(wiremock::ResponseTemplate::new(204))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    sqlx::query("INSERT INTO immich_libraries (id, server_id, name, owner_user_id, import_paths_json, refreshed_at) VALUES ('lib1', 's1', 'Photos', 'u1', '[\"/library/photos\"]', ?)")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    // Exercise scan-based publication with the fork's scoped-scan capability verified.
    sqlx::query("UPDATE immich_servers SET direct_writes_enabled = 1, database_url_enc = 'unused', external_library_move_mode = 'db_only', external_library_api_capable = 1 WHERE id = 's1'")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE mappings SET server_id = 's1', library_id = 'lib1', immich_import_path = '/library/photos' WHERE root_id = 'photos'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let bearer = h.token(&["read", "upload"]).await;
    let photo: &[u8] = b"photo";
    let body = json!({
        "target": { "root_id": "photos", "path": "2024/Trip" },
        "provenance": { "source": "ila", "original_location": { "root_id": "photos", "path": "2024/Trip/c.jpg" } },
        "files": [{ "name": "c.jpg", "role": "original", "size": 5, "sha256": crate::crypto::sha256_hex(photo) }],
        "on_conflict": "fail"
    });
    let done = run_upload(&h, &bearer, &body, &[photo]).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["items"][0]["result"]["asset_id"], Value::Null);
    assert!(h.root.join("2024/Trip/c.jpg").exists());
    let due: Option<i64> =
        sqlx::query_scalar("SELECT due_at FROM scan_schedule WHERE library_id = 'lib1'")
            .fetch_optional(&h.state.db)
            .await
            .unwrap()
            .flatten();
    assert!(due.is_some(), "the upload schedules a scoped fallback");

    // Not imported yet: nothing to fill.
    let filled = crate::ops::uploads::fill_asset_ids(&h.state, "s1", "lib1")
        .await
        .unwrap();
    assert_eq!(filled, 0);
    let operation_id = done["operation_id"].as_str().unwrap();
    let auth = [("authorization", bearer.as_str())];
    // A live listener stays connected past `finished` until the asset id arrives.
    let listener = {
        let app = h.app.clone();
        let req = get(&format!("/api/v1/operations/{operation_id}/events"), &auth);
        tokio::spawn(async move { send(&app, req).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!listener.is_finished(), "the stream waits for the import");
    fake.assets
        .lock()
        .unwrap()
        .push(("/library/photos/2024/Trip/c.jpg".into(), "asset-1".into()));
    let mut events = h.state.events.subscribe();
    crate::watch::scheduler::step(&h.state, i64::MAX / 2)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(events.recv().await.unwrap(),
                crate::state::ServerEvent::OperationUpdated { operation_id: ref id } if id == operation_id)
            {
                break;
            }
        }
    }).await.expect("the scheduler fills imported asset IDs and notifies SSE subscribers");
    let streamed = tokio::time::timeout(Duration::from_secs(20), listener)
        .await
        .expect("the stream ends once the asset id is known")
        .unwrap();
    let streamed = String::from_utf8_lossy(&streamed.body).into_owned();
    let tail = streamed.split("event: finished").nth(1).unwrap_or_default();
    assert!(
        tail.contains("event: item") && tail.contains("asset-1"),
        "{streamed}"
    );
    let r = send(
        &h.app,
        get(&format!("/api/v1/operations/{operation_id}"), &auth),
    )
    .await;
    assert_eq!(
        r.json()["items"][0]["result"]["asset_id"],
        "asset-1",
        "{:?}",
        r.json()
    );
    let again = crate::ops::uploads::fill_asset_ids(&h.state, "s1", "lib1")
        .await
        .unwrap();
    assert_eq!(again, 0);
}

#[tokio::test]
async fn an_upload_immich_cannot_import_is_not_announced_to_it() {
    let h = harness().await;
    h.map_root().await;
    let (server, _fake) = crate::maintenance::tests::fake_server(&h.state).await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/api/libraries/lib1/files/add"))
        .respond_with(wiremock::ResponseTemplate::new(400).set_body_json(json!({
            "message": "Added file is not a supported image or video",
            "error": "Bad Request",
            "statusCode": 400
        })))
        .with_priority(1)
        .expect(0)
        .mount(&server)
        .await;
    sqlx::query("INSERT INTO immich_libraries (id, server_id, name, owner_user_id, import_paths_json, refreshed_at) VALUES ('lib1', 's1', 'Photos', 'u1', '[\"/library/photos\"]', ?)")
        .bind(crate::db::now())
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE immich_servers SET direct_writes_enabled = 1, database_url_enc = 'unused', external_library_move_mode = 'api_only', external_library_api_capable = 1 WHERE id = 's1'")
        .execute(&h.state.db)
        .await
        .unwrap();
    sqlx::query("UPDATE mappings SET server_id = 's1', library_id = 'lib1', immich_import_path = '/library/photos' WHERE root_id = 'photos'")
        .execute(&h.state.db)
        .await
        .unwrap();
    let bearer = h.token(&["read", "upload"]).await;
    let backup: &[u8] = b"backup";
    let name = "example-photo.JPG.myliobak";
    let body = json!({
        "target": { "root_id": "photos", "path": "2024" },
        "files": [{ "name": name, "role": "original", "size": 6, "sha256": crate::crypto::sha256_hex(backup) }],
        "on_conflict": "fail"
    });
    let done = run_upload(&h, &bearer, &body, &[backup]).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert!(h.root.join("2024").join(name).exists());
}

#[tokio::test]
async fn opm_trash_expectations_partial_submit_and_lookup_by_key() {
    let h = harness().await;
    h.map_root().await;
    std::fs::create_dir_all(h.root.join("2024/album/sub")).unwrap();
    std::fs::write(h.root.join("2024/a.jpg"), b"photo a").unwrap();
    std::fs::write(h.root.join("2024/album/b.jpg"), b"photo b").unwrap();
    std::fs::write(h.root.join("2024/album/sub/c.jpg"), b"photo c").unwrap();
    let bearer = h.token(&["read", "manage", "trash"]).await;
    let auth = [("authorization", bearer.as_str())];
    let listing = |path: &str| {
        let uri = format!("/api/v1/folders?root_id=photos&path={path}");
        let app = h.app.clone();
        async move { send(&app, get(&uri, &auth)).await.json() }
    };
    let revision_of = |listing: &Value, name: &str| -> String {
        listing["files"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == name)
            .unwrap()["revision"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let submit = |body: Value| {
        let app = h.app.clone();
        async move { send(&app, json_req("POST", "/api/v1/operations", &auth, &body)).await }
    };

    // Lookup by key has no side effects: unknown keys stay usable.
    let r = send(
        &h.app,
        get("/api/v1/operations?idempotency_key=trash-a", &auth),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.json()["error"]["code"], "not_found");

    // A stale file revision is refused and nothing moves.
    let year = listing("2024").await;
    let a_rev = revision_of(&year, "a.jpg");
    std::thread::sleep(Duration::from_millis(5));
    std::fs::write(h.root.join("2024/a.jpg"), b"photo a, edited").unwrap();
    let r = submit(json!({ "idempotency_key": "trash-a", "actions": [
        { "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.jpg" }, "expected_revision": a_rev }
    ]}))
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["error"]["code"], "revision_changed");
    assert!(h.root.join("2024/a.jpg").exists());

    // A folder whose contents differ from the expected entries is not touched.
    let album = listing("2024/album").await;
    let sub = listing("2024/album/sub").await;
    let mut entries = vec![
        json!({ "path": "b.jpg", "revision": revision_of(&album, "b.jpg") }),
        json!({ "path": "sub/c.jpg", "revision": revision_of(&sub, "c.jpg") }),
    ];
    std::fs::write(h.root.join("2024/album/new.jpg"), b"added later").unwrap();
    let trash_album = |entries: &Vec<Value>, key: &str| {
        json!({ "idempotency_key": key, "actions": [
            { "kind": "trash_folder", "source": { "root_id": "photos", "path": "2024/album" },
              "expected_entries": entries }
        ]})
    };
    let r = submit(trash_album(&entries, "album-1")).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["error"]["code"], "revision_changed");
    assert!(
        r.json()["error"]["message"]
            .as_str()
            .unwrap()
            .contains("1 added")
    );
    assert!(h.root.join("2024/album/new.jpg").exists());

    // Partial submit: the blocked trash fails as an item, the rest runs.
    let r = submit(json!({ "idempotency_key": "partial", "allow_partial": true, "actions": [
        { "kind": "trash", "source": { "root_id": "photos", "path": "2024/a.jpg" }, "expected_revision": a_rev },
        { "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/made" } }
    ]}))
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["items"][0]["status"], "failed", "{done}");
    assert_eq!(done["items"][0]["error"]["code"], "revision_changed");
    assert_eq!(done["items"][1]["status"], "done", "{done}");
    assert!(h.root.join("2024/made").is_dir() && h.root.join("2024/a.jpg").exists());

    // The same submission can be found by its key, without being re-run.
    let r = send(
        &h.app,
        get("/api/v1/operations?idempotency_key=partial", &auth),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["operation_id"], op.as_str());
    assert_eq!(r.json()["status"], done["status"]);

    // With matching entries the folder goes to the trash.
    entries.push(json!({ "path": "new.jpg", "revision": revision_of(&listing("2024/album").await, "new.jpg") }));
    let r = submit(trash_album(&entries, "album-2")).await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{:?}", r.json());
    let op = r.json()["operation_id"].as_str().unwrap().to_owned();
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert!(!h.root.join("2024/album").exists());

    // A key cancelled before submission is reported as such.
    let cancel = json!({ "idempotency_key": "never", "expected_request": {
    "idempotency_key": "never", "actions": [
        { "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/never" } }
    ]}});
    let r = send(
        &h.app,
        json_req("POST", "/api/v1/operations/cancel-by-key", &auth, &cancel),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let r = send(
        &h.app,
        get("/api/v1/operations?idempotency_key=never", &auth),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.json()["error"]["code"], "cancelled_before_submission");
}
