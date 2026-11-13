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
        let body =
            json!({ "name": "client", "kind": "ila", "roots": ["photos"], "actions": actions });
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
    assert_eq!(
        r.headers["x-ism-sha256"].to_str().unwrap(),
        original["sha256"].as_str().unwrap()
    );

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
async fn upload_keeps_the_folder() {
    let h = harness().await;
    h.map_root().await;
    let bearer = h.token(&["read", "upload"]).await;
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
    let done = h.wait_finished(&bearer, &op).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(std::fs::read(h.root.join("2024/New/c.jpg")).unwrap(), photo);
    assert_eq!(
        std::fs::read(h.root.join("2024/New/c.jpg.xmp")).unwrap(),
        xmp
    );
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
