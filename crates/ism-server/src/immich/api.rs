//! Immich HTTP API client. Every call authenticates with one account's API key, so each change
//! is performed as the user who owns the asset.

use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Immich is unreachable: {0}")]
    Network(String),
    #[error("Immich answered {status}: {message}")]
    Status { status: u16, message: String },
    #[error("unexpected Immich response: {0}")]
    Decode(String),
}

impl ApiError {
    /// Route-level absence, distinct from a real `404` returned by a known library.
    pub fn is_external_library_api_unavailable(&self) -> bool {
        match self {
            Self::Status {
                status: 405 | 501, ..
            } => true,
            Self::Status {
                status: 404,
                message,
            } => message.contains("Cannot POST"),
            _ => false,
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(e: reqwest::Error) -> Self {
        Self::Network(e.without_url().to_string())
    }
}

/// Permissions requested for the API key of an administrator account.
pub const ADMIN_PERMISSIONS: &[&str] = &[
    "asset.read",
    "asset.update",
    "asset.delete",
    "asset.view",
    "asset.download",
    "library.read",
    "library.update",
    "library.statistics",
    "adminConfig.read",
    "adminConfig.update",
    "queue.read",
    "queue.update",
    "server.about",
    "user.read",
];

/// Permissions requested for the API key of a regular account.
pub const USER_PERMISSIONS: &[&str] = &[
    "asset.read",
    "asset.update",
    "asset.delete",
    "asset.view",
    "asset.download",
    "user.read",
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub access_token: String,
    pub user_id: String,
    pub is_admin: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Library {
    pub id: String,
    pub name: String,
    pub owner_id: String,
    pub import_paths: Vec<String>,
    #[serde(default)]
    pub exclusion_patterns: Vec<String>,
    #[serde(default)]
    pub asset_count: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueStatistics {
    #[serde(default)]
    pub active: i64,
    #[serde(default)]
    pub waiting: i64,
    #[serde(default)]
    pub delayed: i64,
    #[serde(default)]
    pub paused: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Queue {
    pub name: String,
    #[serde(default)]
    pub is_paused: bool,
    #[serde(default)]
    pub is_running: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
    pub statistics: QueueStatistics,
}

impl Queue {
    /// Explicit Immich status when available, otherwise derives running from active jobs.
    pub fn is_running(&self) -> bool {
        match self.status.as_deref() {
            Some("running") => true,
            Some("paused" | "idle") => false,
            _ => self.is_running.unwrap_or(self.statistics.active > 0),
        }
    }

    /// Nothing running and nothing pending: work held by a pause counts as pending.
    pub fn is_idle(&self) -> bool {
        match self.status.as_deref() {
            Some("idle") => return true,
            Some("running" | "paused") => return false,
            _ => {}
        }
        let s = &self.statistics;
        !self.is_running()
            && s.active == 0
            && s.waiting == 0
            && s.delayed == 0
            && s.paused == 0
            && !self.is_paused
    }

    /// Paused with no job running. On Immich v3.4.1 jobs queued while the queue is paused stay
    /// in `waiting` (or `delayed`) and do not start until it is resumed, so they are harmless.
    pub fn is_held(&self) -> bool {
        match self.status.as_deref() {
            Some("paused") => self.is_paused,
            _ => self.is_paused && !self.is_running(),
        }
    }
}

#[derive(Clone)]
pub struct Api {
    http: reqwest::Client,
    base: String,
    api_key: String,
}

pub fn base_url(url: &str) -> String {
    url.trim()
        .trim_end_matches('/')
        .trim_end_matches("/api")
        .to_owned()
}

async fn check(resp: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    let message = match &body["message"] {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("; "),
        _ => StatusCode::from_u16(status)
            .map(|s| s.to_string())
            .unwrap_or_default(),
    };
    Err(ApiError::Status { status, message })
}

/// Unauthenticated: server version as `major.minor.patch`.
pub async fn server_version(http: &reqwest::Client, url: &str) -> Result<String, ApiError> {
    let resp = check(
        http.get(format!("{}/api/server/version", base_url(url)))
            .send()
            .await?,
    )
    .await?;
    let v: Value = resp.json().await?;
    match (
        v["major"].as_i64(),
        v["minor"].as_i64(),
        v["patch"].as_i64(),
    ) {
        (Some(a), Some(b), Some(c)) => Ok(format!("{a}.{b}.{c}")),
        _ => Err(ApiError::Decode("server version".into())),
    }
}

/// Logs in with email/password, creates a scoped API key and logs the session out again.
/// The password is not kept. Returns `(user_id, is_admin, api_key_id, api_key_secret)`.
pub async fn provision_api_key(
    http: &reqwest::Client,
    url: &str,
    email: &str,
    password: &str,
    key_name: &str,
) -> Result<(String, bool, String, String), ApiError> {
    let base = base_url(url);
    let login: LoginResponse = check(
        http.post(format!("{base}/api/auth/login"))
            .json(&json!({ "email": email, "password": password }))
            .send()
            .await?,
    )
    .await?
    .json()
    .await?;
    let permissions = if login.is_admin {
        ADMIN_PERMISSIONS
    } else {
        USER_PERMISSIONS
    };
    let created: Value = check(
        http.post(format!("{base}/api/api-keys"))
            .bearer_auth(&login.access_token)
            .json(&json!({ "name": key_name, "permissions": permissions }))
            .send()
            .await?,
    )
    .await?
    .json()
    .await?;
    let _ = http
        .post(format!("{base}/api/auth/logout"))
        .bearer_auth(&login.access_token)
        .send()
        .await;
    let secret = created["secret"]
        .as_str()
        .ok_or_else(|| ApiError::Decode("api key secret".into()))?;
    let key_id = created["apiKey"]["id"].as_str().unwrap_or_default();
    Ok((
        login.user_id,
        login.is_admin,
        key_id.to_owned(),
        secret.to_owned(),
    ))
}

impl Api {
    pub fn new(http: reqwest::Client, url: &str, api_key: String) -> Self {
        Self {
            http,
            base: base_url(url),
            api_key,
        }
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<reqwest::Response, ApiError> {
        let mut req = self
            .http
            .request(method, format!("{}/api{path}", self.base))
            .header("x-api-key", &self.api_key);
        if let Some(body) = body {
            req = req.json(&body);
        }
        check(req.send().await?).await
    }

    async fn json<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<T, ApiError> {
        self.call(method, path, body)
            .await?
            .json()
            .await
            .map_err(|e| ApiError::Decode(e.without_url().to_string()))
    }

    pub async fn get_config(&self) -> Result<Value, ApiError> {
        self.json(Method::GET, "/admin/config", None).await
    }

    pub async fn put_config(&self, config: &Value) -> Result<Value, ApiError> {
        self.json(Method::PUT, "/admin/config", Some(config.clone()))
            .await
    }

    pub async fn queues(&self) -> Result<Vec<Queue>, ApiError> {
        self.json(Method::GET, "/queues", None).await
    }

    pub async fn set_queue_paused(&self, name: &str, paused: bool) -> Result<(), ApiError> {
        self.call(
            Method::PUT,
            &format!("/queues/{name}"),
            Some(json!({ "isPaused": paused })),
        )
        .await
        .map(|_| ())
    }

    pub async fn libraries(&self) -> Result<Vec<Library>, ApiError> {
        self.json(Method::GET, "/libraries", None).await
    }

    pub async fn set_exclusion_patterns(
        &self,
        library_id: &str,
        patterns: &[String],
    ) -> Result<(), ApiError> {
        self.call(
            Method::PUT,
            &format!("/libraries/{library_id}"),
            Some(json!({ "exclusionPatterns": patterns })),
        )
        .await
        .map(|_| ())
    }

    /// Queues a scan only for a folder inside the external library.
    pub async fn scan_library_folder(&self, library_id: &str, path: &str) -> Result<(), ApiError> {
        self.call(
            Method::POST,
            &format!("/libraries/{library_id}/scan"),
            Some(json!({ "path": path })),
        )
        .await
        .map(|_| ())
    }

    /// Synchronizes an external library after ISM or OPM has created a file.
    pub async fn sync_library_file_add(
        &self,
        library_id: &str,
        path: &str,
    ) -> Result<(), ApiError> {
        self.call(
            Method::POST,
            &format!("/libraries/{library_id}/files/add"),
            Some(json!({ "path": path })),
        )
        .await
        .map(|_| ())
    }

    /// Synchronizes an external library after ISM or OPM has removed a file.
    pub async fn sync_library_file_remove(
        &self,
        library_id: &str,
        path: &str,
    ) -> Result<(), ApiError> {
        self.call(
            Method::POST,
            &format!("/libraries/{library_id}/files/remove"),
            Some(json!({ "path": path })),
        )
        .await
        .map(|_| ())
    }

    /// Synchronizes an external library after ISM or OPM has moved a file.
    pub async fn sync_library_file_move(
        &self,
        library_id: &str,
        source_path: &str,
        destination_path: &str,
        destination_library_id: Option<&str>,
        asset_id: Option<&str>,
    ) -> Result<(), ApiError> {
        let mut body = json!({
            "sourcePath": source_path,
            "destinationPath": destination_path,
        });
        if let Some(destination_library_id) = destination_library_id {
            body["destinationLibraryId"] = json!(destination_library_id);
        }
        if let Some(asset_id) = asset_id {
            body["assetId"] = json!(asset_id);
        }
        self.call(
            Method::POST,
            &format!("/libraries/{library_id}/files/move"),
            Some(body),
        )
        .await
        .map(|_| ())
    }

    /// Mutation-free route probe: a matching route rejects the invalid UUID with 400.
    pub async fn external_library_api_capable(&self) -> Result<bool, ApiError> {
        match self
            .call(
                Method::POST,
                "/libraries/invalid/files/move",
                Some(json!({
                    "sourcePath": "/__ism_capability_probe__",
                    "destinationPath": "/__ism_capability_probe__"
                })),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(ApiError::Status { status: 400, .. }) => Ok(true),
            Err(error) if error.is_external_library_api_unavailable() => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Moves assets to Immich's trash (`force = false`) or deletes them (`force = true`).
    pub async fn delete_assets(&self, ids: &[String], force: bool) -> Result<(), ApiError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.call(
            Method::DELETE,
            "/assets",
            Some(json!({ "ids": ids, "force": force })),
        )
        .await
        .map(|_| ())
    }

    pub async fn restore_assets(&self, ids: &[String]) -> Result<(), ApiError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.call(
            Method::POST,
            "/trash/restore/assets",
            Some(json!({ "ids": ids })),
        )
        .await
        .map(|_| ())
    }

    pub async fn asset(&self, id: &str) -> Result<Value, ApiError> {
        self.json(Method::GET, &format!("/assets/{id}"), None).await
    }

    /// The id of this account's asset at exactly `original_path`, if Immich imported it.
    pub async fn asset_id_by_path(&self, original_path: &str) -> Result<Option<String>, ApiError> {
        let found: Value = self
            .json(
                Method::POST,
                "/search/metadata",
                Some(json!({ "originalPath": original_path, "size": 10 })),
            )
            .await?;
        Ok(found["assets"]["items"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|a| a["originalPath"].as_str() == Some(original_path))
            .and_then(|a| a["id"].as_str())
            .map(ToOwned::to_owned))
    }
}

/// Reads a boolean at a dotted path of the config document.
pub fn config_bool(config: &Value, path: &[&str]) -> Option<bool> {
    path.iter()
        .try_fold(config, |v, key| v.get(*key))?
        .as_bool()
}

pub fn set_config_bool(config: &mut Value, path: &[&str], value: bool) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };
    let mut cursor = config;
    for key in parents {
        match cursor.get_mut(*key) {
            Some(next) => cursor = next,
            None => return false,
        }
    }
    match cursor.as_object_mut() {
        Some(map) => {
            map.insert((*last).to_owned(), Value::Bool(value));
            true
        }
        None => false,
    }
}

pub const WATCH_ENABLED: &[&str] = &["library", "watch", "enabled"];
pub const SCAN_ENABLED: &[&str] = &["library", "scan", "enabled"];

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path},
    };

    #[test]
    fn edits_nested_config_flags() {
        let mut cfg = json!({ "library": { "watch": { "enabled": true }, "scan": { "enabled": true, "cronExpression": "0 0 * * *" } } });
        assert_eq!(config_bool(&cfg, WATCH_ENABLED), Some(true));
        assert!(set_config_bool(&mut cfg, WATCH_ENABLED, false));
        assert_eq!(config_bool(&cfg, WATCH_ENABLED), Some(false));
        assert_eq!(cfg["library"]["scan"]["cronExpression"], "0 0 * * *");
        assert!(!set_config_bool(&mut cfg, &["nope", "x"], false));
    }

    #[test]
    fn normalises_base_urls() {
        assert_eq!(
            base_url("http://immich.example:2283/api/"),
            "http://immich.example:2283"
        );
    }

    #[test]
    fn decodes_explicit_and_fallback_queue_status() {
        let running: Queue = serde_json::from_value(json!({
            "name": "library",
            "isPaused": false,
            "isRunning": true,
            "status": "running",
            "statistics": { "active": 1, "waiting": 0, "delayed": 0, "paused": 0 }
        }))
        .unwrap();
        assert!(running.is_running());
        assert!(!running.is_idle());

        let paused: Queue = serde_json::from_value(json!({
            "name": "library",
            "isPaused": true,
            "isRunning": false,
            "status": "paused",
            "statistics": { "active": 0, "waiting": 2, "delayed": 0, "paused": 0 }
        }))
        .unwrap();
        assert!(!paused.is_running());
        assert!(!paused.is_idle());
        assert!(paused.is_held());

        let legacy: Queue = serde_json::from_value(json!({
            "name": "library",
            "isPaused": false,
            "statistics": { "active": 0, "waiting": 0, "delayed": 0, "paused": 0 }
        }))
        .unwrap();
        assert!(legacy.is_idle());
        assert!(!legacy.is_held());
    }

    #[test]
    fn distinguishes_missing_route_from_known_library_not_found() {
        assert!(
            ApiError::Status {
                status: 404,
                message: "Cannot POST /api/libraries/invalid/files/move".to_owned(),
            }
            .is_external_library_api_unavailable()
        );
        assert!(
            !ApiError::Status {
                status: 404,
                message: "Library not found".to_owned(),
            }
            .is_external_library_api_unavailable()
        );
        assert!(
            !ApiError::Status {
                status: 409,
                message: "Destination already has an asset".to_owned(),
            }
            .is_external_library_api_unavailable()
        );
    }

    #[tokio::test]
    async fn sends_folder_scan_contract() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/libraries/lib-1/scan"))
            .and(header("x-api-key", "test-key"))
            .and(body_json(json!({ "path": "/mnt/library/photos" })))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let api = Api::new(reqwest::Client::new(), &server.uri(), "test-key".to_owned());
        api.scan_library_folder("lib-1", "/mnt/library/photos")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn sends_external_library_file_sync_contracts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/libraries/lib-1/files/add"))
            .and(header("x-api-key", "test-key"))
            .and(body_json(json!({ "path": "/mnt/library/photo.jpg" })))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/libraries/lib-1/files/remove"))
            .and(header("x-api-key", "test-key"))
            .and(body_json(json!({ "path": "/mnt/library/photo.jpg" })))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/libraries/lib-1/files/move"))
            .and(header("x-api-key", "test-key"))
            .and(body_json(json!({
                "sourcePath": "/mnt/library/a.jpg",
                "destinationPath": "/mnt/library/b.jpg",
                "destinationLibraryId": "lib-2",
                "assetId": "asset-1"
            })))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let api = Api::new(reqwest::Client::new(), &server.uri(), "test-key".to_owned());
        api.sync_library_file_add("lib-1", "/mnt/library/photo.jpg")
            .await
            .unwrap();
        api.sync_library_file_remove("lib-1", "/mnt/library/photo.jpg")
            .await
            .unwrap();
        api.sync_library_file_move(
            "lib-1",
            "/mnt/library/a.jpg",
            "/mnt/library/b.jpg",
            Some("lib-2"),
            Some("asset-1"),
        )
        .await
        .unwrap();
    }
}
