//! Administration API for the web UI (`/api/admin`). Cookie session + `x-ism-request: 1`.

use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use futures_util::stream;
use ism_core::dto::{
    AccountInput, AccountView, AuditEntry, ClientAction, ClientTokenCreated, ClientTokenInput,
    ClientTokenView, ImmichServerInput, ImmichServerView, LibraryView, LoginRequest, MappingInput,
    MappingView, MountView, OverrideInput, Overview, PeerAcceptInput, ResolveConflictInput,
    ScheduleView, SessionInfo, SettingsInput, SettingsView, SetupRequest, ShareInput,
    WatchSettingsInput, WatchStatusView,
};
use ism_core::maintenance::{MaintenanceState, Phase};
use ism_core::mapping::{RootMapping, validate_set};
use ism_core::paths::strip_abs_prefix;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use super::sse;
use crate::auth::{self, Admin, CSRF_HEADER, Principal, SESSION_COOKIE};
use crate::crypto::{self, hash_password, new_id, random_secret, sha256_hex, verify_password};
use crate::db;
use crate::error::{AppError, AppResult, redact};
use crate::files::{self, SENTINEL};
use crate::immich::{api, registry};
use crate::ops;
use crate::peers::admin as peers;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/session", get(session))
        .route("/setup", post(setup))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/overview", get(overview))
        .route("/settings", get(get_settings).put(put_settings))
        .route("/servers", get(list_servers).post(create_server))
        .route("/servers/{id}", put(update_server).delete(delete_server))
        .route("/servers/{id}/check", post(check_server))
        .route("/servers/{id}/libraries/refresh", post(refresh_libraries))
        .route("/servers/{id}/maintenance/retry", post(maintenance_retry))
        .route(
            "/servers/{id}/maintenance/resolve",
            post(maintenance_resolve),
        )
        .route(
            "/servers/{id}/maintenance/reapply",
            post(maintenance_reapply),
        )
        .route("/accounts", get(list_accounts).post(create_account))
        .route("/accounts/{id}", delete(delete_account))
        .route("/libraries", get(list_libraries))
        .route("/mounts", get(list_mounts))
        .route("/mappings", get(list_mappings).post(create_mapping))
        .route(
            "/mappings/{root_id}",
            put(update_mapping).delete(delete_mapping),
        )
        .route("/mappings/{root_id}/watch", put(put_mapping_watch))
        .route("/mappings/{root_id}/watch/reset", post(reset_mapping_watch))
        .route("/tokens", get(list_tokens).post(create_token))
        .route("/tokens/{id}", delete(revoke_token))
        .route("/operations", get(list_operations))
        .route("/operations/{id}", get(get_operation))
        .route("/operations/{id}/cancel", post(cancel_operation))
        .route("/operations/{id}/resolve", post(resolve_operation))
        .route("/trash", get(trash))
        .route("/peers", get(list_peers))
        .route("/peers/invite", post(create_invite))
        .route("/peers/accept", post(accept_invite))
        .route("/peers/{id}", delete(delete_peer))
        .route("/peers/{id}/retry", post(retry_peer))
        .route("/shares", get(list_shares).post(create_share))
        .route("/shares/{id}", put(update_share).delete(delete_share))
        .route("/sync", get(sync_status))
        .route("/sync/schedule", get(get_schedule).put(put_schedule))
        .route("/sync/overrides", get(list_overrides).post(create_override))
        .route("/sync/overrides/{id}", delete(revoke_override))
        .route("/sync/transfers/{id}/retry", post(retry_transfer))
        .route("/sync/transfers/{id}/cancel", post(cancel_transfer))
        .route("/conflicts", get(list_conflicts))
        .route("/conflicts/{id}/resolve", post(resolve_conflict))
        .route("/audit", get(audit))
        .route("/events", get(events))
}

fn actor(p: &Principal) -> String {
    p.key()
}

fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------------------------
// Session

async fn needs_setup(state: &AppState) -> AppResult<bool> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM admin_users")
        .fetch_one(&state.db)
        .await?;
    Ok(n == 0)
}

async fn session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<SessionInfo>> {
    let user = auth::session_user(&state, &headers).await?;
    Ok(Json(SessionInfo {
        needs_setup: needs_setup(&state).await?,
        authenticated: user.is_some(),
        username: user,
        instance_id: state.instance_id.clone(),
        instance_name: state.instance_name().await,
        version: env!("CARGO_PKG_VERSION").to_owned(),
    }))
}

fn require_csrf(headers: &HeaderMap) -> AppResult<()> {
    if headers.get(CSRF_HEADER).is_some_and(|v| v == "1") {
        Ok(())
    } else {
        Err(AppError::forbidden("missing x-ism-request header"))
    }
}

fn check_username(raw: &str) -> AppResult<String> {
    let name = raw.trim();
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_alphanumeric() || "._-@".contains(c))
    {
        return Err(AppError::bad_request(
            "invalid_request",
            "user names use 1-64 letters, digits, '.', '_', '-' or '@'",
        ));
    }
    Ok(name.to_owned())
}

fn check_password(raw: &str) -> AppResult<()> {
    if raw.chars().count() < 10 || raw.len() > 512 {
        return Err(AppError::bad_request(
            "weak_password",
            "passwords need at least 10 characters",
        ));
    }
    Ok(())
}

fn with_cookie(state: &AppState, token: &str, max_age: i64, body: Response) -> Response {
    let mut resp = body;
    if let Ok(v) = auth::session_cookie(state, token, max_age).parse() {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }
    resp
}

async fn setup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SetupRequest>,
) -> AppResult<Response> {
    require_csrf(&headers)?;
    let username = check_username(&req.username)?;
    check_password(&req.password)?;
    let instance_name = req.instance_name.trim();
    if instance_name.is_empty() || instance_name.len() > 64 {
        return Err(AppError::bad_request(
            "invalid_request",
            "the instance name needs 1-64 characters",
        ));
    }
    let hash = hash_password(&req.password);
    let mut tx = state.db.begin().await?;
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM admin_users")
        .fetch_one(&mut *tx)
        .await?;
    if n > 0 {
        return Err(AppError::conflict(
            "already_set_up",
            "this instance is already set up",
        ));
    }
    let id: i64 = sqlx::query_scalar("INSERT INTO admin_users (username, password_hash, created_at) VALUES (?, ?, ?) RETURNING id")
        .bind(&username)
        .bind(&hash)
        .bind(db::now())
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO settings (key, value) VALUES ('instance_name', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(instance_name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    db::audit(
        &state.db,
        &format!("admin:{username}"),
        "setup",
        instance_name,
    )
    .await;
    let token = auth::create_session(&state, id).await?;
    Ok(with_cookie(
        &state,
        &token,
        auth::SESSION_DAYS * 86_400,
        no_content(),
    ))
}

async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> AppResult<Response> {
    require_csrf(&headers)?;
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT id, password_hash FROM admin_users WHERE username = ?")
            .bind(req.username.trim())
            .fetch_optional(&state.db)
            .await?;
    let ok = row
        .as_ref()
        .is_some_and(|(_, hash)| verify_password(&req.password, hash));
    let Some((id, _)) = row.filter(|_| ok) else {
        // Slow down guessing.
        tokio::time::sleep(Duration::from_millis(750)).await;
        db::audit(&state.db, "anonymous", "login.failed", "").await;
        return Err(AppError::unauthorized("wrong user name or password"));
    };
    let token = auth::create_session(&state, id).await?;
    db::audit(
        &state.db,
        &format!("admin:{}", req.username.trim()),
        "login",
        "",
    )
    .await;
    Ok(with_cookie(
        &state,
        &token,
        auth::SESSION_DAYS * 86_400,
        no_content(),
    ))
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> AppResult<Response> {
    require_csrf(&headers)?;
    if let Some(token) = auth::cookie(&headers, SESSION_COOKIE) {
        sqlx::query("DELETE FROM sessions WHERE token_hash = ?")
            .bind(sha256_hex(token.as_bytes()))
            .execute(&state.db)
            .await?;
    }
    Ok(with_cookie(&state, "", 0, no_content()))
}

// ---------------------------------------------------------------------------------------------
// Overview and settings

async fn overview(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Json<Overview>> {
    let mut servers = Vec::new();
    for s in registry::servers(&state).await? {
        servers.push(registry::server_view(&state, &s).await?);
    }
    let roots = files::load_roots(&state.db).await?;
    let count = |sql: &'static str| {
        let db = state.db.clone();
        async move { sqlx::query_scalar::<_, i64>(sql).fetch_one(&db).await }
    };
    Ok(Json(Overview {
        instance_name: state.instance_name().await,
        servers,
        roots_total: roots.len() as i64,
        roots_unavailable: roots.iter().filter(|r| !r.available()).count() as i64,
        operations_running: count(
            "SELECT COUNT(*) FROM operations WHERE status IN ('queued','waiting_maintenance','running')",
        )
        .await?,
        operations_attention: count(
            "SELECT COUNT(*) FROM operations WHERE (status = 'recovery_required' AND recovery_resolved = 0)
             OR (status IN ('failed','partial') AND finished_at > datetime('now','-1 day'))",
        )
        .await?,
        transfers_queued: count("SELECT COUNT(*) FROM transfers WHERE status IN ('queued','running')").await?,
        conflicts_open: count("SELECT COUNT(*) FROM conflicts WHERE status IN ('open','resolving')").await?,
        peers_online: count("SELECT COUNT(*) FROM peers WHERE status = 'online'").await?,
        peers_total: count("SELECT COUNT(*) FROM peers").await?,
        uncovered_libraries: count(
            "SELECT COUNT(*) FROM immich_libraries l WHERE NOT EXISTS (SELECT 1 FROM immich_accounts a
             WHERE a.server_id = l.server_id AND a.immich_user_id = l.owner_user_id AND a.status = 'ok')",
        )
        .await?,
        watchers: {
            let mut views: Vec<_> = crate::watch::status_views(&state)
                .await?
                .into_values()
                .filter(|v| v.watched)
                .collect();
            views.sort_by(|a, b| a.name.cmp(&b.name));
            views
        },
        scans: crate::watch::scheduler::views(&state).await?,
    }))
}

async fn get_settings(
    State(state): State<AppState>,
    Admin(_): Admin,
) -> AppResult<Json<SettingsView>> {
    Ok(Json(crate::settings::admin_view(&state).await?))
}

async fn put_settings(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<SettingsInput>,
) -> AppResult<Json<SettingsView>> {
    let name = input.instance_name.trim();
    if name.is_empty() || name.len() > 64 {
        return Err(AppError::bad_request(
            "invalid_request",
            "the instance name needs 1-64 characters",
        ));
    }
    crate::settings::apply_admin(&state, &input).await?;
    if state.instance_name().await != name {
        db::set_setting(&state.db, "instance_name", name).await?;
        // Clients show the primary's name.
        state.emit(crate::state::ServerEvent::SettingsChanged);
    }
    db::audit(&state.db, &actor(&p), "settings.update", name).await;
    get_settings(State(state), Admin(p)).await
}

// ---------------------------------------------------------------------------------------------
// Immich servers, accounts and libraries

fn check_server_input(input: &ImmichServerInput) -> AppResult<(String, String)> {
    let name = input.name.trim();
    if name.is_empty() || name.len() > 64 {
        return Err(AppError::bad_request(
            "invalid_request",
            "the server name needs 1-64 characters",
        ));
    }
    let url = input.url.trim().trim_end_matches('/');
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| AppError::bad_request("invalid_request", "invalid server URL"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
    {
        return Err(AppError::bad_request(
            "invalid_request",
            "the server URL must be http(s)://host[:port]",
        ));
    }
    if let Some(db_url) = input
        .database_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
        && !(db_url.starts_with("postgres://") || db_url.starts_with("postgresql://"))
    {
        return Err(AppError::bad_request(
            "invalid_request",
            "the database URL must start with postgres://",
        ));
    }
    Ok((name.to_owned(), url.to_owned()))
}

async fn list_servers(
    State(state): State<AppState>,
    Admin(_): Admin,
) -> AppResult<Json<Vec<ImmichServerView>>> {
    let mut out = Vec::new();
    for s in registry::servers(&state).await? {
        out.push(registry::server_view(&state, &s).await?);
    }
    Ok(Json(out))
}

async fn create_server(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<ImmichServerInput>,
) -> AppResult<Response> {
    let (name, url) = check_server_input(&input)?;
    let id = new_id("srv");
    let db_enc = match input
        .database_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
    {
        Some(u) => Some(
            state
                .secrets
                .encrypt(registry::DATABASE_URL_PURPOSE, u)
                .map_err(AppError::internal)?,
        ),
        None => None,
    };
    sqlx::query(
        "INSERT INTO immich_servers (id, name, url, database_url_enc, direct_writes_enabled, \
         external_library_move_mode, maintenance_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(&name)
    .bind(&url)
    .bind(&db_enc)
    .bind(input.direct_writes_enabled as i64)
    .bind(input.external_library_move_mode.as_str())
    .bind(serde_json::to_string(&MaintenanceState::default())?)
    .bind(db::now())
    .execute(&state.db)
    .await?;
    db::audit(&state.db, &actor(&p), "server.create", &name).await;
    let server = registry::check_compatibility(&state, &id).await?;
    Ok((
        StatusCode::CREATED,
        Json(registry::server_view(&state, &server).await?),
    )
        .into_response())
}

fn ensure_idle(server: &registry::Server) -> AppResult<()> {
    if server.maintenance.phase != Phase::Idle || server.maintenance.active_ops > 0 {
        return Err(AppError::conflict(
            "maintenance_active",
            "the server is in a maintenance window; wait until it is idle",
        ));
    }
    Ok(())
}

async fn update_server(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
    Json(input): Json<ImmichServerInput>,
) -> AppResult<Json<ImmichServerView>> {
    let (name, url) = check_server_input(&input)?;
    let current = registry::server(&state, &id).await?;
    let connection_changes = url != current.url || input.database_url.is_some();
    if connection_changes {
        ensure_idle(&current)?;
    }
    let db_enc = match input.database_url.as_deref().map(str::trim) {
        None => current.database_url_enc.clone(),
        Some("") => None,
        Some(u) => Some(
            state
                .secrets
                .encrypt(registry::DATABASE_URL_PURPOSE, u)
                .map_err(AppError::internal)?,
        ),
    };
    sqlx::query(
        "UPDATE immich_servers SET name = ?, url = ?, database_url_enc = ?, direct_writes_enabled = ?, \
         external_library_move_mode = ? WHERE id = ?",
    )
        .bind(&name)
        .bind(&url)
        .bind(&db_enc)
        .bind(input.direct_writes_enabled as i64)
        .bind(input.external_library_move_mode.as_str())
        .bind(&id)
        .execute(&state.db)
        .await?;
    db::audit(&state.db, &actor(&p), "server.update", &name).await;
    let server = if connection_changes {
        registry::check_compatibility(&state, &id).await?
    } else {
        registry::server(&state, &id).await?
    };
    Ok(Json(registry::server_view(&state, &server).await?))
}

async fn delete_server(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    let server = registry::server(&state, &id).await?;
    ensure_idle(&server)?;
    if server.maintenance.unresolved_recoveries > 0 {
        return Err(AppError::conflict(
            "recovery_required",
            "resolve the pending recovery first",
        ));
    }
    registry::forget_pool(&state, &id).await;
    sqlx::query("DELETE FROM immich_servers WHERE id = ?")
        .bind(&id)
        .execute(&state.db)
        .await?;
    sqlx::query("UPDATE mappings SET library_id = NULL WHERE server_id IS NULL")
        .execute(&state.db)
        .await?;
    db::audit(&state.db, &actor(&p), "server.delete", &server.name).await;
    Ok(no_content())
}

async fn check_server(
    State(state): State<AppState>,
    Admin(_): Admin,
    Path(id): Path<String>,
) -> AppResult<Json<ImmichServerView>> {
    let server = registry::check_compatibility(&state, &id).await?;
    Ok(Json(registry::server_view(&state, &server).await?))
}

#[derive(Serialize)]
struct Refreshed {
    libraries: usize,
}

async fn refresh_libraries(
    State(state): State<AppState>,
    Admin(_): Admin,
    Path(id): Path<String>,
) -> AppResult<Json<Refreshed>> {
    let server = registry::server(&state, &id).await?;
    Ok(Json(Refreshed {
        libraries: registry::refresh_libraries(&state, &server).await?,
    }))
}

async fn maintenance_retry(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    crate::maintenance::retry(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "maintenance.retry", &id).await;
    Ok(no_content())
}

async fn maintenance_reapply(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    crate::maintenance::reapply_settings(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "maintenance.reapply", &id).await;
    Ok(no_content())
}

async fn maintenance_resolve(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    crate::maintenance::resolve_recovery(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "maintenance.resolve", &id).await;
    Ok(no_content())
}

type AccountRow = (
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    String,
);

fn account_view(r: AccountRow) -> AccountView {
    AccountView {
        id: r.0,
        server_id: r.1,
        label: r.2,
        email: r.3,
        immich_user_id: r.4,
        is_admin: r.5 != 0,
        status: r.6,
        last_error: r.7,
        created_at: r.8,
    }
}

const ACCOUNT_COLUMNS: &str =
    "id, server_id, label, email, immich_user_id, is_admin, status, last_error, created_at";

async fn list_accounts(
    State(state): State<AppState>,
    Admin(_): Admin,
) -> AppResult<Json<Vec<AccountView>>> {
    let sql = format!("SELECT {ACCOUNT_COLUMNS} FROM immich_accounts ORDER BY server_id, label");
    let rows: Vec<AccountRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .fetch_all(&state.db)
        .await?;
    Ok(Json(rows.into_iter().map(account_view).collect()))
}

async fn create_account(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<AccountInput>,
) -> AppResult<Response> {
    let server = registry::server(&state, &input.server_id).await?;
    let label = input.label.trim();
    let email = input.email.trim();
    if label.is_empty()
        || label.len() > 64
        || email.is_empty()
        || email.len() > 254
        || input.password.is_empty()
    {
        return Err(AppError::bad_request(
            "invalid_request",
            "label, email and password are required",
        ));
    }
    let key_name = format!("Immich Sync Manager ({})", state.instance_name().await);
    let (user_id, is_admin, key_id, secret) =
        api::provision_api_key(&state.http, &server.url, email, &input.password, &key_name)
            .await
            .map_err(|e| AppError::bad_request("immich_login_failed", redact(&e.to_string())))?;
    let key_enc = state
        .secrets
        .encrypt(registry::API_KEY_PURPOSE, &secret)
        .map_err(AppError::internal)?;
    let id = new_id("acc");
    sqlx::query(
        "INSERT INTO immich_accounts (id, server_id, label, email, immich_user_id, is_admin, api_key_enc, api_key_id, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ok', ?)
         ON CONFLICT(server_id, immich_user_id) DO UPDATE SET label = excluded.label, email = excluded.email,
           is_admin = excluded.is_admin, api_key_enc = excluded.api_key_enc, api_key_id = excluded.api_key_id,
           status = 'ok', last_error = NULL",
    )
    .bind(&id)
    .bind(&server.id)
    .bind(label)
    .bind(email)
    .bind(&user_id)
    .bind(is_admin as i64)
    .bind(&key_enc)
    .bind(&key_id)
    .bind(db::now())
    .execute(&state.db)
    .await?;
    db::audit(&state.db, &actor(&p), "account.connect", label).await;
    if is_admin {
        if let Err(e) = registry::refresh_libraries(&state, &server).await {
            tracing::warn!(
                "library refresh after connecting an admin failed: {}",
                e.message()
            );
        }
        if let Err(e) = crate::maintenance::enroll(&state, &server.id).await {
            tracing::warn!("enrolling '{}' failed: {}", server.name, e.message());
        }
    }
    let sql = format!(
        "SELECT {ACCOUNT_COLUMNS} FROM immich_accounts WHERE server_id = ? AND immich_user_id = ?"
    );
    let row: AccountRow = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .bind(&server.id)
        .bind(&user_id)
        .fetch_one(&state.db)
        .await?;
    Ok((StatusCode::CREATED, Json(account_view(row))).into_response())
}

async fn delete_account(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    let label: Option<String> =
        sqlx::query_scalar("SELECT label FROM immich_accounts WHERE id = ?")
            .bind(&id)
            .fetch_optional(&state.db)
            .await?;
    let label = label.ok_or_else(|| AppError::not_found("account not found"))?;
    sqlx::query("DELETE FROM immich_accounts WHERE id = ?")
        .bind(&id)
        .execute(&state.db)
        .await?;
    db::audit(&state.db, &actor(&p), "account.remove", &label).await;
    Ok(no_content())
}

#[allow(clippy::type_complexity)]
async fn list_libraries(
    State(state): State<AppState>,
    Admin(_): Admin,
) -> AppResult<Json<Vec<LibraryView>>> {
    let rows: Vec<(String, String, String, String, String, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT l.id, l.server_id, l.name, l.owner_user_id, l.import_paths_json, l.asset_count,
                (SELECT a.id FROM immich_accounts a WHERE a.server_id = l.server_id AND a.immich_user_id = l.owner_user_id AND a.status = 'ok')
         FROM immich_libraries l ORDER BY l.server_id, l.name",
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(
                |(id, server_id, name, owner, paths, count, account)| LibraryView {
                    covered: account.is_some(),
                    id,
                    server_id,
                    name,
                    owner_user_id: owner,
                    owner_account_id: account,
                    import_paths: db::json_vec(&paths),
                    asset_count: count,
                },
            )
            .collect(),
    ))
}

// ---------------------------------------------------------------------------------------------
// Mounts and mappings

#[derive(Deserialize)]
struct MountQuery {
    path: Option<String>,
}

async fn list_mounts(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(query): Query<MountQuery>,
) -> AppResult<Json<Vec<MountView>>> {
    let roots = files::load_roots(&state.db).await?;
    let base = state.config.mounts_dir.clone();
    let dirs = tokio::task::spawn_blocking(move || {
        let io_error = |error: std::io::Error| {
            tracing::warn!(%error, "cannot browse mounted folder");
            AppError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "mount_unavailable",
                "Cannot read this folder. Check that the volume is mounted and the container has permission to read it.",
            )
        };
        let base = std::path::absolute(base).map_err(io_error)?;
        let path = query.path.map(std::path::PathBuf::from).unwrap_or_else(|| base.clone());
        if !path.is_absolute()
            || path.components().any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(AppError::invalid_location("the folder path must be absolute and cannot contain '..'"));
        }
        let allowed = base.canonicalize().map_err(io_error)?;
        let resolved = path.canonicalize().map_err(io_error)?;
        if !resolved.starts_with(allowed) {
            return Err(AppError::forbidden("the folder must be inside the mounts directory"));
        }
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&path).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if !entry.file_type().map_err(io_error)?.is_dir()
                || entry.file_name().to_string_lossy().starts_with('.')
            {
                continue;
            }
            let path = entry.path().into_os_string().into_string().map_err(|_| {
                AppError::invalid_location("a folder name is not valid UTF-8")
            })?;
            let writable = !entry.metadata().map_err(io_error)?.permissions().readonly();
            let mapped_root = roots
                .iter()
                .find(|r| r.mapping.container_path == path)
                .map(|r| r.mapping.root_id.clone());
            out.push(MountView { path, writable, mapped_root });
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    })
    .await
    .map_err(AppError::internal)??;
    Ok(Json(dirs))
}

fn mapping_view(r: &files::Root, watch: WatchStatusView) -> MappingView {
    let available = r.available();
    MappingView {
        root_id: r.mapping.root_id.clone(),
        name: r.name.clone(),
        container_path: r.mapping.container_path.clone(),
        server_id: r.server_id.clone(),
        library_id: r.library_id.clone(),
        immich_import_path: r.mapping.immich_import_path.clone(),
        opm_prefix: r.mapping.opm_prefix.clone(),
        available,
        availability_detail: (!available)
            .then(|| format!("{SENTINEL} is missing or invalid; check the mounted volume")),
        watch,
    }
}

async fn mapping_view_of(state: &AppState, root_id: &str) -> AppResult<MappingView> {
    let root = files::root(&state.db, root_id).await?;
    let mut watch = crate::watch::status_views(state).await?;
    let view = watch
        .remove(root_id)
        .ok_or_else(|| AppError::not_found("unknown mapping"))?;
    Ok(mapping_view(&root, view))
}

async fn put_mapping_watch(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(root_id): Path<String>,
    Json(input): Json<WatchSettingsInput>,
) -> AppResult<Json<MappingView>> {
    crate::watch::set_settings(&state, &root_id, &input).await?;
    db::audit(&state.db, &actor(&p), "mapping.watch", &root_id).await;
    Ok(Json(mapping_view_of(&state, &root_id).await?))
}

async fn reset_mapping_watch(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(root_id): Path<String>,
) -> AppResult<Json<MappingView>> {
    crate::watch::reset_baseline(&state, &root_id).await?;
    db::audit(&state.db, &actor(&p), "mapping.watch_reset", &root_id).await;
    Ok(Json(mapping_view_of(&state, &root_id).await?))
}

async fn list_mappings(
    State(state): State<AppState>,
    Admin(_): Admin,
) -> AppResult<Json<Vec<MappingView>>> {
    let mut watch = crate::watch::status_views(&state).await?;
    Ok(Json(
        files::load_roots(&state.db)
            .await?
            .iter()
            .filter_map(|r| watch.remove(&r.mapping.root_id).map(|w| mapping_view(r, w)))
            .collect(),
    ))
}

/// Validates a mapping against the others and Immich, and claims its folder with the sentinel.
async fn validate_mapping(
    state: &AppState,
    input: &MappingInput,
    replacing: Option<&str>,
) -> AppResult<RootMapping> {
    let bad = |m: String| AppError::bad_request("invalid_mapping", m);
    let name = input.name.trim();
    if name.is_empty() || name.len() > 64 {
        return Err(bad("the name needs 1-64 characters".into()));
    }
    let mapping = RootMapping {
        root_id: input.root_id.trim().to_owned(),
        container_path: input.container_path.clone(),
        immich_import_path: input.immich_import_path.clone(),
        opm_prefix: input.opm_prefix.clone(),
    }
    .normalized()
    .map_err(|e| bad(e.to_string()))?;
    let roots = files::load_roots(&state.db).await?;
    let mut all: Vec<RootMapping> = roots
        .iter()
        .filter(|r| Some(r.mapping.root_id.as_str()) != replacing)
        .map(|r| r.mapping.clone())
        .collect();
    all.push(mapping.clone());
    validate_set(&all).map_err(|e| bad(e.to_string()))?;

    let dir = std::path::PathBuf::from(&mapping.container_path);
    if !dir.is_dir() {
        return Err(bad(format!(
            "{} is not a folder inside the container",
            mapping.container_path
        )));
    }
    match (&input.server_id, &input.library_id) {
        (Some(server_id), library_id) => {
            registry::server(state, server_id).await?;
            let Some(immich) = &mapping.immich_import_path else {
                return Err(bad(
                    "a root linked to Immich needs its Immich import path".into()
                ));
            };
            if let Some(library_id) = library_id {
                let paths: Option<String> = sqlx::query_scalar(
                    "SELECT import_paths_json FROM immich_libraries WHERE id = ? AND server_id = ?",
                )
                .bind(library_id)
                .bind(server_id)
                .fetch_optional(&state.db)
                .await?;
                let paths = paths
                    .ok_or_else(|| bad("unknown library; refresh the libraries first".into()))?;
                if !db::json_vec(&paths)
                    .iter()
                    .any(|p| strip_abs_prefix(immich, p).is_some())
                {
                    return Err(bad(
                        "the Immich path is not inside an import path of this library".into(),
                    ));
                }
            }
        }
        (None, Some(_)) => return Err(bad("a library needs a server".into())),
        (None, None) => {}
    }
    // Claim the folder; never overwrite or follow an existing marker.
    let mut sentinel = files::marker_path(&mapping);
    if sentinel == dir.join(SENTINEL)
        && roots.iter().any(|root| {
            root.mapping.root_id != mapping.root_id
                && root.mapping.container_path == mapping.container_path
                && root.available()
                && std::fs::read_to_string(&sentinel)
                    .is_ok_and(|owner| owner.trim() == root.mapping.root_id)
        })
    {
        sentinel = dir.join(format!("{SENTINEL}-{}", mapping.root_id));
    }
    match std::fs::symlink_metadata(&sentinel) {
        Ok(meta) if !meta.is_file() || meta.len() > 128 => {
            return Err(AppError::conflict(
                "invalid_root_marker",
                format!("{SENTINEL} must be a regular marker file for this root"),
            ));
        }
        Ok(_) => {
            let owner = std::fs::read_to_string(&sentinel).map_err(|error| {
                AppError::conflict(
                    "invalid_root_marker",
                    format!("cannot read {SENTINEL}: {error}"),
                )
            })?;
            if owner.trim() != mapping.root_id {
                return Err(AppError::conflict(
                    "folder_claimed",
                    format!("this folder has a different or empty {SENTINEL} marker"),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            use std::io::Write;
            let mut marker = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&sentinel)
                .map_err(|e| bad(format!("cannot create {SENTINEL}: {e}")))?;
            marker
                .write_all(format!("{}\n", mapping.root_id).as_bytes())
                .and_then(|()| marker.sync_all())
                .map_err(|e| bad(format!("cannot write {SENTINEL}: {e}")))?;
        }
        Err(error) => {
            return Err(AppError::internal(error));
        }
    }
    Ok(mapping)
}

async fn busy_root(state: &AppState) -> AppResult<()> {
    let busy: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM operations WHERE status IN ('queued','waiting_maintenance','running')",
    )
    .fetch_one(&state.db)
    .await?;
    if busy > 0 {
        return Err(AppError::conflict(
            "operations_running",
            "wait for running operations to finish",
        ));
    }
    Ok(())
}

async fn create_mapping(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<MappingInput>,
) -> AppResult<Response> {
    let _guard = state.replication_operation_lock.lock().await;
    if files::root(&state.db, input.root_id.trim()).await.is_ok() {
        return Err(AppError::conflict(
            "root_exists",
            "a root with this id exists",
        ));
    }
    let m = validate_mapping(&state, &input, None).await?;
    sqlx::query(
        "INSERT INTO mappings (root_id, name, container_path, server_id, library_id, immich_import_path, opm_prefix, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&m.root_id)
    .bind(input.name.trim())
    .bind(&m.container_path)
    .bind(&input.server_id)
    .bind(&input.library_id)
    .bind(&m.immich_import_path)
    .bind(&m.opm_prefix)
    .bind(db::now())
    .execute(&state.db)
    .await?;
    db::audit(&state.db, &actor(&p), "mapping.create", &m.root_id).await;
    state.watch_wake.notify_one();
    Ok((
        StatusCode::CREATED,
        Json(mapping_view_of(&state, &m.root_id).await?),
    )
        .into_response())
}

async fn update_mapping(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(root_id): Path<String>,
    Json(input): Json<MappingInput>,
) -> AppResult<Json<MappingView>> {
    let _guard = state.replication_operation_lock.lock().await;
    let current = files::root(&state.db, &root_id).await?;
    if input.root_id.trim() != root_id {
        return Err(AppError::bad_request(
            "invalid_mapping",
            "the root id cannot change",
        ));
    }
    busy_root(&state).await?;
    let m = validate_mapping(&state, &input, Some(&root_id)).await?;
    if m.container_path != current.mapping.container_path {
        let shares: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shares WHERE root_id = ?")
            .bind(&root_id)
            .fetch_one(&state.db)
            .await?;
        if shares > 0 {
            return Err(AppError::conflict(
                "root_shared",
                "remove the sync shares of this root first",
            ));
        }
    }
    sqlx::query(
        "UPDATE mappings SET name = ?, container_path = ?, server_id = ?, library_id = ?, immich_import_path = ?, opm_prefix = ?
         WHERE root_id = ?",
    )
    .bind(input.name.trim())
    .bind(&m.container_path)
    .bind(&input.server_id)
    .bind(&input.library_id)
    .bind(&m.immich_import_path)
    .bind(&m.opm_prefix)
    .bind(&root_id)
    .execute(&state.db)
    .await?;
    sqlx::query("DELETE FROM file_hashes WHERE root_id = ?")
        .bind(&root_id)
        .execute(&state.db)
        .await?;
    db::audit(&state.db, &actor(&p), "mapping.update", &root_id).await;
    state.watch_wake.notify_one();
    Ok(Json(mapping_view_of(&state, &root_id).await?))
}

async fn delete_mapping(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(root_id): Path<String>,
) -> AppResult<Response> {
    let _guard = state.replication_operation_lock.lock().await;
    let root = files::root(&state.db, &root_id).await?;
    busy_root(&state).await?;
    let shares: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shares WHERE root_id = ?")
        .bind(&root_id)
        .fetch_one(&state.db)
        .await?;
    if shares > 0 {
        return Err(AppError::conflict(
            "root_shared",
            "remove the sync shares of this root first",
        ));
    }
    sqlx::query("DELETE FROM mappings WHERE root_id = ?")
        .bind(&root_id)
        .execute(&state.db)
        .await?;
    sqlx::query("DELETE FROM file_hashes WHERE root_id = ?")
        .bind(&root_id)
        .execute(&state.db)
        .await?;
    // Release the claim only when it is ours; the trash folder is left for the administrator.
    let sentinel = files::marker_path(&root.mapping);
    if root.available() {
        std::fs::remove_file(&sentinel)?;
    }
    db::audit(&state.db, &actor(&p), "mapping.delete", &root_id).await;
    Ok(no_content())
}

// ---------------------------------------------------------------------------------------------
// Client tokens

const TOKEN_KINDS: [&str; 3] = ["opm", "ila", "other"];

type TokenRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
);

fn token_view(r: TokenRow) -> ClientTokenView {
    let actions = serde_json::from_str::<Vec<ClientAction>>(&r.4).unwrap_or_default();
    ClientTokenView {
        id: r.0,
        name: r.1,
        kind: r.2,
        roots: db::json_vec(&r.3),
        actions,
        created_at: r.5,
        last_used_at: r.6,
    }
}

async fn list_tokens(
    State(state): State<AppState>,
    Admin(_): Admin,
) -> AppResult<Json<Vec<ClientTokenView>>> {
    let rows: Vec<TokenRow> = sqlx::query_as(
        "SELECT id, name, kind, roots_json, actions_json, created_at, last_used_at FROM client_tokens
         WHERE revoked_at IS NULL ORDER BY created_at DESC",
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(rows.into_iter().map(token_view).collect()))
}

async fn create_token(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<ClientTokenInput>,
) -> AppResult<Response> {
    let name = input.name.trim();
    if name.is_empty() || name.len() > 64 {
        return Err(AppError::bad_request(
            "invalid_request",
            "the token name needs 1-64 characters",
        ));
    }
    if !TOKEN_KINDS.contains(&input.kind.as_str()) {
        return Err(AppError::bad_request(
            "invalid_request",
            "kind must be opm, ila or other",
        ));
    }
    if input.actions.is_empty() {
        return Err(AppError::bad_request(
            "invalid_request",
            "pick at least one action",
        ));
    }
    for r in &input.roots {
        files::root(&state.db, r).await?;
    }
    let id = crypto::random_secret(9).replace(['-', '_'], "x");
    let secret = random_secret(32).replace('_', "-");
    let mut actions = input.actions.clone();
    actions.sort_by_key(|a| a.as_str());
    actions.dedup();
    let created = db::now();
    sqlx::query(
        "INSERT INTO client_tokens (id, name, kind, secret_hash, roots_json, actions_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(name)
    .bind(&input.kind)
    .bind(sha256_hex(secret.as_bytes()))
    .bind(serde_json::to_string(&input.roots)?)
    .bind(serde_json::to_string(&actions)?)
    .bind(&created)
    .execute(&state.db)
    .await?;
    db::audit(&state.db, &actor(&p), "token.create", name).await;
    Ok((
        StatusCode::CREATED,
        Json(ClientTokenCreated {
            token: ClientTokenView {
                id: id.clone(),
                name: name.to_owned(),
                kind: input.kind,
                roots: input.roots,
                actions,
                created_at: created,
                last_used_at: None,
            },
            secret: format!("ism_{id}_{secret}"),
        }),
    )
        .into_response())
}

async fn revoke_token(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    let n =
        sqlx::query("UPDATE client_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL")
            .bind(db::now())
            .bind(&id)
            .execute(&state.db)
            .await?
            .rows_affected();
    if n == 0 {
        return Err(AppError::not_found("token not found"));
    }
    db::audit(&state.db, &actor(&p), "token.revoke", &id).await;
    Ok(no_content())
}

// ---------------------------------------------------------------------------------------------
// Operations and trash

#[derive(Deserialize)]
struct OpsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

async fn list_operations(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<OpsQuery>,
) -> AppResult<Response> {
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    Ok(Json(ops::list(&state, q.status.as_deref(), limit).await?).into_response())
}

async fn get_operation(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    Ok(Json(ops::get(&state, &p, &id).await?).into_response())
}

async fn cancel_operation(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    Ok(Json(ops::cancel(&state, &p, &id).await?).into_response())
}

async fn resolve_operation(
    State(state): State<AppState>,
    Admin(_): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    ops::resolve_recovery(&state, &id).await?;
    Ok(no_content())
}

#[derive(Deserialize)]
struct TrashQuery {
    root_id: Option<String>,
}

async fn trash(
    State(state): State<AppState>,
    Admin(p): Admin,
    Query(q): Query<TrashQuery>,
) -> AppResult<Response> {
    Ok(Json(ops::trash_list(&state, &p, q.root_id.as_deref()).await?).into_response())
}

// ---------------------------------------------------------------------------------------------
// Peers, shares and sync

async fn list_peers(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Response> {
    Ok(Json(peers::list_peers(&state).await?).into_response())
}

async fn create_invite(State(state): State<AppState>, Admin(p): Admin) -> AppResult<Response> {
    let invite = peers::create_invite(&state).await?;
    db::audit(&state.db, &actor(&p), "peer.invite", "").await;
    Ok(Json(invite).into_response())
}

async fn accept_invite(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<PeerAcceptInput>,
) -> AppResult<Response> {
    let peer = peers::accept(&state, input).await?;
    db::audit(&state.db, &actor(&p), "peer.accept", &peer.name).await;
    state.sync_wake.notify_waiters();
    Ok((StatusCode::CREATED, Json(peer)).into_response())
}

async fn delete_peer(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    peers::delete_peer(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "peer.remove", &id).await;
    Ok(no_content())
}

async fn retry_peer(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    peers::retry_peer(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "peer.retry", &id).await;
    Ok(no_content())
}

async fn list_shares(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Response> {
    Ok(Json(peers::list_shares(&state).await?).into_response())
}

async fn create_share(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<ShareInput>,
) -> AppResult<Response> {
    let share = peers::create_share(&state, input).await?;
    db::audit(&state.db, &actor(&p), "share.create", &share.name).await;
    state.sync_wake.notify_waiters();
    Ok((StatusCode::CREATED, Json(share)).into_response())
}

async fn update_share(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
    Json(input): Json<ShareInput>,
) -> AppResult<Response> {
    let share = peers::update_share(&state, &id, input).await?;
    db::audit(&state.db, &actor(&p), "share.update", &share.name).await;
    state.sync_wake.notify_waiters();
    Ok(Json(share).into_response())
}

async fn delete_share(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    peers::delete_share(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "share.delete", &id).await;
    Ok(no_content())
}

async fn sync_status(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Response> {
    Ok(Json(peers::sync_status(&state).await?).into_response())
}

async fn get_schedule(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Response> {
    Ok(Json(peers::schedule(&state).await?).into_response())
}

async fn put_schedule(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(view): Json<ScheduleView>,
) -> AppResult<Response> {
    let saved = peers::set_schedule(&state, view).await?;
    db::audit(&state.db, &actor(&p), "sync.schedule", &saved.timezone).await;
    state.sync_wake.notify_waiters();
    Ok(Json(saved).into_response())
}

async fn list_overrides(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Response> {
    Ok(Json(peers::active_overrides(&state).await?).into_response())
}

async fn create_override(
    State(state): State<AppState>,
    Admin(p): Admin,
    Json(input): Json<OverrideInput>,
) -> AppResult<Response> {
    let o = peers::create_override(&state, input).await?;
    db::audit(&state.db, &actor(&p), "sync.override", &o.reason).await;
    state.sync_wake.notify_waiters();
    Ok((StatusCode::CREATED, Json(o)).into_response())
}

async fn revoke_override(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    peers::revoke_override(&state, &id).await?;
    db::audit(&state.db, &actor(&p), "sync.override.revoke", &id).await;
    Ok(no_content())
}

async fn retry_transfer(
    State(state): State<AppState>,
    Admin(_): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    peers::retry_transfer(&state, &id).await?;
    state.sync_wake.notify_waiters();
    Ok(no_content())
}

async fn cancel_transfer(
    State(state): State<AppState>,
    Admin(_): Admin,
    Path(id): Path<String>,
) -> AppResult<Response> {
    peers::cancel_transfer(&state, &id).await?;
    Ok(no_content())
}

#[derive(Deserialize)]
struct ConflictsQuery {
    #[serde(default)]
    include_resolved: bool,
}

async fn list_conflicts(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<ConflictsQuery>,
) -> AppResult<Response> {
    Ok(Json(peers::list_conflicts(&state, q.include_resolved).await?).into_response())
}

async fn resolve_conflict(
    State(state): State<AppState>,
    Admin(p): Admin,
    Path(id): Path<String>,
    Json(input): Json<ResolveConflictInput>,
) -> AppResult<Response> {
    peers::resolve(&state, &actor(&p), &id, input.resolution).await?;
    Ok(no_content())
}

// ---------------------------------------------------------------------------------------------
// Audit and live events

#[derive(Deserialize)]
struct AuditQuery {
    limit: Option<i64>,
    before: Option<i64>,
}

async fn audit(
    State(state): State<AppState>,
    Admin(_): Admin,
    Query(q): Query<AuditQuery>,
) -> AppResult<Json<Vec<AuditEntry>>> {
    let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT id, at, actor, action, detail FROM audit WHERE id < ? ORDER BY id DESC LIMIT ?",
    )
    .bind(q.before.unwrap_or(i64::MAX))
    .bind(q.limit.unwrap_or(100).clamp(1, 500))
    .fetch_all(&state.db)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|(id, at, actor, action, detail)| AuditEntry {
                id,
                at,
                actor,
                action,
                detail,
            })
            .collect(),
    ))
}

async fn events(State(state): State<AppState>, Admin(_): Admin) -> AppResult<Response> {
    let rx = state.events.subscribe();
    let s = stream::unfold(rx, |mut rx| async move {
        let ev = match rx.recv().await {
            Ok(e) => Event::default()
                .event("change")
                .data(serde_json::to_string(&e).unwrap_or_default()),
            Err(broadcast::error::RecvError::Lagged(_)) => {
                Event::default().event("resync").data("{}")
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        };
        Some((Ok::<_, Infallible>(ev), rx))
    });
    Ok(sse(s))
}
