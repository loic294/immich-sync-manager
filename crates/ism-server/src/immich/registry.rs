//! Immich servers and accounts known to this instance, and routing of each change to the
//! account that owns the affected library.

use ism_core::dto::{DirectWritesReason, ImmichMoveMode, ImmichServerView, MaintenanceView};
use ism_core::maintenance::{MaintenanceState, Phase};
use sqlx::Row;
use sqlx::postgres::PgPool;

use super::api::{self, Api};
use super::{SUPPORTED_MIGRATION, SUPPORTED_VERSION, pg};
use crate::error::{AppError, AppResult};
use crate::files::{STAGING_DIR, TRASH_DIR};
use crate::state::{AppState, ServerEvent};

pub const API_KEY_PURPOSE: &str = "immich-api-key";
pub const DATABASE_URL_PURPOSE: &str = "immich-database-url";

/// Library exclusions that keep ISM's own folders out of Immich.
pub fn exclusion_patterns() -> Vec<String> {
    vec![format!("**/{TRASH_DIR}/**"), format!("**/{STAGING_DIR}/**")]
}

#[derive(Debug, Clone)]
pub struct Server {
    pub id: String,
    pub name: String,
    pub url: String,
    pub database_url_enc: Option<String>,
    pub direct_writes_enabled: bool,
    pub external_library_move_mode: ImmichMoveMode,
    pub external_library_api_capable: Option<bool>,
    pub version: Option<String>,
    pub compatible: bool,
    pub compatibility_detail: Option<String>,
    pub maintenance: MaintenanceState,
}

fn server_from_row(row: &sqlx::sqlite::SqliteRow) -> AppResult<Server> {
    let maintenance: String = row.try_get("maintenance_json")?;
    let move_mode: String = row.try_get("external_library_move_mode")?;
    let move_mode = ImmichMoveMode::parse(&move_mode).ok_or_else(|| {
        AppError::internal(format!(
            "unknown Immich move mode in stored settings: {move_mode}"
        ))
    })?;
    Ok(Server {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        url: row.try_get("url")?,
        database_url_enc: row.try_get("database_url_enc")?,
        direct_writes_enabled: row.try_get::<i64, _>("direct_writes_enabled")? != 0,
        external_library_move_mode: move_mode,
        external_library_api_capable: row
            .try_get::<Option<i64>, _>("external_library_api_capable")?
            .map(|value| value != 0),
        version: row.try_get("version")?,
        compatible: row.try_get::<i64, _>("compatible")? != 0,
        compatibility_detail: row.try_get("compatibility_detail")?,
        maintenance: serde_json::from_str(&maintenance)?,
    })
}

const SERVER_COLUMNS: &str = "id, name, url, database_url_enc, direct_writes_enabled, \
     external_library_move_mode, external_library_api_capable, version, compatible, compatibility_detail, \
     maintenance_json";

pub async fn servers(state: &AppState) -> AppResult<Vec<Server>> {
    let sql = format!("SELECT {SERVER_COLUMNS} FROM immich_servers ORDER BY name");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_all(&state.db)
        .await?;
    rows.iter().map(server_from_row).collect()
}

pub async fn server(state: &AppState, id: &str) -> AppResult<Server> {
    let sql = format!("SELECT {SERVER_COLUMNS} FROM immich_servers WHERE id = ?");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(&state.db)
        .await?
        .ok_or_else(|| AppError::not_found("Immich server not found"))?;
    server_from_row(&row)
}

pub async fn save_maintenance(
    state: &AppState,
    server_id: &str,
    m: &MaintenanceState,
) -> AppResult<()> {
    sqlx::query("UPDATE immich_servers SET maintenance_json = ? WHERE id = ?")
        .bind(serde_json::to_string(m)?)
        .bind(server_id)
        .execute(&state.db)
        .await?;
    state.emit(ServerEvent::MaintenanceChanged {
        server_id: server_id.to_owned(),
    });
    Ok(())
}

pub fn maintenance_view(m: &MaintenanceState) -> MaintenanceView {
    MaintenanceView {
        phase: m.phase,
        epoch: m.epoch,
        active_ops: m.active_ops,
        queue_paused: m.queue_paused_by_ism,
        drift: m.drift.clone(),
        message: m.message.clone(),
    }
}

pub async fn server_view(state: &AppState, s: &Server) -> AppResult<ImmichServerView> {
    let admin: Option<String> = sqlx::query_scalar(
        "SELECT id FROM immich_accounts WHERE server_id = ? AND is_admin = 1 AND status = 'ok' ORDER BY created_at LIMIT 1",
    )
    .bind(&s.id)
    .fetch_optional(&state.db)
    .await?;
    Ok(ImmichServerView {
        id: s.id.clone(),
        name: s.name.clone(),
        url: s.url.clone(),
        version: s.version.clone(),
        compatible: s.compatible,
        compatibility_detail: s.compatibility_detail.clone(),
        database_configured: s.database_url_enc.is_some(),
        direct_writes_enabled: s.direct_writes_enabled,
        external_library_move_mode: s.external_library_move_mode,
        external_library_api_capable: s.external_library_api_capable,
        admin_account_id: admin,
        maintenance: maintenance_view(&s.maintenance),
    })
}

/// Why managed (direct) writes are refused on a server right now; `None` when they can run.
pub fn write_block(server: &Server) -> Option<(DirectWritesReason, String)> {
    if !(server.compatible && server.database_url_enc.is_some()) {
        return Some((
            DirectWritesReason::NotConfigured,
            "the Immich compatibility check and database access are required".into(),
        ));
    }
    if server.external_library_move_mode == ImmichMoveMode::DbOnly && !server.direct_writes_enabled
    {
        return Some((
            DirectWritesReason::NotConfigured,
            "db_only mode requires direct writes to be enabled".into(),
        ));
    }
    let m = &server.maintenance;
    let detail = |fallback: &str| m.message.clone().unwrap_or_else(|| fallback.to_owned());
    if let Some(drift) = &m.drift {
        return Some((DirectWritesReason::Drift, drift.clone()));
    }
    // An unresolved recovery only holds its own files (path footprints); it never refuses
    // other changes on the server.
    match m.phase {
        Phase::Blocked => Some((
            DirectWritesReason::Maintenance,
            detail("Immich's library queue could not be paused or resumed"),
        )),
        _ => None,
    }
}

async fn api_for(
    state: &AppState,
    server: &Server,
    account_id: &str,
    key_enc: &str,
) -> AppResult<Api> {
    let key = state
        .secrets
        .decrypt(API_KEY_PURPOSE, key_enc)
        .map_err(|_| {
            AppError::internal(format!(
                "cannot decrypt the API key of account {account_id}"
            ))
        })?;
    Ok(Api::new(state.http.clone(), &server.url, key))
}

pub struct AdminApiCandidate {
    pub account_id: String,
    pub label: String,
    pub immich_user_id: String,
    pub api: Api,
}

/// All healthy administrator keys, oldest first. API-native file operations retry a 403 with
/// another healthy admin key so one stale credential cannot mask a valid server administrator.
pub async fn admin_api_candidates(
    state: &AppState,
    server: &Server,
) -> AppResult<Vec<AdminApiCandidate>> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT id, label, immich_user_id, api_key_enc FROM immich_accounts
         WHERE server_id = ? AND is_admin = 1 AND status = 'ok' ORDER BY created_at",
    )
    .bind(&server.id)
    .fetch_all(&state.db)
    .await?;
    if rows.is_empty() {
        return Err(AppError::conflict(
            "admin_account_required",
            format!(
                "connect an Immich administrator account for '{}'",
                server.name
            ),
        ));
    }
    let mut candidates = Vec::with_capacity(rows.len());
    for (account_id, label, immich_user_id, key) in rows {
        candidates.push(AdminApiCandidate {
            api: api_for(state, server, &account_id, &key).await?,
            account_id,
            label,
            immich_user_id,
        });
    }
    Ok(candidates)
}

/// The first healthy administrator account; needed for config, queues and scans.
pub async fn admin_api(state: &AppState, server: &Server) -> AppResult<Api> {
    Ok(admin_api_candidates(state, server).await?.remove(0).api)
}

/// The account of the Immich user `user_id`. Changes to an asset are always made as its owner.
pub async fn api_as_user(state: &AppState, server: &Server, user_id: &str) -> AppResult<Api> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT id, api_key_enc FROM immich_accounts WHERE server_id = ? AND immich_user_id = ? AND status = 'ok'",
    )
    .bind(&server.id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?;
    let (id, key) = row.ok_or_else(|| {
        AppError::conflict(
            "account_missing",
            "no connected account owns this asset; add the owner's Immich account",
        )
    })?;
    api_for(state, server, &id, &key).await
}

pub async fn account_label(state: &AppState, server_id: &str, user_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT label FROM immich_accounts WHERE server_id = ? AND immich_user_id = ?",
    )
    .bind(server_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten()
    .unwrap_or_else(|| "unconnected user".into())
}

pub async fn library_owner(state: &AppState, library_id: &str) -> AppResult<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT owner_user_id FROM immich_libraries WHERE id = ?")
            .bind(library_id)
            .fetch_optional(&state.db)
            .await?,
    )
}

/// A cached connection pool to the server's PostgreSQL database.
pub async fn pg_pool(state: &AppState, server: &Server) -> AppResult<PgPool> {
    if let Some(pool) = state.pg_pools.lock().await.get(&server.id) {
        return Ok(pool.clone());
    }
    let enc = server.database_url_enc.as_deref().ok_or_else(|| {
        AppError::conflict(
            "direct_writes_unavailable",
            "no database connection configured",
        )
    })?;
    let url = state
        .secrets
        .decrypt(DATABASE_URL_PURPOSE, enc)
        .map_err(|_| AppError::internal("cannot decrypt the database connection"))?;
    let pool = pg::connect(&url).await.map_err(|e| {
        AppError::conflict(
            "direct_writes_unavailable",
            format!("database: {}", crate::error::redact(&e.to_string())),
        )
    })?;
    state
        .pg_pools
        .lock()
        .await
        .insert(server.id.clone(), pool.clone());
    Ok(pool)
}

pub async fn forget_pool(state: &AppState, server_id: &str) {
    if let Some(pool) = state.pg_pools.lock().await.remove(server_id) {
        pool.close().await;
    }
}

/// Checks version and schema, then records the result. Direct writes stay disabled unless both
/// match exactly.
pub async fn check_compatibility(state: &AppState, server_id: &str) -> AppResult<Server> {
    crate::activity::observe(
        state,
        format!("compatibility:{server_id}"),
        "Immich compatibility check",
        server_id,
        Some("/servers"),
        |task| async move {
            task.phase("Checking Immich version, database schema and API capabilities");
            check_compatibility_inner(state, server_id).await
        },
    )
    .await
}

async fn check_compatibility_inner(state: &AppState, server_id: &str) -> AppResult<Server> {
    let server = server(state, server_id).await?;
    let (version, mut problems) = match api::server_version(&state.http, &server.url).await {
        Ok(v) => {
            let mut p = Vec::new();
            if v != SUPPORTED_VERSION {
                p.push(format!(
                    "Immich {v} is not supported (needs {SUPPORTED_VERSION})"
                ));
            }
            (Some(v), p)
        }
        Err(e) => (None, vec![crate::error::redact(&e.to_string())]),
    };
    if server.database_url_enc.is_some() {
        forget_pool(state, &server.id).await;
        match pg_pool(state, &server).await {
            Ok(pool) => match pg::latest_migration(&pool).await {
                Ok(Some(name)) if name == SUPPORTED_MIGRATION => {}
                Ok(Some(name)) => problems.push(format!("unexpected database schema ({name})")),
                Ok(None) => problems.push("database has no Immich migrations".into()),
                Err(e) => problems.push(format!(
                    "database: {}",
                    crate::error::redact(&e.to_string())
                )),
            },
            Err(e) => problems.push(e.message().to_owned()),
        }
    } else {
        problems.push("no database connection configured; direct writes are off".into());
    }
    let compatible = problems.is_empty();
    let detail = if compatible {
        format!("Immich {SUPPORTED_VERSION}, schema verified")
    } else {
        problems.join("; ")
    };
    let external_library_api_capable = match admin_api(state, &server).await {
        Ok(api) => api.external_library_api_capable().await.ok(),
        Err(_) => None,
    };
    sqlx::query(
        "UPDATE immich_servers SET version = ?, compatible = ?, compatibility_detail = ?, \
         external_library_api_capable = ? WHERE id = ?",
    )
    .bind(&version)
    .bind(compatible as i64)
    .bind(&detail)
    .bind(external_library_api_capable.map(i64::from))
    .bind(&server.id)
    .execute(&state.db)
    .await?;
    self::server(state, server_id).await
}

/// Reloads the server's libraries (admin view) and adds ISM's exclusion patterns where missing.
pub async fn refresh_libraries(state: &AppState, server: &Server) -> AppResult<usize> {
    crate::activity::observe(
        state,
        format!("libraries:{}", server.id),
        "Refresh Immich libraries",
        &server.name,
        Some("/servers"),
        |task| async move {
            task.phase("Fetching libraries and reconciling exclusion patterns");
            refresh_libraries_inner(state, server, &task).await
        },
    )
    .await
}

async fn refresh_libraries_inner(
    state: &AppState,
    server: &Server,
    task: &crate::activity::Reporter,
) -> AppResult<usize> {
    let api = admin_api(state, server).await?;
    let libraries = api
        .libraries()
        .await
        .map_err(|e| AppError::conflict("immich_error", e.to_string()))?;
    let now = crate::db::now();
    let wanted = exclusion_patterns();
    let mut tx = state.db.begin().await?;
    sqlx::query("DELETE FROM immich_libraries WHERE server_id = ?")
        .bind(&server.id)
        .execute(&mut *tx)
        .await?;
    for (done, lib) in libraries.iter().enumerate() {
        task.progress(
            done as u64,
            Some(libraries.len() as u64),
            "libraries checked",
        );
        let mut patterns = lib.exclusion_patterns.clone();
        let missing: Vec<String> = wanted
            .iter()
            .filter(|p| !patterns.contains(p))
            .cloned()
            .collect();
        if !missing.is_empty() {
            patterns.extend(missing);
            if let Err(e) = api.set_exclusion_patterns(&lib.id, &patterns).await {
                tracing::warn!(library = %lib.id, "cannot add exclusion patterns: {e}");
                patterns = lib.exclusion_patterns.clone();
            }
        }
        sqlx::query(
            "INSERT INTO immich_libraries (id, server_id, name, owner_user_id, import_paths_json, exclusion_patterns_json, asset_count, refreshed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&lib.id)
        .bind(&server.id)
        .bind(&lib.name)
        .bind(&lib.owner_id)
        .bind(serde_json::to_string(&lib.import_paths)?)
        .bind(serde_json::to_string(&patterns)?)
        .bind(lib.asset_count)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(libraries.len())
}
