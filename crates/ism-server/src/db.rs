//! SQLite state database: connection, migrations, settings and audit helpers.

use std::path::Path;
use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

pub async fn connect(data_dir: &Path) -> anyhow::Result<SqlitePool> {
    let path = data_dir.join("ism.sqlite");
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(10));
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    // Unexpired staging might belong to another active connection. It stays unreadable;
    // expiry prevents its publication and makes cleanup safe without stealing its lease.
    sqlx::query("DELETE FROM folder_review_manifests WHERE expires_at <= ?")
        .bind(now())
        .execute(&pool)
        .await?;
    Ok(pool)
}

pub fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn now() -> String {
    ts(Utc::now())
}

pub fn parse_ts(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

pub async fn get_setting(pool: &SqlitePool, key: &str) -> sqlx::Result<Option<String>> {
    sqlx::query_scalar("SELECT value FROM settings WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
}

pub async fn set_setting(pool: &SqlitePool, key: &str, value: &str) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(key)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn audit(pool: &SqlitePool, actor: &str, action: &str, detail: &str) {
    let result = sqlx::query("INSERT INTO audit (at, actor, action, detail) VALUES (?, ?, ?, ?)")
        .bind(now())
        .bind(actor)
        .bind(action)
        .bind(detail)
        .execute(pool)
        .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "could not write audit entry");
    }
}

pub fn json_vec(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn upload_idempotency_migration_preserves_existing_uploads() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query(
            "INSERT INTO uploads (id, principal, request_json, status, created_at)
             VALUES ('old-upload', 'token:example', '{}', 'receiving', '2024-01-01')",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("DROP INDEX uploads_principal_idempotency")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE uploads DROP COLUMN idempotency_key")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 3")
            .execute(&db)
            .await
            .unwrap();
        db.close().await;

        let migrated = super::connect(dir.path()).await.unwrap();
        let row: (String, String, Option<String>) = sqlx::query_as(
            "SELECT id, status, idempotency_key FROM uploads WHERE id = 'old-upload'",
        )
        .fetch_one(&migrated)
        .await
        .unwrap();
        assert_eq!(row, ("old-upload".into(), "receiving".into(), None));
        migrated.close().await;
    }

    #[tokio::test]
    async fn trash_inventory_migration_preserves_existing_entries() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query(
            "INSERT INTO trash (id, root_id, path, quarantine_rel, is_folder, size, trashed_at)
             VALUES ('old-trash', 'photos', 'a.jpg', '.ism-trash/old-trash', 0, 5, '2024-01-01')",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("ALTER TABLE trash DROP COLUMN file_inventory_json")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 2")
            .execute(&db)
            .await
            .unwrap();
        db.close().await;

        let migrated = super::connect(dir.path()).await.unwrap();
        let row: (String, String, Option<String>) = sqlx::query_as(
            "SELECT root_id, path, file_inventory_json FROM trash WHERE id = 'old-trash'",
        )
        .fetch_one(&migrated)
        .await
        .unwrap();
        assert_eq!(row, ("photos".into(), "a.jpg".into(), None));
        migrated.close().await;
    }
}
