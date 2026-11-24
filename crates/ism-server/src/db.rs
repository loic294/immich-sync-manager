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
    async fn transfer_history_version_migration_leaves_legacy_evidence_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query("ALTER TABLE transfer_history DROP COLUMN vv_json")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version=26")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO peers(id,name,url,secret_enc,status,created_at) VALUES('peer','Peer','https://peer.example','unused','online','2025-01-01')")
            .execute(&db).await.unwrap();
        sqlx::query("INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled) VALUES('share','Archive','photos','','[\"peer\"]',1)")
            .execute(&db).await.unwrap();
        sqlx::query("INSERT INTO transfer_history(id,peer_id,share_id,rel_path,sha256,size,queued_at,finished_at) VALUES('transfer','peer','share','photo.jpg','checksum',12,'2025-01-01','2025-01-02')")
            .execute(&db).await.unwrap();
        db.close().await;

        let migrated = super::connect(dir.path()).await.unwrap();
        let row: (String, i64, Option<String>) =
            sqlx::query_as("SELECT sha256,size,vv_json FROM transfer_history WHERE id='transfer'")
                .fetch_one(&migrated)
                .await
                .unwrap();
        assert_eq!(row, ("checksum".into(), 12, None));
        migrated.close().await;
    }

    #[tokio::test]
    async fn share_scan_failure_paths_migration_preserves_existing_failure() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query("ALTER TABLE share_scan_failures DROP COLUMN paths_json")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version=24")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
             VALUES('archive','Archive','photos','archive','[]',1)",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("INSERT INTO share_scan_failures VALUES('archive','permission','2026-01-02')")
            .execute(&db)
            .await
            .unwrap();
        db.close().await;

        let upgraded = super::connect(dir.path()).await.unwrap();
        let row: (String, String, String) = sqlx::query_as(
            "SELECT reason,observed_at,paths_json FROM share_scan_failures WHERE share_id='archive'",
        )
        .fetch_one(&upgraded)
        .await
        .unwrap();
        assert_eq!(row, ("permission".into(), "2026-01-02".into(), "[]".into()));
        upgraded.close().await;
    }

    #[tokio::test]
    async fn share_scan_failure_migration_preserves_existing_replica_state() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query("DROP TABLE share_scan_failures")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version IN (21,24)")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled,last_scan_at)
             VALUES('archive','Archive','photos','archive','[]',1,'2026-01-01')",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO replica_files(share_id,rel_path,size,mtime_ns,sha256,deleted,vv_json)
             VALUES('archive','photo.jpg',5,123,'synthetic-checksum',0,'{}')",
        )
        .execute(&db)
        .await
        .unwrap();
        db.close().await;

        let upgraded = super::connect(dir.path()).await.unwrap();
        let row: (String, i64, String, i64, String) = sqlx::query_as(
            "SELECT s.last_scan_at,r.size,r.sha256,r.deleted,r.vv_json
             FROM shares s JOIN replica_files r ON r.share_id=s.id WHERE s.id='archive'",
        )
        .fetch_one(&upgraded)
        .await
        .unwrap();
        assert_eq!(
            row,
            (
                "2026-01-01".into(),
                5,
                "synthetic-checksum".into(),
                0,
                "{}".into()
            )
        );
        let failures: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM share_scan_failures")
            .fetch_one(&upgraded)
            .await
            .unwrap();
        assert_eq!(
            failures, 0,
            "an upgrade cannot invent historical scan failures"
        );
        sqlx::query(
            "INSERT INTO share_scan_failures(share_id,reason,observed_at)
             VALUES('archive','permission','2026-01-02')",
        )
        .execute(&upgraded)
        .await
        .unwrap();
        sqlx::query("DELETE FROM shares WHERE id='archive'")
            .execute(&upgraded)
            .await
            .unwrap();
        let failures: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM share_scan_failures")
            .fetch_one(&upgraded)
            .await
            .unwrap();
        assert_eq!(failures, 0);
        upgraded.close().await;
    }

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
