//! SQLite state database: connection, migrations, settings and audit helpers.

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection, Sqlite, SqlitePool, Transaction};

const SLOW_WRITER_HOLD: Duration = Duration::from_millis(250);
const SLOW_WRITER_WAIT: Duration = Duration::from_secs(1);

/// One existing immediate transaction, with pool/BEGIN wait separated from body and commit.
/// Dropping an unfinished clock also reports errors or cancellation without logging input data.
pub(crate) struct WriterClock {
    op: &'static str,
    items: u64,
    started: Instant,
    pooled: Option<Instant>,
    locked: Option<Instant>,
    committing: Option<Instant>,
    finished: Option<Instant>,
    stage: &'static str,
}

impl WriterClock {
    pub(crate) fn start(op: &'static str, items: u64) -> Self {
        Self {
            op,
            items,
            started: Instant::now(),
            pooled: None,
            locked: None,
            committing: None,
            finished: None,
            stage: "pool",
        }
    }

    pub(crate) async fn acquire(
        &mut self,
        pool: &SqlitePool,
    ) -> sqlx::Result<sqlx::pool::PoolConnection<Sqlite>> {
        let result = pool.acquire().await;
        if result.is_ok() {
            self.pooled = Some(Instant::now());
            self.stage = "begin";
        } else {
            self.report(false);
        }
        result
    }

    pub(crate) async fn begin<'a>(
        &mut self,
        conn: &'a mut sqlx::SqliteConnection,
    ) -> sqlx::Result<Transaction<'a, Sqlite>> {
        let result = conn.begin_with("BEGIN IMMEDIATE").await;
        if result.is_ok() {
            self.locked = Some(Instant::now());
            self.stage = "body";
        } else {
            self.report(false);
        }
        result
    }

    pub(crate) fn items(&mut self, items: u64) {
        self.items = items;
    }

    pub(crate) async fn commit(&mut self, tx: Transaction<'_, Sqlite>) -> sqlx::Result<()> {
        self.committing = Some(Instant::now());
        self.stage = "commit";
        let result = tx.commit().await;
        self.report(result.is_ok());
        result
    }

    fn times(&self, end: Instant) -> [Duration; 4] {
        let pooled = self.pooled.unwrap_or(end);
        let locked = self.locked.unwrap_or(end);
        let committing = self.committing.unwrap_or(end);
        [
            pooled.duration_since(self.started),
            if self.pooled.is_some() {
                locked.duration_since(pooled)
            } else {
                Duration::ZERO
            },
            if self.locked.is_some() {
                committing.duration_since(locked)
            } else {
                Duration::ZERO
            },
            if self.committing.is_some() {
                end.duration_since(committing)
            } else {
                Duration::ZERO
            },
        ]
    }

    fn report(&mut self, success: bool) {
        self.report_at(Instant::now(), success);
    }

    fn report_at(&mut self, end: Instant, success: bool) {
        self.finished = Some(end);
        let [pool, begin, body, commit] = self.times(end);
        let (pool_wait_ms, begin_wait_ms, body_ms, commit_ms) = (
            pool.as_millis() as u64,
            begin.as_millis() as u64,
            body.as_millis() as u64,
            commit.as_millis() as u64,
        );
        let stage = if success { "complete" } else { self.stage };
        if !success {
            tracing::warn!(
                op = self.op,
                items = self.items,
                stage,
                pool_wait_ms,
                begin_wait_ms,
                body_ms,
                commit_ms,
                "SQLite write failed or abandoned"
            );
        } else if pool + begin >= SLOW_WRITER_WAIT || body + commit >= SLOW_WRITER_HOLD {
            tracing::warn!(
                op = self.op,
                items = self.items,
                stage,
                pool_wait_ms,
                begin_wait_ms,
                body_ms,
                commit_ms,
                "slow SQLite write"
            );
        } else {
            tracing::debug!(
                op = self.op,
                items = self.items,
                stage,
                pool_wait_ms,
                begin_wait_ms,
                body_ms,
                commit_ms,
                "SQLite write"
            );
        }
    }
}

impl Drop for WriterClock {
    fn drop(&mut self) {
        if self.finished.is_none() {
            self.report(false);
        }
    }
}

/// The WAL file is truncated back to this size whenever a checkpoint empties it.
const WAL_SIZE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
/// Above this many WAL pages (4 KiB each, about 128 MiB) the checkpointer asks for a full reset.
const WAL_RESET_PAGES: i64 = 32 * 1024;
const WAL_CHECKPOINT_EVERY: Duration = Duration::from_secs(60);

/// Moves the write-ahead log into the database file. Long readers can stop the automatic
/// checkpoint from ever finishing, which lets the log grow without bound (it reached 3.3 GB).
/// The passive pass never blocks anyone; the reset only runs when the log is large, and waits
/// at most one second for readers so writers are never held back for longer.
pub async fn checkpoint_wal(pool: &SqlitePool) -> sqlx::Result<()> {
    checkpoint_wal_above(pool, WAL_RESET_PAGES).await
}

async fn checkpoint_wal_above(pool: &SqlitePool, reset_pages: i64) -> sqlx::Result<()> {
    let mut conn = pool.acquire().await?;
    let (_, log_pages, _): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_one(&mut *conn)
        .await?;
    if log_pages < reset_pages {
        return Ok(());
    }
    sqlx::query("PRAGMA busy_timeout = 1000")
        .execute(&mut *conn)
        .await?;
    let reset: sqlx::Result<(i64, i64, i64)> = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_one(&mut *conn)
        .await;
    sqlx::query("PRAGMA busy_timeout = 10000")
        .execute(&mut *conn)
        .await?;
    match reset {
        Ok((0, ..)) => tracing::info!(log_pages, "SQLite write-ahead log reset"),
        Ok(_) => tracing::debug!(
            log_pages,
            "SQLite write-ahead log reset skipped: readers busy"
        ),
        Err(e) => tracing::warn!(error = %e, "SQLite write-ahead log reset failed"),
    }
    Ok(())
}

pub async fn run_wal_checkpointer(pool: SqlitePool) {
    loop {
        tokio::time::sleep(WAL_CHECKPOINT_EVERY).await;
        if let Err(e) = checkpoint_wal(&pool).await {
            tracing::warn!(error = %e, "SQLite checkpoint failed");
        }
    }
}

pub async fn connect(data_dir: &Path) -> anyhow::Result<SqlitePool> {
    let path = data_dir.join("ism.sqlite");
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .foreign_keys(true)
        .pragma("journal_size_limit", WAL_SIZE_LIMIT_BYTES.to_string())
        .busy_timeout(std::time::Duration::from_secs(10));
    let pool = SqlitePoolOptions::new()
        .max_connections(16)
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
    use super::WriterClock;
    use crate::test_support::{logs as writer_logs, logs_at as writer_logs_at};
    use sqlx::Connection;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::time::Duration;
    use tracing::instrument::WithSubscriber;

    fn classify(pool_ms: u64, begin_ms: u64, body_ms: u64, commit_ms: u64) -> String {
        let ms = Duration::from_millis;
        let mut clock = WriterClock::start("test_threshold", 2);
        let started = clock.started;
        clock.pooled = Some(started + ms(pool_ms));
        clock.locked = Some(started + ms(pool_ms + begin_ms));
        clock.committing = Some(started + ms(pool_ms + begin_ms + body_ms));
        let end = started + ms(pool_ms + begin_ms + body_ms + commit_ms);
        let (logs, subscriber) = writer_logs_at(tracing::Level::DEBUG);
        tracing::dispatcher::with_default(&subscriber, || clock.report_at(end, true));
        drop(clock);
        String::from_utf8(logs.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn writer_clock_classifies_cumulative_hold_and_wait_thresholds() {
        let slow = classify(0, 0, 125, 125);
        assert!(
            slow.contains("WARN") && slow.contains("slow SQLite write"),
            "{slow}"
        );
        assert!(
            slow.contains("body_ms=125") && slow.contains("commit_ms=125"),
            "{slow}"
        );

        let fast = classify(0, 0, 124, 125);
        assert!(!fast.contains("WARN"), "{fast}");
        assert!(
            fast.contains("DEBUG") && fast.contains("SQLite write"),
            "{fast}"
        );

        let waited = classify(600, 400, 0, 0);
        assert!(
            waited.contains("WARN") && waited.contains("slow SQLite write"),
            "{waited}"
        );
        assert!(waited.contains("pool_wait_ms=600") && waited.contains("begin_wait_ms=400"));

        let short_wait = classify(599, 400, 0, 0);
        assert!(!short_wait.contains("WARN"), "{short_wait}");
        for logs in [slow, fast, waited, short_wait] {
            assert_eq!(logs.lines().count(), 1, "{logs}");
        }
    }

    #[tokio::test]
    async fn checkpointer_empties_a_large_write_ahead_log() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        // Keep the automatic checkpoint from running so the log grows like it does under readers.
        sqlx::query("PRAGMA wal_autocheckpoint = 0")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE wal_probe (id INTEGER PRIMARY KEY, payload BLOB)")
            .execute(&db)
            .await
            .unwrap();
        for _ in 0..200 {
            sqlx::query("INSERT INTO wal_probe (payload) VALUES (zeroblob(8192))")
                .execute(&db)
                .await
                .unwrap();
        }
        let wal = dir.path().join("ism.sqlite-wal");
        assert!(std::fs::metadata(&wal).unwrap().len() > 100_000);
        super::checkpoint_wal_above(&db, 1).await.unwrap();
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
        db.close().await;
    }

    #[tokio::test]
    async fn writer_clock_attributes_pool_exhaustion_without_beginning() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(75))
            .connect_with(db.connect_options().as_ref().clone())
            .await
            .unwrap();
        let held = pool.acquire().await.unwrap();
        let mut clock = WriterClock::start("test_pool_wait", 3);
        let (logs, subscriber) = writer_logs();
        assert!(matches!(
            clock.acquire(&pool).with_subscriber(subscriber).await,
            Err(sqlx::Error::PoolTimedOut)
        ));
        let [pool_wait, begin, body, commit] = clock.times(clock.finished.unwrap());
        assert_eq!(clock.stage, "pool");
        assert!(pool_wait >= Duration::from_millis(50));
        assert_eq!([begin, body, commit], [Duration::ZERO; 3]);
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("SQLite write failed or abandoned").count(), 1);
        assert!(logs.contains("stage=\"pool\""), "{logs}");
        drop(held);
        pool.close().await;
        db.close().await;
    }

    #[tokio::test]
    async fn writer_clock_attributes_sqlite_contention_to_begin_not_body() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                db.connect_options()
                    .as_ref()
                    .clone()
                    .busy_timeout(Duration::from_millis(75)),
            )
            .await
            .unwrap();
        let writer = db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let mut clock = WriterClock::start("test_begin_wait", 7);
        let mut conn = clock.acquire(&pool).await.unwrap();
        let (logs, subscriber) = writer_logs();
        let error = clock
            .begin(&mut conn)
            .with_subscriber(subscriber)
            .await
            .unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(ref e) if e.code().as_deref() == Some("5")));
        let [_, begin, body, commit] = clock.times(clock.finished.unwrap());
        assert_eq!(clock.stage, "begin");
        assert!(begin >= Duration::from_millis(50));
        assert_eq!([body, commit], [Duration::ZERO; 2]);
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("SQLite write failed or abandoned").count(), 1);
        assert!(logs.contains("stage=\"begin\""), "{logs}");
        writer.rollback().await.unwrap();
        drop(conn);
        pool.close().await;
        db.close().await;
    }

    #[tokio::test]
    async fn writer_clock_reports_deferred_commit_failure_and_preserves_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query("CREATE TABLE diagnostic_parent(id INTEGER PRIMARY KEY)")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE diagnostic_child(parent INTEGER REFERENCES diagnostic_parent(id)
             DEFERRABLE INITIALLY DEFERRED)",
        )
        .execute(&db)
        .await
        .unwrap();
        let mut clock = WriterClock::start("test_commit_failure", 1);
        let mut conn = clock.acquire(&db).await.unwrap();
        let mut tx = clock.begin(&mut conn).await.unwrap();
        sqlx::query("INSERT INTO diagnostic_child VALUES(42)")
            .execute(&mut *tx)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (logs, subscriber) = writer_logs();
        let error = clock
            .commit(tx)
            .with_subscriber(subscriber)
            .await
            .unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(ref e) if e.is_foreign_key_violation()));
        let [_, _, body, commit] = clock.times(clock.finished.unwrap());
        assert_eq!(clock.stage, "commit");
        assert!(body >= Duration::from_millis(50));
        assert!(commit > Duration::ZERO);
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("SQLite write failed or abandoned").count(), 1);
        assert!(logs.contains("stage=\"commit\""), "{logs}");
        assert!(!logs.contains("FOREIGN KEY"), "{logs}");
        assert!(!logs.contains("diagnostic_child"), "{logs}");
        // SQLx's original failed-commit rollback is still responsible for atomicity.
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM diagnostic_child")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(count, 0);
        drop(conn);
        db.close().await;
    }

    #[tokio::test]
    async fn writer_clock_reports_body_failure_without_logging_sql_or_input() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query("CREATE TABLE diagnostic_body(input TEXT UNIQUE)")
            .execute(&db)
            .await
            .unwrap();
        let (logs, subscriber) = writer_logs();
        let result: sqlx::Result<()> = async {
            let mut clock = WriterClock::start("test_body_failure", 2);
            let mut conn = clock.acquire(&db).await?;
            let mut tx = clock.begin(&mut conn).await?;
            for _ in 0..2 {
                sqlx::query("INSERT INTO diagnostic_body VALUES(?)")
                    .bind("synthetic-private-payload")
                    .execute(&mut *tx)
                    .await?;
            }
            clock.commit(tx).await
        }
        .with_subscriber(subscriber)
        .await;
        assert!(matches!(result, Err(sqlx::Error::Database(ref e)) if e.is_unique_violation()));
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("SQLite write failed or abandoned").count(), 1);
        assert!(logs.contains("stage=\"body\""), "{logs}");
        assert!(logs.contains("op=\"test_body_failure\""), "{logs}");
        assert!(logs.contains("items=2"), "{logs}");
        assert!(!logs.contains("diagnostic_body"), "{logs}");
        assert!(!logs.contains("synthetic-private-payload"), "{logs}");
        db.close().await;
    }

    #[tokio::test]
    async fn writer_clock_reports_cancelled_body_once_and_releases_writer() {
        let dir = tempfile::tempdir().unwrap();
        let db = super::connect(dir.path()).await.unwrap();
        sqlx::query("CREATE TABLE diagnostic_cancel(input TEXT)")
            .execute(&db)
            .await
            .unwrap();
        let (logs, subscriber) = writer_logs();
        let (ready, entered) = tokio::sync::oneshot::channel();
        let mut work = Box::pin(
            async {
                let mut clock = WriterClock::start("test_cancelled_body", 1);
                let mut conn = clock.acquire(&db).await.unwrap();
                let mut tx = clock.begin(&mut conn).await.unwrap();
                sqlx::query("INSERT INTO diagnostic_cancel VALUES(?)")
                    .bind("synthetic-cancelled-payload")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                ready.send(()).unwrap();
                std::future::pending::<()>().await;
                clock.commit(tx).await.unwrap();
            }
            .with_subscriber(subscriber.clone()),
        );
        tokio::select! {
            result = entered => result.unwrap(),
            _ = &mut work => panic!("work must remain inside the transaction"),
        }
        tracing::dispatcher::with_default(&subscriber, || drop(work));
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("SQLite write failed or abandoned").count(), 1);
        assert!(logs.contains("stage=\"body\""), "{logs}");
        assert!(logs.contains("op=\"test_cancelled_body\""), "{logs}");
        assert!(!logs.contains("diagnostic_cancel"), "{logs}");
        assert!(!logs.contains("synthetic-cancelled-payload"), "{logs}");
        let mut conn = tokio::time::timeout(Duration::from_secs(1), db.acquire())
            .await
            .unwrap()
            .unwrap();
        let tx = conn.begin_with("BEGIN IMMEDIATE").await.unwrap();
        tx.rollback().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM diagnostic_cancel")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(count, 0);
        drop(conn);
        db.close().await;
    }

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
