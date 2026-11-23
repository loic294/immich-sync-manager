//! Observability only: these records never authorize or schedule filesystem mutations.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use ism_core::dto::{BackgroundActivity, BackgroundActivityPage, BackgroundStatus};

use crate::db;
use crate::error::{AppError, AppResult, redact};
use crate::state::{AppState, ServerEvent};

#[derive(Clone)]
struct Pending {
    activity: BackgroundActivity,
    revision: i64,
    saved_revision: i64,
}

#[derive(Default)]
pub struct Registry(Mutex<HashMap<String, Pending>>, Mutex<Option<String>>);

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(error) => {
            // This state is reporting-only, never an admission or mutation authority.
            tracing::error!("background activity lock poisoned; retaining recorded evidence");
            mutex.clear_poison();
            error.into_inner()
        }
    }
}

pub struct Task {
    reporter: Reporter,
    finished: bool,
}

#[derive(Clone)]
pub struct Reporter {
    state: AppState,
    id: String,
}

impl std::ops::Deref for Task {
    type Target = Reporter;
    fn deref(&self) -> &Reporter {
        &self.reporter
    }
}

impl Task {
    pub async fn start(
        state: &AppState,
        key: impl Into<String>,
        title: &str,
        resource: &str,
        page: Option<&str>,
    ) -> AppResult<Self> {
        let now = db::now();
        let activity = BackgroundActivity {
            id: crate::crypto::new_id("activity"),
            task_key: key.into(),
            title: title.into(),
            resource: resource.into(),
            status: BackgroundStatus::Running,
            phase: "Starting".into(),
            done: 0,
            total: None,
            unit: "items".into(),
            started_at: now.clone(),
            updated_at: now,
            finished_at: None,
            next_at: None,
            error: None,
            operation_id: None,
            page: page.map(str::to_owned),
        };
        let id = activity.id.clone();
        persist(
            state,
            &Pending {
                activity: activity.clone(),
                revision: 0,
                saved_revision: 0,
            },
        )
        .await?;
        lock(&state.activity.0).insert(
            id.clone(),
            Pending {
                activity,
                revision: 0,
                saved_revision: 0,
            },
        );
        state.emit(ServerEvent::ActivityChanged);
        Ok(Self {
            reporter: Reporter {
                state: state.clone(),
                id,
            },
            finished: false,
        })
    }

    pub async fn finish<T>(self, result: &AppResult<T>) -> AppResult<()> {
        self.finish_with_phase(result, "Finished").await
    }

    pub async fn finish_with_phase<T>(
        mut self,
        result: &AppResult<T>,
        completed_phase: &str,
    ) -> AppResult<()> {
        self.update(|a| {
            a.finished_at = Some(db::now());
            match result {
                Ok(_) => {
                    a.status = BackgroundStatus::Completed;
                    a.phase = completed_phase.into();
                }
                Err(e) => {
                    a.status = BackgroundStatus::Failed;
                    a.phase = "Needs attention".into();
                    a.error = Some(redact(e.message()));
                }
            }
        });
        self.finished = true;
        let pending = lock(&self.state.activity.0)
            .get(&self.id)
            .cloned()
            .ok_or_else(|| AppError::internal("active background task missing"))?;
        persist(&self.state, &pending).await?;
        {
            let mut registry = lock(&self.state.activity.0);
            if registry
                .get(&self.id)
                .is_some_and(|p| p.revision == pending.revision)
            {
                registry.remove(&self.id);
            }
        }
        self.state.emit(ServerEvent::ActivityChanged);
        Ok(())
    }
}

impl Reporter {
    fn update(&self, f: impl FnOnce(&mut BackgroundActivity)) {
        let mut registry = lock(&self.state.activity.0);
        if let Some(pending) = registry.get_mut(&self.id) {
            let before = pending.activity.clone();
            f(&mut pending.activity);
            if pending.activity != before {
                pending.activity.updated_at = db::now();
                pending.revision += 1;
            }
        }
    }

    pub fn phase(&self, phase: &str) {
        self.update(|a| {
            a.phase = phase.into();
            a.status = BackgroundStatus::Running;
        });
    }

    pub fn waiting(&self, reason: &str) {
        self.update(|a| {
            a.phase = reason.into();
            a.status = BackgroundStatus::Waiting;
        });
    }

    pub fn progress(&self, done: u64, total: Option<u64>, unit: &str) {
        self.update(|a| {
            a.done = done;
            a.total = total;
            a.unit = unit.into();
        });
    }
}

pub async fn observe<T, F: std::future::Future<Output = AppResult<T>>>(
    state: &AppState,
    key: impl Into<String>,
    title: &str,
    resource: &str,
    page: Option<&str>,
    work: impl FnOnce(Reporter) -> F,
) -> AppResult<T> {
    let task = Task::start(state, key, title, resource, page).await?;
    let result = work(task.reporter.clone()).await;
    if let Err(error) = task.finish(&result).await {
        tracing::error!("cannot record background outcome: {}", error.message());
        return Err(error);
    }
    result
}

impl Drop for Task {
    fn drop(&mut self) {
        if !self.finished {
            self.update(|a| {
                a.status = BackgroundStatus::Interrupted;
                a.phase = "Task stopped before reporting an outcome".into();
                a.finished_at = Some(db::now());
            });
            self.state.emit(ServerEvent::ActivityChanged);
        }
    }
}

fn status(a: &BackgroundActivity) -> &'static str {
    match a.status {
        BackgroundStatus::Running => "running",
        BackgroundStatus::Waiting => "waiting",
        BackgroundStatus::Blocked => "blocked",
        BackgroundStatus::Completed => "completed",
        BackgroundStatus::Failed => "failed",
        BackgroundStatus::Interrupted => "interrupted",
    }
}

async fn persist(state: &AppState, pending: &Pending) -> AppResult<()> {
    let a = &pending.activity;
    let mut tx = state.db.begin().await?;
    sqlx::query(
        "INSERT INTO background_activity(id,task_key,status,revision,updated_at,finished_at,activity_json)
         VALUES (?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,
         revision=excluded.revision,updated_at=excluded.updated_at,finished_at=excluded.finished_at,
         activity_json=excluded.activity_json WHERE background_activity.revision < excluded.revision",
    )
    .bind(&a.id).bind(&a.task_key).bind(status(a)).bind(pending.revision)
    .bind(&a.updated_at).bind(&a.finished_at).bind(serde_json::to_string(a)?)
    .execute(&mut *tx).await?;
    if a.finished_at.is_some() {
        // A newer failure replaces the scoped issue; a successful run resolves it.
        sqlx::query(
            "UPDATE background_activity SET resolved=1 WHERE task_key=? AND id!=?
             AND status IN ('failed','interrupted') AND updated_at <= ?",
        )
        .bind(&a.task_key)
        .bind(&a.id)
        .bind(&a.updated_at)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn recover(state: &AppState) -> AppResult<()> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT activity_json,revision FROM background_activity WHERE status IN ('running','waiting')",
    ).fetch_all(&state.db).await?;
    for (json, revision) in rows {
        let mut activity: BackgroundActivity = serde_json::from_str(&json)?;
        activity.status = BackgroundStatus::Interrupted;
        activity.phase = "ISM restarted before this run reported an outcome; queued work is recovered separately".into();
        activity.updated_at = db::now();
        activity.finished_at = Some(activity.updated_at.clone());
        persist(
            state,
            &Pending {
                activity,
                revision: revision + 1,
                saved_revision: revision,
            },
        )
        .await?;
    }
    Ok(())
}

async fn flush(state: &AppState) -> AppResult<()> {
    let pending: Vec<_> = lock(&state.activity.0)
        .values()
        .filter(|p| p.revision != p.saved_revision || p.activity.finished_at.is_some())
        .cloned()
        .collect();
    for item in &pending {
        persist(state, item).await?;
        {
            let mut registry = lock(&state.activity.0);
            if let Some(p) = registry.get_mut(&item.activity.id) {
                p.saved_revision = item.revision;
            }
        }
        if item.activity.finished_at.is_some() {
            let mut registry = lock(&state.activity.0);
            if registry
                .get(&item.activity.id)
                .is_some_and(|p| p.revision == item.revision)
            {
                registry.remove(&item.activity.id);
            }
        }
    }
    if !pending.is_empty() {
        state.emit(ServerEvent::ActivityChanged);
    }
    Ok(())
}

pub async fn prune(state: &AppState) -> AppResult<()> {
    let cutoff = db::ts(chrono::Utc::now() - chrono::Duration::days(7));
    sqlx::query(
        "DELETE FROM background_activity WHERE (status='completed' OR resolved=1) AND
         (finished_at < ? OR id IN (
            SELECT id FROM background_activity WHERE status='completed' OR resolved=1
            ORDER BY finished_at DESC,id DESC LIMIT -1 OFFSET 1000))",
    )
    .bind(cutoff)
    .execute(&state.db)
    .await?;
    Ok(())
}

pub async fn run(state: AppState) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut retention = 0;
    loop {
        tick.tick().await;
        if let Err(e) = flush(&state).await {
            tracing::error!("background activity persistence failed: {}", e.message());
            *lock(&state.activity.1) = Some(
                "Background activity could not be persisted; progress/history may be stale.".into(),
            );
            state.emit(ServerEvent::ActivityChanged);
        } else {
            *lock(&state.activity.1) = None;
        }
        retention += 1;
        if retention >= 60 {
            retention = 0;
            if let Err(e) = prune(&state).await {
                tracing::error!("background activity retention failed: {}", e.message());
            }
        }
    }
}

pub async fn page(state: &AppState, limit: i64, offset: i64) -> AppResult<BackgroundActivityPage> {
    let json: Vec<String> = sqlx::query_scalar(
        "SELECT activity_json FROM background_activity
         WHERE status IN ('running','waiting','blocked') OR
         (status IN ('failed','interrupted') AND resolved=0) ORDER BY updated_at DESC,id",
    )
    .fetch_all(&state.db)
    .await?;
    let mut current: HashMap<String, BackgroundActivity> = json
        .into_iter()
        .map(|j| serde_json::from_str::<BackgroundActivity>(&j).map(|a| (a.id.clone(), a)))
        .collect::<Result<_, _>>()?;
    for pending in lock(&state.activity.0).values() {
        if pending.activity.status == BackgroundStatus::Completed {
            current.remove(&pending.activity.id);
        } else {
            current.insert(pending.activity.id.clone(), pending.activity.clone());
        }
    }
    let total = sqlx::query_scalar(
        "SELECT COUNT(*) FROM background_activity WHERE status='completed' OR resolved=1",
    )
    .fetch_one(&state.db)
    .await?;
    let history: Vec<String> = sqlx::query_scalar(
        "SELECT activity_json FROM background_activity WHERE status='completed' OR resolved=1
         ORDER BY finished_at DESC,id DESC LIMIT ? OFFSET ?",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;
    let mut current: Vec<_> = current.into_values().collect();
    let blockers = super::activity_blockers::get(state).await?;
    let keys: std::collections::HashSet<_> = current
        .iter()
        .filter(|a| {
            matches!(
                a.status,
                BackgroundStatus::Running | BackgroundStatus::Waiting
            )
        })
        .map(|a| a.task_key.clone())
        .collect();
    current.extend(blockers.into_iter().filter(|a| !keys.contains(&a.task_key)));
    current.sort_by(|a, b| a.started_at.cmp(&b.started_at).then(a.id.cmp(&b.id)));
    Ok(BackgroundActivityPage {
        current,
        history: history
            .into_iter()
            .map(|j| serde_json::from_str(&j))
            .collect::<Result<_, _>>()?,
        total,
        observed_at: db::now(),
        warnings: lock(&state.activity.1).iter().cloned().collect(),
    })
}

pub fn spawn(
    state: &AppState,
    name: &'static str,
    work: impl std::future::Future<Output = ()> + Send + 'static,
) {
    use futures_util::FutureExt;
    let state = state.clone();
    tokio::spawn(async move {
        let result = std::panic::AssertUnwindSafe(work).catch_unwind().await;
        let message = if result.is_err() {
            "Background worker panicked"
        } else {
            "Background worker stopped unexpectedly"
        };
        tracing::error!(worker = name, "{message}");
        let outcome: AppResult<()> =
            Err(crate::error::AppError::conflict("worker_stopped", message));
        match Task::start(
            &state,
            format!("worker:{name}"),
            name,
            "ISM",
            Some("/operations"),
        )
        .await
        {
            Ok(task) => {
                if let Err(e) = task.finish(&outcome).await {
                    tracing::error!("cannot persist stopped worker: {}", e.message());
                }
            }
            Err(e) => tracing::error!("cannot report stopped worker: {}", e.message()),
        }
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{config::Config, crypto::SecretBox};

    async fn harness() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::connect(dir.path()).await.unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            pool,
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        (dir, state)
    }

    #[tokio::test]
    async fn transfer_waits_use_the_same_eligibility_evidence_as_the_worker() {
        use crate::peers::transfer::Eligibility;
        let (dir, state) = harness().await;
        let root = dir.path().join("archive");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(".ism-root"), "photos\n").unwrap();
        sqlx::query("INSERT INTO mappings(root_id,name,container_path,created_at) VALUES ('photos','Archive',?,?)")
                            .bind(root.to_string_lossy().as_ref()).bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO peers(id,name,url,secret_enc,status,created_at) VALUES ('peer','Peer example','http://peer.example','unused','online',?)")
                            .bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled) VALUES ('share','Archive','photos','','[\"peer\"]',1)")
                            .execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
                            VALUES ('download','peer','share','photo.jpg','example',1024,'{}','queued',?,?)")
                            .bind(db::now()).bind(db::now()).execute(&state.db).await.unwrap();
        let share = crate::peers::shares(&state).await.unwrap().remove(0);
        crate::peers::admin::set_schedule(
            &state,
            ism_core::dto::ScheduleView {
                timezone: "UTC".into(),
                windows: vec![ism_core::schedule::Window {
                    days: vec![],
                    start: "00:00".into(),
                    end: "00:00".into(),
                }],
            },
        )
        .await
        .unwrap();
        let decision = crate::peers::transfer::eligibility(
            &state,
            &share,
            "peer",
            "download",
            "photo.jpg",
            0,
            &db::now(),
        )
        .await
        .unwrap();
        let Eligibility::Waiting { reason, .. } = decision else {
            panic!("closed hours must wait")
        };
        assert!(reason.contains("allowed file-transfer hours"));
        assert!(
            page(&state, 25, 0)
                .await
                .unwrap()
                .current
                .iter()
                .any(|a| a.phase == reason)
        );
        sqlx::query("UPDATE transfers SET attempts=2 WHERE id='download'")
            .execute(&state.db)
            .await
            .unwrap();
        let p = page(&state, 25, 0).await.unwrap();
        let transfer = p
            .current
            .iter()
            .find(|a| a.task_key == "transfer:download")
            .unwrap();
        assert!(transfer.phase.contains("backoff"));
        assert!(transfer.next_at.is_some());
        sqlx::query("UPDATE peers SET status='offline' WHERE id='peer'")
            .execute(&state.db)
            .await
            .unwrap();
        assert!(
            page(&state, 25, 0)
                .await
                .unwrap()
                .current
                .iter()
                .any(|a| a.phase.contains("come online"))
        );
        sqlx::query("UPDATE peers SET status='online' WHERE id='peer'")
            .execute(&state.db)
            .await
            .unwrap();
        std::fs::remove_file(root.join(".ism-root")).unwrap();
        let p = page(&state, 25, 0).await.unwrap();
        assert!(
            p.current
                .iter()
                .any(|a| a.status == BackgroundStatus::Blocked && a.phase.contains("not mounted"))
        );
    }

    #[tokio::test]
    async fn current_work_is_independent_of_history_and_unchanged_progress_is_not_written() {
        let (_dir, state) = harness().await;
        let first = Task::start(&state, "scan:example", "Inventory scan", "Archive", None)
            .await
            .unwrap();
        let second = Task::start(&state, "scan:example", "Inventory scan", "Archive", None)
            .await
            .unwrap();
        first.waiting("Waiting for the operation lock");
        second.phase("Enumerating files");
        second.progress(12, None, "files discovered");
        flush(&state).await.unwrap();
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM background_activity WHERE id=?")
                .bind(&second.id)
                .fetch_one(&state.db)
                .await
                .unwrap();
        second.progress(12, None, "files discovered");
        flush(&state).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM background_activity WHERE id=?")
                .bind(&second.id)
                .fetch_one(&state.db)
                .await
                .unwrap(),
            revision
        );
        let p = page(&state, 1, 1000).await.unwrap();
        assert_eq!(p.current.len(), 2);
        assert!(
            p.current
                .iter()
                .any(|a| a.status == BackgroundStatus::Waiting)
        );
        assert!(p.current.iter().any(|a| a.done == 12 && a.total.is_none()));
        second.progress(20, Some(30), "files checked");
        second.finish(&Ok(())).await.unwrap();
        let snapshot = page(&state, 1, 0).await.unwrap();
        assert_eq!(snapshot.current.len(), 1);
        assert_eq!(snapshot.history[0].done, 20);
        first
            .finish_with_phase(&Ok(()), "Attempt deferred; work remains queued")
            .await
            .unwrap();
        assert!(
            page(&state, 25, 0)
                .await
                .unwrap()
                .history
                .iter()
                .any(|a| a.phase == "Attempt deferred; work remains queued")
        );
    }

    #[tokio::test]
    async fn failures_are_scoped_deduplicated_and_resolved_only_by_a_finished_run() {
        let (_dir, state) = harness().await;
        for _ in 0..2 {
            let task = Task::start(&state, "peer:example", "Peer poll", "Peer example", None)
                .await
                .unwrap();
            task.finish(&Err::<(), _>(AppError::conflict(
                "offline",
                "http://user:password@peer.example unreachable",
            )))
            .await
            .unwrap();
        }
        let p = page(&state, 25, 0).await.unwrap();
        assert_eq!(p.current.len(), 1);
        assert_eq!(p.current[0].status, BackgroundStatus::Failed);
        assert!(!p.current[0].error.as_deref().unwrap().contains("password"));
        let retry = Task::start(&state, "peer:example", "Peer poll", "Peer example", None)
            .await
            .unwrap();
        assert_eq!(page(&state, 25, 0).await.unwrap().current.len(), 2);
        retry.finish(&Ok(())).await.unwrap();
        let p = page(&state, 25, 0).await.unwrap();
        assert!(p.current.is_empty());
        assert_eq!(p.total, 3);
    }

    #[tokio::test]
    async fn cancellation_restart_and_late_flush_never_claim_a_task_is_still_running() {
        let (_dir, state) = harness().await;
        let task = Task::start(&state, "scan", "Scan", "Archive", None)
            .await
            .unwrap();
        task.phase("Walking files");
        let stale = lock(&state.activity.0).get(&task.id).cloned().unwrap();
        task.finish(&Ok(())).await.unwrap();
        persist(&state, &stale).await.unwrap();
        assert!(page(&state, 25, 0).await.unwrap().current.is_empty());

        let task = Task::start(&state, "aborted", "Scan", "Archive", None)
            .await
            .unwrap();
        drop(task);
        assert_eq!(
            page(&state, 25, 0).await.unwrap().current[0].status,
            BackgroundStatus::Interrupted
        );
        flush(&state).await.unwrap();
        let task = Task::start(&state, "restart", "Scan", "Archive", None)
            .await
            .unwrap();
        // Lose the in-memory update as a process restart would, retaining the durable start.
        drop(task);
        lock(&state.activity.0).clear();
        recover(&state).await.unwrap();
        let p = page(&state, 25, 0).await.unwrap();
        assert_eq!(p.current.len(), 2);
        assert!(
            p.current
                .iter()
                .all(|a| a.status == BackgroundStatus::Interrupted)
        );
    }

    #[tokio::test]
    async fn retention_bounds_history_without_discarding_running_or_unresolved_work() {
        let (_dir, state) = harness().await;
        let active = Task::start(&state, "active", "Scan", "Archive", None)
            .await
            .unwrap();
        let failed = Task::start(&state, "failed", "Scan", "Archive", None)
            .await
            .unwrap();
        failed
            .finish(&Err::<(), _>(AppError::conflict(
                "failed",
                "Unverified evidence",
            )))
            .await
            .unwrap();
        let base = page(&state, 25, 0)
            .await
            .unwrap()
            .current
            .into_iter()
            .find(|a| a.status == BackgroundStatus::Failed)
            .unwrap();
        let mut tx = state.db.begin().await.unwrap();
        for n in 0..1002 {
            let mut a = base.clone();
            a.id = format!("history-{n:04}");
            a.task_key = a.id.clone();
            a.status = BackgroundStatus::Completed;
            a.finished_at = Some(db::ts(chrono::Utc::now() - chrono::Duration::seconds(n)));
            sqlx::query("INSERT INTO background_activity(id,task_key,status,revision,updated_at,finished_at,activity_json)
                                VALUES (?,?,'completed',1,?,?,?)")
                                .bind(&a.id).bind(&a.task_key).bind(&a.updated_at).bind(&a.finished_at)
                                .bind(serde_json::to_string(&a).unwrap()).execute(&mut *tx).await.unwrap();
        }
        tx.commit().await.unwrap();
        prune(&state).await.unwrap();
        assert_eq!(page(&state, 1, 0).await.unwrap().total, 1000);
        sqlx::query("UPDATE background_activity SET finished_at='2000-01-01T00:00:00Z' WHERE status='completed'")
                            .execute(&state.db).await.unwrap();
        prune(&state).await.unwrap();
        let p = page(&state, 25, 0).await.unwrap();
        assert_eq!(p.total, 0);
        assert_eq!(p.current.len(), 2);
        active.finish(&Ok(())).await.unwrap();
    }
}
