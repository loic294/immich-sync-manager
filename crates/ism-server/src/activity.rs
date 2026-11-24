//! Observability only: these records never authorize or schedule filesystem mutations.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use ism_core::dto::{BackgroundActivity, BackgroundActivityPage, BackgroundStatus};

use crate::db;
use crate::error::{AppResult, redact};
use crate::state::{AppState, ServerEvent};

#[derive(Clone)]
struct Pending {
    activity: BackgroundActivity,
    revision: i64,
    saved_revision: i64,
}

#[derive(Default)]
struct UnreadableReports {
    ids: HashSet<String>,
    suppressed: bool,
}

#[derive(Default)]
pub struct Registry(
    Mutex<HashMap<String, Pending>>,
    Mutex<Option<String>>,
    tokio::sync::Mutex<()>,
    AtomicI64,
    Mutex<UnreadableReports>,
);

impl Registry {
    fn revision(&self) -> i64 {
        self.3.fetch_add(1, Ordering::Relaxed) + 1
    }
}

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
            count: 1,
            reasons: Vec::new(),
            examples: Vec::new(),
        };
        let id = activity.id.clone();
        lock(&state.activity.0).insert(
            id.clone(),
            Pending {
                activity,
                revision: state.activity.revision(),
                saved_revision: -1,
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
        self.state.emit(ServerEvent::ActivityChanged);
        Ok(())
    }
}

impl Reporter {
    fn update(&self, f: impl FnOnce(&mut BackgroundActivity)) {
        let mut registry = lock(&self.state.activity.0);
        if let Some(pending) = registry.get_mut(&self.id) {
            if pending.activity.finished_at.is_some() {
                return;
            }
            let before = pending.activity.clone();
            f(&mut pending.activity);
            if pending.activity != before {
                pending.activity.updated_at = db::now();
                pending.revision = self.state.activity.revision();
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

#[derive(sqlx::FromRow)]
struct SavedActivity {
    id: String,
    task_key: String,
    status: String,
    revision: i64,
    updated_at: String,
    finished_at: Option<String>,
    activity_json: String,
}

impl SavedActivity {
    fn decode(&self) -> Option<BackgroundActivity> {
        let activity: BackgroundActivity = serde_json::from_str(&self.activity_json).ok()?;
        (activity.id == self.id
            && activity.task_key == self.task_key
            && status(&activity) == self.status)
            .then_some(activity)
    }

    fn placeholder(&self) -> BackgroundActivity {
        let status = match self.status.as_str() {
            "completed" => BackgroundStatus::Completed,
            "failed" => BackgroundStatus::Failed,
            "interrupted" => BackgroundStatus::Interrupted,
            _ => BackgroundStatus::Blocked,
        };
        BackgroundActivity {
            id: self.id.clone(),
            task_key: self.task_key.clone(),
            title: "Unreadable saved activity".into(),
            resource: "ISM".into(),
            status,
            phase: "Saved activity could not be read; see server logs".into(),
            done: 0,
            total: None,
            unit: "items".into(),
            started_at: String::new(),
            updated_at: self.updated_at.clone(),
            finished_at: self.finished_at.clone(),
            next_at: None,
            error: None,
            operation_id: None,
            page: Some("/operations".into()),
            count: 1,
            reasons: Vec::new(),
            examples: Vec::new(),
        }
    }
}

fn report_unreadable(state: &AppState, id: &str) {
    const LIMIT: usize = 100;
    let mut reports = lock(&state.activity.4);
    if reports.ids.contains(id) {
        return;
    }
    if reports.ids.len() < LIMIT {
        reports.ids.insert(id.to_owned());
        tracing::error!(
            activity_id = id,
            "saved background activity is unreadable or inconsistent; raw data retained"
        );
    } else if !reports.suppressed {
        reports.suppressed = true;
        tracing::error!("additional unreadable background activity records will not be logged");
    }
}

async fn persist(state: &AppState, pending: &Pending) -> AppResult<()> {
    let a = &pending.activity;
    let mut tx = state.db.begin().await?;
    let written = sqlx::query(
        "INSERT INTO background_activity(id,task_key,status,revision,updated_at,finished_at,activity_json)
         VALUES (?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,
         revision=excluded.revision,updated_at=excluded.updated_at,finished_at=excluded.finished_at,
         activity_json=excluded.activity_json WHERE background_activity.revision < excluded.revision",
    )
    .bind(&a.id).bind(&a.task_key).bind(status(a)).bind(pending.revision)
    .bind(&a.updated_at).bind(&a.finished_at).bind(serde_json::to_string(a)?)
    .execute(&mut *tx).await?.rows_affected();
    if a.finished_at.is_some() && written != 0 {
        // Resolve in both directions so delayed/retried writes cannot reopen an older issue.
        // Registry revisions order outcomes even when finish timestamps share a millisecond.
        sqlx::query(
            "UPDATE background_activity SET resolved=1 WHERE task_key=? AND id!=?
             AND status IN ('failed','interrupted')
             AND (finished_at < ? OR (finished_at = ? AND revision < ?))",
        )
        .bind(&a.task_key)
        .bind(&a.id)
        .bind(&a.finished_at)
        .bind(&a.finished_at)
        .bind(pending.revision)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE background_activity SET resolved=1 WHERE id=?
             AND status IN ('failed','interrupted') AND EXISTS (
                 SELECT 1 FROM background_activity newer WHERE newer.task_key=?
                 AND (newer.finished_at > ? OR (newer.finished_at = ? AND newer.revision > ?)))",
        )
        .bind(&a.id)
        .bind(&a.task_key)
        .bind(&a.finished_at)
        .bind(&a.finished_at)
        .bind(pending.revision)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn recover(state: &AppState) -> AppResult<()> {
    let revision: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(revision),0) FROM background_activity")
            .fetch_one(&state.db)
            .await?;
    state.activity.3.fetch_max(revision, Ordering::Relaxed);
    let rows: Vec<SavedActivity> = sqlx::query_as(
        "SELECT id,task_key,status,revision,updated_at,finished_at,activity_json
         FROM background_activity WHERE status IN ('running','waiting')",
    )
    .fetch_all(&state.db)
    .await?;
    for row in rows {
        let Some(mut activity) = row.decode() else {
            report_unreadable(state, &row.id);
            continue;
        };
        activity.status = BackgroundStatus::Interrupted;
        activity.phase = "ISM restarted before this run reported an outcome; queued work is recovered separately".into();
        activity.updated_at = db::now();
        activity.finished_at = Some(activity.updated_at.clone());
        persist(
            state,
            &Pending {
                activity,
                revision: state.activity.revision(),
                saved_revision: row.revision,
            },
        )
        .await?;
    }
    Ok(())
}

async fn flush(state: &AppState) -> AppResult<()> {
    let _flush = state.activity.2.lock().await;
    let pending: Vec<_> = lock(&state.activity.0)
        .values()
        .filter(|p| p.revision != p.saved_revision || p.activity.finished_at.is_some())
        .cloned()
        .collect();
    for item in &pending {
        if let Err(error) = persist(state, item).await {
            tracing::error!(
                "background activity persistence failed: {}",
                error.message()
            );
            *lock(&state.activity.1) = Some(
                "Background activity persistence failed; unsaved evidence is only in memory and will be retried."
                    .into(),
            );
            state.emit(ServerEvent::ActivityChanged);
            return Err(error);
        }
        {
            let mut registry = lock(&state.activity.0);
            if let Some(p) = registry.get_mut(&item.activity.id) {
                p.saved_revision = p.saved_revision.max(item.revision);
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
    *lock(&state.activity.1) = None;
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
            ORDER BY finished_at DESC,revision DESC,id DESC LIMIT -1 OFFSET 1000))",
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
        let _ = flush(&state).await; // flush reports failures and retains evidence for retry.
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
    let rows: Vec<SavedActivity> = sqlx::query_as(
        "SELECT id,task_key,status,revision,updated_at,finished_at,activity_json FROM background_activity
         WHERE status IN ('running','waiting','blocked') OR
         (status IN ('failed','interrupted') AND resolved=0) ORDER BY updated_at DESC,id",
    )
    .fetch_all(&state.db)
    .await?;
    let mut current = HashMap::new();
    let mut unreadable = Vec::new();
    for row in rows {
        if !state
            .worker_evidence
            .current_record(&row.task_key, row.revision)
        {
            continue;
        }
        if let Some(activity) = row.decode() {
            current.insert(activity.id.clone(), activity);
        } else {
            report_unreadable(state, &row.id);
            unreadable.push(row.placeholder());
        }
    }
    let mut warnings: Vec<_> = lock(&state.activity.1).iter().cloned().collect();
    {
        let registry = lock(&state.activity.0);
        if registry.values().any(|p| p.revision != p.saved_revision) {
            warnings.push(
                "Some background activity updates are not yet persisted; in-memory evidence can be lost if ISM stops."
                    .into(),
            );
        }
        for pending in registry.values() {
            if state
                .worker_evidence
                .current_record(&pending.activity.task_key, pending.revision)
            {
                current.insert(pending.activity.id.clone(), pending.activity.clone());
            }
        }
    }
    let total = sqlx::query_scalar(
        "SELECT COUNT(*) FROM background_activity WHERE status='completed' OR resolved=1",
    )
    .fetch_one(&state.db)
    .await?;
    let history: Vec<SavedActivity> = sqlx::query_as(
        "SELECT id,task_key,status,revision,updated_at,finished_at,activity_json
         FROM background_activity WHERE status='completed' OR resolved=1
         ORDER BY finished_at DESC,revision DESC,id DESC LIMIT ? OFFSET ?",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;
    let mut current: Vec<_> = current.into_values().collect();
    let mut has_unreadable = !unreadable.is_empty();
    current.extend(unreadable);
    let history = history
        .into_iter()
        .map(|row| match row.decode() {
            Some(activity) => activity,
            None => {
                has_unreadable = true;
                report_unreadable(state, &row.id);
                row.placeholder()
            }
        })
        .collect();
    if has_unreadable {
        warnings.push(
            "Some saved background activity records could not be read; see server logs.".into(),
        );
    }
    let (blockers, evidence_warnings) = super::activity_blockers::get(state).await?;
    warnings.extend(evidence_warnings);
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
        history,
        total,
        observed_at: db::now(),
        warnings,
    })
}

pub(crate) fn worker_started(state: &AppState, name: &str, kind: crate::worker_evidence::Kind) {
    let revision = state.activity.revision();
    state.worker_evidence.started(name, kind, revision);
    state.emit(ServerEvent::ActivityChanged);
    let state = state.clone();
    let key = format!("worker:{name}");
    tokio::spawn(async move {
        match sqlx::query(
            "UPDATE background_activity SET resolved=1
             WHERE task_key=? AND status IN ('failed','interrupted') AND revision < ?",
        )
        .bind(&key)
        .bind(revision)
        .execute(&state.db)
        .await
        {
            Ok(result) if result.rows_affected() > 0 => state.emit(ServerEvent::ActivityChanged),
            Ok(_) => {}
            Err(error) => tracing::error!(
                worker = %key,
                "cannot resolve previous worker evidence: {}",
                redact(&error.to_string())
            ),
        }
    });
}

pub(crate) async fn worker_stopped(state: &AppState, name: &str, message: &str) {
    state.worker_evidence.stopped(name, message, db::now());
    state.emit(ServerEvent::ActivityChanged);
    let outcome: AppResult<()> = Err(crate::error::AppError::conflict("worker_stopped", message));
    match Task::start(
        state,
        format!("worker:{name}"),
        name,
        "ISM",
        Some("/operations"),
    )
    .await
    {
        Ok(task) => {
            if let Err(error) = task.finish(&outcome).await {
                tracing::error!("cannot persist stopped worker: {}", error.message());
            }
        }
        Err(error) => tracing::error!("cannot report stopped worker: {}", error.message()),
    }
}

pub(crate) fn transfer_stopped(
    state: &AppState,
    id: &str,
    reason: crate::worker_evidence::TransferStop,
) {
    state
        .worker_evidence
        .transfer_stopped(id, reason, state.activity.revision());
    state.emit(ServerEvent::ActivityChanged);
}

pub(crate) fn spawn(
    state: &AppState,
    name: &'static str,
    kind: crate::worker_evidence::Kind,
    work: impl std::future::Future<Output = ()> + Send + 'static,
) {
    use futures_util::FutureExt;
    worker_started(state, name, kind);
    let state = state.clone();
    tokio::spawn(async move {
        let result = std::panic::AssertUnwindSafe(work).catch_unwind().await;
        let message = if result.is_err() {
            "Background worker panicked"
        } else {
            "Background worker stopped unexpectedly"
        };
        tracing::error!(worker = name, "{message}");
        worker_stopped(&state, name, message).await;
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::error::AppError;
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
    async fn only_queued_and_observed_share_scans_are_reported() {
        let (_dir, state) = harness().await;
        for id in ["queued", "observed", "idle"] {
            sqlx::query(
                "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
                 VALUES (?,?,'photos','','[]',1)",
            )
            .bind(id)
            .bind(id)
            .execute(&state.db)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES ('operation','operation','admin','admin','{}','running',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        state
            .share_changes
            .lock()
            .unwrap()
            .insert("queued".into(), std::time::Instant::now());
        let scan = Task::start(
            &state,
            "share:observed",
            "Share inventory scan",
            "observed",
            Some("/peers"),
        )
        .await
        .unwrap();
        scan.waiting("Waiting for the observed scan's admission");
        let current = page(&state, 25, 0).await.unwrap().current;
        let queued = current
            .iter()
            .find(|row| row.task_key == "share:queued")
            .unwrap();
        assert!(queued.phase.contains("readiness has not been checked"));
        let observed = current
            .iter()
            .find(|row| row.task_key == "share:observed")
            .unwrap();
        assert_eq!(observed.phase, "Waiting for the observed scan's admission");
        assert!(current.iter().all(|row| row.task_key != "share:idle"));
        scan.finish(&Ok(())).await.unwrap();
    }

    #[tokio::test]
    async fn stopped_worker_is_reported_only_until_its_runtime_restarts() {
        use ism_core::dto::{AttentionCode, AttentionState};

        let (dir, state) = harness().await;
        sqlx::query(
            "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES ('queued','queued','admin','admin','{}','queued',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        spawn(
            &state,
            "Operation executor",
            crate::worker_evidence::Kind::Operations,
            async { panic!("synthetic worker failure") },
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if page(&state, 25, 0)
                    .await
                    .unwrap()
                    .current
                    .iter()
                    .any(|row| row.task_key == "worker:Operation executor")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let snapshot = crate::attention::snapshot(&state).await.unwrap();
        let failure = snapshot
            .items
            .iter()
            .find(|item| item.id == "worker:Operation executor")
            .unwrap();
        assert_eq!(failure.code, AttentionCode::WorkerStopped);
        assert_eq!(failure.state, AttentionState::Failed);
        assert!(failure.blocks_work);
        assert_eq!(failure.count, 1);
        assert!(
            snapshot
                .items
                .iter()
                .all(|item| item.state != AttentionState::Stalled)
        );
        flush(&state).await.unwrap();

        let restarted = AppState::new(
            Config::for_tests(dir.path()),
            state.db.clone(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        recover(&restarted).await.unwrap();
        spawn(
            &restarted,
            "Operation executor",
            crate::worker_evidence::Kind::Operations,
            std::future::pending(),
        );
        assert!(
            !crate::attention::snapshot(&restarted)
                .await
                .unwrap()
                .items
                .iter()
                .any(|item| item.id == "worker:Operation executor")
        );
        assert!(
            !page(&restarted, 25, 0)
                .await
                .unwrap()
                .current
                .iter()
                .any(|row| row.task_key == "worker:Operation executor")
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let resolved: bool = sqlx::query_scalar(
                    "SELECT resolved FROM background_activity WHERE task_key='worker:Operation executor'",
                )
                .fetch_one(&restarted.db)
                .await
                .unwrap();
                if resolved {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn peer_child_exit_is_reported_while_the_other_child_keeps_running() {
        use ism_core::dto::AttentionCode;

        let (_dir, state) = harness().await;
        let (started, ready) = tokio::sync::oneshot::channel();
        let supervisor = tokio::spawn(crate::peers::supervise(
            state.clone(),
            async { panic!("synthetic peer child failure") },
            async move {
                let _ = started.send(());
                std::future::pending::<()>().await;
            },
            std::future::pending(),
        ));
        ready.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state
                    .worker_evidence
                    .stopped_workers()
                    .iter()
                    .any(|worker| worker.name == "Peer replication")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!supervisor.is_finished());
        let snapshot = crate::attention::snapshot(&state).await.unwrap();
        let failure = snapshot
            .items
            .iter()
            .find(|item| item.id == "worker:Peer replication")
            .unwrap();
        assert_eq!(failure.code, AttentionCode::WorkerStopped);
        assert!(!failure.blocks_work);
        assert!(
            !snapshot
                .items
                .iter()
                .any(|item| item.id == "worker:Peer activity mirror")
        );

        sqlx::query(
            "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
             VALUES ('peer','Peer example','https://peer.example','unused','online',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
             VALUES ('share','Archive','photos','','[\"peer\"]',1)",
        )
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO share_backfills(share_id,peer_id,requested_at) VALUES ('share','peer',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let snapshot = crate::attention::snapshot(&state).await.unwrap();
        let failure = snapshot
            .items
            .iter()
            .find(|item| item.id == "worker:Peer replication")
            .unwrap();
        assert!(failure.blocks_work);
        assert_eq!(failure.count, 1);

        sqlx::query(
            "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
             VALUES ('operation','operation','admin','admin','{}','queued',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let event = serde_json::json!({
            "seq": 1,
            "origin": "remote",
            "origin_seq": 1,
            "share": "Archive",
            "kind": "folder_created",
            "data": { "path": "folder", "vv": {} }
        });
        sqlx::query(
            "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json,status,operation_id)
             VALUES ('peer',1,'remote',1,'Archive',?,'applying','operation')",
        )
        .bind(event.to_string())
        .execute(&state.db)
        .await
        .unwrap();
        let snapshot = crate::attention::snapshot(&state).await.unwrap();
        let failure = snapshot
            .items
            .iter()
            .find(|item| item.id == "worker:Peer replication")
            .unwrap();
        assert_eq!(
            failure.count, 1,
            "an operation already handed to the executor may finish independently"
        );
        supervisor.abort();
        let _ = supervisor.await;
    }

    #[tokio::test]
    async fn conflict_comparisons_report_queued_retries_and_observed_worker_exits() {
        use ism_core::dto::{AttentionCode, AttentionState};

        let (_dir, state) = harness().await;
        sqlx::query(
            "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
             VALUES ('peer','Peer example','https://peer.example','unused','online',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
             VALUES ('share','Archive','photos','','[\"peer\"]',1)",
        )
        .execute(&state.db)
        .await
        .unwrap();
        for (id, status, error, detail, next, generation) in [
            ("queued-a", "queued", None, None, "", 0),
            ("queued-b", "queued", None, None, "", 0),
            (
                "retry",
                "queued",
                Some("peer_busy"),
                Some("The peer did not respond"),
                "2099-01-01T00:00:00.000Z",
                0,
            ),
            (
                "large",
                "blocked",
                Some("version_evidence_too_large"),
                Some("The folder exceeds the comparison limit"),
                "2099-01-01T00:00:00.000Z",
                0,
            ),
            ("active", "running", None, None, "", 7),
            ("unknown", "running", None, None, "", 2),
            ("stopped", "running", None, None, "", 3),
        ] {
            sqlx::query(
                "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,remote_vv_json,peer_id,status,created_at)
                 VALUES (?,'share',?,'changed_on_both','','{}','{}','{}','peer','open',?)",
            )
            .bind(id)
            .bind(format!("{id}.example.jpg"))
            .bind(db::now())
            .execute(&state.db)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE conflict_evidence_jobs SET state=?,error=?,error_detail=?,next_attempt=?,generation=?
                 WHERE target_id=?",
            )
            .bind(status)
            .bind(error)
            .bind(detail)
            .bind(next)
            .bind(generation)
            .bind(id)
            .execute(&state.db)
            .await
            .unwrap();
        }
        state
            .worker_evidence
            .started("old worker", crate::worker_evidence::Kind::Other, 0);
        state
            .worker_evidence
            .conflict_claimed("old worker", "unknown", 1);
        let failing = state.clone();
        let running = state.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let supervisor = tokio::spawn(crate::peers::versions::supervise_workers(
            state.clone(),
            async move {
                failing.worker_evidence.conflict_claimed(
                    "Conflict evidence worker 1",
                    "stopped",
                    3,
                );
                panic!("synthetic comparison worker failure");
            },
            async move {
                running
                    .worker_evidence
                    .conflict_claimed("Conflict evidence worker 2", "active", 7);
                let _ = started.send(());
                std::future::pending::<()>().await;
            },
            std::future::pending(),
        ));
        ready.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state
                    .worker_evidence
                    .stopped_workers()
                    .iter()
                    .any(|worker| worker.name == "Conflict evidence worker 1")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!supervisor.is_finished());
        let current = page(&state, 25, 0).await.unwrap();
        let find = |key: &str| {
            current
                .current
                .iter()
                .find(|row| row.task_key == key)
                .unwrap()
        };
        let queued = find("conflict-evidence:queued:none");
        assert_eq!(queued.count, 2);
        assert_eq!(queued.status, BackgroundStatus::Waiting);
        let retry = find("conflict-evidence:queued:peer_busy");
        assert_eq!(retry.status, BackgroundStatus::Waiting);
        assert_eq!(retry.next_at.as_deref(), Some("2099-01-01T00:00:00.000Z"));
        assert_eq!(retry.error.as_deref(), Some("The peer did not respond"));
        let blocked = find("conflict-evidence:blocked:version_evidence_too_large");
        assert_eq!(blocked.status, BackgroundStatus::Blocked);
        assert!(blocked.next_at.is_none());
        assert_eq!(
            find("conflict-evidence:running").status,
            BackgroundStatus::Running
        );
        assert_eq!(
            find("conflict-evidence:unverified").status,
            BackgroundStatus::Waiting
        );
        let stopped = find("conflict-evidence:stopped");
        assert_eq!(stopped.status, BackgroundStatus::Blocked);
        assert_eq!(stopped.count, 1);
        assert_eq!(stopped.examples[0].resource, "stopped.example.jpg");
        assert!(
            current
                .warnings
                .iter()
                .any(|warning| warning.contains("verifiable"))
        );

        let attention = crate::attention::snapshot(&state).await.unwrap();
        let stopped = attention
            .items
            .iter()
            .find(|item| item.id == "conflict-evidence:stopped")
            .unwrap();
        assert_eq!(stopped.code, AttentionCode::WorkerStopped);
        assert_eq!(stopped.state, AttentionState::Failed);
        assert!(stopped.blocks_work);
        assert_eq!(stopped.count, 1);
        assert_eq!(stopped.href, "/conflicts");
        let blocked = attention
            .items
            .iter()
            .find(|item| item.id == "conflict-evidence:blocked:version_evidence_too_large")
            .unwrap();
        assert!(blocked.blocks_work);
        assert!(blocked.next_at.is_none());
        let unverified = attention
            .items
            .iter()
            .find(|item| item.id == "conflict-evidence:unverified")
            .unwrap();
        assert!(!unverified.blocks_work);
        assert_eq!(unverified.state, AttentionState::Unavailable);

        for next in ["", "invalid"] {
            sqlx::query(
                "UPDATE conflict_evidence_jobs SET error='peer_busy',error_detail='The peer did not respond',next_attempt=?
                 WHERE target_id='queued-a'",
            )
            .bind(next)
            .execute(&state.db)
            .await
            .unwrap();
            let mixed_retries = page(&state, 25, 0).await.unwrap();
            let mixed = mixed_retries
                .current
                .iter()
                .find(|row| row.task_key == "conflict-evidence:queued:peer_busy")
                .unwrap();
            assert_eq!(mixed.count, 2);
            assert!(mixed.next_at.is_none());
        }

        sqlx::query(
            "UPDATE conflict_evidence_jobs SET state='queued',generation=generation+1 WHERE target_id='stopped'",
        )
        .execute(&state.db)
        .await
        .unwrap();
        assert!(
            page(&state, 25, 0)
                .await
                .unwrap()
                .current
                .iter()
                .all(|row| row.task_key != "conflict-evidence:stopped")
        );
        supervisor.abort();
        let _ = supervisor.await;
        state
            .worker_evidence
            .conflict_claimed("Conflict evidence worker 2", "active", 7);
        let after_abort = page(&state, 25, 0).await.unwrap();
        let stopped = after_abort
            .current
            .iter()
            .find(|row| row.task_key == "conflict-evidence:stopped")
            .unwrap();
        assert_eq!(stopped.count, 1);
        assert_eq!(stopped.examples[0].resource, "active.example.jpg");
        let attention = crate::attention::snapshot(&state).await.unwrap();
        for key in [
            "conflict-evidence:queued:none",
            "conflict-evidence:queued:peer_busy",
        ] {
            let queued = after_abort
                .current
                .iter()
                .find(|row| row.task_key == key)
                .unwrap();
            assert_eq!(queued.status, BackgroundStatus::Blocked);
            assert!(queued.phase.contains("worker stopped"));
            assert!(queued.next_at.is_none());
            let item = attention.items.iter().find(|item| item.id == key).unwrap();
            assert_eq!(item.code, AttentionCode::WorkerStopped);
            assert!(item.blocks_work);
        }
    }

    #[tokio::test]
    async fn unreadable_records_do_not_stop_restart_or_hide_other_activity() {
        let (dir, state) = harness().await;
        let task = Task::start(&state, "healthy", "Healthy work", "Archive", None)
            .await
            .unwrap();
        flush(&state).await.unwrap();
        for (id, status, finished_at, json) in [
            ("bad-current", "running", None, "not json"),
            ("bad-history", "completed", Some(db::now()), "{}"),
        ] {
            sqlx::query(
                "INSERT INTO background_activity
                 (id,task_key,status,revision,updated_at,finished_at,activity_json)
                 VALUES (?,?,?,0,?,?,?)",
            )
            .bind(id)
            .bind(id)
            .bind(status)
            .bind(db::now())
            .bind(finished_at)
            .bind(json)
            .execute(&state.db)
            .await
            .unwrap();
        }
        drop(task);
        let restarted = AppState::new(
            Config::for_tests(dir.path()),
            state.db.clone(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();

        let before_recovery = page(&restarted, 25, 0).await;
        let recovery = recover(&restarted).await;
        assert!(
            before_recovery.is_ok() && recovery.is_ok(),
            "snapshot error: {:?}; recovery error: {:?}",
            before_recovery.err(),
            recovery.err()
        );
        let snapshot = page(&restarted, 25, 0).await.unwrap();
        assert_eq!(snapshot.total, 1);
        assert!(
            snapshot
                .warnings
                .iter()
                .any(|warning| warning.contains("could not be read"))
        );
        assert!(snapshot.current.iter().any(|entry| {
            entry.task_key == "healthy" && entry.status == BackgroundStatus::Interrupted
        }));
        assert!(snapshot.current.iter().any(|entry| {
            entry.task_key == "bad-current"
                && entry.status == BackgroundStatus::Blocked
                && entry.phase.contains("read")
                && entry.started_at.is_empty()
        }));
        assert!(snapshot.history.iter().any(|entry| {
            entry.task_key == "bad-history"
                && entry.status == BackgroundStatus::Completed
                && entry.phase.contains("read")
                && entry.started_at.is_empty()
        }));
        let saved: String = sqlx::query_scalar(
            "SELECT activity_json FROM background_activity WHERE id='bad-current'",
        )
        .fetch_one(&state.db)
        .await
        .unwrap();
        assert_eq!(saved, "not json");
        let history: String = sqlx::query_scalar(
            "SELECT activity_json FROM background_activity WHERE id='bad-history'",
        )
        .fetch_one(&state.db)
        .await
        .unwrap();
        assert_eq!(history, "{}");
    }

    #[tokio::test]
    async fn inconsistent_record_identity_does_not_replace_other_activity() {
        let (dir, state) = harness().await;
        let task = Task::start(&state, "healthy", "Healthy work", "Archive", None)
            .await
            .unwrap();
        flush(&state).await.unwrap();
        let healthy_json: String =
            sqlx::query_scalar("SELECT activity_json FROM background_activity WHERE id=?")
                .bind(&task.reporter.id)
                .fetch_one(&state.db)
                .await
                .unwrap();
        let healthy: serde_json::Value = serde_json::from_str(&healthy_json).unwrap();
        let mut mismatches = Vec::new();
        for (id, task_key, status, mismatch) in [
            ("wrong-id", "healthy", "running", "id"),
            ("wrong-key", "stored-key", "running", "task_key"),
            ("wrong-status", "wrong-status", "waiting", "status"),
        ] {
            let mut json = healthy.clone();
            if mismatch != "id" {
                json["id"] = id.into();
            }
            if mismatch != "task_key" {
                json["task_key"] = task_key.into();
            }
            if mismatch != "status" {
                json["status"] = status.into();
            }
            let json = serde_json::to_string(&json).unwrap();
            sqlx::query(
                "INSERT INTO background_activity
                 (id,task_key,status,revision,updated_at,activity_json)
                 VALUES (?,?,?,10000,?,?)",
            )
            .bind(id)
            .bind(task_key)
            .bind(status)
            .bind(db::now())
            .bind(&json)
            .execute(&state.db)
            .await
            .unwrap();
            mismatches.push((id, task_key, json));
        }
        drop(task);
        let restarted = AppState::new(
            Config::for_tests(dir.path()),
            state.db.clone(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();

        recover(&restarted).await.unwrap();
        let snapshot = page(&restarted, 25, 0).await.unwrap();
        assert!(snapshot.current.iter().any(|entry| {
            entry.task_key == "healthy" && entry.status == BackgroundStatus::Interrupted
        }));
        for (id, task_key, original) in mismatches {
            assert!(snapshot.current.iter().any(|entry| {
                entry.id == id
                    && entry.task_key == task_key
                    && entry.status == BackgroundStatus::Blocked
                    && entry.phase.contains("read")
            }));
            let saved: String =
                sqlx::query_scalar("SELECT activity_json FROM background_activity WHERE id=?")
                    .bind(id)
                    .fetch_one(&state.db)
                    .await
                    .unwrap();
            assert_eq!(saved, original);
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM background_activity WHERE task_key='healthy' AND id!=?",
        )
        .bind("wrong-id")
        .fetch_one(&state.db)
        .await
        .unwrap();
        assert!(revision > 10000);
    }

    #[tokio::test]
    async fn observed_work_and_outcomes_do_not_wait_for_another_sqlite_writer() {
        let (_dir, state) = harness().await;
        let mut writer = state.db.begin().await.unwrap();
        sqlx::query("INSERT INTO settings(key,value) VALUES ('writer-lock','held')")
            .execute(&mut *writer)
            .await
            .unwrap();
        let mut reporter = None;
        let observed_state = &state;
        let outcome = tokio::time::timeout(
            Duration::from_millis(250),
            observe(&state, "contention", "Scan", "Archive", None, |r| {
                reporter = Some(r.clone());
                async move {
                    r.progress(3, Some(3), "files");
                    let snapshot = page(observed_state, 25, 0).await.unwrap();
                    assert_eq!(snapshot.current[0].status, BackgroundStatus::Running);
                    assert_eq!(snapshot.current[0].done, 3);
                    Err::<(), _>(AppError::conflict("actual_failure", "Actual work failed"))
                }
            }),
        )
        .await
        .expect("activity recording must not wait for the SQLite writer");
        assert_eq!(outcome.unwrap_err().message(), "Actual work failed");
        let snapshot = page(&state, 25, 0).await.unwrap();
        assert_eq!(snapshot.current[0].status, BackgroundStatus::Failed);
        assert_eq!(snapshot.current[0].done, 3);
        assert!(!snapshot.warnings.is_empty());
        let reporter = reporter.unwrap();
        reporter.phase("Late callback");
        reporter.progress(0, None, "files");
        assert_eq!(
            page(&state, 25, 0).await.unwrap().current[0].status,
            BackgroundStatus::Failed
        );
        let completed = Task::start(&state, "completed", "Scan", "Archive", None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(250), completed.finish(&Ok(())))
            .await
            .expect("finishing must not wait for the SQLite writer")
            .unwrap();
        assert!(
            page(&state, 25, 0)
                .await
                .unwrap()
                .current
                .iter()
                .any(|a| a.status == BackgroundStatus::Completed)
        );
        writer.rollback().await.unwrap();
        flush(&state).await.unwrap();
        let snapshot = page(&state, 25, 0).await.unwrap();
        assert_eq!(snapshot.current[0].done, 3);
        assert!(snapshot.warnings.is_empty());
    }

    #[tokio::test]
    async fn failed_persistence_keeps_terminal_evidence_visible_until_retry() {
        let (_dir, state) = harness().await;
        sqlx::query(
            "CREATE TRIGGER reject_activity BEFORE INSERT ON background_activity
             BEGIN SELECT RAISE(ABORT, 'synthetic recording failure'); END",
        )
        .execute(&state.db)
        .await
        .unwrap();
        assert_eq!(
            observe(&state, "retry", "Scan", "Archive", None, |_| async {
                Ok(42)
            })
            .await
            .unwrap(),
            42
        );
        assert!(flush(&state).await.is_err());
        let snapshot = page(&state, 25, 0).await.unwrap();
        assert_eq!(snapshot.current[0].status, BackgroundStatus::Completed);
        assert_eq!(snapshot.total, 0);
        assert!(!snapshot.warnings.is_empty());
        sqlx::query("DROP TRIGGER reject_activity")
            .execute(&state.db)
            .await
            .unwrap();
        flush(&state).await.unwrap();
        let snapshot = page(&state, 25, 0).await.unwrap();
        assert!(snapshot.current.is_empty());
        assert_eq!(snapshot.history[0].status, BackgroundStatus::Completed);
        assert_eq!(snapshot.total, 1);
        assert!(snapshot.warnings.is_empty());
    }

    #[tokio::test]
    async fn delayed_outcomes_resolve_by_finish_order_not_flush_order() {
        let (_dir, state) = harness().await;
        let failed = Task::start(&state, "scan", "Scan", "Archive", None)
            .await
            .unwrap();
        let id = failed.id.clone();
        failed
            .finish(&Err::<(), _>(AppError::conflict("failed", "Scan failed")))
            .await
            .unwrap();
        let success = Task::start(&state, "scan", "Scan", "Archive", None)
            .await
            .unwrap();
        let success_id = success.id.clone();
        success.finish(&Ok(())).await.unwrap();
        let (older, newer) = {
            let mut registry = lock(&state.activity.0);
            for p in registry.values_mut() {
                p.activity.finished_at = Some("2024-01-01T00:00:00.000Z".into());
            }
            (registry[&id].clone(), registry[&success_id].clone())
        };
        persist(&state, &newer).await.unwrap();
        persist(&state, &older).await.unwrap();
        flush(&state).await.unwrap();
        let snapshot = page(&state, 25, 0).await.unwrap();
        assert!(snapshot.current.is_empty());
        assert_eq!(snapshot.history[0].id, success_id);
        assert_eq!(snapshot.total, 2);

        let revision: i64 = sqlx::query_scalar("SELECT MAX(revision) FROM background_activity")
            .fetch_one(&state.db)
            .await
            .unwrap();
        state.activity.3.store(0, Ordering::Relaxed);
        recover(&state).await.unwrap();
        assert!(state.activity.revision() > revision);
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
                .any(|a| a.phase.ends_with(&reason))
        );
        sqlx::query("UPDATE transfers SET attempts=2 WHERE id='download'")
            .execute(&state.db)
            .await
            .unwrap();
        let p = page(&state, 25, 0).await.unwrap();
        let transfer = p
            .current
            .iter()
            .find(|a| a.task_key == "transfer-queue:peer:share")
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
    async fn blocked_queue_heads_do_not_make_a_later_ready_transfer_look_blocked() {
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
        for i in 0..11 {
            let id = format!("queued-{i:02}");
            let path = format!("photo-{i:02}.jpg");
            sqlx::query("INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
                         VALUES (?,'peer','share',?,'example',10,'{}','queued',?,?)")
                .bind(&id).bind(&path).bind(db::now()).bind(db::now())
                .execute(&state.db).await.unwrap();
            if i < 10 {
                sqlx::query("INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
                             VALUES (?,'peer','share',?,'example',10,'{}','failed',?,?)")
                    .bind(format!("failed-{i:02}")).bind(format!("photo-{i:02}.xmp"))
                    .bind(db::now()).bind(db::now()).execute(&state.db).await.unwrap();
            }
        }
        let share = crate::peers::shares(&state).await.unwrap().remove(0);
        assert!(matches!(
            crate::peers::transfer::eligibility(
                &state,
                &share,
                "peer",
                "queued-00",
                "photo-00.jpg",
                0,
                &db::now()
            )
            .await
            .unwrap(),
            Eligibility::Waiting { blocked: true, .. }
        ));
        assert!(matches!(
            crate::peers::transfer::eligibility(
                &state,
                &share,
                "peer",
                "queued-10",
                "photo-10.jpg",
                0,
                &db::now()
            )
            .await
            .unwrap(),
            Eligibility::Ready(_)
        ));
        let snapshot = page(&state, 25, 0).await.unwrap();
        let queued = snapshot
            .current
            .iter()
            .find(|activity| activity.task_key == "transfer-queue:peer:share")
            .unwrap();
        assert_eq!(queued.count, 11);
        assert_ne!(queued.status, BackgroundStatus::Blocked);
    }

    #[tokio::test]
    async fn bulk_sync_work_is_grouped_with_reasons_instead_of_listed_per_file() {
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
        sqlx::query(
            "INSERT INTO peer_sync_state(peer_id,cursor,progress_json,updated_at)
             VALUES ('peer',0,?,?)",
        )
        .bind(serde_json::to_string(&ism_core::dto::SyncProgress::default()).unwrap())
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let n = 3000;
        let mut tx = state.db.begin().await.unwrap();
        for i in 0..n {
            let path = format!("2024/photo-{i}.jpg");
            let activity = ism_core::dto::SyncActivity {
                id: format!("out-{i}"),
                outgoing: true,
                peer_id: "peer".into(),
                share_name: "Archive".into(),
                relative_path: path.clone(),
                location: ism_core::dto::Location {
                    root_id: "photos".into(),
                    path: path.clone(),
                },
                source: "ISM: Local".into(),
                destination: "ISM: Peer example".into(),
                destination_path: path.clone(),
                status: "failed".into(),
                size: 10,
                bytes_done: 10,
                error: Some(if i % 3 == 0 {
                    format!("{path} changed on the receiver")
                } else {
                    "receiver disk full".into()
                }),
                queued_at: db::now(),
                finished_at: None,
                operation_id: None,
                readd_available: false,
                recovery: None,
            };
            sqlx::query("INSERT INTO outgoing_sync(peer_id,id,activity_json) VALUES ('peer',?,?)")
                .bind(&activity.id)
                .bind(serde_json::to_string(&activity).unwrap())
                .execute(&mut *tx)
                .await
                .unwrap();
            for status in ["queued", "failed"] {
                sqlx::query("INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at,error)
                             VALUES (?,'peer','share',?,'example',100,'{}',?,?,?,?)")
                    .bind(format!("{status}-{i}"))
                    .bind(&path)
                    .bind(status)
                    .bind(db::now())
                    .bind(db::now())
                    .bind((status == "failed").then_some("checksum mismatch"))
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
        }
        tx.commit().await.unwrap();
        let started = std::time::Instant::now();
        let snapshot = page(&state, 25, 0).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(snapshot.current.len(), 4, "{:#?}", snapshot.current);
        assert!(
            snapshot
                .current
                .iter()
                .any(|activity| activity.task_key == "peer-attention:peer"
                    && activity.status == BackgroundStatus::Waiting)
        );
        let outgoing = snapshot
            .current
            .iter()
            .find(|a| a.title == "Outgoing peer synchronization")
            .unwrap();
        assert_eq!(outgoing.count, n);
        assert_eq!(outgoing.status, BackgroundStatus::Blocked);
        assert_eq!(outgoing.reasons[0].reason, "receiver disk full");
        assert_eq!(outgoing.reasons[0].count, 2000);
        assert_eq!(outgoing.reasons[1].reason, "<item> changed on the receiver");
        assert_eq!(outgoing.examples.len(), 5);
        let failed = snapshot
            .current
            .iter()
            .find(|a| a.task_key == "transfer-failed:peer:share")
            .unwrap();
        assert_eq!((failed.count, failed.total), (n, Some(n * 100)));
        let queue = snapshot
            .current
            .iter()
            .find(|a| a.task_key == "transfer-queue:peer:share")
            .unwrap();
        assert_eq!(queue.count, n);
        assert!(!queue.phase.is_empty());
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
        flush(&state).await.unwrap();
        let snapshot = page(&state, 1, 0).await.unwrap();
        assert_eq!(snapshot.current.len(), 1);
        assert_eq!(snapshot.history[0].done, 20);
        first
            .finish_with_phase(&Ok(()), "Attempt deferred; work remains queued")
            .await
            .unwrap();
        flush(&state).await.unwrap();
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
            flush(&state).await.unwrap();
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
        flush(&state).await.unwrap();
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
        flush(&state).await.unwrap();
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
        flush(&state).await.unwrap();
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
        let task = Task::start(&state, "never-saved", "Scan", "Archive", None)
            .await
            .unwrap();
        drop(task);
        lock(&state.activity.0).clear();
        recover(&state).await.unwrap();
        assert_eq!(page(&state, 25, 0).await.unwrap().current.len(), 2);
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
        flush(&state).await.unwrap();
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
