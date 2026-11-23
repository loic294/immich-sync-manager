//! ISM-owned change detection for mapped folders.
//!
//! Immich's own file watcher and scheduled library scan stay off (see `maintenance`). Every
//! mapped folder that belongs to an Immich library or a peer share is watched here: local disks
//! with native change events, network shares by polling (per-mapping mode and interval). A full
//! pass runs after every start; changes are diffed against a snapshot kept in SQLite. Changes ISM
//! announced itself ([`selfwrite`]) are ignored. Outside changes schedule debounced API-first
//! synchronization ([`scheduler`]) and mark peer shares for a rescan. An unreachable or suddenly
//! empty folder pauses detection; it is never read as "everything was deleted".

pub mod fstype;
pub mod ignore;
pub mod native;
pub mod poll;
pub mod scheduler;
pub mod selfwrite;

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ism_core::dto::{WatchMode, WatchSettingsInput, WatchStatusView};
use tokio::sync::mpsc;
use tokio::time::Instant;

pub use selfwrite::{Expected, expect_writes};

use crate::error::{AppError, AppResult};
use crate::files::{self, Root};
use crate::state::{AppState, ServerEvent};

pub const DEFAULT_POLL_SECS: i64 = 300;
/// Native mode still runs a full pass this often, in case events were missed silently.
const NATIVE_SAFETY_PASS: Duration = Duration::from_secs(24 * 3600);
/// A paused native watcher retries a full pass this often.
const PAUSED_RETRY: Duration = Duration::from_secs(60);
/// More scoped paths than this in one batch run a full pass instead.
const MAX_SCOPES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangeKind {
    Added,
    Changed,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Root-relative path.
    pub rel: String,
    pub kind: ChangeKind,
    pub size: Option<i64>,
    pub mtime_ns: Option<i64>,
}

fn mode_str(mode: WatchMode) -> &'static str {
    match mode {
        WatchMode::Auto => "auto",
        WatchMode::Native => "native",
        WatchMode::Poll => "poll",
    }
}

fn parse_mode(s: &str) -> WatchMode {
    match s {
        "native" => WatchMode::Native,
        "poll" => WatchMode::Poll,
        _ => WatchMode::Auto,
    }
}

/// A mapping's watch settings (defaults when none were saved).
pub async fn settings(state: &AppState, root_id: &str) -> AppResult<(WatchMode, u32)> {
    let row: Option<(String, i64)> =
        sqlx::query_as("SELECT mode, poll_interval_secs FROM watch_settings WHERE root_id = ?")
            .bind(root_id)
            .fetch_optional(&state.db)
            .await?;
    Ok(
        row.map_or((WatchMode::Auto, DEFAULT_POLL_SECS as u32), |(m, s)| {
            (parse_mode(&m), u32::try_from(s).unwrap_or(300))
        }),
    )
}

pub async fn set_settings(
    state: &AppState,
    root_id: &str,
    input: &WatchSettingsInput,
) -> AppResult<()> {
    if !(60..=86_400).contains(&input.poll_interval_secs) {
        return Err(AppError::bad_request(
            "invalid_request",
            "the polling interval must be between 60 and 86400 seconds",
        ));
    }
    files::root(&state.db, root_id).await?;
    sqlx::query(
        "INSERT INTO watch_settings (root_id, mode, poll_interval_secs) VALUES (?, ?, ?)
         ON CONFLICT(root_id) DO UPDATE SET mode = excluded.mode, poll_interval_secs = excluded.poll_interval_secs",
    )
    .bind(root_id)
    .bind(mode_str(input.mode))
    .bind(i64::from(input.poll_interval_secs))
    .execute(&state.db)
    .await?;
    state.watch_wake.notify_one();
    state.emit(ServerEvent::WatchChanged);
    Ok(())
}

/// Admin action after a pause the administrator confirmed (for example a folder that was
/// emptied on purpose): forget the snapshot; the next pass records a new baseline.
pub async fn reset_baseline(state: &AppState, root_id: &str) -> AppResult<()> {
    files::root(&state.db, root_id).await?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("DELETE FROM watch_snapshot WHERE root_id = ?")
        .bind(root_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE watch_status SET last_pass_at = NULL, files = 0 WHERE root_id = ?")
        .bind(root_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    state.watch_wake.notify_one();
    state.emit(ServerEvent::WatchChanged);
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct Status {
    effective_mode: Option<String>,
    fs_type: Option<String>,
    last_pass_at: Option<String>,
    last_change_at: Option<String>,
    paused: bool,
    warnings: Vec<String>,
    files: i64,
}

async fn load_status(state: &AppState, root_id: &str) -> AppResult<Status> {
    type Row = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
        String,
        i64,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT effective_mode, fs_type, last_pass_at, last_change_at, paused, warnings_json, files
         FROM watch_status WHERE root_id = ?",
    )
    .bind(root_id)
    .fetch_optional(&state.db)
    .await?;
    Ok(row.map_or_else(Status::default, |r| Status {
        effective_mode: r.0,
        fs_type: r.1,
        last_pass_at: r.2,
        last_change_at: r.3,
        paused: r.4,
        warnings: serde_json::from_str(&r.5).unwrap_or_default(),
        files: r.6,
    }))
}

async fn save_status(state: &AppState, root_id: &str, s: &Status) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO watch_status (root_id, effective_mode, fs_type, last_pass_at, last_change_at, paused, warnings_json, files)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(root_id) DO UPDATE SET effective_mode = excluded.effective_mode, fs_type = excluded.fs_type,
           last_pass_at = excluded.last_pass_at, last_change_at = excluded.last_change_at, paused = excluded.paused,
           warnings_json = excluded.warnings_json, files = excluded.files",
    )
    .bind(root_id)
    .bind(&s.effective_mode)
    .bind(&s.fs_type)
    .bind(&s.last_pass_at)
    .bind(&s.last_change_at)
    .bind(s.paused)
    .bind(serde_json::to_string(&s.warnings)?)
    .bind(s.files)
    .execute(&state.db)
    .await?;
    state.emit(ServerEvent::WatchChanged);
    Ok(())
}

/// Roots that are watched: those of an Immich library or holding a peer share.
async fn watched_roots(state: &AppState) -> AppResult<Vec<Root>> {
    let shared: Vec<String> = sqlx::query_scalar("SELECT DISTINCT root_id FROM shares")
        .fetch_all(&state.db)
        .await?;
    Ok(files::load_roots(&state.db)
        .await?
        .into_iter()
        .filter(|r| r.library_id.is_some() || shared.contains(&r.mapping.root_id))
        .collect())
}

pub async fn status_view(
    state: &AppState,
    root: &Root,
    watched: bool,
) -> AppResult<WatchStatusView> {
    let (mode, poll_interval_secs) = settings(state, &root.mapping.root_id).await?;
    let s = load_status(state, &root.mapping.root_id).await?;
    Ok(WatchStatusView {
        root_id: root.mapping.root_id.clone(),
        name: root.name.clone(),
        watched,
        mode,
        poll_interval_secs,
        effective_mode: s.effective_mode.filter(|_| watched),
        fs_type: s.fs_type,
        last_pass_at: s.last_pass_at,
        last_change_at: s.last_change_at,
        paused: watched && s.paused,
        warnings: if watched { s.warnings } else { Vec::new() },
        files: s.files,
    })
}

/// The watch status of every mapping, keyed by root id.
pub async fn status_views(state: &AppState) -> AppResult<HashMap<String, WatchStatusView>> {
    let watched: BTreeSet<String> = watched_roots(state)
        .await?
        .into_iter()
        .map(|r| r.mapping.root_id)
        .collect();
    let mut out = HashMap::new();
    for root in files::load_roots(&state.db).await? {
        let view = status_view(state, &root, watched.contains(&root.mapping.root_id)).await?;
        out.insert(root.mapping.root_id.clone(), view);
    }
    Ok(out)
}

pub(crate) enum PassOutcome {
    /// The first pass of a folder records what exists without reporting it.
    Baseline,
    Changes(Vec<Change>),
    Paused(String),
}

/// Walks `scope` (or the whole folder), diffs it against the snapshot and stores the result.
pub(crate) async fn pass(
    state: &AppState,
    root: &Root,
    scope: Option<&str>,
) -> AppResult<PassOutcome> {
    let task = crate::activity::Task::start(
        state,
        format!("watch:{}", root.mapping.root_id),
        "File detection scan",
        &root.name,
        Some("/mappings"),
    )
    .await?;
    let result = pass_reporting(state, root, scope, &task).await;
    let phase = match &result {
        Ok(PassOutcome::Paused(reason)) => {
            format!("Detection paused: {}", crate::error::redact(reason))
        }
        Ok(PassOutcome::Baseline) => "Initial file baseline saved".into(),
        Ok(PassOutcome::Changes(changes)) => {
            format!("Detection finished: {} changes recorded", changes.len())
        }
        Err(_) => "Detection ended".into(),
    };
    task.finish_with_phase(&result, &phase).await?;
    result
}

async fn pass_reporting(
    state: &AppState,
    root: &Root,
    scope: Option<&str>,
    task: &crate::activity::Reporter,
) -> AppResult<PassOutcome> {
    task.phase("Checking root availability");
    let root_id = root.mapping.root_id.as_str();
    let available = {
        let root = root.clone();
        tokio::task::spawn_blocking(move || root.available())
            .await
            .unwrap_or(false)
    };
    if !available {
        return Ok(PassOutcome::Paused(format!(
            "the folder is unreachable or its {} marker is missing; detection paused",
            files::SENTINEL
        )));
    }
    let walked = {
        let base = root.base();
        let scope = scope.map(str::to_owned);
        let junk = state.junk_rules();
        task.phase("Enumerating files");
        let reporter = task.clone();
        tokio::task::spawn_blocking(move || {
            poll::walk_reporting(&base, scope.as_deref(), &junk, |done| {
                reporter.progress(done, None, "files discovered")
            })
        })
        .await
    };
    let new = match walked {
        Ok(Ok(tree)) => tree,
        Ok(Err(e)) => {
            return Ok(PassOutcome::Paused(format!(
                "cannot read the folder ({e}); detection paused"
            )));
        }
        Err(e) => return Err(AppError::internal(e)),
    };
    let old = poll::load(state, root_id, scope).await?;
    task.phase("Comparing and saving the file snapshot");
    if scope.is_none() {
        let status = load_status(state, root_id).await?;
        if status.last_pass_at.is_none() {
            let all = poll::diff(&poll::Tree::new(), &new);
            poll::store(state, root_id, &new, &all).await?;
            return Ok(PassOutcome::Baseline);
        }
        if !old.is_empty() && new.is_empty() {
            return Ok(PassOutcome::Paused(
                "the folder is suddenly empty; detection paused (reset the baseline if this is intended)"
                    .into(),
            ));
        }
    }
    let changes = poll::diff(&old, &new);
    poll::store(state, root_id, &new, &changes).await?;
    Ok(PassOutcome::Changes(changes))
}

/// Drops ISM's own changes and routes the rest: Immich synchronization and peer share rescans.
/// Returns the outside changes.
pub(crate) async fn handle(
    state: &AppState,
    root: &Root,
    changes: Vec<Change>,
) -> AppResult<Vec<Change>> {
    if changes.is_empty() {
        return Ok(changes);
    }
    let records = selfwrite::load(state, selfwrite::now_ms()).await?;
    let base = root.base();
    let outside: Vec<Change> = changes
        .into_iter()
        .filter(|c| !selfwrite::is_self(&records, &base.join(&c.rel).to_string_lossy(), c))
        .collect();
    if outside.is_empty() {
        return Ok(outside);
    }
    if let (Some(server_id), Some(library_id)) = (&root.server_id, &root.library_id) {
        let patterns: Option<String> =
            sqlx::query_scalar("SELECT exclusion_patterns_json FROM immich_libraries WHERE id = ?")
                .bind(library_id)
                .fetch_optional(&state.db)
                .await?;
        let patterns: Vec<String> = patterns
            .and_then(|p| serde_json::from_str(&p).ok())
            .unwrap_or_default();
        let relevant: Vec<scheduler::Work> = outside
            .iter()
            .filter_map(|c| {
                let path = files::parse_rel(&c.rel)
                    .ok()
                    .and_then(|rel| root.immich_path(&rel))?;
                (!ignore::excluded(&patterns, &path)).then_some(scheduler::Work {
                    path,
                    kind: match c.kind {
                        ChangeKind::Added => scheduler::WorkKind::Added,
                        ChangeKind::Changed => scheduler::WorkKind::Changed,
                        ChangeKind::Removed => scheduler::WorkKind::Removed,
                    },
                })
            })
            .collect();
        if !relevant.is_empty() {
            let reason = format!("{} outside change(s) in {}", relevant.len(), root.name);
            scheduler::request_work(state, server_id, library_id, &reason, &relevant).await?;
        }
    }
    let settle = Duration::from_secs(crate::peers::SETTLE_SECS.unsigned_abs() + 1);
    let mut woke = false;
    for share in crate::peers::shares(state).await? {
        if !share.enabled || share.root_id != root.mapping.root_id {
            continue;
        }
        let touched = outside.iter().any(|c| {
            files::parse_rel(&c.rel)
                .ok()
                .is_some_and(|rel| share.rel_of(&root.location(&rel)).is_some())
        });
        if touched {
            let due = std::time::Instant::now() + settle;
            if let Ok(mut map) = state.share_changes.lock() {
                map.insert(share.id.clone(), due);
                woke = true;
            }
        }
    }
    if woke {
        state.share_wake.notify_one();
    }
    Ok(outside)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Spec {
    container_path: String,
    mode: WatchMode,
    interval: u32,
}

/// Keeps one watcher task per watched folder, restarting it when its settings change.
pub async fn run(state: AppState) {
    let mut tasks: HashMap<String, (Spec, tokio::task::JoinHandle<()>)> = HashMap::new();
    loop {
        match desired(&state).await {
            Ok(want) => {
                tasks.retain(|id, (spec, handle)| {
                    let keep = want.get(id) == Some(spec) && !handle.is_finished();
                    if !keep {
                        handle.abort();
                    }
                    keep
                });
                for (id, spec) in want {
                    if let std::collections::hash_map::Entry::Vacant(slot) = tasks.entry(id) {
                        let handle = tokio::spawn(root_task(
                            state.clone(),
                            slot.key().clone(),
                            spec.clone(),
                        ));
                        slot.insert((spec, handle));
                    }
                }
            }
            Err(e) => tracing::warn!("watch: {}", e.message()),
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(30)) => {}
            () = state.watch_wake.notified() => {}
        }
    }
}

async fn desired(state: &AppState) -> AppResult<HashMap<String, Spec>> {
    let mut out = HashMap::new();
    for root in watched_roots(state).await? {
        let (mode, interval) = settings(state, &root.mapping.root_id).await?;
        out.insert(
            root.mapping.root_id.clone(),
            Spec {
                container_path: root.mapping.container_path.clone(),
                mode,
                interval,
            },
        );
    }
    Ok(out)
}

/// Collapses changed paths into the fewest folder scopes (a parent covers its children).
fn collapse(paths: &BTreeSet<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in paths {
        if out.last().is_some_and(|last| {
            p.starts_with(last.as_str()) && p.as_bytes().get(last.len()) == Some(&b'/')
        }) {
            continue;
        }
        out.push(p.clone());
    }
    out
}

fn rel_from_event(bases: &[PathBuf], path: &Path) -> Option<String> {
    bases.iter().find_map(|b| {
        let rel = path.strip_prefix(b).ok()?;
        let parts: Option<Vec<&str>> = rel.components().map(|c| c.as_os_str().to_str()).collect();
        Some(parts?.join("/"))
    })
}

struct Task {
    state: AppState,
    root_id: String,
    effective: WatchMode,
    interval: Duration,
    mode_warning: Option<String>,
    paused: bool,
}

impl Task {
    async fn root(&self) -> AppResult<Root> {
        files::root(&self.state.db, &self.root_id).await
    }

    async fn run_pass(&mut self, scope: Option<&str>) -> AppResult<()> {
        if self.paused && scope.is_some() {
            return Ok(());
        }
        let root = self.root().await?;
        let outcome = pass(&self.state, &root, scope).await?;
        let mut status = load_status(&self.state, &self.root_id).await?;
        status.effective_mode = Some(mode_str(self.effective).to_owned());
        status.warnings = self.mode_warning.iter().cloned().collect();
        match outcome {
            PassOutcome::Paused(reason) => {
                if !self.paused {
                    tracing::warn!(root = %root.name, "{reason}");
                }
                self.paused = true;
                status.paused = true;
                status.warnings.push(reason);
            }
            PassOutcome::Baseline => {
                self.paused = false;
                status.paused = false;
                status.last_pass_at = Some(crate::db::now());
            }
            PassOutcome::Changes(changes) => {
                self.paused = false;
                status.paused = false;
                status.last_pass_at = Some(crate::db::now());
                let outside = handle(&self.state, &root, changes).await?;
                if !outside.is_empty() {
                    tracing::info!(root = %root.name, changes = outside.len(), "outside changes detected");
                    status.last_change_at = Some(crate::db::now());
                }
            }
        }
        status.files = sqlx::query_scalar("SELECT COUNT(*) FROM watch_snapshot WHERE root_id = ?")
            .bind(&self.root_id)
            .fetch_one(&self.state.db)
            .await?;
        save_status(&self.state, &self.root_id, &status).await
    }

    fn next_full_after(&self) -> Duration {
        match (self.effective, self.paused) {
            (WatchMode::Native, true) => PAUSED_RETRY.min(self.interval),
            (WatchMode::Native, false) => NATIVE_SAFETY_PASS,
            _ => self.interval,
        }
    }
}

async fn root_task(state: AppState, root_id: String, spec: Spec) {
    let base = PathBuf::from(&spec.container_path);
    let (fs_type, network) = {
        let base = base.clone();
        tokio::task::spawn_blocking(move || fstype::detect(&base))
            .await
            .unwrap_or((None, false))
    };
    let mut task = Task {
        state: state.clone(),
        root_id: root_id.clone(),
        effective: match spec.mode {
            WatchMode::Auto if network => WatchMode::Poll,
            WatchMode::Auto => WatchMode::Native,
            m => m,
        },
        interval: Duration::from_secs(u64::from(spec.interval)),
        mode_warning: None,
        paused: false,
    };
    if let Ok(mut status) = load_status(&state, &root_id).await {
        status.fs_type = fs_type;
        let _ = save_status(&state, &root_id, &status).await;
    }
    let (tx, mut rx) = mpsc::channel(4096);
    let mut watcher = None;
    if task.effective == WatchMode::Native {
        match native::start(&base, tx) {
            Ok(w) => watcher = Some(w),
            Err(e) => {
                task.fallback(format!(
                    "native change events unavailable ({e}); polling instead"
                ));
            }
        }
    }
    let bases: Vec<PathBuf> = [Some(base.clone()), std::fs::canonicalize(&base).ok()]
        .into_iter()
        .flatten()
        .collect();
    let gap = state.config.timing.full_pass_gap;
    let mut last_full: Option<Instant> = None;
    let mut next_full = Instant::now();
    loop {
        if Instant::now() >= next_full {
            if let Err(e) = task.run_pass(None).await {
                tracing::warn!(root = %root_id, "watch pass failed: {}", e.message());
            }
            last_full = Some(Instant::now());
            next_full = Instant::now() + task.next_full_after();
        }
        let mut pending = BTreeSet::new();
        let mut full = false;
        tokio::select! {
            () = tokio::time::sleep_until(next_full) => continue,
            event = rx.recv(), if watcher.is_some() => match event {
                Some(event) => full |= task.collect(event, &bases, &mut pending, &mut watcher),
                None => continue,
            },
        }
        // Debounce: let a burst of events arrive, then look once.
        tokio::time::sleep(state.config.timing.watch_debounce).await;
        while let Ok(event) = rx.try_recv() {
            full |= task.collect(event, &bases, &mut pending, &mut watcher);
        }
        full |= watcher
            .as_ref()
            .is_some_and(native::NativeWatcher::take_overflow);
        let scopes = collapse(&pending);
        if full || scopes.len() > MAX_SCOPES || scopes.iter().any(String::is_empty) {
            let earliest = last_full.map_or_else(Instant::now, |t| t + gap);
            next_full = next_full.min(earliest.max(Instant::now()));
            continue;
        }
        for scope in scopes {
            if let Err(e) = task.run_pass(Some(&scope)).await {
                tracing::warn!(root = %root_id, "watch pass failed: {}", e.message());
            }
        }
    }
}

impl Task {
    fn fallback(&mut self, warning: String) {
        tracing::warn!(root = %self.root_id, "{warning}");
        self.effective = WatchMode::Poll;
        self.mode_warning = Some(warning);
    }

    /// Adds an event's paths; true when a full pass is needed.
    fn collect(
        &mut self,
        event: native::NativeEvent,
        bases: &[PathBuf],
        pending: &mut BTreeSet<String>,
        watcher: &mut Option<native::NativeWatcher>,
    ) -> bool {
        match event {
            native::NativeEvent::Paths(paths) => {
                for p in paths {
                    match rel_from_event(bases, &p) {
                        Some(rel) if rel.is_empty() => return true,
                        Some(rel)
                            if !ignore::ignored(&rel)
                                && !self.state.junk_rules().path_is_junk(&rel) =>
                        {
                            pending.insert(rel);
                        }
                        _ => {}
                    }
                }
                false
            }
            native::NativeEvent::Rescan => true,
            native::NativeEvent::Failed(e) => {
                *watcher = None;
                self.fallback(format!(
                    "native change events failed ({e}); polling instead"
                ));
                true
            }
        }
    }
}

#[cfg(test)]
mod tests;
