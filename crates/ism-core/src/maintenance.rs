//! The write barrier of one connected Immich server.
//!
//! ISM owns file detection: Immich's library watcher and scheduled library scan stay off
//! permanently (they are turned off at enrollment, and ISM alerts when either comes back on).
//! Before a managed operation touches library files, the server pauses Immich's `library` queue
//! (the only queue that imports files or marks them offline) and confirms it idle on several
//! consecutive polls. Overlapping operations share one pause through a reference count; the last
//! release resumes the queue, after which due library scans may start. This module decides
//! *what* to do; the server performs it and persists [`MaintenanceState`] after every transition
//! so a restart resumes from the same place.
//!
//! ```text
//! Idle --admit--> Preparing --paused + idle--> Active (active_ops > 0) --last release--> Resuming
//!   ^                 |                                                                    |
//!   |              failure --> Blocked --retry--> Idle / Resuming                          |
//!   +------------------------------------- resumed ---------------------------------------+
//! ```
//!
//! Settings drift (watcher or scheduled scan turned back on) and unresolved recoveries refuse
//! new work until an administrator acts.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Consecutive idle reads of the paused `library` queue required before work is admitted.
pub const IDLE_CONFIRMATIONS: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum Phase {
    Idle,
    /// Pausing the library queue and confirming it idle.
    Preparing,
    /// The library queue is paused and `active_ops` operations hold the barrier.
    Active,
    /// The last operation released; the library queue is being resumed.
    Resuming,
    /// The queue could not be paused, confirmed idle or resumed; writes are refused until an
    /// admin retries.
    Blocked,
    /// Something changed on disk but could not be completed or verified; writes are refused and
    /// the library queue stays paused until an admin resolves it.
    RecoveryRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceState {
    pub phase: Phase,
    #[serde(default)]
    pub epoch: u64,
    #[serde(default)]
    pub active_ops: u32,
    #[serde(default)]
    pub unresolved_recoveries: u32,
    /// ISM paused (or was about to pause) the `library` queue and still has to resume it.
    #[serde(default)]
    pub queue_paused_by_ism: bool,
    /// Immich's watcher or scheduled scan was found on; writes are refused until an admin
    /// re-applies ISM's settings.
    #[serde(default)]
    pub drift: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl Default for MaintenanceState {
    fn default() -> Self {
        Self {
            phase: Phase::Idle,
            epoch: 0,
            active_ops: 0,
            unresolved_recoveries: 0,
            queue_paused_by_ism: false,
            drift: None,
            message: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// The queue is paused for other operations: share the pause.
    Admit,
    /// Pause the library queue, confirm it idle and ask again.
    Prepare,
    /// Another transition is in progress; ask again later.
    Wait,
    /// Writes are refused (drift, blocked or recovery required).
    Deny(String),
}

impl MaintenanceState {
    pub fn admission(&self) -> Admission {
        if let Some(drift) = &self.drift {
            return Admission::Deny(drift.clone());
        }
        match self.phase {
            Phase::Active => Admission::Admit,
            Phase::Idle => Admission::Prepare,
            Phase::Preparing | Phase::Resuming => Admission::Wait,
            Phase::Blocked | Phase::RecoveryRequired => Admission::Deny(
                self.message
                    .clone()
                    .unwrap_or_else(|| "maintenance needs attention".into()),
            ),
        }
    }

    /// Starts pausing. The pause intent is recorded first so a crash mid-way still resumes.
    pub fn begin_prepare(&mut self) {
        self.phase = Phase::Preparing;
        self.epoch += 1;
        self.queue_paused_by_ism = true;
        self.message = None;
    }

    /// The queue is paused and idle: the preparing operation is admitted.
    pub fn prepared(&mut self) {
        self.phase = Phase::Active;
        self.active_ops += 1;
    }

    /// Another operation shares the pause. Only valid in `Active`.
    pub fn admit(&mut self) {
        self.active_ops += 1;
    }

    /// One operation released. Returns true when the queue must now be resumed.
    pub fn settle(&mut self) -> bool {
        self.active_ops = self.active_ops.saturating_sub(1);
        if self.active_ops == 0 && self.phase == Phase::Active {
            self.phase = Phase::Resuming;
            return true;
        }
        false
    }

    pub fn resumed(&mut self) {
        self.phase = Phase::Idle;
        self.queue_paused_by_ism = false;
        self.message = None;
    }

    pub fn block(&mut self, reason: impl Into<String>) {
        self.phase = Phase::Blocked;
        self.message = Some(reason.into());
    }

    pub fn require_recovery(&mut self, reason: impl Into<String>) {
        self.phase = Phase::RecoveryRequired;
        self.message = Some(reason.into());
    }

    /// Where an admin retry of a blocked barrier goes: resume first if ISM still holds a pause.
    pub fn retry(&mut self) -> bool {
        if self.phase != Phase::Blocked {
            return false;
        }
        self.phase = if self.queue_paused_by_ism {
            Phase::Resuming
        } else {
            Phase::Idle
        };
        self.message = None;
        true
    }

    /// One recovery was handled. With none left, operations still running keep the pause and
    /// otherwise the queue is resumed.
    pub fn resolve_one(&mut self) {
        self.unresolved_recoveries = self.unresolved_recoveries.saturating_sub(1);
        if self.unresolved_recoveries == 0 && self.phase == Phase::RecoveryRequired {
            self.message = None;
            self.phase = if self.active_ops > 0 {
                Phase::Active
            } else if self.queue_paused_by_ism {
                Phase::Resuming
            } else {
                Phase::Idle
            };
        }
    }

    /// After a restart nothing holds the barrier any more; interrupted operations were already
    /// turned into recoveries by the caller.
    pub fn after_restart(&mut self) {
        self.active_ops = 0;
        match self.phase {
            Phase::RecoveryRequired | Phase::Blocked => {}
            _ if self.queue_paused_by_ism => self.phase = Phase::Resuming,
            _ => self.phase = Phase::Idle,
        }
    }

    /// Whether a library scan may start now: nothing holds or is about to hold the pause.
    pub fn scans_allowed(&self) -> bool {
        self.phase == Phase::Idle && self.active_ops == 0 && !self.queue_paused_by_ism
    }
}

/// The drift message when Immich's watcher or scheduled scan is not off.
pub fn settings_drift(watch: Option<bool>, scan: Option<bool>) -> Option<String> {
    let mut on = Vec::new();
    match watch {
        Some(false) => {}
        Some(true) => on.push("file watching"),
        None => on.push("file watching (unreadable)"),
    }
    match scan {
        Some(false) => {}
        Some(true) => on.push("scheduled library scan"),
        None => on.push("scheduled library scan (unreadable)"),
    }
    (!on.is_empty()).then(|| {
        format!(
            "Immich {} must stay off while ISM detects file changes; re-apply ISM's Immich settings",
            on.join(" and ")
        )
    })
}

/// Counts consecutive idle reads; any busy read starts over.
#[derive(Debug, Default, Clone, Copy)]
pub struct IdleConfirmation {
    seen: u32,
}

impl IdleConfirmation {
    /// Records one read; true once [`IDLE_CONFIRMATIONS`] consecutive reads were idle.
    pub fn observe(&mut self, idle: bool) -> bool {
        self.seen = if idle { self.seen + 1 } else { 0 };
        self.seen >= IDLE_CONFIRMATIONS
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn active() -> MaintenanceState {
        let mut s = MaintenanceState::default();
        assert_eq!(s.admission(), Admission::Prepare);
        s.begin_prepare();
        assert_eq!(s.admission(), Admission::Wait);
        s.prepared();
        s
    }

    #[test]
    fn overlapping_operations_share_one_pause() {
        let mut s = active();
        assert_eq!(s.admission(), Admission::Admit);
        s.admit();
        assert_eq!(s.active_ops, 2);
        assert!(!s.settle(), "one operation still holds the pause");
        assert!(!s.scans_allowed());
        assert!(s.settle(), "the last release resumes the queue");
        assert_eq!(s.phase, Phase::Resuming);
        assert_eq!(s.admission(), Admission::Wait);
        s.resumed();
        assert!(s.scans_allowed());
        assert!(!s.queue_paused_by_ism);
    }

    #[test]
    fn drift_refuses_work_until_cleared() {
        let mut s = MaintenanceState {
            drift: settings_drift(Some(true), Some(false)),
            ..Default::default()
        };
        assert!(matches!(s.admission(), Admission::Deny(m) if m.contains("file watching")));
        s.drift = None;
        assert_eq!(s.admission(), Admission::Prepare);
        assert_eq!(settings_drift(Some(false), Some(false)), None);
        assert!(settings_drift(None, Some(false)).is_some());
    }

    #[test]
    fn recovery_keeps_the_pause_until_resolved() {
        let mut s = active();
        s.unresolved_recoveries = 1;
        s.require_recovery("broken");
        assert!(!s.settle(), "a recovery never resumes the queue by itself");
        assert!(matches!(s.admission(), Admission::Deny(_)));
        s.resolve_one();
        assert_eq!(s.phase, Phase::Resuming);
    }

    #[test]
    fn restart_resumes_a_pause_nobody_holds() {
        let mut s = active();
        s.after_restart();
        assert_eq!((s.phase, s.active_ops), (Phase::Resuming, 0));
        let mut crashed_while_pausing = MaintenanceState::default();
        crashed_while_pausing.begin_prepare();
        crashed_while_pausing.after_restart();
        assert_eq!(crashed_while_pausing.phase, Phase::Resuming);
        let mut idle = MaintenanceState::default();
        idle.after_restart();
        assert_eq!(idle.phase, Phase::Idle);
    }

    #[test]
    fn retry_resumes_before_allowing_work() {
        let mut s = MaintenanceState::default();
        s.begin_prepare();
        s.block("queue busy");
        assert!(s.retry());
        assert_eq!(s.phase, Phase::Resuming);
        s.resumed();
        s.block("other");
        assert!(s.retry());
        assert_eq!(s.phase, Phase::Idle);
    }

    #[test]
    fn idle_needs_consecutive_reads() {
        let mut c = IdleConfirmation::default();
        assert!(!c.observe(true));
        assert!(!c.observe(true));
        assert!(!c.observe(false));
        assert!(!c.observe(true));
        assert!(!c.observe(true));
        assert!(c.observe(true));
    }

    #[test]
    fn old_states_deserialize() {
        let s: MaintenanceState = serde_json::from_str(
            r#"{"phase":"idle","epoch":3,"active_ops":0,"unresolved_recoveries":0,"last_activity":null,"quiet_deadline":null,"saved":{"watch_enabled":null,"scan_enabled":null,"paused_queues":[]},"affected_libraries":[],"message":null}"#,
        )
        .expect("pre-0006 state");
        assert_eq!(s.epoch, 3);
    }
}
