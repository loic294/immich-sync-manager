//! The maintenance state machine of one connected Immich server.
//!
//! Immich's library watcher deletes the asset row when a watched file disappears, so file
//! watching and scheduled scans must be off before any managed file changes, and must stay off
//! until every change has settled. This module decides *what* to do; the server performs it and
//! persists [`MaintenanceState`] after every transition so a restart resumes the same phase.
//!
//! Lifecycle of one epoch:
//!
//! ```text
//! Idle --admit--> Preparing --prepared--> Active --(no work for 15 min)--> Reconciling
//!   ^                 |                     ^  |                               |
//!   |              failure                admit settle                       scanned
//!   |                 v                                                        v
//!   +---- restored -- Blocked                        Idle <---- restored --- Restoring
//! ```
//!
//! Work that arrives during `Reconciling`/`Restoring` waits for the next epoch: files never
//! change while a reconciliation scan may be reading them.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The user-specified quiet period: 15 minutes without admitted work before scanning.
pub const QUIET_PERIOD_SECS: i64 = 15 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum Phase {
    Idle,
    Preparing,
    Active,
    Reconciling,
    Restoring,
    /// Preparation failed and nothing was changed; writes are refused until an admin retries.
    Blocked,
    /// Something changed on disk but could not be completed or verified; writes are refused and
    /// watchers stay off until an admin resolves it.
    RecoveryRequired,
}

/// Settings this coordinator changed, with the values they had before, so restoration puts back
/// exactly what the administrator had and nothing else.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedSettings {
    /// `library.watch.enabled` before we turned it off; `None` when we did not change it.
    pub watch_enabled: Option<bool>,
    /// `library.scan.enabled` before we turned it off; `None` when we did not change it.
    pub scan_enabled: Option<bool>,
    /// Queues we paused (and must resume).
    pub paused_queues: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceState {
    pub phase: Phase,
    pub epoch: u64,
    pub active_ops: u32,
    pub unresolved_recoveries: u32,
    pub last_activity: Option<DateTime<Utc>>,
    pub quiet_deadline: Option<DateTime<Utc>>,
    pub saved: SavedSettings,
    /// Immich library ids touched during this epoch; each gets a reconciliation scan.
    pub affected_libraries: Vec<String>,
    pub message: Option<String>,
}

impl Default for MaintenanceState {
    fn default() -> Self {
        Self {
            phase: Phase::Idle,
            epoch: 0,
            active_ops: 0,
            unresolved_recoveries: 0,
            last_activity: None,
            quiet_deadline: None,
            saved: SavedSettings::default(),
            affected_libraries: Vec::new(),
            message: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Watchers are off and queues quiet: go ahead.
    Admit,
    /// Start an epoch (turn watchers off, drain queues) and ask again.
    Prepare,
    /// Another phase is in progress; ask again later.
    Wait,
    /// Writes are refused (blocked or recovery required).
    Deny(String),
}

impl MaintenanceState {
    pub fn admission(&self) -> Admission {
        match self.phase {
            Phase::Active => Admission::Admit,
            Phase::Idle => Admission::Prepare,
            Phase::Preparing | Phase::Reconciling | Phase::Restoring => Admission::Wait,
            Phase::Blocked | Phase::RecoveryRequired => Admission::Deny(
                self.message
                    .clone()
                    .unwrap_or_else(|| "maintenance needs attention".into()),
            ),
        }
    }

    pub fn begin_prepare(&mut self) {
        self.phase = Phase::Preparing;
        self.epoch += 1;
        self.saved = SavedSettings::default();
        self.affected_libraries.clear();
        self.message = None;
    }

    pub fn prepared(&mut self, saved: SavedSettings, now: DateTime<Utc>) {
        self.phase = Phase::Active;
        self.saved = saved;
        self.last_activity = Some(now);
        self.quiet_deadline = Some(now + Duration::seconds(QUIET_PERIOD_SECS));
    }

    pub fn block(&mut self, reason: impl Into<String>) {
        self.phase = Phase::Blocked;
        self.message = Some(reason.into());
    }

    pub fn require_recovery(&mut self, reason: impl Into<String>) {
        self.phase = Phase::RecoveryRequired;
        self.message = Some(reason.into());
    }

    /// One managed operation starts touching files. Only valid in `Active`.
    pub fn admit(&mut self, now: DateTime<Utc>, library_ids: &[String]) {
        self.active_ops += 1;
        self.last_activity = Some(now);
        self.quiet_deadline = None;
        for id in library_ids {
            if !self.affected_libraries.contains(id) {
                self.affected_libraries.push(id.clone());
            }
        }
    }

    /// One managed operation finished. The quiet timer restarts from now.
    pub fn settle(&mut self, now: DateTime<Utc>) {
        self.active_ops = self.active_ops.saturating_sub(1);
        self.last_activity = Some(now);
        if self.active_ops == 0 {
            self.quiet_deadline = Some(now + Duration::seconds(QUIET_PERIOD_SECS));
        }
    }

    /// True exactly when the 15-minute quiet period has fully elapsed with nothing running.
    pub fn ready_to_reconcile(&self, now: DateTime<Utc>) -> bool {
        self.phase == Phase::Active
            && self.active_ops == 0
            && self.unresolved_recoveries == 0
            && self.quiet_deadline.is_some_and(|deadline| now >= deadline)
    }

    pub fn seconds_until_reconcile(&self, now: DateTime<Utc>) -> Option<i64> {
        if self.phase != Phase::Active || self.active_ops > 0 {
            return None;
        }
        self.quiet_deadline
            .map(|deadline| (deadline - now).num_seconds().max(0))
    }

    pub fn begin_reconcile(&mut self) {
        self.phase = Phase::Reconciling;
    }

    pub fn begin_restore(&mut self) {
        self.phase = Phase::Restoring;
    }

    pub fn restored(&mut self) {
        self.phase = Phase::Idle;
        self.saved = SavedSettings::default();
        self.affected_libraries.clear();
        self.quiet_deadline = None;
        self.message = None;
    }
}

/// What restoring one boolean setting should do. Only a value this coordinator changed is put
/// back, and only when nobody changed it meanwhile (it still holds the value we wrote).
pub fn restore_value(
    saved_before: Option<bool>,
    value_we_wrote: bool,
    current: bool,
) -> Option<bool> {
    match saved_before {
        Some(before) if current == value_we_wrote && before != current => Some(before),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).single().unwrap()
    }

    fn active() -> MaintenanceState {
        let mut s = MaintenanceState::default();
        assert_eq!(s.admission(), Admission::Prepare);
        s.begin_prepare();
        assert_eq!(s.admission(), Admission::Wait);
        s.prepared(SavedSettings::default(), t(0));
        s
    }

    #[test]
    fn reconciles_only_after_exactly_900_idle_seconds() {
        let mut s = active();
        s.admit(t(10), &["lib".into()]);
        assert!(
            !s.ready_to_reconcile(t(5000)),
            "never while work is running"
        );
        s.settle(t(100));
        assert!(!s.ready_to_reconcile(t(100 + 899)));
        assert!(s.ready_to_reconcile(t(100 + 900)));
        assert_eq!(s.seconds_until_reconcile(t(100 + 600)), Some(300));
    }

    #[test]
    fn new_work_restarts_the_quiet_period() {
        let mut s = active();
        s.admit(t(0), &[]);
        s.settle(t(10));
        s.admit(t(800), &[]);
        s.settle(t(850));
        assert!(!s.ready_to_reconcile(t(910)));
        assert!(s.ready_to_reconcile(t(850 + 900)));
    }

    #[test]
    fn concurrent_operations_all_have_to_settle() {
        let mut s = active();
        s.admit(t(0), &[]);
        s.admit(t(1), &[]);
        s.settle(t(2));
        assert!(!s.ready_to_reconcile(t(2000)));
        s.settle(t(3));
        assert!(s.ready_to_reconcile(t(903)));
    }

    #[test]
    fn unresolved_recovery_keeps_maintenance_on() {
        let mut s = active();
        s.unresolved_recoveries = 1;
        assert!(!s.ready_to_reconcile(t(100_000)));
    }

    #[test]
    fn work_waits_during_reconciliation_and_blocks_on_failure() {
        let mut s = active();
        s.begin_reconcile();
        assert_eq!(s.admission(), Admission::Wait);
        s.begin_restore();
        assert_eq!(s.admission(), Admission::Wait);
        s.restored();
        assert_eq!(s.admission(), Admission::Prepare);
        s.block("config file managed");
        assert!(matches!(s.admission(), Admission::Deny(_)));
    }

    #[test]
    fn restoration_respects_admin_changes() {
        // We turned watching off (it was on): put it back.
        assert_eq!(restore_value(Some(true), false, false), Some(true));
        // An admin turned it on meanwhile: leave it.
        assert_eq!(restore_value(Some(true), false, true), None);
        // We never changed it: leave it.
        assert_eq!(restore_value(None, false, false), None);
    }
}
