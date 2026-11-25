//! In-memory deduplication for repeated wait diagnostics.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const WAIT_LOG_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const WAIT_LOG_SWEEP_INTERVAL: Duration = Duration::from_secs(30 * 60);

struct WaitLog {
    reason: String,
    next_at: Option<String>,
    blocked: Option<bool>,
    logged_at: Instant,
}

#[derive(Default)]
struct WaitLogState {
    entries: HashMap<String, WaitLog>,
    last_sweep: Option<Instant>,
}

impl WaitLogState {
    fn expire(&mut self, now: Instant) {
        if self
            .last_sweep
            .is_some_and(|last| now.duration_since(last) < WAIT_LOG_SWEEP_INTERVAL)
        {
            return;
        }
        self.entries
            .retain(|_, entry| now.duration_since(entry.logged_at) < WAIT_LOG_INTERVAL);
        if self.entries.len() < self.entries.capacity() / 2 {
            self.entries.shrink_to_fit();
        }
        self.last_sweep = Some(now);
    }
}

#[derive(Default)]
pub(crate) struct WaitLogs(Mutex<WaitLogState>);

impl WaitLogs {
    pub(crate) fn take_report(
        &self,
        id: &str,
        reason: &str,
        next_at: Option<&str>,
        blocked: Option<bool>,
    ) -> bool {
        let now = Instant::now();
        let mut state = match self.0.lock() {
            Ok(state) => state,
            Err(error) => {
                tracing::error!("wait log lock poisoned; retaining recorded evidence");
                self.0.clear_poison();
                error.into_inner()
            }
        };
        state.expire(now);
        if state.entries.get(id).is_some_and(|entry| {
            entry.reason == reason
                && entry.next_at.as_deref() == next_at
                && entry.blocked == blocked
                && now.duration_since(entry.logged_at) < WAIT_LOG_INTERVAL
        }) {
            return false;
        }
        state.entries.insert(
            id.to_owned(),
            WaitLog {
                reason: reason.to_owned(),
                next_at: next_at.map(str::to_owned),
                blocked,
                logged_at: now,
            },
        );
        true
    }

    pub(crate) fn expire(&self) {
        let mut state = match self.0.lock() {
            Ok(state) => state,
            Err(error) => {
                tracing::error!("wait log lock poisoned; retaining recorded evidence");
                self.0.clear_poison();
                error.into_inner()
            }
        };
        state.expire(Instant::now());
    }

    pub(crate) fn clear(&self, id: &str) {
        match self.0.lock() {
            Ok(mut state) => {
                state.entries.remove(id);
            }
            Err(error) => {
                tracing::error!("wait log lock poisoned; retaining recorded evidence");
                self.0.clear_poison();
                error.into_inner().entries.remove(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn distinct_transfer_waits_are_not_dropped_at_large_backlogs() {
        let waits = super::WaitLogs::default();
        for id in 0..33_000 {
            assert!(
                waits.take_report(
                    &format!("transfer-{id}"),
                    "Peer is not online",
                    None,
                    Some(true)
                ),
                "wait for transfer-{id} was not recorded"
            );
        }
        for id in [0, 32_999] {
            assert!(!waits.take_report(
                &format!("transfer-{id}"),
                "Peer is not online",
                None,
                Some(true)
            ));
        }
    }

    #[test]
    fn expired_wait_records_are_removed_without_losing_recent_ones() {
        let now = std::time::Instant::now();
        let mut state = super::WaitLogState::default();
        for (id, logged_at) in [("old", now), ("recent", now + super::WAIT_LOG_INTERVAL)] {
            state.entries.insert(
                id.into(),
                super::WaitLog {
                    reason: "Peer is not online".into(),
                    next_at: None,
                    blocked: Some(true),
                    logged_at,
                },
            );
        }
        state.last_sweep = Some(now);
        state.expire(now + super::WAIT_LOG_INTERVAL + std::time::Duration::from_secs(1));
        assert!(!state.entries.contains_key("old"));
        assert!(state.entries.contains_key("recent"));
    }
}
