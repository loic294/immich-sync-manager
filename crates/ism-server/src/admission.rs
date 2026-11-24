//! Admission between managed operations and replica bookkeeping.
//!
//! Every participant takes the same mutex to be admitted. An operation is admitted exclusively
//! and registers itself as *pending* before it releases the mutex to wait for the maintenance
//! barrier; it changes no files in that phase. Share scans (which change no library files either)
//! may run beside pending operations. Before its first change the operation re-takes the mutex,
//! which waits for any admitted scan, and becomes *mutating*; scans then wait until it finishes.
//! Every other participant waits until no operation is registered, as before.
//!
//! Registrations live only in memory: after a restart or panic, operation rows left `running` or
//! `waiting_maintenance` have none and must be treated as mutating.

use std::pin::pin;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, MutexGuard, Notify, broadcast};

use crate::state::ServerEvent;

pub(crate) const WAIT_NOTICE_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Pending,
    Mutating,
}

#[derive(Default)]
pub struct OperationGate {
    mutex: Mutex<()>,
    operations: std::sync::Mutex<Vec<(String, Phase)>>,
    released: Notify,
    reporting: std::sync::Mutex<Reporting>,
    sequence: AtomicU64,
    events: Option<broadcast::Sender<ServerEvent>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Holder {
    Scan { share_id: String },
    Operation { id: String, phase: Phase },
    Unknown,
}

#[derive(Clone)]
pub(crate) struct Waiting {
    pub operation_id: String,
    pub started: Instant,
    pub holder: Holder,
    token: u64,
    holder_token: Option<u64>,
}

#[derive(Default)]
struct Reporting {
    holder: Option<(u64, Holder)>,
    waiters: Vec<Waiting>,
}

pub struct GateGuard<'a> {
    gate: &'a OperationGate,
    guard: Option<MutexGuard<'a, ()>>,
    token: u64,
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        let mut reporting = self.gate.reporting();
        if reporting
            .holder
            .as_ref()
            .is_some_and(|(token, _)| *token == self.token)
        {
            reporting.holder = None;
        }
        drop(self.guard.take());
        let changed = !reporting.waiters.is_empty();
        drop(reporting);
        if changed {
            self.gate.changed();
        }
    }
}

struct Waiter<'a> {
    gate: &'a OperationGate,
    token: u64,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.gate
            .reporting()
            .waiters
            .retain(|entry| entry.token != self.token);
        self.gate.changed();
    }
}

impl OperationGate {
    pub(crate) fn with_events(events: broadcast::Sender<ServerEvent>) -> Self {
        Self {
            events: Some(events),
            ..Self::default()
        }
    }

    fn reporting(&self) -> std::sync::MutexGuard<'_, Reporting> {
        self.reporting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn changed(&self) {
        if let Some(events) = &self.events {
            let _ = events.send(ServerEvent::ActivityChanged);
        }
    }

    pub(crate) fn waiting(&self) -> Vec<Waiting> {
        let operations = self.operations();
        let reporting = self.reporting();
        let holder = operations.first().map_or_else(
            || {
                reporting
                    .holder
                    .as_ref()
                    .map_or(Holder::Unknown, |(_, holder)| holder.clone())
            },
            |(id, phase)| Holder::Operation {
                id: id.clone(),
                phase: *phase,
            },
        );
        reporting
            .waiters
            .iter()
            .cloned()
            .map(|mut wait| {
                wait.holder = holder.clone();
                wait.holder_token = reporting.holder.as_ref().map(|(token, _)| *token);
                wait
            })
            .collect()
    }

    pub(crate) fn still_waiting(&self, candidate: &Waiting) -> bool {
        self.waiting().iter().any(|current| {
            current.token == candidate.token
                && current.holder == candidate.holder
                && current.holder_token == candidate.holder_token
        })
    }

    pub(crate) async fn lock_operation(&self, id: &str) -> GateGuard<'_> {
        self.lock_operation_after(id, WAIT_NOTICE_AFTER).await
    }

    async fn lock_operation_after(&self, id: &str, notice_after: Duration) -> GateGuard<'_> {
        let token = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.reporting().waiters.push(Waiting {
            operation_id: id.to_owned(),
            started: Instant::now(),
            holder: Holder::Unknown,
            token,
            holder_token: None,
        });
        let waiter = Waiter { gate: self, token };
        self.changed();
        let acquisition = self.lock_when(|ops| ops.is_empty(), Holder::Unknown, Some(token));
        tokio::pin!(acquisition);
        let guard = tokio::select! {
            biased;
            guard = &mut acquisition => guard,
            () = tokio::time::sleep(notice_after) => {
                self.changed();
                acquisition.await
            }
        };
        drop(waiter);
        guard
    }

    #[cfg(test)]
    pub(crate) fn age_wait_for_test(&self, operation_id: &str, age: Duration) {
        for wait in &mut self.reporting().waiters {
            if wait.operation_id == operation_id {
                wait.started = Instant::now() - age;
            }
        }
    }

    /// Exclusive admission: waits until no managed operation is registered.
    pub async fn lock(&self) -> GateGuard<'_> {
        self.lock_when(|ops| ops.is_empty(), Holder::Unknown, None)
            .await
    }

    /// Admission for share scans: waits only for operations that may be changing files. No
    /// operation can start changing files until the guard is dropped.
    #[cfg(test)]
    pub async fn lock_scan(&self) -> GateGuard<'_> {
        self.lock_when(
            |ops| ops.iter().all(|(_, phase)| *phase == Phase::Pending),
            Holder::Unknown,
            None,
        )
        .await
    }

    pub(crate) async fn lock_share_scan(&self, share_id: &str) -> GateGuard<'_> {
        self.lock_when(
            |ops| ops.iter().all(|(_, phase)| *phase == Phase::Pending),
            Holder::Scan {
                share_id: share_id.to_owned(),
            },
            None,
        )
        .await
    }

    async fn lock_when(
        &self,
        admit: impl Fn(&[(String, Phase)]) -> bool,
        holder: Holder,
        waiter: Option<u64>,
    ) -> GateGuard<'_> {
        loop {
            let guard = self.mutex.lock().await;
            // Registered before checking, so a change in between still wakes us.
            let mut changed = pin!(self.released.notified());
            changed.as_mut().enable();
            if admit(&self.operations()) {
                let token = self.sequence.fetch_add(1, Ordering::Relaxed);
                let mut reporting = self.reporting();
                if let Some(waiter) = waiter {
                    reporting.waiters.retain(|entry| entry.token != waiter);
                }
                reporting.holder = Some((token, holder));
                let changed = waiter.is_some() || !reporting.waiters.is_empty();
                drop(reporting);
                if changed {
                    self.changed();
                }
                return GateGuard {
                    gate: self,
                    guard: Some(guard),
                    token,
                };
            }
            drop(guard);
            changed.await;
        }
    }

    fn operations(&self) -> std::sync::MutexGuard<'_, Vec<(String, Phase)>> {
        self.operations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers an operation as pending. `_admitted` proves it was admitted exclusively.
    pub fn register(&self, _admitted: &GateGuard<'_>, operation_id: &str) -> Registration<'_> {
        self.operations()
            .push((operation_id.to_owned(), Phase::Pending));
        self.changed();
        Registration {
            gate: self,
            operation_id: operation_id.to_owned(),
        }
    }

    /// Whether this process tracks the operation (pending or mutating).
    pub fn is_registered(&self, operation_id: &str) -> bool {
        self.operations().iter().any(|(id, _)| id == operation_id)
    }

    /// Whether a registered operation may be changing files (a snapshot, for reporting only).
    pub fn mutating(&self) -> bool {
        self.operations()
            .iter()
            .any(|(_, phase)| *phase == Phase::Mutating)
    }
}

/// Unregisters the operation when it stops (including on error or panic).
pub struct Registration<'a> {
    gate: &'a OperationGate,
    operation_id: String,
}

impl Registration<'_> {
    /// Waits for admitted scans to finish, then marks the operation as changing files.
    pub async fn begin_mutating(&self) {
        let _admitted = self.gate.mutex.lock().await;
        for (id, phase) in self.gate.operations().iter_mut() {
            if *id == self.operation_id {
                *phase = Phase::Mutating;
            }
        }
        self.gate.changed();
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.gate
            .operations()
            .retain(|(id, _)| *id != self.operation_id);
        self.gate.released.notify_waiters();
        self.gate.changed();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn admitted<T>(future: impl std::future::Future<Output = T>) -> bool {
        tokio::time::timeout(Duration::from_millis(50), future)
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn scans_run_beside_pending_operations_but_never_beside_mutation() {
        let gate = OperationGate::default();
        let exclusive = gate.lock().await;
        let operation = gate.register(&exclusive, "op");
        drop(exclusive);
        assert!(gate.is_registered("op") && !gate.mutating());

        let scan = gate.lock_scan().await;
        assert!(!admitted(gate.lock()).await, "other bookkeeping waits");
        {
            let mutating = operation.begin_mutating();
            let mut mutating = pin!(mutating);
            assert!(
                !admitted(mutating.as_mut()).await,
                "mutation waits for the admitted scan"
            );
            assert!(!gate.mutating());
            drop(scan);
            assert!(admitted(mutating).await);
        }
        assert!(gate.mutating());
        assert!(!admitted(gate.lock_scan()).await);

        let waiting = gate.lock_scan();
        let mut waiting = pin!(waiting);
        assert!(!admitted(waiting.as_mut()).await);
        drop(operation);
        assert!(admitted(waiting).await);
        assert!(!gate.is_registered("op"));
        assert!(admitted(gate.lock()).await);
    }

    #[tokio::test]
    async fn operation_wait_observes_scan_owner_and_clears_on_release() {
        let gate = OperationGate::default();
        let scan = gate.lock_share_scan("share-example").await;
        let operation = gate.lock_operation("operation-example");
        tokio::pin!(operation);
        assert!(!admitted(operation.as_mut()).await);
        let waiting = gate.waiting();
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].operation_id, "operation-example");
        assert_eq!(
            waiting[0].holder,
            Holder::Scan {
                share_id: "share-example".into()
            }
        );
        drop(scan);
        let admitted = operation.await;
        assert!(gate.waiting().is_empty());
        drop(admitted);
    }

    #[tokio::test]
    async fn operation_wait_tracks_registered_and_unknown_holders_and_cancellation() {
        let gate = OperationGate::default();
        let unknown = gate.lock().await;
        {
            let waiting = gate.lock_operation("operation-example");
            tokio::pin!(waiting);
            assert!(!admitted(waiting.as_mut()).await);
            assert_eq!(gate.waiting()[0].holder, Holder::Unknown);
        }
        assert!(gate.waiting().is_empty());
        let registered = gate.register(&unknown, "holding-operation");
        drop(unknown);
        {
            let waiting = gate.lock_operation("operation-example");
            tokio::pin!(waiting);
            assert!(!admitted(waiting.as_mut()).await);
            assert_eq!(
                gate.waiting()[0].holder,
                Holder::Operation {
                    id: "holding-operation".into(),
                    phase: Phase::Pending
                }
            );
            drop(registered);
            let acquired = waiting.await;
            assert!(gate.waiting().is_empty());
            drop(acquired);
        }
    }

    #[tokio::test]
    async fn operation_wait_reports_holder_change_without_changing_admission() {
        let (events, mut receiver) = tokio::sync::broadcast::channel(32);
        let gate = OperationGate::with_events(events);
        let exclusive = gate.lock().await;
        let registered = gate.register(&exclusive, "other-operation");
        drop(exclusive);
        let scan = gate.lock_share_scan("share-example").await;
        let waiting = gate.lock_operation("operation-example");
        tokio::pin!(waiting);
        assert!(!admitted(waiting.as_mut()).await);
        assert_eq!(
            gate.waiting()[0].holder,
            Holder::Operation {
                id: "other-operation".into(),
                phase: Phase::Pending
            }
        );
        let before = gate.waiting().remove(0);
        while receiver.try_recv().is_ok() {}
        drop(registered);
        assert!(receiver.try_recv().is_ok());
        assert_eq!(
            gate.waiting()[0].holder,
            Holder::Scan {
                share_id: "share-example".into()
            }
        );
        assert!(!gate.still_waiting(&before));
        assert!(gate.still_waiting(&gate.waiting()[0]));
        drop(scan);
        let acquired = waiting.await;
        assert!(gate.waiting().is_empty());
        drop(acquired);
    }

    #[tokio::test]
    async fn sustained_wait_emits_a_transition_event_without_releasing_the_gate() {
        let (events, mut receiver) = tokio::sync::broadcast::channel(32);
        let gate = OperationGate::with_events(events);
        let scan = gate.lock_share_scan("share-example").await;
        let waiting = gate.lock_operation_after("operation-example", Duration::from_millis(20));
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), waiting.as_mut())
                .await
                .is_err()
        );
        while receiver.try_recv().is_ok() {}
        assert!(
            tokio::time::timeout(Duration::from_millis(60), waiting.as_mut())
                .await
                .is_err()
        );
        assert!(receiver.try_recv().is_ok());
        assert_eq!(gate.waiting().len(), 1);
        drop(scan);
        drop(waiting.await);
        assert!(gate.waiting().is_empty());
    }
}
