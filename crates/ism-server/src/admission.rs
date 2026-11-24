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

use tokio::sync::{Mutex, MutexGuard, Notify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pending,
    Mutating,
}

#[derive(Default)]
pub struct OperationGate {
    mutex: Mutex<()>,
    operations: std::sync::Mutex<Vec<(String, Phase)>>,
    released: Notify,
}

impl OperationGate {
    /// Exclusive admission: waits until no managed operation is registered.
    pub async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock_when(|ops| ops.is_empty()).await
    }

    /// Admission for share scans: waits only for operations that may be changing files. No
    /// operation can start changing files until the guard is dropped.
    pub async fn lock_scan(&self) -> MutexGuard<'_, ()> {
        self.lock_when(|ops| ops.iter().all(|(_, phase)| *phase == Phase::Pending))
            .await
    }

    async fn lock_when(&self, admit: impl Fn(&[(String, Phase)]) -> bool) -> MutexGuard<'_, ()> {
        loop {
            let guard = self.mutex.lock().await;
            // Registered before checking, so a change in between still wakes us.
            let mut changed = pin!(self.released.notified());
            changed.as_mut().enable();
            if admit(&self.operations()) {
                return guard;
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
    pub fn register(&self, _admitted: &MutexGuard<'_, ()>, operation_id: &str) -> Registration<'_> {
        self.operations()
            .push((operation_id.to_owned(), Phase::Pending));
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
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.gate
            .operations()
            .retain(|(id, _)| *id != self.operation_id);
        self.gate.released.notify_waiters();
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
}
