//! Runtime observations of actual worker exits, never elapsed-time guesses.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Other,
    Operations,
    Transfers,
    PeerReplication,
    PeerWorkers,
    Recovery,
}

#[derive(Clone)]
pub(crate) struct WorkerFailure {
    pub name: String,
    pub kind: Kind,
    pub message: String,
    pub observed_at: String,
}

struct Worker {
    revision: i64,
    kind: Kind,
    failure: Option<WorkerFailure>,
}

#[derive(Clone)]
pub(crate) struct TransferFailure {
    pub reason: TransferStop,
    pub observed_at: String,
    revision: i64,
}

#[derive(Clone)]
pub(crate) struct ConflictClaim {
    pub target_id: String,
    pub generation: i64,
    pub started_at: String,
}

#[derive(Clone)]
pub(crate) struct ConflictFailure {
    pub stopped_at: String,
}

pub(crate) type ConflictKey = (String, i64);

pub(crate) struct ConflictObservations {
    pub active: HashMap<ConflictKey, String>,
    pub stopped: HashMap<ConflictKey, ConflictFailure>,
    pub stopped_workers: HashSet<String>,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum TransferStop {
    TaskStopped,
    OutcomeNotSaved,
}

impl TransferStop {
    pub fn key(self) -> &'static str {
        match self {
            Self::TaskStopped => "task-stopped",
            Self::OutcomeNotSaved => "outcome-not-saved",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::TaskStopped => "The transfer task stopped before reporting an outcome",
            Self::OutcomeNotSaved => "The transfer task ended but its outcome could not be saved",
        }
    }
}

#[derive(Default)]
struct State {
    workers: HashMap<String, Worker>,
    transfers: HashMap<String, TransferFailure>,
    conflict_claims: HashMap<String, ConflictClaim>,
    conflict_failures: HashMap<ConflictKey, ConflictFailure>,
}

#[derive(Default)]
pub(crate) struct Registry(Mutex<State>);

impl Registry {
    fn lock(&self) -> MutexGuard<'_, State> {
        match self.0.lock() {
            Ok(state) => state,
            Err(error) => {
                tracing::error!("worker evidence lock poisoned; retaining recorded evidence");
                self.0.clear_poison();
                error.into_inner()
            }
        }
    }

    pub fn started(&self, name: &str, kind: Kind, revision: i64) {
        self.lock().workers.insert(
            name.into(),
            Worker {
                revision,
                kind,
                failure: None,
            },
        );
    }

    pub fn stopped(&self, name: &str, message: &str, observed_at: String) {
        let mut state = self.lock();
        let Some(worker) = state.workers.get_mut(name) else {
            tracing::error!(worker = name, "unregistered worker stopped");
            return;
        };
        worker.failure = Some(WorkerFailure {
            name: name.into(),
            kind: worker.kind,
            message: message.into(),
            observed_at,
        });
    }

    pub fn current_record(&self, key: &str, revision: i64) -> bool {
        let Some(name) = key.strip_prefix("worker:") else {
            return true;
        };
        self.lock()
            .workers
            .get(name)
            .is_none_or(|worker| revision > worker.revision)
    }

    pub fn stopped_workers(&self) -> Vec<WorkerFailure> {
        let mut workers: Vec<_> = self
            .lock()
            .workers
            .values()
            .filter_map(|worker| worker.failure.clone())
            .collect();
        workers.sort_by(|a, b| a.name.cmp(&b.name));
        workers
    }

    pub fn transfer_stopped(&self, id: &str, reason: TransferStop, revision: i64) {
        self.lock().transfers.insert(
            id.into(),
            TransferFailure {
                reason,
                observed_at: crate::db::now(),
                revision,
            },
        );
    }

    pub fn transfer_started(&self, id: &str) {
        self.lock().transfers.remove(id);
    }

    pub fn transfer_failures(&self) -> HashMap<String, TransferFailure> {
        self.lock().transfers.clone()
    }

    pub fn discard_finished_transfers(
        &self,
        observed: &HashMap<String, TransferFailure>,
        running: &HashSet<String>,
    ) {
        let mut state = self.lock();
        for (id, failure) in observed {
            if !running.contains(id)
                && state
                    .transfers
                    .get(id)
                    .is_some_and(|current| current.revision == failure.revision)
            {
                state.transfers.remove(id);
            }
        }
    }

    pub fn conflict_claimed(&self, worker: &str, target_id: &str, generation: i64) {
        let mut state = self.lock();
        if state
            .workers
            .get(worker)
            .is_none_or(|worker| worker.failure.is_some())
        {
            tracing::error!(
                worker,
                "unregistered or stopped worker claimed conflict evidence"
            );
            return;
        }
        state
            .conflict_failures
            .remove(&(target_id.to_owned(), generation));
        state.conflict_claims.insert(
            worker.to_owned(),
            ConflictClaim {
                target_id: target_id.to_owned(),
                generation,
                started_at: crate::db::now(),
            },
        );
    }

    pub fn conflict_finished(&self, worker: &str, target_id: &str, generation: i64) {
        let mut state = self.lock();
        if state
            .conflict_claims
            .get(worker)
            .is_some_and(|claim| claim.target_id == target_id && claim.generation == generation)
        {
            state.conflict_claims.remove(worker);
        }
    }

    pub fn conflict_worker_stopped(&self, worker: &str) {
        let mut state = self.lock();
        if let Some(claim) = state.conflict_claims.remove(worker) {
            state.conflict_failures.insert(
                (claim.target_id, claim.generation),
                ConflictFailure {
                    stopped_at: crate::db::now(),
                },
            );
        }
    }

    pub fn conflict_supervisor_stopped(&self, names: &[&str]) {
        let mut state = self.lock();
        let stopped_at = crate::db::now();
        for &name in names {
            if let Some(claim) = state.conflict_claims.remove(name) {
                state.conflict_failures.insert(
                    (claim.target_id, claim.generation),
                    ConflictFailure {
                        stopped_at: stopped_at.clone(),
                    },
                );
            }
            if let Some(worker) = state.workers.get_mut(name)
                && worker.failure.is_none()
            {
                worker.failure = Some(WorkerFailure {
                    name: name.into(),
                    kind: worker.kind,
                    message: "Conflict evidence supervisor stopped".into(),
                    observed_at: stopped_at.clone(),
                });
            }
        }
    }

    pub fn conflict_observations(&self) -> ConflictObservations {
        let state = self.lock();
        ConflictObservations {
            active: state
                .conflict_claims
                .values()
                .map(|claim| {
                    (
                        (claim.target_id.clone(), claim.generation),
                        claim.started_at.clone(),
                    )
                })
                .collect(),
            stopped: state.conflict_failures.clone(),
            stopped_workers: state
                .workers
                .iter()
                .filter(|(_, worker)| worker.failure.is_some())
                .map(|(name, _)| name.clone())
                .collect(),
        }
    }
}
