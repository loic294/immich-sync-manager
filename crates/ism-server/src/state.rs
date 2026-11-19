//! Shared application state.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use ism_core::dto::OperationCommitted;
use serde::Serialize;
use sqlx::SqlitePool;
use sqlx::postgres::PgPool;
use tokio::sync::{Mutex, Notify, broadcast};

use crate::config::Config;
use crate::crypto::SecretBox;

/// Live events for SSE subscribers.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    OperationUpdated {
        operation_id: String,
    },
    OperationCommitted(OperationCommitted),
    MaintenanceChanged {
        server_id: String,
    },
    SyncChanged,
    ConflictsChanged,
    /// Watcher status or the library scan queue changed.
    WatchChanged,
    /// An ISM-requested library scan finished (the library queue went idle). Uploads and peer
    /// copies waiting for Immich asset IDs can look them up now.
    LibraryScanned {
        server_id: String,
        library_id: String,
    },
    /// The shared file settings or the primary instance changed.
    SettingsChanged,
}

pub struct Inner {
    pub config: Config,
    pub db: SqlitePool,
    pub secrets: SecretBox,
    pub instance_id: String,
    pub http: reqwest::Client,
    pub events: broadcast::Sender<ServerEvent>,
    /// Wakes the operation executor.
    pub ops_wake: Notify,
    /// Wakes the maintenance controller.
    pub maintenance_wake: Notify,
    /// Wakes the replication workers.
    pub sync_wake: Notify,
    /// Wakes the library scan scheduler (a scan was requested or a barrier released).
    pub scan_wake: Notify,
    /// Wakes the watcher supervisor (mappings or watch settings changed).
    pub watch_wake: Notify,
    /// Shares with locally detected changes and when to scan them (after files settle).
    pub share_changes: std::sync::Mutex<HashMap<String, std::time::Instant>>,
    /// Wakes the share scanner when `share_changes` gains an entry.
    pub share_wake: Notify,
    pub pg_pools: Mutex<HashMap<String, PgPool>>,
    /// Serialises read-modify-write of the per-server maintenance state.
    pub maintenance_lock: Mutex<()>,
    /// Serializes replica bookkeeping with managed operations.
    pub replication_operation_lock: Mutex<()>,
    /// The shared file settings in force; replaced as a whole by `settings::update`.
    pub shared_settings: std::sync::RwLock<Arc<crate::settings::Snapshot>>,
    /// Serializes read-modify-write of the shared file settings.
    pub shared_settings_lock: Mutex<()>,
    request_locks: Mutex<HashMap<String, Weak<Mutex<()>>>>,
}

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

impl std::ops::Deref for AppState {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

impl AppState {
    pub async fn new(config: Config, db: SqlitePool, secrets: SecretBox) -> anyhow::Result<Self> {
        let instance_id = match crate::db::get_setting(&db, "instance_id").await? {
            Some(id) => id,
            None => {
                let id = crate::crypto::new_id("ism");
                crate::db::set_setting(&db, "instance_id", &id).await?;
                id
            }
        };
        let shared_settings = crate::settings::load(&db, &instance_id).await?;
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        let (events, _) = broadcast::channel(512);
        Ok(Self(Arc::new(Inner {
            config,
            db,
            secrets,
            instance_id,
            http,
            events,
            ops_wake: Notify::new(),
            maintenance_wake: Notify::new(),
            sync_wake: Notify::new(),
            scan_wake: Notify::new(),
            watch_wake: Notify::new(),
            share_changes: std::sync::Mutex::new(HashMap::new()),
            share_wake: Notify::new(),
            pg_pools: Mutex::new(HashMap::new()),
            maintenance_lock: Mutex::new(()),
            replication_operation_lock: Mutex::new(()),
            shared_settings: std::sync::RwLock::new(Arc::new(crate::settings::Snapshot::new(
                shared_settings,
            ))),
            shared_settings_lock: Mutex::new(()),
            request_locks: Mutex::new(HashMap::new()),
        })))
    }

    pub async fn lock_upload(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_key(id).await
    }

    pub async fn lock_operation_submission(
        &self,
        principal: &str,
        key: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let hash = crate::crypto::sha256_hex(format!("{principal}\0{key}").as_bytes());
        self.lock_key(&format!("operation:{hash}")).await
    }

    async fn lock_key(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.request_locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(id.to_owned(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }

    pub fn emit(&self, event: ServerEvent) {
        let _ = self.events.send(event);
    }

    pub async fn instance_name(&self) -> String {
        crate::db::get_setting(&self.db, "instance_name")
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "Immich Sync Manager".into())
    }
}
