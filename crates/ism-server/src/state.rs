//! Shared application state.

use std::collections::HashMap;
use std::sync::Arc;

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
    OperationUpdated { operation_id: String },
    OperationCommitted(OperationCommitted),
    MaintenanceChanged { server_id: String },
    SyncChanged,
    ConflictsChanged,
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
    pub pg_pools: Mutex<HashMap<String, PgPool>>,
    /// Serialises read-modify-write of the per-server maintenance state.
    pub maintenance_lock: Mutex<()>,
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
            pg_pools: Mutex::new(HashMap::new()),
            maintenance_lock: Mutex::new(()),
        })))
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
