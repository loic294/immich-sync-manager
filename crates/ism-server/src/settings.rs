//! The "Files and sidecars" settings shared with peers and OPM: storage, the in-memory copy the
//! workers read, and the primary-instance replication (see `ism_core::file_settings`).

use std::sync::Arc;

use ism_core::dto::{
    ClientSettings, FileSettings, InstanceRef, PrimaryCandidate, SettingsInput, SettingsView,
};
use ism_core::file_settings::{JunkRules, ManagedByPrimary, SharedSettings};
use sqlx::SqlitePool;

use crate::db;
use crate::error::{AppError, AppResult};
use crate::peers::net;
use crate::state::{AppState, ServerEvent};

const KEY: &str = "shared_settings";
/// Before shared settings, ISM kept only a comma-separated companion list under this key.
const LEGACY_COMPANIONS_KEY: &str = "companion_extensions";

/// The settings in force and the rules derived from them.
#[derive(Debug)]
pub struct Snapshot {
    pub settings: SharedSettings,
    pub junk: Arc<JunkRules>,
}

impl Snapshot {
    pub fn new(settings: SharedSettings) -> Self {
        let junk = Arc::new(settings.files.junk_rules());
        Self { settings, junk }
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Reads the stored settings, migrating the legacy companion list on first start.
pub async fn load(db: &SqlitePool, self_id: &str) -> anyhow::Result<SharedSettings> {
    if let Some(raw) = db::get_setting(db, KEY).await? {
        match serde_json::from_str::<SharedSettings>(&raw) {
            Ok(mut settings) => {
                settings.files = settings.files.lenient();
                return Ok(settings);
            }
            Err(error) => {
                tracing::warn!("stored file settings are unreadable ({error}); using defaults");
                return Ok(SharedSettings::initial(self_id, FileSettings::default()));
            }
        }
    }
    let files = match db::get_setting(db, LEGACY_COMPANIONS_KEY).await? {
        Some(raw) => {
            let mut sidecar_extensions = vec!["xmp".to_owned()];
            sidecar_extensions.extend(raw.split(',').map(str::to_owned));
            FileSettings {
                sidecar_extensions,
                ..FileSettings::default()
            }
            .lenient()
        }
        None => FileSettings::default(),
    };
    let settings = SharedSettings::initial(self_id, files);
    db::set_setting(db, KEY, &serde_json::to_string(&settings)?).await?;
    sqlx::query("DELETE FROM settings WHERE key = ?")
        .bind(LEGACY_COMPANIONS_KEY)
        .execute(db)
        .await?;
    Ok(settings)
}

impl AppState {
    pub fn shared_settings(&self) -> Arc<Snapshot> {
        match self.shared_settings.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// The ignored names and prefixes in force.
    pub fn junk_rules(&self) -> Arc<JunkRules> {
        self.shared_settings().junk.clone()
    }

    /// The extensions of files that travel with their original.
    pub fn sidecar_extensions(&self) -> Vec<String> {
        self.shared_settings()
            .settings
            .files
            .sidecar_extensions
            .clone()
    }
}

/// Applies `change` under the settings lock; stores, publishes and announces the result when it
/// reports a change.
async fn update(
    state: &AppState,
    change: impl FnOnce(&mut SharedSettings) -> AppResult<bool>,
) -> AppResult<bool> {
    let _guard = state.shared_settings_lock.lock().await;
    let mut next = state.shared_settings().settings.clone();
    if !change(&mut next)? {
        return Ok(false);
    }
    db::set_setting(&state.db, KEY, &serde_json::to_string(&next)?).await?;
    let snapshot = Arc::new(Snapshot::new(next));
    match state.shared_settings.write() {
        Ok(mut guard) => *guard = snapshot,
        Err(poisoned) => *poisoned.into_inner() = snapshot,
    }
    state.emit(ServerEvent::SettingsChanged);
    Ok(true)
}

/// The name to show for an instance: ours, a paired peer's, or the one recorded with it.
async fn instance_name(state: &AppState, instance_id: &str, recorded: Option<&str>) -> String {
    if instance_id == state.instance_id {
        return state.instance_name().await;
    }
    let peer: Option<String> = sqlx::query_scalar("SELECT name FROM peers WHERE instance_id = ?")
        .bind(instance_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    peer.or_else(|| recorded.map(str::to_owned))
        .unwrap_or_else(|| instance_id.to_owned())
}

async fn primary_ref(state: &AppState, settings: &SharedSettings) -> Option<InstanceRef> {
    let id = settings.primary_id()?;
    Some(InstanceRef {
        instance_id: id.to_owned(),
        name: instance_name(state, id, settings.primary.name.as_deref()).await,
    })
}

/// `GET /api/v1/client/settings` and the `settings_changed` event.
pub async fn client_view(state: &AppState) -> ClientSettings {
    let snapshot = state.shared_settings();
    let settings = &snapshot.settings;
    ClientSettings {
        files: settings.files.clone(),
        revision: settings.revision,
        origin_instance_id: settings.origin_instance_id.clone(),
        primary: primary_ref(state, settings).await,
    }
}

/// `GET /api/admin/settings`.
pub async fn admin_view(state: &AppState) -> AppResult<SettingsView> {
    let snapshot = state.shared_settings();
    let settings = &snapshot.settings;
    let instance_name = state.instance_name().await;
    let mut primary_candidates = vec![PrimaryCandidate {
        instance_id: state.instance_id.clone(),
        name: instance_name.clone(),
        this_instance: true,
    }];
    for peer in net::peers(state).await? {
        if let Some(instance_id) = peer.instance_id {
            primary_candidates.push(PrimaryCandidate {
                instance_id,
                name: peer.name,
                this_instance: false,
            });
        }
    }
    let primary = primary_ref(state, settings).await;
    if let Some(primary) = &primary
        && !primary_candidates
            .iter()
            .any(|c| c.instance_id == primary.instance_id)
    {
        // Chosen elsewhere and not paired with this instance: keep it selectable.
        primary_candidates.push(PrimaryCandidate {
            instance_id: primary.instance_id.clone(),
            name: primary.name.clone(),
            this_instance: false,
        });
    }
    Ok(SettingsView {
        instance_id: state.instance_id.clone(),
        instance_name,
        files: settings.files.clone(),
        files_revision: settings.revision,
        primary,
        files_editable: settings.editable_by(&state.instance_id),
        primary_candidates,
    })
}

/// Applies `PUT /api/admin/settings`: the primary first, then the files (refused while another
/// instance is primary, unless unchanged).
pub async fn apply_admin(state: &AppState, input: &SettingsInput) -> AppResult<()> {
    let files = match &input.files {
        Some(files) => Some(files.normalized().map_err(|errors| {
            let message = errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            AppError::bad_request("invalid_request", message)
        })?),
        None => None,
    };
    let primary = match input.primary_instance_id.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(id) => {
            let view = admin_view(state).await?;
            let candidate = view
                .primary_candidates
                .into_iter()
                .find(|c| c.instance_id == id)
                .ok_or_else(|| {
                    AppError::bad_request(
                        "invalid_request",
                        "the primary must be this instance or a paired peer",
                    )
                })?;
            Some((candidate.instance_id, candidate.name))
        }
    };
    let self_id = state.instance_id.clone();
    let changed = update(state, |settings| {
        let now = now_ms();
        let mut changed = settings.designate(primary, &self_id, now);
        if let Some(files) = files {
            changed |= settings
                .set_files(files, &self_id, now)
                .map_err(|ManagedByPrimary| {
                    AppError::conflict(
                        "settings_managed_by_primary",
                        "another instance is primary; change the file settings there",
                    )
                })?;
        }
        Ok(changed)
    })
    .await?;
    if changed {
        // Peers pick the new state up on their next poll; poll them now.
        state.sync_wake.notify_one();
    }
    Ok(())
}

/// Folds in the state a peer serves (`GET /api/peer/v1/settings`).
pub async fn merge_remote(state: &AppState, remote: &SharedSettings) -> AppResult<bool> {
    let self_id = state.instance_id.clone();
    update(state, |settings| {
        Ok(settings.merge(remote, &self_id, now_ms()))
    })
    .await
}
