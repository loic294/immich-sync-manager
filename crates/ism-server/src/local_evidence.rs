//! Local conditions independent of whether a file transfer is already queued.

use std::collections::HashSet;

use ism_core::dto::{
    ActivityExample, AttentionCode, AttentionCompleteness, AttentionItem, AttentionLevel,
    AttentionOwner, AttentionState,
};
use ism_core::maintenance::{MaintenanceState, Phase};

use crate::db;
use crate::error::AppResult;
use crate::state::AppState;

#[derive(sqlx::FromRow)]
struct Server {
    id: String,
    name: String,
    compatible: bool,
    maintenance_json: String,
}

#[derive(sqlx::FromRow)]
struct Conflict {
    share_id: String,
    share_name: String,
    peer_id: String,
    rel_path: String,
    count: i64,
}

#[derive(sqlx::FromRow)]
struct Watch {
    root_id: String,
    name: String,
    last_pass_at: Option<String>,
}

#[derive(sqlx::FromRow)]
struct Scan {
    library_id: String,
    library_name: Option<String>,
    due_at: Option<i64>,
    last_finished_at: Option<String>,
    last_error: String,
    pending: i64,
}

pub(crate) struct Selection {
    pub peer_id: String,
    pub roots: HashSet<String>,
    pub shares: HashSet<String>,
    pub servers: HashSet<String>,
    pub libraries: HashSet<String>,
}

fn base(state: &AppState, instance_name: &str, observed_at: &str) -> AttentionItem {
    AttentionItem {
        id: String::new(),
        code: AttentionCode::MaintenanceBlocked,
        state: AttentionState::Blocked,
        attention: AttentionLevel::ActionRequired,
        blocks_work: true,
        owner: AttentionOwner::Local,
        instance_id: Some(state.instance_id.clone()),
        instance_name: Some(instance_name.into()),
        peer_id: None,
        share_id: None,
        share_name: None,
        count: 1,
        total_count: Some(1),
        next_at: None,
        recheck_at: None,
        observed_at: observed_at.into(),
        title: String::new(),
        message: String::new(),
        href: "/servers".into(),
        operation_id: None,
        acknowledged: None,
        unresolved: Some(true),
        completeness: AttentionCompleteness::Complete,
        examples: Vec::new(),
    }
}

pub(crate) async fn snapshot(
    state: &AppState,
    instance_name: &str,
    observed_at: &str,
) -> AppResult<(Vec<AttentionItem>, Vec<String>)> {
    selected_snapshot(state, instance_name, observed_at, None).await
}

pub(crate) async fn selected_snapshot(
    state: &AppState,
    instance_name: &str,
    observed_at: &str,
    selection: Option<&Selection>,
) -> AppResult<(Vec<AttentionItem>, Vec<String>)> {
    let share_ids = selection
        .map(|s| serde_json::to_string(&s.shares))
        .transpose()?;
    let root_ids = selection
        .map(|s| serde_json::to_string(&s.roots))
        .transpose()?;
    let library_ids = selection
        .map(|s| serde_json::to_string(&s.libraries))
        .transpose()?;
    let peer_id = selection.map(|s| s.peer_id.as_str());
    let mut items = Vec::new();
    let mut warnings = Vec::new();
    let template = base(state, instance_name, observed_at);
    let servers: Vec<Server> = sqlx::query_as(
        "SELECT id,name,compatible,maintenance_json FROM immich_servers ORDER BY id",
    )
    .fetch_all(&state.db)
    .await?;
    for server in servers {
        if selection.is_some_and(|s| !s.servers.contains(&server.id)) {
            continue;
        }
        if !server.compatible {
            let mut item = template.clone();
            item.id = format!("compatibility:{}", server.id);
            item.title = format!("Check Immich compatibility: {}", server.name);
            item.message = "Changes that require direct database access cannot proceed. Run the compatibility check in Servers; ISM supports Immich 3.4.1.".into();
            items.push(item);
        }
        let maintenance: MaintenanceState = match serde_json::from_str(&server.maintenance_json) {
            Ok(maintenance) => maintenance,
            Err(error) => {
                tracing::error!(server_id = %server.id, error = %error, "maintenance evidence is unreadable");
                let mut item = template.clone();
                item.id = format!("maintenance:{}", server.id);
                item.state = AttentionState::Unavailable;
                item.blocks_work = false;
                item.unresolved = None;
                item.completeness = AttentionCompleteness::Unavailable;
                item.title = format!("Cannot verify maintenance: {}", server.name);
                item.message = "Saved maintenance status is unreadable. ISM cannot confirm that file changes are safe; review the server in Servers.".into();
                warnings.push(format!(
                    "Maintenance status for {} could not be read.",
                    server.name
                ));
                items.push(item);
                continue;
            }
        };
        if maintenance.scans_allowed() && maintenance.drift.is_none() {
            continue;
        }
        let mut item = template.clone();
        item.id = format!("maintenance:{}", server.id);
        if maintenance.drift.is_some() {
            item.title = format!("Restore safe Immich settings: {}", server.name);
            item.message = "Immich's file watcher or scheduled scan was turned on. Reapply ISM settings in Servers before managed file changes can continue.".into();
        } else {
            match maintenance.phase {
                Phase::Blocked => {
                    item.title = format!("Maintenance needs repair: {}", server.name);
                    item.message = "ISM could not safely pause or resume Immich's library queue. Review the server and retry maintenance in Servers.".into();
                }
                Phase::RecoveryRequired => {
                    item.title = format!("Resolve file recovery: {}", server.name);
                    item.message = "Some file changes could not be verified. Only those files are held; resolve them in Operations. Other files keep syncing.".into();
                    item.href = "/operations".into();
                }
                Phase::Preparing | Phase::Active | Phase::Resuming => {
                    item.code = AttentionCode::MaintenanceWaiting;
                    item.state = AttentionState::Waiting;
                    item.attention = AttentionLevel::Notice;
                    item.title = format!("Library scans are paused: {}", server.name);
                    item.message = match maintenance.phase {
                        Phase::Preparing => "ISM is pausing Immich's library queue before file changes. It will continue automatically when the queue is safely idle.".into(),
                        Phase::Active => "Library scans are paused for file operations. See their current status in Operations; scans wait until maintenance is released.".into(),
                        _ => "ISM is resuming Immich's library queue after file changes. Library scans will continue automatically.".into(),
                    };
                }
                Phase::Idle => {
                    item.title = format!("Library scans cannot start: {}", server.name);
                    item.message = "ISM still records a paused library queue or an unfinished file operation. Review maintenance in Servers and current work in Operations.".into();
                }
            }
        }
        items.push(item);
    }

    if selection.is_none() {
        let share_scans: Vec<(String, String, String, String, String, String, String)> =
            sqlx::query_as(
                "SELECT s.id,s.name,f.reason,f.observed_at,s.root_id,s.path,f.paths_json
         FROM share_scan_failures f
         JOIN shares s ON s.id=f.share_id WHERE s.enabled=1 ORDER BY s.id",
            )
            .fetch_all(&state.db)
            .await?;
        for (id, name, reason, at, root_id, path, paths_json) in share_scans {
            let mut item = template.clone();
            item.id = format!("share-scan:{id}");
            item.code = AttentionCode::ShareScanFailed;
            item.state = AttentionState::Failed;
            item.share_id = Some(id);
            item.share_name = Some(name.clone());
            item.title = format!("Shared files could not be checked: {name}");
            let folder = if path.is_empty() {
                format!("{root_id}:/")
            } else {
                format!("{root_id}:{path}")
            };
            item.message = if reason == "permission" {
                format!(
                    "Permission was denied while checking shared folder '{folder}'. On the NAS, give the account running ISM permission to read the affected files and to read and traverse their folders. Check file ownership and ACLs if access is still denied. ISM retries automatically."
                )
            } else if reason == "other" {
                format!(
                    "Check folder '{folder}' in Peers and review server logs. ISM retries automatically."
                )
            } else {
                "The share scan failed. Check server logs; ISM retries automatically.".into()
            };
            let paths: Vec<String> = serde_json::from_str(&paths_json)?;
            if !paths.is_empty() {
                item.message.push_str(&format!(
                    " Paths that could not be checked (relative to the shared folder): {}.",
                    paths.iter().take(5).cloned().collect::<Vec<_>>().join(", ")
                ));
            }
            item.examples = paths
                .into_iter()
                .take(5)
                .map(|resource| ActivityExample {
                    resource,
                    detail: Some(
                        "Path that could not be checked, relative to the shared folder".into(),
                    ),
                })
                .collect();
            item.href = "/peers".into();
            item.observed_at = at;
            items.push(item);
        }
    }

    let conflicts: Vec<Conflict> = sqlx::query_as(
        "WITH decisions AS (
             SELECT c.share_id,s.name AS share_name,c.peer_id,c.rel_path,
                    COUNT(*) OVER (PARTITION BY c.share_id,c.peer_id) AS count,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.share_id,c.peer_id ORDER BY c.created_at,c.id
                    ) AS position
             FROM conflicts c JOIN shares s ON s.id=c.share_id WHERE c.status='open'
                 AND (? IS NULL OR c.share_id IN (SELECT value FROM json_each(?)))
                 AND (? IS NULL OR c.peer_id=?)
         )
         SELECT share_id,share_name,peer_id,rel_path,count FROM decisions
         WHERE position<=5 ORDER BY share_id,peer_id,position",
    )
    .bind(&share_ids)
    .bind(&share_ids)
    .bind(peer_id)
    .bind(peer_id)
    .fetch_all(&state.db)
    .await?;
    for conflict in conflicts {
        let id = format!(
            "conflict-decisions:{}:{}",
            conflict.share_id, conflict.peer_id
        );
        if items.last().is_none_or(|item| item.id != id) {
            let mut item = template.clone();
            item.id = id;
            item.code = AttentionCode::RecoveryDecision;
            item.title = format!("Resolve conflicts: {}", conflict.share_name);
            item.message = "Review these conflicts and choose how to resolve them in Conflicts. These files cannot sync until their conflicts are resolved.".into();
            item.href = "/conflicts".into();
            item.count = conflict.count.max(0) as u64;
            item.total_count = Some(item.count);
            item.peer_id = Some(conflict.peer_id);
            item.share_id = Some(conflict.share_id);
            item.share_name = Some(conflict.share_name);
            items.push(item);
        }
        if let Some(item) = items.last_mut() {
            item.examples.push(ActivityExample {
                resource: conflict.rel_path,
                detail: None,
            });
        }
    }

    let roots: Vec<_> = crate::files::load_roots(&state.db)
        .await?
        .into_iter()
        .filter(|root| selection.is_none_or(|s| s.roots.contains(&root.mapping.root_id)))
        .collect();
    let available = crate::queue_evidence::root_availability(
        state,
        roots
            .iter()
            .map(|root| {
                (
                    root.mapping.root_id.clone(),
                    Some(root.mapping.container_path.clone()),
                )
            })
            .collect(),
    )
    .await;
    for root in roots {
        let availability = available.get(&root.mapping.root_id).copied().flatten();
        if availability == Some(true) {
            continue;
        }
        let mut item = template.clone();
        item.id = format!("mapping:{}", root.mapping.root_id);
        item.code = AttentionCode::MappingUnavailable;
        item.href = "/mappings".into();
        item.recheck_at = Some(db::ts(chrono::Utc::now() + chrono::Duration::seconds(30)));
        if availability == Some(false) {
            item.title = format!("Restore mapped folder: {}", root.name);
            item.message = "The mapped folder's marker is missing or invalid. Restore the folder and its matching marker in Mappings; ISM will not treat unavailable files as deleted.".into();
        } else {
            item.state = AttentionState::Unavailable;
            item.attention = AttentionLevel::None;
            item.blocks_work = false;
            item.unresolved = None;
            item.completeness = AttentionCompleteness::Unavailable;
            item.title = format!("Cannot check mapped folder: {}", root.name);
            item.message = "The folder check did not finish. ISM cannot confirm that this folder is available.".into();
            warnings.push(format!(
                "Folder availability for {} could not be verified.",
                root.name
            ));
        }
        items.push(item);
    }
    let watches: Vec<Watch> = sqlx::query_as(
        "SELECT m.root_id,m.name,w.last_pass_at FROM watch_status w
         JOIN mappings m ON m.root_id=w.root_id WHERE w.paused=1 AND
         (m.library_id IS NOT NULL OR EXISTS(SELECT 1 FROM shares s WHERE s.root_id=m.root_id))
         AND (? IS NULL OR m.root_id IN (SELECT value FROM json_each(?)))
         ORDER BY m.root_id",
    )
    .bind(&root_ids)
    .bind(&root_ids)
    .fetch_all(&state.db)
    .await?;
    for watch in watches {
        let mut item = template.clone();
        item.id = format!("watch:{}", watch.root_id);
        item.code = AttentionCode::WatcherPaused;
        item.title = format!("File detection is paused: {}", watch.name);
        item.message = "ISM stopped watching this folder because it became unavailable or unexpectedly empty. Check it in Mappings; reset its baseline only if the empty folder is intentional.".into();
        item.href = "/mappings".into();
        if let Some(at) = watch.last_pass_at {
            item.observed_at = at;
        }
        items.push(item);
    }
    let scans: Vec<Scan> = sqlx::query_as(
        "SELECT s.library_id,l.name AS library_name,s.due_at,s.last_finished_at,s.last_error,
                (SELECT COUNT(*) FROM scan_work w
                 WHERE w.library_id=s.library_id AND w.id>s.acked_through) AS pending
         FROM scan_schedule s LEFT JOIN immich_libraries l ON l.id=s.library_id
         WHERE s.running=0 AND s.last_error IS NOT NULL
             AND (? IS NULL OR s.library_id IN (SELECT value FROM json_each(?)))
         ORDER BY s.library_id",
    )
    .bind(&library_ids)
    .bind(&library_ids)
    .fetch_all(&state.db)
    .await?;
    for scan in scans {
        let mut item = template.clone();
        item.id = format!("library:{}", scan.library_id);
        item.code = AttentionCode::LibraryScanFailed;
        item.state = AttentionState::Failed;
        item.blocks_work = scan.pending > 0;
        item.count = scan.pending.max(0) as u64;
        item.total_count = Some(item.count);
        item.title = format!(
            "Library synchronization failed: {}",
            scan.library_name.unwrap_or(scan.library_id)
        );
        item.href = "/mappings".into();
        // No automatic retry is scheduled here: the kept requests run again only with the next
        // change requested for this library.
        let kept = if scan.pending > 0 {
            format!(
                "ISM kept {} synchronization request(s); no retry is scheduled, and they run again with the next change to this library.",
                scan.pending
            )
        } else {
            "No synchronization requests are waiting.".to_owned()
        };
        item.message = format!("{kept} {}", crate::error::redact(&scan.last_error));
        if let Some(at) = scan.last_finished_at {
            item.observed_at = at;
        }
        if let Some(at) = scan
            .due_at
            .and_then(chrono::DateTime::from_timestamp_millis)
        {
            item.state = AttentionState::Waiting;
            item.attention = AttentionLevel::Notice;
            item.message = "The last library synchronization failed; another attempt is scheduled. Review the library in Mappings if it fails again.".into();
            item.next_at = Some(db::ts(at));
            if at > chrono::Utc::now() {
                item.recheck_at = item.next_at.clone();
            }
        }
        items.push(item);
    }
    Ok((items, warnings))
}
