//! Durable, receiver-owned recovery. Admission never reports a queued repair as complete.

use std::time::Duration;

use ism_core::dto::{
    ConflictResolution, RecoveryBatchInput, RecoveryBatchResult, RecoveryDecision, RecoveryKind,
    RecoveryPage, RecoveryState, RecoveryTarget, RecoveryView,
};
use ism_core::paths::RelPath;
use ism_core::version::{Causality, VersionVector};
use serde::{Deserialize, Serialize};

use super::{Share, admin, net};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::{AppState, ServerEvent};

fn kind_name(kind: RecoveryKind) -> &'static str {
    match kind {
        RecoveryKind::Operation => "operation",
        RecoveryKind::Transfer => "transfer",
        RecoveryKind::Conflict => "conflict",
    }
}

fn parse_kind(kind: &str) -> AppResult<RecoveryKind> {
    match kind {
        "operation" => Ok(RecoveryKind::Operation),
        "transfer" => Ok(RecoveryKind::Transfer),
        "conflict" => Ok(RecoveryKind::Conflict),
        _ => Err(AppError::internal("invalid persisted recovery kind")),
    }
}

fn parse_state(state: &str) -> AppResult<RecoveryState> {
    match state {
        "queued" => Ok(RecoveryState::Queued),
        "fixing" => Ok(RecoveryState::Fixing),
        "waiting" => Ok(RecoveryState::Waiting),
        "needs_decision" => Ok(RecoveryState::NeedsDecision),
        "fixed" => Ok(RecoveryState::Fixed),
        "not_retryable" => Ok(RecoveryState::NotRetryable),
        _ => Err(AppError::internal("invalid persisted recovery state")),
    }
}

#[derive(Serialize, Deserialize)]
struct PlanAttempts {
    fingerprint: String,
    admissions: u32,
}

async fn admit_plan(
    state: &AppState,
    kind: &str,
    id: &str,
    evidence: &serde_json::Value,
) -> AppResult<()> {
    let fingerprint = crate::crypto::sha256_hex(&serde_json::to_vec(evidence)?);
    let records:Vec<Option<String>>=sqlx::query_scalar(
        "SELECT evidence_json FROM sync_recovery WHERE kind=?1 AND target_id=?2
         UNION ALL
         SELECT r.evidence_json FROM sync_recovery_links l JOIN sync_recovery r ON r.id=l.recovery_id
         WHERE l.kind=?1 AND l.target_id=?2",
    ).bind(kind).bind(id).fetch_all(&state.db).await?;
    let mut admissions = 0;
    for raw in records.into_iter().flatten() {
        let previous: PlanAttempts = serde_json::from_str(&raw)?;
        if previous.fingerprint == fingerprint {
            admissions = admissions.max(previous.admissions);
        }
    }
    if admissions >= 3 {
        return Err(AppError::conflict(
            "recovery_plan_unchanged",
            "The verified recovery plan has not changed; waiting for new evidence",
        ));
    }
    let evidence = serde_json::to_string(&PlanAttempts {
        fingerprint,
        admissions: admissions + 1,
    })?;
    let saved = sqlx::query(
        "UPDATE sync_recovery SET evidence_json=? WHERE kind=? AND target_id=? AND state='fixing'",
    )
    .bind(evidence)
    .bind(kind)
    .bind(id)
    .execute(&state.db)
    .await?;
    if saved.rows_affected() != 1 {
        return Err(AppError::conflict(
            "recovery_changed",
            "recovery admission changed",
        ));
    }
    Ok(())
}

async fn operation_plan_evidence(
    state: &AppState,
    actions: &[ism_core::dto::OperationAction],
    previews: &[ism_core::dto::PreviewResponse],
) -> AppResult<serde_json::Value> {
    let mut locations: Vec<&ism_core::dto::Location> = previews
        .iter()
        .flat_map(|preview| &preview.items)
        .flat_map(|item| &item.files)
        .flat_map(|file| file.from.iter().chain(file.to.iter()))
        .collect();
    locations.sort_by(|a, b| (&a.root_id, &a.path).cmp(&(&b.root_id, &b.path)));
    locations.dedup();
    let mut filesystem = Vec::new();
    for location in locations {
        let root = files::root(&state.db, &location.root_id).await?;
        root.ensure_available()?;
        let path = root.abs(&files::parse_rel(&location.path)?)?;
        let observed=tokio::task::spawn_blocking(move || -> AppResult<serde_json::Value> {
            let meta=match std::fs::symlink_metadata(&path) {
                Ok(meta)=>meta,
                Err(error) if error.kind()==std::io::ErrorKind::NotFound=>return Ok(serde_json::Value::Null),
                Err(error)=>return Err(error.into()),
            };
            let hash=if meta.is_file() {
                files::replacement_fingerprint(&path)?.map(|(_,hash)|hash)
                    .ok_or_else(||AppError::conflict("recovery_changed","file disappeared during verification"))?
            } else if meta.is_dir() {
                crate::crypto::sha256_hex(&serde_json::to_vec(&crate::ops::plan::folder_inventory(&path)?)?)
            } else {
                return Err(AppError::invalid_location("recovery file is not a regular file or directory"));
            };
            #[cfg(not(unix))]
            let permissions=meta.permissions().readonly().to_string();
            #[cfg(unix)]
            let permissions={
                use std::os::unix::fs::PermissionsExt;
                meta.permissions().mode().to_string()
            };
            Ok(serde_json::json!({"hash":hash,"revision":files::revision(&meta),"permissions":permissions}))
        }).await.map_err(AppError::internal)??;
        let server_configuration: Option<String> = sqlx::query_scalar(
            "SELECT json_object('url',s.url,'database',s.database_url_enc,'version',s.version,
                'accounts',(SELECT json_group_array(api_key_enc) FROM
                    (SELECT api_key_enc FROM immich_accounts WHERE server_id=s.id ORDER BY id)))
             FROM immich_servers s JOIN mappings m ON m.server_id=s.id WHERE m.root_id=?",
        )
        .bind(&location.root_id)
        .fetch_optional(&state.db)
        .await?;
        let server_revision =
            server_configuration.map(|value| crate::crypto::sha256_hex(value.as_bytes()));
        filesystem.push(serde_json::json!({"location":location,"observed":observed,"server_revision":server_revision}));
    }
    let mut previews = previews.to_vec();
    let location_key = |location: &Option<ism_core::dto::Location>| {
        location
            .as_ref()
            .map(|location| (location.root_id.clone(), location.path.clone()))
    };
    for item in previews.iter_mut().flat_map(|preview| &mut preview.items) {
        item.files
            .sort_by_key(|file| (location_key(&file.from), location_key(&file.to), file.copy));
        item.assets.sort_by(|a, b| a.asset_id.cmp(&b.asset_id));
        item.warnings
            .sort_by(|a, b| (&a.code, &a.message).cmp(&(&b.code, &b.message)));
        item.blockers
            .sort_by(|a, b| (&a.code, &a.message).cmp(&(&b.code, &b.message)));
    }
    Ok(
        serde_json::json!({"kind":"operation","actions":actions,"previews":previews,"filesystem":filesystem}),
    )
}

type ViewRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
);

fn view(row: ViewRow) -> AppResult<RecoveryView> {
    let (kind, id, state, code, detail, replacement_operation_id, title) = row;
    let state = parse_state(&state)?;
    Ok(RecoveryView {
        target: RecoveryTarget {
            kind: parse_kind(&kind)?,
            id,
            peer_id: None,
        },
        title,
        state,
        can_fix: matches!(state, RecoveryState::Queued | RecoveryState::Waiting)
            && code.as_deref() != Some("recovery_plan_unchanged"),
        // Exhaustion must be proven independently of the number of failed attempts.
        can_dismiss: state == RecoveryState::NotRetryable
            && parse_kind(&kind)? == RecoveryKind::Transfer,
        code,
        detail,
        replacement_operation_id,
        local_instance: None,
        peer_instance: None,
    })
}

pub async fn get(
    state: &AppState,
    kind: RecoveryKind,
    target_id: &str,
) -> AppResult<Option<RecoveryView>> {
    let row: Option<ViewRow> = sqlx::query_as(
        "SELECT kind,target_id,state,code,detail,replacement_operation_id,
            COALESCE((SELECT label FROM operations WHERE r.kind='operation' AND id=r.target_id),
                     (SELECT rel_path FROM transfers WHERE r.kind='transfer' AND id=r.target_id),
                     (SELECT rel_path FROM conflicts WHERE r.kind='conflict' AND id=r.target_id),target_id)
         FROM sync_recovery r WHERE kind = ? AND target_id = ?",
    )
    .bind(kind_name(kind))
    .bind(target_id)
    .fetch_optional(&state.db)
    .await?;
    match row.map(view).transpose()? {
        Some(item) => Ok(Some(decorate(state, item).await?)),
        None => Ok(None),
    }
}

fn mirrored_view(
    peer: String,
    json: &str,
    error: Option<String>,
    updated: Option<String>,
    stream_fresh: Option<bool>,
) -> AppResult<RecoveryView> {
    let mut item: RecoveryView = serde_json::from_str(json)?;
    item.target.peer_id = Some(peer);
    let fresh = stream_fresh.unwrap_or_else(|| {
        updated
            .as_deref()
            .and_then(db::parse_ts)
            .is_some_and(|at| chrono::Utc::now() - at <= chrono::Duration::seconds(30))
    });
    if let Some(error) =
        error.or_else(|| (!fresh).then(|| "No recent recovery state from the receiver.".into()))
    {
        item.state = RecoveryState::Waiting;
        item.can_fix = true;
        item.can_dismiss = false;
        item.code = Some("peer_recovery_unavailable".into());
        item.detail = Some(error);
    }
    Ok(item)
}

fn outgoing_view(
    peer: String,
    json: &str,
    error: Option<String>,
    updated: Option<String>,
    pending: bool,
    stream_fresh: Option<bool>,
) -> AppResult<RecoveryView> {
    if pending {
        let mut item: RecoveryView = serde_json::from_str(json)?;
        item.target.peer_id = Some(peer);
        Ok(item)
    } else {
        mirrored_view(peer, json, error, updated, stream_fresh)
    }
}

pub(super) async fn outgoing(
    state: &AppState,
    peer: &str,
    kind: RecoveryKind,
    id: &str,
) -> AppResult<Option<RecoveryView>> {
    let row: Option<(String, Option<String>, Option<String>, bool)> = sqlx::query_as(
        "SELECT q.view_json,s.error,q.updated_at,q.state!='accepted' FROM outgoing_recovery_requests q
         LEFT JOIN peer_recovery_state s ON s.peer_id=q.peer_id
         WHERE q.peer_id=?1 AND q.kind=?2 AND q.target_id=?3
         UNION ALL SELECT r.view_json,s.error,s.updated_at,0 FROM outgoing_recovery r
         LEFT JOIN peer_recovery_state s ON s.peer_id=r.peer_id
         WHERE r.peer_id=?1 AND r.kind=?2 AND r.target_id=?3
         AND NOT EXISTS(SELECT 1 FROM outgoing_recovery_requests q
             WHERE q.peer_id=r.peer_id AND q.kind=r.kind AND q.target_id=r.target_id)",
    )
    .bind(peer)
    .bind(kind_name(kind))
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    row.map(|(json, error, updated, pending)| {
        outgoing_view(
            peer.into(),
            &json,
            error,
            updated,
            pending,
            state.peer_streams.fresh(peer),
        )
    })
    .transpose()
}

async fn decorate(state: &AppState, mut item: RecoveryView) -> AppResult<RecoveryView> {
    item.local_instance = Some(state.instance_name().await);
    let peer:Option<String>=match item.target.kind {
        RecoveryKind::Operation=>sqlx::query_scalar::<_,String>("SELECT principal FROM operations WHERE id=?")
            .bind(&item.target.id).fetch_optional(&state.db).await?
            .and_then(|p|p.strip_prefix("peer:").map(str::to_owned)),
        RecoveryKind::Transfer=>sqlx::query_scalar(
            "SELECT peer_id FROM transfers WHERE id=?1 UNION ALL SELECT peer_id FROM transfer_history WHERE id=?1 LIMIT 1",
        ).bind(&item.target.id).fetch_optional(&state.db).await?,
        RecoveryKind::Conflict=>sqlx::query_scalar("SELECT peer_id FROM conflicts WHERE id=?")
            .bind(&item.target.id).fetch_optional(&state.db).await?,
    };
    if let Some(peer) = peer {
        item.peer_instance = sqlx::query_scalar("SELECT name FROM peers WHERE id=?")
            .bind(peer)
            .fetch_optional(&state.db)
            .await?;
    }
    Ok(item)
}

pub async fn discover(state: &AppState) -> AppResult<()> {
    // Reads run on every page load; only take the write lock when something is missing.
    let missing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operations o
             WHERE o.status IN ('failed','partial','recovery_required') AND (
                 o.principal LIKE 'peer:%' OR EXISTS (
                     SELECT 1 FROM operation_items i WHERE i.operation_id = o.id
                     AND i.error_code = 'peer_bookkeeping_failed'))
             AND NOT EXISTS (SELECT 1 FROM sync_recovery r
                 WHERE r.kind='operation' AND r.target_id=o.id))
         OR EXISTS(SELECT 1 FROM transfers t WHERE t.status='failed'
             AND NOT EXISTS (SELECT 1 FROM sync_recovery r
                 WHERE r.kind='transfer' AND r.target_id=t.id))
         OR EXISTS(SELECT 1 FROM conflicts c WHERE c.status='open'
             AND NOT EXISTS (SELECT 1 FROM sync_recovery r
                 WHERE r.kind='conflict' AND r.target_id=c.id))",
    )
    .fetch_one(&state.db)
    .await?;
    if !missing {
        return Ok(());
    }
    let now = db::now();
    let mut tx = state.db.begin().await?;
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery (id,kind,target_id,created_at,updated_at)
         SELECT 'operation:' || o.id,'operation',o.id,?,? FROM operations o
         WHERE o.status IN ('failed','partial','recovery_required') AND (
             o.principal LIKE 'peer:%' OR EXISTS (
                 SELECT 1 FROM operation_items i WHERE i.operation_id = o.id
                 AND i.error_code = 'peer_bookkeeping_failed'))",
    )
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery (id,kind,target_id,created_at,updated_at)
         SELECT 'transfer:' || id,'transfer',id,?,? FROM transfers WHERE status = 'failed'",
    )
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT OR IGNORE INTO sync_recovery (id,kind,target_id,state,code,created_at,updated_at)
         SELECT 'conflict:' || id,'conflict',id,'needs_decision','choose_version',?,?
         FROM conflicts WHERE status = 'open'",
    )
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

pub async fn page(state: &AppState, limit: i64, offset: i64) -> AppResult<RecoveryPage> {
    discover(state).await?;
    let mut tx = state.db.begin().await?;
    let local_total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sync_recovery WHERE state != 'fixed'")
            .fetch_one(&mut *tx)
            .await?;
    let rows: Vec<ViewRow> = sqlx::query_as(
        "SELECT kind,target_id,state,code,detail,replacement_operation_id,
            COALESCE((SELECT label FROM operations WHERE r.kind='operation' AND id=r.target_id),
                     (SELECT rel_path FROM transfers WHERE r.kind='transfer' AND id=r.target_id),
                     (SELECT rel_path FROM conflicts WHERE r.kind='conflict' AND id=r.target_id),target_id)
         FROM sync_recovery r
         WHERE state != 'fixed' ORDER BY created_at,id LIMIT ? OFFSET ?",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let mut items = Vec::new();
    for row in rows {
        items.push(decorate(state, view(row)?).await?);
    }
    type OutgoingRow = (String, String, Option<String>, Option<String>, bool);
    let outgoing: Vec<OutgoingRow> = sqlx::query_as(
        "SELECT q.peer_id,q.view_json,s.error,q.updated_at,q.state!='accepted' AS pending
         FROM outgoing_recovery_requests q LEFT JOIN peer_recovery_state s ON s.peer_id=q.peer_id
         UNION ALL SELECT r.peer_id,r.view_json,s.error,s.updated_at,0 FROM outgoing_recovery r
         LEFT JOIN peer_recovery_state s ON s.peer_id=r.peer_id
         WHERE NOT EXISTS(SELECT 1 FROM outgoing_recovery_requests q
             WHERE q.peer_id=r.peer_id AND q.kind=r.kind AND q.target_id=r.target_id)
         ORDER BY 1,2",
    )
    .fetch_all(&state.db)
    .await?;
    let total = local_total + i64::try_from(outgoing.len()).map_err(AppError::internal)?;
    let skip = usize::try_from((offset - local_total).max(0)).map_err(AppError::internal)?;
    let remaining = usize::try_from(limit)
        .map_err(AppError::internal)?
        .saturating_sub(items.len());
    for (peer, json, error, updated, pending) in outgoing.into_iter().skip(skip).take(remaining) {
        let fresh = state.peer_streams.fresh(&peer);
        items.push(outgoing_view(peer, &json, error, updated, pending, fresh)?);
    }
    Ok(RecoveryPage { total, rows: items })
}

pub(super) async fn peer_conflicts_page(
    state: &AppState,
    limit: i64,
    offset: i64,
) -> AppResult<RecoveryPage> {
    type OutgoingRow = (String, String, String, Option<String>, Option<String>, bool);
    let mut tx = state.db.begin().await?;
    let total: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM outgoing_recovery_requests WHERE kind='conflict') +
         (SELECT COUNT(*) FROM outgoing_recovery r WHERE r.kind='conflict'
          AND NOT EXISTS(SELECT 1 FROM outgoing_recovery_requests q
              WHERE q.peer_id=r.peer_id AND q.kind=r.kind AND q.target_id=r.target_id))",
    )
    .fetch_one(&mut *tx)
    .await?;
    let rows: Vec<OutgoingRow> = sqlx::query_as(
        "SELECT q.peer_id,q.target_id,q.view_json,s.error,q.updated_at,q.state!='accepted' AS pending
         FROM outgoing_recovery_requests q LEFT JOIN peer_recovery_state s ON s.peer_id=q.peer_id
         WHERE q.kind='conflict'
         UNION ALL SELECT r.peer_id,r.target_id,r.view_json,s.error,s.updated_at,0 FROM outgoing_recovery r
         LEFT JOIN peer_recovery_state s ON s.peer_id=r.peer_id
         WHERE r.kind='conflict' AND NOT EXISTS(SELECT 1 FROM outgoing_recovery_requests q
             WHERE q.peer_id=r.peer_id AND q.kind=r.kind AND q.target_id=r.target_id)
         ORDER BY 1,2 LIMIT ? OFFSET ?",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let rows = rows
        .into_iter()
        .map(|(peer, _, json, error, updated, pending)| {
            let fresh = state.peer_streams.fresh(&peer);
            outgoing_view(peer, &json, error, updated, pending, fresh)
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(RecoveryPage { rows, total })
}

async fn exists(state: &AppState, target: &RecoveryTarget) -> AppResult<bool> {
    let sql = match target.kind {
        RecoveryKind::Operation => "SELECT EXISTS(SELECT 1 FROM operations WHERE id = ?)",
        RecoveryKind::Transfer => {
            "SELECT EXISTS(SELECT 1 FROM transfers WHERE id = ? UNION ALL
             SELECT 1 FROM transfer_history WHERE id = ?1)"
        }
        RecoveryKind::Conflict => "SELECT EXISTS(SELECT 1 FROM conflicts WHERE id = ?)",
    };
    Ok(sqlx::query_scalar(sql)
        .bind(&target.id)
        .fetch_one(&state.db)
        .await?)
}

pub async fn peer_page(state: &AppState, peer: &net::Peer) -> AppResult<RecoveryPage> {
    discover(state).await?;
    let targets:Vec<(String,String)>=sqlx::query_as(
        "SELECT kind,target_id FROM sync_recovery r WHERE r.state!='fixed' AND (
            (r.kind='operation' AND EXISTS(SELECT 1 FROM operations o WHERE o.id=r.target_id AND o.principal=?1))
            OR (r.kind='transfer' AND (EXISTS(SELECT 1 FROM transfers t WHERE t.id=r.target_id AND t.peer_id=?2)
                OR EXISTS(SELECT 1 FROM transfer_history h WHERE h.id=r.target_id AND h.peer_id=?2)))
            OR (r.kind='conflict' AND EXISTS(SELECT 1 FROM conflicts c WHERE c.id=r.target_id AND c.peer_id=?2)))
         ORDER BY r.id LIMIT 5001",
    ).bind(format!("peer:{}",peer.id)).bind(&peer.id).fetch_all(&state.db).await?;
    if targets.len() > 5000 {
        return Err(AppError::conflict(
            "recovery_backlog_limit",
            "receiver recovery backlog exceeds 5000 issues",
        ));
    }
    let mut rows = Vec::new();
    for (kind, id) in targets {
        if let Some(item) = get(state, parse_kind(&kind)?, &id).await? {
            rows.push(item);
        }
    }
    Ok(RecoveryPage {
        total: i64::try_from(rows.len()).map_err(AppError::internal)?,
        rows,
    })
}

pub async fn refresh(state: &AppState, peer: &net::Peer) -> AppResult<()> {
    let page: RecoveryPage = net::get_json(state, peer, "/api/peer/v1/recovery")
        .await
        .map_err(|e| AppError::conflict("peer_recovery_unavailable", e))?;
    if page.rows.len() > 5000
        || usize::try_from(page.total).ok() != Some(page.rows.len())
        || page.rows.iter().any(|row| row.target.peer_id.is_some())
    {
        return Err(AppError::conflict(
            "invalid_peer_recovery",
            "receiver returned an invalid recovery snapshot",
        ));
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("DELETE FROM outgoing_recovery WHERE peer_id=?")
        .bind(&peer.id)
        .execute(&mut *tx)
        .await?;
    for row in page.rows {
        sqlx::query(
            "INSERT INTO outgoing_recovery (peer_id,kind,target_id,view_json) VALUES (?,?,?,?)",
        )
        .bind(&peer.id)
        .bind(kind_name(row.target.kind))
        .bind(&row.target.id)
        .bind(serde_json::to_string(&row)?)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM outgoing_recovery_requests WHERE peer_id=? AND state='accepted'")
        .bind(&peer.id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO peer_recovery_state (peer_id,updated_at,error) VALUES (?,?,NULL)
        ON CONFLICT(peer_id) DO UPDATE SET updated_at=excluded.updated_at,error=NULL",
    )
    .bind(&peer.id)
    .bind(db::now())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn unavailable(state: &AppState, peer: &str, error: &AppError) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO peer_recovery_state (peer_id,error) VALUES (?,?)
        ON CONFLICT(peer_id) DO UPDATE SET error=excluded.error",
    )
    .bind(peer)
    .bind(format!("{}: {}", error.code(), error.message()))
    .execute(&state.db)
    .await?;
    Ok(())
}

pub async fn submit(state: &AppState, input: RecoveryBatchInput) -> AppResult<RecoveryBatchResult> {
    if input.targets.len() > 5000 || input.all && !input.targets.is_empty() {
        return Err(AppError::bad_request(
            "invalid_recovery_batch",
            "choose up to 5000 explicit targets or all outstanding issues",
        ));
    }
    if input.resolution == Some(ConflictResolution::Skip) {
        return Err(AppError::conflict(
            "recovery_not_exhausted",
            "dismissal is not a recovery action",
        ));
    }
    discover(state).await?;
    let mut targets = input.targets;
    if input.all {
        let snapshot = page(state, 5001, 0).await?;
        if snapshot.total > 5000 {
            return Err(AppError::bad_request(
                "recovery_batch_limit",
                "select up to 5000 issues per batch",
            ));
        }
        targets = snapshot.rows.into_iter().map(|row| row.target).collect();
    }
    let mut items = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for target in targets {
        if !seen.insert((
            kind_name(target.kind),
            target.id.clone(),
            target.peer_id.clone(),
        )) {
            continue;
        }
        match submit_one(state, target.clone(), input.resolution, None).await {
            Ok(mut outcome) => items.append(&mut outcome),
            Err(error) => {
                tracing::warn!(target=%target.id, code=%error.code(), "recovery admission: {}",error.message());
                items.push(RecoveryView {
                    title: target.id.clone(),
                    target,
                    state: RecoveryState::Waiting,
                    code: Some(error.code().into()),
                    detail: Some(error.message().into()),
                    can_fix: true,
                    can_dismiss: false,
                    replacement_operation_id: None,
                    local_instance: None,
                    peer_instance: None,
                });
            }
        }
    }
    state.emit(ServerEvent::SyncChanged);
    state.sync_wake.notify_one();
    Ok(RecoveryBatchResult { items })
}

async fn submit_one(
    state: &AppState,
    target: RecoveryTarget,
    resolution: Option<ConflictResolution>,
    decision_revision: Option<&str>,
) -> AppResult<Vec<RecoveryView>> {
    let mut items = Vec::new();
    if let Some(peer_id) = &target.peer_id {
        let peer = net::peer(state, peer_id).await?;
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        let existing: Option<(String, Option<String>, String, Option<String>)> = sqlx::query_as(
            "SELECT view_json,resolution,state,decision_revision FROM outgoing_recovery_requests
             WHERE peer_id=? AND kind=? AND target_id=?",
        )
        .bind(peer_id)
        .bind(kind_name(target.kind))
        .bind(&target.id)
        .fetch_optional(&mut *tx)
        .await?;
        let resolution = resolution
            .map(|value| serde_json::to_string(&value))
            .transpose()?;
        if let Some((json, prior, status, prior_revision)) = existing {
            let prior_view: RecoveryView = serde_json::from_str(&json)?;
            if resolution.is_some()
                && (resolution != prior || decision_revision != prior_revision.as_deref())
                && (status == "forwarding"
                    || status == "accepted" && prior_view.state != RecoveryState::NeedsDecision)
            {
                return Err(AppError::conflict(
                    "recovery_decision_in_progress",
                    "the receiver is admitting the current version decision",
                ));
            }
            if resolution.is_none()
                || resolution == prior && decision_revision == prior_revision.as_deref()
            {
                items.push(serde_json::from_str(&json)?);
                tx.commit().await?;
                return Ok(items);
            }
        }
        let json: Option<String> = sqlx::query_scalar(
            "SELECT view_json FROM outgoing_recovery WHERE peer_id=? AND kind=? AND target_id=?",
        )
        .bind(peer_id)
        .bind(kind_name(target.kind))
        .bind(&target.id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(json) = json else {
            return Err(AppError::not_found(
                "the receiver recovery target is unavailable",
            ));
        };
        let mut item: RecoveryView = serde_json::from_str(&json)?;
        if matches!(
            item.state,
            RecoveryState::Fixed | RecoveryState::NotRetryable
        ) {
            item.target.peer_id = Some(peer_id.clone());
            tx.commit().await?;
            return Ok(vec![item]);
        }
        item.target = target.clone();
        item.state = RecoveryState::Queued;
        item.code = Some("awaiting_receiver".into());
        item.detail = None;
        item.can_fix = false;
        item.can_dismiss = false;
        item.local_instance = Some(peer.name);
        item.peer_instance = Some(state.instance_name().await);
        sqlx::query(
            "INSERT INTO outgoing_recovery_requests(peer_id,kind,target_id,resolution,decision_revision,view_json,updated_at)
             VALUES (?,?,?,?,?,?,?) ON CONFLICT(peer_id,kind,target_id)
             DO UPDATE SET resolution=excluded.resolution,state='queued',
                decision_revision=excluded.decision_revision,view_json=excluded.view_json,updated_at=excluded.updated_at",
        ).bind(peer_id).bind(kind_name(target.kind)).bind(&target.id).bind(resolution)
        .bind(decision_revision).bind(serde_json::to_string(&item)?).bind(db::now()).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(vec![item]);
    }
    if !exists(state, &target).await? {
        return Err(AppError::not_found("recovery target not found"));
    }
    let kind = kind_name(target.kind);
    let id = format!("{kind}:{}", target.id);
    let now = db::now();
    let resolution = resolution
        .map(|value| serde_json::to_string(&value))
        .transpose()?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let prior: Option<(Option<String>, String, bool)> = sqlx::query_as(
        "SELECT resolution,state,EXISTS(SELECT 1 FROM conflicts c
            WHERE sync_recovery.kind='conflict' AND c.id=sync_recovery.target_id AND c.status!='open')
         FROM sync_recovery WHERE kind=? AND target_id=?",
    ).bind(kind).bind(&target.id).fetch_optional(&mut *tx).await?;
    if let Some((previous, status, decision_started)) = prior
        && resolution.is_some()
        && previous.is_some()
        && resolution != previous
        && (decision_started || matches!(status.as_str(), "fixing" | "fixed"))
    {
        return Err(AppError::conflict(
            "recovery_decision_in_progress",
            "the current version decision is already executing",
        ));
    }
    sqlx::query(
            "INSERT INTO sync_recovery (id,kind,target_id,state,resolution,decision_revision,created_at,updated_at)
             VALUES (?,?,?,'queued',?,?,?,?)
             ON CONFLICT(kind,target_id) DO UPDATE
             SET state = CASE WHEN sync_recovery.state IN ('fixing','fixed','not_retryable') THEN sync_recovery.state
                              WHEN sync_recovery.state='needs_decision' AND excluded.resolution IS NULL
                              THEN 'needs_decision'
                              ELSE 'queued' END,
                 resolution = COALESCE(excluded.resolution,sync_recovery.resolution),
                 decision_revision=COALESCE(excluded.decision_revision,sync_recovery.decision_revision),
                 updated_at = excluded.updated_at",
        )
        .bind(&id)
        .bind(kind)
        .bind(&target.id)
        .bind(resolution)
        .bind(decision_revision)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if let Some(item) = get(state, target.kind, &target.id).await? {
        items.push(item);
    }
    Ok(items)
}

/// Admission only records intent. Network and filesystem verification belong to the worker.
async fn queue_local_decisions(
    state: &AppState,
    decisions: &[&RecoveryDecision],
    resolution: ConflictResolution,
) -> AppResult<Vec<RecoveryView>> {
    if decisions.is_empty() {
        return Ok(Vec::new());
    }
    let local_instance = state.instance_name().await;
    let resolution = serde_json::to_string(&resolution)?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    type AdmissionRow = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<AdmissionRow> = sqlx::query_as(
        "SELECT c.rel_path,c.status,p.name,r.resolution,r.state,r.decision_revision
         FROM json_each(?) d
         LEFT JOIN conflicts c ON c.id=json_extract(d.value,'$.target.id')
         LEFT JOIN peers p ON p.id=c.peer_id
         LEFT JOIN sync_recovery r ON r.kind='conflict' AND r.target_id=c.id
         ORDER BY CAST(d.key AS INTEGER)",
    )
    .bind(serde_json::to_string(decisions)?)
    .fetch_all(&mut *tx)
    .await?;
    let mut items = Vec::new();
    let mut accepted = Vec::new();
    let mut positions = std::collections::HashMap::new();
    for (decision, (title, status, peer_instance, prior, prior_state, prior_revision)) in
        decisions.iter().zip(rows)
    {
        let error = if status.is_none() {
            Some(("not_found", "recovery target not found"))
        } else if prior.is_some()
            && (prior.as_deref() != Some(&resolution)
                || prior_revision.as_deref() != Some(&decision.revision))
            && (status.as_deref() != Some("open")
                || matches!(prior_state.as_deref(), Some("fixing" | "fixed")))
        {
            Some((
                "recovery_decision_in_progress",
                "the current version decision is already executing",
            ))
        } else if status.as_deref() != Some("open") && prior.is_none() {
            Some(("not_open", "this conflict is already being resolved"))
        } else {
            None
        };
        let mut item = RecoveryView {
            target: decision.target.clone(),
            title: title.unwrap_or_else(|| decision.target.id.clone()),
            state: RecoveryState::Queued,
            code: None,
            detail: None,
            can_fix: false,
            can_dismiss: false,
            replacement_operation_id: None,
            local_instance: Some(local_instance.clone()),
            peer_instance,
        };
        if let Some((code, detail)) = error {
            tracing::warn!(target=%decision.target.id, code, "version decision deferred: {detail}");
            item.state = RecoveryState::Waiting;
            item.code = Some(code.into());
            item.detail = Some(detail.into());
        } else {
            positions.insert(decision.target.id.clone(), items.len());
            accepted.push(*decision);
        }
        items.push(item);
    }
    if !accepted.is_empty() {
        type QueuedRow = (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let queued: Vec<QueuedRow> = sqlx::query_as(
            "INSERT INTO sync_recovery
                (id,kind,target_id,state,resolution,decision_revision,created_at,updated_at)
             SELECT 'conflict:' || json_extract(d.value,'$.target.id'),'conflict',
                json_extract(d.value,'$.target.id'),'queued',?,
                json_extract(d.value,'$.revision'),?,? FROM json_each(?) d WHERE 1
             ON CONFLICT(kind,target_id) DO UPDATE SET
                state=CASE WHEN sync_recovery.state IN ('fixing','fixed','not_retryable')
                    THEN sync_recovery.state ELSE 'queued' END,
                resolution=excluded.resolution,decision_revision=excluded.decision_revision,
                code=CASE WHEN sync_recovery.state IN ('fixing','fixed','not_retryable')
                    THEN sync_recovery.code ELSE NULL END,
                detail=CASE WHEN sync_recovery.state IN ('fixing','fixed','not_retryable')
                    THEN sync_recovery.detail ELSE NULL END,
                updated_at=excluded.updated_at
             RETURNING target_id,state,code,detail,replacement_operation_id",
        )
        .bind(&resolution)
        .bind(db::now())
        .bind(db::now())
        .bind(serde_json::to_string(&accepted)?)
        .fetch_all(&mut *tx)
        .await?;
        for (id, status, code, detail, replacement) in queued {
            let position = positions.get(&id).ok_or_else(|| {
                AppError::internal("queued decision is outside the admitted batch")
            })?;
            let item = &mut items[*position];
            item.state = parse_state(&status)?;
            item.code = code;
            item.detail = detail;
            item.replacement_operation_id = replacement;
        }
    }
    tx.commit().await?;
    Ok(items)
}

pub async fn submit_decisions(
    state: &AppState,
    input: ism_core::dto::RecoveryDecisionInput,
) -> AppResult<RecoveryBatchResult> {
    if input.decisions.is_empty()
        || input.decisions.len() > 5000
        || input.resolution == ConflictResolution::Skip
    {
        return Err(AppError::bad_request(
            "invalid_recovery_decision",
            "choose up to 5000 explicit version decisions",
        ));
    }
    if input.decisions.iter().any(|decision| {
        decision.target.kind != RecoveryKind::Conflict
            || decision.revision.len() != 64
            || !decision
                .revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err(AppError::bad_request(
            "invalid_recovery_decision",
            "each conflict needs its current evidence revision",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let decisions: Vec<_> = input
        .decisions
        .into_iter()
        .filter(|decision| {
            seen.insert((decision.target.id.clone(), decision.target.peer_id.clone()))
        })
        .collect();
    let local: Vec<_> = decisions
        .iter()
        .filter(|decision| decision.target.peer_id.is_none())
        .collect();
    let mut queued: std::collections::HashMap<_, _> =
        queue_local_decisions(state, &local, input.resolution)
            .await?
            .into_iter()
            .map(|item| (item.target.id.clone(), item))
            .collect();
    let mut items = Vec::new();
    for decision in decisions {
        let target = decision.target;
        if target.peer_id.is_none() {
            items.push(queued.remove(&target.id).ok_or_else(|| {
                AppError::internal("local decision admission returned an incomplete batch")
            })?);
            continue;
        }
        let outcome = submit_one(
            state,
            target.clone(),
            Some(input.resolution),
            Some(&decision.revision),
        )
        .await;
        match outcome {
            Ok(mut result) => items.append(&mut result),
            Err(error) => {
                tracing::warn!(target=%target.id,code=%error.code(),"version decision deferred: {}",error.message());
                let item = if let Some(peer) = &target.peer_id {
                    outgoing(state, peer, target.kind, &target.id).await?
                } else {
                    get(state, target.kind, &target.id).await?
                };
                let mut item = item.unwrap_or(RecoveryView {
                    title: target.id.clone(),
                    target: target.clone(),
                    state: RecoveryState::Waiting,
                    code: None,
                    detail: None,
                    can_fix: false,
                    can_dismiss: false,
                    replacement_operation_id: None,
                    local_instance: None,
                    peer_instance: None,
                });
                item.state = if error.code() == "version_conflict" {
                    RecoveryState::NeedsDecision
                } else {
                    RecoveryState::Waiting
                };
                item.code = Some(error.code().into());
                item.detail = Some(error.message().into());
                item.can_dismiss = false;
                items.push(item);
            }
        }
    }
    state.sync_wake.notify_one();
    state.emit(ServerEvent::SyncChanged);
    Ok(RecoveryBatchResult { items })
}

/// Restricts receiver commands to this peer's own work, not arbitrary admin operations.
pub async fn authorize_peer(
    state: &AppState,
    peer: &net::Peer,
    input: &RecoveryBatchInput,
) -> AppResult<()> {
    if input.all || input.resolution == Some(ConflictResolution::Skip) {
        return Err(AppError::forbidden(
            "peer recovery only admits explicit repairs",
        ));
    }
    for target in &input.targets {
        if target.peer_id.is_some() {
            return Err(AppError::forbidden(
                "recovery forwarding loops are forbidden",
            ));
        }
        let allowed: bool = match target.kind {
            RecoveryKind::Operation => {
                sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM operations WHERE id = ? AND principal = ?)",
                )
                .bind(&target.id)
                .bind(format!("peer:{}", peer.id))
                .fetch_one(&state.db)
                .await?
            }
            RecoveryKind::Transfer => {
                sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM transfers WHERE id = ?1 AND peer_id = ?2
                 UNION ALL SELECT 1 FROM transfer_history WHERE id = ?1 AND peer_id = ?2)",
                )
                .bind(&target.id)
                .bind(&peer.id)
                .fetch_one(&state.db)
                .await?
            }
            RecoveryKind::Conflict => {
                sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM conflicts WHERE id=? AND peer_id=?)",
                )
                .bind(&target.id)
                .bind(&peer.id)
                .fetch_one(&state.db)
                .await?
            }
        };
        if !allowed {
            return Err(AppError::forbidden(
                "this recovery belongs to another principal",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEvidence {
    pub path: String,
    pub sha256: Option<String>,
    pub size: Option<u64>,
    pub vv: VersionVector,
    pub deleted: bool,
    #[serde(default)]
    pub last_move_seq: Option<i64>,
    #[serde(default)]
    pub inventory: Option<Vec<crate::ops::plan::TrashEntry>>,
    /// RFC 3339 modification time of the file; older peers omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<String>,
}

/// Follows committed move lineage only. Content similarity alone is not a file identity.
pub async fn current_path(
    state: &AppState,
    share: &Share,
    rel: &str,
    version: &VersionVector,
) -> AppResult<String> {
    Ok(path_after_event(state, share, rel, version, None).await?.0)
}

async fn path_after_event(
    state: &AppState,
    share: &Share,
    rel: &str,
    version: &VersionVector,
    event: Option<(&str, i64)>,
) -> AppResult<(String, Option<i64>)> {
    RelPath::parse(rel).map_err(|e| AppError::invalid_location(e.to_string()))?;
    const LINEAGE_PAGE: usize = 128;
    let mut tx = state.db.begin().await?;
    let watermark: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq),0) FROM event_log")
        .fetch_one(&mut *tx)
        .await?;
    let invalid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM event_log WHERE share_name=?
         AND kind IN ('file_changed','file_moved','folder_created','folder_moved','folder_pruned')
         AND CASE WHEN json_valid(data_json) THEN
             json_type(data_json) IS NOT 'object'
             OR json_type(data_json,'$.path') IS NOT 'text'
             OR (json_type(data_json,'$.from') IS NOT NULL
                 AND json_type(data_json,'$.from') NOT IN ('text','null'))
             OR (json_type(data_json,'$.vv') IS NOT NULL
                 AND json_type(data_json,'$.vv') IS NOT 'object')
             ELSE 1 END
         AND seq<=?)",
    )
    .bind(&share.name)
    .bind(watermark)
    .fetch_one(&mut *tx)
    .await?;
    if invalid {
        return Err(AppError::conflict(
            "recovery_evidence_missing",
            "the share contains an invalid committed event",
        ));
    }
    let mut path = rel.to_owned();
    let mut lineage = version.clone();
    let mut anchor = None;
    if let Some((origin, seq)) = event {
        anchor=sqlx::query_scalar("SELECT seq FROM event_log WHERE share_name=? AND origin=? AND origin_seq=? AND json_extract(data_json,'$.path')=? AND seq<=?")
            .bind(&share.name).bind(origin).bind(seq).bind(rel).bind(watermark).fetch_optional(&mut *tx).await?;
        if anchor.is_none() {
            return Err(AppError::conflict(
                "recovery_evidence_missing",
                "committed event identity is unavailable",
            ));
        }
    }
    if event.is_none() && *version != VersionVector::default() {
        let mut before = watermark;
        loop {
            let candidates: Vec<(i64, String)> = sqlx::query_as(
                "SELECT seq,data_json FROM event_log WHERE share_name=?
                 AND kind IN ('file_changed','file_moved') AND json_valid(data_json)
                 AND json_extract(data_json,'$.path')=? AND seq<=?
                 ORDER BY seq DESC LIMIT ?",
            )
            .bind(&share.name)
            .bind(rel)
            .bind(before)
            .bind(LINEAGE_PAGE as i64)
            .fetch_all(&mut *tx)
            .await?;
            for (seq, data) in &candidates {
                let candidate: super::EventData = serde_json::from_str(data)?;
                if candidate.vv == *version {
                    anchor = Some(*seq);
                    break;
                }
            }
            if anchor.is_some() || candidates.len() < LINEAGE_PAGE {
                break;
            }
            let Some((last, _)) = candidates.last() else {
                break;
            };
            if *last == i64::MIN {
                break;
            }
            before = last - 1;
            tokio::task::yield_now().await;
        }
    }
    reject_pending_prunes(&mut tx, &share.name, &path).await?;
    let mut last_move_seq = anchor;
    let mut after = anchor.unwrap_or(0);
    let mut examined: usize = 0;
    loop {
        let mut ancestors = vec![path.clone()];
        ancestors.extend(
            path.match_indices('/')
                .map(|(index, _)| path[..index].to_owned()),
        );
        let next: Option<(i64, String, String)> = sqlx::query_as(
            "SELECT seq,kind,data_json FROM event_log
             WHERE share_name=?1 AND kind IN ('file_moved','folder_moved') AND json_valid(data_json)
               AND seq>?2 AND seq<=?6 AND kind='file_moved' AND json_extract(data_json,'$.from')=?3
             UNION ALL
             SELECT seq,kind,data_json FROM event_log
             WHERE share_name=?1 AND kind IN ('file_moved','folder_moved') AND json_valid(data_json)
               AND seq>?2 AND seq<=?6 AND ?5 AND kind='folder_moved' AND json_extract(data_json,'$.from')
                   IN (SELECT value FROM json_each(?4))
             UNION ALL
             SELECT seq,kind,data_json FROM event_log
             WHERE share_name=?1 AND kind='folder_pruned' AND seq>?2 AND seq<=?6
             ORDER BY seq LIMIT 1",
        )
        .bind(&share.name)
        .bind(after)
        .bind(&path)
        .bind(serde_json::to_string(&ancestors)?)
        .bind(anchor.is_some())
        .bind(watermark)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((seq, kind, data)) = next else {
            break;
        };
        examined += 1;
        if examined.is_multiple_of(LINEAGE_PAGE) {
            tokio::task::yield_now().await;
        }
        after = seq;
        let event: super::EventData = serde_json::from_str(&data)?;
        if kind == super::FOLDER_PRUNED {
            if super::inbox::overlaps(&path, &event.path) {
                return Err(AppError::conflict(
                    "recovery_evidence_incomplete",
                    "folder prune prevents verifying this recovery lineage",
                ));
            }
            continue;
        }
        // Folder events have no file version vector. Their committed ordering after the
        // matching content announcement supplies the lineage; an unanchored guess is refused.
        if !(kind == "folder_moved" && anchor.is_some()
            || matches!(event.vv.compare(&lineage), Causality::Dominates))
        {
            continue;
        }
        let Some(from) = &event.from else { continue };
        let suffix = if path == *from {
            Some("")
        } else if kind == "folder_moved" {
            path.strip_prefix(&format!("{from}/"))
        } else {
            None
        };
        if let Some(suffix) = suffix {
            let candidate = if suffix.is_empty() {
                event.path.clone()
            } else {
                format!("{}/{suffix}", event.path)
            };
            RelPath::parse(&candidate).map_err(|e| AppError::invalid_location(e.to_string()))?;
            path = candidate;
            reject_pending_prunes(&mut tx, &share.name, &path).await?;
            lineage.merge(&event.vv);
            last_move_seq = Some(seq);
        }
    }
    tx.commit().await?;
    Ok((path, last_move_seq))
}

async fn reject_pending_prunes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    share: &str,
    path: &str,
) -> AppResult<()> {
    let mut cursor: Option<(String, i64)> = None;
    loop {
        let rows: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT peer_id,seq,event_json FROM peer_inbox
             WHERE share_name=? AND status != 'done' AND json_extract(event_json,'$.kind')=?
               AND (? IS NULL OR peer_id>? OR (peer_id=? AND seq>?))
             ORDER BY peer_id,seq LIMIT 128",
        )
        .bind(share)
        .bind(super::FOLDER_PRUNED)
        .bind(cursor.as_ref().map(|(peer, _)| peer))
        .bind(cursor.as_ref().map(|(peer, _)| peer))
        .bind(cursor.as_ref().map(|(peer, _)| peer))
        .bind(cursor.as_ref().map(|(_, seq)| seq))
        .fetch_all(&mut **tx)
        .await?;
        for (_, _, raw) in &rows {
            let event: super::PeerEvent = serde_json::from_str(raw)?;
            if super::inbox::overlaps(path, &event.data.path) {
                return Err(AppError::conflict(
                    "recovery_evidence_incomplete",
                    "folder prune prevents verifying this recovery lineage",
                ));
            }
        }
        if rows.len() < 128 {
            return Ok(());
        }
        cursor = rows.last().map(|(peer, seq, _)| (peer.clone(), *seq));
        tokio::task::yield_now().await;
    }
}

pub async fn evidence(
    state: &AppState,
    peer_id: Option<&str>,
    share_name: &str,
    rel: &str,
    version: &VersionVector,
) -> AppResult<FileEvidence> {
    evidence_at_event(state, peer_id, share_name, rel, version, None).await
}

pub async fn evidence_at_event(
    state: &AppState,
    peer_id: Option<&str>,
    share_name: &str,
    rel: &str,
    version: &VersionVector,
    event: Option<(&str, i64)>,
) -> AppResult<FileEvidence> {
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|s| s.name == share_name && peer_id.is_none_or(|peer| s.includes(peer)))
        .ok_or_else(|| AppError::not_found("shared recovery location not found"))?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let (path, last_move_seq) = path_after_event(state, &share, rel, version, event).await?;
    let abs = root.abs(&share.root_rel(&path)?)?;
    let permit = state
        .version_scan_limit
        .clone()
        .acquire_owned()
        .await
        .map_err(AppError::internal)?;
    match std::fs::symlink_metadata(&abs) {
        Ok(metadata) if metadata.is_dir() => {
            let folder = abs.clone();
            let inventory = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let inventory = crate::ops::plan::folder_inventory_bounded(&folder, 10_000)
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::InvalidData {
                            AppError::conflict(
                                "version_evidence_too_large",
                                "folder exceeds the 10,000-entry version evidence limit",
                            )
                        } else {
                            error.into()
                        }
                    })?;
                if files::revision(&std::fs::symlink_metadata(&folder)?)
                    != files::revision(&metadata)
                {
                    return Err(AppError::conflict(
                        "revision_changed",
                        "folder changed during verification",
                    ));
                }
                Ok(inventory)
            })
            .await
            .map_err(AppError::internal)??;
            return Ok(FileEvidence {
                path,
                sha256: None,
                size: None,
                vv: version.clone(),
                deleted: false,
                last_move_seq,
                inventory: Some(inventory),
                modified_at: None,
            });
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let fingerprint_path = abs.clone();
    let fingerprint = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        files::replacement_fingerprint(&fingerprint_path)
    })
    .await
    .map_err(AppError::internal)??;
    let row = super::row(state, &share.id, &path).await?;
    if let Some((_, hash)) = &fingerprint
        && row
            .as_ref()
            .is_some_and(|row| row.deleted || row.sha256.as_deref() != Some(hash.as_str()))
    {
        return Err(AppError::conflict(
            "revision_changed",
            "file content has not yet been committed by the share scan",
        ));
    }
    let (size, modified_at) = if let Some((revision, _)) = &fingerprint {
        let metadata = std::fs::metadata(&abs)?;
        if files::revision(&metadata) != *revision {
            return Err(AppError::conflict(
                "revision_changed",
                "recovery evidence changed while being verified",
            ));
        }
        (
            Some(metadata.len()),
            Some(chrono::DateTime::from_timestamp_nanos(files::mtime_ns(&metadata)).to_rfc3339()),
        )
    } else {
        (None, None)
    };
    Ok(FileEvidence {
        path,
        sha256: fingerprint.map(|(_, sha)| sha),
        size,
        vv: row.as_ref().map(|r| r.vv.clone()).unwrap_or_default(),
        deleted: row.is_some_and(|r| r.deleted),
        last_move_seq,
        inventory: None,
        modified_at,
    })
}

/// Peers usually sit behind a proxy that gives up after about a minute; answer well before it.
const EVIDENCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(40);

pub async fn remote_evidence(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    rel: &str,
    vv: &VersionVector,
) -> AppResult<FileEvidence> {
    remote_event_evidence(state, peer, share, rel, vv, None).await
}

async fn remote_event_evidence(
    state: &AppState,
    peer: &net::Peer,
    share: &Share,
    rel: &str,
    vv: &VersionVector,
    event: Option<(&str, i64)>,
) -> AppResult<FileEvidence> {
    let vv = serde_json::to_string(vv)?;
    let mut params = vec![("share", share.name.as_str()), ("path", rel), ("vv", &vv)];
    let sequence = event.map(|(_, seq)| seq.to_string());
    if let Some((origin, _)) = event {
        params.push(("origin", origin));
        if let Some(seq) = &sequence {
            params.push(("origin_seq", seq));
        }
    }
    let query = reqwest::Url::parse_with_params("http://ism.example", params)
        .map_err(AppError::internal)?;
    let path = format!(
        "/api/peer/v1/recovery/file?{}",
        query.query().unwrap_or_default()
    );
    let evidence: FileEvidence = net::get_json_within(state, peer, &path, Some(EVIDENCE_TIMEOUT))
        .await
        .map_err(|e| AppError::conflict("peer_evidence_unavailable", e))?;
    RelPath::parse(&evidence.path).map_err(|e| AppError::invalid_location(e.to_string()))?;
    if evidence.size.is_some() != evidence.sha256.is_some() {
        return Err(AppError::conflict(
            "invalid_peer_evidence",
            "incomplete file evidence",
        ));
    }
    Ok(evidence)
}

async fn repair_transfer(state: &AppState, id: &str) -> AppResult<bool> {
    type TransferRow = (String, String, String, String, i64, String, String);
    let row: Option<TransferRow> = sqlx::query_as(
        "SELECT peer_id,share_id,rel_path,sha256,size,vv_json,status FROM transfers WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    let Some((peer_id, share_id, rel, hash, size, vv, status)) = row else {
        let operation: Option<Option<String>> =
            sqlx::query_scalar("SELECT operation_id FROM transfer_history WHERE id = ?")
                .bind(id)
                .fetch_optional(&state.db)
                .await?;
        return match operation {
            Some(Some(id)) => operation_completed(state, &id).await,
            Some(None) => Ok(true),
            None => Err(AppError::not_found(
                "transfer evidence is no longer available",
            )),
        };
    };
    if matches!(status.as_str(), "queued" | "running") {
        return Ok(false);
    }
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|s| s.id == share_id && s.includes(&peer_id))
        .ok_or_else(|| AppError::conflict("peer_share_changed", "share is unavailable"))?;
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let staged = root.base().join(files::STAGING_DIR).join("peer").join(id);
    let local = root.abs(&share.root_rel(&rel)?)?;
    let peer = net::peer(state, &peer_id).await?;
    let version: VersionVector = serde_json::from_str(&vv)?;
    let evidence = remote_evidence(state, &peer, &share, &rel, &version).await?;
    if evidence.deleted
        && evidence.sha256.is_none()
        && evidence.inventory.is_none()
        && evidence.vv.compare(&version) == Causality::Dominates
        && recover_absent_deletion(state, id, &share, &rel, &vv, &evidence).await?
    {
        return Ok(true);
    }
    for candidate in [&staged, &local] {
        if evidence.path == rel
            && (evidence.sha256.as_deref() == Some(hash.as_str())
                || candidate == &local && evidence.deleted)
            && files::replacement_fingerprint(candidate)?.is_some_and(|(_, sha)| sha == hash)
            && i64::try_from(std::fs::symlink_metadata(candidate)?.len()).ok() == Some(size)
        {
            admit_plan(
                state,
                "transfer",
                id,
                &serde_json::json!({
                    "kind":"retained_copy","path":rel,"sha256":hash,"size":size,"vv":vv,
                    "staged":candidate==&staged,
                }),
            )
            .await?;
            sqlx::query("UPDATE transfers SET status='queued',error=NULL,attempts=0,updated_at=? WHERE id=? AND status='failed' AND vv_json=?")
                .bind(db::now()).bind(id).bind(&vv).execute(&state.db).await?;
            state.sync_wake.notify_one();
            return Ok(false);
        }
    }
    if let (Some(remote_hash), Some(size)) = (&evidence.sha256, evidence.size) {
        if evidence.vv.compare(&version) == Causality::Concurrent
            && evidence.path == rel
            && files::replacement_fingerprint(&local)?.is_some()
        {
            let _guard = state.replication_operation_lock.lock().await;
            super::register_local(state, &share, &root, &rel).await?;
            super::conflict(
                state,
                super::ConflictInput {
                    share: &share,
                    rel: &rel,
                    kind: "changed_on_both",
                    detail: "Choose a version".into(),
                    remote: ism_core::dto::ConflictSide {
                        instance: peer.name.clone(),
                        sha256: Some(remote_hash.clone()),
                        size: Some(size),
                        modified_at: None,
                        deleted: false,
                    },
                    remote_vv: &evidence.vv,
                    peer_id: &peer_id,
                    from: None,
                },
            )
            .await?;
            discover(state).await?;
            sqlx::query("INSERT OR IGNORE INTO sync_recovery_links (recovery_id,kind,target_id)
                SELECT ?,'conflict',id FROM conflicts WHERE share_id=? AND rel_path=? AND status='open'")
                .bind(format!("transfer:{id}")).bind(&share.id).bind(&rel).execute(&state.db).await?;
            return Err(AppError::conflict(
                "choose_version",
                "Choose a version inside ISM",
            ));
        }
        if !matches!(
            evidence.vv.compare(&version),
            Causality::Equal | Causality::Dominates
        ) || evidence.vv.compare(&version) == Causality::Equal && *remote_hash != hash
        {
            return Err(AppError::conflict(
                "peer_content_changed",
                "current peer content needs a version decision",
            ));
        }
        admit_plan(
            state,
            "transfer",
            id,
            &serde_json::json!({
                "kind":"peer_copy","share":share.id,"peer":peer_id,"evidence":evidence,
            }),
        )
        .await?;
        let _guard = state.replication_operation_lock.lock().await;
        if *remote_hash != hash && std::fs::symlink_metadata(&staged).is_ok() {
            let retained =
                staged.with_file_name(format!("{id}.{hash}.{}", crate::crypto::new_id("retained")));
            std::fs::rename(&staged, retained)?;
        }
        if evidence.path != rel {
            let root = files::root(&state.db, &share.root_id).await?;
            root.ensure_available()?;
            let old = root.abs(&share.root_rel(&rel)?)?;
            let destination = root.abs(&share.root_rel(&evidence.path)?)?;
            if let Some((revision, local_hash)) = files::replacement_fingerprint(&old)?
                && local_hash == *remote_hash
                && files::replacement_fingerprint(&destination)?.is_none()
            {
                let request = ism_core::dto::OperationRequest {
                    idempotency_key: format!("recovery-move:{id}:{}", evidence.path),
                    label: Some(format!("Recover move of {}", evidence.path)),
                    actions: vec![ism_core::dto::OperationAction::Move {
                        source: share.location(&rel)?,
                        target: share.location(&evidence.path)?,
                        expected_revision: Some(revision),
                    }],
                    allow_owner_transfer: false,
                    allow_partial: false,
                };
                let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
                let unchanged: Option<String> = sqlx::query_scalar(
                    "SELECT vv_json FROM transfers WHERE id=? AND status='failed'",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
                if unchanged.as_ref() != Some(&vv) {
                    return Err(AppError::conflict(
                        "recovery_changed",
                        "transfer changed during repair",
                    ));
                }
                let accepted = crate::ops::insert_in(
                    &mut tx,
                    &crate::auth::Principal::Peer {
                        peer_id: peer_id.clone(),
                    },
                    &request,
                    None,
                )
                .await?;
                sqlx::query(
                    "INSERT INTO pending_peer_ops (operation_id,idx,share_id,kind,from_path,to_path,vv_json,peer_id)
                     VALUES (?,0,?,'move',?,?,?,?)",
                )
                .bind(&accepted.operation_id).bind(&share.id).bind(&rel).bind(&evidence.path)
                .bind(serde_json::to_string(&evidence.vv)?).bind(&peer_id)
                .execute(&mut *tx).await?;
                super::transfer::archive_transfer(&mut tx, id, Some(&accepted.operation_id))
                    .await?;
                sqlx::query("DELETE FROM transfers WHERE id=?")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                crate::ops::announce_queued(state, &accepted);
                return Ok(false);
            }
        }
        let changed = sqlx::query(
            "UPDATE transfers SET rel_path=?,sha256=?,size=?,vv_json=?,status='queued',
                     attempts=0,error=NULL,bytes_done=0,source_retained_token=NULL,
                     source_rel_path=NULL,source_vv_json=NULL,updated_at=?
             WHERE id=? AND status='failed' AND vv_json=?",
        )
        .bind(&evidence.path)
        .bind(remote_hash)
        .bind(i64::try_from(size).map_err(AppError::internal)?)
        .bind(serde_json::to_string(&evidence.vv)?)
        .bind(db::now())
        .bind(id)
        .bind(&vv)
        .execute(&state.db)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "recovery_changed",
                "transfer changed during repair",
            ));
        }
        state.sync_wake.notify_one();
        return Ok(false);
    }
    let current_local = self::evidence(state, None, &share.name, &rel, &version).await?;
    if current_local.sha256.is_some()
        && (current_local.sha256.as_deref() == Some(&hash)
            || current_local.vv.compare(&version) == Causality::Dominates)
    {
        sqlx::query("UPDATE transfers SET rel_path=?,status='queued',error=NULL,attempts=0,updated_at=? WHERE id=? AND status='failed' AND vv_json=?")
            .bind(&current_local.path).bind(db::now()).bind(id).bind(&vv).execute(&state.db).await?;
        state.sync_wake.notify_one();
        return Ok(false);
    }
    if current_local.sha256.is_some() {
        return Err(AppError::conflict(
            "recovery_evidence_changed",
            "a different valid local copy is retained",
        ));
    }
    let wanted = super::retained::Wanted {
        share: share.name.clone(),
        path: rel.clone(),
        sha256: hash.clone(),
        size: u64::try_from(size).map_err(AppError::internal)?,
    };
    let retained = {
        let _guard = state.replication_operation_lock.lock().await;
        super::retained::discover(state, None, &wanted).await?
    };
    let mut complete = retained.complete && evidence.inventory.is_none();
    for source in retained.sources {
        match queue_retained_transfer(state, id, &vv, None, &source).await {
            Ok(_) => return Ok(false),
            Err(error) => {
                tracing::warn!(transfer=id,code=%error.code(),"retained local copy deferred: {}",error.message());
                complete = false;
            }
        }
    }
    for other in net::peers(state)
        .await?
        .into_iter()
        .filter(|peer| share.includes(&peer.id))
    {
        if other.id != peer_id {
            match remote_evidence(state, &other, &share, &rel, &version).await {
                Ok(copy)
                    if copy.sha256.as_deref() == Some(hash.as_str())
                        && copy.size == Some(wanted.size) =>
                {
                    let _guard = state.replication_operation_lock.lock().await;
                    admit_plan(
                        state,
                        "transfer",
                        id,
                        &serde_json::json!({
                            "kind":"alternate_peer_copy","peer":other.id,"path":copy.path,
                            "sha256":hash,"size":size,"vv":vv,
                        }),
                    )
                    .await?;
                    let changed=sqlx::query(
                        "UPDATE transfers SET peer_id=?,rel_path=?,status='queued',error=NULL,attempts=0,
                         source_retained_token=NULL,source_rel_path=NULL,source_vv_json=NULL,updated_at=?
                         WHERE id=? AND status='failed' AND vv_json=?",
                    ).bind(&other.id).bind(&copy.path).bind(db::now()).bind(id).bind(&vv)
                    .execute(&state.db).await?;
                    if changed.rows_affected() != 1 {
                        return Err(AppError::conflict(
                            "recovery_changed",
                            "transfer changed during repair",
                        ));
                    }
                    state.sync_wake.notify_one();
                    return Ok(false);
                }
                Ok(copy) => complete &= copy.sha256.is_none() && copy.inventory.is_none(),
                Err(error) => {
                    tracing::warn!(transfer=id,peer=%other.id,code=%error.code(),"alternate live copy unverified: {}",error.message());
                    complete = false;
                }
            }
        }
        match super::retained::remote_discover(state, &other, &wanted).await {
            Ok(discovery) => {
                complete &= discovery.complete;
                for source in discovery.sources {
                    match queue_retained_transfer(state, id, &vv, Some(&other), &source).await {
                        Ok(_) => return Ok(false),
                        Err(error) => {
                            tracing::warn!(transfer=id,peer=%other.id,code=%error.code(),"retained peer copy deferred: {}",error.message());
                            complete = false;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(transfer=id,peer=%other.id,code=%error.code(),"retained peer inventory unverified: {}",error.message());
                complete = false;
            }
        }
    }
    if complete {
        Err(AppError::conflict(
            "recovery_exhausted",
            "No complete copy remains on the available share peers, in staging, uploads, or applicable ISM trash",
        ))
    } else {
        Err(AppError::conflict(
            "recovery_evidence_missing",
            "Retained content is not yet fully verified; recovery will wait",
        ))
    }
}

async fn queue_retained_transfer(
    state: &AppState,
    id: &str,
    version: &str,
    peer: Option<&net::Peer>,
    source: &super::retained::Source,
) -> AppResult<bool> {
    let _guard = state.replication_operation_lock.lock().await;
    let saved: Option<String> =
        sqlx::query_scalar("SELECT vv_json FROM transfers WHERE id=? AND status='failed'")
            .bind(id)
            .fetch_optional(&state.db)
            .await?;
    if saved.as_deref() != Some(version) {
        return Err(AppError::conflict(
            "recovery_changed",
            "transfer changed during retained-copy verification",
        ));
    }
    admit_plan(
        state,
        "transfer",
        id,
        &serde_json::json!({
            "kind":"verified_retained_copy","wanted":source.wanted,
            "peer":peer.map(|peer|&peer.id),"vv":version,
        }),
    )
    .await?;
    match peer {
        Some(peer) => super::retained::queue_remote(state, id, peer, source).await,
        None => super::retained::stage_local(state, id, source).await,
    }
}

async fn recover_absent_deletion(
    state: &AppState,
    id: &str,
    share: &Share,
    original: &str,
    saved_version: &str,
    deleted: &FileEvidence,
) -> AppResult<bool> {
    let root = files::root(&state.db, &share.root_id).await?;
    let paths = std::collections::BTreeSet::from([original, deleted.path.as_str()]);
    let mut snapshots = Vec::new();
    for path in paths {
        super::inbox::require_stable_transfer(state, share, &[path], id).await?;
        let rel = share.root_rel(path)?;
        if files::replacement_fingerprint(&root.abs(&rel)?)?.is_some() {
            return Ok(false);
        }
        let snapshot = super::row(state, &share.id, path).await?;
        if snapshot.as_ref().is_some_and(|row| {
            !matches!(
                deleted.vv.compare(&row.vv),
                Causality::Equal | Causality::Dominates
            )
        }) {
            return Err(AppError::conflict(
                "recovery_evidence_changed",
                "a different local version must be recovered before applying the deletion",
            ));
        }
        crate::ops::exec::verified_file_absence_at_root(state, &root, &rel).await?;
        snapshots.push((path.to_owned(), snapshot));
    }
    let _guard = state.replication_operation_lock.lock().await;
    root.ensure_available()?;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let unchanged: Option<String> =
        sqlx::query_scalar("SELECT vv_json FROM transfers WHERE id=? AND status='failed'")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    if unchanged.as_deref() != Some(saved_version) {
        return Err(AppError::conflict(
            "recovery_changed",
            "transfer changed during deletion verification",
        ));
    }
    for (path, snapshot) in snapshots {
        super::inbox::require_stable_transfer(state, share, &[&path], id).await?;
        let current = super::row_in(&mut tx, &share.id, &path).await?;
        if current != snapshot
            || files::replacement_fingerprint(&root.abs(&share.root_rel(&path)?)?)?.is_some()
        {
            return Err(AppError::conflict(
                "recovery_changed",
                "local deletion evidence changed during verification",
            ));
        }
        super::put_row_in(
            &mut tx,
            &share.id,
            &path,
            &super::Row {
                size: 0,
                mtime_ns: 0,
                sha256: None,
                deleted: true,
                vv: deleted.vv.clone(),
            },
        )
        .await?;
    }
    super::transfer::archive_transfer(&mut tx, id, None).await?;
    sqlx::query("DELETE FROM transfers WHERE id=?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    state.sync_wake.notify_waiters();
    Ok(true)
}

async fn operation_completed(state: &AppState, id: &str) -> AppResult<bool> {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM operations WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db)
        .await?;
    Ok(status.as_deref() == Some("completed"))
}

async fn verify_applied_folder_event(state: &AppState, id: &str) -> AppResult<bool> {
    let pending: Option<(String, String, String)> = sqlx::query_as(
        "SELECT peer_id,share_id,event_json FROM pending_peer_events WHERE operation_id=?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    let Some((peer_id, share_id, raw)) = pending else {
        return Ok(false);
    };
    let event: super::PeerEvent = serde_json::from_str(&raw)?;
    if !matches!(event.kind.as_str(), "folder_created" | "folder_moved") {
        return Ok(false);
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM operation_items WHERE operation_id=?")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    if count != 1 {
        return Ok(false);
    }
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|s| s.id == share_id && s.includes(&peer_id))
        .ok_or_else(|| AppError::conflict("peer_share_changed", "share no longer available"))?;
    let peer = net::peer(state, &peer_id).await?;
    let remote = remote_event_evidence(
        state,
        &peer,
        &share,
        &event.data.path,
        &event.data.vv,
        Some((&event.origin, event.origin_seq)),
    )
    .await?;
    let Some(inventory) = &remote.inventory else {
        return Ok(false);
    };
    let local = evidence(state, None, &share.name, &remote.path, &remote.vv).await?;
    if local.inventory.is_none()
        || event.kind == "folder_moved" && local.inventory.as_ref() != Some(inventory)
    {
        return Ok(false);
    }
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    for entry in inventory.iter().filter(|entry| entry.sha256.is_some()) {
        let role = ism_core::sidecar::companion_role(&entry.name, &state.sidecar_extensions())
            .map_or(ism_core::dto::FileRole::Original, |role| match role {
                ism_core::sidecar::CompanionRole::Sidecar => ism_core::dto::FileRole::Sidecar,
                _ => ism_core::dto::FileRole::Companion,
            });
        let rel = RelPath::parse(&entry.name)
            .map_err(|e| AppError::invalid_location(e.to_string()))?
            .rebase(&RelPath::root(), &share.root_rel(&remote.path)?)
            .ok_or_else(|| AppError::internal("folder entry cannot be rebased"))?;
        crate::ops::exec::verified_asset_at_root(state, &root, &rel, role).await?;
    }
    let _guard = state.replication_operation_lock.lock().await;
    if let Some(from) = &event.data.from
        && *from != remote.path
    {
        match std::fs::symlink_metadata(root.abs(&share.root_rel(from)?)?) {
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if evidence(state, None, &share.name, &remote.path, &remote.vv)
        .await?
        .inventory
        != local.inventory
    {
        return Err(AppError::conflict(
            "recovery_changed",
            "folder changed during verification",
        ));
    }
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    let saved: Option<String> =
        sqlx::query_scalar("SELECT event_json FROM pending_peer_events WHERE operation_id=?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    if saved.as_deref() != Some(raw.as_str()) {
        return Err(AppError::conflict(
            "recovery_changed",
            "pending folder event changed",
        ));
    }
    sqlx::query("UPDATE operation_items SET status='done',result_json=? WHERE operation_id=? AND idx=0 AND status IN ('failed','skipped')")
        .bind(serde_json::json!({"recovery_verified":true,"location":remote.path,"evidence":remote}).to_string())
        .bind(id).execute(&mut *tx).await?;
    tx.commit().await?;
    super::operation_finished(state, id, &[]).await?;
    state.emit(ServerEvent::OperationUpdated {
        operation_id: id.into(),
    });
    operation_completed(state, id).await
}

async fn repair_operation(state: &AppState, id: &str) -> AppResult<bool> {
    let replacement: Option<String> = sqlx::query_scalar(
        "SELECT replacement_operation_id FROM sync_recovery WHERE kind='operation' AND target_id=?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?
    .flatten();
    if let Some(replacement) = replacement {
        return operation_completed(state, &replacement).await;
    }
    if operation_completed(state, id).await? {
        return Ok(true);
    }
    let (status, principal): (String, String) =
        sqlx::query_as("SELECT status,principal FROM operations WHERE id = ?")
            .bind(id)
            .fetch_one(&state.db)
            .await?;
    if matches!(
        status.as_str(),
        "queued" | "running" | "waiting_maintenance"
    ) {
        return Ok(false);
    }
    if admin::restore_missing_folder(state, id).await? {
        // The parent operation, not prerequisite reception, remains the completion target.
        return Ok(false);
    }
    if status == "recovery_required" {
        match super::transfer::recheck_publish_recovery(state, id).await {
            Ok(()) => return operation_completed(state, id).await,
            Err(e) if e.code() == "recovery_recheck_failed" => {
                return crate::ops::exec::recover_bookkeeping(state, id).await;
            }
            Err(e) => return Err(e),
        }
    }
    if verify_applied_folder_event(state, id).await? {
        return Ok(true);
    }
    /// A failed plan may describe a move that already happened independently. Verify both peers
    /// before completing its bookkeeping; destination existence alone is never enough.
    async fn verify_applied_peer_move(state: &AppState, id: &str) -> AppResult<bool> {
        type PendingMove = (i64, String, String, String, String, String, Option<String>);
        let pending: Vec<PendingMove> = sqlx::query_as(
            "SELECT idx,share_id,kind,to_path,vv_json,peer_id,from_path FROM pending_peer_ops
                 WHERE operation_id=? AND kind IN ('move','folder_move') ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&state.db)
        .await?;
        if pending.is_empty() {
            return Ok(false);
        }
        let item_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM operation_items WHERE operation_id=?")
                .bind(id)
                .fetch_one(&state.db)
                .await?;
        if item_count != pending.len() as i64 {
            return Ok(false);
        }
        let shares = super::shares(state).await?;
        let mut verified = Vec::new();
        for (index, share_id, kind, to, vv, peer_id, _) in &pending {
            let share = shares
                .iter()
                .find(|share| share.id == *share_id && share.includes(peer_id))
                .ok_or_else(|| {
                    AppError::conflict("peer_share_changed", "share no longer available")
                })?;
            let peer = net::peer(state, peer_id).await?;
            let vv: VersionVector = serde_json::from_str(vv)?;
            let remote = remote_evidence(state, &peer, share, to, &vv).await?;
            let local = evidence(state, None, &share.name, &remote.path, &remote.vv).await?;
            let matches = match kind.as_str() {
                "folder_move" => remote
                    .inventory
                    .as_ref()
                    .is_some_and(|tree| local.inventory.as_ref() == Some(tree)),
                _ => {
                    remote.sha256.is_some()
                        && local.sha256 == remote.sha256
                        && local.size == remote.size
                }
            };
            if !matches {
                return Ok(false);
            }
            let root = files::root(&state.db, &share.root_id).await?;
            root.ensure_available()?;
            let names: Vec<&str> = match &remote.inventory {
                Some(tree) => tree
                    .iter()
                    .filter(|entry| entry.sha256.is_some())
                    .map(|entry| entry.name.as_str())
                    .collect(),
                None => vec![""],
            };
            for name in names {
                let location = RelPath::parse(name)
                    .map_err(|e| AppError::invalid_location(e.to_string()))?
                    .rebase(&RelPath::root(), &share.root_rel(&remote.path)?)
                    .ok_or_else(|| AppError::internal("move entry cannot be rebased"))?;
                let role = ism_core::sidecar::companion_role(
                    location
                        .name()
                        .ok_or_else(|| AppError::invalid_location("file has no name"))?,
                    &state.sidecar_extensions(),
                )
                .map_or(ism_core::dto::FileRole::Original, |role| match role {
                    ism_core::sidecar::CompanionRole::Sidecar => ism_core::dto::FileRole::Sidecar,
                    _ => ism_core::dto::FileRole::Companion,
                });
                crate::ops::exec::verified_asset_at_root(state, &root, &location, role).await?;
            }
            verified.push((*index, remote));
        }
        let _guard = state.replication_operation_lock.lock().await;
        let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
        for (index, remote) in &verified {
            let (_, share_id, _, _, _, _, from) = pending
                .iter()
                .find(|(idx, ..)| idx == index)
                .ok_or_else(|| AppError::internal("missing verified move"))?;
            let share = shares
                .iter()
                .find(|share| share.id == *share_id)
                .ok_or_else(|| {
                    AppError::conflict("peer_share_changed", "share no longer available")
                })?;
            let root = files::root(&state.db, &share.root_id).await?;
            root.ensure_available()?;
            if let Some(from) = from {
                match std::fs::symlink_metadata(root.abs(&share.root_rel(from)?)?) {
                    Ok(_) => return Ok(false),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            } else {
                return Err(AppError::conflict(
                    "recovery_evidence_missing",
                    "move source is not recorded",
                ));
            }
            let local = evidence(state, None, &share.name, &remote.path, &remote.vv).await?;
            if local.sha256 != remote.sha256
                || local.size != remote.size
                || local.inventory != remote.inventory
            {
                return Err(AppError::conflict(
                    "recovery_changed",
                    "destination changed during verification",
                ));
            }
            let unchanged: Option<PendingMove> = sqlx::query_as(
                "SELECT idx,share_id,kind,to_path,vv_json,peer_id,from_path FROM pending_peer_ops WHERE operation_id=? AND idx=?",
            )
            .bind(id)
            .bind(index)
            .fetch_optional(&mut *tx)
            .await?;
            if unchanged.as_ref() != pending.iter().find(|(idx, ..)| idx == index) {
                return Err(AppError::conflict(
                    "recovery_changed",
                    "pending move changed during verification",
                ));
            }
            sqlx::query("UPDATE pending_peer_ops SET to_path=? WHERE operation_id=? AND idx=?")
                .bind(&remote.path)
                .bind(id)
                .bind(index)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE operation_items SET status='done',result_json=? WHERE operation_id=? AND idx=? AND status IN ('failed','skipped')")
                    .bind(serde_json::json!({ "recovery_verified": true, "location": remote.path }).to_string())
                    .bind(id).bind(index).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        super::operation_finished(state, id, &[]).await?;
        sqlx::query("UPDATE operations SET status='completed',finished_at=?,recovery_resolved=1 WHERE id=? AND status IN ('failed','partial')")
                .bind(db::now()).bind(id).execute(&state.db).await?;
        state.emit(ServerEvent::OperationUpdated {
            operation_id: id.into(),
        });
        Ok(true)
    }
    if verify_applied_peer_move(state, id).await? {
        return operation_completed(state, id).await;
    }
    if !principal.starts_with("peer:") {
        return Err(AppError::conflict(
            "recovery_evidence_missing",
            "execution evidence is incomplete",
        ));
    }
    let items: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT idx,kind,action_json FROM operation_items WHERE operation_id=? AND status IN ('failed','skipped','pending') ORDER BY idx",
    )
    .bind(id)
    .fetch_all(&state.db)
    .await?;
    if items.iter().any(|(_, kind, _)| kind == "publish_upload") {
        match crate::maintenance::retry_failed_publish_upload(state, id).await {
            Ok(()) => return Ok(false),
            Err(e)
                if matches!(
                    e.code(),
                    "publish_retry_unverified" | "publish_retry_invalid"
                ) =>
            {
                tracing::debug!(operation = %id, "staged retry unavailable: {}", e.message());
            }
            Err(e) => return Err(e),
        }
    }
    for (index, kind, action) in items {
        if kind != "publish_upload" {
            let principal = crate::auth::Principal::Peer {
                peer_id: principal
                    .strip_prefix("peer:")
                    .ok_or_else(|| AppError::forbidden("recovery requires a peer operation"))?
                    .to_owned(),
            };
            let actions = vec![crate::ops::exec::parse_action(&action)?];
            let preview = crate::ops::preview(
                state,
                &principal,
                &ism_core::dto::OperationRequest {
                    idempotency_key: format!("recovery-preview:{id}:{index}"),
                    label: None,
                    actions: actions.clone(),
                    allow_owner_transfer: false,
                    allow_partial: false,
                },
            )
            .await?;
            if !preview.ok {
                let blocker =
                    preview.blockers.into_iter().next().ok_or_else(|| {
                        AppError::internal("blocked recovery preview has no blocker")
                    })?;
                return Err(AppError::conflict_dynamic(blocker.code, blocker.message));
            }
            let evidence = operation_plan_evidence(state, &actions, &[preview]).await?;
            admit_plan(state, "operation", id, &evidence).await?;
        }
        admin::readd_sync_operation(state, id, index).await?;
    }
    Ok(false)
}

async fn process(state: &AppState, kind: &str, id: &str) -> AppResult<bool> {
    let links: Vec<(String, String)> =
        sqlx::query_as("SELECT kind,target_id FROM sync_recovery_links WHERE recovery_id=?")
            .bind(format!("{kind}:{id}"))
            .fetch_all(&state.db)
            .await?;
    if !links.is_empty() {
        for (kind, target) in &links {
            if get(state, parse_kind(kind)?, target)
                .await?
                .is_some_and(|r| r.state == RecoveryState::Fixed)
            {
                continue;
            }
            let done = if kind == "conflict" {
                sqlx::query_scalar::<_, String>("SELECT status FROM conflicts WHERE id=?")
                    .bind(target)
                    .fetch_optional(&state.db)
                    .await?
                    .as_deref()
                    == Some("resolved")
            } else if kind == "operation" {
                operation_completed(state, target).await?
            } else {
                let published: Option<Option<String>> =
                    sqlx::query_scalar("SELECT operation_id FROM transfer_history WHERE id=?")
                        .bind(target)
                        .fetch_optional(&state.db)
                        .await?;
                match published {
                    Some(Some(operation)) => operation_completed(state, &operation).await?,
                    Some(None) => true,
                    None => false,
                }
            };
            if !done {
                return Ok(false);
            }
        }
        if kind == "operation" {
            sqlx::query("UPDATE operation_items SET status='done' WHERE operation_id=? AND status IN ('failed','skipped')")
                .bind(id).execute(&state.db).await?;
            super::operation_finished(state, id, &[]).await?;
            sqlx::query("UPDATE operations SET status='completed',finished_at=?,recovery_resolved=1 WHERE id=? AND status IN ('failed','partial','cancelled')")
                .bind(db::now()).bind(id).execute(&state.db).await?;
            state.emit(ServerEvent::OperationUpdated {
                operation_id: id.into(),
            });
        }
        return if kind == "transfer" {
            let original: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transfers WHERE id=?)")
                    .bind(id)
                    .fetch_one(&state.db)
                    .await?;
            if original {
                repair_transfer(state, id).await
            } else {
                Ok(true)
            }
        } else {
            Ok(true)
        };
    }
    match parse_kind(kind)? {
        RecoveryKind::Operation => repair_operation(state, id).await,
        RecoveryKind::Transfer => repair_transfer(state, id).await,
        RecoveryKind::Conflict => {
            let status: String = sqlx::query_scalar("SELECT status FROM conflicts WHERE id=?")
                .bind(id)
                .fetch_one(&state.db)
                .await?;
            if status == "resolved" {
                return Ok(true);
            }
            let resolution: Option<String> = sqlx::query_scalar(
                "SELECT resolution FROM sync_recovery WHERE kind='conflict' AND target_id=?",
            )
            .bind(id)
            .fetch_one(&state.db)
            .await?;
            if let Some(resolution) = resolution {
                if status == "open" {
                    let revision:Option<String>=sqlx::query_scalar(
                        "SELECT decision_revision FROM sync_recovery WHERE kind='conflict' AND target_id=?",
                    ).bind(id).fetch_one(&state.db).await?;
                    if let Some(ref revision) = revision
                        && super::versions::get(
                            state,
                            &RecoveryTarget {
                                kind: RecoveryKind::Conflict,
                                id: id.into(),
                                peer_id: None,
                            },
                        )
                        .await?
                        .revision
                            != *revision
                    {
                        return Err(AppError::conflict(
                            "version_conflict",
                            "versions changed before the repair started",
                        ));
                    }
                    admin::resolve_reviewed(
                        state,
                        "recovery",
                        id,
                        serde_json::from_str(&resolution)?,
                        revision.as_deref(),
                    )
                    .await?;
                }
                Ok(false)
            } else {
                Err(AppError::conflict(
                    "choose_version",
                    "Choose a version inside ISM",
                ))
            }
        }
    }
}

const RECOVERY_CONCURRENCY_PER_KIND: usize = 4;
const RECOVERY_CONCURRENCY_PER_PEER: usize = 1;
const RECOVERY_CONCURRENCY_UNATTRIBUTED: usize = RECOVERY_CONCURRENCY_PER_KIND - 1;
const RECOVERY_KINDS: [&str; 3] = ["operation", "transfer", "conflict"];
const FORWARD_CONCURRENCY: usize = 4;

type EligibleJob = (String, String, String, Option<String>);

async fn eligible_jobs(
    state: &AppState,
    limit: usize,
    kind: Option<&str>,
    active_peers: Option<&std::collections::HashMap<Option<String>, usize>>,
    active_jobs: &[&str],
) -> AppResult<Vec<EligibleJob>> {
    let mut peers = active_peers.cloned().unwrap_or_default();
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
    let mut cursor: Option<(String, String)> = None;
    let mut jobs = Vec::new();
    let active_jobs = serde_json::to_string(active_jobs)?;
    loop {
        type Row = (String, String, String, Option<String>, String);
        let saturated = serde_json::to_string(
            &peers
                .iter()
                .filter_map(|(peer, count)| {
                    (*count >= RECOVERY_CONCURRENCY_PER_PEER)
                        .then_some(peer.as_ref())
                        .flatten()
                })
                .collect::<Vec<_>>(),
        )?;
        let unattributed_saturated =
            peers.get(&None).copied().unwrap_or_default() >= RECOVERY_CONCURRENCY_UNATTRIBUTED;
        let rows: Vec<Row> = sqlx::query_as(
            "WITH candidates AS (SELECT r.id,r.kind,r.target_id,
                CASE r.kind
                    WHEN 'conflict' THEN (SELECT peer_id FROM conflicts WHERE id=r.target_id)
                    WHEN 'transfer' THEN COALESCE(
                        (SELECT peer_id FROM transfers WHERE id=r.target_id),
                        (SELECT peer_id FROM transfer_history WHERE id=r.target_id))
                    WHEN 'operation' THEN (
                        SELECT CASE
                            WHEN COUNT(*)=0 THEN (
                                SELECT CASE WHEN substr(principal,1,5)='peer:' AND length(principal)>5
                                    THEN substr(principal,6) END
                                FROM operations WHERE id=r.target_id)
                            WHEN MIN(peer_id)=MAX(peer_id) THEN MIN(peer_id)
                        END
                        FROM (SELECT peer_id FROM pending_peer_ops WHERE operation_id=r.target_id
                              UNION ALL
                              SELECT peer_id FROM pending_peer_events WHERE operation_id=r.target_id))
                END AS peer_id,
                r.updated_at
             FROM sync_recovery r
             WHERE (r.state='queued' OR (r.state='waiting' AND r.updated_at < ?)
                 OR (r.state='needs_decision' AND EXISTS(SELECT 1 FROM sync_recovery_links l JOIN conflicts c ON l.kind='conflict' AND c.id=l.target_id
                     WHERE l.recovery_id=r.id AND c.status='resolved')))
                AND (? IS NULL OR r.kind=?)
                AND r.id NOT IN (SELECT value FROM json_each(?))
                AND (? IS NULL OR (r.updated_at,r.id) > (?,?)))
             SELECT id,kind,target_id,peer_id,updated_at FROM candidates
             WHERE (peer_id IS NOT NULL OR ?=0)
             AND (peer_id IS NULL OR NOT EXISTS (
                 SELECT 1 FROM json_each(?) saturated WHERE saturated.value=peer_id
             ))
             ORDER BY updated_at,id LIMIT 100",
        )
        .bind(&cutoff)
        .bind(kind)
        .bind(kind)
        .bind(&active_jobs)
        .bind(cursor.as_ref().map(|(updated, _)| updated))
        .bind(cursor.as_ref().map(|(updated, _)| updated))
        .bind(cursor.as_ref().map(|(_, id)| id))
        .bind(unattributed_saturated)
        .bind(&saturated)
        .fetch_all(&state.db)
        .await?;
        let count = rows.len();
        for (job, job_kind, target, peer, updated) in rows {
            cursor = Some((updated, job.clone()));
            if active_peers.is_some() {
                let capacity = if peer.is_some() {
                    RECOVERY_CONCURRENCY_PER_PEER
                } else {
                    RECOVERY_CONCURRENCY_UNATTRIBUTED
                };
                let active = peers.entry(peer.clone()).or_default();
                if *active >= capacity {
                    continue;
                }
                *active += 1;
            }
            jobs.push((job, job_kind, target, peer));
            if jobs.len() == limit {
                return Ok(jobs);
            }
        }
        if count < 100 {
            return Ok(jobs);
        }
    }
}

async fn run_job(state: &AppState, job: String, kind: String, id: String) -> AppResult<()> {
    let claimed = sqlx::query(
        "UPDATE sync_recovery SET state='fixing',updated_at=?
         WHERE id=? AND (state='queued' OR (state='waiting' AND updated_at < ?)
             OR (state='needs_decision' AND EXISTS(
                 SELECT 1 FROM sync_recovery_links l JOIN conflicts c
                 ON l.kind='conflict' AND c.id=l.target_id
                 WHERE l.recovery_id=sync_recovery.id AND c.status='resolved')))",
    )
    .bind(db::now())
    .bind(&job)
    .bind((chrono::Utc::now() - chrono::Duration::seconds(30)).to_rfc3339())
    .execute(&state.db)
    .await?;
    if claimed.rows_affected() != 1 {
        return Ok(());
    }
    state.emit(ServerEvent::SyncChanged);
    let result = crate::activity::observe(
        state,
        format!("recovery:{job}"),
        "Recovery execution",
        &format!("{kind}: {id}"),
        Some("/sync"),
        |task| {
            let (kind, id) = (&kind, &id);
            async move {
                task.phase("Verifying evidence and applying the recovery plan");
                process(state, kind, id).await
            }
        },
    )
    .await;
    let (status, code, detail, consumes_attempt) = match result {
        Ok(true) => ("fixed", None, None, false),
        Ok(false) => (
            "waiting",
            Some("awaiting_completion".to_owned()),
            None,
            false,
        ),
        Err(e) => {
            tracing::warn!(recovery = %job, code = %e.code(), "recovery deferred: {}", e.message());
            (
                if e.code() == "recovery_exhausted" {
                    "not_retryable"
                } else if matches!(e.code(), "choose_version" | "version_conflict") {
                    "needs_decision"
                } else {
                    "waiting"
                },
                Some(e.code().to_owned()),
                Some(e.message().to_owned()),
                !matches!(
                    e.code(),
                    "recovery_required"
                        | "maintenance_blocked"
                        | "root_unavailable"
                        | "peer_evidence_unavailable"
                        | "peer_unreachable"
                        | "peer_share_changed"
                        | "recovery_evidence_missing"
                        | "recovery_evidence_changed"
                        | "recovery_plan_unchanged"
                        | "revision_changed"
                        | "peer_busy"
                        | "direct_writes_unavailable"
                        | "choose_version"
                        | "version_conflict"
                ),
            )
        }
    };
    if kind == "conflict" && status == "needs_decision" {
        super::versions::reprepare(state, &id).await?;
    }
    sqlx::query(
        "UPDATE sync_recovery SET state=?,code=?,detail=?,updated_at=?,attempts=attempts+?
             WHERE id=? AND state='fixing'",
    )
    .bind(status)
    .bind(code)
    .bind(&detail)
    .bind(db::now())
    .bind(i64::from(consumes_attempt))
    .bind(&job)
    .execute(&state.db)
    .await?;
    state.emit(ServerEvent::SyncChanged);
    Ok(())
}

#[cfg(test)]
pub async fn tick(state: &AppState) -> AppResult<()> {
    discover(state).await?;
    for (job, kind, id, _) in eligible_jobs(state, 50, None, None, &[]).await? {
        run_job(state, job, kind, id).await?;
    }
    Ok(())
}

async fn local_loop(state: AppState) {
    let mut workers = tokio::task::JoinSet::new();
    let mut running: std::collections::HashMap<tokio::task::Id, (&str, String, Option<String>)> =
        std::collections::HashMap::new();
    let mut interrupted = std::collections::HashSet::new();
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        for job in interrupted.clone() {
            match sqlx::query(
                "UPDATE sync_recovery SET state='waiting',code='recovery_worker_interrupted',
                 detail='Recovery was interrupted. ISM will retry.',updated_at=?
                 WHERE id=? AND state='fixing'",
            )
            .bind(db::now())
            .bind(&job)
            .execute(&state.db)
            .await
            {
                Ok(result) => {
                    interrupted.remove(&job);
                    if result.rows_affected() != 0 {
                        state.emit(ServerEvent::SyncChanged);
                    }
                }
                Err(error) => {
                    tracing::error!(recovery=%job, error=%error, "could not requeue interrupted recovery");
                }
            }
        }
        if running.len() < RECOVERY_CONCURRENCY_PER_KIND * RECOVERY_KINDS.len() {
            if let Err(error) = discover(&state).await {
                tracing::error!("recovery queue: {}", error.message());
            } else {
                for kind in RECOVERY_KINDS {
                    let active = running
                        .values()
                        .filter(|(running_kind, _, _)| *running_kind == kind)
                        .count();
                    let available = RECOVERY_CONCURRENCY_PER_KIND - active;
                    if available == 0 {
                        continue;
                    }
                    let mut active_peers = std::collections::HashMap::new();
                    for (_, _, peer) in running
                        .values()
                        .filter(|(running_kind, _, _)| *running_kind == kind)
                    {
                        *active_peers.entry(peer.clone()).or_default() += 1;
                    }
                    let active_jobs: Vec<&str> = running
                        .values()
                        .map(|(_, job, _)| job.as_str())
                        .chain(interrupted.iter().map(String::as_str))
                        .collect();
                    match eligible_jobs(
                        &state,
                        available,
                        Some(kind),
                        Some(&active_peers),
                        &active_jobs,
                    )
                    .await
                    {
                        Ok(jobs) => {
                            for (job, job_kind, id, peer) in jobs {
                                let state = state.clone();
                                let running_job = job.clone();
                                let handle = workers
                                    .spawn(async move { run_job(&state, job, job_kind, id).await });
                                running.insert(handle.id(), (kind, running_job, peer));
                            }
                        }
                        Err(error) => tracing::error!("recovery queue: {}", error.message()),
                    }
                }
            }
        }
        tokio::select! {
            result = workers.join_next_with_id(), if !workers.is_empty() => {
                match result {
                    Some(Ok((id, result))) => {
                        let running_job = running.remove(&id);
                        if let Err(error) = result {
                            tracing::error!("recovery worker: {}", error.message());
                            if let Some((_, job, _)) = running_job {
                                interrupted.insert(job);
                            }
                        }
                    }
                    Some(Err(error)) => {
                        if let Some((_, job, _)) = running.remove(&error.id()) {
                            interrupted.insert(job);
                        }
                        tracing::error!("recovery task: {error}");
                    }
                    _ => {}
                }
            }
            () = tokio::time::sleep(tick) => {}
        }
    }
}

async fn forward_peer(state: &AppState, peer_id: &str) -> AppResult<()> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    type ForwardRow = (String, String, Option<String>, String, Option<String>);
    let rows: Vec<ForwardRow> = sqlx::query_as(
        "SELECT kind,target_id,resolution,view_json,decision_revision FROM outgoing_recovery_requests
         WHERE peer_id=? AND (state='queued' OR (state='waiting' AND updated_at<?))
         ORDER BY updated_at,kind,target_id LIMIT 100",
    )
    .bind(peer_id)
    .bind(&cutoff)
    .fetch_all(&mut *tx)
    .await?;
    let Some((_, _, resolution, _, revision)) = rows.first() else {
        return Ok(());
    };
    let resolution = resolution.clone();
    let verified = revision.is_some();
    let mut targets = Vec::new();
    for (kind, id, choice, _, revision) in &rows {
        if *choice != resolution || revision.is_some() != verified {
            continue;
        }
        let claimed = sqlx::query(
            "UPDATE outgoing_recovery_requests SET state='forwarding'
             WHERE peer_id=? AND kind=? AND target_id=? AND
                (state='queued' OR (state='waiting' AND updated_at<?))",
        )
        .bind(peer_id)
        .bind(kind)
        .bind(id)
        .bind(&cutoff)
        .execute(&mut *tx)
        .await?;
        if claimed.rows_affected() == 1 {
            targets.push(RecoveryTarget {
                kind: parse_kind(kind)?,
                id: id.clone(),
                peer_id: None,
            });
        }
    }
    tx.commit().await?;
    if targets.is_empty() {
        return Ok(());
    }
    let result: AppResult<RecoveryBatchResult> = async {
        let peer = net::peer(state, peer_id).await?;
        let input = RecoveryBatchInput {
            targets: targets.clone(),
            all: false,
            resolution: resolution
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
        };
        let (endpoint, body) = if verified {
            let decisions = targets
                .iter()
                .map(|target| {
                    let revision = rows
                        .iter()
                        .find(|(kind, id, _, _, _)| {
                            *kind == kind_name(target.kind) && *id == target.id
                        })
                        .and_then(|row| row.4.clone())
                        .ok_or_else(|| AppError::internal("missing persisted decision revision"))?;
                    Ok(ism_core::dto::RecoveryDecision {
                        target: target.clone(),
                        revision,
                    })
                })
                .collect::<AppResult<Vec<_>>>()?;
            (
                "/api/peer/v1/recovery/decisions",
                serde_json::to_vec(&ism_core::dto::RecoveryDecisionInput {
                    decisions,
                    resolution: input
                        .resolution
                        .ok_or_else(|| AppError::internal("missing persisted version choice"))?,
                })?,
            )
        } else {
            ("/api/peer/v1/recovery", serde_json::to_vec(&input)?)
        };
        let response = net::request(
            state,
            &peer.url,
            &peer.secret,
            reqwest::Method::POST,
            endpoint,
            Some(body),
        )
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| AppError::conflict("peer_unreachable", net::net_error(&e)))?;
        if !response.status().is_success() {
            return Err(AppError::conflict(
                "peer_recovery_unavailable",
                format!("Receiver returned HTTP {}", response.status()),
            ));
        }
        let result: RecoveryBatchResult = response.json().await.map_err(AppError::internal)?;
        let mut seen = std::collections::HashSet::new();
        if result.items.len() != targets.len()
            || result.items.iter().any(|item| {
                item.target.peer_id.is_some()
                    || !targets.iter().any(|target| target == &item.target)
                    || !seen.insert((kind_name(item.target.kind), item.target.id.clone()))
            })
        {
            return Err(AppError::conflict(
                "invalid_peer_recovery",
                "receiver returned mismatched recovery targets",
            ));
        }
        Ok(result)
    }
    .await;
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    match result {
        Ok(result) => {
            for mut item in result.items {
                item.target.peer_id = Some(peer_id.to_owned());
                let status = if item.state == RecoveryState::Waiting {
                    "waiting"
                } else {
                    "accepted"
                };
                sqlx::query(
                    "UPDATE outgoing_recovery_requests SET state=?,view_json=?,updated_at=?
                     WHERE peer_id=? AND kind=? AND target_id=? AND state='forwarding'",
                )
                .bind(status)
                .bind(serde_json::to_string(&item)?)
                .bind(db::now())
                .bind(peer_id)
                .bind(kind_name(item.target.kind))
                .bind(&item.target.id)
                .execute(&mut *tx)
                .await?;
            }
        }
        Err(error) => {
            tracing::warn!(peer=%peer_id, code=%error.code(), "outgoing recovery waiting: {}", error.message());
            for (kind, id, _, json, _) in rows {
                let mut item: RecoveryView = serde_json::from_str(&json)?;
                item.state = RecoveryState::Waiting;
                item.code = Some(error.code().into());
                item.detail = Some(error.message().into());
                item.can_fix = false;
                item.can_dismiss = false;
                sqlx::query(
                    "UPDATE outgoing_recovery_requests SET state='waiting',view_json=?,updated_at=?
                     WHERE peer_id=? AND kind=? AND target_id=? AND state='forwarding'",
                )
                .bind(serde_json::to_string(&item)?)
                .bind(db::now())
                .bind(peer_id)
                .bind(kind)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    tx.commit().await?;
    state.emit_ui(ServerEvent::SyncChanged);
    Ok(())
}

async fn forward_loop(state: AppState) {
    let mut workers = tokio::task::JoinSet::new();
    let mut running = std::collections::HashMap::new();
    let mut resetting = std::collections::HashSet::new();
    let tick = Duration::from_secs(state.config.tick_secs.max(1));
    loop {
        for peer in resetting.clone() {
            match sqlx::query(
                "UPDATE outgoing_recovery_requests SET state='queued'
                 WHERE peer_id=? AND state='forwarding'",
            )
            .bind(&peer)
            .execute(&state.db)
            .await
            {
                Ok(_) => {
                    resetting.remove(&peer);
                }
                Err(error) => tracing::error!(peer=%peer, "outgoing recovery claim reset: {error}"),
            }
        }
        if running.len() < FORWARD_CONCURRENCY {
            let excluded: Vec<&str> = running
                .values()
                .map(String::as_str)
                .chain(resetting.iter().map(String::as_str))
                .collect();
            let excluded = match serde_json::to_string(&excluded) {
                Ok(excluded) => excluded,
                Err(error) => {
                    tracing::error!("cannot encode active recovery peers: {error}");
                    tokio::time::sleep(tick).await;
                    continue;
                }
            };
            let peers = sqlx::query_scalar::<_, String>(
                "SELECT peer_id FROM outgoing_recovery_requests
                 WHERE (state='queued' OR (state='waiting' AND updated_at<?))
                 AND peer_id NOT IN (SELECT value FROM json_each(?))
                 GROUP BY peer_id ORDER BY MIN(updated_at),peer_id LIMIT ?",
            )
            .bind((chrono::Utc::now() - chrono::Duration::seconds(30)).to_rfc3339())
            .bind(excluded)
            .bind((FORWARD_CONCURRENCY - running.len()) as i64)
            .fetch_all(&state.db)
            .await;
            match peers {
                Ok(peers) => {
                    for peer in peers {
                        let state = state.clone();
                        let worker_peer = peer.clone();
                        let handle = workers.spawn(async move {
                            crate::activity::observe(
                                &state,
                                format!("recovery-forward:{worker_peer}"),
                                "Forward recovery request",
                                &worker_peer,
                                Some("/sync"),
                                |task| {
                                    let (state, peer) = (&state, &worker_peer);
                                    async move {
                                        task.phase("Forwarding recovery decisions to peer");
                                        forward_peer(state, peer).await
                                    }
                                },
                            )
                            .await
                        });
                        running.insert(handle.id(), peer);
                    }
                }
                Err(error) => tracing::error!("outgoing recovery queue: {error}"),
            }
        }
        tokio::select! {
            result = workers.join_next_with_id(), if !workers.is_empty() => {
                let (id, error) = match result {
                    Some(Ok((id, result))) => (id, result.err().map(|e| e.message().to_owned())),
                    Some(Err(error)) => (error.id(), Some(error.to_string())),
                    None => continue,
                };
                if let Some(peer) = running.remove(&id)
                    && let Some(error) = error
                {
                    tracing::error!(peer=%peer, "outgoing recovery worker: {error}");
                    resetting.insert(peer);
                }
            }
            () = tokio::time::sleep(tick) => {}
        }
    }
}

async fn reset_claims(state: &AppState, query: &'static str, queue: &str) {
    let mut failed = false;
    loop {
        match sqlx::query(query).execute(&state.db).await {
            Ok(_) => {
                if failed {
                    tracing::info!(queue, "recovery restart succeeded");
                }
                return;
            }
            Err(error) if !failed => {
                tracing::error!(queue, %error, "recovery restart failed; retrying");
                failed = true;
            }
            Err(error) => tracing::debug!(queue, %error, "recovery restart retry failed"),
        }
        tokio::time::sleep(Duration::from_secs(state.config.tick_secs.max(1))).await;
    }
}

pub async fn run(state: AppState) {
    let local = async {
        reset_claims(
            &state,
            "UPDATE sync_recovery SET state='queued' WHERE state='fixing'",
            "local",
        )
        .await;
        local_loop(state.clone()).await;
    };
    let outgoing = async {
        reset_claims(
            &state,
            "UPDATE outgoing_recovery_requests SET state='queued' WHERE state='forwarding'",
            "outgoing",
        )
        .await;
        forward_loop(state.clone()).await;
    };
    tokio::join!(local, outgoing);
}

#[cfg(test)]
mod policy_tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::{config::Config, crypto::SecretBox};

    async fn startup_reset_retries_without_blocking_other_queue(block_local: bool) {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let app = axum::Router::new()
                .fallback(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE });
            axum::serve(listener, app).await.unwrap();
        });
        let secret = state
            .secrets
            .encrypt(net::SECRET_PURPOSE, "synthetic-key")
            .unwrap();
        sqlx::query("INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at) VALUES('peer','Peer',?,'remote',?,'online',?)")
            .bind(url).bind(secret).bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query(
            "INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at)
             VALUES ('conflict:missing','conflict','missing',?,?,?)",
        )
        .bind(if block_local { "fixing" } else { "queued" })
        .bind(db::now())
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let item = RecoveryView {
            target: RecoveryTarget {
                kind: RecoveryKind::Transfer,
                id: "missing".into(),
                peer_id: None,
            },
            title: "photo.jpg".into(),
            state: RecoveryState::Queued,
            can_fix: false,
            can_dismiss: false,
            code: None,
            detail: None,
            replacement_operation_id: None,
            local_instance: None,
            peer_instance: None,
        };
        sqlx::query("INSERT INTO outgoing_recovery_requests(peer_id,kind,target_id,state,view_json,updated_at) VALUES('peer','transfer','missing',?,?,?)")
            .bind(if block_local { "queued" } else { "forwarding" })
            .bind(serde_json::to_string(&item).unwrap())
            .bind(db::now())
            .execute(&state.db)
            .await
            .unwrap();
        sqlx::query(if block_local {
            "CREATE TRIGGER reject_startup_reset BEFORE UPDATE ON sync_recovery
             WHEN OLD.state='fixing' AND NEW.state='queued'
             BEGIN SELECT RAISE(FAIL,'injected startup error'); END"
        } else {
            "CREATE TRIGGER reject_startup_reset BEFORE UPDATE ON outgoing_recovery_requests
             WHEN OLD.state='forwarding' AND NEW.state='queued'
             BEGIN SELECT RAISE(FAIL,'injected startup error'); END"
        })
        .execute(&state.db)
        .await
        .unwrap();
        let worker = tokio::spawn(run(state.clone()));
        let independent = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let query = if block_local {
                    "SELECT state FROM outgoing_recovery_requests WHERE peer_id='peer'"
                } else {
                    "SELECT state FROM sync_recovery WHERE id='conflict:missing'"
                };
                let status: String = sqlx::query_scalar(query)
                    .fetch_one(&state.db)
                    .await
                    .unwrap();
                if status == "waiting" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        sqlx::query("DROP TRIGGER reject_startup_reset")
            .execute(&state.db)
            .await
            .unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let query = if block_local {
                    "SELECT state FROM sync_recovery WHERE id='conflict:missing'"
                } else {
                    "SELECT state FROM outgoing_recovery_requests WHERE peer_id='peer'"
                };
                let status: String = sqlx::query_scalar(query)
                    .fetch_one(&state.db)
                    .await
                    .unwrap();
                if status == "waiting" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        worker.abort();
        server.abort();
        assert!(independent.is_ok(), "an unrelated queue did not run");
        assert!(recovered.is_ok(), "the startup reset was not retried");
    }

    #[tokio::test]
    async fn startup_reset_errors_do_not_stop_either_recovery_queue() {
        startup_reset_retries_without_blocking_other_queue(true).await;
        startup_reset_retries_without_blocking_other_queue(false).await;
    }

    #[tokio::test]
    async fn claim_rechecks_waiting_and_decision_eligibility() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        for (id, status, updated) in [
            ("conflict:waiting", "waiting", "2024-01-01T00:00:00Z"),
            ("conflict:decision", "queued", "2024-01-01T00:00:00Z"),
        ] {
            sqlx::query("INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at) VALUES(?,'conflict',?,?,?,?)")
                .bind(id).bind(id.strip_prefix("conflict:").unwrap()).bind(status)
                .bind(db::now()).bind(updated).execute(&state.db).await.unwrap();
        }
        let selected = eligible_jobs(&state, 2, None, None, &[]).await.unwrap();
        assert_eq!(selected.len(), 2);
        sqlx::query("UPDATE sync_recovery SET updated_at=? WHERE id='conflict:waiting'")
            .bind(db::now())
            .execute(&state.db)
            .await
            .unwrap();
        sqlx::query("UPDATE sync_recovery SET state='needs_decision' WHERE id='conflict:decision'")
            .execute(&state.db)
            .await
            .unwrap();
        for (job, kind, id, _) in selected {
            run_job(&state, job, kind, id).await.unwrap();
        }
        let states: Vec<(String, String)> =
            sqlx::query_as("SELECT id,state FROM sync_recovery ORDER BY id")
                .fetch_all(&state.db)
                .await
                .unwrap();
        assert_eq!(
            states,
            vec![
                ("conflict:decision".into(), "needs_decision".into()),
                ("conflict:waiting".into(), "waiting".into()),
            ]
        );
    }

    #[tokio::test]
    async fn unattributed_backlog_does_not_consume_all_kind_slots() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let mut tx = state.db.begin().await.unwrap();
        for n in 0..150 {
            let id = format!("unknown-{n:03}");
            sqlx::query("INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at) VALUES(?,'operation',?,'queued','2024-01-01','2024-01-01')")
                .bind(format!("operation:{id}")).bind(&id).execute(&mut *tx).await.unwrap();
        }
        sqlx::query("INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at) VALUES('operation:peer','operation','peer','queued','2024-01-02','2024-01-02')")
            .execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO pending_peer_ops(operation_id,idx,share_id,kind,vv_json,peer_id) VALUES('peer',0,'share','file_changed','{}','peer-a')")
            .execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let active = std::collections::HashMap::new();
        let jobs = eligible_jobs(&state, 4, Some("operation"), Some(&active), &[])
            .await
            .unwrap();
        assert_eq!(jobs.len(), 4);
        assert_eq!(jobs.iter().filter(|job| job.3.is_none()).count(), 3);
        assert!(jobs.iter().any(|job| job.3.as_deref() == Some("peer-a")));
        let active = std::collections::HashMap::from([(None, 3)]);
        let jobs = eligible_jobs(&state, 4, Some("operation"), Some(&active), &[])
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].3.as_deref(), Some("peer-a"));
    }

    #[tokio::test]
    async fn operation_attribution_includes_pending_events_and_rejects_ambiguity() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at) VALUES('peer-a','A','https://a.example','a','secret','online',?)")
            .bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled) VALUES('share','Share','root','','[]',1)")
            .execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at) VALUES('event','event','admin:test','test','{}','failed',?)")
            .bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO pending_peer_events(operation_id,peer_id,share_id,origin,origin_seq,expected_cursor,event_json) VALUES('event','peer-a','share','source',1,0,'{}')")
            .execute(&state.db).await.unwrap();
        sqlx::query("INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at) VALUES('operation:event','operation','event','queued',?,?)")
            .bind(db::now()).bind(db::now()).execute(&state.db).await.unwrap();
        let active = std::collections::HashMap::new();
        let jobs = eligible_jobs(&state, 4, Some("operation"), Some(&active), &[])
            .await
            .unwrap();
        assert_eq!(jobs[0].3.as_deref(), Some("peer-a"));
        sqlx::query("INSERT INTO pending_peer_ops(operation_id,idx,share_id,kind,vv_json,peer_id) VALUES('event',0,'share','file_changed','{}','peer-b')")
            .execute(&state.db).await.unwrap();
        let jobs = eligible_jobs(&state, 4, Some("operation"), Some(&active), &[])
            .await
            .unwrap();
        assert_eq!(jobs[0].3, None);
    }

    #[tokio::test]
    async fn stalled_unattributed_operation_does_not_block_independent_operation() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        for (id, status) in [("held", "recovery_required"), ("complete", "completed")] {
            sqlx::query("INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at) VALUES(?,?,'admin:test','test','{}',?,?)")
                .bind(id).bind(id).bind(status).bind(db::now()).execute(&state.db).await.unwrap();
        }
        sqlx::query("INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at) VALUES('operation:held','operation','held','queued',?,?)")
            .bind(db::now()).bind(db::now()).execute(&state.db).await.unwrap();
        let guard = state.replication_operation_lock.lock().await;
        let worker = tokio::spawn(local_loop(state.clone()));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status: String =
                    sqlx::query_scalar("SELECT state FROM sync_recovery WHERE id='operation:held'")
                        .fetch_one(&state.db)
                        .await
                        .unwrap();
                if status == "fixing" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first recovery was not claimed");
        sqlx::query("INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at) VALUES('operation:complete','operation','complete','queued',?,?)")
            .bind(db::now()).bind(db::now()).execute(&state.db).await.unwrap();
        let independent = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status: String = sqlx::query_scalar(
                    "SELECT state FROM sync_recovery WHERE id='operation:complete'",
                )
                .fetch_one(&state.db)
                .await
                .unwrap();
                if status == "fixed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let held: String =
            sqlx::query_scalar("SELECT state FROM sync_recovery WHERE id='operation:held'")
                .fetch_one(&state.db)
                .await
                .unwrap();
        worker.abort();
        drop(guard);
        assert!(independent.is_ok(), "independent recovery did not progress");
        assert_eq!(held, "fixing");
    }

    #[tokio::test]
    async fn a_slow_peer_does_not_hold_other_forwarding_slots() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let (entered, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_url = format!("http://{}", stalled.local_addr().unwrap());
        let hold = std::sync::Arc::new(tokio::sync::Notify::new());
        let stalled_hold = hold.clone();
        let stalled_server = tokio::spawn(async move {
            let app = axum::Router::new().fallback(move || {
                let entered = entered.clone();
                let hold = stalled_hold.clone();
                async move {
                    entered.send(()).unwrap();
                    hold.notified().await;
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            });
            axum::serve(stalled, app).await.unwrap();
        });
        let fast = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fast_url = format!("http://{}", fast.local_addr().unwrap());
        let fast_server = tokio::spawn(async move {
            let app = axum::Router::new()
                .fallback(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE });
            axum::serve(fast, app).await.unwrap();
        });
        let secret = state
            .secrets
            .encrypt(net::SECRET_PURPOSE, "synthetic-key")
            .unwrap();
        for n in 0..5 {
            let id = format!("peer-{n}");
            sqlx::query("INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at) VALUES(?,?,?,?,?,'online',?)")
                .bind(&id).bind(&id).bind(if n == 0 { &stalled_url } else { &fast_url })
                .bind(format!("instance-{n}")).bind(&secret).bind(db::now())
                .execute(&state.db).await.unwrap();
            let item = RecoveryView {
                target: RecoveryTarget {
                    kind: RecoveryKind::Transfer,
                    id: format!("transfer-{n}"),
                    peer_id: None,
                },
                title: format!("photo-{n}.jpg"),
                state: RecoveryState::Queued,
                can_fix: false,
                can_dismiss: false,
                code: None,
                detail: None,
                replacement_operation_id: None,
                local_instance: None,
                peer_instance: None,
            };
            sqlx::query("INSERT INTO outgoing_recovery_requests(peer_id,kind,target_id,state,view_json,updated_at) VALUES(?,'transfer',?,'queued',?,?)")
                .bind(&id).bind(&item.target.id).bind(serde_json::to_string(&item).unwrap())
                .bind(format!("2024-01-0{}T00:00:00Z", n + 1))
                .execute(&state.db).await.unwrap();
        }
        let worker = tokio::spawn(forward_loop(state.clone()));
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let status: String = sqlx::query_scalar(
                    "SELECT state FROM outgoing_recovery_requests WHERE peer_id='peer-4'",
                )
                .fetch_one(&state.db)
                .await
                .unwrap();
                if status == "waiting" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            requests.try_recv().is_err(),
            "the stalled peer was forwarded more than once concurrently"
        );
        hold.notify_waiters();
        worker.abort();
        stalled_server.abort();
        fast_server.abort();
        assert!(
            completed.is_ok(),
            "an unrelated peer waited for the stalled peer"
        );
    }

    #[tokio::test]
    async fn failed_forward_claim_reset_does_not_duplicate_a_request() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let (entered, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let app = axum::Router::new().fallback(move || {
                let entered = entered.clone();
                async move {
                    entered.send(()).unwrap();
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            });
            axum::serve(listener, app).await.unwrap();
        });
        let secret = state
            .secrets
            .encrypt(net::SECRET_PURPOSE, "synthetic-key")
            .unwrap();
        sqlx::query("INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at) VALUES('peer','Peer',?,'remote',?,'online',?)")
            .bind(url).bind(secret).bind(db::now()).execute(&state.db).await.unwrap();
        let item = RecoveryView {
            target: RecoveryTarget {
                kind: RecoveryKind::Transfer,
                id: "missing".into(),
                peer_id: None,
            },
            title: "photo.jpg".into(),
            state: RecoveryState::Queued,
            can_fix: false,
            can_dismiss: false,
            code: None,
            detail: None,
            replacement_operation_id: None,
            local_instance: None,
            peer_instance: None,
        };
        sqlx::query("INSERT INTO outgoing_recovery_requests(peer_id,kind,target_id,state,view_json,updated_at) VALUES('peer','transfer','missing','queued',?,?)")
            .bind(serde_json::to_string(&item).unwrap())
            .bind(db::now())
            .execute(&state.db).await.unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_forward_result BEFORE UPDATE ON outgoing_recovery_requests
             WHEN OLD.state='forwarding' AND NEW.state='waiting'
             BEGIN SELECT RAISE(FAIL,'injected finalization error'); END",
        )
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_forward_reset BEFORE UPDATE ON outgoing_recovery_requests
             WHEN OLD.state='forwarding' AND NEW.state='queued'
             BEGIN SELECT RAISE(FAIL,'injected reset error'); END",
        )
        .execute(&state.db)
        .await
        .unwrap();
        let worker = tokio::spawn(forward_loop(state.clone()));
        tokio::time::timeout(Duration::from_secs(3), requests.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let page = crate::activity::page(&state, 50, 0).await.unwrap();
                if page.current.iter().chain(&page.history).any(|activity| {
                    activity.task_key == "recovery-forward:peer"
                        && activity.status == ism_core::dto::BackgroundStatus::Failed
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("forward finalization did not fail");
        let lock = state.db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), requests.recv())
                .await
                .is_err(),
            "the pending claim was forwarded twice"
        );
        lock.commit().await.unwrap();
        sqlx::query("DROP TRIGGER reject_forward_result")
            .execute(&state.db)
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER reject_forward_reset")
            .execute(&state.db)
            .await
            .unwrap();
        let retry = tokio::time::timeout(Duration::from_secs(3), requests.recv()).await;
        worker.abort();
        server.abort();
        assert!(matches!(retry, Ok(Some(()))), "claim was not retried");
    }

    #[test]
    fn stale_receiver_evidence_never_authorizes_a_version_decision_or_dismissal() {
        let item = RecoveryView {
            target: RecoveryTarget {
                kind: RecoveryKind::Conflict,
                id: "conflict".into(),
                peer_id: None,
            },
            title: "Trip/photo.jpg".into(),
            state: RecoveryState::NeedsDecision,
            can_fix: false,
            can_dismiss: false,
            code: Some("choose_version".into()),
            detail: None,
            replacement_operation_id: None,
            local_instance: Some("Beta".into()),
            peer_instance: Some("Alpha".into()),
        };
        let json = serde_json::to_string(&item).unwrap();
        let current = mirrored_view("peer".into(), &json, None, Some(db::now()), None).unwrap();
        assert_eq!(current.state, RecoveryState::NeedsDecision);
        assert_eq!(current.target.peer_id.as_deref(), Some("peer"));
        let stale = mirrored_view(
            "peer".into(),
            &json,
            None,
            Some((chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339()),
            None,
        )
        .unwrap();
        assert_eq!(stale.state, RecoveryState::Waiting);
        assert!(!stale.can_dismiss);
        assert_eq!(stale.code.as_deref(), Some("peer_recovery_unavailable"));
        let offline = mirrored_view(
            "peer".into(),
            &json,
            Some("Receiver unreachable".into()),
            Some(db::now()),
            None,
        )
        .unwrap();
        assert_eq!(offline.state, RecoveryState::Waiting);
    }

    #[tokio::test]
    async fn current_file_move_version_anchors_later_folder_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let share = Share {
            id: "family".into(),
            name: "Family".into(),
            root_id: "photos".into(),
            path: RelPath::parse("shared").unwrap(),
            peer_ids: Vec::new(),
            enabled: true,
            junk: state.junk_rules(),
        };
        let mut original = VersionVector::default();
        original.increment("source");
        let mut moved = original.clone();
        moved.increment("source");
        for (kind, data) in [
            (
                "file_changed",
                super::super::EventData {
                    path: "old/photo.jpg".into(),
                    vv: original,
                    ..Default::default()
                },
            ),
            (
                "file_moved",
                super::super::EventData {
                    path: "current/photo.jpg".into(),
                    from: Some("old/photo.jpg".into()),
                    vv: moved.clone(),
                    ..Default::default()
                },
            ),
            (
                "folder_moved",
                super::super::EventData {
                    path: "archive/current".into(),
                    from: Some("current".into()),
                    ..Default::default()
                },
            ),
        ] {
            super::super::append_local(&state, &share.name, kind, &data)
                .await
                .unwrap();
        }
        assert_eq!(
            current_path(&state, &share, "current/photo.jpg", &moved)
                .await
                .unwrap(),
            "archive/current/photo.jpg",
        );
        assert_eq!(
            current_path(
                &state,
                &share,
                "current/photo.jpg",
                &VersionVector::default()
            )
            .await
            .unwrap(),
            "current/photo.jpg",
            "an empty file version must not anchor folder ordering",
        );
    }

    #[tokio::test]
    async fn recovery_lineage_rejects_later_overlapping_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let share = Share {
            id: "family".into(),
            name: "Family".into(),
            root_id: "photos".into(),
            path: RelPath::parse("shared").unwrap(),
            peer_ids: Vec::new(),
            enabled: true,
            junk: state.junk_rules(),
        };
        let mut version = VersionVector::default();
        version.increment("source");
        for (kind, path, from, vv) in [
            (
                super::super::FOLDER_PRUNED,
                "old",
                None,
                VersionVector::default(),
            ),
            ("file_changed", "old/photo.jpg", None, version.clone()),
            ("folder_moved", "new", Some("old"), VersionVector::default()),
        ] {
            super::super::append_local(
                &state,
                &share.name,
                kind,
                &super::super::EventData {
                    path: path.into(),
                    from: from.map(str::to_owned),
                    vv,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(
            current_path(&state, &share, "old/photo.jpg", &version)
                .await
                .unwrap(),
            "new/photo.jpg"
        );
        sqlx::query(
            "INSERT INTO peers(id,name,url,secret_enc,status,created_at)
             VALUES ('peer','Peer','https://peer.example','synthetic','online',?)",
        )
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let pending = super::super::PeerEvent {
            seq: 1,
            origin: "peer-instance".into(),
            origin_seq: 1,
            share: share.name.clone(),
            kind: super::super::FOLDER_PRUNED.into(),
            data: super::super::EventData {
                path: "new".into(),
                ..Default::default()
            },
        };
        sqlx::query(
            "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json)
             VALUES ('peer',1,?,?,?,?)",
        )
        .bind(&pending.origin)
        .bind(pending.origin_seq)
        .bind(&pending.share)
        .bind(serde_json::to_string(&pending).unwrap())
        .execute(&state.db)
        .await
        .unwrap();
        assert!(
            current_path(&state, &share, "old/photo.jpg", &version)
                .await
                .is_err(),
            "an unresolved prune at the relocated destination blocks lineage"
        );
        sqlx::query("DELETE FROM peer_inbox WHERE peer_id = 'peer'")
            .execute(&state.db)
            .await
            .unwrap();
        let mut tx = state.db.begin().await.unwrap();
        for seq in 1..=129 {
            let mut item = pending.clone();
            item.seq = seq;
            item.origin_seq = seq;
            item.data.path = if seq == 129 { "new" } else { "unrelated" }.into();
            sqlx::query(
                "INSERT INTO peer_inbox(peer_id,seq,origin,origin_seq,share_name,event_json)
                 VALUES ('peer',?,?,?,?,?)",
            )
            .bind(seq)
            .bind(&item.origin)
            .bind(item.origin_seq)
            .bind(&item.share)
            .bind(serde_json::to_string(&item).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        assert!(
            current_path(&state, &share, "old/photo.jpg", &version)
                .await
                .is_err(),
            "an overlapping prune beyond the first page must block lineage"
        );
        sqlx::query("DELETE FROM peer_inbox WHERE peer_id = 'peer'")
            .execute(&state.db)
            .await
            .unwrap();
        super::super::append_local(
            &state,
            &share.name,
            super::super::FOLDER_PRUNED,
            &super::super::EventData {
                path: "new".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            current_path(&state, &share, "old/photo.jpg", &version)
                .await
                .is_err()
        );
        assert_eq!(
            current_path(&state, &share, "other/photo.jpg", &version)
                .await
                .unwrap(),
            "other/photo.jpg"
        );
    }

    #[tokio::test]
    async fn replacement_plans_inherit_the_budget_across_restart_but_new_evidence_can_progress() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        for id in ["parent", "replacement"] {
            sqlx::query(
                "INSERT INTO sync_recovery (id,kind,target_id,state,created_at,updated_at)
                 VALUES (?,'operation',?,'fixing',?,?)",
            )
            .bind(format!("operation:{id}"))
            .bind(id)
            .bind(db::now())
            .bind(db::now())
            .execute(&state.db)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO sync_recovery_links VALUES ('operation:parent','operation','replacement')",
        )
        .execute(&state.db)
        .await
        .unwrap();
        let evidence = serde_json::json!({"path":"Trip/photo.jpg","sha256":"unchanged"});
        for _ in 0..3 {
            admit_plan(&state, "operation", "parent", &evidence)
                .await
                .unwrap();
        }
        state.db.close().await;
        drop(state);
        let restarted = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            admit_plan(&restarted, "operation", "replacement", &evidence)
                .await
                .unwrap_err()
                .code(),
            "recovery_plan_unchanged",
            "creating a replacement must not give an unchanged failed plan a new budget",
        );
        admit_plan(
            &restarted,
            "operation",
            "replacement",
            &serde_json::json!({"path":"Trip/current/photo.jpg","sha256":"changed"}),
        )
        .await
        .unwrap();
        let saved: String = sqlx::query_scalar(
            "SELECT evidence_json FROM sync_recovery WHERE target_id='replacement'",
        )
        .fetch_one(&restarted.db)
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<PlanAttempts>(&saved)
                .unwrap()
                .admissions,
            1
        );
    }

    #[tokio::test]
    async fn a_superseded_transfer_waits_for_its_verified_successor_then_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        for id in ["superseded", "successor"] {
            sqlx::query(
                "INSERT INTO sync_recovery (id,kind,target_id,state,created_at,updated_at)
                 VALUES (?,'transfer',?,'waiting',?,?)",
            )
            .bind(format!("transfer:{id}"))
            .bind(id)
            .bind(db::now())
            .bind(db::now())
            .execute(&state.db)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO sync_recovery_links VALUES ('transfer:superseded','transfer','successor')",
        )
        .execute(&state.db)
        .await
        .unwrap();
        assert!(!process(&state, "transfer", "superseded").await.unwrap());
        sqlx::query("UPDATE sync_recovery SET state='fixed' WHERE target_id='successor'")
            .execute(&state.db)
            .await
            .unwrap();
        assert!(
            process(&state, "transfer", "superseded").await.unwrap(),
            "a verified successor is sufficient even when the obsolete queue entry no longer exists"
        );
    }

    #[tokio::test]
    async fn changed_bytes_with_unchanged_metadata_enable_a_new_verified_plan() {
        use ism_core::dto::{
            FileRole, Location, OperationAction, PlannedFile, PreviewItem, PreviewResponse,
        };
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        let root = dir.path().join("photos");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join(".ism-root"), "photos\n").unwrap();
        sqlx::query("INSERT INTO mappings (root_id,name,container_path,created_at) VALUES ('photos','Photos',?,?)")
            .bind(root.to_string_lossy().as_ref()).bind(db::now()).execute(&state.db).await.unwrap();
        sqlx::query(
            "INSERT INTO sync_recovery (id,kind,target_id,state,created_at,updated_at)
             VALUES ('operation:move','operation','move','fixing',?,?)",
        )
        .bind(db::now())
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        let source = Location {
            root_id: "photos".into(),
            path: "photo.jpg".into(),
        };
        let target = Location {
            root_id: "photos".into(),
            path: "moved.jpg".into(),
        };
        let actions = vec![OperationAction::Move {
            source: source.clone(),
            target: target.clone(),
            expected_revision: None,
        }];
        let previews = vec![PreviewResponse {
            ok: true,
            blockers: vec![],
            items: vec![PreviewItem {
                index: 0,
                kind: "move".into(),
                files: vec![PlannedFile {
                    from: Some(source),
                    to: Some(target),
                    role: FileRole::Original,
                    copy: false,
                }],
                assets: vec![],
                blockers: vec![],
                warnings: vec![],
            }],
        }];
        let path = root.join("photo.jpg");
        std::fs::write(&path, b"original-one").unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let evidence = operation_plan_evidence(&state, &actions, &previews)
            .await
            .unwrap();
        for _ in 0..3 {
            admit_plan(&state, "operation", "move", &evidence)
                .await
                .unwrap();
        }
        assert_eq!(
            admit_plan(&state, "operation", "move", &evidence)
                .await
                .unwrap_err()
                .code(),
            "recovery_plan_unchanged"
        );
        std::fs::write(&path, b"original-two").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(metadata.modified().unwrap())
            .unwrap();
        assert_eq!(
            files::revision(&metadata),
            files::revision(&std::fs::metadata(&path).unwrap())
        );
        let changed = operation_plan_evidence(&state, &actions, &previews)
            .await
            .unwrap();
        admit_plan(&state, "operation", "move", &changed)
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn failed_worker_finalization_does_not_leave_recovery_fixing() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(
            Config::for_tests(dir.path()),
            crate::db::connect(dir.path()).await.unwrap(),
            SecretBox::open_or_create(dir.path()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sync_recovery(id,kind,target_id,state,created_at,updated_at)
             VALUES ('conflict:missing','conflict','missing','queued',?,?)",
        )
        .bind(db::now())
        .bind(db::now())
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_worker_result BEFORE UPDATE ON sync_recovery
             WHEN OLD.id='conflict:missing' AND OLD.state='fixing' AND NEW.code='internal'
             BEGIN SELECT RAISE(FAIL,'injected finalization error'); END",
        )
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_interrupted_reset BEFORE UPDATE ON sync_recovery
             WHEN NEW.code='recovery_worker_interrupted'
             BEGIN SELECT RAISE(FAIL,'injected reset error'); END",
        )
        .execute(&state.db)
        .await
        .unwrap();
        let worker = tokio::spawn(local_loop(state.clone()));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status: String = sqlx::query_scalar(
                    "SELECT state FROM sync_recovery WHERE id='conflict:missing'",
                )
                .fetch_one(&state.db)
                .await
                .unwrap();
                if status == "fixing" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("worker was not claimed");
        tokio::time::sleep(Duration::from_millis(150)).await;
        sqlx::query("DROP TRIGGER reject_interrupted_reset")
            .execute(&state.db)
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let state: (String, Option<String>) = sqlx::query_as(
                    "SELECT state,code FROM sync_recovery WHERE id='conflict:missing'",
                )
                .fetch_one(&state.db)
                .await
                .unwrap();
                if state.0 == "waiting" && state.1.as_deref() == Some("recovery_worker_interrupted")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "an interrupted worker remained stuck fixing"
        );
        sqlx::query("DROP TRIGGER reject_worker_result")
            .execute(&state.db)
            .await
            .unwrap();
        sqlx::query("UPDATE sync_recovery SET updated_at='2024-01-01T00:00:00Z' WHERE id='conflict:missing'")
            .execute(&state.db)
            .await
            .unwrap();
        let retried = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let (status, code): (String, Option<String>) = sqlx::query_as(
                    "SELECT state,code FROM sync_recovery WHERE id='conflict:missing'",
                )
                .fetch_one(&state.db)
                .await
                .unwrap();
                if status == "waiting" && code.as_deref() == Some("internal") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        worker.abort();
        assert!(retried.is_ok(), "the interrupted recovery was not retried");
    }
}
