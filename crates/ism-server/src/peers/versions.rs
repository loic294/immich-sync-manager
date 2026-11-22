//! Current, owner-scoped evidence for in-app version decisions.

use axum::http::header;
use axum::response::{IntoResponse, Response};
use ism_core::dto::{
    RecoveryFileVersion, RecoveryKind, RecoveryTarget, RecoveryVersion, RecoveryVersions,
};
use serde::Deserialize;

use super::{Share, admin, net};
use crate::error::{AppError, AppResult};
use crate::files;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct Query {
    pub kind: RecoveryKind,
    pub id: String,
    pub peer_id: Option<String>,
}

#[derive(Deserialize)]
pub struct PreviewQuery {
    #[serde(flatten)]
    pub target: Query,
    pub revision: String,
    pub path: String,
    pub source: bool,
}

impl Query {
    pub fn target(&self) -> RecoveryTarget {
        RecoveryTarget {
            kind: self.kind,
            id: self.id.clone(),
            peer_id: self.peer_id.clone(),
        }
    }
}

pub(super) struct Context {
    pub share: Share,
    pub peer_id: String,
    pub local_paths: Vec<String>,
    pub remote_path: String,
    pub remote_state: String,
    pub remote_files: Vec<(String, String, u64)>,
    pub remote_directories: Vec<String>,
}

fn role(state: &AppState, path: &str) -> String {
    ism_core::sidecar::companion_role(
        path.rsplit('/').next().unwrap_or(path),
        &state.sidecar_extensions(),
    )
    .map_or_else(|| "original".into(), |role| role.as_str().into())
}

pub(super) async fn local(
    state: &AppState,
    share: &Share,
    path: &str,
) -> AppResult<RecoveryVersion> {
    type FileEntry = (String, String, u64, String);
    let root = files::root(&state.db, &share.root_id).await?;
    root.ensure_available()?;
    let abs = root.abs(&share.root_rel(path)?)?;
    let base = path.to_owned();
    let entries = tokio::task::spawn_blocking(
        move || -> AppResult<(String, Vec<FileEntry>, Vec<String>)> {
            let meta = match std::fs::symlink_metadata(&abs) {
                Ok(meta) => meta,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(("absent".into(), Vec::new(), Vec::new()));
                }
                Err(error) => return Err(error.into()),
            };
            let folder = meta.is_dir();
            if !folder && !meta.is_file() {
                return Err(AppError::invalid_location(
                    "version evidence is not a regular file or directory",
                ));
            }
            let mut out = Vec::new();
            let mut directories = Vec::new();
            let paths = if folder {
                let entries = crate::ops::plan::folder_inventory(&abs)?;
                directories.push(base.clone());
                directories.extend(
                    entries
                        .iter()
                        .filter(|entry| entry.sha256.is_none())
                        .map(|entry| format!("{base}/{}", entry.name)),
                );
                directories.sort();
                entries
                    .into_iter()
                    .filter_map(|entry| entry.sha256.map(|hash| (entry.name, hash)))
                    .collect()
            } else {
                let hash = files::replacement_fingerprint(&abs)?
                    .ok_or_else(|| AppError::conflict("revision_changed", "file disappeared"))?
                    .1;
                vec![(String::new(), hash)]
            };
            for (name, expected) in paths {
                let file = if name.is_empty() {
                    abs.clone()
                } else {
                    abs.join(&name)
                };
                let before = std::fs::symlink_metadata(&file)?;
                let (revision, hash) = files::replacement_fingerprint(&file)?
                    .ok_or_else(|| AppError::conflict("revision_changed", "file disappeared"))?;
                if !before.is_file() || revision != files::revision(&before) || hash != expected {
                    return Err(AppError::conflict(
                        "revision_changed",
                        "version changed during verification",
                    ));
                }
                let path = if name.is_empty() {
                    base.clone()
                } else {
                    format!("{base}/{name}")
                };
                let modified =
                    chrono::DateTime::from_timestamp_nanos(files::mtime_ns(&before)).to_rfc3339();
                out.push((path, hash, before.len(), modified));
            }
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok((
                if folder { "folder" } else { "file" }.into(),
                out,
                directories,
            ))
        },
    )
    .await
    .map_err(AppError::internal)??;
    Ok(RecoveryVersion {
        instance: state.instance_name().await,
        path: path.into(),
        state: entries.0,
        files: entries
            .1
            .into_iter()
            .map(|(path, sha256, size, modified_at)| RecoveryFileVersion {
                role: role(state, &path),
                path,
                sha256,
                size: Some(size),
                modified_at: Some(modified_at),
            })
            .collect(),
        directories: entries.2,
    })
}

pub async fn get(state: &AppState, target: &RecoveryTarget) -> AppResult<RecoveryVersions> {
    if let Some(peer_id) = &target.peer_id {
        // Only known work owned by this receiver can be forwarded.
        if super::recovery::outgoing(state, peer_id, target.kind, &target.id)
            .await?
            .is_none()
        {
            return Err(AppError::not_found("receiver recovery target not found"));
        }
        let peer = net::peer(state, peer_id).await?;
        let kind = serde_json::to_value(target.kind)?;
        let kind = kind
            .as_str()
            .ok_or_else(|| AppError::internal("invalid recovery kind"))?;
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [("kind", kind), ("id", target.id.as_str())],
        )
        .map_err(AppError::internal)?;
        return net::get_json(
            state,
            &peer,
            &format!(
                "/api/peer/v1/recovery/versions?{}",
                url.query().unwrap_or_default()
            ),
        )
        .await
        .map_err(|error| AppError::conflict("peer_evidence_unavailable", error));
    }
    if target.kind != RecoveryKind::Conflict {
        return Err(AppError::bad_request(
            "version_decision_required",
            "version evidence belongs to a conflict",
        ));
    }
    let context = admin::version_context(state, &target.id).await?;
    let peer = net::peer(state, &context.peer_id).await?;
    let mut receiving = Vec::new();
    for path in context.local_paths {
        receiving.push(local(state, &context.share, &path).await?);
    }
    let mut directories = context.remote_directories;
    directories.sort();
    directories.dedup();
    let source = RecoveryVersion {
        instance: peer.name,
        path: context.remote_path,
        state: context.remote_state,
        files: context
            .remote_files
            .into_iter()
            .map(|(path, sha256, size)| RecoveryFileVersion {
                role: role(state, &path),
                path,
                sha256,
                size: Some(size),
                modified_at: None,
            })
            .collect(),
        directories,
    };
    let revision = crate::crypto::sha256_hex(&serde_json::to_vec(&(&receiving, &source))?);
    let view = RecoveryVersions {
        share: context.share.name.clone(),
        revision,
        receiving,
        source,
    };
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("DELETE FROM recovery_version_snapshots WHERE created_at<?")
        .bind((chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO recovery_version_snapshots(target_id,revision,share_id,peer_id,view_json,created_at)
         VALUES (?,?,?,?,?,?) ON CONFLICT(target_id,revision) DO UPDATE SET created_at=excluded.created_at",
    ).bind(&target.id).bind(&view.revision).bind(&context.share.id).bind(&context.peer_id)
    .bind(serde_json::to_string(&view)?).bind(crate::db::now()).execute(&mut *tx).await?;
    sqlx::query(
        "DELETE FROM recovery_version_snapshots WHERE target_id=? AND revision NOT IN
            (SELECT revision FROM recovery_version_snapshots WHERE target_id=? ORDER BY created_at DESC LIMIT 4)",
    ).bind(&target.id).bind(&target.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(view)
}

type MovedVersion<'a> = (&'a str, &'a str, &'a [(String, String, u64)], &'a [String]);

pub(super) async fn validate_reviewed(
    state: &AppState,
    id: &str,
    revision: &str,
    remote: Option<&super::recovery::FileEvidence>,
    moved: Option<MovedVersion<'_>>,
) -> AppResult<()> {
    let snapshot: Option<(String, String, String)> = sqlx::query_as(
        "SELECT share_id,peer_id,view_json FROM recovery_version_snapshots
         WHERE target_id=? AND revision=? AND created_at>=?",
    )
    .bind(id)
    .bind(revision)
    .bind((chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
    .fetch_optional(&state.db)
    .await?;
    let (share_id, peer_id, json) = snapshot
        .ok_or_else(|| AppError::conflict("version_conflict", "review current versions again"))?;
    let view: RecoveryVersions = serde_json::from_str(&json)?;
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == share_id && share.includes(&peer_id))
        .ok_or_else(|| {
            AppError::conflict("peer_share_changed", "the reviewed share is unavailable")
        })?;
    for expected in &view.receiving {
        if serde_json::to_value(local(state, &share, &expected.path).await?)?
            != serde_json::to_value(expected)?
        {
            return Err(AppError::conflict(
                "version_conflict",
                "receiving files changed after review",
            ));
        }
    }
    if let Some(remote) = remote {
        let status = if remote.deleted {
            "deleted"
        } else if remote.sha256.is_some() {
            "file"
        } else {
            "absent"
        };
        let matches = remote.inventory.is_none()
            && view.source.path == remote.path
            && view.source.state == status
            && match (&remote.sha256, remote.size, view.source.files.as_slice()) {
                (Some(hash), Some(size), [file]) => {
                    file.path == remote.path && file.sha256 == *hash && file.size == Some(size)
                }
                (None, None, []) => true,
                _ => false,
            };
        if !matches {
            return Err(AppError::conflict(
                "version_conflict",
                "source files changed after review",
            ));
        }
    }
    if let Some((location, state, files, directories)) = moved {
        let mut directories = directories.to_vec();
        directories.sort();
        directories.dedup();
        let matches = view.source.path == location
            && view.source.state == state
            && view.source.directories == directories
            && view.source.files.len() == files.len()
            && files.iter().all(|(path, hash, size)| {
                view.source.files.iter().any(|reviewed| {
                    reviewed.path == *path
                        && reviewed.sha256 == *hash
                        && reviewed.size == Some(*size)
                })
            });
        if !matches {
            return Err(AppError::conflict(
                "version_conflict",
                "source files changed after review",
            ));
        }
    }
    Ok(())
}

pub async fn preview(state: &AppState, query: &PreviewQuery) -> AppResult<Response> {
    if let Some(peer_id) = &query.target.peer_id {
        if super::recovery::outgoing(state, peer_id, query.target.kind, &query.target.id)
            .await?
            .is_none()
        {
            return Err(AppError::not_found("receiver recovery target not found"));
        }
        let peer = net::peer(state, peer_id).await?;
        let source = query.source.to_string();
        let kind = serde_json::to_value(query.target.kind)?;
        let kind = kind
            .as_str()
            .ok_or_else(|| AppError::internal("invalid recovery kind"))?;
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [
                ("kind", kind),
                ("id", query.target.id.as_str()),
                ("revision", query.revision.as_str()),
                ("path", query.path.as_str()),
                ("source", source.as_str()),
            ],
        )
        .map_err(AppError::internal)?;
        let response = net::request(
            state,
            &peer.url,
            &peer.secret,
            reqwest::Method::GET,
            &format!(
                "/api/peer/v1/recovery/preview?{}",
                url.query().unwrap_or_default()
            ),
            None,
        )
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|error| AppError::conflict("preview_unavailable", net::net_error(&error)))?;
        return crate::routes::activity::image_response(response).await;
    }
    if query.target.kind != RecoveryKind::Conflict {
        return Err(AppError::not_found("version preview not found"));
    }
    let snapshot: Option<(String, String, String)> = sqlx::query_as(
        "SELECT share_id,peer_id,view_json FROM recovery_version_snapshots
         WHERE target_id=? AND revision=? AND created_at>=?",
    )
    .bind(&query.target.id)
    .bind(&query.revision)
    .bind((chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339())
    .fetch_optional(&state.db)
    .await?;
    let (share_id, peer_id, json) = snapshot
        .ok_or_else(|| AppError::conflict("preview_expired", "refresh current versions"))?;
    let view: RecoveryVersions = serde_json::from_str(&json)?;
    let selected = if query.source {
        view.source
            .files
            .iter()
            .find(|file| file.path == query.path)
    } else {
        view.receiving
            .iter()
            .flat_map(|version| &version.files)
            .find(|file| file.path == query.path)
    }
    .ok_or_else(|| AppError::not_found("file is outside this version decision"))?;
    let mime = match query
        .path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "webp" => "image/webp",
        "avif" => "image/avif",
        _ => return Err(AppError::not_found("this file has no image preview")),
    };
    let share = super::shares(state)
        .await?
        .into_iter()
        .find(|share| share.id == share_id && share.includes(&peer_id))
        .ok_or_else(|| {
            AppError::conflict("peer_share_changed", "the preview share is unavailable")
        })?;
    let bytes = if query.source {
        let peer = net::peer(state, &peer_id).await?;
        let url = reqwest::Url::parse_with_params(
            "http://ism.example",
            [
                ("share", share.name.as_str()),
                ("path", query.path.as_str()),
                ("sha256", selected.sha256.as_str()),
            ],
        )
        .map_err(AppError::internal)?;
        let mut response = net::request(
            state,
            &peer.url,
            &peer.secret,
            reqwest::Method::GET,
            &format!("/api/peer/v1/content?{}", url.query().unwrap_or_default()),
            None,
        )
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|error| AppError::conflict("preview_unavailable", net::net_error(&error)))?;
        if !response.status().is_success() {
            return Err(AppError::conflict(
                "preview_unavailable",
                format!("Peer returned HTTP {}", response.status()),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(AppError::internal)? {
            if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                return Err(AppError::conflict(
                    "preview_unavailable",
                    "image exceeds the bounded preview size",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        bytes
    } else {
        let root = files::root(&state.db, &share.root_id).await?;
        root.ensure_available()?;
        let path = root.abs(&share.root_rel(&query.path)?)?;
        let file = tokio::fs::File::open(&path).await?;
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        file.take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(AppError::conflict(
                "preview_unavailable",
                "image exceeds the bounded preview size",
            ));
        }
        bytes
    };
    if crate::crypto::sha256_hex(&bytes) != selected.sha256 {
        return Err(AppError::conflict(
            "preview_changed",
            "refresh current versions",
        ));
    }
    Ok((
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, "private, no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        bytes,
    )
        .into_response())
}
