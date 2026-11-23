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

const SNAPSHOT_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const SNAPSHOTS_PER_TARGET: usize = 4;

#[derive(Clone)]
pub struct Snapshot {
    pub share_id: String,
    pub peer_id: String,
    pub view: RecoveryVersions,
    pub created: std::time::Instant,
}

fn remember(
    state: &AppState,
    target: &str,
    share_id: &str,
    peer_id: &str,
    view: &RecoveryVersions,
) {
    let mut snapshots = state
        .version_snapshots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshots.retain(|_, snapshot| snapshot.created.elapsed() < SNAPSHOT_TTL);
    snapshots.insert(
        (target.to_owned(), view.revision.clone()),
        Snapshot {
            share_id: share_id.to_owned(),
            peer_id: peer_id.to_owned(),
            view: view.clone(),
            created: std::time::Instant::now(),
        },
    );
    let mut revisions: Vec<_> = snapshots
        .iter()
        .filter(|((id, _), _)| id == target)
        .map(|((_, revision), snapshot)| (snapshot.created, revision.clone()))
        .collect();
    revisions.sort_by(|a, b| b.0.cmp(&a.0));
    for (_, revision) in revisions.into_iter().skip(SNAPSHOTS_PER_TARGET) {
        snapshots.remove(&(target.to_owned(), revision));
    }
}

fn recall(state: &AppState, target: &str, revision: &str) -> Option<Snapshot> {
    state
        .version_snapshots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&(target.to_owned(), revision.to_owned()))
        .filter(|snapshot| snapshot.created.elapsed() < SNAPSHOT_TTL)
        .cloned()
}

#[cfg(test)]
pub(super) fn expire(state: &AppState, target: &str) {
    state
        .version_snapshots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|(id, _), _| id != target);
}

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
    /// Path, checksum, size and modification time of each source file.
    pub remote_files: Vec<(String, String, u64, Option<String>)>,
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
                        .filter(|entry| entry.sha256.is_none() && !entry.name.is_empty())
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

/// The comparison must answer before a reverse proxy in front of this instance times out.
const COMPARISON_DEADLINE: std::time::Duration = std::time::Duration::from_secs(50);

/// [`get`] bounded by a deadline; a slow peer becomes a retryable error instead of a proxy 504.
pub async fn get_bounded(state: &AppState, target: &RecoveryTarget) -> AppResult<RecoveryVersions> {
    within(COMPARISON_DEADLINE, get(state, target)).await
}

async fn within<T>(
    deadline: std::time::Duration,
    work: impl std::future::Future<Output = AppResult<T>>,
) -> AppResult<T> {
    tokio::time::timeout(deadline, work)
        .await
        .unwrap_or_else(|_| {
            Err(AppError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "peer_busy",
                "The other instance is busy. Try again in a moment.",
            ))
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
            .map(|(path, sha256, size, modified_at)| RecoveryFileVersion {
                role: role(state, &path),
                path,
                sha256,
                size: Some(size),
                modified_at,
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
    remember(
        state,
        &target.id,
        &context.share.id,
        &context.peer_id,
        &view,
    );
    Ok(view)
}

type MovedVersion<'a> = (&'a str, &'a str, &'a [(String, String, u64)], &'a [String]);

pub(super) async fn validate_reviewed(
    state: &AppState,
    id: &str,
    revision: &str,
    remote: Option<&super::recovery::FileEvidence>,
    moved: Option<MovedVersion<'_>>,
) -> AppResult<Vec<RecoveryVersion>> {
    let Snapshot {
        share_id,
        peer_id,
        view,
        ..
    } = recall(state, id, revision)
        .ok_or_else(|| AppError::conflict("version_conflict", "review current versions again"))?;
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
    Ok(view.receiving)
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
    let Snapshot {
        share_id,
        peer_id,
        view,
        ..
    } = recall(state, &query.target.id, &query.revision)
        .ok_or_else(|| AppError::conflict("preview_expired", "refresh current versions"))?;
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

#[cfg(test)]
mod deadline_tests {
    #[tokio::test]
    async fn slow_comparison_becomes_retryable_instead_of_hanging() {
        let error = super::within(
            std::time::Duration::from_millis(20),
            std::future::pending::<crate::error::AppResult<()>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code(), "peer_busy");
    }
}
