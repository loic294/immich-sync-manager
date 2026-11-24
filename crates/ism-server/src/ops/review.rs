//! Local, immutable folder review manifests. These are evidence, never mutation grants.
//!
//! This protocol layer deliberately does not change legacy vector-based peer contracts.
//! Callers must still authorize the managed action and revalidate inside maintenance.

use std::path::PathBuf;
use std::sync::Arc;

use ism_core::dto::{ClientAction, Location};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tokio::sync::{Semaphore, mpsc};

use super::tree;
use crate::auth::Principal;
use crate::error::{AppError, AppResult};
use crate::state::AppState;
use crate::{crypto, db, files};

const BATCH: usize = 128;
pub const MAX_PAGE: usize = 500;

#[derive(Debug, Clone)]
pub struct Manifest {
    pub id: String,
    pub revision: String,
    pub entry_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Relative to the reviewed folder; the empty name is the folder itself.
    pub name: String,
    /// None represents a directory, including empty directories.
    pub sha256: Option<String>,
    pub size: Option<i64>,
}

#[derive(Debug)]
pub struct Page {
    pub manifest: Manifest,
    pub entries: Vec<Entry>,
    pub next_after: Option<String>,
}

fn changed() -> AppError {
    AppError::conflict(
        "review_changed",
        "the folder review is unavailable or changed",
    )
}

fn authorize(principal: &Principal, location: &Location) -> AppResult<()> {
    if matches!(principal, Principal::Peer { .. }) {
        return Err(AppError::forbidden(
            "a sender cannot review or authorize receiver-owned folder data",
        ));
    }
    principal.require(ClientAction::Read, &location.root_id)
}

async fn resolve(
    state: &AppState,
    principal: &Principal,
    location: &Location,
) -> AppResult<PathBuf> {
    authorize(principal, location)?;
    let root = files::root(&state.db, &location.root_id).await?;
    root.ensure_available()?;
    root.abs(&files::parse_rel(&location.path)?)
}

struct Scan {
    entries: mpsc::Receiver<tree::Entry>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn scan(path: PathBuf, limit: Arc<Semaphore>) -> AppResult<Scan> {
    let permit = limit.acquire_owned().await.map_err(AppError::internal)?;
    let (send, entries) = mpsc::channel(BATCH);
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        tree::walk(&path, None, |entry| {
            send.blocking_send(entry).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Interrupted, "review scan cancelled")
            })
        })
    });
    Ok(Scan { entries, task })
}

fn persisted(entry: tree::Entry) -> AppResult<Entry> {
    Ok(Entry {
        name: entry.name,
        sha256: entry.sha256,
        size: entry
            .size
            .map(i64::try_from)
            .transpose()
            .map_err(AppError::internal)?,
    })
}

type EntryRow = (String, Option<String>, Option<i64>);

fn entry(row: EntryRow) -> Entry {
    Entry {
        name: row.0,
        sha256: row.1,
        size: row.2,
    }
}

async fn rows(
    pool: &SqlitePool,
    id: &str,
    after: Option<&str>,
    limit: usize,
) -> AppResult<Vec<Entry>> {
    let rows: Vec<EntryRow> = sqlx::query_as(
        "SELECT name,sha256,size FROM folder_review_entries
         WHERE manifest_id=? AND (? IS NULL OR name > ? COLLATE BINARY)
         ORDER BY name COLLATE BINARY LIMIT ?",
    )
    .bind(id)
    .bind(after)
    .bind(after)
    .bind(i64::try_from(limit).map_err(AppError::internal)?)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(entry).collect())
}

async fn digest(pool: &SqlitePool, id: &str, location: &Location) -> AppResult<String> {
    let mut hash = Sha256::new();
    hash.update(b"ism-folder-review-v1\0");
    let scope = serde_json::to_vec(location)?;
    hash.update((scope.len() as u64).to_be_bytes());
    hash.update(scope);
    let mut after = None;
    loop {
        let entries = rows(pool, id, after.as_deref(), BATCH).await?;
        if entries.is_empty() {
            break;
        }
        for item in &entries {
            let bytes = serde_json::to_vec(&(&item.name, &item.sha256, item.size))?;
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        after = entries.last().map(|item| item.name.clone());
    }
    Ok(hex::encode(hash.finalize()))
}

async fn verify_tree(
    pool: &SqlitePool,
    id: &str,
    count: i64,
    path: PathBuf,
    limit: Arc<Semaphore>,
) -> AppResult<()> {
    let mut scan = scan(path, limit).await?;
    let mut actual_count = 0i64;
    while let Some(item) = scan.entries.recv().await {
        let actual = persisted(item)?;
        let expected: Option<EntryRow> = sqlx::query_as(
            "SELECT name,sha256,size FROM folder_review_entries WHERE manifest_id=? AND name=?",
        )
        .bind(id)
        .bind(&actual.name)
        .fetch_optional(pool)
        .await?;
        if expected.map(entry).as_ref() != Some(&actual) {
            return Err(changed());
        }
        actual_count = actual_count.checked_add(1).ok_or_else(changed)?;
    }
    scan.task.await.map_err(AppError::internal)??;
    if actual_count != count {
        return Err(changed());
    }
    Ok(())
}

async fn build(
    pool: &SqlitePool,
    owner: &str,
    location: &Location,
    path: PathBuf,
    limit: Arc<Semaphore>,
) -> AppResult<Manifest> {
    let id = crypto::random_secret(24);
    sqlx::query("DELETE FROM folder_review_manifests WHERE expires_at <= ?")
        .bind(db::now())
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO folder_review_manifests(id,owner,root_id,path,state,expires_at)
         VALUES (?,?,?,?,'building',?)",
    )
    .bind(&id)
    .bind(owner)
    .bind(&location.root_id)
    .bind(&location.path)
    .bind(db::ts(chrono::Utc::now() + chrono::Duration::hours(24)))
    .execute(pool)
    .await?;
    let result = build_entries(pool, &id, location, path, limit).await;
    if result.is_err()
        && let Err(error) = sqlx::query("DELETE FROM folder_review_manifests WHERE id=?")
            .bind(&id)
            .execute(pool)
            .await
    {
        tracing::error!(error = %error, "could not remove incomplete folder review");
    }
    result
}

async fn build_entries(
    pool: &SqlitePool,
    id: &str,
    location: &Location,
    path: PathBuf,
    limit: Arc<Semaphore>,
) -> AppResult<Manifest> {
    let mut scan = scan(path.clone(), limit.clone()).await?;
    let mut count = 0i64;
    loop {
        let mut batch = Vec::with_capacity(BATCH);
        while batch.len() < BATCH {
            let Some(item) = scan.entries.recv().await else {
                break;
            };
            batch.push(persisted(item)?);
        }
        if batch.is_empty() {
            break;
        }
        let mut tx = pool.begin().await?;
        for item in batch {
            sqlx::query(
                "INSERT INTO folder_review_entries(manifest_id,name,sha256,size) VALUES (?,?,?,?)",
            )
            .bind(id)
            .bind(item.name)
            .bind(item.sha256)
            .bind(item.size)
            .execute(&mut *tx)
            .await?;
            count = count.checked_add(1).ok_or_else(changed)?;
        }
        tx.commit().await?;
    }
    scan.task.await.map_err(AppError::internal)??;
    verify_tree(pool, id, count, path, limit).await?;
    let revision = digest(pool, id, location).await?;
    let published = sqlx::query(
        "UPDATE folder_review_manifests SET state='ready',revision=?,entry_count=?
         WHERE id=? AND state='building' AND expires_at > ?",
    )
    .bind(&revision)
    .bind(count)
    .bind(id)
    .bind(db::now())
    .execute(pool)
    .await?;
    if published.rows_affected() != 1 {
        return Err(changed());
    }
    Ok(Manifest {
        id: id.into(),
        revision,
        entry_count: count,
    })
}

/// Capture twice before publication; neither incomplete nor interrupted scans are readable.
pub async fn capture(
    state: &AppState,
    principal: &Principal,
    location: &Location,
) -> AppResult<Manifest> {
    let path = resolve(state, principal, location).await?;
    build(
        &state.db,
        &principal.key(),
        location,
        path,
        state.version_scan_limit.clone(),
    )
    .await
}

async fn recall(
    pool: &SqlitePool,
    owner: &str,
    location: &Location,
    id: &str,
    revision: &str,
) -> AppResult<Manifest> {
    let count: Option<i64> = sqlx::query_scalar(
        "SELECT entry_count FROM folder_review_manifests
         WHERE id=? AND owner=? AND root_id=? AND path=? AND revision=?
         AND state='ready' AND expires_at > ?",
    )
    .bind(id)
    .bind(owner)
    .bind(&location.root_id)
    .bind(&location.path)
    .bind(revision)
    .bind(db::now())
    .fetch_optional(pool)
    .await?;
    Ok(Manifest {
        id: id.into(),
        revision: revision.into(),
        entry_count: count.ok_or_else(changed)?,
    })
}

pub async fn page(
    state: &AppState,
    principal: &Principal,
    location: &Location,
    id: &str,
    revision: &str,
    after: Option<&str>,
    limit: usize,
) -> AppResult<Page> {
    resolve(state, principal, location).await?;
    read_page(
        &state.db,
        &principal.key(),
        location,
        id,
        revision,
        after,
        limit,
    )
    .await
}

async fn read_page(
    pool: &SqlitePool,
    owner: &str,
    location: &Location,
    id: &str,
    revision: &str,
    after: Option<&str>,
    limit: usize,
) -> AppResult<Page> {
    if !(1..=MAX_PAGE).contains(&limit) {
        return Err(AppError::bad_request(
            "bad_review_page_size",
            "folder review page size must be between 1 and 500",
        ));
    }
    let manifest = recall(pool, owner, location, id, revision).await?;
    let mut entries = rows(pool, id, after, limit + 1).await?;
    let next_after = if entries.len() > limit {
        entries.truncate(limit);
        entries.last().map(|item| item.name.clone())
    } else {
        None
    };
    // Expiry/cleanup racing this page must fail closed, not return a partial tree.
    recall(pool, owner, location, id, revision).await?;
    Ok(Page {
        manifest,
        entries,
        next_after,
    })
}

/// Rehash the entire live tree, not just the displayed page. Call again immediately
/// before a managed mutation while holding the maintenance barrier.
pub async fn verify(
    state: &AppState,
    principal: &Principal,
    location: &Location,
    id: &str,
    revision: &str,
) -> AppResult<()> {
    let path = resolve(state, principal, location).await?;
    let manifest = recall(&state.db, &principal.key(), location, id, revision).await?;
    verify_tree(
        &state.db,
        id,
        manifest.entry_count,
        path,
        state.version_scan_limit.clone(),
    )
    .await?;
    recall(&state.db, &principal.key(), location, id, revision).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
