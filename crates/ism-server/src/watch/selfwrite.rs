//! Self-write records: paths ISM is about to change, so the watcher does not report its own
//! moves, trash, restores, uploads and peer copies as outside changes.

use std::path::{Path, PathBuf};

use super::{Change, ChangeKind};
use crate::error::AppResult;
use crate::state::AppState;

/// One expected change. Files carry their final size and modification time when known; a tree
/// covers a folder and everything below it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expected {
    pub path: PathBuf,
    pub is_tree: bool,
    pub size: Option<i64>,
    pub mtime_ns: Option<i64>,
}

impl Expected {
    pub fn tree(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            is_tree: true,
            size: None,
            mtime_ns: None,
        }
    }

    pub fn file(path: impl Into<PathBuf>, size: Option<i64>, mtime_ns: Option<i64>) -> Self {
        Self {
            path: path.into(),
            is_tree: false,
            size,
            mtime_ns,
        }
    }

    /// `target` will receive what is at `source` now (a rename keeps size and mtime).
    pub fn moved(source: &Path, target: &Path) -> Vec<Self> {
        match std::fs::symlink_metadata(source) {
            Ok(meta) if meta.is_dir() => vec![Self::tree(source), Self::tree(target)],
            Ok(meta) => {
                let size = i64::try_from(meta.len()).ok();
                let mtime = Some(crate::files::mtime_ns(&meta));
                vec![
                    Self::file(source, size, mtime),
                    Self::file(target, size, mtime),
                ]
            }
            Err(_) => vec![Self::tree(source), Self::tree(target)],
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub path: String,
    pub is_tree: bool,
    pub size: Option<i64>,
    pub mtime_ns: Option<i64>,
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Records expected changes before ISM touches the files. They outlive at least one full
/// polling pass of every mapping.
pub async fn expect_writes(state: &AppState, items: &[Expected]) -> AppResult<()> {
    if items.is_empty() {
        return Ok(());
    }
    let longest: Option<i64> =
        sqlx::query_scalar("SELECT MAX(poll_interval_secs) FROM watch_settings")
            .fetch_one(&state.db)
            .await?;
    let ttl_ms = i64::try_from(state.config.timing.self_write_ttl.as_millis())
        .unwrap_or(i64::MAX / 4)
        .max(
            2 * 1000
                * longest
                    .unwrap_or(super::DEFAULT_POLL_SECS)
                    .max(super::DEFAULT_POLL_SECS),
        );
    let expires = now_ms().saturating_add(ttl_ms);
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    for item in items {
        sqlx::query(
            "INSERT INTO self_writes (path, is_tree, size, mtime_ns, expires_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(item.path.to_string_lossy().into_owned())
        .bind(item.is_tree)
        .bind(item.size)
        .bind(item.mtime_ns)
        .bind(expires)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Unexpired records; expired ones are pruned.
pub(crate) async fn load(state: &AppState, now_ms: i64) -> AppResult<Vec<Record>> {
    sqlx::query("DELETE FROM self_writes WHERE expires_at <= ?")
        .bind(now_ms)
        .execute(&state.db)
        .await?;
    let rows: Vec<(String, bool, Option<i64>, Option<i64>)> =
        sqlx::query_as("SELECT path, is_tree, size, mtime_ns FROM self_writes")
            .fetch_all(&state.db)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(path, is_tree, size, mtime_ns)| Record {
            path,
            is_tree,
            size,
            mtime_ns,
        })
        .collect())
}

/// Whether a detected change at `abs` is one ISM announced.
pub(crate) fn is_self(records: &[Record], abs: &str, change: &Change) -> bool {
    records.iter().any(|r| {
        if r.is_tree {
            return abs == r.path
                || abs
                    .strip_prefix(r.path.as_str())
                    .is_some_and(|rest| rest.starts_with('/'));
        }
        if abs != r.path {
            return false;
        }
        change.kind == ChangeKind::Removed
            || (r.size.is_none_or(|s| Some(s) == change.size)
                && r.mtime_ns.is_none_or(|m| Some(m) == change.mtime_ns))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(kind: ChangeKind, size: i64, mtime: i64) -> Change {
        Change {
            rel: "a.jpg".into(),
            kind,
            size: Some(size),
            mtime_ns: Some(mtime),
        }
    }

    #[test]
    fn matches_announced_writes_only() {
        let records = vec![
            Record {
                path: "/m/a.jpg".into(),
                is_tree: false,
                size: Some(10),
                mtime_ns: Some(5),
            },
            Record {
                path: "/m/trip".into(),
                is_tree: true,
                size: None,
                mtime_ns: None,
            },
        ];
        assert!(is_self(
            &records,
            "/m/a.jpg",
            &change(ChangeKind::Added, 10, 5)
        ));
        assert!(
            !is_self(&records, "/m/a.jpg", &change(ChangeKind::Changed, 11, 5)),
            "a different size is an outside change"
        );
        assert!(is_self(
            &records,
            "/m/a.jpg",
            &change(ChangeKind::Removed, 0, 0)
        ));
        assert!(is_self(
            &records,
            "/m/trip/x/b.jpg",
            &change(ChangeKind::Added, 1, 1)
        ));
        assert!(!is_self(
            &records,
            "/m/trips/b.jpg",
            &change(ChangeKind::Added, 1, 1)
        ));
        assert!(!is_self(
            &records,
            "/m/b.jpg",
            &change(ChangeKind::Added, 10, 5)
        ));
    }
}
