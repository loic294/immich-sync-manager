//! Polling passes: walk a mapped folder (or part of it), diff against the persisted snapshot.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use ism_core::file_settings::JunkRules;

use super::{Change, ChangeKind, ignore};
use crate::error::AppResult;
use crate::state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub size: i64,
    pub mtime_ns: i64,
    pub inode: Option<i64>,
}

/// Root-relative path (with `/` separators) to file state.
pub type Tree = BTreeMap<String, Entry>;

fn entry(meta: &std::fs::Metadata) -> Entry {
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        i64::try_from(meta.ino()).ok()
    };
    #[cfg(not(unix))]
    let inode = None;
    Entry {
        size: i64::try_from(meta.len()).unwrap_or(i64::MAX),
        mtime_ns: crate::files::mtime_ns(meta),
        inode,
    }
}

fn rel_of(base: &Path, path: &Path) -> Option<String> {
    let rel = path.strip_prefix(base).ok()?;
    let parts: Option<Vec<&str>> = rel.components().map(|c| c.as_os_str().to_str()).collect();
    Some(parts?.join("/"))
}

/// Files under `base/scope` (the whole folder without a scope). Symlinks are not followed;
/// ISM-ignored paths and `junk` names are skipped. A missing scope is empty; any other error
/// fails the pass.
#[cfg(test)]
pub fn walk(base: &Path, scope: Option<&str>, junk: &JunkRules) -> io::Result<Tree> {
    walk_reporting(base, scope, junk, |_| {})
}

pub fn walk_reporting(
    base: &Path,
    scope: Option<&str>,
    junk: &JunkRules,
    mut progress: impl FnMut(u64),
) -> io::Result<Tree> {
    let skip = |rel: &str| ignore::ignored(rel) || junk.path_is_junk(rel);
    let start = scope.map_or_else(|| base.to_path_buf(), |s| base.join(s));
    let mut tree = Tree::new();
    match std::fs::symlink_metadata(&start) {
        Err(e) if e.kind() == io::ErrorKind::NotFound && scope.is_some() => return Ok(tree),
        Err(e) => return Err(e),
        Ok(meta) if meta.is_file() => {
            if let Some(rel) = scope.filter(|r| !skip(r)) {
                tree.insert(rel.to_owned(), entry(&meta));
                progress(tree.len() as u64);
            }
            return Ok(tree);
        }
        Ok(meta) if !meta.is_dir() => return Ok(tree),
        Ok(_) => {}
    }
    let walker = walkdir::WalkDir::new(&start)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| e.depth() == 0 || e.file_name().to_str().is_some_and(|n| !skip(n)));
    for item in walker {
        let item = match item {
            Ok(item) => item,
            // Removed while walking: the next pass sees it.
            Err(e)
                if e.io_error()
                    .is_some_and(|e| e.kind() == io::ErrorKind::NotFound) =>
            {
                continue;
            }
            Err(e) => return Err(io::Error::other(e.to_string())),
        };
        if !item.file_type().is_file() {
            continue;
        }
        let Some(rel) = rel_of(base, item.path()) else {
            continue;
        };
        if skip(&rel) {
            continue;
        }
        match item.metadata() {
            Ok(meta) => {
                tree.insert(rel, entry(&meta));
                progress(tree.len() as u64);
            }
            Err(e)
                if e.io_error()
                    .is_some_and(|e| e.kind() == io::ErrorKind::NotFound) => {}
            Err(e) => return Err(io::Error::other(e.to_string())),
        }
    }
    Ok(tree)
}

/// Added, changed and removed files between two trees.
pub fn diff(old: &Tree, new: &Tree) -> Vec<Change> {
    let mut out = Vec::new();
    for (rel, n) in new {
        let kind = match old.get(rel) {
            None => ChangeKind::Added,
            Some(o)
                if o.size != n.size
                    || o.mtime_ns != n.mtime_ns
                    || (o.inode.is_some() && n.inode.is_some() && o.inode != n.inode) =>
            {
                ChangeKind::Changed
            }
            Some(_) => continue,
        };
        out.push(Change {
            rel: rel.clone(),
            kind,
            size: Some(n.size),
            mtime_ns: Some(n.mtime_ns),
        });
    }
    for (rel, o) in old {
        if !new.contains_key(rel) {
            out.push(Change {
                rel: rel.clone(),
                kind: ChangeKind::Removed,
                size: Some(o.size),
                mtime_ns: Some(o.mtime_ns),
            });
        }
    }
    out
}

fn scope_bounds(scope: Option<&str>) -> (String, String, String) {
    match scope {
        // `/` + 1 is `0`: the range holds exactly the paths below the scope.
        Some(s) => (s.to_owned(), format!("{s}/"), format!("{s}0")),
        None => (String::new(), String::new(), "\u{10FFFF}".to_owned()),
    }
}

/// The snapshot part a pass over `scope` compares against.
pub async fn load(state: &AppState, root_id: &str, scope: Option<&str>) -> AppResult<Tree> {
    let (exact, lo, hi) = scope_bounds(scope);
    let rows: Vec<(String, i64, i64, Option<i64>)> = sqlx::query_as(
        "SELECT rel_path, size, mtime_ns, inode FROM watch_snapshot
         WHERE root_id = ? AND rel_path = ?
         UNION ALL
         SELECT rel_path, size, mtime_ns, inode FROM watch_snapshot
         WHERE root_id = ? AND rel_path >= ? AND rel_path < ? AND rel_path != ?",
    )
    .bind(root_id)
    .bind(exact)
    .bind(root_id)
    .bind(lo)
    .bind(hi)
    .bind(scope.unwrap_or(""))
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(rel, size, mtime_ns, inode)| {
            (
                rel,
                Entry {
                    size,
                    mtime_ns,
                    inode,
                },
            )
        })
        .collect())
}

/// Stores the result of a pass.
pub async fn store(
    state: &AppState,
    root_id: &str,
    new: &Tree,
    changes: &[Change],
) -> AppResult<()> {
    let mut tx = state.db.begin_with("BEGIN IMMEDIATE").await?;
    for c in changes {
        match new.get(&c.rel) {
            Some(e) => {
                sqlx::query(
                    "INSERT INTO watch_snapshot (root_id, rel_path, size, mtime_ns, inode) VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(root_id, rel_path) DO UPDATE SET size = excluded.size,
                       mtime_ns = excluded.mtime_ns, inode = excluded.inode",
                )
                .bind(root_id)
                .bind(&c.rel)
                .bind(e.size)
                .bind(e.mtime_ns)
                .bind(e.inode)
                .execute(&mut *tx)
                .await?;
            }
            None => {
                sqlx::query("DELETE FROM watch_snapshot WHERE root_id = ? AND rel_path = ?")
                    .bind(root_id)
                    .bind(&c.rel)
                    .execute(&mut *tx)
                    .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn walks_and_diffs() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::fs::create_dir_all(base.join("trip/.ism-trash")).unwrap();
        std::fs::write(base.join(".ism-root"), "r").unwrap();
        std::fs::write(base.join("trip/a.jpg"), "aa").unwrap();
        std::fs::write(base.join("trip/b.jpg"), "bb").unwrap();
        std::fs::write(base.join("trip/.ism-trash/x.jpg"), "x").unwrap();
        std::fs::write(base.join("trip/c.jpg.part"), "c").unwrap();
        std::fs::write(base.join("trip/Thumbs.db"), "t").unwrap();
        std::fs::write(base.join("trip/._a.jpg"), "r").unwrap();
        let junk = JunkRules::default();
        let old = walk(base, None, &junk).unwrap();
        assert_eq!(old.keys().collect::<Vec<_>>(), ["trip/a.jpg", "trip/b.jpg"]);

        std::fs::write(base.join("trip/a.jpg"), "changed").unwrap();
        std::fs::remove_file(base.join("trip/b.jpg")).unwrap();
        std::fs::write(base.join("new.jpg"), "n").unwrap();
        let new = walk(base, None, &junk).unwrap();
        let mut changes: Vec<_> = diff(&old, &new)
            .into_iter()
            .map(|c| (c.rel, c.kind))
            .collect();
        changes.sort();
        assert_eq!(
            changes,
            [
                ("new.jpg".to_owned(), ChangeKind::Added),
                ("trip/a.jpg".to_owned(), ChangeKind::Changed),
                ("trip/b.jpg".to_owned(), ChangeKind::Removed),
            ]
        );
        assert!(diff(&new, &new).is_empty());
        assert_eq!(walk(base, Some("trip"), &junk).unwrap().len(), 1);
        assert_eq!(walk(base, Some("trip/a.jpg"), &junk).unwrap().len(), 1);
        assert!(
            walk(base, Some("trip/Thumbs.db"), &junk)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            walk(base, Some("trip"), &JunkRules::none()).unwrap().len(),
            3
        );
        assert!(walk(base, Some("gone"), &junk).unwrap().is_empty());
        assert!(walk(&base.join("missing"), None, &junk).is_err());
    }
}
