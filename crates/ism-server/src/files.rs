//! Mapped roots on disk: lookup, availability, safe path resolution, revisions and hashes.

use std::fs::{self, Metadata};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use ism_core::dto::{FileEntry, Location};
use ism_core::mapping::{PathSide, RootMapping, resolve};
use ism_core::paths::RelPath;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::db;
use crate::error::{AppError, AppResult};

/// The marker file proving a root is mounted. A missing marker means "unavailable", never "empty".
pub const SENTINEL: &str = ".ism-root";
/// Quarantine folder for recoverable trash, inside each root (same volume, so moves are renames).
pub const TRASH_DIR: &str = ".ism-trash";
/// Staging folder for uploads and peer transfers, inside each root.
pub const STAGING_DIR: &str = ".ism-staging";
/// Names that are never listed, synced or offered as content.
pub const HIDDEN: &[&str] = &[SENTINEL, TRASH_DIR, STAGING_DIR];

#[derive(Debug, Clone)]
pub struct Root {
    pub mapping: RootMapping,
    pub name: String,
    pub server_id: Option<String>,
    pub library_id: Option<String>,
}

impl Root {
    pub fn base(&self) -> PathBuf {
        PathBuf::from(&self.mapping.container_path)
    }

    pub fn available(&self) -> bool {
        self.base().join(SENTINEL).is_file()
    }

    pub fn ensure_available(&self) -> AppResult<()> {
        if self.available() {
            Ok(())
        } else {
            Err(AppError::root_unavailable(&self.mapping.root_id))
        }
    }

    /// The absolute path of `rel`, refusing anything that escapes the root through symlinks.
    pub fn abs(&self, rel: &RelPath) -> AppResult<PathBuf> {
        let base = self.base();
        let path = base.join(rel.as_str());
        let canonical_base = base.canonicalize()?;
        // Check the deepest existing ancestor: the path itself may not exist yet.
        let mut probe = path.as_path();
        loop {
            match probe.canonicalize() {
                Ok(real) => {
                    if real.starts_with(&canonical_base) {
                        return Ok(path);
                    }
                    return Err(AppError::invalid_location("path leaves its root"));
                }
                Err(_) => match probe.parent() {
                    Some(parent) => probe = parent,
                    None => return Err(AppError::invalid_location("path leaves its root")),
                },
            }
        }
    }

    pub fn location(&self, rel: &RelPath) -> Location {
        Location {
            root_id: self.mapping.root_id.clone(),
            path: rel.as_str().to_owned(),
        }
    }

    pub fn immich_path(&self, rel: &RelPath) -> Option<String> {
        self.mapping.immich_abs(rel)
    }
}

type RootRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub async fn load_roots(pool: &SqlitePool) -> AppResult<Vec<Root>> {
    let rows: Vec<RootRow> = sqlx::query_as(
        "SELECT root_id, name, container_path, server_id, library_id, immich_import_path, opm_prefix
         FROM mappings ORDER BY root_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(root_id, name, container_path, server_id, library_id, immich, opm)| Root {
                mapping: RootMapping {
                    root_id,
                    container_path,
                    immich_import_path: immich,
                    opm_prefix: opm,
                },
                name,
                server_id,
                library_id,
            },
        )
        .collect())
}

pub async fn root(pool: &SqlitePool, root_id: &str) -> AppResult<Root> {
    load_roots(pool)
        .await?
        .into_iter()
        .find(|r| r.mapping.root_id == root_id)
        .ok_or_else(|| AppError::invalid_location(format!("unknown root '{root_id}'")))
}

pub fn parse_rel(raw: &str) -> AppResult<RelPath> {
    let rel = RelPath::parse(raw).map_err(|e| AppError::invalid_location(e.to_string()))?;
    if rel
        .segments()
        .next()
        .is_some_and(|first| HIDDEN.contains(&first))
    {
        return Err(AppError::invalid_location("reserved path"));
    }
    Ok(rel)
}

/// Resolves a location to its root, relative path and absolute path.
pub async fn locate(pool: &SqlitePool, loc: &Location) -> AppResult<(Root, RelPath, PathBuf)> {
    let root = root(pool, &loc.root_id).await?;
    root.ensure_available()?;
    let rel = parse_rel(&loc.path)?;
    let abs = root.abs(&rel)?;
    Ok((root, rel, abs))
}

pub fn resolve_immich_path(roots: &[Root], immich_path: &str) -> Option<(Root, RelPath)> {
    let mappings: Vec<RootMapping> = roots.iter().map(|r| r.mapping.clone()).collect();
    let (mapping, rel) = resolve(&mappings, PathSide::Immich, immich_path)?;
    let root = roots
        .iter()
        .find(|r| r.mapping.root_id == mapping.root_id)?;
    Some((root.clone(), rel))
}

pub fn mtime_ns(meta: &Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// `"{size}-{mtime_ns}"`: changes whenever the content is rewritten.
pub fn revision(meta: &Metadata) -> String {
    format!("{}-{}", meta.len(), mtime_ns(meta))
}

pub fn modified_rfc3339(meta: &Metadata) -> Option<String> {
    let ns = mtime_ns(meta);
    chrono::DateTime::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32).map(db::ts)
}

pub fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// The sha256 of a file, cached by (root, path, size, mtime).
pub async fn cached_sha256(
    pool: &SqlitePool,
    root_id: &str,
    rel: &RelPath,
    abs: &Path,
) -> AppResult<String> {
    let meta = fs::metadata(abs)?;
    let (size, mtime) = (meta.len() as i64, mtime_ns(&meta));
    let cached: Option<String> = sqlx::query_scalar(
        "SELECT sha256 FROM file_hashes WHERE root_id = ? AND path = ? AND size = ? AND mtime_ns = ?",
    )
    .bind(root_id)
    .bind(rel.as_str())
    .bind(size)
    .bind(mtime)
    .fetch_optional(pool)
    .await?;
    if let Some(hash) = cached {
        return Ok(hash);
    }
    let owned = abs.to_owned();
    let hash = tokio::task::spawn_blocking(move || sha256_file(&owned))
        .await
        .map_err(AppError::internal)??;
    sqlx::query(
        "INSERT INTO file_hashes (root_id, path, size, mtime_ns, sha256) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(root_id, path) DO UPDATE SET size = excluded.size, mtime_ns = excluded.mtime_ns,
         sha256 = excluded.sha256",
    )
    .bind(root_id)
    .bind(rel.as_str())
    .bind(size)
    .bind(mtime)
    .bind(&hash)
    .execute(pool)
    .await?;
    Ok(hash)
}

/// Visible entries of a directory: (folders, files), sorted by name.
/// Sub-folder names and files with their metadata.
pub type Listing = (Vec<String>, Vec<(String, Metadata)>);

pub fn list_dir(abs: &Path, rel: &RelPath) -> AppResult<Listing> {
    let mut folders = Vec::new();
    let mut files = Vec::new();
    for entry in fs::read_dir(abs)? {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if (rel.is_root() && HIDDEN.contains(&name.as_str())) || name.starts_with(".ism-") {
            continue;
        }
        let meta = entry.metadata()?;
        if meta.is_dir() {
            folders.push(name);
        } else if meta.is_file() {
            files.push((name, meta));
        }
    }
    folders.sort();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok((folders, files))
}

pub fn file_entry(rel: &RelPath, name: &str, meta: &Metadata) -> AppResult<FileEntry> {
    let path = rel
        .join(name)
        .map_err(|e| AppError::invalid_location(e.to_string()))?;
    Ok(FileEntry {
        name: name.to_owned(),
        path: path.as_str().to_owned(),
        size: meta.len(),
        revision: revision(meta),
        modified_at: modified_rfc3339(meta),
    })
}

/// Configured companion extensions (settings key `companion_extensions`, comma separated).
pub async fn companion_extensions(pool: &SqlitePool) -> Vec<String> {
    match db::get_setting(pool, "companion_extensions")
        .await
        .ok()
        .flatten()
    {
        Some(raw) => raw
            .split(',')
            .map(|s| s.trim().trim_start_matches('.').to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect(),
        None => ism_core::sidecar::DEFAULT_COMPANION_EXTENSIONS
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
    }
}

/// Renames `from` to `to` without replacing an existing file. Falls back to copy + verify +
/// remove across file systems.
pub fn move_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    if to.symlink_metadata().is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "destination exists",
        ));
    }
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(18) /* EXDEV */ => {
            copy_verified(from, to)?;
            if from.is_dir() {
                fs::remove_dir_all(from)
            } else {
                fs::remove_file(from)
            }
        }
        Err(e) => Err(e),
    }
}

pub fn copy_verified(from: &Path, to: &Path) -> io::Result<()> {
    if from.is_dir() {
        fs::create_dir_all(to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            copy_verified(&entry.path(), &to.join(entry.file_name()))?;
        }
        return Ok(());
    }
    let tmp = to.with_extension("ism-part");
    fs::copy(from, &tmp)?;
    fs::File::open(&tmp)?.sync_all()?;
    if sha256_file(from)? != sha256_file(&tmp)? {
        let _ = fs::remove_file(&tmp);
        return Err(io::Error::other("copy verification failed"));
    }
    fs::rename(&tmp, to)
}

/// Total size of a file or directory tree.
pub fn tree_size(path: &Path) -> u64 {
    walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(Result::ok)
        .filter_map(|e| e.metadata().ok())
        .filter(Metadata::is_file)
        .map(|m| m.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root_at(dir: &Path) -> Root {
        Root {
            mapping: RootMapping {
                root_id: "r".into(),
                container_path: dir.to_string_lossy().into_owned(),
                immich_import_path: Some("/external/r".into()),
                opm_prefix: None,
            },
            name: "R".into(),
            server_id: None,
            library_id: None,
        }
    }

    #[test]
    fn availability_needs_the_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_at(dir.path());
        assert!(!root.available());
        fs::write(dir.path().join(SENTINEL), "").unwrap();
        assert!(root.available());
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_cannot_escape_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).unwrap();
        let root = root_at(dir.path());
        assert!(root.abs(&RelPath::parse("link/x.jpg").unwrap()).is_err());
        assert!(root.abs(&RelPath::parse("new/x.jpg").unwrap()).is_ok());
    }

    #[test]
    fn moves_never_replace() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("sub/b");
        fs::write(&a, "1").unwrap();
        move_no_replace(&a, &b).unwrap();
        fs::write(&a, "2").unwrap();
        assert!(move_no_replace(&a, &b).is_err());
        assert_eq!(fs::read_to_string(&b).unwrap(), "1");
    }

    #[test]
    fn hidden_paths_are_reserved() {
        assert!(parse_rel(".ism-trash/x").is_err());
        assert!(parse_rel("a/.ism-trash").is_ok());
    }
}
