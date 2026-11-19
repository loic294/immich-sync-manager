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
        let marker = marker_path(&self.mapping);
        fs::symlink_metadata(&marker).is_ok_and(|meta| meta.is_file() && meta.len() <= 128)
            && fs::read_to_string(marker).is_ok_and(|owner| owner.trim() == self.mapping.root_id)
    }

    pub fn ensure_available(&self) -> AppResult<()> {
        if self.available() {
            Ok(())
        } else {
            Err(AppError::root_unavailable(&self.mapping.root_id))
        }
    }

    /// The absolute path of `rel`, refusing symlinks in existing path components.
    pub fn abs(&self, rel: &RelPath) -> AppResult<PathBuf> {
        let base = self.base();
        base.canonicalize()?;
        let mut path = base;
        let mut missing = false;
        for segment in rel.segments() {
            path.push(segment);
            if missing {
                continue;
            }
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(AppError::invalid_location("symlinks are not allowed"));
                }
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => missing = true,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(path)
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

/// Shared folders use a separate marker for the additional root; legacy claims stay unchanged.
pub fn marker_path(mapping: &RootMapping) -> PathBuf {
    let dir = Path::new(&mapping.container_path);
    let specific = dir.join(format!("{SENTINEL}-{}", mapping.root_id));
    match fs::symlink_metadata(&specific) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => dir.join(SENTINEL),
        _ => specific,
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
    if rel.segments().enumerate().any(|(index, segment)| {
        segment.starts_with(".ism-root") || (index == 0 && HIDDEN.contains(&segment))
    }) {
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

/// `"{size}-{mtime_ns}"`: a metadata revision, not a content hash.
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

/// Reads a fresh fingerprint without trusting the metadata-keyed hash cache.
pub fn replacement_fingerprint(path: &Path) -> io::Result<Option<(String, String)>> {
    let before = match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => meta,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a regular file",
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let hash = sha256_file(path)?;
    let after = fs::symlink_metadata(path)?;
    if !after.is_file() || revision(&before) != revision(&after) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file changed while calculating its hash",
        ));
    }
    Ok(Some((revision(&after), hash)))
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

/// Atomically renames `from` to `to` without replacing an existing entry.
pub fn move_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    rename_no_replace(from, to)
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        from,
        rustix::fs::CWD,
        to,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(Into::into)
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn rename_no_replace(_from: &Path, _to: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace rename is unavailable on this platform",
    ))
}

/// Copies a file to a uniquely named temporary file, verifies it, and publishes it without
/// replacing an existing destination.
pub fn copy_verified(from: &Path, to: &Path) -> io::Result<()> {
    let parent = to
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".ism-copy-")
        .tempfile_in(parent)?;
    let mut source = fs::File::open(from)?;
    if !source.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source is not a file",
        ));
    }
    io::copy(&mut source, &mut tmp)?;
    tmp.as_file().sync_all()?;
    if sha256_file(from)? != sha256_file(tmp.path())? {
        return Err(io::Error::other("copy verification failed"));
    }
    tmp.persist_noclobber(to).map(|_| ()).map_err(|e| e.error)
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
        assert!(!root.available());
        fs::write(dir.path().join(SENTINEL), "other\n").unwrap();
        assert!(!root.available());
        fs::write(dir.path().join(SENTINEL), "r\n").unwrap();
        assert!(root.available());
    }

    #[test]
    fn shared_root_marker_is_independent_and_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let root = root_at(dir.path());
        let marker = dir.path().join(".ism-root-r");
        fs::write(dir.path().join(SENTINEL), "other\n").unwrap();
        fs::write(&marker, "r\n").unwrap();
        assert!(root.available());
        fs::write(&marker, "").unwrap();
        assert!(!root.available());
        fs::write(&marker, "other\n").unwrap();
        assert!(!root.available());
        fs::remove_file(&marker).unwrap();
        assert!(!root.available());
        for path in [".ism-root-r", "album/.ism-root", "album/.ism-root-r"] {
            assert!(parse_rel(path).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn availability_rejects_a_symlinked_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("actual-marker");
        fs::write(&marker, "r\n").unwrap();
        std::os::unix::fs::symlink(&marker, dir.path().join(SENTINEL)).unwrap();
        assert!(!root_at(dir.path()).available());
        fs::remove_file(dir.path().join(SENTINEL)).unwrap();
        fs::write(dir.path().join(SENTINEL), "r\n").unwrap();
        std::os::unix::fs::symlink(&marker, dir.path().join(".ism-root-r")).unwrap();
        assert!(!root_at(dir.path()).available());
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_refused_even_inside_the_root_or_when_dangling() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("real")).unwrap();
        fs::write(dir.path().join("real/photo.jpg"), "photo").unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("outside")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("inside")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("missing"), dir.path().join("dangling"))
            .unwrap();
        std::os::unix::fs::symlink(
            dir.path().join("real/photo.jpg"),
            dir.path().join("photo.jpg"),
        )
        .unwrap();
        let root = root_at(dir.path());
        for rel in [
            "outside/x.jpg",
            "inside/photo.jpg",
            "dangling/x.jpg",
            "photo.jpg",
        ] {
            assert!(root.abs(&RelPath::parse(rel).unwrap()).is_err(), "{rel}");
        }
        assert!(root.abs(&RelPath::parse("real/photo.jpg").unwrap()).is_ok());
        assert!(root.abs(&RelPath::parse("new/x.jpg").unwrap()).is_ok());
        assert_eq!(root.abs(&RelPath::root()).unwrap(), dir.path());
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
    fn concurrent_moves_do_not_replace_the_winner() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..50 {
            let a = dir.path().join(format!("a{i}"));
            let b = dir.path().join(format!("b{i}"));
            let target = dir.path().join(format!("target{i}"));
            fs::write(&a, "a").unwrap();
            fs::write(&b, "b").unwrap();
            let barrier = std::sync::Barrier::new(2);
            let (left, right) = std::thread::scope(|s| {
                let first = s.spawn(|| {
                    barrier.wait();
                    move_no_replace(&a, &target)
                });
                barrier.wait();
                let second = move_no_replace(&b, &target);
                (first.join().unwrap(), second)
            });
            assert_ne!(left.is_ok(), right.is_ok());
            if left.is_ok() {
                assert_eq!(fs::read_to_string(&target).unwrap(), "a");
                assert_eq!(fs::read_to_string(&b).unwrap(), "b");
            } else {
                assert_eq!(fs::read_to_string(&target).unwrap(), "b");
                assert_eq!(fs::read_to_string(&a).unwrap(), "a");
            }
        }
    }

    #[test]
    fn verified_copy_does_not_replace_destination_or_existing_part_file() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("photo.jpg");
        let existing_part = dir.path().join("photo.ism-part");
        fs::write(&source, "source").unwrap();
        fs::write(&existing_part, "other").unwrap();
        copy_verified(&source, &destination).unwrap();
        assert_eq!(fs::read_to_string(&destination).unwrap(), "source");
        assert_eq!(fs::read_to_string(&existing_part).unwrap(), "other");
        fs::write(&source, "changed").unwrap();
        assert!(copy_verified(&source, &destination).is_err());
        assert_eq!(fs::read_to_string(&destination).unwrap(), "source");
    }

    #[test]
    fn hidden_paths_are_reserved() {
        assert!(parse_rel(".ism-trash/x").is_err());
        assert!(parse_rel("a/.ism-trash").is_ok());
    }
}
