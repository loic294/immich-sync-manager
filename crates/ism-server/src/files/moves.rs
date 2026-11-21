//! Verified cross-filesystem moves, with explicit recovery after destination publication.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct MoveError {
    pub error: io::Error,
    pub recovery_required: bool,
}

impl From<io::Error> for MoveError {
    fn from(error: io::Error) -> Self {
        Self {
            error,
            recovery_required: false,
        }
    }
}

pub fn move_verified(from: &Path, to: &Path) -> Result<(), MoveError> {
    move_with(from, to, super::rename_no_replace)
}

fn move_with(
    from: &Path,
    to: &Path,
    rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<(), MoveError> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    match rename(from, to) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {
            copy_move_with(from, to, super::rename_no_replace, remove_source)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Entry {
    hash: Option<String>,
    size: Option<u64>,
    modified: SystemTime,
    readonly: bool,
    #[cfg(unix)]
    mode: u32,
}

type Inventory = BTreeMap<PathBuf, Entry>;

fn inventory(path: &Path) -> io::Result<Inventory> {
    let mut result = BTreeMap::new();
    for entry in walkdir::WalkDir::new(path).follow_links(false) {
        let entry = entry.map_err(io::Error::other)?;
        let meta = fs::symlink_metadata(entry.path())?;
        let hash = if meta.is_file() {
            let (revision, hash) = super::replacement_fingerprint(entry.path())?
                .ok_or_else(|| io::Error::other("source disappeared"))?;
            if revision != super::revision(&meta) {
                return Err(io::Error::other("source changed during verification"));
            }
            Some(hash)
        } else if meta.is_dir() {
            None
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cross-filesystem moves refuse symlinks and special files",
            ));
        };
        result.insert(
            entry
                .path()
                .strip_prefix(path)
                .map_err(io::Error::other)?
                .to_owned(),
            Entry {
                hash,
                size: meta.is_file().then_some(meta.len()),
                modified: meta.modified()?,
                readonly: meta.permissions().readonly(),
                #[cfg(unix)]
                mode: {
                    use std::os::unix::fs::PermissionsExt;
                    meta.permissions().mode()
                },
            },
        );
    }
    Ok(result)
}

fn check_inventory(path: &Path, expected: &Inventory) -> io::Result<()> {
    if &inventory(path)? != expected {
        return Err(io::Error::other(
            "move contents or metadata changed during verification",
        ));
    }
    Ok(())
}

fn copy_tree(from: &Path, to: &Path, expected: &Inventory) -> io::Result<()> {
    for (rel, entry) in expected {
        let source = from.join(rel);
        let target = to.join(rel);
        // Joining an empty relative path appends a separator, which is invalid for files.
        let source = if rel.as_os_str().is_empty() {
            from
        } else {
            &source
        };
        let target = if rel.as_os_str().is_empty() {
            to
        } else {
            &target
        };
        if entry.hash.is_none() {
            fs::create_dir(target)?;
        } else {
            let mut input = fs::File::open(source)?;
            let mut output = fs::File::options()
                .write(true)
                .create_new(true)
                .open(target)?;
            io::copy(&mut input, &mut output)?;
            output.set_times(fs::FileTimes::new().set_modified(entry.modified))?;
            fs::set_permissions(target, fs::symlink_metadata(source)?.permissions())?;
            output.sync_all()?;
        }
    }
    // Set directory metadata only after writing children, deepest directories first.
    for (rel, entry) in expected
        .iter()
        .rev()
        .filter(|(_, entry)| entry.hash.is_none())
    {
        let target = to.join(rel);
        let directory = fs::File::open(&target)?;
        directory.set_times(fs::FileTimes::new().set_modified(entry.modified))?;
        fs::set_permissions(&target, fs::symlink_metadata(from.join(rel))?.permissions())?;
        directory.sync_all()?;
    }
    Ok(())
}

fn remove_source(from: &Path, expected: &Inventory) -> io::Result<()> {
    for (rel, entry) in expected.iter().rev() {
        let path = if rel.as_os_str().is_empty() {
            from.to_owned()
        } else {
            from.join(rel)
        };
        if entry.hash.is_some() {
            let actual = inventory(&path)?;
            if actual.get(Path::new("")) != Some(entry) {
                return Err(io::Error::other("source changed before removal"));
            }
            fs::remove_file(path)?;
        } else {
            // Never recursively delete entries absent from the verified inventory.
            fs::remove_dir(path)?;
        }
    }
    Ok(())
}

fn copy_move_with(
    from: &Path,
    to: &Path,
    publish: impl FnOnce(&Path, &Path) -> io::Result<()>,
    remove: impl FnOnce(&Path, &Inventory) -> io::Result<()>,
) -> Result<(), MoveError> {
    let parent = to
        .parent()
        .ok_or_else(|| io::Error::other("destination has no parent"))?;
    match fs::symlink_metadata(to) {
        Ok(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists).into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let expected = inventory(from)?;
    let staging = tempfile::Builder::new()
        .prefix(".ism-move-")
        .tempdir_in(parent)?;
    let staged = staging.path().join("payload");
    let mut published = false;
    let result: io::Result<()> = (|| {
        copy_tree(from, &staged, &expected)?;
        check_inventory(&staged, &expected)?;
        check_inventory(from, &expected)?;
        publish(&staged, to)?;
        published = true;
        fs::File::open(parent)?.sync_all()?;
        check_inventory(to, &expected)?;
        check_inventory(from, &expected)?;
        remove(from, &expected)?;
        fs::File::open(
            from.parent()
                .ok_or_else(|| io::Error::other("source has no parent"))?,
        )?
        .sync_all()?;
        Ok(())
    })();
    let staging_path = staging.path().to_owned();
    if let Err(cleanup) = staging.close() {
        return Err(MoveError {
            error: io::Error::new(
                cleanup.kind(),
                format!(
                    "move result: {result:?}; could not remove staging {}: {cleanup}",
                    staging_path.display()
                ),
            ),
            recovery_required: true,
        });
    }
    result.map_err(|error| MoveError {
        error: io::Error::new(
            error.kind(),
            format!(
                "{} -> {}: {error}{}",
                from.display(),
                to.display(),
                if published {
                    "; destination published; inspect both paths before recovery"
                } else {
                    ""
                }
            ),
        ),
        recovery_required: published,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn cross_device(_: &Path, _: &Path) -> io::Result<()> {
        Err(io::ErrorKind::CrossesDevices.into())
    }

    #[test]
    fn cross_device_move_preserves_content_metadata_and_can_move_back() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("photo.jpg");
        let to = dir.path().join("destination/photo.jpg");
        fs::write(&from, b"synthetic photo").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&from, fs::Permissions::from_mode(0o640)).unwrap();
        }
        let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        fs::File::options()
            .write(true)
            .open(&from)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        move_with(&from, &to, cross_device).unwrap();
        assert!(!from.exists());
        assert_eq!(fs::read(&to).unwrap(), b"synthetic photo");
        assert_eq!(fs::metadata(&to).unwrap().modified().unwrap(), modified);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&to).unwrap().permissions().mode() & 0o777,
                0o640
            );
        }
        move_with(&to, &from, cross_device).unwrap();
        assert!(!to.exists());
        assert_eq!(fs::read(&from).unwrap(), b"synthetic photo");
    }

    #[test]
    fn cross_device_folder_move_includes_companions_and_empty_directories() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("album");
        let to = dir.path().join("destination/album");
        fs::create_dir_all(from.join("nested/empty")).unwrap();
        fs::write(from.join("nested/photo.jpg"), b"synthetic photo").unwrap();
        fs::write(from.join("nested/photo.jpg.xmp"), b"synthetic sidecar").unwrap();
        move_with(&from, &to, cross_device).unwrap();
        assert!(!from.exists());
        assert_eq!(
            fs::read(to.join("nested/photo.jpg")).unwrap(),
            b"synthetic photo"
        );
        assert_eq!(
            fs::read(to.join("nested/photo.jpg.xmp")).unwrap(),
            b"synthetic sidecar"
        );
        assert!(to.join("nested/empty").is_dir());
        move_with(&to, &from, cross_device).unwrap();
        assert!(!to.exists());
        assert!(from.join("nested/empty").is_dir());
        assert_eq!(
            fs::read(from.join("nested/photo.jpg.xmp")).unwrap(),
            b"synthetic sidecar"
        );
    }

    #[test]
    fn cross_device_publication_never_replaces_a_racing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("photo.jpg");
        let to = dir.path().join("target.jpg");
        fs::write(&from, b"original").unwrap();
        let error = copy_move_with(
            &from,
            &to,
            |staged, target| {
                fs::write(target, b"other writer")?;
                super::super::rename_no_replace(staged, target)
            },
            remove_source,
        )
        .unwrap_err();
        assert!(!error.recovery_required);
        assert_eq!(error.error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&from).unwrap(), b"original");
        assert_eq!(fs::read(&to).unwrap(), b"other writer");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn cross_device_source_change_after_publication_requires_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("photo.jpg");
        let to = dir.path().join("target.jpg");
        fs::write(&from, b"original").unwrap();
        let error = copy_move_with(
            &from,
            &to,
            |staged, target| {
                super::super::rename_no_replace(staged, target)?;
                fs::write(&from, b"changed by external writer")
            },
            remove_source,
        )
        .unwrap_err();
        assert!(error.recovery_required);
        assert_eq!(fs::read(&from).unwrap(), b"changed by external writer");
        assert_eq!(fs::read(&to).unwrap(), b"original");
    }

    #[test]
    fn cross_device_partial_source_removal_keeps_the_verified_destination() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("album");
        let to = dir.path().join("target");
        fs::create_dir(&from).unwrap();
        fs::write(from.join("photo.jpg"), b"photo").unwrap();
        fs::write(from.join("photo.jpg.xmp"), b"sidecar").unwrap();
        let error = copy_move_with(&from, &to, super::super::rename_no_replace, |path, _| {
            fs::remove_file(path.join("photo.jpg"))?;
            Err(io::ErrorKind::PermissionDenied.into())
        })
        .unwrap_err();
        assert!(error.recovery_required);
        assert!(from.join("photo.jpg.xmp").is_file());
        assert_eq!(fs::read(to.join("photo.jpg")).unwrap(), b"photo");
        assert_eq!(fs::read(to.join("photo.jpg.xmp")).unwrap(), b"sidecar");
    }

    #[cfg(unix)]
    #[test]
    fn cross_device_folder_with_symlinks_is_refused_before_publication() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("album");
        let to = dir.path().join("target");
        fs::create_dir(&from).unwrap();
        fs::write(from.join("photo.jpg"), b"photo").unwrap();
        std::os::unix::fs::symlink("photo.jpg", from.join("link")).unwrap();
        let error = move_with(&from, &to, cross_device).unwrap_err();
        assert!(!error.recovery_required);
        assert!(from.join("photo.jpg").is_file());
        assert!(!to.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn cross_device_fallback_is_not_used_for_other_rename_errors() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("photo.jpg");
        let to = dir.path().join("target");
        fs::write(&from, b"photo").unwrap();
        let error = move_with(&from, &to, |_, _| {
            Err(io::ErrorKind::PermissionDenied.into())
        })
        .unwrap_err();
        assert_eq!(error.error.kind(), io::ErrorKind::PermissionDenied);
        assert!(!error.recovery_required);
        assert!(from.is_file());
        assert!(!to.exists());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    #[cfg_attr(
        target_os = "macos",
        ignore = "requires a separate filesystem in ISM_TEST_CROSS_DEVICE_DIR"
    )]
    fn cross_device_move_between_real_filesystems() {
        let source = tempfile::tempdir().unwrap();
        #[cfg(target_os = "linux")]
        let destination = tempfile::tempdir_in("/dev/shm").unwrap();
        #[cfg(target_os = "macos")]
        let destination =
            tempfile::tempdir_in(std::env::var_os("ISM_TEST_CROSS_DEVICE_DIR").unwrap()).unwrap();
        let file_from = source.path().join("single.jpg");
        let file_to = destination.path().join("single.jpg");
        fs::write(&file_from, b"single photo").unwrap();
        move_verified(&file_from, &file_to).unwrap();
        assert!(!file_from.exists());
        assert_eq!(fs::read(&file_to).unwrap(), b"single photo");
        move_verified(&file_to, &file_from).unwrap();
        assert!(!file_to.exists());
        let from = source.path().join("album");
        let to = destination.path().join("album");
        fs::create_dir_all(from.join("empty")).unwrap();
        fs::write(from.join("photo.jpg"), b"photo").unwrap();
        fs::write(from.join("photo.jpg.xmp"), b"sidecar").unwrap();
        assert_eq!(
            super::super::rename_no_replace(&from, &to)
                .unwrap_err()
                .kind(),
            io::ErrorKind::CrossesDevices
        );
        move_verified(&from, &to).unwrap();
        assert!(!from.exists());
        assert_eq!(fs::read(to.join("photo.jpg")).unwrap(), b"photo");
        move_verified(&to, &from).unwrap();
        assert!(!to.exists());
        assert_eq!(fs::read(from.join("photo.jpg.xmp")).unwrap(), b"sidecar");
        assert!(from.join("empty").is_dir());
    }
}
