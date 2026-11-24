//! One streaming, fail-closed traversal for operation and review evidence.

use std::fs;
use std::io;
use std::path::Path;

use crate::files;

const MAX_DEPTH: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub name: String,
    pub sha256: Option<String>,
    pub size: Option<u64>,
}

pub(crate) fn walk(
    path: &Path,
    mut remaining: Option<usize>,
    mut visit: impl FnMut(Entry) -> io::Result<()>,
) -> io::Result<()> {
    fn descend(
        base: &Path,
        path: &Path,
        depth: usize,
        remaining: &mut Option<usize>,
        visit: &mut impl FnMut(Entry) -> io::Result<()>,
    ) -> io::Result<()> {
        if let Some(remaining) = remaining {
            *remaining = remaining.checked_sub(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "folder exceeds the version evidence entry limit",
                )
            })?;
        }
        if depth > MAX_DEPTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "folder exceeds the supported traversal depth",
            ));
        }
        let before = fs::symlink_metadata(path)?;
        let mut name = String::new();
        for component in path
            .strip_prefix(base)
            .map_err(io::Error::other)?
            .components()
        {
            if !name.is_empty() {
                name.push('/');
            }
            name.push_str(
                component
                    .as_os_str()
                    .to_str()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF-8 name"))?,
            );
        }
        if before.is_dir() {
            visit(Entry {
                name,
                sha256: None,
                size: None,
            })?;
            // Keep directory iterators open: WalkDir can buffer wide directories when
            // its open-handle limit is exceeded. Depth instead has an explicit bound.
            for child in fs::read_dir(path)? {
                descend(base, &child?.path(), depth + 1, remaining, visit)?;
            }
        } else if before.is_file() && depth != 0 {
            let (revision, hash) = files::replacement_fingerprint(path)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file disappeared"))?;
            if revision != files::revision(&before) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file changed during traversal",
                ));
            }
            visit(Entry {
                name,
                sha256: Some(hash),
                size: Some(before.len()),
            })?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a directory or folder contains a symlink or special file",
            ));
        }
        let after = fs::symlink_metadata(path)?;
        if before.file_type() != after.file_type()
            || files::revision(&before) != files::revision(&after)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entry changed during traversal",
            ));
        }
        Ok(())
    }
    descend(path, path, 0, &mut remaining, &mut visit)
}
