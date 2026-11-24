//! One streaming, fail-closed traversal for operation and review evidence.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};

use crate::files;

const MAX_DEPTH: usize = 128;
const SCAN_BUFFER: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub name: String,
    pub sha256: Option<String>,
    pub size: Option<u64>,
}

pub(super) struct Scan {
    entries: mpsc::Receiver<Entry>,
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
}

impl Scan {
    pub(super) async fn next(&mut self) -> io::Result<Option<Entry>> {
        if let Some(entry) = self.entries.recv().await {
            return Ok(Some(entry));
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            result.map_err(io::Error::other)??;
        }
        Ok(None)
    }
}

pub(super) async fn scan(path: PathBuf, limit: Arc<Semaphore>) -> io::Result<Scan> {
    let permit = limit.acquire_owned().await.map_err(io::Error::other)?;
    let (send, entries) = mpsc::channel(SCAN_BUFFER);
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        walk(&path, None, |entry| {
            send.blocking_send(entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Interrupted, "tree scan cancelled"))
        })
    });
    Ok(Scan {
        entries,
        task: Some(task),
    })
}

/// Verify legacy expected inventories without collecting a second inventory.
/// The bitmap accounts for each expected path exactly once, even if a directory iterator
/// repeats a name during concurrent changes. Reference-backed expectations need durable
/// visited-path accounting instead; this interface does not authorize their use.
pub(super) async fn matches_inline(
    path: PathBuf,
    expected: &[super::plan::TrashEntry],
    limit: Arc<Semaphore>,
) -> io::Result<bool> {
    if expected
        .first()
        .is_none_or(|entry| !entry.name.is_empty() || entry.sha256.is_some())
        || expected.windows(2).any(|pair| pair[0].name >= pair[1].name)
    {
        return Ok(false);
    }
    let mut observed = vec![0u8; expected.len().div_ceil(8)];
    let mut count = 0;
    let mut matches = true;
    let mut scan = scan(path, limit).await?;
    while let Some(actual) = scan.next().await? {
        let Ok(index) = expected.binary_search_by(|entry| entry.name.cmp(&actual.name)) else {
            matches = false;
            continue;
        };
        let mask = 1u8 << (index % 8);
        if observed[index / 8] & mask != 0 || expected[index].sha256 != actual.sha256 {
            matches = false;
        }
        observed[index / 8] |= mask;
        count += 1;
    }
    Ok(matches && count == expected.len())
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn cancelled_next_still_verifies_worker_completion() {
        let (send, entries) = mpsc::channel(1);
        drop(send);
        let (finish, finished) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            finished.await.unwrap();
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "synthetic unreadable directory",
            ))
        });
        let mut scan = Scan {
            entries,
            task: Some(task),
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), scan.next())
                .await
                .is_err()
        );
        finish.send(()).unwrap();
        assert_eq!(
            scan.next().await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}
