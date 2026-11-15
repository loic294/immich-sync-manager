//! Native change events (inotify, FSEvents, ...) through the `notify` crate.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;

pub enum NativeEvent {
    Paths(Vec<PathBuf>),
    /// Events were lost (kernel overflow or a full channel): a full pass is needed.
    Rescan,
    /// The watcher failed (for example the watch limit); fall back to polling.
    Failed(String),
}

pub struct NativeWatcher {
    _watcher: notify::RecommendedWatcher,
    overflow: Arc<AtomicBool>,
}

impl NativeWatcher {
    /// True once since the last call if events were dropped.
    pub fn take_overflow(&self) -> bool {
        self.overflow.swap(false, Ordering::SeqCst)
    }
}

pub fn start(base: &Path, tx: mpsc::Sender<NativeEvent>) -> notify::Result<NativeWatcher> {
    let overflow = Arc::new(AtomicBool::new(false));
    let flag = overflow.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        let event = match res {
            Ok(event) if event.need_rescan() => NativeEvent::Rescan,
            Ok(event) if event.kind.is_access() => return,
            Ok(event) => NativeEvent::Paths(event.paths),
            Err(e) => NativeEvent::Failed(e.to_string()),
        };
        if tx.try_send(event).is_err() {
            flag.store(true, Ordering::SeqCst);
        }
    })?;
    watcher.watch(base, RecursiveMode::Recursive)?;
    Ok(NativeWatcher {
        _watcher: watcher,
        overflow,
    })
}
