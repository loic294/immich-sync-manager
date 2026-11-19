//! Process configuration from environment variables.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;

const DEFAULT_PEER_TRANSFER_CONCURRENCY: usize = 3;
const MAX_PEER_TRANSFER_CONCURRENCY: usize = 32;

#[derive(Debug, Clone)]
pub struct Config {
    /// `ISM_LISTEN`, default `0.0.0.0:8080`.
    pub listen: SocketAddr,
    /// `ISM_DATA_DIR`, default `/data`. Database, key file.
    pub data_dir: PathBuf,
    /// `ISM_WEB_DIR`, default `/app/web`. The built single-page app.
    pub web_dir: PathBuf,
    /// `ISM_MOUNTS_DIR`, default `/mnt`. Where volumes are mounted, offered in the mapping UI.
    pub mounts_dir: PathBuf,
    /// `ISM_SECURE_COOKIES` (`true` behind HTTPS).
    pub secure_cookies: bool,
    /// `ISM_WORKER_TICK_SECS`, default 5. How often background workers wake up.
    pub tick_secs: u64,
    /// `ISM_SCAN_INTERVAL_SECS`, default 300. How often shared folders are scanned for changes.
    pub scan_interval_secs: u64,
    /// Maximum number of peer file transfers running concurrently, default 3.
    pub peer_transfer_concurrency: usize,
    /// Timings of the Immich maintenance barrier.
    pub timing: Timing,
}

#[derive(Debug, Clone)]
pub struct Timing {
    /// How often queue counters are polled.
    pub poll: std::time::Duration,
    /// `ISM_QUIESCE_TIMEOUT_SECS`, default 600. Longest wait for Immich's `library` queue to
    /// drain and stay idle while paused; on timeout the queue is resumed and the server blocked.
    pub quiesce_timeout: std::time::Duration,
    /// Wait after pausing the queue before the idle confirmations start.
    pub settle: std::time::Duration,
    /// `ISM_SCAN_TIMEOUT_SECS`, default 7200. Longest wait for a library scan to finish.
    pub scan_timeout: std::time::Duration,
    /// Longest wait of an operation for maintenance admission.
    pub admission_timeout: std::time::Duration,
    /// `ISM_SCAN_DELAY_SECS`, default 120. A library scan runs this long after the last detected
    /// change; every further change restarts the timer.
    pub scan_delay: std::time::Duration,
    /// How often Immich's watcher and scheduled-scan settings are checked for drift.
    pub drift_check: std::time::Duration,
    /// Minimum lifetime of a self-write record (raised to cover twice the longest poll interval).
    pub self_write_ttl: std::time::Duration,
    /// Native change events are collected this long before they are checked.
    pub watch_debounce: std::time::Duration,
    /// Minimum gap between full passes triggered by lost native events.
    pub full_pass_gap: std::time::Duration,
}

impl Default for Timing {
    fn default() -> Self {
        use std::time::Duration;
        Self {
            poll: Duration::from_secs(2),
            quiesce_timeout: Duration::from_secs(600),
            settle: Duration::from_secs(5),
            scan_timeout: Duration::from_secs(7200),
            admission_timeout: Duration::from_secs(900),
            scan_delay: Duration::from_secs(120),
            drift_check: Duration::from_secs(60),
            self_write_ttl: Duration::from_secs(900),
            watch_debounce: Duration::from_secs(2),
            full_pass_gap: Duration::from_secs(30),
        }
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn peer_transfer_concurrency(raw: Option<&str>) -> anyhow::Result<usize> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_PEER_TRANSFER_CONCURRENCY);
    };
    let value = raw
        .trim()
        .parse::<usize>()
        .with_context(|| "ISM_PEER_TRANSFER_CONCURRENCY must be an integer from 1 to 32")?;
    anyhow::ensure!(
        (1..=MAX_PEER_TRANSFER_CONCURRENCY).contains(&value),
        "ISM_PEER_TRANSFER_CONCURRENCY must be from 1 to 32"
    );
    Ok(value)
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let transfer_concurrency_raw = env("ISM_PEER_TRANSFER_CONCURRENCY");
        Ok(Self {
            listen: env("ISM_LISTEN")
                .unwrap_or_else(|| "0.0.0.0:8080".into())
                .parse()
                .context("ISM_LISTEN must be an address like 0.0.0.0:8080")?,
            data_dir: env("ISM_DATA_DIR").unwrap_or_else(|| "/data".into()).into(),
            web_dir: env("ISM_WEB_DIR")
                .unwrap_or_else(|| "/app/web".into())
                .into(),
            mounts_dir: env("ISM_MOUNTS_DIR")
                .unwrap_or_else(|| "/mnt".into())
                .into(),
            secure_cookies: env("ISM_SECURE_COOKIES").is_some_and(|v| v == "true" || v == "1"),
            tick_secs: env("ISM_WORKER_TICK_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            scan_interval_secs: env("ISM_SCAN_INTERVAL_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            peer_transfer_concurrency: peer_transfer_concurrency(
                transfer_concurrency_raw.as_deref(),
            )?,
            timing: {
                let mut t = Timing::default();
                if let Some(v) = env("ISM_QUIESCE_TIMEOUT_SECS").and_then(|v| v.parse().ok()) {
                    t.quiesce_timeout = std::time::Duration::from_secs(v);
                }
                if let Some(v) = env("ISM_SCAN_TIMEOUT_SECS").and_then(|v| v.parse().ok()) {
                    t.scan_timeout = std::time::Duration::from_secs(v);
                }
                if let Some(v) = env("ISM_SCAN_DELAY_SECS").and_then(|v| v.parse().ok()) {
                    t.scan_delay = std::time::Duration::from_secs(v);
                }
                t
            },
        })
    }
}

impl Config {
    /// A configuration for tests rooted in `dir`, with fast timings.
    #[cfg(test)]
    pub fn for_tests(dir: &std::path::Path) -> Self {
        use std::time::Duration;
        Self {
            listen: "127.0.0.1:0".parse().expect("address"),
            data_dir: dir.to_path_buf(),
            web_dir: dir.join("web"),
            mounts_dir: dir.to_path_buf(),
            secure_cookies: false,
            tick_secs: 1,
            scan_interval_secs: 5,
            peer_transfer_concurrency: DEFAULT_PEER_TRANSFER_CONCURRENCY,
            timing: Timing {
                poll: Duration::from_millis(20),
                quiesce_timeout: Duration::from_millis(500),
                settle: Duration::from_millis(10),
                scan_timeout: Duration::from_millis(500),
                admission_timeout: Duration::from_secs(5),
                scan_delay: Duration::from_millis(200),
                drift_check: Duration::ZERO,
                self_write_ttl: Duration::from_secs(60),
                watch_debounce: Duration::from_millis(50),
                full_pass_gap: Duration::ZERO,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::peer_transfer_concurrency;

    #[test]
    fn validates_peer_transfer_concurrency() {
        assert_eq!(peer_transfer_concurrency(None).unwrap(), 3);
        assert_eq!(peer_transfer_concurrency(Some("8")).unwrap(), 8);
        assert!(peer_transfer_concurrency(Some("0")).is_err());
        assert!(peer_transfer_concurrency(Some("33")).is_err());
        assert!(peer_transfer_concurrency(Some("many")).is_err());
    }
}
