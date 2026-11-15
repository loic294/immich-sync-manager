-- ISM owns file detection: Immich's watcher and scheduled scan stay off.

-- Per-mapping watch settings. Missing rows mean mode 'auto' and a 300 s polling interval.
CREATE TABLE watch_settings (
    root_id TEXT PRIMARY KEY REFERENCES mappings(root_id) ON DELETE CASCADE,
    mode TEXT NOT NULL DEFAULT 'auto' CHECK (mode IN ('auto', 'native', 'poll')),
    poll_interval_secs INTEGER NOT NULL DEFAULT 300 CHECK (poll_interval_secs BETWEEN 60 AND 86400)
);

-- The last known state of every watched file, so restarts and polling passes can diff.
CREATE TABLE watch_snapshot (
    root_id TEXT NOT NULL REFERENCES mappings(root_id) ON DELETE CASCADE,
    rel_path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    inode INTEGER,
    PRIMARY KEY (root_id, rel_path)
);

CREATE TABLE watch_status (
    root_id TEXT PRIMARY KEY REFERENCES mappings(root_id) ON DELETE CASCADE,
    effective_mode TEXT,
    fs_type TEXT,
    last_pass_at TEXT,
    last_change_at TEXT,
    paused INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    files INTEGER NOT NULL DEFAULT 0
);

-- Paths ISM is about to write, so the watcher does not mistake them for outside changes.
CREATE TABLE self_writes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    is_tree INTEGER NOT NULL DEFAULT 0,
    size INTEGER,
    mtime_ns INTEGER,
    expires_at INTEGER NOT NULL
);
CREATE INDEX self_writes_path ON self_writes (path);
CREATE INDEX self_writes_expiry ON self_writes (expires_at);

-- One row per Immich library ISM scans; due_at (unix ms) is set while a scan is pending.
CREATE TABLE scan_schedule (
    library_id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL REFERENCES immich_servers(id) ON DELETE CASCADE,
    due_at INTEGER,
    reasons_json TEXT NOT NULL DEFAULT '[]',
    running INTEGER NOT NULL DEFAULT 0,
    last_started_at TEXT,
    last_finished_at TEXT,
    last_error TEXT
);

-- The previous barrier restored Immich settings after a quiet period. Interrupted
-- reconciliations need an administrator; a queue it paused must be resumed.
UPDATE immich_servers
SET maintenance_json = json_set(
    maintenance_json,
    '$.phase', 'recovery_required',
    '$.unresolved_recoveries', COALESCE(json_extract(maintenance_json, '$.unresolved_recoveries'), 0) + 1,
    '$.message', 'a reconciliation of the previous ISM version was interrupted; check the library and resolve'
)
WHERE json_valid(maintenance_json)
  AND json_extract(maintenance_json, '$.phase') IN ('reconciling', 'restoring');

UPDATE immich_servers
SET maintenance_json = json_set(maintenance_json, '$.queue_paused_by_ism', json('true'))
WHERE json_valid(maintenance_json)
  AND json_array_length(COALESCE(json_extract(maintenance_json, '$.saved.paused_queues'), '[]')) > 0;
