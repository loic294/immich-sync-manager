CREATE TABLE share_scan_failures (
    share_id TEXT PRIMARY KEY REFERENCES shares(id) ON DELETE CASCADE,
    reason TEXT NOT NULL CHECK (reason IN ('permission', 'other', 'system')),
    observed_at TEXT NOT NULL
);
