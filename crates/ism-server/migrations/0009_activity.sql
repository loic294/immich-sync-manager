ALTER TABLE operations ADD COLUMN handled_at TEXT;
ALTER TABLE operation_items ADD COLUMN progress_json TEXT;

CREATE TABLE transfer_history (
    id TEXT PRIMARY KEY,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    rel_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    queued_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    operation_id TEXT REFERENCES operations(id)
);
CREATE INDEX transfer_history_finished ON transfer_history(finished_at DESC, id);
CREATE INDEX transfers_queue_order ON transfers(status, queued_at, id);
CREATE INDEX operations_activity_order ON operations(created_at DESC, id);
