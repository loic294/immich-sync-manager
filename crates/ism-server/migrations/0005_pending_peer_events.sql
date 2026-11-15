CREATE TABLE pending_peer_events (
    operation_id TEXT PRIMARY KEY REFERENCES operations(id) ON DELETE RESTRICT,
    peer_id TEXT NOT NULL UNIQUE REFERENCES peers(id) ON DELETE RESTRICT,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE RESTRICT,
    origin TEXT NOT NULL,
    origin_seq INTEGER NOT NULL,
    expected_cursor INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    UNIQUE (origin, origin_seq)
);
