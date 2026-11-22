CREATE TABLE sync_recovery (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('operation', 'transfer', 'conflict')),
    target_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    code TEXT,
    detail TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    resolution TEXT,
    decision_revision TEXT,
    replacement_operation_id TEXT,
    evidence_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (kind, target_id)
);
CREATE INDEX sync_recovery_work ON sync_recovery (state, updated_at);

ALTER TABLE transfers ADD COLUMN source_rel_path TEXT;
ALTER TABLE transfers ADD COLUMN source_vv_json TEXT;
ALTER TABLE transfers ADD COLUMN source_retained_token TEXT;
ALTER TABLE transfers ADD COLUMN restore_parent_operation_id TEXT REFERENCES operations(id);

CREATE TABLE peer_decision_receipts (
    origin TEXT NOT NULL,
    origin_seq INTEGER NOT NULL,
    share_name TEXT NOT NULL,
    evidence_peer_id TEXT NOT NULL,
    PRIMARY KEY(origin,origin_seq)
);

CREATE TABLE sync_recovery_links (
    recovery_id TEXT NOT NULL REFERENCES sync_recovery(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('operation','transfer','conflict')),
    target_id TEXT NOT NULL,
    PRIMARY KEY (recovery_id,kind,target_id)
);

CREATE TABLE outgoing_recovery (
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    view_json TEXT NOT NULL CHECK(json_valid(view_json)),
    PRIMARY KEY(peer_id,kind,target_id)
);
CREATE TABLE peer_recovery_state (
    peer_id TEXT PRIMARY KEY REFERENCES peers(id) ON DELETE CASCADE,
    updated_at TEXT,
    error TEXT
);

CREATE TABLE outgoing_recovery_requests (
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind IN ('operation','transfer','conflict')),
    target_id TEXT NOT NULL,
    resolution TEXT,
    decision_revision TEXT,
    state TEXT NOT NULL DEFAULT 'queued',
    view_json TEXT NOT NULL CHECK(json_valid(view_json)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(peer_id,kind,target_id)
);

CREATE TABLE recovery_version_snapshots (
    target_id TEXT NOT NULL,
    revision TEXT NOT NULL,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    view_json TEXT NOT NULL CHECK(json_valid(view_json)),
    created_at TEXT NOT NULL,
    PRIMARY KEY(target_id,revision)
);

-- Stable, peer-bound retained read identities; no clock expiry strands scheduled transfers.
CREATE TABLE retained_sources (
    token TEXT PRIMARY KEY,
    peer_id TEXT REFERENCES peers(id) ON DELETE CASCADE,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    wanted_json TEXT NOT NULL CHECK(json_valid(wanted_json)),
    locator_json TEXT NOT NULL CHECK(json_valid(locator_json)),
    UNIQUE(peer_id,share_id,wanted_json,locator_json)
);

CREATE TRIGGER sync_recovery_insert AFTER INSERT ON sync_recovery BEGIN
    DELETE FROM sync_changes WHERE transfer_id IN (
        SELECT id FROM transfers WHERE NEW.kind='transfer' AND id=NEW.target_id
        UNION SELECT id FROM transfer_history
            WHERE (NEW.kind='transfer' AND id=NEW.target_id) OR (NEW.kind='operation' AND operation_id=NEW.target_id)
        UNION SELECT 'operation:'||operation_id||':'||idx FROM pending_peer_ops
            WHERE NEW.kind='operation' AND operation_id=NEW.target_id
        UNION SELECT 'operation:'||operation_id||':0' FROM pending_peer_events
            WHERE NEW.kind='operation' AND operation_id=NEW.target_id
    );
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id)
    SELECT id,peer_id FROM transfers WHERE NEW.kind='transfer' AND id=NEW.target_id
    UNION SELECT id,peer_id FROM transfer_history
        WHERE (NEW.kind='transfer' AND id=NEW.target_id) OR (NEW.kind='operation' AND operation_id=NEW.target_id)
    UNION SELECT 'operation:'||operation_id||':'||idx,peer_id FROM pending_peer_ops
        WHERE NEW.kind='operation' AND operation_id=NEW.target_id
    UNION SELECT 'operation:'||operation_id||':0',peer_id FROM pending_peer_events
        WHERE NEW.kind='operation' AND operation_id=NEW.target_id;
END;
CREATE TRIGGER sync_recovery_update AFTER UPDATE ON sync_recovery BEGIN
    DELETE FROM sync_changes WHERE transfer_id IN (
        SELECT id FROM transfers WHERE NEW.kind='transfer' AND id=NEW.target_id
        UNION SELECT id FROM transfer_history
            WHERE (NEW.kind='transfer' AND id=NEW.target_id) OR (NEW.kind='operation' AND operation_id=NEW.target_id)
        UNION SELECT 'operation:'||operation_id||':'||idx FROM pending_peer_ops
            WHERE NEW.kind='operation' AND operation_id=NEW.target_id
        UNION SELECT 'operation:'||operation_id||':0' FROM pending_peer_events
            WHERE NEW.kind='operation' AND operation_id=NEW.target_id
    );
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id)
    SELECT id,peer_id FROM transfers WHERE NEW.kind='transfer' AND id=NEW.target_id
    UNION SELECT id,peer_id FROM transfer_history
        WHERE (NEW.kind='transfer' AND id=NEW.target_id) OR (NEW.kind='operation' AND operation_id=NEW.target_id)
    UNION SELECT 'operation:'||operation_id||':'||idx,peer_id FROM pending_peer_ops
        WHERE NEW.kind='operation' AND operation_id=NEW.target_id
    UNION SELECT 'operation:'||operation_id||':0',peer_id FROM pending_peer_events
        WHERE NEW.kind='operation' AND operation_id=NEW.target_id;
END;
