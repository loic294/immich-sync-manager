-- Receipt acknowledges durable, validated input, not a successfully applied effect.
ALTER TABLE peers ADD COLUMN received_cursor INTEGER NOT NULL DEFAULT 0;
UPDATE peers SET received_cursor = cursor;

CREATE TABLE peer_event_receipts (
    origin TEXT NOT NULL,
    origin_seq INTEGER NOT NULL,
    share_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (origin, origin_seq)
);
CREATE TABLE peer_inbox (
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    origin TEXT NOT NULL,
    origin_seq INTEGER NOT NULL,
    share_name TEXT NOT NULL,
    event_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'received',
    operation_id TEXT REFERENCES operations(id) ON DELETE RESTRICT,
    error TEXT,
    attempted_at TEXT,
    PRIMARY KEY (peer_id, seq)
);
CREATE INDEX peer_inbox_identity ON peer_inbox(origin, origin_seq);
CREATE INDEX peer_inbox_operation ON peer_inbox(operation_id);
CREATE TABLE peer_inbox_dependencies (
    peer_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    depends_peer_id TEXT NOT NULL,
    depends_seq INTEGER NOT NULL,
    PRIMARY KEY (peer_id, seq, depends_peer_id, depends_seq),
    FOREIGN KEY (peer_id, seq) REFERENCES peer_inbox(peer_id, seq) ON DELETE CASCADE,
    FOREIGN KEY (depends_peer_id, depends_seq) REFERENCES peer_inbox(peer_id, seq) ON DELETE CASCADE
);

-- Rebuild rather than dropping the old unique peer index (a SQLite autoindex).
DROP TRIGGER sync_pending_event_insert;
DROP TRIGGER sync_pending_event_delete;
DROP TRIGGER sync_pending_operation_update;
ALTER TABLE pending_peer_events RENAME TO pending_peer_events_old;
CREATE TABLE pending_peer_events (
    operation_id TEXT PRIMARY KEY REFERENCES operations(id) ON DELETE RESTRICT,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE RESTRICT,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE RESTRICT,
    origin TEXT NOT NULL,
    origin_seq INTEGER NOT NULL,
    expected_cursor INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    UNIQUE (origin, origin_seq)
);
INSERT INTO pending_peer_events SELECT * FROM pending_peer_events_old;
DROP TABLE pending_peer_events_old;
CREATE INDEX pending_peer_events_peer ON pending_peer_events(peer_id);
CREATE TRIGGER sync_pending_event_insert AFTER INSERT ON pending_peer_events BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    VALUES ('operation:' || NEW.operation_id || ':0', NEW.peer_id);
END;
CREATE TRIGGER sync_pending_event_delete AFTER DELETE ON pending_peer_events BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    VALUES ('operation:' || OLD.operation_id || ':0', OLD.peer_id);
END;
CREATE TRIGGER sync_pending_operation_update AFTER UPDATE OF status,finished_at ON operations BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    SELECT 'operation:' || operation_id || ':' || idx, peer_id
    FROM pending_peer_ops WHERE operation_id = NEW.id;
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    SELECT 'operation:' || operation_id || ':0', peer_id
    FROM pending_peer_events WHERE operation_id = NEW.id;
END;

-- Preserve in-flight folder receipts across an upgrade, without claiming their effects.
INSERT INTO peer_inbox(peer_id, seq, origin, origin_seq, share_name, event_json, status, operation_id)
SELECT peer_id, json_extract(event_json, '$.seq'), origin, origin_seq,
       json_extract(event_json, '$.share'), event_json, 'applying', operation_id
FROM pending_peer_events;
INSERT OR IGNORE INTO peer_event_receipts(origin, origin_seq, share_name, kind, data_json)
SELECT origin, origin_seq, json_extract(event_json, '$.share'),
       json_extract(event_json, '$.kind'), json_extract(event_json, '$.data')
FROM pending_peer_events;
UPDATE peers SET received_cursor = MAX(received_cursor,
    COALESCE((SELECT MAX(seq) FROM peer_inbox WHERE peer_id = peers.id), 0));

-- Inventory pagination is durable even when individual entries cannot yet apply.
CREATE TABLE peer_backfill_inbox (
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    entry_json TEXT NOT NULL,
    next_entry_json TEXT,
    operation_id TEXT REFERENCES operations(id) ON DELETE RESTRICT,
    error TEXT,
    attempted_at TEXT,
    PRIMARY KEY (peer_id, share_id, path)
);
ALTER TABLE share_backfills ADD COLUMN attempted_at TEXT;

CREATE TABLE completed_peer_bookkeeping (
    operation_id TEXT PRIMARY KEY REFERENCES operations(id) ON DELETE CASCADE
);

CREATE TRIGGER inbox_pending_event_insert AFTER INSERT ON pending_peer_events BEGIN
    UPDATE peer_inbox SET operation_id = NEW.operation_id, status = 'applying', error = NULL
    WHERE origin = NEW.origin AND origin_seq = NEW.origin_seq AND status != 'done';
END;
CREATE TRIGGER inbox_pending_operation_retry AFTER INSERT ON pending_peer_ops BEGIN
    UPDATE peer_inbox SET operation_id = NEW.operation_id, error = NULL
    WHERE operation_id IN (
        SELECT operation_id FROM pending_peer_ops
        WHERE operation_id != NEW.operation_id AND peer_id = NEW.peer_id AND share_id = NEW.share_id
          AND kind = NEW.kind AND from_path IS NEW.from_path AND to_path IS NEW.to_path
          AND vv_json = NEW.vv_json
    );
    UPDATE peer_backfill_inbox SET operation_id = NEW.operation_id, error = NULL
    WHERE operation_id IN (
        SELECT operation_id FROM pending_peer_ops
        WHERE operation_id != NEW.operation_id AND peer_id = NEW.peer_id AND share_id = NEW.share_id
          AND kind = NEW.kind AND from_path IS NEW.from_path AND to_path IS NEW.to_path
          AND vv_json = NEW.vv_json
    );
END;
