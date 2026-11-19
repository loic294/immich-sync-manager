CREATE TABLE sync_runs (
    peer_id TEXT PRIMARY KEY REFERENCES peers(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    started_at TEXT NOT NULL
);
ALTER TABLE transfers ADD COLUMN run_id TEXT NOT NULL DEFAULT '';
ALTER TABLE transfer_history ADD COLUMN run_id TEXT NOT NULL DEFAULT '';
CREATE INDEX transfers_sync_run ON transfers(peer_id,run_id);
CREATE INDEX transfer_history_sync_run ON transfer_history(peer_id,run_id);
CREATE INDEX transfer_history_publication ON transfer_history(operation_id);

INSERT INTO sync_runs (peer_id, run_id, started_at)
SELECT peer_id, 'upgrade:' || peer_id, MIN(queued_at) FROM (
    SELECT peer_id, queued_at FROM transfers
    UNION ALL
    SELECT h.peer_id, h.queued_at FROM transfer_history h JOIN operations o ON o.id = h.operation_id
    WHERE o.status NOT IN ('completed','cancelled')
) GROUP BY peer_id;
UPDATE transfers SET run_id = 'upgrade:' || peer_id;
UPDATE transfer_history SET run_id = 'upgrade:' || peer_id
WHERE operation_id IN (SELECT id FROM operations WHERE status NOT IN ('completed','cancelled'));

CREATE TABLE sync_byte_samples (
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    at INTEGER NOT NULL,
    bytes INTEGER NOT NULL CHECK (bytes > 0)
);
CREATE INDEX sync_byte_samples_time ON sync_byte_samples(at);

-- One coalesced change per transfer, including tombstones. Replacements advance the cursor
-- without retaining a log entry for every progress update.
CREATE TABLE sync_changes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_id TEXT NOT NULL UNIQUE,
    peer_id TEXT NOT NULL
);
CREATE INDEX sync_changes_peer_cursor ON sync_changes(peer_id, seq);
INSERT INTO sync_changes (transfer_id, peer_id)
SELECT id, peer_id FROM transfers UNION ALL SELECT id, peer_id FROM transfer_history;

CREATE TRIGGER sync_transfer_insert AFTER INSERT ON transfers BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id) VALUES (NEW.id,NEW.peer_id);
END;
CREATE TRIGGER sync_transfer_update AFTER UPDATE ON transfers BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id) VALUES (NEW.id,NEW.peer_id);
END;
CREATE TRIGGER sync_transfer_delete AFTER DELETE ON transfers BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id) VALUES (OLD.id,OLD.peer_id);
END;
CREATE TRIGGER sync_history_insert AFTER INSERT ON transfer_history BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id) VALUES (NEW.id,NEW.peer_id);
END;
CREATE TRIGGER sync_history_delete AFTER DELETE ON transfer_history BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id) VALUES (OLD.id,OLD.peer_id);
END;
CREATE TRIGGER sync_publication_update AFTER UPDATE OF status,finished_at ON operations BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id,peer_id)
    SELECT id,peer_id FROM transfer_history WHERE operation_id = NEW.id;
END;
CREATE TRIGGER sync_peer_delete AFTER DELETE ON peers BEGIN
    DELETE FROM sync_changes WHERE peer_id = OLD.id;
END;

CREATE TABLE outgoing_sync (
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    activity_json TEXT NOT NULL CHECK (json_valid(activity_json)),
    PRIMARY KEY (peer_id,id)
);
CREATE TABLE peer_sync_state (
    peer_id TEXT PRIMARY KEY REFERENCES peers(id) ON DELETE CASCADE,
    cursor INTEGER NOT NULL DEFAULT 0,
    progress_json TEXT,
    updated_at TEXT,
    error TEXT
);
