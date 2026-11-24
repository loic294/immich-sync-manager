CREATE TABLE share_backfill_generation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_generation INTEGER NOT NULL CHECK (last_generation >= 0)
);

INSERT INTO share_backfill_generation (id, last_generation) VALUES (1, 0);

ALTER TABLE share_backfills ADD COLUMN generation INTEGER NOT NULL DEFAULT 0;

CREATE TRIGGER share_backfill_membership_changed
AFTER UPDATE OF peer_ids_json, enabled ON shares
WHEN (OLD.peer_ids_json != NEW.peer_ids_json OR OLD.enabled != NEW.enabled)
 AND EXISTS (SELECT 1 FROM share_backfills WHERE share_id = NEW.id)
BEGIN
    SELECT CASE WHEN (SELECT last_generation FROM share_backfill_generation WHERE id = 1)
                     >= 9223372036854775807
        THEN RAISE(ABORT, 'peer backfill generation exhausted') END;
    UPDATE share_backfill_generation SET last_generation = last_generation + 1 WHERE id = 1;
    UPDATE share_backfills
       SET generation = (SELECT last_generation FROM share_backfill_generation WHERE id = 1),
           after_path = '',
           attempted_at = NULL
     WHERE share_id = NEW.id;
END;
