-- Materialize the JSON-array predicate once per request change, not per UI refresh.
-- NULL marks unreadable JSON so the reader still evaluates it and fails closed.
ALTER TABLE uploads ADD COLUMN has_original INTEGER;
UPDATE uploads SET has_original = CASE
    WHEN NOT json_valid(request_json) THEN NULL
    WHEN json_extract(request_json,'$.existing_original.name') IS NOT NULL THEN 1
    WHEN EXISTS(SELECT 1 FROM json_each(request_json,'$.files') f
                WHERE f.type='text' AND NOT json_valid(f.value)) THEN NULL
    ELSE EXISTS(SELECT 1 FROM json_each(request_json,'$.files') f
                WHERE json_extract(f.value,'$.role')='original')
END;

CREATE TRIGGER uploads_original_insert AFTER INSERT ON uploads BEGIN
    UPDATE uploads SET has_original =
        CASE
            WHEN NOT json_valid(NEW.request_json) THEN NULL
            WHEN json_extract(NEW.request_json,'$.existing_original.name') IS NOT NULL THEN 1
            WHEN EXISTS(SELECT 1 FROM json_each(NEW.request_json,'$.files') f
                        WHERE f.type='text' AND NOT json_valid(f.value)) THEN NULL
            ELSE EXISTS(SELECT 1 FROM json_each(NEW.request_json,'$.files') f
                        WHERE json_extract(f.value,'$.role')='original')
        END
    WHERE id=NEW.id;
END;
CREATE TRIGGER uploads_original_update AFTER UPDATE OF request_json ON uploads
WHEN OLD.request_json IS NOT NEW.request_json BEGIN
    UPDATE uploads SET has_original =
        CASE
            WHEN NOT json_valid(NEW.request_json) THEN NULL
            WHEN json_extract(NEW.request_json,'$.existing_original.name') IS NOT NULL THEN 1
            WHEN EXISTS(SELECT 1 FROM json_each(NEW.request_json,'$.files') f
                        WHERE f.type='text' AND NOT json_valid(f.value)) THEN NULL
            ELSE EXISTS(SELECT 1 FROM json_each(NEW.request_json,'$.files') f
                        WHERE json_extract(f.value,'$.role')='original')
        END
    WHERE id=NEW.id;
END;

CREATE INDEX uploads_original_evidence ON uploads(created_at,operation_id,id)
WHERE has_original IS NOT 0;
-- No JSON in the index predicate: writes of unreadable results must still succeed.
CREATE INDEX operation_items_published_uploads ON operation_items(operation_id)
WHERE kind='publish_upload' AND status='done' AND result_json IS NOT NULL;
CREATE INDEX watch_snapshot_covering ON watch_snapshot(root_id,rel_path,size,mtime_ns,inode);

-- Changes and their invalidation commit together, including writes outside the peer worker.
CREATE TABLE outgoing_sync_revision (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    revision INTEGER NOT NULL,
    deleted_revision INTEGER NOT NULL
);
INSERT INTO outgoing_sync_revision VALUES(1,0,0);
ALTER TABLE outgoing_sync ADD COLUMN evidence_revision INTEGER NOT NULL DEFAULT 0;
CREATE INDEX outgoing_sync_evidence_revision ON outgoing_sync(evidence_revision);
CREATE TRIGGER outgoing_sync_revision_insert AFTER INSERT ON outgoing_sync BEGIN
    UPDATE outgoing_sync_revision SET revision=revision+1 WHERE singleton=1;
    UPDATE outgoing_sync SET evidence_revision=
        (SELECT revision FROM outgoing_sync_revision WHERE singleton=1)
    WHERE peer_id=NEW.peer_id AND id=NEW.id;
END;
CREATE TRIGGER outgoing_sync_revision_delete AFTER DELETE ON outgoing_sync BEGIN
    UPDATE outgoing_sync_revision SET revision=revision+1,deleted_revision=revision+1
    WHERE singleton=1;
END;
CREATE TRIGGER outgoing_sync_revision_update AFTER UPDATE ON outgoing_sync
WHEN OLD.peer_id IS NOT NEW.peer_id OR OLD.id IS NOT NEW.id
  OR OLD.activity_json IS NOT NEW.activity_json BEGIN
    UPDATE outgoing_sync_revision SET revision=revision+1,
        deleted_revision=CASE WHEN OLD.peer_id IS NOT NEW.peer_id OR OLD.id IS NOT NEW.id
                              THEN revision+1 ELSE deleted_revision END
    WHERE singleton=1;
    UPDATE outgoing_sync SET evidence_revision=
        (SELECT revision FROM outgoing_sync_revision WHERE singleton=1)
    WHERE peer_id=NEW.peer_id AND id=NEW.id;
END;
