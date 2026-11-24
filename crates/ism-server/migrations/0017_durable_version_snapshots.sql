CREATE TABLE recovery_version_snapshots (
    target_id TEXT NOT NULL REFERENCES conflicts(id) ON DELETE CASCADE,
    revision TEXT NOT NULL,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    view_json TEXT NOT NULL CHECK(json_valid(view_json)),
    created_at TEXT NOT NULL,
    PRIMARY KEY(target_id, revision)
);

CREATE TABLE conflict_evidence_jobs (
    target_id TEXT PRIMARY KEY REFERENCES conflicts(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt TEXT NOT NULL DEFAULT '',
    error TEXT,
    error_detail TEXT
);
CREATE INDEX conflict_evidence_jobs_ready ON conflict_evidence_jobs(state, next_attempt);

-- Recovery follows only matching paths, not every event in a share.
CREATE INDEX recovery_event_anchor ON event_log(
    share_name, json_extract(data_json, '$.path'), seq
) WHERE kind IN ('file_changed','file_moved') AND json_valid(data_json);
CREATE INDEX recovery_event_move ON event_log(
    share_name, json_extract(data_json, '$.from'), seq
) WHERE kind IN ('file_moved','folder_moved') AND json_valid(data_json);
CREATE INDEX recovery_event_invalid ON event_log(share_name, seq)
WHERE kind IN ('file_changed','file_moved','folder_created','folder_moved')
  AND CASE WHEN json_valid(data_json) THEN
      json_type(data_json) IS NOT 'object'
      OR json_type(data_json,'$.path') IS NOT 'text'
      OR (json_type(data_json,'$.from') IS NOT NULL
          AND json_type(data_json,'$.from') NOT IN ('text','null'))
      OR (json_type(data_json,'$.vv') IS NOT NULL
          AND json_type(data_json,'$.vv') IS NOT 'object')
      ELSE 1 END;

CREATE TRIGGER conflict_evidence_insert AFTER INSERT ON conflicts
WHEN NEW.status = 'open'
BEGIN
    INSERT INTO conflict_evidence_jobs(target_id) VALUES (NEW.id);
END;

CREATE TRIGGER conflict_evidence_update AFTER UPDATE OF
    status, share_id, rel_path, kind, detail, local_json, remote_json, remote_vv_json, peer_id, remote_from
ON conflicts
WHEN NEW.status = 'open' AND (
    OLD.status != NEW.status OR OLD.share_id IS NOT NEW.share_id OR
    OLD.rel_path IS NOT NEW.rel_path OR OLD.kind IS NOT NEW.kind OR
    OLD.detail IS NOT NEW.detail OR OLD.local_json IS NOT NEW.local_json OR
    OLD.remote_json IS NOT NEW.remote_json OR OLD.remote_vv_json IS NOT NEW.remote_vv_json OR
    OLD.peer_id IS NOT NEW.peer_id OR OLD.remote_from IS NOT NEW.remote_from
)
BEGIN
    INSERT INTO conflict_evidence_jobs(target_id) VALUES (NEW.id)
    ON CONFLICT(target_id) DO UPDATE SET generation = generation + 1,
        state = 'queued', attempts = 0, next_attempt = '', error = NULL, error_detail = NULL;
END;

CREATE TRIGGER conflict_evidence_resolved AFTER UPDATE OF status ON conflicts
WHEN NEW.status = 'resolved' AND OLD.status != 'resolved'
BEGIN
    DELETE FROM recovery_version_snapshots WHERE target_id = NEW.id;
    DELETE FROM conflict_evidence_jobs WHERE target_id = NEW.id;
END;

INSERT INTO conflict_evidence_jobs(target_id)
SELECT id FROM conflicts WHERE status = 'open';
