-- Pending scoped work is coalesced per exact path and file/folder family at admission, so
-- repeated changes no longer add rows. The key is derived from the stored request, so rows
-- written before this migration get the same key as new ones without being rewritten.
ALTER TABLE scan_work ADD COLUMN work_key TEXT GENERATED ALWAYS AS (
  CASE json_extract(work_json, '$.kind') WHEN 'Folder' THEN 'folder:' ELSE 'file:' END
  || json_extract(work_json, '$.path')
) VIRTUAL;
CREATE INDEX scan_work_key ON scan_work (library_id, work_key, id);

-- A successful synchronization acknowledges its whole batch with this single value. Rows at or
-- below it are no longer work and are deleted afterwards in small transactions.
ALTER TABLE scan_schedule ADD COLUMN acked_through INTEGER NOT NULL DEFAULT 0;
