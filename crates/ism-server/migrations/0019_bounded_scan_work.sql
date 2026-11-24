-- Pending scoped work is coalesced per exact path and file/folder family at admission, so
-- repeated changes no longer add rows. Rows written before this migration have no key; the
-- scheduler synchronizes them as their own batch before any keyed work.
ALTER TABLE scan_work ADD COLUMN work_key TEXT;
CREATE INDEX scan_work_key ON scan_work (library_id, work_key, id) WHERE work_key IS NOT NULL;
CREATE INDEX scan_work_legacy ON scan_work (library_id, id) WHERE work_key IS NULL;

-- A successful synchronization acknowledges its whole batch with this single value. Rows at or
-- below it are no longer work and are deleted afterwards in small transactions.
ALTER TABLE scan_schedule ADD COLUMN acked_through INTEGER NOT NULL DEFAULT 0;
