-- Persist exact Immich paths until their debounced synchronization succeeds.
CREATE TABLE scan_work (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    library_id TEXT NOT NULL REFERENCES scan_schedule(library_id) ON DELETE CASCADE,
    work_json TEXT NOT NULL
);
CREATE INDEX scan_work_library ON scan_work (library_id, id);
ALTER TABLE scan_schedule ADD COLUMN running_through INTEGER;

-- Old pending requests have no recoverable scope. Never replay them as full scans.
UPDATE scan_schedule
SET due_at = NULL, running = 0,
    last_error = 'legacy scan request has no folder scope; request a scoped synchronization again'
WHERE due_at IS NOT NULL OR running = 1;
