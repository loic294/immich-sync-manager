CREATE TABLE folder_review_manifests (
    id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    root_id TEXT NOT NULL REFERENCES mappings(root_id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('building', 'ready')),
    revision TEXT,
    entry_count INTEGER NOT NULL DEFAULT 0,
    expires_at TEXT NOT NULL,
    CHECK ((state = 'ready') = (revision IS NOT NULL))
);

CREATE INDEX folder_review_expiry ON folder_review_manifests(expires_at);

CREATE TABLE folder_review_entries (
    manifest_id TEXT NOT NULL REFERENCES folder_review_manifests(id) ON DELETE CASCADE,
    name TEXT NOT NULL COLLATE BINARY,
    sha256 TEXT,
    size INTEGER,
    CHECK ((sha256 IS NULL AND size IS NULL) OR
           (sha256 IS NOT NULL AND size IS NOT NULL AND size >= 0)),
    PRIMARY KEY(manifest_id, name)
) WITHOUT ROWID;

CREATE TRIGGER folder_review_entries_insert BEFORE INSERT ON folder_review_entries
WHEN (SELECT state FROM folder_review_manifests WHERE id = NEW.manifest_id) = 'ready'
BEGIN
    SELECT RAISE(ABORT, 'ready folder review is immutable');
END;

CREATE TRIGGER folder_review_entries_update BEFORE UPDATE ON folder_review_entries
BEGIN
    SELECT RAISE(ABORT, 'folder review entries cannot be updated');
END;

CREATE TRIGGER folder_review_entries_delete BEFORE DELETE ON folder_review_entries
WHEN EXISTS (SELECT 1 FROM folder_review_manifests
             WHERE id = OLD.manifest_id AND state = 'ready')
BEGIN
    SELECT RAISE(ABORT, 'ready folder review is immutable');
END;

CREATE TRIGGER folder_review_ready_update BEFORE UPDATE ON folder_review_manifests
WHEN OLD.state = 'ready'
BEGIN
    SELECT RAISE(ABORT, 'ready folder review is immutable');
END;
