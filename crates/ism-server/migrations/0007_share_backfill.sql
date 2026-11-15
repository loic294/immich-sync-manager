-- Shares that must catch up with a peer's earlier state: created when a share becomes active
-- for a peer here, or when the peer announces it enabled the share. `after_path` is the
-- inventory page cursor (paths sorted), so a backfill resumes where it stopped.
CREATE TABLE share_backfills (
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    after_path TEXT NOT NULL DEFAULT '',
    requested_at TEXT NOT NULL,
    PRIMARY KEY (share_id, peer_id)
);

-- The source path of a remote move that conflicted, so resolving it can apply the move.
ALTER TABLE conflicts ADD COLUMN remote_from TEXT;
