-- An operation whose Immich server is unreachable is put back in the queue for later instead of
-- failing or holding up other work.
ALTER TABLE operations ADD COLUMN retry_after TEXT;
ALTER TABLE operations ADD COLUMN deferrals INTEGER NOT NULL DEFAULT 0;
