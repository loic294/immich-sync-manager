-- Existing shares retain their history; newly created shares must not replay a previous
-- share's paths when its name is reused for a different folder.
ALTER TABLE shares ADD COLUMN event_start_seq INTEGER NOT NULL DEFAULT 0;
