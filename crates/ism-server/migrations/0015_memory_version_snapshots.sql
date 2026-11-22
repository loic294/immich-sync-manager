-- Version review snapshots are short-lived and now kept in memory, so comparing
-- versions never needs a database write.
DROP TABLE IF EXISTS recovery_version_snapshots;
