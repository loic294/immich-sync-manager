CREATE INDEX background_activity_outcome
ON background_activity(task_key,finished_at,revision);
DROP INDEX background_activity_key;

CREATE INDEX background_activity_unresolved_outcome
ON background_activity(task_key,finished_at,revision)
WHERE resolved=0 AND status IN ('failed','interrupted');
