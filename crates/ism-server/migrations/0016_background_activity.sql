CREATE TABLE background_activity (
    id TEXT PRIMARY KEY,
    task_key TEXT NOT NULL,
    status TEXT NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    activity_json TEXT NOT NULL
);
CREATE INDEX background_activity_current ON background_activity(status, resolved);
CREATE INDEX background_activity_key ON background_activity(task_key);
CREATE INDEX background_activity_history ON background_activity(finished_at DESC, id DESC);
