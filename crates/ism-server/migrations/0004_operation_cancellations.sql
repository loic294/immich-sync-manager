CREATE TABLE operation_cancellations (
    principal TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (principal, idempotency_key)
);
