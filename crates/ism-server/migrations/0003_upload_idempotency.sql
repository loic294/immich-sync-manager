ALTER TABLE uploads ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX uploads_principal_idempotency
    ON uploads (principal, idempotency_key);
