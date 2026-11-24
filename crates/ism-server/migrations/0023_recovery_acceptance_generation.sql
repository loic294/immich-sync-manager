-- A receiver snapshot may clear only acceptances recorded before its request started.
ALTER TABLE peer_recovery_state ADD COLUMN acceptance_generation INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(acceptance_generation) = 'integer' AND acceptance_generation >= 0);
ALTER TABLE outgoing_recovery_requests ADD COLUMN accepted_generation INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(accepted_generation) = 'integer' AND accepted_generation >= 0);
