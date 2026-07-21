ALTER TABLE cache_keepalive_sessions
    ADD COLUMN accounting_key_id TEXT,
    ADD COLUMN running_since_unix_secs BIGINT;

ALTER TABLE cache_keepalive_sessions
    DROP CONSTRAINT cache_keepalive_sessions_enqueue_state_check,
    ADD CONSTRAINT cache_keepalive_sessions_enqueue_state_check
        CHECK (enqueue_state IN ('pending', 'enqueued', 'running'));
