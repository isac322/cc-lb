SET LOCAL lock_timeout = '1s';

CREATE TABLE IF NOT EXISTS thread_sticky_subagent_v1 (
    principal_id TEXT NOT NULL,
    observed_session_id TEXT NOT NULL,
    observed_at_unix_secs BIGINT NOT NULL CHECK (observed_at_unix_secs >= 0),
    expires_at_unix_secs BIGINT NOT NULL CHECK (expires_at_unix_secs >= 0),
    PRIMARY KEY (principal_id, observed_session_id)
);

CREATE INDEX IF NOT EXISTS idx_thread_sticky_subagent_v1_expires
    ON thread_sticky_subagent_v1 (expires_at_unix_secs);
