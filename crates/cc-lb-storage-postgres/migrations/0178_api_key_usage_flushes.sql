SET LOCAL lock_timeout = '1s';

CREATE TABLE IF NOT EXISTS api_key_usage_flushes_v1 (
    flush_id UUID PRIMARY KEY,
    applied_at_unix_secs BIGINT NOT NULL CHECK (applied_at_unix_secs >= 0)
);

CREATE INDEX IF NOT EXISTS idx_api_key_usage_flushes_v1_applied_at
    ON api_key_usage_flushes_v1 (applied_at_unix_secs);
