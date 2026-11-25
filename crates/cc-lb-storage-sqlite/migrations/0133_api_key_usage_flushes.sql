CREATE TABLE IF NOT EXISTS api_key_usage_flushes_v1 (
    flush_id TEXT PRIMARY KEY,
    applied_at_unix_secs INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_api_key_usage_flushes_v1_applied_at
    ON api_key_usage_flushes_v1 (applied_at_unix_secs);
