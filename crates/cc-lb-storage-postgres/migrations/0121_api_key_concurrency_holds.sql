SET LOCAL lock_timeout = '1s';

CREATE TABLE IF NOT EXISTS api_key_concurrency_holds_v1 (
    hold_id UUID PRIMARY KEY,
    key_id TEXT NOT NULL,
    writer_epoch UUID NOT NULL,
    acquired_at_unix_secs BIGINT NOT NULL CHECK (acquired_at_unix_secs >= 0)
);

CREATE INDEX IF NOT EXISTS api_key_concurrency_holds_v1_key_id_idx
    ON api_key_concurrency_holds_v1 (key_id);
