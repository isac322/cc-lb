CREATE TABLE IF NOT EXISTS anthropic_compatibility_kv_v1 (
    key                          TEXT PRIMARY KEY,
    value                        TEXT NOT NULL,
    last_updated_at_unix_secs    BIGINT NOT NULL CHECK (last_updated_at_unix_secs >= 0),
    last_attempt_at_unix_secs    BIGINT NOT NULL CHECK (last_attempt_at_unix_secs >= 0),
    last_error                   TEXT,
    source_url                   TEXT
);

CREATE INDEX IF NOT EXISTS anthropic_compatibility_kv_v1_last_updated_idx
    ON anthropic_compatibility_kv_v1 (last_updated_at_unix_secs);
