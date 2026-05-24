CREATE TABLE IF NOT EXISTS config_history_v1 (
    revision BIGINT PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS config_history_v1_created_at ON config_history_v1 (created_at);
