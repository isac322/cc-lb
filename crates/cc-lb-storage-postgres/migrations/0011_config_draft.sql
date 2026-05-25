CREATE TABLE IF NOT EXISTS config_draft_v1 (
    id TEXT PRIMARY KEY CHECK (id = 'singleton'),
    config JSONB NOT NULL,
    revision BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
