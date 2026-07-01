CREATE TABLE IF NOT EXISTS effective_config_v1 (
    id TEXT PRIMARY KEY CHECK (id = 'singleton'),
    revision BIGINT NOT NULL,
    config JSONB NOT NULL,
    applied_at BIGINT NOT NULL
);
