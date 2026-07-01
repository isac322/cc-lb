CREATE TABLE IF NOT EXISTS effective_config_v1 (
    id TEXT PRIMARY KEY CHECK (id = 'singleton'),
    revision INTEGER NOT NULL,
    config TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);
