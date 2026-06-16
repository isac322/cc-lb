CREATE TABLE IF NOT EXISTS audit_entries_v1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    route TEXT NOT NULL,
    upstream TEXT NOT NULL,
    model TEXT,
    status INTEGER NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    duration_ms INTEGER NOT NULL,
    agent_label TEXT,
    api_key_id TEXT,
    cost_usd_micros INTEGER,
    limit_violation TEXT,
    admin_action TEXT,
    actor TEXT,
    kind TEXT,
    payload TEXT
);

CREATE TABLE IF NOT EXISTS request_events_v1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_rollups_v2 (
    principal_id TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    request_count INTEGER NOT NULL DEFAULT 0,
    cost_usd_micros INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (principal_id, window_start)
);
