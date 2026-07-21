CREATE TABLE cache_keepalive_turns (
    source_ref_id TEXT PRIMARY KEY,
    session_key_hash TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    accounting_key_id TEXT,
    upstream_id TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cache_creation_input_tokens INTEGER NOT NULL,
    cache_creation_input_tokens_5m INTEGER NOT NULL,
    cache_creation_input_tokens_1h INTEGER NOT NULL,
    cache_read_input_tokens INTEGER NOT NULL,
    cost_micros INTEGER NOT NULL,
    hit_miss TEXT NOT NULL,
    ts INTEGER NOT NULL
);

CREATE TABLE cache_keepalive_decisions (
    source_ref_id TEXT PRIMARY KEY,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    generation INTEGER NOT NULL,
    ts INTEGER NOT NULL
);
