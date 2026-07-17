CREATE TABLE IF NOT EXISTS cache_keepalive_turns (
    source_ref_id TEXT PRIMARY KEY,
    session_key_hash TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    accounting_key_id TEXT,
    upstream_id UUID NOT NULL,
    model TEXT NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cache_creation_input_tokens BIGINT NOT NULL,
    cache_creation_input_tokens_5m BIGINT NOT NULL,
    cache_creation_input_tokens_1h BIGINT NOT NULL,
    cache_read_input_tokens BIGINT NOT NULL,
    cost_micros BIGINT NOT NULL,
    hit_miss TEXT NOT NULL,
    ts BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_keepalive_decisions (
    source_ref_id TEXT PRIMARY KEY,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    generation BIGINT NOT NULL,
    ts BIGINT NOT NULL
);
