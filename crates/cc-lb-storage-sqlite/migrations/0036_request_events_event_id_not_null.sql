-- no-transaction
BEGIN TRANSACTION;

DROP TABLE IF EXISTS request_events_v1_new;

CREATE TABLE request_events_v1_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    upstream_id TEXT,
    payload TEXT NOT NULL,
    principal_id BLOB,
    created_at INTEGER,
    key_id TEXT,
    model TEXT,
    upstream_name TEXT,
    cache_state TEXT,
    thread_id TEXT,
    message_id TEXT,
    message_index INTEGER CHECK (message_index IS NULL OR message_index >= 0),
    message_count INTEGER CHECK (message_count IS NULL OR message_count >= 0),
    cache_control_block_count INTEGER CHECK (cache_control_block_count IS NULL OR cache_control_block_count >= 0),
    cache_breakpoints TEXT NOT NULL DEFAULT '[]',
    cache_prefix_hash TEXT,
    input_tokens INTEGER CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens IS NULL OR output_tokens >= 0),
    cache_creation_input_tokens INTEGER CHECK (cache_creation_input_tokens IS NULL OR cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER CHECK (cache_read_input_tokens IS NULL OR cache_read_input_tokens >= 0),
    event_id TEXT NOT NULL UNIQUE,
    error_code TEXT NULL,
    upstream_error_type TEXT NULL,
    upstream_error_message TEXT NULL,
    thinking_tokens INTEGER NULL,
    web_search_requests INTEGER NULL,
    web_fetch_requests INTEGER NULL,
    service_tier TEXT NULL,
    inference_geo TEXT NULL,
    cache_creation_input_tokens_5m INTEGER NULL,
    cache_creation_input_tokens_1h INTEGER NULL
);

INSERT INTO request_events_v1_new (
    id, request_id, ts, event_type, upstream_id, payload, principal_id, created_at,
    key_id, model, upstream_name, cache_state, thread_id, message_id, message_index,
    message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash,
    input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens,
    event_id, error_code, upstream_error_type, upstream_error_message, thinking_tokens,
    web_search_requests, web_fetch_requests, service_tier, inference_geo,
    cache_creation_input_tokens_5m, cache_creation_input_tokens_1h
)
SELECT
    id, request_id, ts, event_type, upstream_id, payload, principal_id, created_at,
    key_id, model, upstream_name, cache_state, thread_id, message_id, message_index,
    message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash,
    input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens,
    COALESCE(event_id, printf('%d-legacy-%d', id, ts)) AS event_id,
    error_code, upstream_error_type, upstream_error_message, thinking_tokens,
    web_search_requests, web_fetch_requests, service_tier, inference_geo,
    cache_creation_input_tokens_5m, cache_creation_input_tokens_1h
FROM request_events_v1;

DROP TABLE request_events_v1;
ALTER TABLE request_events_v1_new RENAME TO request_events_v1;

CREATE INDEX IF NOT EXISTS request_events_v1_upstream_id_idx
    ON request_events_v1 (upstream_id);
CREATE INDEX IF NOT EXISTS request_events_v1_thread_ts
    ON request_events_v1 (thread_id, ts);
CREATE INDEX IF NOT EXISTS request_events_v1_cache_prefix_ts
    ON request_events_v1 (cache_prefix_hash, ts);
CREATE INDEX IF NOT EXISTS request_events_v1_cache_state_ts
    ON request_events_v1 (cache_state, ts);

COMMIT;
