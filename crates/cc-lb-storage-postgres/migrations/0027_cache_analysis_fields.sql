ALTER TABLE request_events_v1
    ADD COLUMN IF NOT EXISTS key_id TEXT,
    ADD COLUMN IF NOT EXISTS model TEXT,
    ADD COLUMN IF NOT EXISTS upstream_name TEXT,
    ADD COLUMN IF NOT EXISTS cache_state TEXT,
    ADD COLUMN IF NOT EXISTS thread_id TEXT,
    ADD COLUMN IF NOT EXISTS message_id TEXT,
    ADD COLUMN IF NOT EXISTS message_index BIGINT CHECK (message_index IS NULL OR message_index >= 0),
    ADD COLUMN IF NOT EXISTS message_count BIGINT CHECK (message_count IS NULL OR message_count >= 0),
    ADD COLUMN IF NOT EXISTS cache_control_block_count BIGINT CHECK (cache_control_block_count IS NULL OR cache_control_block_count >= 0),
    ADD COLUMN IF NOT EXISTS cache_prefix_hash TEXT,
    ADD COLUMN IF NOT EXISTS input_tokens BIGINT CHECK (input_tokens IS NULL OR input_tokens >= 0),
    ADD COLUMN IF NOT EXISTS output_tokens BIGINT CHECK (output_tokens IS NULL OR output_tokens >= 0),
    ADD COLUMN IF NOT EXISTS cache_creation_input_tokens BIGINT CHECK (cache_creation_input_tokens IS NULL OR cache_creation_input_tokens >= 0),
    ADD COLUMN IF NOT EXISTS cache_read_input_tokens BIGINT CHECK (cache_read_input_tokens IS NULL OR cache_read_input_tokens >= 0);

CREATE INDEX IF NOT EXISTS request_events_v1_thread_ts ON request_events_v1 (thread_id, ts);
CREATE INDEX IF NOT EXISTS request_events_v1_cache_prefix_ts ON request_events_v1 (cache_prefix_hash, ts);
CREATE INDEX IF NOT EXISTS request_events_v1_cache_state_ts ON request_events_v1 (cache_state, ts);

