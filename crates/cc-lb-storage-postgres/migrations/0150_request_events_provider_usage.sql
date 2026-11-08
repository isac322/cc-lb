SET LOCAL lock_timeout = '1s';
ALTER TABLE request_events_v1
    ADD COLUMN IF NOT EXISTS cache_write_input_tokens BIGINT,
    ADD COLUMN IF NOT EXISTS cache_write_input_tokens_30m BIGINT,
    ADD COLUMN IF NOT EXISTS cache_write_input_tokens_12h BIGINT,
    ADD COLUMN IF NOT EXISTS list_cost_cache_write_micros BIGINT,
    ADD COLUMN IF NOT EXISTS list_cost_cache_write_30m_micros BIGINT,
    ADD COLUMN IF NOT EXISTS list_cost_cache_write_12h_micros BIGINT;
