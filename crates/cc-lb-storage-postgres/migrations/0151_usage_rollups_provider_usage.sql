ALTER TABLE usage_rollups_v2
    ADD COLUMN IF NOT EXISTS cache_write_input_tokens BIGINT,
    ADD COLUMN IF NOT EXISTS cache_write_input_tokens_30m BIGINT,
    ADD COLUMN IF NOT EXISTS cache_write_input_tokens_12h BIGINT,
    ADD COLUMN IF NOT EXISTS cost_cache_write_30m_micros BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_cache_write_12h_micros BIGINT NOT NULL DEFAULT 0;
