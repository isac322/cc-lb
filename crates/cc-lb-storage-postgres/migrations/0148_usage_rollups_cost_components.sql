SET LOCAL lock_timeout = '1s';

ALTER TABLE usage_rollups_v2
    ADD COLUMN IF NOT EXISTS cost_input_micros BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_output_micros BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_cache_creation_5m_micros BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_cache_creation_1h_micros BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_cache_read_micros BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_components_event_count BIGINT NOT NULL DEFAULT 0;
