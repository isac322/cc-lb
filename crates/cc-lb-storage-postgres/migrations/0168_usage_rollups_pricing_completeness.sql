SET LOCAL lock_timeout = '1s';

ALTER TABLE usage_rollups_v2
    ADD COLUMN IF NOT EXISTS priced_event_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unpriced_event_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unmetered_event_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS effective_cache_write_input_tokens BIGINT;
