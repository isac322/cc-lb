ALTER TABLE usage_rollups_v2 ADD COLUMN priced_event_count INTEGER NOT NULL DEFAULT 0 CHECK (priced_event_count >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN unpriced_event_count INTEGER NOT NULL DEFAULT 0 CHECK (unpriced_event_count >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN unmetered_event_count INTEGER NOT NULL DEFAULT 0 CHECK (unmetered_event_count >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN effective_cache_write_input_tokens INTEGER CHECK (effective_cache_write_input_tokens IS NULL OR effective_cache_write_input_tokens >= 0);
