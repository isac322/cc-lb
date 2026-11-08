ALTER TABLE usage_rollups_v2 ADD COLUMN cache_write_input_tokens INTEGER;
ALTER TABLE usage_rollups_v2 ADD COLUMN cache_write_input_tokens_30m INTEGER;
ALTER TABLE usage_rollups_v2 ADD COLUMN cache_write_input_tokens_12h INTEGER;
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_cache_write_30m_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_cache_write_30m_micros >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_cache_write_12h_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_cache_write_12h_micros >= 0);
