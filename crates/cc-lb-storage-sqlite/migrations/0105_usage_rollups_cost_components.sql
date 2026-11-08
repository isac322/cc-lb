ALTER TABLE usage_rollups_v2 ADD COLUMN cost_input_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_input_micros >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_output_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_output_micros >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_cache_creation_5m_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_cache_creation_5m_micros >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_cache_creation_1h_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_cache_creation_1h_micros >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_cache_read_micros INTEGER NOT NULL DEFAULT 0 CHECK (cost_cache_read_micros >= 0);
ALTER TABLE usage_rollups_v2 ADD COLUMN cost_components_event_count INTEGER NOT NULL DEFAULT 0 CHECK (cost_components_event_count >= 0);
