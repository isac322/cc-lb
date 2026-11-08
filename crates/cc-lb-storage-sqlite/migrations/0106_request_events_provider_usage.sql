ALTER TABLE request_events_v1 ADD COLUMN cache_write_input_tokens INTEGER;
ALTER TABLE request_events_v1 ADD COLUMN cache_write_input_tokens_30m INTEGER;
ALTER TABLE request_events_v1 ADD COLUMN cache_write_input_tokens_12h INTEGER;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_cache_write_micros INTEGER;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_cache_write_30m_micros INTEGER;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_cache_write_12h_micros INTEGER;
