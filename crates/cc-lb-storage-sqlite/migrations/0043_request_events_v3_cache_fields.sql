ALTER TABLE request_events_v1 ADD COLUMN matched_v3_cache_key TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN breakpoint_content_block_index INTEGER NULL CHECK (breakpoint_content_block_index IS NULL OR breakpoint_content_block_index >= 0);
ALTER TABLE request_events_v1 ADD COLUMN matched_content_block_index INTEGER NULL CHECK (matched_content_block_index IS NULL OR matched_content_block_index >= 0);
ALTER TABLE request_events_v1 ADD COLUMN lookback_distance INTEGER NULL CHECK (lookback_distance IS NULL OR lookback_distance >= 0);
ALTER TABLE request_events_v1 ADD COLUMN predicted_cache_read_tokens INTEGER NULL CHECK (predicted_cache_read_tokens IS NULL OR predicted_cache_read_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN predicted_cache_creation_tokens_5m INTEGER NULL CHECK (predicted_cache_creation_tokens_5m IS NULL OR predicted_cache_creation_tokens_5m >= 0);
ALTER TABLE request_events_v1 ADD COLUMN predicted_cache_creation_tokens_1h INTEGER NULL CHECK (predicted_cache_creation_tokens_1h IS NULL OR predicted_cache_creation_tokens_1h >= 0);
ALTER TABLE request_events_v1 ADD COLUMN token_estimate_source TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN cache_value_micros INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN formula_winner_upstream_id TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN kept_upstream_id TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN wrh_key_source TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN lineage_would_have_predicted_read_tokens INTEGER NULL CHECK (lineage_would_have_predicted_read_tokens IS NULL OR lineage_would_have_predicted_read_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN lineage_would_have_picked_upstream_id TEXT NULL;

DROP INDEX IF EXISTS request_events_v1_cache_prefix_ts;
CREATE INDEX IF NOT EXISTS request_events_v1_v3_cache_key_ts
    ON request_events_v1 (matched_v3_cache_key, ts);
