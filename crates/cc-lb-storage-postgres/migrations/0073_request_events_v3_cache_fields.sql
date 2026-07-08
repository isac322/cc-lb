ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS matched_v3_cache_key TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS breakpoint_content_block_index BIGINT NULL CHECK (breakpoint_content_block_index IS NULL OR breakpoint_content_block_index >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS matched_content_block_index BIGINT NULL CHECK (matched_content_block_index IS NULL OR matched_content_block_index >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS lookback_distance BIGINT NULL CHECK (lookback_distance IS NULL OR lookback_distance >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS predicted_cache_read_tokens BIGINT NULL CHECK (predicted_cache_read_tokens IS NULL OR predicted_cache_read_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS predicted_cache_creation_tokens_5m BIGINT NULL CHECK (predicted_cache_creation_tokens_5m IS NULL OR predicted_cache_creation_tokens_5m >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS predicted_cache_creation_tokens_1h BIGINT NULL CHECK (predicted_cache_creation_tokens_1h IS NULL OR predicted_cache_creation_tokens_1h >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS token_estimate_source TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS cache_value_micros BIGINT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS formula_winner_upstream_id UUID NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS kept_upstream_id UUID NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS wrh_key_source TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS lineage_would_have_predicted_read_tokens BIGINT NULL CHECK (lineage_would_have_predicted_read_tokens IS NULL OR lineage_would_have_predicted_read_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS lineage_would_have_picked_upstream_id UUID NULL;

DROP INDEX IF EXISTS request_events_v1_cache_prefix_ts;
CREATE INDEX IF NOT EXISTS request_events_v1_v3_cache_key_ts
  ON request_events_v1 (matched_v3_cache_key, ts);
