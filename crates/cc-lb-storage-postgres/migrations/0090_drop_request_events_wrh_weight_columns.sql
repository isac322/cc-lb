ALTER TABLE request_events_v1 DROP COLUMN IF EXISTS wrh_key_source;
ALTER TABLE request_events_v1 DROP COLUMN IF EXISTS quota_weight_factor;
ALTER TABLE request_events_v1 DROP COLUMN IF EXISTS quota_cache_multiplier;
ALTER TABLE request_events_v1 DROP COLUMN IF EXISTS quota_effective_weight;
ALTER TABLE request_events_v1 DROP COLUMN IF EXISTS quota_uniform_fallback;
