ALTER TABLE request_events_v1 ADD COLUMN quota_urgency_5h DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_urgency_7d DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_urgency_combined DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_weight_factor DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_cache_multiplier DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_warning_multiplier DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_effective_weight DOUBLE PRECISION;
ALTER TABLE request_events_v1 ADD COLUMN quota_uniform_fallback BOOLEAN;
