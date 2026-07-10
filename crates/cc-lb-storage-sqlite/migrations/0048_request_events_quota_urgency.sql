ALTER TABLE request_events_v1 ADD COLUMN quota_urgency_5h REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_urgency_7d REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_urgency_combined REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_weight_factor REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_cache_multiplier REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_warning_multiplier REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_effective_weight REAL;
ALTER TABLE request_events_v1 ADD COLUMN quota_uniform_fallback INTEGER
    CHECK (quota_uniform_fallback IN (0, 1));
