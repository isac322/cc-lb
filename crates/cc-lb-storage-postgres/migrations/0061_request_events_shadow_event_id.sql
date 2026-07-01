ALTER TABLE request_events_v1 ADD COLUMN shadow_event_id TEXT NULL;

CREATE INDEX IF NOT EXISTS request_events_v1_shadow_event_id_idx
ON request_events_v1(shadow_event_id) WHERE shadow_event_id IS NOT NULL;
