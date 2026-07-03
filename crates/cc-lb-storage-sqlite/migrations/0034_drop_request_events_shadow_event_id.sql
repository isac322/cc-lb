DROP INDEX IF EXISTS request_events_v1_shadow_event_id_idx;
ALTER TABLE request_events_v1 DROP COLUMN shadow_event_id;
