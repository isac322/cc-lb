ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS source_kind TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS source_ref_id TEXT NULL;
