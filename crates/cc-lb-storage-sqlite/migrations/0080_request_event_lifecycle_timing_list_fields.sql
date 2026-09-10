ALTER TABLE request_events_v1 ADD COLUMN list_request_body_read_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_request_body_bytes INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_finalize_ms INTEGER NULL;
