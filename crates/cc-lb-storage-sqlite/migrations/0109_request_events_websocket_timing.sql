ALTER TABLE request_events_v1 ADD COLUMN stream_first_output_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN stream_inter_output_avg_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN provider_session_id TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN provider_connection_id TEXT NULL;
