ALTER TABLE request_events_v1
    ADD COLUMN IF NOT EXISTS stream_first_output_ms BIGINT NULL,
    ADD COLUMN IF NOT EXISTS stream_inter_output_avg_ms BIGINT NULL,
    ADD COLUMN IF NOT EXISTS provider_session_id TEXT NULL,
    ADD COLUMN IF NOT EXISTS provider_connection_id TEXT NULL;
