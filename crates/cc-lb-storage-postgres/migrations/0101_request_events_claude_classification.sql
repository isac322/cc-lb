ALTER TABLE request_events_v1
    ADD COLUMN observed_session_id TEXT,
    ADD COLUMN request_kind TEXT;
