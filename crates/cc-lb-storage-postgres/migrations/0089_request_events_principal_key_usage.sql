CREATE INDEX IF NOT EXISTS request_events_v1_principal_key_ts_idx
    ON request_events_v1 (principal_id, key_id, ts);
