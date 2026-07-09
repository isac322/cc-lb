ALTER TABLE request_events_v1 ALTER COLUMN tx_id SET DEFAULT pg_current_xact_id();
ALTER TABLE request_events_v1 ALTER COLUMN tx_id SET NOT NULL;
