-- Terminal observation guarantee + extended Anthropic usage fields.
-- All columns are NULL by default; existing rows keep NULL.
-- The partial UNIQUE INDEX on event_id enforces dedup only for new (NOT NULL) rows
-- so the migration is zero-downtime and requires no backfill.

ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS event_id TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS error_code TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS upstream_error_type TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS upstream_error_message TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS thinking_tokens BIGINT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS web_search_requests BIGINT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS web_fetch_requests BIGINT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS service_tier TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS inference_geo TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS cache_creation_input_tokens_5m BIGINT NULL;
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS cache_creation_input_tokens_1h BIGINT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS request_events_v1_event_id_idx
ON request_events_v1(event_id) WHERE event_id IS NOT NULL;
