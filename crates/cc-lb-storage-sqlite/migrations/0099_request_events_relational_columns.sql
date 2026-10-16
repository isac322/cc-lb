ALTER TABLE request_events_v1 ADD COLUMN principal_kind TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN body_chunk_count INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN body_bytes INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN stream_message_start_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN stream_content_block_start_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN stream_message_stop_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN stream_last_chunk_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN stream_total_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN sse_event_count INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN content_delta_count INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN ping_count INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_http_version TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_request_id TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_content_encoding TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_content_length INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_body_bytes INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_body_end TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_body_error_cause TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_body_error_io_kind TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_body_error_h2_reason TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_stream_warning_type TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN upstream_stream_warning_message TEXT NULL;

UPDATE request_events_v1
SET principal_kind = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.principal_kind') END, principal_kind),
    body_chunk_count = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.body_chunk_count') AS INTEGER) END, body_chunk_count),
    body_bytes = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.body_bytes') AS INTEGER) END, body_bytes),
    stream_message_start_ms = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.stream_message_start_ms') AS INTEGER) END, stream_message_start_ms),
    stream_content_block_start_ms = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.stream_content_block_start_ms') AS INTEGER) END, stream_content_block_start_ms),
    stream_message_stop_ms = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.stream_message_stop_ms') AS INTEGER) END, stream_message_stop_ms),
    stream_last_chunk_ms = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.stream_last_chunk_ms') AS INTEGER) END, stream_last_chunk_ms),
    stream_total_ms = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.stream_total_ms') AS INTEGER) END, stream_total_ms),
    sse_event_count = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.sse_event_count') AS INTEGER) END, sse_event_count),
    content_delta_count = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.content_delta_count') AS INTEGER) END, content_delta_count),
    ping_count = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.ping_count') AS INTEGER) END, ping_count),
    upstream_http_version = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_http_version') END, upstream_http_version),
    upstream_request_id = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_request_id') END, upstream_request_id),
    upstream_content_encoding = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_content_encoding') END, upstream_content_encoding),
    upstream_content_length = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.upstream_content_length') AS INTEGER) END, upstream_content_length),
    upstream_body_bytes = COALESCE(CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.upstream_body_bytes') AS INTEGER) END, upstream_body_bytes),
    upstream_body_end = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_body_end') END, upstream_body_end),
    upstream_body_error_cause = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_body_error_cause') END, upstream_body_error_cause),
    upstream_body_error_io_kind = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_body_error_io_kind') END, upstream_body_error_io_kind),
    upstream_body_error_h2_reason = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_body_error_h2_reason') END, upstream_body_error_h2_reason),
    upstream_stream_warning_type = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_stream_warning_type') END, upstream_stream_warning_type),
    upstream_stream_warning_message = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_stream_warning_message') END, upstream_stream_warning_message)
WHERE payload IS NOT NULL;

ALTER TABLE request_events_v1 DROP COLUMN payload;

DROP INDEX IF EXISTS usage_rollups_v2_interval_tokens_cover_idx;
DROP INDEX IF EXISTS usage_rollups_v2_token_interval_idx;
DROP INDEX IF EXISTS idx_audit_log_actor_subject;
CREATE INDEX IF NOT EXISTS idx_cache_keepalive_decisions_ts ON cache_keepalive_decisions (ts);
CREATE INDEX IF NOT EXISTS idx_cache_keepalive_turns_ts ON cache_keepalive_turns (ts);
