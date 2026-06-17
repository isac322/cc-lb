ALTER TABLE request_events_v1 ADD COLUMN principal_id BLOB;
ALTER TABLE request_events_v1 ADD COLUMN created_at INTEGER;
ALTER TABLE request_events_v1 ADD COLUMN key_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN model TEXT;
ALTER TABLE request_events_v1 ADD COLUMN upstream_name TEXT;
ALTER TABLE request_events_v1 ADD COLUMN cache_state TEXT;
ALTER TABLE request_events_v1 ADD COLUMN thread_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN message_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN message_index INTEGER CHECK (message_index IS NULL OR message_index >= 0);
ALTER TABLE request_events_v1 ADD COLUMN message_count INTEGER CHECK (message_count IS NULL OR message_count >= 0);
ALTER TABLE request_events_v1 ADD COLUMN cache_control_block_count INTEGER CHECK (cache_control_block_count IS NULL OR cache_control_block_count >= 0);
ALTER TABLE request_events_v1 ADD COLUMN cache_breakpoints TEXT NOT NULL DEFAULT '[]';
ALTER TABLE request_events_v1 ADD COLUMN cache_prefix_hash TEXT;
ALTER TABLE request_events_v1 ADD COLUMN input_tokens INTEGER CHECK (input_tokens IS NULL OR input_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN output_tokens INTEGER CHECK (output_tokens IS NULL OR output_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN cache_creation_input_tokens INTEGER CHECK (cache_creation_input_tokens IS NULL OR cache_creation_input_tokens >= 0);
ALTER TABLE request_events_v1 ADD COLUMN cache_read_input_tokens INTEGER CHECK (cache_read_input_tokens IS NULL OR cache_read_input_tokens >= 0);

-- Keep sqlite's request_id/event_type/upstream_id columns as local operational
-- aids; this migration adds postgres-compatible analytics columns beside them.
UPDATE request_events_v1
   SET principal_id = COALESCE(
           CASE WHEN json_valid(payload) THEN json_extract(payload, '$.principal_id') END,
           (SELECT audit_log_v1.principal_id
              FROM audit_log_v1
             WHERE audit_log_v1.request_id = request_events_v1.request_id
             ORDER BY audit_log_v1.id ASC
             LIMIT 1)
       ),
       created_at = COALESCE(created_at, ts),
       key_id = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.key_id') END,
       model = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.model') END,
       upstream_name = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.upstream_name') END,
       cache_state = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.cache_state') END,
       thread_id = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.thread_id') END,
       message_id = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.message_id') END,
       message_index = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.message_index') AS INTEGER) END,
       message_count = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.message_count') AS INTEGER) END,
       cache_control_block_count = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.cache_control_block_count') AS INTEGER) END,
       cache_breakpoints = COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.cache_breakpoints') END, '[]'),
       cache_prefix_hash = CASE WHEN json_valid(payload) THEN json_extract(payload, '$.cache_prefix_hash') END,
       input_tokens = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.input_tokens') AS INTEGER) END,
       output_tokens = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.output_tokens') AS INTEGER) END,
       cache_creation_input_tokens = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.cache_creation_input_tokens') AS INTEGER) END,
       cache_read_input_tokens = CASE WHEN json_valid(payload) THEN CAST(json_extract(payload, '$.cache_read_input_tokens') AS INTEGER) END;

UPDATE request_events_v1
   SET created_at = strftime('%s', 'now')
 WHERE created_at IS NULL;

CREATE INDEX IF NOT EXISTS request_events_v1_thread_ts
    ON request_events_v1 (thread_id, ts);
CREATE INDEX IF NOT EXISTS request_events_v1_cache_prefix_ts
    ON request_events_v1 (cache_prefix_hash, ts);
CREATE INDEX IF NOT EXISTS request_events_v1_cache_state_ts
    ON request_events_v1 (cache_state, ts);
