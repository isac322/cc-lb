-- no-transaction
-- Materialize the remaining request-event latency, streaming, and abnormal
-- stream termination fields as typed relational columns, backfill existing
-- rows in bounded 2,000-row batches that commit independently and mark progress
-- by setting payload = NULL, and then drop the payload column.
DO $$
DECLARE
    v_rows INTEGER;
BEGIN
    SET LOCAL lock_timeout = '1s';

    ALTER TABLE request_events_v1
        ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT '',
        ADD COLUMN IF NOT EXISTS principal_kind TEXT NULL,
        ADD COLUMN IF NOT EXISTS duration_ms BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS auth_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS route_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS limit_reserve_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS bulkhead_wait_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS dns_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS connect_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS connection_reused BOOLEAN NULL,
        ADD COLUMN IF NOT EXISTS proxy_setup_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS shape_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS sign_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS upstream_ttfb_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS upstream_body_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS finalize_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS json_parse_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS cache_structure_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS cache_token_key_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS cache_count_lookup_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS cache_tokenizer_queue_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS cache_serialize_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS cache_tokenize_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS prepare_signer_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS request_body_read_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS request_body_bytes BIGINT NULL,
        ADD COLUMN IF NOT EXISTS request_body_first_chunk_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS request_body_receive_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS request_body_wait_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS request_body_process_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS request_body_chunk_count BIGINT NULL,
        ADD COLUMN IF NOT EXISTS response_body_wait_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS response_body_process_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS response_body_downstream_poll_gap_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS retry_overhead_ms DOUBLE PRECISION NULL,
        ADD COLUMN IF NOT EXISTS body_chunk_count BIGINT NULL,
        ADD COLUMN IF NOT EXISTS body_bytes BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_message_start_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_content_block_start_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_first_content_delta_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_last_content_delta_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_message_stop_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_last_chunk_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS stream_total_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS sse_event_count BIGINT NULL,
        ADD COLUMN IF NOT EXISTS content_delta_count BIGINT NULL,
        ADD COLUMN IF NOT EXISTS ping_count BIGINT NULL,
        ADD COLUMN IF NOT EXISTS inter_token_avg_ms BIGINT NULL,
        ADD COLUMN IF NOT EXISTS upstream_http_version TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_request_id TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_content_encoding TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_content_length BIGINT NULL,
        ADD COLUMN IF NOT EXISTS upstream_body_bytes BIGINT NULL,
        ADD COLUMN IF NOT EXISTS upstream_body_end TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_body_error_cause TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_body_error_io_kind TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_body_error_h2_reason TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_stream_warning_type TEXT NULL,
        ADD COLUMN IF NOT EXISTS upstream_stream_warning_message TEXT NULL;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'request_events_v1'
          AND column_name = 'payload'
    ) THEN
        COMMIT;

        IF EXISTS (SELECT 1 FROM request_events_v1 WHERE payload IS NOT NULL LIMIT 1) THEN
            LOOP
                SET LOCAL lock_timeout = '1s';

                WITH batch AS (
                    SELECT
                        seq,
                        CASE
                            WHEN octet_length(payload) > 0
                                 AND left(convert_from(payload, 'UTF8'), 1) = '{'
                            THEN convert_from(payload, 'UTF8')::jsonb
                            ELSE '{}'::jsonb
                        END AS j
                    FROM request_events_v1
                    WHERE payload IS NOT NULL
                    ORDER BY seq ASC
                    LIMIT 2000
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE request_events_v1 AS t
                SET request_id = COALESCE(NULLIF(b.j ->> 'request_id', ''), t.request_id),
                    principal_kind = COALESCE(b.j ->> 'principal_kind', t.principal_kind),
                    duration_ms = COALESCE((b.j ->> 'duration_ms')::bigint, t.duration_ms),
                    auth_ms = COALESCE((b.j ->> 'auth_ms')::bigint, t.auth_ms),
                    route_ms = COALESCE((b.j ->> 'route_ms')::bigint, t.route_ms),
                    limit_reserve_ms = COALESCE((b.j ->> 'limit_reserve_ms')::bigint, t.limit_reserve_ms),
                    bulkhead_wait_ms = COALESCE((b.j ->> 'bulkhead_wait_ms')::bigint, t.bulkhead_wait_ms),
                    dns_ms = COALESCE((b.j ->> 'dns_ms')::bigint, t.dns_ms),
                    connect_ms = COALESCE((b.j ->> 'connect_ms')::bigint, t.connect_ms),
                    connection_reused = COALESCE((b.j ->> 'connection_reused')::boolean, t.connection_reused),
                    proxy_setup_ms = COALESCE((b.j ->> 'proxy_setup_ms')::bigint, t.proxy_setup_ms),
                    shape_ms = COALESCE((b.j ->> 'shape_ms')::bigint, t.shape_ms),
                    sign_ms = COALESCE((b.j ->> 'sign_ms')::bigint, t.sign_ms),
                    upstream_ttfb_ms = COALESCE((b.j ->> 'upstream_ttfb_ms')::bigint, t.upstream_ttfb_ms),
                    upstream_body_ms = COALESCE((b.j ->> 'upstream_body_ms')::bigint, t.upstream_body_ms),
                    finalize_ms = COALESCE((b.j ->> 'finalize_ms')::bigint, t.finalize_ms),
                    json_parse_ms = COALESCE((b.j ->> 'json_parse_ms')::double precision, t.json_parse_ms),
                    cache_structure_ms = COALESCE((b.j ->> 'cache_structure_ms')::double precision, t.cache_structure_ms),
                    cache_token_key_ms = COALESCE((b.j ->> 'cache_token_key_ms')::double precision, t.cache_token_key_ms),
                    cache_count_lookup_ms = COALESCE((b.j ->> 'cache_count_lookup_ms')::double precision, t.cache_count_lookup_ms),
                    cache_tokenizer_queue_ms = COALESCE((b.j ->> 'cache_tokenizer_queue_ms')::double precision, t.cache_tokenizer_queue_ms),
                    cache_serialize_ms = COALESCE((b.j ->> 'cache_serialize_ms')::double precision, t.cache_serialize_ms),
                    cache_tokenize_ms = COALESCE((b.j ->> 'cache_tokenize_ms')::double precision, t.cache_tokenize_ms),
                    prepare_signer_ms = COALESCE((b.j ->> 'prepare_signer_ms')::double precision, t.prepare_signer_ms),
                    request_body_read_ms = COALESCE((b.j ->> 'request_body_read_ms')::bigint, t.request_body_read_ms),
                    request_body_bytes = COALESCE((b.j ->> 'request_body_bytes')::bigint, t.request_body_bytes),
                    request_body_first_chunk_ms = COALESCE((b.j ->> 'request_body_first_chunk_ms')::double precision, t.request_body_first_chunk_ms),
                    request_body_receive_ms = COALESCE((b.j ->> 'request_body_receive_ms')::double precision, t.request_body_receive_ms),
                    request_body_wait_ms = COALESCE((b.j ->> 'request_body_wait_ms')::double precision, t.request_body_wait_ms),
                    request_body_process_ms = COALESCE((b.j ->> 'request_body_process_ms')::double precision, t.request_body_process_ms),
                    request_body_chunk_count = COALESCE((b.j ->> 'request_body_chunk_count')::bigint, t.request_body_chunk_count),
                    response_body_wait_ms = COALESCE((b.j ->> 'response_body_wait_ms')::double precision, t.response_body_wait_ms),
                    response_body_process_ms = COALESCE((b.j ->> 'response_body_process_ms')::double precision, t.response_body_process_ms),
                    response_body_downstream_poll_gap_ms = COALESCE((b.j ->> 'response_body_downstream_poll_gap_ms')::double precision, t.response_body_downstream_poll_gap_ms),
                    retry_overhead_ms = COALESCE((b.j ->> 'retry_overhead_ms')::double precision, t.retry_overhead_ms),
                    body_chunk_count = COALESCE((b.j ->> 'body_chunk_count')::bigint, t.body_chunk_count),
                    body_bytes = COALESCE((b.j ->> 'body_bytes')::bigint, t.body_bytes),
                    stream_message_start_ms = COALESCE((b.j ->> 'stream_message_start_ms')::bigint, t.stream_message_start_ms),
                    stream_content_block_start_ms = COALESCE((b.j ->> 'stream_content_block_start_ms')::bigint, t.stream_content_block_start_ms),
                    stream_first_content_delta_ms = COALESCE((b.j ->> 'stream_first_content_delta_ms')::bigint, t.stream_first_content_delta_ms),
                    stream_last_content_delta_ms = COALESCE((b.j ->> 'stream_last_content_delta_ms')::bigint, t.stream_last_content_delta_ms),
                    stream_message_stop_ms = COALESCE((b.j ->> 'stream_message_stop_ms')::bigint, t.stream_message_stop_ms),
                    stream_last_chunk_ms = COALESCE((b.j ->> 'stream_last_chunk_ms')::bigint, t.stream_last_chunk_ms),
                    stream_total_ms = COALESCE((b.j ->> 'stream_total_ms')::bigint, t.stream_total_ms),
                    sse_event_count = COALESCE((b.j ->> 'sse_event_count')::bigint, t.sse_event_count),
                    content_delta_count = COALESCE((b.j ->> 'content_delta_count')::bigint, t.content_delta_count),
                    ping_count = COALESCE((b.j ->> 'ping_count')::bigint, t.ping_count),
                    inter_token_avg_ms = COALESCE((b.j ->> 'inter_token_avg_ms')::bigint, t.inter_token_avg_ms),
                    upstream_http_version = COALESCE(b.j ->> 'upstream_http_version', t.upstream_http_version),
                    upstream_request_id = COALESCE(b.j ->> 'upstream_request_id', t.upstream_request_id),
                    upstream_content_encoding = COALESCE(b.j ->> 'upstream_content_encoding', t.upstream_content_encoding),
                    upstream_content_length = COALESCE((b.j ->> 'upstream_content_length')::bigint, t.upstream_content_length),
                    upstream_body_bytes = COALESCE((b.j ->> 'upstream_body_bytes')::bigint, t.upstream_body_bytes),
                    upstream_body_end = COALESCE(b.j ->> 'upstream_body_end', t.upstream_body_end),
                    upstream_body_error_cause = COALESCE(b.j ->> 'upstream_body_error_cause', t.upstream_body_error_cause),
                    upstream_body_error_io_kind = COALESCE(b.j ->> 'upstream_body_error_io_kind', t.upstream_body_error_io_kind),
                    upstream_body_error_h2_reason = COALESCE(b.j ->> 'upstream_body_error_h2_reason', t.upstream_body_error_h2_reason),
                    upstream_stream_warning_type = COALESCE(b.j ->> 'upstream_stream_warning_type', t.upstream_stream_warning_type),
                    upstream_stream_warning_message = COALESCE(b.j ->> 'upstream_stream_warning_message', t.upstream_stream_warning_message),
                    payload = NULL
                FROM batch AS b
                WHERE t.seq = b.seq;

                GET DIAGNOSTICS v_rows = ROW_COUNT;
                EXIT WHEN v_rows = 0;
                COMMIT;
            END LOOP;
        END IF;

        SET LOCAL lock_timeout = '1s';
        LOCK TABLE request_events_v1 IN ACCESS EXCLUSIVE MODE;

        IF EXISTS (SELECT 1 FROM request_events_v1 WHERE payload IS NOT NULL LIMIT 1) THEN
            LOOP
                WITH batch AS (
                    SELECT
                        seq,
                        CASE
                            WHEN octet_length(payload) > 0
                                 AND left(convert_from(payload, 'UTF8'), 1) = '{'
                            THEN convert_from(payload, 'UTF8')::jsonb
                            ELSE '{}'::jsonb
                        END AS j
                    FROM request_events_v1
                    WHERE payload IS NOT NULL
                    ORDER BY seq ASC
                    LIMIT 2000
                )
                UPDATE request_events_v1 AS t
                SET request_id = COALESCE(NULLIF(b.j ->> 'request_id', ''), t.request_id),
                    principal_kind = COALESCE(b.j ->> 'principal_kind', t.principal_kind),
                    duration_ms = COALESCE((b.j ->> 'duration_ms')::bigint, t.duration_ms),
                    auth_ms = COALESCE((b.j ->> 'auth_ms')::bigint, t.auth_ms),
                    route_ms = COALESCE((b.j ->> 'route_ms')::bigint, t.route_ms),
                    limit_reserve_ms = COALESCE((b.j ->> 'limit_reserve_ms')::bigint, t.limit_reserve_ms),
                    bulkhead_wait_ms = COALESCE((b.j ->> 'bulkhead_wait_ms')::bigint, t.bulkhead_wait_ms),
                    dns_ms = COALESCE((b.j ->> 'dns_ms')::bigint, t.dns_ms),
                    connect_ms = COALESCE((b.j ->> 'connect_ms')::bigint, t.connect_ms),
                    connection_reused = COALESCE((b.j ->> 'connection_reused')::boolean, t.connection_reused),
                    proxy_setup_ms = COALESCE((b.j ->> 'proxy_setup_ms')::bigint, t.proxy_setup_ms),
                    shape_ms = COALESCE((b.j ->> 'shape_ms')::bigint, t.shape_ms),
                    sign_ms = COALESCE((b.j ->> 'sign_ms')::bigint, t.sign_ms),
                    upstream_ttfb_ms = COALESCE((b.j ->> 'upstream_ttfb_ms')::bigint, t.upstream_ttfb_ms),
                    upstream_body_ms = COALESCE((b.j ->> 'upstream_body_ms')::bigint, t.upstream_body_ms),
                    finalize_ms = COALESCE((b.j ->> 'finalize_ms')::bigint, t.finalize_ms),
                    json_parse_ms = COALESCE((b.j ->> 'json_parse_ms')::double precision, t.json_parse_ms),
                    cache_structure_ms = COALESCE((b.j ->> 'cache_structure_ms')::double precision, t.cache_structure_ms),
                    cache_token_key_ms = COALESCE((b.j ->> 'cache_token_key_ms')::double precision, t.cache_token_key_ms),
                    cache_count_lookup_ms = COALESCE((b.j ->> 'cache_count_lookup_ms')::double precision, t.cache_count_lookup_ms),
                    cache_tokenizer_queue_ms = COALESCE((b.j ->> 'cache_tokenizer_queue_ms')::double precision, t.cache_tokenizer_queue_ms),
                    cache_serialize_ms = COALESCE((b.j ->> 'cache_serialize_ms')::double precision, t.cache_serialize_ms),
                    cache_tokenize_ms = COALESCE((b.j ->> 'cache_tokenize_ms')::double precision, t.cache_tokenize_ms),
                    prepare_signer_ms = COALESCE((b.j ->> 'prepare_signer_ms')::double precision, t.prepare_signer_ms),
                    request_body_read_ms = COALESCE((b.j ->> 'request_body_read_ms')::bigint, t.request_body_read_ms),
                    request_body_bytes = COALESCE((b.j ->> 'request_body_bytes')::bigint, t.request_body_bytes),
                    request_body_first_chunk_ms = COALESCE((b.j ->> 'request_body_first_chunk_ms')::double precision, t.request_body_first_chunk_ms),
                    request_body_receive_ms = COALESCE((b.j ->> 'request_body_receive_ms')::double precision, t.request_body_receive_ms),
                    request_body_wait_ms = COALESCE((b.j ->> 'request_body_wait_ms')::double precision, t.request_body_wait_ms),
                    request_body_process_ms = COALESCE((b.j ->> 'request_body_process_ms')::double precision, t.request_body_process_ms),
                    request_body_chunk_count = COALESCE((b.j ->> 'request_body_chunk_count')::bigint, t.request_body_chunk_count),
                    response_body_wait_ms = COALESCE((b.j ->> 'response_body_wait_ms')::double precision, t.response_body_wait_ms),
                    response_body_process_ms = COALESCE((b.j ->> 'response_body_process_ms')::double precision, t.response_body_process_ms),
                    response_body_downstream_poll_gap_ms = COALESCE((b.j ->> 'response_body_downstream_poll_gap_ms')::double precision, t.response_body_downstream_poll_gap_ms),
                    retry_overhead_ms = COALESCE((b.j ->> 'retry_overhead_ms')::double precision, t.retry_overhead_ms),
                    body_chunk_count = COALESCE((b.j ->> 'body_chunk_count')::bigint, t.body_chunk_count),
                    body_bytes = COALESCE((b.j ->> 'body_bytes')::bigint, t.body_bytes),
                    stream_message_start_ms = COALESCE((b.j ->> 'stream_message_start_ms')::bigint, t.stream_message_start_ms),
                    stream_content_block_start_ms = COALESCE((b.j ->> 'stream_content_block_start_ms')::bigint, t.stream_content_block_start_ms),
                    stream_first_content_delta_ms = COALESCE((b.j ->> 'stream_first_content_delta_ms')::bigint, t.stream_first_content_delta_ms),
                    stream_last_content_delta_ms = COALESCE((b.j ->> 'stream_last_content_delta_ms')::bigint, t.stream_last_content_delta_ms),
                    stream_message_stop_ms = COALESCE((b.j ->> 'stream_message_stop_ms')::bigint, t.stream_message_stop_ms),
                    stream_last_chunk_ms = COALESCE((b.j ->> 'stream_last_chunk_ms')::bigint, t.stream_last_chunk_ms),
                    stream_total_ms = COALESCE((b.j ->> 'stream_total_ms')::bigint, t.stream_total_ms),
                    sse_event_count = COALESCE((b.j ->> 'sse_event_count')::bigint, t.sse_event_count),
                    content_delta_count = COALESCE((b.j ->> 'content_delta_count')::bigint, t.content_delta_count),
                    ping_count = COALESCE((b.j ->> 'ping_count')::bigint, t.ping_count),
                    inter_token_avg_ms = COALESCE((b.j ->> 'inter_token_avg_ms')::bigint, t.inter_token_avg_ms),
                    upstream_http_version = COALESCE(b.j ->> 'upstream_http_version', t.upstream_http_version),
                    upstream_request_id = COALESCE(b.j ->> 'upstream_request_id', t.upstream_request_id),
                    upstream_content_encoding = COALESCE(b.j ->> 'upstream_content_encoding', t.upstream_content_encoding),
                    upstream_content_length = COALESCE((b.j ->> 'upstream_content_length')::bigint, t.upstream_content_length),
                    upstream_body_bytes = COALESCE((b.j ->> 'upstream_body_bytes')::bigint, t.upstream_body_bytes),
                    upstream_body_end = COALESCE(b.j ->> 'upstream_body_end', t.upstream_body_end),
                    upstream_body_error_cause = COALESCE(b.j ->> 'upstream_body_error_cause', t.upstream_body_error_cause),
                    upstream_body_error_io_kind = COALESCE(b.j ->> 'upstream_body_error_io_kind', t.upstream_body_error_io_kind),
                    upstream_body_error_h2_reason = COALESCE(b.j ->> 'upstream_body_error_h2_reason', t.upstream_body_error_h2_reason),
                    upstream_stream_warning_type = COALESCE(b.j ->> 'upstream_stream_warning_type', t.upstream_stream_warning_type),
                    upstream_stream_warning_message = COALESCE(b.j ->> 'upstream_stream_warning_message', t.upstream_stream_warning_message),
                    payload = NULL
                FROM batch AS b
                WHERE t.seq = b.seq;

                GET DIAGNOSTICS v_rows = ROW_COUNT;
                EXIT WHEN v_rows = 0;
            END LOOP;
        END IF;

        ALTER TABLE request_events_v1 DROP COLUMN IF EXISTS payload;
    END IF;
END $$;
