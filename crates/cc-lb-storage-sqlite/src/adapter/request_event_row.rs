use cc_lb_storage_api::{RequestEvent, StorageError, StorageResult};
use sqlx::{Row, sqlite::SqliteRow};
use uuid::Uuid;

use super::request_events::i64_to_u64;
use crate::map_sqlx_error;

pub(super) const REQUEST_EVENT_COLUMNS: &str = "id, ts, request_id, source_kind, source_ref_id, event_kind, list_ts_ms, principal_id, key_id, principal_kind, upstream_id, upstream_name, model, list_status, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, thread_id, observed_session_id, request_kind, claude_agent_id, claude_parent_agent_id, parent_session_id, client_app, session_id_source, thinking_budget_tokens, reasoning_effort, token_estimate_source, list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, list_cost_cache_read_micros, list_auth_ms, list_route_ms, list_limit_reserve_ms, list_json_parse_ms, list_cache_structure_ms, list_cache_token_key_ms, list_cache_count_lookup_ms, list_cache_tokenizer_queue_ms, list_cache_serialize_ms, list_cache_tokenize_ms, list_prepare_signer_ms, list_bulkhead_wait_ms, list_dns_ms, list_connect_ms, list_connection_reused, list_request_body_read_ms, list_request_body_first_chunk_ms, list_request_body_receive_ms, list_request_body_wait_ms, list_request_body_process_ms, list_request_body_chunk_count, list_request_body_bytes, list_duration_ms, list_proxy_setup_ms, list_shape_ms, list_sign_ms, list_upstream_ttfb_ms, list_upstream_body_ms, list_response_body_wait_ms, list_response_body_process_ms, list_response_body_downstream_poll_gap_ms, list_retry_overhead_ms, list_finalize_ms, body_chunk_count, body_bytes, stream_message_start_ms, stream_content_block_start_ms, list_stream_first_content_delta_ms, list_stream_last_content_delta_ms, stream_message_stop_ms, stream_last_chunk_ms, stream_total_ms, sse_event_count, content_delta_count, ping_count, list_inter_token_avg_ms, error_code, event_id, thinking_tokens, service_tier, upstream_error_type, upstream_error_message, upstream_http_version, upstream_request_id, upstream_content_encoding, upstream_content_length, upstream_body_bytes, upstream_body_end, upstream_body_error_cause, upstream_body_error_io_kind, upstream_body_error_h2_reason, upstream_stream_warning_type, upstream_stream_warning_message";

pub(super) fn row_to_request_event(row: &SqliteRow) -> StorageResult<RequestEvent> {
    Ok(RequestEvent {
        ts: i64_to_u64(
            row.try_get("ts").map_err(map_sqlx_error)?,
            "request event ts",
        )?,
        request_id: row.try_get("request_id").map_err(map_sqlx_error)?,
        source_kind: row.try_get("source_kind").map_err(map_sqlx_error)?,
        source_ref_id: row.try_get("source_ref_id").map_err(map_sqlx_error)?,
        event_kind: row
            .try_get::<Option<String>, _>("event_kind")
            .map_err(map_sqlx_error)?
            .map(|value| {
                value.parse().map_err(|_| StorageError::Corrupted {
                    message: format!("request event unknown event_kind value: {value}"),
                })
            })
            .transpose()?,
        ts_ms: optional_u64(row, "list_ts_ms")?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        key_id: row.try_get("key_id").map_err(map_sqlx_error)?,
        principal_kind: row.try_get("principal_kind").map_err(map_sqlx_error)?,
        upstream_id: row
            .try_get::<Option<String>, _>("upstream_id")
            .map_err(map_sqlx_error)?
            .map(|value| {
                Uuid::parse_str(&value).map_err(|_| StorageError::Corrupted {
                    message: "request event upstream_id is not a valid uuid".to_owned(),
                })
            })
            .transpose()?,
        upstream_name: row.try_get("upstream_name").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        status: u16::try_from(
            row.try_get::<i64, _>("list_status")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "request event status out of range".to_owned(),
        })?,
        input_tokens: optional_u64(row, "input_tokens")?,
        output_tokens: optional_u64(row, "output_tokens")?,
        cache_creation_input_tokens: optional_u64(row, "cache_creation_input_tokens")?,
        cache_creation_input_tokens_5m: optional_u64(row, "cache_creation_input_tokens_5m")?,
        cache_creation_input_tokens_1h: optional_u64(row, "cache_creation_input_tokens_1h")?,
        cache_read_input_tokens: optional_u64(row, "cache_read_input_tokens")?,
        thread_id: row.try_get("thread_id").map_err(map_sqlx_error)?,
        observed_session_id: row.try_get("observed_session_id").map_err(map_sqlx_error)?,
        request_kind: row.try_get("request_kind").map_err(map_sqlx_error)?,
        claude_agent_id: row.try_get("claude_agent_id").map_err(map_sqlx_error)?,
        claude_parent_agent_id: row
            .try_get("claude_parent_agent_id")
            .map_err(map_sqlx_error)?,
        parent_session_id: row.try_get("parent_session_id").map_err(map_sqlx_error)?,
        client_app: row.try_get("client_app").map_err(map_sqlx_error)?,
        session_id_source: row.try_get("session_id_source").map_err(map_sqlx_error)?,
        thinking_budget_tokens: optional_u64(row, "thinking_budget_tokens")?,
        reasoning_effort: row.try_get("reasoning_effort").map_err(map_sqlx_error)?,
        token_estimate_source: row
            .try_get("token_estimate_source")
            .map_err(map_sqlx_error)?,
        cost_usd_micros: row
            .try_get("list_cost_usd_micros")
            .map_err(map_sqlx_error)?,
        cost_input_micros: row
            .try_get("list_cost_input_micros")
            .map_err(map_sqlx_error)?,
        cost_output_micros: row
            .try_get("list_cost_output_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_creation_5m_micros: row
            .try_get("list_cost_cache_creation_5m_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_creation_1h_micros: row
            .try_get("list_cost_cache_creation_1h_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_read_micros: row
            .try_get("list_cost_cache_read_micros")
            .map_err(map_sqlx_error)?,
        auth_ms: optional_u64(row, "list_auth_ms")?,
        route_ms: optional_u64(row, "list_route_ms")?,
        limit_reserve_ms: optional_u64(row, "list_limit_reserve_ms")?,
        json_parse_ms: row.try_get("list_json_parse_ms").map_err(map_sqlx_error)?,
        cache_structure_ms: row
            .try_get("list_cache_structure_ms")
            .map_err(map_sqlx_error)?,
        cache_token_key_ms: row
            .try_get("list_cache_token_key_ms")
            .map_err(map_sqlx_error)?,
        cache_count_lookup_ms: row
            .try_get("list_cache_count_lookup_ms")
            .map_err(map_sqlx_error)?,
        cache_tokenizer_queue_ms: row
            .try_get("list_cache_tokenizer_queue_ms")
            .map_err(map_sqlx_error)?,
        cache_serialize_ms: row
            .try_get("list_cache_serialize_ms")
            .map_err(map_sqlx_error)?,
        cache_tokenize_ms: row
            .try_get("list_cache_tokenize_ms")
            .map_err(map_sqlx_error)?,
        prepare_signer_ms: row
            .try_get("list_prepare_signer_ms")
            .map_err(map_sqlx_error)?,
        bulkhead_wait_ms: optional_u64(row, "list_bulkhead_wait_ms")?,
        dns_ms: optional_u64(row, "list_dns_ms")?,
        connect_ms: optional_u64(row, "list_connect_ms")?,
        connection_reused: row
            .try_get("list_connection_reused")
            .map_err(map_sqlx_error)?,
        request_body_read_ms: optional_u64(row, "list_request_body_read_ms")?,
        request_body_first_chunk_ms: row
            .try_get("list_request_body_first_chunk_ms")
            .map_err(map_sqlx_error)?,
        request_body_receive_ms: row
            .try_get("list_request_body_receive_ms")
            .map_err(map_sqlx_error)?,
        request_body_wait_ms: row
            .try_get("list_request_body_wait_ms")
            .map_err(map_sqlx_error)?,
        request_body_process_ms: row
            .try_get("list_request_body_process_ms")
            .map_err(map_sqlx_error)?,
        request_body_chunk_count: optional_u64(row, "list_request_body_chunk_count")?,
        request_body_bytes: optional_u64(row, "list_request_body_bytes")?,
        duration_ms: i64_to_u64(
            row.try_get("list_duration_ms").map_err(map_sqlx_error)?,
            "request event duration_ms",
        )?,
        proxy_setup_ms: optional_u64(row, "list_proxy_setup_ms")?,
        shape_ms: optional_u64(row, "list_shape_ms")?,
        sign_ms: optional_u64(row, "list_sign_ms")?,
        upstream_ttfb_ms: optional_u64(row, "list_upstream_ttfb_ms")?,
        upstream_body_ms: optional_u64(row, "list_upstream_body_ms")?,
        response_body_wait_ms: row
            .try_get("list_response_body_wait_ms")
            .map_err(map_sqlx_error)?,
        response_body_process_ms: row
            .try_get("list_response_body_process_ms")
            .map_err(map_sqlx_error)?,
        response_body_downstream_poll_gap_ms: row
            .try_get("list_response_body_downstream_poll_gap_ms")
            .map_err(map_sqlx_error)?,
        retry_overhead_ms: row
            .try_get("list_retry_overhead_ms")
            .map_err(map_sqlx_error)?,
        finalize_ms: optional_u64(row, "list_finalize_ms")?,
        body_chunk_count: optional_u64(row, "body_chunk_count")?,
        body_bytes: optional_u64(row, "body_bytes")?,
        stream_message_start_ms: optional_u64(row, "stream_message_start_ms")?,
        stream_content_block_start_ms: optional_u64(row, "stream_content_block_start_ms")?,
        stream_first_content_delta_ms: optional_u64(row, "list_stream_first_content_delta_ms")?,
        stream_last_content_delta_ms: optional_u64(row, "list_stream_last_content_delta_ms")?,
        stream_message_stop_ms: optional_u64(row, "stream_message_stop_ms")?,
        stream_last_chunk_ms: optional_u64(row, "stream_last_chunk_ms")?,
        stream_total_ms: optional_u64(row, "stream_total_ms")?,
        sse_event_count: optional_u64(row, "sse_event_count")?,
        content_delta_count: optional_u64(row, "content_delta_count")?,
        ping_count: optional_u64(row, "ping_count")?,
        inter_token_avg_ms: optional_u64(row, "list_inter_token_avg_ms")?,
        error_code: row.try_get("error_code").map_err(map_sqlx_error)?,
        event_id: row.try_get("event_id").map_err(map_sqlx_error)?,
        thinking_tokens: optional_u64(row, "thinking_tokens")?,
        service_tier: row.try_get("service_tier").map_err(map_sqlx_error)?,
        upstream_error_type: row.try_get("upstream_error_type").map_err(map_sqlx_error)?,
        upstream_error_message: row
            .try_get("upstream_error_message")
            .map_err(map_sqlx_error)?,
        upstream_http_version: row
            .try_get("upstream_http_version")
            .map_err(map_sqlx_error)?,
        upstream_request_id: row.try_get("upstream_request_id").map_err(map_sqlx_error)?,
        upstream_content_encoding: row
            .try_get("upstream_content_encoding")
            .map_err(map_sqlx_error)?,
        upstream_content_length: optional_u64(row, "upstream_content_length")?,
        upstream_body_bytes: optional_u64(row, "upstream_body_bytes")?,
        upstream_body_end: row.try_get("upstream_body_end").map_err(map_sqlx_error)?,
        upstream_body_error_cause: row
            .try_get("upstream_body_error_cause")
            .map_err(map_sqlx_error)?,
        upstream_body_error_io_kind: row
            .try_get("upstream_body_error_io_kind")
            .map_err(map_sqlx_error)?,
        upstream_body_error_h2_reason: row
            .try_get("upstream_body_error_h2_reason")
            .map_err(map_sqlx_error)?,
        upstream_stream_warning_type: row
            .try_get("upstream_stream_warning_type")
            .map_err(map_sqlx_error)?,
        upstream_stream_warning_message: row
            .try_get("upstream_stream_warning_message")
            .map_err(map_sqlx_error)?,
        cache_control_message_indices: Vec::new(),
        cache_breakpoints: Vec::new(),
        routing_trace: None,
        internal_errors: Vec::new(),
        iterations: None,
    })
}

fn optional_u64(row: &SqliteRow, column: &str) -> StorageResult<Option<u64>> {
    row.try_get::<Option<i64>, _>(column)
        .map_err(map_sqlx_error)?
        .map(|value| i64_to_u64(value, column))
        .transpose()
}
