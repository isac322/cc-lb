use cc_lb_storage_api::{RequestEvent, StorageError, StorageResult};
use sqlx::{Row, postgres::PgRow};

use super::parse_event_kind;
use crate::{adapter::i64_to_u64, error_map::map_sqlx_error};

macro_rules! event_select {
    () => { "SELECT seq, EXTRACT(EPOCH FROM ts)::bigint AS ts_secs, request_id, source_kind, source_ref_id, event_kind, list_ts_ms, principal_id, key_id, principal_kind, upstream_id, upstream_name, model, list_status, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, thread_id, observed_session_id, request_kind, claude_agent_id, claude_parent_agent_id, parent_session_id, client_app, session_id_source, thinking_budget_tokens, reasoning_effort, token_estimate_source, list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, list_cost_cache_read_micros, auth_ms, route_ms, limit_reserve_ms, json_parse_ms, cache_structure_ms, cache_token_key_ms, cache_count_lookup_ms, cache_tokenizer_queue_ms, cache_serialize_ms, cache_tokenize_ms, prepare_signer_ms, bulkhead_wait_ms, dns_ms, connect_ms, connection_reused, request_body_read_ms, request_body_first_chunk_ms, request_body_receive_ms, request_body_wait_ms, request_body_process_ms, request_body_chunk_count, request_body_bytes, duration_ms, proxy_setup_ms, shape_ms, sign_ms, upstream_ttfb_ms, upstream_body_ms, response_body_wait_ms, response_body_process_ms, response_body_downstream_poll_gap_ms, retry_overhead_ms, finalize_ms, body_chunk_count, body_bytes, stream_message_start_ms, stream_content_block_start_ms, stream_first_content_delta_ms, stream_last_content_delta_ms, stream_message_stop_ms, stream_last_chunk_ms, stream_total_ms, sse_event_count, content_delta_count, ping_count, inter_token_avg_ms, error_code, event_id, thinking_tokens, service_tier, upstream_error_type, upstream_error_message, upstream_http_version, upstream_request_id, upstream_content_encoding, upstream_content_length, upstream_body_bytes, upstream_body_end, upstream_body_error_cause, upstream_body_error_io_kind, upstream_body_error_h2_reason, upstream_stream_warning_type, upstream_stream_warning_message FROM request_events_v1 " };
}

pub(super) const CURSOR_SQL: &str = concat!(
    event_select!(),
    "WHERE seq > $1 AND seq <= $2 AND tx_id < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY seq ASC LIMIT $3"
);
pub(super) const EXPORT_SQL: &str = concat!(
    event_select!(),
    "WHERE seq > $1 AND seq <= $2 AND ($3::bigint IS NULL OR list_ts_ms >= $3) AND ($4::bigint IS NULL OR list_ts_ms < $4) AND tx_id < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY seq ASC LIMIT $5"
);
pub(crate) const GET_SQL: &str = concat!(event_select!(), "WHERE event_id = $1");

pub(crate) fn from_row(row: &PgRow) -> StorageResult<RequestEvent> {
    Ok(RequestEvent {
        ts: i64_to_u64(
            row.try_get("ts_secs").map_err(map_sqlx_error)?,
            "request event ts",
        )?,
        request_id: row.try_get("request_id").map_err(map_sqlx_error)?,
        source_kind: row.try_get("source_kind").map_err(map_sqlx_error)?,
        source_ref_id: row.try_get("source_ref_id").map_err(map_sqlx_error)?,
        event_kind: row
            .try_get::<Option<String>, _>("event_kind")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(parse_event_kind)
            .transpose()?,
        ts_ms: row
            .try_get::<Option<i64>, _>("list_ts_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event ts_ms"))
            .transpose()?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        key_id: row.try_get("key_id").map_err(map_sqlx_error)?,
        principal_kind: row.try_get("principal_kind").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        upstream_name: row.try_get("upstream_name").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        status: u16::try_from(
            row.try_get::<Option<i32>, _>("list_status")
                .map_err(map_sqlx_error)?
                .ok_or_else(|| StorageError::Corrupted {
                    message: "request event status missing".to_owned(),
                })?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "request event status out of range".to_owned(),
        })?,
        input_tokens: row
            .try_get::<Option<i64>, _>("input_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event input_tokens"))
            .transpose()?,
        output_tokens: row
            .try_get::<Option<i64>, _>("output_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event output_tokens"))
            .transpose()?,
        cache_creation_input_tokens: row
            .try_get::<Option<i64>, _>("cache_creation_input_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event cache_creation_input_tokens"))
            .transpose()?,
        cache_creation_input_tokens_5m: row
            .try_get::<Option<i64>, _>("cache_creation_input_tokens_5m")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event cache_creation_input_tokens_5m"))
            .transpose()?,
        cache_creation_input_tokens_1h: row
            .try_get::<Option<i64>, _>("cache_creation_input_tokens_1h")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event cache_creation_input_tokens_1h"))
            .transpose()?,
        cache_read_input_tokens: row
            .try_get::<Option<i64>, _>("cache_read_input_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event cache_read_input_tokens"))
            .transpose()?,
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
        thinking_budget_tokens: row
            .try_get::<Option<i64>, _>("thinking_budget_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event thinking_budget_tokens"))
            .transpose()?,
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
        auth_ms: row
            .try_get::<Option<i64>, _>("auth_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event auth_ms"))
            .transpose()?,
        route_ms: row
            .try_get::<Option<i64>, _>("route_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event route_ms"))
            .transpose()?,
        limit_reserve_ms: row
            .try_get::<Option<i64>, _>("limit_reserve_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event limit_reserve_ms"))
            .transpose()?,
        json_parse_ms: row.try_get("json_parse_ms").map_err(map_sqlx_error)?,
        cache_structure_ms: row.try_get("cache_structure_ms").map_err(map_sqlx_error)?,
        cache_token_key_ms: row.try_get("cache_token_key_ms").map_err(map_sqlx_error)?,
        cache_count_lookup_ms: row
            .try_get("cache_count_lookup_ms")
            .map_err(map_sqlx_error)?,
        cache_tokenizer_queue_ms: row
            .try_get("cache_tokenizer_queue_ms")
            .map_err(map_sqlx_error)?,
        cache_serialize_ms: row.try_get("cache_serialize_ms").map_err(map_sqlx_error)?,
        cache_tokenize_ms: row.try_get("cache_tokenize_ms").map_err(map_sqlx_error)?,
        prepare_signer_ms: row.try_get("prepare_signer_ms").map_err(map_sqlx_error)?,
        bulkhead_wait_ms: row
            .try_get::<Option<i64>, _>("bulkhead_wait_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event bulkhead_wait_ms"))
            .transpose()?,
        dns_ms: row
            .try_get::<Option<i64>, _>("dns_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event dns_ms"))
            .transpose()?,
        connect_ms: row
            .try_get::<Option<i64>, _>("connect_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event connect_ms"))
            .transpose()?,
        connection_reused: row.try_get("connection_reused").map_err(map_sqlx_error)?,
        request_body_read_ms: row
            .try_get::<Option<i64>, _>("request_body_read_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event request_body_read_ms"))
            .transpose()?,
        request_body_first_chunk_ms: row
            .try_get("request_body_first_chunk_ms")
            .map_err(map_sqlx_error)?,
        request_body_receive_ms: row
            .try_get("request_body_receive_ms")
            .map_err(map_sqlx_error)?,
        request_body_wait_ms: row
            .try_get("request_body_wait_ms")
            .map_err(map_sqlx_error)?,
        request_body_process_ms: row
            .try_get("request_body_process_ms")
            .map_err(map_sqlx_error)?,
        request_body_chunk_count: row
            .try_get::<Option<i64>, _>("request_body_chunk_count")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event request_body_chunk_count"))
            .transpose()?,
        request_body_bytes: row
            .try_get::<Option<i64>, _>("request_body_bytes")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event request_body_bytes"))
            .transpose()?,
        duration_ms: i64_to_u64(
            row.try_get("duration_ms").map_err(map_sqlx_error)?,
            "request event duration_ms",
        )?,
        proxy_setup_ms: row
            .try_get::<Option<i64>, _>("proxy_setup_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event proxy_setup_ms"))
            .transpose()?,
        shape_ms: row
            .try_get::<Option<i64>, _>("shape_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event shape_ms"))
            .transpose()?,
        sign_ms: row
            .try_get::<Option<i64>, _>("sign_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event sign_ms"))
            .transpose()?,
        upstream_ttfb_ms: row
            .try_get::<Option<i64>, _>("upstream_ttfb_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event upstream_ttfb_ms"))
            .transpose()?,
        upstream_body_ms: row
            .try_get::<Option<i64>, _>("upstream_body_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event upstream_body_ms"))
            .transpose()?,
        response_body_wait_ms: row
            .try_get("response_body_wait_ms")
            .map_err(map_sqlx_error)?,
        response_body_process_ms: row
            .try_get("response_body_process_ms")
            .map_err(map_sqlx_error)?,
        response_body_downstream_poll_gap_ms: row
            .try_get("response_body_downstream_poll_gap_ms")
            .map_err(map_sqlx_error)?,
        retry_overhead_ms: row.try_get("retry_overhead_ms").map_err(map_sqlx_error)?,
        finalize_ms: row
            .try_get::<Option<i64>, _>("finalize_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event finalize_ms"))
            .transpose()?,
        body_chunk_count: row
            .try_get::<Option<i64>, _>("body_chunk_count")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event body_chunk_count"))
            .transpose()?,
        body_bytes: row
            .try_get::<Option<i64>, _>("body_bytes")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event body_bytes"))
            .transpose()?,
        stream_message_start_ms: row
            .try_get::<Option<i64>, _>("stream_message_start_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_message_start_ms"))
            .transpose()?,
        stream_content_block_start_ms: row
            .try_get::<Option<i64>, _>("stream_content_block_start_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_content_block_start_ms"))
            .transpose()?,
        stream_first_content_delta_ms: row
            .try_get::<Option<i64>, _>("stream_first_content_delta_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_first_content_delta_ms"))
            .transpose()?,
        stream_last_content_delta_ms: row
            .try_get::<Option<i64>, _>("stream_last_content_delta_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_last_content_delta_ms"))
            .transpose()?,
        stream_message_stop_ms: row
            .try_get::<Option<i64>, _>("stream_message_stop_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_message_stop_ms"))
            .transpose()?,
        stream_last_chunk_ms: row
            .try_get::<Option<i64>, _>("stream_last_chunk_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_last_chunk_ms"))
            .transpose()?,
        stream_total_ms: row
            .try_get::<Option<i64>, _>("stream_total_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event stream_total_ms"))
            .transpose()?,
        sse_event_count: row
            .try_get::<Option<i64>, _>("sse_event_count")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event sse_event_count"))
            .transpose()?,
        content_delta_count: row
            .try_get::<Option<i64>, _>("content_delta_count")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event content_delta_count"))
            .transpose()?,
        ping_count: row
            .try_get::<Option<i64>, _>("ping_count")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event ping_count"))
            .transpose()?,
        inter_token_avg_ms: row
            .try_get::<Option<i64>, _>("inter_token_avg_ms")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event inter_token_avg_ms"))
            .transpose()?,
        error_code: row.try_get("error_code").map_err(map_sqlx_error)?,
        event_id: row.try_get("event_id").map_err(map_sqlx_error)?,
        thinking_tokens: row
            .try_get::<Option<i64>, _>("thinking_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event thinking_tokens"))
            .transpose()?,
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
        upstream_content_length: row
            .try_get::<Option<i64>, _>("upstream_content_length")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event upstream_content_length"))
            .transpose()?,
        upstream_body_bytes: row
            .try_get::<Option<i64>, _>("upstream_body_bytes")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "request event upstream_body_bytes"))
            .transpose()?,
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
        cache_breakpoints: Vec::new(),
        cache_control_message_indices: Vec::new(),
        routing_trace: None,
        iterations: None,
        internal_errors: Vec::new(),
    })
}
