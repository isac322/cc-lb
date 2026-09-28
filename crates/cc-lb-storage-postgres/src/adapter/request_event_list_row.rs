use cc_lb_storage_api::{RequestEventListItem, RequestEventUpstream, StorageError, StorageResult};
use serde::Deserialize;

use crate::adapter::i64_to_u64;

use super::request_event_list_sql::{ListRow, parse_event_kind};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ListPayload {
    request_id: Option<String>,
    duration_ms: Option<i64>,
    request_body_read_ms: Option<i64>,
    request_body_bytes: Option<i64>,
    request_body_first_chunk_ms: Option<f64>,
    request_body_receive_ms: Option<f64>,
    request_body_wait_ms: Option<f64>,
    request_body_process_ms: Option<f64>,
    request_body_chunk_count: Option<i64>,
    response_body_wait_ms: Option<f64>,
    response_body_process_ms: Option<f64>,
    response_body_downstream_poll_gap_ms: Option<f64>,
    retry_overhead_ms: Option<f64>,
    auth_ms: Option<i64>,
    route_ms: Option<i64>,
    limit_reserve_ms: Option<i64>,
    json_parse_ms: Option<f64>,
    cache_structure_ms: Option<f64>,
    cache_token_key_ms: Option<f64>,
    cache_count_lookup_ms: Option<f64>,
    cache_tokenizer_queue_ms: Option<f64>,
    cache_serialize_ms: Option<f64>,
    cache_tokenize_ms: Option<f64>,
    prepare_signer_ms: Option<f64>,
    bulkhead_wait_ms: Option<i64>,
    dns_ms: Option<i64>,
    connect_ms: Option<i64>,
    connection_reused: Option<bool>,
    limit_reconcile_ms: Option<i64>,
    finalize_ms: Option<i64>,
    proxy_setup_ms: Option<i64>,
    shape_ms: Option<i64>,
    sign_ms: Option<i64>,
    upstream_ttfb_ms: Option<i64>,
    upstream_body_ms: Option<i64>,
    stream_first_content_delta_ms: Option<i64>,
    stream_last_content_delta_ms: Option<i64>,
    inter_token_avg_ms: Option<i64>,
    cost_usd_micros: Option<i64>,
    cost_input_micros: Option<i64>,
    cost_output_micros: Option<i64>,
    cost_cache_creation_5m_micros: Option<i64>,
    cost_cache_creation_1h_micros: Option<i64>,
    cost_cache_read_micros: Option<i64>,
}

pub(super) fn list_row_to_item(row: ListRow) -> StorageResult<RequestEventListItem> {
    let payload = match row.payload.as_deref() {
        Some(payload) => {
            serde_json::from_slice::<Option<ListPayload>>(payload)?.unwrap_or_default()
        }
        None => ListPayload::default(),
    };

    Ok(RequestEventListItem {
        ts: i64_to_u64(row.ts_secs, "request event list ts")?,
        ts_ms: row
            .ts_ms
            .map(|value| i64_to_u64(value, "request event list ts_ms"))
            .transpose()?,
        request_id: payload.request_id.unwrap_or_default(),
        event_id: row.event_id,
        source_kind: row.source_kind,
        event_kind: row
            .event_kind
            .as_deref()
            .map(parse_event_kind)
            .transpose()?,
        principal_id: row.principal_id,
        upstream: row.upstream.as_deref().map(parse_upstream).transpose()?,
        upstream_id: row.upstream_id,
        upstream_name: row.upstream_name,
        thread_id: row.thread_id,
        observed_session_id: row.observed_session_id,
        request_kind: row.request_kind,
        claude_agent_id: row.claude_agent_id,
        claude_parent_agent_id: row.claude_parent_agent_id,
        parent_session_id: row.parent_session_id,
        client_app: row.client_app,
        session_id_source: row.session_id_source,
        model: row.model,
        reasoning_effort: row.reasoning_effort,
        thinking_budget_tokens: row
            .thinking_budget_tokens
            .map(|value| i64_to_u64(value, "request event list thinking_budget_tokens"))
            .transpose()?,
        thinking_tokens: row
            .thinking_tokens
            .map(|value| i64_to_u64(value, "request event list thinking_tokens"))
            .transpose()?,
        service_tier: row.service_tier,
        status: match row.status {
            Some(value) => u16::try_from(value).map_err(|_| StorageError::Corrupted {
                message: "request event list status out of range".to_owned(),
            })?,
            None => {
                return Err(StorageError::Corrupted {
                    message: "request event list status missing from payload".to_owned(),
                });
            }
        },
        error_code: row.error_code,
        upstream_error_type: row.upstream_error_type,
        upstream_error_message: row.upstream_error_message,
        duration_ms: payload
            .duration_ms
            .map(|value| i64_to_u64(value, "request event list duration_ms"))
            .transpose()?
            .unwrap_or(0),
        request_body_read_ms: payload
            .request_body_read_ms
            .map(|value| i64_to_u64(value, "request event list request_body_read_ms"))
            .transpose()?,
        request_body_bytes: payload
            .request_body_bytes
            .map(|value| i64_to_u64(value, "request event list request_body_bytes"))
            .transpose()?,
        request_body_first_chunk_ms: payload.request_body_first_chunk_ms,
        request_body_receive_ms: payload.request_body_receive_ms,
        request_body_wait_ms: payload.request_body_wait_ms,
        request_body_process_ms: payload.request_body_process_ms,
        request_body_chunk_count: payload
            .request_body_chunk_count
            .map(|value| i64_to_u64(value, "request event list request_body_chunk_count"))
            .transpose()?,
        response_body_wait_ms: payload.response_body_wait_ms,
        response_body_process_ms: payload.response_body_process_ms,
        response_body_downstream_poll_gap_ms: payload.response_body_downstream_poll_gap_ms,
        retry_overhead_ms: payload.retry_overhead_ms,
        auth_ms: payload
            .auth_ms
            .map(|value| i64_to_u64(value, "request event list auth_ms"))
            .transpose()?,
        route_ms: payload
            .route_ms
            .map(|value| i64_to_u64(value, "request event list route_ms"))
            .transpose()?,
        limit_reserve_ms: payload
            .limit_reserve_ms
            .map(|value| i64_to_u64(value, "request event list limit_reserve_ms"))
            .transpose()?,
        json_parse_ms: payload.json_parse_ms,
        cache_structure_ms: payload.cache_structure_ms,
        cache_token_key_ms: payload.cache_token_key_ms,
        cache_count_lookup_ms: payload.cache_count_lookup_ms,
        cache_tokenizer_queue_ms: payload.cache_tokenizer_queue_ms,
        cache_serialize_ms: payload.cache_serialize_ms,
        cache_tokenize_ms: payload.cache_tokenize_ms,
        prepare_signer_ms: payload.prepare_signer_ms,
        bulkhead_wait_ms: payload
            .bulkhead_wait_ms
            .map(|value| i64_to_u64(value, "request event list bulkhead_wait_ms"))
            .transpose()?,
        dns_ms: payload
            .dns_ms
            .map(|value| i64_to_u64(value, "request event list dns_ms"))
            .transpose()?,
        connect_ms: payload
            .connect_ms
            .map(|value| i64_to_u64(value, "request event list connect_ms"))
            .transpose()?,
        connection_reused: payload.connection_reused,
        limit_reconcile_ms: payload
            .limit_reconcile_ms
            .map(|value| i64_to_u64(value, "request event list limit_reconcile_ms"))
            .transpose()?,
        finalize_ms: payload
            .finalize_ms
            .map(|value| i64_to_u64(value, "request event list finalize_ms"))
            .transpose()?,
        proxy_setup_ms: payload
            .proxy_setup_ms
            .map(|value| i64_to_u64(value, "request event list proxy_setup_ms"))
            .transpose()?,
        shape_ms: payload
            .shape_ms
            .map(|value| i64_to_u64(value, "request event list shape_ms"))
            .transpose()?,
        sign_ms: payload
            .sign_ms
            .map(|value| i64_to_u64(value, "request event list sign_ms"))
            .transpose()?,
        upstream_ttfb_ms: payload
            .upstream_ttfb_ms
            .map(|value| i64_to_u64(value, "request event list upstream_ttfb_ms"))
            .transpose()?,
        upstream_body_ms: payload
            .upstream_body_ms
            .map(|value| i64_to_u64(value, "request event list upstream_body_ms"))
            .transpose()?,
        stream_first_content_delta_ms: payload
            .stream_first_content_delta_ms
            .map(|value| i64_to_u64(value, "request event list stream_first_content_delta_ms"))
            .transpose()?,
        stream_last_content_delta_ms: payload
            .stream_last_content_delta_ms
            .map(|value| i64_to_u64(value, "request event list stream_last_content_delta_ms"))
            .transpose()?,
        inter_token_avg_ms: payload
            .inter_token_avg_ms
            .map(|value| i64_to_u64(value, "request event list inter_token_avg_ms"))
            .transpose()?,
        input_tokens: row
            .input_tokens
            .map(|value| i64_to_u64(value, "request event list input_tokens"))
            .transpose()?,
        output_tokens: row
            .output_tokens
            .map(|value| i64_to_u64(value, "request event list output_tokens"))
            .transpose()?,
        cache_creation_input_tokens: row
            .cache_creation_input_tokens
            .map(|value| i64_to_u64(value, "request event list cache_creation_input_tokens"))
            .transpose()?,
        cache_creation_input_tokens_5m: row
            .cache_creation_input_tokens_5m
            .map(|value| i64_to_u64(value, "request event list cache_creation_input_tokens_5m"))
            .transpose()?,
        cache_creation_input_tokens_1h: row
            .cache_creation_input_tokens_1h
            .map(|value| i64_to_u64(value, "request event list cache_creation_input_tokens_1h"))
            .transpose()?,
        cache_read_input_tokens: row
            .cache_read_input_tokens
            .map(|value| i64_to_u64(value, "request event list cache_read_input_tokens"))
            .transpose()?,
        cost_usd_micros: payload.cost_usd_micros,
        cost_input_micros: payload.cost_input_micros,
        cost_output_micros: payload.cost_output_micros,
        cost_cache_creation_5m_micros: payload.cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros: payload.cost_cache_creation_1h_micros,
        cost_cache_read_micros: payload.cost_cache_read_micros,
    })
}

fn parse_upstream(value: &str) -> StorageResult<RequestEventUpstream> {
    match value {
        "anthropic_direct" => Ok(RequestEventUpstream::AnthropicDirect),
        other => Err(StorageError::Corrupted {
            message: format!("request event list unknown upstream value: {other}"),
        }),
    }
}
