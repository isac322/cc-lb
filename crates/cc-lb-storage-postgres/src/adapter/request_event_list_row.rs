use cc_lb_storage_api::{RequestEventListItem, RequestEventUpstream, StorageError, StorageResult};

use crate::adapter::i64_to_u64;

use super::request_event_list_sql::ListRow;

pub(super) fn list_row_to_item(row: ListRow) -> StorageResult<RequestEventListItem> {
    Ok(RequestEventListItem {
        ts: i64_to_u64(row.ts_secs, "request event list ts")?,
        ts_ms: row
            .ts_ms
            .map(|value| i64_to_u64(value, "request event list ts_ms"))
            .transpose()?,
        request_id: row.request_id,
        event_id: row.event_id,
        principal_id: row.principal_id,
        upstream: row.upstream.as_deref().map(parse_upstream).transpose()?,
        upstream_id: row.upstream_id,
        upstream_name: row.upstream_name,
        thread_id: row.thread_id,
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
        duration_ms: row
            .duration_ms
            .map(|value| i64_to_u64(value, "request event list duration_ms"))
            .transpose()?
            .unwrap_or(0),
        auth_ms: row
            .auth_ms
            .map(|value| i64_to_u64(value, "request event list auth_ms"))
            .transpose()?,
        route_ms: row
            .route_ms
            .map(|value| i64_to_u64(value, "request event list route_ms"))
            .transpose()?,
        limit_reserve_ms: row
            .limit_reserve_ms
            .map(|value| i64_to_u64(value, "request event list limit_reserve_ms"))
            .transpose()?,
        bulkhead_wait_ms: row
            .bulkhead_wait_ms
            .map(|value| i64_to_u64(value, "request event list bulkhead_wait_ms"))
            .transpose()?,
        dns_ms: row
            .dns_ms
            .map(|value| i64_to_u64(value, "request event list dns_ms"))
            .transpose()?,
        connect_ms: row
            .connect_ms
            .map(|value| i64_to_u64(value, "request event list connect_ms"))
            .transpose()?,
        connection_reused: row.connection_reused,
        limit_reconcile_ms: row
            .limit_reconcile_ms
            .map(|value| i64_to_u64(value, "request event list limit_reconcile_ms"))
            .transpose()?,
        observability_post_ms: row
            .observability_post_ms
            .map(|value| i64_to_u64(value, "request event list observability_post_ms"))
            .transpose()?,
        proxy_setup_ms: row
            .proxy_setup_ms
            .map(|value| i64_to_u64(value, "request event list proxy_setup_ms"))
            .transpose()?,
        shape_ms: row
            .shape_ms
            .map(|value| i64_to_u64(value, "request event list shape_ms"))
            .transpose()?,
        sign_ms: row
            .sign_ms
            .map(|value| i64_to_u64(value, "request event list sign_ms"))
            .transpose()?,
        upstream_ttfb_ms: row
            .upstream_ttfb_ms
            .map(|value| i64_to_u64(value, "request event list upstream_ttfb_ms"))
            .transpose()?,
        upstream_body_ms: row
            .upstream_body_ms
            .map(|value| i64_to_u64(value, "request event list upstream_body_ms"))
            .transpose()?,
        stream_first_content_delta_ms: row
            .stream_first_content_delta_ms
            .map(|value| i64_to_u64(value, "request event list stream_first_content_delta_ms"))
            .transpose()?,
        stream_last_content_delta_ms: row
            .stream_last_content_delta_ms
            .map(|value| i64_to_u64(value, "request event list stream_last_content_delta_ms"))
            .transpose()?,
        inter_token_avg_ms: row
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
        cost_usd_micros: row.cost_usd_micros,
        cost_input_micros: row.cost_input_micros,
        cost_output_micros: row.cost_output_micros,
        cost_cache_creation_5m_micros: row.cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros: row.cost_cache_creation_1h_micros,
        cost_cache_read_micros: row.cost_cache_read_micros,
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
