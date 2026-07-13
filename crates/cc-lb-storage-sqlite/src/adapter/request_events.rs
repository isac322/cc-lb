use async_trait::async_trait;
use cc_lb_storage_api::{
    RequestEvent, RequestEventListItem, RequestEventListQuery, RequestEventStore,
    RequestEventStreamFilters, StorageError, StorageResult,
};
use sqlx::AssertSqlSafe;
use uuid::Uuid;

use super::request_event_list_sql;
use crate::{SqliteStorage, map_sqlx_error};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl RequestEventStore for SqliteStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
        let payload = serde_json::to_string(event)?;
        let cache_breakpoints = serde_json::to_string(&event.cache_breakpoints)?;
        let event_id = storage_event_id(event);
        let inserted_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO request_events_v1 \
             (request_id, ts, event_type, upstream_id, principal_id, created_at, key_id, model, upstream_name, cache_state, thread_id, message_id, message_index, message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash, input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, event_id, error_code, upstream_error_type, upstream_error_message, thinking_tokens, web_search_requests, web_fetch_requests, service_tier, inference_geo, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, matched_v3_cache_key, breakpoint_content_block_index, matched_content_block_index, lookback_distance, predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, token_estimate_source, cache_value_micros, formula_winner_upstream_id, kept_upstream_id, quota_urgency_5h, quota_urgency_7d, quota_urgency_combined, quota_weight_factor, quota_cache_multiplier, quota_warning_multiplier, quota_effective_weight, quota_uniform_fallback, wrh_key_source, lineage_would_have_predicted_read_tokens, lineage_would_have_picked_upstream_id, thinking_budget_tokens, reasoning_effort, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING \
             RETURNING id",
        )
        .bind(&event.request_id)
        .bind(u64_to_i64(event_ts_secs(event), "request event ts")?)
        .bind("request")
        .bind(event.upstream_id.map(|id| id.to_string()))
        .bind(event.principal_id.as_deref())
        .bind(u64_to_i64(event_ts_secs(event), "request event created_at")?)
        .bind(event.key_id.as_deref())
        .bind(event.model.as_deref())
        .bind(event.upstream_name.as_deref())
        .bind(event.cache_state.map(|state| state.as_str()))
        .bind(event.thread_id.as_deref())
        .bind(event.message_id.as_deref())
        .bind(option_u64_to_i64(event.message_index, "request event message_index")?)
        .bind(option_u64_to_i64(event.message_count, "request event message_count")?)
        .bind(option_u64_to_i64(
            event.cache_control_block_count,
            "request event cache_control_block_count",
        )?)
        .bind(cache_breakpoints)
        .bind(event.cache_prefix_hash.as_deref())
        .bind(option_u64_to_i64(event.input_tokens, "request event input_tokens")?)
        .bind(option_u64_to_i64(event.output_tokens, "request event output_tokens")?)
        .bind(option_u64_to_i64(
            event.cache_creation_input_tokens,
            "request event cache_creation_input_tokens",
        )?)
        .bind(option_u64_to_i64(
            event.cache_read_input_tokens,
            "request event cache_read_input_tokens",
        )?)
        .bind(event_id.as_str())
        .bind(event.error_code.as_deref())
        .bind(event.upstream_error_type.as_deref())
        .bind(event.upstream_error_message.as_deref())
        .bind(option_u64_to_i64(event.thinking_tokens, "request event thinking_tokens")?)
        .bind(option_u64_to_i64(event.web_search_requests, "request event web_search_requests")?)
        .bind(option_u64_to_i64(event.web_fetch_requests, "request event web_fetch_requests")?)
        .bind(event.service_tier.as_deref())
        .bind(event.inference_geo.as_deref())
        .bind(option_u64_to_i64(
            event.cache_creation_input_tokens_5m,
            "request event cache_creation_input_tokens_5m",
        )?)
        .bind(option_u64_to_i64(
            event.cache_creation_input_tokens_1h,
            "request event cache_creation_input_tokens_1h",
        )?)
        .bind(event.matched_v3_cache_key.as_deref())
        .bind(option_u64_to_i64(
            event.breakpoint_content_block_index,
            "request event breakpoint_content_block_index",
        )?)
        .bind(option_u64_to_i64(
            event.matched_content_block_index,
            "request event matched_content_block_index",
        )?)
        .bind(option_u64_to_i64(
            event.lookback_distance,
            "request event lookback_distance",
        )?)
        .bind(option_u64_to_i64(
            event.predicted_cache_read_tokens,
            "request event predicted_cache_read_tokens",
        )?)
        .bind(option_u64_to_i64(
            event.predicted_cache_creation_tokens_5m,
            "request event predicted_cache_creation_tokens_5m",
        )?)
        .bind(option_u64_to_i64(
            event.predicted_cache_creation_tokens_1h,
            "request event predicted_cache_creation_tokens_1h",
        )?)
        .bind(event.token_estimate_source.as_deref())
        .bind(event.cache_value_micros)
        .bind(event.formula_winner_upstream_id.map(|id| id.to_string()))
        .bind(event.kept_upstream_id.map(|id| id.to_string()))
        .bind(event.quota_urgency_5h)
        .bind(event.quota_urgency_7d)
        .bind(event.quota_urgency_combined)
        .bind(event.quota_weight_factor)
        .bind(event.quota_cache_multiplier)
        .bind(event.quota_warning_multiplier)
        .bind(event.quota_effective_weight)
        .bind(event.quota_uniform_fallback)
        .bind(event.wrh_key_source.as_deref())
        .bind(option_u64_to_i64(
            event.lineage_would_have_predicted_read_tokens,
            "request event lineage_would_have_predicted_read_tokens",
        )?)
        .bind(event.lineage_would_have_picked_upstream_id.map(|id| id.to_string()))
        .bind(option_u64_to_i64(event.thinking_budget_tokens, "thinking_budget_tokens")?)
        .bind(event.reasoning_effort.as_deref())
        .bind(payload)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        let id = match inserted_id {
            Some(id) => id,
            None => select_existing_event_id(self, &event_id).await?,
        };
        i64_to_u64(id, "request event cursor")
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        query_request_events(self, since, until, limit, "ASC").await
    }

    async fn query_recent_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        query_request_events(self, since, until, limit, "DESC").await
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_secs = (cutoff_ms_x_1m / KEY_SEQUENCE_SCALE) / 1_000;
        let result = sqlx::query(
            "DELETE FROM request_events_v1 \
             WHERE id IN ( \
                 SELECT id FROM request_events_v1 \
                 WHERE ts < ? \
                 ORDER BY id ASC \
                 LIMIT ? \
             )",
        )
        .bind(u64_to_i64(
            cutoff_secs,
            "request event prune before cutoff",
        )?)
        .bind(u64_to_i64(
            batch_size as u64,
            "request event prune before batch size",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }

    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        let cursor =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM request_events_v1")
                .fetch_one(self.pool())
                .await
                .map_err(map_sqlx_error)?;
        i64_to_u64(cursor, "request event cursor")
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        filters: &RequestEventStreamFilters,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        if limit == 0 || until <= after {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT id, payload FROM request_events_v1 \
             WHERE id > ? AND id <= ? \
             ORDER BY id ASC LIMIT ?",
        )
        .bind(u64_to_i64(after, "request event cursor after")?)
        .bind(u64_to_i64(until, "request event cursor until")?)
        .bind(usize_to_i64(limit.min(500), "request event cursor limit")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|(id, payload)| {
                let cursor = i64_to_u64(id, "request event cursor")?;
                let event = serde_json::from_str::<RequestEvent>(&payload)?;
                Ok((cursor, event))
            })
            .filter(|result| match result {
                Ok((_, event)) => request_event_matches_filters(event, filters),
                Err(_) => true,
            })
            .collect()
    }

    async fn list_request_events(
        &self,
        query: &RequestEventListQuery,
    ) -> StorageResult<Vec<RequestEventListItem>> {
        request_event_list_sql::list_request_events(self, query).await
    }

    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        request_event_list_sql::get_request_event(self, event_id).await
    }
}

async fn select_existing_event_id(storage: &SqliteStorage, event_id: &str) -> StorageResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT id FROM request_events_v1 WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(storage.pool())
        .await
        .map_err(map_sqlx_error)
}

async fn query_request_events(
    storage: &SqliteStorage,
    since: u64,
    until: u64,
    limit: usize,
    direction: &str,
) -> StorageResult<Vec<RequestEvent>> {
    if limit == 0 || until < since {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT payload FROM request_events_v1 \
         WHERE ts >= ? AND ts <= ? \
         ORDER BY id {direction} LIMIT ?"
    );
    let rows = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(u64_to_i64(since, "request event since")?)
        .bind(u64_to_i64_upper(until))
        .bind(usize_to_i64(limit, "request event limit")?)
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .collect()
}

fn event_ts_secs(event: &RequestEvent) -> u64 {
    event.ts_ms.map(|ts_ms| ts_ms / 1_000).unwrap_or(event.ts)
}

fn storage_event_id(event: &RequestEvent) -> String {
    event
        .event_id
        .clone()
        .unwrap_or_else(|| format!("{}-legacy-live-{}", event_ts_secs(event), Uuid::now_v7()))
}

fn request_event_matches_filters(
    event: &RequestEvent,
    filters: &RequestEventStreamFilters,
) -> bool {
    if let Some(principal_id) = filters.principal_id.as_deref()
        && event.principal_id.as_deref() != Some(principal_id)
    {
        return false;
    }
    if let Some(model) = filters.model.as_deref()
        && event.model.as_deref() != Some(model)
    {
        return false;
    }
    if let Some(upstream) = filters.upstream
        && event.upstream != Some(upstream)
    {
        return false;
    }
    if let Some(upstream_id) = filters.upstream_id
        && event.upstream_id != Some(upstream_id)
    {
        return false;
    }
    if let Some(status_class) = filters.status_class
        && !status_class.matches(event.status)
    {
        return false;
    }
    true
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

pub(super) fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite INTEGER"),
    })
}

pub(super) fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

pub(super) fn u64_to_i64_upper(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) fn usize_to_i64(value: usize, field: &str) -> StorageResult<i64> {
    i64::try_from(value).or_else(|_| {
        if value == usize::MAX {
            Ok(i64::MAX)
        } else {
            Err(StorageError::Fatal {
                message: format!("{field} cannot be represented as sqlite INTEGER"),
            })
        }
    })
}
