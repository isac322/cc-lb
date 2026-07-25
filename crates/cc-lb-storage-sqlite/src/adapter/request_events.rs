use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveProjectionStore, CacheKeepaliveTurnRow, RequestEvent,
    RequestEventKeyLastUsed, RequestEventKeyLastUsedQuery, RequestEventKeyUsageBucket,
    RequestEventKeyUsageQuery, RequestEventListItem, RequestEventListQuery,
    RequestEventProjections, RequestEventStore, RequestEventStreamFilters, StorageError,
    StorageResult,
};
use sqlx::AssertSqlSafe;
use std::time::Instant;
use uuid::Uuid;

use super::request_event_list_sql;
use crate::{SqliteStorage, map_sqlx_error};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl CacheKeepaliveProjectionStore for SqliteStorage {
    async fn append_cache_keepalive_decision(
        &self,
        decision: &CacheKeepaliveDecisionRow,
    ) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        insert_keepalive_decision_in_tx(&mut tx, decision).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }
}

#[async_trait]
impl RequestEventStore for SqliteStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
        let event_id = storage_event_id(event);
        let mut tx = self.begin_immediate().await?;
        let inserted_id = insert_request_event_in_tx(&mut tx, event, &event_id).await?;
        let id = match inserted_id {
            Some(id) => id,
            None => select_existing_event_id_in_tx(&mut tx, &event_id).await?,
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        i64_to_u64(id, "request event cursor")
    }

    async fn append_request_event_with_projections(
        &self,
        event: &RequestEvent,
        projections: &RequestEventProjections,
    ) -> StorageResult<u64> {
        let event_id = storage_event_id(event);
        let mut tx = self.begin_immediate().await?;
        let inserted_id = insert_request_event_in_tx(&mut tx, event, &event_id).await?;
        let id = match inserted_id {
            Some(id) => id,
            None => select_existing_event_id_in_tx(&mut tx, &event_id).await?,
        };
        if let Some(turn) = projections.turn.as_ref() {
            insert_keepalive_turn_in_tx(&mut tx, turn).await?;
        }
        insert_keepalive_decision_in_tx(&mut tx, &projections.decision).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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

        let rows = sqlx::query_as::<_, (i64, Option<String>, Option<String>, String)>(
            "SELECT id, source_kind, source_ref_id, payload FROM request_events_v1 \
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
            .map(|(id, source_kind, source_ref_id, payload)| {
                let cursor = i64_to_u64(id, "request event cursor")?;
                let event = request_event_from_storage(&payload, source_kind, source_ref_id)?;
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
        let start = Instant::now();
        let result = request_event_list_sql::list_request_events(self, query).await;
        record_storage_operation("request_event_list", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn request_event_key_last_used(
        &self,
        query: &RequestEventKeyLastUsedQuery,
    ) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
        let start = Instant::now();
        let result = request_event_key_last_used(self, query).await;
        record_storage_operation("request_event_key_last_used", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn request_event_key_usage(
        &self,
        query: &RequestEventKeyUsageQuery,
    ) -> StorageResult<Vec<RequestEventKeyUsageBucket>> {
        let start = Instant::now();
        let result = request_event_key_usage(self, query).await;
        record_storage_operation("request_event_key_usage", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        request_event_list_sql::get_request_event(self, event_id).await
    }
}

async fn request_event_key_last_used(
    storage: &SqliteStorage,
    query: &RequestEventKeyLastUsedQuery,
) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
    if query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }

    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT key_id, MAX(ts) AS last_used_at_unix_secs \
         FROM request_events_v1 \
         WHERE principal_id = ? \
           AND key_id IS NOT NULL \
           AND key_id <> '' \
           AND ts >= ? \
           AND ts <= ? \
         GROUP BY key_id",
    )
    .bind(query.principal_id.as_str())
    .bind(u64_to_i64(
        query.since_unix_secs,
        "request event key last-used since",
    )?)
    .bind(u64_to_i64_upper(query.until_unix_secs))
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|(key_id, last_used_at_unix_secs)| {
            Ok(RequestEventKeyLastUsed {
                key_id,
                last_used_at_unix_secs: i64_to_u64(
                    last_used_at_unix_secs,
                    "request event key last-used timestamp",
                )?,
            })
        })
        .collect()
}

async fn request_event_key_usage(
    storage: &SqliteStorage,
    query: &RequestEventKeyUsageQuery,
) -> StorageResult<Vec<RequestEventKeyUsageBucket>> {
    if query.bucket_count == 0 || query.step_ms == 0 || query.range_end_ms < query.range_start_ms {
        return Ok(Vec::new());
    }

    let bucket_count = usize::try_from(query.bucket_count).map_err(|_| StorageError::Fatal {
        message: "request event key usage bucket_count cannot be represented as usize".to_owned(),
    })?;
    let mut buckets = vec![RequestEventKeyUsageBucket::default(); bucket_count];
    for (index, bucket) in buckets.iter_mut().enumerate() {
        let bucket_start_ms = query
            .range_start_ms
            .saturating_add((index as u64).saturating_mul(query.step_ms));
        bucket.bucket_start_unix_secs = bucket_start_ms / 1_000;
    }

    let rows = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT MIN(((list_ts_ms - ?) / ?), ?) AS bucket_index, \
                COUNT(*) AS request_count, \
                COALESCE(SUM(COALESCE(input_tokens, 0) \
                    + COALESCE(cache_creation_input_tokens, 0) \
                    + COALESCE(cache_read_input_tokens, 0)), 0) AS input_tokens, \
                COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens, \
                COALESCE(SUM(COALESCE(list_cost_usd_micros, 0)), 0) AS cost_usd_micros \
         FROM request_events_v1 \
         WHERE principal_id = ? \
           AND key_id = ? \
           AND list_ts_ms >= ? \
           AND list_ts_ms <= ? \
         GROUP BY bucket_index",
    )
    .bind(u64_to_i64(
        query.range_start_ms,
        "request event key usage range start",
    )?)
    .bind(u64_to_i64(query.step_ms, "request event key usage step")?)
    .bind(u64_to_i64(
        query.bucket_count.saturating_sub(1),
        "request event key usage last bucket",
    )?)
    .bind(query.principal_id.as_str())
    .bind(query.key_id.as_str())
    .bind(u64_to_i64(
        query.range_start_ms,
        "request event key usage lower bound",
    )?)
    .bind(u64_to_i64_upper(query.range_end_ms))
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    for (bucket_index, request_count, input_tokens, output_tokens, cost_usd_micros) in rows {
        let bucket_index = usize::try_from(bucket_index).map_err(|_| StorageError::Corrupted {
            message: "request event key usage bucket index is negative".to_owned(),
        })?;
        let Some(bucket) = buckets.get_mut(bucket_index) else {
            return Err(StorageError::Corrupted {
                message: "request event key usage bucket index is out of range".to_owned(),
            });
        };
        bucket.request_count = i64_to_u64(request_count, "request event key usage request count")?;
        bucket.input_tokens = i64_to_u64(input_tokens, "request event key usage input tokens")?;
        bucket.output_tokens = i64_to_u64(output_tokens, "request event key usage output tokens")?;
        bucket.cost_usd_micros = cost_usd_micros;
    }

    Ok(buckets)
}

fn record_storage_operation<T>(operation: &'static str, start: Instant, result: &StorageResult<T>) {
    let status = if result.is_ok() { "ok" } else { "error" };
    metrics::histogram!(
        "cc_lb_storage_operation_duration_seconds",
        "store" => "sqlite",
        "operation" => operation,
        "status" => status
    )
    .record(start.elapsed().as_secs_f64());
    if result.is_err() {
        metrics::counter!(
            "cc_lb_storage_operation_errors_total",
            "store" => "sqlite",
            "operation" => operation
        )
        .increment(1);
    }
}

async fn insert_request_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: &RequestEvent,
    event_id: &str,
) -> StorageResult<Option<i64>> {
    let payload = serde_json::to_string(event)?;
    let cache_breakpoints = serde_json::to_string(&event.cache_breakpoints)?;
    sqlx::query_scalar::<_, i64>(
            "INSERT INTO request_events_v1 \
             (request_id, ts, event_type, source_kind, source_ref_id, upstream_id, principal_id, created_at, key_id, model, upstream_name, cache_state, thread_id, message_id, message_index, message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash, input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, event_id, error_code, upstream_error_type, upstream_error_message, thinking_tokens, web_search_requests, web_fetch_requests, service_tier, inference_geo, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, matched_v3_cache_key, breakpoint_content_block_index, matched_content_block_index, lookback_distance, predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, token_estimate_source, cache_value_micros, formula_winner_upstream_id, kept_upstream_id, quota_urgency_5h, quota_urgency_7d, quota_urgency_combined, quota_warning_multiplier, lineage_would_have_predicted_read_tokens, lineage_would_have_picked_upstream_id, thinking_budget_tokens, reasoning_effort, payload, list_ts_ms, list_event_key, list_upstream, list_status, list_duration_ms, list_auth_ms, list_route_ms, list_limit_reserve_ms, list_bulkhead_wait_ms, list_dns_ms, list_connect_ms, list_connection_reused, list_limit_reconcile_ms, list_observability_post_ms, list_proxy_setup_ms, list_shape_ms, list_sign_ms, list_upstream_ttfb_ms, list_upstream_body_ms, list_stream_first_content_delta_ms, list_stream_last_content_delta_ms, list_inter_token_avg_ms, list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, list_cost_cache_read_micros, observed_session_id, request_kind, claude_agent_id, claude_parent_agent_id, parent_session_id, client_app, session_id_source) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING \
             RETURNING id",
        )
        .bind(&event.request_id)
        .bind(u64_to_i64(event_ts_secs(event), "request event ts")?)
        .bind("request")
        .bind(event.source_kind.as_deref())
        .bind(event.source_ref_id.as_deref())
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
        .bind(event_id)
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
        .bind(event.quota_warning_multiplier)
        .bind(option_u64_to_i64(
            event.lineage_would_have_predicted_read_tokens,
            "request event lineage_would_have_predicted_read_tokens",
        )?)
        .bind(event.lineage_would_have_picked_upstream_id.map(|id| id.to_string()))
        .bind(option_u64_to_i64(event.thinking_budget_tokens, "thinking_budget_tokens")?)
        .bind(event.reasoning_effort.as_deref())
        .bind(payload)
        .bind(u64_to_i64(
            event.ts_ms.unwrap_or_else(|| event_ts_secs(event).saturating_mul(1_000)),
            "request event list_ts_ms",
        )?)
        .bind(event.event_id.as_deref().unwrap_or(&event.request_id))
        .bind(event.upstream.map(|upstream| match upstream {
            cc_lb_storage_api::RequestEventUpstream::AnthropicDirect => "anthropic_direct",
        }))
        .bind(i64::from(event.status))
        .bind(u64_to_i64(event.duration_ms, "request event list_duration_ms")?)
        .bind(option_u64_to_i64(event.auth_ms, "request event list_auth_ms")?)
        .bind(option_u64_to_i64(event.route_ms, "request event list_route_ms")?)
        .bind(option_u64_to_i64(
            event.limit_reserve_ms,
            "request event list_limit_reserve_ms",
        )?)
        .bind(option_u64_to_i64(
            event.bulkhead_wait_ms,
            "request event list_bulkhead_wait_ms",
        )?)
        .bind(option_u64_to_i64(event.dns_ms, "request event list_dns_ms")?)
        .bind(option_u64_to_i64(
            event.connect_ms,
            "request event list_connect_ms",
        )?)
        .bind(event.connection_reused.map(i64::from))
        .bind(option_u64_to_i64(
            event.limit_reconcile_ms,
            "request event list_limit_reconcile_ms",
        )?)
        .bind(option_u64_to_i64(
            event.observability_post_ms,
            "request event list_observability_post_ms",
        )?)
        .bind(option_u64_to_i64(
            event.proxy_setup_ms,
            "request event list_proxy_setup_ms",
        )?)
        .bind(option_u64_to_i64(event.shape_ms, "request event list_shape_ms")?)
        .bind(option_u64_to_i64(event.sign_ms, "request event list_sign_ms")?)
        .bind(option_u64_to_i64(
            event.upstream_ttfb_ms,
            "request event list_upstream_ttfb_ms",
        )?)
        .bind(option_u64_to_i64(
            event.upstream_body_ms,
            "request event list_upstream_body_ms",
        )?)
        .bind(option_u64_to_i64(
            event.stream_first_content_delta_ms,
            "request event list_stream_first_content_delta_ms",
        )?)
        .bind(option_u64_to_i64(
            event.stream_last_content_delta_ms,
            "request event list_stream_last_content_delta_ms",
        )?)
        .bind(option_u64_to_i64(
            event.inter_token_avg_ms,
            "request event list_inter_token_avg_ms",
        )?)
        .bind(event.cost_usd_micros)
        .bind(event.cost_input_micros)
        .bind(event.cost_output_micros)
        .bind(event.cost_cache_creation_5m_micros)
        .bind(event.cost_cache_creation_1h_micros)
        .bind(event.cost_cache_read_micros)
        .bind(event.observed_session_id.as_deref())
        .bind(event.request_kind.as_deref())
        .bind(event.claude_agent_id.as_deref())
        .bind(event.claude_parent_agent_id.as_deref())
        .bind(event.parent_session_id.as_deref())
        .bind(event.client_app.as_deref())
        .bind(event.session_id_source.as_deref())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)
}
async fn select_existing_event_id_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event_id: &str,
) -> StorageResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT id FROM request_events_v1 WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx_error)
}

async fn insert_keepalive_turn_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &CacheKeepaliveTurnRow,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_turns \
         (source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source_ref_id) DO NOTHING",
    )
    .bind(&row.source_ref_id)
    .bind(&row.session_key_hash)
    .bind(&row.principal_id)
    .bind(row.accounting_key_id.as_deref())
    .bind(row.upstream_id.to_string())
    .bind(&row.model)
    .bind(u64_to_i64(row.input_tokens, "cache keepalive input_tokens")?)
    .bind(u64_to_i64(row.output_tokens, "cache keepalive output_tokens")?)
    .bind(u64_to_i64(
        row.cache_creation_input_tokens,
        "cache keepalive cache_creation_input_tokens",
    )?)
    .bind(u64_to_i64(
        row.cache_creation_input_tokens_5m,
        "cache keepalive cache_creation_input_tokens_5m",
    )?)
    .bind(u64_to_i64(
        row.cache_creation_input_tokens_1h,
        "cache keepalive cache_creation_input_tokens_1h",
    )?)
    .bind(u64_to_i64(
        row.cache_read_input_tokens,
        "cache keepalive cache_read_input_tokens",
    )?)
    .bind(row.cost_micros)
    .bind(&row.hit_miss)
    .bind(u64_to_i64(row.ts, "cache keepalive ts")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_keepalive_decision_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &CacheKeepaliveDecisionRow,
) -> StorageResult<()> {
    let config_snapshot = row
        .config_snapshot
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    sqlx::query(
        "INSERT INTO cache_keepalive_decisions \
         (source_ref_id, principal_id, session_key_hash, upstream_id, decision, reason, error, generation, ttl, config_snapshot, last_message_at_ms, ts) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source_ref_id) DO NOTHING",
    )
    .bind(&row.source_ref_id)
    .bind(&row.principal_id)
    .bind(row.session_key_hash.as_deref())
    .bind(row.upstream_id.to_string())
    .bind(&row.decision)
    .bind(&row.reason)
    .bind(row.error.as_deref())
    .bind(u64_to_i64(row.generation, "cache keepalive generation")?)
    .bind(cache_keepalive_ttl_to_db(row.ttl))
    .bind(config_snapshot)
    .bind(u64_to_i64(
        row.last_message_at_ms,
        "cache keepalive decision last_message_at_ms",
    )?)
    .bind(u64_to_i64(row.ts, "cache keepalive decision ts")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn cache_keepalive_ttl_to_db(ttl: cc_lb_storage_api::CacheTtl) -> &'static str {
    match ttl {
        cc_lb_storage_api::CacheTtl::Ttl5m => "5m",
        cc_lb_storage_api::CacheTtl::Ttl1h => "1h",
    }
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
        "SELECT source_kind, source_ref_id, payload FROM request_events_v1 \
         WHERE ts >= ? AND ts <= ? \
         ORDER BY id {direction} LIMIT ?"
    );
    let rows = sqlx::query_as::<_, (Option<String>, Option<String>, String)>(AssertSqlSafe(sql))
        .bind(u64_to_i64(since, "request event since")?)
        .bind(u64_to_i64_upper(until))
        .bind(usize_to_i64(limit, "request event limit")?)
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|(source_kind, source_ref_id, payload)| {
            request_event_from_storage(&payload, source_kind, source_ref_id)
        })
        .collect()
}

fn request_event_from_storage(
    payload: &str,
    source_kind: Option<String>,
    source_ref_id: Option<String>,
) -> StorageResult<RequestEvent> {
    let mut event = serde_json::from_str::<RequestEvent>(payload)?;
    if source_kind.is_some() {
        event.source_kind = source_kind;
    }
    if source_ref_id.is_some() {
        event.source_ref_id = source_ref_id;
    }
    Ok(event)
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
    if let Some(thread_id) = filters.thread_id.as_deref()
        && event.thread_id.as_deref() != Some(thread_id)
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
