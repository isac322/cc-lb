use async_trait::async_trait;
use cc_lb_storage_api::{
    RequestEvent, RequestEventKeyLastUsed, RequestEventKeyLastUsedQuery,
    RequestEventKeyUsageBucket, RequestEventKeyUsageQuery, RequestEventListItem,
    RequestEventListQuery, RequestEventStore, RequestEventStreamFilters, StorageError,
    StorageResult,
};
use chrono::{DateTime, Utc};

use super::request_event_list_sql;
use crate::{
    adapter::{
        PostgresStorage, i64_to_u64, u64_to_i64, unix_secs_to_datetime,
        unix_secs_to_datetime_lower, unix_secs_to_datetime_upper,
    },
    error_map::map_sqlx_error,
};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl RequestEventStore for PostgresStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
        let payload = serde_json::to_vec(event)?;
        let cache_breakpoints = serde_json::to_value(&event.cache_breakpoints)?;
        let event_id = storage_event_id(event);
        let inserted_seq = sqlx::query_scalar::<_, i64>(
            "INSERT INTO request_events_v1 \
              (ts, principal_id, upstream_id, key_id, model, upstream_name, cache_state, thread_id, message_id, \
               message_index, message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash, \
                input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, \
                 event_id, error_code, upstream_error_type, upstream_error_message, \
                 thinking_tokens, web_search_requests, web_fetch_requests, \
                  service_tier, inference_geo, \
                  cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, \
                  matched_v3_cache_key, breakpoint_content_block_index, matched_content_block_index, lookback_distance, \
                  predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, \
                   token_estimate_source, cache_value_micros, formula_winner_upstream_id, kept_upstream_id, \
                   quota_urgency_5h, quota_urgency_7d, quota_urgency_combined, quota_weight_factor, \
                   quota_cache_multiplier, quota_warning_multiplier, quota_effective_weight, quota_uniform_fallback, \
                   wrh_key_source, lineage_would_have_predicted_read_tokens, lineage_would_have_picked_upstream_id, \
                   thinking_budget_tokens, reasoning_effort, payload, created_at) \
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,NOW()) \
              ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING \
              RETURNING seq",
        )
        .bind(unix_secs_to_datetime(event.ts, "request event ts")?)
        .bind(event.principal_id.as_deref())
        .bind(event.upstream_id)
        .bind(event.key_id.as_deref())
        .bind(event.model.as_deref())
        .bind(event.upstream_name.as_deref())
        .bind(event.cache_state.map(|state| state.as_str()))
        .bind(event.thread_id.as_deref())
        .bind(event.message_id.as_deref())
        .bind(
            event
                .message_index
                .map(|value| u64_to_i64(value, "request event message_index"))
                .transpose()?,
        )
        .bind(
            event
                .message_count
                .map(|value| u64_to_i64(value, "request event message_count"))
                .transpose()?,
        )
        .bind(
            event
                .cache_control_block_count
                .map(|value| u64_to_i64(value, "request event cache_control_block_count"))
                .transpose()?,
        )
        .bind(cache_breakpoints)
        .bind(event.cache_prefix_hash.as_deref())
        .bind(
            event
                .input_tokens
                .map(|value| u64_to_i64(value, "request event input_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .output_tokens
                .map(|value| u64_to_i64(value, "request event output_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .cache_creation_input_tokens
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .cache_read_input_tokens
                .map(|value| u64_to_i64(value, "request event cache_read_input_tokens"))
                .transpose()?,
        )
        .bind(event_id.as_str())
        .bind(event.error_code.as_deref())
        .bind(event.upstream_error_type.as_deref())
        .bind(event.upstream_error_message.as_deref())
        .bind(
            event
                .thinking_tokens
                .map(|value| u64_to_i64(value, "request event thinking_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .web_search_requests
                .map(|value| u64_to_i64(value, "request event web_search_requests"))
                .transpose()?,
        )
        .bind(
            event
                .web_fetch_requests
                .map(|value| u64_to_i64(value, "request event web_fetch_requests"))
                .transpose()?,
        )
        .bind(event.service_tier.as_deref())
        .bind(event.inference_geo.as_deref())
        .bind(
            event
                .cache_creation_input_tokens_5m
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens_5m"))
                .transpose()?,
        )
        .bind(
            event
                .cache_creation_input_tokens_1h
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens_1h"))
                .transpose()?,
        )
        .bind(event.matched_v3_cache_key.as_deref())
        .bind(
            event
                .breakpoint_content_block_index
                .map(|value| u64_to_i64(value, "request event breakpoint_content_block_index"))
                .transpose()?,
        )
        .bind(
            event
                .matched_content_block_index
                .map(|value| u64_to_i64(value, "request event matched_content_block_index"))
                .transpose()?,
        )
        .bind(
            event
                .lookback_distance
                .map(|value| u64_to_i64(value, "request event lookback_distance"))
                .transpose()?,
        )
        .bind(
            event
                .predicted_cache_read_tokens
                .map(|value| u64_to_i64(value, "request event predicted_cache_read_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .predicted_cache_creation_tokens_5m
                .map(|value| u64_to_i64(value, "request event predicted_cache_creation_tokens_5m"))
                .transpose()?,
        )
        .bind(
            event
                .predicted_cache_creation_tokens_1h
                .map(|value| u64_to_i64(value, "request event predicted_cache_creation_tokens_1h"))
                .transpose()?,
        )
        .bind(event.token_estimate_source.as_deref())
        .bind(event.cache_value_micros)
        .bind(event.formula_winner_upstream_id)
        .bind(event.kept_upstream_id)
        .bind(event.quota_urgency_5h)
        .bind(event.quota_urgency_7d)
        .bind(event.quota_urgency_combined)
        .bind(event.quota_weight_factor)
        .bind(event.quota_cache_multiplier)
        .bind(event.quota_warning_multiplier)
        .bind(event.quota_effective_weight)
        .bind(event.quota_uniform_fallback)
        .bind(event.wrh_key_source.as_deref())
        .bind(
            event
                .lineage_would_have_predicted_read_tokens
                .map(|value| u64_to_i64(value, "request event lineage_would_have_predicted_read_tokens"))
                .transpose()?,
        )
        .bind(event.lineage_would_have_picked_upstream_id)
        .bind(
            event
                .thinking_budget_tokens
                .map(|value| u64_to_i64(value, "request event thinking_budget_tokens"))
                .transpose()?,
        )
        .bind(event.reasoning_effort.as_deref())
        .bind(payload)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        let seq = match inserted_seq {
            Some(seq) => seq,
            None => select_existing_event_id(self, &event_id).await?,
        };
        i64_to_u64(seq, "request event cursor")
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }
        let Some(since) = unix_secs_to_datetime_lower(since, "request event since")? else {
            return Ok(Vec::new());
        };
        let until = unix_secs_to_datetime_upper(until, "request event until")?;

        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2)              ORDER BY seq ASC LIMIT $3",
        )
        .bind(since)
        .bind(until)
        .bind(u64_to_i64(limit as u64, "request event limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|payload| serde_json::from_slice(&payload).map_err(Into::into))
            .collect()
    }

    async fn query_recent_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }
        let Some(since) = unix_secs_to_datetime_lower(since, "request event since")? else {
            return Ok(Vec::new());
        };
        let until = unix_secs_to_datetime_upper(until, "request event until")?;

        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2)              ORDER BY seq DESC LIMIT $3",
        )
        .bind(since)
        .bind(until)
        .bind(u64_to_i64(limit as u64, "request event limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|payload| serde_json::from_slice(&payload).map_err(Into::into))
            .collect()
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_ms = cutoff_ms_x_1m / KEY_SEQUENCE_SCALE;
        let result = sqlx::query(
            "DELETE FROM request_events_v1              WHERE seq IN (                 SELECT seq FROM request_events_v1                 WHERE ts < $1                 ORDER BY seq ASC                 LIMIT $2             )",
        )
        .bind(unix_millis_to_datetime(
            cutoff_ms,
            "request event prune before cutoff",
        )?)
        .bind(u64_to_i64(
            batch_size as u64,
            "request event prune before batch size",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }

    /// Returns the highest `seq` that is safe to treat as "current" for a
    /// subsequent `query_request_events_between_cursors` call.
    ///
    /// Uses the same `pg_snapshot_xmin` horizon as that query. A plain
    /// `MAX(seq)` is unsound here: `BIGSERIAL` values are allocated by
    /// `nextval()` before commit, so a transaction with a lower xid can
    /// commit *after* one with a higher xid whose row already counts toward
    /// `MAX(seq)`. If callers advance a "last seen" bookmark to that
    /// uncapped `MAX(seq)`, the lower-xid row can still be behind
    /// `pg_snapshot_xmin` at query time, be excluded by the between-cursors
    /// filter, and then be permanently skipped once the bookmark moves past
    /// it. Capping the max to rows already below the xmin horizon keeps the
    /// two queries consistent: anything counted here is guaranteed to be
    /// returned by a between-cursors call using an equal-or-later snapshot,
    /// because `pg_snapshot_xmin` never decreases over time.
    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        let cursor = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(seq) FILTER ( \
                 WHERE COALESCE(tx_id, '0'::xid8) < pg_snapshot_xmin(pg_current_snapshot()) \
             ), 0)::bigint FROM request_events_v1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        i64_to_u64(cursor, "request event cursor")
    }

    /// Query committed request events in cursor order without skipping rows whose
    /// `BIGSERIAL` value was allocated by a transaction that has not become visible yet.
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

        let rows = sqlx::query_as::<_, (i64, Vec<u8>)>(
            "SELECT seq, payload FROM request_events_v1 \
             WHERE seq > $1 AND seq <= $2 \
               AND COALESCE(tx_id, '0'::xid8) < pg_snapshot_xmin(pg_current_snapshot()) \
             ORDER BY seq ASC LIMIT $3",
        )
        .bind(u64_to_i64(after, "request event cursor after")?)
        .bind(u64_to_i64(until, "request event cursor until")?)
        .bind(u64_to_i64(
            limit.min(500) as u64,
            "request event cursor limit",
        )?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|(seq, payload)| {
                let cursor = i64_to_u64(seq, "request event cursor")?;
                let event = serde_json::from_slice::<RequestEvent>(&payload)?;
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

    async fn request_event_key_last_used(
        &self,
        query: &RequestEventKeyLastUsedQuery,
    ) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
        request_event_key_last_used(self, query).await
    }

    async fn request_event_key_usage(
        &self,
        query: &RequestEventKeyUsageQuery,
    ) -> StorageResult<Vec<RequestEventKeyUsageBucket>> {
        request_event_key_usage(self, query).await
    }

    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        request_event_list_sql::get_request_event(self, event_id).await
    }
}

async fn request_event_key_last_used(
    storage: &PostgresStorage,
    query: &RequestEventKeyLastUsedQuery,
) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
    if query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }
    let Some(since) =
        unix_secs_to_datetime_lower(query.since_unix_secs, "request event key last-used since")?
    else {
        return Ok(Vec::new());
    };
    let until =
        unix_secs_to_datetime_upper(query.until_unix_secs, "request event key last-used until")?;

    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT key_id, EXTRACT(EPOCH FROM MAX(ts))::bigint AS last_used_at_unix_secs \
         FROM request_events_v1 \
         WHERE principal_id = $1 \
           AND key_id IS NOT NULL \
           AND key_id <> '' \
           AND ts >= $2 \
           AND ($3::timestamptz IS NULL OR ts <= $3) \
         GROUP BY key_id",
    )
    .bind(query.principal_id.as_str())
    .bind(since)
    .bind(until)
    .fetch_all(&storage.pool)
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
    storage: &PostgresStorage,
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
        "SELECT LEAST(((event_ts_ms - $1) / $2), $3)::bigint AS bucket_index, \
                COUNT(*)::bigint AS request_count, \
                COALESCE(SUM(COALESCE(input_tokens, 0) \
                    + COALESCE(cache_creation_input_tokens, 0) \
                    + COALESCE(cache_read_input_tokens, 0)), 0)::bigint AS input_tokens, \
                COALESCE(SUM(COALESCE(output_tokens, 0)), 0)::bigint AS output_tokens, \
                COALESCE(SUM(COALESCE((payload_jsonb ->> 'cost_usd_micros')::bigint, 0)), 0)::bigint AS cost_usd_micros \
         FROM ( \
             SELECT COALESCE((convert_from(payload, 'UTF8')::jsonb ->> 'ts_ms')::bigint, EXTRACT(EPOCH FROM ts)::bigint * 1000) AS event_ts_ms, \
                    convert_from(payload, 'UTF8')::jsonb AS payload_jsonb, \
                    input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens \
             FROM request_events_v1 \
             WHERE principal_id = $4 AND key_id = $5 \
         ) matched \
         WHERE event_ts_ms >= $6 AND event_ts_ms <= $7 \
         GROUP BY bucket_index",
    )
    .bind(u64_to_i64(query.range_start_ms, "request event key usage range start")?)
    .bind(u64_to_i64(query.step_ms, "request event key usage step")?)
    .bind(u64_to_i64(
        query.bucket_count.saturating_sub(1),
        "request event key usage last bucket",
    )?)
    .bind(query.principal_id.as_str())
    .bind(query.key_id.as_str())
    .bind(u64_to_i64(query.range_start_ms, "request event key usage lower bound")?)
    .bind(u64_to_i64(query.range_end_ms, "request event key usage upper bound")?)
    .fetch_all(&storage.pool)
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

async fn select_existing_event_id(storage: &PostgresStorage, event_id: &str) -> StorageResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT seq FROM request_events_v1 WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(&storage.pool)
        .await
        .map_err(map_sqlx_error)
}

fn storage_event_id(event: &RequestEvent) -> String {
    event
        .event_id
        .clone()
        .unwrap_or_else(|| format!("{}-legacy-live-{}", event.ts, uuid::Uuid::now_v7()))
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

impl PostgresStorage {
    pub async fn prune_request_events(&self, older_than: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM request_events_v1 WHERE ts < $1")
            .bind(unix_secs_to_datetime(
                older_than,
                "request event prune cutoff",
            )?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn unix_millis_to_datetime(value: u64, field: &str) -> StorageResult<DateTime<Utc>> {
    let millis = i64::try_from(value).map_err(|_| cc_lb_storage_api::StorageError::Fatal {
        message: format!("{field} cannot be represented as postgres timestamptz"),
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis).ok_or_else(|| {
        cc_lb_storage_api::StorageError::Fatal {
            message: format!("{field} cannot be represented as postgres timestamptz"),
        }
    })
}
