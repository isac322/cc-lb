use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveProjectionStore, CacheKeepaliveTurnRow, RequestEvent,
    RequestEventHistogramBucket, RequestEventHistogramQuery, RequestEventKeyLastUsed,
    RequestEventKeyLastUsedQuery, RequestEventKeyUsageBucket, RequestEventKeyUsageQuery,
    RequestEventListItem, RequestEventListQuery, RequestEventPrincipalCostBucket,
    RequestEventPrincipalCostQuery, RequestEventProjections, RequestEventStore,
    RequestEventStreamFilters, StorageError, StorageResult, model_filter_like_pattern,
    model_filter_matches, normalize_usage_rollup_dimension,
};
use sqlx::AssertSqlSafe;
use std::{collections::BTreeMap, time::Instant};
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

    async fn request_event_principal_costs(
        &self,
        query: &RequestEventPrincipalCostQuery,
    ) -> StorageResult<Vec<RequestEventPrincipalCostBucket>> {
        let start = Instant::now();
        let result = request_event_principal_costs(self, query).await;
        record_storage_operation("request_event_principal_costs", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn request_event_histogram(
        &self,
        query: &RequestEventHistogramQuery,
    ) -> StorageResult<Vec<RequestEventHistogramBucket>> {
        let start = Instant::now();
        let result = request_event_histogram(self, query).await;
        record_storage_operation("request_event_histogram", start, &result);
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

type PrincipalCostRow = (Option<String>, i64, i64, i64, i64, i64, i64, i64, i64);

const PRINCIPAL_COST_AGGREGATE_PREFIX: &str = "SELECT principal_id, bucket_index, \
        COALESCE(SUM(CASE WHEN list_cost_usd_micros > 0 \
            THEN list_cost_usd_micros ELSE 0 END), 0) AS total_cost_micros, \
        MAX(CASE WHEN list_cost_input_micros IS NOT NULL \
            OR list_cost_output_micros IS NOT NULL \
            OR list_cost_cache_creation_5m_micros IS NOT NULL \
            OR list_cost_cache_creation_1h_micros IS NOT NULL \
            OR list_cost_cache_read_micros IS NOT NULL \
            THEN 1 ELSE 0 END) AS component_costs_recorded, \
        COALESCE(SUM(CASE WHEN list_cost_input_micros > 0 \
            THEN list_cost_input_micros ELSE 0 END), 0) AS cost_input_micros, \
        COALESCE(SUM(CASE WHEN list_cost_output_micros > 0 \
            THEN list_cost_output_micros ELSE 0 END), 0) AS cost_output_micros, \
        COALESCE(SUM(CASE WHEN list_cost_cache_creation_5m_micros > 0 \
            THEN list_cost_cache_creation_5m_micros ELSE 0 END), 0) \
            AS cost_cache_creation_5m_micros, \
        COALESCE(SUM(CASE WHEN list_cost_cache_creation_1h_micros > 0 \
            THEN list_cost_cache_creation_1h_micros ELSE 0 END), 0) \
            AS cost_cache_creation_1h_micros, \
        COALESCE(SUM(CASE WHEN list_cost_cache_read_micros > 0 \
            THEN list_cost_cache_read_micros ELSE 0 END), 0) \
            AS cost_cache_read_micros \
    FROM (";

const PRINCIPAL_COST_AGGREGATE_SUFFIX: &str = ") matched \
    GROUP BY principal_id, bucket_index \
    ORDER BY bucket_index ASC, principal_id ASC";

const UUID_PRINCIPAL_COST_SOURCE_SQL: &str = "SELECT principal_id, \
        ((list_ts_ms - ?1) / ?2) AS bucket_index, \
        list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
        list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
        list_cost_cache_read_micros \
    FROM request_events_v1 \
    WHERE list_ts_ms >= ?1 \
      AND list_ts_ms < ?3 \
      AND principal_id IN (SELECT CAST(value AS TEXT) FROM json_each(?4))";

const FILTERED_UUID_PRINCIPAL_COST_SOURCE_SQL: &str = "SELECT principal_id, \
        ((list_ts_ms - ?1) / ?2) AS bucket_index, \
        list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
        list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
        list_cost_cache_read_micros \
    FROM request_events_v1 \
    WHERE list_ts_ms >= ?1 \
      AND list_ts_ms < ?3 \
      AND upstream_id = ?4 \
      AND principal_id IN (SELECT CAST(value AS TEXT) FROM json_each(?5))";

const NON_UUID_PRINCIPAL_COST_SOURCE_SQL: &str = "SELECT principal_id, \
        ((list_ts_ms - ?1) / ?2) AS bucket_index, \
        list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
        list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
        list_cost_cache_read_micros \
    FROM request_events_v1 \
    WHERE list_ts_ms >= ?1 \
      AND list_ts_ms < ?3 \
      AND (principal_id IS NULL \
        OR length(principal_id) <> 36 \
        OR length(replace(principal_id, '-', '')) <> 32 \
        OR substr(principal_id, 9, 1) <> '-' \
        OR substr(principal_id, 14, 1) <> '-' \
        OR substr(principal_id, 19, 1) <> '-' \
        OR substr(principal_id, 24, 1) <> '-' \
        OR lower(replace(principal_id, '-', '')) GLOB '*[^0-9a-f]*')";

const FILTERED_NON_UUID_PRINCIPAL_COST_SOURCE_SQL: &str = "SELECT principal_id, \
        ((list_ts_ms - ?1) / ?2) AS bucket_index, \
        list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
        list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
        list_cost_cache_read_micros \
    FROM request_events_v1 \
    WHERE list_ts_ms >= ?1 \
      AND list_ts_ms < ?3 \
      AND upstream_id = ?4 \
      AND (principal_id IS NULL \
        OR length(principal_id) <> 36 \
        OR length(replace(principal_id, '-', '')) <> 32 \
        OR substr(principal_id, 9, 1) <> '-' \
        OR substr(principal_id, 14, 1) <> '-' \
        OR substr(principal_id, 19, 1) <> '-' \
        OR substr(principal_id, 24, 1) <> '-' \
        OR lower(replace(principal_id, '-', '')) GLOB '*[^0-9a-f]*')";

fn is_canonical_uuid_principal(value: &str) -> bool {
    value.len() == 36 && Uuid::parse_str(value).is_ok()
}

async fn fetch_principal_cost_rows(
    storage: &SqliteStorage,
    source_sql: &'static str,
    range_start_ms: i64,
    bucket_width_ms: i64,
    range_end_ms: i64,
) -> StorageResult<Vec<PrincipalCostRow>> {
    sqlx::query_as::<_, PrincipalCostRow>(AssertSqlSafe(format!(
        "{PRINCIPAL_COST_AGGREGATE_PREFIX}{source_sql}{PRINCIPAL_COST_AGGREGATE_SUFFIX}"
    )))
    .bind(range_start_ms)
    .bind(bucket_width_ms)
    .bind(range_end_ms)
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)
}

async fn fetch_filtered_principal_cost_rows(
    storage: &SqliteStorage,
    source_sql: &'static str,
    range_start_ms: i64,
    bucket_width_ms: i64,
    range_end_ms: i64,
    upstream_id: String,
) -> StorageResult<Vec<PrincipalCostRow>> {
    sqlx::query_as::<_, PrincipalCostRow>(AssertSqlSafe(format!(
        "{PRINCIPAL_COST_AGGREGATE_PREFIX}{source_sql}{PRINCIPAL_COST_AGGREGATE_SUFFIX}"
    )))
    .bind(range_start_ms)
    .bind(bucket_width_ms)
    .bind(range_end_ms)
    .bind(upstream_id)
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)
}

async fn fetch_uuid_principal_cost_rows(
    storage: &SqliteStorage,
    source_sql: &'static str,
    range_start_ms: i64,
    bucket_width_ms: i64,
    range_end_ms: i64,
    principal_keys_json: &str,
) -> StorageResult<Vec<PrincipalCostRow>> {
    sqlx::query_as::<_, PrincipalCostRow>(AssertSqlSafe(format!(
        "{PRINCIPAL_COST_AGGREGATE_PREFIX}{source_sql}{PRINCIPAL_COST_AGGREGATE_SUFFIX}"
    )))
    .bind(range_start_ms)
    .bind(bucket_width_ms)
    .bind(range_end_ms)
    .bind(principal_keys_json)
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)
}

async fn fetch_filtered_uuid_principal_cost_rows(
    storage: &SqliteStorage,
    source_sql: &'static str,
    range_start_ms: i64,
    bucket_width_ms: i64,
    range_end_ms: i64,
    upstream_id: String,
    principal_keys_json: &str,
) -> StorageResult<Vec<PrincipalCostRow>> {
    sqlx::query_as::<_, PrincipalCostRow>(AssertSqlSafe(format!(
        "{PRINCIPAL_COST_AGGREGATE_PREFIX}{source_sql}{PRINCIPAL_COST_AGGREGATE_SUFFIX}"
    )))
    .bind(range_start_ms)
    .bind(bucket_width_ms)
    .bind(range_end_ms)
    .bind(upstream_id)
    .bind(principal_keys_json)
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)
}

fn merge_principal_cost_rows(
    buckets: &mut BTreeMap<(String, u64), RequestEventPrincipalCostBucket>,
    rows: Vec<PrincipalCostRow>,
    selected_principals: &[String],
    query: &RequestEventPrincipalCostQuery,
) -> StorageResult<()> {
    for (
        principal_id,
        bucket_index,
        total_cost_micros,
        component_costs_recorded,
        cost_input_micros,
        cost_output_micros,
        cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros,
        cost_cache_read_micros,
    ) in rows
    {
        let principal = normalize_usage_rollup_dimension(principal_id.as_deref());
        if !selected_principals
            .iter()
            .any(|selected| selected == &principal)
        {
            continue;
        }
        let bucket_index = i64_to_u64(bucket_index, "request event principal cost bucket index")?;
        let bucket_offset = bucket_index
            .checked_mul(query.bucket_width_secs)
            .ok_or_else(|| StorageError::Corrupted {
                message: "request event principal cost bucket offset overflowed".to_owned(),
            })?;
        let bucket_start_unix_secs = query
            .since_unix_secs
            .checked_add(bucket_offset)
            .filter(|bucket_start| *bucket_start < query.until_unix_secs)
            .ok_or_else(|| StorageError::Corrupted {
                message: "request event principal cost bucket is out of range".to_owned(),
            })?;
        let bucket = buckets
            .entry((principal.clone(), bucket_start_unix_secs))
            .or_insert_with(|| RequestEventPrincipalCostBucket {
                principal,
                bucket_start_unix_secs,
                ..RequestEventPrincipalCostBucket::default()
            });

        add_principal_cost(
            &mut bucket.total_cost_micros,
            total_cost_micros,
            "request event principal total cost",
        )?;
        bucket.component_costs_recorded |= component_costs_recorded != 0;
        add_principal_cost(
            &mut bucket.cost_input_micros,
            cost_input_micros,
            "request event principal input cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_output_micros,
            cost_output_micros,
            "request event principal output cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_cache_creation_5m_micros,
            cost_cache_creation_5m_micros,
            "request event principal cache creation 5m cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_cache_creation_1h_micros,
            cost_cache_creation_1h_micros,
            "request event principal cache creation 1h cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_cache_read_micros,
            cost_cache_read_micros,
            "request event principal cache read cost",
        )?;
    }
    Ok(())
}

async fn request_event_principal_costs(
    storage: &SqliteStorage,
    query: &RequestEventPrincipalCostQuery,
) -> StorageResult<Vec<RequestEventPrincipalCostBucket>> {
    if query.bucket_width_secs == 0
        || query.until_unix_secs <= query.since_unix_secs
        || query.principal_keys.is_empty()
    {
        return Ok(Vec::new());
    }

    let range_start_ms = seconds_to_millis(
        query.since_unix_secs,
        "request event principal cost range start",
    )?;
    let range_end_ms = seconds_to_millis(
        query.until_unix_secs,
        "request event principal cost range end",
    )?;
    let bucket_width_ms = seconds_to_millis(
        query.bucket_width_secs,
        "request event principal cost bucket width",
    )?;
    let uuid_keys = query
        .principal_keys
        .iter()
        .filter(|key| is_canonical_uuid_principal(key))
        .cloned()
        .collect::<Vec<_>>();
    let mut buckets = BTreeMap::<(String, u64), RequestEventPrincipalCostBucket>::new();

    if !uuid_keys.is_empty() {
        let principal_keys_json = serde_json::to_string(&uuid_keys)?;
        let rows = match query.upstream_id {
            Some(upstream_id) => {
                fetch_filtered_uuid_principal_cost_rows(
                    storage,
                    FILTERED_UUID_PRINCIPAL_COST_SOURCE_SQL,
                    range_start_ms,
                    bucket_width_ms,
                    range_end_ms,
                    upstream_id.to_string(),
                    &principal_keys_json,
                )
                .await?
            }
            None => {
                fetch_uuid_principal_cost_rows(
                    storage,
                    UUID_PRINCIPAL_COST_SOURCE_SQL,
                    range_start_ms,
                    bucket_width_ms,
                    range_end_ms,
                    &principal_keys_json,
                )
                .await?
            }
        };
        merge_principal_cost_rows(&mut buckets, rows, &uuid_keys, query)?;
    }
    let rows = match query.upstream_id {
        Some(upstream_id) => {
            fetch_filtered_principal_cost_rows(
                storage,
                FILTERED_NON_UUID_PRINCIPAL_COST_SOURCE_SQL,
                range_start_ms,
                bucket_width_ms,
                range_end_ms,
                upstream_id.to_string(),
            )
            .await?
        }
        None => {
            fetch_principal_cost_rows(
                storage,
                NON_UUID_PRINCIPAL_COST_SOURCE_SQL,
                range_start_ms,
                bucket_width_ms,
                range_end_ms,
            )
            .await?
        }
    };
    merge_principal_cost_rows(&mut buckets, rows, &query.principal_keys, query)?;

    Ok(buckets.into_values().collect())
}

/// The histogram filters its window on `ts` (seconds), exactly like the list
/// query, so the two always count the same rows. That predicate alone cannot
/// use an index: `request_events_v1` has no `ts`-leading index, only
/// `list_ts_ms`-leading ones. The extra `list_ts_ms` bound in the query exists
/// solely to give SQLite a seekable leading-column range; it is deliberately
/// WIDER than the `ts` window so it can never decide membership even if a
/// writer ever sets `ts` and `ts_ms` from separate clock reads.
///
/// Do not tighten this to the exact window.
const HISTOGRAM_INDEX_HINT_SLACK_MS: u64 = 60_000;

async fn request_event_histogram(
    storage: &SqliteStorage,
    query: &RequestEventHistogramQuery,
) -> StorageResult<Vec<RequestEventHistogramBucket>> {
    if query.bucket_count == 0
        || query.bucket_ms == 0
        || query.until_unix_secs < query.since_unix_secs
    {
        return Ok(Vec::new());
    }

    let bucket_count = usize::try_from(query.bucket_count).map_err(|_| StorageError::Fatal {
        message: "request event histogram bucket_count cannot be represented as usize".to_owned(),
    })?;
    let range_start_ms = query.since_unix_secs.saturating_mul(1_000);
    let mut buckets = vec![RequestEventHistogramBucket::default(); bucket_count];
    for (index, bucket) in buckets.iter_mut().enumerate() {
        let bucket_start_ms =
            range_start_ms.saturating_add((index as u64).saturating_mul(query.bucket_ms));
        bucket.bucket_start_unix_secs = bucket_start_ms / 1_000;
    }

    let (status_min, status_max) = query
        .filters
        .status_class
        .map(request_event_list_sql::status_class_range)
        .map_or((None, None), |(min, max)| (Some(min), Some(max)));
    let (source_kind_all, source_kind_exact) =
        request_event_list_sql::source_kind_filter(query.source_kind.as_deref());

    let rows = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT MIN(MAX((list_ts_ms - ?1) / ?2, 0), ?3) AS bucket_index, \
                COUNT(*) AS total_count, \
                COUNT(CASE WHEN list_status >= 500 \
                    OR (list_status BETWEEN 200 AND 299 \
                        AND error_code = 'upstream_stream_error') \
                    THEN 1 END) AS error_count \
         FROM request_events_v1 \
         WHERE ts >= ?4 AND ts <= ?5 \
           AND list_ts_ms >= ?15 AND list_ts_ms < ?16 \
           AND (?6 IS NULL OR principal_id = ?6) \
           AND (?7 IS NULL OR lower(model) LIKE ?7 ESCAPE '\\') \
           AND (?8 IS NULL OR upstream_id = ?8) \
           AND (?14 IS NULL OR thread_id = ?14) \
           AND (?9 IS NULL OR list_upstream = ?9) \
           AND (?10 IS NULL OR list_status BETWEEN ?10 AND ?11) \
           AND ( \
                 ?12 = 1 \
              OR (?13 IS NOT NULL AND source_kind = ?13) \
              OR (?13 IS NULL AND (source_kind IS NULL OR source_kind <> 'renewal')) \
           ) \
         GROUP BY bucket_index",
    )
    .bind(u64_to_i64(
        range_start_ms,
        "request event histogram range start",
    )?)
    .bind(u64_to_i64(
        query.bucket_ms,
        "request event histogram bucket width",
    )?)
    .bind(u64_to_i64(
        query.bucket_count.saturating_sub(1),
        "request event histogram last bucket",
    )?)
    .bind(u64_to_i64(
        query.since_unix_secs,
        "request event histogram since",
    )?)
    .bind(u64_to_i64_upper(query.until_unix_secs))
    .bind(query.filters.principal_id.as_deref())
    .bind(
        query
            .filters
            .model
            .as_deref()
            .map(model_filter_like_pattern),
    )
    .bind(query.filters.upstream_id.map(|id| id.to_string()))
    .bind(
        query
            .filters
            .upstream
            .map(request_event_list_sql::upstream_as_str),
    )
    .bind(status_min)
    .bind(status_max)
    .bind(i64::from(source_kind_all))
    .bind(source_kind_exact)
    .bind(query.filters.thread_id.as_deref())
    .bind(u64_to_i64(
        range_start_ms.saturating_sub(HISTOGRAM_INDEX_HINT_SLACK_MS),
        "request event histogram index hint lower bound",
    )?)
    .bind(u64_to_i64_upper(
        query
            .until_unix_secs
            .saturating_add(1)
            .saturating_mul(1_000)
            .saturating_add(HISTOGRAM_INDEX_HINT_SLACK_MS),
    ))
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    for (bucket_index, total_count, error_count) in rows {
        let bucket_index = usize::try_from(bucket_index).map_err(|_| StorageError::Corrupted {
            message: "request event histogram bucket index is negative".to_owned(),
        })?;
        let Some(bucket) = buckets.get_mut(bucket_index) else {
            return Err(StorageError::Corrupted {
                message: "request event histogram bucket index is out of range".to_owned(),
            });
        };
        bucket.total_count = i64_to_u64(total_count, "request event histogram total count")?;
        bucket.error_count = i64_to_u64(error_count, "request event histogram error count")?;
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
    sqlx::query_scalar::<_, i64>(concat!(
        "INSERT INTO request_events_v1 (",
        "request_id, ts, event_type, source_kind, source_ref_id, upstream_id, ",
        "principal_id, created_at, key_id, model, upstream_name, cache_state, ",
        "thread_id, message_id, message_index, message_count, ",
        "cache_control_block_count, cache_breakpoints, cache_prefix_hash, ",
        "input_tokens, output_tokens, cache_creation_input_tokens, ",
        "cache_read_input_tokens, event_id, error_code, upstream_error_type, ",
        "upstream_error_message, thinking_tokens, web_search_requests, ",
        "web_fetch_requests, service_tier, inference_geo, ",
        "cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, ",
        "matched_v3_cache_key, breakpoint_content_block_index, ",
        "matched_content_block_index, lookback_distance, ",
        "predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, ",
        "predicted_cache_creation_tokens_1h, token_estimate_source, ",
        "cache_value_micros, formula_winner_upstream_id, kept_upstream_id, ",
        "quota_urgency_5h, quota_urgency_7d, quota_urgency_combined, ",
        "quota_warning_multiplier, lineage_would_have_predicted_read_tokens, ",
        "lineage_would_have_picked_upstream_id, thinking_budget_tokens, ",
        "reasoning_effort, payload, list_ts_ms, list_event_key, list_upstream, ",
        "list_status, list_duration_ms, list_auth_ms, list_route_ms, ",
        "list_limit_reserve_ms, list_json_parse_ms, list_cache_structure_ms, ",
        "list_cache_token_key_ms, list_cache_count_lookup_ms, ",
        "list_cache_tokenizer_queue_ms, list_cache_serialize_ms, ",
        "list_cache_tokenize_ms, list_prepare_signer_ms, list_bulkhead_wait_ms, ",
        "list_dns_ms, list_connect_ms, list_connection_reused, ",
        "list_limit_reconcile_ms, list_observability_post_ms, ",
        "list_proxy_setup_ms, list_shape_ms, list_sign_ms, list_upstream_ttfb_ms, ",
        "list_upstream_body_ms, list_stream_first_content_delta_ms, ",
        "list_stream_last_content_delta_ms, list_inter_token_avg_ms, ",
        "list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, ",
        "list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, ",
        "list_cost_cache_read_micros, observed_session_id, request_kind, ",
        "claude_agent_id, claude_parent_agent_id, parent_session_id, client_app, ",
        "session_id_source, list_request_body_read_ms, list_request_body_bytes, ",
        "list_finalize_ms, list_request_body_first_chunk_ms, ",
        "list_request_body_receive_ms, list_request_body_wait_ms, ",
        "list_request_body_process_ms, list_request_body_chunk_count, ",
        "list_response_body_wait_ms, list_response_body_process_ms, ",
        "list_response_body_downstream_poll_gap_ms, list_retry_overhead_ms",
        ") VALUES (",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?",
        ") ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING ",
        "RETURNING id",
    ))
    .bind(&event.request_id)
    .bind(u64_to_i64(event_ts_secs(event), "request event ts")?)
    .bind("request")
    .bind(event.source_kind.as_deref())
    .bind(event.source_ref_id.as_deref())
    .bind(event.upstream_id.map(|id| id.to_string()))
    .bind(event.principal_id.as_deref())
    .bind(u64_to_i64(
        event_ts_secs(event),
        "request event created_at",
    )?)
    .bind(event.key_id.as_deref())
    .bind(event.model.as_deref())
    .bind(event.upstream_name.as_deref())
    .bind(event.cache_state.map(|state| state.as_str()))
    .bind(event.thread_id.as_deref())
    .bind(event.message_id.as_deref())
    .bind(option_u64_to_i64(
        event.message_index,
        "request event message_index",
    )?)
    .bind(option_u64_to_i64(
        event.message_count,
        "request event message_count",
    )?)
    .bind(option_u64_to_i64(
        event.cache_control_block_count,
        "request event cache_control_block_count",
    )?)
    .bind(cache_breakpoints)
    .bind(event.cache_prefix_hash.as_deref())
    .bind(option_u64_to_i64(
        event.input_tokens,
        "request event input_tokens",
    )?)
    .bind(option_u64_to_i64(
        event.output_tokens,
        "request event output_tokens",
    )?)
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
    .bind(option_u64_to_i64(
        event.thinking_tokens,
        "request event thinking_tokens",
    )?)
    .bind(option_u64_to_i64(
        event.web_search_requests,
        "request event web_search_requests",
    )?)
    .bind(option_u64_to_i64(
        event.web_fetch_requests,
        "request event web_fetch_requests",
    )?)
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
    .bind(
        event
            .lineage_would_have_picked_upstream_id
            .map(|id| id.to_string()),
    )
    .bind(option_u64_to_i64(
        event.thinking_budget_tokens,
        "thinking_budget_tokens",
    )?)
    .bind(event.reasoning_effort.as_deref())
    .bind(payload)
    .bind(u64_to_i64(
        event
            .ts_ms
            .unwrap_or_else(|| event_ts_secs(event).saturating_mul(1_000)),
        "request event list_ts_ms",
    )?)
    .bind(event.event_id.as_deref().unwrap_or(&event.request_id))
    .bind(event.upstream.map(|upstream| match upstream {
        cc_lb_storage_api::RequestEventUpstream::AnthropicDirect => "anthropic_direct",
    }))
    .bind(i64::from(event.status))
    .bind(u64_to_i64(
        event.duration_ms,
        "request event list_duration_ms",
    )?)
    .bind(option_u64_to_i64(
        event.auth_ms,
        "request event list_auth_ms",
    )?)
    .bind(option_u64_to_i64(
        event.route_ms,
        "request event list_route_ms",
    )?)
    .bind(option_u64_to_i64(
        event.limit_reserve_ms,
        "request event list_limit_reserve_ms",
    )?)
    .bind(event.json_parse_ms)
    .bind(event.cache_structure_ms)
    .bind(event.cache_token_key_ms)
    .bind(event.cache_count_lookup_ms)
    .bind(event.cache_tokenizer_queue_ms)
    .bind(event.cache_serialize_ms)
    .bind(event.cache_tokenize_ms)
    .bind(event.prepare_signer_ms)
    .bind(option_u64_to_i64(
        event.bulkhead_wait_ms,
        "request event list_bulkhead_wait_ms",
    )?)
    .bind(option_u64_to_i64(
        event.dns_ms,
        "request event list_dns_ms",
    )?)
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
    .bind(option_u64_to_i64(
        event.shape_ms,
        "request event list_shape_ms",
    )?)
    .bind(option_u64_to_i64(
        event.sign_ms,
        "request event list_sign_ms",
    )?)
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
    .bind(option_u64_to_i64(
        event.request_body_read_ms,
        "request event list_request_body_read_ms",
    )?)
    .bind(option_u64_to_i64(
        event.request_body_bytes,
        "request event list_request_body_bytes",
    )?)
    .bind(option_u64_to_i64(
        event.finalize_ms,
        "request event list_finalize_ms",
    )?)
    .bind(event.request_body_first_chunk_ms)
    .bind(event.request_body_receive_ms)
    .bind(event.request_body_wait_ms)
    .bind(event.request_body_process_ms)
    .bind(option_u64_to_i64(
        event.request_body_chunk_count,
        "request event list_request_body_chunk_count",
    )?)
    .bind(event.response_body_wait_ms)
    .bind(event.response_body_process_ms)
    .bind(event.response_body_downstream_poll_gap_ms)
    .bind(event.retry_overhead_ms)
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
        && !model_filter_matches(model, event.model.as_deref())
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

fn seconds_to_millis(value: u64, field: &str) -> StorageResult<i64> {
    let value = value
        .checked_mul(1_000)
        .ok_or_else(|| StorageError::Fatal {
            message: format!("{field} cannot be represented as milliseconds"),
        })?;
    u64_to_i64(value, field)
}

fn add_principal_cost(target: &mut u64, value: i64, field: &str) -> StorageResult<()> {
    let value = i64_to_u64(value, field)?;
    *target = target
        .checked_add(value)
        .ok_or_else(|| StorageError::Corrupted {
            message: format!("{field} aggregate overflowed"),
        })?;
    Ok(())
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
