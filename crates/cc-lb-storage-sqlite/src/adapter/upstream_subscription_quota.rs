use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaLatestRecord,
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaStore,
};
use sqlx::{AssertSqlSafe, Row};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl UpstreamSubscriptionQuotaStore for SqliteStorage {
    async fn put_subscription_quota_batch(
        &self,
        records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        for record in records {
            let payload = serde_json::to_string(record)?;
            sqlx::query(
                "INSERT INTO upstream_subscription_quotas_v1 \
                 (upstream_id, sample_id, sample_kind, observed_at, input_tokens, output_tokens, request_count, cost_usd_micros) \
                 VALUES (?, ?, ?, ?, 0, 0, 0, 0) \
                 ON CONFLICT(upstream_id, sample_id) DO UPDATE SET \
                 sample_kind = excluded.sample_kind, \
                 observed_at = excluded.observed_at, \
                 input_tokens = excluded.input_tokens, \
                 output_tokens = excluded.output_tokens, \
                 request_count = excluded.request_count, \
                 cost_usd_micros = excluded.cost_usd_micros",
            )
            .bind(record.upstream_id.to_string())
            .bind(record.sample_id.to_string())
            .bind(payload)
            .bind(u64_to_i64(
                record.observed_at_unix_millis,
                "subscription quota observed_at_unix_millis",
            )?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            let observed_at = u64_to_i64(
                record.observed_at_unix_millis,
                "subscription quota observed_at_unix_millis",
            )?;
            let payload = serde_json::to_string(record)?;
            sqlx::query(
                "INSERT INTO upstream_subscription_quota_latest_v1 \
                 (upstream_id, window, source, payload, observed_at) \
                 VALUES (?, ?, ?, ?, ?) \
                 ON CONFLICT(upstream_id, window, source) DO UPDATE SET \
                 payload = excluded.payload, \
                 observed_at = excluded.observed_at \
                 WHERE excluded.observed_at >= upstream_subscription_quota_latest_v1.observed_at",
            )
            .bind(record.upstream_id.to_string())
            .bind(record.window.as_str())
            .bind(record.source.as_str())
            .bind(payload)
            .bind(observed_at)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn put_subscription_quota(
        &self,
        record: &SubscriptionQuotaObservationRecord,
    ) -> StorageResult<()> {
        self.put_subscription_quota_batch(std::slice::from_ref(record))
            .await
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>> {
        list_latest_records_for_upstreams(self, upstream_ids).await
    }

    async fn list_subscription_quota_series(
        &self,
        query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
        if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
            return Ok(Vec::new());
        }
        let requested_windows = query.windows.iter().copied().collect::<BTreeSet<_>>();
        let requested_sources = filtered_sources(&query)
            .into_iter()
            .collect::<BTreeSet<_>>();
        if requested_sources.is_empty() {
            return Ok(Vec::new());
        }
        let records = list_records_for_upstreams(self, &query.upstream_ids)
            .await?
            .into_iter()
            .filter(|record| {
                requested_windows.contains(&record.window)
                    && requested_sources.contains(&record.source)
                    && record.observed_at_unix_millis >= query.since_unix_millis
                    && record.observed_at_unix_millis <= query.until_unix_millis
            })
            .collect::<Vec<_>>();
        Ok(build_series(records, &query))
    }

    async fn delete_subscription_quota_before(
        &self,
        cutoff_unix_millis: u64,
        batch_size: u32,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let result = sqlx::query(
            "DELETE FROM upstream_subscription_quotas_v1 \
             WHERE rowid IN ( \
                 SELECT rowid FROM upstream_subscription_quotas_v1 \
                 WHERE observed_at < ? \
                 ORDER BY observed_at ASC, upstream_id ASC, sample_id ASC \
                 LIMIT ? \
             )",
        )
        .bind(u64_to_i64(
            cutoff_unix_millis,
            "subscription quota cutoff_unix_millis",
        )?)
        .bind(i64::from(batch_size))
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

async fn list_latest_records_for_upstreams(
    storage: &SqliteStorage,
    upstream_ids: &[Uuid],
) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>> {
    if upstream_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", upstream_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT payload FROM upstream_subscription_quota_latest_v1 \
         WHERE upstream_id IN ({placeholders}) \
         ORDER BY upstream_id ASC, window ASC, source ASC"
    );
    let mut query = sqlx::query(AssertSqlSafe(sql));
    for upstream_id in upstream_ids {
        query = query.bind(upstream_id.to_string());
    }
    let rows = query
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|row| {
            let payload: String = row.try_get("payload").map_err(map_sqlx_error)?;
            serde_json::from_str::<SubscriptionQuotaLatestRecord>(&payload)
                .map_err(StorageError::from)
        })
        .collect()
}

async fn list_records_for_upstreams(
    storage: &SqliteStorage,
    upstream_ids: &[Uuid],
) -> StorageResult<Vec<SubscriptionQuotaObservationRecord>> {
    if upstream_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", upstream_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT sample_kind FROM upstream_subscription_quotas_v1 \
         WHERE upstream_id IN ({placeholders}) \
         ORDER BY upstream_id ASC, observed_at ASC, sample_id ASC"
    );
    let mut query = sqlx::query(AssertSqlSafe(sql));
    for upstream_id in upstream_ids {
        query = query.bind(upstream_id.to_string());
    }
    let rows = query
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|row| {
            let payload: String = row.try_get("sample_kind").map_err(map_sqlx_error)?;
            serde_json::from_str::<SubscriptionQuotaObservationRecord>(&payload)
                .map_err(StorageError::from)
        })
        .collect()
}

fn build_series(
    records: Vec<SubscriptionQuotaObservationRecord>,
    query: &SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaSeries> {
    let bucket_secs = query.bucket_secs.max(1);
    let mut groups: BTreeMap<
        (Uuid, SubscriptionQuotaWindow, u8),
        Vec<SubscriptionQuotaObservationRecord>,
    > = BTreeMap::new();
    for record in records {
        groups
            .entry((
                record.upstream_id,
                record.window,
                source_merge_code(query.source_merge),
            ))
            .or_default()
            .push(record);
    }

    let mut series = Vec::new();
    for ((upstream_id, window, source_code), group_records) in groups {
        let mut buckets: BTreeMap<u64, Vec<SubscriptionQuotaObservationRecord>> = BTreeMap::new();
        for record in group_records {
            let bucket = (record.observed_at_unix_millis / 1000) / bucket_secs * bucket_secs;
            buckets.entry(bucket).or_default().push(record);
        }
        let mut bucket_records = buckets
            .into_iter()
            .map(|(bucket_start_unix_secs, records)| {
                bucket_from_records(bucket_start_unix_secs, &records)
            })
            .collect::<Vec<_>>();
        downsample(&mut bucket_records, query.max_points_per_series as usize);
        series.push(SubscriptionQuotaSeries {
            upstream_id,
            window,
            source: source_merge_from_code(source_code),
            buckets: bucket_records,
        });
    }
    series
}

fn source_merge_code(source: SubscriptionQuotaSourceMerge) -> u8 {
    match source {
        SubscriptionQuotaSourceMerge::Header => 1,
        SubscriptionQuotaSourceMerge::Api => 2,
        SubscriptionQuotaSourceMerge::Merged => 3,
    }
}

fn source_merge_from_code(code: u8) -> SubscriptionQuotaSourceMerge {
    match code {
        1 => SubscriptionQuotaSourceMerge::Header,
        2 => SubscriptionQuotaSourceMerge::Api,
        _ => SubscriptionQuotaSourceMerge::Merged,
    }
}

fn bucket_from_records(
    bucket_start_unix_secs: u64,
    records: &[SubscriptionQuotaObservationRecord],
) -> SubscriptionQuotaBucket {
    let mut util_count = 0u32;
    let mut util_sum = 0.0;
    let mut util_min: Option<f64> = None;
    let mut util_max: Option<f64> = None;
    let mut last = &records[0];
    let mut sources_seen = BTreeSet::new();

    for record in records {
        sources_seen.insert(record.source);
        if record.observed_at_unix_millis >= last.observed_at_unix_millis {
            last = record;
        }
        if let Some(utilization) = record.utilization {
            util_count += 1;
            util_sum += utilization;
            util_min = Some(util_min.map_or(utilization, |value| value.min(utilization)));
            util_max = Some(util_max.map_or(utilization, |value| value.max(utilization)));
        }
    }

    SubscriptionQuotaBucket {
        bucket_start_unix_secs,
        observed: true,
        sample_count: u32::try_from(records.len()).unwrap_or(u32::MAX),
        utilization_min: util_min,
        utilization_avg: (util_count > 0).then_some(util_sum / f64::from(util_count)),
        utilization_max: util_max,
        utilization_last: last.utilization,
        status_last: last.status,
        resets_at_unix_secs_last: last.resets_at_unix_secs,
        observed_at_unix_millis_last: Some(last.observed_at_unix_millis),
        sources_seen: sources_seen.into_iter().collect(),
    }
}

fn downsample<T: Clone>(items: &mut Vec<T>, max_points: usize) {
    if max_points == 0 {
        items.clear();
        return;
    }
    if items.len() <= max_points {
        return;
    }
    if max_points == 1 {
        items.truncate(1);
        return;
    }
    let original = items.clone();
    let last_index = original.len() - 1;
    let mut sampled = Vec::with_capacity(max_points);
    for point in 0..max_points {
        sampled.push(original[point * last_index / (max_points - 1)].clone());
    }
    *items = sampled;
}

fn filtered_sources(query: &SubscriptionQuotaSeriesQuery) -> Vec<SubscriptionQuotaSource> {
    query
        .sources
        .iter()
        .copied()
        .filter(|source| match query.source_merge {
            SubscriptionQuotaSourceMerge::Merged => true,
            SubscriptionQuotaSourceMerge::Header => *source == SubscriptionQuotaSource::Header,
            SubscriptionQuotaSourceMerge::Api => *source == SubscriptionQuotaSource::Api,
        })
        .collect()
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}
