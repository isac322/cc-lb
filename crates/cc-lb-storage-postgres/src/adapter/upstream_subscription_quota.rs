use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaLatestRecord,
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSeries,
    SubscriptionQuotaSeriesQuery, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
};
use sqlx::{PgConnection, Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl UpstreamSubscriptionQuotaStore for PostgresStorage {
    async fn put_subscription_quota_batch(
        &self,
        records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        for record in records {
            insert_observation(&mut tx, record).await?;
            upsert_latest(&mut tx, record).await?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT * FROM upstream_subscription_quota_latest_v1 \
             WHERE upstream_id = ANY($1::uuid[]) \
             ORDER BY upstream_id ASC, \"window\" ASC, source ASC",
        )
        .bind(upstream_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_record).collect()
    }

    async fn list_subscription_quota_series(
        &self,
        query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
        if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
            return Ok(Vec::new());
        }
        let windows = query
            .windows
            .iter()
            .map(|window| window.as_str())
            .collect::<Vec<_>>();
        let sources = filtered_sources(&query);
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        let source_names = sources
            .iter()
            .map(|source| source.as_str())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT * FROM upstream_subscription_quota_observations_v1 \
             WHERE upstream_id = ANY($1::uuid[]) \
             AND \"window\" = ANY($2::text[]) \
             AND source = ANY($3::text[]) \
             AND observed_at_unix_millis >= $4 \
             AND observed_at_unix_millis <= $5 \
             ORDER BY upstream_id ASC, \"window\" ASC, source ASC, observed_at_unix_millis ASC, sample_id ASC",
        )
        .bind(&query.upstream_ids)
        .bind(&windows)
        .bind(&source_names)
        .bind(u64_to_i64(query.since_unix_millis, "subscription quota since_unix_millis")?)
        .bind(u64_to_i64(query.until_unix_millis, "subscription quota until_unix_millis")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let records = rows
            .into_iter()
            .map(row_to_record)
            .collect::<StorageResult<Vec<_>>>()?;
        build_series(records, &query)
    }

    async fn delete_subscription_quota_before(
        &self,
        cutoff_unix_millis: u64,
        batch_size: u32,
    ) -> StorageResult<u64> {
        let result = sqlx::query(
            "WITH victims AS ( \
                 SELECT ctid FROM upstream_subscription_quota_observations_v1 \
                 WHERE observed_at_unix_millis < $1 \
                 LIMIT $2 \
             ) \
             DELETE FROM upstream_subscription_quota_observations_v1 \
             WHERE ctid IN (SELECT ctid FROM victims)",
        )
        .bind(u64_to_i64(
            cutoff_unix_millis,
            "subscription quota cutoff_unix_millis",
        )?)
        .bind(i64::from(batch_size))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

async fn insert_observation(
    conn: &mut PgConnection,
    record: &SubscriptionQuotaObservationRecord,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_observations_v1 \
         (upstream_id, \"window\", source, sample_kind, observed_at_unix_millis, sample_id, \
          utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim, disabled_reason, \
          extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) \
         ON CONFLICT DO NOTHING",
    )
    .bind(record.upstream_id)
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .bind(record.sample_kind.as_str())
    .bind(u64_to_i64(record.observed_at_unix_millis, "subscription quota observed_at_unix_millis")?)
    .bind(record.sample_id)
    .bind(record.utilization)
    .bind(record.status.map(SubscriptionQuotaStatus::as_str))
    .bind(record.resets_at_unix_secs.map(|value| u64_to_i64(value, "subscription quota resets_at_unix_secs")).transpose()?)
    .bind(record.surpassed_threshold)
    .bind(&record.representative_claim)
    .bind(&record.disabled_reason)
    .bind(record.extra_usage_enabled)
    .bind(record.extra_usage_monthly_limit)
    .bind(record.extra_usage_used_credits)
    .bind(u64_to_i64(record.ingested_at_unix_millis, "subscription quota ingested_at_unix_millis")?)
    .execute(conn)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn upsert_latest(
    conn: &mut PgConnection,
    record: &SubscriptionQuotaObservationRecord,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_latest_v1 \
         (upstream_id, \"window\", source, sample_kind, observed_at_unix_millis, sample_id, \
          utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim, disabled_reason, \
          extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) \
         ON CONFLICT (upstream_id, \"window\", source) DO UPDATE SET \
         sample_kind = EXCLUDED.sample_kind, observed_at_unix_millis = EXCLUDED.observed_at_unix_millis, \
         sample_id = EXCLUDED.sample_id, utilization = EXCLUDED.utilization, status = EXCLUDED.status, \
         resets_at_unix_secs = EXCLUDED.resets_at_unix_secs, surpassed_threshold = EXCLUDED.surpassed_threshold, \
         representative_claim = EXCLUDED.representative_claim, disabled_reason = EXCLUDED.disabled_reason, \
         extra_usage_enabled = EXCLUDED.extra_usage_enabled, extra_usage_monthly_limit = EXCLUDED.extra_usage_monthly_limit, \
         extra_usage_used_credits = EXCLUDED.extra_usage_used_credits, ingested_at_unix_millis = EXCLUDED.ingested_at_unix_millis \
         WHERE EXCLUDED.observed_at_unix_millis >= upstream_subscription_quota_latest_v1.observed_at_unix_millis",
    )
    .bind(record.upstream_id)
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .bind(record.sample_kind.as_str())
    .bind(u64_to_i64(record.observed_at_unix_millis, "subscription quota observed_at_unix_millis")?)
    .bind(record.sample_id)
    .bind(record.utilization)
    .bind(record.status.map(SubscriptionQuotaStatus::as_str))
    .bind(record.resets_at_unix_secs.map(|value| u64_to_i64(value, "subscription quota resets_at_unix_secs")).transpose()?)
    .bind(record.surpassed_threshold)
    .bind(&record.representative_claim)
    .bind(&record.disabled_reason)
    .bind(record.extra_usage_enabled)
    .bind(record.extra_usage_monthly_limit)
    .bind(record.extra_usage_used_credits)
    .bind(u64_to_i64(record.ingested_at_unix_millis, "subscription quota ingested_at_unix_millis")?)
    .execute(conn)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn row_to_record(row: PgRow) -> StorageResult<SubscriptionQuotaObservationRecord> {
    let window = parse_window(&row.try_get::<String, _>("window").map_err(map_sqlx_error)?)?;
    let source = parse_source(&row.try_get::<String, _>("source").map_err(map_sqlx_error)?)?;
    let sample_kind = parse_sample_kind(
        &row.try_get::<String, _>("sample_kind")
            .map_err(map_sqlx_error)?,
    )?;
    let status = row
        .try_get::<Option<String>, _>("status")
        .map_err(map_sqlx_error)?
        .as_deref()
        .map(parse_status)
        .transpose()?;
    Ok(SubscriptionQuotaObservationRecord {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        window,
        source,
        sample_kind,
        observed_at_unix_millis: i64_to_u64(
            row.try_get("observed_at_unix_millis")
                .map_err(map_sqlx_error)?,
            "subscription quota observed_at_unix_millis",
        )?,
        sample_id: row.try_get("sample_id").map_err(map_sqlx_error)?,
        utilization: row.try_get("utilization").map_err(map_sqlx_error)?,
        status,
        resets_at_unix_secs: row
            .try_get::<Option<i64>, _>("resets_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "subscription quota resets_at_unix_secs"))
            .transpose()?,
        surpassed_threshold: row.try_get("surpassed_threshold").map_err(map_sqlx_error)?,
        representative_claim: row
            .try_get("representative_claim")
            .map_err(map_sqlx_error)?,
        disabled_reason: row.try_get("disabled_reason").map_err(map_sqlx_error)?,
        extra_usage_enabled: row.try_get("extra_usage_enabled").map_err(map_sqlx_error)?,
        extra_usage_monthly_limit: row
            .try_get("extra_usage_monthly_limit")
            .map_err(map_sqlx_error)?,
        extra_usage_used_credits: row
            .try_get("extra_usage_used_credits")
            .map_err(map_sqlx_error)?,
        ingested_at_unix_millis: i64_to_u64(
            row.try_get("ingested_at_unix_millis")
                .map_err(map_sqlx_error)?,
            "subscription quota ingested_at_unix_millis",
        )?,
    })
}

pub(crate) fn build_series(
    records: Vec<SubscriptionQuotaObservationRecord>,
    query: &SubscriptionQuotaSeriesQuery,
) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
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
    Ok(series)
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
    let mut count = 0u32;
    let mut sum = 0.0;
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    let mut last = &records[0];
    let mut sources_seen = BTreeSet::new();
    for record in records {
        sources_seen.insert(record.source);
        if record.observed_at_unix_millis >= last.observed_at_unix_millis {
            last = record;
        }
        if let Some(utilization) = record.utilization {
            count += 1;
            sum += utilization;
            min = Some(min.map_or(utilization, |value| value.min(utilization)));
            max = Some(max.map_or(utilization, |value| value.max(utilization)));
        }
    }
    SubscriptionQuotaBucket {
        bucket_start_unix_secs,
        observed: true,
        sample_count: u32::try_from(records.len()).unwrap_or(u32::MAX),
        utilization_min: min,
        utilization_avg: (count > 0).then_some(sum / f64::from(count)),
        utilization_max: max,
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
        let index = point * last_index / (max_points - 1);
        sampled.push(original[index].clone());
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

fn parse_window(value: &str) -> StorageResult<SubscriptionQuotaWindow> {
    SubscriptionQuotaWindow::from_str(value).ok_or_else(|| StorageError::Corrupted {
        message: format!("invalid subscription quota window {value}"),
    })
}

fn parse_source(value: &str) -> StorageResult<SubscriptionQuotaSource> {
    SubscriptionQuotaSource::from_str(value).ok_or_else(|| StorageError::Corrupted {
        message: format!("invalid subscription quota source {value}"),
    })
}

fn parse_status(value: &str) -> StorageResult<SubscriptionQuotaStatus> {
    SubscriptionQuotaStatus::from_str(value).ok_or_else(|| StorageError::Corrupted {
        message: format!("invalid subscription quota status {value}"),
    })
}

fn parse_sample_kind(value: &str) -> StorageResult<SubscriptionQuotaSampleKind> {
    SubscriptionQuotaSampleKind::from_str(value).ok_or_else(|| StorageError::Corrupted {
        message: format!("invalid subscription quota sample kind {value}"),
    })
}
