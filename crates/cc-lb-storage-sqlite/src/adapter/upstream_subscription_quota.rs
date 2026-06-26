use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaLatestRecord,
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSeries,
    SubscriptionQuotaSeriesQuery, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
};
use sqlx::{AssertSqlSafe, Row, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl UpstreamSubscriptionQuotaStore for SqliteStorage {
    async fn put_subscription_quota_batch(
        &self,
        records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        for record in records {
            insert_observation(&mut tx, record).await?;
            upsert_latest(&mut tx, record).await?;
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
        let records =
            list_records_for_query(self, &query, &requested_windows, &requested_sources).await?;
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
            "DELETE FROM upstream_subscription_quota_observations_v1 \
             WHERE rowid IN ( \
                 SELECT rowid FROM upstream_subscription_quota_observations_v1 \
                 WHERE observed_at_unix_millis < ? \
                 ORDER BY observed_at_unix_millis ASC, upstream_id ASC, sample_id ASC \
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
        "SELECT * FROM upstream_subscription_quota_latest_v1 \
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

    rows.into_iter().map(row_to_record).collect()
}

// Filter (window, source, observed_at_unix_millis) at SQL level so the composite
// `upstream_subscription_quota_obs_series_idx` is used. Mirrors the Postgres
// adapter; the prior `SELECT * WHERE upstream_id IN (?)` shape over-fetched by
// ~130× on a 1h view and filtered in Rust.
async fn list_records_for_query(
    storage: &SqliteStorage,
    query: &SubscriptionQuotaSeriesQuery,
    requested_windows: &BTreeSet<SubscriptionQuotaWindow>,
    requested_sources: &BTreeSet<SubscriptionQuotaSource>,
) -> StorageResult<Vec<SubscriptionQuotaObservationRecord>> {
    if query.upstream_ids.is_empty() || requested_windows.is_empty() || requested_sources.is_empty()
    {
        return Ok(Vec::new());
    }
    let upstream_placeholders = std::iter::repeat_n("?", query.upstream_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let window_placeholders = std::iter::repeat_n("?", requested_windows.len())
        .collect::<Vec<_>>()
        .join(", ");
    let source_placeholders = std::iter::repeat_n("?", requested_sources.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT * FROM upstream_subscription_quota_observations_v1 \
         WHERE upstream_id IN ({upstream_placeholders}) \
         AND window IN ({window_placeholders}) \
         AND source IN ({source_placeholders}) \
         AND observed_at_unix_millis >= ? \
         AND observed_at_unix_millis <= ? \
         ORDER BY upstream_id ASC, window ASC, source ASC, observed_at_unix_millis ASC, sample_id ASC"
    );
    let mut q = sqlx::query(AssertSqlSafe(sql));
    for upstream_id in &query.upstream_ids {
        q = q.bind(upstream_id.to_string());
    }
    for window in requested_windows {
        q = q.bind(window.as_str().to_owned());
    }
    for source in requested_sources {
        q = q.bind(source.as_str().to_owned());
    }
    q = q.bind(u64_to_i64(
        query.since_unix_millis,
        "subscription quota since_unix_millis",
    )?);
    q = q.bind(u64_to_i64(
        query.until_unix_millis,
        "subscription quota until_unix_millis",
    )?);
    let rows = q.fetch_all(storage.pool()).await.map_err(map_sqlx_error)?;

    rows.into_iter().map(row_to_record).collect()
}

async fn insert_observation(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &SubscriptionQuotaObservationRecord,
) -> StorageResult<()> {
    let upgrade_paths_json = encode_upgrade_paths(record.upgrade_paths.as_ref())?;
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_observations_v1 \
         (upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id, \
           utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim, \
           fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, upgrade_paths, \
           disabled_reason, \
           extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(record.upstream_id.to_string())
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .bind(record.sample_kind.as_str())
    .bind(u64_to_i64(record.observed_at_unix_millis, "subscription quota observed_at_unix_millis")?)
    .bind(record.sample_id.to_string())
    .bind(record.utilization)
    .bind(record.status.map(SubscriptionQuotaStatus::as_str))
    .bind(record.resets_at_unix_secs.map(|value| u64_to_i64(value, "subscription quota resets_at_unix_secs")).transpose()?)
    .bind(record.surpassed_threshold)
    .bind(&record.representative_claim)
    .bind(record.fallback_percentage)
    .bind(record.fallback_available)
    .bind(record.overage_in_use)
    .bind(record.overage_period_monthly_utilization)
    .bind(upgrade_paths_json)
    .bind(&record.disabled_reason)
    .bind(record.extra_usage_enabled)
    .bind(record.extra_usage_monthly_limit)
    .bind(record.extra_usage_used_credits)
    .bind(u64_to_i64(record.ingested_at_unix_millis, "subscription quota ingested_at_unix_millis")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn upsert_latest(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &SubscriptionQuotaObservationRecord,
) -> StorageResult<()> {
    let upgrade_paths_json = encode_upgrade_paths(record.upgrade_paths.as_ref())?;
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_latest_v1 \
         (upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id, \
           utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim, \
           fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, upgrade_paths, \
           disabled_reason, \
           extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
         ON CONFLICT(upstream_id, window, source) DO UPDATE SET \
         sample_kind = excluded.sample_kind, observed_at_unix_millis = excluded.observed_at_unix_millis, \
         sample_id = excluded.sample_id, utilization = excluded.utilization, status = excluded.status, \
         resets_at_unix_secs = excluded.resets_at_unix_secs, surpassed_threshold = excluded.surpassed_threshold, \
          representative_claim = excluded.representative_claim, fallback_percentage = excluded.fallback_percentage, \
          fallback_available = excluded.fallback_available, overage_in_use = excluded.overage_in_use, \
          overage_period_monthly_utilization = excluded.overage_period_monthly_utilization, \
          upgrade_paths = excluded.upgrade_paths, \
          disabled_reason = excluded.disabled_reason, \
         extra_usage_enabled = excluded.extra_usage_enabled, extra_usage_monthly_limit = excluded.extra_usage_monthly_limit, \
         extra_usage_used_credits = excluded.extra_usage_used_credits, ingested_at_unix_millis = excluded.ingested_at_unix_millis \
         WHERE excluded.observed_at_unix_millis >= upstream_subscription_quota_latest_v1.observed_at_unix_millis",
    )
    .bind(record.upstream_id.to_string())
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .bind(record.sample_kind.as_str())
    .bind(u64_to_i64(record.observed_at_unix_millis, "subscription quota observed_at_unix_millis")?)
    .bind(record.sample_id.to_string())
    .bind(record.utilization)
    .bind(record.status.map(SubscriptionQuotaStatus::as_str))
    .bind(record.resets_at_unix_secs.map(|value| u64_to_i64(value, "subscription quota resets_at_unix_secs")).transpose()?)
    .bind(record.surpassed_threshold)
    .bind(&record.representative_claim)
    .bind(record.fallback_percentage)
    .bind(record.fallback_available)
    .bind(record.overage_in_use)
    .bind(record.overage_period_monthly_utilization)
    .bind(upgrade_paths_json)
    .bind(&record.disabled_reason)
    .bind(record.extra_usage_enabled)
    .bind(record.extra_usage_monthly_limit)
    .bind(record.extra_usage_used_credits)
    .bind(u64_to_i64(record.ingested_at_unix_millis, "subscription quota ingested_at_unix_millis")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn encode_upgrade_paths(value: Option<&Vec<String>>) -> StorageResult<Option<String>> {
    match value {
        Some(paths) => {
            serde_json::to_string(paths)
                .map(Some)
                .map_err(|error| StorageError::Corrupted {
                    message: format!(
                        "failed to serialize subscription quota upgrade_paths: {error}"
                    ),
                })
        }
        None => Ok(None),
    }
}

fn decode_upgrade_paths(raw: Option<String>) -> StorageResult<Option<Vec<String>>> {
    match raw {
        Some(text) => {
            serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| StorageError::Corrupted {
                    message: format!("invalid subscription quota upgrade_paths {text}: {error}"),
                })
        }
        None => Ok(None),
    }
}

fn row_to_record(row: SqliteRow) -> StorageResult<SubscriptionQuotaObservationRecord> {
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
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    let sample_id = row
        .try_get::<String, _>("sample_id")
        .map_err(map_sqlx_error)?;

    Ok(SubscriptionQuotaObservationRecord {
        upstream_id: parse_uuid(&upstream_id, "subscription quota upstream_id")?,
        window,
        source,
        sample_kind,
        observed_at_unix_millis: i64_to_u64(
            row.try_get("observed_at_unix_millis")
                .map_err(map_sqlx_error)?,
            "subscription quota observed_at_unix_millis",
        )?,
        sample_id: parse_uuid(&sample_id, "subscription quota sample_id")?,
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
        fallback_percentage: row.try_get("fallback_percentage").map_err(map_sqlx_error)?,
        fallback_available: row.try_get("fallback_available").map_err(map_sqlx_error)?,
        overage_in_use: row.try_get("overage_in_use").map_err(map_sqlx_error)?,
        overage_period_monthly_utilization: row
            .try_get("overage_period_monthly_utilization")
            .map_err(map_sqlx_error)?,
        upgrade_paths: decode_upgrade_paths(row.try_get("upgrade_paths").map_err(map_sqlx_error)?)?,
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

fn parse_uuid(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field} {value}: {error}"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative in sqlite storage"),
    })
}
