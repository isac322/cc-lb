use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaCheckpointRange,
    SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaCheckpointRecord,
    SubscriptionQuotaLatestRecord, SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSemanticFingerprint, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
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
            upsert_latest(&mut tx, record).await?;
            insert_checkpoint_if_changed(&mut tx, &SubscriptionQuotaCheckpointRecord::from(record))
                .await?;
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
        let sources = filtered_sources(&query);
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        let range_query = SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: query.upstream_ids.clone(),
            windows: query.windows.clone(),
            sources,
            since_unix_millis: query.since_unix_millis,
            until_unix_millis: query.until_unix_millis,
        };
        let ranges = list_checkpoint_ranges_for_query(self, &range_query).await?;
        Ok(build_series_from_checkpoint_ranges(ranges, &query))
    }

    async fn put_subscription_quota_checkpoints(
        &self,
        records: &[SubscriptionQuotaCheckpointRecord],
    ) -> StorageResult<usize> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let mut inserted = 0usize;
        for record in records {
            if insert_checkpoint_if_changed(&mut tx, record).await? {
                inserted += 1;
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(inserted)
    }

    async fn list_latest_subscription_quota_checkpoints_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaCheckpointRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT DISTINCT ON (upstream_id, \"window\", source) * \
             FROM upstream_subscription_quota_checkpoints_v1 \
             WHERE upstream_id = ANY($1::uuid[]) \
             ORDER BY upstream_id ASC, \"window\" ASC, source ASC, \
                      changed_at_unix_millis DESC, sample_id DESC",
        )
        .bind(upstream_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_checkpoint_record).collect()
    }

    async fn list_subscription_quota_checkpoint_ranges(
        &self,
        query: SubscriptionQuotaCheckpointRangeQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaCheckpointRange>> {
        list_checkpoint_ranges_for_query(self, &query).await
    }
}

type CheckpointKey = (Uuid, SubscriptionQuotaWindow, SubscriptionQuotaSource);

async fn insert_checkpoint_if_changed(
    conn: &mut PgConnection,
    record: &SubscriptionQuotaCheckpointRecord,
) -> StorageResult<bool> {
    let latest_fingerprint = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT semantic_fingerprint FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = $1 AND \"window\" = $2 AND source = $3 \
         ORDER BY changed_at_unix_millis DESC, sample_id DESC \
         LIMIT 1",
    )
    .bind(record.upstream_id)
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;

    if latest_fingerprint.as_deref() == Some(&record.semantic_fingerprint.as_bytes()[..]) {
        return Ok(false);
    }

    let upgrade_paths_json = encode_upgrade_paths(record.upgrade_paths.as_ref())?;
    let result = sqlx::query(
        "INSERT INTO upstream_subscription_quota_checkpoints_v1 \
         (upstream_id, \"window\", source, changed_at_unix_millis, sample_id, semantic_fingerprint, \
          sample_kind, representative_claim, utilization, status, resets_at_unix_secs, \
          surpassed_threshold, fallback_percentage, fallback_available, overage_in_use, \
          overage_period_monthly_utilization, upgrade_paths, disabled_reason, extra_usage_enabled, \
          extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22) \
         ON CONFLICT DO NOTHING",
    )
    .bind(record.upstream_id)
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .bind(u64_to_i64(
        record.changed_at_unix_millis,
        "subscription quota checkpoint changed_at_unix_millis",
    )?)
    .bind(record.sample_id)
    .bind(&record.semantic_fingerprint.as_bytes()[..])
    .bind(record.sample_kind.as_str())
    .bind(&record.representative_claim)
    .bind(record.utilization)
    .bind(record.status.map(SubscriptionQuotaStatus::as_str))
    .bind(
        record
            .resets_at_unix_secs
            .map(|value| u64_to_i64(value, "subscription quota resets_at_unix_secs"))
            .transpose()?,
    )
    .bind(record.surpassed_threshold)
    .bind(record.fallback_percentage)
    .bind(record.fallback_available)
    .bind(record.overage_in_use)
    .bind(record.overage_period_monthly_utilization)
    .bind(upgrade_paths_json)
    .bind(&record.disabled_reason)
    .bind(record.extra_usage_enabled)
    .bind(record.extra_usage_monthly_limit)
    .bind(record.extra_usage_used_credits)
    .bind(u64_to_i64(
        record.ingested_at_unix_millis,
        "subscription quota ingested_at_unix_millis",
    )?)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected() == 1)
}

async fn list_checkpoint_ranges_for_query(
    storage: &PostgresStorage,
    query: &SubscriptionQuotaCheckpointRangeQuery,
) -> StorageResult<Vec<SubscriptionQuotaCheckpointRange>> {
    if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
        return Ok(Vec::new());
    }
    let windows = query
        .windows
        .iter()
        .map(|window| window.as_str())
        .collect::<Vec<_>>();
    let sources = query
        .sources
        .iter()
        .map(|source| source.as_str())
        .collect::<Vec<_>>();
    let anchors = list_checkpoint_anchors(storage, query, &windows, &sources).await?;
    let checkpoints = list_checkpoints_inside_range(storage, query, &windows, &sources).await?;
    let mut ranges = BTreeMap::<CheckpointKey, SubscriptionQuotaCheckpointRange>::new();
    for anchor in anchors {
        let key = checkpoint_key(&anchor);
        ranges
            .entry(key)
            .or_insert_with(|| range_for_key(key))
            .left_anchor = Some(anchor);
    }
    for checkpoint in checkpoints {
        let key = checkpoint_key(&checkpoint);
        ranges
            .entry(key)
            .or_insert_with(|| range_for_key(key))
            .checkpoints
            .push(checkpoint);
    }
    Ok(ranges.into_values().collect())
}

async fn list_checkpoint_anchors(
    storage: &PostgresStorage,
    query: &SubscriptionQuotaCheckpointRangeQuery,
    windows: &[&str],
    sources: &[&str],
) -> StorageResult<Vec<SubscriptionQuotaCheckpointRecord>> {
    let rows = sqlx::query(
        "SELECT DISTINCT ON (upstream_id, \"window\", source) * \
         FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ANY($1::uuid[]) \
         AND \"window\" = ANY($2::text[]) \
         AND source = ANY($3::text[]) \
         AND changed_at_unix_millis < $4 \
         ORDER BY upstream_id ASC, \"window\" ASC, source ASC, \
                  changed_at_unix_millis DESC, sample_id DESC",
    )
    .bind(&query.upstream_ids)
    .bind(windows)
    .bind(sources)
    .bind(u64_to_i64(
        query.since_unix_millis,
        "subscription quota checkpoint since_unix_millis",
    )?)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_checkpoint_record).collect()
}

async fn list_checkpoints_inside_range(
    storage: &PostgresStorage,
    query: &SubscriptionQuotaCheckpointRangeQuery,
    windows: &[&str],
    sources: &[&str],
) -> StorageResult<Vec<SubscriptionQuotaCheckpointRecord>> {
    let rows = sqlx::query(
        "SELECT * FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ANY($1::uuid[]) \
         AND \"window\" = ANY($2::text[]) \
         AND source = ANY($3::text[]) \
         AND changed_at_unix_millis >= $4 \
         AND changed_at_unix_millis <= $5 \
         ORDER BY upstream_id ASC, \"window\" ASC, source ASC, changed_at_unix_millis ASC, sample_id ASC",
    )
    .bind(&query.upstream_ids)
    .bind(windows)
    .bind(sources)
    .bind(u64_to_i64(
        query.since_unix_millis,
        "subscription quota checkpoint since_unix_millis",
    )?)
    .bind(u64_to_i64(
        query.until_unix_millis,
        "subscription quota checkpoint until_unix_millis",
    )?)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_checkpoint_record).collect()
}

fn checkpoint_key(record: &SubscriptionQuotaCheckpointRecord) -> CheckpointKey {
    (record.upstream_id, record.window, record.source)
}

fn range_for_key(key: CheckpointKey) -> SubscriptionQuotaCheckpointRange {
    let (upstream_id, window, source) = key;
    SubscriptionQuotaCheckpointRange {
        upstream_id,
        window,
        source,
        left_anchor: None,
        checkpoints: Vec::new(),
    }
}

async fn upsert_latest(
    conn: &mut PgConnection,
    record: &SubscriptionQuotaObservationRecord,
) -> StorageResult<()> {
    let upgrade_paths_json = encode_upgrade_paths(record.upgrade_paths.as_ref())?;
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_latest_v1 \
         (upstream_id, \"window\", source, sample_kind, observed_at_unix_millis, sample_id, \
           utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim, \
           fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, upgrade_paths, \
           disabled_reason, \
           extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) \
         ON CONFLICT (upstream_id, \"window\", source) DO UPDATE SET \
         sample_kind = EXCLUDED.sample_kind, observed_at_unix_millis = EXCLUDED.observed_at_unix_millis, \
         sample_id = EXCLUDED.sample_id, utilization = EXCLUDED.utilization, status = EXCLUDED.status, \
         resets_at_unix_secs = EXCLUDED.resets_at_unix_secs, surpassed_threshold = EXCLUDED.surpassed_threshold, \
          representative_claim = EXCLUDED.representative_claim, fallback_percentage = EXCLUDED.fallback_percentage, \
          fallback_available = EXCLUDED.fallback_available, overage_in_use = EXCLUDED.overage_in_use, \
          overage_period_monthly_utilization = EXCLUDED.overage_period_monthly_utilization, \
          upgrade_paths = EXCLUDED.upgrade_paths, \
          disabled_reason = EXCLUDED.disabled_reason, \
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
    .execute(conn)
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

fn row_to_checkpoint_record(row: PgRow) -> StorageResult<SubscriptionQuotaCheckpointRecord> {
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
    let fingerprint = row
        .try_get::<Vec<u8>, _>("semantic_fingerprint")
        .map_err(map_sqlx_error)?;
    Ok(SubscriptionQuotaCheckpointRecord {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        window,
        source,
        changed_at_unix_millis: i64_to_u64(
            row.try_get("changed_at_unix_millis")
                .map_err(map_sqlx_error)?,
            "subscription quota checkpoint changed_at_unix_millis",
        )?,
        semantic_fingerprint: fingerprint_from_vec(fingerprint)?,
        sample_kind,
        sample_id: row.try_get("sample_id").map_err(map_sqlx_error)?,
        representative_claim: row
            .try_get("representative_claim")
            .map_err(map_sqlx_error)?,
        utilization: row.try_get("utilization").map_err(map_sqlx_error)?,
        status,
        resets_at_unix_secs: row
            .try_get::<Option<i64>, _>("resets_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "subscription quota resets_at_unix_secs"))
            .transpose()?,
        surpassed_threshold: row.try_get("surpassed_threshold").map_err(map_sqlx_error)?,
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

fn fingerprint_from_vec(bytes: Vec<u8>) -> StorageResult<SubscriptionQuotaSemanticFingerprint> {
    let len = bytes.len();
    let array = <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: format!("subscription quota checkpoint fingerprint has {len} bytes"),
    })?;
    Ok(SubscriptionQuotaSemanticFingerprint::from_bytes(array))
}

fn build_series_from_checkpoint_ranges(
    ranges: Vec<SubscriptionQuotaCheckpointRange>,
    query: &SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaSeries> {
    let bucket_secs = query.bucket_secs.max(1);
    let mut groups: BTreeMap<
        (Uuid, SubscriptionQuotaWindow, u8),
        Vec<SubscriptionQuotaCheckpointRange>,
    > = BTreeMap::new();
    for range in ranges {
        groups
            .entry((
                range.upstream_id,
                range.window,
                source_merge_code(query.source_merge),
            ))
            .or_default()
            .push(range);
    }
    let mut series = Vec::new();
    for ((upstream_id, window, source_code), group_ranges) in groups {
        let mut bucket_records = buckets_from_checkpoint_ranges(group_ranges, bucket_secs, query);
        downsample(
            &mut bucket_records,
            usize::try_from(query.max_points_per_series).unwrap_or(usize::MAX),
        );
        series.push(SubscriptionQuotaSeries {
            upstream_id,
            window,
            source: source_merge_from_code(source_code),
            buckets: bucket_records,
        });
    }
    series
}

#[derive(Debug, Clone)]
struct CheckpointStream {
    checkpoints: Vec<SubscriptionQuotaCheckpointRecord>,
    index: usize,
    current: Option<SubscriptionQuotaCheckpointRecord>,
}

#[derive(Debug)]
struct BucketPoint {
    record: SubscriptionQuotaObservationRecord,
    is_change: bool,
}

fn buckets_from_checkpoint_ranges(
    ranges: Vec<SubscriptionQuotaCheckpointRange>,
    bucket_secs: u64,
    query: &SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaBucket> {
    let mut streams = ranges
        .into_iter()
        .map(stream_from_range)
        .filter(|stream| !stream.checkpoints.is_empty())
        .collect::<Vec<_>>();
    streams.sort_by(|left, right| {
        let left_source = left.checkpoints[0].source.as_str();
        let right_source = right.checkpoints[0].source.as_str();
        left_source.cmp(right_source)
    });
    let Some(mut bucket_start) = streams
        .iter()
        .filter_map(|stream| stream.checkpoints.first())
        .map(|checkpoint| bucket_start_unix_secs(checkpoint.changed_at_unix_millis, bucket_secs))
        .min()
    else {
        return Vec::new();
    };
    let end_bucket = bucket_start_unix_secs(query.until_unix_millis, bucket_secs);
    let mut buckets = Vec::new();
    while bucket_start <= end_bucket {
        let mut points = Vec::new();
        for stream in &mut streams {
            push_bucket_points(stream, bucket_start, bucket_secs, &mut points);
        }
        if !points.is_empty() {
            buckets.push(bucket_from_points(bucket_start, &points));
        }
        let Some(next_bucket) = bucket_start.checked_add(bucket_secs) else {
            break;
        };
        bucket_start = next_bucket;
    }
    buckets
}

fn stream_from_range(range: SubscriptionQuotaCheckpointRange) -> CheckpointStream {
    let mut checkpoints =
        Vec::with_capacity(range.checkpoints.len() + usize::from(range.left_anchor.is_some()));
    if let Some(anchor) = range.left_anchor {
        checkpoints.push(anchor);
    }
    checkpoints.extend(range.checkpoints);
    checkpoints.sort_by_key(|checkpoint| (checkpoint.changed_at_unix_millis, checkpoint.sample_id));
    CheckpointStream {
        checkpoints,
        index: 0,
        current: None,
    }
}

fn push_bucket_points(
    stream: &mut CheckpointStream,
    bucket_start: u64,
    bucket_secs: u64,
    points: &mut Vec<BucketPoint>,
) {
    let start_index = stream.index;
    while stream.index < stream.checkpoints.len()
        && bucket_start_unix_secs(
            stream.checkpoints[stream.index].changed_at_unix_millis,
            bucket_secs,
        ) == bucket_start
    {
        let checkpoint = stream.checkpoints[stream.index].clone();
        stream.current = Some(checkpoint.clone());
        points.push(BucketPoint {
            record: record_from_checkpoint(&checkpoint),
            is_change: true,
        });
        stream.index += 1;
    }
    if stream.index == start_index
        && let Some(checkpoint) = &stream.current
    {
        points.push(BucketPoint {
            record: record_from_checkpoint(checkpoint),
            is_change: false,
        });
    }
}

fn bucket_start_unix_secs(timestamp_unix_millis: u64, bucket_secs: u64) -> u64 {
    let timestamp_unix_secs = timestamp_unix_millis / 1_000;
    timestamp_unix_secs / bucket_secs * bucket_secs
}

fn record_from_checkpoint(
    checkpoint: &SubscriptionQuotaCheckpointRecord,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id: checkpoint.upstream_id,
        window: checkpoint.window,
        source: checkpoint.source,
        sample_kind: checkpoint.sample_kind,
        observed_at_unix_millis: checkpoint.changed_at_unix_millis,
        sample_id: checkpoint.sample_id,
        utilization: checkpoint.utilization,
        status: checkpoint.status,
        resets_at_unix_secs: checkpoint.resets_at_unix_secs,
        surpassed_threshold: checkpoint.surpassed_threshold,
        representative_claim: checkpoint.representative_claim.clone(),
        fallback_percentage: checkpoint.fallback_percentage,
        fallback_available: checkpoint.fallback_available,
        overage_in_use: checkpoint.overage_in_use,
        overage_period_monthly_utilization: checkpoint.overage_period_monthly_utilization,
        upgrade_paths: checkpoint.upgrade_paths.clone(),
        disabled_reason: checkpoint.disabled_reason.clone(),
        extra_usage_enabled: checkpoint.extra_usage_enabled,
        extra_usage_monthly_limit: checkpoint.extra_usage_monthly_limit,
        extra_usage_used_credits: checkpoint.extra_usage_used_credits,
        ingested_at_unix_millis: checkpoint.ingested_at_unix_millis,
    }
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

fn bucket_from_points(
    bucket_start_unix_secs: u64,
    points: &[BucketPoint],
) -> SubscriptionQuotaBucket {
    let mut count = 0u32;
    let mut sum = 0.0;
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    let mut last = &points[0].record;
    let mut sources_seen = BTreeSet::new();
    let mut sample_count = 0u32;
    for point in points {
        let record = &point.record;
        sources_seen.insert(record.source);
        if point.is_change {
            sample_count = sample_count.saturating_add(1);
        }
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
        sample_count,
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
