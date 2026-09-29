use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLot,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamSubscriptionQuotaAggregateStore,
    UpstreamSubscriptionQuotaStore,
};
use sqlx::{AssertSqlSafe, Row, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl UpstreamSubscriptionQuotaStore for SqliteStorage {
    async fn record_subscription_quota_samples(
        &self,
        records: &[SubscriptionQuotaSample],
    ) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        for record in records {
            upsert_latest(&mut tx, record).await?;
            if record.sample_kind != SubscriptionQuotaSampleKind::Absent {
                insert_checkpoint_if_changed(
                    &mut tx,
                    &SubscriptionQuotaCheckpointRecord::from(record),
                )
                .await?;
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaSample>> {
        list_latest_records_for_upstreams(self, upstream_ids).await
    }

    async fn put_subscription_quota_checkpoints(
        &self,
        records: &[SubscriptionQuotaCheckpointRecord],
    ) -> StorageResult<usize> {
        let mut tx = self.begin_immediate().await?;
        let mut inserted = 0usize;
        for record in records {
            if insert_checkpoint_if_changed(&mut tx, record).await? {
                inserted += 1;
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(inserted)
    }
}

#[async_trait]
impl UpstreamSubscriptionQuotaAggregateStore for SqliteStorage {
    async fn list_subscription_quota_slim_checkpoints(
        &self,
        query: SubscriptionQuotaCheckpointRangeQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSlimCheckpoint>> {
        list_slim_checkpoints_for_query(self, &query).await
    }

    async fn list_subscription_quota_provider_lots(
        &self,
        query: SubscriptionQuotaProviderLotQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaProviderLot>> {
        let sources = query
            .sources
            .iter()
            .copied()
            .filter(|source| source_matches_merge(*source, query.source_merge))
            .collect::<Vec<_>>();
        let checkpoints = list_slim_checkpoints_for_query(
            self,
            &SubscriptionQuotaCheckpointRangeQuery {
                upstream_ids: query.upstream_ids.clone(),
                windows: query.windows.clone(),
                sources,
                since_unix_millis: query.since_unix_millis,
                until_unix_millis: query.until_unix_millis,
            },
        )
        .await?;
        Ok(provider_lots_from_slim_checkpoints(checkpoints, &query))
    }
}

async fn list_latest_records_for_upstreams(
    storage: &SqliteStorage,
    upstream_ids: &[Uuid],
) -> StorageResult<Vec<SubscriptionQuotaSample>> {
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

pub(super) async fn insert_checkpoint_if_changed(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &SubscriptionQuotaCheckpointRecord,
) -> StorageResult<bool> {
    let latest_fingerprint = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT semantic_fingerprint FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ? AND window = ? AND source = ? \
         ORDER BY changed_at_unix_millis DESC, sample_id DESC \
         LIMIT 1",
    )
    .bind(record.upstream_id.to_string())
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;

    if latest_fingerprint.as_deref() == Some(&record.semantic_fingerprint.as_bytes()[..]) {
        return Ok(false);
    }

    let upgrade_paths_json = encode_upgrade_paths(record.upgrade_paths.as_ref())?;
    let result = sqlx::query(
        "INSERT INTO upstream_subscription_quota_checkpoints_v1 \
         (upstream_id, window, source, changed_at_unix_millis, sample_id, semantic_fingerprint, \
          sample_kind, representative_claim, utilization, status, resets_at_unix_secs, \
          surpassed_threshold, fallback_percentage, fallback_available, overage_in_use, \
          overage_period_monthly_utilization, upgrade_paths, disabled_reason, extra_usage_enabled, \
          extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(record.upstream_id.to_string())
    .bind(record.window.as_str())
    .bind(record.source.as_str())
    .bind(u64_to_i64(
        record.changed_at_unix_millis,
        "subscription quota checkpoint changed_at_unix_millis",
    )?)
    .bind(record.sample_id.to_string())
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
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected() == 1)
}

async fn list_slim_checkpoints_for_query(
    storage: &SqliteStorage,
    query: &SubscriptionQuotaCheckpointRangeQuery,
) -> StorageResult<Vec<SubscriptionQuotaSlimCheckpoint>> {
    if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
        return Ok(Vec::new());
    }
    let windows = query.windows.iter().copied().collect::<BTreeSet<_>>();
    let sources = query.sources.iter().copied().collect::<BTreeSet<_>>();
    let upstream_placeholders = placeholders(query.upstream_ids.len());
    let window_placeholders = placeholders(windows.len());
    let source_placeholders = placeholders(sources.len());
    let projection = "checkpoint.upstream_id, checkpoint.window, checkpoint.source, \
                      checkpoint.changed_at_unix_millis, checkpoint.sample_id, \
                      checkpoint.utilization, checkpoint.status, checkpoint.resets_at_unix_secs";
    let sql = format!(
        "WITH anchor_ts AS ( \
             SELECT upstream_id, window, source, MAX(changed_at_unix_millis) AS changed_at_unix_millis \
             FROM upstream_subscription_quota_checkpoints_v1 \
             WHERE upstream_id IN ({upstream_placeholders}) \
             AND window IN ({window_placeholders}) \
             AND source IN ({source_placeholders}) \
             AND changed_at_unix_millis < ? \
             GROUP BY upstream_id, window, source \
         ), \
         anchor_ids AS ( \
             SELECT checkpoint.upstream_id, checkpoint.window, checkpoint.source, \
                    checkpoint.changed_at_unix_millis, MAX(checkpoint.sample_id) AS sample_id \
             FROM upstream_subscription_quota_checkpoints_v1 checkpoint \
             INNER JOIN anchor_ts anchor \
               ON anchor.upstream_id = checkpoint.upstream_id \
              AND anchor.window = checkpoint.window \
              AND anchor.source = checkpoint.source \
              AND anchor.changed_at_unix_millis = checkpoint.changed_at_unix_millis \
             GROUP BY checkpoint.upstream_id, checkpoint.window, checkpoint.source, \
                      checkpoint.changed_at_unix_millis \
         ) \
         SELECT {projection} FROM upstream_subscription_quota_checkpoints_v1 checkpoint \
         INNER JOIN anchor_ids anchor \
           ON anchor.upstream_id = checkpoint.upstream_id \
          AND anchor.window = checkpoint.window \
          AND anchor.source = checkpoint.source \
          AND anchor.changed_at_unix_millis = checkpoint.changed_at_unix_millis \
          AND anchor.sample_id = checkpoint.sample_id \
         UNION ALL \
         SELECT {projection} FROM upstream_subscription_quota_checkpoints_v1 checkpoint \
         WHERE checkpoint.upstream_id IN ({upstream_placeholders}) \
         AND checkpoint.window IN ({window_placeholders}) \
         AND checkpoint.source IN ({source_placeholders}) \
         AND checkpoint.changed_at_unix_millis >= ? \
         AND checkpoint.changed_at_unix_millis <= ? \
         ORDER BY upstream_id ASC, window ASC, source ASC, \
                  changed_at_unix_millis ASC, sample_id ASC"
    );
    let since = u64_to_i64(
        query.since_unix_millis,
        "subscription quota checkpoint since_unix_millis",
    )?;
    let until = u64_to_i64(
        query.until_unix_millis,
        "subscription quota checkpoint until_unix_millis",
    )?;
    let mut q = sqlx::query(AssertSqlSafe(sql));
    q = bind_checkpoint_filters(q, query, &windows, &sources).bind(since);
    q = bind_checkpoint_filters(q, query, &windows, &sources)
        .bind(since)
        .bind(until);

    let start = Instant::now();
    let mut tx = storage.pool().begin().await.map_err(map_sqlx_error)?;
    let rows_result = q.fetch_all(&mut *tx).await.map_err(map_sqlx_error);
    record_storage_operation("quota_slim_checkpoint_range", start, &rows_result);
    storage.record_pool_metrics();
    let rows = rows_result?;
    tx.commit().await.map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_slim_checkpoint).collect()
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

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn bind_checkpoint_filters<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    query: &SubscriptionQuotaCheckpointRangeQuery,
    windows: &BTreeSet<SubscriptionQuotaWindow>,
    sources: &BTreeSet<SubscriptionQuotaSource>,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    for upstream_id in &query.upstream_ids {
        q = q.bind(upstream_id.to_string());
    }
    for window in windows {
        q = q.bind(window.as_str().to_owned());
    }
    for source in sources {
        q = q.bind(source.as_str().to_owned());
    }
    q
}

async fn upsert_latest(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &SubscriptionQuotaSample,
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

pub(super) fn row_to_record(row: SqliteRow) -> StorageResult<SubscriptionQuotaSample> {
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

    Ok(SubscriptionQuotaSample {
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

fn row_to_slim_checkpoint(row: SqliteRow) -> StorageResult<SubscriptionQuotaSlimCheckpoint> {
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    let sample_id = row
        .try_get::<String, _>("sample_id")
        .map_err(map_sqlx_error)?;
    let status = row
        .try_get::<Option<String>, _>("status")
        .map_err(map_sqlx_error)?
        .as_deref()
        .map(parse_status)
        .transpose()?;

    Ok(SubscriptionQuotaSlimCheckpoint {
        upstream_id: parse_uuid(&upstream_id, "subscription quota checkpoint upstream_id")?,
        window: parse_window(&row.try_get::<String, _>("window").map_err(map_sqlx_error)?)?,
        source: parse_source(&row.try_get::<String, _>("source").map_err(map_sqlx_error)?)?,
        changed_at_unix_millis: i64_to_u64(
            row.try_get("changed_at_unix_millis")
                .map_err(map_sqlx_error)?,
            "subscription quota checkpoint changed_at_unix_millis",
        )?,
        sample_id: parse_uuid(&sample_id, "subscription quota checkpoint sample_id")?,
        utilization: row.try_get("utilization").map_err(map_sqlx_error)?,
        status,
        resets_at_unix_secs: row
            .try_get::<Option<i64>, _>("resets_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "subscription quota resets_at_unix_secs"))
            .transpose()?,
    })
}

const PROVIDER_LOT_BUCKET_SECS: u64 = 60;
const PROVIDER_LOT_RESET_DROP_THRESHOLD: f64 = 0.5;

struct SlimCheckpointStream {
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    next_index: usize,
    current_index: Option<usize>,
}

impl SlimCheckpointStream {
    fn new(mut checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>, start_bucket: u64) -> Self {
        checkpoints
            .sort_by_key(|checkpoint| (checkpoint.changed_at_unix_millis, checkpoint.sample_id));
        let mut next_index = 0;
        while next_index < checkpoints.len()
            && bucket_start_unix_secs(
                checkpoints[next_index].changed_at_unix_millis,
                PROVIDER_LOT_BUCKET_SECS,
            ) < start_bucket
        {
            next_index += 1;
        }
        Self {
            checkpoints,
            current_index: next_index.checked_sub(1),
            next_index,
        }
    }

    fn advance_to_bucket(&mut self, bucket_start: u64) {
        while self.next_index < self.checkpoints.len()
            && bucket_start_unix_secs(
                self.checkpoints[self.next_index].changed_at_unix_millis,
                PROVIDER_LOT_BUCKET_SECS,
            ) == bucket_start
        {
            self.current_index = Some(self.next_index);
            self.next_index += 1;
        }
    }

    fn current(&self) -> Option<&SubscriptionQuotaSlimCheckpoint> {
        self.current_index.map(|index| &self.checkpoints[index])
    }
}

#[derive(Clone, Copy)]
struct ProviderLotObservation {
    bucket_start_unix_secs: u64,
    observed_at_unix_millis: u64,
    utilization: f64,
    resets_at_unix_secs: Option<u64>,
}

struct ProviderLotCycle {
    first_observed_at_unix_millis: u64,
    last: ProviderLotObservation,
}

fn provider_lots_from_slim_checkpoints(
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    query: &SubscriptionQuotaProviderLotQuery,
) -> Vec<SubscriptionQuotaProviderLot> {
    let mut groups = BTreeMap::<
        (Uuid, SubscriptionQuotaWindow),
        BTreeMap<SubscriptionQuotaSource, Vec<SubscriptionQuotaSlimCheckpoint>>,
    >::new();
    for checkpoint in checkpoints {
        groups
            .entry((checkpoint.upstream_id, checkpoint.window))
            .or_default()
            .entry(checkpoint.source)
            .or_default()
            .push(checkpoint);
    }

    let mut lots = Vec::new();
    for ((upstream_id, window), source_checkpoints) in groups {
        let Some(window_secs) = provider_lot_window_secs(window) else {
            continue;
        };
        let mut bucket_start =
            bucket_start_unix_secs(query.since_unix_millis, PROVIDER_LOT_BUCKET_SECS);
        let mut streams = source_checkpoints
            .into_values()
            .map(|checkpoints| SlimCheckpointStream::new(checkpoints, bucket_start))
            .collect::<Vec<_>>();
        if streams.is_empty() {
            continue;
        }
        let end_bucket = bucket_start_unix_secs(query.until_unix_millis, PROVIDER_LOT_BUCKET_SECS);
        let seeded = streams
            .iter()
            .filter_map(SlimCheckpointStream::current)
            .max_by(|left, right| {
                (
                    left.changed_at_unix_millis,
                    left.source.as_str(),
                    left.sample_id,
                )
                    .cmp(&(
                        right.changed_at_unix_millis,
                        right.source.as_str(),
                        right.sample_id,
                    ))
            })
            .and_then(|checkpoint| {
                checkpoint
                    .utilization
                    .map(|utilization| ProviderLotObservation {
                        bucket_start_unix_secs: bucket_start
                            .saturating_sub(PROVIDER_LOT_BUCKET_SECS),
                        observed_at_unix_millis: checkpoint.changed_at_unix_millis,
                        utilization,
                        resets_at_unix_secs: checkpoint.resets_at_unix_secs,
                    })
            });
        let mut cycle = seeded.map(|observation| ProviderLotCycle {
            first_observed_at_unix_millis: observation.observed_at_unix_millis,
            last: observation,
        });
        let mut previous = seeded;
        while bucket_start <= end_bucket {
            for stream in &mut streams {
                stream.advance_to_bucket(bucket_start);
            }
            let latest = streams
                .iter()
                .filter_map(SlimCheckpointStream::current)
                .max_by(|left, right| {
                    (
                        left.changed_at_unix_millis,
                        left.source.as_str(),
                        left.sample_id,
                    )
                        .cmp(&(
                            right.changed_at_unix_millis,
                            right.source.as_str(),
                            right.sample_id,
                        ))
                });
            if let Some(checkpoint) = latest
                && let Some(utilization) = checkpoint.utilization
            {
                let observation = ProviderLotObservation {
                    bucket_start_unix_secs: bucket_start,
                    observed_at_unix_millis: checkpoint.changed_at_unix_millis,
                    utilization,
                    resets_at_unix_secs: checkpoint.resets_at_unix_secs,
                };
                if previous
                    .is_some_and(|previous| provider_lot_starts_new_cycle(previous, observation))
                    && let Some(completed) = cycle.take()
                {
                    lots.push(provider_lot_from_cycle(
                        upstream_id,
                        window,
                        query.source_merge,
                        window_secs,
                        completed,
                        previous.map_or(bucket_start, |value| value.bucket_start_unix_secs),
                    ));
                }
                match &mut cycle {
                    Some(cycle) => cycle.last = observation,
                    None => {
                        cycle = Some(ProviderLotCycle {
                            first_observed_at_unix_millis: observation.observed_at_unix_millis,
                            last: observation,
                        });
                    }
                }
                previous = Some(observation);
            }
            let Some(next_bucket) = bucket_start.checked_add(PROVIDER_LOT_BUCKET_SECS) else {
                break;
            };
            bucket_start = next_bucket;
        }
        if let Some(completed) = cycle {
            lots.push(provider_lot_from_cycle(
                upstream_id,
                window,
                query.source_merge,
                window_secs,
                completed,
                query.evaluation_unix_secs,
            ));
        }
    }
    lots
}

fn provider_lot_starts_new_cycle(
    previous: ProviderLotObservation,
    current: ProviderLotObservation,
) -> bool {
    let reset_changed = match (previous.resets_at_unix_secs, current.resets_at_unix_secs) {
        (Some(left), Some(right)) => left.abs_diff(right) > PROVIDER_LOT_BUCKET_SECS,
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    };
    reset_changed || previous.utilization - current.utilization >= PROVIDER_LOT_RESET_DROP_THRESHOLD
}

fn provider_lot_from_cycle(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    source: SubscriptionQuotaSourceMerge,
    window_secs: u64,
    cycle: ProviderLotCycle,
    evaluation_unix_secs: u64,
) -> SubscriptionQuotaProviderLot {
    SubscriptionQuotaProviderLot {
        upstream_id,
        window,
        source,
        provider_start_unix_secs: cycle
            .last
            .resets_at_unix_secs
            .map(|reset| reset.saturating_sub(window_secs))
            .or(Some(cycle.first_observed_at_unix_millis / 1_000)),
        provider_reset_unix_secs: cycle.last.resets_at_unix_secs,
        observed_at_unix_millis: cycle.last.observed_at_unix_millis,
        evaluation_unix_secs,
        utilization: cycle.last.utilization,
    }
}

const fn provider_lot_window_secs(window: SubscriptionQuotaWindow) -> Option<u64> {
    match window {
        SubscriptionQuotaWindow::FiveHour => Some(5 * 3_600),
        SubscriptionQuotaWindow::SevenDay
        | SubscriptionQuotaWindow::SevenDaySonnet
        | SubscriptionQuotaWindow::SevenDayOpus
        | SubscriptionQuotaWindow::SevenDayFable => Some(7 * 24 * 3_600),
        SubscriptionQuotaWindow::Overage | SubscriptionQuotaWindow::Unified => None,
    }
}

fn bucket_start_unix_secs(timestamp_unix_millis: u64, bucket_secs: u64) -> u64 {
    let timestamp_unix_secs = timestamp_unix_millis / 1_000;
    timestamp_unix_secs / bucket_secs * bucket_secs
}

fn source_matches_merge(
    source: SubscriptionQuotaSource,
    source_merge: SubscriptionQuotaSourceMerge,
) -> bool {
    match source_merge {
        SubscriptionQuotaSourceMerge::Merged => true,
        SubscriptionQuotaSourceMerge::Header => source == SubscriptionQuotaSource::Header,
        SubscriptionQuotaSourceMerge::Api => source == SubscriptionQuotaSource::Api,
    }
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
