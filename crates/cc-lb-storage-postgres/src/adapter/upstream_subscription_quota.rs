use std::collections::BTreeSet;

use async_trait::async_trait;
use cc_lb_storage_api::{
    ChangeChannel, StorageError, StorageResult, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLot,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaAggregateStore,
    UpstreamSubscriptionQuotaStore,
};
use sqlx::{PgConnection, Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

mod aggregate;

#[async_trait]
impl UpstreamSubscriptionQuotaStore for PostgresStorage {
    async fn record_subscription_quota_samples(
        &self,
        records: &[SubscriptionQuotaSample],
    ) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
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

        // The batch is committed; a failed NOTIFY only delays peer hydration,
        // so log instead of reporting the write as failed.
        let upstream_ids: BTreeSet<Uuid> =
            records.iter().map(|record| record.upstream_id).collect();
        for upstream_id in upstream_ids {
            if let Err(error) = sqlx::query("SELECT pg_notify($1, $2)")
                .bind(ChangeChannel::SubscriptionQuota.postgres_channel())
                .bind(upstream_id.to_string())
                .execute(&self.pool)
                .await
            {
                tracing::warn!(%error, "subscription quota change notify failed");
            }
        }

        Ok(())
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaSample>> {
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
}

#[async_trait]
impl UpstreamSubscriptionQuotaAggregateStore for PostgresStorage {
    async fn list_subscription_quota_slim_checkpoints(
        &self,
        query: SubscriptionQuotaCheckpointRangeQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSlimCheckpoint>> {
        aggregate::list_slim_checkpoints(self, &query).await
    }

    async fn list_subscription_quota_provider_lots(
        &self,
        query: SubscriptionQuotaProviderLotQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaProviderLot>> {
        aggregate::list_provider_lots(self, &query).await
    }
}

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

async fn upsert_latest(
    conn: &mut PgConnection,
    record: &SubscriptionQuotaSample,
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

fn row_to_record(row: PgRow) -> StorageResult<SubscriptionQuotaSample> {
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
    Ok(SubscriptionQuotaSample {
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
