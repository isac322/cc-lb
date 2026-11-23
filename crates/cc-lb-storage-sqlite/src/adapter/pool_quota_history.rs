use async_trait::async_trait;
use cc_lb_storage_api::{
    PoolQuotaChartPointRecord, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord,
    PoolQuotaSnapshotSummaryRecord, Provider, StorageResult, SubscriptionQuotaWindow,
};

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl PoolQuotaHistoryStore for SqliteStorage {
    async fn prune_pool_quota_history_before(
        &self,
        cutoff_unix_secs: i64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let result = sqlx::query(
            "DELETE FROM pool_subscription_quota_history_v1 WHERE rowid IN ( \
             SELECT candidate.rowid FROM pool_subscription_quota_history_v1 candidate \
             WHERE candidate.snapshot_at_unix_secs < ?1 \
               AND candidate.rowid <> ( \
                 SELECT anchor.rowid FROM pool_subscription_quota_history_v1 anchor \
                 WHERE anchor.provider = candidate.provider
                   AND anchor.quota_window = candidate.quota_window \
                   AND anchor.snapshot_at_unix_secs <= ?1 \
                 ORDER BY anchor.snapshot_at_unix_secs DESC LIMIT 1 \
               ) \
             ORDER BY candidate.snapshot_at_unix_secs, candidate.quota_window LIMIT ?2)",
        )
        .bind(cutoff_unix_secs)
        .bind(super::request_events::usize_to_i64(
            batch_size,
            "pool quota prune batch size",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }

    async fn record_pool_quota_snapshots(
        &self,
        records: &[PoolQuotaSnapshotRecord],
    ) -> StorageResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut tx = self.begin_immediate().await?;
        for record in records {
            sqlx::query(
                r#"INSERT INTO pool_subscription_quota_history_v1 (
                    provider, snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
                    capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                    stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                    header_contributing_upstreams, api_contributing_upstreams,
                    max_observed_at_unix_millis, computed_at_unix_millis,
                    policy_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, snapshot_at_unix_secs, quota_window) DO UPDATE SET
                    utilization = excluded.utilization,
                    weighted_utilization_sum = excluded.weighted_utilization_sum,
                    capacity_ratio_sum = excluded.capacity_ratio_sum,
                    eligible_upstreams = excluded.eligible_upstreams,
                    contributing_upstreams = excluded.contributing_upstreams,
                    stale_upstreams = excluded.stale_upstreams,
                    missing_observation_upstreams = excluded.missing_observation_upstreams,
                    missing_metadata_upstreams = excluded.missing_metadata_upstreams,
                    header_contributing_upstreams = excluded.header_contributing_upstreams,
                    api_contributing_upstreams = excluded.api_contributing_upstreams,
                    max_observed_at_unix_millis = excluded.max_observed_at_unix_millis,
                    computed_at_unix_millis = excluded.computed_at_unix_millis,
                    policy_version = excluded.policy_version"#,
            )
            .bind(provider_key(record.provider))
            .bind(record.snapshot_at_unix_secs)
            .bind(record.window.as_str())
            .bind(record.utilization)
            .bind(record.weighted_utilization_sum)
            .bind(record.capacity_ratio_sum)
            .bind(record.eligible_upstreams)
            .bind(record.contributing_upstreams)
            .bind(record.stale_upstreams)
            .bind(record.missing_observation_upstreams)
            .bind(record.missing_metadata_upstreams)
            .bind(record.header_contributing_upstreams)
            .bind(record.api_contributing_upstreams)
            .bind(record.max_observed_at_unix_millis)
            .bind(record.computed_at_unix_millis)
            .bind(record.policy_version)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_latest_pool_quota_snapshot_summaries(
        &self,
        provider: Provider,
        windows: &[SubscriptionQuotaWindow],
    ) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
        super::pool_quota_history_summary::list_latest(self, provider, windows).await
    }

    async fn list_pool_quota_snapshot_summaries_in_range(
        &self,
        provider: Provider,
        windows: &[SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
    ) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
        super::pool_quota_history_summary::list_range(
            self,
            provider,
            windows,
            since_unix_secs,
            until_unix_secs,
        )
        .await
    }

    async fn list_pool_quota_chart_points_in_range(
        &self,
        provider: Provider,
        windows: &[SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
        bucket_secs: Option<i64>,
    ) -> StorageResult<Vec<PoolQuotaChartPointRecord>> {
        super::pool_quota_history_summary::list_chart_range(
            self,
            provider,
            windows,
            since_unix_secs,
            until_unix_secs,
            bucket_secs,
        )
        .await
    }
}

pub(super) fn provider_key(provider: Provider) -> &'static str {
    match provider {
        Provider::Anthropic => "anthropic",
        Provider::Openai => "openai",
        Provider::Gemini => "gemini",
    }
}
