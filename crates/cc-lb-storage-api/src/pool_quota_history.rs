use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{StorageResult, upstream_subscription_quota::SubscriptionQuotaWindow};

pub const POOL_QUOTA_POLICY_VERSION: i32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolQuotaSnapshotRecord {
    pub snapshot_at_unix_secs: i64,
    pub window: SubscriptionQuotaWindow,
    pub utilization: Option<f64>,
    pub weighted_utilization_sum: f64,
    pub capacity_ratio_sum: f64,
    pub eligible_upstreams: i64,
    pub contributing_upstreams: i64,
    pub stale_upstreams: i64,
    pub missing_observation_upstreams: i64,
    pub missing_metadata_upstreams: i64,
    pub header_contributing_upstreams: i64,
    pub api_contributing_upstreams: i64,
    pub max_observed_at_unix_millis: Option<i64>,
    pub contributors_json: Option<String>,
    pub computed_at_unix_millis: i64,
    pub policy_version: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolQuotaSnapshotSummaryRecord {
    pub snapshot_at_unix_secs: i64,
    pub window: SubscriptionQuotaWindow,
    pub utilization: Option<f64>,
    pub weighted_utilization_sum: f64,
    pub capacity_ratio_sum: f64,
    pub eligible_upstreams: i64,
    pub contributing_upstreams: i64,
    pub stale_upstreams: i64,
    pub missing_observation_upstreams: i64,
    pub missing_metadata_upstreams: i64,
    pub header_contributing_upstreams: i64,
    pub api_contributing_upstreams: i64,
    pub max_observed_at_unix_millis: Option<i64>,
    pub computed_at_unix_millis: i64,
    pub policy_version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolQuotaContributorBlob {
    pub snapshot_at_unix_secs: i64,
    pub window: SubscriptionQuotaWindow,
    pub contributors_json: String,
}

impl From<&PoolQuotaSnapshotRecord> for PoolQuotaSnapshotSummaryRecord {
    fn from(record: &PoolQuotaSnapshotRecord) -> Self {
        Self {
            snapshot_at_unix_secs: record.snapshot_at_unix_secs,
            window: record.window,
            utilization: record.utilization,
            weighted_utilization_sum: record.weighted_utilization_sum,
            capacity_ratio_sum: record.capacity_ratio_sum,
            eligible_upstreams: record.eligible_upstreams,
            contributing_upstreams: record.contributing_upstreams,
            stale_upstreams: record.stale_upstreams,
            missing_observation_upstreams: record.missing_observation_upstreams,
            missing_metadata_upstreams: record.missing_metadata_upstreams,
            header_contributing_upstreams: record.header_contributing_upstreams,
            api_contributing_upstreams: record.api_contributing_upstreams,
            max_observed_at_unix_millis: record.max_observed_at_unix_millis,
            computed_at_unix_millis: record.computed_at_unix_millis,
            policy_version: record.policy_version,
        }
    }
}

#[async_trait]
pub trait PoolQuotaHistoryStore: Send + Sync {
    async fn record_pool_quota_snapshots(
        &self,
        records: &[PoolQuotaSnapshotRecord],
    ) -> StorageResult<()>;

    async fn list_latest_pool_quota_snapshots(
        &self,
        windows: &[SubscriptionQuotaWindow],
    ) -> StorageResult<Vec<PoolQuotaSnapshotRecord>>;

    async fn list_pool_quota_snapshots_in_range(
        &self,
        windows: &[SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
    ) -> StorageResult<Vec<PoolQuotaSnapshotRecord>>;

    async fn list_pool_quota_contributor_blobs_page(
        &self,
        after: Option<(i64, SubscriptionQuotaWindow)>,
        limit: u32,
    ) -> StorageResult<Vec<PoolQuotaContributorBlob>>;

    async fn list_latest_pool_quota_snapshot_summaries(
        &self,
        windows: &[SubscriptionQuotaWindow],
    ) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
        let records = self.list_latest_pool_quota_snapshots(windows).await?;
        Ok(records
            .iter()
            .map(PoolQuotaSnapshotSummaryRecord::from)
            .collect())
    }

    async fn list_pool_quota_snapshot_summaries_in_range(
        &self,
        windows: &[SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
    ) -> StorageResult<Vec<PoolQuotaSnapshotSummaryRecord>> {
        let records = self
            .list_pool_quota_snapshots_in_range(windows, since_unix_secs, until_unix_secs)
            .await?;
        Ok(records
            .iter()
            .map(PoolQuotaSnapshotSummaryRecord::from)
            .collect())
    }

    async fn delete_pool_quota_snapshots_before(
        &self,
        cutoff_unix_secs: i64,
        batch_size: u32,
    ) -> StorageResult<u64>;
}
