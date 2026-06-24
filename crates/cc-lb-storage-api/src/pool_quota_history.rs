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

    async fn delete_pool_quota_snapshots_before(
        &self,
        cutoff_unix_secs: i64,
        batch_size: u32,
    ) -> StorageResult<u64>;
}
