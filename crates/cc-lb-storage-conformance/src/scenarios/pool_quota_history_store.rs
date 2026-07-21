use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, SubscriptionQuotaWindow};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn fable_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PoolQuotaHistoryStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let mut record = PoolQuotaSnapshotRecord {
            snapshot_at_unix_secs: 1_800_000_000,
            window: SubscriptionQuotaWindow::SevenDayFable,
            utilization: Some(0.28),
            weighted_utilization_sum: 1.4,
            capacity_ratio_sum: 5.0,
            eligible_upstreams: 2,
            contributing_upstreams: 1,
            stale_upstreams: 0,
            missing_observation_upstreams: 1,
            missing_metadata_upstreams: 0,
            header_contributing_upstreams: 1,
            api_contributing_upstreams: 0,
            max_observed_at_unix_millis: Some(1_800_000_000_000),
            computed_at_unix_millis: 1_800_000_000_500,
            policy_version: 1,
        };
        storage
            .record_pool_quota_snapshots(std::slice::from_ref(&record))
            .await?;

        let latest = storage
            .list_latest_pool_quota_snapshots(&[SubscriptionQuotaWindow::SevenDayFable])
            .await?;
        ensure!(
            latest == vec![record.clone()],
            "latest Fable row round-trips"
        );

        record.utilization = Some(0.31);
        record.weighted_utilization_sum = 1.55;
        storage
            .record_pool_quota_snapshots(std::slice::from_ref(&record))
            .await?;
        let range = storage
            .list_pool_quota_snapshots_in_range(
                &[SubscriptionQuotaWindow::SevenDayFable],
                1_800_000_000,
                1_800_000_000,
            )
            .await?;
        ensure!(
            range == vec![record],
            "Fable primary-key upsert replaces the row"
        );
        Ok(())
    })
    .await
}
