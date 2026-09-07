use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    PoolQuotaChartPointRecord, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord,
    SubscriptionQuotaWindow,
};

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

        let chart_records = vec![
            pool_chart_record(-121, Some(0.10)),
            pool_chart_record(-120, Some(0.20)),
            pool_chart_record(-61, Some(0.40)),
            pool_chart_record(-60, None),
            pool_chart_record(-1, None),
            pool_chart_record(0, Some(0.20)),
        ];
        storage.record_pool_quota_snapshots(&chart_records).await?;

        let exact_chart = storage
            .list_pool_quota_chart_points_in_range(
                &[SubscriptionQuotaWindow::FiveHour],
                -121,
                0,
                None,
            )
            .await?;
        ensure!(
            exact_chart
                == vec![
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -121,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.10),
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -120,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.20),
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -61,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.40),
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -60,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: None,
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -1,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: None,
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: 0,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.20),
                    },
                ],
            "unbucketed chart projection preserves every stored point"
        );

        let bucketed_chart = storage
            .list_pool_quota_chart_points_in_range(
                &[SubscriptionQuotaWindow::FiveHour],
                -121,
                0,
                Some(60),
            )
            .await?;
        ensure!(
            bucketed_chart
                == vec![
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -180,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.10),
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -120,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.40),
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: -60,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: None,
                    },
                    PoolQuotaChartPointRecord {
                        snapshot_at_unix_secs: 0,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.20),
                    },
                ],
            "bucketed chart projection uses Euclidean epoch buckets and SQL MAX null semantics"
        );

        let empty_chart = storage
            .list_pool_quota_chart_points_in_range(
                &[SubscriptionQuotaWindow::FiveHour],
                1,
                0,
                Some(60),
            )
            .await?;
        ensure!(
            empty_chart.is_empty(),
            "chart projection preserves the empty inverted-range contract"
        );
        Ok(())
    })
    .await
}

fn pool_chart_record(
    snapshot_at_unix_secs: i64,
    utilization: Option<f64>,
) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        snapshot_at_unix_secs,
        window: SubscriptionQuotaWindow::FiveHour,
        utilization,
        weighted_utilization_sum: utilization.unwrap_or_default(),
        capacity_ratio_sum: 1.0,
        eligible_upstreams: 1,
        contributing_upstreams: i64::from(utilization.is_some()),
        stale_upstreams: 0,
        missing_observation_upstreams: i64::from(utilization.is_none()),
        missing_metadata_upstreams: 0,
        header_contributing_upstreams: i64::from(utilization.is_some()),
        api_contributing_upstreams: 0,
        max_observed_at_unix_millis: utilization.map(|_| snapshot_at_unix_secs * 1_000),
        computed_at_unix_millis: snapshot_at_unix_secs * 1_000,
        policy_version: 1,
    }
}
