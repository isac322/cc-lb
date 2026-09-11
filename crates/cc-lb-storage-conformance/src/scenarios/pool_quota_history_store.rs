use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    PoolQuotaChartPointRecord, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord,
    PoolQuotaSnapshotSummaryRecord, SubscriptionQuotaWindow,
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

pub async fn summary_latest_and_range<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PoolQuotaHistoryStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let older = summary_record(1_800_001_000, 10);
        let newer = summary_record(1_800_001_060, 20);
        let other_window = PoolQuotaSnapshotRecord {
            window: SubscriptionQuotaWindow::SevenDayFable,
            ..summary_record(1_800_001_030, 30)
        };
        storage
            .record_pool_quota_snapshots(&[older.clone(), newer.clone(), other_window.clone()])
            .await?;

        let latest = storage
            .list_latest_pool_quota_snapshot_summaries(&[SubscriptionQuotaWindow::FiveHour])
            .await?;
        ensure!(
            latest == vec![PoolQuotaSnapshotSummaryRecord::from(&newer)],
            "latest pool quota summary must preserve every field from the newest stored row"
        );

        let range = storage
            .list_pool_quota_snapshot_summaries_in_range(
                &[SubscriptionQuotaWindow::FiveHour],
                older.snapshot_at_unix_secs,
                newer.snapshot_at_unix_secs,
            )
            .await?;
        ensure!(
            range
                == vec![
                    PoolQuotaSnapshotSummaryRecord::from(&older),
                    PoolQuotaSnapshotSummaryRecord::from(&newer),
                ],
            "pool quota summary range must be inclusive and ordered by timestamp ascending"
        );
        ensure!(
            storage
                .list_latest_pool_quota_snapshot_summaries(&[])
                .await?
                .is_empty(),
            "an empty window list must return no latest pool quota summaries"
        );
        ensure!(
            storage
                .list_pool_quota_snapshot_summaries_in_range(
                    &[SubscriptionQuotaWindow::FiveHour],
                    newer.snapshot_at_unix_secs,
                    older.snapshot_at_unix_secs,
                )
                .await?
                .is_empty(),
            "an inverted range must return no pool quota summaries"
        );
        ensure!(
            storage
                .list_pool_quota_snapshot_summaries_in_range(
                    &[SubscriptionQuotaWindow::FiveHour],
                    newer.snapshot_at_unix_secs + 1,
                    newer.snapshot_at_unix_secs + 10,
                )
                .await?
                .is_empty(),
            "a range without stored pool quota summaries must return empty"
        );
        Ok(())
    })
    .await
}

fn summary_record(snapshot_at_unix_secs: i64, seed: i64) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        snapshot_at_unix_secs,
        window: SubscriptionQuotaWindow::FiveHour,
        utilization: Some(seed as f64 / 100.0),
        weighted_utilization_sum: seed as f64 + 0.25,
        capacity_ratio_sum: seed as f64 + 0.5,
        eligible_upstreams: seed + 1,
        contributing_upstreams: seed + 2,
        stale_upstreams: seed + 3,
        missing_observation_upstreams: seed + 4,
        missing_metadata_upstreams: seed + 5,
        header_contributing_upstreams: seed + 6,
        api_contributing_upstreams: seed + 7,
        max_observed_at_unix_millis: Some(snapshot_at_unix_secs * 1_000 + seed),
        computed_at_unix_millis: snapshot_at_unix_secs * 1_000 + seed + 1,
        policy_version: seed as i32,
    }
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
