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
            provider: cc_lb_storage_api::Provider::Anthropic,
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
            .list_latest_pool_quota_snapshots(
                cc_lb_storage_api::Provider::Anthropic,
                &[SubscriptionQuotaWindow::SevenDayFable],
            )
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
                cc_lb_storage_api::Provider::Anthropic,
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
                cc_lb_storage_api::Provider::Anthropic,
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
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -121,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.10),
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -120,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.20),
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -61,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.40),
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -60,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: None,
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -1,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: None,
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: 0,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.20),
                    },
                ],
            "unbucketed chart projection preserves every stored point"
        );

        let bucketed_chart = storage
            .list_pool_quota_chart_points_in_range(
                cc_lb_storage_api::Provider::Anthropic,
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
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -180,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.10),
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -120,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.40),
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: -60,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: None,
                    },
                    PoolQuotaChartPointRecord {
                        provider: cc_lb_storage_api::Provider::Anthropic,
                        snapshot_at_unix_secs: 0,
                        window: SubscriptionQuotaWindow::FiveHour,
                        utilization: Some(0.20),
                    },
                ],
            "bucketed chart projection uses Euclidean epoch buckets and SQL MAX null semantics"
        );

        let empty_chart = storage
            .list_pool_quota_chart_points_in_range(
                cc_lb_storage_api::Provider::Anthropic,
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
        provider: cc_lb_storage_api::Provider::Anthropic,
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

pub async fn provider_isolation_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PoolQuotaHistoryStore,
{
    with_conformance_fixture(backend, |storage| async move {
        for (provider, windows) in provider_windows() {
            let records = windows
                .iter()
                .map(|&window| provider_record(provider, window, 10, Some(0.25)))
                .collect::<Vec<_>>();
            storage.record_pool_quota_snapshots(&records).await?;
        }
        let replacement = provider_record(
            cc_lb_storage_api::Provider::Openai,
            SubscriptionQuotaWindow::FiveHour,
            10,
            Some(0.75),
        );
        storage
            .record_pool_quota_snapshots(std::slice::from_ref(&replacement))
            .await?;
        storage
            .record_pool_quota_snapshots(std::slice::from_ref(&replacement))
            .await?;
        for (provider, windows) in provider_windows() {
            for window in windows {
                let expected = provider_record(
                    provider,
                    window,
                    10,
                    Some(
                        if provider == cc_lb_storage_api::Provider::Openai
                            && window == SubscriptionQuotaWindow::FiveHour
                        {
                            0.75
                        } else {
                            0.25
                        },
                    ),
                );
                let latest = storage
                    .list_latest_pool_quota_snapshots(provider, &[window])
                    .await?;
                ensure!(
                    latest == vec![expected.clone()],
                    "provider latest isolation"
                );
                let raw = storage
                    .list_pool_quota_snapshots_in_range(provider, &[window], 10, 10)
                    .await?;
                ensure!(
                    raw == latest,
                    "provider timestamp/window upsert is idempotent"
                );
                let summary = storage
                    .list_latest_pool_quota_snapshot_summaries(provider, &[window])
                    .await?;
                ensure!(
                    summary
                        == vec![cc_lb_storage_api::PoolQuotaSnapshotSummaryRecord::from(
                            &expected
                        )],
                    "latest summaries preserve provider and all metadata"
                );
                let range = storage
                    .list_pool_quota_snapshot_summaries_in_range(provider, &[window], 10, 10)
                    .await?;
                ensure!(range == summary, "summary range provider isolation");
                let chart = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], 10, 10, None)
                    .await?;
                ensure!(
                    chart
                        == vec![PoolQuotaChartPointRecord {
                            provider,
                            window,
                            snapshot_at_unix_secs: 10,
                            utilization: expected.utilization,
                        }],
                    "chart provider isolation"
                );
            }
        }
        Ok(())
    })
    .await
}

pub async fn provider_prune_anchors<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PoolQuotaHistoryStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let mut series_count = 0;
        for (provider, windows) in provider_windows() {
            let anchor_at = match provider {
                cc_lb_storage_api::Provider::Anthropic => 20,
                cc_lb_storage_api::Provider::Openai => 18,
                cc_lb_storage_api::Provider::Gemini => 19,
            };
            for window in windows {
                series_count += 1;
                storage
                    .record_pool_quota_snapshots(&[
                        provider_record(provider, window, 10, Some(0.1)),
                        provider_record(provider, window, anchor_at, None),
                        provider_record(provider, window, 30, Some(0.3)),
                    ])
                    .await?;
            }
        }
        ensure!(storage.prune_pool_quota_history_before(25, 0).await? == 0);
        ensure!(storage.prune_pool_quota_history_before(25, 1).await? == 1);
        ensure!(
            storage.prune_pool_quota_history_before(25, 100).await? == series_count - 1,
            "each provider/window retains its own anchor within the global batch budget"
        );
        ensure!(storage.prune_pool_quota_history_before(25, 100).await? == 0);
        for (provider, windows) in provider_windows() {
            for window in windows {
                let raw = storage
                    .list_pool_quota_snapshots_in_range(provider, &[window], 0, 40)
                    .await?;
                ensure!(raw.len() == 2 && raw[0].utilization.is_none());
                ensure!(raw[1].snapshot_at_unix_secs == 30);
                ensure!(
                    storage
                        .list_pool_quota_snapshots_in_range(provider, &[window], 25, 29)
                        .await?
                        .is_empty(),
                    "raw range does not synthesize anchors"
                );
                let summaries = storage
                    .list_pool_quota_snapshot_summaries_in_range(provider, &[window], 25, 29)
                    .await?;
                ensure!(
                    summaries.len() == 1
                        && summaries[0].provider == provider
                        && summaries[0].snapshot_at_unix_secs == 25
                        && summaries[0].utilization.is_none(),
                    "idle summaries retain provider-scoped stale anchors"
                );
                let chart = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], 25, 35, None)
                    .await?;
                ensure!(
                    chart.len() == 2
                        && chart[0].snapshot_at_unix_secs == 25
                        && chart[0].utilization.is_none()
                        && chart[1].snapshot_at_unix_secs == 30
                        && chart[1].utilization == Some(0.3),
                    "recovery does not erase the retained stale interval"
                );
            }
        }
        Ok(())
    })
    .await
}

pub async fn provider_chart_gaps_and_peaks<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PoolQuotaHistoryStore,
{
    with_conformance_fixture(backend, |storage| async move {
        for (provider, windows) in provider_windows() {
            for window in windows {
                let records = [
                    (-61, Some(0.1)),
                    (-59, Some(0.2)),
                    (-1, None),
                    (0, Some(0.3)),
                    (1, Some(0.8)),
                    (59, Some(0.4)),
                    (60, None),
                    (119, Some(0.6)),
                    (120, Some(0.7)),
                ]
                .map(|(at, value)| provider_record(provider, window, at, value));
                storage.record_pool_quota_snapshots(&records).await?;
                let full = storage
                    .list_pool_quota_snapshot_summaries_in_range(provider, &[window], -60, 120)
                    .await?;
                let exact = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], -60, 120, None)
                    .await?;
                ensure!(full.len() == records.len() && exact.len() == full.len());
                ensure!(
                    full.iter().zip(&exact).all(|(summary, chart)| {
                        summary.provider == chart.provider
                            && summary.window == chart.window
                            && summary.snapshot_at_unix_secs == chart.snapshot_at_unix_secs
                            && summary.utilization == chart.utilization
                    }),
                    "full and compact unbucketed projections agree on left anchors and gaps"
                );
                let buckets = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], -60, 120, Some(60))
                    .await?;
                let anthropic = provider == cc_lb_storage_api::Provider::Anthropic;
                let expected = [
                    (-60, if anthropic { Some(0.2) } else { None }),
                    (0, Some(0.8)),
                    (60, if anthropic { Some(0.6) } else { None }),
                    (120, Some(0.7)),
                ];
                ensure!(
                    buckets == expected.map(|(at, value)| PoolQuotaChartPointRecord {
                        provider,
                        window,
                        snapshot_at_unix_secs: at,
                        utilization: value,
                    }),
                    "non-Anthropic any-null buckets remain gaps; healthy peaks and Anthropic MAX remain unchanged"
                );
                let left_null = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], 61, 119, Some(60))
                    .await?;
                ensure!(
                    left_null.len() == 1
                        && left_null[0].utilization == if anthropic { Some(0.6) } else { None },
                    "a synthetic null left anchor participates in bucket gap preservation"
                );
                let null_only = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], 60, 60, Some(60))
                    .await?;
                ensure!(
                    null_only.len() == 1 && null_only[0].utilization.is_none(),
                    "all-null buckets remain unavailable for every provider"
                );
                ensure!(
                    storage
                        .list_pool_quota_snapshot_summaries_in_range(provider, &[window], -120, -62)
                        .await?
                        .is_empty()
                        && storage
                            .list_pool_quota_chart_points_in_range(provider, &[window], -120, -62, None)
                            .await?
                            .is_empty(),
                    "history before the first snapshot remains unknown"
                );
                let boundary = storage
                    .list_pool_quota_chart_points_in_range(provider, &[window], 119, 119, None)
                    .await?;
                ensure!(
                    boundary.len() == 1 && boundary[0].utilization == Some(0.6),
                    "an exact boundary suppresses the stale left anchor"
                );
                ensure!(
                    storage
                        .list_pool_quota_chart_points_in_range(provider, &[window], 0, 120, Some(0))
                        .await
                        .is_err(),
                    "nonpositive chart buckets remain invalid"
                );
            }
        }
        Ok(())
    })
    .await
}

fn provider_windows() -> [(cc_lb_storage_api::Provider, Vec<SubscriptionQuotaWindow>); 3] {
    use SubscriptionQuotaWindow::{
        FiveHour, GeminiFiveHour, GeminiSevenDay, SevenDay, ThirdPartyFiveHour, ThirdPartySevenDay,
    };
    use cc_lb_storage_api::Provider;
    [
        (Provider::Anthropic, vec![FiveHour, SevenDay]),
        (Provider::Openai, vec![FiveHour, SevenDay]),
        (
            Provider::Gemini,
            vec![
                GeminiFiveHour,
                GeminiSevenDay,
                ThirdPartyFiveHour,
                ThirdPartySevenDay,
            ],
        ),
    ]
}

fn provider_record(
    provider: cc_lb_storage_api::Provider,
    window: SubscriptionQuotaWindow,
    at: i64,
    utilization: Option<f64>,
) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        provider,
        window,
        stale_upstreams: i64::from(utilization.is_none()),
        missing_observation_upstreams: 0,
        ..pool_chart_record(at, utilization)
    }
}
