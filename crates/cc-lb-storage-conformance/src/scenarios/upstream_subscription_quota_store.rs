use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionQuotaStore,
{
    append_then_list_latest_roundtrip(Arc::clone(&backend)).await?;
    same_millis_appends_with_different_sample_ids_dont_collide(Arc::clone(&backend)).await?;
    header_and_api_sources_coexist_in_latest(Arc::clone(&backend)).await?;
    latest_is_monotonic_in_millis(Arc::clone(&backend)).await?;
    series_returns_buckets_with_correct_bounds(Arc::clone(&backend)).await?;
    series_source_merge_merged_collapses_both_sources(Arc::clone(&backend)).await?;
    series_source_merge_header_filters_api(Arc::clone(&backend)).await?;
    series_max_points_per_series_downsamples(Arc::clone(&backend)).await?;
    process_start_marker_persists_with_sample_kind(Arc::clone(&backend)).await?;
    empty_upstream_ids_returns_empty(Arc::clone(&backend)).await?;
    series_filters_observed_at_window(backend).await?;
    Ok(())
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamSubscriptionQuotaStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(append_then_list_latest_roundtrip, |storage| async move {
    let upstream = upstream_id(1);
    let record = observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.25);
    storage.put_subscription_quota(&record).await?;
    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    ensure!(
        latest == [record],
        "latest should round-trip inserted record"
    );
    Ok(())
});

scenario!(
    same_millis_appends_with_different_sample_ids_dont_collide,
    |storage| async move {
        let upstream = upstream_id(2);
        let first = observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1);
        let second = observation(upstream, 100, 2, SubscriptionQuotaSource::Header, 0.2);
        storage
            .put_subscription_quota_batch(&[first.clone(), second.clone()])
            .await?;
        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                0,
                200,
                60,
                10,
                SubscriptionQuotaSourceMerge::Header,
            ))
            .await?;
        ensure!(series.len() == 1, "expected one header series");
        ensure!(
            series[0].buckets[0].sample_count == 2,
            "same-millis samples should both persist"
        );
        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest == [second],
            ">= latest upsert should let later same-millis write win"
        );
        Ok(())
    }
);

scenario!(
    header_and_api_sources_coexist_in_latest,
    |storage| async move {
        let upstream = upstream_id(3);
        storage
            .put_subscription_quota_batch(&[
                observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1),
                observation(upstream, 110, 2, SubscriptionQuotaSource::Api, 0.2),
            ])
            .await?;
        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest.len() == 2,
            "header and api latest rows should coexist"
        );
        Ok(())
    }
);

scenario!(latest_is_monotonic_in_millis, |storage| async move {
    let upstream = upstream_id(4);
    let newer = observation(upstream, 200, 1, SubscriptionQuotaSource::Header, 0.9);
    let older = observation(upstream, 100, 2, SubscriptionQuotaSource::Header, 0.1);
    storage.put_subscription_quota(&newer).await?;
    storage.put_subscription_quota(&older).await?;
    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    ensure!(latest == [newer], "older append must not replace latest");
    let series = storage
        .list_subscription_quota_series(series_query(
            upstream,
            0,
            300,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ))
        .await?;
    ensure!(
        series[0].buckets[0].sample_count == 2,
        "observations should contain both records"
    );
    Ok(())
});

scenario!(
    series_returns_buckets_with_correct_bounds,
    |storage| async move {
        let upstream = upstream_id(5);
        storage
            .put_subscription_quota_batch(&[
                observation(upstream, 0, 1, SubscriptionQuotaSource::Header, 0.1),
                observation(upstream, 30_000, 2, SubscriptionQuotaSource::Header, 0.3),
                observation(upstream, 60_000, 3, SubscriptionQuotaSource::Header, 0.6),
            ])
            .await?;
        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                0,
                60_000,
                60,
                10,
                SubscriptionQuotaSourceMerge::Header,
            ))
            .await?;
        ensure!(series.len() == 1, "expected one series");
        ensure!(series[0].buckets.len() == 2, "expected two buckets");
        ensure!(
            series[0].buckets[0].bucket_start_unix_secs == 0,
            "first bucket starts at 0"
        );
        ensure!(
            series[0].buckets[0].sample_count == 2,
            "first bucket has t=0 and t=30s"
        );
        ensure!(
            series[0].buckets[1].bucket_start_unix_secs == 60,
            "second bucket starts at 60s"
        );
        Ok(())
    }
);

scenario!(
    series_source_merge_merged_collapses_both_sources,
    |storage| async move {
        let upstream = upstream_id(6);
        storage
            .put_subscription_quota_batch(&[
                observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1),
                observation(upstream, 100, 2, SubscriptionQuotaSource::Api, 0.2),
            ])
            .await?;
        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                0,
                200,
                60,
                10,
                SubscriptionQuotaSourceMerge::Merged,
            ))
            .await?;
        ensure!(series.len() == 1, "merged query should return one series");
        ensure!(
            series[0].source == SubscriptionQuotaSourceMerge::Merged,
            "series source should be merged"
        );
        ensure!(
            series[0].buckets[0].sources_seen.len() == 2,
            "merged bucket sees both sources"
        );
        Ok(())
    }
);

scenario!(
    series_source_merge_header_filters_api,
    |storage| async move {
        let upstream = upstream_id(7);
        storage
            .put_subscription_quota_batch(&[
                observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1),
                observation(upstream, 100, 2, SubscriptionQuotaSource::Api, 0.8),
            ])
            .await?;
        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                0,
                200,
                60,
                10,
                SubscriptionQuotaSourceMerge::Header,
            ))
            .await?;
        ensure!(series.len() == 1, "header query should return one series");
        ensure!(
            series[0].buckets[0].sample_count == 1,
            "api sample should be filtered"
        );
        ensure!(
            series[0].buckets[0].sources_seen == [SubscriptionQuotaSource::Header],
            "only header source seen"
        );
        Ok(())
    }
);

scenario!(
    series_max_points_per_series_downsamples,
    |storage| async move {
        let upstream = upstream_id(8);
        let records = (0..100)
            .map(|idx| {
                observation(
                    upstream,
                    idx * 1_000,
                    idx + 1,
                    SubscriptionQuotaSource::Header,
                    0.5,
                )
            })
            .collect::<Vec<_>>();
        storage.put_subscription_quota_batch(&records).await?;
        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                0,
                99_000,
                1,
                10,
                SubscriptionQuotaSourceMerge::Header,
            ))
            .await?;
        ensure!(
            series[0].buckets.len() <= 10,
            "series should downsample to cap"
        );
        Ok(())
    }
);

scenario!(
    process_start_marker_persists_with_sample_kind,
    |storage| async move {
        let upstream = upstream_id(11);
        let mut marker = observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.0);
        marker.sample_kind = SubscriptionQuotaSampleKind::ProcessStart;
        storage.put_subscription_quota(&marker).await?;
        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest[0].sample_kind == SubscriptionQuotaSampleKind::ProcessStart,
            "marker kind should persist"
        );
        let sample = observation(upstream, 200, 2, SubscriptionQuotaSource::Header, 0.5);
        storage.put_subscription_quota(&sample).await?;
        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        ensure!(latest == [sample], "newer real sample should become latest");
        Ok(())
    }
);

scenario!(empty_upstream_ids_returns_empty, |storage| async move {
    ensure!(
        storage
            .list_latest_subscription_quota_for_upstreams(&[])
            .await?
            .is_empty(),
        "empty latest input should return empty"
    );
    let mut query = series_query(
        upstream_id(12),
        0,
        100,
        60,
        10,
        SubscriptionQuotaSourceMerge::Merged,
    );
    query.upstream_ids.clear();
    ensure!(
        storage
            .list_subscription_quota_series(query)
            .await?
            .is_empty(),
        "empty series input should return empty"
    );
    Ok(())
});

scenario!(series_filters_observed_at_window, |storage| async move {
    let upstream = upstream_id(13);
    storage
        .put_subscription_quota_batch(&[
            observation(upstream, 50, 1, SubscriptionQuotaSource::Header, 0.1),
            observation(upstream, 100, 2, SubscriptionQuotaSource::Header, 0.2),
            observation(upstream, 200, 3, SubscriptionQuotaSource::Header, 0.3),
            observation(upstream, 300, 4, SubscriptionQuotaSource::Header, 0.4),
        ])
        .await?;
    let series = storage
        .list_subscription_quota_series(series_query(
            upstream,
            100,
            200,
            1,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ))
        .await?;
    ensure!(series.len() == 1, "expected one bounded series");
    let count = series[0]
        .buckets
        .iter()
        .map(|bucket| bucket.sample_count)
        .sum::<u32>();
    ensure!(
        count == 2,
        "only observations within inclusive bounds should remain"
    );
    Ok(())
});

fn observation(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u64,
    source: SubscriptionQuotaSource,
    utilization: f64,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::from_u128(sample_id as u128),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(1_800_000_000 + observed_at_unix_millis / 1000),
        surpassed_threshold: Some(0.75),
        representative_claim: Some(format!("claim-{sample_id}")),
        fallback_percentage: Some(0.5),
        fallback_available: Some(true),
        overage_in_use: Some(true),
        overage_period_monthly_utilization: Some(0.2),
        upgrade_paths: Some(vec!["max_5x".to_owned(), "team_growth".to_owned()]),
        disabled_reason: None,
        extra_usage_enabled: Some(true),
        extra_usage_monthly_limit: Some(10.0),
        extra_usage_used_credits: Some(utilization),
        ingested_at_unix_millis: observed_at_unix_millis + 1,
    }
}

fn series_query(
    upstream_id: Uuid,
    since_unix_millis: u64,
    until_unix_millis: u64,
    bucket_secs: u64,
    max_points_per_series: u32,
    source_merge: SubscriptionQuotaSourceMerge,
) -> SubscriptionQuotaSeriesQuery {
    SubscriptionQuotaSeriesQuery {
        upstream_ids: vec![upstream_id],
        windows: vec![SubscriptionQuotaWindow::FiveHour],
        sources: vec![
            SubscriptionQuotaSource::Header,
            SubscriptionQuotaSource::Api,
        ],
        since_unix_millis,
        until_unix_millis,
        bucket_secs,
        max_points_per_series,
        source_merge,
    }
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x1000 + value)
}
