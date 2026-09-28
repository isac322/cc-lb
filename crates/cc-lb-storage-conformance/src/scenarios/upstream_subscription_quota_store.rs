use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSeriesQuery, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore, UsageRollupStore,
    UsageTokenInterval, UsageTokenIntervalStore, UsageTokenIntervalSum,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionQuotaStore
        + UpstreamSubscriptionQuotaAggregateStore
        + UsageTokenIntervalStore
        + RequestEventStore
        + UsageRollupStore,
{
    append_then_list_latest_roundtrip(Arc::clone(&backend)).await?;
    same_millis_appends_with_different_sample_ids_dont_collide(Arc::clone(&backend)).await?;
    header_and_api_sources_coexist_in_latest(Arc::clone(&backend)).await?;
    latest_is_monotonic_in_millis(Arc::clone(&backend)).await?;
    series_returns_buckets_with_correct_bounds(Arc::clone(&backend)).await?;
    series_source_merge_merged_collapses_both_sources(Arc::clone(&backend)).await?;
    series_source_merge_header_filters_api(Arc::clone(&backend)).await?;
    series_max_points_per_series_downsamples(Arc::clone(&backend)).await?;
    absent_replaces_latest_without_erasing_history(Arc::clone(&backend)).await?;
    absent_is_idempotent(Arc::clone(&backend)).await?;
    empty_upstream_ids_returns_empty(Arc::clone(&backend)).await?;
    series_filters_observed_at_window(Arc::clone(&backend)).await?;
    checkpoint_writer_latest_freshness(Arc::clone(&backend)).await?;
    checkpoint_writer_decrease(Arc::clone(&backend)).await?;
    checkpoint_series_anchor_merge(Arc::clone(&backend)).await?;
    checkpoint_history(Arc::clone(&backend)).await?;
    aggregate_store(backend).await?;
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

macro_rules! aggregate_scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamSubscriptionQuotaStore
                + UpstreamSubscriptionQuotaAggregateStore
                + UsageTokenIntervalStore
                + RequestEventStore
                + UsageRollupStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(append_then_list_latest_roundtrip, |storage| async move {
    let upstream = upstream_id(1);
    let record = observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.25);
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&record))
        .await?;
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
            .record_subscription_quota_samples(&[first.clone(), second.clone()])
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
            .record_subscription_quota_samples(&[
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
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&newer))
        .await?;
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&older))
        .await?;
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
            .record_subscription_quota_samples(&[
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
            .record_subscription_quota_samples(&[
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
            .record_subscription_quota_samples(&[
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
        storage.record_subscription_quota_samples(&records).await?;
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
    absent_replaces_latest_without_erasing_history,
    |storage| async move {
        let upstream = upstream_id(12);
        let sample = observation(upstream, 100, 1, SubscriptionQuotaSource::Api, 0.25);
        storage
            .record_subscription_quota_samples(std::slice::from_ref(&sample))
            .await?;

        let mut absent = observation(upstream, 200, 2, SubscriptionQuotaSource::Api, 0.0);
        absent.sample_kind = SubscriptionQuotaSampleKind::Absent;
        absent.utilization = None;
        absent.status = None;
        absent.resets_at_unix_secs = None;
        storage
            .record_subscription_quota_samples(std::slice::from_ref(&absent))
            .await?;

        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        ensure!(latest == [absent], "absent marker should become latest");

        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                0,
                300,
                60,
                10,
                SubscriptionQuotaSourceMerge::Api,
            ))
            .await?;
        ensure!(series.len() == 1, "historical sample series should remain");
        let sample_count = series[0]
            .buckets
            .iter()
            .map(|bucket| bucket.sample_count)
            .sum::<u32>();
        ensure!(
            sample_count == 1,
            "absent marker must not become a utilization checkpoint"
        );
        ensure!(
            series[0]
                .buckets
                .last()
                .and_then(|bucket| bucket.utilization_last)
                == Some(0.25),
            "last historical utilization should remain the real sample"
        );
        Ok(())
    }
);
scenario!(absent_is_idempotent, |storage| async move {
    let upstream = upstream_id(13);
    let mut absent = observation(upstream, 200, 2, SubscriptionQuotaSource::Api, 0.0);
    absent.sample_kind = SubscriptionQuotaSampleKind::Absent;
    absent.utilization = None;
    absent.status = None;
    absent.resets_at_unix_secs = None;

    storage
        .record_subscription_quota_samples(std::slice::from_ref(&absent))
        .await?;
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&absent))
        .await?;

    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    ensure!(
        latest == [absent],
        "repeated absent marker should remain one latest row"
    );

    let series = storage
        .list_subscription_quota_series(series_query(
            upstream,
            0,
            300,
            60,
            10,
            SubscriptionQuotaSourceMerge::Api,
        ))
        .await?;
    ensure!(
        series.is_empty(),
        "repeated absent marker must not create utilization checkpoints"
    );
    Ok(())
});

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
    const HOUR_MILLIS: u64 = 60 * 60 * 1_000;

    let upstream = upstream_id(13);
    let since_unix_millis = HOUR_MILLIS;
    let until_unix_millis = since_unix_millis + HOUR_MILLIS;
    storage
        .record_subscription_quota_samples(&[
            observation(
                upstream,
                since_unix_millis - 30_000,
                1,
                SubscriptionQuotaSource::Header,
                0.1,
            ),
            observation(
                upstream,
                since_unix_millis,
                2,
                SubscriptionQuotaSource::Header,
                0.2,
            ),
            observation(
                upstream,
                since_unix_millis + 90_000,
                3,
                SubscriptionQuotaSource::Api,
                0.8,
            ),
            observation(
                upstream,
                since_unix_millis + 120_000,
                4,
                SubscriptionQuotaSource::Header,
                0.4,
            ),
            observation(
                upstream,
                until_unix_millis,
                5,
                SubscriptionQuotaSource::Api,
                0.6,
            ),
            observation(
                upstream,
                until_unix_millis + 1_000,
                6,
                SubscriptionQuotaSource::Header,
                0.9,
            ),
        ])
        .await?;
    let series = storage
        .list_subscription_quota_series(series_query(
            upstream,
            since_unix_millis,
            until_unix_millis,
            60,
            100,
            SubscriptionQuotaSourceMerge::Merged,
        ))
        .await?;
    ensure!(series.len() == 1, "expected one bounded merged series");
    let buckets = &series[0].buckets;
    ensure!(
        buckets.len() == 61
            && buckets.iter().all(|bucket| bucket.observed)
            && buckets[0].bucket_start_unix_secs == since_unix_millis / 1_000
            && buckets[60].bucket_start_unix_secs == until_unix_millis / 1_000,
        "the inclusive one-hour window must contain exactly 61 observed requested-range buckets"
    );
    ensure!(
        buckets
            .iter()
            .map(|bucket| bucket.sample_count)
            .sum::<u32>()
            == 4,
        "only the four inclusive in-range checkpoints should count as samples"
    );
    ensure!(
        buckets[0].sample_count == 1
            && buckets[0].utilization_min == Some(0.2)
            && buckets[0].utilization_max == Some(0.2)
            && buckets[0].utilization_last == Some(0.2)
            && buckets[0].observed_at_unix_millis_last == Some(since_unix_millis)
            && buckets[0].sources_seen == [SubscriptionQuotaSource::Header],
        "the exact-since header checkpoint must replace the pre-range anchor in the first bucket"
    );
    ensure!(
        buckets[1].sample_count == 1
            && buckets[1].utilization_min == Some(0.2)
            && buckets[1].utilization_max == Some(0.8)
            && buckets[1].utilization_last == Some(0.8)
            && buckets[1].observed_at_unix_millis_last == Some(since_unix_millis + 90_000)
            && buckets[1].sources_seen
                == [
                    SubscriptionQuotaSource::Header,
                    SubscriptionQuotaSource::Api
                ],
        "the first api checkpoint must merge with carried header state in its bucket"
    );
    ensure!(
        buckets[2].sample_count == 1
            && buckets[2].utilization_min == Some(0.4)
            && buckets[2].utilization_max == Some(0.8)
            && buckets[2].utilization_last == Some(0.4)
            && buckets[2].observed_at_unix_millis_last == Some(since_unix_millis + 120_000)
            && buckets[2].sources_seen
                == [
                    SubscriptionQuotaSource::Header,
                    SubscriptionQuotaSource::Api
                ],
        "the later header checkpoint must become the exact last observation while api state carries"
    );
    ensure!(
        buckets[59].sample_count == 0
            && buckets[59].utilization_last == Some(0.4)
            && buckets[59].observed_at_unix_millis_last == Some(since_unix_millis + 120_000),
        "dense gap buckets must carry the latest per-source state without synthetic samples"
    );
    ensure!(
        buckets[60].sample_count == 1
            && buckets[60].utilization_min == Some(0.4)
            && buckets[60].utilization_max == Some(0.6)
            && buckets[60].utilization_last == Some(0.6)
            && buckets[60].observed_at_unix_millis_last == Some(until_unix_millis)
            && buckets[60].sources_seen
                == [
                    SubscriptionQuotaSource::Header,
                    SubscriptionQuotaSource::Api
                ],
        "the exact-until api checkpoint must be included and the post-range header checkpoint excluded"
    );
    Ok(())
});

scenario!(checkpoint_writer_latest_freshness, |storage| async move {
    let upstream = upstream_id(24);
    let first = observation(upstream, 0, 1, SubscriptionQuotaSource::Header, 0.31);
    let mut after_heartbeat =
        observation(upstream, 31_000, 2, SubscriptionQuotaSource::Header, 0.31);
    after_heartbeat.resets_at_unix_secs = first.resets_at_unix_secs;
    let mut freshest = observation(upstream, 35_000, 3, SubscriptionQuotaSource::Header, 0.31);
    freshest.resets_at_unix_secs = first.resets_at_unix_secs;
    storage
        .record_subscription_quota_samples(&[first.clone(), after_heartbeat, freshest.clone()])
        .await?;

    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    ensure!(
        latest == [freshest],
        "latest sidecar should advance on unchanged observations"
    );

    let checkpoints = storage
        .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
        .await?;
    ensure!(
        checkpoints.len() == 1,
        "unchanged quota state should create one checkpoint"
    );
    ensure!(
        checkpoints[0].changed_at_unix_millis == first.observed_at_unix_millis,
        "duplicate semantic checkpoint should leave first checkpoint latest"
    );
    Ok(())
});

scenario!(checkpoint_writer_decrease, |storage| async move {
    let upstream = upstream_id(25);
    let first = observation(upstream, 0, 1, SubscriptionQuotaSource::Header, 0.31);
    let decreased = observation(upstream, 1_000, 2, SubscriptionQuotaSource::Header, 0.30);
    storage
        .record_subscription_quota_samples(&[first, decreased.clone()])
        .await?;

    let checkpoints = storage
        .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
        .await?;
    ensure!(
        checkpoints.len() == 1,
        "utilization decrease should insert a checkpoint"
    );
    ensure!(
        checkpoints[0].changed_at_unix_millis == decreased.observed_at_unix_millis,
        "latest checkpoint should be the decrease"
    );
    ensure!(
        checkpoints[0].utilization == decreased.utilization,
        "decrease payload should be persisted exactly"
    );
    Ok(())
});

scenario!(checkpoint_series_anchor_merge, |storage| async move {
    let upstream = upstream_id(26);
    let header_anchor = checkpoint(&observation(
        upstream,
        30_000,
        1,
        SubscriptionQuotaSource::Header,
        0.10,
    ));
    let header_first_change = checkpoint(&observation(
        upstream,
        75_000,
        2,
        SubscriptionQuotaSource::Header,
        0.20,
    ));
    let header_second_change = checkpoint(&observation(
        upstream,
        90_000,
        3,
        SubscriptionQuotaSource::Header,
        0.40,
    ));
    let header_late_change = checkpoint(&observation(
        upstream,
        180_000,
        4,
        SubscriptionQuotaSource::Header,
        0.60,
    ));
    let api_anchor = checkpoint(&observation(
        upstream,
        45_000,
        5,
        SubscriptionQuotaSource::Api,
        0.80,
    ));
    let api_change = checkpoint(&observation(
        upstream,
        120_000,
        6,
        SubscriptionQuotaSource::Api,
        0.70,
    ));
    storage
        .put_subscription_quota_checkpoints(&[
            header_anchor,
            header_first_change,
            header_second_change,
            header_late_change,
            api_anchor,
            api_change,
        ])
        .await?;

    let header_series = storage
        .list_subscription_quota_series(series_query(
            upstream,
            60_000,
            240_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ))
        .await?;
    ensure!(header_series.len() == 1, "expected one header series");
    let header_buckets = &header_series[0].buckets;
    assert_bucket_starts(header_buckets, &[60, 120, 180, 240])?;
    ensure!(
        header_buckets[0].utilization_last == Some(0.40),
        "first requested bucket should apply its in-range header checkpoints over the anchor"
    );
    ensure!(
        header_buckets[0].sample_count == 2,
        "same-minute in-range header changes should remain distinct inside the bucket"
    );
    ensure!(
        header_buckets[0].observed_at_unix_millis_last == Some(90_000),
        "same-minute bucket should preserve the exact timestamp of the last checkpoint"
    );
    ensure!(
        header_buckets[1].sample_count == 0 && header_buckets[1].utilization_last == Some(0.40),
        "gap bucket should carry forward the latest header checkpoint without adding a change count"
    );
    ensure!(
        header_buckets[1].observed_at_unix_millis_last == Some(90_000),
        "carry-forward bucket should keep the exact source checkpoint timestamp"
    );
    ensure!(
        header_buckets
            .iter()
            .all(|bucket| bucket.sources_seen == [SubscriptionQuotaSource::Header]),
        "header series should not contain api provenance"
    );

    let api_series = storage
        .list_subscription_quota_series(series_query(
            upstream,
            60_000,
            180_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Api,
        ))
        .await?;
    ensure!(api_series.len() == 1, "expected one api series");
    let api_buckets = &api_series[0].buckets;
    assert_bucket_starts(api_buckets, &[60, 120, 180])?;
    ensure!(
        api_buckets[0].sample_count == 0
            && api_buckets[0].utilization_last == Some(0.80)
            && api_buckets[1].utilization_last == Some(0.70)
            && api_buckets[2].utilization_last == Some(0.70),
        "api anchor should seed the requested range and carry forward independently of header checkpoints"
    );
    ensure!(
        api_buckets
            .iter()
            .all(|bucket| bucket.sources_seen == [SubscriptionQuotaSource::Api]),
        "api series should not contain header provenance"
    );

    let merged_upstream = upstream_id(27);
    storage
        .put_subscription_quota_checkpoints(&[
            checkpoint(&observation(
                merged_upstream,
                60_000,
                7,
                SubscriptionQuotaSource::Header,
                0.30,
            )),
            checkpoint(&observation(
                merged_upstream,
                90_000,
                8,
                SubscriptionQuotaSource::Api,
                0.90,
            )),
        ])
        .await?;
    let merged_series = storage
        .list_subscription_quota_series(series_query(
            merged_upstream,
            60_000,
            119_999,
            60,
            10,
            SubscriptionQuotaSourceMerge::Merged,
        ))
        .await?;
    ensure!(merged_series.len() == 1, "expected one merged series");
    let merged_bucket = &merged_series[0].buckets[0];
    ensure!(
        merged_bucket.sample_count == 2
            && merged_bucket.utilization_min == Some(0.30)
            && merged_bucket.utilization_avg == Some(0.60)
            && merged_bucket.utilization_max == Some(0.90)
            && merged_bucket.utilization_last == Some(0.90)
            && merged_bucket.observed_at_unix_millis_last == Some(90_000)
            && merged_bucket.sources_seen
                == [
                    SubscriptionQuotaSource::Header,
                    SubscriptionQuotaSource::Api
                ],
        "merged checkpoint bucket should match the existing read-time merge policy on equivalent data"
    );

    let no_anchor_upstream = upstream_id(28);
    storage
        .put_subscription_quota_checkpoints(&[checkpoint(&observation(
            no_anchor_upstream,
            180_000,
            9,
            SubscriptionQuotaSource::Header,
            0.50,
        ))])
        .await?;
    let no_anchor_series = storage
        .list_subscription_quota_series(series_query(
            no_anchor_upstream,
            60_000,
            240_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ))
        .await?;
    ensure!(no_anchor_series.len() == 1, "expected one no-anchor series");
    let no_anchor_buckets = &no_anchor_series[0].buckets;
    assert_bucket_starts(no_anchor_buckets, &[180, 240])?;
    ensure!(
        no_anchor_buckets[0].utilization_last == Some(0.50),
        "series without a left anchor should start at the first real checkpoint"
    );
    Ok(())
});

scenario!(
    checkpoint_series_old_anchor_is_bounded_and_dense,
    |storage| async move {
        const DAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
        const HOUR_MILLIS: u64 = 60 * 60 * 1_000;

        let upstream = upstream_id(32);
        let since_unix_millis = 180 * DAY_MILLIS;
        let until_unix_millis = since_unix_millis + HOUR_MILLIS;
        let anchor = checkpoint(&observation(
            upstream,
            1_000,
            1,
            SubscriptionQuotaSource::Header,
            0.42,
        ));
        storage
            .put_subscription_quota_checkpoints(std::slice::from_ref(&anchor))
            .await?;

        let series = storage
            .list_subscription_quota_series(series_query(
                upstream,
                since_unix_millis,
                until_unix_millis,
                60,
                100,
                SubscriptionQuotaSourceMerge::Header,
            ))
            .await?;
        ensure!(series.len() == 1, "expected one anchor-only series");
        let buckets = &series[0].buckets;
        ensure!(
            buckets.len() == 61,
            "an inclusive one-hour range at 60-second resolution must contain exactly 61 buckets"
        );
        ensure!(
            buckets.first().map(|bucket| bucket.bucket_start_unix_secs)
                == Some(since_unix_millis / 1_000)
                && buckets.last().map(|bucket| bucket.bucket_start_unix_secs)
                    == Some(until_unix_millis / 1_000),
            "an old anchor must not materialize buckets outside the requested range"
        );
        ensure!(
            buckets.iter().all(|bucket| {
                bucket.observed
                    && bucket.sample_count == 0
                    && bucket.utilization_last == Some(0.42)
                    && bucket.observed_at_unix_millis_last == Some(1_000)
                    && bucket.sources_seen == [SubscriptionQuotaSource::Header]
            }),
            "anchor-only buckets must preserve the exact carried state without counting synthetic samples"
        );
        Ok(())
    }
);

pub async fn checkpoint_history<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionQuotaStore,
{
    checkpoint_series_old_anchor_is_bounded_and_dense(Arc::clone(&backend)).await?;
    sample_writer_persists_seven_day_fable_window(Arc::clone(&backend)).await?;
    checkpoint_history_suppresses_duplicate_semantic_state(Arc::clone(&backend)).await?;
    checkpoint_history_returns_left_anchor_and_in_range_rows(Arc::clone(&backend)).await?;
    checkpoint_history_keeps_sources_separate(Arc::clone(&backend)).await?;
    checkpoint_history_orders_same_millis_ties_deterministically(backend).await?;
    Ok(())
}

pub async fn aggregate_store<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionQuotaStore
        + UpstreamSubscriptionQuotaAggregateStore
        + UsageTokenIntervalStore
        + RequestEventStore
        + UsageRollupStore,
{
    slim_checkpoints_preserve_left_anchor_sources_and_same_millis_ties(Arc::clone(&backend))
        .await?;
    provider_lots_keep_header_and_api_streams_separate(Arc::clone(&backend)).await?;
    provider_lots_old_anchor_preserves_boundary_reset(Arc::clone(&backend)).await?;
    interval_sums_match_legacy_inclusive_boundary_bytes(backend).await?;
    Ok(())
}

aggregate_scenario!(
    slim_checkpoints_preserve_left_anchor_sources_and_same_millis_ties,
    |storage| async move {
        let upstream = upstream_id(30);
        let checkpoints = [
            checkpoint(&observation(
                upstream,
                1_000,
                1,
                SubscriptionQuotaSource::Header,
                0.1,
            )),
            checkpoint(&observation(
                upstream,
                1_000,
                2,
                SubscriptionQuotaSource::Header,
                0.2,
            )),
            checkpoint(&observation(
                upstream,
                2_000,
                3,
                SubscriptionQuotaSource::Header,
                0.3,
            )),
            checkpoint(&observation(
                upstream,
                2_000,
                4,
                SubscriptionQuotaSource::Header,
                0.4,
            )),
            checkpoint(&observation(
                upstream,
                1_000,
                5,
                SubscriptionQuotaSource::Api,
                0.5,
            )),
            checkpoint(&observation(
                upstream,
                2_000,
                6,
                SubscriptionQuotaSource::Api,
                0.6,
            )),
        ];
        storage
            .put_subscription_quota_checkpoints(&checkpoints)
            .await?;

        let query = checkpoint_query(upstream, 2_000, 2_000);
        let mut expected = storage
            .list_subscription_quota_checkpoint_ranges(query.clone())
            .await?
            .into_iter()
            .flat_map(|range| {
                range
                    .left_anchor
                    .into_iter()
                    .chain(range.checkpoints)
                    .map(slim_checkpoint)
            })
            .collect::<Vec<_>>();
        let mut actual = storage
            .list_subscription_quota_slim_checkpoints(query)
            .await?;
        // The two backends physically order rows by the source column's wire
        // string (e.g. "api" < "header" alphabetically), not by the
        // `SubscriptionQuotaSource` enum's declaration order used by
        // `list_subscription_quota_checkpoint_ranges`'s `BTreeMap` grouping.
        // Sort both projections onto the same key so this scenario asserts
        // per-key content parity without depending on either ordering choice.
        let sort_key = |row: &cc_lb_storage_api::SubscriptionQuotaSlimCheckpoint| {
            (
                row.source.as_str().to_owned(),
                row.changed_at_unix_millis,
                row.sample_id,
            )
        };
        expected.sort_by_key(sort_key);
        actual.sort_by_key(sort_key);

        ensure!(
            actual == expected,
            "slim checkpoints must preserve the legacy left-anchor and inclusive-range projection: actual={actual:?} expected={expected:?}"
        );

        let header_sample_ids = actual
            .iter()
            .filter(|row| row.source == SubscriptionQuotaSource::Header)
            .map(|row| row.sample_id)
            .collect::<Vec<_>>();
        ensure!(
            header_sample_ids == [Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4)],
            "header slim checkpoints must select the greatest left-anchor tie and sort in-range ties by sample_id"
        );
        let api_sample_ids = actual
            .iter()
            .filter(|row| row.source == SubscriptionQuotaSource::Api)
            .map(|row| row.sample_id)
            .collect::<Vec<_>>();
        ensure!(
            api_sample_ids == [Uuid::from_u128(5), Uuid::from_u128(6)],
            "api slim checkpoints must not merge with the header stream"
        );
        Ok(())
    }
);

aggregate_scenario!(
    provider_lots_keep_header_and_api_streams_separate,
    |storage| async move {
        let upstream = upstream_id(31);
        let mut header = observation(upstream, 60_000, 1, SubscriptionQuotaSource::Header, 0.2);
        header.resets_at_unix_secs = Some(36_000);
        let mut api = observation(upstream, 60_000, 2, SubscriptionQuotaSource::Api, 0.8);
        api.resets_at_unix_secs = Some(54_000);
        storage
            .put_subscription_quota_checkpoints(&[checkpoint(&header), checkpoint(&api)])
            .await?;

        for (source_merge, expected_source, expected_utilization, expected_reset) in [
            (
                SubscriptionQuotaSourceMerge::Header,
                SubscriptionQuotaSourceMerge::Header,
                0.2,
                36_000,
            ),
            (
                SubscriptionQuotaSourceMerge::Api,
                SubscriptionQuotaSourceMerge::Api,
                0.8,
                54_000,
            ),
        ] {
            let lots = storage
                .list_subscription_quota_provider_lots(SubscriptionQuotaProviderLotQuery {
                    upstream_ids: vec![upstream],
                    windows: vec![SubscriptionQuotaWindow::FiveHour],
                    sources: vec![
                        SubscriptionQuotaSource::Header,
                        SubscriptionQuotaSource::Api,
                    ],
                    since_unix_millis: 0,
                    until_unix_millis: 120_000,
                    source_merge,
                    evaluation_unix_secs: 120,
                })
                .await?;
            ensure!(
                lots.len() == 1,
                "each source must yield one independent lot"
            );
            let lot = &lots[0];
            ensure!(
                lot.source == expected_source
                    && lot.utilization == expected_utilization
                    && lot.provider_reset_unix_secs == Some(expected_reset)
                    && lot.provider_start_unix_secs == Some(expected_reset - 18_000),
                "provider lots must retain the selected source's reset cycle and utilization"
            );
        }
        Ok(())
    }
);

aggregate_scenario!(
    provider_lots_old_anchor_preserves_boundary_reset,
    |storage| async move {
        const DAY_SECS: u64 = 24 * 60 * 60;
        const FIVE_HOURS_SECS: u64 = 5 * 60 * 60;

        let upstream = upstream_id(33);
        let since_unix_secs = 180 * DAY_SECS;
        let since_unix_millis = since_unix_secs * 1_000;
        let old_reset_unix_secs = since_unix_secs + FIVE_HOURS_SECS;
        let new_reset_unix_secs = old_reset_unix_secs + FIVE_HOURS_SECS;
        let mut anchor = observation(upstream, 1_000, 1, SubscriptionQuotaSource::Header, 0.9);
        anchor.resets_at_unix_secs = Some(old_reset_unix_secs);
        let mut reset = observation(
            upstream,
            since_unix_millis,
            2,
            SubscriptionQuotaSource::Header,
            0.1,
        );
        reset.resets_at_unix_secs = Some(new_reset_unix_secs);
        storage
            .put_subscription_quota_checkpoints(&[checkpoint(&anchor), checkpoint(&reset)])
            .await?;

        let lots = storage
            .list_subscription_quota_provider_lots(SubscriptionQuotaProviderLotQuery {
                upstream_ids: vec![upstream],
                windows: vec![SubscriptionQuotaWindow::FiveHour],
                sources: vec![SubscriptionQuotaSource::Header],
                since_unix_millis,
                until_unix_millis: since_unix_millis + 60_000,
                source_merge: SubscriptionQuotaSourceMerge::Header,
                evaluation_unix_secs: since_unix_secs + 60,
            })
            .await?;
        ensure!(
            lots.len() == 2,
            "an exact-boundary reset must close the anchored provider lot and open the new lot"
        );
        ensure!(
            lots[0].provider_start_unix_secs == Some(since_unix_secs)
                && lots[0].provider_reset_unix_secs == Some(old_reset_unix_secs)
                && lots[0].observed_at_unix_millis == 1_000
                && lots[0].evaluation_unix_secs == since_unix_secs - 60
                && lots[0].utilization == 0.9,
            "the old anchor must seed the pre-range provider cycle without shifting its values"
        );
        ensure!(
            lots[1].provider_start_unix_secs == Some(old_reset_unix_secs)
                && lots[1].provider_reset_unix_secs == Some(new_reset_unix_secs)
                && lots[1].observed_at_unix_millis == since_unix_millis
                && lots[1].evaluation_unix_secs == since_unix_secs + 60
                && lots[1].utilization == 0.1,
            "the boundary checkpoint must remain the first observation of the new provider cycle"
        );
        Ok(())
    }
);

pub async fn interval_sums_match_legacy_inclusive_boundary_bytes<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionQuotaStore
        + UpstreamSubscriptionQuotaAggregateStore
        + UsageTokenIntervalStore
        + RequestEventStore
        + UsageRollupStore,
{
    let wait_backend = Arc::clone(&backend);
    with_conformance_fixture(backend, move |storage| async move {
        let upstream = upstream_id(32);
        let provider_start = 1_799_985_000;
        let cc_start = 1_800_000_000;
        let provider_sample_end = 1_800_003_000;
        let now_unix_secs = 1_800_003_600;
        let buckets = [
            (provider_start, 11_u64),
            (cc_start, 17_u64),
            (provider_sample_end, 13_u64),
            (now_unix_secs, 19_u64),
        ];
        for (timestamp, tokens) in buckets {
            storage
                .append_request_event(&usage_event(
                    timestamp,
                    &format!("quota-boundary-{timestamp}"),
                    upstream,
                    tokens,
                ))
                .await?;
        }
        wait_backend
            .wait_for_events_visible_for_rollup(&storage)
            .await?;
        storage.rollup_usage_once().await?;

        let intervals = [
            UsageTokenInterval {
                interval_id: 1,
                upstream_id: upstream,
                start_unix_secs: provider_start,
                end_unix_secs: provider_sample_end,
            },
            UsageTokenInterval {
                interval_id: 2,
                upstream_id: upstream,
                start_unix_secs: provider_start,
                end_unix_secs: cc_start,
            },
            UsageTokenInterval {
                interval_id: 3,
                upstream_id: upstream,
                start_unix_secs: cc_start,
                end_unix_secs: now_unix_secs,
            },
            UsageTokenInterval {
                interval_id: 4,
                upstream_id: upstream,
                start_unix_secs: provider_sample_end,
                end_unix_secs: now_unix_secs,
            },
        ];
        let actual = storage.sum_usage_tokens_for_intervals(&intervals).await?;
        let expected = intervals
            .iter()
            .map(|interval| UsageTokenIntervalSum {
                interval_id: interval.interval_id,
                tokens: legacy_inclusive_tokens(
                    &buckets,
                    interval.start_unix_secs,
                    interval.end_unix_secs,
                ),
            })
            .collect::<Vec<_>>();

        ensure!(
            actual == expected,
            "interval sums must include provider-start, provider-sample-end, cc-start, and now buckets"
        );
        ensure!(
            serde_json::to_vec(&actual)? == serde_json::to_vec(&expected)?,
            "interval-sum JSON must be byte-identical to the legacy inclusive admin token result"
        );
        Ok(())
    })
    .await
}

scenario!(
    sample_writer_persists_seven_day_fable_window,
    |storage| async move {
        let upstream = upstream_id(29);
        let mut sample = observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.4);
        sample.window = SubscriptionQuotaWindow::SevenDayFable;
        let checkpoint = checkpoint(&sample);

        storage
            .record_subscription_quota_samples(std::slice::from_ref(&sample))
            .await?;

        let latest_samples = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest_samples == [sample],
            "latest sample should return the persisted Fable record"
        );

        let latest_checkpoints = storage
            .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest_checkpoints == [checkpoint.clone()],
            "latest checkpoint should return the persisted Fable record"
        );

        let ranges = storage
            .list_subscription_quota_checkpoint_ranges(SubscriptionQuotaCheckpointRangeQuery {
                upstream_ids: vec![upstream],
                windows: vec![SubscriptionQuotaWindow::SevenDayFable],
                sources: vec![SubscriptionQuotaSource::Header],
                since_unix_millis: 0,
                until_unix_millis: 200,
            })
            .await?;
        ensure!(ranges.len() == 1, "expected one Fable checkpoint range");
        ensure!(
            ranges[0].window == SubscriptionQuotaWindow::SevenDayFable
                && ranges[0].checkpoints == [checkpoint],
            "Fable checkpoint range should preserve its window and payload"
        );
        Ok(())
    }
);

scenario!(
    checkpoint_history_suppresses_duplicate_semantic_state,
    |storage| async move {
        let upstream = upstream_id(20);
        let first = checkpoint(&observation(
            upstream,
            100,
            1,
            SubscriptionQuotaSource::Header,
            0.4,
        ));
        let mut duplicate = first.clone();
        duplicate.changed_at_unix_millis = 200;
        duplicate.sample_id = Uuid::from_u128(2);
        duplicate.representative_claim = Some("changed-evidence".to_owned());
        duplicate.ingested_at_unix_millis = 201;

        let first_inserted = storage
            .put_subscription_quota_checkpoints(std::slice::from_ref(&first))
            .await?;
        ensure!(first_inserted == 1, "first semantic state should insert");

        let duplicate_inserted = storage
            .put_subscription_quota_checkpoints(std::slice::from_ref(&duplicate))
            .await?;
        ensure!(
            duplicate_inserted == 0,
            "persisted duplicate semantic state should be skipped on a later call"
        );

        let latest = storage
            .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest == [first],
            "latest checkpoint should remain first row"
        );
        Ok(())
    }
);

scenario!(
    checkpoint_history_returns_left_anchor_and_in_range_rows,
    |storage| async move {
        let upstream = upstream_id(21);
        let checkpoints = [
            checkpoint(&observation(
                upstream,
                100,
                1,
                SubscriptionQuotaSource::Header,
                0.1,
            )),
            checkpoint(&observation(
                upstream,
                200,
                2,
                SubscriptionQuotaSource::Header,
                0.2,
            )),
            checkpoint(&observation(
                upstream,
                300,
                3,
                SubscriptionQuotaSource::Header,
                0.3,
            )),
        ];
        storage
            .put_subscription_quota_checkpoints(&checkpoints)
            .await?;

        let ranges = storage
            .list_subscription_quota_checkpoint_ranges(checkpoint_query(upstream, 150, 250))
            .await?;
        ensure!(ranges.len() == 1, "expected one checkpoint range");
        ensure!(
            ranges[0].left_anchor.as_ref() == Some(&checkpoints[0]),
            "last checkpoint before since should be returned as anchor"
        );
        ensure!(
            ranges[0].checkpoints == [checkpoints[1].clone()],
            "range should include only checkpoints inside inclusive bounds"
        );

        let no_anchor = storage
            .list_subscription_quota_checkpoint_ranges(checkpoint_query(upstream, 50, 150))
            .await?;
        ensure!(
            no_anchor.len() == 1,
            "in-range checkpoint should create range"
        );
        ensure!(
            no_anchor[0].left_anchor.is_none(),
            "range before first checkpoint must not invent an anchor"
        );
        ensure!(
            no_anchor[0].checkpoints == [checkpoints[0].clone()],
            "first checkpoint should be returned without synthetic zero state"
        );

        let empty_before_first = storage
            .list_subscription_quota_checkpoint_ranges(checkpoint_query(upstream, 0, 50))
            .await?;
        ensure!(
            empty_before_first.is_empty(),
            "range before first checkpoint must not invent zero or null state"
        );
        Ok(())
    }
);

scenario!(
    checkpoint_history_keeps_sources_separate,
    |storage| async move {
        let upstream = upstream_id(22);
        let header = checkpoint(&observation(
            upstream,
            100,
            1,
            SubscriptionQuotaSource::Header,
            0.2,
        ));
        let api = checkpoint(&observation(
            upstream,
            100,
            2,
            SubscriptionQuotaSource::Api,
            0.8,
        ));
        storage
            .put_subscription_quota_checkpoints(&[header.clone(), api.clone()])
            .await?;

        let ranges = storage
            .list_subscription_quota_checkpoint_ranges(checkpoint_query(upstream, 0, 200))
            .await?;
        ensure!(ranges.len() == 2, "header and api should remain separate");
        ensure!(
            ranges[0].source == SubscriptionQuotaSource::Header,
            "header range should sort before api by source key"
        );
        ensure!(
            ranges[1].source == SubscriptionQuotaSource::Api,
            "api range should remain physical api stream"
        );
        ensure!(
            ranges[0].checkpoints == [header] && ranges[1].checkpoints == [api],
            "source streams should not collapse"
        );
        Ok(())
    }
);

scenario!(
    checkpoint_history_orders_same_millis_ties_deterministically,
    |storage| async move {
        let upstream = upstream_id(23);
        let first_sample = checkpoint(&observation(
            upstream,
            500,
            1,
            SubscriptionQuotaSource::Header,
            0.1,
        ));
        let second_sample = checkpoint(&observation(
            upstream,
            500,
            2,
            SubscriptionQuotaSource::Header,
            0.2,
        ));
        storage
            .put_subscription_quota_checkpoints(&[second_sample.clone(), first_sample.clone()])
            .await?;

        let ranges = storage
            .list_subscription_quota_checkpoint_ranges(checkpoint_query(upstream, 0, 1_000))
            .await?;
        ensure!(ranges.len() == 1, "expected one same-millis range");
        ensure!(
            ranges[0].checkpoints == [first_sample.clone(), second_sample.clone()],
            "same-millis checkpoints should sort by sample_id ascending"
        );
        let latest = storage
            .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
            .await?;
        ensure!(
            latest == [second_sample],
            "latest tie should be deterministic by greatest sample_id"
        );
        Ok(())
    }
);

fn observation(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u64,
    source: SubscriptionQuotaSource,
    utilization: f64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
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

fn checkpoint(record: &SubscriptionQuotaSample) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(record)
}

fn slim_checkpoint(
    record: SubscriptionQuotaCheckpointRecord,
) -> cc_lb_storage_api::SubscriptionQuotaSlimCheckpoint {
    cc_lb_storage_api::SubscriptionQuotaSlimCheckpoint {
        upstream_id: record.upstream_id,
        window: record.window,
        source: record.source,
        changed_at_unix_millis: record.changed_at_unix_millis,
        sample_id: record.sample_id,
        utilization: record.utilization,
        status: record.status,
        resets_at_unix_secs: record.resets_at_unix_secs,
    }
}

fn legacy_inclusive_tokens(
    buckets: &[(u64, u64)],
    start_unix_secs: u64,
    end_unix_secs: u64,
) -> u64 {
    buckets
        .iter()
        .filter(|(bucket_start_unix_secs, _)| {
            *bucket_start_unix_secs >= start_unix_secs && *bucket_start_unix_secs <= end_unix_secs
        })
        .map(|(_, tokens)| *tokens)
        .sum()
}

fn usage_event(
    timestamp: u64,
    request_id: &str,
    upstream_id: Uuid,
    input_tokens: u64,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(timestamp.saturating_mul(1_000)),
        request_id: request_id.to_owned(),
        event_id: Some(request_id.to_owned()),
        principal_id: Some("quota-boundary-principal".to_owned()),
        key_id: Some("quota-boundary-key".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some("quota-boundary-upstream".to_owned()),
        model: Some("quota-boundary-model".to_owned()),
        status: 200,
        input_tokens: Some(input_tokens),
        output_tokens: Some(0),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        cost_usd_micros: Some(0),
        duration_ms: 1,
        ..Default::default()
    }
}

fn checkpoint_query(
    upstream_id: Uuid,
    since_unix_millis: u64,
    until_unix_millis: u64,
) -> SubscriptionQuotaCheckpointRangeQuery {
    SubscriptionQuotaCheckpointRangeQuery {
        upstream_ids: vec![upstream_id],
        windows: vec![SubscriptionQuotaWindow::FiveHour],
        sources: vec![
            SubscriptionQuotaSource::Header,
            SubscriptionQuotaSource::Api,
        ],
        since_unix_millis,
        until_unix_millis,
    }
}

fn assert_bucket_starts(
    buckets: &[cc_lb_storage_api::SubscriptionQuotaBucket],
    expected: &[u64],
) -> Result<()> {
    let actual = buckets
        .iter()
        .map(|bucket| bucket.bucket_start_unix_secs)
        .collect::<Vec<_>>();
    ensure!(
        actual == expected,
        "bucket starts {actual:?} should equal {expected:?}"
    );
    Ok(())
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x1000 + value)
}
