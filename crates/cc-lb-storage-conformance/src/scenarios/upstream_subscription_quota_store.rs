use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore, UsageRollupStore,
    UsageTokenInterval, UsageTokenIntervalStore, UsageTokenIntervalSum,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

use self::checkpoint_rows::{checkpoint_rows, sort_checkpoint_records};

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
    absent_replaces_latest_without_erasing_history(Arc::clone(&backend)).await?;
    absent_is_idempotent(Arc::clone(&backend)).await?;
    empty_upstream_ids_returns_empty(Arc::clone(&backend)).await?;
    slim_checkpoints_filter_observed_at_window(Arc::clone(&backend)).await?;
    checkpoint_writer_latest_freshness(Arc::clone(&backend)).await?;
    checkpoint_writer_decrease(Arc::clone(&backend)).await?;
    slim_checkpoints_select_per_source_left_anchors(Arc::clone(&backend)).await?;
    checkpoint_history(Arc::clone(&backend)).await?;
    aggregate_store(backend).await?;
    Ok(())
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamSubscriptionQuotaStore + UpstreamSubscriptionQuotaAggregateStore,
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
        let checkpoints = checkpoint_rows(&*storage, &[upstream]).await?;
        ensure!(
            checkpoints == [checkpoint(&first), checkpoint(&second)],
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
    ensure!(
        latest == [newer.clone()],
        "older append must not replace latest"
    );
    let checkpoints = checkpoint_rows(&*storage, &[upstream]).await?;
    ensure!(
        checkpoints == [checkpoint(&older), checkpoint(&newer)],
        "observations should contain both records"
    );
    Ok(())
});

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

        let checkpoints = checkpoint_rows(&*storage, &[upstream]).await?;
        ensure!(
            checkpoints.len() == 1,
            "absent marker must not become a utilization checkpoint"
        );
        ensure!(
            checkpoints == [checkpoint(&sample)],
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

    ensure!(
        checkpoint_rows(&*storage, &[upstream]).await?.is_empty(),
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
    let mut query = checkpoint_query(upstream_id(12), 0, 100);
    query.upstream_ids.clear();
    ensure!(
        storage
            .list_subscription_quota_slim_checkpoints(query)
            .await?
            .is_empty(),
        "empty slim checkpoint input should return empty"
    );
    Ok(())
});

scenario!(
    slim_checkpoints_filter_observed_at_window,
    |storage| async move {
        const HOUR_MILLIS: u64 = 60 * 60 * 1_000;

        let upstream = upstream_id(13);
        let since_unix_millis = HOUR_MILLIS;
        let until_unix_millis = since_unix_millis + HOUR_MILLIS;
        let header_anchor = observation(
            upstream,
            since_unix_millis - 30_000,
            1,
            SubscriptionQuotaSource::Header,
            0.1,
        );
        let header_at_since = observation(
            upstream,
            since_unix_millis,
            2,
            SubscriptionQuotaSource::Header,
            0.2,
        );
        let api_first = observation(
            upstream,
            since_unix_millis + 90_000,
            3,
            SubscriptionQuotaSource::Api,
            0.8,
        );
        let header_later = observation(
            upstream,
            since_unix_millis + 120_000,
            4,
            SubscriptionQuotaSource::Header,
            0.4,
        );
        let api_at_until = observation(
            upstream,
            until_unix_millis,
            5,
            SubscriptionQuotaSource::Api,
            0.6,
        );
        let header_after_until = observation(
            upstream,
            until_unix_millis + 1_000,
            6,
            SubscriptionQuotaSource::Header,
            0.9,
        );
        storage
            .record_subscription_quota_samples(&[
                header_anchor.clone(),
                header_at_since.clone(),
                api_first.clone(),
                header_later.clone(),
                api_at_until.clone(),
                header_after_until,
            ])
            .await?;

        let mut actual = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(
                upstream,
                since_unix_millis,
                until_unix_millis,
            ))
            .await?;
        actual.sort_by_key(slim_sort_key);
        let mut expected = [
            &header_anchor,
            &header_at_since,
            &header_later,
            &api_first,
            &api_at_until,
        ]
        .into_iter()
        .map(|sample| slim_checkpoint(checkpoint(sample)))
        .collect::<Vec<_>>();
        expected.sort_by_key(slim_sort_key);
        ensure!(
            actual == expected,
            "slim checkpoints must return the pre-range header anchor, both exact-boundary checkpoints, and exclude the post-range checkpoint: actual={actual:?} expected={expected:?}"
        );

        let header_only = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                sources: vec![SubscriptionQuotaSource::Header],
                ..checkpoint_query(upstream, since_unix_millis, until_unix_millis)
            })
            .await?;
        ensure!(
            header_only
                == [
                    slim_checkpoint(checkpoint(&header_anchor)),
                    slim_checkpoint(checkpoint(&header_at_since)),
                    slim_checkpoint(checkpoint(&header_later)),
                ],
            "a header-only source filter must exclude api checkpoints"
        );
        Ok(())
    }
);

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

    let checkpoints = checkpoint_rows(&*storage, &[upstream]).await?;
    ensure!(
        checkpoints.len() == 1,
        "unchanged quota state should create one checkpoint"
    );
    ensure!(
        checkpoints[0] == checkpoint(&first),
        "duplicate semantic checkpoint should leave first checkpoint latest"
    );
    Ok(())
});

scenario!(checkpoint_writer_decrease, |storage| async move {
    let upstream = upstream_id(25);
    let first = observation(upstream, 0, 1, SubscriptionQuotaSource::Header, 0.31);
    let decreased = observation(upstream, 1_000, 2, SubscriptionQuotaSource::Header, 0.30);
    storage
        .record_subscription_quota_samples(&[first.clone(), decreased.clone()])
        .await?;

    let checkpoints = checkpoint_rows(&*storage, &[upstream]).await?;
    ensure!(
        checkpoints.len() == 2,
        "utilization decrease should insert a checkpoint"
    );
    ensure!(
        checkpoints[1].changed_at_unix_millis == decreased.observed_at_unix_millis,
        "latest checkpoint should be the decrease"
    );
    ensure!(
        checkpoints == [checkpoint(&first), checkpoint(&decreased)],
        "decrease payload should be persisted exactly"
    );
    Ok(())
});

scenario!(
    slim_checkpoints_select_per_source_left_anchors,
    |storage| async move {
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
                header_anchor.clone(),
                header_first_change.clone(),
                header_second_change.clone(),
                header_late_change.clone(),
                api_anchor.clone(),
                api_change.clone(),
            ])
            .await?;

        let header_rows = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                sources: vec![SubscriptionQuotaSource::Header],
                ..checkpoint_query(upstream, 60_000, 240_000)
            })
            .await?;
        ensure!(
            header_rows
                == [
                    slim_checkpoint(header_anchor.clone()),
                    slim_checkpoint(header_first_change.clone()),
                    slim_checkpoint(header_second_change.clone()),
                    slim_checkpoint(header_late_change.clone()),
                ],
            "header rows must start with the header left anchor and contain only header provenance"
        );

        let api_rows = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                sources: vec![SubscriptionQuotaSource::Api],
                ..checkpoint_query(upstream, 60_000, 180_000)
            })
            .await?;
        ensure!(
            api_rows
                == [
                    slim_checkpoint(api_anchor.clone()),
                    slim_checkpoint(api_change.clone()),
                ],
            "api anchor must be selected independently of header checkpoints"
        );

        let mut both_rows = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 60_000, 240_000))
            .await?;
        both_rows.sort_by_key(slim_sort_key);
        let mut expected_both = [
            header_anchor,
            header_first_change,
            header_second_change,
            header_late_change,
            api_anchor,
            api_change,
        ]
        .into_iter()
        .map(slim_checkpoint)
        .collect::<Vec<_>>();
        expected_both.sort_by_key(slim_sort_key);
        ensure!(
            both_rows == expected_both,
            "a two-source query must return one left anchor per source plus in-range rows"
        );

        let merged_upstream = upstream_id(27);
        let merged_header = checkpoint(&observation(
            merged_upstream,
            60_000,
            7,
            SubscriptionQuotaSource::Header,
            0.30,
        ));
        let merged_api = checkpoint(&observation(
            merged_upstream,
            90_000,
            8,
            SubscriptionQuotaSource::Api,
            0.90,
        ));
        storage
            .put_subscription_quota_checkpoints(&[merged_header.clone(), merged_api.clone()])
            .await?;
        let mut merged_rows = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(
                merged_upstream,
                60_000,
                119_999,
            ))
            .await?;
        merged_rows.sort_by_key(slim_sort_key);
        let mut expected_merged = vec![slim_checkpoint(merged_header), slim_checkpoint(merged_api)];
        expected_merged.sort_by_key(slim_sort_key);
        ensure!(
            merged_rows == expected_merged,
            "in-range rows from both sources must be returned for read-time merging"
        );

        let no_anchor_upstream = upstream_id(28);
        let no_anchor_first = checkpoint(&observation(
            no_anchor_upstream,
            180_000,
            9,
            SubscriptionQuotaSource::Header,
            0.50,
        ));
        storage
            .put_subscription_quota_checkpoints(std::slice::from_ref(&no_anchor_first))
            .await?;
        let no_anchor_rows = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                sources: vec![SubscriptionQuotaSource::Header],
                ..checkpoint_query(no_anchor_upstream, 60_000, 240_000)
            })
            .await?;
        ensure!(
            no_anchor_rows == [slim_checkpoint(no_anchor_first)],
            "rows without a left anchor should start at the first real checkpoint"
        );
        Ok(())
    }
);

scenario!(
    slim_checkpoints_return_old_left_anchor,
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

        let rows = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                sources: vec![SubscriptionQuotaSource::Header],
                ..checkpoint_query(upstream, since_unix_millis, until_unix_millis)
            })
            .await?;
        ensure!(
            rows == [slim_checkpoint(anchor)],
            "an arbitrarily old left anchor must be returned with its exact carried state"
        );
        Ok(())
    }
);

pub async fn checkpoint_history<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionQuotaStore + UpstreamSubscriptionQuotaAggregateStore,
{
    slim_checkpoints_return_old_left_anchor(Arc::clone(&backend)).await?;
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
        // Per key: the greatest-sample_id checkpoint before `since` as the
        // left anchor, then every checkpoint inside the inclusive range.
        let mut expected = checkpoints[1..]
            .iter()
            .cloned()
            .map(slim_checkpoint)
            .collect::<Vec<_>>();
        let mut actual = storage
            .list_subscription_quota_slim_checkpoints(query)
            .await?;
        // The two backends physically order rows by the source column's wire
        // string (e.g. "api" < "header" alphabetically). Sort both projections
        // onto the same key so this scenario asserts per-key content without
        // depending on the cross-source ordering.
        expected.sort_by_key(slim_sort_key);
        actual.sort_by_key(slim_sort_key);

        ensure!(
            actual == expected,
            "slim checkpoints must return the left anchor and inclusive-range rows: actual={actual:?} expected={expected:?}"
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

        let persisted_checkpoints = checkpoint_rows(&*storage, &[upstream]).await?;
        ensure!(
            persisted_checkpoints == [checkpoint.clone()],
            "latest checkpoint should return the persisted Fable record"
        );

        let rows = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                upstream_ids: vec![upstream],
                windows: vec![SubscriptionQuotaWindow::SevenDayFable],
                sources: vec![SubscriptionQuotaSource::Header],
                since_unix_millis: 0,
                until_unix_millis: 200,
            })
            .await?;
        ensure!(rows.len() == 1, "expected one Fable checkpoint row");
        ensure!(
            rows[0].window == SubscriptionQuotaWindow::SevenDayFable
                && rows == [slim_checkpoint(checkpoint)],
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

        let persisted = checkpoint_rows(&*storage, &[upstream]).await?;
        ensure!(
            persisted == [first],
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

        ensure!(
            checkpoint_rows(&*storage, &[upstream]).await? == checkpoints,
            "every checkpoint should persist its full payload exactly"
        );

        let rows = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 150, 250))
            .await?;
        ensure!(
            rows.len() == 2,
            "expected the left anchor plus one in-range checkpoint"
        );
        ensure!(
            rows[0] == slim_checkpoint(checkpoints[0].clone()),
            "last checkpoint before since should be returned as anchor"
        );
        ensure!(
            rows[1] == slim_checkpoint(checkpoints[1].clone()),
            "range should include only checkpoints inside inclusive bounds"
        );

        let no_anchor = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 50, 150))
            .await?;
        ensure!(
            no_anchor == [slim_checkpoint(checkpoints[0].clone())],
            "range before first checkpoint must not invent an anchor; the first checkpoint is returned without synthetic zero state"
        );

        let empty_before_first = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 0, 50))
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

        let mut rows = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 0, 200))
            .await?;
        rows.sort_by_key(slim_sort_key);
        ensure!(rows.len() == 2, "header and api should remain separate");
        let mut expected_rows = vec![
            slim_checkpoint(header.clone()),
            slim_checkpoint(api.clone()),
        ];
        expected_rows.sort_by_key(slim_sort_key);
        ensure!(rows == expected_rows, "source streams should not collapse");
        let mut expected_records = vec![header, api];
        sort_checkpoint_records(&mut expected_records);
        ensure!(
            checkpoint_rows(&*storage, &[upstream]).await? == expected_records,
            "api rows should remain the physical api stream with their full payload"
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

        let rows = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 0, 1_000))
            .await?;
        ensure!(
            rows == [
                slim_checkpoint(first_sample.clone()),
                slim_checkpoint(second_sample.clone()),
            ],
            "same-millis checkpoints should sort by sample_id ascending"
        );
        ensure!(
            checkpoint_rows(&*storage, &[upstream]).await? == [first_sample, second_sample.clone()],
            "both same-millis checkpoints should persist their full payload"
        );
        let anchor = storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query(upstream, 501, 1_000))
            .await?;
        ensure!(
            anchor == [slim_checkpoint(second_sample)],
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

fn checkpoint(record: &SubscriptionQuotaSample) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(record)
}

fn slim_checkpoint(record: SubscriptionQuotaCheckpointRecord) -> SubscriptionQuotaSlimCheckpoint {
    SubscriptionQuotaSlimCheckpoint {
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

fn slim_sort_key(row: &SubscriptionQuotaSlimCheckpoint) -> (String, u64, Uuid) {
    (
        row.source.as_str().to_owned(),
        row.changed_at_unix_millis,
        row.sample_id,
    )
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x1000 + value)
}

/// Test-support reader for the full persisted checkpoint payload.
///
/// Storage only exposes slim checkpoints (no fingerprint, claim, or
/// fallback/overage/extra-usage fields), so scenarios that assert what the
/// writer persisted read the backend table directly.
mod checkpoint_rows {
    use std::any::Any;

    use anyhow::{Result, anyhow, bail};
    use cc_lb_storage_api::{
        SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
        SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    };
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    use sqlx::Row;
    use uuid::Uuid;

    /// Persisted checkpoint payload with the fingerprint kept as stored bytes
    /// so scenarios compare exactly what the backend wrote.
    #[derive(Debug, Clone, PartialEq)]
    pub(super) struct CheckpointRow {
        pub upstream_id: Uuid,
        pub window: SubscriptionQuotaWindow,
        pub source: SubscriptionQuotaSource,
        pub changed_at_unix_millis: u64,
        pub semantic_fingerprint: [u8; 32],
        pub sample_kind: SubscriptionQuotaSampleKind,
        pub sample_id: Uuid,
        pub representative_claim: Option<String>,
        pub utilization: Option<f64>,
        pub status: Option<SubscriptionQuotaStatus>,
        pub resets_at_unix_secs: Option<u64>,
        pub surpassed_threshold: Option<f64>,
        pub fallback_percentage: Option<f64>,
        pub fallback_available: Option<bool>,
        pub overage_in_use: Option<bool>,
        pub overage_period_monthly_utilization: Option<f64>,
        pub upgrade_paths: Option<Vec<String>>,
        pub disabled_reason: Option<String>,
        pub extra_usage_enabled: Option<bool>,
        pub extra_usage_monthly_limit: Option<f64>,
        pub extra_usage_used_credits: Option<f64>,
        pub ingested_at_unix_millis: u64,
    }

    impl From<&SubscriptionQuotaCheckpointRecord> for CheckpointRow {
        fn from(record: &SubscriptionQuotaCheckpointRecord) -> Self {
            Self {
                upstream_id: record.upstream_id,
                window: record.window,
                source: record.source,
                changed_at_unix_millis: record.changed_at_unix_millis,
                semantic_fingerprint: *record.semantic_fingerprint.as_bytes(),
                sample_kind: record.sample_kind,
                sample_id: record.sample_id,
                representative_claim: record.representative_claim.clone(),
                utilization: record.utilization,
                status: record.status,
                resets_at_unix_secs: record.resets_at_unix_secs,
                surpassed_threshold: record.surpassed_threshold,
                fallback_percentage: record.fallback_percentage,
                fallback_available: record.fallback_available,
                overage_in_use: record.overage_in_use,
                overage_period_monthly_utilization: record.overage_period_monthly_utilization,
                upgrade_paths: record.upgrade_paths.clone(),
                disabled_reason: record.disabled_reason.clone(),
                extra_usage_enabled: record.extra_usage_enabled,
                extra_usage_monthly_limit: record.extra_usage_monthly_limit,
                extra_usage_used_credits: record.extra_usage_used_credits,
                ingested_at_unix_millis: record.ingested_at_unix_millis,
            }
        }
    }

    impl PartialEq<SubscriptionQuotaCheckpointRecord> for CheckpointRow {
        fn eq(&self, other: &SubscriptionQuotaCheckpointRecord) -> bool {
            self == &Self::from(other)
        }
    }

    /// Decodes one checkpoint row whose uuid columns are selected as text.
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    macro_rules! checkpoint_row_from_row {
        ($row:expr) => {{
            let row = $row;
            let upstream_id: String = row.try_get("upstream_id")?;
            let window: String = row.try_get("window")?;
            let source: String = row.try_get("source")?;
            let sample_id: String = row.try_get("sample_id")?;
            let fingerprint: Vec<u8> = row.try_get("semantic_fingerprint")?;
            let sample_kind: String = row.try_get("sample_kind")?;
            let status: Option<String> = row.try_get("status")?;
            let upgrade_paths: Option<String> = row.try_get("upgrade_paths")?;
            CheckpointRow {
                upstream_id: Uuid::parse_str(&upstream_id)?,
                window: SubscriptionQuotaWindow::from_str(&window)
                    .ok_or_else(|| anyhow!("invalid checkpoint window {window}"))?,
                source: SubscriptionQuotaSource::from_str(&source)
                    .ok_or_else(|| anyhow!("invalid checkpoint source {source}"))?,
                changed_at_unix_millis: u64::try_from(
                    row.try_get::<i64, _>("changed_at_unix_millis")?,
                )?,
                semantic_fingerprint: <[u8; 32]>::try_from(fingerprint)
                    .map_err(|bytes| anyhow!("checkpoint fingerprint has {} bytes", bytes.len()))?,
                sample_kind: SubscriptionQuotaSampleKind::from_str(&sample_kind)
                    .ok_or_else(|| anyhow!("invalid checkpoint sample kind {sample_kind}"))?,
                sample_id: Uuid::parse_str(&sample_id)?,
                representative_claim: row.try_get("representative_claim")?,
                utilization: row.try_get("utilization")?,
                status: status
                    .map(|status| {
                        SubscriptionQuotaStatus::from_str(&status)
                            .ok_or_else(|| anyhow!("invalid checkpoint status {status}"))
                    })
                    .transpose()?,
                resets_at_unix_secs: row
                    .try_get::<Option<i64>, _>("resets_at_unix_secs")?
                    .map(u64::try_from)
                    .transpose()?,
                surpassed_threshold: row.try_get("surpassed_threshold")?,
                fallback_percentage: row.try_get("fallback_percentage")?,
                fallback_available: row.try_get("fallback_available")?,
                overage_in_use: row.try_get("overage_in_use")?,
                overage_period_monthly_utilization: row
                    .try_get("overage_period_monthly_utilization")?,
                upgrade_paths: upgrade_paths
                    .map(|text| serde_json::from_str::<Vec<String>>(&text))
                    .transpose()?,
                disabled_reason: row.try_get("disabled_reason")?,
                extra_usage_enabled: row.try_get("extra_usage_enabled")?,
                extra_usage_monthly_limit: row.try_get("extra_usage_monthly_limit")?,
                extra_usage_used_credits: row.try_get("extra_usage_used_credits")?,
                ingested_at_unix_millis: u64::try_from(
                    row.try_get::<i64, _>("ingested_at_unix_millis")?,
                )?,
            }
        }};
    }

    /// Returns every persisted checkpoint row for `upstream_ids`, sorted with
    /// [`sort_checkpoint_rows`].
    pub(super) async fn checkpoint_rows<S>(
        storage: &S,
        upstream_ids: &[Uuid],
    ) -> Result<Vec<CheckpointRow>>
    where
        S: Any + Send + Sync,
    {
        let storage: &(dyn Any + Send + Sync) = storage;
        #[cfg(feature = "sqlite")]
        {
            if let Some(sqlite) = storage.downcast_ref::<cc_lb_storage_sqlite::SqliteStorage>() {
                return sqlite_rows(sqlite, upstream_ids).await;
            }
        }
        #[cfg(feature = "postgres")]
        {
            if let Some(postgres) =
                storage.downcast_ref::<cc_lb_storage_postgres::PostgresStorage>()
            {
                return postgres_rows(postgres, upstream_ids).await;
            }
        }
        let _ = (storage, upstream_ids);
        bail!("checkpoint rows are not readable for this conformance backend")
    }

    /// Orders records by `(upstream_id, window, source,
    /// changed_at_unix_millis, sample_id)`.
    pub(super) fn sort_checkpoint_records(records: &mut [SubscriptionQuotaCheckpointRecord]) {
        records.sort_by_key(|record| {
            (
                record.upstream_id,
                record.window,
                record.source,
                record.changed_at_unix_millis,
                record.sample_id,
            )
        });
    }

    /// Orders persisted rows by `(upstream_id, window, source,
    /// changed_at_unix_millis, sample_id)`.
    fn sort_checkpoint_rows(rows: &mut [CheckpointRow]) {
        rows.sort_by_key(|row| {
            (
                row.upstream_id,
                row.window,
                row.source,
                row.changed_at_unix_millis,
                row.sample_id,
            )
        });
    }

    #[cfg(feature = "sqlite")]
    async fn sqlite_rows(
        storage: &cc_lb_storage_sqlite::SqliteStorage,
        upstream_ids: &[Uuid],
    ) -> Result<Vec<CheckpointRow>> {
        let mut records = Vec::new();
        for upstream_id in upstream_ids {
            let rows = sqlx::query(
                "SELECT * FROM upstream_subscription_quota_checkpoints_v1 WHERE upstream_id = ?",
            )
            .bind(upstream_id.to_string())
            .fetch_all(storage.pool())
            .await?;
            for row in &rows {
                records.push(checkpoint_row_from_row!(row));
            }
        }
        sort_checkpoint_rows(&mut records);
        Ok(records)
    }

    #[cfg(feature = "postgres")]
    async fn postgres_rows(
        storage: &cc_lb_storage_postgres::PostgresStorage,
        upstream_ids: &[Uuid],
    ) -> Result<Vec<CheckpointRow>> {
        let rows = sqlx::query(
            "SELECT upstream_id::text AS upstream_id, \"window\", source, \
                    changed_at_unix_millis, sample_id::text AS sample_id, \
                    semantic_fingerprint, sample_kind, representative_claim, utilization, \
                    status, resets_at_unix_secs, surpassed_threshold, fallback_percentage, \
                    fallback_available, overage_in_use, overage_period_monthly_utilization, \
                    upgrade_paths, disabled_reason, extra_usage_enabled, \
                    extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis \
             FROM upstream_subscription_quota_checkpoints_v1 \
             WHERE upstream_id = ANY($1::uuid[])",
        )
        .bind(upstream_ids)
        .fetch_all(storage.pool())
        .await?;
        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            records.push(checkpoint_row_from_row!(row));
        }
        sort_checkpoint_rows(&mut records);
        Ok(records)
    }
}
