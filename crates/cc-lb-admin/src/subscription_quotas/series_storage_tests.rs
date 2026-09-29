//! End-to-end series checks over real SQLite storage: quota samples and
//! checkpoints go through the storage write path, and series are built by
//! `list_subscription_quota_series` from slim checkpoints.

use std::sync::Arc;

use cc_lb_storage_api::{
    MetaStore, SubscriptionQuotaBucket, SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use tempfile::TempDir;
use uuid::Uuid;

use super::list_subscription_quota_series;

#[tokio::test]
async fn same_millis_samples_both_count_in_their_bucket() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(2);
    storage
        .record_subscription_quota_samples(&[
            observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1),
            observation(upstream, 100, 2, SubscriptionQuotaSource::Header, 0.2),
        ])
        .await
        .expect("record samples");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            0,
            200,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(series.len(), 1, "expected one header series");
    assert_eq!(
        series[0].buckets[0].sample_count, 2,
        "same-millis samples should both persist"
    );
}

#[tokio::test]
async fn older_append_still_counts_in_series() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(4);
    let newer = observation(upstream, 200, 1, SubscriptionQuotaSource::Header, 0.9);
    let older = observation(upstream, 100, 2, SubscriptionQuotaSource::Header, 0.1);
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&newer))
        .await
        .expect("record newer sample");
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&older))
        .await
        .expect("record older sample");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            0,
            300,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(
        series[0].buckets[0].sample_count, 2,
        "observations should contain both records"
    );
}

#[tokio::test]
async fn series_returns_buckets_with_correct_bounds() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(5);
    storage
        .record_subscription_quota_samples(&[
            observation(upstream, 0, 1, SubscriptionQuotaSource::Header, 0.1),
            observation(upstream, 30_000, 2, SubscriptionQuotaSource::Header, 0.3),
            observation(upstream, 60_000, 3, SubscriptionQuotaSource::Header, 0.6),
        ])
        .await
        .expect("record samples");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            0,
            60_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(series.len(), 1, "expected one series");
    assert_eq!(series[0].buckets.len(), 2, "expected two buckets");
    assert_eq!(
        series[0].buckets[0].bucket_start_unix_secs, 0,
        "first bucket starts at 0"
    );
    assert_eq!(
        series[0].buckets[0].sample_count, 2,
        "first bucket has t=0 and t=30s"
    );
    assert_eq!(
        series[0].buckets[1].bucket_start_unix_secs, 60,
        "second bucket starts at 60s"
    );
}

#[tokio::test]
async fn series_source_merge_merged_collapses_both_sources() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(6);
    storage
        .record_subscription_quota_samples(&[
            observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1),
            observation(upstream, 100, 2, SubscriptionQuotaSource::Api, 0.2),
        ])
        .await
        .expect("record samples");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            0,
            200,
            60,
            10,
            SubscriptionQuotaSourceMerge::Merged,
        ),
    )
    .await;
    assert_eq!(series.len(), 1, "merged query should return one series");
    assert_eq!(
        series[0].source,
        SubscriptionQuotaSourceMerge::Merged,
        "series source should be merged"
    );
    assert_eq!(
        series[0].buckets[0].sources_seen.len(),
        2,
        "merged bucket sees both sources"
    );
}

#[tokio::test]
async fn series_source_merge_header_filters_api() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(7);
    storage
        .record_subscription_quota_samples(&[
            observation(upstream, 100, 1, SubscriptionQuotaSource::Header, 0.1),
            observation(upstream, 100, 2, SubscriptionQuotaSource::Api, 0.8),
        ])
        .await
        .expect("record samples");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            0,
            200,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(series.len(), 1, "header query should return one series");
    assert_eq!(
        series[0].buckets[0].sample_count, 1,
        "api sample should be filtered"
    );
    assert_eq!(
        series[0].buckets[0].sources_seen,
        [SubscriptionQuotaSource::Header],
        "only header source seen"
    );
}

#[tokio::test]
async fn series_max_points_per_series_downsamples() {
    let (_dir, storage) = open_storage().await;
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
    storage
        .record_subscription_quota_samples(&records)
        .await
        .expect("record samples");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            0,
            99_000,
            1,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert!(
        series[0].buckets.len() <= 10,
        "series should downsample to cap"
    );
}

#[tokio::test]
async fn absent_marker_keeps_the_real_sample_series() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(12);
    storage
        .record_subscription_quota_samples(&[observation(
            upstream,
            100,
            1,
            SubscriptionQuotaSource::Api,
            0.25,
        )])
        .await
        .expect("record sample");
    storage
        .record_subscription_quota_samples(&[absent(upstream)])
        .await
        .expect("record absent marker");

    let series = load_series(
        &storage,
        series_query(upstream, 0, 300, 60, 10, SubscriptionQuotaSourceMerge::Api),
    )
    .await;
    assert_eq!(series.len(), 1, "historical sample series should remain");
    let sample_count = series[0]
        .buckets
        .iter()
        .map(|bucket| bucket.sample_count)
        .sum::<u32>();
    assert_eq!(
        sample_count, 1,
        "absent marker must not become a utilization checkpoint"
    );
    assert_eq!(
        series[0]
            .buckets
            .last()
            .and_then(|bucket| bucket.utilization_last),
        Some(0.25),
        "last historical utilization should remain the real sample"
    );
}

#[tokio::test]
async fn repeated_absent_marker_creates_no_series() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(13);
    let absent = absent(upstream);
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&absent))
        .await
        .expect("record absent marker");
    storage
        .record_subscription_quota_samples(std::slice::from_ref(&absent))
        .await
        .expect("record repeated absent marker");

    let series = load_series(
        &storage,
        series_query(upstream, 0, 300, 60, 10, SubscriptionQuotaSourceMerge::Api),
    )
    .await;
    assert!(
        series.is_empty(),
        "repeated absent marker must not create utilization checkpoints"
    );
}

#[tokio::test]
async fn empty_upstream_ids_return_empty_series() {
    let (_dir, storage) = open_storage().await;
    let mut query = series_query(
        upstream_id(12),
        0,
        100,
        60,
        10,
        SubscriptionQuotaSourceMerge::Merged,
    );
    query.upstream_ids.clear();
    assert!(
        load_series(&storage, query).await.is_empty(),
        "empty series input should return empty"
    );
}

#[tokio::test]
async fn series_filters_observed_at_window() {
    const HOUR_MILLIS: u64 = 60 * 60 * 1_000;

    let (_dir, storage) = open_storage().await;
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
        .await
        .expect("record samples");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            since_unix_millis,
            until_unix_millis,
            60,
            100,
            SubscriptionQuotaSourceMerge::Merged,
        ),
    )
    .await;
    assert_eq!(series.len(), 1, "expected one bounded merged series");
    let buckets = &series[0].buckets;
    assert!(
        buckets.len() == 61
            && buckets.iter().all(|bucket| bucket.observed)
            && buckets[0].bucket_start_unix_secs == since_unix_millis / 1_000
            && buckets[60].bucket_start_unix_secs == until_unix_millis / 1_000,
        "the inclusive one-hour window must contain exactly 61 observed requested-range buckets"
    );
    assert_eq!(
        buckets
            .iter()
            .map(|bucket| bucket.sample_count)
            .sum::<u32>(),
        4,
        "only the four inclusive in-range checkpoints should count as samples"
    );
    assert!(
        buckets[0].sample_count == 1
            && buckets[0].utilization_min == Some(0.2)
            && buckets[0].utilization_max == Some(0.2)
            && buckets[0].utilization_last == Some(0.2)
            && buckets[0].observed_at_unix_millis_last == Some(since_unix_millis)
            && buckets[0].sources_seen == [SubscriptionQuotaSource::Header],
        "the exact-since header checkpoint must replace the pre-range anchor in the first bucket"
    );
    assert!(
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
    assert!(
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
    assert!(
        buckets[59].sample_count == 0
            && buckets[59].utilization_last == Some(0.4)
            && buckets[59].observed_at_unix_millis_last == Some(since_unix_millis + 120_000),
        "dense gap buckets must carry the latest per-source state without synthetic samples"
    );
    assert!(
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
}

#[tokio::test]
async fn checkpoint_series_anchor_merge() {
    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(26);
    storage
        .put_subscription_quota_checkpoints(&[
            checkpoint(&observation(
                upstream,
                30_000,
                1,
                SubscriptionQuotaSource::Header,
                0.10,
            )),
            checkpoint(&observation(
                upstream,
                75_000,
                2,
                SubscriptionQuotaSource::Header,
                0.20,
            )),
            checkpoint(&observation(
                upstream,
                90_000,
                3,
                SubscriptionQuotaSource::Header,
                0.40,
            )),
            checkpoint(&observation(
                upstream,
                180_000,
                4,
                SubscriptionQuotaSource::Header,
                0.60,
            )),
            checkpoint(&observation(
                upstream,
                45_000,
                5,
                SubscriptionQuotaSource::Api,
                0.80,
            )),
            checkpoint(&observation(
                upstream,
                120_000,
                6,
                SubscriptionQuotaSource::Api,
                0.70,
            )),
        ])
        .await
        .expect("put checkpoints");

    let header_series = load_series(
        &storage,
        series_query(
            upstream,
            60_000,
            240_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(header_series.len(), 1, "expected one header series");
    let header_buckets = &header_series[0].buckets;
    assert_bucket_starts(header_buckets, &[60, 120, 180, 240]);
    assert_eq!(
        header_buckets[0].utilization_last,
        Some(0.40),
        "first requested bucket should apply its in-range header checkpoints over the anchor"
    );
    assert_eq!(
        header_buckets[0].sample_count, 2,
        "same-minute in-range header changes should remain distinct inside the bucket"
    );
    assert_eq!(
        header_buckets[0].observed_at_unix_millis_last,
        Some(90_000),
        "same-minute bucket should preserve the exact timestamp of the last checkpoint"
    );
    assert!(
        header_buckets[1].sample_count == 0 && header_buckets[1].utilization_last == Some(0.40),
        "gap bucket should carry forward the latest header checkpoint without adding a change count"
    );
    assert_eq!(
        header_buckets[1].observed_at_unix_millis_last,
        Some(90_000),
        "carry-forward bucket should keep the exact source checkpoint timestamp"
    );
    assert!(
        header_buckets
            .iter()
            .all(|bucket| bucket.sources_seen == [SubscriptionQuotaSource::Header]),
        "header series should not contain api provenance"
    );

    let api_series = load_series(
        &storage,
        series_query(
            upstream,
            60_000,
            180_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Api,
        ),
    )
    .await;
    assert_eq!(api_series.len(), 1, "expected one api series");
    let api_buckets = &api_series[0].buckets;
    assert_bucket_starts(api_buckets, &[60, 120, 180]);
    assert!(
        api_buckets[0].sample_count == 0
            && api_buckets[0].utilization_last == Some(0.80)
            && api_buckets[1].utilization_last == Some(0.70)
            && api_buckets[2].utilization_last == Some(0.70),
        "api anchor should seed the requested range and carry forward independently of header checkpoints"
    );
    assert!(
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
        .await
        .expect("put merged checkpoints");
    let merged_series = load_series(
        &storage,
        series_query(
            merged_upstream,
            60_000,
            119_999,
            60,
            10,
            SubscriptionQuotaSourceMerge::Merged,
        ),
    )
    .await;
    assert_eq!(merged_series.len(), 1, "expected one merged series");
    let merged_bucket = &merged_series[0].buckets[0];
    assert!(
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
        .await
        .expect("put no-anchor checkpoint");
    let no_anchor_series = load_series(
        &storage,
        series_query(
            no_anchor_upstream,
            60_000,
            240_000,
            60,
            10,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(no_anchor_series.len(), 1, "expected one no-anchor series");
    let no_anchor_buckets = &no_anchor_series[0].buckets;
    assert_bucket_starts(no_anchor_buckets, &[180, 240]);
    assert_eq!(
        no_anchor_buckets[0].utilization_last,
        Some(0.50),
        "series without a left anchor should start at the first real checkpoint"
    );
}

#[tokio::test]
async fn checkpoint_series_old_anchor_is_bounded_and_dense() {
    const DAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
    const HOUR_MILLIS: u64 = 60 * 60 * 1_000;

    let (_dir, storage) = open_storage().await;
    let upstream = upstream_id(32);
    let since_unix_millis = 180 * DAY_MILLIS;
    let until_unix_millis = since_unix_millis + HOUR_MILLIS;
    storage
        .put_subscription_quota_checkpoints(&[checkpoint(&observation(
            upstream,
            1_000,
            1,
            SubscriptionQuotaSource::Header,
            0.42,
        ))])
        .await
        .expect("put anchor checkpoint");

    let series = load_series(
        &storage,
        series_query(
            upstream,
            since_unix_millis,
            until_unix_millis,
            60,
            100,
            SubscriptionQuotaSourceMerge::Header,
        ),
    )
    .await;
    assert_eq!(series.len(), 1, "expected one anchor-only series");
    let buckets = &series[0].buckets;
    assert_eq!(
        buckets.len(),
        61,
        "an inclusive one-hour range at 60-second resolution must contain exactly 61 buckets"
    );
    assert!(
        buckets.first().map(|bucket| bucket.bucket_start_unix_secs)
            == Some(since_unix_millis / 1_000)
            && buckets.last().map(|bucket| bucket.bucket_start_unix_secs)
                == Some(until_unix_millis / 1_000),
        "an old anchor must not materialize buckets outside the requested range"
    );
    assert!(
        buckets.iter().all(|bucket| {
            bucket.observed
                && bucket.sample_count == 0
                && bucket.utilization_last == Some(0.42)
                && bucket.observed_at_unix_millis_last == Some(1_000)
                && bucket.sources_seen == [SubscriptionQuotaSource::Header]
        }),
        "anchor-only buckets must preserve the exact carried state without counting synthetic samples"
    );
}

async fn open_storage() -> (TempDir, SqliteStorage) {
    let dir = tempfile::tempdir().expect("storage dir");
    let database_url = format!(
        "sqlite://{}",
        dir.path()
            .join("subscription-quota-series.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("storage opens");
    storage.initialize().await.expect("storage initializes");
    (dir, storage)
}

async fn load_series(
    storage: &SqliteStorage,
    query: SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaSeries> {
    list_subscription_quota_series(storage, query)
        .await
        .expect("list subscription quota series")
}

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
        sample_id: Uuid::from_u128(u128::from(sample_id)),
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

fn absent(upstream_id: Uuid) -> SubscriptionQuotaSample {
    let mut absent = observation(upstream_id, 200, 2, SubscriptionQuotaSource::Api, 0.0);
    absent.sample_kind = SubscriptionQuotaSampleKind::Absent;
    absent.utilization = None;
    absent.status = None;
    absent.resets_at_unix_secs = None;
    absent
}

fn checkpoint(record: &SubscriptionQuotaSample) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(record)
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

fn assert_bucket_starts(buckets: &[SubscriptionQuotaBucket], expected: &[u64]) {
    let actual = buckets
        .iter()
        .map(|bucket| bucket.bucket_start_unix_secs)
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "bucket starts {actual:?} should equal {expected:?}"
    );
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x1000 + value)
}
