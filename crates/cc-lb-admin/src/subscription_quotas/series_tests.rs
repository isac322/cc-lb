use std::collections::{BTreeMap, BTreeSet};

use cc_lb_storage_api::{
    SubscriptionQuotaBucket, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use uuid::Uuid;

use super::build_series;

#[derive(Clone, Copy)]
struct CheckpointFixture {
    source: SubscriptionQuotaSource,
    changed_at_unix_millis: u64,
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<u64>,
}

struct ReferenceCheckpointStream {
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    next_index: usize,
    current_index: Option<usize>,
}

#[derive(Clone, Copy)]
struct ReferenceBucketPoint {
    source: SubscriptionQuotaSource,
    changed_at_unix_millis: u64,
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<u64>,
    is_change: bool,
}

#[test]
fn streamed_series_matches_reference_for_multi_window_boundaries_and_downsampling() {
    // Given: two source streams include left anchors plus exact interval and bucket boundaries.
    let upstream_id = Uuid::from_u128(1);
    let query = SubscriptionQuotaSeriesQuery {
        upstream_ids: vec![upstream_id],
        windows: vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ],
        sources: SubscriptionQuotaSource::all().to_vec(),
        since_unix_millis: 60_000,
        until_unix_millis: 300_000,
        bucket_secs: 60,
        max_points_per_series: 3,
        source_merge: SubscriptionQuotaSourceMerge::Merged,
    };
    let checkpoints = vec![
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Header,
                changed_at_unix_millis: 0,
                utilization: Some(0.1),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(400),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Header,
                changed_at_unix_millis: 60_000,
                utilization: Some(0.2),
                status: Some(SubscriptionQuotaStatus::AllowedWarning),
                resets_at_unix_secs: Some(500),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Header,
                changed_at_unix_millis: 119_999,
                utilization: Some(0.3),
                status: Some(SubscriptionQuotaStatus::Rejected),
                resets_at_unix_secs: Some(600),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Header,
                changed_at_unix_millis: 120_000,
                utilization: Some(0.4),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(700),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Header,
                changed_at_unix_millis: 300_000,
                utilization: Some(0.8),
                status: Some(SubscriptionQuotaStatus::AllowedWarning),
                resets_at_unix_secs: Some(800),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Api,
                changed_at_unix_millis: 60_000,
                utilization: Some(0.5),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(510),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Api,
                changed_at_unix_millis: 120_000,
                utilization: Some(0.6),
                status: Some(SubscriptionQuotaStatus::Rejected),
                resets_at_unix_secs: Some(710),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Api,
                changed_at_unix_millis: 240_000,
                utilization: Some(0.7),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(720),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::SevenDay,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Header,
                changed_at_unix_millis: 0,
                utilization: Some(0.9),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(900),
            },
        ),
        checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::SevenDay,
            CheckpointFixture {
                source: SubscriptionQuotaSource::Api,
                changed_at_unix_millis: 180_000,
                utilization: Some(0.95),
                status: Some(SubscriptionQuotaStatus::Rejected),
                resets_at_unix_secs: Some(950),
            },
        ),
    ];
    let expected = reference_build_series(checkpoints.clone(), &query);

    // When: the production builder streams selected buckets.
    let actual = build_series(checkpoints, &query);

    // Then: every element and serialized byte match the pre-streaming reference.
    assert_eq!(actual, expected);
    assert_eq!(
        serde_json::to_vec(&actual).expect("series serializes"),
        serde_json::to_vec(&expected).expect("reference series serializes"),
    );
    let five_hour = actual
        .iter()
        .find(|series| series.window == SubscriptionQuotaWindow::FiveHour)
        .expect("five-hour series is present");
    assert_eq!(
        five_hour
            .buckets
            .iter()
            .map(|bucket| bucket.bucket_start_unix_secs)
            .collect::<Vec<_>>(),
        vec![0, 120, 300],
    );
    assert_eq!(five_hour.buckets[1].utilization_last, Some(0.4));
    assert_eq!(five_hour.buckets[2].utilization_last, Some(0.8));
}

fn checkpoint(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    fixture: CheckpointFixture,
) -> SubscriptionQuotaSlimCheckpoint {
    let source_offset = match fixture.source {
        SubscriptionQuotaSource::Header => 1,
        SubscriptionQuotaSource::Api => 2,
    };
    SubscriptionQuotaSlimCheckpoint {
        upstream_id,
        window,
        source: fixture.source,
        changed_at_unix_millis: fixture.changed_at_unix_millis,
        sample_id: Uuid::from_u128(u128::from(fixture.changed_at_unix_millis) * 10 + source_offset),
        utilization: fixture.utilization,
        status: fixture.status,
        resets_at_unix_secs: fixture.resets_at_unix_secs,
    }
}

fn reference_build_series(
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    query: &SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaSeries> {
    let mut groups = BTreeMap::<
        (Uuid, SubscriptionQuotaWindow),
        BTreeMap<SubscriptionQuotaSource, Vec<SubscriptionQuotaSlimCheckpoint>>,
    >::new();
    for checkpoint in checkpoints {
        groups
            .entry((checkpoint.upstream_id, checkpoint.window))
            .or_default()
            .entry(checkpoint.source)
            .or_default()
            .push(checkpoint);
    }

    groups
        .into_iter()
        .map(|((upstream_id, window), source_checkpoints)| {
            let mut buckets = reference_buckets_from_checkpoints(
                source_checkpoints.into_values(),
                query.bucket_secs.max(1),
                query.until_unix_millis,
            );
            reference_downsample(
                &mut buckets,
                usize::try_from(query.max_points_per_series).unwrap_or(usize::MAX),
            );
            SubscriptionQuotaSeries {
                upstream_id,
                window,
                source: query.source_merge,
                buckets,
            }
        })
        .collect()
}

fn reference_buckets_from_checkpoints(
    source_checkpoints: impl Iterator<Item = Vec<SubscriptionQuotaSlimCheckpoint>>,
    bucket_secs: u64,
    until_unix_millis: u64,
) -> Vec<SubscriptionQuotaBucket> {
    let mut streams = source_checkpoints
        .filter(|checkpoints| !checkpoints.is_empty())
        .map(|checkpoints| ReferenceCheckpointStream {
            checkpoints,
            next_index: 0,
            current_index: None,
        })
        .collect::<Vec<_>>();
    streams.sort_by(|left, right| {
        left.checkpoints[0]
            .source
            .as_str()
            .cmp(right.checkpoints[0].source.as_str())
    });
    let Some(mut bucket_start) = streams
        .iter()
        .filter_map(|stream| stream.checkpoints.first())
        .map(|checkpoint| reference_bucket_start(checkpoint.changed_at_unix_millis, bucket_secs))
        .min()
    else {
        return Vec::new();
    };
    let end_bucket = reference_bucket_start(until_unix_millis, bucket_secs);
    let mut buckets = Vec::new();
    while bucket_start <= end_bucket {
        let mut points = Vec::with_capacity(streams.len());
        for stream in &mut streams {
            reference_push_bucket_points(stream, bucket_start, bucket_secs, &mut points);
        }
        if !points.is_empty() {
            buckets.push(reference_bucket_from_points(bucket_start, &points));
        }
        let Some(next_bucket) = bucket_start.checked_add(bucket_secs) else {
            break;
        };
        bucket_start = next_bucket;
    }
    buckets
}

fn reference_push_bucket_points(
    stream: &mut ReferenceCheckpointStream,
    bucket_start: u64,
    bucket_secs: u64,
    points: &mut Vec<ReferenceBucketPoint>,
) {
    let start_index = stream.next_index;
    while stream.next_index < stream.checkpoints.len()
        && reference_bucket_start(
            stream.checkpoints[stream.next_index].changed_at_unix_millis,
            bucket_secs,
        ) == bucket_start
    {
        stream.current_index = Some(stream.next_index);
        let checkpoint = &stream.checkpoints[stream.next_index];
        points.push(ReferenceBucketPoint {
            source: checkpoint.source,
            changed_at_unix_millis: checkpoint.changed_at_unix_millis,
            utilization: checkpoint.utilization,
            status: checkpoint.status,
            resets_at_unix_secs: checkpoint.resets_at_unix_secs,
            is_change: true,
        });
        stream.next_index += 1;
    }
    if stream.next_index == start_index
        && let Some(index) = stream.current_index
    {
        let checkpoint = &stream.checkpoints[index];
        points.push(ReferenceBucketPoint {
            source: checkpoint.source,
            changed_at_unix_millis: checkpoint.changed_at_unix_millis,
            utilization: checkpoint.utilization,
            status: checkpoint.status,
            resets_at_unix_secs: checkpoint.resets_at_unix_secs,
            is_change: false,
        });
    }
}

fn reference_bucket_from_points(
    bucket_start_unix_secs: u64,
    points: &[ReferenceBucketPoint],
) -> SubscriptionQuotaBucket {
    let mut utilization_count = 0_u32;
    let mut utilization_sum = 0.0;
    let mut utilization_min: Option<f64> = None;
    let mut utilization_max: Option<f64> = None;
    let mut last = points[0];
    let mut sources_seen = BTreeSet::new();
    let mut sample_count = 0_u32;

    for point in points {
        sources_seen.insert(point.source);
        if point.is_change {
            sample_count = sample_count.saturating_add(1);
        }
        if point.changed_at_unix_millis >= last.changed_at_unix_millis {
            last = *point;
        }
        if let Some(utilization) = point.utilization {
            utilization_count += 1;
            utilization_sum += utilization;
            utilization_min =
                Some(utilization_min.map_or(utilization, |value| value.min(utilization)));
            utilization_max =
                Some(utilization_max.map_or(utilization, |value| value.max(utilization)));
        }
    }

    SubscriptionQuotaBucket {
        bucket_start_unix_secs,
        observed: true,
        sample_count,
        utilization_min,
        utilization_avg: (utilization_count > 0)
            .then_some(utilization_sum / f64::from(utilization_count)),
        utilization_max,
        utilization_last: last.utilization,
        status_last: last.status,
        resets_at_unix_secs_last: last.resets_at_unix_secs,
        observed_at_unix_millis_last: Some(last.changed_at_unix_millis),
        sources_seen: sources_seen.into_iter().collect(),
    }
}

fn reference_downsample(items: &mut Vec<SubscriptionQuotaBucket>, max_points: usize) {
    if max_points == 0 {
        items.clear();
        return;
    }
    if items.len() <= max_points {
        return;
    }
    if max_points == 1 {
        items.truncate(1);
        return;
    }

    let original = std::mem::take(items);
    let last_index = original.len() - 1;
    let mut original = original.into_iter().enumerate().peekable();
    items.reserve(max_points);
    for point in 0..max_points {
        let target = point * last_index / (max_points - 1);
        while original.peek().is_some_and(|(index, _)| *index < target) {
            original.next();
        }
        if let Some((_, bucket)) = original.next() {
            items.push(bucket);
        }
    }
}

const fn reference_bucket_start(timestamp_unix_millis: u64, bucket_secs: u64) -> u64 {
    let timestamp_unix_secs = timestamp_unix_millis / 1_000;
    timestamp_unix_secs / bucket_secs * bucket_secs
}
