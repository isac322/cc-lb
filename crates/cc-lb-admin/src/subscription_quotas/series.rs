use std::collections::BTreeMap;

use cc_lb_storage_api::{
    Storage, StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery, SubscriptionQuotaSlimCheckpoint,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow,
};
use uuid::Uuid;

pub(super) async fn list_subscription_quota_series(
    storage: &dyn Storage,
    query: SubscriptionQuotaSeriesQuery,
) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
    if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
        return Ok(Vec::new());
    }
    let sources = query
        .sources
        .iter()
        .copied()
        .filter(|source| source_matches_merge(*source, query.source_merge))
        .collect::<Vec<_>>();
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let checkpoints = storage
        .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: query.upstream_ids.clone(),
            windows: query.windows.clone(),
            sources,
            since_unix_millis: query.since_unix_millis,
            until_unix_millis: query.until_unix_millis,
        })
        .await?;
    Ok(build_series(checkpoints, &query))
}

fn build_series(
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
            let mut buckets = buckets_from_checkpoints(
                source_checkpoints.into_values(),
                query.bucket_secs.max(1),
                query.until_unix_millis,
            );
            downsample(
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

struct CheckpointStream {
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    next_index: usize,
    current_index: Option<usize>,
}

#[derive(Clone, Copy)]
struct BucketPoint {
    source: SubscriptionQuotaSource,
    changed_at_unix_millis: u64,
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<u64>,
    is_change: bool,
}

fn buckets_from_checkpoints(
    source_checkpoints: impl Iterator<Item = Vec<SubscriptionQuotaSlimCheckpoint>>,
    bucket_secs: u64,
    until_unix_millis: u64,
) -> Vec<SubscriptionQuotaBucket> {
    let mut streams = source_checkpoints
        .filter(|checkpoints| !checkpoints.is_empty())
        .map(|checkpoints| CheckpointStream {
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
        .map(|checkpoint| bucket_start_unix_secs(checkpoint.changed_at_unix_millis, bucket_secs))
        .min()
    else {
        return Vec::new();
    };
    let end_bucket = bucket_start_unix_secs(until_unix_millis, bucket_secs);
    let mut buckets = Vec::new();
    while bucket_start <= end_bucket {
        let mut points = Vec::with_capacity(streams.len());
        for stream in &mut streams {
            push_bucket_points(stream, bucket_start, bucket_secs, &mut points);
        }
        if !points.is_empty() {
            buckets.push(bucket_from_points(bucket_start, &points));
        }
        let Some(next_bucket) = bucket_start.checked_add(bucket_secs) else {
            break;
        };
        bucket_start = next_bucket;
    }
    buckets
}

fn push_bucket_points(
    stream: &mut CheckpointStream,
    bucket_start: u64,
    bucket_secs: u64,
    points: &mut Vec<BucketPoint>,
) {
    let start_index = stream.next_index;
    while stream.next_index < stream.checkpoints.len()
        && bucket_start_unix_secs(
            stream.checkpoints[stream.next_index].changed_at_unix_millis,
            bucket_secs,
        ) == bucket_start
    {
        stream.current_index = Some(stream.next_index);
        points.push(bucket_point(&stream.checkpoints[stream.next_index], true));
        stream.next_index += 1;
    }
    if stream.next_index == start_index
        && let Some(index) = stream.current_index
    {
        points.push(bucket_point(&stream.checkpoints[index], false));
    }
}

fn bucket_point(checkpoint: &SubscriptionQuotaSlimCheckpoint, is_change: bool) -> BucketPoint {
    BucketPoint {
        source: checkpoint.source,
        changed_at_unix_millis: checkpoint.changed_at_unix_millis,
        utilization: checkpoint.utilization,
        status: checkpoint.status,
        resets_at_unix_secs: checkpoint.resets_at_unix_secs,
        is_change,
    }
}

fn bucket_from_points(
    bucket_start_unix_secs: u64,
    points: &[BucketPoint],
) -> SubscriptionQuotaBucket {
    let mut utilization_count = 0_u32;
    let mut utilization_sum = 0.0;
    let mut utilization_min: Option<f64> = None;
    let mut utilization_max: Option<f64> = None;
    let mut last = points[0];
    let mut sources_seen = std::collections::BTreeSet::new();
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

fn downsample(items: &mut Vec<SubscriptionQuotaBucket>, max_points: usize) {
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

const fn bucket_start_unix_secs(timestamp_unix_millis: u64, bucket_secs: u64) -> u64 {
    let timestamp_unix_secs = timestamp_unix_millis / 1_000;
    timestamp_unix_secs / bucket_secs * bucket_secs
}

const fn source_matches_merge(
    source: SubscriptionQuotaSource,
    source_merge: SubscriptionQuotaSourceMerge,
) -> bool {
    match source_merge {
        SubscriptionQuotaSourceMerge::Merged => true,
        SubscriptionQuotaSourceMerge::Header => matches!(source, SubscriptionQuotaSource::Header),
        SubscriptionQuotaSourceMerge::Api => matches!(source, SubscriptionQuotaSource::Api),
    }
}
