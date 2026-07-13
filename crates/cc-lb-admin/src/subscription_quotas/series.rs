use std::collections::BTreeMap;

use cc_lb_storage_api::{
    Storage, StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery, SubscriptionQuotaSlimCheckpoint,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow,
};
use uuid::Uuid;

/// Upper bound on the bucket-accumulator pre-sizing hint so a pathological
/// (since, until, bucket_secs) combination cannot request an unreasonable
/// upfront allocation; the `Vec` still grows past this via normal doubling.
const MAX_BUCKET_CAPACITY_HINT: usize = 1_000_000;

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

/// Per-(upstream, window) checkpoint groups, keyed by source. Only two
/// sources exist today (`SubscriptionQuotaSource::all()` is a fixed
/// two-element slice); a compile-time assertion below fails loudly if a
/// third source is ever added, instead of this struct silently dropping it.
#[derive(Default)]
struct SourceCheckpoints {
    header: Vec<SubscriptionQuotaSlimCheckpoint>,
    api: Vec<SubscriptionQuotaSlimCheckpoint>,
}

const _: () = assert!(SubscriptionQuotaSource::all().len() == 2);

impl SourceCheckpoints {
    fn push(&mut self, checkpoint: SubscriptionQuotaSlimCheckpoint) {
        match checkpoint.source {
            SubscriptionQuotaSource::Header => self.header.push(checkpoint),
            SubscriptionQuotaSource::Api => self.api.push(checkpoint),
        }
    }

    fn into_checkpoint_lists(self) -> [Vec<SubscriptionQuotaSlimCheckpoint>; 2] {
        [self.header, self.api]
    }
}

fn build_series(
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    query: &SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaSeries> {
    let mut groups = BTreeMap::<(Uuid, SubscriptionQuotaWindow), SourceCheckpoints>::new();
    for checkpoint in checkpoints {
        groups
            .entry((checkpoint.upstream_id, checkpoint.window))
            .or_default()
            .push(checkpoint);
    }

    let max_points = usize::try_from(query.max_points_per_series).unwrap_or(usize::MAX);
    groups
        .into_iter()
        .map(|((upstream_id, window), source_checkpoints)| {
            let buckets = sampled_buckets_from_checkpoints(
                source_checkpoints,
                query.bucket_secs.max(1),
                query.until_unix_millis,
                max_points,
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
struct LastObservation {
    changed_at_unix_millis: u64,
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<u64>,
}

/// Stack-resident bucket aggregate: no heap allocation is touched while a
/// bucket is being observed. `sources_seen` is a two-bit mask (one bit per
/// `SubscriptionQuotaSource` variant) instead of a per-bucket `BTreeSet`, and
/// the running "last observation wins ties" state is a plain `Option`
/// instead of retaining every point. The only heap allocation this produces
/// is the small `sources_seen: Vec` created once in `finalize`, and only for
/// buckets that survive downsampling.
struct BucketAccumulator {
    bucket_start_unix_secs: u64,
    sample_count: u32,
    utilization_count: u32,
    utilization_sum: f64,
    utilization_min: Option<f64>,
    utilization_max: Option<f64>,
    sources_seen_bitmask: u8,
    last: Option<LastObservation>,
}

impl BucketAccumulator {
    const fn new(bucket_start_unix_secs: u64) -> Self {
        Self {
            bucket_start_unix_secs,
            sample_count: 0,
            utilization_count: 0,
            utilization_sum: 0.0,
            utilization_min: None,
            utilization_max: None,
            sources_seen_bitmask: 0,
            last: None,
        }
    }

    fn observe(&mut self, checkpoint: &SubscriptionQuotaSlimCheckpoint, is_change: bool) {
        self.sources_seen_bitmask |= source_bit(checkpoint.source);
        if is_change {
            self.sample_count = self.sample_count.saturating_add(1);
        }
        let is_newer = match self.last {
            Some(last) => checkpoint.changed_at_unix_millis >= last.changed_at_unix_millis,
            None => true,
        };
        if is_newer {
            self.last = Some(LastObservation {
                changed_at_unix_millis: checkpoint.changed_at_unix_millis,
                utilization: checkpoint.utilization,
                status: checkpoint.status,
                resets_at_unix_secs: checkpoint.resets_at_unix_secs,
            });
        }
        if let Some(utilization) = checkpoint.utilization {
            self.utilization_count += 1;
            self.utilization_sum += utilization;
            self.utilization_min = Some(
                self.utilization_min
                    .map_or(utilization, |value| value.min(utilization)),
            );
            self.utilization_max = Some(
                self.utilization_max
                    .map_or(utilization, |value| value.max(utilization)),
            );
        }
    }

    fn finalize(self) -> Option<SubscriptionQuotaBucket> {
        let last = self.last?;
        Some(SubscriptionQuotaBucket {
            bucket_start_unix_secs: self.bucket_start_unix_secs,
            observed: true,
            sample_count: self.sample_count,
            utilization_min: self.utilization_min,
            utilization_avg: (self.utilization_count > 0)
                .then_some(self.utilization_sum / f64::from(self.utilization_count)),
            utilization_max: self.utilization_max,
            utilization_last: last.utilization,
            status_last: last.status,
            resets_at_unix_secs_last: last.resets_at_unix_secs,
            observed_at_unix_millis_last: Some(last.changed_at_unix_millis),
            sources_seen: sources_seen_from_bitmask(self.sources_seen_bitmask),
        })
    }
}

const fn source_bit(source: SubscriptionQuotaSource) -> u8 {
    match source {
        SubscriptionQuotaSource::Header => 0b01,
        SubscriptionQuotaSource::Api => 0b10,
    }
}

fn sources_seen_from_bitmask(bitmask: u8) -> Vec<SubscriptionQuotaSource> {
    SubscriptionQuotaSource::all()
        .iter()
        .copied()
        .filter(|source| bitmask & source_bit(*source) != 0)
        .collect()
}

/// Builds one series' buckets, streamed and downsampled without ever
/// materializing more than `max_points` final `SubscriptionQuotaBucket`
/// values. A single pass over the checkpoint streams produces a `Vec` of
/// stack-only `BucketAccumulator`s (pre-sized from the query's time range);
/// only the buckets selected by the existing uniform-downsample formula are
/// converted into the heap-owning `SubscriptionQuotaBucket` output type.
fn sampled_buckets_from_checkpoints(
    source_checkpoints: SourceCheckpoints,
    bucket_secs: u64,
    until_unix_millis: u64,
    max_points: usize,
) -> Vec<SubscriptionQuotaBucket> {
    if max_points == 0 {
        return Vec::new();
    }
    let mut streams = source_checkpoints
        .into_checkpoint_lists()
        .into_iter()
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
    let Some(start_bucket) = streams
        .iter()
        .filter_map(|stream| stream.checkpoints.first())
        .map(|checkpoint| bucket_start_unix_secs(checkpoint.changed_at_unix_millis, bucket_secs))
        .min()
    else {
        return Vec::new();
    };
    let end_bucket = bucket_start_unix_secs(until_unix_millis, bucket_secs);
    let accumulators = accumulate_buckets(&mut streams, start_bucket, end_bucket, bucket_secs);
    select_and_finalize(accumulators, max_points)
}

fn accumulate_buckets(
    streams: &mut [CheckpointStream],
    start_bucket: u64,
    end_bucket: u64,
    bucket_secs: u64,
) -> Vec<BucketAccumulator> {
    let estimated_buckets =
        (end_bucket.saturating_sub(start_bucket) / bucket_secs).saturating_add(1);
    let capacity_hint = usize::try_from(estimated_buckets)
        .unwrap_or(usize::MAX)
        .min(MAX_BUCKET_CAPACITY_HINT);
    let mut accumulators = Vec::with_capacity(capacity_hint);
    let mut bucket_start = start_bucket;
    while bucket_start <= end_bucket {
        let mut accumulator = BucketAccumulator::new(bucket_start);
        for stream in streams.iter_mut() {
            observe_stream_into_accumulator(stream, bucket_start, bucket_secs, &mut accumulator);
        }
        if accumulator.last.is_some() {
            accumulators.push(accumulator);
        }
        let Some(next_bucket) = bucket_start.checked_add(bucket_secs) else {
            break;
        };
        bucket_start = next_bucket;
    }
    accumulators
}

fn observe_stream_into_accumulator(
    stream: &mut CheckpointStream,
    bucket_start: u64,
    bucket_secs: u64,
    accumulator: &mut BucketAccumulator,
) {
    let start_index = stream.next_index;
    while stream.next_index < stream.checkpoints.len()
        && bucket_start_unix_secs(
            stream.checkpoints[stream.next_index].changed_at_unix_millis,
            bucket_secs,
        ) == bucket_start
    {
        stream.current_index = Some(stream.next_index);
        accumulator.observe(&stream.checkpoints[stream.next_index], true);
        stream.next_index += 1;
    }
    if stream.next_index == start_index
        && let Some(index) = stream.current_index
    {
        accumulator.observe(&stream.checkpoints[index], false);
    }
}

/// Mirrors the original uniform-downsample selection (skip-then-take over an
/// enumerated, peekable sequence) so the selected bucket indices are
/// byte-identical to before; the only difference is that `finalize` (the
/// allocation for `sources_seen`) now only runs for the `max_points` buckets
/// actually kept.
fn select_and_finalize(
    accumulators: Vec<BucketAccumulator>,
    max_points: usize,
) -> Vec<SubscriptionQuotaBucket> {
    if accumulators.len() <= max_points {
        return accumulators
            .into_iter()
            .filter_map(BucketAccumulator::finalize)
            .collect();
    }
    if max_points == 1 {
        return accumulators
            .into_iter()
            .take(1)
            .filter_map(BucketAccumulator::finalize)
            .collect();
    }

    let last_index = accumulators.len() - 1;
    let mut source = accumulators.into_iter().enumerate().peekable();
    let mut buckets = Vec::with_capacity(max_points);
    for point in 0..max_points {
        let target = point * last_index / (max_points - 1);
        while source.peek().is_some_and(|(index, _)| *index < target) {
            source.next();
        }
        if let Some((_, accumulator)) = source.next()
            && let Some(bucket) = accumulator.finalize()
        {
            buckets.push(bucket);
        }
    }
    buckets
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

#[cfg(test)]
#[path = "series_tests.rs"]
mod tests;
