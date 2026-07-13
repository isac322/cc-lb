use std::{cmp::Ordering, collections::BTreeMap};

use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaProviderLot,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow,
};
use uuid::Uuid;

use super::slim::list_slim_checkpoints;
use crate::adapter::PostgresStorage;

const PROVIDER_LOT_BUCKET_SECS: u64 = 60;
const RESET_DROP_THRESHOLD: f64 = 0.5;

pub(in super::super) async fn list_provider_lots(
    storage: &PostgresStorage,
    query: &SubscriptionQuotaProviderLotQuery,
) -> StorageResult<Vec<SubscriptionQuotaProviderLot>> {
    let sources = query
        .sources
        .iter()
        .copied()
        .filter(|source| source_matches_merge(*source, query.source_merge))
        .collect::<Vec<_>>();
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let checkpoints = list_slim_checkpoints(
        storage,
        &SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: query.upstream_ids.clone(),
            windows: query.windows.clone(),
            sources,
            since_unix_millis: query.since_unix_millis,
            until_unix_millis: query.until_unix_millis,
        },
    )
    .await?;
    Ok(provider_lots_from_checkpoints(checkpoints, query))
}

fn provider_lots_from_checkpoints(
    checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    query: &SubscriptionQuotaProviderLotQuery,
) -> Vec<SubscriptionQuotaProviderLot> {
    let mut groups =
        BTreeMap::<(Uuid, SubscriptionQuotaWindow), Vec<SubscriptionQuotaSlimCheckpoint>>::new();
    for checkpoint in checkpoints {
        groups
            .entry((checkpoint.upstream_id, checkpoint.window))
            .or_default()
            .push(checkpoint);
    }
    let mut lots = Vec::new();
    for ((upstream_id, window), mut checkpoints) in groups {
        let Some(window_secs) = window_secs(window) else {
            continue;
        };
        checkpoints.sort_by(compare_checkpoints);
        lots.extend(lots_for_group(
            upstream_id,
            window,
            window_secs,
            query.source_merge,
            query.until_unix_millis,
            query.evaluation_unix_secs,
            &checkpoints,
        ));
    }
    lots
}

#[derive(Clone, Copy)]
struct Observation {
    bucket_start_unix_secs: u64,
    observed_at_unix_millis: u64,
    utilization: f64,
    resets_at_unix_secs: Option<u64>,
}

struct Cycle {
    first_observed_at_unix_millis: u64,
    last: Observation,
}

fn lots_for_group(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    window_secs: u64,
    source_merge: SubscriptionQuotaSourceMerge,
    until_unix_millis: u64,
    evaluation_unix_secs: u64,
    checkpoints: &[SubscriptionQuotaSlimCheckpoint],
) -> Vec<SubscriptionQuotaProviderLot> {
    let Some(first) = checkpoints.first() else {
        return Vec::new();
    };
    let mut latest_by_source: [Option<&SubscriptionQuotaSlimCheckpoint>; 2] = [None, None];
    let mut index = 0usize;
    let mut bucket_start = quota_bucket_start(first.changed_at_unix_millis);
    let end_bucket = quota_bucket_start(until_unix_millis);
    let mut cycle: Option<Cycle> = None;
    let mut lots = Vec::new();

    while bucket_start <= end_bucket {
        while let Some(checkpoint) = checkpoints.get(index)
            && quota_bucket_start(checkpoint.changed_at_unix_millis) == bucket_start
        {
            latest_by_source[source_index(checkpoint.source)] = Some(checkpoint);
            index += 1;
        }
        let selected = latest_by_source
            .iter()
            .flatten()
            .copied()
            .max_by(|left, right| compare_checkpoints(left, right));
        if let Some(checkpoint) = selected
            && let Some(utilization) = checkpoint.utilization
        {
            let observation = Observation {
                bucket_start_unix_secs: bucket_start,
                observed_at_unix_millis: checkpoint.changed_at_unix_millis,
                utilization,
                resets_at_unix_secs: checkpoint.resets_at_unix_secs,
            };
            match cycle.as_mut() {
                Some(current) if starts_new_cycle(current.last, observation) => {
                    lots.push(lot_from_cycle(
                        upstream_id,
                        window,
                        window_secs,
                        source_merge,
                        current,
                        current.last.bucket_start_unix_secs,
                    ));
                    cycle = Some(Cycle {
                        first_observed_at_unix_millis: observation.observed_at_unix_millis,
                        last: observation,
                    });
                }
                Some(current) => current.last = observation,
                None => {
                    cycle = Some(Cycle {
                        first_observed_at_unix_millis: observation.observed_at_unix_millis,
                        last: observation,
                    });
                }
            }
        }
        let Some(next_bucket) = bucket_start.checked_add(PROVIDER_LOT_BUCKET_SECS) else {
            break;
        };
        bucket_start = next_bucket;
    }
    if let Some(cycle) = cycle {
        lots.push(lot_from_cycle(
            upstream_id,
            window,
            window_secs,
            source_merge,
            &cycle,
            evaluation_unix_secs,
        ));
    }
    lots
}

fn lot_from_cycle(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    window_secs: u64,
    source: SubscriptionQuotaSourceMerge,
    cycle: &Cycle,
    evaluation_unix_secs: u64,
) -> SubscriptionQuotaProviderLot {
    SubscriptionQuotaProviderLot {
        upstream_id,
        window,
        source,
        provider_start_unix_secs: cycle
            .last
            .resets_at_unix_secs
            .map(|reset| reset.saturating_sub(window_secs))
            .or(Some(cycle.first_observed_at_unix_millis / 1_000)),
        provider_reset_unix_secs: cycle.last.resets_at_unix_secs,
        observed_at_unix_millis: cycle.last.observed_at_unix_millis,
        evaluation_unix_secs,
        utilization: cycle.last.utilization,
    }
}

fn starts_new_cycle(previous: Observation, current: Observation) -> bool {
    let reset_changed = match (previous.resets_at_unix_secs, current.resets_at_unix_secs) {
        (Some(left), Some(right)) => left.abs_diff(right) > PROVIDER_LOT_BUCKET_SECS,
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    };
    reset_changed || previous.utilization - current.utilization >= RESET_DROP_THRESHOLD
}

fn compare_checkpoints(
    left: &SubscriptionQuotaSlimCheckpoint,
    right: &SubscriptionQuotaSlimCheckpoint,
) -> Ordering {
    left.changed_at_unix_millis
        .cmp(&right.changed_at_unix_millis)
        .then_with(|| left.source.as_str().cmp(right.source.as_str()))
        .then_with(|| left.sample_id.cmp(&right.sample_id))
}

const fn source_index(source: SubscriptionQuotaSource) -> usize {
    match source {
        SubscriptionQuotaSource::Api => 0,
        SubscriptionQuotaSource::Header => 1,
    }
}

const fn source_matches_merge(
    source: SubscriptionQuotaSource,
    source_merge: SubscriptionQuotaSourceMerge,
) -> bool {
    match source_merge {
        SubscriptionQuotaSourceMerge::Header => matches!(source, SubscriptionQuotaSource::Header),
        SubscriptionQuotaSourceMerge::Api => matches!(source, SubscriptionQuotaSource::Api),
        SubscriptionQuotaSourceMerge::Merged => true,
    }
}

const fn window_secs(window: SubscriptionQuotaWindow) -> Option<u64> {
    match window {
        SubscriptionQuotaWindow::FiveHour => Some(5 * 3_600),
        SubscriptionQuotaWindow::SevenDay
        | SubscriptionQuotaWindow::SevenDaySonnet
        | SubscriptionQuotaWindow::SevenDayOpus
        | SubscriptionQuotaWindow::SevenDayFable => Some(7 * 24 * 3_600),
        SubscriptionQuotaWindow::Overage | SubscriptionQuotaWindow::Unified => None,
    }
}

const fn quota_bucket_start(timestamp_unix_millis: u64) -> u64 {
    timestamp_unix_millis / 1_000 / PROVIDER_LOT_BUCKET_SECS * PROVIDER_LOT_BUCKET_SECS
}
