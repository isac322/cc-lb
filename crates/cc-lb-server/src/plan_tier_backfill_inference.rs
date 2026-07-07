use std::collections::BTreeMap;

use cc_lb_engine::plan_capacity::tier_key_from_seed_ratio;
use cc_lb_storage_api::{
    PoolQuotaContributorBlob, Storage, StorageError, SubscriptionQuotaWindow, TierResolutionSource,
    UpstreamPlanTierRecord,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::plan_tier_backfill::{BACKFILL_PROVENANCE, BackfillReport};

const PAGE_LIMIT: u32 = 500;

pub fn infer_pool_quota_blob_backfill(
    blobs: &[PoolQuotaContributorBlob],
    now_unix_millis: i64,
) -> BackfillInference {
    let mut winners = BTreeMap::new();
    let mut report = BackfillReport::default();
    for blob in blobs {
        process_blob(blob, &mut winners, &mut report);
    }
    inference_from_parts(winners, report, now_unix_millis)
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackfillInference {
    pub intervals_by_upstream: BTreeMap<Uuid, Vec<UpstreamPlanTierRecord>>,
    pub report: BackfillReport,
}

#[derive(Debug, Deserialize)]
struct RawContributor {
    upstream_id: String,
    ratio: f64,
    source: String,
    state: ContributorState,
    observed_at_unix_millis: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ContributorState {
    Fresh,
    Stale,
    Missing,
}

#[derive(Debug, Clone, PartialEq)]
struct Candidate {
    upstream_id: Uuid,
    snapshot_at_unix_secs: i64,
    ratio: f64,
    state: ContributorState,
    observed_at_unix_millis: i64,
    window: SubscriptionQuotaWindow,
}

#[derive(Debug, Clone, PartialEq)]
struct ResolvedSnapshot {
    snapshot_at_unix_secs: i64,
    tier_key: Option<String>,
    resolution_source: TierResolutionSource,
    resolved_ratio_snapshot: Option<f64>,
    observed_at_unix_millis: i64,
}

pub(crate) async fn infer_from_storage(
    storage: &dyn Storage,
    now_unix_millis: i64,
) -> Result<BackfillInference, StorageError> {
    let mut after = None;
    let mut winners = BTreeMap::new();
    let mut report = BackfillReport::default();
    loop {
        let page = storage
            .list_pool_quota_contributor_blobs_page(after, PAGE_LIMIT)
            .await?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some((last.snapshot_at_unix_secs, last.window));
        for blob in &page {
            process_blob(blob, &mut winners, &mut report);
        }
    }
    Ok(inference_from_parts(winners, report, now_unix_millis))
}

fn process_blob(
    blob: &PoolQuotaContributorBlob,
    winners: &mut BTreeMap<(Uuid, i64), Candidate>,
    report: &mut BackfillReport,
) {
    report.scanned_blobs += 1;
    parse_blob(blob, winners, report);
}

fn inference_from_parts(
    winners: BTreeMap<(Uuid, i64), Candidate>,
    report: BackfillReport,
    now_unix_millis: i64,
) -> BackfillInference {
    BackfillInference {
        intervals_by_upstream: intervals_by_upstream(winners, now_unix_millis),
        report,
    }
}

fn parse_blob(
    blob: &PoolQuotaContributorBlob,
    winners: &mut BTreeMap<(Uuid, i64), Candidate>,
    report: &mut BackfillReport,
) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&blob.contributors_json) else {
        report.malformed_blobs += 1;
        return;
    };
    let serde_json::Value::Array(entries) = value else {
        report.malformed_blobs += 1;
        return;
    };
    for entry in entries {
        match contributor_from_value(entry, blob) {
            Some(candidate) => insert_winner(candidate, winners),
            None => report.malformed_entries += 1,
        }
    }
}

fn insert_winner(candidate: Candidate, winners: &mut BTreeMap<(Uuid, i64), Candidate>) {
    let key = (candidate.upstream_id, candidate.snapshot_at_unix_secs);
    if winners
        .get(&key)
        .is_none_or(|current| candidate_rank(&candidate) > candidate_rank(current))
    {
        winners.insert(key, candidate);
    }
}

fn contributor_from_value(
    value: serde_json::Value,
    blob: &PoolQuotaContributorBlob,
) -> Option<Candidate> {
    let raw = serde_json::from_value::<RawContributor>(value).ok()?;
    if raw.source != "merged" || !raw.ratio.is_finite() || raw.observed_at_unix_millis < 0 {
        return None;
    }
    Some(Candidate {
        upstream_id: Uuid::parse_str(&raw.upstream_id).ok()?,
        snapshot_at_unix_secs: blob.snapshot_at_unix_secs,
        ratio: raw.ratio,
        state: raw.state,
        observed_at_unix_millis: raw.observed_at_unix_millis,
        window: blob.window,
    })
}

fn candidate_rank(candidate: &Candidate) -> (i64, u8, u8) {
    (
        candidate.observed_at_unix_millis,
        match candidate.state {
            ContributorState::Fresh => 3,
            ContributorState::Stale => 2,
            ContributorState::Missing => 1,
        },
        match candidate.window {
            SubscriptionQuotaWindow::FiveHour => 1,
            SubscriptionQuotaWindow::SevenDay => 2,
            SubscriptionQuotaWindow::SevenDaySonnet
            | SubscriptionQuotaWindow::SevenDayOpus
            | SubscriptionQuotaWindow::Overage
            | SubscriptionQuotaWindow::Unified => 0,
        },
    )
}

fn intervals_by_upstream(
    winners: BTreeMap<(Uuid, i64), Candidate>,
    now_unix_millis: i64,
) -> BTreeMap<Uuid, Vec<UpstreamPlanTierRecord>> {
    let mut snapshots_by_upstream: BTreeMap<Uuid, Vec<ResolvedSnapshot>> = BTreeMap::new();
    for candidate in winners.into_values() {
        let tier = tier_key_from_seed_ratio(candidate.ratio);
        snapshots_by_upstream
            .entry(candidate.upstream_id)
            .or_default()
            .push(ResolvedSnapshot {
                snapshot_at_unix_secs: candidate.snapshot_at_unix_secs,
                tier_key: tier.map(|tier| tier.as_str().to_owned()),
                resolution_source: tier
                    .map(|_| TierResolutionSource::Backfill)
                    .unwrap_or(TierResolutionSource::Unknown),
                resolved_ratio_snapshot: tier.map(|_| candidate.ratio),
                observed_at_unix_millis: candidate.observed_at_unix_millis,
            });
    }
    snapshots_by_upstream
        .into_iter()
        .map(|(upstream_id, snapshots)| {
            (
                upstream_id,
                intervals_from_snapshots(upstream_id, snapshots, now_unix_millis),
            )
        })
        .collect()
}

fn intervals_from_snapshots(
    upstream_id: Uuid,
    snapshots: Vec<ResolvedSnapshot>,
    now_unix_millis: i64,
) -> Vec<UpstreamPlanTierRecord> {
    let mut intervals = Vec::new();
    let mut run: Option<ResolvedSnapshot> = None;
    for snapshot in snapshots {
        match run.as_ref() {
            Some(current) if same_resolution(current, &snapshot) => {}
            Some(current) => {
                intervals.push(record_from_snapshot(
                    upstream_id,
                    current,
                    snapshot.snapshot_at_unix_secs * 1_000,
                ));
                run = Some(snapshot);
            }
            None => run = Some(snapshot),
        }
    }
    if let Some(current) = run {
        intervals.push(record_from_snapshot(upstream_id, &current, now_unix_millis));
    }
    intervals
}

fn same_resolution(left: &ResolvedSnapshot, right: &ResolvedSnapshot) -> bool {
    left.tier_key == right.tier_key
        && left.resolution_source == right.resolution_source
        && left.resolved_ratio_snapshot == right.resolved_ratio_snapshot
}

fn record_from_snapshot(
    upstream_id: Uuid,
    snapshot: &ResolvedSnapshot,
    effective_to_unix_millis: i64,
) -> UpstreamPlanTierRecord {
    UpstreamPlanTierRecord {
        upstream_id,
        organization_uuid: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
        tier_key: snapshot.tier_key.clone(),
        resolution_source: snapshot.resolution_source,
        resolved_ratio_snapshot: snapshot.resolved_ratio_snapshot,
        observed_at_unix_millis: snapshot.observed_at_unix_millis,
        effective_from_unix_millis: snapshot.snapshot_at_unix_secs * 1_000,
        effective_to_unix_millis: Some(effective_to_unix_millis),
        provenance: BACKFILL_PROVENANCE.to_owned(),
        created_at_unix_millis: effective_to_unix_millis,
    }
}
