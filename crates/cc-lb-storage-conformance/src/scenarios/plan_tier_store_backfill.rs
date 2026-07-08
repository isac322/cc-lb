use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::plan_tiers::{
    BackfillApplyOutcome, PlanTierStore, TierResolutionSource, UpstreamPlanTierRecord,
};
use cc_lb_storage_api::{BackfillApplyCounts, StorageError};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn upstream_tier_backfill_intervals<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PlanTierStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let provenance = "pool_subscription_quota_history_backfill:v1";
        closed_interval_insert_and_idempotency(storage.as_ref(), provenance).await?;
        cap_at_forward_row_keeps_one_open(storage.as_ref(), provenance).await?;
        conflict_different_payload(storage.as_ref(), provenance).await?;
        conflict_overlapping_batch_intervals_leaves_no_partial_rows(storage.as_ref(), provenance)
            .await
    })
    .await
}

async fn closed_interval_insert_and_idempotency(
    storage: &dyn PlanTierStore,
    provenance: &str,
) -> Result<()> {
    let closed_id = Uuid::from_u128(0x3333);
    let intervals = vec![
        backfill_record(closed_id, Some("max_5x"), Some(5.0), 1_000, 2_000),
        backfill_record(closed_id, Some("max_20x"), Some(20.0), 2_000, 3_000),
    ];
    let outcome = storage
        .backfill_upstream_plan_tier_intervals(closed_id, &intervals, 10_000, provenance)
        .await?;
    ensure!(
        outcome
            == BackfillApplyOutcome::Applied(BackfillApplyCounts {
                inserted: 2,
                skipped_zero_dur: 0,
                capped: 0,
            }),
        "closed backfill inserts both intervals: {outcome:?}"
    );
    let during_first = storage.list_upstream_plan_tiers_as_of(1_500).await?;
    let row = during_first
        .iter()
        .find(|row| row.upstream_id == closed_id)
        .expect("backfilled row present as-of");
    ensure!(
        row.tier_key.as_deref() == Some("max_5x"),
        "first tier as-of"
    );
    ensure!(
        row.resolution_source == TierResolutionSource::Backfill,
        "backfill source round-trips"
    );
    let current = storage.list_current_upstream_plan_tiers().await?;
    ensure!(
        current.iter().all(|row| row.upstream_id != closed_id),
        "backfill never inserts an open row"
    );
    let rerun = storage
        .backfill_upstream_plan_tier_intervals(closed_id, &intervals, 10_000, provenance)
        .await?;
    ensure!(
        rerun == BackfillApplyOutcome::Skipped,
        "provenance marker makes per-upstream rerun a no-op"
    );
    Ok(())
}

async fn cap_at_forward_row_keeps_one_open(
    storage: &dyn PlanTierStore,
    provenance: &str,
) -> Result<()> {
    let capped_id = Uuid::from_u128(0x4444);
    storage
        .append_upstream_plan_tier(&forward_record(capped_id, 2_500))
        .await?;
    let intervals = vec![
        backfill_record(capped_id, Some("max_5x"), Some(5.0), 1_000, 2_000),
        backfill_record(capped_id, Some("max_20x"), Some(20.0), 2_000, 4_000),
        backfill_record(capped_id, Some("max_20x"), Some(20.0), 2_500, 3_000),
    ];
    let outcome = storage
        .backfill_upstream_plan_tier_intervals(capped_id, &intervals, 10_000, provenance)
        .await?;
    ensure!(
        outcome
            == BackfillApplyOutcome::Applied(BackfillApplyCounts {
                inserted: 2,
                skipped_zero_dur: 1,
                capped: 1,
            }),
        "backfill caps at earliest forward row and skips zero-duration intervals"
    );
    let current = storage.list_current_upstream_plan_tiers().await?;
    let current_rows = current
        .iter()
        .filter(|row| row.upstream_id == capped_id)
        .count();
    ensure!(current_rows == 1, "one-open-row index remains satisfied");
    Ok(())
}

async fn conflict_different_payload(storage: &dyn PlanTierStore, provenance: &str) -> Result<()> {
    let conflict_id = Uuid::from_u128(0x5555);
    let intervals = vec![
        backfill_record(conflict_id, Some("max_5x"), Some(5.0), 1_000, 2_000),
        backfill_record(conflict_id, Some("max_20x"), Some(20.0), 1_000, 2_000),
    ];
    let conflict = storage
        .backfill_upstream_plan_tier_intervals(conflict_id, &intervals, 10_000, provenance)
        .await;
    ensure!(
        matches!(conflict, Err(StorageError::Conflict { .. })),
        "different payload at the same backfill PK conflicts"
    );
    Ok(())
}

async fn conflict_overlapping_batch_intervals_leaves_no_partial_rows(
    storage: &dyn PlanTierStore,
    provenance: &str,
) -> Result<()> {
    let conflict_id = Uuid::from_u128(0x6666);
    let intervals = vec![
        backfill_record(conflict_id, Some("max_5x"), Some(5.0), 1_000, 3_000),
        backfill_record(conflict_id, Some("max_20x"), Some(20.0), 2_000, 4_000),
    ];
    let conflict = storage
        .backfill_upstream_plan_tier_intervals(conflict_id, &intervals, 10_000, provenance)
        .await;
    ensure!(
        matches!(conflict, Err(StorageError::Conflict { .. })),
        "overlapping backfill intervals with different PKs conflict"
    );
    let rows = storage.list_upstream_plan_tiers_as_of(1_500).await?;
    ensure!(
        rows.iter().all(|row| row.upstream_id != conflict_id),
        "overlap conflict leaves no earlier partial row"
    );
    Ok(())
}

fn backfill_record(
    upstream_id: Uuid,
    tier: Option<&str>,
    ratio_snapshot: Option<f64>,
    from: i64,
    to: i64,
) -> UpstreamPlanTierRecord {
    UpstreamPlanTierRecord {
        upstream_id,
        organization_uuid: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
        tier_key: tier.map(str::to_owned),
        resolution_source: tier
            .map(|_| TierResolutionSource::Backfill)
            .unwrap_or(TierResolutionSource::Unknown),
        resolved_ratio_snapshot: ratio_snapshot,
        observed_at_unix_millis: from + 7,
        effective_from_unix_millis: from,
        effective_to_unix_millis: Some(to),
        provenance: "pool_subscription_quota_history_backfill:v1".to_owned(),
        created_at_unix_millis: 9_999,
    }
}

fn forward_record(upstream_id: Uuid, from: i64) -> UpstreamPlanTierRecord {
    UpstreamPlanTierRecord {
        upstream_id,
        organization_uuid: Some("org-a".to_owned()),
        organization_type: Some("claude_max".to_owned()),
        rate_limit_tier: Some("default_claude_max_20x".to_owned()),
        seat_tier: None,
        tier_key: Some("max_20x".to_owned()),
        resolution_source: TierResolutionSource::Builtin,
        resolved_ratio_snapshot: Some(20.0),
        observed_at_unix_millis: from,
        effective_from_unix_millis: from,
        effective_to_unix_millis: None,
        provenance: "conformance".to_owned(),
        created_at_unix_millis: from,
    }
}
