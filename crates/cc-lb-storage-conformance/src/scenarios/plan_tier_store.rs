use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::plan_tiers::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, PlanTierStore, TierResolutionSource,
    UpstreamPlanTierRecord,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PlanTierStore,
{
    seeded_catalog_visible(Arc::clone(&backend)).await?;
    catalog_scd2_change_and_as_of(Arc::clone(&backend)).await?;
    catalog_upsert_idempotent(Arc::clone(&backend)).await?;
    catalog_rejects_invalid_ratio(Arc::clone(&backend)).await?;
    catalog_ordering_conflict(Arc::clone(&backend)).await?;
    override_absent_as_empty_scd2(Arc::clone(&backend)).await?;
    override_idempotent(Arc::clone(&backend)).await?;
    upstream_tier_scd2_and_as_of(Arc::clone(&backend)).await?;
    upstream_tier_unknown(Arc::clone(&backend)).await?;
    super::plan_tier_store_backfill::upstream_tier_backfill_intervals(backend).await
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: PlanTierStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

fn ratio_record(tier: &str, ratio: f64, from: i64) -> PlanTierRatioRecord {
    PlanTierRatioRecord {
        tier_key: tier.to_owned(),
        pro_relative_ratio: ratio,
        effective_from_unix_millis: from,
        effective_to_unix_millis: None,
        provenance: "conformance".to_owned(),
        created_at_unix_millis: from.max(1),
    }
}

fn override_record(
    organization_type: Option<&str>,
    rate_limit_tier: Option<&str>,
    seat_tier: Option<&str>,
    tier: &str,
    from: i64,
) -> MetadataTierMappingOverrideRecord {
    MetadataTierMappingOverrideRecord {
        organization_type: organization_type.map(str::to_owned),
        rate_limit_tier: rate_limit_tier.map(str::to_owned),
        seat_tier: seat_tier.map(str::to_owned),
        tier_key: tier.to_owned(),
        effective_from_unix_millis: from,
        effective_to_unix_millis: None,
        provenance: "conformance".to_owned(),
        created_at_unix_millis: from.max(1),
    }
}

fn upstream_record(
    upstream_id: Uuid,
    tier: Option<&str>,
    source: TierResolutionSource,
    ratio_snapshot: Option<f64>,
    from: i64,
) -> UpstreamPlanTierRecord {
    UpstreamPlanTierRecord {
        upstream_id,
        organization_uuid: Some("org-a".to_owned()),
        organization_type: Some("claude_max".to_owned()),
        rate_limit_tier: Some("default_claude_max_5x".to_owned()),
        seat_tier: None,
        tier_key: tier.map(str::to_owned),
        resolution_source: source,
        resolved_ratio_snapshot: ratio_snapshot,
        observed_at_unix_millis: from,
        effective_from_unix_millis: from,
        effective_to_unix_millis: None,
        provenance: "conformance".to_owned(),
        created_at_unix_millis: from.max(1),
    }
}

fn ratio_of(rows: &[PlanTierRatioRecord], tier: &str) -> Option<f64> {
    rows.iter()
        .find(|row| row.tier_key == tier)
        .map(|row| row.pro_relative_ratio)
}

scenario!(seeded_catalog_visible, |storage| async move {
    let rows = storage.list_current_plan_tier_ratios().await?;
    ensure!(ratio_of(&rows, "pro") == Some(1.0), "pro seed = 1.0");
    ensure!(
        ratio_of(&rows, "team_standard") == Some(1.25),
        "team_standard seed = 1.25"
    );
    ensure!(ratio_of(&rows, "max_5x") == Some(5.0), "max_5x seed = 5.0");
    ensure!(
        ratio_of(&rows, "team_premium") == Some(6.25),
        "team_premium seed = 6.25"
    );
    ensure!(
        ratio_of(&rows, "max_20x") == Some(20.0),
        "max_20x seed = 20.0"
    );
    ensure!(
        rows.iter()
            .all(|row| row.effective_to_unix_millis.is_none()),
        "all current rows are open"
    );
    Ok(())
});

scenario!(catalog_scd2_change_and_as_of, |storage| async move {
    storage
        .upsert_plan_tier_ratio(&ratio_record("max_5x", 7.5, 1_000))
        .await?;
    let current = storage.list_current_plan_tier_ratios().await?;
    ensure!(
        ratio_of(&current, "max_5x") == Some(7.5),
        "current = new ratio"
    );
    let before = storage.list_plan_tier_ratios_as_of(500).await?;
    ensure!(
        ratio_of(&before, "max_5x") == Some(5.0),
        "as-of before = old ratio"
    );
    let after = storage.list_plan_tier_ratios_as_of(2_000).await?;
    ensure!(
        ratio_of(&after, "max_5x") == Some(7.5),
        "as-of after = new ratio"
    );
    Ok(())
});

scenario!(catalog_upsert_idempotent, |storage| async move {
    storage
        .upsert_plan_tier_ratio(&ratio_record("max_20x", 20.0, 5_000))
        .await?;
    let current = storage.list_current_plan_tier_ratios().await?;
    let row = current
        .iter()
        .find(|row| row.tier_key == "max_20x")
        .expect("max_20x present");
    ensure!(
        row.effective_from_unix_millis == 0,
        "idempotent re-upsert of same ratio must NOT open a new interval"
    );
    ensure!(row.pro_relative_ratio == 20.0, "ratio unchanged");
    Ok(())
});

scenario!(catalog_rejects_invalid_ratio, |storage| async move {
    ensure!(
        storage
            .upsert_plan_tier_ratio(&ratio_record("max_5x", f64::NAN, 1_000))
            .await
            .is_err(),
        "NaN ratio rejected"
    );
    ensure!(
        storage
            .upsert_plan_tier_ratio(&ratio_record("max_5x", f64::INFINITY, 1_000))
            .await
            .is_err(),
        "infinite ratio rejected"
    );
    ensure!(
        storage
            .upsert_plan_tier_ratio(&ratio_record("max_5x", 0.0, 1_000))
            .await
            .is_err(),
        "zero ratio rejected"
    );
    ensure!(
        storage
            .upsert_plan_tier_ratio(&ratio_record("max_5x", -1.0, 1_000))
            .await
            .is_err(),
        "negative ratio rejected"
    );
    Ok(())
});

scenario!(catalog_ordering_conflict, |storage| async move {
    ensure!(
        storage
            .upsert_plan_tier_ratio(&ratio_record("max_5x", 9.0, 0))
            .await
            .is_err(),
        "a differing ratio at effective_from <= current open row is a conflict"
    );
    storage
        .upsert_plan_tier_ratio(&ratio_record("max_5x", 9.0, 3_000))
        .await?;
    let current = storage.list_current_plan_tier_ratios().await?;
    ensure!(
        ratio_of(&current, "max_5x") == Some(9.0),
        "forward change applied"
    );
    Ok(())
});

scenario!(override_absent_as_empty_scd2, |storage| async move {
    storage
        .upsert_metadata_tier_override(&override_record(
            Some("claude_team"),
            None,
            None,
            "team_standard",
            1_000,
        ))
        .await?;
    let current = storage.list_current_metadata_tier_overrides().await?;
    let row = current
        .iter()
        .find(|row| row.organization_type.as_deref() == Some("claude_team"))
        .expect("override present");
    ensure!(
        row.rate_limit_tier.is_none() && row.seat_tier.is_none(),
        "absent fields round-trip back to None, not empty string"
    );
    ensure!(row.tier_key == "team_standard", "maps to seeded tier");

    storage
        .upsert_metadata_tier_override(&override_record(
            Some("claude_team"),
            None,
            None,
            "max_5x",
            2_000,
        ))
        .await?;
    let current = storage.list_current_metadata_tier_overrides().await?;
    let row = current
        .iter()
        .find(|row| row.organization_type.as_deref() == Some("claude_team"))
        .expect("override present");
    ensure!(row.tier_key == "max_5x", "SCD2 change reflected in current");

    let before = storage.list_metadata_tier_overrides_as_of(1_500).await?;
    let row = before
        .iter()
        .find(|row| row.organization_type.as_deref() == Some("claude_team"))
        .expect("override present as-of");
    ensure!(row.tier_key == "team_standard", "as-of returns old mapping");
    Ok(())
});

scenario!(override_idempotent, |storage| async move {
    storage
        .upsert_metadata_tier_override(&override_record(
            Some("x"),
            Some("y"),
            Some("z"),
            "pro",
            1_000,
        ))
        .await?;
    storage
        .upsert_metadata_tier_override(&override_record(
            Some("x"),
            Some("y"),
            Some("z"),
            "pro",
            2_000,
        ))
        .await?;
    let current = storage.list_current_metadata_tier_overrides().await?;
    let matching: Vec<_> = current
        .iter()
        .filter(|row| row.organization_type.as_deref() == Some("x"))
        .collect();
    ensure!(
        matching.len() == 1,
        "exactly one open override for the triple"
    );
    ensure!(
        matching[0].effective_from_unix_millis == 1_000,
        "idempotent re-upsert keeps the original interval"
    );
    Ok(())
});

scenario!(upstream_tier_scd2_and_as_of, |storage| async move {
    let id = Uuid::from_u128(0x1111);
    storage
        .append_upstream_plan_tier(&upstream_record(
            id,
            Some("max_5x"),
            TierResolutionSource::Builtin,
            Some(5.0),
            1_000,
        ))
        .await?;
    storage
        .append_upstream_plan_tier(&upstream_record(
            id,
            Some("max_5x"),
            TierResolutionSource::Builtin,
            Some(5.0),
            2_000,
        ))
        .await?;
    let current = storage.list_current_upstream_plan_tiers().await?;
    let row = current
        .iter()
        .find(|row| row.upstream_id == id)
        .expect("upstream present");
    ensure!(
        row.effective_from_unix_millis == 1_000,
        "idempotent re-append keeps the original interval"
    );
    ensure!(row.tier_key.as_deref() == Some("max_5x"), "tier unchanged");

    storage
        .append_upstream_plan_tier(&upstream_record(
            id,
            Some("max_20x"),
            TierResolutionSource::Builtin,
            Some(20.0),
            3_000,
        ))
        .await?;
    let current = storage.list_current_upstream_plan_tiers().await?;
    let row = current
        .iter()
        .find(|row| row.upstream_id == id)
        .expect("upstream present");
    ensure!(
        row.tier_key.as_deref() == Some("max_20x"),
        "SCD2 change reflected"
    );

    let before = storage.list_upstream_plan_tiers_as_of(2_500).await?;
    let row = before
        .iter()
        .find(|row| row.upstream_id == id)
        .expect("upstream present as-of");
    ensure!(
        row.tier_key.as_deref() == Some("max_5x"),
        "as-of returns old tier"
    );
    Ok(())
});

scenario!(upstream_tier_unknown, |storage| async move {
    let id = Uuid::from_u128(0x2222);
    storage
        .append_upstream_plan_tier(&upstream_record(
            id,
            None,
            TierResolutionSource::Unknown,
            None,
            1_000,
        ))
        .await?;
    let current = storage.list_current_upstream_plan_tiers().await?;
    let row = current
        .iter()
        .find(|row| row.upstream_id == id)
        .expect("unknown upstream present");
    ensure!(row.tier_key.is_none(), "unknown carries no tier_key");
    ensure!(
        row.resolution_source == TierResolutionSource::Unknown,
        "resolution source is unknown"
    );
    Ok(())
});
