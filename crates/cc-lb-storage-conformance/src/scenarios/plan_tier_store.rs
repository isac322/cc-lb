use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::plan_tiers::{PlanTierStore, TierResolutionSource, UpstreamPlanTierRecord};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PlanTierStore,
{
    seeded_catalog_visible(Arc::clone(&backend)).await?;
    upstream_tier_scd2(Arc::clone(&backend)).await?;
    upstream_tier_unknown(backend).await
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

scenario!(seeded_catalog_visible, |storage| async move {
    let rows = storage.list_current_plan_tier_ratios().await?;
    let ratio_of = |tier: &str| {
        rows.iter()
            .find(|row| row.tier_key == tier)
            .map(|row| row.pro_relative_ratio)
    };
    ensure!(ratio_of("free") == Some(0.2), "free seed = 0.2");
    ensure!(ratio_of("pro") == Some(1.0), "pro seed = 1.0");
    ensure!(
        ratio_of("team_standard") == Some(1.25),
        "team_standard seed = 1.25"
    );
    ensure!(ratio_of("max_5x") == Some(5.0), "max_5x seed = 5.0");
    ensure!(
        ratio_of("team_premium") == Some(6.25),
        "team_premium seed = 6.25"
    );
    ensure!(ratio_of("max_20x") == Some(20.0), "max_20x seed = 20.0");
    ensure!(
        rows.iter()
            .all(|row| row.effective_to_unix_millis.is_none()),
        "all current rows are open"
    );
    Ok(())
});

scenario!(upstream_tier_scd2, |storage| async move {
    let id = Uuid::from_u128(0x1111);
    storage
        .append_upstream_plan_tier(&upstream_record(
            id,
            Some("free"),
            TierResolutionSource::Builtin,
            Some(0.2),
            500,
        ))
        .await?;
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
