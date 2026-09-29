use std::collections::{BTreeMap, HashMap};

use super::*;

const MODEL: &str = "tier-aware-model";

fn rate(input_micros: u64, output_micros: u64) -> TierRate {
    TierRate {
        input_per_million_usd: UsdPerMillion::from_micros_usd(input_micros),
        output_per_million_usd: UsdPerMillion::from_micros_usd(output_micros),
    }
}

fn tier_snapshot(with_explicit_tiers: bool) -> CatalogSnapshot {
    let by_tier = if with_explicit_tiers {
        BTreeMap::from([
            ("batch".to_owned(), rate(1_000_000, 4_000_000)),
            ("flex".to_owned(), rate(1_500_000, 6_000_000)),
            ("priority".to_owned(), rate(3_000_000, 12_000_000)),
        ])
    } else {
        BTreeMap::new()
    };
    let pricing = Pricing {
        model: MODEL.to_owned(),
        input_per_million_usd: UsdPerMillion::from_whole_usd(2),
        output_per_million_usd: UsdPerMillion::from_whole_usd(8),
        by_tier,
    };
    let cache_creation_by_tier = with_explicit_tiers.then(|| {
        BTreeMap::from([
            (
                "batch".to_owned(),
                UsdPerMillion::from_micros_usd(2_000_000),
            ),
            ("flex".to_owned(), UsdPerMillion::from_micros_usd(3_000_000)),
            (
                "priority".to_owned(),
                UsdPerMillion::from_micros_usd(6_000_000),
            ),
        ])
    });
    let cache_read_by_tier = with_explicit_tiers.then(|| {
        BTreeMap::from([
            ("batch".to_owned(), UsdPerMillion::from_micros_usd(500_000)),
            ("flex".to_owned(), UsdPerMillion::from_micros_usd(750_000)),
            (
                "priority".to_owned(),
                UsdPerMillion::from_micros_usd(2_000_000),
            ),
        ])
    });

    CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 1,
        models: HashMap::from([(MODEL.to_owned(), pricing)]),
        raw_json: b"{}".to_vec(),
        cache_creation_per_million_usd: HashMap::from([(
            MODEL.to_owned(),
            UsdPerMillion::from_whole_usd(4),
        )]),
        cache_read_per_million_usd: HashMap::from([(
            MODEL.to_owned(),
            UsdPerMillion::from_whole_usd(1),
        )]),
        cache_creation_per_million_usd_by_tier: cache_creation_by_tier
            .map(|rates| HashMap::from([(MODEL.to_owned(), rates)]))
            .unwrap_or_default(),
        cache_read_per_million_usd_by_tier: cache_read_by_tier
            .map(|rates| HashMap::from([(MODEL.to_owned(), rates)]))
            .unwrap_or_default(),
        status: CatalogStatus::Ok,
    }
}

fn total_for(service_tier: Option<&str>, with_explicit_tiers: bool) -> i64 {
    let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
    global_catalog().install_snapshot(tier_snapshot(with_explicit_tiers));
    virtual_cost_micros_full(
        MODEL,
        1_000_000,
        1_000_000,
        1_000_000,
        1_000_000,
        1_000_000,
        service_tier,
    )
    .total_micros
}

#[test]
fn lookup_resolves_tier_prices_and_preserves_discovered_rates() {
    // Given a local catalog with explicit tiers.
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(tier_snapshot(true));

    // When priority pricing is looked up.
    let pricing = catalog
        .lookup(MODEL, Some("priority"))
        .expect("model should be present");

    // Then the base fields are resolved while all discovered tiers remain available.
    assert_eq!(
        pricing.input_per_million_usd,
        UsdPerMillion::from_whole_usd(3)
    );
    assert_eq!(
        pricing.output_per_million_usd,
        UsdPerMillion::from_whole_usd(12)
    );
    assert_eq!(pricing.by_tier.len(), 3);
}

#[test]
fn routing_cache_pricing_resolves_requested_tier_for_every_component() {
    // Given a local catalog with explicit priority rates for model and cache tokens.
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(tier_snapshot(true));

    // When routing cache pricing is looked up for the requested priority tier.
    let pricing = catalog
        .routing_cache_pricing(MODEL, Some("priority"))
        .expect("model should be present");

    // Then every routing estimate rate comes from the tier-aware catalog resolver.
    assert_eq!(
        pricing.input_per_million_usd,
        UsdPerMillion::from_whole_usd(3)
    );
    assert_eq!(
        pricing.cache_creation_5m_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(6))
    );
    assert_eq!(
        pricing.cache_creation_1h_per_million_usd,
        Some(UsdPerMillion::from_micros_usd(6_000_000))
    );
    assert_eq!(
        pricing.cache_read_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(2))
    );
}

#[test]
fn routing_cache_pricing_uses_batch_fallback_for_every_component() {
    // Given a catalog with only base model and cache rates.
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(tier_snapshot(false));

    // When routing cache pricing is looked up for batch.
    let pricing = catalog
        .routing_cache_pricing(MODEL, Some("batch"))
        .expect("model should be present");

    // Then the existing exact-half batch fallback applies to every component.
    assert_eq!(
        pricing.input_per_million_usd,
        UsdPerMillion::from_whole_usd(1)
    );
    assert_eq!(
        pricing.cache_creation_5m_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(2))
    );
    assert_eq!(
        pricing.cache_creation_1h_per_million_usd,
        Some(UsdPerMillion::from_micros_usd(2_000_000))
    );
    assert_eq!(
        pricing.cache_read_per_million_usd,
        Some(UsdPerMillion::from_micros_usd(500_000))
    );
}

#[test]
fn routing_cache_pricing_falls_back_to_base_for_unrecognized_tier() {
    // Given explicit known tiers, when a future tier is requested, then base rates remain usable.
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(tier_snapshot(true));

    let pricing = catalog
        .routing_cache_pricing(MODEL, Some("future-tier"))
        .expect("model should be present");

    assert_eq!(
        pricing.input_per_million_usd,
        UsdPerMillion::from_whole_usd(2)
    );
    assert_eq!(
        pricing.cache_creation_5m_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(4))
    );
    assert_eq!(
        pricing.cache_read_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(1))
    );
}

#[test]
fn estimate_max_uses_batch_fallback_when_catalog_has_no_tier_keys() {
    // Given a local catalog with only base input and output prices.
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(tier_snapshot(false));

    // When a batch maximum cost is estimated.
    let estimate = catalog.estimate_max(MODEL, 1_000_000, 1_000_000, Some("batch"));

    // Then both base prices are halved exactly.
    assert_eq!(estimate, Some(5_000_000));
}

#[test]
fn explicit_priority_rates_are_used_when_service_tier_is_priority() {
    // Given an explicit priority rate, when priority cost is computed, then every component uses it.
    assert_eq!(total_for(Some("priority"), true), 29_000_000);
}

#[test]
fn explicit_batch_rates_are_used_when_service_tier_is_batch() {
    // Given an explicit batch rate, when batch cost is computed, then every component uses it.
    assert_eq!(total_for(Some("batch"), true), 9_500_000);
}

#[test]
fn explicit_flex_rates_are_used_when_service_tier_is_flex() {
    // Given an explicit flex rate, when flex cost is computed, then every component uses it.
    assert_eq!(total_for(Some("flex"), true), 14_250_000);
}

#[test]
fn base_rates_are_used_when_service_tier_is_standard() {
    // Given explicit tiers, when standard is computed, then base component prices remain unchanged.
    assert_eq!(total_for(Some("standard"), true), 19_000_000);
}

#[test]
fn base_rates_are_used_when_service_tier_is_absent() {
    // Given explicit tiers, when no tier is observed, then base component prices remain unchanged.
    assert_eq!(total_for(None, true), 19_000_000);
}

#[test]
fn base_rates_are_used_when_service_tier_is_auto() {
    // Given explicit tiers, when auto is observed, then base component prices remain unchanged.
    assert_eq!(total_for(Some("auto"), true), 19_000_000);
}

#[test]
fn standard_only_is_canonicalized_to_base_pricing() {
    // Given the provider's standard-only request value, when canonicalized, then it selects base pricing.
    assert_eq!(canonical_service_tier(Some("standard_only")), None);
}

#[test]
fn priority_falls_back_to_base_when_catalog_has_no_tier_keys() {
    // Given a sparse catalog, when priority is observed, then all base prices are used.
    assert_eq!(total_for(Some("priority"), false), 19_000_000);
}

#[test]
fn batch_falls_back_to_exact_half_when_catalog_has_no_tier_keys() {
    // Given a sparse catalog, when batch is observed, then every base price is halved exactly.
    assert_eq!(total_for(Some("batch"), false), 9_500_000);
}

#[test]
fn flex_falls_back_to_base_when_catalog_has_no_tier_keys() {
    // Given a sparse catalog, when flex is observed, then all base prices are used.
    assert_eq!(total_for(Some("flex"), false), 19_000_000);
}

#[test]
fn unknown_tier_falls_back_to_base_when_catalog_has_no_matching_keys() {
    // Given a future unknown tier, when no matching catalog key exists, then base prices are used.
    assert_eq!(total_for(Some("ultra"), false), 19_000_000);
}
