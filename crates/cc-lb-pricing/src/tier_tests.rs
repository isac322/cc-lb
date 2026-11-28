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
        native: None,
        model: MODEL.to_owned(),
        input_per_million_usd: UsdPerMillion::from_whole_usd(2),
        output_per_million_usd: UsdPerMillion::from_whole_usd(8),
        input_image_per_million_usd: None,
        output_image_per_million_usd: None,
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
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    }
}

fn total_for(service_tier: Option<&str>, with_explicit_tiers: bool) -> i64 {
    let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
    global_catalog().install_snapshot(tier_snapshot(with_explicit_tiers));
    crate::cost::virtual_cost_micros_full(
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

#[test]
fn routing_cache_pricing_for_prompt_tokens_applies_threshold_bands_and_requires_count() {
    let catalog = PriceCatalog::new_empty();
    let mut snapshot = tier_snapshot(false);
    let mut bands = BTreeMap::new();
    bands.insert(
        100_000,
        crate::ThresholdBand {
            default: crate::ThresholdRate {
                input_per_million_usd: Some(UsdPerMillion::from_whole_usd(10)),
                output_per_million_usd: Some(UsdPerMillion::from_whole_usd(40)),
                cache_creation_per_million_usd: Some(UsdPerMillion::from_whole_usd(20)),
                cache_read_per_million_usd: Some(UsdPerMillion::from_whole_usd(5)),
            },
            by_tier: BTreeMap::new(),
        },
    );
    snapshot.above_tokens_rates.insert(MODEL.to_owned(), bands);
    catalog.install_snapshot(snapshot);

    assert!(catalog.has_routing_cache_threshold_bands(MODEL, None));
    assert!(
        catalog
            .routing_cache_pricing_for_prompt_tokens(MODEL, None, None)
            .is_none(),
        "tiered models cannot fabricate base routing rates without a gross prompt token count"
    );

    let at_boundary = catalog
        .routing_cache_pricing_for_prompt_tokens(MODEL, None, Some(100_000))
        .expect("at boundary resolves base band");
    assert_eq!(
        at_boundary.input_per_million_usd,
        UsdPerMillion::from_whole_usd(2)
    );
    assert_eq!(
        at_boundary.cache_creation_5m_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(4))
    );
    assert_eq!(
        at_boundary.cache_creation_1h_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(4))
    );
    assert_eq!(
        at_boundary.cache_read_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(1))
    );

    let above_boundary = catalog
        .routing_cache_pricing_for_prompt_tokens(MODEL, None, Some(120_000))
        .expect("above boundary resolves threshold band");
    assert_eq!(
        above_boundary.input_per_million_usd,
        UsdPerMillion::from_whole_usd(10)
    );
    assert_eq!(
        above_boundary.cache_creation_5m_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(20))
    );
    assert_eq!(
        above_boundary.cache_creation_1h_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(20))
    );
    assert_eq!(
        above_boundary.cache_read_per_million_usd,
        Some(UsdPerMillion::from_whole_usd(5))
    );

    // Output-only threshold bands do not affect input/cache pricing, so None resolves base rates.
    let output_only_catalog = PriceCatalog::new_empty();
    let mut output_only_snapshot = tier_snapshot(false);
    output_only_snapshot.above_tokens_rates.insert(
        MODEL.to_owned(),
        BTreeMap::from([(
            200_000,
            crate::ThresholdBand {
                default: crate::ThresholdRate {
                    input_per_million_usd: None,
                    output_per_million_usd: Some(UsdPerMillion::from_whole_usd(40)),
                    cache_creation_per_million_usd: None,
                    cache_read_per_million_usd: None,
                },
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    crate::ThresholdRate {
                        input_per_million_usd: Some(UsdPerMillion::from_whole_usd(12)),
                        output_per_million_usd: None,
                        cache_creation_per_million_usd: None,
                        cache_read_per_million_usd: None,
                    },
                )]),
            },
        )]),
    );
    output_only_catalog.install_snapshot(output_only_snapshot);
    assert!(!output_only_catalog.has_routing_cache_threshold_bands(MODEL, None));
    assert_eq!(
        output_only_catalog.routing_cache_pricing_for_prompt_tokens(MODEL, None, None),
        output_only_catalog.routing_cache_pricing_for_prompt_tokens(MODEL, None, Some(250_000))
    );
    assert!(output_only_catalog.has_routing_cache_threshold_bands(MODEL, Some("priority")));
    assert!(
        output_only_catalog
            .routing_cache_pricing_for_prompt_tokens(MODEL, Some("priority"), None)
            .is_none()
    );
    let priority_above = output_only_catalog
        .routing_cache_pricing_for_prompt_tokens(MODEL, Some("priority"), Some(250_000))
        .expect("priority tier threshold band resolves above threshold");
    assert_eq!(
        priority_above.input_per_million_usd,
        UsdPerMillion::from_whole_usd(12)
    );

    // Flat models still resolve without a prompt token count.
    let flat_catalog = PriceCatalog::new_empty();
    flat_catalog.install_snapshot(tier_snapshot(true));
    assert!(!flat_catalog.has_routing_cache_threshold_bands(MODEL, Some("priority")));
    assert_eq!(
        flat_catalog.routing_cache_pricing_for_prompt_tokens(MODEL, Some("priority"), None),
        flat_catalog.routing_cache_pricing_for_prompt_tokens(
            MODEL,
            Some("priority"),
            Some(120_000)
        )
    );
}

#[test]
fn keepalive_cache_rates_for_prompt_tokens_checks_selected_ttl_sensitivity() {
    use cc_lb_storage_api::CacheTtl;

    // 1. Input-only band: 5m savings stay computable without a prompt count, while 1h
    //    depends on input pricing and still requires a count.
    let input_only_catalog = PriceCatalog::new_empty();
    let mut input_only_snap = CatalogSnapshot::empty_cost_disabled();
    input_only_snap.status = CatalogStatus::Ok;
    input_only_snap.models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
            native: None,
        },
    );
    input_only_snap
        .cache_creation_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(3_750_000));
    input_only_snap
        .cache_read_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(300_000));
    input_only_snap.above_tokens_rates.insert(
        MODEL.to_owned(),
        BTreeMap::from([(
            200_000,
            crate::ThresholdBand {
                default: crate::ThresholdRate {
                    input_per_million_usd: Some(UsdPerMillion::from_whole_usd(6)),
                    output_per_million_usd: None,
                    cache_creation_per_million_usd: None,
                    cache_read_per_million_usd: None,
                },
                by_tier: BTreeMap::new(),
            },
        )]),
    );
    input_only_catalog.install_snapshot(input_only_snap);

    let expected_5m = (
        UsdPerMillion::from_micros_usd(3_750_000),
        UsdPerMillion::from_micros_usd(300_000),
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl5m,
            None
        ),
        Some(expected_5m)
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl5m,
            Some(200_000),
        ),
        Some(expected_5m)
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl5m,
            Some(250_000),
        ),
        Some(expected_5m)
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl1h,
            None
        ),
        None
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl1h,
            Some(200_000),
        ),
        Some((
            UsdPerMillion::from_whole_usd(6),
            UsdPerMillion::from_micros_usd(300_000),
        ))
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl1h,
            Some(250_000),
        ),
        Some((
            UsdPerMillion::from_whole_usd(12),
            UsdPerMillion::from_micros_usd(300_000),
        ))
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl30m,
            Some(250_000),
        ),
        None
    );
    assert_eq!(
        input_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl12h,
            Some(250_000),
        ),
        None
    );

    // 2. Cache-read-only threshold band gates both 5m and 1h when count is missing.
    let read_only_catalog = PriceCatalog::new_empty();
    let mut read_only_snap = tier_snapshot(false);
    read_only_snap.above_tokens_rates.insert(
        MODEL.to_owned(),
        BTreeMap::from([(
            200_000,
            crate::ThresholdBand {
                default: crate::ThresholdRate {
                    input_per_million_usd: None,
                    output_per_million_usd: None,
                    cache_creation_per_million_usd: None,
                    cache_read_per_million_usd: Some(UsdPerMillion::from_whole_usd(2)),
                },
                by_tier: BTreeMap::new(),
            },
        )]),
    );
    read_only_catalog.install_snapshot(read_only_snap);
    assert_eq!(
        read_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl5m,
            None
        ),
        None
    );
    assert_eq!(
        read_only_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl1h,
            None
        ),
        None
    );

    // 3. Full cache+input threshold band (Haiku-style) requires a count for both 5m and 1h
    //    and switches strictly above the threshold.
    let haiku_catalog = PriceCatalog::new_empty();
    let mut haiku_snap = CatalogSnapshot::empty_cost_disabled();
    haiku_snap.status = CatalogStatus::Ok;
    haiku_snap.models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_micros_usd(100_000),
            output_per_million_usd: UsdPerMillion::from_micros_usd(500_000),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
            native: None,
        },
    );
    haiku_snap
        .cache_creation_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(125_000));
    haiku_snap
        .cache_read_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(10_000));
    haiku_snap.above_tokens_rates.insert(
        MODEL.to_owned(),
        BTreeMap::from([(
            100_000,
            crate::ThresholdBand {
                default: crate::ThresholdRate {
                    input_per_million_usd: Some(UsdPerMillion::from_micros_usd(500_000)),
                    output_per_million_usd: Some(UsdPerMillion::from_micros_usd(2_500_000)),
                    cache_creation_per_million_usd: Some(UsdPerMillion::from_micros_usd(625_000)),
                    cache_read_per_million_usd: Some(UsdPerMillion::from_micros_usd(50_000)),
                },
                by_tier: BTreeMap::new(),
            },
        )]),
    );
    haiku_catalog.install_snapshot(haiku_snap);
    assert_eq!(
        haiku_catalog.keepalive_cache_rates_for_prompt_tokens(MODEL, None, CacheTtl::Ttl5m, None),
        None
    );
    assert_eq!(
        haiku_catalog.keepalive_cache_rates_for_prompt_tokens(MODEL, None, CacheTtl::Ttl1h, None),
        None
    );
    assert_eq!(
        haiku_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl5m,
            Some(100_000),
        ),
        Some((
            UsdPerMillion::from_micros_usd(125_000),
            UsdPerMillion::from_micros_usd(10_000),
        ))
    );
    assert_eq!(
        haiku_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl5m,
            Some(100_001),
        ),
        Some((
            UsdPerMillion::from_micros_usd(625_000),
            UsdPerMillion::from_micros_usd(50_000),
        ))
    );
    assert_eq!(
        haiku_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl1h,
            Some(100_000),
        ),
        Some((
            UsdPerMillion::from_micros_usd(200_000),
            UsdPerMillion::from_micros_usd(10_000),
        ))
    );
    assert_eq!(
        haiku_catalog.keepalive_cache_rates_for_prompt_tokens(
            MODEL,
            None,
            CacheTtl::Ttl1h,
            Some(100_001),
        ),
        Some((
            UsdPerMillion::from_micros_usd(1_000_000),
            UsdPerMillion::from_micros_usd(50_000),
        ))
    );
}

#[test]
fn canonical_service_tier_treats_base_aliases_as_none_while_preserving_catalog_override_keys() {
    for base in ["", " ", "standard", "STANDARD", "standard_only", "auto"] {
        assert_eq!(canonical_service_tier(Some(base)), None, "base={base:?}");
    }
    assert_eq!(canonical_service_tier(None), None);
    for explicit in ["default", "Default", "unspecified", "priority", "ultrafast"] {
        assert_eq!(
            canonical_service_tier(Some(explicit)).as_deref(),
            Some(explicit.trim().to_ascii_lowercase().as_str())
        );
    }
    assert_eq!(
        canonical_service_tier(Some(" priority ")).as_deref(),
        Some("priority")
    );
    assert_eq!(
        canonical_service_tier(Some("batches")).as_deref(),
        Some("batch")
    );
}

#[test]
fn default_tier_uses_base_pricing_while_priority_applies_priority_rates() {
    let base_total = total_for(None, true);
    assert_eq!(total_for(Some("default"), true), base_total);
    assert_eq!(total_for(Some("auto"), true), base_total);
    assert_eq!(total_for(Some("unspecified"), true), base_total);
    assert!(total_for(Some("priority"), true) > base_total);
    // An uncataloged tier such as `ultrafast` falls back gracefully to base pricing.
    assert_eq!(total_for(Some("ultrafast"), true), base_total);
}

#[test]
fn explicit_default_and_unspecified_catalog_rates_override_base_rates() {
    let raw = serde_json::json!({
        "override-tier-model": {
            "litellm_provider": "openai",
            "input_cost_per_token": 0.000002,
            "output_cost_per_token": 0.000008,
            "input_cost_per_token_default": 0.000003,
            "output_cost_per_token_default": 0.000010,
            "input_cost_per_token_unspecified": 0.000001,
            "output_cost_per_token_unspecified": 0.000004,
            "cache_read_input_token_cost": 0.0000005,
            "cache_read_input_token_cost_default": 0.00000075
        }
    });
    let snapshot = crate::catalog_parser::parse_litellm_json(
        raw.to_string().as_bytes(),
        &cc_lb_clock::SystemClock,
    )
    .expect("catalog parses");

    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(snapshot.clone());

    assert_eq!(
        catalog.estimate_max("override-tier-model", 1_000_000, 1_000_000, None),
        Some(10_000_000)
    );
    assert_eq!(
        catalog.estimate_max("override-tier-model", 1_000_000, 1_000_000, Some("default")),
        Some(13_000_000)
    );
    assert_eq!(
        catalog.estimate_max(
            "override-tier-model",
            1_000_000,
            1_000_000,
            Some("unspecified")
        ),
        Some(5_000_000)
    );

    let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
    global_catalog().install_snapshot(snapshot);
    let default_cost = crate::cost::virtual_cost_micros_full(
        "override-tier-model",
        1_000_000,
        1_000_000,
        0,
        0,
        1_000_000,
        Some("default"),
    );
    assert_eq!(default_cost.input_micros, 3_000_000);
    assert_eq!(default_cost.output_micros, 10_000_000);
    assert_eq!(default_cost.cache_read_micros, 750_000);
    assert_eq!(default_cost.total_micros, 13_750_000);

    let unspecified_cost = crate::cost::virtual_cost_micros_full(
        "override-tier-model",
        1_000_000,
        1_000_000,
        0,
        0,
        1_000_000,
        Some("unspecified"),
    );
    assert_eq!(unspecified_cost.input_micros, 1_000_000);
    assert_eq!(unspecified_cost.output_micros, 4_000_000);
    assert_eq!(unspecified_cost.cache_read_micros, 500_000);
    assert_eq!(unspecified_cost.total_micros, 5_500_000);
}
