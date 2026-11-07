use crate::tier_resolver::{apply_threshold_bands, resolve_optional_price, resolve_tier_rate};
use crate::{
    ComputedCostBreakdown, PricingStatus, UsdPerMillion, canonical_service_tier, global_catalog,
    normalize_model_id,
};

/// Multiplier the provider applies to base input pricing for a 1-hour cache write.
const CACHE_CREATION_1H_BASE_INPUT_MULTIPLIER: u64 = 2;

/// 1-hour cache-write price, derived from the base input price of whichever pricing path is in
/// use (catalog tier rate, or the Anthropic family fallback in `cc-lb-engine`).
///
/// Unlike the older `1.6 * price_5m` form, `2 * base_input` does not structurally guarantee
/// `price_1h > price_5m`: a catalog whose 5m rate exceeds 2x base input would invert TTL cost
/// ordering. That is data hygiene, not an invariant clamped here.
pub fn cache_creation_1h_micros_from_input(input_micros_per_million: u64) -> u64 {
    input_micros_per_million.saturating_mul(CACHE_CREATION_1H_BASE_INPUT_MULTIPLIER)
}

pub(crate) fn resolved_cache_creation_1h_price(base_input_price: UsdPerMillion) -> UsdPerMillion {
    UsdPerMillion::from_micros_usd(cache_creation_1h_micros_from_input(
        base_input_price.as_micros_usd(),
    ))
}

pub fn virtual_cost_micros_full(
    model: &str,
    input: u64,
    output: u64,
    cache_creation_5m_input: u64,
    cache_creation_1h_input: u64,
    cache_read_input: u64,
    service_tier: Option<&str>,
) -> ComputedCostBreakdown {
    virtual_cost_micros_full_with_context(
        model,
        input,
        output,
        cache_creation_5m_input.saturating_add(cache_creation_1h_input),
        cache_creation_5m_input,
        cache_creation_1h_input,
        cache_read_input,
        service_tier,
    )
    .unwrap_or_else(ComputedCostBreakdown::unknown)
}

pub fn virtual_cost_micros_full_with_context(
    model: &str,
    input: u64,
    output: u64,
    cache_creation_input_tokens: u64,
    cache_creation_5m_input: u64,
    cache_creation_1h_input: u64,
    cache_read_input: u64,
    service_tier: Option<&str>,
) -> Option<ComputedCostBreakdown> {
    virtual_cost_micros_full_with_context_and_images(
        model,
        input,
        output,
        0,
        0,
        cache_creation_input_tokens,
        cache_creation_5m_input,
        cache_creation_1h_input,
        cache_read_input,
        service_tier,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn virtual_cost_micros_full_with_context_and_images(
    model: &str,
    input: u64,
    output: u64,
    input_image_tokens: u64,
    output_image_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_creation_5m_input: u64,
    cache_creation_1h_input: u64,
    cache_read_input: u64,
    service_tier: Option<&str>,
) -> Option<ComputedCostBreakdown> {
    let normalized = normalize_model_id(model);
    let snapshot = global_catalog().current();
    let Some((model_key, pricing)) = snapshot
        .models
        .get_key_value(model)
        .or_else(|| snapshot.models.get_key_value(&normalized))
    else {
        record_missing_price_field(&normalized, "model");
        return None;
    };

    let tier = canonical_service_tier(service_tier);
    let base_rate = resolve_tier_rate(pricing, tier.as_deref());
    let mut input_price = base_rate.input_per_million_usd;
    let mut output_price = base_rate.output_per_million_usd;
    let mut cache_creation_price = resolve_optional_price(
        snapshot
            .cache_creation_per_million_usd
            .get(model_key)
            .copied(),
        snapshot
            .cache_creation_per_million_usd_by_tier
            .get(model_key),
        tier.as_deref(),
    );
    let mut cache_read_price = resolve_optional_price(
        snapshot.cache_read_per_million_usd.get(model_key).copied(),
        snapshot.cache_read_per_million_usd_by_tier.get(model_key),
        tier.as_deref(),
    );

    let gross_input_tokens = input
        .saturating_add(cache_read_input)
        .saturating_add(cache_creation_input_tokens);
    apply_threshold_bands(
        snapshot.above_tokens_rates.get(model_key),
        gross_input_tokens,
        tier.as_deref(),
        &mut input_price,
        &mut output_price,
        Some(&mut cache_creation_price),
        Some(&mut cache_read_price),
    );
    let input_image_tokens = input_image_tokens.min(input);
    let output_image_tokens = output_image_tokens.min(output);
    let input_image_price = pricing
        .input_image_per_million_usd
        .unwrap_or(input_price);
    let output_image_price = pricing
        .output_image_per_million_usd
        .unwrap_or(output_price);


    let generic_creation = cache_creation_input_tokens
        .saturating_sub(cache_creation_5m_input.saturating_add(cache_creation_1h_input));
    let input_micros = component_cost(input.saturating_sub(input_image_tokens), input_price)
        .saturating_add(component_cost(input_image_tokens, input_image_price))
        .saturating_add(component_cost(
            generic_creation,
            cache_creation_price.unwrap_or(input_price),
        ));
    let output_micros = component_cost(output.saturating_sub(output_image_tokens), output_price)
        .saturating_add(component_cost(output_image_tokens, output_image_price));

    let (cc_5m_micros, cc_1h_micros) = if let Some(price_5m) = cache_creation_price {
        let price_1h = resolved_cache_creation_1h_price(input_price);
        (
            component_cost(cache_creation_5m_input, price_5m),
            component_cost(cache_creation_1h_input, price_1h),
        )
    } else {
        if cache_creation_5m_input.saturating_add(cache_creation_1h_input) > 0 {
            record_missing_cache_field(&normalized, "cache_creation_per_million_usd");
        }
        (0, 0)
    };

    let cr_micros = cache_read_price
        .map(|price| component_cost(cache_read_input, price))
        .unwrap_or_else(|| {
            if cache_read_input > 0 {
                record_missing_cache_field(&normalized, "cache_read_per_million_usd");
            }
            0
        });

    let to_i64 = |v: u128| -> i64 { v.try_into().unwrap_or(i64::MAX) };
    let input_i = to_i64(input_micros);
    let output_i = to_i64(output_micros);
    let cc5_i = to_i64(cc_5m_micros);
    let cc1_i = to_i64(cc_1h_micros);
    let cr_i = to_i64(cr_micros);
    let total = input_i
        .saturating_add(output_i)
        .saturating_add(cc5_i)
        .saturating_add(cc1_i)
        .saturating_add(cr_i);

    Some(ComputedCostBreakdown {
        input_micros: input_i,
        output_micros: output_i,
        cache_creation_5m_micros: cc5_i,
        cache_creation_1h_micros: cc1_i,
        cache_read_micros: cr_i,
        total_micros: total,
        pricing_status: PricingStatus::Known,
    })
}

fn component_cost(tokens: u64, price: UsdPerMillion) -> u128 {
    u128::from(tokens) * u128::from(price.as_micros_usd()) / 1_000_000
}

fn record_missing_cache_field(model: &str, field: &'static str) {
    record_missing_price_field(model, field);
}

pub(crate) fn record_missing_price_field(model: &str, field: &'static str) {
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => model.to_owned(),
        "field" => field
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use crate::{
        CatalogSnapshot, CatalogStatus, GLOBAL_TEST_LOCK, LocalCacheInputEstimate, Pricing,
        ThresholdBand, ThresholdRate, TierRate,
    };

    #[test]
    fn one_hour_cache_write_uses_base_input_when_five_minute_price_differs() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "cache-1h-base-input-test";
        let base_input_price = UsdPerMillion::from_micros_usd(3_000_000);
        let nonstandard_5m_price = UsdPerMillion::from_micros_usd(10_000_000);
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: base_input_price,
                    output_per_million_usd: UsdPerMillion::from_micros_usd(15_000_000),
                    input_image_per_million_usd: None,
                    output_image_per_million_usd: None,
                    by_tier: BTreeMap::new(),
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::from([(
                model.to_owned(),
                nonstandard_5m_price,
            )]),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        });

        let breakdown = virtual_cost_micros_full(model, 0, 0, 0, 1_000_000, 0, None);

        assert_eq!(breakdown.cache_creation_1h_micros, 6_000_000);
    }
    #[test]
    fn modality_final_cost_uses_image_rates_tiers_and_cache_pricing() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "modality-final-cost-test";
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                    output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                    input_image_per_million_usd: Some(UsdPerMillion::from_whole_usd(8)),
                    output_image_per_million_usd: Some(UsdPerMillion::from_whole_usd(32)),
                    by_tier: BTreeMap::from([(
                        "priority".to_owned(),
                        TierRate {
                            input_per_million_usd: UsdPerMillion::from_whole_usd(5),
                            output_per_million_usd: UsdPerMillion::from_whole_usd(10),
                        },
                    )]),
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::from([(
                model.to_owned(),
                UsdPerMillion::from_micros_usd(3_750_000),
            )]),
            cache_read_per_million_usd: HashMap::from([(
                model.to_owned(),
                UsdPerMillion::from_micros_usd(1_250_000),
            )]),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        });

        let breakdown = virtual_cost_micros_full_with_context_and_images(
            model,
            1_011,
            3,
            1_000,
            3,
            100,
            40,
            60,
            50,
            Some("priority"),
        )
        .expect("known model");

        assert_eq!(breakdown.input_micros, 8_055);
        assert_eq!(breakdown.output_micros, 96);
        assert_eq!(breakdown.cache_creation_5m_micros, 150);
        assert_eq!(breakdown.cache_creation_1h_micros, 600);
        assert_eq!(breakdown.cache_read_micros, 62);
        assert_eq!(breakdown.total_micros, 8_963);

        let clamped = virtual_cost_micros_full_with_context_and_images(
            model,
            1_011,
            3,
            5_000,
            99,
            100,
            40,
            60,
            50,
            Some("priority"),
        )
        .expect("known model");
        assert_eq!(clamped.input_micros, 8_088);
        assert_eq!(clamped.output_micros, 96);
        assert_eq!(clamped.cache_creation_5m_micros, 150);
        assert_eq!(clamped.cache_creation_1h_micros, 600);
        assert_eq!(clamped.cache_read_micros, 62);
    }

    #[test]
    fn modality_final_cost_matches_context_cost_when_image_tokens_are_zero() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "modality-text-equivalence-test";
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                    output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                    input_image_per_million_usd: Some(UsdPerMillion::from_whole_usd(99)),
                    output_image_per_million_usd: Some(UsdPerMillion::from_whole_usd(99)),
                    by_tier: BTreeMap::new(),
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        });

        let context = virtual_cost_micros_full_with_context(
            model, 11, 3, 0, 0, 0, 0, None,
        )
        .expect("known model");
        let modality = virtual_cost_micros_full_with_context_and_images(
            model, 11, 3, 0, 0, 0, 0, 0, 0, None,
        )
        .expect("known model");
        assert_eq!(modality, context);
    }


    #[test]
    fn threshold_rates_activate_only_above_the_cliff_and_inherit_sparse_fields() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "threshold-cliff-test";
        let base_input = UsdPerMillion::from_whole_usd(1);
        let base_output = UsdPerMillion::from_whole_usd(2);
        let mut bands = BTreeMap::new();
        bands.insert(
            200_000,
            ThresholdBand {
                default: ThresholdRate {
                    output_per_million_usd: Some(UsdPerMillion::from_whole_usd(4)),
                    ..ThresholdRate::default()
                },
                by_tier: BTreeMap::new(),
            },
        );
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: base_input,
                    output_per_million_usd: base_output,
                    input_image_per_million_usd: None,
                    output_image_per_million_usd: None,
                    by_tier: BTreeMap::new(),
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::from([(model.to_owned(), bands)]),
            status: CatalogStatus::Ok,
        });

        let at_cliff =
            virtual_cost_micros_full_with_context(model, 200_000, 1_000_000, 0, 0, 0, 0, None)
                .expect("known model");
        let above_cliff =
            virtual_cost_micros_full_with_context(model, 200_001, 1_000_000, 0, 0, 0, 0, None)
                .expect("known model");
        assert_eq!(at_cliff.input_micros, 200_000);
        assert_eq!(at_cliff.output_micros, 2_000_000);
        assert_eq!(above_cliff.input_micros, 200_001);
        assert_eq!(above_cliff.output_micros, 4_000_000);

        let above_output =
            virtual_cost_micros_full_with_context(model, 0, 1_000_000, 200_001, 0, 0, 0, None)
                .expect("known model");
        assert_eq!(above_output.output_micros, 4_000_000);
    }

    #[test]
    fn sparse_tier_threshold_inherits_default_threshold_components_per_field() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "sparse-tier-threshold-test";
        let mut by_tier = BTreeMap::new();
        by_tier.insert(
            "priority".to_owned(),
            crate::TierRate {
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(12),
            },
        );
        let mut band_by_tier = BTreeMap::new();
        band_by_tier.insert(
            "priority".to_owned(),
            ThresholdRate {
                output_per_million_usd: Some(UsdPerMillion::from_whole_usd(20)),
                ..ThresholdRate::default()
            },
        );
        let mut bands = BTreeMap::new();
        bands.insert(
            200_000,
            ThresholdBand {
                default: ThresholdRate {
                    input_per_million_usd: Some(UsdPerMillion::from_whole_usd(99)),
                    cache_creation_per_million_usd: Some(UsdPerMillion::from_whole_usd(10)),
                    cache_read_per_million_usd: Some(UsdPerMillion::from_whole_usd(5)),
                    ..ThresholdRate::default()
                },
                by_tier: band_by_tier,
            },
        );
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                    output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                    input_image_per_million_usd: None,
                    output_image_per_million_usd: None,
                    by_tier,
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::from([(
                model.to_owned(),
                UsdPerMillion::from_whole_usd(1),
            )]),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::from([(
                model.to_owned(),
                BTreeMap::from([("priority".to_owned(), UsdPerMillion::from_whole_usd(2))]),
            )]),
            above_tokens_rates: HashMap::from([(model.to_owned(), bands)]),
            status: CatalogStatus::Ok,
        });

        let priority_above = virtual_cost_micros_full_with_context(
            model,
            300_000,
            1_000_000,
            0,
            0,
            0,
            100_000,
            Some("priority"),
        )
        .expect("known model");
        assert_eq!(priority_above.input_micros, 29_700_000);
        assert_eq!(priority_above.output_micros, 20_000_000);
        assert_eq!(priority_above.cache_read_micros, 500_000);

        let estimated_at_cliff = global_catalog()
            .estimate_max(model, 200_000, 0, Some("priority"))
            .expect("estimate at cliff");
        let estimated_above_cliff = global_catalog()
            .estimate_max(model, 300_000, 0, Some("priority"))
            .expect("estimate above cliff");
        assert_eq!(estimated_at_cliff, 600_000);
        assert_eq!(estimated_above_cliff, 29_700_000);
        // Explicit cache token bound (150k input + 60k cache = 210k > 200k cliff):
        // reserves both 150k input and 60k cache at the $99/M cliff rate => 20,790,000 micros.
        let estimated_explicit_cache_crossing_cliff = global_catalog()
            .estimate_max_with_cache(model, 150_000, 0, 60_000, false, Some("priority"))
            .expect("estimate explicit cache crossing cliff");
        assert_eq!(estimated_explicit_cache_crossing_cliff, 20_790_000);

        // Opaque/external cache marker below body cliff (50k input, worst-case bound pads to
        // OPAQUE_CACHE_WORST_CASE_INPUT_TOKENS = 1,048,576 total tokens = 50k input + 998,576 cache):
        // reserves 1,048,576 tokens at $99/M => 103,809,024 micros.
        let estimated_cache_bearing_below_cliff = global_catalog()
            .estimate_max_with_cache(model, 50_000, 0, 0, true, Some("priority"))
            .expect("estimate cache-bearing below cliff");
        assert_eq!(estimated_cache_bearing_below_cliff, 103_809_024);
    }

    #[test]
    fn generic_cache_creation_tokens_use_active_creation_or_input_price() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "generic-cache-creation-test";
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                    output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                    input_image_per_million_usd: None,
                    output_image_per_million_usd: None,
                    by_tier: BTreeMap::new(),
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        });

        let breakdown = virtual_cost_micros_full_with_context(model, 0, 0, 100, 0, 0, 0, None)
            .expect("known model");
        assert_eq!(breakdown.input_micros, 300);
        assert_eq!(breakdown.total_micros, 300);
    }

    #[test]
    fn keepalive_misclassified_one_hour_write_uses_five_minute_price() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "keepalive-nested-1h-accounting-test";
        global_catalog().install_snapshot(CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: UsdPerMillion::from_micros_usd(3_000_000),
                    output_per_million_usd: UsdPerMillion::from_micros_usd(15_000_000),
                    input_image_per_million_usd: None,
                    output_image_per_million_usd: None,
                    by_tier: BTreeMap::new(),
                },
            )]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::from([(
                model.to_owned(),
                UsdPerMillion::from_micros_usd(3_750_000),
            )]),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        });

        let one_hour_tokens = 42;
        let misclassified =
            virtual_cost_micros_full_with_context(model, 0, 0, one_hour_tokens, 0, 0, 0, None)
                .expect("controlled pricing catalog has the test model");
        let correctly_split = virtual_cost_micros_full_with_context(
            model,
            0,
            0,
            one_hour_tokens,
            0,
            one_hour_tokens,
            0,
            None,
        )
        .expect("controlled pricing catalog has the test model");

        assert_eq!(misclassified.input_micros, 157);
        assert_eq!(misclassified.cache_creation_5m_micros, 0);
        assert_eq!(misclassified.cache_creation_1h_micros, 0);
        assert_eq!(misclassified.total_micros, 157);
        assert_eq!(correctly_split.input_micros, 0);
        assert_eq!(correctly_split.cache_creation_5m_micros, 0);
        assert_eq!(correctly_split.cache_creation_1h_micros, 252);
        assert_eq!(correctly_split.total_micros, 252);
    }

    fn install_local_snapshot(
        model: &str,
        pricing: Pricing,
        cache_creation_5m: Option<UsdPerMillion>,
        cache_read: Option<UsdPerMillion>,
        above_tokens_rates: BTreeMap<u64, ThresholdBand>,
    ) {
        let mut snapshot = CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models: HashMap::from([(model.to_owned(), pricing)]),
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        };
        if let Some(price) = cache_creation_5m {
            snapshot
                .cache_creation_per_million_usd
                .insert(model.to_owned(), price);
        }
        if let Some(price) = cache_read {
            snapshot
                .cache_read_per_million_usd
                .insert(model.to_owned(), price);
        }
        if !above_tokens_rates.is_empty() {
            snapshot
                .above_tokens_rates
                .insert(model.to_owned(), above_tokens_rates);
        }
        global_catalog().install_snapshot(snapshot);
    }

    #[test]
    fn local_cache_pricing_uses_disjoint_tail_and_five_minute_prefix() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "local-disjoint-regions-test";
        install_local_snapshot(
            model,
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
            Some(UsdPerMillion::from_micros_usd(3_750_000)),
            None,
            BTreeMap::new(),
        );

        let estimate = global_catalog().estimate_max_with_local_cache(
            model,
            LocalCacheInputEstimate {
                uncached_input_tokens: 10_001,
                cache_prefix_5m_tokens: 16_138,
                cache_prefix_1h_tokens: 0,
            },
            0,
            None,
        );

        // Independent arithmetic: floor(10_001 * $3/M) + floor(16_138 * $3.75/M)
        // = 30,003 + 60,517 = 90,520 micros.
        assert_eq!(estimate, Some(90_520));
    }

    #[test]
    fn local_cache_pricing_derives_one_hour_write_from_input_without_five_minute_price() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "local-derived-one-hour-test";
        install_local_snapshot(
            model,
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
            None,
            None,
            BTreeMap::new(),
        );

        let estimate = global_catalog().estimate_max_with_local_cache(
            model,
            LocalCacheInputEstimate {
                uncached_input_tokens: 0,
                cache_prefix_5m_tokens: 0,
                cache_prefix_1h_tokens: 1_000_000,
            },
            0,
            None,
        );

        // No catalog cache price exists: the conservative cold-write candidate is
        // 2 * active input = 2 * $3/M, so one million 1h tokens cost $6.
        assert_eq!(estimate, Some(6_000_000));
    }

    #[test]
    fn local_cache_pricing_uses_nonstandard_rates_for_mixed_one_hour_and_five_minute_regions() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "local-mixed-cache-prices-test";
        install_local_snapshot(
            model,
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
            Some(UsdPerMillion::from_micros_usd(4_250_000)),
            Some(UsdPerMillion::from_micros_usd(5_000_000)),
            BTreeMap::new(),
        );

        let estimate = global_catalog().estimate_max_with_local_cache(
            model,
            LocalCacheInputEstimate {
                uncached_input_tokens: 2_000_000,
                cache_prefix_5m_tokens: 4_000,
                cache_prefix_1h_tokens: 6_000,
            },
            0,
            None,
        );

        // Tail uses input ($3/M), P5 uses max($3, $4.25, $5)/M, and
        // P1 uses max($3, $4.25, $5, $6 derived)/M:
        // 6,000,000 + 20,000 + 36,000 = 6,056,000 micros.
        assert_eq!(estimate, Some(6_056_000));
    }

    #[test]
    fn local_cache_pricing_resolves_unique_gross_threshold_and_sparse_service_tier_fields() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "local-threshold-tier-test";
        let mut bands = BTreeMap::new();
        bands.insert(
            200_000,
            ThresholdBand {
                default: ThresholdRate {
                    input_per_million_usd: Some(UsdPerMillion::from_whole_usd(99)),
                    output_per_million_usd: Some(UsdPerMillion::from_whole_usd(18)),
                    cache_creation_per_million_usd: Some(UsdPerMillion::from_whole_usd(10)),
                    cache_read_per_million_usd: Some(UsdPerMillion::from_whole_usd(5)),
                },
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    ThresholdRate {
                        input_per_million_usd: Some(UsdPerMillion::from_whole_usd(7)),
                        ..ThresholdRate::default()
                    },
                )]),
            },
        );
        install_local_snapshot(
            model,
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(12),
                    },
                )]),
            },
            Some(UsdPerMillion::from_whole_usd(4)),
            Some(UsdPerMillion::from_whole_usd(1)),
            bands,
        );

        let at_threshold = global_catalog().estimate_max_with_local_cache(
            model,
            LocalCacheInputEstimate {
                uncached_input_tokens: 100_000,
                cache_prefix_5m_tokens: 60_000,
                cache_prefix_1h_tokens: 40_000,
            },
            1_000,
            Some("priority"),
        );
        let above_threshold = global_catalog().estimate_max_with_local_cache(
            model,
            LocalCacheInputEstimate {
                uncached_input_tokens: 100_001,
                cache_prefix_5m_tokens: 60_000,
                cache_prefix_1h_tokens: 40_000,
            },
            1_000,
            Some("priority"),
        );

        // Unique gross is exactly 200,000, then 200,001, not whole input
        // plus the prefixes again. Below the band: 100,000 * $3/M +
        // 60,000 * $4/M + 40,000 * $6/M + 1,000 * $12/M = 792,000 micros.
        // Above it: tier input overrides $99/M with $7/M; sparse cache/output
        // fields inherit $10/$5/$18 per million from the default band.
        // 100,001 * $7/M + 60,000 * $10/M + 40,000 * $14/M +
        // 1,000 * $18/M = 1,878,007 micros.
        assert_eq!(at_threshold, Some(792_000));
        assert_eq!(above_threshold, Some(1_878_007));
    }

    #[test]
    fn local_cache_pricing_returns_none_for_unknown_model() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        global_catalog().install_snapshot(CatalogSnapshot::empty_cost_disabled());

        let estimate = global_catalog().estimate_max_with_local_cache(
            "missing-local-model",
            LocalCacheInputEstimate {
                uncached_input_tokens: 1,
                cache_prefix_5m_tokens: 2,
                cache_prefix_1h_tokens: 3,
            },
            0,
            None,
        );

        assert_eq!(estimate, None);
    }

    #[test]
    fn local_cache_pricing_saturates_cost_overflow() {
        let _guard = GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let model = "local-overflow-test";
        install_local_snapshot(
            model,
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_micros_usd(u64::MAX),
                output_per_million_usd: UsdPerMillion::from_whole_usd(1),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
            None,
            None,
            BTreeMap::new(),
        );

        let estimate = global_catalog().estimate_max_with_local_cache(
            model,
            LocalCacheInputEstimate {
                uncached_input_tokens: u64::MAX,
                cache_prefix_5m_tokens: 0,
                cache_prefix_1h_tokens: 0,
            },
            0,
            None,
        );

        assert_eq!(estimate, Some(u64::MAX));
    }
}
