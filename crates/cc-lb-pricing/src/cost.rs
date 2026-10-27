use crate::tier_resolver::{resolve_optional_price, resolve_tier_rate};
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

#[allow(clippy::too_many_arguments)]
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
    if let Some(bands) = snapshot.above_tokens_rates.get(model_key) {
        for (threshold, band) in bands {
            if gross_input_tokens <= *threshold {
                break;
            }
            let threshold_rate = tier
                .as_deref()
                .and_then(|tier| band.by_tier.get(tier))
                .unwrap_or(&band.default);
            if let Some(price) = threshold_rate.input_per_million_usd {
                input_price = price;
            }
            if let Some(price) = threshold_rate.output_per_million_usd {
                output_price = price;
            }
            if let Some(price) = threshold_rate.cache_creation_per_million_usd {
                cache_creation_price = Some(price);
            }
            if let Some(price) = threshold_rate.cache_read_per_million_usd {
                cache_read_price = Some(price);
            }
        }
    }

    let generic_creation = cache_creation_input_tokens
        .saturating_sub(cache_creation_5m_input.saturating_add(cache_creation_1h_input));
    let input_micros = component_cost(input, input_price).saturating_add(component_cost(
        generic_creation,
        cache_creation_price.unwrap_or(input_price),
    ));
    let output_micros = component_cost(output, output_price);

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

pub(crate) fn token_cost_micros(
    input_tokens: u64,
    output_tokens: u64,
    pricing: &crate::Pricing,
) -> u64 {
    let total = component_cost(input_tokens, pricing.input_per_million_usd)
        + component_cost(output_tokens, pricing.output_per_million_usd);
    total.try_into().unwrap_or(u64::MAX)
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
        CatalogSnapshot, CatalogStatus, GLOBAL_TEST_LOCK, Pricing, ThresholdBand, ThresholdRate,
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
}
