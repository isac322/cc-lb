use crate::tier_resolver::{resolve_optional_price, resolve_tier_rate};
use crate::{
    ComputedCostBreakdown, CostEstimate, PricingStatus, UpstreamKind, UsdPerMillion,
    canonical_service_tier, global_catalog, normalize_model_id,
};

const CACHE_CREATION_1H_NUMERATOR: u128 = 8;
const CACHE_CREATION_1H_DENOMINATOR: u128 = 5;

pub(crate) fn cache_creation_1h_price(price_5m: UsdPerMillion) -> UsdPerMillion {
    let numer = u128::from(price_5m.as_micros_usd()) * CACHE_CREATION_1H_NUMERATOR;
    let micros = (numer + CACHE_CREATION_1H_DENOMINATOR / 2) / CACHE_CREATION_1H_DENOMINATOR;
    UsdPerMillion::from_micros_usd(micros.try_into().unwrap_or(u64::MAX))
}

#[deprecated(note = "use virtual_cost_micros_full to include cache token costs and upstream kind")]
pub fn virtual_cost_micros(model: &str, input_tokens: u64, output_tokens: u64) -> CostEstimate {
    virtual_cost_micros_full(model, input_tokens, output_tokens, 0, 0, 0, None, None)
        .into_estimate()
}

// CLIPPY-ALLOW: component token counts and tier context are the stable public pricing API.
#[allow(clippy::too_many_arguments)]
pub fn virtual_cost_micros_full(
    model: &str,
    input: u64,
    output: u64,
    cache_creation_5m_input: u64,
    cache_creation_1h_input: u64,
    cache_read_input: u64,
    upstream_kind: Option<UpstreamKind>,
    service_tier: Option<&str>,
) -> ComputedCostBreakdown {
    let normalized = normalize_model_id(model, upstream_kind);
    let snapshot = global_catalog().current();
    let Some(pricing) = snapshot.models.get(&normalized) else {
        record_missing_price_field(&normalized, "model");
        return ComputedCostBreakdown::unknown();
    };

    let tier = canonical_service_tier(service_tier);
    let rate = resolve_tier_rate(pricing, tier.as_deref());
    let cc_5m_price = resolve_optional_price(
        snapshot
            .cache_creation_per_million_usd
            .get(&normalized)
            .copied(),
        snapshot
            .cache_creation_per_million_usd_by_tier
            .get(&normalized),
        tier.as_deref(),
    );
    let cache_read_price = resolve_optional_price(
        snapshot
            .cache_read_per_million_usd
            .get(&normalized)
            .copied(),
        snapshot.cache_read_per_million_usd_by_tier.get(&normalized),
        tier.as_deref(),
    );

    let input_micros = component_cost(input, rate.input_per_million_usd);
    let output_micros = component_cost(output, rate.output_per_million_usd);

    let (cc_5m_micros, cc_1h_micros) = if let Some(price_5m) = cc_5m_price {
        (
            component_cost(cache_creation_5m_input, price_5m),
            component_cost(cache_creation_1h_input, cache_creation_1h_price(price_5m)),
        )
    } else {
        if cache_creation_5m_input + cache_creation_1h_input > 0 {
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

    ComputedCostBreakdown {
        input_micros: input_i,
        output_micros: output_i,
        cache_creation_5m_micros: cc5_i,
        cache_creation_1h_micros: cc1_i,
        cache_read_micros: cr_i,
        total_micros: total,
        pricing_status: PricingStatus::Known,
    }
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
