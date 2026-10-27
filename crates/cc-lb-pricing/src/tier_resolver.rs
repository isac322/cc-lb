use std::collections::BTreeMap;

use crate::{Pricing, ThresholdBand, TierRate, UsdPerMillion};

pub(crate) fn resolve_tier_rate(pricing: &Pricing, tier: Option<&str>) -> TierRate {
    if let Some(rate) = tier.and_then(|name| pricing.by_tier.get(name)) {
        return rate.clone();
    }
    let input = fallback_price(pricing.input_per_million_usd, tier);
    let output = fallback_price(pricing.output_per_million_usd, tier);
    TierRate {
        input_per_million_usd: input,
        output_per_million_usd: output,
    }
}

pub(crate) fn resolve_optional_price(
    base: Option<UsdPerMillion>,
    by_tier: Option<&BTreeMap<String, UsdPerMillion>>,
    tier: Option<&str>,
) -> Option<UsdPerMillion> {
    tier.and_then(|name| by_tier.and_then(|rates| rates.get(name)))
        .copied()
        .or_else(|| base.map(|price| fallback_price(price, tier)))
}
pub(crate) fn resolve_threshold_component(
    default_price: Option<UsdPerMillion>,
    tier_price: Option<UsdPerMillion>,
    tier: Option<&str>,
) -> Option<UsdPerMillion> {
    tier_price.or_else(|| default_price.map(|price| fallback_price(price, tier)))
}

pub(crate) fn apply_threshold_bands(
    bands: Option<&BTreeMap<u64, ThresholdBand>>,
    gross_input_tokens: u64,
    tier: Option<&str>,
    input_price: &mut UsdPerMillion,
    output_price: &mut UsdPerMillion,
    mut cache_creation_price: Option<&mut Option<UsdPerMillion>>,
    mut cache_read_price: Option<&mut Option<UsdPerMillion>>,
) {
    let Some(bands) = bands else {
        return;
    };
    for (threshold, band) in bands {
        if gross_input_tokens <= *threshold {
            break;
        }
        let tier_rate = tier.and_then(|name| band.by_tier.get(name));
        if let Some(price) = resolve_threshold_component(
            band.default.input_per_million_usd,
            tier_rate.and_then(|rate| rate.input_per_million_usd),
            tier,
        ) {
            *input_price = price;
        }
        if let Some(price) = resolve_threshold_component(
            band.default.output_per_million_usd,
            tier_rate.and_then(|rate| rate.output_per_million_usd),
            tier,
        ) {
            *output_price = price;
        }
        if let Some(target) = cache_creation_price.as_deref_mut()
            && let Some(price) = resolve_threshold_component(
                band.default.cache_creation_per_million_usd,
                tier_rate.and_then(|rate| rate.cache_creation_per_million_usd),
                tier,
            )
        {
            *target = Some(price);
        }
        if let Some(target) = cache_read_price.as_deref_mut()
            && let Some(price) = resolve_threshold_component(
                band.default.cache_read_per_million_usd,
                tier_rate.and_then(|rate| rate.cache_read_per_million_usd),
                tier,
            )
        {
            *target = Some(price);
        }
    }
}

fn fallback_price(base: UsdPerMillion, tier: Option<&str>) -> UsdPerMillion {
    match tier {
        Some("batch") => UsdPerMillion::from_micros_usd(base.as_micros_usd() / 2),
        Some(_) | None => base,
    }
}
