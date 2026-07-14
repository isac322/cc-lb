use std::collections::BTreeMap;

use crate::{Pricing, TierRate, UsdPerMillion};

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

fn fallback_price(base: UsdPerMillion, tier: Option<&str>) -> UsdPerMillion {
    match tier {
        Some("batch") => UsdPerMillion::from_micros_usd(base.as_micros_usd() / 2),
        Some(_) | None => base,
    }
}
