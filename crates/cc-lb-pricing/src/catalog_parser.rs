use std::collections::{BTreeMap, BTreeSet, HashMap};

use cc_lb_clock::Clock;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

use crate::loader::LoaderError;
use crate::{
    CatalogSnapshot, CatalogStatus, Pricing, ThresholdBand, ThresholdRate, TierRate, UsdPerMillion,
};

type RawTierCosts<'a> = BTreeMap<String, (&'a str, f64)>;

pub(crate) fn parse_litellm_json(
    bytes: &[u8],
    clock: &dyn Clock,
) -> Result<CatalogSnapshot, LoaderError> {
    let root: HashMap<String, Value> =
        serde_json::from_slice(bytes).map_err(|error| LoaderError::Json(error.to_string()))?;
    let mut models = HashMap::new();
    let mut cache_creation_per_million_usd = HashMap::new();
    let mut cache_read_per_million_usd = HashMap::new();
    let mut cache_creation_per_million_usd_by_tier = HashMap::new();
    let mut cache_read_per_million_usd_by_tier = HashMap::new();
    let mut above_tokens_rates = HashMap::new();

    for (model, value) in root {
        if model == "sample_spec" {
            continue;
        }

        let Some(object) = value.as_object() else {
            continue;
        };
        let Some(input_cost) = object.get("input_cost_per_token").and_then(Value::as_f64) else {
            record_missing_catalog_field(&model, "input_cost_per_token");
            continue;
        };
        let Some(output_cost) = object.get("output_cost_per_token").and_then(Value::as_f64) else {
            record_missing_catalog_field(&model, "output_cost_per_token");
            continue;
        };

        let input_per_million_usd =
            usd_per_token_to_per_million(&model, "input_cost_per_token", input_cost)?;
        let output_per_million_usd =
            usd_per_token_to_per_million(&model, "output_cost_per_token", output_cost)?;

        if let Some(cache_creation_cost) = object
            .get("cache_creation_input_token_cost")
            .and_then(Value::as_f64)
        {
            cache_creation_per_million_usd.insert(
                model.clone(),
                usd_per_token_to_per_million(
                    &model,
                    "cache_creation_input_token_cost",
                    cache_creation_cost,
                )?,
            );
        }

        if let Some(cache_read_cost) = object
            .get("cache_read_input_token_cost")
            .and_then(Value::as_f64)
        {
            cache_read_per_million_usd.insert(
                model.clone(),
                usd_per_token_to_per_million(
                    &model,
                    "cache_read_input_token_cost",
                    cache_read_cost,
                )?,
            );
        }

        let input_by_tier = discover_tier_costs(object, "input_cost_per_token");
        let output_by_tier = discover_tier_costs(object, "output_cost_per_token");
        let tier_names = input_by_tier
            .keys()
            .chain(output_by_tier.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut by_tier = BTreeMap::new();
        for tier in tier_names {
            let tier_input = convert_discovered_cost(
                &model,
                input_by_tier.get(&tier),
                fallback_for_discovered_tier(&tier, input_per_million_usd),
            )?;
            let tier_output = convert_discovered_cost(
                &model,
                output_by_tier.get(&tier),
                fallback_for_discovered_tier(&tier, output_per_million_usd),
            )?;
            by_tier.insert(
                tier,
                TierRate {
                    input_per_million_usd: tier_input,
                    output_per_million_usd: tier_output,
                },
            );
        }

        insert_cache_tier_costs(
            &mut cache_creation_per_million_usd_by_tier,
            &model,
            discover_tier_costs(object, "cache_creation_input_token_cost"),
        )?;
        insert_cache_tier_costs(
            &mut cache_read_per_million_usd_by_tier,
            &model,
            discover_tier_costs(object, "cache_read_input_token_cost"),
        )?;

        let mut model_above_tokens_rates = BTreeMap::<u64, ThresholdBand>::new();
        insert_above_token_costs(
            &mut model_above_tokens_rates,
            &model,
            object,
            "input_cost_per_token",
            |rate| &mut rate.input_per_million_usd,
        )?;
        insert_above_token_costs(
            &mut model_above_tokens_rates,
            &model,
            object,
            "output_cost_per_token",
            |rate| &mut rate.output_per_million_usd,
        )?;
        insert_above_token_costs(
            &mut model_above_tokens_rates,
            &model,
            object,
            "cache_creation_input_token_cost",
            |rate| &mut rate.cache_creation_per_million_usd,
        )?;
        insert_above_token_costs(
            &mut model_above_tokens_rates,
            &model,
            object,
            "cache_read_input_token_cost",
            |rate| &mut rate.cache_read_per_million_usd,
        )?;
        if !model_above_tokens_rates.is_empty() {
            above_tokens_rates.insert(model.clone(), model_above_tokens_rates);
        }

        models.insert(
            model.clone(),
            Pricing {
                model,
                input_per_million_usd,
                output_per_million_usd,
                by_tier,
            },
        );
    }

    if models.is_empty() {
        return Err(LoaderError::Validation(
            "litellm catalog contains no models with input_cost_per_token and output_cost_per_token"
                .to_owned(),
        ));
    }

    Ok(CatalogSnapshot {
        payload_hash: hex::encode(Sha256::digest(bytes)),
        fetched_at_ms: cc_lb_clock::unix_millis(clock.now())
            .try_into()
            .unwrap_or(u64::MAX),
        models,
        raw_json: bytes.to_vec(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier,
        cache_read_per_million_usd_by_tier,
        above_tokens_rates,
        status: CatalogStatus::Ok,
    })
}

fn discover_tier_costs<'a>(object: &'a Map<String, Value>, base: &str) -> RawTierCosts<'a> {
    object
        .iter()
        .filter_map(|(key, value)| {
            let suffix = key.strip_prefix(base)?.strip_prefix('_')?;
            if suffix.to_ascii_lowercase().contains("above_")
                || suffix.to_ascii_lowercase().contains("cache_hit")
                || suffix.to_ascii_lowercase().contains("hr")
            {
                return None;
            }
            let tier = match suffix {
                "batches" => "batch",
                _ => suffix,
            };
            value
                .as_f64()
                .map(|cost| (tier.to_owned(), (key.as_str(), cost)))
        })
        .collect()
}

fn convert_discovered_cost(
    model: &str,
    discovered: Option<&(&str, f64)>,
    fallback: UsdPerMillion,
) -> Result<UsdPerMillion, LoaderError> {
    discovered.map_or(Ok(fallback), |(field, cost)| {
        usd_per_token_to_per_million(model, field, *cost)
    })
}

const fn fallback_for_discovered_tier(tier: &str, base: UsdPerMillion) -> UsdPerMillion {
    match tier.as_bytes() {
        b"batch" => UsdPerMillion::from_micros_usd(base.as_micros_usd() / 2),
        _ => base,
    }
}

fn insert_cache_tier_costs(
    target: &mut HashMap<String, BTreeMap<String, UsdPerMillion>>,
    model: &str,
    discovered: RawTierCosts<'_>,
) -> Result<(), LoaderError> {
    if discovered.is_empty() {
        return Ok(());
    }
    let mut rates = BTreeMap::new();
    for (tier, (field, cost)) in discovered {
        rates.insert(tier, usd_per_token_to_per_million(model, field, cost)?);
    }
    target.insert(model.to_owned(), rates);
    Ok(())
}

fn insert_above_token_costs<F>(
    target: &mut BTreeMap<u64, ThresholdBand>,
    model: &str,
    object: &Map<String, Value>,
    base: &str,
    mut set_rate: F,
) -> Result<(), LoaderError>
where
    F: FnMut(&mut ThresholdRate) -> &mut Option<UsdPerMillion>,
{
    for (key, value) in object {
        let Some((threshold, tier)) = parse_above_token_key(key, base) else {
            continue;
        };
        let Some(cost) = value.as_f64() else {
            continue;
        };
        let price = usd_per_token_to_per_million(model, key, cost)?;
        let band = target.entry(threshold).or_default();
        let rate = match tier {
            Some(tier) => band.by_tier.entry(tier).or_default(),
            None => &mut band.default,
        };
        *set_rate(rate) = Some(price);
    }
    Ok(())
}

fn parse_above_token_key(key: &str, base: &str) -> Option<(u64, Option<String>)> {
    let suffix = key.strip_prefix(base)?.strip_prefix('_')?;
    let parts = suffix.split('_').collect::<Vec<_>>();
    let above_index = parts
        .iter()
        .position(|part| part.eq_ignore_ascii_case("above"))?;
    if !parts.get(above_index + 2)?.eq_ignore_ascii_case("tokens") {
        return None;
    }
    let threshold = parse_token_threshold(parts[above_index + 1])?;
    let mut tier = None;
    for candidate in parts[..above_index]
        .iter()
        .chain(parts[above_index + 3..].iter())
    {
        let canonical = match candidate.to_ascii_lowercase().as_str() {
            "priority" => "priority",
            "batch" | "batches" => "batch",
            "flex" => "flex",
            "ultrafast" => "ultrafast",
            _ => return None,
        };
        if tier.replace(canonical.to_owned()).is_some() {
            return None;
        }
    }
    Some((threshold, tier))
}

fn parse_token_threshold(raw: &str) -> Option<u64> {
    let normalized = raw.to_ascii_lowercase();
    let (digits, multiplier) = if let Some(digits) = normalized.strip_suffix('k') {
        (digits, 1_000_u64)
    } else if let Some(digits) = normalized.strip_suffix('m') {
        (digits, 1_000_000_u64)
    } else {
        (normalized.as_str(), 1)
    };
    digits.parse::<u64>().ok()?.checked_mul(multiplier)
}

fn usd_per_token_to_per_million(
    model: &str,
    field: &str,
    cost_per_token: f64,
) -> Result<UsdPerMillion, LoaderError> {
    if !cost_per_token.is_finite() || cost_per_token < 0.0 {
        return Err(LoaderError::Validation(format!(
            "invalid {field} for model {model}"
        )));
    }

    let micros_per_million = cost_per_token * 1_000_000.0 * 1_000_000.0;
    if micros_per_million > u64::MAX as f64 {
        return Err(LoaderError::Validation(format!(
            "{field} for model {model} exceeds u64 micros"
        )));
    }

    Ok(UsdPerMillion::from_micros_usd(
        micros_per_million.round() as u64
    ))
}

fn record_missing_catalog_field(model: &str, field: &'static str) {
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => model.to_owned(),
        "field" => field
    )
    .increment(1);
}
