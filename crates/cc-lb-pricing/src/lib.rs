#![forbid(unsafe_code)]

mod catalog_parser;
mod cost;
pub mod lifecycle_pricing_subscriber;
pub mod loader;
mod loader_cache;
mod tier_resolver;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

pub use cost::{
    cache_creation_1h_micros_from_input, virtual_cost_micros_full,
    virtual_cost_micros_full_with_context,
};
use cost::{record_missing_price_field, resolved_cache_creation_1h_price};
use tier_resolver::{resolve_optional_price, resolve_tier_rate};

#[cfg(test)]
pub(crate) static GLOBAL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub use lifecycle_pricing_subscriber::{
    PricingSubscriberHandle, spawn_lifecycle_pricing_subscriber,
};
pub use loader::{FetchedCatalog, LiteLlmLoader, LoaderError};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TierRate {
    pub input_per_million_usd: UsdPerMillion,
    pub output_per_million_usd: UsdPerMillion,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdRate {
    pub input_per_million_usd: Option<UsdPerMillion>,
    pub output_per_million_usd: Option<UsdPerMillion>,
    pub cache_creation_per_million_usd: Option<UsdPerMillion>,
    pub cache_read_per_million_usd: Option<UsdPerMillion>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdBand {
    pub default: ThresholdRate,
    #[serde(default)]
    pub by_tier: BTreeMap<String, ThresholdRate>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Pricing {
    pub model: String,
    pub input_per_million_usd: UsdPerMillion,
    pub output_per_million_usd: UsdPerMillion,
    #[serde(default)]
    pub by_tier: BTreeMap<String, TierRate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingCachePricing {
    pub input_per_million_usd: UsdPerMillion,
    pub cache_creation_5m_per_million_usd: Option<UsdPerMillion>,
    pub cache_creation_1h_per_million_usd: Option<UsdPerMillion>,
    pub cache_read_per_million_usd: Option<UsdPerMillion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PricingStatus {
    Known,
    Unknown,
}

/// Per-component cost breakdown of a single request, in micros USD.
///
/// `total_micros == input + output + cache_creation_5m + cache_creation_1h + cache_read`.
/// When `pricing_status == Unknown`, all numeric fields are 0.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComputedCostBreakdown {
    pub input_micros: i64,
    pub output_micros: i64,
    pub cache_creation_5m_micros: i64,
    pub cache_creation_1h_micros: i64,
    pub cache_read_micros: i64,
    pub total_micros: i64,
    pub pricing_status: PricingStatus,
}

impl ComputedCostBreakdown {
    pub const fn unknown() -> Self {
        Self {
            input_micros: 0,
            output_micros: 0,
            cache_creation_5m_micros: 0,
            cache_creation_1h_micros: 0,
            cache_read_micros: 0,
            total_micros: 0,
            pricing_status: PricingStatus::Unknown,
        }
    }
}

impl From<ComputedCostBreakdown> for cc_lb_request_log::CostBreakdown {
    fn from(breakdown: ComputedCostBreakdown) -> Self {
        match breakdown.pricing_status {
            PricingStatus::Known => Self {
                total_micros: Some(breakdown.total_micros),
                input_micros: Some(breakdown.input_micros),
                output_micros: Some(breakdown.output_micros),
                cache_creation_5m_micros: Some(breakdown.cache_creation_5m_micros),
                cache_creation_1h_micros: Some(breakdown.cache_creation_1h_micros),
                cache_read_micros: Some(breakdown.cache_read_micros),
            },
            PricingStatus::Unknown => Self::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct UsdPerMillion(u64);

impl UsdPerMillion {
    pub const fn from_whole_usd(whole_usd: u64) -> Self {
        Self(whole_usd * 1_000_000)
    }

    pub const fn from_micros_usd(micros_usd: u64) -> Self {
        Self(micros_usd)
    }

    pub const fn as_micros_usd(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogStatus {
    Ok,
    CostDisabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub payload_hash: String,
    pub fetched_at_ms: u64,
    pub models: HashMap<String, Pricing>,
    pub raw_json: Vec<u8>,
    pub cache_creation_per_million_usd: HashMap<String, UsdPerMillion>,
    pub cache_read_per_million_usd: HashMap<String, UsdPerMillion>,
    #[serde(default)]
    pub cache_creation_per_million_usd_by_tier: HashMap<String, BTreeMap<String, UsdPerMillion>>,
    #[serde(default)]
    pub cache_read_per_million_usd_by_tier: HashMap<String, BTreeMap<String, UsdPerMillion>>,
    #[serde(default)]
    pub above_tokens_rates: HashMap<String, BTreeMap<u64, ThresholdBand>>,
    pub status: CatalogStatus,
}

impl CatalogSnapshot {
    pub fn empty_cost_disabled() -> Self {
        Self {
            payload_hash: String::new(),
            fetched_at_ms: 0,
            models: HashMap::new(),
            raw_json: Vec::new(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::CostDisabled,
        }
    }
}

pub struct PriceCatalog {
    snapshot: ArcSwap<CatalogSnapshot>,
}

/// Policy-defined worst-case gross input token bound (1 MiB / 1,048,576 tokens) reserved
/// when a request carries an opaque or server-side cache reference whose cached prefix size
/// cannot be bounded from the local request body alone.
pub const OPAQUE_CACHE_WORST_CASE_INPUT_TOKENS: u64 = 1_048_576;

impl PriceCatalog {
    pub fn new_empty() -> Arc<Self> {
        Arc::new(Self {
            snapshot: ArcSwap::from_pointee(CatalogSnapshot::empty_cost_disabled()),
        })
    }

    pub fn install_snapshot(&self, snap: CatalogSnapshot) {
        self.snapshot.store(Arc::new(snap));
    }

    pub fn current(&self) -> Arc<CatalogSnapshot> {
        self.snapshot.load_full()
    }

    pub fn lookup(&self, model: &str, service_tier: Option<&str>) -> Option<Pricing> {
        let snapshot = self.current();
        let mut pricing = snapshot
            .models
            .get(model)
            .or_else(|| snapshot.models.get(&normalize_model_id(model)))
            .cloned()?;
        let tier = canonical_service_tier(service_tier);
        let resolved = resolve_tier_rate(&pricing, tier.as_deref());
        pricing.input_per_million_usd = resolved.input_per_million_usd;
        pricing.output_per_million_usd = resolved.output_per_million_usd;
        Some(pricing)
    }

    pub fn routing_cache_pricing(
        &self,
        model: &str,
        service_tier: Option<&str>,
    ) -> Option<RoutingCachePricing> {
        let snapshot = self.current();
        let (model_key, pricing) = snapshot
            .models
            .get_key_value(model)
            .or_else(|| snapshot.models.get_key_value(&normalize_model_id(model)))?;
        let tier = canonical_service_tier(service_tier);
        let rate = resolve_tier_rate(pricing, tier.as_deref());
        let cache_creation_5m_per_million_usd = resolve_optional_price(
            snapshot
                .cache_creation_per_million_usd
                .get(model_key)
                .copied(),
            snapshot
                .cache_creation_per_million_usd_by_tier
                .get(model_key),
            tier.as_deref(),
        );
        let cache_read_per_million_usd = resolve_optional_price(
            snapshot.cache_read_per_million_usd.get(model_key).copied(),
            snapshot.cache_read_per_million_usd_by_tier.get(model_key),
            tier.as_deref(),
        );
        let cache_creation_1h_per_million_usd = if cache_creation_5m_per_million_usd.is_some() {
            Some(resolved_cache_creation_1h_price(rate.input_per_million_usd))
        } else {
            None
        };
        Some(RoutingCachePricing {
            input_per_million_usd: rate.input_per_million_usd,
            cache_creation_5m_per_million_usd,
            cache_creation_1h_per_million_usd,
            cache_read_per_million_usd,
        })
    }

    pub fn estimate_max(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<u64> {
        self.estimate_max_with_cache(model, max_input, max_output, 0, false, service_tier)
    }

    pub fn estimate_max_with_cache(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        can_cross_cache_cliff: bool,
        service_tier: Option<&str>,
    ) -> Option<u64> {
        let normalized = normalize_model_id(model);
        let snapshot = self.current();
        let Some((model_key, base_pricing)) = snapshot
            .models
            .get_key_value(model)
            .or_else(|| snapshot.models.get_key_value(&normalized))
        else {
            record_missing_price_field(&normalized, "model");
            return None;
        };
        let tier = canonical_service_tier(service_tier);
        let resolved = resolve_tier_rate(base_pricing, tier.as_deref());
        let mut input_price = resolved.input_per_million_usd;
        let mut output_price = resolved.output_per_million_usd;
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
        let bands = snapshot.above_tokens_rates.get(model_key);
        let effective_cache_tokens = if can_cross_cache_cliff {
            let max_threshold_bound = bands
                .and_then(|b| b.keys().next_back().copied())
                .map(|t| t.saturating_add(1))
                .unwrap_or(0);
            let worst_case_gross = OPAQUE_CACHE_WORST_CASE_INPUT_TOKENS.max(max_threshold_bound);
            estimated_cache_tokens.max(worst_case_gross.saturating_sub(max_input))
        } else {
            estimated_cache_tokens
        };
        let gross_input = max_input.saturating_add(effective_cache_tokens);
        crate::tier_resolver::apply_threshold_bands(
            bands,
            gross_input,
            tier.as_deref(),
            &mut input_price,
            &mut output_price,
            Some(&mut cache_creation_price),
            Some(&mut cache_read_price),
        );
        let mut cache_unit_price = input_price;
        for candidate in [cache_creation_price, cache_read_price]
            .into_iter()
            .flatten()
        {
            if candidate.as_micros_usd() > cache_unit_price.as_micros_usd() {
                cache_unit_price = candidate;
            }
        }
        let input_cost =
            u128::from(max_input) * u128::from(input_price.as_micros_usd()) / 1_000_000;
        let cache_cost = u128::from(effective_cache_tokens)
            * u128::from(cache_unit_price.as_micros_usd())
            / 1_000_000;
        let output_cost =
            u128::from(max_output) * u128::from(output_price.as_micros_usd()) / 1_000_000;
        let total = input_cost
            .saturating_add(cache_cost)
            .saturating_add(output_cost);
        Some(u64::try_from(total).unwrap_or(u64::MAX))
    }

    pub fn status(&self) -> CatalogStatus {
        self.current().status
    }
}

static GLOBAL_CATALOG: OnceLock<Arc<PriceCatalog>> = OnceLock::new();

pub fn global_catalog() -> &'static Arc<PriceCatalog> {
    GLOBAL_CATALOG.get_or_init(PriceCatalog::new_empty)
}

pub fn normalize_model_id(model: &str) -> String {
    let model = model
        .strip_prefix("openai/")
        .or_else(|| model.strip_prefix("google/"))
        .or_else(|| model.strip_prefix("models/"))
        .unwrap_or(model);
    match model {
        "claude-opus-4-8-20250514" => "claude-opus-4-8".to_owned(),
        _ => model.to_owned(),
    }
}

pub fn canonical_service_tier(raw: Option<&str>) -> Option<String> {
    let normalized = raw?.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "standard" | "standard_only" | "auto" => None,
        "batch" | "batches" => Some("batch".to_owned()),
        _ => Some(normalized),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pricing(model: &str, input_usd: u64, output_usd: u64) -> Pricing {
        Pricing {
            model: model.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(input_usd),
            output_per_million_usd: UsdPerMillion::from_whole_usd(output_usd),
            by_tier: BTreeMap::new(),
        }
    }

    fn snapshot_with(model: &str, pricing: Pricing) -> CatalogSnapshot {
        let mut models = HashMap::new();
        models.insert(model.to_owned(), pricing);

        CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 1,
            models,
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            above_tokens_rates: HashMap::new(),
            status: CatalogStatus::Ok,
        }
    }

    #[test]
    fn normalize_none_keeps_model_unchanged() {
        assert_eq!(
            normalize_model_id("claude-3-5-sonnet-20241022"),
            "claude-3-5-sonnet-20241022"
        );
    }

    #[test]
    fn normalize_strips_known_provider_namespaces_before_aliases() {
        for (model, expected) in [
            ("models/gemini-2.5-pro", "gemini-2.5-pro"),
            ("google/gemini-2.5-pro", "gemini-2.5-pro"),
            ("openai/gpt-5", "gpt-5"),
            ("models/claude-opus-4-8-20250514", "claude-opus-4-8"),
            ("unknown/gpt-5", "unknown/gpt-5"),
            ("myopenai/gpt-5", "myopenai/gpt-5"),
            ("gpt-5", "gpt-5"),
        ] {
            assert_eq!(normalize_model_id(model), expected);
        }
    }

    #[test]
    fn lookup_resolves_namespaced_standard_catalog_keys() {
        for (model, namespaced) in [
            ("gemini-2.5-pro", "models/gemini-2.5-pro"),
            ("gemini-2.5-pro", "google/gemini-2.5-pro"),
            ("gpt-5", "openai/gpt-5"),
        ] {
            let catalog = PriceCatalog::new_empty();
            let inserted = pricing(model, 3, 15);
            catalog.install_snapshot(snapshot_with(model, inserted.clone()));
            assert_eq!(catalog.lookup(namespaced, None), Some(inserted));
        }
    }

    #[test]
    fn lookup_prefers_exact_namespaced_and_dated_catalog_keys() {
        for (exact, normalized) in [
            ("openai/gpt-5", "gpt-5"),
            ("google/gemini-2.5-pro", "gemini-2.5-pro"),
            ("models/gemini-2.5-pro", "gemini-2.5-pro"),
            ("claude-opus-4-8-20250514", "claude-opus-4-8"),
        ] {
            let catalog = PriceCatalog::new_empty();
            let inserted = pricing(exact, 10, 20);
            let mut snapshot = snapshot_with(exact, inserted.clone());
            snapshot
                .models
                .insert(normalized.to_owned(), pricing(normalized, 1, 2));
            catalog.install_snapshot(snapshot);
            assert_eq!(catalog.lookup(exact, None), Some(inserted));
        }
    }

    #[test]
    fn routing_cache_pricing_uses_the_resolved_catalog_key() {
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = snapshot_with_cache_prices(
            "openai/gpt-5",
            pricing("openai/gpt-5", 10, 20),
            Some(UsdPerMillion::from_whole_usd(11)),
            Some(UsdPerMillion::from_whole_usd(12)),
        );
        snapshot
            .models
            .insert("gpt-5".to_owned(), pricing("gpt-5", 1, 2));
        catalog.install_snapshot(snapshot);

        let resolved = catalog
            .routing_cache_pricing("openai/gpt-5", None)
            .expect("exact namespaced cache pricing");
        assert_eq!(
            resolved.input_per_million_usd,
            UsdPerMillion::from_whole_usd(10)
        );
        assert_eq!(
            resolved.cache_creation_5m_per_million_usd,
            Some(UsdPerMillion::from_whole_usd(11))
        );
        assert_eq!(
            resolved.cache_read_per_million_usd,
            Some(UsdPerMillion::from_whole_usd(12))
        );

        catalog.install_snapshot(snapshot_with_cache_prices(
            "gemini-2.5-pro",
            pricing("gemini-2.5-pro", 3, 15),
            None,
            Some(UsdPerMillion::from_whole_usd(1)),
        ));
        let resolved = catalog
            .routing_cache_pricing("models/gemini-2.5-pro", None)
            .expect("normalized cache pricing");
        assert_eq!(
            resolved.cache_read_per_million_usd,
            Some(UsdPerMillion::from_whole_usd(1))
        );
    }

    #[test]
    fn install_snapshot_visible_via_lookup() {
        let catalog = PriceCatalog::new_empty();
        let inserted = pricing("claude-3-5-sonnet-20241022", 3, 15);
        catalog.install_snapshot(snapshot_with(
            "claude-3-5-sonnet-20241022",
            inserted.clone(),
        ));

        assert_eq!(
            catalog.lookup("claude-3-5-sonnet-20241022", None),
            Some(inserted)
        );
    }

    #[test]
    fn lookup_maps_dated_opus_4_8_to_undated_catalog_entry() {
        let catalog = PriceCatalog::new_empty();
        let inserted = pricing("claude-opus-4-8", 15, 75);
        catalog.install_snapshot(snapshot_with("claude-opus-4-8", inserted.clone()));

        assert_eq!(
            catalog.lookup("claude-opus-4-8-20250514", None),
            Some(inserted)
        );
    }

    #[test]
    fn estimate_max_computes_input_and_output_cost() {
        let catalog = PriceCatalog::new_empty();
        catalog.install_snapshot(snapshot_with(
            "claude-3-5-sonnet-20241022",
            pricing("claude-3-5-sonnet-20241022", 3, 15),
        ));

        assert_eq!(
            catalog.estimate_max("claude-3-5-sonnet-20241022", 2_000_000, 1_000_000, None),
            Some(21_000_000)
        );
    }

    #[test]
    fn virtual_cost_micros_full_marks_unknown_when_model_absent() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        global_catalog().install_snapshot(CatalogSnapshot::empty_cost_disabled());

        let breakdown = virtual_cost_micros_full("missing-model", 1, 1, 1, 1, 1, None);
        assert_eq!(breakdown, ComputedCostBreakdown::unknown());
        assert_eq!(breakdown.pricing_status, PricingStatus::Unknown);
    }

    fn snapshot_with_cache_prices(
        model: &str,
        pricing: Pricing,
        cache_creation_5m: Option<UsdPerMillion>,
        cache_read: Option<UsdPerMillion>,
    ) -> CatalogSnapshot {
        let mut snap = snapshot_with(model, pricing);
        if let Some(price) = cache_creation_5m {
            snap.cache_creation_per_million_usd
                .insert(model.to_owned(), price);
        }
        if let Some(price) = cache_read {
            snap.cache_read_per_million_usd
                .insert(model.to_owned(), price);
        }
        snap
    }

    #[test]
    fn breakdown_splits_5m_and_1h() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        let model = "claude-3-5-sonnet-20241022";
        global_catalog().install_snapshot(snapshot_with_cache_prices(
            model,
            pricing(model, 3, 15),
            Some(UsdPerMillion::from_micros_usd(3_750_000)),
            Some(UsdPerMillion::from_micros_usd(300_000)),
        ));

        let breakdown = virtual_cost_micros_full(model, 0, 0, 1_000_000, 1_000_000, 0, None);
        assert_eq!(breakdown.cache_creation_5m_micros, 3_750_000);
        assert_eq!(breakdown.cache_creation_1h_micros, 6_000_000);
        assert_eq!(breakdown.total_micros, 9_750_000);
        assert_eq!(breakdown.pricing_status, PricingStatus::Known);
    }

    #[test]
    fn breakdown_cache_read_uses_catalog_price_directly() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        let model = "claude-3-5-sonnet-20241022";
        global_catalog().install_snapshot(snapshot_with_cache_prices(
            model,
            pricing(model, 3, 15),
            Some(UsdPerMillion::from_micros_usd(3_750_000)),
            Some(UsdPerMillion::from_micros_usd(300_000)),
        ));

        let breakdown = virtual_cost_micros_full(model, 0, 0, 0, 0, 1_000_000, None);
        assert_eq!(breakdown.cache_read_micros, 300_000);
        assert_eq!(breakdown.cache_creation_5m_micros, 0);
        assert_eq!(breakdown.cache_creation_1h_micros, 0);
        assert_eq!(breakdown.total_micros, 300_000);
    }

    #[test]
    fn breakdown_handles_missing_cache_creation_price() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        let model = "claude-3-5-sonnet-20241022";
        global_catalog().install_snapshot(snapshot_with_cache_prices(
            model,
            pricing(model, 3, 15),
            None,
            Some(UsdPerMillion::from_micros_usd(300_000)),
        ));

        let breakdown = virtual_cost_micros_full(model, 0, 0, 1_000_000, 1_000_000, 0, None);
        assert_eq!(breakdown.cache_creation_5m_micros, 0);
        assert_eq!(breakdown.cache_creation_1h_micros, 0);
        assert_eq!(breakdown.pricing_status, PricingStatus::Known);
    }

    #[test]
    fn breakdown_total_matches_component_sum() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        let model = "claude-3-5-sonnet-20241022";
        global_catalog().install_snapshot(snapshot_with_cache_prices(
            model,
            pricing(model, 3, 15),
            Some(UsdPerMillion::from_micros_usd(3_750_000)),
            Some(UsdPerMillion::from_micros_usd(300_000)),
        ));

        let breakdown =
            virtual_cost_micros_full(model, 2_000_000, 1_000_000, 100_000, 200_000, 500_000, None);
        let expected_sum = breakdown.input_micros
            + breakdown.output_micros
            + breakdown.cache_creation_5m_micros
            + breakdown.cache_creation_1h_micros
            + breakdown.cache_read_micros;
        assert_eq!(breakdown.total_micros, expected_sum);
    }
}

#[cfg(test)]
mod tier_tests;
