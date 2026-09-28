#![forbid(unsafe_code)]

mod catalog_parser;
mod cost;
pub mod lifecycle_pricing_subscriber;
pub mod loader;
mod loader_cache;
mod tier_resolver;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

pub use cost::{cache_creation_1h_micros_from_input, virtual_cost_micros_full};
use cost::{record_missing_price_field, resolved_cache_creation_1h_price, token_cost_micros};
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
            status: CatalogStatus::CostDisabled,
        }
    }
}

pub struct PriceCatalog {
    snapshot: ArcSwap<CatalogSnapshot>,
}

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
        let normalized = normalize_model_id(model);
        let mut pricing = self.current().models.get(&normalized).cloned()?;
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
        let normalized = normalize_model_id(model);
        let snapshot = self.current();
        let pricing = snapshot.models.get(&normalized)?;
        let tier = canonical_service_tier(service_tier);
        let rate = resolve_tier_rate(pricing, tier.as_deref());
        let cache_creation_5m_per_million_usd = resolve_optional_price(
            snapshot
                .cache_creation_per_million_usd
                .get(&normalized)
                .copied(),
            snapshot
                .cache_creation_per_million_usd_by_tier
                .get(&normalized),
            tier.as_deref(),
        );
        let cache_read_per_million_usd = resolve_optional_price(
            snapshot
                .cache_read_per_million_usd
                .get(&normalized)
                .copied(),
            snapshot.cache_read_per_million_usd_by_tier.get(&normalized),
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
        let normalized = normalize_model_id(model);
        let Some(pricing) = self.lookup(model, service_tier) else {
            record_missing_price_field(&normalized, "model");
            return None;
        };
        Some(token_cost_micros(max_input, max_output, &pricing))
    }

    pub fn status(&self) -> CatalogStatus {
        self.current().status
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogAlreadyInitialized;

impl fmt::Display for CatalogAlreadyInitialized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("price catalog is already initialized")
    }
}

impl std::error::Error for CatalogAlreadyInitialized {}

static GLOBAL_CATALOG: OnceLock<Arc<PriceCatalog>> = OnceLock::new();

pub fn global_catalog() -> &'static Arc<PriceCatalog> {
    GLOBAL_CATALOG.get_or_init(PriceCatalog::new_empty)
}

pub fn init_global_catalog(catalog: Arc<PriceCatalog>) -> Result<(), CatalogAlreadyInitialized> {
    GLOBAL_CATALOG
        .set(catalog)
        .map_err(|_catalog| CatalogAlreadyInitialized)
}

pub fn normalize_model_id(model: &str) -> String {
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
