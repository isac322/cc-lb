#![forbid(unsafe_code)]

pub mod loader;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

pub use loader::{LiteLlmLoader, PriceCatalogStatus};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Pricing {
    pub model: String,
    pub input_per_million_usd: UsdPerMillion,
    pub output_per_million_usd: UsdPerMillion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CostEstimate {
    pub micros_usd: Option<u64>,
    pub pricing_status: PricingStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PricingStatus {
    Known,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct UsdPerMillion(u64);

impl UsdPerMillion {
    pub const fn from_whole_usd(whole_usd: u64) -> Self {
        Self(whole_usd * 1_000_000)
    }

    pub const fn as_micros_usd(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    AnthropicKey,
    AnthropicOAuth,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogStatus {
    Ok,
    Stale,
    CostDisabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub fetched_at_ms: u64,
    pub models: HashMap<String, Pricing>,
    pub raw_json: Vec<u8>,
    pub cache_creation_per_million_usd: HashMap<String, UsdPerMillion>,
    pub cache_read_per_million_usd: HashMap<String, UsdPerMillion>,
    pub status: CatalogStatus,
}

impl CatalogSnapshot {
    pub fn empty_cost_disabled() -> Self {
        Self {
            fetched_at_ms: 0,
            models: HashMap::new(),
            raw_json: Vec::new(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
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

    pub fn lookup(&self, model: &str, upstream_kind: Option<UpstreamKind>) -> Option<Pricing> {
        let normalized = normalize_model_id(model, upstream_kind);
        self.current().models.get(&normalized).cloned()
    }

    pub fn estimate_max(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        upstream_kind: Option<UpstreamKind>,
    ) -> Option<u64> {
        let normalized = normalize_model_id(model, upstream_kind);
        let Some(pricing) = self.current().models.get(&normalized).cloned() else {
            record_missing_price_field(&normalized, "model");
            return None;
        };
        Some(token_cost_micros(
            max_input,
            pricing.input_per_million_usd,
            max_output,
            pricing.output_per_million_usd,
            0,
            None,
            0,
            None,
            &pricing.model,
        ))
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

pub fn pricing_for_model(model: &str) -> Option<Pricing> {
    global_catalog().lookup(model, None)
}

pub fn pricing_for_model_with_kind(
    model: &str,
    upstream_kind: Option<UpstreamKind>,
) -> Option<Pricing> {
    global_catalog().lookup(model, upstream_kind)
}

#[deprecated(note = "use virtual_cost_micros_full to include cache token costs and upstream kind")]
pub fn virtual_cost_micros(model: &str, input_tokens: u64, output_tokens: u64) -> CostEstimate {
    virtual_cost_micros_full(model, input_tokens, output_tokens, 0, 0, None)
}

pub fn virtual_cost_micros_full(
    model: &str,
    input: u64,
    output: u64,
    cache_creation_input: u64,
    cache_read_input: u64,
    upstream_kind: Option<UpstreamKind>,
) -> CostEstimate {
    let normalized = normalize_model_id(model, upstream_kind);
    let snapshot = global_catalog().current();
    let Some(pricing) = snapshot.models.get(&normalized) else {
        record_missing_price_field(&normalized, "model");
        return CostEstimate {
            micros_usd: None,
            pricing_status: PricingStatus::Unknown,
        };
    };

    let cache_creation_price = snapshot
        .cache_creation_per_million_usd
        .get(&normalized)
        .copied();
    let cache_read_price = snapshot
        .cache_read_per_million_usd
        .get(&normalized)
        .copied();

    CostEstimate {
        micros_usd: Some(token_cost_micros(
            input,
            pricing.input_per_million_usd,
            output,
            pricing.output_per_million_usd,
            cache_creation_input,
            cache_creation_price,
            cache_read_input,
            cache_read_price,
            &normalized,
        )),
        pricing_status: PricingStatus::Known,
    }
}

pub fn normalize_model_id(model: &str, _upstream_kind: Option<UpstreamKind>) -> String {
    model.to_owned()
}

#[allow(clippy::too_many_arguments)]
fn token_cost_micros(
    input_tokens: u64,
    input_price: UsdPerMillion,
    output_tokens: u64,
    output_price: UsdPerMillion,
    cache_creation_input_tokens: u64,
    cache_creation_price: Option<UsdPerMillion>,
    cache_read_input_tokens: u64,
    cache_read_price: Option<UsdPerMillion>,
    model: &str,
) -> u64 {
    let total = component_cost(input_tokens, input_price)
        + component_cost(output_tokens, output_price)
        + cache_creation_price
            .map(|price| component_cost(cache_creation_input_tokens, price))
            .unwrap_or_else(|| {
                if cache_creation_input_tokens > 0 {
                    record_missing_cache_field(model, "cache_creation_per_million_usd");
                }
                0
            })
        + cache_read_price
            .map(|price| component_cost(cache_read_input_tokens, price))
            .unwrap_or_else(|| {
                if cache_read_input_tokens > 0 {
                    record_missing_cache_field(model, "cache_read_per_million_usd");
                }
                0
            });
    total.try_into().unwrap_or(u64::MAX)
}

fn component_cost(tokens: u64, price: UsdPerMillion) -> u128 {
    u128::from(tokens) * u128::from(price.as_micros_usd()) / 1_000_000
}

fn record_missing_cache_field(model: &str, field: &'static str) {
    record_missing_price_field(model, field);
}

fn record_missing_price_field(model: &str, field: &'static str) {
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => model.to_owned(),
        "field" => field
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn pricing(model: &str, input_usd: u64, output_usd: u64) -> Pricing {
        Pricing {
            model: model.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(input_usd),
            output_per_million_usd: UsdPerMillion::from_whole_usd(output_usd),
        }
    }

    fn snapshot_with(model: &str, pricing: Pricing) -> CatalogSnapshot {
        let mut models = HashMap::new();
        models.insert(model.to_owned(), pricing);

        CatalogSnapshot {
            fetched_at_ms: 1,
            models,
            raw_json: b"{}".to_vec(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            status: CatalogStatus::Ok,
        }
    }

    #[test]
    fn normalize_none_keeps_model_unchanged() {
        assert_eq!(
            normalize_model_id("claude-3-5-sonnet-20241022", None),
            "claude-3-5-sonnet-20241022"
        );
    }

    #[test]
    fn normalize_anthropic_kind_keeps_model_unchanged() {
        assert_eq!(
            normalize_model_id(
                "claude-3-5-sonnet-20241022",
                Some(UpstreamKind::AnthropicKey)
            ),
            "claude-3-5-sonnet-20241022"
        );
        assert_eq!(
            normalize_model_id(
                "claude-3-5-sonnet-20241022",
                Some(UpstreamKind::AnthropicOAuth)
            ),
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
    fn pricing_for_model_uses_populated_global_catalog() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        let model = "claude-3-5-haiku-20241022";
        let inserted = pricing(model, 1, 5);
        let snapshot = snapshot_with(model, inserted.clone());
        let catalog = PriceCatalog::new_empty();
        catalog.install_snapshot(snapshot.clone());
        if init_global_catalog(catalog).is_err() {
            global_catalog().install_snapshot(snapshot);
        }

        assert_eq!(pricing_for_model(model), Some(inserted));
    }

    #[allow(deprecated)]
    #[test]
    fn virtual_cost_micros_deprecated_wrapper_still_estimates_known_model() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        let model = "claude-opus-4-1-20250805";
        let inserted = pricing(model, 15, 75);
        global_catalog().install_snapshot(snapshot_with(model, inserted));

        assert_eq!(
            virtual_cost_micros(model, 1_000_000, 2_000_000),
            CostEstimate {
                micros_usd: Some(165_000_000),
                pricing_status: PricingStatus::Known,
            }
        );
    }

    #[test]
    fn virtual_cost_micros_full_marks_unknown_when_model_absent() {
        let _guard = GLOBAL_TEST_LOCK.lock().expect("global catalog test lock");
        global_catalog().install_snapshot(CatalogSnapshot::empty_cost_disabled());

        assert_eq!(
            virtual_cost_micros_full("missing-model", 1, 1, 1, 1, None),
            CostEstimate {
                micros_usd: None,
                pricing_status: PricingStatus::Unknown,
            }
        );
    }
}
