use std::sync::Arc;
use std::time::Duration;

use cc_lb_pricing::{LiteLlmLoader, PriceCatalog, TierRate, UsdPerMillion};
use cc_lb_storage_api::PriceCatalogCache;
use cc_lb_testkit::InMemoryStorage;
use http::StatusCode;
use tokio::net::TcpListener;

use crate::support::{ObservedRequest, serve_single_http_response};

const TIERED_LITELLM_JSON: &str = r#"
{
  "tiered-model": {
    "input_cost_per_token": 0.000002,
    "output_cost_per_token": 0.000008,
    "cache_read_input_token_cost": 0.000001,
    "input_cost_per_token_priority": 0.000003,
    "output_cost_per_token_priority": 0.000012,
    "input_cost_per_token_batches": 0.000001,
    "output_cost_per_token_batches": 0.000004,
    "input_cost_per_token_flex": 0.0000015,
    "output_cost_per_token_flex": 0.000006,
    "cache_read_input_token_cost_priority": 0.000002,
    "input_cost_per_token_above_200k_tokens": 0.000099,
    "cache_read_input_token_cost_cache_hit": 0.000099
  }
}
"#;

const PARTIAL_TIER_LITELLM_JSON: &str = r#"
{
  "batch-input-only": {
    "input_cost_per_token": 0.000002,
    "output_cost_per_token": 0.000008,
    "input_cost_per_token_batches": 0.0000015
  },
  "batch-output-only": {
    "input_cost_per_token": 0.000002,
    "output_cost_per_token": 0.000008,
    "output_cost_per_token_batches": 0.000005
  },
  "priority-input-only": {
    "input_cost_per_token": 0.000002,
    "output_cost_per_token": 0.000008,
    "input_cost_per_token_priority": 0.000003
  },
  "flex-output-only": {
    "input_cost_per_token": 0.000002,
    "output_cost_per_token": 0.000008,
    "output_cost_per_token_flex": 0.000006
  }
}
"#;

const FIXED_UNIX_SECS: u64 = 1_700_000_000;
const FIXED_UNIX_MILLIS: u64 = FIXED_UNIX_SECS * 1_000;

#[tokio::test]
async fn t3__refresh_discovers_canonical_tiers_and_excludes_non_tier_suffixes()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_single_http_response(listener, StatusCode::OK, TIERED_LITELLM_JSON);
    let dir = tempfile::tempdir()?;
    let storage = InMemoryStorage::new();
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        storage.clone(),
        format!("http://{address}/prices"),
        Duration::from_secs(60 * 60),
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    loader.refresh_once().await?;
    let snapshot = catalog.current();
    let pricing = snapshot
        .models
        .get("tiered-model")
        .expect("tiered model should be parsed");

    assert_eq!(server.await??, expected_request());
    assert_eq!(
        pricing.by_tier.get("priority"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(12),
        })
    );
    assert_eq!(
        pricing.by_tier.get("batch"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_whole_usd(1),
            output_per_million_usd: UsdPerMillion::from_whole_usd(4),
        })
    );
    assert_eq!(
        pricing.by_tier.get("flex"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_micros_usd(1_500_000),
            output_per_million_usd: UsdPerMillion::from_whole_usd(6),
        })
    );
    assert_eq!(pricing.by_tier.len(), 3);
    assert_eq!(
        snapshot
            .cache_read_per_million_usd_by_tier
            .get("tiered-model")
            .and_then(|rates| rates.get("priority")),
        Some(&UsdPerMillion::from_whole_usd(2))
    );
    assert_eq!(
        snapshot
            .cache_read_per_million_usd_by_tier
            .get("tiered-model")
            .map(std::collections::BTreeMap::len),
        Some(1)
    );
    let persisted = storage
        .get_price_snapshot()
        .await?
        .expect("refresh must persist the tiered catalog");
    assert_eq!(persisted.json_bytes, TIERED_LITELLM_JSON.as_bytes());
    assert_eq!(persisted.fetched_at_ms, FIXED_UNIX_MILLIS);
    Ok(())
}

#[tokio::test]
async fn t3__refresh_applies_component_fallbacks_to_partial_tier_rates()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_single_http_response(listener, StatusCode::OK, PARTIAL_TIER_LITELLM_JSON);
    let dir = tempfile::tempdir()?;
    let storage = InMemoryStorage::new();
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        storage.clone(),
        format!("http://{address}/prices"),
        Duration::from_secs(60 * 60),
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    loader.refresh_once().await?;
    let snapshot = catalog.current();

    assert_eq!(server.await??, expected_request());
    assert_eq!(
        snapshot.models["batch-input-only"].by_tier.get("batch"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_micros_usd(1_500_000),
            output_per_million_usd: UsdPerMillion::from_whole_usd(4),
        })
    );
    assert_eq!(
        snapshot.models["batch-output-only"].by_tier.get("batch"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_whole_usd(1),
            output_per_million_usd: UsdPerMillion::from_whole_usd(5),
        })
    );
    assert_eq!(
        snapshot.models["priority-input-only"]
            .by_tier
            .get("priority"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(8),
        })
    );
    assert_eq!(
        snapshot.models["flex-output-only"].by_tier.get("flex"),
        Some(&TierRate {
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(6),
        })
    );
    let persisted = storage
        .get_price_snapshot()
        .await?
        .expect("refresh must persist the partial tier catalog");
    assert_eq!(persisted.json_bytes, PARTIAL_TIER_LITELLM_JSON.as_bytes());
    assert_eq!(persisted.fetched_at_ms, FIXED_UNIX_MILLIS);
    Ok(())
}

fn expected_request() -> ObservedRequest {
    ObservedRequest {
        method: "GET".to_owned(),
        path: "/prices".to_owned(),
    }
}
