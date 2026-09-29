use std::sync::Arc;

use cc_lb_pricing::{LiteLlmLoader, PriceCatalog, TierRate, UsdPerMillion};
use cc_lb_storage_api::MetaStore;
use cc_lb_storage_sqlite::SqliteStorage;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

#[tokio::test]
async fn refresh_discovers_canonical_tiers_and_excludes_non_tier_suffixes()
-> Result<(), Box<dyn std::error::Error>> {
    // Given a LiteLLM catalog with tier keys and similarly prefixed decoys.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_string(TIERED_LITELLM_JSON))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        storage,
        format!("{}/prices", server.uri()),
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    // When the catalog is refreshed and parsed.
    let fetched = loader.fetch_and_fingerprint().await?;
    loader.persist_snapshot(&fetched).await?;
    loader.install_latest_local().await?;
    let snapshot = catalog.current();
    let pricing = snapshot
        .models
        .get("tiered-model")
        .expect("tiered model should be parsed");

    // Then suffixes are data-driven, canonical, complete, and decoys are excluded.
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
    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn refresh_applies_component_fallbacks_to_partial_tier_rates()
-> Result<(), Box<dyn std::error::Error>> {
    // Given a LiteLLM catalog with one explicit component per discovered tier.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_string(PARTIAL_TIER_LITELLM_JSON))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        storage,
        format!("{}/prices", server.uri()),
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    // When the catalog is refreshed and parsed.
    let fetched = loader.fetch_and_fingerprint().await?;
    loader.persist_snapshot(&fetched).await?;
    loader.install_latest_local().await?;
    let snapshot = catalog.current();

    // Then missing batch components use half base while other tiers use full base.
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
    server.verify().await;
    Ok(())
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("tier-loader.sqlite").display()
    );
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &database_url,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    )
    .await?;
    storage.initialize().await?;
    Ok(storage)
}
