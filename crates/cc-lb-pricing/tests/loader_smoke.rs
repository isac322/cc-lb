use std::sync::Arc;
use std::time::Duration;

use cc_lb_pricing::{CatalogStatus, LiteLlmLoader, PriceCatalog};
use cc_lb_storage_redb::Storage;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SAMPLE_LITELLM_JSON: &str = r#"
{
  "sample_spec": {"max_tokens": 0},
  "claude-3-5-sonnet-20241022": {"input_cost_per_token": 0.000003, "output_cost_per_token": 0.000015, "cache_creation_input_token_cost": 0.00000375, "cache_read_input_token_cost": 0.0000003, "mode": "chat", "max_tokens": 8192},
  "claude-3-5-haiku-20241022": {"input_cost_per_token": 0.0000008, "output_cost_per_token": 0.000004, "mode": "chat", "max_tokens": 8192},
  "claude-3-opus-20240229": {"input_cost_per_token": 0.000015, "output_cost_per_token": 0.000075, "mode": "chat", "max_tokens": 4096}
}
"#;

#[tokio::test]
async fn fetch_install_persist() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_string(SAMPLE_LITELLM_JSON))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(&dir.path().join("storage.redb"), [13; 32])?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage.clone(),
        format!("{}/prices", server.uri()),
        Duration::from_secs(60 * 60),
        dir.path().join("litellm-cache.json"),
    );

    let handle = loader.start_daemon();
    assert!(LiteLlmLoader::wait_for_first_snapshot(&catalog, Duration::from_secs(5)).await);
    assert!(catalog.lookup("claude-3-5-sonnet-20241022", None).is_some());
    // NOTE [Priority-3 footgun]: wait_for_first_snapshot fires when the in-memory
    // PriceCatalog is populated, but the redb write happens on a separate
    // background hop. Under cargo-llvm-cov instrumentation the persist lags by
    // up to a few hundred ms; poll the disk snapshot for up to 5 s real time
    // before failing instead of asserting once and racing the writer.
    let persist_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while storage.get_price_snapshot()?.is_none() && std::time::Instant::now() < persist_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(storage.get_price_snapshot()?.is_some());
    server.verify().await;

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn cold_start_3_retry_fail_with_disk_cache() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(500))
        .expect(3)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let storage = Arc::new(Storage::open(&dir.path().join("storage.redb"), [17; 32])?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage,
        format!("{}/prices", server.uri()),
        Duration::from_secs(60 * 60),
        cache_path,
    );

    let handle = loader.start_daemon();
    assert!(LiteLlmLoader::wait_for_first_snapshot(&catalog, Duration::from_secs(5)).await);
    assert!(catalog.lookup("claude-3-5-sonnet-20241022", None).is_some());
    server.verify().await;

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn cold_start_3_retry_fail_no_disk_cache() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(500))
        .expect(3)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(&dir.path().join("storage.redb"), [19; 32])?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage,
        format!("{}/prices", server.uri()),
        Duration::from_secs(60 * 60),
        dir.path().join("missing-cache.json"),
    );

    let handle = loader.start_daemon();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(catalog.status(), CatalogStatus::CostDisabled);
    server.verify().await;

    handle.abort();
    let _ = handle.await;
    Ok(())
}
