use std::sync::Arc;
use std::time::Duration;

use cc_lb_pricing::{CatalogStatus, LiteLlmLoader, PriceCatalog};
use cc_lb_storage_api::{BackendKind, MetaStore, PriceCatalogCache};
use cc_lb_storage_sqlite::SqliteStorage;
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
async fn refresh_once_fetches_installs_and_persists() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_string(SAMPLE_LITELLM_JSON))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "storage-13.sqlite").await?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage.clone(),
        format!("{}/prices", server.uri()),
        Duration::from_secs(60 * 60),
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::SystemClock),
    );

    loader.refresh_once().await?;
    assert!(catalog.lookup("claude-3-5-sonnet-20241022", None).is_some());
    assert!(storage.get_price_snapshot().await?.is_some());
    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn install_latest_local_reads_disk_cache_after_refresh_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let storage = Arc::new(sqlite_storage(&dir, "storage-17.sqlite").await?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage,
        format!("{}/prices", server.uri()),
        Duration::from_secs(60 * 60),
        cache_path,
        Arc::new(cc_lb_clock::SystemClock),
    );

    let error = loader
        .refresh_once()
        .await
        .expect_err("refresh should fail");
    assert!(error.to_string().contains("unexpected status 500"));
    assert!(loader.install_latest_local().await?);
    assert!(catalog.lookup("claude-3-5-sonnet-20241022", None).is_some());
    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn install_latest_local_returns_false_without_cache_after_refresh_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "storage-19.sqlite").await?);
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage,
        format!("{}/prices", server.uri()),
        Duration::from_secs(60 * 60),
        dir.path().join("missing-cache.json"),
        Arc::new(cc_lb_clock::SystemClock),
    );

    let error = loader
        .refresh_once()
        .await
        .expect_err("refresh should fail");
    assert!(error.to_string().contains("unexpected status 500"));
    assert!(!loader.install_latest_local().await?);
    assert_eq!(catalog.status(), CatalogStatus::CostDisabled);
    server.verify().await;
    Ok(())
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}
