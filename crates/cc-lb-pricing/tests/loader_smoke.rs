use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, LiteLlmLoader, LoaderError, PriceCatalog};
use cc_lb_storage_api::{
    PriceCatalogCache, PriceCatalogSnapshotFetch, PriceCatalogSnapshotMetadata, StorageError,
    StorageResult,
};
use cc_lb_testkit::InMemoryStorage;
use http::StatusCode;
use sha2::{Digest as _, Sha256};
use tokio::net::TcpListener;

use crate::support::{ObservedRequest, serve_single_http_response};

const SAMPLE_LITELLM_JSON: &str = r#"
{
  "sample_spec": {"max_tokens": 0},
  "claude-3-5-sonnet-20241022": {"input_cost_per_token": 0.000003, "output_cost_per_token": 0.000015, "cache_creation_input_token_cost": 0.00000375, "cache_read_input_token_cost": 0.0000003, "mode": "chat", "max_tokens": 8192},
  "claude-3-5-haiku-20241022": {"input_cost_per_token": 0.0000008, "output_cost_per_token": 0.000004, "mode": "chat", "max_tokens": 8192},
  "claude-3-opus-20240229": {"input_cost_per_token": 0.000015, "output_cost_per_token": 0.000075, "mode": "chat", "max_tokens": 4096}
}
"#;

const FIXED_UNIX_SECS: u64 = 1_700_000_000;
const FIXED_UNIX_MILLIS: u64 = FIXED_UNIX_SECS * 1_000;

enum LocalPollStorage {
    Unchanged,
    Error,
    Corrupted,
}

impl PriceCatalogCache for LocalPollStorage {
    fn put_price_snapshot<'a, 'b, 'future>(
        &'a self,
        _json_bytes: &'b [u8],
        _fetched_at_ms: u64,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + 'future>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async { panic!("local poll must not write a payload") })
    }

    fn get_price_snapshot_if_changed<'a, 'b, 'future>(
        &'a self,
        current_hash: &'b str,
    ) -> Pin<Box<dyn Future<Output = StorageResult<PriceCatalogSnapshotFetch>> + Send + 'future>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            match self {
                Self::Unchanged => {
                    assert_eq!(current_hash, "current-hash");
                    Ok(PriceCatalogSnapshotFetch::Unchanged(
                        PriceCatalogSnapshotMetadata {
                            payload_hash: current_hash.to_owned(),
                            fetched_at_ms: FIXED_UNIX_MILLIS,
                        },
                    ))
                }
                Self::Error => Err(StorageError::Unavailable {
                    message: "test storage unavailable".to_owned(),
                }),
                Self::Corrupted => {
                    assert_eq!(current_hash, "current-hash");
                    Err(StorageError::Corrupted {
                        message: "test price catalog payload hash mismatch".to_owned(),
                    })
                }
            }
        })
    }
}

#[tokio::test]
async fn t3__refresh_once_fetches_installs_and_persists() -> Result<(), Box<dyn std::error::Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_single_http_response(listener, StatusCode::OK, SAMPLE_LITELLM_JSON);
    let dir = tempfile::tempdir()?;
    let storage = InMemoryStorage::new();
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage.clone(),
        format!("http://{address}/prices"),
        Duration::from_secs(60 * 60),
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    loader.refresh_once().await?;

    assert_eq!(server.await??, expected_request());
    assert!(
        catalog
            .lookup("claude-3-5-sonnet-20241022", None, None)
            .is_some()
    );
    assert_eq!(
        catalog.current().payload_hash,
        hex::encode(Sha256::digest(SAMPLE_LITELLM_JSON.as_bytes()))
    );
    let persisted = storage
        .get_price_snapshot()
        .await?
        .expect("refresh must persist the fetched catalog");
    assert_eq!(persisted.json_bytes, SAMPLE_LITELLM_JSON.as_bytes());
    assert_eq!(persisted.fetched_at_ms, FIXED_UNIX_MILLIS);
    Ok(())
}

#[tokio::test]
async fn t3__install_latest_local_reads_disk_cache_after_refresh_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_single_http_response(listener, StatusCode::INTERNAL_SERVER_ERROR, "");
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let storage = InMemoryStorage::new();
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage,
        format!("http://{address}/prices"),
        Duration::from_secs(60 * 60),
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    let error = loader
        .refresh_once()
        .await
        .expect_err("refresh should fail");

    assert_eq!(server.await??, expected_request());
    assert!(error.to_string().contains("unexpected status 500"));
    assert!(loader.install_latest_local().await?);
    assert!(
        catalog
            .lookup("claude-3-5-sonnet-20241022", None, None)
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn t3__install_latest_local_returns_false_without_cache_after_refresh_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_single_http_response(listener, StatusCode::INTERNAL_SERVER_ERROR, "");
    let dir = tempfile::tempdir()?;
    let storage = InMemoryStorage::new();
    let catalog = PriceCatalog::new_empty();
    let loader = LiteLlmLoader::new(
        catalog.clone(),
        storage,
        format!("http://{address}/prices"),
        Duration::from_secs(60 * 60),
        dir.path().join("missing-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    let error = loader
        .refresh_once()
        .await
        .expect_err("refresh should fail");

    assert_eq!(server.await??, expected_request());
    assert!(error.to_string().contains("unexpected status 500"));
    assert!(!loader.install_latest_local().await?);
    assert_eq!(catalog.status(), CatalogStatus::CostDisabled);
    Ok(())
}

#[tokio::test]
async fn t3__install_latest_local_does_not_fetch_or_replace_payload_when_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "current-hash".to_owned(),
        fetched_at_ms: FIXED_UNIX_MILLIS,
        models: HashMap::new(),
        raw_json: vec![b'x'; 1_600_000],
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
    let before = catalog.current();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        Arc::new(LocalPollStorage::Unchanged),
        "http://unused.invalid/prices".to_owned(),
        Duration::from_secs(60 * 60),
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    let installed = loader.install_latest_local().await?;

    assert!(!installed);
    assert!(Arc::ptr_eq(&before, &catalog.current()));
    Ok(())
}

#[tokio::test]
async fn t3__install_latest_local_retains_snapshot_and_skips_disk_fallback_on_storage_error()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "current-hash".to_owned(),
        fetched_at_ms: FIXED_UNIX_MILLIS,
        models: HashMap::new(),
        raw_json: vec![b'x'; 1_600_000],
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
    let before = catalog.current();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        Arc::new(LocalPollStorage::Error),
        "http://unused.invalid/prices".to_owned(),
        Duration::from_secs(60 * 60),
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    let error = loader
        .install_latest_local()
        .await
        .expect_err("storage failure must propagate");

    assert!(error.to_string().contains("test storage unavailable"));
    assert!(Arc::ptr_eq(&before, &catalog.current()));
    Ok(())
}

#[tokio::test]
async fn t3__install_latest_local_propagates_corruption_without_installing_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "current-hash".to_owned(),
        fetched_at_ms: FIXED_UNIX_MILLIS,
        models: HashMap::new(),
        raw_json: b"known-good-catalog".to_vec(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
    let before = catalog.current();
    let loader = LiteLlmLoader::new(
        Arc::clone(&catalog),
        Arc::new(LocalPollStorage::Corrupted),
        "http://unused.invalid/prices".to_owned(),
        Duration::from_secs(60 * 60),
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(FIXED_UNIX_SECS)),
    );

    let error = loader
        .install_latest_local()
        .await
        .expect_err("storage corruption must propagate");

    assert!(matches!(
        error,
        LoaderError::Storage(message)
            if message.contains("test price catalog payload hash mismatch")
    ));
    assert!(Arc::ptr_eq(&before, &catalog.current()));
    Ok(())
}

fn expected_request() -> ObservedRequest {
    ObservedRequest {
        method: "GET".to_owned(),
        path: "/prices".to_owned(),
    }
}
