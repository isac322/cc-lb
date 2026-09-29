use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, LiteLlmLoader, LoaderError, PriceCatalog};
use cc_lb_storage_api::{
    MetaStore, PriceCatalogCache, PriceCatalogSnapshotFetch, PriceCatalogSnapshotMetadata,
    StorageError, StorageResult,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sha2::{Digest as _, Sha256};
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
                            fetched_at_ms: 1_700_000_000_000,
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
async fn fetch_persist_and_install_latest_local_round_trips_catalog()
-> Result<(), Box<dyn std::error::Error>> {
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
        dir.path().join("litellm-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    let fetched = loader.fetch_and_fingerprint().await?;
    loader.persist_snapshot(&fetched).await?;
    assert!(loader.install_latest_local().await?);
    assert!(catalog.lookup("claude-3-5-sonnet-20241022", None).is_some());
    assert_eq!(
        catalog.current().payload_hash,
        hex::encode(Sha256::digest(SAMPLE_LITELLM_JSON.as_bytes()))
    );
    assert!(matches!(
        storage.get_price_snapshot_if_changed("").await?,
        PriceCatalogSnapshotFetch::Changed(_)
    ));
    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn install_latest_local_reads_disk_cache_after_fetch_failure()
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
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    let error = loader
        .fetch_and_fingerprint()
        .await
        .expect_err("fetch should fail");
    assert!(error.to_string().contains("unexpected status 500"));
    assert!(loader.install_latest_local().await?);
    assert!(catalog.lookup("claude-3-5-sonnet-20241022", None).is_some());
    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn install_latest_local_returns_false_without_cache_after_fetch_failure()
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
        dir.path().join("missing-cache.json"),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    let error = loader
        .fetch_and_fingerprint()
        .await
        .expect_err("fetch should fail");
    assert!(error.to_string().contains("unexpected status 500"));
    assert!(!loader.install_latest_local().await?);
    assert_eq!(catalog.status(), CatalogStatus::CostDisabled);
    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn install_latest_local_does_not_fetch_or_replace_payload_when_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    // Given an installed large snapshot and stale fallback bytes on disk.
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "current-hash".to_owned(),
        fetched_at_ms: 1_700_000_000_000,
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
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    // When storage reports that the current hash is unchanged.
    let installed = loader.install_latest_local().await?;

    // Then neither the payload accessor nor disk fallback runs, and the Arc is unchanged.
    assert!(!installed);
    assert!(Arc::ptr_eq(&before, &catalog.current()));
    Ok(())
}

#[tokio::test]
async fn install_latest_local_retains_snapshot_and_skips_disk_fallback_on_storage_error()
-> Result<(), Box<dyn std::error::Error>> {
    // Given an installed snapshot and stale fallback bytes on disk.
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "current-hash".to_owned(),
        fetched_at_ms: 1_700_000_000_000,
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
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    // When the conditional storage lookup fails.
    let error = loader
        .install_latest_local()
        .await
        .expect_err("storage failure must propagate");

    // Then stale disk bytes are not installed and the prior Arc remains live.
    assert!(error.to_string().contains("test storage unavailable"));
    assert!(Arc::ptr_eq(&before, &catalog.current()));
    Ok(())
}

#[tokio::test]
async fn install_latest_local_propagates_corruption_without_installing_payload()
-> Result<(), Box<dyn std::error::Error>> {
    // Given a known-good installed snapshot and stale fallback bytes on disk.
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("litellm-cache.json");
    tokio::fs::write(&cache_path, SAMPLE_LITELLM_JSON).await?;
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "current-hash".to_owned(),
        fetched_at_ms: 1_700_000_000_000,
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
        cache_path,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    );

    // When storage rejects the current row because its payload hash is corrupted.
    let error = loader
        .install_latest_local()
        .await
        .expect_err("storage corruption must propagate");

    // Then no payload or stale disk fallback is installed and the prior Arc remains live.
    assert!(matches!(
        error,
        LoaderError::Storage(message)
            if message.contains("test price catalog payload hash mismatch")
    ));
    assert!(Arc::ptr_eq(&before, &catalog.current()));
    Ok(())
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &database_url,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
    )
    .await?;
    storage.initialize().await?;
    Ok(storage)
}
