use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use async_trait::async_trait;
use cc_lb_storage_api::{
    PriceCatalogCache, PriceCatalogSnapshotFetch, PriceCatalogSnapshotMetadata,
    PriceCatalogSnapshotRecord, StorageError,
};
use sha2::{Digest, Sha256};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

#[async_trait]
pub trait PriceCatalogCorruptionBackend: ConformanceBackend {
    async fn corrupt_latest_price_catalog_hash(&self, fixture: &Self::Fixture) -> Result<()>;
}

pub async fn roundtrip_smoke<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PriceCatalogCache,
{
    with_conformance_fixture(backend, |storage| async move {
        ensure!(
            storage.get_price_snapshot_if_changed("missing").await?
                == PriceCatalogSnapshotFetch::Missing,
            "conditional fetch should report a missing catalog"
        );

        let first_json_bytes = br#"{"models":[]}"#;
        let same_fetched_at_ms = 1_765_000_123;
        storage
            .put_price_snapshot(first_json_bytes, same_fetched_at_ms)
            .await?;

        let json_bytes = br#"{
  "models": [
    {"id": "claude-sonnet-4-5", "price": 12345}
  ]
        }"#;
        storage
            .put_price_snapshot(json_bytes, same_fetched_at_ms)
            .await?;

        let first_payload_hash = sha256_hex(first_json_bytes);
        let payload_hash = sha256_hex(json_bytes);
        let expected_latest = PriceCatalogSnapshotRecord {
            json_bytes: json_bytes.to_vec(),
            fetched_at_ms: same_fetched_at_ms,
        };
        ensure!(
            storage.get_price_snapshot_if_changed("").await?
                == PriceCatalogSnapshotFetch::Changed(expected_latest.clone()),
            "an absent current hash must return the latest raw payload"
        );
        ensure!(
            storage
                .get_price_snapshot_if_changed(&first_payload_hash)
                .await?
                == PriceCatalogSnapshotFetch::Changed(expected_latest.clone()),
            "an old hash must return the higher-id payload when fetched_at_ms is tied"
        );
        ensure!(
            storage.get_price_snapshot_if_changed(&payload_hash).await?
                == PriceCatalogSnapshotFetch::Unchanged(PriceCatalogSnapshotMetadata {
                    payload_hash: payload_hash.clone(),
                    fetched_at_ms: same_fetched_at_ms,
                }),
            "unchanged conditional fetch must return metadata without payload"
        );

        let PriceCatalogSnapshotFetch::Changed(snapshot) =
            storage.get_price_snapshot_if_changed("").await?
        else {
            bail!("snapshot should exist");
        };
        ensure!(
            snapshot
                == PriceCatalogSnapshotRecord {
                    json_bytes: json_bytes.to_vec(),
                    fetched_at_ms: same_fetched_at_ms,
                },
            "price catalog snapshot must round-trip"
        );
        ensure!(
            snapshot.json_bytes == json_bytes,
            "price catalog json bytes must round-trip"
        );

        Ok(())
    })
    .await
}

pub async fn corrupted_payload_hash_is_rejected<B>(backend: Arc<B>) -> Result<()>
where
    B: PriceCatalogCorruptionBackend,
    B::Storage: PriceCatalogCache,
{
    let fixture = backend.create_fixture().await?;
    let result = async {
        let storage = backend.open(&fixture).await?;
        storage
            .put_price_snapshot(br#"{"models":[]}"#, 1_765_000_123)
            .await?;
        backend.corrupt_latest_price_catalog_hash(&fixture).await?;

        let error = storage
            .get_price_snapshot_if_changed("")
            .await
            .expect_err("a mismatched stored payload hash must be rejected");
        ensure!(
            matches!(error, StorageError::Corrupted { .. }),
            "a mismatched stored payload hash must return StorageError::Corrupted, got {error}"
        );
        Ok(())
    }
    .await;
    let teardown = backend.teardown(fixture).await;
    result?;
    teardown
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub async fn put_same_payload_twice_updates_fetched_at<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PriceCatalogCache,
{
    with_conformance_fixture(backend, |storage| async move {
        let payload = br#"{"models":[{"id":"claude-sonnet-4-5","price":12345}]}"#;
        storage.put_price_snapshot(payload, 1_000).await?;
        storage.put_price_snapshot(payload, 2_000).await?;
        storage.put_price_snapshot(payload, 1_500).await?;

        let PriceCatalogSnapshotFetch::Changed(snapshot) =
            storage.get_price_snapshot_if_changed("").await?
        else {
            bail!("snapshot should exist after repeated puts");
        };
        ensure!(
            snapshot.json_bytes == payload,
            "payload should round-trip after dedup"
        );
        ensure!(
            snapshot.fetched_at_ms == 1_500,
            "fetched_at_ms should reflect the last put (got {})",
            snapshot.fetched_at_ms,
        );
        Ok(())
    })
    .await
}
