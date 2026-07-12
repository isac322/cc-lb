use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    PriceCatalogCache, PriceCatalogSnapshotFetch, PriceCatalogSnapshotMetadata,
    PriceCatalogSnapshotRecord,
};
use sha2::{Digest, Sha256};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn roundtrip_smoke<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PriceCatalogCache,
{
    with_conformance_fixture(backend, |storage| async move {
        ensure!(
            storage.get_price_snapshot().await?.is_none(),
            "empty price catalog should return None"
        );
        ensure!(
            storage.get_price_snapshot_if_changed("missing").await?
                == PriceCatalogSnapshotFetch::Missing,
            "conditional fetch should report a missing catalog"
        );

        let first_json_bytes = br#"{"models":[]}"#;
        let fetched_at_ms = 1_765_000_123;
        storage
            .put_price_snapshot(first_json_bytes, fetched_at_ms)
            .await?;

        let json_bytes = br#"{
  "models": [
    {"id": "claude-sonnet-4-5", "price": 12345}
  ]
}"#;
        storage
            .put_price_snapshot(json_bytes, fetched_at_ms)
            .await?;

        let payload_hash = sha256_hex(json_bytes);
        ensure!(
            storage
                .get_price_snapshot_if_changed("outdated-hash")
                .await?
                == PriceCatalogSnapshotFetch::Changed(PriceCatalogSnapshotRecord {
                    json_bytes: json_bytes.to_vec(),
                    fetched_at_ms,
                }),
            "changed conditional fetch must return the deterministic latest raw payload"
        );
        ensure!(
            storage.get_price_snapshot_if_changed(&payload_hash).await?
                == PriceCatalogSnapshotFetch::Unchanged(PriceCatalogSnapshotMetadata {
                    payload_hash: payload_hash.clone(),
                    fetched_at_ms,
                }),
            "unchanged conditional fetch must return metadata without payload"
        );

        let snapshot = storage
            .get_price_snapshot()
            .await?
            .context("snapshot should exist")?;
        ensure!(
            snapshot
                == PriceCatalogSnapshotRecord {
                    json_bytes: json_bytes.to_vec(),
                    fetched_at_ms,
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

        let snapshot = storage
            .get_price_snapshot()
            .await?
            .context("snapshot should exist after repeated puts")?;
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
