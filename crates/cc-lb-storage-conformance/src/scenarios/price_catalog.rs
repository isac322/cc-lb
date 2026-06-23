use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord};

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

        let first_json_bytes = br#"{"models":[]}"#;
        storage
            .put_price_snapshot(first_json_bytes, 1_765_000_100)
            .await?;

        let json_bytes = br#"{"models":[{"id":"claude-sonnet-4-5","price":12345}]}"#;
        let fetched_at_ms = 1_765_000_123;
        storage
            .put_price_snapshot(json_bytes, fetched_at_ms)
            .await?;

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
