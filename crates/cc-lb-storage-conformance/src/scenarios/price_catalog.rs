use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

#[ignore = "un-ignored in T22/T23"]
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
