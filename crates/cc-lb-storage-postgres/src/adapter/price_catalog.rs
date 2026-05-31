use async_trait::async_trait;
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord, StorageResult};

use crate::adapter::PostgresStorage;

#[async_trait]
impl PriceCatalogCache for PostgresStorage {
    async fn put_price_snapshot(
        &self,
        _json_bytes: &[u8],
        _fetched_at_ms: u64,
    ) -> StorageResult<()> {
        // TODO(price-catalog-pg-0022): wire to litellm_price_catalog table once the schema lands.
        Ok(())
    }

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        Ok(None)
    }
}
