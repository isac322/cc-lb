use async_trait::async_trait;
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl PriceCatalogCache for SqliteStorage {
    async fn put_price_snapshot(
        &self,
        _json_bytes: &[u8],
        _fetched_at_ms: u64,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        unimplemented!()
    }
}
