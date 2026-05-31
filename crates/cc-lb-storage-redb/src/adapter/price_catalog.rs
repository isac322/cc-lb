use async_trait::async_trait;
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord, StorageResult};

use crate::RedbStorage;
use crate::adapter::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl PriceCatalogCache for RedbStorage {
    async fn put_price_snapshot(&self, json_bytes: &[u8], fetched_at_ms: u64) -> StorageResult<()> {
        let storage = self.clone();
        let bytes = json_bytes.to_vec();
        tokio::task::spawn_blocking(move || {
            RedbStorage::put_price_snapshot(&storage, &bytes, fetched_at_ms)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        let storage = self.clone();
        let snapshot =
            tokio::task::spawn_blocking(move || RedbStorage::get_price_snapshot(&storage))
                .await
                .map_err(map_join_err)?
                .map_err(map_redb_err)?;
        Ok(snapshot.map(|s| PriceCatalogSnapshotRecord {
            json_bytes: s.json_bytes,
            fetched_at_ms: s.fetched_at_ms,
        }))
    }
}
