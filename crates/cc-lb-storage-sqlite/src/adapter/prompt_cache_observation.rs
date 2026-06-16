use async_trait::async_trait;
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore, StorageResult};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl PromptCacheObservationStore for SqliteStorage {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn list_active_for_upstream(
        &self,
        _upstream_id: Uuid,
        _not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        unimplemented!()
    }

    async fn purge_expired_before(&self, _ts_unix_secs: u64) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn count(&self) -> StorageResult<u64> {
        unimplemented!()
    }
}
