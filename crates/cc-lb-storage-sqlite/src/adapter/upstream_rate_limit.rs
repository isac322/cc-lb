use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl UpstreamRateLimitStateStore for SqliteStorage {
    async fn put_observation(
        &self,
        _record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn list_for_upstream_ids(
        &self,
        _upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        unimplemented!()
    }
}
