use async_trait::async_trait;
use cc_lb_storage_api::{RequestEvent, RequestEventStore, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl RequestEventStore for SqliteStorage {
    async fn append_request_event(&self, _event: &RequestEvent) -> StorageResult<()> {
        unimplemented!()
    }

    async fn query_request_events(
        &self,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        unimplemented!()
    }

    async fn query_recent_request_events(
        &self,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        unimplemented!()
    }

    async fn prune_request_events_before(
        &self,
        _cutoff_ms_x_1m: u64,
        _batch_size: usize,
    ) -> StorageResult<u64> {
        unimplemented!()
    }
}
