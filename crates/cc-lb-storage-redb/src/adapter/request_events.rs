use async_trait::async_trait;
use cc_lb_storage_api::{RequestEventStore, StorageResult, types::RequestEvent};

use crate::RedbStorage;

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl RequestEventStore for RedbStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<()> {
        let storage = self.clone();
        let event = event.clone();

        tokio::task::spawn_blocking(move || RedbStorage::append_request_event(&storage, &event))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::query_request_events(&storage, since, until, limit)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn query_recent_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::query_recent_request_events(&storage, since, until, limit)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::prune_request_events_before(&storage, cutoff_ms_x_1m, batch_size)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}
