use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UsageRollupStore,
    types::{UsageRollup, UsageRollupResolution, UsageRollupRun},
};

use crate::RedbStorage;

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl UsageRollupStore for RedbStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || RedbStorage::rollup_usage_once(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || RedbStorage::query_usage_rollups(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn query_usage_rollups_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::query_usage_rollups_in_range(
                &storage,
                resolution,
                window_start_unix_secs,
                window_end_unix_secs,
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || RedbStorage::usage_rollup_checkpoint(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        run: &UsageRollupRun,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let _run = run.clone();

        tokio::task::spawn_blocking(move || RedbStorage::rollup_usage_once(&storage))
            .await
            .map_err(map_join_err)?
            .map(|_| ())
            .map_err(map_redb_err)
    }
}
