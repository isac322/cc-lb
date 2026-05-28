use async_trait::async_trait;
use cc_lb_storage_api::{
    ConfigStore, StorageResult,
    types::{ConfigDraftState, HistoryEntry, HistorySummary},
};

use crate::RedbStorage;

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl ConfigStore for RedbStorage {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::get_config_draft(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn put_config_draft(
        &self,
        new: ConfigDraftState,
        expected_revision: u64,
    ) -> StorageResult<u64> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            RedbStorage::put_config_draft(&storage, new, expected_revision)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            RedbStorage::set_last_validated_revision(&storage, revision, error)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn append_config_history(
        &self,
        revision: u64,
        config_toml: String,
        applied_at_unix_secs: u64,
        summary: HistorySummary,
    ) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            RedbStorage::append_config_history(
                &storage,
                revision,
                config_toml,
                applied_at_unix_secs,
                summary,
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn list_config_history(&self, limit: usize) -> StorageResult<Vec<HistoryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::list_config_history(&storage, limit))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_config_history(&self, revision: u64) -> StorageResult<Option<HistoryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::get_config_history(&storage, revision))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}
