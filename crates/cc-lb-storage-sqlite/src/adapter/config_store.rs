use async_trait::async_trait;
use cc_lb_storage_api::{
    ConfigDraftState, ConfigStore, HistoryEntry, HistorySummary, StorageResult,
};

use crate::SqliteStorage;

#[async_trait]
impl ConfigStore for SqliteStorage {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState> {
        unimplemented!()
    }

    async fn put_config_draft(
        &self,
        _new: ConfigDraftState,
        _expected_revision: u64,
    ) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn set_last_validated_revision(
        &self,
        _revision: u64,
        _error: Option<String>,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn append_config_history(
        &self,
        _revision: u64,
        _config_toml: String,
        _applied_at_unix_secs: u64,
        _summary: HistorySummary,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn list_config_history(&self, _limit: usize) -> StorageResult<Vec<HistoryEntry>> {
        unimplemented!()
    }

    async fn get_config_history(&self, _revision: u64) -> StorageResult<Option<HistoryEntry>> {
        unimplemented!()
    }
}
