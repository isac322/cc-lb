use async_trait::async_trait;
use cc_lb_storage_api::{
    types::{
        ConfigDraftState as ApiConfigDraftState, HistoryEntry as ApiHistoryEntry,
        HistorySummary as ApiHistorySummary,
    },
    ConfigStore, StorageResult,
};

use crate::{
    ConfigDraftState as RedbConfigDraftState, HistoryEntry as RedbHistoryEntry,
    HistorySummary as RedbHistorySummary, RedbStorage,
};

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl ConfigStore for RedbStorage {
    async fn get_config_draft(&self) -> StorageResult<ApiConfigDraftState> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::get_config_draft(&storage))
            .await
            .map_err(map_join_err)?
            .map(to_api_config_draft_state)
            .map_err(map_redb_err)
    }

    async fn put_config_draft(
        &self,
        new: ApiConfigDraftState,
        expected_revision: u64,
    ) -> StorageResult<u64> {
        let storage = self.clone();
        let new = to_redb_config_draft_state(new);
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
        summary: ApiHistorySummary,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let summary = to_redb_history_summary(summary);
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

    async fn list_config_history(&self, limit: usize) -> StorageResult<Vec<ApiHistoryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::list_config_history(&storage, limit))
            .await
            .map_err(map_join_err)?
            .map(|entries| entries.into_iter().map(to_api_history_entry).collect())
            .map_err(map_redb_err)
    }

    async fn get_config_history(&self, revision: u64) -> StorageResult<Option<ApiHistoryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::get_config_history(&storage, revision))
            .await
            .map_err(map_join_err)?
            .map(|entry| entry.map(to_api_history_entry))
            .map_err(map_redb_err)
    }
}

fn to_redb_config_draft_state(state: ApiConfigDraftState) -> RedbConfigDraftState {
    RedbConfigDraftState {
        draft: state.draft,
        revision: state.revision,
        last_validated_revision: state.last_validated_revision,
        last_validation_error: state.last_validation_error,
        saved_at_unix_secs: state.saved_at_unix_secs,
    }
}

fn to_api_config_draft_state(state: RedbConfigDraftState) -> ApiConfigDraftState {
    ApiConfigDraftState {
        draft: state.draft,
        revision: state.revision,
        last_validated_revision: state.last_validated_revision,
        last_validation_error: state.last_validation_error,
        saved_at_unix_secs: state.saved_at_unix_secs,
    }
}

fn to_redb_history_summary(summary: ApiHistorySummary) -> RedbHistorySummary {
    RedbHistorySummary {
        upstreams: summary.upstreams,
        principals: summary.principals,
        plugin_count: summary.plugin_count,
        tls_enabled: summary.tls_enabled,
    }
}

fn to_api_history_summary(summary: RedbHistorySummary) -> ApiHistorySummary {
    ApiHistorySummary {
        upstreams: summary.upstreams,
        principals: summary.principals,
        plugin_count: summary.plugin_count,
        tls_enabled: summary.tls_enabled,
    }
}

fn to_api_history_entry(entry: RedbHistoryEntry) -> ApiHistoryEntry {
    ApiHistoryEntry {
        revision: entry.revision,
        config_toml: entry.config_toml,
        applied_at_unix_secs: entry.applied_at_unix_secs,
        summary: to_api_history_summary(entry.summary),
    }
}
