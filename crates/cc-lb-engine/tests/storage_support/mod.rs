#![allow(dead_code)]

use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use cc_lb_storage_api as storage_api;

#[derive(Default)]
pub struct TestStorage {
    request_events: Mutex<Vec<storage_api::RequestEvent>>,
    request_event_cursor: AtomicU64,
    config_draft: Mutex<storage_api::ConfigDraftState>,
    effective_config: Mutex<Option<storage_api::EffectiveConfig>>,
    killswitch_enabled: Mutex<bool>,
}

impl TestStorage {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn as_request_event_store(self: &Arc<Self>) -> Arc<dyn storage_api::RequestEventStore> {
        self.clone()
    }
}

#[async_trait]
impl storage_api::AuditStore for TestStorage {
    async fn append_audit(
        &self,
        _entry: &storage_api::AuditEntry,
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }

    async fn query_audit(
        &self,
        _principal_id: Option<&str>,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::AuditEntry>> {
        Ok(Vec::new())
    }

    async fn prune_audit(&self, _older_than: u64) -> storage_api::StorageResult<u64> {
        Ok(0)
    }
}

#[async_trait]
impl storage_api::RequestEventStore for TestStorage {
    async fn append_request_event(
        &self,
        event: &storage_api::RequestEvent,
    ) -> storage_api::StorageResult<u64> {
        lock_or_storage_error(&self.request_events)?.push(event.clone());
        Ok(self.request_event_cursor.fetch_add(1, Ordering::Relaxed) + 1)
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::RequestEvent>> {
        let mut events = lock_or_storage_error(&self.request_events)?
            .iter()
            .filter(|event| event.ts >= since && event.ts <= until)
            .cloned()
            .collect::<Vec<_>>();
        events.truncate(limit);
        Ok(events)
    }

    async fn current_request_event_cursor(&self) -> storage_api::StorageResult<u64> {
        Ok(self.request_event_cursor.load(Ordering::Relaxed))
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        filters: &storage_api::RequestEventStreamFilters,
    ) -> storage_api::StorageResult<Vec<(u64, storage_api::RequestEvent)>> {
        let rows = lock_or_storage_error(&self.request_events)?
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                let cursor = u64::try_from(index).ok()?.saturating_add(1);
                if cursor > after
                    && cursor <= until
                    && request_event_matches_filters(event, filters)
                {
                    Some((cursor, event.clone()))
                } else {
                    None
                }
            })
            .take(limit.min(500))
            .collect();
        Ok(rows)
    }
}

fn request_event_matches_filters(
    event: &storage_api::RequestEvent,
    filters: &storage_api::RequestEventStreamFilters,
) -> bool {
    if let Some(principal_id) = filters.principal_id.as_deref()
        && event.principal_id.as_deref() != Some(principal_id)
    {
        return false;
    }
    if let Some(model) = filters.model.as_deref()
        && event.model.as_deref() != Some(model)
    {
        return false;
    }
    if let Some(upstream) = filters.upstream
        && event.upstream != Some(upstream)
    {
        return false;
    }
    if let Some(upstream_id) = filters.upstream_id
        && event.upstream_id != Some(upstream_id)
    {
        return false;
    }
    if let Some(status_class) = filters.status_class
        && !status_class.matches(event.status)
    {
        return false;
    }
    true
}

#[async_trait]
impl storage_api::UsageRollupStore for TestStorage {
    async fn rollup_usage_once(&self) -> storage_api::StorageResult<storage_api::UsageRollupRun> {
        Ok(storage_api::UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: None,
        })
    }

    async fn query_usage_rollups(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::UsageRollup>> {
        Ok(Vec::new())
    }

    async fn query_usage_rollups_in_range(
        &self,
        _resolution: storage_api::UsageRollupResolution,
        _window_start_unix_secs: u64,
        _window_end_unix_secs: u64,
    ) -> storage_api::StorageResult<Vec<storage_api::UsageRollup>> {
        Ok(Vec::new())
    }

    async fn usage_rollup_checkpoint(&self) -> storage_api::StorageResult<Option<u64>> {
        Ok(None)
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        _run: &storage_api::UsageRollupRun,
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }
}

#[async_trait]
impl storage_api::OAuthCredentialStore for TestStorage {
    async fn put_oauth_ciphertext(
        &self,
        _principal_id: &str,
        _provider: &str,
        _ciphertext: &[u8],
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }

    async fn get_oauth_ciphertext(
        &self,
        _principal_id: &str,
        _provider: &str,
    ) -> storage_api::StorageResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn delete_oauth(
        &self,
        _principal_id: &str,
        _provider: &str,
    ) -> storage_api::StorageResult<bool> {
        Ok(false)
    }

    async fn put_anthropic_api_key_ciphertext(
        &self,
        _storage_key: &str,
        _ciphertext: &[u8],
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        _storage_key: &str,
    ) -> storage_api::StorageResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

#[async_trait]
impl storage_api::ApiKeyStore for TestStorage {
    async fn put_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _ciphertext: &[u8],
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }

    async fn get_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
    ) -> storage_api::StorageResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn list_api_key_ciphertexts(
        &self,
        _principal_id: &str,
    ) -> storage_api::StorageResult<Vec<(String, Vec<u8>)>> {
        Ok(Vec::new())
    }

    async fn revoke_api_key(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _revoked_ciphertext: &[u8],
    ) -> storage_api::StorageResult<bool> {
        Ok(false)
    }
}

#[async_trait]
impl storage_api::ConfigStore for TestStorage {
    async fn get_config_draft(&self) -> storage_api::StorageResult<storage_api::ConfigDraftState> {
        Ok(lock_or_storage_error(&self.config_draft)?.clone())
    }

    async fn put_config_draft(
        &self,
        mut new: storage_api::ConfigDraftState,
        expected_revision: u64,
    ) -> storage_api::StorageResult<u64> {
        let mut draft = lock_or_storage_error(&self.config_draft)?;
        if draft.revision != expected_revision {
            return Err(storage_api::StorageError::Conflict {
                message: format!(
                    "expected revision {expected_revision}, found {}",
                    draft.revision
                ),
            });
        }
        new.revision = expected_revision + 1;
        *draft = new;
        Ok(draft.revision)
    }

    async fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> storage_api::StorageResult<()> {
        let mut draft = lock_or_storage_error(&self.config_draft)?;
        draft.last_validated_revision = Some(revision);
        draft.last_validation_error = error;
        Ok(())
    }

    async fn get_effective_config(
        &self,
    ) -> storage_api::StorageResult<Option<storage_api::EffectiveConfig>> {
        Ok(lock_or_storage_error(&self.effective_config)?.clone())
    }

    async fn put_effective_config(
        &self,
        revision: u64,
        config: serde_json::Value,
        applied_at_unix_secs: u64,
    ) -> storage_api::StorageResult<()> {
        *lock_or_storage_error(&self.effective_config)? = Some(storage_api::EffectiveConfig {
            revision,
            config,
            applied_at_unix_secs,
        });
        Ok(())
    }

    async fn append_config_history(
        &self,
        _revision: u64,
        _config_toml: String,
        _applied_at_unix_secs: u64,
        _summary: storage_api::HistorySummary,
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }

    async fn list_config_history(
        &self,
        _limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::HistoryEntry>> {
        Ok(Vec::new())
    }

    async fn get_config_history(
        &self,
        _revision: u64,
    ) -> storage_api::StorageResult<Option<storage_api::HistoryEntry>> {
        Ok(None)
    }
}

#[async_trait]
impl storage_api::MetaStore for TestStorage {
    async fn initialize(
        &self,
        _requested: storage_api::BackendKind,
    ) -> storage_api::StorageResult<()> {
        Ok(())
    }

    async fn contract_version(&self) -> storage_api::StorageResult<u32> {
        Ok(storage_api::CURRENT_CONTRACT_VERSION)
    }

    async fn backend_kind(&self) -> storage_api::StorageResult<storage_api::BackendKind> {
        Ok(storage_api::BackendKind::Sqlite)
    }

    async fn killswitch_enabled(&self) -> storage_api::StorageResult<bool> {
        Ok(*lock_or_storage_error(&self.killswitch_enabled)?)
    }

    async fn set_killswitch_enabled(&self, enabled: bool) -> storage_api::StorageResult<()> {
        *lock_or_storage_error(&self.killswitch_enabled)? = enabled;
        Ok(())
    }
}

fn lock_or_storage_error<T>(mutex: &Mutex<T>) -> storage_api::StorageResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| storage_api::StorageError::Fatal {
        message: "test storage lock poisoned".to_owned(),
    })
}
