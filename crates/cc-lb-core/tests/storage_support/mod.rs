#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use cc_lb_storage_api as storage_api;

type QuotaKey = (String, u64, storage_api::BucketKind);

#[derive(Default)]
pub struct TestStorage {
    quotas: Mutex<HashMap<QuotaKey, u64>>,
    request_events: Mutex<Vec<storage_api::RequestEvent>>,
    limit_states: Mutex<Vec<storage_api::PrincipalLimitState>>,
    config_draft: Mutex<storage_api::ConfigDraftState>,
    killswitch_enabled: Mutex<bool>,
}

impl TestStorage {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn as_storage(self: &Arc<Self>) -> Arc<dyn storage_api::Storage> {
        self.clone()
    }

    pub async fn incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: storage_api::BucketKind,
        amount: u64,
    ) -> storage_api::StorageResult<u64> {
        storage_api::QuotaStore::incr_quota(self, principal_id, window_start, kind, amount).await
    }

    pub async fn get_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: storage_api::BucketKind,
    ) -> storage_api::StorageResult<u64> {
        storage_api::QuotaStore::get_quota(self, principal_id, window_start, kind).await
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
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.request_events)?.push(event.clone());
        Ok(())
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
}

#[async_trait]
impl storage_api::QuotaStore for TestStorage {
    async fn incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: storage_api::BucketKind,
        amount: u64,
    ) -> storage_api::StorageResult<u64> {
        let mut quotas = lock_or_storage_error(&self.quotas)?;
        let key = quota_key(principal_id, window_start, kind);
        let current = quotas.get(&key).copied().unwrap_or(0);
        let updated =
            current
                .checked_add(amount)
                .ok_or_else(|| storage_api::StorageError::Fatal {
                    message: "test quota counter overflow".to_owned(),
                })?;
        quotas.insert(key, updated);
        Ok(updated)
    }

    async fn try_incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: storage_api::BucketKind,
        amount: u64,
        capacity: u64,
    ) -> storage_api::StorageResult<Option<u64>> {
        let mut quotas = lock_or_storage_error(&self.quotas)?;
        let key = quota_key(principal_id, window_start, kind);
        let current = quotas.get(&key).copied().unwrap_or(0);
        let Some(updated) = current.checked_add(amount) else {
            return Err(storage_api::StorageError::Fatal {
                message: "test quota counter overflow".to_owned(),
            });
        };
        if updated > capacity {
            return Ok(None);
        }
        quotas.insert(key, updated);
        Ok(Some(updated))
    }

    async fn get_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: storage_api::BucketKind,
    ) -> storage_api::StorageResult<u64> {
        let quotas = lock_or_storage_error(&self.quotas)?;
        Ok(quotas
            .get(&quota_key(principal_id, window_start, kind))
            .copied()
            .unwrap_or(0))
    }

    async fn adjust_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: storage_api::BucketKind,
        delta: i64,
    ) -> storage_api::StorageResult<u64> {
        let mut quotas = lock_or_storage_error(&self.quotas)?;
        let key = quota_key(principal_id, window_start, kind);
        let current = quotas.get(&key).copied().unwrap_or(0);
        let updated = if delta >= 0 {
            current.checked_add(delta as u64)
        } else {
            current.checked_sub(delta.unsigned_abs())
        }
        .ok_or_else(|| storage_api::StorageError::Fatal {
            message: "test quota adjustment out of range".to_owned(),
        })?;
        quotas.insert(key, updated);
        Ok(updated)
    }

    async fn sweep_old_quotas(
        &self,
        older_than_window_start: u64,
    ) -> storage_api::StorageResult<u64> {
        let mut quotas = lock_or_storage_error(&self.quotas)?;
        let before = quotas.len();
        quotas.retain(|(_, window_start, _), _| *window_start >= older_than_window_start);
        Ok((before - quotas.len()) as u64)
    }
}

#[async_trait]
impl storage_api::LimitStateStore for TestStorage {
    async fn put_principal_limit_state(
        &self,
        state: &storage_api::PrincipalLimitState,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.limit_states)?.push(state.clone());
        Ok(())
    }

    async fn get_principal_limit_state(
        &self,
        principal_id: &str,
        identity_kind: storage_api::PrincipalLimitIdentityKind,
        identity_value: Option<&str>,
        window: &str,
        kind: storage_api::PrincipalLimitKind,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalLimitState>> {
        let states = lock_or_storage_error(&self.limit_states)?;
        Ok(states
            .iter()
            .find(|state| {
                state.principal_id == principal_id
                    && state.identity_kind == identity_kind
                    && state.identity_value.as_deref() == identity_value
                    && state.window == window
                    && state.kind == kind
            })
            .cloned())
    }

    async fn list_principal_limit_states(
        &self,
        principal_id: &str,
    ) -> storage_api::StorageResult<Vec<storage_api::PrincipalLimitState>> {
        Ok(lock_or_storage_error(&self.limit_states)?
            .iter()
            .filter(|state| state.principal_id == principal_id)
            .cloned()
            .collect())
    }
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
        Ok(storage_api::BackendKind::Redb)
    }

    async fn killswitch_enabled(&self) -> storage_api::StorageResult<bool> {
        Ok(*lock_or_storage_error(&self.killswitch_enabled)?)
    }

    async fn set_killswitch_enabled(&self, enabled: bool) -> storage_api::StorageResult<()> {
        *lock_or_storage_error(&self.killswitch_enabled)? = enabled;
        Ok(())
    }
}

fn quota_key(principal_id: &str, window_start: u64, kind: storage_api::BucketKind) -> QuotaKey {
    (principal_id.to_owned(), window_start, kind)
}

fn lock_or_storage_error<T>(mutex: &Mutex<T>) -> storage_api::StorageResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| storage_api::StorageError::Fatal {
        message: "test storage lock poisoned".to_owned(),
    })
}
