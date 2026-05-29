use async_trait::async_trait;

use crate::{BackendKind, RuntimeChangeNotifier, StorageError, StorageResult, types::*};

pub const CURRENT_CONTRACT_VERSION: u32 = 1;

#[async_trait]
pub trait AuditStore: Send + Sync {
    async fn append_audit(&self, entry: &AuditEntry) -> StorageResult<()>;

    async fn append_audit_entries(&self, entries: &[AuditEntry]) -> StorageResult<()> {
        for entry in entries {
            self.append_audit(entry).await?;
        }
        Ok(())
    }

    async fn query_audit(
        &self,
        principal_id: Option<&str>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<AuditEntry>>;

    async fn prune_audit(&self, older_than: u64) -> StorageResult<u64>;

    async fn prune_audit_before(
        &self,
        cutoff_ts_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let _ = cutoff_ts_x_1m;
        let _ = batch_size;
        Err(StorageError::Fatal {
            message: "prune_audit_before is not implemented for this storage backend".to_owned(),
        })
    }
}

#[async_trait]
pub trait RequestEventStore: Send + Sync {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<()>;

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>>;

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let _ = cutoff_ms_x_1m;
        let _ = batch_size;
        Err(StorageError::Fatal {
            message: "prune_request_events_before is not implemented for this storage backend"
                .to_owned(),
        })
    }
}

#[async_trait]
pub trait QuotaStore: Send + Sync {
    async fn incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        amount: u64,
    ) -> StorageResult<u64>;

    async fn try_incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        amount: u64,
        capacity: u64,
    ) -> StorageResult<Option<u64>>;

    async fn get_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
    ) -> StorageResult<u64>;

    async fn adjust_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        delta: i64,
    ) -> StorageResult<u64>;

    async fn sweep_old_quotas(&self, older_than_window_start: u64) -> StorageResult<u64>;
}

#[async_trait]
pub trait LimitStateStore: Send + Sync {
    async fn put_principal_limit_state(&self, state: &PrincipalLimitState) -> StorageResult<()>;

    async fn get_principal_limit_state(
        &self,
        principal_id: &str,
        identity_kind: PrincipalLimitIdentityKind,
        identity_value: Option<&str>,
        window: &str,
        kind: PrincipalLimitKind,
    ) -> StorageResult<Option<PrincipalLimitState>>;

    async fn list_principal_limit_states(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<PrincipalLimitState>>;
}

#[async_trait]
pub trait UsageRollupStore: Send + Sync {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun>;

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>>;

    async fn query_usage_rollups_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>>;

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>>;

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        run: &UsageRollupRun,
    ) -> StorageResult<()>;
}

#[async_trait]
pub trait OAuthCredentialStore: Send + Sync {
    async fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()>;

    async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>>;

    async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool>;

    async fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()>;

    async fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>>;
}

#[async_trait]
pub trait ApiKeyStore: Send + Sync {
    async fn put_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()>;

    async fn get_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<Vec<u8>>>;

    async fn list_api_key_ciphertexts(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>>;

    async fn revoke_api_key(
        &self,
        principal_id: &str,
        key_id: &str,
        revoked_ciphertext: &[u8],
    ) -> StorageResult<bool>;
}

#[async_trait]
pub trait ManagedKeyStore: Send + Sync {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord>;

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>>;

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>>;

    async fn list_by_principal(&self, principal_id: &str)
    -> StorageResult<Vec<StoredApiKeyRecord>>;

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>>;

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> StorageResult<()>;

    async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()>;
}

#[async_trait]
pub trait ConfigStore: Send + Sync {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState>;

    async fn put_config_draft(
        &self,
        new: ConfigDraftState,
        expected_revision: u64,
    ) -> StorageResult<u64>;

    async fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> StorageResult<()>;

    async fn append_config_history(
        &self,
        revision: u64,
        config_toml: String,
        applied_at_unix_secs: u64,
        summary: HistorySummary,
    ) -> StorageResult<()>;

    async fn list_config_history(&self, limit: usize) -> StorageResult<Vec<HistoryEntry>>;

    async fn get_config_history(&self, revision: u64) -> StorageResult<Option<HistoryEntry>>;
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn initialize(&self, requested: BackendKind) -> StorageResult<()>;

    async fn contract_version(&self) -> StorageResult<u32>;

    async fn backend_kind(&self) -> StorageResult<BackendKind>;

    async fn killswitch_enabled(&self) -> StorageResult<bool>;

    async fn set_killswitch_enabled(&self, enabled: bool) -> StorageResult<()>;
}

#[async_trait]
pub trait Storage:
    AuditStore
    + crate::plugin_registry::PluginRegistryStore
    + crate::principal::PrincipalStore
    + crate::upstream::UpstreamStore
    + RequestEventStore
    + QuotaStore
    + LimitStateStore
    + UsageRollupStore
    + OAuthCredentialStore
    + ApiKeyStore
    + ConfigStore
    + MetaStore
    + RuntimeChangeNotifier
    + Send
    + Sync
    + 'static
{
}

impl<T> Storage for T where
    T: AuditStore
        + crate::plugin_registry::PluginRegistryStore
        + crate::principal::PrincipalStore
        + crate::upstream::UpstreamStore
        + RequestEventStore
        + QuotaStore
        + LimitStateStore
        + UsageRollupStore
        + OAuthCredentialStore
        + ApiKeyStore
        + ConfigStore
        + MetaStore
        + RuntimeChangeNotifier
        + Send
        + Sync
        + 'static
{
}
