use async_trait::async_trait;

use crate::{
    BackendKind, CacheKeepaliveDecisionRow, RequestEventHistogramBucket,
    RequestEventHistogramQuery, RequestEventKeyLastUsed, RequestEventKeyLastUsedQuery,
    RequestEventKeyUsageBucket, RequestEventKeyUsageQuery, RequestEventPrincipalCostBucket,
    RequestEventPrincipalCostQuery, RequestEventProjections, RuntimeChangeNotifier, StorageError,
    StorageResult,
    anthropic_compatibility_kv::AnthropicCompatibilityKvStore,
    cache_keepalive_sessions::{CacheKeepaliveSessionReadStore, CacheKeepaliveSessionStore},
    organization_metadata::OrganizationMetadataStore,
    prompt_cache_observation::PromptCacheObservationStore,
    types::*,
    upstream_rate_limit::UpstreamRateLimitStateStore,
    upstream_subscription_metadata::UpstreamSubscriptionMetadataStore,
    upstream_subscription_quota::{
        UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore,
    },
    warmup_attempts::UpstreamWarmupAttemptStore,
};

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
    /// Idempotent insert. On duplicate `event_id`, returns the existing row's cursor via a SELECT fallback. This is load-bearing for retry semantics.
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64>;

    async fn append_request_event_with_projections(
        &self,
        event: &RequestEvent,
        projections: &RequestEventProjections,
    ) -> StorageResult<u64> {
        let _ = projections;
        self.append_request_event(event).await
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>>;

    /// Return at most `limit` events within `[since, until]` ordered by
    /// timestamp DESCENDING (newest first). The descending direction is the
    /// load-bearing contract: callers serving "recent events" rely on this to
    /// not lose newly-written events when `limit` is small. Backends MUST
    /// scan in reverse instead of pulling oldest-first and re-sorting.
    async fn query_recent_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        let _ = (since, until, limit);
        Err(StorageError::Fatal {
            message: "query_recent_request_events is not implemented for this storage backend"
                .to_owned(),
        })
    }

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

    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        Err(StorageError::Fatal {
            message: "current_request_event_cursor not implemented".to_owned(),
        })
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        filters: &RequestEventStreamFilters,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        let _ = (after, until, limit, filters);
        Err(StorageError::Fatal {
            message: "query_request_events_between_cursors not implemented".to_owned(),
        })
    }

    /// Returns a page of the request-event LIST view: cursor, limit, and
    /// `query.filters` are all evaluated in SQL, and only the columns the
    /// list view displays are selected and decoded (never the full
    /// `payload`). See [`crate::RequestEventListQuery`] for the exact
    /// ordering/cursor contract.
    async fn list_request_events(
        &self,
        query: &RequestEventListQuery,
    ) -> StorageResult<Vec<RequestEventListItem>> {
        let _ = query;
        Err(StorageError::Fatal {
            message: "list_request_events is not implemented for this storage backend".to_owned(),
        })
    }

    async fn request_event_key_last_used(
        &self,
        query: &RequestEventKeyLastUsedQuery,
    ) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
        let _ = query;
        Err(StorageError::Fatal {
            message: "request_event_key_last_used is not implemented for this storage backend"
                .to_owned(),
        })
    }

    async fn request_event_key_usage(
        &self,
        query: &RequestEventKeyUsageQuery,
    ) -> StorageResult<Vec<RequestEventKeyUsageBucket>> {
        let _ = query;
        Err(StorageError::Fatal {
            message: "request_event_key_usage is not implemented for this storage backend"
                .to_owned(),
        })
    }

    async fn request_event_principal_costs(
        &self,
        query: &RequestEventPrincipalCostQuery,
    ) -> StorageResult<Vec<RequestEventPrincipalCostBucket>> {
        let _ = query;
        Ok(Vec::new())
    }

    async fn request_event_histogram(
        &self,
        query: &RequestEventHistogramQuery,
    ) -> StorageResult<Vec<RequestEventHistogramBucket>> {
        let _ = query;
        Err(StorageError::Fatal {
            message: "request_event_histogram is not implemented for this storage backend"
                .to_owned(),
        })
    }

    /// Fetches the single full [`RequestEvent`] for the DETAIL view, byte-identical
    /// to what `append_request_event` persisted. Returns `Ok(None)` when no row
    /// with this `event_id` exists.
    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        let _ = event_id;
        Err(StorageError::Fatal {
            message: "get_request_event is not implemented for this storage backend".to_owned(),
        })
    }
}

#[async_trait]
pub trait CacheKeepaliveProjectionStore: Send + Sync {
    async fn append_cache_keepalive_decision(
        &self,
        decision: &CacheKeepaliveDecisionRow,
    ) -> StorageResult<()>;
}

#[async_trait]
pub trait UsageRollupStore: Send + Sync {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun>;

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>>;

    /// CATEGORY-3: `UsageRollup.upstream_id` is the stable cross-table identity for quota joins;
    /// `upstream_name` is best-effort display data captured from the upstream's current name and
    /// may drift across renames.
    async fn query_usage_rollups_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>>;

    /// Reads usage rollups for analysis while limiting the storage scan to the requested
    /// upstreams. Backends should override this to push the upstream predicate into SQL.
    async fn query_usage_rollups_for_upstreams_in_range(
        &self,
        upstream_ids: &[uuid::Uuid],
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rollups = self
            .query_usage_rollups_in_range(resolution, window_start_unix_secs, window_end_unix_secs)
            .await?;
        rollups.retain(|rollup| upstream_ids.contains(&rollup.upstream_id));
        Ok(rollups)
    }
    async fn query_overview_excluded_error_buckets_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<OverviewExcludedErrorBucket>> {
        let _ = (resolution, window_start_unix_secs, window_end_unix_secs);
        Ok(Vec::new())
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>>;

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        run: &UsageRollupRun,
    ) -> StorageResult<()>;
}

#[async_trait]
pub trait ApiKeyUsageBucketStore: Send + Sync {
    async fn register_api_key_usage_writer(
        &self,
        writer_epoch: uuid::Uuid,
        lease_until_unix_secs: u64,
    ) -> StorageResult<()>;

    async fn flush_api_key_usage(
        &self,
        flush: &crate::ApiKeyUsageFlush,
    ) -> StorageResult<crate::ApiKeyUsageFlushResult>;

    async fn query_api_key_usage_buckets(
        &self,
        query: &crate::ApiKeyUsageBucketQuery,
    ) -> StorageResult<Vec<crate::ApiKeyUsageBucket>>;

    async fn compact_api_key_usage_buckets(
        &self,
        writer_inactive_after_secs: u64,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> StorageResult<crate::ApiKeyUsageCompactionRun>;
}

#[async_trait]
pub trait UsageTokenIntervalStore: Send + Sync {
    /// Sums all token columns for each interval. Both boundaries are inclusive:
    /// `bucket_start >= start_unix_secs AND bucket_start <= end_unix_secs`.
    async fn sum_usage_tokens_for_intervals(
        &self,
        intervals: &[UsageTokenInterval],
    ) -> StorageResult<Vec<UsageTokenIntervalSum>>;
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

    async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
        let _ = key;
        Err(StorageError::Fatal {
            message: "get_meta_value is not implemented for this storage backend".to_owned(),
        })
    }

    async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
        let _ = (key, value);
        Err(StorageError::Fatal {
            message: "put_meta_value is not implemented for this storage backend".to_owned(),
        })
    }
}

#[async_trait]
pub trait PriceCatalogCache: Send + Sync {
    async fn put_price_snapshot(&self, json_bytes: &[u8], fetched_at_ms: u64) -> StorageResult<()>;

    async fn get_price_snapshot_if_changed(
        &self,
        current_hash: &str,
    ) -> StorageResult<PriceCatalogSnapshotFetch>;

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        match self.get_price_snapshot_if_changed("").await? {
            PriceCatalogSnapshotFetch::Missing => Ok(None),
            PriceCatalogSnapshotFetch::Changed(record) => Ok(Some(record)),
            PriceCatalogSnapshotFetch::Unchanged(_) => Err(StorageError::Corrupted {
                message: "price catalog returned unchanged for an empty compatibility hash"
                    .to_owned(),
            }),
        }
    }
}

#[async_trait]
pub trait Storage:
    AuditStore
    + crate::plugin_registry::PluginRegistryStore
    + crate::plan_tiers::PlanTierStore
    + crate::pool_quota_history::PoolQuotaHistoryStore
    + crate::principal::PrincipalStore
    + crate::upstream::UpstreamStore
    + RequestEventStore
    + CacheKeepaliveProjectionStore
    + UpstreamRateLimitStateStore
    + UpstreamSubscriptionQuotaStore
    + UpstreamSubscriptionQuotaAggregateStore
    + UpstreamSubscriptionMetadataStore
    + UpstreamWarmupAttemptStore
    + PromptCacheObservationStore
    + OrganizationMetadataStore
    + AnthropicCompatibilityKvStore
    + CacheKeepaliveSessionStore
    + CacheKeepaliveSessionReadStore
    + UsageRollupStore
    + ApiKeyUsageBucketStore
    + UsageTokenIntervalStore
    + OAuthCredentialStore
    + ApiKeyStore
    + PriceCatalogCache
    + ConfigStore
    + MetaStore
    + RuntimeChangeNotifier
    + crate::PluginBlobRepo
    + Send
    + Sync
    + 'static
{
}

impl<T> Storage for T where
    T: AuditStore
        + crate::plugin_registry::PluginRegistryStore
        + crate::plan_tiers::PlanTierStore
        + crate::pool_quota_history::PoolQuotaHistoryStore
        + crate::principal::PrincipalStore
        + crate::upstream::UpstreamStore
        + RequestEventStore
        + CacheKeepaliveProjectionStore
        + UpstreamRateLimitStateStore
        + UpstreamSubscriptionQuotaStore
        + UpstreamSubscriptionQuotaAggregateStore
        + UpstreamSubscriptionMetadataStore
        + UpstreamWarmupAttemptStore
        + PromptCacheObservationStore
        + OrganizationMetadataStore
        + AnthropicCompatibilityKvStore
        + CacheKeepaliveSessionStore
        + CacheKeepaliveSessionReadStore
        + UsageRollupStore
        + UsageTokenIntervalStore
        + ApiKeyUsageBucketStore
        + OAuthCredentialStore
        + ApiKeyStore
        + PriceCatalogCache
        + ConfigStore
        + MetaStore
        + RuntimeChangeNotifier
        + crate::PluginBlobRepo
        + Send
        + Sync
        + 'static
        + ConfigStore
        + MetaStore
        + RuntimeChangeNotifier
        + Send
        + Sync
        + 'static
{
}
