use async_trait::async_trait;

use crate::{
    ApiKeyMutation, AuditEntry, AuditQueryScope, BackendKind, CacheKeepaliveDecisionRow,
    ConfigDraftState, HistoryEntry, IssueParams, OverviewExcludedErrorBucket,
    PriceCatalogSnapshotFetch, RequestEvent, RequestEventExportPage, RequestEventHistogramBucket,
    RequestEventHistogramQuery, RequestEventKeyLastUsed, RequestEventKeyLastUsedQuery,
    RequestEventListItem, RequestEventListQuery, RequestEventPrincipalCostBucket,
    RequestEventPrincipalCostQuery, RequestEventProjections, RequestEventStreamFilters,
    RuntimeChangeNotifier, StorageError, StorageResult, StoredApiKeyRecord, UsageRollup,
    UsageRollupResolution, UsageRollupRun, UsageTokenInterval, UsageTokenIntervalSum,
    anthropic_compatibility_kv::AnthropicCompatibilityKvStore,
    cache_keepalive_sessions::{CacheKeepaliveSessionReadStore, CacheKeepaliveSessionStore},
    oauth_pkce::OAuthPkceStore,
    organization_metadata::OrganizationMetadataStore,
    prompt_cache_observation::PromptCacheObservationStore,
    upstream_affinity::UpstreamAffinityStore,
    upstream_rate_limit::UpstreamRateLimitStateStore,
    upstream_subscription_metadata::UpstreamSubscriptionMetadataStore,
    upstream_subscription_quota::{
        UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore,
    },
    warmup_attempts::UpstreamWarmupAttemptStore,
};

#[async_trait]
pub trait AuditStore: Send + Sync {
    async fn append_audit(&self, entry: &AuditEntry) -> StorageResult<()>;

    async fn append_audit_entries(&self, entries: &[AuditEntry]) -> StorageResult<()> {
        for entry in entries {
            self.append_audit(entry).await?;
        }
        Ok(())
    }

    /// Returns matching entries newest first, with newer insertions first on timestamp ties.
    /// When `admin_only` is true, only entries with an admin action are matched,
    /// and that filter applies before `limit`.
    async fn query_recent_audit(
        &self,
        scope: AuditQueryScope<'_>,
        since: u64,
        until: u64,
        limit: usize,
        admin_only: bool,
    ) -> StorageResult<Vec<AuditEntry>>;

    async fn query_audit_by_actor(
        &self,
        authority: &str,
        subject: &str,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<AuditEntry>>;

    async fn prune_audit_before(
        &self,
        cutoff_ts_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64>;
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
    /// `query.filters` are evaluated in SQL, and only the relational columns
    /// displayed in the list view are selected and decoded.
    /// See [`crate::RequestEventListQuery`] for the ordering/cursor contract.
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

    /// Fetches the persisted relational fields for the DETAIL view.
    /// In-memory diagnostics are returned empty. Returns `Ok(None)` when no
    /// row with this `event_id` exists.
    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        let _ = event_id;
        Err(StorageError::Fatal {
            message: "get_request_event is not implemented for this storage backend".to_owned(),
        })
    }

    /// Returns the highest cursor `C` such that no row with `seq <= C` can
    /// still commit: every such row is already visible (or never will be).
    /// Paging `export_request_events` with `until_seq` clamped to `C` and
    /// resuming from the last returned `seq` therefore never skips a lower
    /// `seq` committed by a slower transaction. Postgres waits at most
    /// `max_wait` for in-flight transactions and fails with `Unavailable` on
    /// timeout; single-writer backends return the current maximum.
    async fn request_event_stable_seq_ceiling(
        &self,
        max_wait: std::time::Duration,
    ) -> StorageResult<u64> {
        let _ = max_wait;
        Err(StorageError::Fatal {
            message: "request_event_stable_seq_ceiling is not implemented for this storage backend"
                .to_owned(),
        })
    }

    /// Returns `(MIN(seq), MAX(seq))` over rows whose stored list timestamp
    /// lies in the half-open window `[from_ts_ms, to_ts_ms)`, or `None` when
    /// no row matches. Operator export only.
    async fn request_event_seq_bounds(
        &self,
        from_ts_ms: u64,
        to_ts_ms: u64,
    ) -> StorageResult<Option<(u64, u64)>> {
        let _ = (from_ts_ms, to_ts_ms);
        Err(StorageError::Fatal {
            message: "request_event_seq_bounds is not implemented for this storage backend"
                .to_owned(),
        })
    }

    /// Returns one page of full events in ascending cursor order (see
    /// [`RequestEventExportPage`]). Postgres returns only rows already below
    /// the snapshot xmin horizon, like `query_request_events_between_cursors`.
    async fn export_request_events(
        &self,
        page: &RequestEventExportPage,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        let _ = page;
        Err(StorageError::Fatal {
            message: "export_request_events is not implemented for this storage backend".to_owned(),
        })
    }

    /// Returns `(utc_day_index, row_count)` pairs, ascending by day, where
    /// `utc_day_index = list_ts_ms / 86_400_000`, over rows with
    /// `from_ts_ms <= list_ts_ms < to_ts_ms`. Reads no payload.
    async fn request_event_day_counts(
        &self,
        from_ts_ms: u64,
        to_ts_ms: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        let _ = (from_ts_ms, to_ts_ms);
        Err(StorageError::Fatal {
            message: "request_event_day_counts is not implemented for this storage backend"
                .to_owned(),
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

    /// Removes at most `batch_size` expired rows across usage and excluded-error
    /// rollups. Minute and hour resolutions have independent retention horizons.
    async fn prune_usage_rollups_before(
        &self,
        minute_cutoff_unix_secs: u64,
        hour_cutoff_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let _ = (minute_cutoff_unix_secs, hour_cutoff_unix_secs, batch_size);
        Ok(0)
    }

    /// CATEGORY-3: `UsageRollup.upstream_id` is the stable cross-table identity for quota joins;
    /// `upstream_name` is best-effort display data captured from the upstream's current name and
    /// may drift across renames.
    async fn query_usage_rollups_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>>;

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

/// Cluster-wide concurrent-request hold store (issue 807).
///
/// Postgres implements this against `api_key_concurrency_holds_v1` so the
/// `Concurrent` limit is enforced across replicas. The SQLite adapter is a
/// no-op: single-instance deployments keep the local in-process counter.
#[async_trait]
pub trait ApiKeyConcurrencyHoldStore: Send + Sync {
    async fn insert_api_key_concurrency_hold(
        &self,
        hold: &crate::ApiKeyConcurrencyHold,
    ) -> StorageResult<()>;

    async fn delete_api_key_concurrency_hold(&self, hold_id: uuid::Uuid) -> StorageResult<()>;

    /// Counts live holds for `key_id`, excluding rows older than
    /// `max_age_secs` relative to `now_unix_secs` (crash leftovers).
    async fn count_api_key_concurrency_holds(
        &self,
        key_id: &str,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> StorageResult<u64>;

    /// Deletes every hold older than `max_age_secs` relative to
    /// `now_unix_secs`. Returns the number of rows removed.
    async fn delete_expired_api_key_concurrency_holds(
        &self,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> StorageResult<u64>;
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

    async fn set_config_validation(
        &self,
        revision: u64,
        valid: bool,
        validation: serde_json::Value,
    ) -> StorageResult<()>;

    async fn append_config_history(
        &self,
        revision: u64,
        applied_at_unix_secs: u64,
    ) -> StorageResult<()>;

    async fn list_config_history(&self, limit: usize) -> StorageResult<Vec<HistoryEntry>>;
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn initialize(&self) -> StorageResult<()>;

    async fn backend_kind(&self) -> StorageResult<BackendKind>;

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

    /// Atomically writes `value` only if the key currently holds `expected`
    /// (`None` = the key must be absent). Returns `true` when the write
    /// landed and `false` when the current value differed; exactly one of
    /// several concurrent callers expecting the same value wins.
    async fn compare_and_put_meta_value(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &str,
    ) -> StorageResult<bool> {
        let _ = (key, expected, value);
        Err(StorageError::Fatal {
            message: "compare_and_put_meta_value is not implemented for this storage backend"
                .to_owned(),
        })
    }

    /// Verifies, without migrating, that every migration embedded in this
    /// binary is recorded as successfully applied with the same checksum.
    /// A missing version or a checksum mismatch returns `Fatal` listing them;
    /// versions applied by a newer binary are allowed.
    async fn verify_schema_current(&self) -> StorageResult<()> {
        Err(StorageError::Fatal {
            message: "verify_schema_current is not implemented for this storage backend".to_owned(),
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
}

#[async_trait]
pub trait Storage:
    AuditStore
    + crate::plugin_registry::PluginRegistryStore
    + crate::plan_tiers::PlanTierStore
    + crate::pool_quota_history::PoolQuotaHistoryStore
    + crate::principal::PrincipalStore
    + crate::upstream::UpstreamStore
    + crate::upstream_api_key::UpstreamApiKeyStore
    + UpstreamAffinityStore
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
    + ApiKeyConcurrencyHoldStore
    + UsageTokenIntervalStore
    + PriceCatalogCache
    + ConfigStore
    + MetaStore
    + RuntimeChangeNotifier
    + OAuthPkceStore
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
        + crate::upstream_api_key::UpstreamApiKeyStore
        + UpstreamAffinityStore
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
        + ApiKeyConcurrencyHoldStore
        + PriceCatalogCache
        + ConfigStore
        + MetaStore
        + RuntimeChangeNotifier
        + OAuthPkceStore
        + Send
        + Sync
        + 'static
{
}
