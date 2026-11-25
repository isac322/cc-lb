//! Control-plane primitives for admin APIs, dynamic views, audit output, and metadata refresh.
//!
//! This crate owns operator-facing services and subscribers that coordinate
//! stored configuration, API keys, audit records, and runtime status without
//! depending on the data-plane engine crate.

pub mod anthropic_api_key;
pub mod anthropic_compat;
pub mod anthropic_metadata;
pub mod api_keys;
pub mod audit_payload;
pub mod audit_writer;
pub mod dynamic_view;
pub mod event_bus;
pub mod gemini_period_unknown_quota;
pub mod openai_limit_resets;
pub mod openai_org_api;
pub mod post_commit;
pub mod split_admin_bridge;
pub mod subscription_metadata_hook;
pub mod subscription_quota_snapshots;
pub mod subscription_quotas;
pub mod traits;
pub mod upstream_validation;
pub mod warmup_ports;

pub use audit_payload::AuditPayload;
pub use audit_writer::{AuditDropped, AuditEntry, AuditWriterSink, spawn_audit_writer};
pub use dynamic_view::{
    ApplyStatus, DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamRateLimitCache,
    UpstreamReadinessSummary, UpstreamStatusEntry, UpstreamStatusSnapshot,
};
pub use event_bus::{
    DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, InMemoryBus, LifecycleDispatcherHandle, RequestEventBus,
    new_in_memory_bus,
};
pub use openai_org_api::{
    OpenAiBucketWidth, OpenAiCostAmount, OpenAiCostResult, OpenAiCostsBucket, OpenAiCostsPage,
    OpenAiCostsQuery, OpenAiOrgApi, OpenAiOrgApiError, OpenAiOrgLimits, OpenAiTextUsageBucket,
    OpenAiTextUsagePage, OpenAiTextUsageResult, OpenAiUsageQuery,
};
pub use post_commit::{PostCommitPermit, PostCommitTracker};
pub use split_admin_bridge::{
    ManualWarmupBridgeResult, ManualWarmupJobResult, OAuthUsageRefreshJobResult,
    SPLIT_BRIDGE_DEFAULT_TIMEOUT, SPLIT_BRIDGE_POLL_INTERVAL, SPLIT_BRIDGE_RESULT_TTL_SECS,
    SPLIT_MANUAL_WARMUP_RESULT_PREFIX, SPLIT_USAGE_REFRESH_RESULT_PREFIX,
    load_manual_warmup_result, load_oauth_usage_refresh_result, manual_warmup_idempotency_key,
    manual_warmup_result_meta_key, oauth_refresh_bridge_idempotency_key,
    oauth_usage_refresh_idempotency_key, oauth_usage_refresh_result_meta_key,
    prune_expired_split_bridge_results, record_manual_warmup_expired_before_start,
    record_manual_warmup_result, record_oauth_usage_refresh_result, take_manual_warmup_result,
    take_oauth_usage_refresh_result,
};
pub use subscription_metadata_hook::{
    MetadataHookEnqueueError, MetadataHookHandle, MetadataHookRequest, MetadataRefreshEnqueue,
    MetadataRefreshError, MetadataRefreshRecords, fetch_metadata_only, run_metadata_refresh,
    start_subscription_metadata_hook,
};
pub use traits::{
    NoopPromptCacheObservationSink, NoopSubscriptionQuotaCache, PromptCacheObservationEnqueueError,
    PromptCacheObservationSinkLike, SubscriptionQuotaCacheLike,
};
pub use upstream_validation::validate_upstream;
pub use warmup_ports::{
    LimitResetUsageRefresh, LimitResetUsageRefreshPort, WarmupWindowCheck, WarmupWindowVerifier,
};
