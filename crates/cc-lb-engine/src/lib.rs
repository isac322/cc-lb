#![forbid(unsafe_code)]
//! cc-lb data-plane engine for request routing, dispatch, lifecycle production, and runtime subscribers.

#[cfg(not(loom))]
pub mod anthropic_compat {
    pub use cc_lb_control::anthropic_compat::*;
}
#[cfg(not(loom))]
pub mod anthropic_metadata {
    pub use cc_lb_control::anthropic_metadata::*;
}
pub mod api_keys {
    pub use cc_lb_control::api_keys::*;
}
#[cfg(not(loom))]
pub mod builtin_filters;
#[cfg(not(loom))]
mod bulkhead;
#[cfg(not(loom))]
#[cfg(not(loom))]
mod circuit_breaker;
#[cfg(not(loom))]
pub mod clock;
#[cfg(not(loom))]
mod dns_cache;
#[cfg(not(loom))]
mod drain;
#[cfg(not(loom))]
mod error_format;
#[cfg(not(loom))]
mod error_normalizer;
#[cfg(not(loom))]
pub mod event_bus {
    pub use cc_lb_contract::event_bus::{
        BusReceiver, LifecycleBusReceiver, RequestEventBus, RequestEventPhase, RequestEventUpdate,
    };
    pub use cc_lb_control::event_bus::*;
}
#[cfg(not(loom))]
mod hop_by_hop;
#[cfg(not(loom))]
pub mod instrumented_connector;
#[cfg(not(loom))]
pub mod lifecycle;
pub mod lifecycle_api_key_metrics_subscriber;
pub mod lifecycle_cache_hit_miss_subscriber;
#[cfg(not(loom))]
pub mod lifecycle_cache_observation_subscriber;
pub mod lifecycle_event_assembler;
pub mod lifecycle_event_logger;
pub mod lifecycle_hook_adapter;
pub mod lifecycle_limit_reconcile_subscriber;
pub mod lifecycle_limit_rejection_audit_subscriber;
pub mod lifecycle_prompt_cache_drift_subscriber;
pub mod lifecycle_prompt_cache_observation_subscriber;
#[cfg(not(loom))]
pub mod lifecycle_rate_limit_header_subscriber;
pub mod lifecycle_routing_tier_subscriber;
#[cfg(not(loom))]
pub mod lifecycle_subscription_quota_subscriber;
#[cfg(not(loom))]
pub mod limit_state_writer;
pub mod metrics_labels;
#[cfg(not(loom))]
pub mod model_resolution;
#[cfg(not(loom))]
pub mod pg_notify_fanout;
pub mod plan_capacity;
#[cfg(not(loom))]
pub mod prompt_cache_simulator;
#[allow(dead_code)]
mod rate_limit_headers;
#[cfg(not(loom))]
pub mod request_timing;
#[cfg(not(loom))]
pub(crate) mod response_transform;
#[cfg(not(loom))]
mod sse_error_frame;
#[cfg(not(loom))]
mod sse_relay;
#[cfg(not(loom))]
pub mod storage_tail_poller;
#[cfg(not(loom))]
pub mod subscription_metadata_hook {
    pub use cc_lb_control::subscription_metadata_hook::*;
}
#[cfg(not(loom))]
pub mod subscription_quota_events;
#[cfg(not(loom))]
mod terminal_observer;
#[cfg(not(loom))]
pub mod tokenizer;
#[cfg(not(loom))]
pub mod upstream_rate_limit_events;
#[cfg(not(loom))]
pub mod usage_decoder;
#[cfg(not(loom))]
mod usage_parser;
#[cfg(not(loom))]
pub mod usage_pruner;
#[cfg(not(loom))]
pub mod warmup_attempts;
#[cfg(not(loom))]
pub use anthropic_metadata::make_metadata_http_client;
#[cfg(not(loom))]
pub use bulkhead::{
    Bulkhead, BulkheadDispatch, BulkheadError, BulkheadRegistry, BulkheadRuntimeConfig,
    ExecuteError, make_default_dispatcher, make_http_dispatcher_with_connector,
};
#[cfg(not(loom))]
pub use cc_lb_contract::ReplicaIdentity;
pub use cc_lb_control::audit_payload::AuditPayload;
#[cfg(not(loom))]
pub use cc_lb_control::audit_writer::{
    AuditDropped, AuditEntry, AuditWriterSink, spawn_audit_writer,
};
#[cfg(not(loom))]
pub use cc_lb_control::dynamic_view::{
    ApplyStatus, DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamRateLimitCache,
    UpstreamStatusEntry, UpstreamStatusSnapshot,
};
#[cfg(not(loom))]
pub use cc_lb_control::{
    NoopSubscriptionQuotaCache, PromptCacheObservationCacheLike,
    PromptCacheObservationEnqueueError, PromptCacheObservationSinkLike, SubscriptionQuotaCacheLike,
};
#[cfg(not(loom))]
pub use cc_lb_plugin_api::ApiKeyAwareSignerFactory;
#[cfg(not(loom))]
pub use circuit_breaker::{
    BreakerError, BreakerRegistry, BreakerRuntimeConfig, BreakerState, CircuitBreaker,
    CircuitBreakerDispatch, Permit,
};
#[cfg(not(loom))]
pub use clock::{Clock, ClockHandle, SystemClock, TestClock, unix_millis, unix_secs};
#[doc(hidden)]
#[cfg(not(loom))]
pub use dns_cache::make_resolver_with_factory;
#[cfg(not(loom))]
pub use dns_cache::{
    CachingDnsConnector, DnsCacheError, DnsResolveFuture, DnsResolver, DnsResolverConfig,
    make_resolver,
};
#[cfg(not(loom))]
pub use drain::{DrainController, proxy_drain_middleware};
#[cfg(not(loom))]
pub use error_format::{anthropic_error_body, anthropic_error_response};
#[cfg(not(loom))]
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
#[cfg(not(loom))]
pub use event_bus::{
    BusError, DEFAULT_BROADCAST_CAPACITY, DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY,
    DEFAULT_LIFECYCLE_HOOK_ADAPTER_CAPACITY, DEFAULT_LIFECYCLE_PRICING_CAPACITY,
    DEFAULT_LIFECYCLE_PROMPT_CACHE_OBSERVATION_CAPACITY, DEFAULT_LIFECYCLE_WRITER_CAPACITY,
    EventFanout, InMemoryBus, InMemoryFanout, new_in_memory_bus, record_dashboard_sse_lagged,
};
#[cfg(not(loom))]
pub use hop_by_hop::{HopByHopStripLayer, HopByHopStripService, strip_hop_by_hop};
#[cfg(not(loom))]
pub use lifecycle::{
    AuthLimitSubject, Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig,
    LimitCostEstimator, LimitSubjectProvider, ProxyError, RequestKind, UpstreamDispatch,
    build_candidates, build_subscription_quota_samples, observe_rate_limits,
    parse_request_cache_breakpoints,
};
pub use lifecycle_api_key_metrics_subscriber::{
    ApiKeyMetricsSubscriberHandle, spawn_lifecycle_api_key_metrics_subscriber,
};
pub use lifecycle_cache_hit_miss_subscriber::{
    CacheHitMissSubscriberHandle, spawn_lifecycle_cache_hit_miss_subscriber,
};
#[cfg(not(loom))]
pub use lifecycle_cache_observation_subscriber::{
    CacheObservationSubscriberHandle, spawn_lifecycle_cache_observation_subscriber,
};
pub use lifecycle_event_assembler::{RequestEventAssemblerHandle, spawn_request_event_assembler};
pub use lifecycle_event_logger::{LifecycleEventLoggerHandle, spawn_lifecycle_event_logger};
pub use lifecycle_hook_adapter::{
    ObservabilityHookAdapterHandle, spawn_observability_hook_adapter,
};
pub use lifecycle_limit_reconcile_subscriber::{
    LimitReconcileSubscriberHandle, spawn_lifecycle_limit_reconcile_subscriber,
};
pub use lifecycle_limit_rejection_audit_subscriber::{
    LimitRejectionAuditSubscriberHandle, spawn_lifecycle_limit_rejection_audit_subscriber,
};
pub use lifecycle_prompt_cache_drift_subscriber::{
    PromptCacheDriftSubscriberHandle, spawn_lifecycle_prompt_cache_drift_subscriber,
};
pub use lifecycle_prompt_cache_observation_subscriber::{
    PromptCacheObservationSubscriberHandle, spawn_lifecycle_prompt_cache_observation_subscriber,
};
#[cfg(not(loom))]
pub use lifecycle_rate_limit_header_subscriber::{
    RateLimitHeaderSubscriberHandle, spawn_lifecycle_rate_limit_header_subscriber,
};
pub use lifecycle_routing_tier_subscriber::{
    RoutingTierSubscriberHandle, spawn_lifecycle_routing_tier_subscriber,
};
#[cfg(not(loom))]
pub use lifecycle_subscription_quota_subscriber::{
    SubscriptionQuotaSubscriberHandle, spawn_lifecycle_subscription_quota_subscriber,
};
#[cfg(not(loom))]
pub use limit_state_writer::{
    PrincipalLimitStateEnqueueError, PrincipalLimitStateSink, start_principal_limit_state_writer,
};
pub use metrics_labels::{
    NotifyDropReason, NotifyHttpOutcome, NotifySentOutcome, PartialTrigger,
    PgListenerReconnectReason, ResetReason,
};
#[cfg(not(loom))]
pub use pg_notify_fanout::{
    DEFAULT_PG_NOTIFY_CHANNEL, PARTIAL_NOTIFY_MPSC_CAPACITY, PartialRetentionCache, PgListener,
    PgNotifier, PgNotifyFanout,
};
pub use rate_limit_headers::{
    UnifiedQuotaObservation, clamp_utilization_fraction, parse_anthropic_unified_headers,
    percent_to_utilization_fraction,
};
#[cfg(not(loom))]
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
#[cfg(not(loom))]
pub use sse_relay::{
    PromptCacheObservationEventEmitter, RelayError, SseBatchConfig, SseRelay, StreamingUsage,
};
#[cfg(not(loom))]
pub use storage_tail_poller::{StorageTailPoller, StorageTailUpdate};
#[cfg(not(loom))]
pub use subscription_metadata_hook::{
    MetadataHookEnqueueError, MetadataHookHandle, MetadataHookRequest, MetadataRefreshEnqueue,
    MetadataRefreshError, MetadataRefreshRecords, fetch_metadata_only, run_metadata_refresh,
    start_subscription_metadata_hook,
};
#[cfg(not(loom))]
pub use subscription_quota_events::{
    SubscriptionQuotaEnqueueError, SubscriptionQuotaSink, SubscriptionQuotaWriterConfig,
    start_subscription_quota_writer, unified_observation_to_sample,
};
#[cfg(not(loom))]
pub use terminal_observer::LifecycleContext;
#[cfg(not(loom))]
pub use upstream_rate_limit_events::{
    UpstreamRateLimitEnqueueError, UpstreamRateLimitSink, start_upstream_rate_limit_writer,
};
