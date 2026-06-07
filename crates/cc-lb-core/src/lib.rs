#![forbid(unsafe_code)]

#[cfg(not(loom))]
pub mod anthropic_compat;
#[cfg(not(loom))]
pub mod anthropic_metadata;
pub mod api_keys;
pub mod audit_payload;
#[cfg(not(loom))]
pub mod audit_writer;
#[cfg(not(loom))]
mod bulkhead;
#[cfg(not(loom))]
mod circuit_breaker;
#[cfg(not(loom))]
pub mod clock;
#[cfg(not(loom))]
mod dashboard_broadcaster;
#[cfg(not(loom))]
mod dns_cache;
#[cfg(not(loom))]
mod drain;
#[cfg(not(loom))]
mod dynamic_view;
#[cfg(not(loom))]
mod error_format;
#[cfg(not(loom))]
mod error_normalizer;
#[cfg(not(loom))]
mod hop_by_hop;
#[cfg(not(loom))]
pub mod lifecycle;
#[cfg(not(loom))]
pub mod poll_schedule_estimator;
#[cfg(not(loom))]
#[allow(dead_code)]
mod rate_limit_headers;
#[cfg(not(loom))]
mod sse_error_frame;
#[cfg(not(loom))]
mod sse_relay;
#[cfg(not(loom))]
pub mod subscription_metadata_hook;
#[cfg(not(loom))]
pub mod subscription_quota_events;
#[cfg(not(loom))]
pub mod tokenizer;
#[cfg(not(loom))]
pub mod upstream_rate_limit_events;
#[cfg(not(loom))]
pub mod usage_pruner;
pub mod usage_rollup_job;
#[cfg(not(loom))]
pub use anthropic_metadata::make_metadata_http_client;
pub use audit_payload::AuditPayload;
#[cfg(not(loom))]
pub use audit_writer::{AuditDropped, AuditEntry, AuditWriterSink, spawn_audit_writer};
#[cfg(not(loom))]
pub use bulkhead::{
    Bulkhead, BulkheadConfig, BulkheadDispatch, BulkheadError, BulkheadRegistry, ExecuteError,
    make_default_dispatcher, make_http_dispatcher_with_connector,
};
#[cfg(not(loom))]
pub use cc_lb_plugin_api::ApiKeyAwareSignerFactory;
#[cfg(not(loom))]
pub use circuit_breaker::{
    BreakerConfig, BreakerError, BreakerRegistry, BreakerState, CircuitBreaker,
    CircuitBreakerConfig, CircuitBreakerDispatch, Permit,
};
#[cfg(not(loom))]
pub use clock::{Clock, ClockHandle, SystemClock, TestClock};
#[cfg(not(loom))]
pub use dashboard_broadcaster::{DashboardBroadcaster, record_dashboard_sse_lagged};
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
pub use dynamic_view::{
    ApplyStatus, DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamRateLimitCache,
    UpstreamStatusEntry, UpstreamStatusSnapshot,
};
#[cfg(not(loom))]
pub use error_format::{anthropic_error_body, anthropic_error_response};
#[cfg(not(loom))]
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
#[cfg(not(loom))]
pub use hop_by_hop::{HopByHopStripLayer, HopByHopStripService, strip_hop_by_hop};
#[cfg(not(loom))]
pub use lifecycle::{
    Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig, LimitSubject,
    LimitSubjectProvider, NoopSubscriptionQuotaCache, ProxyError, ReplicaIdentity, RequestKind,
    SubscriptionQuotaCacheLike, UpstreamDispatch, build_candidates, observe_rate_limits,
    observe_subscription_quota_headers,
};
#[cfg(not(loom))]
pub use poll_schedule_estimator::{EstimatorConfig, PollScheduleEstimator, ThrottleObservation};
#[cfg(not(loom))]
pub use rate_limit_headers::{
    UnifiedQuotaObservation, clamp_utilization_fraction, parse_anthropic_unified_headers,
    percent_to_utilization_fraction,
};
#[cfg(not(loom))]
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
#[cfg(not(loom))]
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay, StreamingUsage};
#[cfg(not(loom))]
pub use subscription_metadata_hook::{
    MetadataHookHandle, MetadataHookRequest, MetadataRefreshError, MetadataRefreshRecords,
    fetch_metadata_only, run_metadata_refresh, start_subscription_metadata_hook,
};
#[cfg(not(loom))]
pub use subscription_quota_events::{
    SubscriptionQuotaEnqueueError, SubscriptionQuotaSink, SubscriptionQuotaWriterConfig,
    start_subscription_quota_writer,
};
#[cfg(not(loom))]
pub use upstream_rate_limit_events::{
    UpstreamRateLimitEnqueueError, UpstreamRateLimitSink, start_upstream_rate_limit_writer,
};
