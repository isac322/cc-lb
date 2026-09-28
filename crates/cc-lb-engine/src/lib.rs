#![forbid(unsafe_code)]
//! cc-lb data-plane engine for request routing, dispatch, lifecycle production, and runtime subscribers.

#[cfg(not(loom))]
pub mod attempt_rail;
#[cfg(not(loom))]
pub mod authn_rail;
#[cfg(not(loom))]
mod body_io_timing;
#[cfg(not(loom))]
pub mod builtin_filters;
#[cfg(not(loom))]
mod bulkhead;
#[cfg(not(loom))]
pub mod cache_keepalive;
#[cfg(not(loom))]
mod circuit_breaker;
#[cfg(not(loom))]
mod completion_observer;
#[cfg(not(loom))]
mod dns_cache;
#[cfg(not(loom))]
mod downstream_stream_drop_guard;
#[cfg(not(loom))]
mod drain;
#[cfg(not(loom))]
mod error_format;
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
pub mod lifecycle_limit_reconcile_subscriber;
pub mod lifecycle_limit_rejection_audit_subscriber;
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
#[cfg(not(loom))]
pub mod prompt_cache_simulator;
#[cfg(not(loom))]
mod request_classification;
pub mod request_context;
#[cfg(not(loom))]
pub mod request_timing;
#[cfg(not(loom))]
pub(crate) mod response_transform;
#[cfg(not(loom))]
mod sse_error_frame;
#[cfg(not(loom))]
pub mod storage_tail_poller;
#[cfg(not(loom))]
pub mod subscription_quota_events;
#[cfg(not(loom))]
mod terminal_observer;
#[cfg(not(loom))]
pub mod tokenizer;
#[cfg(not(loom))]
pub(crate) mod upstream_affinity;
#[cfg(not(loom))]
pub mod upstream_rate_limit_events;
#[cfg(not(loom))]
pub mod usage_decoder;
#[cfg(not(loom))]
mod usage_parser;
#[cfg(not(loom))]
pub mod warmup_attempts;
#[cfg(not(loom))]
pub use authn_rail::{Authenticated, authenticate_first, reject_unauthenticated};
#[cfg(not(loom))]
pub use bulkhead::{
    Bulkhead, BulkheadDispatch, BulkheadError, BulkheadRegistry, BulkheadRuntimeConfig,
    ExecuteError, make_default_dispatcher,
};
#[cfg(not(loom))]
pub use cc_lb_upstream::ApiKeyAwareSignerFactory;
#[cfg(not(loom))]
pub use circuit_breaker::{
    BreakerError, BreakerRegistry, BreakerRuntimeConfig, BreakerState, CircuitBreaker,
    CircuitBreakerDispatch, Permit,
};
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
pub use hop_by_hop::{HopByHopStripLayer, HopByHopStripService, strip_hop_by_hop};
#[cfg(not(loom))]
pub use lifecycle::{
    AuthLimitSubject, Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig,
    LimitCostEstimator, LimitSubjectProvider, ProxyError, RequestKind, UpstreamDispatch,
    build_candidates, observe_rate_limits, parse_request_cache_breakpoints,
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
pub use lifecycle_limit_reconcile_subscriber::{
    LimitReconcileSubscriberHandle, spawn_lifecycle_limit_reconcile_subscriber,
};
pub use lifecycle_limit_rejection_audit_subscriber::{
    LimitRejectionAuditSubscriberHandle, spawn_lifecycle_limit_rejection_audit_subscriber,
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
#[cfg(not(loom))]
pub use storage_tail_poller::StorageTailPoller;
#[cfg(not(loom))]
pub use subscription_quota_events::{
    SubscriptionQuotaEnqueueError, SubscriptionQuotaSink, SubscriptionQuotaWriterConfig,
    start_subscription_quota_writer,
};
#[cfg(not(loom))]
pub use terminal_observer::{
    InternalFailure, LifecycleContext, TerminalClassification, UpstreamErrorCode,
};
#[cfg(not(loom))]
pub use upstream_rate_limit_events::{
    UpstreamRateLimitEnqueueError, UpstreamRateLimitSink, start_upstream_rate_limit_writer,
};
