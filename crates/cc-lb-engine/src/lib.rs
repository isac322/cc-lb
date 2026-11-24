#![forbid(unsafe_code)]
//! cc-lb data-plane engine for request routing, dispatch, lifecycle production, and runtime subscribers.

#[cfg(not(loom))]
pub mod attempt_rail;
#[cfg(not(loom))]
pub mod authn_rail;
#[cfg(not(loom))]
mod body_io_timing;
#[cfg(not(loom))]
pub use cc_lb_routing_eval::builtin_filters;
#[cfg(not(loom))]
mod bulkhead;
#[cfg(not(loom))]
pub mod cache_keepalive;
#[cfg(not(loom))]
mod circuit_breaker;
#[cfg(not(loom))]
pub mod codex_quota_observation;
#[cfg(not(loom))]
pub use cc_lb_proxy_telemetry::completion_observer;
#[cfg(not(loom))]
mod dns_cache;
#[cfg(not(loom))]
mod downstream_stream_drop_guard;
#[cfg(not(loom))]
mod drain;
#[cfg(not(loom))]
mod error_format;
#[cfg(not(loom))]
mod gemini_oauth_catalog;
#[cfg(not(loom))]
mod hop_by_hop;
#[cfg(not(loom))]
pub use cc_lb_proxy_telemetry::instrumented_connector;
#[cfg(not(loom))]
pub mod lifecycle;
#[cfg(not(loom))]
mod limit_input_estimate;
#[cfg(not(loom))]
pub use cc_lb_routing_eval::model_resolution;
#[cfg(not(loom))]
pub mod openai_compat;
#[cfg(not(loom))]
pub use cc_lb_routing_eval::prompt_cache_simulator;
#[cfg(not(loom))]
pub mod provider_cache;
pub(crate) mod provider_capability;
#[cfg(not(loom))]
mod provider_image_tokens;
#[cfg(not(loom))]
mod provider_input_estimate;
#[cfg(not(loom))]
pub use cc_lb_routing_eval::request_classification;
pub mod request_context;
#[cfg(not(loom))]
pub use request_context::{CodexBackendSurface, ProviderScope, RequestContext};
#[cfg(not(loom))]
pub mod realtime_websocket;
#[cfg(not(loom))]
pub use cc_lb_proxy_telemetry::request_timing;
#[cfg(not(loom))]
pub(crate) mod response_transform;
#[cfg(not(loom))]
pub mod responses_websocket;
#[cfg(not(loom))]
mod sse_error_frame;
#[cfg(not(loom))]
pub mod subscription_quota_events;
#[cfg(not(loom))]
pub mod terminal_observer;
#[cfg(not(loom))]
pub use cc_lb_routing_eval::tokenizer;
#[cfg(not(loom))]
pub(crate) mod upstream_affinity;
#[cfg(not(loom))]
pub mod usage_decoder;
#[cfg(not(loom))]
mod usage_parser;
#[cfg(not(loom))]
pub use authn_rail::{
    Authenticated, authenticate_first, reject_unauthenticated_for_provider,
    reject_unauthenticated_for_route,
};
#[cfg(not(loom))]
pub use bulkhead::{
    Bulkhead, BulkheadDispatch, BulkheadError, BulkheadRegistry, BulkheadRuntimeConfig,
    ExecuteError, OwnedBulkheadGuard, make_default_dispatcher,
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
pub use error_format::{
    anthropic_error_body, anthropic_error_response, provider_scope_error_response,
};
#[cfg(not(loom))]
pub use hop_by_hop::{HopByHopStripLayer, HopByHopStripService, strip_hop_by_hop};
#[cfg(not(loom))]
pub use lifecycle::{
    AuthLimitSubject, Body, DispatchError, Lifecycle, LifecycleConfig, LimitCostEstimator,
    LimitSubjectProvider, ProxyError, RequestKind, UpstreamDispatch, build_candidates,
    live_eligible, observe_rate_limits,
};
#[cfg(not(loom))]
pub use provider_capability::is_unscoped_gemini_http_route;
#[cfg(not(loom))]
pub use subscription_quota_events::{SubscriptionQuotaEnqueueError, SubscriptionQuotaSink};
#[cfg(not(loom))]
pub use terminal_observer::{
    InternalFailure, LifecycleContext, TerminalClassification, UpstreamErrorCode,
};
