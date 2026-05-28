#![forbid(unsafe_code)]

pub mod api_keys;
pub mod audit_writer;
mod bulkhead;
mod circuit_breaker;
mod clock;
mod dashboard_broadcaster;
mod dns_cache;
mod drain;
mod error_format;
mod error_normalizer;
mod hop_by_hop;
mod lifecycle;
mod sse_error_frame;
mod sse_relay;
pub mod usage_pruner;

pub use audit_writer::{AuditDropped, AuditEntry, AuditWriterSink, spawn_audit_writer};
pub use bulkhead::{
    Bulkhead, BulkheadConfig, BulkheadDispatch, BulkheadError, BulkheadRegistry, ExecuteError,
    make_default_dispatcher, make_http_dispatcher_with_connector,
};
pub use circuit_breaker::{
    BreakerConfig, BreakerError, BreakerRegistry, BreakerState, CircuitBreaker,
    CircuitBreakerConfig, CircuitBreakerDispatch, Permit,
};
pub use clock::{Clock, MockClock, SystemClock};
pub use dashboard_broadcaster::{DashboardBroadcaster, record_dashboard_sse_lagged};
#[doc(hidden)]
pub use dns_cache::make_resolver_with_factory;
pub use dns_cache::{
    CachingDnsConnector, DnsCacheError, DnsResolveFuture, DnsResolver, DnsResolverConfig,
    make_resolver,
};
pub use drain::{DrainController, proxy_drain_middleware};
pub use error_format::{anthropic_error_body, anthropic_error_response};
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
pub use hop_by_hop::{HopByHopStripLayer, HopByHopStripService, strip_hop_by_hop};
pub use lifecycle::{
    ApiKeyAwareSignerFactory, Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig,
    LimitSubject, LimitSubjectProvider, ProxyError, UpstreamDispatch,
};
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay, StreamingUsage};
