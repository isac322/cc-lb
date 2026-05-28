#![forbid(unsafe_code)]

pub mod api_keys;
#[cfg(not(loom))]
pub mod audit_writer;
#[cfg(not(loom))]
mod bulkhead;
#[cfg(not(loom))]
mod circuit_breaker;
#[cfg(not(loom))]
mod clock;
#[cfg(not(loom))]
mod dashboard_broadcaster;
#[cfg(not(loom))]
mod dns_cache;
#[cfg(not(loom))]
mod drain;
#[cfg(not(loom))]
mod error_format;
#[cfg(not(loom))]
mod error_normalizer;
#[cfg(not(loom))]
mod hop_by_hop;
#[cfg(not(loom))]
mod lifecycle;
#[cfg(not(loom))]
mod sse_error_frame;
#[cfg(not(loom))]
mod sse_relay;
#[cfg(not(loom))]
pub mod usage_pruner;
#[cfg(not(loom))]
pub use audit_writer::{AuditDropped, AuditEntry, AuditWriterSink, spawn_audit_writer};
#[cfg(not(loom))]
pub use bulkhead::{
    Bulkhead, BulkheadConfig, BulkheadDispatch, BulkheadError, BulkheadRegistry, ExecuteError,
    make_default_dispatcher, make_http_dispatcher_with_connector,
};
#[cfg(not(loom))]
pub use circuit_breaker::{
    BreakerConfig, BreakerError, BreakerRegistry, BreakerState, CircuitBreaker,
    CircuitBreakerConfig, CircuitBreakerDispatch, Permit,
};
#[cfg(not(loom))]
pub use clock::{Clock, MockClock, SystemClock};
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
pub use error_format::{anthropic_error_body, anthropic_error_response};
#[cfg(not(loom))]
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
#[cfg(not(loom))]
pub use hop_by_hop::{HopByHopStripLayer, HopByHopStripService, strip_hop_by_hop};
#[cfg(not(loom))]
pub use lifecycle::{
    ApiKeyAwareSignerFactory, Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig,
    LimitSubject, LimitSubjectProvider, ProxyError, UpstreamDispatch, ReplicaIdentity,
};
#[cfg(not(loom))]
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
#[cfg(not(loom))]
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay, StreamingUsage};
