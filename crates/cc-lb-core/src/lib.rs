#![forbid(unsafe_code)]

pub mod api_keys;
pub mod audit_writer;
mod bulkhead;
mod circuit_breaker;
mod clock;
mod dns_cache;
mod drain;
mod error_format;
mod error_normalizer;
mod hop_by_hop;
mod lifecycle;
mod sse_error_frame;
mod sse_relay;
pub mod usage_pruner;

pub use audit_writer::{spawn_audit_writer, AuditDropped, AuditEntry, AuditWriterSink};
pub use bulkhead::{
    make_default_dispatcher, make_http_dispatcher_with_connector, Bulkhead, BulkheadConfig,
    BulkheadDispatch, BulkheadError, BulkheadRegistry, ExecuteError,
};
pub use circuit_breaker::{
    BreakerConfig, BreakerError, BreakerRegistry, BreakerState, CircuitBreaker,
    CircuitBreakerConfig, CircuitBreakerDispatch, Permit,
};
pub use clock::{Clock, MockClock, SystemClock};
#[doc(hidden)]
pub use dns_cache::make_resolver_with_factory;
pub use drain::{proxy_drain_middleware, DrainController};
pub use dns_cache::{
    make_resolver, CachingDnsConnector, DnsCacheError, DnsResolveFuture, DnsResolver,
    DnsResolverConfig,
};
pub use error_format::{anthropic_error_body, anthropic_error_response};
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
pub use hop_by_hop::{strip_hop_by_hop, HopByHopStripLayer, HopByHopStripService};
pub use lifecycle::{
    ApiKeyAwareSignerFactory, Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig,
    LimitSubject, LimitSubjectProvider, ProxyError, UpstreamDispatch,
};
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay, StreamingUsage};
