#![forbid(unsafe_code)]

mod bulkhead;
mod circuit_breaker;
mod clock;
mod dns_cache;
mod error_format;
mod error_normalizer;
mod hop_by_hop;
mod lifecycle;
mod quota;
mod quota_sweep;
mod sse_error_frame;
mod sse_relay;

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
pub use dns_cache::{
    make_resolver, CachingDnsConnector, DnsCacheError, DnsResolveFuture, DnsResolver,
    DnsResolverConfig,
};
pub use error_format::{anthropic_error_body, anthropic_error_response};
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
pub use hop_by_hop::{strip_hop_by_hop, HopByHopStripLayer, HopByHopStripService};
pub use lifecycle::{
    Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig, ProxyError, UpstreamDispatch,
};
pub use quota::{
    current_window_start, BucketKind, QuotaConfig, QuotaDecision, QuotaError, QuotaManager,
    QuotaPolicy, Reservation,
};
pub use quota_sweep::start_sweep;
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay};
