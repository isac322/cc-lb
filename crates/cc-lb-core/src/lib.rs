#![forbid(unsafe_code)]

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
mod limit_state_writer;
mod quota;
mod quota_sweep;
mod rate_limit_headers;
mod request_events;
mod sse_error_frame;
mod sse_relay;

pub use bulkhead::{
    Bulkhead, BulkheadConfig, BulkheadDispatch, BulkheadError, BulkheadRegistry, ExecuteError,
    make_default_dispatcher, make_http_dispatcher_with_connector,
};
pub use circuit_breaker::{
    BreakerConfig, BreakerError, BreakerRegistry, BreakerState, CircuitBreaker,
    CircuitBreakerConfig, CircuitBreakerDispatch, Permit,
};
pub use clock::{Clock, MockClock, SystemClock};
pub use dashboard_broadcaster::{
    DEFAULT_DASHBOARD_BROADCAST_CAPACITY, DashboardBroadcaster, record_dashboard_sse_lagged,
};
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
    Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig, ProxyError, UpstreamDispatch,
};
pub use limit_state_writer::{
    DEFAULT_PRINCIPAL_LIMIT_STATE_CHANNEL_CAPACITY, PrincipalLimitStateEnqueueError,
    PrincipalLimitStateSink, start_principal_limit_state_writer,
};
pub use quota::{
    BucketKind, QuotaConfig, QuotaDecision, QuotaError, QuotaManager, QuotaPolicy, Reservation,
    current_window_start,
};
pub use quota_sweep::start_sweep;
pub use request_events::{
    DEFAULT_REQUEST_EVENT_CHANNEL_CAPACITY, RequestEventEnqueueError, RequestEventSink,
    start_request_event_writer,
};
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay};
