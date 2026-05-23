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
    make_default_dispatcher, make_http_dispatcher_with_connector, Bulkhead, BulkheadConfig,
    BulkheadDispatch, BulkheadError, BulkheadRegistry, ExecuteError,
};
pub use circuit_breaker::{
    BreakerConfig, BreakerError, BreakerRegistry, BreakerState, CircuitBreaker,
    CircuitBreakerConfig, CircuitBreakerDispatch, Permit,
};
pub use clock::{Clock, MockClock, SystemClock};
pub use dashboard_broadcaster::{
    record_dashboard_sse_lagged, DashboardBroadcaster, DEFAULT_DASHBOARD_BROADCAST_CAPACITY,
};
#[doc(hidden)]
pub use dns_cache::make_resolver_with_factory;
pub use dns_cache::{
    make_resolver, CachingDnsConnector, DnsCacheError, DnsResolveFuture, DnsResolver,
    DnsResolverConfig,
};
pub use drain::{proxy_drain_middleware, DrainController};
pub use error_format::{anthropic_error_body, anthropic_error_response};
pub use error_normalizer::{ErrorNormalizer, NormalizerError, UpstreamKind};
pub use hop_by_hop::{strip_hop_by_hop, HopByHopStripLayer, HopByHopStripService};
pub use lifecycle::{
    Body, DispatchError, HyperDispatcher, Lifecycle, LifecycleConfig, ProxyError, UpstreamDispatch,
};
pub use limit_state_writer::{
    start_principal_limit_state_writer, PrincipalLimitStateEnqueueError, PrincipalLimitStateSink,
    DEFAULT_PRINCIPAL_LIMIT_STATE_CHANNEL_CAPACITY,
};
pub use quota::{
    current_window_start, BucketKind, QuotaConfig, QuotaDecision, QuotaError, QuotaManager,
    QuotaPolicy, Reservation,
};
pub use quota_sweep::start_sweep;
pub use request_events::{
    start_request_event_writer, RequestEventEnqueueError, RequestEventSink,
    DEFAULT_REQUEST_EVENT_CHANNEL_CAPACITY,
};
pub use sse_error_frame::{make_error_frame, make_error_frame_from_json};
pub use sse_relay::{RelayError, SseBatchConfig, SseRelay};
