//! Contract types and traits shared by cc-lb runtime layers.
//!
//! This crate retains bridge exports while lifecycle vocabulary resides in
//! `cc-lb-lifecycle` and Tokio lifecycle transport resides in `cc-lb-control`.

#![deny(missing_debug_implementations)]

#[doc(hidden)]
pub use cc_lb_control::{
    BusReceiver, DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, LifecycleBusReceiver,
    ReplicaIdentityProvider, RequestEventBus,
};

#[doc(hidden)]
pub use cc_lb_lifecycle::{
    AuthFailure, AuthInfo, EventId, LifecycleEvent, LimitDecisionKind, LimitRequestSummary,
    LimitSubject, ParseFailure, ParseInfo, PromptCacheObservationKindWire,
    PromptCacheObservationWire, RouteFailure, RouteInfo, RouteSummary, StreamError, StreamSuccess,
    TerminationReason, UsageSnapshot, UsageSource,
};

#[doc(hidden)]
pub use cc_lb_request_log::{
    CostBreakdown, FinalRequestEventUpdate, HeaderSnapshot, RequestCacheBreakpoint,
    RequestCacheBreakpointSource, RequestCacheLookbackPrefix, RequestCacheState, RequestEvent,
    RequestEventPartial, RequestEventPhase, RequestEventUpdate, RequestEventUpstream,
};

#[doc(hidden)]
pub use cc_lb_observability::{EngineMetricsHook, NoopMetricsHook};

#[doc(hidden)]
pub use cc_lb_storage_api::{AuditEntry, AuditSink, KeyStatus, Limit, LimitKind};

#[doc(hidden)]
pub use cc_lb_domain::{ANTHROPIC_IDENTITY_HEADERS, PrincipalKindLite, ReplicaIdentity};
