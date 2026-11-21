//! Volatile lifecycle-event vocabulary.

#![forbid(unsafe_code)]

mod event;
mod stage_parse_auth;
mod stage_route_limit;
mod stage_usage_stream;
mod termination;

pub use event::{
    DEFAULT_LIFECYCLE_IDLE_TTL, DEFAULT_LIFECYCLE_MAX_TTL,
    DEFAULT_STREAM_ACTIVITY_HEARTBEAT_INTERVAL, EventId, LifecycleEvent, RequestIoTimings,
    RequestSetupTimings, UpstreamBodyEnd, UpstreamStreamDiagnostics,
};
pub use stage_parse_auth::{AuthFailure, AuthInfo, ParseFailure, ParseInfo};
pub use stage_route_limit::{
    LimitDecisionKind, LimitRequestSummary, LimitSubject, RouteFailure, RouteInfo, RouteSummary,
};
pub use stage_usage_stream::{StreamError, StreamSuccess, UsageSnapshot, UsageSource};
pub use termination::TerminationReason;

#[cfg(test)]
mod tests;
