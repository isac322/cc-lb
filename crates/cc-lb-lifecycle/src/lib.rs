//! Volatile lifecycle-event vocabulary.

#![forbid(unsafe_code)]

mod event;
mod prompt_cache;
mod stage_parse_auth;
mod stage_route_limit;
mod stage_usage_stream;
mod termination;

pub use event::{EventId, LifecycleEvent, RequestSetupTimings};
pub use prompt_cache::{PromptCacheObservationKindWire, PromptCacheObservationWire};
pub use stage_parse_auth::{AuthFailure, AuthInfo, ParseFailure, ParseInfo};
pub use stage_route_limit::{
    LimitDecisionKind, LimitRequestSummary, LimitSubject, RouteFailure, RouteInfo, RouteSummary,
};
pub use stage_usage_stream::{StreamError, StreamSuccess, UsageSnapshot, UsageSource};
pub use termination::TerminationReason;

#[cfg(test)]
mod tests;
