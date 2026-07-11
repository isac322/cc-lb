//! Stable persisted request-log read-model vocabulary.

#![forbid(unsafe_code)]

mod cache;
mod header_snapshot;
mod request_event;
mod request_event_update;

pub use cache::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheLookbackPrefix,
    RequestCacheState, RequestEventUpstream,
};
pub use header_snapshot::{CostBreakdown, HeaderSnapshot};
pub use request_event::RequestEvent;
pub use request_event_update::{
    FinalRequestEventUpdate, RequestEventPartial, RequestEventPhase, RequestEventUpdate,
};
