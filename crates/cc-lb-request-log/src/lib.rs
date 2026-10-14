//! Stable persisted request-log read-model vocabulary.

#![forbid(unsafe_code)]

mod cache;
mod event_kind;
mod header_snapshot;
mod request_diagnostics;
mod request_event;
mod request_event_update;
mod storage_tail;

pub use cache::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheLookbackPrefix,
    RequestCacheState,
};
pub use event_kind::{ParseRequestEventKindError, RequestEventKind};
pub use header_snapshot::{CostBreakdown, HeaderSnapshot};
pub use request_diagnostics::{
    REQUEST_DIAGNOSTIC_PAYLOAD_KEYS, REQUEST_DIAGNOSTICS_SCHEMA, RequestDiagnostics,
    RequestDiagnosticsRecord, RequestDiagnosticsSource, render_request_diagnostics_line,
    strip_payload_diagnostics,
};
pub use request_event::RequestEvent;
pub use request_event_update::{
    FinalRequestEventUpdate, RequestEventPartial, RequestEventPhase, RequestEventUpdate,
};
pub use storage_tail::StorageTailUpdate;
