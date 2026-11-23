#![forbid(unsafe_code)]
//! Out-of-band proxy timing, connector instrumentation, trace propagation, and async completion logging.

pub mod completion_observer;
pub mod instrumented_connector;
pub mod request_timing;
pub mod trace_context;

pub use completion_observer::{
    CompletionObservationDrop, CompletionObserver, NumericAttribute, StreamCompletionLog,
    StreamLatency, numeric_attribute,
};
pub use instrumented_connector::InstrumentedHttpsConnector;
pub use request_timing::{
    DnsResolutionOutcome, DnsSnapshot, REQUEST_STAGE_TIMINGS, RequestStageTimings, dns_snapshot,
    finalize_connection_reused_if_unset, mark_connector_called, record_bulkhead_wait,
    record_connect, record_dns, with_timings,
};
pub use trace_context::inject_current_trace_context;
