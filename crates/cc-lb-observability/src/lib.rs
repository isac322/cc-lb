#![forbid(unsafe_code)]

//! Observability primitives for `cc-lb` tracing, metrics, hooks, and panic reporting.

#[path = "metrics.rs"]
mod cclb_metrics;
mod hook;
mod init;
mod panic_hook;
mod redaction;
mod trace_layer;

pub use cc_lb_plugin_api::{ObservabilityError, ObservabilityHook, ObserveEvent};
pub use cclb_metrics::{prometheus14_metric_definitions, touch_prometheus14_metrics};
pub use hook::{
    dropped_events_total, increment_dropped_events_by, BoundedChannelHook,
    DEFAULT_HOOK_CHANNEL_CAPACITY,
};
pub use init::{
    init, metric_definitions, panic_total, register_metrics, InitError, MetricDefinition,
    MetricKind, ObservabilityConfig, TracingGuard,
};
pub use panic_hook::install_panic_hook;
pub use redaction::{RedactingMakeWriter, RedactionLayer, RedactionPolicy, REDACTED};
pub use trace_layer::{trace_layer, ObservabilityTraceLayer};
