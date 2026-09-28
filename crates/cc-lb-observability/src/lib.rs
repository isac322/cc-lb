#![forbid(unsafe_code)]

//! Observability primitives for `cc-lb` tracing, metrics, and panic reporting.

#[path = "metrics.rs"]
mod cclb_metrics;
mod dropped_events;
mod engine_hook;
mod init;
mod panic_hook;
mod propagation;
mod redaction;
mod trace_layer;

pub use cclb_metrics::{prometheus14_metric_definitions, touch_prometheus14_metrics};
pub use dropped_events::increment_dropped_events_by;
pub use engine_hook::{EngineMetricsHook, MetricsCrateHook, NoopMetricsHook};
pub use init::{
    InitError, MetricDefinition, MetricKind, ObservabilityConfig, TracingGuard, init,
    metric_definitions, panic_total, register_metrics,
};
pub use panic_hook::install_panic_hook;
pub use propagation::{inject_current_trace_context, parent_context_from_headers};
pub use redaction::{
    REDACTED, ROUTING_REASON_MAX_BYTES, ROUTING_TRACE_SIZE_CAP_BYTES, RedactingMakeWriter,
    RedactionLayer, RedactionPolicy, enforce_routing_trace_caps, redact_internal_errors,
    redact_routing_trace, truncate_reason,
};
pub use trace_layer::{
    ObservabilityTraceLayer, ProxyMakeSpan, ProxyOnBodyChunk, ProxyOnRequest, ProxyOnResponse,
    RouteTemplateFn, trace_layer,
};

pub mod cache_observation_dropped_reason {
    // Fixed reasons keep metric label cardinality bounded.
    pub const QUEUE_FULL: &str = "queue_full";
    pub const CHANNEL_CLOSED: &str = "channel_closed";
    pub const BELOW_THRESHOLD: &str = "below_threshold";
}

pub mod cache_observation_store_kind {
    pub const SQLITE: &str = "sqlite";
    pub const POSTGRES: &str = "postgres";
}

/// Increment cache hit counter by upstream and model.
pub fn inc_cache_hit(upstream: &str, model: &str) {
    metrics::counter!(
        "cc_lb_cache_hit_total",
        "upstream" => upstream.to_owned(),
        "model" => model.to_owned()
    )
    .increment(1);
}

/// Increment cache miss counter by upstream and model.
pub fn inc_cache_miss(upstream: &str, model: &str) {
    metrics::counter!(
        "cc_lb_cache_miss_total",
        "upstream" => upstream.to_owned(),
        "model" => model.to_owned()
    )
    .increment(1);
}
pub fn inc_cache_observation_dropped(reason: &str) {
    metrics::counter!(
        "cc_lb_cache_observation_dropped_total",
        "reason" => reason.to_owned()
    )
    .increment(1);
}

/// Record a failed shared-cache lookup without classifying provider cache usage.
pub fn inc_cache_observation_read_failed() {
    metrics::counter!("cc_lb_cache_observation_read_failed_total").increment(1);
}

pub fn inc_cache_observation_write_failed(store: &str) {
    metrics::counter!(
        "cc_lb_cache_observation_write_failed_total",
        "store" => store.to_owned()
    )
    .increment(1);
}
