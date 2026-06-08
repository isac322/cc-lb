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
    BoundedChannelHook, DEFAULT_HOOK_CHANNEL_CAPACITY, dropped_events_total,
    increment_dropped_events_by,
};
pub use init::{
    InitError, MetricDefinition, MetricKind, ObservabilityConfig, TracingGuard, init,
    metric_definitions, panic_total, register_metrics,
};
pub use panic_hook::install_panic_hook;
pub use redaction::{REDACTED, RedactingMakeWriter, RedactionLayer, RedactionPolicy};
pub use trace_layer::{ObservabilityTraceLayer, trace_layer};

pub mod cache_observation_dropped_reason {
    // Keep this enum-like set bounded: queue_full, below_threshold, status_4xx, abort.
    pub const QUEUE_FULL: &str = "queue_full";
    pub const BELOW_THRESHOLD: &str = "below_threshold";
    pub const STATUS_4XX: &str = "status_4xx";
    pub const ABORT: &str = "abort";
}

pub mod cache_observation_store_kind {
    pub const REDB: &str = "redb";
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

pub fn observe_cache_token_drift(upstream: &str, model: &str, drift: i32) {
    metrics::histogram!(
        "cc_lb_cache_token_drift",
        "upstream" => upstream.to_owned(),
        "model" => model.to_owned()
    )
    .record(f64::from(drift));
}

pub fn inc_cache_observation_dropped(reason: &str) {
    metrics::counter!(
        "cc_lb_cache_observation_dropped_total",
        "reason" => reason.to_owned()
    )
    .increment(1);
}

pub fn inc_cache_observation_write_failed(store: &str) {
    metrics::counter!(
        "cc_lb_cache_observation_write_failed_total",
        "store" => store.to_owned()
    )
    .increment(1);
}
