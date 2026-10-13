use metrics::Unit;

use crate::{MetricDefinition, MetricKind};

pub const PROMETHEUS14_METRIC_DEFINITIONS: [MetricDefinition; 27] = [
    MetricDefinition {
        name: "cclb_api_key_requests_total",
        kind: MetricKind::Counter,
        description: "Total API key requests by key, principal, model, upstream kind, and status.",
        labels: &["key_id", "principal_id", "model", "upstream_kind", "status"],
    },
    MetricDefinition {
        name: "cclb_api_key_tokens_total",
        kind: MetricKind::Counter,
        description: "Total API key tokens by key and token kind.",
        labels: &["key_id", "kind"],
    },
    MetricDefinition {
        name: "cclb_api_key_cost_usd_micro_total",
        kind: MetricKind::Counter,
        description: "Total API key virtual cost in micro USD.",
        labels: &["key_id"],
    },
    MetricDefinition {
        name: "cclb_api_key_concurrent",
        kind: MetricKind::Gauge,
        description: "Current API key concurrent request count.",
        labels: &["key_id"],
    },
    MetricDefinition {
        name: "cclb_limit_hits_total",
        kind: MetricKind::Counter,
        description: "Total API key limit hits by limit kind and key.",
        labels: &["kind", "key_id"],
    },
    MetricDefinition {
        name: "cclb_price_catalog_missing_field_total",
        kind: MetricKind::Counter,
        description: "Total price catalog missing fields by model and field.",
        labels: &["model", "field"],
    },
    MetricDefinition {
        name: "cclb_audit_writer_dropped_total",
        kind: MetricKind::Counter,
        description: "Total audit writer events dropped before enqueue.",
        labels: &[],
    },
    MetricDefinition {
        name: "cclb_key_auth_failures_total",
        kind: MetricKind::Counter,
        description: "Total built-in API key authentication failures by reason.",
        labels: &["reason"],
    },
    MetricDefinition {
        name: "cclb_concurrent_rejects_total",
        kind: MetricKind::Counter,
        description: "Total API key concurrent limit rejects by key.",
        labels: &["key_id"],
    },
    MetricDefinition {
        name: "cc_lb_cache_hit_total",
        kind: MetricKind::Counter,
        description: "Total cache hit responses by upstream and model.",
        labels: &["upstream", "model"],
    },
    MetricDefinition {
        name: "cc_lb_cache_miss_total",
        kind: MetricKind::Counter,
        description: "Total cache miss responses by upstream and model.",
        labels: &["upstream", "model"],
    },
    MetricDefinition {
        name: "cc_lb_cache_observation_dropped_total",
        kind: MetricKind::Counter,
        description: "Total prompt-cache observations dropped by fixed reason.",
        labels: &["reason"],
    },
    MetricDefinition {
        name: "cc_lb_cache_observation_write_failed_total",
        kind: MetricKind::Counter,
        description: "Total prompt-cache observation store writes that failed by store kind.",
        labels: &["store"],
    },
    MetricDefinition {
        name: "cc_lb_cache_observation_read_failed_total",
        kind: MetricKind::Counter,
        description: "Total shared prompt-cache observation lookups that failed.",
        labels: &[],
    },
    MetricDefinition {
        name: "cc_lb_dropped_events_total",
        kind: MetricKind::Counter,
        description: "Total events dropped from a bounded channel or queue by reason.",
        labels: &["reason"],
    },
    MetricDefinition {
        name: "cc_lb_request_diagnostics_lines_total",
        kind: MetricKind::Counter,
        description: "Total request diagnostics stdout lines by outcome.",
        labels: &["outcome"],
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_hits_total",
        kind: MetricKind::Counter,
        description: "Total compiled Wasm module cache hits.",
        labels: &[],
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_misses_total",
        kind: MetricKind::Counter,
        description: "Total compiled Wasm module cache misses.",
        labels: &[],
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_evictions_total",
        kind: MetricKind::Counter,
        description: "Total compiled Wasm module cache evictions.",
        labels: &[],
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_entries",
        kind: MetricKind::Gauge,
        description: "Current compiled Wasm module cache entry count.",
        labels: &[],
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_bytes",
        kind: MetricKind::Gauge,
        description: "Current compiled Wasm module cache byte size.",
        labels: &[],
    },
    MetricDefinition {
        name: "cc_lb_storage_operation_duration_seconds",
        kind: MetricKind::Histogram,
        description: "Storage operation latency by store, operation, and status.",
        labels: &["store", "operation", "status"],
    },
    MetricDefinition {
        name: "cc_lb_upstream_affinity_batch_keys",
        kind: MetricKind::Histogram,
        description: "Input key count per non-empty affinity storage operation by store and operation.",
        labels: &["store", "operation"],
    },
    MetricDefinition {
        name: "cc_lb_storage_operation_errors_total",
        kind: MetricKind::Counter,
        description: "Storage operation errors by store and operation.",
        labels: &["store", "operation"],
    },
    MetricDefinition {
        name: "cc_lb_sqlx_pool_size",
        kind: MetricKind::Gauge,
        description: "Current SQLx pool connection count by store.",
        labels: &["store"],
    },
    MetricDefinition {
        name: "cc_lb_sqlx_pool_idle",
        kind: MetricKind::Gauge,
        description: "Current SQLx pool idle connection count by store.",
        labels: &["store"],
    },
    MetricDefinition {
        name: "cc_lb_sqlx_pool_in_use",
        kind: MetricKind::Gauge,
        description: "Current SQLx pool in-use connection count by store.",
        labels: &["store"],
    },
];

pub(crate) fn register_prometheus14_metrics() {
    metrics::describe_counter!(
        "cclb_api_key_requests_total",
        Unit::Count,
        "Total API key requests by key, principal, model, upstream kind, and status."
    );
    metrics::describe_counter!(
        "cclb_api_key_tokens_total",
        Unit::Count,
        "Total API key tokens by key and token kind."
    );
    metrics::describe_counter!(
        "cclb_api_key_cost_usd_micro_total",
        Unit::Count,
        "Total API key virtual cost in micro USD."
    );
    metrics::describe_gauge!(
        "cclb_api_key_concurrent",
        Unit::Count,
        "Current API key concurrent request count."
    );
    metrics::describe_counter!(
        "cclb_limit_hits_total",
        Unit::Count,
        "Total API key limit hits by limit kind and key."
    );
    metrics::describe_counter!(
        "cclb_price_catalog_missing_field_total",
        Unit::Count,
        "Total price catalog missing fields by model and field."
    );
    metrics::describe_counter!(
        "cclb_audit_writer_dropped_total",
        Unit::Count,
        "Total audit writer events dropped before enqueue."
    );
    metrics::describe_counter!(
        "cclb_key_auth_failures_total",
        Unit::Count,
        "Total built-in API key authentication failures by reason."
    );
    metrics::describe_counter!(
        "cclb_concurrent_rejects_total",
        Unit::Count,
        "Total API key concurrent limit rejects by key."
    );
    metrics::describe_counter!(
        "cc_lb_cache_hit_total",
        Unit::Count,
        "Total cache hit responses by upstream and model."
    );
    metrics::describe_counter!(
        "cc_lb_cache_miss_total",
        Unit::Count,
        "Total cache miss responses by upstream and model."
    );
    metrics::describe_counter!(
        "cc_lb_cache_observation_dropped_total",
        Unit::Count,
        "Total prompt-cache observations dropped by fixed reason."
    );
    metrics::describe_counter!(
        "cc_lb_cache_observation_write_failed_total",
        Unit::Count,
        "Total prompt-cache observation store writes that failed by store kind."
    );
    metrics::describe_counter!(
        "cc_lb_cache_observation_read_failed_total",
        Unit::Count,
        "Total shared prompt-cache observation lookups that failed."
    );
    metrics::describe_counter!(
        "cc_lb_dropped_events_total",
        Unit::Count,
        "Total events dropped from a bounded channel or queue by reason."
    );
    metrics::describe_counter!(
        "cc_lb_request_diagnostics_lines_total",
        Unit::Count,
        "Total request diagnostics stdout lines by outcome."
    );
    metrics::describe_counter!(
        "cc_lb_compiled_module_cache_hits_total",
        Unit::Count,
        "Total compiled Wasm module cache hits."
    );
    metrics::describe_counter!(
        "cc_lb_compiled_module_cache_misses_total",
        Unit::Count,
        "Total compiled Wasm module cache misses."
    );
    metrics::describe_counter!(
        "cc_lb_compiled_module_cache_evictions_total",
        Unit::Count,
        "Total compiled Wasm module cache evictions."
    );
    metrics::describe_gauge!(
        "cc_lb_compiled_module_cache_entries",
        Unit::Count,
        "Current compiled Wasm module cache entry count."
    );
    metrics::describe_gauge!(
        "cc_lb_compiled_module_cache_bytes",
        Unit::Bytes,
        "Current compiled Wasm module cache byte size."
    );
    metrics::describe_histogram!(
        "cc_lb_storage_operation_duration_seconds",
        Unit::Seconds,
        "Storage operation latency by store, operation, and status."
    );
    metrics::describe_histogram!(
        "cc_lb_upstream_affinity_batch_keys",
        Unit::Count,
        "Input key count per non-empty affinity storage operation by store and operation."
    );
    metrics::describe_counter!(
        "cc_lb_storage_operation_errors_total",
        Unit::Count,
        "Storage operation errors by store and operation."
    );
    metrics::describe_gauge!(
        "cc_lb_sqlx_pool_size",
        Unit::Count,
        "Current SQLx pool connection count by store."
    );
    metrics::describe_gauge!(
        "cc_lb_sqlx_pool_idle",
        Unit::Count,
        "Current SQLx pool idle connection count by store."
    );
    metrics::describe_gauge!(
        "cc_lb_sqlx_pool_in_use",
        Unit::Count,
        "Current SQLx pool in-use connection count by store."
    );
}

pub fn prometheus14_metric_definitions() -> &'static [MetricDefinition] {
    &PROMETHEUS14_METRIC_DEFINITIONS
}

pub fn touch_prometheus14_metrics() {
    metrics::counter!(
        "cclb_api_key_requests_total",
        "key_id" => "smoke-key",
        "principal_id" => "smoke-principal",
        "model" => "smoke-model",
        "upstream_kind" => "anthropic",
        "status" => "200"
    )
    .increment(1);
    metrics::counter!(
        "cclb_api_key_tokens_total",
        "key_id" => "smoke-key",
        "kind" => "input"
    )
    .increment(1);
    metrics::counter!(
        "cclb_api_key_cost_usd_micro_total",
        "key_id" => "smoke-key"
    )
    .increment(1);
    metrics::gauge!("cclb_api_key_concurrent", "key_id" => "smoke-key").set(1.0);
    metrics::counter!(
        "cclb_limit_hits_total",
        "kind" => "Requests",
        "key_id" => "smoke-key"
    )
    .increment(1);
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => "smoke-model",
        "field" => "input_cost_per_token"
    )
    .increment(1);
    metrics::counter!("cclb_audit_writer_dropped_total").increment(1);
    metrics::counter!("cclb_key_auth_failures_total", "reason" => "InvalidKey").increment(1);
    metrics::counter!("cclb_concurrent_rejects_total", "key_id" => "smoke-key").increment(1);
    metrics::counter!(
        "cc_lb_cache_hit_total",
        "upstream" => "smoke-upstream",
        "model" => "smoke-model"
    )
    .increment(1);
    metrics::counter!(
        "cc_lb_cache_miss_total",
        "upstream" => "smoke-upstream",
        "model" => "smoke-model"
    )
    .increment(1);
    metrics::counter!(
        "cc_lb_cache_observation_dropped_total",
        "reason" => "queue_full"
    )
    .increment(1);
    metrics::counter!(
        "cc_lb_cache_observation_write_failed_total",
        "store" => "sqlite"
    )
    .increment(1);
    metrics::counter!("cc_lb_cache_observation_read_failed_total").increment(1);
    metrics::counter!("cc_lb_dropped_events_total", "reason" => "queue_full").increment(1);
    metrics::counter!(
        "cc_lb_request_diagnostics_lines_total",
        "outcome" => "written"
    )
    .increment(1);
    metrics::counter!("cc_lb_compiled_module_cache_hits_total").increment(1);
    metrics::counter!("cc_lb_compiled_module_cache_misses_total").increment(1);
    metrics::counter!("cc_lb_compiled_module_cache_evictions_total").increment(1);
    metrics::gauge!("cc_lb_compiled_module_cache_entries").set(1.0);
    metrics::gauge!("cc_lb_compiled_module_cache_bytes").set(1.0);
    metrics::histogram!(
        "cc_lb_storage_operation_duration_seconds",
        "store" => "sqlite",
        "operation" => "smoke",
        "status" => "ok"
    )
    .record(0.0);
    metrics::histogram!(
        "cc_lb_upstream_affinity_batch_keys",
        "store" => "sqlite",
        "operation" => "smoke"
    )
    .record(1.0);
    metrics::counter!(
        "cc_lb_storage_operation_errors_total",
        "store" => "sqlite",
        "operation" => "smoke"
    )
    .increment(1);
    metrics::gauge!("cc_lb_sqlx_pool_size", "store" => "sqlite").set(1.0);
    metrics::gauge!("cc_lb_sqlx_pool_idle", "store" => "sqlite").set(1.0);
    metrics::gauge!("cc_lb_sqlx_pool_in_use", "store" => "sqlite").set(0.0);
}

/// Zero-initialize only label-free series and series whose labels come from a
/// documented fixed vocabulary; see `touch_metrics` for the rationale.
pub(crate) fn touch_prometheus14_metric_handles() {
    metrics::counter!("cclb_audit_writer_dropped_total").increment(0);
    for reason in [
        crate::cache_observation_dropped_reason::QUEUE_FULL,
        crate::cache_observation_dropped_reason::CHANNEL_CLOSED,
        crate::cache_observation_dropped_reason::BELOW_THRESHOLD,
    ] {
        metrics::counter!(
            "cc_lb_cache_observation_dropped_total",
            "reason" => reason
        )
        .increment(0);
    }
    for store in [
        crate::cache_observation_store_kind::SQLITE,
        crate::cache_observation_store_kind::POSTGRES,
    ] {
        metrics::counter!(
            "cc_lb_cache_observation_write_failed_total",
            "store" => store
        )
        .increment(0);
    }
    metrics::counter!("cc_lb_cache_observation_read_failed_total").increment(0);
    for outcome in REQUEST_DIAGNOSTICS_LINE_OUTCOMES {
        metrics::counter!(
            "cc_lb_request_diagnostics_lines_total",
            "outcome" => outcome
        )
        .increment(0);
    }
    for reason in REQUEST_DIAGNOSTICS_DROP_REASONS {
        metrics::counter!("cc_lb_dropped_events_total", "reason" => reason).increment(0);
    }
    metrics::counter!("cc_lb_compiled_module_cache_hits_total").increment(0);
    metrics::counter!("cc_lb_compiled_module_cache_misses_total").increment(0);
    metrics::counter!("cc_lb_compiled_module_cache_evictions_total").increment(0);
    metrics::gauge!("cc_lb_compiled_module_cache_entries").set(0.0);
    metrics::gauge!("cc_lb_compiled_module_cache_bytes").set(0.0);
}

/// Fixed `outcome` vocabulary of `cc_lb_request_diagnostics_lines_total`; must match
/// `cc_lb_engine::request_diagnostics_sink`.
const REQUEST_DIAGNOSTICS_LINE_OUTCOMES: [&str; 3] = ["written", "encode_error", "write_error"];

/// `cc_lb_dropped_events_total` reasons emitted by the request diagnostics sink; must match
/// `cc_lb_engine::request_diagnostics_sink`.
const REQUEST_DIAGNOSTICS_DROP_REASONS: [&str; 7] = [
    "request_diagnostics_queue_full",
    "request_diagnostics_strip_queue_full",
    "request_diagnostics_strip_send_timeout",
    "request_diagnostics_queue_closed",
    "request_diagnostics_job_panic",
    "request_diagnostics_worker_panic",
    "request_diagnostics_shutdown_timeout",
];
