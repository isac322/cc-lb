use metrics::Unit;

use crate::{MetricDefinition, MetricKind};

pub const PROMETHEUS14_METRIC_DEFINITIONS: [MetricDefinition; 30] = [
    MetricDefinition {
        name: "cclb_api_key_requests_total",
        kind: MetricKind::Counter,
        description: "Total API key requests by key, principal, model, upstream kind, and status.",
    },
    MetricDefinition {
        name: "cclb_api_key_tokens_total",
        kind: MetricKind::Counter,
        description: "Total API key tokens by key and token kind.",
    },
    MetricDefinition {
        name: "cclb_api_key_cost_usd_micro_total",
        kind: MetricKind::Counter,
        description: "Total API key virtual cost in micro USD.",
    },
    MetricDefinition {
        name: "cclb_api_key_concurrent",
        kind: MetricKind::Gauge,
        description: "Current API key concurrent request count.",
    },
    MetricDefinition {
        name: "cclb_limit_hits_total",
        kind: MetricKind::Counter,
        description: "Total API key limit hits by limit kind and key.",
    },
    MetricDefinition {
        name: "cclb_price_catalog_refresh_failures_total",
        kind: MetricKind::Counter,
        description: "Total price catalog refresh failures.",
    },
    MetricDefinition {
        name: "cclb_price_catalog_missing_field_total",
        kind: MetricKind::Counter,
        description: "Total price catalog missing fields by model and field.",
    },
    MetricDefinition {
        name: "cclb_price_catalog_validation_failures_total",
        kind: MetricKind::Counter,
        description: "Total price catalog validation failures.",
    },
    MetricDefinition {
        name: "cclb_usage_writer_dropped_total",
        kind: MetricKind::Counter,
        description: "Total usage writer events dropped before enqueue.",
    },
    MetricDefinition {
        name: "cclb_limit_state_writer_dropped_total",
        kind: MetricKind::Counter,
        description: "Total limit state writer events dropped before enqueue.",
    },
    MetricDefinition {
        name: "cclb_audit_writer_dropped_total",
        kind: MetricKind::Counter,
        description: "Total audit writer events dropped before enqueue.",
    },
    MetricDefinition {
        name: "cclb_key_auth_failures_total",
        kind: MetricKind::Counter,
        description: "Total built-in API key authentication failures by reason.",
    },
    MetricDefinition {
        name: "cclb_concurrent_rejects_total",
        kind: MetricKind::Counter,
        description: "Total API key concurrent limit rejects by key.",
    },
    MetricDefinition {
        name: "cclb_streaming_usage_missing_total",
        kind: MetricKind::Counter,
        description: "Total streaming responses that finished without parsed usage by dialect.",
    },
    MetricDefinition {
        name: "cc_lb_cache_hit_total",
        kind: MetricKind::Counter,
        description: "Total cache hit responses by upstream and model.",
    },
    MetricDefinition {
        name: "cc_lb_cache_miss_total",
        kind: MetricKind::Counter,
        description: "Total cache miss responses by upstream and model.",
    },
    MetricDefinition {
        name: "cc_lb_cache_observation_dropped_total",
        kind: MetricKind::Counter,
        description: "Total prompt-cache observations dropped by fixed reason.",
    },
    MetricDefinition {
        name: "cc_lb_cache_observation_write_failed_total",
        kind: MetricKind::Counter,
        description: "Total prompt-cache observation store writes that failed by store kind.",
    },
    MetricDefinition {
        name: "cc_lb_dropped_events_total",
        kind: MetricKind::Counter,
        description: "Total events dropped from a bounded channel or queue by reason.",
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_hits_total",
        kind: MetricKind::Counter,
        description: "Total compiled Wasm module cache hits.",
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_misses_total",
        kind: MetricKind::Counter,
        description: "Total compiled Wasm module cache misses.",
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_evictions_total",
        kind: MetricKind::Counter,
        description: "Total compiled Wasm module cache evictions.",
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_entries",
        kind: MetricKind::Gauge,
        description: "Current compiled Wasm module cache entry count.",
    },
    MetricDefinition {
        name: "cc_lb_compiled_module_cache_bytes",
        kind: MetricKind::Gauge,
        description: "Current compiled Wasm module cache byte size.",
    },
    MetricDefinition {
        name: "cc_lb_storage_operation_duration_seconds",
        kind: MetricKind::Histogram,
        description: "Storage operation latency by store, operation, and status.",
    },
    MetricDefinition {
        name: "cc_lb_upstream_affinity_batch_keys",
        kind: MetricKind::Histogram,
        description: "Input key count per non-empty affinity storage operation by store and operation.",
    },
    MetricDefinition {
        name: "cc_lb_storage_operation_errors_total",
        kind: MetricKind::Counter,
        description: "Storage operation errors by store and operation.",
    },
    MetricDefinition {
        name: "cc_lb_sqlx_pool_size",
        kind: MetricKind::Gauge,
        description: "Current SQLx pool connection count by store.",
    },
    MetricDefinition {
        name: "cc_lb_sqlx_pool_idle",
        kind: MetricKind::Gauge,
        description: "Current SQLx pool idle connection count by store.",
    },
    MetricDefinition {
        name: "cc_lb_sqlx_pool_in_use",
        kind: MetricKind::Gauge,
        description: "Current SQLx pool in-use connection count by store.",
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
        "cclb_price_catalog_refresh_failures_total",
        Unit::Count,
        "Total price catalog refresh failures."
    );
    metrics::describe_counter!(
        "cclb_price_catalog_missing_field_total",
        Unit::Count,
        "Total price catalog missing fields by model and field."
    );
    metrics::describe_counter!(
        "cclb_price_catalog_validation_failures_total",
        Unit::Count,
        "Total price catalog validation failures."
    );
    metrics::describe_counter!(
        "cclb_usage_writer_dropped_total",
        Unit::Count,
        "Total usage writer events dropped before enqueue."
    );
    metrics::describe_counter!(
        "cclb_limit_state_writer_dropped_total",
        Unit::Count,
        "Total limit state writer events dropped before enqueue."
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
        "cclb_streaming_usage_missing_total",
        Unit::Count,
        "Total streaming responses that finished without parsed usage by dialect."
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
        "cc_lb_dropped_events_total",
        Unit::Count,
        "Total events dropped from a bounded channel or queue by reason."
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
    metrics::counter!("cclb_price_catalog_refresh_failures_total").increment(1);
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => "smoke-model",
        "field" => "input_cost_per_token"
    )
    .increment(1);
    metrics::counter!("cclb_price_catalog_validation_failures_total").increment(1);
    metrics::counter!("cclb_usage_writer_dropped_total").increment(1);
    metrics::counter!("cclb_limit_state_writer_dropped_total").increment(1);
    metrics::counter!("cclb_audit_writer_dropped_total").increment(1);
    metrics::counter!("cclb_key_auth_failures_total", "reason" => "InvalidKey").increment(1);
    metrics::counter!("cclb_concurrent_rejects_total", "key_id" => "smoke-key").increment(1);
    metrics::counter!("cclb_streaming_usage_missing_total", "dialect" => "anthropic").increment(1);
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

pub(crate) fn touch_prometheus14_metric_handles() {
    metrics::counter!(
        "cclb_api_key_requests_total",
        "key_id" => "unknown",
        "principal_id" => "unknown",
        "model" => "unknown",
        "upstream_kind" => "unknown",
        "status" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cclb_api_key_tokens_total",
        "key_id" => "unknown",
        "kind" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cclb_api_key_cost_usd_micro_total",
        "key_id" => "unknown"
    )
    .increment(0);
    metrics::gauge!("cclb_api_key_concurrent", "key_id" => "unknown").set(0.0);
    metrics::counter!(
        "cclb_limit_hits_total",
        "kind" => "unknown",
        "key_id" => "unknown"
    )
    .increment(0);
    metrics::counter!("cclb_price_catalog_refresh_failures_total").increment(0);
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => "unknown",
        "field" => "unknown"
    )
    .increment(0);
    metrics::counter!("cclb_price_catalog_validation_failures_total").increment(0);
    metrics::counter!("cclb_usage_writer_dropped_total").increment(0);
    metrics::counter!("cclb_limit_state_writer_dropped_total").increment(0);
    metrics::counter!("cclb_audit_writer_dropped_total").increment(0);
    metrics::counter!("cclb_key_auth_failures_total", "reason" => "unknown").increment(0);
    metrics::counter!("cclb_concurrent_rejects_total", "key_id" => "unknown").increment(0);
    metrics::counter!("cclb_streaming_usage_missing_total", "dialect" => "unknown").increment(0);
    metrics::counter!(
        "cc_lb_cache_hit_total",
        "upstream" => "unknown",
        "model" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_cache_miss_total",
        "upstream" => "unknown",
        "model" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_cache_observation_dropped_total",
        "reason" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_cache_observation_write_failed_total",
        "store" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_dropped_events_total",
        "reason" => "unknown"
    )
    .increment(0);
    metrics::counter!("cc_lb_compiled_module_cache_hits_total").increment(0);
    metrics::counter!("cc_lb_compiled_module_cache_misses_total").increment(0);
    metrics::counter!("cc_lb_compiled_module_cache_evictions_total").increment(0);
    metrics::gauge!("cc_lb_compiled_module_cache_entries").set(0.0);
    metrics::gauge!("cc_lb_compiled_module_cache_bytes").set(0.0);
    metrics::histogram!(
        "cc_lb_storage_operation_duration_seconds",
        "store" => "unknown",
        "operation" => "unknown",
        "status" => "unknown"
    )
    .record(0.0);
    metrics::histogram!(
        "cc_lb_upstream_affinity_batch_keys",
        "store" => "unknown",
        "operation" => "unknown"
    )
    .record(0.0);
    metrics::counter!(
        "cc_lb_storage_operation_errors_total",
        "store" => "unknown",
        "operation" => "unknown"
    )
    .increment(0);
    metrics::gauge!("cc_lb_sqlx_pool_size", "store" => "unknown").set(0.0);
    metrics::gauge!("cc_lb_sqlx_pool_idle", "store" => "unknown").set(0.0);
    metrics::gauge!("cc_lb_sqlx_pool_in_use", "store" => "unknown").set(0.0);
}
