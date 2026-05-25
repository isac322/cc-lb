use metrics::Unit;

use crate::{MetricDefinition, MetricKind};

pub const PROMETHEUS14_METRIC_DEFINITIONS: [MetricDefinition; 14] = [
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
    metrics::counter!("cclb_streaming_usage_missing_total", "dialect" => "bedrock").increment(1);
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
}
