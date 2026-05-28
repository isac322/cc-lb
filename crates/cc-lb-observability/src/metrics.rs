use std::time::Duration;

use metrics::Unit;

use crate::{MetricDefinition, MetricKind};

pub const REBIND_DURATION_BUCKETS: [f64; 11] =
    [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

pub const DYNAMIC_RUNTIME_METRIC_DEFINITIONS: [MetricDefinition; 8] = [
    MetricDefinition {
        name: "cclb_rebind_total",
        kind: MetricKind::Counter,
        description: "Total dynamic view rebind attempts by outcome.",
    },
    MetricDefinition {
        name: "cclb_rebind_duration_seconds",
        kind: MetricKind::Histogram,
        description: "Dynamic view rebind duration in seconds.",
    },
    MetricDefinition {
        name: "cclb_oauth_refresh_total",
        kind: MetricKind::Counter,
        description: "OAuth token refresh attempts by upstream and outcome.",
    },
    MetricDefinition {
        name: "cclb_oauth_refresh_duration_seconds",
        kind: MetricKind::Histogram,
        description: "OAuth token refresh duration in seconds.",
    },
    MetricDefinition {
        name: "cclb_oauth_refresh_lag_seconds",
        kind: MetricKind::Gauge,
        description: "Seconds since an upstream OAuth refresh window opened.",
    },
    MetricDefinition {
        name: "cclb_wasm_cache_materialize_total",
        kind: MetricKind::Counter,
        description: "Wasm cache materialization results by outcome.",
    },
    MetricDefinition {
        name: "cclb_reconcile_total",
        kind: MetricKind::Counter,
        description: "Dynamic reconciliation passes by outcome.",
    },
    MetricDefinition {
        name: "cclb_notify_received_total",
        kind: MetricKind::Counter,
        description: "Runtime change notifications received by channel.",
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebindOutcome {
    Success,
    Error,
}

impl RebindOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OAuthRefreshOutcome {
    Success,
    HttpError,
    Network,
    Parse,
}

impl OAuthRefreshOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::HttpError => "http_error",
            Self::Network => "network",
            Self::Parse => "parse",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WasmCacheMaterializeOutcome {
    Hit,
    MissPersisted,
    MissFetched,
    Error,
}

impl WasmCacheMaterializeOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::MissPersisted => "miss_persisted",
            Self::MissFetched => "miss_fetched",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileOutcome {
    Changed,
    Unchanged,
}

impl ReconcileOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Changed => "changed",
            Self::Unchanged => "unchanged",
        }
    }
}

pub fn register_dynamic_runtime_metrics() {
    metrics::describe_counter!(
        "cclb_rebind_total",
        Unit::Count,
        "Total dynamic view rebind attempts by outcome."
    );
    metrics::describe_histogram!(
        "cclb_rebind_duration_seconds",
        Unit::Seconds,
        "Dynamic view rebind duration in seconds."
    );
    metrics::describe_counter!(
        "cclb_oauth_refresh_total",
        Unit::Count,
        "OAuth token refresh attempts by upstream and outcome."
    );
    metrics::describe_histogram!(
        "cclb_oauth_refresh_duration_seconds",
        Unit::Seconds,
        "OAuth token refresh duration in seconds."
    );
    metrics::describe_gauge!(
        "cclb_oauth_refresh_lag_seconds",
        Unit::Seconds,
        "Seconds since an upstream OAuth refresh window opened."
    );
    metrics::describe_counter!(
        "cclb_wasm_cache_materialize_total",
        Unit::Count,
        "Wasm cache materialization results by outcome."
    );
    metrics::describe_counter!(
        "cclb_reconcile_total",
        Unit::Count,
        "Dynamic reconciliation passes by outcome."
    );
    metrics::describe_counter!(
        "cclb_notify_received_total",
        Unit::Count,
        "Runtime change notifications received by channel."
    );
}

pub fn record_rebind(outcome: RebindOutcome, duration: Duration) {
    metrics::counter!("cclb_rebind_total", "outcome" => outcome.label()).increment(1);
    metrics::histogram!("cclb_rebind_duration_seconds").record(duration.as_secs_f64());
}

pub fn record_oauth_refresh(upstream: &str, outcome: OAuthRefreshOutcome, duration: Duration) {
    metrics::counter!(
        "cclb_oauth_refresh_total",
        "upstream" => upstream.to_owned(),
        "outcome" => outcome.label()
    )
    .increment(1);
    metrics::histogram!("cclb_oauth_refresh_duration_seconds").record(duration.as_secs_f64());
}

pub fn set_oauth_refresh_lag(upstream: &str, seconds: f64) {
    metrics::gauge!(
        "cclb_oauth_refresh_lag_seconds",
        "upstream" => upstream.to_owned()
    )
    .set(seconds);
}

pub fn record_wasm_cache_materialize(outcome: WasmCacheMaterializeOutcome) {
    metrics::counter!(
        "cclb_wasm_cache_materialize_total",
        "outcome" => outcome.label()
    )
    .increment(1);
}

pub fn record_reconcile(outcome: ReconcileOutcome) {
    metrics::counter!("cclb_reconcile_total", "outcome" => outcome.label()).increment(1);
}

pub fn record_notify_received(channel: &'static str) {
    metrics::counter!("cclb_notify_received_total", "channel" => channel).increment(1);
}

pub fn touch_dynamic_runtime_metric_handles() {
    metrics::counter!("cclb_rebind_total", "outcome" => "success").increment(0);
    metrics::counter!("cclb_rebind_total", "outcome" => "error").increment(0);
    metrics::histogram!("cclb_rebind_duration_seconds").record(0.0);
    for outcome in ["success", "http_error", "network", "parse"] {
        metrics::counter!(
            "cclb_oauth_refresh_total",
            "upstream" => "unknown",
            "outcome" => outcome
        )
        .increment(0);
    }
    metrics::histogram!("cclb_oauth_refresh_duration_seconds").record(0.0);
    metrics::gauge!("cclb_oauth_refresh_lag_seconds", "upstream" => "unknown").set(0.0);
    for outcome in ["hit", "miss_persisted", "miss_fetched", "error"] {
        metrics::counter!("cclb_wasm_cache_materialize_total", "outcome" => outcome).increment(0);
    }
    metrics::counter!("cclb_reconcile_total", "outcome" => "changed").increment(0);
    metrics::counter!("cclb_reconcile_total", "outcome" => "unchanged").increment(0);
    for channel in ["upstream", "principal", "plugin_registry", "plugin_chain"] {
        metrics::counter!("cclb_notify_received_total", "channel" => channel).increment(0);
    }
}

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
    metrics::counter!("cclb_streaming_usage_missing_total", "dialect" => "anthropic").increment(1);
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

#[cfg(test)]
mod tests {
    use metrics::{Key, Label};
    use metrics_util::{CompositeKey, debugging::DebuggingRecorder};

    use super::*;

    fn with_debugging_recorder(test: impl FnOnce(metrics_util::debugging::Snapshotter)) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || test(snapshotter));
    }

    #[test]
    fn dynamic_runtime_metrics_register_and_record() {
        with_debugging_recorder(|snapshotter| {
            register_dynamic_runtime_metrics();
            record_rebind(RebindOutcome::Success, Duration::from_millis(25));
            record_oauth_refresh(
                "oauth-upstream",
                OAuthRefreshOutcome::HttpError,
                Duration::from_millis(50),
            );
            set_oauth_refresh_lag("oauth-upstream", 12.5);
            record_wasm_cache_materialize(WasmCacheMaterializeOutcome::MissFetched);
            record_reconcile(ReconcileOutcome::Changed);
            record_notify_received("upstream");

            let metrics = snapshotter.snapshot().into_hashmap();
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Counter,
                Key::from_parts("cclb_rebind_total", vec![Label::new("outcome", "success")])
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Histogram,
                Key::from_name("cclb_rebind_duration_seconds")
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Counter,
                Key::from_parts(
                    "cclb_oauth_refresh_total",
                    vec![
                        Label::new("upstream", "oauth-upstream"),
                        Label::new("outcome", "http_error"),
                    ]
                )
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Histogram,
                Key::from_name("cclb_oauth_refresh_duration_seconds")
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Gauge,
                Key::from_parts(
                    "cclb_oauth_refresh_lag_seconds",
                    vec![Label::new("upstream", "oauth-upstream")]
                )
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Counter,
                Key::from_parts(
                    "cclb_wasm_cache_materialize_total",
                    vec![Label::new("outcome", "miss_fetched")]
                )
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Counter,
                Key::from_parts(
                    "cclb_reconcile_total",
                    vec![Label::new("outcome", "changed")]
                )
            )));
            assert!(metrics.contains_key(&CompositeKey::new(
                metrics_util::MetricKind::Counter,
                Key::from_parts(
                    "cclb_notify_received_total",
                    vec![Label::new("channel", "upstream")]
                )
            )));
        });
    }

    #[test]
    fn rebind_buckets_match_task_33_plan() {
        assert_eq!(
            REBIND_DURATION_BUCKETS,
            [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
        );
    }
}
