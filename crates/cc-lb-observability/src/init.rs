use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use metrics::Unit;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use thiserror::Error;
use tracing_subscriber::filter::ParseError;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::{EnvFilter, Registry};

use crate::cclb_metrics::{
    PROMETHEUS14_METRIC_DEFINITIONS, register_prometheus14_metrics,
    touch_prometheus14_metric_handles,
};
use crate::panic_hook::install_panic_hook;
use crate::redaction::{RedactingMakeWriter, RedactionLayer, RedactionPolicy};

type BoxedRegistryLayer = Box<dyn Layer<Registry> + Send + Sync>;

static PANIC_TOTAL: AtomicU64 = AtomicU64::new(0);

const REQUEST_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const PLUGIN_CALL_DURATION_BUCKETS: [f64; 11] = [
    0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
];
const SUBSCRIPTION_QUOTA_BATCH_BUCKETS: [f64; 9] =
    [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservabilityConfig {
    pub tracing_level: String,
    pub otlp_endpoint: Option<String>,
    pub prometheus_endpoint: Option<String>,
    pub log_redaction: bool,
    pub user_prompt_redaction: bool,
    pub hook_channel_capacity: usize,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            tracing_level: "info".to_owned(),
            otlp_endpoint: None,
            prometheus_endpoint: None,
            log_redaction: true,
            user_prompt_redaction: false,
            hook_channel_capacity: crate::DEFAULT_HOOK_CHANNEL_CAPACITY,
        }
    }
}

#[derive(Debug)]
pub struct TracingGuard {
    prometheus_handle: Option<PrometheusHandle>,
    tracer_provider: Option<SdkTracerProvider>,
}

impl TracingGuard {
    pub fn prometheus_handle(&self) -> Option<&PrometheusHandle> {
        self.prometheus_handle.as_ref()
    }

    pub fn flush_metrics(&self) {
        if let Some(handle) = &self.prometheus_handle {
            let _ = handle.render();
        }
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

#[derive(Debug, Error)]
pub enum InitError {
    #[error("invalid tracing filter: {source}")]
    TracingFilter { source: ParseError },
    #[error("failed to install tracing subscriber: {source}")]
    TracingSubscriber {
        #[from]
        source: tracing::subscriber::SetGlobalDefaultError,
    },
    #[error("invalid prometheus endpoint {endpoint:?}: {source}")]
    PrometheusEndpoint {
        endpoint: String,
        source: std::net::AddrParseError,
    },
    #[error("failed to install prometheus recorder: {message}")]
    Prometheus { message: String },
    #[error("failed to build OTLP exporter: {message}")]
    OtlpExporter { message: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricDefinition {
    pub name: &'static str,
    pub kind: MetricKind,
    pub description: &'static str,
}

const METRIC_DEFINITIONS: [MetricDefinition; 54] = [
    MetricDefinition {
        name: "cc_lb_requests_total",
        kind: MetricKind::Counter,
        description: "Total proxied requests by principal, upstream, model, and status.",
    },
    MetricDefinition {
        name: "cc_lb_request_duration_seconds",
        kind: MetricKind::Histogram,
        description: "End-to-end proxied request duration in seconds.",
    },
    MetricDefinition {
        name: "cc_lb_oauth_refresh_total",
        kind: MetricKind::Counter,
        description: "OAuth credential refresh attempts by principal, provider, and outcome.",
    },
    MetricDefinition {
        name: "cc_lb_quota_active_principals_total",
        kind: MetricKind::Gauge,
        description: "Number of principals with active quota windows.",
    },
    MetricDefinition {
        name: "cc_lb_quota_rejected_total",
        kind: MetricKind::Counter,
        description: "Quota rejections by principal and quota kind.",
    },
    MetricDefinition {
        name: "cc_lb_dropped_events_total",
        kind: MetricKind::Counter,
        description: "Observability events dropped by non-blocking bounded queues.",
    },
    MetricDefinition {
        name: "cc_lb_circuit_breaker_state",
        kind: MetricKind::Gauge,
        description: "Circuit breaker state by upstream: 0 closed, 1 half-open, 2 open.",
    },
    MetricDefinition {
        name: "cc_lb_panic_total",
        kind: MetricKind::Counter,
        description: "Total process panics observed by the tracing panic hook.",
    },
    MetricDefinition {
        name: "cc_lb_drain_in_progress",
        kind: MetricKind::Gauge,
        description: "Whether graceful drain is currently active.",
    },
    MetricDefinition {
        name: "cc_lb_drain_force_closed_total",
        kind: MetricKind::Counter,
        description: "Proxy request handlers still in flight when the drain deadline elapsed.",
    },
    MetricDefinition {
        name: "cc_lb_config_reload_total",
        kind: MetricKind::Counter,
        description: "Configuration reload attempts by outcome.",
    },
    MetricDefinition {
        name: "cc_lb_config_reload_failed_total",
        kind: MetricKind::Counter,
        description: "Configuration reload failures.",
    },
    MetricDefinition {
        name: "cc_lb_tls_reload_total",
        kind: MetricKind::Counter,
        description: "TLS certificate reload attempts by outcome.",
    },
    MetricDefinition {
        name: "cc_lb_sse_events_total",
        kind: MetricKind::Counter,
        description: "SSE events relayed by upstream and event type.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_call_duration_seconds",
        kind: MetricKind::Histogram,
        description: "Plugin hook call duration in seconds.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_memories_utilization_ratio",
        kind: MetricKind::Gauge,
        description: "Wasmtime memory pool utilization ratio.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_core_instances_utilization_ratio",
        kind: MetricKind::Gauge,
        description: "Wasmtime core instance pool utilization ratio.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_memories_used",
        kind: MetricKind::Gauge,
        description: "Wasmtime pooled memories currently in use.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_core_instances_used",
        kind: MetricKind::Gauge,
        description: "Wasmtime pooled core instances currently in use.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_memories_total",
        kind: MetricKind::Gauge,
        description: "Configured Wasmtime pooled memory capacity.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_core_instances_total",
        kind: MetricKind::Gauge,
        description: "Configured Wasmtime pooled core instance capacity.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_memory_max_pages",
        kind: MetricKind::Gauge,
        description: "Configured maximum Wasmtime memory pages per plugin call.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_memory_reservation_bytes",
        kind: MetricKind::Gauge,
        description: "Configured Wasmtime memory reservation bytes per pooled memory.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_memory_guard_bytes",
        kind: MetricKind::Gauge,
        description: "Configured Wasmtime memory guard bytes per pooled memory.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_virtual_reservation_bytes",
        kind: MetricKind::Gauge,
        description: "Computed Wasmtime virtual reservation bytes for the memory pool.",
    },
    MetricDefinition {
        name: "cc_lb_plugin_pool_saturation_total",
        kind: MetricKind::Counter,
        description: "Wasmtime pooling allocator saturation events by resource.",
    },
    MetricDefinition {
        name: "cc_lb_tokens_total",
        kind: MetricKind::Counter,
        description: "Tokens observed from upstream responses by principal, upstream, model, and direction (input/output).",
    },
    MetricDefinition {
        name: "cc_lb_virtual_cost_usd_total",
        kind: MetricKind::Counter,
        description: "Virtual cost in micro-USD attributed to proxied responses by principal, upstream, and model.",
    },
    MetricDefinition {
        name: "cc_lb_stream_terminations_total",
        kind: MetricKind::Counter,
        description: "Response stream terminations by bounded outcome and cause.",
    },
    PROMETHEUS14_METRIC_DEFINITIONS[0],
    PROMETHEUS14_METRIC_DEFINITIONS[1],
    PROMETHEUS14_METRIC_DEFINITIONS[2],
    PROMETHEUS14_METRIC_DEFINITIONS[3],
    PROMETHEUS14_METRIC_DEFINITIONS[4],
    PROMETHEUS14_METRIC_DEFINITIONS[5],
    PROMETHEUS14_METRIC_DEFINITIONS[6],
    PROMETHEUS14_METRIC_DEFINITIONS[7],
    PROMETHEUS14_METRIC_DEFINITIONS[8],
    PROMETHEUS14_METRIC_DEFINITIONS[9],
    PROMETHEUS14_METRIC_DEFINITIONS[10],
    PROMETHEUS14_METRIC_DEFINITIONS[11],
    PROMETHEUS14_METRIC_DEFINITIONS[12],
    PROMETHEUS14_METRIC_DEFINITIONS[13],
    PROMETHEUS14_METRIC_DEFINITIONS[14],
    PROMETHEUS14_METRIC_DEFINITIONS[15],
    PROMETHEUS14_METRIC_DEFINITIONS[16],
    PROMETHEUS14_METRIC_DEFINITIONS[17],
    MetricDefinition {
        name: "cc_lb_prompt_cache_analysis_duration_seconds",
        kind: MetricKind::Histogram,
        description: "Prompt-cache analysis duration in seconds.",
    },
    MetricDefinition {
        name: "cc_lb_prompt_cache_token_count_cache_total",
        kind: MetricKind::Counter,
        description: "Prompt-cache token count cache lookups by result.",
    },
    MetricDefinition {
        name: "cc_lb_prompt_cache_tokenizer_inflight",
        kind: MetricKind::Gauge,
        description: "Current number of in-flight prompt-cache tokenizer operations.",
    },
    MetricDefinition {
        name: "cc_lb_prompt_cache_tokenized_bytes_total",
        kind: MetricKind::Counter,
        description: "Total bytes processed by the prompt-cache tokenizer.",
    },
    MetricDefinition {
        name: "cc_lb_prompt_cache_tokenized_tokens_total",
        kind: MetricKind::Counter,
        description: "Total tokens produced by the prompt-cache tokenizer.",
    },
    MetricDefinition {
        name: "cc_lb_prompt_cache_tokenizer_fallback_prefixes_total",
        kind: MetricKind::Counter,
        description: "Total prompt-cache prefixes requiring tokenizer threshold fallback.",
    },
    MetricDefinition {
        name: "cc_lb_prompt_cache_analysis_worker_failed_total",
        kind: MetricKind::Counter,
        description: "Total prompt-cache analysis worker failures recovered by exact fallback.",
    },
];

pub fn init(cfg: &ObservabilityConfig) -> Result<TracingGuard, InitError> {
    let prometheus_handle = install_prometheus(cfg)?;
    // Register describes + touch counters AFTER installing the recorder so
    // zero-value touches (`.absolute(0)`) actually materialize on the
    // Prometheus scrape path.
    register_metrics();

    let policy = RedactionPolicy::new(cfg.user_prompt_redaction);
    install_panic_hook(policy);

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.tracing_level))
        .map_err(|source| InitError::TracingFilter { source })?;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(RedactingMakeWriter::new(std::io::stdout, policy));

    let mut layers: Vec<BoxedRegistryLayer> =
        vec![RedactionLayer::new(policy).boxed(), fmt_layer.boxed()];

    let tracer_provider = if let Some(endpoint) = cfg.otlp_endpoint.as_deref() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|source| InitError::OtlpExporter {
                message: source.to_string(),
            })?;
        let provider = SdkTracerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name("cc-lb")
                    .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
                    .build(),
            )
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer("cc-lb");
        layers.push(tracing_opentelemetry::layer().with_tracer(tracer).boxed());
        Some(provider)
    } else {
        None
    };

    let subscriber = filtered_subscriber(layers, env_filter);
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(TracingGuard {
        prometheus_handle,
        tracer_provider,
    })
}

fn filtered_subscriber(
    layers: Vec<BoxedRegistryLayer>,
    env_filter: EnvFilter,
) -> impl tracing::Subscriber + Send + Sync {
    // Keep the global filter outside the dynamic layer collection. A `Vec<Layer>`
    // combines callsite interest by taking the most permissive result, so placing
    // `EnvFilter` inside the vector lets an unfiltered output layer enable events
    // that the configured filter rejected.
    Registry::default().with(layers).with(env_filter)
}

pub fn metric_definitions() -> &'static [MetricDefinition] {
    &METRIC_DEFINITIONS
}

pub fn register_metrics() {
    metrics::describe_counter!(
        "cc_lb_requests_total",
        Unit::Count,
        "Total proxied requests by principal, upstream, model, and status."
    );
    metrics::describe_histogram!(
        "cc_lb_request_duration_seconds",
        Unit::Seconds,
        "End-to-end proxied request duration in seconds."
    );
    metrics::describe_counter!(
        "cc_lb_oauth_refresh_total",
        Unit::Count,
        "OAuth credential refresh attempts by principal, provider, and outcome."
    );
    metrics::describe_gauge!(
        "cc_lb_quota_active_principals_total",
        Unit::Count,
        "Number of principals with active quota windows."
    );
    metrics::describe_counter!(
        "cc_lb_quota_rejected_total",
        Unit::Count,
        "Quota rejections by principal and quota kind."
    );
    metrics::describe_counter!(
        "cc_lb_dropped_events_total",
        Unit::Count,
        "Observability events dropped by non-blocking bounded queues."
    );
    metrics::describe_gauge!(
        "cc_lb_circuit_breaker_state",
        Unit::Count,
        "Circuit breaker state by upstream: 0 closed, 1 half-open, 2 open."
    );
    metrics::describe_counter!(
        "cc_lb_panic_total",
        Unit::Count,
        "Total process panics observed by the tracing panic hook."
    );
    metrics::describe_gauge!(
        "cc_lb_drain_in_progress",
        Unit::Count,
        "Whether graceful drain is currently active."
    );
    metrics::describe_counter!(
        "cc_lb_drain_force_closed_total",
        Unit::Count,
        "Proxy request handlers still in flight when the drain deadline elapsed."
    );
    metrics::describe_counter!(
        "cc_lb_config_reload_total",
        Unit::Count,
        "Configuration reload attempts by outcome."
    );
    metrics::describe_counter!(
        "cc_lb_config_reload_failed_total",
        Unit::Count,
        "Configuration reload failures."
    );
    metrics::describe_counter!(
        "cc_lb_tls_reload_total",
        Unit::Count,
        "TLS certificate reload attempts by outcome."
    );
    metrics::describe_counter!(
        "cc_lb_lifecycle_events_total",
        Unit::Count,
        "Total lifecycle events emitted by event kind."
    );
    metrics::describe_counter!(
        "cc_lb_sse_events_total",
        Unit::Count,
        "SSE events relayed by upstream and event type."
    );
    metrics::describe_histogram!(
        "cc_lb_plugin_call_duration_seconds",
        Unit::Seconds,
        "Plugin hook call duration in seconds."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_memories_utilization_ratio",
        Unit::Count,
        "Wasmtime memory pool utilization ratio."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_core_instances_utilization_ratio",
        Unit::Count,
        "Wasmtime core instance pool utilization ratio."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_memories_used",
        Unit::Count,
        "Wasmtime pooled memories currently in use."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_core_instances_used",
        Unit::Count,
        "Wasmtime pooled core instances currently in use."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_memories_total",
        Unit::Count,
        "Configured Wasmtime pooled memory capacity."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_core_instances_total",
        Unit::Count,
        "Configured Wasmtime pooled core instance capacity."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_memory_max_pages",
        Unit::Count,
        "Configured maximum Wasmtime memory pages per plugin call."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_memory_reservation_bytes",
        Unit::Bytes,
        "Configured Wasmtime memory reservation bytes per pooled memory."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_memory_guard_bytes",
        Unit::Bytes,
        "Configured Wasmtime memory guard bytes per pooled memory."
    );
    metrics::describe_gauge!(
        "cc_lb_plugin_pool_virtual_reservation_bytes",
        Unit::Bytes,
        "Computed Wasmtime virtual reservation bytes for the memory pool."
    );
    metrics::describe_counter!(
        "cc_lb_plugin_pool_saturation_total",
        Unit::Count,
        "Wasmtime pooling allocator saturation events by resource."
    );
    metrics::describe_counter!(
        "cc_lb_tokens_total",
        Unit::Count,
        "Tokens observed from upstream responses by principal, upstream, model, and direction (input/output)."
    );
    metrics::describe_counter!(
        "cc_lb_virtual_cost_usd_total",
        Unit::Count,
        "Virtual cost in micro-USD attributed to proxied responses by principal, upstream, and model."
    );
    metrics::describe_counter!(
        "cc_lb_stream_terminations_total",
        Unit::Count,
        "Response stream terminations by bounded outcome and cause."
    );
    metrics::describe_histogram!(
        "cc_lb_prompt_cache_analysis_duration_seconds",
        Unit::Seconds,
        "Prompt-cache analysis duration in seconds."
    );
    metrics::describe_counter!(
        "cc_lb_prompt_cache_token_count_cache_total",
        Unit::Count,
        "Prompt-cache token count cache lookups by result."
    );
    metrics::describe_gauge!(
        "cc_lb_prompt_cache_tokenizer_inflight",
        Unit::Count,
        "Current number of in-flight prompt-cache tokenizer operations."
    );
    metrics::describe_counter!(
        "cc_lb_prompt_cache_tokenized_bytes_total",
        Unit::Bytes,
        "Total bytes processed by the prompt-cache tokenizer."
    );
    metrics::describe_counter!(
        "cc_lb_prompt_cache_tokenized_tokens_total",
        Unit::Count,
        "Total tokens produced by the prompt-cache tokenizer."
    );
    metrics::describe_counter!(
        "cc_lb_prompt_cache_tokenizer_fallback_prefixes_total",
        Unit::Count,
        "Total prompt-cache prefixes requiring tokenizer threshold fallback."
    );
    metrics::describe_counter!(
        "cc_lb_prompt_cache_analysis_worker_failed_total",
        Unit::Count,
        "Total prompt-cache analysis worker failures recovered by exact fallback."
    );
    metrics::describe_counter!(
        "cc_lb_limit_reservation_ttl_evicted_total",
        Unit::Count,
        "Limit reservations refunded by the TTL sweeper after exceeding their live-request TTL."
    );
    register_prometheus14_metrics();

    touch_metrics();
}

pub(crate) fn increment_panic_total() {
    PANIC_TOTAL.fetch_add(1, Ordering::Relaxed);
    metrics::counter!("cc_lb_panic_total").increment(1);
}

pub fn panic_total() -> u64 {
    PANIC_TOTAL.load(Ordering::Relaxed)
}

fn install_prometheus(cfg: &ObservabilityConfig) -> Result<Option<PrometheusHandle>, InitError> {
    let builder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("cc_lb_request_duration_seconds".to_owned()),
            &REQUEST_DURATION_BUCKETS,
        )
        .map_err(|source| InitError::Prometheus {
            message: source.to_string(),
        })?
        .set_buckets_for_metric(
            Matcher::Full("cc_lb_plugin_call_duration_seconds".to_owned()),
            &PLUGIN_CALL_DURATION_BUCKETS,
        )
        .map_err(|source| InitError::Prometheus {
            message: source.to_string(),
        })?
        .set_buckets_for_metric(
            Matcher::Full("subscription_quota_writer_batch_size".to_owned()),
            &SUBSCRIPTION_QUOTA_BATCH_BUCKETS,
        )
        .map_err(|source| InitError::Prometheus {
            message: source.to_string(),
        })?;

    if let Some(endpoint) = cfg.prometheus_endpoint.as_deref() {
        let addr = parse_socket_addr(endpoint)?;
        builder
            .with_http_listener(addr)
            .install()
            .map_err(|source| InitError::Prometheus {
                message: source.to_string(),
            })?;
        Ok(None)
    } else {
        builder
            .install_recorder()
            .map(Some)
            .map_err(|source| InitError::Prometheus {
                message: source.to_string(),
            })
    }
}

fn parse_socket_addr(endpoint: &str) -> Result<SocketAddr, InitError> {
    endpoint
        .parse::<SocketAddr>()
        .map_err(|source| InitError::PrometheusEndpoint {
            endpoint: endpoint.to_owned(),
            source,
        })
}

fn touch_metrics() {
    metrics::counter!(
        "cc_lb_requests_total",
        "principal" => "unknown",
        "upstream" => "unknown",
        "model" => "unknown",
        "status" => "unknown"
    )
    .increment(0);
    metrics::histogram!(
        "cc_lb_request_duration_seconds",
        "principal" => "unknown",
        "upstream" => "unknown",
        "model" => "unknown"
    )
    .record(0.0);
    metrics::counter!(
        "cc_lb_oauth_refresh_total",
        "principal" => "unknown",
        "provider" => "unknown",
        "outcome" => "unknown"
    )
    .increment(0);
    metrics::gauge!("cc_lb_quota_active_principals_total").set(0.0);
    metrics::counter!(
        "cc_lb_quota_rejected_total",
        "principal" => "unknown",
        "kind" => "unknown"
    )
    .increment(0);
    metrics::counter!("cc_lb_dropped_events_total", "reason" => "none").increment(0);
    metrics::gauge!("cc_lb_circuit_breaker_state", "upstream" => "unknown").set(0.0);
    metrics::counter!("cc_lb_panic_total").increment(0);
    metrics::gauge!("cc_lb_drain_in_progress").set(0.0);
    metrics::counter!("cc_lb_drain_force_closed_total").increment(0);
    metrics::counter!("cc_lb_config_reload_total", "outcome" => "unknown").increment(0);
    metrics::counter!("cc_lb_config_reload_failed_total").increment(0);
    metrics::counter!("cc_lb_tls_reload_total", "outcome" => "success").increment(0);
    metrics::counter!("cc_lb_tls_reload_total", "outcome" => "failure").increment(0);
    metrics::counter!("cc_lb_limit_reservation_ttl_evicted_total").absolute(0);
    metrics::counter!(
        "cc_lb_sse_events_total",
        "upstream" => "unknown",
        "event_type" => "unknown"
    )
    .increment(0);
    metrics::histogram!(
        "cc_lb_plugin_call_duration_seconds",
        "plugin" => "unknown",
        "hook" => "unknown"
    )
    .record(0.0);
    metrics::gauge!("cc_lb_plugin_pool_memories_utilization_ratio").set(0.0);
    metrics::gauge!("cc_lb_plugin_pool_core_instances_utilization_ratio").set(0.0);
    metrics::gauge!("cc_lb_plugin_pool_memories_used").set(0.0);
    metrics::gauge!("cc_lb_plugin_pool_core_instances_used").set(0.0);
    metrics::gauge!("cc_lb_plugin_pool_memories_total").set(0.0);
    metrics::gauge!("cc_lb_plugin_pool_core_instances_total").set(0.0);
    metrics::gauge!("cc_lb_plugin_memory_max_pages").set(0.0);
    metrics::gauge!("cc_lb_plugin_memory_reservation_bytes").set(0.0);
    metrics::gauge!("cc_lb_plugin_memory_guard_bytes").set(0.0);
    metrics::gauge!("cc_lb_plugin_pool_virtual_reservation_bytes").set(0.0);
    metrics::counter!("cc_lb_plugin_pool_saturation_total", "resource" => "unknown").increment(0);
    metrics::histogram!("subscription_quota_writer_batch_size").record(0.0);
    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => "unknown",
        "upstream" => "unknown",
        "model" => "unknown",
        "direction" => "input"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => "unknown",
        "upstream" => "unknown",
        "model" => "unknown",
        "direction" => "output"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_virtual_cost_usd_total",
        "principal" => "unknown",
        "upstream" => "unknown",
        "model" => "unknown"
    )
    .increment(0);
    metrics::counter!(
        "cc_lb_stream_terminations_total",
        "outcome" => "completed",
        "cause" => "none"
    )
    .increment(0);
    for stage in ["queue", "tokenize", "total"] {
        metrics::histogram!(
            "cc_lb_prompt_cache_analysis_duration_seconds",
            "stage" => stage
        )
        .record(0.0);
    }
    for result in ["hit", "miss", "coalesced"] {
        metrics::counter!(
            "cc_lb_prompt_cache_token_count_cache_total",
            "result" => result
        )
        .increment(0);
    }
    metrics::gauge!("cc_lb_prompt_cache_tokenizer_inflight").set(0.0);
    metrics::counter!("cc_lb_prompt_cache_tokenized_bytes_total").increment(0);
    metrics::counter!("cc_lb_prompt_cache_tokenized_tokens_total").increment(0);
    metrics::counter!("cc_lb_prompt_cache_tokenizer_fallback_prefixes_total").increment(0);
    metrics::counter!("cc_lb_prompt_cache_analysis_worker_failed_total").increment(0);
    touch_prometheus14_metric_handles();
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::Context;

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingLayer {
        levels: Arc<Mutex<Vec<Level>>>,
    }

    impl<S> Layer<S> for RecordingLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            self.levels
                .lock()
                .expect("recorded levels lock poisoned")
                .push(*event.metadata().level());
        }
    }

    #[test]
    fn configured_filter_applies_to_dynamic_layers() {
        let recording = RecordingLayer::default();
        let levels = Arc::clone(&recording.levels);
        let subscriber = filtered_subscriber(vec![recording.boxed()], EnvFilter::new("info"));

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("filtered debug event");
            tracing::info!("retained info event");
        });

        assert_eq!(
            *levels.lock().expect("recorded levels lock poisoned"),
            vec![Level::INFO]
        );
    }
}
