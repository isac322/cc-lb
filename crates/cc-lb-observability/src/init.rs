use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use metrics::Unit;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
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

static PANIC_TOTAL: AtomicU64 = AtomicU64::new(0);

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
    _tracer_provider: Option<SdkTracerProvider>,
}

impl TracingGuard {
    pub fn prometheus_handle(&self) -> Option<&PrometheusHandle> {
        self.prometheus_handle.as_ref()
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

const METRIC_DEFINITIONS: [MetricDefinition; 31] = [
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
        description: "In-flight proxy requests force-closed after the drain deadline.",
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
        name: "cc_lb_extism_call_duration_seconds",
        kind: MetricKind::Histogram,
        description: "Extism plugin hook call duration in seconds.",
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
];

pub fn init(cfg: &ObservabilityConfig) -> Result<TracingGuard, InitError> {
    register_metrics();

    let prometheus_handle = install_prometheus(cfg)?;
    let policy = RedactionPolicy::new(cfg.user_prompt_redaction);
    install_panic_hook(policy);

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.tracing_level))
        .map_err(|source| InitError::TracingFilter { source })?;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(RedactingMakeWriter::new(std::io::stdout, policy));

    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
        env_filter.boxed(),
        RedactionLayer::new(policy).boxed(),
        fmt_layer.boxed(),
    ];

    let tracer_provider = if let Some(endpoint) = cfg.otlp_endpoint.as_deref() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|source| InitError::OtlpExporter {
                message: source.to_string(),
            })?;
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer("cc-lb");
        layers.push(tracing_opentelemetry::layer().with_tracer(tracer).boxed());
        Some(provider)
    } else {
        None
    };

    let subscriber = Registry::default().with(layers);
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(TracingGuard {
        prometheus_handle,
        _tracer_provider: tracer_provider,
    })
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
        "In-flight proxy requests force-closed after the drain deadline."
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
        "cc_lb_sse_events_total",
        Unit::Count,
        "SSE events relayed by upstream and event type."
    );
    metrics::describe_histogram!(
        "cc_lb_extism_call_duration_seconds",
        Unit::Seconds,
        "Extism plugin hook call duration in seconds."
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
    let builder = PrometheusBuilder::new();

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
    metrics::counter!(
        "cc_lb_sse_events_total",
        "upstream" => "unknown",
        "event_type" => "unknown"
    )
    .increment(0);
    metrics::histogram!(
        "cc_lb_extism_call_duration_seconds",
        "plugin" => "unknown",
        "hook" => "unknown"
    )
    .record(0.0);
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
    touch_prometheus14_metric_handles();
}
