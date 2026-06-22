use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use opentelemetry::global;
use opentelemetry::trace::{SpanId, SpanKind, TraceContextExt as _, TracerProvider as _};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use tower::{Layer, ServiceExt, service_fn};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::prelude::*;

use super::*;

const ZERO_TRACE_ID: &str = "00000000000000000000000000000000";
const INBOUND_TRACEPARENT: &str = "00-11111111111111111111111111111111-2222222222222222-01";

#[derive(Clone, Debug, Default)]
struct TestPayload {
    traceparent: Option<String>,
    observed: Arc<Mutex<Option<ObservedContext>>>,
}

impl TraceparentCarrier for TestPayload {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedContext {
    trace_id: String,
    span_id: String,
}

#[derive(Clone, Debug, Default)]
struct CapturingExporter {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl CapturingExporter {
    fn spans(&self) -> Vec<SpanData> {
        self.spans.lock().expect("spans lock").clone()
    }
}

impl SpanExporter for CapturingExporter {
    async fn export(&self, mut batch: Vec<SpanData>) -> OTelSdkResult {
        self.spans.lock().expect("spans lock").append(&mut batch);
        Ok(())
    }
}

#[test]
fn shared_modules_traceparent_round_trips_current_span_into_worker_layer() {
    let (provider, _exporter, subscriber) = test_subscriber();

    tracing::subscriber::with_default(subscriber, || {
        let layer = TraceparentLayer::new();
        let observed = Arc::new(Mutex::new(None));
        let mut payload = TestPayload {
            traceparent: None,
            observed: Arc::clone(&observed),
        };

        let producer_span = tracing::info_span!("shared-modules-producer");
        let producer_context = {
            let _entered = producer_span.enter();
            let context = current_context();
            layer.inject_current(&mut payload);
            context
        };

        let traceparent = payload.traceparent().expect("traceparent injected");
        assert!(traceparent.starts_with("00-"));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds");
        let service = layer.layer(service_fn(|payload: TestPayload| async move {
            *payload.observed.lock().expect("observed lock") = Some(current_context());
            Ok::<_, Infallible>(())
        }));

        runtime
            .block_on(service.oneshot(payload))
            .expect("handler succeeds");
        let consumer_context = observed
            .lock()
            .expect("observed lock")
            .clone()
            .expect("consumer observed context");

        assert_eq!(consumer_context.trace_id, producer_context.trace_id);
        assert_ne!(consumer_context.span_id, producer_context.span_id);
        assert_ne!(consumer_context.trace_id, ZERO_TRACE_ID);
    });

    drop(provider);
}

#[test]
fn traceparent_span_uses_consumer_kind_and_links_inbound_context() {
    let (provider, exporter, subscriber) = test_subscriber();

    tracing::subscriber::with_default(subscriber, || {
        let payload = TestPayload {
            traceparent: Some(INBOUND_TRACEPARENT.to_owned()),
            observed: Arc::new(Mutex::new(None)),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds");
        let service =
            TraceparentLayer::new().layer(service_fn(|_payload: TestPayload| async move {
                let span = tracing::info_span!("inner-handler");
                let _entered = span.enter();
                Ok::<_, Infallible>(current_context())
            }));

        let inner = runtime
            .block_on(service.oneshot(payload))
            .expect("handler succeeds");
        assert_eq!(inner.trace_id, "11111111111111111111111111111111");
    });

    drop(provider);
    let spans = exporter.spans();
    let scheduler = spans
        .iter()
        .find(|span| span.name == "scheduler.job")
        .expect("scheduler job span exported");
    let inner = spans
        .iter()
        .find(|span| span.name == "inner-handler")
        .expect("inner handler span exported");

    assert_eq!(scheduler.span_kind, SpanKind::Consumer);
    assert_eq!(
        scheduler.parent_span_id,
        SpanId::from_hex("2222222222222222").expect("span id parses")
    );
    assert!(scheduler.links.links.iter().any(|link| {
        format!("{:032x}", link.span_context.trace_id()) == "11111111111111111111111111111111"
    }));
    assert_eq!(
        scheduler
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "traceparent")
            .map(|attribute| attribute.value.to_string()),
        Some(INBOUND_TRACEPARENT.to_owned())
    );
    assert_eq!(
        format!("{:032x}", inner.span_context.trace_id()),
        "11111111111111111111111111111111"
    );
}

fn test_subscriber() -> (
    SdkTracerProvider,
    CapturingExporter,
    impl tracing::Subscriber + Send + Sync,
) {
    let exporter = CapturingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("cc-lb-scheduler-shared-modules");
    global::set_text_map_propagator(TraceContextPropagator::new());
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    (provider, exporter, subscriber)
}

fn current_context() -> ObservedContext {
    let context = Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    ObservedContext {
        trace_id: format!("{:032x}", span_context.trace_id()),
        span_id: format!("{:016x}", span_context.span_id()),
    }
}
