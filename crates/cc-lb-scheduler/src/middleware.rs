//! Middleware for job processing pipelines.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tower::{Layer, Service};
use tracing::{Instrument as _, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

type TraceparentFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub trait TraceparentCarrier {
    fn traceparent(&self) -> Option<&str>;
    fn set_traceparent(&mut self, traceparent: Option<String>);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TraceparentLayer;

impl TraceparentLayer {
    pub const fn new() -> Self {
        Self
    }

    pub fn inject_current<P>(&self, payload: &mut P) -> Option<String>
    where
        P: TraceparentCarrier,
    {
        let traceparent = current_traceparent();
        payload.set_traceparent(traceparent.clone());
        traceparent
    }

    pub fn span_for<P>(&self, payload: &P) -> Span
    where
        P: TraceparentCarrier,
    {
        let span = tracing::info_span!(
            "scheduler.job",
            traceparent = payload.traceparent().unwrap_or_default()
        );

        if let Some(parent) = parent_context(payload) {
            let _ = span.set_parent(parent);
        }

        span
    }
}

impl<S> Layer<S> for TraceparentLayer {
    type Service = TraceparentService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceparentService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct TraceparentService<S> {
    inner: S,
}

impl<S, P> Service<P> for TraceparentService<S>
where
    P: TraceparentCarrier + Send + 'static,
    S: Service<P>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = TraceparentFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, payload: P) -> Self::Future {
        let span = TraceparentLayer::new().span_for(&payload);
        let future = self.inner.call(payload);

        Box::pin(async move { future.instrument(span).await })
    }
}

pub fn current_traceparent() -> Option<String> {
    let mut carrier = TraceparentHeader::default();
    let current = Span::current().context();
    TraceContextPropagator::new().inject_context(&current, &mut carrier);
    carrier.traceparent
}

fn parent_context<P>(payload: &P) -> Option<opentelemetry::Context>
where
    P: TraceparentCarrier,
{
    let traceparent = payload.traceparent()?;
    let carrier = TraceparentHeader {
        traceparent: Some(traceparent.to_owned()),
    };
    let parent = TraceContextPropagator::new().extract(&carrier);

    if parent.span().span_context().is_valid() {
        Some(parent)
    } else {
        None
    }
}

#[derive(Debug, Default)]
struct TraceparentHeader {
    traceparent: Option<String>,
}

impl Extractor for TraceparentHeader {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            return self.traceparent.as_deref();
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        if self.traceparent.is_some() {
            vec!["traceparent"]
        } else {
            Vec::new()
        }
    }
}

impl Injector for TraceparentHeader {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case("traceparent") {
            self.traceparent = Some(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex, Once};

    use opentelemetry::global;
    use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tower::{Layer, ServiceExt, service_fn};
    use tracing::Span;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;

    use super::*;

    const ZERO_TRACE_ID: &str = "00000000000000000000000000000000";

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

    #[tokio::test]
    async fn shared_modules_traceparent_round_trips_current_span_into_worker_layer() {
        init_otel_for_tests();

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

        let service = layer.layer(service_fn(|payload: TestPayload| async move {
            *payload.observed.lock().expect("observed lock") = Some(current_context());
            Ok::<_, Infallible>(())
        }));

        service.oneshot(payload).await.expect("handler succeeds");
        let consumer_context = observed
            .lock()
            .expect("observed lock")
            .clone()
            .expect("consumer observed context");

        assert_eq!(consumer_context.trace_id, producer_context.trace_id);
        assert_ne!(consumer_context.span_id, producer_context.span_id);
        assert_ne!(consumer_context.trace_id, ZERO_TRACE_ID);
    }

    fn init_otel_for_tests() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let tracer_provider = SdkTracerProvider::builder().build();
            let tracer = tracer_provider.tracer("cc-lb-scheduler-shared-modules");
            global::set_tracer_provider(tracer_provider);
            global::set_text_map_propagator(TraceContextPropagator::new());

            let subscriber = tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(tracer));
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
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
}
