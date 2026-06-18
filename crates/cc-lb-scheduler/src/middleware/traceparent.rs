use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tower::{Layer, Service};
use tracing::{Instrument as _, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use super::lifecycle::SchedulerMetricsService;

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

    pub const fn with_scheduler_metrics(self) -> TraceparentMetricsLayer {
        TraceparentMetricsLayer
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
        let traceparent = payload.traceparent().unwrap_or_default();
        let span = tracing::info_span!(
            "scheduler.job",
            traceparent = traceparent,
            otel.kind = "consumer"
        );

        if let Some(parent) = parent_context(payload) {
            span.add_link(parent.span().span_context().clone());
            let _ = span.set_parent(parent);
        }

        span
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TraceparentMetricsLayer;

impl<S> Layer<S> for TraceparentMetricsLayer {
    type Service = TraceparentService<SchedulerMetricsService<S>>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceparentService {
            inner: SchedulerMetricsService::new(inner),
        }
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
#[path = "traceparent_tests.rs"]
mod traceparent_tests;
