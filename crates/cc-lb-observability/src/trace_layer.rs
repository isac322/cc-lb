use std::time::Duration;

use http::{Request, Response};
use tower_http::trace::{
    HttpMakeClassifier, MakeSpan, OnBodyChunk, OnRequest, OnResponse, TraceLayer,
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

pub type ObservabilityTraceLayer = TraceLayer<
    HttpMakeClassifier,
    ProxyMakeSpan,
    ProxyOnRequest,
    ProxyOnResponse,
    ProxyOnBodyChunk,
>;

pub fn trace_layer() -> ObservabilityTraceLayer {
    TraceLayer::new_for_http()
        .make_span_with(ProxyMakeSpan::default())
        .on_request(ProxyOnRequest)
        .on_response(ProxyOnResponse)
        .on_body_chunk(ProxyOnBodyChunk)
}

pub type RouteTemplateFn = fn(&str) -> Option<&'static str>;

#[derive(Clone, Copy, Debug, Default)]
pub struct ProxyMakeSpan {
    route_template: Option<RouteTemplateFn>,
}

impl ProxyMakeSpan {
    pub fn with_route_template(route_template: RouteTemplateFn) -> Self {
        Self {
            route_template: Some(route_template),
        }
    }
}

impl<B> MakeSpan<B> for ProxyMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        let span = tracing::info_span!(
            "proxy.request",
            otel.name = tracing::field::Empty,
            otel.kind = "server",
            http.request.method = %request.method(),
            http.route = tracing::field::Empty,
            url.path = request.uri().path(),
            http.response.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            cc_lb.request.id = tracing::field::Empty,
        );
        match self
            .route_template
            .and_then(|template| template(request.uri().path()))
        {
            Some(route) => {
                span.record("http.route", route);
                span.record(
                    "otel.name",
                    format!("{} {route}", request.method()).as_str(),
                );
            }
            None => {
                span.record("otel.name", request.method().as_str());
            }
        }
        if let Some(parent) = crate::parent_context_from_headers(request.headers()) {
            let _ = span.set_parent(parent);
        }
        span
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProxyOnRequest;

impl<B> OnRequest<B> for ProxyOnRequest {
    fn on_request(&mut self, request: &Request<B>, span: &Span) {
        let request_id = header_value(request, "request-id")
            .or_else(|| header_value(request, "x-request-id"))
            .unwrap_or("");
        let user_agent = header_value(request, "user-agent").unwrap_or("");
        if !request_id.is_empty() {
            span.record("cc_lb.request.id", request_id);
        }

        tracing::info!(
            target: "cc_lb_observability::http",
            method = %request.method(),
            path = request.uri().path(),
            query_present = request.uri().query().is_some(),
            request_id,
            downstream_user_agent = user_agent,
            "request_started"
        );
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProxyOnResponse;

impl<B> OnResponse<B> for ProxyOnResponse {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        let status = response.status();
        span.record("http.response.status_code", u64::from(status.as_u16()));
        if status.is_server_error() {
            span.record("otel.status_code", "ERROR");
        }
        let duration_seconds = latency.as_secs_f64();
        let status_label = status.as_u16().to_string();

        // Observe proxy response headers, including local responses inside this layer.
        // Drain rejections bypass this hook before authentication and are not counted by
        // the started/header or terminal lifecycle metrics.
        // This is not terminal completion of a response.
        metrics::counter!(
            "cc_lb_requests_started_total",
            "principal" => "unknown",
            "upstream" => "unknown",
            "model" => "unknown",
            "status" => status_label
        )
        .increment(1);
        metrics::histogram!(
            "cc_lb_request_headers_duration_seconds",
            "principal" => "unknown",
            "upstream" => "unknown",
            "model" => "unknown"
        )
        .record(duration_seconds);

        tracing::info!(
            target: "cc_lb_observability::http",
            status = status.as_u16(),
            latency_ms = latency.as_millis(),
            "response_headers_started"
        );
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProxyOnBodyChunk;

impl<B> OnBodyChunk<B> for ProxyOnBodyChunk
where
    B: AsRef<[u8]>,
{
    fn on_body_chunk(&mut self, chunk: &B, latency: Duration, _span: &Span) {
        let chunk_bytes = chunk.as_ref().len();

        // Transport body chunks are not SSE events and are intentionally observed
        // without creating per-chunk child spans or parsing the chunk contents.
        // Provider attribution is unavailable here, so upstream is always "unknown".
        metrics::counter!(
            "cc_lb_response_body_chunks_total",
            "upstream" => "unknown"
        )
        .increment(1);

        tracing::debug!(
            target: "cc_lb_observability::http",
            chunk_bytes,
            latency_ms = latency.as_millis(),
            "response_transport_body_chunk"
        );
    }
}

fn header_value<'a, B>(request: &'a Request<B>, name: &'static str) -> Option<&'a str> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}
