use std::time::Duration;

use cc_lb_plugin_api::{ObservabilityHook, ObserveEvent};
use http::{Request, Response};
use tower_http::trace::{
    DefaultMakeSpan, HttpMakeClassifier, OnBodyChunk, OnRequest, OnResponse, TraceLayer,
};
use tracing::Span;

pub type ObservabilityTraceLayer<H> = TraceLayer<
    HttpMakeClassifier,
    DefaultMakeSpan,
    ObserveOnRequest<H>,
    ObserveOnResponse<H>,
    ObserveOnBodyChunk<H>,
>;

pub fn trace_layer<H>(hook: H) -> ObservabilityTraceLayer<H>
where
    H: ObservabilityHook + Clone,
{
    TraceLayer::new_for_http()
        .on_request(ObserveOnRequest { hook: hook.clone() })
        .on_response(ObserveOnResponse { hook: hook.clone() })
        .on_body_chunk(ObserveOnBodyChunk {
            hook,
            next_batch_index: 0,
        })
}

#[derive(Clone, Debug)]
pub struct ObserveOnRequest<H> {
    hook: H,
}

impl<H, B> OnRequest<B> for ObserveOnRequest<H>
where
    H: ObservabilityHook,
{
    fn on_request(&mut self, request: &Request<B>, _span: &Span) {
        let request_id = header_value(request, "request-id")
            .or_else(|| header_value(request, "x-request-id"))
            .unwrap_or_default();
        let user_agent = header_value(request, "user-agent");

        tracing::info!(
            target: "cc_lb_observability::http",
            method = %request.method(),
            path = request.uri().path(),
            query_present = request.uri().query().is_some(),
            request_id = request_id.as_str(),
            downstream_user_agent = user_agent.as_deref().unwrap_or(""),
            "request_started"
        );

        let _ = self.hook.observe(ObserveEvent::RequestStarted {
            request_id,
            downstream_user_agent: user_agent,
        });
    }
}

#[derive(Clone, Debug)]
pub struct ObserveOnResponse<H> {
    hook: H,
}

impl<H, B> OnResponse<B> for ObserveOnResponse<H>
where
    H: ObservabilityHook,
{
    fn on_response(self, response: &Response<B>, latency: Duration, _span: &Span) {
        let status = response.status();
        let duration_seconds = latency.as_secs_f64();
        let status_label = status.as_u16().to_string();

        metrics::counter!(
            "cc_lb_requests_total",
            "principal" => "unknown",
            "upstream" => "unknown",
            "model" => "unknown",
            "status" => status_label
        )
        .increment(1);
        metrics::histogram!(
            "cc_lb_request_duration_seconds",
            "principal" => "unknown",
            "upstream" => "unknown",
            "model" => "unknown"
        )
        .record(duration_seconds);

        tracing::info!(
            target: "cc_lb_observability::http",
            status = status.as_u16(),
            latency_ms = latency.as_millis(),
            "response_finished"
        );

        let _ = self.hook.observe(ObserveEvent::RequestFinished {
            status,
            input_tokens: None,
            output_tokens: None,
            duration_ms: latency.as_millis().try_into().unwrap_or(u64::MAX),
        });
    }
}

#[derive(Clone, Debug)]
pub struct ObserveOnBodyChunk<H> {
    hook: H,
    next_batch_index: u64,
}

impl<H, B> OnBodyChunk<B> for ObserveOnBodyChunk<H>
where
    H: ObservabilityHook,
    B: AsRef<[u8]>,
{
    fn on_body_chunk(&mut self, chunk: &B, latency: Duration, _span: &Span) {
        let chunk_bytes = chunk.as_ref().len();
        let batch_index = self.next_batch_index;
        self.next_batch_index = self.next_batch_index.saturating_add(1);

        metrics::counter!(
            "cc_lb_sse_events_total",
            "upstream" => "unknown",
            "event_type" => "body_chunk"
        )
        .increment(1);

        tracing::debug!(
            target: "cc_lb_observability::http",
            batch_index,
            chunk_bytes,
            latency_ms = latency.as_millis(),
            "response_body_chunk"
        );

        let _ = self.hook.observe(ObserveEvent::Chunk {
            batch_index,
            event_count: 1,
            total_bytes: chunk_bytes,
        });
    }
}

fn header_value<B>(request: &Request<B>, name: &'static str) -> Option<String> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
