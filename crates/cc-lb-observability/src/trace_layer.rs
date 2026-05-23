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
            "status" => status_label.clone()
        )
        .increment(1);
        metrics::histogram!(
            "cc_lb_request_duration_seconds",
            "principal" => "unknown",
            "upstream" => "unknown",
            "model" => "unknown",
            "status" => status_label
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use cc_lb_plugin_api::{ObservabilityError, ObserveEvent};
    use http::StatusCode;
    use tower_http::trace::{OnBodyChunk as _, OnRequest as _, OnResponse as _};

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingHook {
        events: Arc<Mutex<Vec<ObserveEvent>>>,
    }

    impl RecordingHook {
        fn events(&self) -> Vec<ObserveEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl ObservabilityHook for RecordingHook {
        fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[test]
    fn request_observer_prefers_request_id_and_records_user_agent() {
        let hook = RecordingHook::default();
        let mut observer = ObserveOnRequest { hook: hook.clone() };
        let request = Request::builder()
            .method("POST")
            .uri("/v1/messages?stream=true")
            .header("request-id", "primary")
            .header("x-request-id", "fallback")
            .header("user-agent", "real-client")
            .body(())
            .unwrap();

        observer.on_request(&request, &Span::none());

        assert_eq!(
            hook.events(),
            vec![ObserveEvent::RequestStarted {
                request_id: "primary".to_owned(),
                downstream_user_agent: Some("real-client".to_owned()),
            }]
        );
    }

    #[test]
    fn request_observer_falls_back_to_x_request_id() {
        let hook = RecordingHook::default();
        let mut observer = ObserveOnRequest { hook: hook.clone() };
        let request = Request::builder()
            .header("x-request-id", "fallback")
            .body(())
            .unwrap();

        observer.on_request(&request, &Span::none());

        assert_eq!(
            hook.events(),
            vec![ObserveEvent::RequestStarted {
                request_id: "fallback".to_owned(),
                downstream_user_agent: None,
            }]
        );
    }

    #[test]
    fn response_observer_records_status_and_latency() {
        let hook = RecordingHook::default();
        let observer = ObserveOnResponse { hook: hook.clone() };
        let response = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(())
            .unwrap();

        observer.on_response(&response, Duration::from_millis(123), &Span::none());

        assert_eq!(
            hook.events(),
            vec![ObserveEvent::RequestFinished {
                status: StatusCode::TOO_MANY_REQUESTS,
                input_tokens: None,
                output_tokens: None,
                duration_ms: 123,
            }]
        );
    }

    #[test]
    fn response_observer_saturates_large_latency() {
        let hook = RecordingHook::default();
        let observer = ObserveOnResponse { hook: hook.clone() };
        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();

        observer.on_response(&response, Duration::MAX, &Span::none());

        assert_eq!(
            hook.events(),
            vec![ObserveEvent::RequestFinished {
                status: StatusCode::OK,
                input_tokens: None,
                output_tokens: None,
                duration_ms: u64::MAX,
            }]
        );
    }

    #[test]
    fn body_chunk_observer_records_monotonic_batches() {
        let hook = RecordingHook::default();
        let mut observer = ObserveOnBodyChunk {
            hook: hook.clone(),
            next_batch_index: 0,
        };

        observer.on_body_chunk(
            &b"hello".as_slice(),
            Duration::from_millis(1),
            &Span::none(),
        );
        observer.on_body_chunk(
            &b"world!".as_slice(),
            Duration::from_millis(2),
            &Span::none(),
        );

        assert_eq!(
            hook.events(),
            vec![
                ObserveEvent::Chunk {
                    batch_index: 0,
                    event_count: 1,
                    total_bytes: 5,
                },
                ObserveEvent::Chunk {
                    batch_index: 1,
                    event_count: 1,
                    total_bytes: 6,
                },
            ]
        );
    }

    #[test]
    fn body_chunk_observer_saturates_batch_index() {
        let hook = RecordingHook::default();
        let mut observer = ObserveOnBodyChunk {
            hook: hook.clone(),
            next_batch_index: u64::MAX,
        };

        observer.on_body_chunk(&b"x".as_slice(), Duration::ZERO, &Span::none());
        observer.on_body_chunk(&b"y".as_slice(), Duration::ZERO, &Span::none());

        assert_eq!(
            hook.events(),
            vec![
                ObserveEvent::Chunk {
                    batch_index: u64::MAX,
                    event_count: 1,
                    total_bytes: 1,
                },
                ObserveEvent::Chunk {
                    batch_index: u64::MAX,
                    event_count: 1,
                    total_bytes: 1,
                },
            ]
        );
    }

    #[test]
    fn trace_layer_builds_with_recording_hook() {
        let _layer = trace_layer(RecordingHook::default());
    }
}
