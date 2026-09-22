use crate::common;

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, UpstreamDispatch, UpstreamRateLimitSink};
use cc_lb_storage_api::RateLimitKind;
use cc_lb_upstream::SignedRequest;
use http::header::{CONTENT_TYPE, HeaderName};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use serde_json::json;
use tokio::time::{Duration, timeout};

use common::{
    RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState, collect_body,
    lifecycle_with_parts, messages_request,
};

#[tokio::test]
async fn unauthorized_refresh_observes_only_final_attempt() {
    let state = TestState::default();
    let (sink, mut receiver) = UpstreamRateLimitSink::with_capacity(16);
    let test_bus = TestLifecycleBus::new().with_rate_limit_header_subscriber(sink);
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state.clone()),
        Arc::new(TestRouter {
            base_url: url::Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        Arc::new(SequencedDispatch {
            state: state.clone(),
            responses: Arc::new(Mutex::new(VecDeque::from([
                ResponseSpec {
                    status: StatusCode::UNAUTHORIZED,
                    headers: rate_limit_headers(111),
                },
                ResponseSpec {
                    status: StatusCode::OK,
                    headers: rate_limit_headers(222),
                },
            ]))),
        }),
        vec![Arc::new(RecordingHook::default())],
        cc_lb_engine::LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 2);
    assert_eq!(state.refresh_count.load(Ordering::Relaxed), 1);
    let record = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("observation arrives")
        .expect("observation channel remains open");
    assert_eq!(record.kind, RateLimitKind::Requests);
    assert_eq!(record.remaining, Some(222));
    assert!(receiver.try_recv().is_err());
}

#[derive(Clone)]
struct ResponseSpec {
    status: StatusCode,
    headers: HeaderMap,
}

struct SequencedDispatch {
    state: TestState,
    responses: Arc<Mutex<VecDeque<ResponseSpec>>>,
}

#[async_trait]
impl UpstreamDispatch for SequencedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.state.upstream_calls.fetch_add(1, Ordering::Relaxed);
        let spec = self
            .responses
            .lock()
            .expect("response sequence lock")
            .pop_front()
            .expect("test response remains");
        let body = if spec.status.is_success() {
            json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}})
        } else {
            json!({"type":"error","error":{"type":"authentication_error","message":"forced"}})
        };
        let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
        *response.status_mut() = spec.status;
        *response.headers_mut() = spec.headers;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }
}

fn rate_limit_headers(requests_remaining: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-requests-limit"),
        HeaderValue::from_static("1000"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-requests-remaining"),
        HeaderValue::from_str(&requests_remaining.to_string()).expect("test header value parses"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-requests-reset"),
        HeaderValue::from_static("2026-05-29T00:00:00Z"),
    );
    headers
}
