mod common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, UpstreamDispatch, UpstreamRateLimitSink};
use cc_lb_plugin_api::SignedRequest;
use cc_lb_storage_api::{RateLimitKind, UpstreamRateLimitObservationRecord};
use http::header::{CONTENT_TYPE, HeaderName};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use serde_json::json;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState,
    collect_body, lifecycle_with, lifecycle_with_parts, messages_request,
};

#[tokio::test]
async fn successful_response_enqueues_records_keyed_by_selected_upstream() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let (sink, mut receiver) = UpstreamRateLimitSink::with_capacity(16);
    let test_bus = TestLifecycleBus::new().with_rate_limit_header_subscriber(sink);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state),
        MockDispatch {
            state: TestState::default(),
            mode: DispatchMode::HeadersOk(rate_limit_headers(997, 42)),
        },
        hook,
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    let records = recv_records(&mut receiver, 2).await;
    assert_observed(&records, RateLimitKind::Requests, Some(997));
    assert_observed(&records, RateLimitKind::Tokens, Some(42));
    for record in records {
        assert_eq!(record.upstream_id, default_upstream_id());
        assert_eq!(record.window, "default");
        assert!(record.observed_at_unix_secs > 0);
    }
}

#[tokio::test]
async fn too_many_requests_enqueues_but_server_error_does_not() {
    let (sink, mut receiver) = UpstreamRateLimitSink::with_capacity(16);
    let test_bus_ok = TestLifecycleBus::new().with_rate_limit_header_subscriber(sink);
    let lifecycle = lifecycle_for_response(StatusCode::TOO_MANY_REQUESTS, rate_limit_headers(0, 1))
        .with_event_bus(test_bus_ok.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let records = recv_records(&mut receiver, 2).await;
    assert_observed(&records, RateLimitKind::Requests, Some(0));
    assert_observed(&records, RateLimitKind::Tokens, Some(1));

    let (sink, mut receiver) = UpstreamRateLimitSink::with_capacity(16);
    let test_bus_err = TestLifecycleBus::new().with_rate_limit_header_subscriber(sink);
    let lifecycle =
        lifecycle_for_response(StatusCode::INTERNAL_SERVER_ERROR, rate_limit_headers(9, 9))
            .with_event_bus(test_bus_err.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(receiver.try_recv().is_err());
}

fn lifecycle_for_response(status: StatusCode, headers: HeaderMap) -> cc_lb_engine::Lifecycle {
    let state = TestState::default();
    lifecycle_with_parts(
        TestAuthn::new(state),
        Arc::new(TestRouter {
            base_url: url::Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        Arc::new(StaticResponseDispatch { status, headers }),
        vec![Arc::new(RecordingHook::default())],
        cc_lb_engine::LifecycleConfig::default(),
    )
}

struct StaticResponseDispatch {
    status: StatusCode,
    headers: HeaderMap,
}

#[async_trait]
impl UpstreamDispatch for StaticResponseDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let body = if self.status.is_success() {
            json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}})
        } else {
            json!({"type":"error","error":{"type":"rate_limit_error","message":"forced"}})
        };
        let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }
}

async fn recv_records(
    receiver: &mut Receiver<UpstreamRateLimitObservationRecord>,
    count: usize,
) -> Vec<UpstreamRateLimitObservationRecord> {
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("observation arrives")
                .expect("observation channel remains open"),
        );
    }
    assert!(receiver.try_recv().is_err());
    records
}

fn assert_observed(
    records: &[UpstreamRateLimitObservationRecord],
    kind: RateLimitKind,
    remaining: Option<u64>,
) {
    assert!(
        records
            .iter()
            .any(|record| record.kind == kind && record.remaining == remaining),
        "missing {kind:?} observation in {records:?}"
    );
}

fn rate_limit_headers(requests_remaining: u64, tokens_remaining: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, "anthropic-ratelimit-requests-limit", "1000");
    insert_header(
        &mut headers,
        "anthropic-ratelimit-requests-remaining",
        &requests_remaining.to_string(),
    );
    insert_header(
        &mut headers,
        "anthropic-ratelimit-requests-reset",
        "2026-05-29T00:00:00Z",
    );
    insert_header(&mut headers, "anthropic-ratelimit-tokens-limit", "100000");
    insert_header(
        &mut headers,
        "anthropic-ratelimit-tokens-remaining",
        &tokens_remaining.to_string(),
    );
    insert_header(
        &mut headers,
        "anthropic-ratelimit-tokens-reset",
        "2026-05-29T00:00:01Z",
    );
    headers
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) {
    headers.insert(
        HeaderName::from_bytes(name.as_bytes()).expect("test header name parses"),
        HeaderValue::from_str(value).expect("test header value parses"),
    );
}

fn default_upstream_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("default upstream id parses")
}
