mod common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, LifecycleConfig, UpstreamDispatch};
use cc_lb_plugin_api::SignedRequest;
use http::{Response, StatusCode};

use common::{RecordingHook, TestAuthn, TestRouter, TestState, collect_body, lifecycle_with_parts};

#[tokio::test]
async fn cache_keepalive_without_scheduler_is_response_noop() {
    let upstream_body = Bytes::from_static(
        br#"{"type":"message","id":"msg_keepalive_noop","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}"#,
    );
    let state = TestState::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state),
        Arc::new(TestRouter {
            base_url: "http://upstream.local/".parse().expect("test URL parses"),
        }),
        Arc::new(FixedSuccessDispatch {
            body: upstream_body.clone(),
        }),
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    );

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
        ))
        .expect("test request builds");

    let response = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles request without keepalive scheduler");

    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, upstream_body);
}

struct FixedSuccessDispatch {
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for FixedSuccessDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}
