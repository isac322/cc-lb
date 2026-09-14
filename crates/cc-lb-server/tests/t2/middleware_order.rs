use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
};
use cc_lb_config::Config;
use cc_lb_engine::{DispatchError, UpstreamDispatch};
use cc_lb_server::app::build_app_for_testing_with_dispatch;
use cc_lb_upstream::SignedRequest;
use tower::ServiceExt;

#[tokio::test]
async fn t2__middleware_order() {
    let mut config = Config::default();
    config.body.messages_cap_bytes = 256;
    let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(UnexpectedDispatch);
    let app = build_app_for_testing_with_dispatch(
        config,
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await
    .expect("build app");
    let body = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"this body is intentionally larger than the configured cap because it repeats text text text text text text text text text text text text text text text text text text text text text text text text"}],"max_tokens":1}"#;

    let response = app
        .router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("connection", "X-Hop-Test")
                .header("x-hop-test", "strip-me")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("oversized post");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(!response.headers().contains_key("x-hop-test"));
}

struct UnexpectedDispatch;

#[async_trait]
impl UpstreamDispatch for UnexpectedDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<Response<cc_lb_engine::Body>, DispatchError> {
        panic!("oversized request must not reach upstream dispatch")
    }
}
