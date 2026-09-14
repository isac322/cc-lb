use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
};
use bytes::Bytes;
use cc_lb_config::Config;
use cc_lb_engine::{Body as UpstreamBody, DispatchError, UpstreamDispatch};
use cc_lb_server::app::{build_app_for_testing, build_app_for_testing_with_dispatch};
use cc_lb_upstream::SignedRequest;
use http_body_util::{BodyExt, Full};
use tower::ServiceExt;

#[tokio::test]
async fn t2__cache_keepalive_disabled_principal_builds_without_scheduler_dependency_cycle()
-> Result<(), Box<dyn std::error::Error>> {
    let app =
        build_app_for_testing(Config::default(), cc_lb_testkit::fixed_clock(1_700_000_000)).await?;

    assert_eq!(
        app.proxy_addr.port(),
        Config::default().listener.proxy_addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn t2__router_handles_cache_control_request() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = test_app(calls.clone()).await;
    let body = r#"{"model":"claude-3-5-sonnet-20241022","max_tokens":10,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hi"}]}"#;

    let response = post_messages(app.router, body).await;
    let status = response.status();
    let body = body_text(response).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#""type":"message""#));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn t2__router_handles_no_cache_control_request() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = test_app(calls.clone()).await;
    let body = r#"{"model":"claude-3-5-sonnet-20241022","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;

    let response = post_messages(app.router, body).await;
    let status = response.status();
    let body = body_text(response).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#""type":"message""#));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

async fn test_app(calls: Arc<AtomicUsize>) -> cc_lb_server::app::App {
    let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(MessageDispatch { calls });
    build_app_for_testing_with_dispatch(
        Config::default(),
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await
    .expect("build app")
}

async fn post_messages(router: axum::Router, body: &'static str) -> Response<Body> {
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("post messages")
}

async fn body_text(response: Response<Body>) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf-8 response")
}

struct MessageDispatch {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl UpstreamDispatch for MessageDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<Response<UpstreamBody>, DispatchError> {
        assert_eq!(request.url().path(), "/v1/messages");
        self.calls.fetch_add(1, Ordering::SeqCst);
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(UpstreamBody::new(Full::new(Bytes::from_static(
                br#"{"id":"msg_fake_000000000000000000000000","type":"message","role":"assistant","content":[],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50}}"#,
            ))))
            .map_err(|error| DispatchError::RequestBuild {
                reason: error.to_string(),
            })
    }
}
