use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use bytes::Bytes;
use cc_lb_config::Config;
use cc_lb_engine::{DispatchError, UpstreamDispatch};
use cc_lb_server::app::build_app_for_testing_with_dispatch;
use cc_lb_upstream::SignedRequest;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn t2__testing_constructor_uses_injected_dispatch() {
    let dispatched_urls = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(RecordingDispatch {
        dispatched_urls: Arc::clone(&dispatched_urls),
    });
    let app = build_app_for_testing_with_dispatch(
        Config::default(),
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await
    .expect("build app with injected dispatcher");

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .body(Body::from(Bytes::from_static(
                    br#"{"model":"claude-test","max_tokens":1,"messages":[{"role":"user","content":"ping"}]}"#,
                )))
                .expect("request builds"),
        )
        .await
        .expect("proxy response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body collects")
        .to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("response is JSON");
    assert_eq!(json["id"], "msg_injected_dispatch");
    assert_eq!(json["usage"]["input_tokens"], 1);
    assert_eq!(json["usage"]["output_tokens"], 1);
    assert_eq!(
        dispatched_urls
            .lock()
            .expect("dispatch URLs lock")
            .as_slice(),
        &["https://api.anthropic.com/v1/messages".to_owned()]
    );
}
#[tokio::test]
async fn t2__terminal_dispatch_error_mapping_table() {
    for (case, error, expected_status, expected_type, expected_message, retry_after) in [
        (
            "invalid_uri",
            ErrorKind::InvalidUri,
            StatusCode::BAD_GATEWAY,
            "api_error",
            "upstream request failed",
            None,
        ),
        (
            "request_build",
            ErrorKind::RequestBuild,
            StatusCode::BAD_GATEWAY,
            "api_error",
            "upstream request failed",
            None,
        ),
        (
            "transport",
            ErrorKind::Transport,
            StatusCode::BAD_GATEWAY,
            "api_error",
            "upstream request failed",
            None,
        ),
        (
            "bulkhead_full",
            ErrorKind::BulkheadFull,
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded_error",
            "upstream bulkhead queue is full",
            Some("3"),
        ),
    ] {
        let app = build_app_for_testing_with_dispatch(
            Config::default(),
            cc_lb_testkit::fixed_clock(1_700_000_000),
            Some(Arc::new(ErrorDispatch(error))),
        )
        .await
        .unwrap_or_else(|source| panic!("case={case}: build app: {source}"));
        let response = app
            .router
            .clone()
            .oneshot(messages_request())
            .await
            .unwrap_or_else(|source| panic!("case={case}: proxy response: {source}"));

        assert_eq!(response.status(), expected_status, "case={case}");
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            retry_after,
            "case={case}"
        );
        let body = response
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|source| panic!("case={case}: collect body: {source}"))
            .to_bytes();
        let json: Value = serde_json::from_slice(&body)
            .unwrap_or_else(|source| panic!("case={case}: response JSON: {source}"));
        assert_eq!(json["type"], "error", "case={case}");
        assert_eq!(json["error"]["type"], expected_type, "case={case}");
        assert_eq!(json["error"]["message"], expected_message, "case={case}");
    }
}

#[derive(Clone, Copy)]
enum ErrorKind {
    InvalidUri,
    RequestBuild,
    Transport,
    BulkheadFull,
}

struct ErrorDispatch(ErrorKind);

#[async_trait]
impl UpstreamDispatch for ErrorDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Err(match self.0 {
            ErrorKind::InvalidUri => DispatchError::InvalidUri {
                reason: "invalid URI fixture".to_owned(),
            },
            ErrorKind::RequestBuild => DispatchError::RequestBuild {
                reason: "request build fixture".to_owned(),
            },
            ErrorKind::Transport => DispatchError::Transport {
                reason: "transport fixture".to_owned(),
            },
            ErrorKind::BulkheadFull => DispatchError::BulkheadFull {
                retry_after: Duration::from_secs(3),
            },
        })
    }
}

fn messages_request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Body::from(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":1,"messages":[{"role":"user","content":"ping"}]}"#,
        )))
        .expect("request builds")
}

struct RecordingDispatch {
    dispatched_urls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let (url, _method, _headers, _body) = request.into_parts();
        self.dispatched_urls
            .lock()
            .expect("dispatch URLs lock")
            .push(url.to_string());
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(Bytes::from_static(
                br#"{"id":"msg_injected_dispatch","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
            )))
            .expect("scripted response builds"))
    }
}
