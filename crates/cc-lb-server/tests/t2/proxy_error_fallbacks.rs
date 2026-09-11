use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use cc_lb_config::Config;
use cc_lb_engine::{DispatchError, UpstreamDispatch};
use cc_lb_server::app::{build_app_for_testing, build_app_for_testing_with_dispatch};
use cc_lb_upstream::SignedRequest;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn t2__readyz_uses_declared_runtime_readiness_without_proxy_traffic() -> TestResult<()> {
    let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(UnexpectedDispatch);
    let app = build_app_for_testing_with_dispatch(
        Config::default(),
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await?;

    let response = app
        .router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    let json: Value = serde_json::from_slice(&body)?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["ready"], true);
    Ok(())
}

#[tokio::test]
async fn t2__proxy_fallbacks_return_anthropic_json_errors() -> TestResult<()> {
    let app =
        build_app_for_testing(Config::default(), cc_lb_testkit::fixed_clock(1_700_000_000)).await?;

    for (method, path, expected_status, expected_message) in [
        (
            "GET",
            "/not-a-proxy-route",
            StatusCode::NOT_FOUND,
            "requested proxy path was not found",
        ),
        (
            "POST",
            "/v1/models",
            StatusCode::METHOD_NOT_ALLOWED,
            "method is not allowed for this proxy path",
        ),
    ] {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response.into_body().collect().await?.to_bytes();
        let json: Value = serde_json::from_slice(&body)?;

        assert_eq!(status, expected_status);
        assert!(content_type.starts_with("application/json"));
        assert_eq!(json["type"], "error");
        assert_eq!(json["error"]["type"], "not_found");
        assert_eq!(json["error"]["message"], expected_message);
    }

    Ok(())
}

struct UnexpectedDispatch;

#[async_trait]
impl UpstreamDispatch for UnexpectedDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<Response<cc_lb_engine::Body>, DispatchError> {
        panic!("readiness test must not dispatch proxy traffic")
    }
}
