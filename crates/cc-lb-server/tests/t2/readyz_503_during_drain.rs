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
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn t2__readyz_503_during_drain() {
    let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(UnexpectedDispatch);
    let app = build_app_for_testing_with_dispatch(
        Config::default(),
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await
    .expect("build app");

    let healthy_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .expect("healthy ready request"),
        )
        .await
        .expect("healthy readyz");
    assert_eq!(healthy_response.status(), StatusCode::OK);
    let healthy_body = healthy_response
        .into_body()
        .collect()
        .await
        .expect("healthy ready body")
        .to_bytes();
    let healthy_json: Value = serde_json::from_slice(&healthy_body).expect("healthy ready json");
    assert_eq!(healthy_json["ready"], true);

    app.drain_controller().trigger();
    assert!(app.drain_controller().is_draining());

    let response = app
        .router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .expect("ready request"),
        )
        .await
        .expect("readyz");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("ready body")
        .to_bytes();
    let json: Value = serde_json::from_slice(&body).expect("ready json");
    assert_eq!(json["ready"], false);
    assert_eq!(json["reason"], "draining");
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
