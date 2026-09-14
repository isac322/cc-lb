use axum::Router;
use axum::body::Body;
use axum::http::header::{CONTENT_TYPE, RETRY_AFTER};
use axum::http::{Request, StatusCode};
use axum::middleware;
use axum::routing::post;
use cc_lb_server::drain::{DrainController, proxy_drain_middleware};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn t2__drain_rejects_new_with_retry_after() {
    let controller = DrainController::new();
    let app = Router::new()
        .route("/v1/messages", post(|| async { "unexpected" }))
        .route_layer(middleware::from_fn_with_state(
            controller.clone(),
            proxy_drain_middleware,
        ));
    controller.trigger();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("drain middleware responds");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[RETRY_AFTER], "60");
    assert_eq!(response.headers().get(CONTENT_TYPE), None);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("drain response body collects")
        .to_bytes();
    assert_eq!(body.as_ref(), b"draining");
}
