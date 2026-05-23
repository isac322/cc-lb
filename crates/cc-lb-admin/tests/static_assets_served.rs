use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::Config;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    AdminState {
        storage: None,
        limit_engine: cc_lb_core::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            Arc::new(arc_swap::ArcSwap::from(
                cc_lb_core::api_keys::principal_view::PrincipalView::from_config(&Config::default()),
            )),
        ),
        lifecycle: None,
        principal_view: Arc::new(arc_swap::ArcSwap::from(
            cc_lb_core::api_keys::principal_view::PrincipalView::from_config(
                &cc_lb_admin::CurrentConfig::current_config((Arc::new(Config::default())).as_ref()),
            ),
        )),
        config: Arc::new(Config::default()),
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_static_assets_served() {
    let app = router(test_state());

    let req = Request::builder()
        .method("GET")
        .uri("/")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/html");

    let req = Request::builder()
        .method("GET")
        .uri("/style.css")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/css");

    let req = Request::builder()
        .method("GET")
        .uri("/app.js")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/javascript"
    );
}
