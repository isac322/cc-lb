use crate::config_admin_common;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    response::Response,
};
use cc_lb_admin::{AdminState, router};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn test_state() -> AdminState {
    config_admin_common::test_state_without_storage()
}

async fn get(app: &Router, uri: &str) -> Response {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap()
}

async fn body_text(response: Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn header_value(response: &Response, name: header::HeaderName) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

fn assert_header_starts(response: &Response, name: header::HeaderName, expected: &str) {
    let value = header_value(response, name);
    assert!(
        value.starts_with(expected),
        "expected header to start with {expected:?}, got {value:?}"
    );
}

fn discover_index_asset(index_html: &str, suffix: &str) -> String {
    index_html
        .split(['"', '\''])
        .find(|part| part.starts_with("/assets/index-") && part.ends_with(suffix))
        .unwrap_or_else(|| panic!("missing index asset ending in {suffix}"))
        .to_string()
}

#[tokio::test]
async fn t2__test_static_assets_served() {
    let app = router(test_state());

    let response = get(&app, "/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/html");
    let index_html = body_text(response).await;

    let js_path = discover_index_asset(&index_html, ".js");
    let response = get(&app, &js_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/javascript");

    let css_path = discover_index_asset(&index_html, ".css");
    let response = get(&app, &css_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/css");
}
