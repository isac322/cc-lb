use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    response::Response,
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::RedbStorage;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    AdminState {
        storage: test_storage(),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config: Arc::new(Config::default()),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
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

fn header_value(response: &Response, name: header::HeaderName) -> &str {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
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
async fn test_static_assets_served() {
    let app = router(test_state());

    let response = get(&app, "/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/html");
    assert_eq!(header_value(&response, header::CACHE_CONTROL), "no-cache");
    let index_html = body_text(response).await;
    assert!(index_html.contains(r#"<div id="root">"#));

    let js_path = discover_index_asset(&index_html, ".js");
    let response = get(&app, &js_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/javascript");
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "public, max-age=31536000, immutable"
    );

    let css_path = discover_index_asset(&index_html, ".css");
    let response = get(&app, &css_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/css");
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "public, max-age=31536000, immutable"
    );

    let response = get(&app, "/admin/health").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "application/json");

    let response = get(&app, "/unknown-deep-link").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/html");
    assert_eq!(header_value(&response, header::CACHE_CONTROL), "no-cache");
    let fallback_html = body_text(response).await;
    assert!(fallback_html.contains(r#"<div id="root">"#));

    let response = get(&app, "/admin/totally-unknown-route").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path).unwrap());
    std::mem::forget(dir);
    storage
}
