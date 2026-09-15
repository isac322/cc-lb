use crate::admin_test_common;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    response::Response,
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        config_path: None,
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

async fn get(app: &Router, uri: &str) -> Response {
    request(app, uri, None).await
}

async fn get_with_if_none_match(app: &Router, uri: &str, value: &str) -> Response {
    request(app, uri, Some(value)).await
}

async fn request(app: &Router, uri: &str, if_none_match: Option<&str>) -> Response {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(value) = if_none_match {
        builder = builder.header(header::IF_NONE_MATCH, value);
    }
    let req = builder.body(Body::empty()).unwrap();
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

fn assert_strong_lowercase_sha256_etag(etag: &str) {
    assert_eq!(etag.len(), 66, "etag must be quoted 64-char sha256 hex");
    assert!(etag.starts_with('"'));
    assert!(etag.ends_with('"'));
    assert!(
        etag.as_bytes()[1..65]
            .iter()
            .all(|byte| { matches!(byte, b'0'..=b'9' | b'a'..=b'f') })
    );
}

fn discover_index_asset(index_html: &str) -> String {
    index_html
        .split(['"', '\''])
        .find(|part| part.starts_with("/assets/") && part.ends_with(".js"))
        .unwrap_or_else(|| panic!("missing index script asset"))
        .to_owned()
}

#[tokio::test]
async fn asset_responses_are_immutable_and_support_conditional_get() {
    let app = router(test_state());

    let index = get(&app, "/").await;
    let asset_path = discover_index_asset(&body_text(index).await);

    let response = get(&app, &asset_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = header_value(&response, header::CONTENT_TYPE);
    assert!(
        content_type.starts_with("text/javascript"),
        "unexpected asset content type: {content_type}"
    );
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "public, max-age=31536000, immutable"
    );
    let etag = header_value(&response, header::ETAG);
    assert_strong_lowercase_sha256_etag(&etag);

    let response = get_with_if_none_match(&app, &asset_path, &etag).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header_value(&response, header::ETAG), etag);
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "public, max-age=31536000, immutable"
    );
    assert_eq!(header_value(&response, header::CONTENT_TYPE), content_type);
    assert!(body_text(response).await.is_empty());

    let response = get_with_if_none_match(&app, &asset_path, "*").await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "public, max-age=31536000, immutable"
    );
    assert_eq!(header_value(&response, header::CONTENT_TYPE), content_type);
    assert!(body_text(response).await.is_empty());
}

#[tokio::test]
async fn index_and_spa_fallback_revalidate_and_support_conditional_get() {
    let app = router(test_state());

    let response = get(&app, "/").await;
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = header_value(&response, header::CONTENT_TYPE);
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "no-cache, must-revalidate"
    );
    let etag = header_value(&response, header::ETAG);
    assert_strong_lowercase_sha256_etag(&etag);

    let response = get_with_if_none_match(&app, "/", &etag).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header_value(&response, header::ETAG), etag);
    assert_eq!(header_value(&response, header::CONTENT_TYPE), content_type);
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "no-cache, must-revalidate"
    );
    assert!(body_text(response).await.is_empty());

    let response = get(&app, "/dashboard/not-a-real-route").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_value(&response, header::CACHE_CONTROL),
        "no-cache, must-revalidate"
    );
    assert_strong_lowercase_sha256_etag(&header_value(&response, header::ETAG));
}

#[tokio::test]
async fn admin_unknown_paths_do_not_fallback_to_spa() {
    let app = router(test_state());

    let response = get(&app, "/admin").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = get(&app, "/admin/not-a-real-route").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
