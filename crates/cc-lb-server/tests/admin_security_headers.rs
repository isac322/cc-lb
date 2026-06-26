use axum::{
    body::Body,
    http::{Request, StatusCode, header},
    response::Response,
};
use cc_lb_config::Config;
use cc_lb_server::app::build_app_for_testing;
use http_body_util::BodyExt;
use tower::ServiceExt;

const SECURITY_HEADERS: [(&str, &str); 8] = [
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "strict-origin-when-cross-origin"),
    ("x-frame-options", "DENY"),
    ("cross-origin-opener-policy", "same-origin"),
    ("cross-origin-resource-policy", "same-origin"),
    ("origin-agent-cluster", "?1"),
    (
        "permissions-policy",
        "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()",
    ),
    (
        "content-security-policy",
        "default-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; font-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'",
    ),
];

async fn get(router: axum::Router, uri: &str) -> Response {
    router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn get_with_if_none_match(router: axum::Router, uri: &str, etag: &str) -> Response {
    router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header(header::IF_NONE_MATCH, etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn post_messages(router: axum::Router) -> Response {
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn body_text(response: Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn discover_index_asset(index_html: &str) -> String {
    index_html
        .split(['"', '\''])
        .find(|part| part.starts_with("/assets/") && part.ends_with(".js"))
        .unwrap_or_else(|| panic!("missing index script asset"))
        .to_owned()
}

fn assert_security_headers(response: &Response) {
    for (name, expected) in SECURITY_HEADERS {
        let actual = response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert_eq!(actual, expected, "unexpected {name}");
    }
}

fn assert_security_headers_absent(response: &Response) {
    for (name, _) in SECURITY_HEADERS {
        assert!(
            !response.headers().contains_key(name),
            "proxy response unexpectedly included {name}"
        );
    }
}

#[tokio::test]
async fn admin_responses_include_browser_security_headers() {
    let app = build_app_for_testing(Config::default()).await.unwrap();

    let html = get(app.admin_router.clone(), "/").await;
    assert_eq!(html.status(), StatusCode::OK);
    assert_security_headers(&html);
    let asset_path = discover_index_asset(&body_text(html).await);

    let asset = get(app.admin_router.clone(), &asset_path).await;
    assert_eq!(asset.status(), StatusCode::OK);
    assert_security_headers(&asset);
    let etag = asset
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();

    let cached = get_with_if_none_match(app.admin_router.clone(), &asset_path, &etag).await;
    assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);
    assert_security_headers(&cached);

    let json = get(app.admin_router, "/admin/health").await;
    assert_eq!(json.status(), StatusCode::OK);
    assert_security_headers(&json);
}

#[tokio::test]
async fn proxy_responses_omit_admin_browser_security_headers() {
    let app = build_app_for_testing(Config::default()).await.unwrap();

    let health = get(app.router.clone(), "/healthz").await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_security_headers_absent(&health);

    let messages = post_messages(app.router).await;
    assert_security_headers_absent(&messages);
