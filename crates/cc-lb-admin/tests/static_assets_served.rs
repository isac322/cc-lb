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
        build: admin_test_common::test_build_info(),
        startup_config_overrides: Default::default(),
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        dynamic_view_rebinder: None,
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
            .expect("production Anthropic origin parses"),
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

fn is_spa_skip_placeholder(index_html: &str) -> bool {
    index_html
        .split(['"', '\''])
        .any(|part| part == "/assets/index-skip.js")
}

fn discover_bootstrap_asset(index_html: &str) -> String {
    index_html
        .split(['"', '\''])
        .find(|part| *part == "/theme-init.js")
        .unwrap_or_else(|| panic!("missing external theme bootstrap"))
        .to_owned()
}

fn assert_scripts_are_external(index_html: &str) {
    let mut script_count = 0;
    for script in index_html.split("<script").skip(1) {
        let tag = script
            .split_once("</script>")
            .map_or(script, |(tag, _)| tag);
        assert!(tag.contains("src="), "index contains an inline script");
        script_count += 1;
    }
    assert!(script_count > 0, "index contains no script tags");
}

fn discover_font_assets(css: &str) -> Vec<String> {
    let mut assets = Vec::new();
    let mut remaining = css;
    while let Some((_, after_url)) = remaining.split_once("url") {
        remaining = after_url;
        let Some(value) = after_url.trim_start().strip_prefix('(') else {
            continue;
        };
        let value = value.trim_start();
        let (value, after_value) = if let Some(value) = value.strip_prefix('"') {
            let Some((value, after_quote)) = value.split_once('"') else {
                continue;
            };
            let Some(after_value) = after_quote.trim_start().strip_prefix(')') else {
                continue;
            };
            (value, after_value)
        } else if let Some(value) = value.strip_prefix('\'') {
            let Some((value, after_quote)) = value.split_once('\'') else {
                continue;
            };
            let Some(after_value) = after_quote.trim_start().strip_prefix(')') else {
                continue;
            };
            (value, after_value)
        } else {
            let Some((value, after_value)) = value.split_once(')') else {
                continue;
            };
            (value.trim(), after_value)
        };
        remaining = after_value;
        let path = value.split(['?', '#']).next().unwrap_or(value);
        if path.starts_with("/assets/")
            && (path.ends_with(".woff") || path.ends_with(".woff2"))
            && !assets.iter().any(|asset| asset == path)
        {
            assets.push(path.to_owned());
        }
    }
    assets
}

#[test]
fn font_discovery_handles_css_url_syntax() {
    let css = r#"
        @font-face { src: url(/assets/regular.woff2) format("woff2"); }
        @font-face { src: url( '/assets/bold.woff?version=1#font' ) format('woff'); }
        @font-face { src: url ( "/assets/italic.woff2#font" ); }
        body { background: url(/assets/image.png); }
    "#;
    assert_eq!(
        discover_font_assets(css),
        vec![
            "/assets/regular.woff2".to_owned(),
            "/assets/bold.woff".to_owned(),
            "/assets/italic.woff2".to_owned()
        ]
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
    let index_html = body_text(response).await;

    let index_is_placeholder = is_spa_skip_placeholder(&index_html);
    assert_scripts_are_external(&index_html);
    if !index_is_placeholder {
        let bootstrap_path = discover_bootstrap_asset(&index_html);
        let response = get(&app, &bootstrap_path).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_header_starts(&response, header::CONTENT_TYPE, "text/javascript");
        assert!(!body_text(response).await.is_empty());
    }

    let js_path = discover_index_asset(&index_html, ".js");
    let response = get(&app, &js_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/javascript");

    let css_path = discover_index_asset(&index_html, ".css");
    let response = get(&app, &css_path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_header_starts(&response, header::CONTENT_TYPE, "text/css");
    let css = body_text(response).await;
    assert!(
        !css.contains("data:font/") && !css.contains("data:application/font"),
        "font resources must not use data URLs"
    );
    if index_is_placeholder {
        return;
    }
    let font_paths = discover_font_assets(&css);
    assert!(
        !font_paths.is_empty(),
        "CSS must reference packaged font assets"
    );
    for font_path in font_paths {
        assert!(
            font_path.starts_with("/assets/"),
            "font must be served from a same-origin asset path: {font_path}"
        );
        let response = get(&app, &font_path).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_header_starts(&response, header::CONTENT_TYPE, "font/");
    }
}
