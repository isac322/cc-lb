use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::{Config, PluginRef};
use cc_lb_core::DashboardBroadcaster;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn configured_authn_and_router_plugins_are_listed() {
    let app = router(test_state(config_with_plugins()));
    let (status, json, _) = authorized_json(app, "/admin/plugins").await;

    assert_eq!(status, StatusCode::OK);
    let plugins = json["plugins"].as_array().unwrap();
    assert_eq!(plugins.len(), 2);

    let authn = plugin_by_slot(plugins, "authn");
    assert_eq!(authn["name"], "authn-plugin");
    assert_eq!(authn["wasm_path"], "/tmp/authn.wasm");
    assert_eq!(authn["loaded"], true);
    assert_eq!(authn["disabled"], false);
    assert_eq!(authn["failure_count"], 0);
    assert_eq!(authn["last_error"], Value::Null);

    let router = plugin_by_slot(plugins, "router");
    assert_eq!(router["name"], "router-plugin");
    assert_eq!(router["slot"], "router");
}

#[tokio::test]
async fn empty_plugin_config_returns_empty_list() {
    let app = router(test_state(Config::default()));
    let (status, json, _) = authorized_json(app, "/admin/plugins").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json, json!({ "plugins": [] }));
}

#[tokio::test]
async fn plugins_status_requires_admin_auth() {
    let app = router(test_state(config_with_plugins()));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/plugins")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn plugins_status_response_excludes_payload_and_config_secret_terms() {
    let app = router(test_state(config_with_plugins()));
    let (status, _, body) = authorized_json(app, "/admin/plugins").await;

    assert_eq!(status, StatusCode::OK);
    assert_forbidden_bytes_absent(&body);
}

fn test_state(config: Config) -> AdminState {
    AdminState {
        storage: None,
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config: Arc::new(config),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

fn config_with_plugins() -> Config {
    let mut config = Config::default();
    config.plugins.authn_plugin = Some(plugin_ref("authn-plugin", "/tmp/authn.wasm"));
    config.plugins.router_plugin = Some(plugin_ref("router-plugin", "/tmp/router.wasm"));
    config.plugins.authn_plugin.as_mut().unwrap().config = json!({
        "Authorization": "Bearer should-not-return",
        "client_secret": "secret-value"
    });
    config
}

fn plugin_ref(name: &str, wasm_path: &str) -> PluginRef {
    PluginRef {
        name: name.to_string(),
        wasm_path: Some(PathBuf::from(wasm_path)),
        sse_per_event: true,
        batched_events_per_flush: 7,
        batched_flush_ms: 11,
        ..PluginRef::default()
    }
}

async fn authorized_json(app: axum::Router, uri: &str) -> (StatusCode, Value, Vec<u8>) {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json, body.to_vec())
}

fn plugin_by_slot<'a>(plugins: &'a [Value], slot: &str) -> &'a Value {
    plugins
        .iter()
        .find(|plugin| plugin["slot"] == slot)
        .expect("plugin slot present")
}

fn assert_forbidden_bytes_absent(body: &[u8]) {
    for forbidden in forbidden_terms() {
        assert!(
            !body
                .windows(forbidden.len())
                .any(|window| window == forbidden.as_bytes()),
            "forbidden term present: {forbidden}"
        );
    }
}

fn forbidden_terms() -> Vec<String> {
    vec![
        ["mes", "sages"].concat(),
        ["sys", "tem"].concat(),
        ["too", "ls"].concat(),
        ["tool", "_use"].concat(),
        ["con", "tent"].concat(),
        ["Authori", "zation"].concat(),
        ["client", "_secret"].concat(),
    ]
}
