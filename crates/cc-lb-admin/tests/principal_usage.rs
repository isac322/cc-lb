use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::{Config, PrincipalSpec};
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::{RequestEvent, RequestEventUpstream, Storage};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn existing_principal_usage_is_filtered_and_grouped_by_model() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("principal-usage.redb");
    let storage = Storage::open(&path, [42; 32]).unwrap();
    let base = current_minute_base();
    seed_usage_events(&storage, base);
    storage.rollup_usage_once().unwrap();

    let app = router(test_state(
        config_with_principals(&["principal-a", "principal-b"]),
        Some(Arc::new(storage)),
    ));
    let (status, json, body) =
        authorized_json(app, "/admin/principals/principal-a/usage?range=1h").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["range"], "1h");
    assert_eq!(json["step"], "minute");
    assert_eq!(json["group_by"], "model");
    assert_eq!(json["observed"], true);
    assert_eq!(json.get("truncated_series_count"), None);

    let series = json["series"].as_array().unwrap();
    assert_eq!(series.len(), 2);
    let keys = series
        .iter()
        .map(|series| series["key"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["model-a", "model-b"]);
    assert_eq!(sum_requests(series), 2);
    assert!(!body_contains(&body, b"principal-b"));
    assert!(!body_contains(&body, b"model-c"));
}

#[tokio::test]
async fn unknown_principal_usage_returns_404() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, json, _) = authorized_json(app, "/admin/principals/ghost/usage").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json, serde_json::json!({ "error": "unknown_principal" }));
}

#[tokio::test]
async fn invalid_principal_usage_id_returns_400() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, json, _) = authorized_json(app, "/admin/principals/bad%20id/usage").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_principal_id");
}

#[tokio::test]
async fn principal_usage_rejects_group_by_param() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, json, _) = authorized_json(
        app,
        "/admin/principals/principal-a/usage?range=1h&group_by=principal",
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_group_by");
}

#[tokio::test]
async fn empty_storage_usage_returns_empty_unobserved_series() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, json, _) =
        authorized_json(app, "/admin/principals/principal-a/usage?range=1h").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["observed"], false);
    assert_eq!(json["group_by"], "model");
    assert_eq!(json["series"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn principal_usage_requires_admin_auth() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/principals/principal-a/usage?range=1h")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn principal_usage_response_excludes_payload_terms() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, _, body) =
        authorized_json(app, "/admin/principals/principal-a/usage?range=15m").await;

    assert_eq!(status, StatusCode::OK);
    assert_forbidden_bytes_absent(&body);
}

fn test_state(config: Config, storage: Option<Arc<Storage>>) -> AdminState {
    AdminState {
        storage,
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

fn config_with_principals(ids: &[&str]) -> Config {
    let mut config = Config::default();
    for id in ids {
        config
            .principals
            .insert((*id).to_owned(), PrincipalSpec::default());
    }
    config
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

fn seed_usage_events(storage: &Storage, base: u64) {
    for event in [
        event(base + 5, "req-a-1", "principal-a", "model-a"),
        event(base + 65, "req-a-2", "principal-a", "model-b"),
        event(base + 10, "req-b-1", "principal-b", "model-c"),
    ] {
        storage.append_request_event(&event).unwrap();
    }
}

fn event(ts: u64, request_id: &str, principal: &str, model: &str) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some(model.to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(2),
        duration_ms: 25,
        error_code: None,
    }
}

fn current_minute_base() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now - (now % 60) - 180
}

fn sum_requests(series: &[Value]) -> u64 {
    series
        .iter()
        .flat_map(|series| series["buckets"].as_array().unwrap())
        .map(|bucket| bucket["request_count"].as_u64().unwrap())
        .sum()
}

fn body_contains(body: &[u8], needle: &[u8]) -> bool {
    body.windows(needle.len()).any(|window| window == needle)
}

fn assert_forbidden_bytes_absent(body: &[u8]) {
    for forbidden in forbidden_terms() {
        assert!(
            !body_contains(body, forbidden.as_bytes()),
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
    ]
}
