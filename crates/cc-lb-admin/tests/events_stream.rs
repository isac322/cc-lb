use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{
    AdminState,
    events::{apply_filters_to_event, parse_stream_filters},
    router,
};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_api::{RequestEvent, RequestEventUpstream};
use cc_lb_storage_redb::RedbStorage;
use http_body_util::BodyExt;
use std::collections::HashMap;
use tower::ServiceExt;

fn test_state(broadcaster: Arc<DashboardBroadcaster>) -> AdminState {
    AdminState {
        storage: test_storage(),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: broadcaster,
        config: Arc::new(Config::default()),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn authorized_stream_receives_metadata_request_event() {
    let broadcaster = Arc::new(DashboardBroadcaster::with_capacity(8));
    let app = router(test_state(broadcaster.clone()));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    broadcaster.publish(event());
    let mut body = response.into_body();
    let wire = read_request_sse(&mut body).await;

    assert!(wire.contains("event: request"));
    assert!(wire.contains("id: 1"));
    assert!(wire.contains(r#""request_id":"req-stream""#));
    for key in forbidden_json_keys() {
        assert!(
            !wire.contains(&format!(r#""{key}""#)),
            "forbidden JSON key present in SSE wire bytes: {key}"
        );
    }
}

#[tokio::test]
async fn unauthenticated_stream_request_is_rejected() {
    let broadcaster = Arc::new(DashboardBroadcaster::with_capacity(8));
    let app = router(test_state(broadcaster));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn stream_rejects_invalid_filter_params_before_sse_starts() {
    let broadcaster = Arc::new(DashboardBroadcaster::with_capacity(8));
    let app = router(test_state(broadcaster));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream?upstream=garbage")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json, serde_json::json!({ "error": "invalid_upstream" }));
}

#[test]
fn stream_filters_match_only_metadata_fields() {
    let mut map = HashMap::new();
    map.insert("principal_id".to_string(), "principal-a".to_string());
    map.insert("model".to_string(), "claude-test".to_string());
    map.insert("upstream".to_string(), "anthropic_direct".to_string());
    map.insert("status_class".to_string(), "2xx".to_string());
    let filters = parse_stream_filters(&map).unwrap();

    let matching = event();
    assert!(apply_filters_to_event(&matching, &filters));

    let mut wrong_principal = event();
    wrong_principal.principal_id = Some("principal-b".to_string());
    assert!(!apply_filters_to_event(&wrong_principal, &filters));

    let mut wrong_status = event();
    wrong_status.status = 404;
    assert!(!apply_filters_to_event(&wrong_status, &filters));
}

async fn read_request_sse(body: &mut Body) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut wire = String::new();
        loop {
            let frame = body
                .frame()
                .await
                .expect("SSE response remains open")
                .expect("SSE frame is readable");
            if let Ok(data) = frame.into_data() {
                wire.push_str(std::str::from_utf8(&data).expect("SSE frame is UTF-8"));
                if wire.contains("event: request") && wire.contains("\n\n") {
                    return wire;
                }
            }
        }
    })
    .await
    .expect("request SSE event arrives")
}

fn event() -> RequestEvent {
    RequestEvent {
        ts: 1,
        request_id: "req-stream".to_string(),
        principal_id: Some("principal-a".to_string()),
        principal_kind: Some("api_key".to_string()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some("claude-test".to_string()),
        status: 200,
        input_tokens: Some(11),
        output_tokens: Some(7),
        duration_ms: 33,
        error_code: None,
    }
}

fn forbidden_json_keys() -> Vec<String> {
    vec![
        ["mes", "sages"].concat(),
        ["sys", "tem"].concat(),
        ["too", "ls"].concat(),
        ["tool", "_use"].concat(),
        ["con", "tent"].concat(),
    ]
}

fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path).unwrap());
    std::mem::forget(dir);
    storage
}
