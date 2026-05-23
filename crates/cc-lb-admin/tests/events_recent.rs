use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::{RequestEvent, RequestEventUpstream, Storage};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn authorized_empty_storage_returns_unobserved_empty_events() {
    let app = router(test_state(None));
    let (status, json, _) = authorized_json(app, "/admin/events/recent").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["events"].as_array().unwrap().len(), 0);
    assert_eq!(json["observed"], false);
    assert_eq!(json["count"], 0);
    assert_eq!(json["limit"], 100);
}

#[tokio::test]
async fn authorized_seeded_events_return_newest_first() {
    let (_dir, storage) = seeded_storage();
    let app = router(test_state(Some(storage)));
    let (status, json, _) = authorized_json(app, "/admin/events/recent?limit=10").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["observed"], true);
    assert_eq!(json["count"], 5);
    assert_eq!(json["limit"], 10);
    let events = json["events"].as_array().unwrap();
    let ids = request_ids(events);
    assert_eq!(ids, vec!["req-d", "req-e", "req-c", "req-b", "req-a"]);
}

#[tokio::test]
async fn recent_events_filter_by_principal_id() {
    let (_dir, storage) = seeded_storage();
    let app = router(test_state(Some(storage)));
    let (status, json, _) = authorized_json(
        app,
        "/admin/events/recent?limit=10&principal_id=principal-a",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        request_ids(json["events"].as_array().unwrap()),
        vec!["req-d", "req-a"]
    );
    assert!(
        json["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["principal_id"] == "principal-a")
    );
}

#[tokio::test]
async fn recent_events_filter_by_status_class() {
    let (_dir, storage) = seeded_storage();
    let app = router(test_state(Some(storage)));
    let (status, json, _) = authorized_json(app, "/admin/events/recent?status_class=4xx").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        request_ids(json["events"].as_array().unwrap()),
        vec!["req-e", "req-b"]
    );
    assert!(json["events"].as_array().unwrap().iter().all(|event| {
        let status = event["status"].as_u64().unwrap();
        (400..=499).contains(&status)
    }));
}

#[tokio::test]
async fn recent_events_filter_by_upstream() {
    let (_dir, storage) = seeded_storage();
    let app = router(test_state(Some(storage)));
    let (status, json, _) =
        authorized_json(app, "/admin/events/recent?upstream=anthropic_direct").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        request_ids(json["events"].as_array().unwrap()),
        vec!["req-e", "req-a"]
    );
    assert!(
        json["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["upstream"] == "anthropic_direct")
    );
}

#[tokio::test]
async fn recent_events_filter_by_model() {
    let (_dir, storage) = seeded_storage();
    let app = router(test_state(Some(storage)));
    let (status, json, _) = authorized_json(app, "/admin/events/recent?model=claude-a").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        request_ids(json["events"].as_array().unwrap()),
        vec!["req-e", "req-a"]
    );
    assert!(
        json["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["model"] == "claude-a")
    );
}

#[tokio::test]
async fn recent_events_rejects_invalid_upstream() {
    let app = router(test_state(None));
    let (status, json, _) = authorized_json(app, "/admin/events/recent?upstream=garbage").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json, serde_json::json!({ "error": "invalid_upstream" }));
}

#[tokio::test]
async fn recent_events_rejects_invalid_status_class() {
    let app = router(test_state(None));
    let (status, json, _) = authorized_json(app, "/admin/events/recent?status_class=9xx").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json, serde_json::json!({ "error": "invalid_status_class" }));
}

#[tokio::test]
async fn recent_events_rejects_invalid_limits() {
    let app = router(test_state(None));

    let (status, json, _) = authorized_json(app.clone(), "/admin/events/recent?limit=0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json, serde_json::json!({ "error": "invalid_limit" }));

    let (status, json, _) = authorized_json(app, "/admin/events/recent?limit=1000").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json, serde_json::json!({ "error": "limit_too_large" }));
}

#[tokio::test]
async fn recent_events_requires_admin_auth() {
    let app = router(test_state(None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/recent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn recent_events_response_excludes_payload_terms() {
    let (_dir, storage) = seeded_storage();
    let app = router(test_state(Some(storage)));
    let (status, _, body) = authorized_json(app, "/admin/events/recent?limit=10").await;

    assert_eq!(status, StatusCode::OK);
    assert_forbidden_bytes_absent(&body);
}

fn test_state(storage: Option<Arc<Storage>>) -> AdminState {
    AdminState {
        storage,
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

fn seeded_storage() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(&dir.path().join("events.redb"), [43; 32]).unwrap();
    for event in [
        event(
            1_800_000_000,
            "req-a",
            "principal-a",
            "claude-a",
            200,
            RequestEventUpstream::AnthropicDirect,
        ),
        event(
            1_800_000_010,
            "req-b",
            "principal-b",
            "claude-b",
            404,
            RequestEventUpstream::BedrockRuntime,
        ),
        event(
            1_800_000_020,
            "req-c",
            "principal-c",
            "claude-c",
            302,
            RequestEventUpstream::Vertex,
        ),
        event(
            1_800_000_030,
            "req-d",
            "principal-a",
            "claude-d",
            503,
            RequestEventUpstream::BedrockMantle,
        ),
        event(
            1_800_000_030,
            "req-e",
            "principal-e",
            "claude-a",
            429,
            RequestEventUpstream::AnthropicDirect,
        ),
    ] {
        storage.append_request_event(&event).unwrap();
    }
    (dir, Arc::new(storage))
}

fn event(
    ts: u64,
    request_id: &str,
    principal_id: &str,
    model: &str,
    status: u16,
    upstream: RequestEventUpstream,
) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal_id.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(upstream),
        model: Some(model.to_owned()),
        status,
        input_tokens: Some(11),
        output_tokens: Some(7),
        duration_ms: 33,
        error_code: if status >= 400 {
            Some("upstream_error".to_owned())
        } else {
            None
        },
    }
}

fn request_ids(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .map(|event| event["request_id"].as_str().unwrap())
        .collect()
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
    ]
}
