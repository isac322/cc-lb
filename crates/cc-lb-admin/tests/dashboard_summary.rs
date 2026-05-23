use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::{RedbStorage, RequestEvent, RequestEventUpstream};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn authorized_summary_returns_totals_and_zero_filled_sparkline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dashboard.redb");
    let storage = RedbStorage::open(&path).unwrap();
    let base = current_minute_base();
    seed_summary_events(&storage, base);
    storage.rollup_usage_once().unwrap();

    let app = router(test_state(Some(Arc::new(storage))));
    let (status, json, _) = authorized_json(app, "/admin/dashboard/summary?range=1h").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["range"], "1h");
    assert_eq!(json["step"], "minute");
    assert_eq!(json["observed"], true);
    assert_eq!(json["totals"]["request_count"], 3);
    assert_eq!(json["totals"]["input_tokens"], 22);
    assert_eq!(json["totals"]["output_tokens"], 28);
    assert_eq!(json["totals"]["error_count"], 1);
    assert_eq!(json["totals"]["virtual_cost_micros"], 486);
    assert_f64(json["totals"]["error_rate"].as_f64().unwrap(), 1.0 / 3.0);
    assert_f64(json["totals"]["avg_latency_ms"].as_f64().unwrap(), 150.0);

    let buckets = json["sparkline"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 60);
    assert_contiguous(buckets, 60);

    let first = find_bucket(buckets, base);
    assert_eq!(first["request_count"], 2);
    assert_eq!(first["input_tokens"], 15);
    assert_eq!(first["output_tokens"], 20);
    assert_eq!(first["error_count"], 1);
    assert_eq!(first["latency_ms_sum"], 400);
    assert_eq!(first["latency_count"], 2);
    assert_eq!(first["virtual_cost_micros"], 345);

    let second = find_bucket(buckets, base + 60);
    assert_eq!(second["request_count"], 1);
    assert_eq!(second["input_tokens"], 7);
    assert_eq!(second["output_tokens"], 8);
    assert_eq!(second["error_count"], 0);
    assert_eq!(second["latency_ms_sum"], 50);
    assert_eq!(second["latency_count"], 1);
    assert_eq!(second["virtual_cost_micros"], 141);

    let non_zero_buckets = buckets
        .iter()
        .filter(|bucket| bucket["request_count"].as_u64().unwrap() > 0)
        .count();
    assert_eq!(non_zero_buckets, 2);
}

#[tokio::test]
async fn summary_empty_storage_returns_zero_buckets() {
    let app = router(test_state(None));
    let (status, json, _) = authorized_json(app, "/admin/dashboard/summary?range=15m").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["observed"], false);
    assert_eq!(json["totals"]["request_count"], 0);
    assert_eq!(json["totals"]["input_tokens"], 0);
    assert_eq!(json["totals"]["output_tokens"], 0);
    assert_eq!(json["totals"]["error_count"], 0);
    assert_eq!(json["totals"]["error_rate"], 0.0);
    assert_eq!(json["totals"]["avg_latency_ms"], 0.0);

    let buckets = json["sparkline"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 15);
    assert_contiguous(buckets, 60);
    assert!(
        buckets
            .iter()
            .all(|bucket| bucket["request_count"].as_u64().unwrap() == 0)
    );
}

#[tokio::test]
async fn summary_requires_admin_auth() {
    let app = router(test_state(None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/dashboard/summary?range=1h")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn summary_rejects_invalid_range() {
    let app = router(test_state(None));
    let (status, json, _) = authorized_json(app, "/admin/dashboard/summary?range=999h").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_range");
}

#[tokio::test]
async fn summary_response_excludes_payload_terms() {
    let app = router(test_state(None));
    let (status, _, body) = authorized_json(app, "/admin/dashboard/summary?range=15m").await;

    assert_eq!(status, StatusCode::OK);
    assert_forbidden_bytes_absent(&body);
}

fn test_state(storage: Option<Arc<RedbStorage>>) -> AdminState {
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

fn current_minute_base() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now - (now % 60) - 180
}

fn seed_summary_events(storage: &Storage, base: u64) {
    for event in [
        event(
            base + 5,
            "req-summary-1",
            "principal-a",
            "claude-sonnet-4-5",
            200,
            Some(10),
            Some(20),
            100,
        ),
        event(
            base + 30,
            "req-summary-2",
            "principal-a",
            "claude-sonnet-4-5",
            429,
            Some(5),
            None,
            300,
        ),
        event(
            base + 65,
            "req-summary-3",
            "principal-a",
            "claude-sonnet-4-5",
            200,
            Some(7),
            Some(8),
            50,
        ),
    ] {
        storage.append_request_event(&event).unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
fn event(
    ts: u64,
    request_id: &str,
    principal: &str,
    model: &str,
    status: u16,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    duration_ms: u64,
) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some(model.to_owned()),
        status,
        input_tokens,
        output_tokens,
        duration_ms,
        error_code: None,
    }
}

fn assert_contiguous(buckets: &[Value], step: u64) {
    let first = buckets[0]["bucket_start_unix_secs"].as_u64().unwrap();
    for (index, bucket) in buckets.iter().enumerate() {
        assert_eq!(
            bucket["bucket_start_unix_secs"].as_u64().unwrap(),
            first + (index as u64 * step)
        );
    }
}

fn find_bucket(buckets: &[Value], bucket_start: u64) -> &Value {
    buckets
        .iter()
        .find(|bucket| bucket["bucket_start_unix_secs"].as_u64().unwrap() == bucket_start)
        .expect("bucket exists")
}

fn assert_f64(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_001,
        "{actual} != {expected}"
    );
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
