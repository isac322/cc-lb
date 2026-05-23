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
async fn authorized_usage_returns_all_hour_series() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dashboard.redb");
    let storage = RedbStorage::open(&path).unwrap();
    let hour_bucket = current_hour_start() - 3_600;
    seed_usage_events(&storage, hour_bucket + 120);
    storage.rollup_usage_once().unwrap();

    let app = router(test_state(Some(Arc::new(storage))));
    let (status, json, _) = authorized_json(app, "/admin/usage?range=24h&step=hour").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["range"], "24h");
    assert_eq!(json["step"], "hour");
    assert_eq!(json["group_by"], "none");
    assert_eq!(json["observed"], true);

    let series = json["series"].as_array().unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["key"], "all");
    let buckets = series[0]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 24);
    assert_contiguous(buckets, 3_600);

    let bucket = find_bucket(buckets, hour_bucket);
    assert_eq!(bucket["request_count"], 3);
    assert_eq!(bucket["input_tokens"], 22);
    assert_eq!(bucket["output_tokens"], 28);
    assert_eq!(bucket["error_count"], 1);
    assert_eq!(bucket["latency_ms_sum"], 450);
    assert_eq!(bucket["latency_count"], 3);
    assert_eq!(bucket["virtual_cost_micros"], 486);
}

#[tokio::test]
async fn usage_empty_storage_returns_all_zero_series() {
    let app = router(test_state(None));
    let (status, json, _) = authorized_json(app, "/admin/usage?range=15m").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["observed"], false);
    let series = json["series"].as_array().unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["key"], "all");
    let buckets = series[0]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 15);
    assert_contiguous(buckets, 60);
    assert!(
        buckets
            .iter()
            .all(|bucket| bucket["request_count"].as_u64().unwrap() == 0)
    );
}

#[tokio::test]
async fn usage_requires_admin_auth() {
    let app = router(test_state(None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/usage?range=1h")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn usage_rejects_invalid_params() {
    let app = router(test_state(None));

    for (uri, error) in [
        ("/admin/usage?range=999h", "invalid_range"),
        (
            "/admin/usage?range=7d&step=minute",
            "step_too_fine_for_range",
        ),
        ("/admin/usage?range=1h&group_by=garbage", "invalid_group_by"),
    ] {
        let (status, json, _) = authorized_json(app.clone(), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"], error);
    }
}

#[tokio::test]
async fn usage_groups_by_model_with_contiguous_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dashboard.redb");
    let storage = RedbStorage::open(&path).unwrap();
    let base = current_minute_base();
    for (offset, model) in [(5, "model-a"), (10, "model-b"), (15, "model-c")] {
        storage
            .append_request_event(&event(
                base + offset,
                &format!("req-{model}"),
                "principal-a",
                model,
                200,
                Some(1),
                Some(2),
                25,
            ))
            .unwrap();
    }
    storage.rollup_usage_once().unwrap();

    let app = router(test_state(Some(Arc::new(storage))));
    let (status, json, _) = authorized_json(app, "/admin/usage?range=1h&group_by=model").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["group_by"], "model");
    assert_eq!(json.get("truncated_series_count"), None);

    let series = json["series"].as_array().unwrap();
    assert_eq!(series.len(), 3);
    let keys = series
        .iter()
        .map(|series| series["key"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["model-a", "model-b", "model-c"]);

    for item in series {
        let buckets = item["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 60);
        assert_contiguous(buckets, 60);
        assert_eq!(sum_requests(buckets), 1);
        assert_eq!(find_bucket(buckets, base)["request_count"], 1);
    }
}

#[tokio::test]
async fn usage_response_excludes_payload_terms() {
    let app = router(test_state(None));
    let (status, _, body) = authorized_json(app, "/admin/usage?range=15m").await;

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
    let now = unix_now_secs();
    now - (now % 60) - 180
}

fn current_hour_start() -> u64 {
    let now = unix_now_secs();
    now - (now % 3_600)
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn seed_usage_events(storage: &Storage, base: u64) {
    for event in [
        event(
            base + 5,
            "req-usage-1",
            "principal-a",
            "claude-sonnet-4-5",
            200,
            Some(10),
            Some(20),
            100,
        ),
        event(
            base + 30,
            "req-usage-2",
            "principal-a",
            "claude-sonnet-4-5",
            429,
            Some(5),
            None,
            300,
        ),
        event(
            base + 65,
            "req-usage-3",
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

fn sum_requests(buckets: &[Value]) -> u64 {
    buckets
        .iter()
        .map(|bucket| bucket["request_count"].as_u64().unwrap())
        .sum()
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
