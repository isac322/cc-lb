use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{AuthStrategy, Config, UpstreamKind, UpstreamSpec};
use cc_lb_core::{BreakerConfig, BreakerRegistry, DashboardBroadcaster, DrainController};
use cc_lb_storage_redb::RedbStorage;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn unknown_upstream_returns_404() {
    let app = router(test_state(
        test_config(),
        None,
        Some(Arc::new(BreakerRegistry::new())),
        None,
    ));
    let (status, json, _) = authorized_json(app, "/admin/upstreams/missing/health").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json, serde_json::json!({ "error": "unknown_upstream" }));
}

#[tokio::test]
async fn known_upstream_without_registered_breaker_is_unobserved() {
    let app = router(test_state(
        test_config(),
        None,
        Some(Arc::new(BreakerRegistry::new())),
        None,
    ));
    let (status, json, _) = authorized_json(app, "/admin/upstreams/anthropic/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["name"], "anthropic");
    assert_eq!(json["kind"], "anthropic_direct");
    assert_eq!(json["breaker"]["observed"], false);
    assert_eq!(json["breaker"]["state"], "unobserved");
}

#[tokio::test]
async fn registered_open_breaker_is_reported() {
    let registry = Arc::new(BreakerRegistry::new());
    let breaker = registry.breaker(
        "anthropic",
        BreakerConfig {
            failures_to_open: 1,
            failure_window: Duration::from_secs(60),
            half_open_after: Duration::from_secs(60),
            half_open_max_in_flight: 1,
        },
    );
    breaker.permit().unwrap().record_failure();

    let app = router(test_state(test_config(), None, Some(registry), None));
    let (status, json, _) = authorized_json(app, "/admin/upstreams/anthropic/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["breaker"]["observed"], true);
    assert_eq!(json["breaker"]["state"], "open");
    assert_eq!(json["breaker"]["failure_count"], 1);
}

#[tokio::test]
async fn killswitch_state_reflects_storage_value() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RedbStorage::open(&dir.path().join("status.redb")).unwrap());
    storage.set_killswitch_enabled(true).unwrap();

    let app = router(test_state(test_config(), Some(storage.clone()), None, None));
    let (status, json, _) = authorized_json(app, "/admin/upstreams/anthropic/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["killswitch"], true);

    storage.set_killswitch_enabled(false).unwrap();
    let app = router(test_state(test_config(), Some(storage), None, None));
    let (status, json, _) = authorized_json(app, "/admin/upstreams/anthropic/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["killswitch"], false);
}

#[tokio::test]
async fn drain_state_uses_global_drain_controller() {
    let drain_controller = DrainController::new();
    drain_controller.trigger();

    let app = router(test_state(
        test_config(),
        None,
        None,
        Some(drain_controller),
    ));
    let (status, json, _) = authorized_json(app, "/admin/upstreams/anthropic/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["drain"]["draining"], true);
    assert_eq!(json["drain"]["in_flight"], 0);
}

#[tokio::test]
async fn upstream_health_requires_admin_auth() {
    let app = router(test_state(test_config(), None, None, None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/upstreams/anthropic/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn upstream_health_response_excludes_payload_and_secret_terms() {
    let app = router(test_state(test_config(), None, None, None));
    let (status, _, body) = authorized_json(app, "/admin/upstreams/anthropic/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_forbidden_bytes_absent(&body);
}

fn test_state(
    config: Config,
    storage: Option<Arc<RedbStorage>>,
    breaker_registry: Option<Arc<BreakerRegistry>>,
    drain_controller: Option<DrainController>,
) -> AdminState {
    AdminState {
        storage: storage.unwrap_or_else(test_storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager: None,
        lifecycle: None,
        breaker_registry,
        drain_controller,
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

fn test_config() -> Config {
    let mut config = Config::default();
    config.upstreams.insert(
        "anthropic".to_string(),
        UpstreamSpec {
            kind: UpstreamKind::AnthropicDirect,
            base_url: Some(url::Url::parse("https://api.anthropic.com").unwrap()),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
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
        ["sk", "-ant"].concat(),
        ["ae", "ad"].concat(),
        ["refresh", "_token"].concat(),
    ]
}

fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path).unwrap());
    std::mem::forget(dir);
    storage
}
