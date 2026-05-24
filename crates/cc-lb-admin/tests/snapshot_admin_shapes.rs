use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{AuthStrategy, Config, PrincipalSpec, QuotasConfig, UpstreamKind, UpstreamSpec};
use cc_lb_core::{DashboardBroadcaster, QuotaManager, QuotaPolicy};
use cc_lb_storage_redb::{BucketKind, RedbStorage};
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

fn test_config() -> Config {
    let mut config = Config::default();
    config.signers.anthropic_oauth.scopes = vec![
        "placeholder-scope".to_string(),
        "placeholder-scope-2".to_string(),
    ];
    config.aead.key_env = "SECRET_STORAGE_KEY".to_string();
    config.admin.token_env = "SECRET_ADMIN_TOKEN".to_string();
    config.quotas = QuotasConfig {
        default_window_secs: 60,
        default_requests_per_window: 1_000,
        default_input_tokens: 1_000_000,
        default_output_tokens: 1_000_000,
    };
    config.upstreams.insert(
        "anthropic".to_string(),
        UpstreamSpec {
            kind: UpstreamKind::AnthropicDirect,
            base_url: Some(url::Url::parse("https://api.anthropic.com").unwrap()),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: Some("anthropic-key".to_string()),
        },
    );
    config.principals.insert(
        "alice".to_string(),
        PrincipalSpec {
            quotas: Some(QuotasConfig {
                default_window_secs: 3_600,
                default_requests_per_window: 123,
                default_input_tokens: 456,
                default_output_tokens: 789,
            }),
            disabled: None,
            allowed_models: vec!["claude-3-5-sonnet".to_string()],
            credentials_ref: Some("anthropic-key".to_string()),
        },
    );
    config
}

fn test_state(
    config: Config,
    storage: Option<Arc<RedbStorage>>,
    quota_manager: Option<Arc<QuotaManager>>,
) -> AdminState {
    AdminState {
        storage: storage.unwrap_or_else(test_storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager,
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

async fn json_response(app: axum::Router, method: &str, uri: &str) -> Value {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn snapshot_admin_config_current() {
    let app = router(test_state(test_config(), None, None));
    let mut json = json_response(app, "GET", "/admin/config/current").await;
    json["effective_revision_unix_secs"] = serde_json::json!(0);

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn snapshot_admin_principals() {
    let app = router(test_state(test_config(), None, None));
    let json = json_response(app, "GET", "/admin/principals").await;

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn snapshot_admin_upstreams() {
    let app = router(test_state(test_config(), None, None));
    let json = json_response(app, "GET", "/admin/upstreams").await;

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn snapshot_admin_quota() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&db_path).unwrap());
    let quota_policy = QuotaPolicy {
        window_secs: 3_600,
        capacity_requests: 123,
        capacity_input_tokens: 456,
        capacity_output_tokens: 789,
    };
    let quota_manager = Arc::new(QuotaManager::new(storage.clone(), quota_policy));
    quota_manager
        .set_principal_policy("alice", quota_policy)
        .await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let window_start = (now / quota_policy.window_secs) * quota_policy.window_secs;
    storage
        .incr_quota("alice", window_start, BucketKind::Requests, 2)
        .unwrap();
    storage
        .incr_quota("alice", window_start, BucketKind::InputTokens, 3)
        .unwrap();
    storage
        .incr_quota("alice", window_start, BucketKind::OutputTokens, 4)
        .unwrap();

    let app = router(test_state(
        test_config(),
        Some(storage),
        Some(quota_manager),
    ));
    let mut json = json_response(app, "GET", "/admin/principals/alice/quota").await;
    json["window_start"] = serde_json::json!(0);

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path).unwrap());
    std::mem::forget(dir);
    storage
}
