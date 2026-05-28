use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{
    AuthStrategy, Config, Limit, LimitKind, PrincipalSpec, PrincipalType, UpstreamKind,
    UpstreamSpec,
};
use cc_lb_core::api_keys::{
    concurrent_guard::KeyConcurrencyManager, limit_engine::LimitEngine,
    principal_view::PrincipalView,
};
use cc_lb_storage_redb::Storage;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

fn test_config() -> Config {
    let mut config = Config::default();
    config.aead.key_env = "SECRET_STORAGE_KEY".to_string();
    config.admin.token_env = "SECRET_ADMIN_TOKEN".to_string();
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
            principal_type: PrincipalType::Machine,
            default_limits: vec![
                Limit {
                    kind: LimitKind::Requests,
                    window: Duration::from_secs(3_600),
                    cap_micros: 123,
                },
                Limit {
                    kind: LimitKind::InputTokens,
                    window: Duration::from_secs(3_600),
                    cap_micros: 456,
                },
                Limit {
                    kind: LimitKind::OutputTokens,
                    window: Duration::from_secs(3_600),
                    cap_micros: 789,
                },
            ],
            enabled: true,
            allowed_models: vec!["claude-3-5-sonnet".to_string()],
            credentials_ref: Some("anthropic-key".to_string()),
            router_plugin: None,
            observability_hooks: None,
        },
    );
    config
}

fn test_state(config: Config, storage: Option<Arc<Storage>>) -> AdminState {
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&config, std::collections::HashMap::new())
            .expect("principal view builds"),
    ));
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        principal_view.clone(),
    );
    AdminState {
        storage: storage.map(|s| s as Arc<dyn cc_lb_storage_api::Storage>),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine,
        lifecycle: None,
        audit_sink: None,
        principal_view,
        config: Arc::new(config),
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
    let app = router(test_state(test_config(), None));
    let json = json_response(app, "GET", "/admin/config/current").await;

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn snapshot_admin_principals() {
    let app = router(test_state(test_config(), None));
    let json = json_response(app, "GET", "/admin/principals").await;

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn snapshot_admin_upstreams() {
    let app = router(test_state(test_config(), None));
    let json = json_response(app, "GET", "/admin/upstreams").await;

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn snapshot_admin_principal_limits() {
    let app = router(test_state(test_config(), None));
    let mut json = json_response(app, "GET", "/admin/principals/alice/limits").await;
    scrub_limit_timestamps(&mut json);

    insta::assert_snapshot!(serde_json::to_string_pretty(&json).unwrap());
}

fn scrub_limit_timestamps(value: &mut Value) {
    let Some(identities) = value.get_mut("identities").and_then(Value::as_array_mut) else {
        return;
    };
    for identity in identities {
        let Some(windows) = identity.get_mut("windows").and_then(Value::as_array_mut) else {
            continue;
        };
        for window in windows {
            let Some(snapshots) = window.get_mut("snapshots").and_then(Value::as_array_mut) else {
                continue;
            };
            for snapshot in snapshots {
                snapshot["observed_at_unix_secs"] = serde_json::json!(0);
                snapshot["stored_at_unix_secs"] = serde_json::json!(0);
                snapshot["reset"] = serde_json::json!("1970-01-01T00:00:00Z");
            }
        }
    }
}
