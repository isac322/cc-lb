use crate::admin_test_common;

use crate::config_admin_common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{AdminAuthProviderConfig, Config};
use cc_lb_storage_api::{ConfigStore, HistorySummary};
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

#[tokio::test]
async fn config_diff_route_returns_history_difference() {
    let (_dir, storage) = config_admin_common::temp_storage().await;
    let mut from_config = Config::default();
    from_config.body.messages_cap_bytes = 100;
    let mut to_config = Config::default();
    to_config.body.messages_cap_bytes = 200;
    for (revision, config) in [(1, &from_config), (2, &to_config)] {
        storage
            .append_config_history(
                revision,
                toml::to_string_pretty(config).unwrap(),
                1000 + revision,
                HistorySummary { tls_enabled: false },
            )
            .await
            .unwrap();
    }
    let app = config_admin_common::app(config_admin_common::test_state(
        Config::default(),
        Some(storage),
    ));

    let (status, _, json, _) = config_admin_common::authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=2",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["from"], 1);
    assert_eq!(json["to"], 2);
    assert!(json["diff"].as_array().unwrap().iter().any(|item| {
        item["path"] == "body.messages_cap_bytes" && item["from"] == 100 && item["to"] == 200
    }));
}

#[tokio::test]
async fn config_diff_reads_history_with_removed_prompt_cache_switches() {
    let (_dir, storage) = config_admin_common::temp_storage().await;
    storage
        .append_config_history(
            1,
            r#"
[body]
messages_cap_bytes = 100

[prompt_cache_shadow]
enabled = true

[lifecycle_prompt_cache_drift_subscriber]
enabled = true

[lifecycle_prompt_cache_observation_subscriber]
enabled = true
"#
            .to_owned(),
            1001,
            HistorySummary { tls_enabled: false },
        )
        .await
        .unwrap();
    let mut to_config = Config::default();
    to_config.body.messages_cap_bytes = 200;
    storage
        .append_config_history(
            2,
            toml::to_string_pretty(&to_config).unwrap(),
            1002,
            HistorySummary { tls_enabled: false },
        )
        .await
        .unwrap();
    let app = config_admin_common::app(config_admin_common::test_state(
        Config::default(),
        Some(storage),
    ));

    let (status, _, json, _) = config_admin_common::authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=2",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["diff"].as_array().unwrap().iter().any(|item| {
        item["path"] == "body.messages_cap_bytes" && item["from"] == 100 && item["to"] == 200
    }));
}

#[tokio::test]
async fn current_config_exposes_upstream_affinity_ttl_path() {
    let mut config = Config::default();
    config.upstream_affinity.ttl_days = 14;
    let app = config_admin_common::app(config_admin_common::test_state(config, None));

    let (status, _, json, _) =
        config_admin_common::authed_json(app, "GET", "/admin/config/current", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["upstream_affinity"]["ttl_days"], 14);
}

#[tokio::test]
async fn current_config_masks_nested_provider_token_env() {
    let mut config = Config::default();
    config.admin.token_env = "TOP_LEVEL_ADMIN_TOKEN_ENV".to_owned();
    config
        .admin
        .auth
        .providers
        .push(AdminAuthProviderConfig::StaticToken {
            id: "nested-static-token".to_owned(),
            token_env: "NESTED_PROVIDER_TOKEN_ENV".to_owned(),
        });
    let app = config_admin_common::app(config_admin_common::test_state(config, None));

    let (status, _, json, _) =
        config_admin_common::authed_json(app, "GET", "/admin/config/current", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["admin"]["token_env"], "[REDACTED]");
    assert_eq!(
        json["admin"]["auth"]["providers"][0]["token_env"],
        "[REDACTED]"
    );
    let response_body = json.to_string();
    assert!(!response_body.contains("TOP_LEVEL_ADMIN_TOKEN_ENV"));
    assert!(!response_body.contains("NESTED_PROVIDER_TOKEN_ENV"));
}

#[tokio::test]
async fn config_diff_current_admin_config_smoke() {
    let response = router(test_state())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/config/current")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
