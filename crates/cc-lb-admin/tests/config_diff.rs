use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::{AdminAuthProviderConfig, Config};
use cc_lb_storage_api::{ConfigStore, HistorySummary};

#[tokio::test]
async fn t2__config_diff_route_returns_history_difference() {
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
                HistorySummary {
                    upstreams: 0,
                    principals: 0,
                    plugin_count: 0,
                    tls_enabled: false,
                },
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
async fn t2__current_config_exposes_upstream_affinity_ttl_path() {
    let mut config = Config::default();
    config.upstream_affinity.ttl_days = 14;
    let app = config_admin_common::app(config_admin_common::test_state(
        config,
        None::<std::sync::Arc<cc_lb_testkit::InMemoryStorage>>,
    ));

    let (status, _, json, _) =
        config_admin_common::authed_json(app, "GET", "/admin/config/current", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["upstream_affinity"]["ttl_days"], 14);
}

#[tokio::test]
async fn t2__current_config_masks_nested_provider_token_env() {
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
    let app = config_admin_common::app(config_admin_common::test_state(
        config,
        None::<std::sync::Arc<cc_lb_testkit::InMemoryStorage>>,
    ));

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
