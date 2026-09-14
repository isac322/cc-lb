use crate::config_admin_common;

use axum::http::StatusCode;
use config_admin_common::{app, authed_json, test_state_without_storage, unauthenticated_status};

#[tokio::test]
async fn authorized_schema_returns_schema_and_checklist() {
    let app = app(test_state_without_storage());
    let (status, _, json, _) = authed_json(app, "GET", "/admin/config/schema", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.get("schema").is_some());
    assert_eq!(json["coverage_checklist"].as_array().unwrap().len(), 20);
}

#[tokio::test]
async fn schema_excludes_database_owned_resources() {
    let app = app(test_state_without_storage());
    let (_, _, json, _) = authed_json(app, "GET", "/admin/config/schema", None).await;
    let properties = json["schema"]["properties"].as_object().unwrap();

    for resource in [
        "principals",
        "upstreams",
        "plugins",
        "plugin_chains",
        "quotas",
    ] {
        assert!(
            !properties.contains_key(resource),
            "database-owned resource {resource} must not appear in config schema"
        );
    }
}

#[tokio::test]
async fn schema_matches_runtime_owned_config_contract() {
    let app = app(test_state_without_storage());
    let (_, _, json, _) = authed_json(app, "GET", "/admin/config/schema", None).await;
    let properties = json["schema"]["properties"].as_object().unwrap();
    let checklist = json["coverage_checklist"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        checklist,
        vec![
            "listener",
            "body",
            "timeouts",
            "request_event_retention_days",
            "price_catalog",
            "storage",
            "scheduler",
            "upstream_affinity",
            "aead",
            "observability",
            "admin",
            "event_bus",
            "cluster",
            "oauth",
            "subscription_quota",
            "runtime",
            "circuit_breaker",
            "bulkhead",
            "prompt_cache_shadow",
            "limit_reservation_ttl",
        ]
    );
    for removed in [
        "tls",
        "downstream_auth",
        "api_keys",
        "dns",
        "egress",
        "lifecycle_hook_adapter",
        "lifecycle_pricing_subscriber",
        "lifecycle_cache_observation_subscriber",
        "lifecycle_rate_limit_header_subscriber",
        "lifecycle_subscription_quota_subscriber",
        "lifecycle_limit_rejection_audit_subscriber",
        "lifecycle_api_key_metrics_subscriber",
        "lifecycle_cache_hit_miss_subscriber",
        "lifecycle_routing_tier_subscriber",
        "lifecycle_limit_reconcile_subscriber",
    ] {
        assert!(
            !properties.contains_key(removed),
            "removed config property {removed} remains in the schema"
        );
    }
    assert!(
        !json["schema"]["$defs"]["PriceCatalogConfig"]["properties"]
            .as_object()
            .unwrap()
            .contains_key("refresh_interval")
    );
    let listener_properties = json["schema"]["$defs"]["ListenerConfig"]["properties"]
        .as_object()
        .unwrap();
    assert!(listener_properties.contains_key("tls"));
    assert!(!listener_properties.contains_key("unix_socket"));
    assert_eq!(
        json["schema"]["$defs"]["AdminConfig"]["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["auth"]
    );
}

#[tokio::test]
async fn schema_and_coverage_include_upstream_affinity() {
    let app = app(test_state_without_storage());
    let (_, _, json, _) = authed_json(app, "GET", "/admin/config/schema", None).await;

    assert!(
        json["schema"]["properties"]
            .as_object()
            .unwrap()
            .contains_key("upstream_affinity")
    );
    assert!(
        json["coverage_checklist"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "upstream_affinity")
    );
    assert_eq!(
        json["schema"]["$defs"]["UpstreamAffinityConfig"]["properties"]["ttl_days"]["minimum"],
        1
    );
}

#[tokio::test]
async fn schema_contains_property_for_each_coverage_item() {
    let app = app(test_state_without_storage());
    let (_, _, json, _) = authed_json(app, "GET", "/admin/config/schema", None).await;
    let properties = json["schema"]["properties"].as_object().unwrap();

    for item in json["coverage_checklist"].as_array().unwrap() {
        let name = item.as_str().unwrap();
        assert!(
            properties.contains_key(name),
            "missing schema property {name}"
        );
    }
}

#[tokio::test]
async fn schema_response_is_cacheable_for_sixty_seconds() {
    let app = app(test_state_without_storage());
    let (status, headers, _, _) = authed_json(app, "GET", "/admin/config/schema", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("cache-control").unwrap(), "max-age=60");
}

#[tokio::test]
async fn schema_requires_admin_auth() {
    let app = app(test_state_without_storage());
    let status = unauthenticated_status(app, "GET", "/admin/config/schema").await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
