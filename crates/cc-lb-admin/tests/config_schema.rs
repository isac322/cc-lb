use crate::config_admin_common;

use axum::http::StatusCode;
use config_admin_common::{app, authed_json, test_state_without_storage, unauthenticated_status};

#[tokio::test]
async fn authorized_schema_returns_schema_and_checklist() {
    let app = app(test_state_without_storage());
    let (status, _, json, _) = authed_json(app, "GET", "/admin/config/schema", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json.get("schema").is_some());
    assert!(json["coverage_checklist"].as_array().unwrap().len() >= 16);
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
