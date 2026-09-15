use crate::admin_test_common;

use axum::http::StatusCode;
use std::collections::HashSet;

#[tokio::test]
async fn static_token_actor_fields_are_recorded_and_queryable() {
    let server = admin_test_common::spawn_admin_server().await;

    let draft =
        serde_json::to_value(cc_lb_config::Config::default()).expect("default config serializes");
    let (status, _, _) = server
        .client
        .json(
            "PUT",
            "/admin/v1/config/draft",
            Some(serde_json::json!({
                "draft": draft,
                "expected_revision": 0,
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, audit) = server.client.get("/admin/v1/audit").await;
    assert_eq!(status, StatusCode::OK);
    let config_draft = audit["entries"]
        .as_array()
        .expect("audit entries array")
        .iter()
        .find(|entry| entry["admin_action"] == "config_draft_put")
        .expect("config draft audit entry");
    assert_eq!(config_draft["actor_authority"], "static-token");
    assert_eq!(config_draft["actor_subject"], "test-static-token");
    assert_eq!(config_draft["actor_kind"], "break_glass");

    let (status, _, filtered) = server
        .client
        .get("/admin/v1/audit?actor_authority=static-token&actor_subject=test-static-token")
        .await;
    assert_eq!(status, StatusCode::OK);
    let entries = filtered["entries"].as_array().expect("audit entries array");
    assert_eq!(entries.len(), 2);
    let actions = entries
        .iter()
        .filter_map(|entry| entry["admin_action"].as_str())
        .collect::<HashSet<_>>();
    assert_eq!(actions, HashSet::from(["config_draft_put", "audit_query"]));
    assert!(entries.iter().all(|entry| {
        entry["actor_authority"] == "static-token"
            && entry["actor_subject"] == "test-static-token"
            && entry["actor_kind"] == "break_glass"
    }));

    let (status, _, other_actor) = server
        .client
        .get("/admin/v1/audit?actor_authority=static-token&actor_subject=other")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        other_actor["entries"]
            .as_array()
            .expect("audit entries array")
            .len(),
        0
    );

    let (status, _, invalid_combination) = server
        .client
        .get(
            "/admin/v1/audit?principal_id=principal-a&actor_authority=static-token&actor_subject=test-static-token",
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_combination["error"], "validation_failed");
    assert_eq!(invalid_combination["field"], "principal_id");
}
