mod config_admin_common;

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_json, config_value, expected_revision_body, put_body, temp_storage, test_state,
    test_state_without_storage,
};
use serde_json::json;

#[tokio::test]
async fn invalid_draft_validate_reports_false_and_keeps_last_validated_revision() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(json!({ "upstreams": "wrong-type" }), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(1)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["valid"], false);
    assert_eq!(json["revision"], 1);
    assert!(!json["error"].as_str().unwrap().is_empty());

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(draft["last_validated_revision"], serde_json::Value::Null);
    assert!(!draft["last_validation_error"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn valid_draft_validate_marks_current_revision_valid() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(123), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(1)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["valid"], true);
    assert_eq!(json["revision"], 1);
    assert!(json.get("error").is_none());

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(draft["last_validated_revision"], 1);
    assert_eq!(draft["last_validation_error"], serde_json::Value::Null);
}

#[tokio::test]
async fn stale_validate_revision_returns_conflict() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(123), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(0)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
}

#[tokio::test]
async fn validate_without_storage_returns_unavailable() {
    let app = app(test_state_without_storage());
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(0)),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["error"], "storage_unavailable");
}
