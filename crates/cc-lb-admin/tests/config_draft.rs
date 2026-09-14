use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_storage_api::{ConfigDraftState, ConfigStore};
use config_admin_common::{
    app, authed_json, config_value, expected_revision_body, put_body, temp_storage, test_state,
};
use serde_json::json;

#[tokio::test]
async fn get_no_draft_returns_zero_revision_and_null_payload() {
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let (status, _, json, _) = authed_json(app, "GET", "/admin/config/draft", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["draft"], serde_json::Value::Null);
    assert_eq!(json["revision"], 0);
    assert_eq!(json["last_validated_revision"], serde_json::Value::Null);
    assert_eq!(json["last_validation_error"], serde_json::Value::Null);
    assert_eq!(json["saved_at_unix_secs"], serde_json::Value::Null);
}

#[tokio::test]
async fn put_draft_from_zero_revision_saves_invalid_json_without_validation() {
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let draft = json!({ "not_config": true });
    let (status, _, json, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(draft.clone(), 0)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["revision"], 1);
    assert!(json["saved_at_unix_secs"].as_u64().unwrap() > 0);

    let (_, _, json, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(json["draft"], draft);
    assert_eq!(json["revision"], 1);
}

#[tokio::test]
async fn stale_put_returns_current_revision_conflict() {
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(json!({ "not_config": true }), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "PUT",
        "/admin/config/draft",
        Some(put_body(json!({ "other": true }), 0)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
    assert_eq!(json["current_revision"], 1);
}

#[tokio::test]
async fn saving_new_draft_invalidates_last_validated_revision() {
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let (status, _, first, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(100), 0)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["revision"], 1);

    let (status, _, validated, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(validated["valid"], true);

    let (status, _, second, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(200), 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["revision"], 2);

    let (_, _, json, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(json["revision"], 2);
    assert_eq!(json["last_validated_revision"], serde_json::Value::Null);
    assert_eq!(json["last_validation_error"], serde_json::Value::Null);
    assert_eq!(json["draft"]["timeouts"]["idle_secs"], 200);
}

#[tokio::test]
async fn validate_rejects_database_owned_top_level_keys() {
    for key in [
        "principals",
        "upstreams",
        "plugins",
        "plugin_chains",
        "quotas",
    ] {
        let (_dir, storage) = temp_storage().await;
        let app = app(test_state(
            config_admin_common::minimal_config(),
            Some(storage),
        ));
        let mut draft = serde_json::Map::new();
        draft.insert(key.to_owned(), json!({}));

        let (status, _, saved, _) = authed_json(
            app.clone(),
            "PUT",
            "/admin/config/draft",
            Some(put_body(serde_json::Value::Object(draft), 0)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _, validated, _) = authed_json(
            app,
            "POST",
            "/admin/config/draft/validate",
            Some(expected_revision_body(saved["revision"].as_u64().unwrap())),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(validated["valid"], false);
        let error = validated["error"].as_str().unwrap();
        assert!(error.contains("unknown field"));
        assert!(error.contains(key));
    }
}

#[tokio::test]
async fn get_draft_purges_expired_invalid_draft() {
    let (_dir, storage) = temp_storage().await;
    storage
        .put_config_draft(
            ConfigDraftState {
                draft: Some(json!({ "some": "bad" })),
                revision: 0,
                last_validated_revision: None,
                last_validation_error: None,
                saved_at_unix_secs: Some(1),
            },
            0,
        )
        .await
        .unwrap();
    storage
        .set_last_validated_revision(1, Some("unknown field `some`".to_owned()))
        .await
        .unwrap();
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let (status, _, body, _) = authed_json(app, "GET", "/admin/config/draft", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["draft"], serde_json::Value::Null);
    assert_eq!(body["last_validation_error"], serde_json::Value::Null);
    assert_eq!(body["saved_at_unix_secs"], serde_json::Value::Null);
}
