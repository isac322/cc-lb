use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_storage_api::{ConfigDraftState, ConfigStore};
use config_admin_common::{
    app, authed_json, config_value, put_body, temp_storage, test_state_with_config_path,
    write_config_file,
};
use serde_json::json;

fn app_with_file(
    dir: &tempfile::TempDir,
    storage: std::sync::Arc<cc_lb_storage_sqlite::SqliteStorage>,
) -> axum::Router {
    let config = config_admin_common::minimal_config();
    let path = write_config_file(dir.path(), &config);
    app(test_state_with_config_path(config, Some(storage), path))
}

#[tokio::test]
async fn get_no_draft_returns_zero_revision_and_null_payload() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);

    let (status, headers, json, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    assert_eq!(json["draft"], serde_json::Value::Null);
    assert_eq!(json["revision"], 0);
    assert_eq!(json["last_validated_revision"], serde_json::Value::Null);
    assert_eq!(json["last_validation"], serde_json::Value::Null);
    assert_eq!(json["saved_at_unix_secs"], serde_json::Value::Null);
}

#[tokio::test]
async fn put_draft_saves_partial_json_without_validation_and_normalizes_nulls() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let draft = json!({
        "listener": { "admin_addr": null },
        "not_config": true,
    });

    let (status, _, saved, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(saved["revision"], 1);
    let (_, _, stored, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;
    assert_eq!(
        stored["draft"],
        json!({ "listener": {}, "not_config": true })
    );
}

#[tokio::test]
async fn put_draft_stores_only_storage_url_sentinel() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let secret = "postgres://replacement-user:replacement-password@localhost/db";

    let (_, _, saved, bytes) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(
            json!({ "storage": { "kind": "postgres", "url": secret } }),
            0,
        )),
    )
    .await;
    assert_eq!(saved["revision"], 1);
    assert!(!String::from_utf8_lossy(&bytes).contains(secret));

    let (_, _, stored, bytes) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;
    assert_eq!(
        stored["draft"]["storage"]["url"],
        cc_lb_config::STORAGE_URL_REDACTION_SENTINEL
    );
    assert!(!String::from_utf8_lossy(&bytes).contains(secret));
}

#[tokio::test]
async fn stale_put_returns_current_revision_conflict() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(json!({ "first": true }), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(json!({ "second": true }), 0)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
    assert_eq!(json["current_revision"], 1);
}

#[tokio::test]
async fn saving_new_draft_invalidates_prior_validation() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);

    let (_, _, first, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(100), 0)),
    )
    .await;
    let (_, _, validated, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": first["revision"] })),
    )
    .await;
    assert_eq!(validated["file"]["valid"], true);

    let (_, _, second, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(200), 1)),
    )
    .await;
    assert_eq!(second["revision"], 2);

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;
    assert_eq!(draft["last_validated_revision"], serde_json::Value::Null);
    assert_eq!(draft["last_validation"], serde_json::Value::Null);
}

#[tokio::test]
async fn get_draft_purges_expired_invalid_draft() {
    let (dir, storage) = temp_storage().await;
    storage
        .put_config_draft(
            ConfigDraftState {
                draft: Some(json!({ "some": "bad" })),
                revision: 0,
                last_validated_revision: None,
                last_validation: None,
                saved_at_unix_secs: Some(1),
            },
            0,
        )
        .await
        .unwrap();
    let validation = json!({
        "revision": 1,
        "file": { "valid": false, "issues": [] },
        "effective": { "valid": false, "issues": [] },
        "filesystem": [],
        "overrides": [],
    });
    storage
        .set_config_validation(1, false, validation)
        .await
        .unwrap();
    let app = app_with_file(&dir, storage);

    let (_, _, body, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;

    assert_eq!(body["draft"], serde_json::Value::Null);
    assert_eq!(body["last_validation"], serde_json::Value::Null);
    assert_eq!(body["saved_at_unix_secs"], serde_json::Value::Null);
}
