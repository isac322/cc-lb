use crate::config_admin_common;

use std::fs;

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_json, config_value, put_body, temp_storage, test_state_with_config_path,
    write_config_file,
};
use serde_json::json;

async fn prepared_app(
    draft: serde_json::Value,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    axum::Router,
    serde_json::Value,
) {
    let (dir, storage) = temp_storage().await;
    let config = config_admin_common::minimal_config();
    let config_dir = dir.path().join("config");
    fs::create_dir(&config_dir).unwrap();
    let path = write_config_file(&config_dir, &config);
    let app = app(test_state_with_config_path(
        config,
        Some(storage),
        path.clone(),
    ));
    let (_, _, editor, _) = authed_json(app.clone(), "GET", "/admin/v1/config/editor", None).await;
    let (_, _, saved, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;
    let revision = saved["revision"].as_u64().unwrap();
    let (_, _, report, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": revision })),
    )
    .await;
    assert_eq!(report["file"]["valid"], true);
    assert_eq!(report["effective"]["valid"], true);
    (dir, path, app, editor)
}

#[tokio::test]
async fn save_atomically_replaces_file_preserves_runtime_and_clears_draft() {
    let mut draft = config_value(321);
    draft["runtime"]["data_dir"] = json!("/tmp/new-runtime-data");
    let (_dir, path, app, editor) = prepared_app(draft).await;

    let (status, _, response, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/save",
        Some(json!({
            "expected_revision": 1,
            "expected_fingerprint": editor["file"]["fingerprint"],
            "confirm_self_lockout": false,
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["revision"], 2);
    assert_eq!(response["restart_required"], true);
    assert!(
        response["fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    let saved = fs::read_to_string(path).unwrap();
    assert!(saved.contains("upstream_total_secs = 321"));

    let (_, _, editor_after, _) =
        authed_json(app.clone(), "GET", "/admin/v1/config/editor", None).await;
    assert_eq!(editor_after["draft"], serde_json::Value::Null);
    assert_eq!(editor_after["revision"], 2);
    assert_eq!(editor_after["restart_required"], true);
    assert_ne!(
        editor_after["file_config"]["runtime"]["data_dir"],
        editor_after["effective_config"]["runtime"]["data_dir"]
    );

    let (_, _, history, _) = authed_json(app, "GET", "/admin/v1/config/history", None).await;
    assert_eq!(history["entries"][0]["revision"], 2);
    assert!(
        history["entries"][0]["saved_at_unix_secs"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[tokio::test]
async fn save_rejects_fingerprint_conflict_and_records_failure_audit() {
    let (_dir, path, app, editor) = prepared_app(config_value(222)).await;
    fs::write(&path, "[listener]\nproxy_addr = \"[::]:8888\"\n").unwrap();

    let (status, _, response, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/save",
        Some(json!({
            "expected_revision": 1,
            "expected_fingerprint": editor["file"]["fingerprint"],
            "confirm_self_lockout": false,
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "file_changed");
    assert!(
        response["current_fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    let (_, _, audit, _) = authed_json(app, "GET", "/admin/v1/audit", None).await;
    let entry = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["admin_action"] == "config_save_failed")
        .unwrap();
    assert_eq!(entry["payload"]["reason"], "file_changed");
}

#[cfg(unix)]
#[tokio::test]
async fn save_rechecks_permission_capability_after_validation() {
    use std::os::unix::fs::PermissionsExt;

    let (_dir, path, app, editor) = prepared_app(config_value(444)).await;
    let config_dir = path.parent().unwrap();
    fs::set_permissions(config_dir, fs::Permissions::from_mode(0o555)).unwrap();

    let (status, _, response, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/save",
        Some(json!({
            "expected_revision": 1,
            "expected_fingerprint": editor["file"]["fingerprint"],
            "confirm_self_lockout": false,
        })),
    )
    .await;
    fs::set_permissions(config_dir, fs::Permissions::from_mode(0o755)).unwrap();

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "file_not_writable");
}

#[tokio::test]
async fn save_creates_missing_file_when_parent_supports_atomic_replace() {
    let (dir, storage) = temp_storage().await;
    let path = dir.path().join("new-config.toml");
    let config = config_admin_common::minimal_config();
    let app = app(test_state_with_config_path(
        config,
        Some(storage),
        path.clone(),
    ));
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(555), 0)),
    )
    .await;
    let _ = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;

    let (status, _, response, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/save",
        Some(json!({
            "expected_revision": 1,
            "expected_fingerprint": null,
            "confirm_self_lockout": false,
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(path.exists());
    assert!(
        response["fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[tokio::test]
async fn save_requires_confirmation_when_active_admin_provider_changes() {
    let (dir, storage) = temp_storage().await;
    let mut config = config_admin_common::minimal_config();
    config.admin.auth.providers = vec![cc_lb_config::AdminAuthProviderConfig::StaticToken {
        id: "test-static-token".to_owned(),
        token_env: "CC_LB_ADMIN_TOKEN".to_owned(),
    }];
    let path = write_config_file(dir.path(), &config);
    let app = app(test_state_with_config_path(
        config.clone(),
        Some(storage),
        path,
    ));
    let (_, _, editor, _) = authed_json(app.clone(), "GET", "/admin/v1/config/editor", None).await;
    let mut draft = serde_json::to_value(config).unwrap();
    draft["admin"]["auth"]["providers"] = json!([]);
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;
    let _ = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;

    let (status, _, response, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/save",
        Some(json!({
            "expected_revision": 1,
            "expected_fingerprint": editor["file"]["fingerprint"],
            "confirm_self_lockout": false,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "self_lockout_confirmation_required");

    let (status, _, _, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/save",
        Some(json!({
            "expected_revision": 1,
            "expected_fingerprint": editor["file"]["fingerprint"],
            "confirm_self_lockout": true,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}
