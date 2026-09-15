use crate::config_admin_common;

use std::fs;

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_json, temp_storage, test_state_with_config_path, unauthenticated_status,
    write_config_file,
};

#[tokio::test]
async fn editor_returns_schema_defaults_file_effective_and_metadata() {
    let (dir, storage) = temp_storage().await;
    let config = config_admin_common::minimal_config();
    let path = write_config_file(dir.path(), &config);
    let app = app(test_state_with_config_path(
        config,
        Some(storage),
        path.clone(),
    ));

    let (status, headers, json, _) = authed_json(app, "GET", "/admin/v1/config/editor", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    assert!(json["schema"]["properties"].is_object());
    assert!(json["default_config"].is_object());
    assert!(json["file_config"].is_object());
    assert!(json["effective_config"].is_object());
    assert_eq!(json["draft"], serde_json::Value::Null);
    assert_eq!(json["revision"], 0);
    assert_eq!(json["last_validation"], serde_json::Value::Null);
    assert_eq!(json["file"]["path"], path.display().to_string());
    assert_eq!(json["file"]["exists"], true);
    assert_eq!(json["file"]["mode"], "writable");
    assert!(
        json["file"]["fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["restart_required"], true);
}

#[tokio::test]
async fn editor_reports_missing_file_with_writable_parent() {
    let (dir, storage) = temp_storage().await;
    let path = dir.path().join("missing.toml");
    let config = config_admin_common::minimal_config();
    let app = app(test_state_with_config_path(config, Some(storage), path));

    let (_, _, json, _) = authed_json(app, "GET", "/admin/v1/config/editor", None).await;

    assert_eq!(json["file"]["exists"], false);
    assert_eq!(json["file"]["mode"], "writable");
    assert_eq!(json["file"]["fingerprint"], serde_json::Value::Null);
    assert_eq!(json["file_config"], serde_json::json!({}));
}

#[cfg(unix)]
#[tokio::test]
async fn editor_reports_read_only_parent() {
    use std::os::unix::fs::PermissionsExt;

    let (dir, storage) = temp_storage().await;
    let config_dir = dir.path().join("readonly");
    fs::create_dir(&config_dir).unwrap();
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o555)).unwrap();
    let path = config_dir.join("cc-lb.toml");
    let config = config_admin_common::minimal_config();
    let app = app(test_state_with_config_path(config, Some(storage), path));

    let (_, _, json, _) = authed_json(app, "GET", "/admin/v1/config/editor", None).await;

    assert_eq!(json["file"]["mode"], "read_only");
    assert!(
        json["file"]["reason"]
            .as_str()
            .unwrap()
            .contains("read-only")
    );
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn editor_redacts_storage_url_as_whole_value() {
    let (dir, storage) = temp_storage().await;
    let secret = "postgres://secret-user:secret-password@localhost/secret-db";
    let raw = format!(
        "[storage]\nkind = \"postgres\"\nurl = \"{secret}\"\n\n[cluster]\ninstance_url = \"https://node.example\"\n"
    );
    let path = dir.path().join("cc-lb.toml");
    fs::write(&path, &raw).unwrap();
    let config = cc_lb_config::Config::from_stored_toml(&raw).unwrap();
    let app = app(test_state_with_config_path(config, Some(storage), path));

    let (_, _, json, bytes) = authed_json(app, "GET", "/admin/v1/config/editor", None).await;

    assert_eq!(
        json["file_config"]["storage"]["url"],
        cc_lb_config::STORAGE_URL_REDACTION_SENTINEL
    );
    assert_eq!(
        json["effective_config"]["storage"]["url"],
        cc_lb_config::STORAGE_URL_REDACTION_SENTINEL
    );
    assert!(!String::from_utf8_lossy(&bytes).contains(secret));
}

#[tokio::test]
async fn legacy_current_and_schema_routes_are_removed() {
    let (dir, storage) = temp_storage().await;
    let config = config_admin_common::minimal_config();
    let path = write_config_file(dir.path(), &config);
    let state = test_state_with_config_path(config, Some(storage), path);

    for route in ["/admin/v1/config/current", "/admin/v1/config/schema"] {
        let (status, _, _) =
            config_admin_common::authed_bytes(app(state.clone()), "GET", route, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn editor_requires_admin_auth() {
    let (dir, storage) = temp_storage().await;
    let config = config_admin_common::minimal_config();
    let path = write_config_file(dir.path(), &config);
    let app = app(test_state_with_config_path(config, Some(storage), path));

    assert_eq!(
        unauthenticated_status(app, "GET", "/admin/v1/config/editor").await,
        StatusCode::UNAUTHORIZED
    );
}
