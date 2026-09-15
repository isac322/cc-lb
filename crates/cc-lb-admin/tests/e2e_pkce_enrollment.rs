use crate::config_admin_common;

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_json, temp_storage, test_state_with_config_path, write_config_file,
};

#[tokio::test]
async fn e2e_pkce_enrollment_v1_config_editor_smoke() {
    let (dir, storage) = temp_storage().await;
    let config = config_admin_common::minimal_config();
    let path = write_config_file(dir.path(), &config);
    let app = app(test_state_with_config_path(
        config,
        Some(storage),
        path.clone(),
    ));

    let (status, _, json, _) = authed_json(app, "GET", "/admin/v1/config/editor", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["file_config"].is_object());
    assert!(json["effective_config"].is_object());
    assert_eq!(json["revision"], 0);
    assert_eq!(json["draft"], serde_json::Value::Null);
    assert_eq!(json["file"]["path"], path.display().to_string());
    assert_eq!(json["file"]["exists"], true);
    assert_eq!(json["file"]["mode"], "writable");
    assert!(
        json["file"]["fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}
