use crate::config_admin_common;

use std::{fs, process::Command};

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_bytes, authed_json, put_body, temp_storage, test_state_with_config_path,
};
use serde_json::json;

const EFFECTIVE_FAILURE_CHILD: &str = "CC_LB_CONFIG_DOWNLOAD_EFFECTIVE_FAILURE_CHILD";

#[tokio::test]
async fn validated_download_restores_secret_only_in_attachment_and_audits_without_payload() {
    let (dir, storage) = temp_storage().await;
    let secret = "postgres://download-user:download-password@localhost/private-db";
    let raw = format!(
        "[storage]\nkind = \"postgres\"\nurl = \"{secret}\"\n\n[cluster]\ninstance_url = \"https://node.example\"\n"
    );
    let path = dir.path().join("cc-lb.toml");
    fs::write(&path, &raw).unwrap();
    let config = cc_lb_config::Config::from_stored_toml(&raw).unwrap();
    let app = app(test_state_with_config_path(config, Some(storage), path));
    let (_, _, editor, editor_bytes) =
        authed_json(app.clone(), "GET", "/admin/v1/config/editor", None).await;
    assert!(!String::from_utf8_lossy(&editor_bytes).contains(secret));
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(editor["file_config"].clone(), 0)),
    )
    .await;
    let (_, _, report, report_bytes) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;
    assert_eq!(report["file"]["valid"], true);
    assert!(!String::from_utf8_lossy(&report_bytes).contains(secret));

    let (status, headers, body) = authed_bytes(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/download",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    assert_eq!(
        headers.get("content-type").unwrap(),
        "application/toml; charset=utf-8"
    );
    assert!(
        headers
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("attachment")
    );
    let downloaded = String::from_utf8(body.to_vec()).unwrap();
    assert!(downloaded.contains(secret));
    let parsed = cc_lb_config::Config::from_stored_toml(&downloaded).unwrap();
    assert_eq!(parsed.storage, config_admin_storage(secret));

    let (_, _, audit, audit_bytes) = authed_json(app, "GET", "/admin/v1/audit", None).await;
    let entry = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["admin_action"] == "config_download")
        .unwrap();
    assert_eq!(entry["payload"], serde_json::Value::Null);
    assert!(!String::from_utf8_lossy(&audit_bytes).contains(secret));
}

#[tokio::test]
async fn unvalidated_or_stale_download_is_rejected() {
    let (dir, storage) = temp_storage().await;
    let config = config_admin_common::minimal_config();
    let path = config_admin_common::write_config_file(dir.path(), &config);
    let app = app(test_state_with_config_path(config, Some(storage), path));
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_admin_common::config_value(123), 0)),
    )
    .await;

    let (status, _, json, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/download",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "draft_not_validated");

    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/draft/download",
        Some(json!({ "expected_revision": 0 })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
}

#[tokio::test]
async fn download_remains_available_when_only_effective_validation_fails() {
    if std::env::var_os(EFFECTIVE_FAILURE_CHILD).is_some() {
        let (dir, storage) = temp_storage().await;
        let config = config_admin_common::minimal_config();
        let path = config_admin_common::write_config_file(dir.path(), &config);
        let app = app(test_state_with_config_path(config, Some(storage), path));
        let _ = authed_json(
            app.clone(),
            "PUT",
            "/admin/v1/config/draft",
            Some(put_body(config_admin_common::config_value(123), 0)),
        )
        .await;
        let (_, _, report, _) = authed_json(
            app.clone(),
            "POST",
            "/admin/v1/config/draft/validate",
            Some(json!({ "expected_revision": 1 })),
        )
        .await;
        assert_eq!(report["file"]["valid"], true);
        assert_eq!(report["effective"]["valid"], false);
        let (status, _, body) = authed_bytes(
            app,
            "POST",
            "/admin/v1/config/draft/download",
            Some(json!({ "expected_revision": 1 })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            cc_lb_config::Config::from_stored_toml(std::str::from_utf8(&body).unwrap()).is_ok()
        );
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("config_download::download_remains_available_when_only_effective_validation_fails")
        .env(EFFECTIVE_FAILURE_CHILD, "1")
        .env("CC_LB_UPSTREAM_AFFINITY__TTL_DAYS", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn config_admin_storage(secret: &str) -> cc_lb_config::StorageConfig {
    cc_lb_config::StorageConfig::Postgres {
        url: secret.to_owned(),
        pool: cc_lb_config::PostgresPoolConfig::default(),
    }
}
