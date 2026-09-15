use crate::config_admin_common;

use std::process::Command;

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_json, config_value, put_body, temp_storage, test_state_with_config_path,
    test_state_without_storage, write_config_file,
};
use serde_json::json;

const ENV_ONLY_CHILD: &str = "CC_LB_ADMIN_CONFIG_ENV_ONLY_CHILD";

fn app_with_file(
    dir: &tempfile::TempDir,
    storage: std::sync::Arc<cc_lb_storage_sqlite::SqliteStorage>,
) -> axum::Router {
    let config = config_admin_common::minimal_config();
    let path = write_config_file(dir.path(), &config);
    app(test_state_with_config_path(config, Some(storage), path))
}

#[tokio::test]
async fn invalid_draft_returns_structured_dual_validation_and_persists_report() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(json!({ "upstreams": "wrong-type" }), 0)),
    )
    .await;
    let (status, _, report, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(report["revision"], 1);
    assert_eq!(report["file"]["valid"], false);
    assert_eq!(report["effective"]["valid"], false);
    for issue in report["file"]["issues"].as_array().unwrap() {
        assert!(issue["path"].is_string());
        assert!(issue["code"].is_string());
        assert!(issue["message"].is_string());
        assert!(matches!(
            issue["severity"].as_str(),
            Some("error" | "warning")
        ));
    }

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;
    assert_eq!(draft["last_validated_revision"], serde_json::Value::Null);
    assert_eq!(draft["last_validation"], report);
}

#[tokio::test]
async fn valid_draft_marks_exact_revision_valid() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(123), 0)),
    )
    .await;
    let (_, _, report, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;
    let (_, _, draft, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;

    assert_eq!(report["file"]["valid"], true);
    assert_eq!(report["effective"]["valid"], true);
    assert!(report["filesystem"].as_array().unwrap().is_empty());
    assert_eq!(draft["last_validated_revision"], 1);
    assert_eq!(draft["last_validation"], report);
}

#[tokio::test]
async fn semantic_error_reports_exact_field_path() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let mut draft = config_value(123);
    draft["upstream_affinity"]["ttl_days"] = json!(0);
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;

    let (_, _, report, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;

    assert_eq!(
        report["file"]["issues"][0]["path"],
        "upstream_affinity.ttl_days"
    );
    assert_eq!(report["file"]["issues"][0]["severity"], "error");
}

#[tokio::test]
async fn unknown_recurring_scheduler_job_is_rejected_at_its_dynamic_path() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let mut draft = config_value(123);
    draft["scheduler"]["recurring_jobs"]["not_a_runtime_job"] = json!({
        "enabled": true,
        "interval_secs": 60,
        "jitter_secs": 0,
    });
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;
    let (_, _, report, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;

    assert_eq!(report["file"]["valid"], false);
    let file_issues = report["file"]["issues"].as_array().unwrap();
    assert_eq!(file_issues.len(), 1);
    assert_eq!(
        file_issues[0]["path"],
        "scheduler.recurring_jobs.not_a_runtime_job"
    );
    assert_eq!(file_issues[0]["code"], "unknown_recurring_job");

    let (_, _, persisted, _) = authed_json(app, "GET", "/admin/v1/config/draft", None).await;
    assert_eq!(
        persisted["draft"]["scheduler"]["recurring_jobs"]["not_a_runtime_job"],
        json!({
            "enabled": true,
            "interval_secs": 60,
            "jitter_secs": 0,
        })
    );
}

#[tokio::test]
async fn storage_url_sentinel_requires_file_source_or_transient_replacement() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let draft = json!({
        "storage": { "kind": "postgres", "url": "replacement is transient" },
        "cluster": { "instance_url": "https://node.example" },
    });
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;

    let (_, _, missing, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 1 })),
    )
    .await;
    assert_eq!(
        missing["file"]["issues"][0]["code"],
        "secret_source_missing"
    );

    let secret = "postgres://replacement-user:replacement-password@localhost/db";
    let (_, _, valid, bytes) = authed_json(
        app,
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({
            "expected_revision": 1,
            "storage_url_replacement": secret,
        })),
    )
    .await;
    assert_eq!(valid["file"]["valid"], true);
    assert_eq!(valid["effective"]["valid"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains(secret));
}

#[tokio::test]
async fn transient_storage_url_replacement_populates_an_absent_file_field() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let draft = json!({
        "storage": { "kind": "postgres" },
        "cluster": { "instance_url": "https://node.example" },
    });
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(draft, 0)),
    )
    .await;
    let secret = "postgres://new-user:new-password@localhost/db";
    let (_, _, report, bytes) = authed_json(
        app,
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({
            "expected_revision": 1,
            "storage_url_replacement": secret,
        })),
    )
    .await;

    assert_eq!(report["file"]["valid"], true);
    assert_eq!(report["effective"]["valid"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains(secret));
}

#[tokio::test]
async fn stale_validate_revision_returns_conflict() {
    let (dir, storage) = temp_storage().await;
    let app = app_with_file(&dir, storage);
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(123), 0)),
    )
    .await;

    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 0 })),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
    assert_eq!(json["current_revision"], 1);
}

#[tokio::test]
async fn validate_without_storage_returns_unavailable() {
    let app = app(test_state_without_storage());
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/v1/config/draft/validate",
        Some(json!({ "expected_revision": 0 })),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["error"], "storage_unavailable");
}

#[tokio::test]
async fn env_only_required_value_is_a_file_warning_and_valid_effective_value() {
    if std::env::var_os(ENV_ONLY_CHILD).is_some() {
        let (dir, storage) = temp_storage().await;
        let app = app_with_file(&dir, storage);
        let draft = json!({
            "storage": { "kind": "postgres" },
            "cluster": { "instance_url": "https://node.example" },
        });
        let _ = authed_json(
            app.clone(),
            "PUT",
            "/admin/v1/config/draft",
            Some(put_body(draft, 0)),
        )
        .await;
        let (_, _, report, _) = authed_json(
            app,
            "POST",
            "/admin/v1/config/draft/validate",
            Some(json!({ "expected_revision": 1 })),
        )
        .await;
        assert_eq!(report["file"]["valid"], true);
        assert_eq!(report["effective"]["valid"], true);
        assert!(
            report["file"]["issues"]
                .as_array()
                .unwrap()
                .iter()
                .any(|issue| {
                    issue["path"] == "storage.url"
                        && issue["code"] == "env_supplied_required_value"
                        && issue["severity"] == "warning"
                })
        );
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("config_validate::env_only_required_value_is_a_file_warning_and_valid_effective_value")
        .env(ENV_ONLY_CHILD, "1")
        .env(
            "CC_LB_STORAGE__URL",
            "postgres://env-user:env-password@localhost/db",
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
