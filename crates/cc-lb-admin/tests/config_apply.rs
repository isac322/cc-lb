mod config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use config_admin_common::{
    TestReloader, app, apply_state, authed_json, config_value, expected_revision_body,
    minimal_config, put_body, temp_storage, test_state, test_state_without_storage, write_config,
};

#[tokio::test]
async fn apply_without_validate_first_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let initial = minimal_config();
    write_config(&config_path, &initial);
    let (_storage_dir, storage) = temp_storage();
    let reloader = Arc::new(TestReloader::new(config_path.clone(), initial));
    let app = app(apply_state(storage, config_path, reloader));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(111), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/apply",
        Some(expected_revision_body(1)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "unvalidated_revision");
}

#[tokio::test]
async fn apply_after_validate_with_stale_revision_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let initial = minimal_config();
    write_config(&config_path, &initial);
    let (_storage_dir, storage) = temp_storage();
    let reloader = Arc::new(TestReloader::new(config_path.clone(), initial));
    let app = app(apply_state(storage, config_path, reloader));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(111), 0)),
    )
    .await;
    let _ = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(1)),
    )
    .await;
    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(222), 1)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/apply",
        Some(expected_revision_body(1)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
}

#[tokio::test]
async fn apply_validated_matching_revision_writes_file_reloads_and_audits_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let initial = minimal_config();
    write_config(&config_path, &initial);
    let (_storage_dir, storage) = temp_storage();
    let reloader = Arc::new(TestReloader::new(config_path.clone(), initial));
    let app = app(apply_state(
        storage.clone(),
        config_path.clone(),
        reloader.clone(),
    ));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(333), 0)),
    )
    .await;
    let _ = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(1)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/apply",
        Some(expected_revision_body(1)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["applied_revision"], 1);
    assert!(json["applied_at_unix_secs"].as_u64().unwrap() > 0);
    assert!(
        std::fs::read_to_string(&config_path)
            .unwrap()
            .contains("default_requests_per_window = 333")
    );
    assert_eq!(reloader.reloads(), 1);
    assert_eq!(reloader.current().quotas.default_requests_per_window, 333);

    let audit = storage.query_audit(None, 0, u64::MAX, 10).unwrap();
    let apply = audit
        .iter()
        .find(|entry| entry.kind.as_deref() == Some("config_apply"))
        .unwrap();
    assert_eq!(apply.payload.as_ref().unwrap()["revision"], 1);
    assert_eq!(apply.payload.as_ref().unwrap()["actor"], "admin");
    assert!(
        apply.payload.as_ref().unwrap()["applied_at_unix_secs"]
            .as_u64()
            .unwrap()
            > 0
    );
    let audit_json = serde_json::to_vec(&audit).unwrap();
    config_admin_common::assert_private(&audit_json);
    assert!(!String::from_utf8_lossy(&audit_json).contains("default_requests_per_window"));
}

#[tokio::test]
async fn apply_without_storage_returns_unavailable() {
    let app = app(test_state_without_storage());
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/apply",
        Some(expected_revision_body(0)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "unvalidated_revision");
}

#[tokio::test]
async fn apply_without_config_path_returns_unavailable_after_validation() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(minimal_config(), Some(storage)));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(444), 0)),
    )
    .await;
    let _ = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(1)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/apply",
        Some(expected_revision_body(1)),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["error"], "config_path_missing");
}
