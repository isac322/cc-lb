mod config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_storage_redb::HistorySummary;
use config_admin_common::{
    TestReloader, app, apply_state, authed_json, config_value, expected_revision_body,
    minimal_config, put_body, temp_storage, write_config,
};

#[tokio::test]
async fn diff_same_revision_returns_empty_diff() {
    let app = app_with_two_applies().await;

    let (status, _, json, _) = authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=1",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["from"], 1);
    assert_eq!(json["to"], 1);
    assert!(json["diff"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn diff_different_revisions_returns_path_level_changes() {
    let app = app_with_two_applies().await;

    let (status, _, json, _) = authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=2",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let changes = json["diff"].as_array().unwrap();
    assert!(changes.iter().any(|change| {
        change["path"] == "quotas.default_requests_per_window"
            && change["from"] == 10
            && change["to"] == 20
    }));
}

#[tokio::test]
async fn diff_unknown_revision_returns_not_found() {
    let app = app_with_two_applies().await;

    let (status, _, json, _) = authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=999",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json["error"], "unknown_revision");
    assert_eq!(json["missing"], 999);
}

#[tokio::test]
async fn diff_redacts_secret_like_values() {
    let (_dir, storage) = temp_storage();
    let mut first = minimal_config();
    first.admin.token_env = "FIRST_ADMIN_TOKEN_ENV".to_owned();
    let mut second = minimal_config();
    second.admin.token_env = "SECOND_ADMIN_TOKEN_ENV".to_owned();
    let summary = HistorySummary {
        upstreams: 0,
        principals: 0,
        plugin_count: 0,
        tls_enabled: false,
    };
    storage
        .append_config_history(
            1,
            toml::to_string_pretty(&first).unwrap(),
            1,
            summary.clone(),
        )
        .unwrap();
    storage
        .append_config_history(2, toml::to_string_pretty(&second).unwrap(), 2, summary)
        .unwrap();
    let app = app(config_admin_common::test_state(
        minimal_config(),
        Some(storage),
    ));

    let (status, _, json, _) = authed_json(
        app,
        "GET",
        "/admin/config/diff?from_revision=1&to_revision=2",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let change = json["diff"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["path"] == "admin.token_env")
        .unwrap();
    assert_eq!(change["from"], "<redacted>");
    assert_eq!(change["to"], "<redacted>");
}

async fn app_with_two_applies() -> axum::Router {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let initial = minimal_config();
    write_config(&config_path, &initial);
    let (_storage_dir, storage) = temp_storage();
    let reloader = Arc::new(TestReloader::new(config_path.clone(), initial));
    let app = app(apply_state(storage, config_path, reloader));

    apply_revision(app.clone(), 0, 10, 1).await;
    apply_revision(app.clone(), 1, 20, 2).await;
    app
}

async fn apply_revision(app: axum::Router, expected: u64, requests: u64, revision: u64) {
    let (status, _, put, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(requests), expected)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(put["revision"], revision);

    let (status, _, validate, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(revision)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(validate["valid"], true);

    let (status, _, apply, _) = authed_json(
        app,
        "POST",
        "/admin/config/apply",
        Some(expected_revision_body(revision)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(apply["applied_revision"], revision);
}
