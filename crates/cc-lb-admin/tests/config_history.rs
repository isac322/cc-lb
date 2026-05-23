mod config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_storage_redb::HistorySummary;
use config_admin_common::{
    TestReloader, app, apply_state, authed_json, config_value, expected_revision_body,
    minimal_config, put_body, temp_storage, write_config,
};

#[tokio::test]
async fn empty_history_returns_empty_list() {
    let (_dir, storage) = temp_storage();
    let app = app(config_admin_common::test_state(
        minimal_config(),
        Some(storage),
    ));

    let (status, _, json, _) = authed_json(app, "GET", "/admin/config/history", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["history"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn history_after_two_applies_is_newest_first() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let initial = minimal_config();
    write_config(&config_path, &initial);
    let (_storage_dir, storage) = temp_storage();
    let reloader = Arc::new(TestReloader::new(config_path.clone(), initial));
    let app = app(apply_state(storage, config_path, reloader));

    apply_revision(app.clone(), 0, 10, 1).await;
    apply_revision(app.clone(), 1, 20, 2).await;

    let (status, _, json, _) = authed_json(app, "GET", "/admin/config/history", None).await;
    assert_eq!(status, StatusCode::OK);
    let history = json["history"].as_array().unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0]["revision"], 2);
    assert_eq!(history[1]["revision"], 1);
    assert_eq!(history[0]["config_summary"]["principals"], 0);
}

#[tokio::test]
async fn history_limit_query_is_clamped_to_one_hundred_and_storage_keeps_fifty() {
    let (_dir, storage) = temp_storage();
    let config = minimal_config();
    let toml = toml::to_string_pretty(&config).unwrap();
    let summary = HistorySummary {
        upstreams: 0,
        principals: 0,
        plugin_count: 0,
        tls_enabled: false,
    };
    for revision in 1..=120 {
        storage
            .append_config_history(revision, toml.clone(), revision, summary.clone())
            .unwrap();
    }
    let app = app(config_admin_common::test_state(
        minimal_config(),
        Some(storage),
    ));

    let (status, _, json, _) =
        authed_json(app, "GET", "/admin/config/history?limit=1000", None).await;

    assert_eq!(status, StatusCode::OK);
    let history = json["history"].as_array().unwrap();
    assert_eq!(history.len(), 50);
    assert_eq!(history[0]["revision"], 120);
    assert_eq!(history[49]["revision"], 71);
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
