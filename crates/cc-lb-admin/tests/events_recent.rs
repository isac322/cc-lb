mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};

#[tokio::test]
async fn events_recent_returns_empty_with_no_traffic() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 0);
    assert_eq!(body["count"], 0);
}

#[tokio::test]
async fn events_recent_accepts_limit_param() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/events/recent?limit=10", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["limit"], 10);
}

#[tokio::test]
async fn events_recent_rejects_invalid_limit() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));
    let (status, _, _) =
        authed_bytes(app(state), "GET", "/admin/events/recent?limit=abc", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn events_recent_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
