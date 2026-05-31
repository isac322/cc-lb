mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};

#[tokio::test]
async fn usage_returns_200_grouped_by_model() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["range"], "1h");
    assert_eq!(body["group_by"], "model");
}

#[tokio::test]
async fn usage_returns_200_grouped_by_principal() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["group_by"], "principal");
}

#[tokio::test]
async fn usage_rejects_invalid_group_by() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=bogus",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn usage_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
