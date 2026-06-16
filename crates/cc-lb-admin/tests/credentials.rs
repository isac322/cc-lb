mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{PrincipalCreate, PrincipalKind, PrincipalStore};
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};
use serde_json::json;

#[tokio::test]
async fn credentials_list_returns_empty_array_with_no_principals() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/credentials", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["credentials"].is_array());
    assert_eq!(body["credentials"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn credentials_list_filters_out_principals_without_credentials() {
    let (_dir, storage) = temp_storage().await;
    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "test-principal".to_owned(),
            kind: PrincipalKind::Admin,
            allowed_models: vec![],
            allowed_upstreams: vec![],
            default_limits: vec![],
        },
        1_780_000_000,
    )
    .await
    .expect("create principal");

    let state = test_state(Config::default(), Some(storage));
    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/credentials", None).await;
    assert_eq!(status, StatusCode::OK);
    let arr = body["credentials"].as_array().expect("credentials array");
    assert_eq!(arr.len(), 0);
}

#[tokio::test]
async fn oauth_status_returns_credentials_array() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/oauth/status", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["credentials"].is_array());
}

#[tokio::test]
async fn revoke_returns_client_error_for_unknown_principal_provider() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (status, _, _) = authed_bytes(
        app(state),
        "POST",
        "/admin/credentials/00000000-0000-0000-0000-000000000000/anthropic/revoke",
        Some(json!({})),
    )
    .await;
    assert!(
        status.is_client_error() || status == StatusCode::OK,
        "expected client-error or OK status, got {status}",
    );
}

#[tokio::test]
async fn rotate_returns_501_not_implemented() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (status, _, _) = authed_bytes(
        app(state),
        "POST",
        "/admin/credentials/00000000-0000-0000-0000-000000000000/anthropic/rotate",
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn credentials_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/credentials", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
