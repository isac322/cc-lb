use crate::config_admin_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_admin::AdminState;
use cc_lb_config::Config;
use config_admin_common::{TOKEN, app, authed_json, temp_storage, test_state};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn authed_patch_json(
    app: axum::Router,
    uri: &str,
    if_match: &str,
    body: Value,
) -> (StatusCode, Value, Option<String>) {
    let request = Request::builder()
        .method("PATCH")
        .uri(uri)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .header("If-Match", if_match)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let raw = response.into_body().collect().await.unwrap().to_bytes();
    let value = if raw.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&raw).unwrap_or(Value::Null)
    };
    (status, value, etag)
}

async fn create_principal(state: AdminState) -> (Value, String) {
    let (status, headers, body, _) = authed_json(
        app(state),
        "POST",
        "/admin/v1/principals",
        Some(json!({"name":"oauth-compat-roundtrip","kind":"machine"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "principal create failed: {body:?}"
    );
    (
        body,
        headers.get("etag").unwrap().to_str().unwrap().to_owned(),
    )
}

#[tokio::test]
async fn openai_oauth_compat_roundtrip_set_get_invalid_and_clear() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (created, etag) = create_principal(state.clone()).await;
    let id = created["id"].as_str().unwrap();
    let (status, body, etag2) = authed_patch_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({"openai_oauth_compat":{"store_true":"reject"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch failed: {body:?}");
    let etag = etag2.unwrap();
    let (status, _, body, _) = authed_json(
        app(state.clone()),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["openai_oauth_compat"]["store_true"], "reject");
    let (status, _, _) = authed_patch_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({"openai_oauth_compat":{"store_true":"invalid"}}),
    )
    .await;
    assert!(status.is_client_error());
    let (status, _, _) = authed_patch_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({"openai_oauth_compat":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["openai_oauth_compat"].is_null());
}
