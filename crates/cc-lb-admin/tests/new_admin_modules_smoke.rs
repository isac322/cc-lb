use crate::admin_test_common;

use admin_test_common::spawn_admin_server;
use axum::http::{StatusCode, header};

#[tokio::test]
async fn t2__v1_status_handler_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server.client.get("/admin/v1/status").await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn t2__admin_json_extractor_rejections_use_json_envelope() {
    let server = spawn_admin_server().await;
    let (status, headers, body) = server
        .client
        .post_raw("/admin/config/draft/validate", "application/json", "{")
        .await;
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json rejection body");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    );
    assert_eq!(json["error"], "validation_failed");
    assert!(json["message"].as_str().unwrap().contains("EOF"));
}

#[tokio::test]
async fn plugins_status_alias_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server.client.get("/admin/plugins").await;
    let _ = status;
}

#[tokio::test]
async fn v1_keys_list_smoke() {
    let server = spawn_admin_server().await;
    let principal = Uuid::new_v4();
    let (status, _, _) = server
        .client
        .get(&format!("/admin/v1/principals/{principal}/keys"))
        .await;
    let _ = status;
}

#[tokio::test]
async fn v1_keys_issue_smoke() {
    let server = spawn_admin_server().await;
    let principal = Uuid::new_v4();
    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal}/keys"),
            json!({ "label": "smoke" }),
        )
        .await;
    let _ = status;
}

#[tokio::test]
async fn v1_keys_revoke_smoke() {
    let server = spawn_admin_server().await;
    let principal = Uuid::new_v4();
    let key = Uuid::new_v4();
    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal}/keys/{key}/revoke"),
            json!({}),
        )
        .await;
    let _ = status;
}
