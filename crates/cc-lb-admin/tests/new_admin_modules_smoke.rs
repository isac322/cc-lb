mod admin_test_common;

use admin_test_common::spawn_admin_server;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn dashboard_summary_with_range_smoke() {
    let server = spawn_admin_server().await;
    for range in ["1h", "6h", "24h", "7d", "30d"] {
        let (status, _, _) = server
            .client
            .get(&format!("/admin/dashboard/summary?range={range}"))
            .await;
        let _ = status;
    }
}

#[tokio::test]
async fn dashboard_summary_invalid_range_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server
        .client
        .get("/admin/dashboard/summary?range=not-a-range")
        .await;
    let _ = status;
}

#[tokio::test]
async fn dashboard_usage_with_step_and_group_by_smoke() {
    let server = spawn_admin_server().await;
    for (range, step, group) in [
        ("1h", "1m", "principal"),
        ("24h", "1h", "upstream"),
        ("7d", "1h", "model"),
        ("30d", "1d", "principal"),
    ] {
        let (status, _, _) = server
            .client
            .get(&format!(
                "/admin/usage?range={range}&step={step}&group_by={group}"
            ))
            .await;
        let _ = status;
    }
}

#[tokio::test]
async fn dashboard_usage_invalid_params_smoke() {
    let server = spawn_admin_server().await;
    for query in [
        "?range=24h&step=garbage",
        "?range=24h&group_by=garbage",
        "?range=1h&step=1d",
    ] {
        let (status, _, _) = server.client.get(&format!("/admin/usage{query}")).await;
        let _ = status;
    }
}

#[tokio::test]
async fn events_recent_smoke() {
    let server = spawn_admin_server().await;
    for query in ["", "?limit=10", "?principal_id=p", "?route=admin_v1_status"] {
        let (status, _, _) = server
            .client
            .get(&format!("/admin/events/recent{query}"))
            .await;
        let _ = status;
    }
}

#[tokio::test]
async fn status_handler_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server.client.get("/admin/status").await;
    let _ = status;
}

#[tokio::test]
async fn plugins_status_alias_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server.client.get("/admin/plugins").await;
    let _ = status;
}

#[tokio::test]
async fn credentials_list_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server.client.get("/admin/credentials").await;
    let _ = status;
}

#[tokio::test]
async fn credentials_oauth_status_smoke() {
    let server = spawn_admin_server().await;
    let (status, _, _) = server.client.get("/admin/oauth/status").await;
    let _ = status;
}

#[tokio::test]
async fn credentials_rotate_smoke() {
    let server = spawn_admin_server().await;
    let principal = Uuid::new_v4();
    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/credentials/{principal}/anthropic/rotate"),
            json!({}),
        )
        .await;
    let _ = status;
}

#[tokio::test]
async fn credentials_revoke_smoke() {
    let server = spawn_admin_server().await;
    let principal = Uuid::new_v4();
    let (status, _, _) = server
        .client
        .post_json(
            &format!("/admin/credentials/{principal}/anthropic/revoke"),
            json!({}),
        )
        .await;
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
