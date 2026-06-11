mod admin_test_common;

use std::time::{Duration, Instant};

use axum::http::{StatusCode, header};
use serde_json::{Value, json};

#[tokio::test]
async fn get_put_router_terminal_strategy_round_trips_and_audits() {
    let server = admin_test_common::spawn_admin_server();
    let (_, _, created) = create_principal(&server.client, "router-terminal-alpha").await;
    let id = created_id(&created);

    let (status, headers, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}/router-terminal"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "strategy": "first-pick", "revision": 0 }));
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, headers, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}/router-terminal"),
            json!({ "strategy": "random" }),
            Some(&etag),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "strategy": "random", "revision": 1 }));
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        "W/\"1\""
    );

    let (status, headers, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}/router-terminal"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "strategy": "random", "revision": 1 }));
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        "W/\"1\""
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_audit = false;
    while Instant::now() < deadline {
        let entries = server
            .storage
            .query_audit(Some(&id), 0, u64::MAX, 20)
            .unwrap();
        saw_audit = entries
            .iter()
            .filter_map(|entry| entry.admin_action.as_deref())
            .any(|action| {
                action.contains("principal_update") && action.contains("router_terminal_strategy")
            });
        if saw_audit {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_audit);
}

#[tokio::test]
async fn put_router_terminal_stale_revision_returns_409() {
    let server = admin_test_common::spawn_admin_server();
    let (_, headers, created) = create_principal(&server.client, "router-terminal-stale").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, _, _) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}/router-terminal"),
            json!({ "strategy": "random" }),
            Some(&etag),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}/router-terminal"),
            json!({ "strategy": "first-pick" }),
            Some(&etag),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({ "error": "stale_revision", "current_revision": 1 })
    );
}

#[tokio::test]
async fn put_router_terminal_rejects_unsupported_strategy() {
    let server = admin_test_common::spawn_admin_server();
    let (_, headers, created) = create_principal(&server.client, "router-terminal-invalid").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, _, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}/router-terminal"),
            json!({ "strategy": "unsupported" }),
            Some(&etag),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_router_terminal_strategy");
    assert_eq!(body["allowed"], json!(["first-pick", "random"]));

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}/router-terminal"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "strategy": "first-pick", "revision": 0 }));
}

async fn create_principal(
    client: &admin_test_common::AdminClient,
    name: &str,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    client
        .post_json(
            "/admin/v1/principals",
            json!({ "name": name, "kind": "machine", "allowed_models": [], "default_limits": [] }),
        )
        .await
}

fn created_id(body: &Value) -> String {
    body["id"].as_str().unwrap().to_owned()
}
