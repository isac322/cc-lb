mod admin_test_common;

use axum::http::{StatusCode, header};
use cc_lb_storage_api::{
    PluginChainEntryInput, PluginRegistryStore, PluginSlot, WasmBlob, WasmRegistryEntryInput,
};
use serde_json::{Value, json};

#[tokio::test]
async fn create_201_with_etag_and_location() {
    let server = admin_test_common::spawn_admin_server();

    let (status, headers, body) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({
                "name": "alpha",
                "kind": "machine",
                "allowed_models": ["claude-3-*"],
                "default_limits": []
            }),
        )
        .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["name"], "alpha");
    assert_eq!(body["kind"], "machine");
    assert_eq!(body["enabled"], true);
    assert_eq!(body["revision"], 0);
    let id = body["id"].as_str().unwrap();
    assert_eq!(
        server
            .client
            .header_str(&headers, header::LOCATION.as_str()),
        format!("/admin/v1/principals/{id}")
    );
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        "W/\"0\""
    );
}

#[tokio::test]
async fn list_paginates_with_x_total_count() {
    let server = admin_test_common::spawn_admin_server();
    for name in ["alpha", "bravo", "charlie"] {
        server
            .client
            .post_json(
                "/admin/v1/principals",
                json!({ "name": name, "kind": "machine", "allowed_models": [], "default_limits": [] }),
            )
            .await;
    }

    let (status, headers, body) = server
        .client
        .get("/admin/v1/principals?after=1&limit=1")
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(server.client.header_str(&headers, "x-total-count"), "3");
    assert_eq!(body["principals"].as_array().unwrap().len(), 1);
    assert_eq!(body["principals"][0]["name"], "bravo");
}

#[tokio::test]
async fn get_returns_etag_with_weak_revision() {
    let server = admin_test_common::spawn_admin_server();
    let (_, _, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);

    let (status, headers, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], id);
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        "W/\"0\""
    );
}

#[tokio::test]
async fn update_correct_if_match_bumps_revision() {
    let server = admin_test_common::spawn_admin_server();
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server.client.header_str(&headers, header::ETAG.as_str());

    let (status, headers, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-renamed", "allowed_models": ["claude-opus"] }),
            Some(etag),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "alpha-renamed");
    assert_eq!(body["allowed_models"], json!(["claude-opus"]));
    assert_eq!(body["revision"], 1);
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        "W/\"1\""
    );
}

#[tokio::test]
async fn allowed_upstreams_round_trips_through_create_patch_and_get() {
    let server = admin_test_common::spawn_admin_server();
    let first_upstream = "11111111-1111-1111-1111-111111111111";
    let second_upstream = "22222222-2222-2222-2222-222222222222";
    let (_, headers, created) = server
        .client
        .post_json(
            "/admin/v1/principals",
            json!({
                "name": "alpha",
                "kind": "machine",
                "allowed_models": [],
                "allowed_upstreams": [first_upstream],
                "default_limits": []
            }),
        )
        .await;
    let id = created_id(&created);
    let etag = server.client.header_str(&headers, header::ETAG.as_str());

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allowed_upstreams"], json!([first_upstream]));

    let (status, headers, body) = server
        .client
        .json(
            "PATCH",
            &format!("/admin/v1/principals/{id}"),
            Some(json!({ "allowed_upstreams": [second_upstream] })),
            &[(header::IF_MATCH.as_str(), etag)],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allowed_upstreams"], json!([second_upstream]));
    assert_eq!(body["revision"], 1);

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allowed_upstreams"], json!([second_upstream]));
    assert_eq!(
        server.client.header_str(&headers, header::ETAG.as_str()),
        "W/\"1\""
    );
}

#[tokio::test]
async fn update_stale_if_match_returns_409_with_current_revision() {
    let server = admin_test_common::spawn_admin_server();
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();
    let (status, _, _) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-one" }),
            Some(&etag),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-two" }),
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
async fn update_without_if_match_returns_428() {
    let server = admin_test_common::spawn_admin_server();
    let (_, _, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);

    let (status, _, body) = server
        .client
        .put_json(
            &format!("/admin/v1/principals/{id}"),
            json!({ "name": "alpha-renamed" }),
            None,
        )
        .await;

    assert_eq!(status, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(body["error"], "if_match_required");
}

#[tokio::test]
async fn enable_disable_persists_and_audits() {
    let server = admin_test_common::spawn_admin_server();
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, headers, body) = server
        .client
        .post_with_if_match(&format!("/admin/v1/principals/{id}/disable"), &etag)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["enabled"], false);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();

    let (status, _, body) = server
        .client
        .post_with_if_match(&format!("/admin/v1/principals/{id}/enable"), &etag)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["enabled"], true);
    assert_eq!(body["revision"], 2);

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let entries = server
        .storage
        .query_audit(Some(&id), 0, u64::MAX, 20)
        .unwrap();
    let actions = entries
        .iter()
        .filter_map(|entry| entry.admin_action.as_deref())
        .collect::<Vec<_>>();
    assert!(
        actions
            .iter()
            .any(|action| action.contains("principal_update") && action.contains("enabled"))
    );
}

#[tokio::test]
async fn delete_cascade_blocks_when_plugin_chain_exists_else_soft_deletes() {
    let server = admin_test_common::spawn_admin_server();
    let (_, headers, created) = create_principal(&server.client, "alpha").await;
    let id = created_id(&created);
    let etag = server
        .client
        .header_str(&headers, header::ETAG.as_str())
        .to_owned();
    let principal_id = id.parse().unwrap();
    let registry = server
        .storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [7; 32],
                bytes: b"wasm".to_vec(),
                size_bytes: 4,
                parse_validated_at_unix_secs: 1,
            },
            WasmRegistryEntryInput {
                name: "filter-plugin".to_owned(),
                original_filename: "filter.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1,
                uploaded_by_admin_id: principal_id,
            },
        )
        .await
        .unwrap();
    let chain_entry = server
        .storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlot::Router,
            order: 1000,
            wasm_registry_id: registry.id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 1000,
        })
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .delete(&format!("/admin/v1/principals/{id}"), &etag)
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "referenced_by");
    assert_eq!(body["references"][0]["kind"], "plugin_chain");
    assert_eq!(body["references"][0]["id"], chain_entry.id.to_string());

    assert!(
        server
            .storage
            .delete_chain_entry(chain_entry.id)
            .await
            .unwrap()
    );
    let (status, _, body) = server
        .client
        .delete(&format!("/admin/v1/principals/{id}"), &etag)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, Value::Null);

    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/principals/{id}"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown_principal");
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
