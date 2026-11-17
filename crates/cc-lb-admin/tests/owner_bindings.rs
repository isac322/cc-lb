use crate::admin_test_common;

use admin_test_common::spawn_admin_server;
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamKind, UpstreamStore,
};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn owner_binding_create_get_and_auth_boundary() {
    let server = spawn_admin_server().await;
    let principal = PrincipalStore::create(
        server.storage.as_ref(),
        PrincipalCreate {
            name: "owner-binding-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("principal creates");
    let upstream = UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "owner-binding-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("upstream creates");
    let binding_id = "owner-binding-test";
    let body = json!({
        "binding_id": binding_id,
        "principal_id": principal.id.to_string(),
        "provider": "anthropic",
        "owner_id": "owner-a",
        "upstream_id": upstream.id.to_string(),
        "local_user_id": "local-a",
        "allowed_workspaces": ["local-workspace"],
        "provider_workspace_id": "vendor-workspace"
    });

    let (status, headers, created) = server
        .client
        .json("POST", "/admin/v1/account-owner-bindings", Some(body), &[])
        .await;
    assert_eq!(status, axum::http::StatusCode::CREATED);
    assert_eq!(
        headers.get("etag").and_then(|v| v.to_str().ok()),
        Some("W/\"1\"")
    );
    assert_eq!(created["binding_id"], binding_id);
    assert_eq!(created["revision"], 1);
    assert_eq!(created["owner_generation"], 1);

    let (status, _, fetched) = server
        .client
        .get("/admin/v1/account-owner-bindings/owner-binding-test")
        .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(fetched["principal_id"], principal.id.to_string());
    assert_eq!(fetched["upstream_id"], upstream.id.to_string());
    let (status, _, audit) = server.client.get("/admin/v1/audit?limit=100").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(audit["entries"].as_array().unwrap().iter().any(|entry| {
        entry["admin_action"] == "owner_binding_create"
            && entry["principal_id"] == principal.id.to_string()
            && entry["upstream"] == upstream.id.to_string()
    }));
    let duplicate = server
        .client
        .json(
            "POST",
            "/admin/v1/account-owner-bindings",
            Some(json!({
                "binding_id": binding_id,
                "principal_id": principal.id.to_string(),
                "provider": "anthropic",
                "owner_id": "owner-a",
                "upstream_id": upstream.id.to_string(),
                "local_user_id": "local-a",
                "allowed_workspaces": ["another-workspace"],
                "provider_workspace_id": "vendor-workspace"
            })),
            &[],
        )
        .await;
    assert_eq!(duplicate.0, axum::http::StatusCode::CONFLICT);

    let unauthenticated = server
        .client
        .get_without_auth("/admin/v1/account-owner-bindings/owner-binding-test")
        .await;
    assert_eq!(unauthenticated.0, axum::http::StatusCode::UNAUTHORIZED);
}
