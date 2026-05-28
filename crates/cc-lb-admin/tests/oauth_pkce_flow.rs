mod admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_storage_redb::Storage;
use http_body_util::BodyExt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tower::ServiceExt;

fn test_config(_issuer_base_url: String) -> Config {
    Config::default()
}

fn test_state(storage: Arc<Storage>, issuer_base_url: String) -> AdminState {
    let config = test_config(issuer_base_url);
    AdminState {
        storage: Some(storage.clone()),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

async fn spawn_mock_oauth() -> SocketAddr {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock_anthropic_oauth_server::app())
            .await
            .unwrap();
    });
    addr
}

async fn request_json(
    app: axum::Router,
    uri: &str,
    body: String,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("Authorization", "Bearer test-token")
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

#[tokio::test]
async fn legacy_oauth_pkce_routes_are_gone() {
    let oauth_addr = spawn_mock_oauth().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Storage::open(&db_path, [0u8; 32]).unwrap());
    let app = router(test_state(storage, format!("http://{oauth_addr}")));

    let (status, _) = request_json(
        app.clone(),
        "/admin/oauth/start",
        r#"{"principal_id":"alice","provider":"anthropic_oauth"}"#.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);

    let (status, _) = request_json(
        app,
        "/admin/oauth/complete",
        r#"{"state_token":"legacy","code":"legacy"}"#.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
}
