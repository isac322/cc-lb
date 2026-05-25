use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::Config;
use cc_lb_storage_redb::Storage;
use http_body_util::{BodyExt, Empty};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tower::ServiceExt;
use url::Url;

fn test_config(issuer_base_url: String) -> Config {
    let mut config = Config::default();
    config.signers.anthropic_oauth.issuer_base_url = issuer_base_url;
    config.signers.anthropic_oauth.client_id = "client-test".to_string();
    config.signers.anthropic_oauth.redirect_uri = "http://127.0.0.1/callback".to_string();
    config
}

fn test_state(storage: Arc<Storage>, issuer_base_url: String) -> AdminState {
    AdminState {
        storage: Some(storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: cc_lb_core::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            Arc::new(arc_swap::ArcSwap::from(
                cc_lb_core::api_keys::principal_view::PrincipalView::from_config(&Config::default()),
            )),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view: Arc::new(arc_swap::ArcSwap::from(
            cc_lb_core::api_keys::principal_view::PrincipalView::from_config(
                &cc_lb_admin::CurrentConfig::current_config(
                    (Arc::new(test_config(issuer_base_url.clone()))).as_ref(),
                ),
            ),
        )),
        config: Arc::new(test_config(issuer_base_url)),
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

async fn authorize_code(authorize_url: &str) -> String {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client: Client<_, Empty<bytes::Bytes>> =
        Client::builder(TokioExecutor::new()).build(connector);
    let request = http::Request::get(authorize_url)
        .body(Empty::<bytes::Bytes>::new())
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    let location = response
        .headers()
        .get(http::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let redirect = Url::parse(location).unwrap();
    redirect
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .unwrap()
}

#[tokio::test]
async fn test_oauth_pkce_flow() {
    let oauth_addr = spawn_mock_oauth().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Storage::open(&db_path, [0u8; 32]).unwrap());
    let app = router(test_state(storage.clone(), format!("http://{oauth_addr}")));

    let (status, start) = request_json(
        app.clone(),
        "/admin/oauth/start",
        r#"{"principal_id":"alice","provider":"anthropic_oauth"}"#.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().unwrap()).await;
    let state_token = start["state_token"].as_str().unwrap();

    let (status, _) = request_json(
        app,
        "/admin/oauth/complete",
        format!(r#"{{"state_token":"{state_token}","code":"{code}"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let creds = storage
        .get_oauth("alice", "anthropic_oauth")
        .unwrap()
        .unwrap();
    assert!(creds.access_token.starts_with("sk-ant-oat01-MOCK-alice-"));
    assert!(creds.refresh_token.starts_with("sk-ant-ort01-MOCK-"));
}
