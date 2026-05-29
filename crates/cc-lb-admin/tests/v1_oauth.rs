//! Task 20 keeps the existing principal-keyed PKCE helper intact and wraps it with an
//! upstream-keyed in-process state map: the serialized state token contains the upstream UUID and
//! nonce, while the server-side entry binds that token to the upstream UUID, verifier, and start
//! revision. This preserves the shared token exchange path but makes completion upstream-scoped.

mod admin_test_common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_core::spawn_audit_writer;
use cc_lb_storage_api::{UpstreamCreate, UpstreamKind, UpstreamStore};
use cc_lb_storage_redb::Storage;
use http_body_util::{BodyExt, Empty};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const MASTER_KEY: [u8; 32] = [7; 32];

struct Fixture {
    app: axum::Router,
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    _audit_task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new() -> Self {
        let oauth_addr = spawn_fake_anthropic().await;
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Storage::open(&db_path, MASTER_KEY).expect("storage opens"));
        let aead = Arc::new(AeadService::from_master_key(MASTER_KEY));
        let config = test_config(oauth_addr);
        let (audit_sink, audit_task) = spawn_audit_writer(storage.clone(), 64);
        let state = AdminState {
            storage: Some(Arc::clone(&storage)),
            runtime_storage: Some(storage.clone()),
            aead: Arc::clone(&aead),
            limit_engine: admin_test_common::limit_engine(),
            lifecycle: None,
            audit_sink: Some(Arc::new(audit_sink)),
            dynamic_view: admin_test_common::dynamic_view_holder(&config),
            config: Arc::new(config),
            admin_token: Some("test-token".to_owned()),
            start_time: std::time::Instant::now(),
        };

        Self {
            app: router(state),
            storage,
            aead,
            _audit_task: audit_task,
        }
    }

    async fn create_upstream(
        &self,
        name: &str,
        kind: UpstreamKind,
    ) -> cc_lb_storage_api::UpstreamRecord {
        self.storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind,
                base_url: None,
                api_key_ciphertext: None,
            })
            .await
            .expect("create upstream")
    }

    async fn start(&self, upstream_id: Uuid) -> (StatusCode, Value) {
        self.post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/oauth/start"),
            json!({}),
        )
        .await
    }

    async fn complete(
        &self,
        upstream_id: Uuid,
        state_token: &str,
        code: &str,
    ) -> (StatusCode, Value) {
        self.post_json(
            &format!("/admin/v1/upstreams/{upstream_id}/oauth/complete"),
            json!({ "state_token": state_token, "code": code }),
        )
        .await
    }

    async fn post_json(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("Authorization", "Bearer test-token")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        json_response(response).await
    }
}

#[tokio::test]
async fn happy_pkce_roundtrip_persists_encrypted_tokens_and_emits_redacted_audit() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("claude-sub", UpstreamKind::AnthropicOauth)
        .await;

    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");

    let (status, complete) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(complete["upstream_id"], upstream.id.to_string());
    assert_eq!(
        complete["access_token_fingerprint"]
            .as_str()
            .expect("fingerprint")
            .len(),
        8
    );

    let stored = fixture
        .storage
        .get_by_id(upstream.id)
        .await
        .expect("get upstream")
        .expect("stored upstream");
    let bundle: OAuthTokenBundle = stored
        .oauth_credentials
        .expect("oauth credentials")
        .decrypt(&fixture.aead, upstream.id.as_bytes())
        .expect("decrypt tokens");
    assert!(bundle.access_token.starts_with("sk-ant-oat01-"));
    assert!(bundle.refresh_token.starts_with("sk-ant-ort01-"));
    assert_eq!(
        complete["access_token_fingerprint"],
        fingerprint(&bundle.access_token)
    );

    let audit = audit_actions(&fixture.storage).await;
    assert!(
        audit
            .iter()
            .any(|entry| entry.contains("upstream_oauth_start"))
    );
    assert!(
        audit
            .iter()
            .any(|entry| entry.contains("upstream_oauth_complete"))
    );
    assert!(audit.iter().any(|entry| {
        entry.contains(
            complete["access_token_fingerprint"]
                .as_str()
                .expect("fingerprint"),
        )
    }));
    assert!(
        !audit
            .iter()
            .any(|entry| entry.contains(&bundle.access_token))
    );
    assert!(
        !audit
            .iter()
            .any(|entry| entry.contains(&bundle.refresh_token))
    );
}

#[tokio::test]
async fn tampered_state_token_returns_400_invalid_state() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("tampered-state", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;

    let (status, body) = fixture.complete(upstream.id, "tampered", &code).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_state");
}

#[tokio::test]
async fn reusing_code_after_completion_returns_400() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("reuse-code", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, _) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = fixture.complete(upstream.id, state_token, &code).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_state");
}

#[tokio::test]
async fn upstream_wrong_kind_anthropic_api_key_returns_400() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("api-key-upstream", UpstreamKind::AnthropicApiKey)
        .await;

    let (status, body) = fixture.start(upstream.id).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "wrong_kind");
}

#[tokio::test]
async fn audit_entries_contain_fingerprint_only_no_raw_tokens() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("audit-redaction", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, complete) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let stored = fixture
        .storage
        .get_by_id(upstream.id)
        .await
        .expect("get upstream")
        .expect("stored upstream");
    let bundle: OAuthTokenBundle = stored
        .oauth_credentials
        .expect("oauth credentials")
        .decrypt(&fixture.aead, upstream.id.as_bytes())
        .expect("decrypt tokens");
    let serialized_audit = audit_actions(&fixture.storage).await.join("\n");

    assert!(
        serialized_audit.contains(
            complete["access_token_fingerprint"]
                .as_str()
                .expect("fingerprint")
        )
    );
    assert!(!serialized_audit.contains(&bundle.access_token));
    assert!(!serialized_audit.contains(&bundle.refresh_token));
    assert!(!complete.to_string().contains(&bundle.access_token));
    assert!(!complete.to_string().contains(&bundle.refresh_token));
}

#[tokio::test]
async fn complete_with_stale_revision_returns_409() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("stale-revision", UpstreamKind::AnthropicOauth)
        .await;
    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    fixture
        .storage
        .set_enabled(upstream.id, upstream.revision, false)
        .await
        .expect("bump revision");
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");

    let (status, body) = fixture.complete(upstream.id, state_token, &code).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "stale_revision");
    assert_eq!(body["current_revision"], upstream.revision + 1);
}

fn test_config(oauth_addr: SocketAddr) -> Config {
    let mut config = Config::default();
    config.oauth.anthropic = Some(AnthropicOAuthConfig {
        client_id: "client-test".to_owned(),
        auth_url: Url::parse(&format!("http://{oauth_addr}/oauth/authorize")).expect("auth url"),
        token_url: Url::parse(&format!("http://{oauth_addr}/oauth/token")).expect("token url"),
        redirect_uri: Url::parse("http://127.0.0.1/callback").expect("redirect url"),
        scopes: vec!["org:profile".to_owned()],
    });
    config
}

async fn spawn_fake_anthropic() -> SocketAddr {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind fixture");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            fake_anthropic::app(fake_anthropic::AppConfig::default()),
        )
        .await
        .expect("serve fake anthropic");
    });
    addr
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
        .expect("authorize request");
    let response = client.request(request).await.expect("authorize response");
    assert_eq!(response.status(), StatusCode::FOUND);
    let location = response
        .headers()
        .get(http::header::LOCATION)
        .expect("location")
        .to_str()
        .expect("location string");
    let redirect = Url::parse(location).expect("redirect location");
    redirect
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .expect("code")
}

async fn json_response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn audit_actions(storage: &Storage) -> Vec<String> {
    tokio::time::sleep(Duration::from_millis(250)).await;
    storage
        .query_audit(None, 0, u64::MAX, 100)
        .expect("query audit")
        .into_iter()
        .filter_map(|entry| entry.admin_action)
        .collect()
}

fn fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    let mut out = String::with_capacity(8);
    for byte in &digest[..4] {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
