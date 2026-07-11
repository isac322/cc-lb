//! Task 20 keeps the existing principal-keyed PKCE helper intact and wraps it with an
//! upstream-keyed in-process state map: the serialized state token contains the upstream UUID and
//! nonce, while the server-side entry binds that token to the upstream UUID, verifier, and start
//! revision. This preserves the shared token exchange path but makes completion upstream-scoped.

use crate::admin_test_common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_control::spawn_audit_writer;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{AuditStore, UpstreamCreate, UpstreamStore};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
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
const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;

struct Fixture {
    _temp_dir: tempfile::TempDir,
    app: axum::Router,
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
    _audit_task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new() -> Self {
        let oauth_addr = spawn_fake_anthropic().await;
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let test_clock = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
        let clock: ClockHandle = test_clock.clone();
        let storage = admin_test_common::sqlite_storage_with_clock(
            temp_dir.path(),
            "test.sqlite",
            clock.clone(),
        )
        .await;
        let aead = Arc::new(AeadService::from_master_key(MASTER_KEY));
        let config = test_config(oauth_addr);
        let (audit_sink, audit_task) = spawn_audit_writer(storage.clone(), 64);
        let state = AdminState {
            storage: Some(storage.clone()),
            key_store: Some(admin_test_common::key_store(storage.clone())),
            aead: Arc::clone(&aead),
            limit_engine: admin_test_common::limit_engine_with_clock(clock.clone()),
            lifecycle: None,
            subscription_metadata_hook: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            audit_sink: Some(Arc::new(audit_sink)),
            dynamic_view: admin_test_common::dynamic_view_holder(&config),
            config: Arc::new(config),
            scheduler: None,
            admin_token: Some("test-token".to_owned()),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: cc_lb_admin::events::storage_tail_channel(),
            clock: clock.clone(),
        };

        Self {
            _temp_dir: temp_dir,
            app: router(state),
            storage,
            aead,
            clock,
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
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
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

    async fn start_draft(&self) -> (StatusCode, Value) {
        self.post_json("/admin/v1/oauth/draft/start", json!({}))
            .await
    }

    async fn complete_draft(&self, state_token: &str, code: &str) -> (StatusCode, Value) {
        self.post_json(
            "/admin/v1/oauth/draft/complete",
            json!({ "state_token": state_token, "code": code }),
        )
        .await
    }

    async fn create_from_draft(&self, state_token: &str, name: &str) -> (StatusCode, Value) {
        self.post_json(
            "/admin/v1/upstreams/from-oauth-draft",
            json!({ "state_token": state_token, "name": name }),
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

    async fn get_oauth_status(&self, upstream_id: Uuid) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/admin/v1/upstreams/{upstream_id}/oauth/status"))
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        json_response(response).await
    }

    async fn seed_expired_credentials(
        &self,
        upstream: &cc_lb_storage_api::UpstreamRecord,
    ) -> cc_lb_storage_api::UpstreamRecord {
        let expired_at = now_unix_secs(self.clock.as_ref()).saturating_sub(3600);
        let bundle = OAuthTokenBundle {
            access_token: "sk-ant-oat01-expired-seed".to_owned(),
            refresh_token: "sk-ant-ort01-expired-seed".to_owned(),
            expires_at_unix_secs: expired_at,
            scopes: vec!["org:profile".to_owned()],
        };
        let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
            self.aead.as_ref(),
            &bundle,
            upstream.id.as_bytes(),
        )
        .expect("encrypt expired bundle");
        self.storage
            .store_oauth_tokens(upstream.id, upstream.revision, encrypted)
            .await
            .expect("seed expired tokens")
    }
}

fn now_unix_secs(clock: &dyn cc_lb_clock::Clock) -> u64 {
    cc_lb_engine::clock::unix_secs(clock.now())
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
async fn unknown_upstream_start_returns_json_404() {
    let fixture = Fixture::new().await;

    let (status, body) = fixture.start(Uuid::new_v4()).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "upstream_not_found");
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

#[tokio::test]
async fn draft_start_returns_top_level_authorize_url_and_state() {
    let fixture = Fixture::new().await;

    let (status, start) = fixture.start_draft().await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        start["authorize_url"]
            .as_str()
            .is_some_and(|url| url.contains("/oauth/authorize") && url.contains("state="))
    );
    assert!(
        start["state_token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );
    assert!(start.get("revision").is_none());
}

#[tokio::test]
async fn draft_complete_invalid_code_reaches_endpoint_and_returns_structured_error() {
    let fixture = Fixture::new().await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");

    let (status, body) = fixture
        .complete_draft(state_token, "invalid-test-code")
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_grant");
}

#[tokio::test]
async fn create_from_incomplete_draft_returns_invalid_state() {
    let fixture = Fixture::new().await;
    let (status, start) = fixture.start_draft().await;
    assert_eq!(status, StatusCode::OK);
    let state_token = start["state_token"].as_str().expect("state token");

    let (status, body) = fixture
        .create_from_draft(state_token, "draft-upstream")
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_state");
}

#[tokio::test]
async fn oauth_status_reflects_completion_realtime() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("realtime-status", UpstreamKind::AnthropicOauth)
        .await;

    let (status, before) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(before["has_credentials"], false);
    assert_eq!(before["status"], "missing");
    assert!(before["expires_at_unix_secs"].is_null());

    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, _complete) = fixture.complete(upstream.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, after) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["has_credentials"], true);
    assert_eq!(after["status"], "active");
    let expires = after["expires_at_unix_secs"]
        .as_u64()
        .expect("expires_at_unix_secs present after complete");
    assert!(
        expires > now_unix_secs(fixture.clock.as_ref()),
        "expires_at_unix_secs {expires} should be in the future"
    );
    assert_eq!(after["refresh_token_present"], true);
}

#[tokio::test]
async fn oauth_status_unchanged_when_only_start_called() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("start-only-no-flip", UpstreamKind::AnthropicOauth)
        .await;

    let (status, before) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(before["has_credentials"], false);
    assert_eq!(before["status"], "missing");

    let (status, start) = fixture.start(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert!(start["authorize_url"].as_str().is_some());

    let (status, after) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        before, after,
        "/oauth/status must be byte-identical when /oauth/complete has not run"
    );
}

#[tokio::test]
async fn oauth_status_flips_from_expired_to_active_after_reconnect() {
    let fixture = Fixture::new().await;
    let upstream = fixture
        .create_upstream("reconnect-expired", UpstreamKind::AnthropicOauth)
        .await;
    let seeded = fixture.seed_expired_credentials(&upstream).await;

    let (status, before) = fixture.get_oauth_status(upstream.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(before["has_credentials"], true);
    assert_eq!(before["status"], "expired");
    let before_expires = before["expires_at_unix_secs"]
        .as_u64()
        .expect("seeded expiry");
    assert!(
        before_expires < now_unix_secs(fixture.clock.as_ref()),
        "seeded expiry {before_expires} must be in the past"
    );

    let (status, start) = fixture.start(seeded.id).await;
    assert_eq!(status, StatusCode::OK);
    let code = authorize_code(start["authorize_url"].as_str().expect("authorize_url")).await;
    let state_token = start["state_token"].as_str().expect("state_token");
    let (status, _complete) = fixture.complete(seeded.id, state_token, &code).await;
    assert_eq!(status, StatusCode::OK);

    let (status, after) = fixture.get_oauth_status(seeded.id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["has_credentials"], true);
    assert_eq!(after["status"], "active");
    let after_expires = after["expires_at_unix_secs"]
        .as_u64()
        .expect("expiry after reconnect");
    assert!(
        after_expires > now_unix_secs(fixture.clock.as_ref()),
        "post-reconnect expiry {after_expires} must be in the future"
    );
    assert_ne!(
        before_expires, after_expires,
        "expiry must change once new tokens land"
    );
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
    // NOTE [Priority-3 footgun]: cc-lb's audit pipeline is a spawned background writer
    // (spawn_audit_writer + bounded mpsc). The previous flat 250ms sleep was enough for
    // nextest's plain debug binaries but raced on CI under cargo-llvm-cov instrumented
    // binaries (slower runtime) - the `upstream_oauth_complete` row was occasionally
    // not yet flushed when the assertion fired. Both callers of this helper assert
    // after the oauth_complete handler returned 200, so polling until that entry is
    // present is the correct readiness gate; the fingerprint is embedded in the same
    // row, so its assertion in the second caller is satisfied transitively.
    const BUDGET: Duration = Duration::from_secs(2);
    const INTERVAL: Duration = Duration::from_millis(25);
    let deadline = std::time::Instant::now() + BUDGET;
    loop {
        let entries: Vec<String> = storage
            .query_audit(None, 0, u64::MAX, 100)
            .await
            .expect("query audit")
            .into_iter()
            .filter_map(|entry| entry.admin_action)
            .collect();
        if entries
            .iter()
            .any(|e| e.contains("upstream_oauth_complete"))
        {
            return entries;
        }
        if std::time::Instant::now() >= deadline {
            return entries;
        }
        tokio::time::sleep(INTERVAL).await;
    }
}

fn fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    let mut out = String::with_capacity(8);
    for byte in &digest[..4] {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
