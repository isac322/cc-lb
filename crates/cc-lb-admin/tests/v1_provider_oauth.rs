//! Provider subscription-login contract: device/paste exchange, identity, and bootstrap jobs.

use crate::admin_test_common;
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use cc_lb_admin::{AdminState, router};
use cc_lb_aead::{AeadEncryptedField, AeadService, OAuthTokenBundle};
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::{AntigravityOAuthConfig, Config, GeminiOAuthConfig, OpenAiOAuthConfig};
use cc_lb_storage_api::{
    OAuthTokensCreate, UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path},
};

const NOW: u64 = 1_700_000_000;
const ANTIGRAVITY_CLIENT_ID: &str = "test-antigravity-client-id";
const ANTIGRAVITY_CLIENT_SECRET: &str = "test-antigravity-client-secret";

struct Fixture {
    _dir: tempfile::TempDir,
    app: axum::Router,
    storage: Arc<SqliteStorage>,
    aead: Arc<AeadService>,
    clock: Arc<TestClock>,
    #[cfg(feature = "sqlite")]
    scheduler_pool: sqlx::SqlitePool,
}

impl Fixture {
    async fn new(mock: &MockServer) -> Self {
        let dir = tempfile::tempdir().expect("temporary storage");
        let clock = Arc::new(TestClock::new_at_secs(NOW));
        let clock_handle: ClockHandle = clock.clone();
        let storage = admin_test_common::sqlite_storage_with_clock(
            dir.path(),
            "providers.sqlite",
            clock_handle.clone(),
        )
        .await;
        let aead = Arc::new(AeadService::from_master_key([7; 32]));
        let mut config = Config::default();
        config.oauth.openai = Some(OpenAiOAuthConfig {
            device_usercode_url: format!("{}/device/usercode", mock.uri())
                .parse()
                .expect("URL"),
            device_token_url: format!("{}/device/token", mock.uri()).parse().expect("URL"),
            token_url: format!("{}/openai/token", mock.uri()).parse().expect("URL"),
            ..OpenAiOAuthConfig::default()
        });
        config.oauth.gemini = Some(GeminiOAuthConfig {
            token_url: format!("{}/gemini/token", mock.uri()).parse().expect("URL"),
            userinfo_url: format!("{}/userinfo", mock.uri()).parse().expect("URL"),
            code_assist_url: format!("{}/v1internal", mock.uri()).parse().expect("URL"),
            ..GeminiOAuthConfig::default()
        });
        config.oauth.antigravity = Some(AntigravityOAuthConfig {
            client_id: ANTIGRAVITY_CLIENT_ID.to_owned(),
            client_secret: ANTIGRAVITY_CLIENT_SECRET.to_owned(),
            token_url: format!("{}/gemini/token", mock.uri()).parse().expect("URL"),
            userinfo_url: format!("{}/userinfo", mock.uri()).parse().expect("URL"),
            code_assist_url: mock.uri().parse().expect("URL"),
            ..AntigravityOAuthConfig::default()
        });
        #[cfg(feature = "sqlite")]
        let scheduler_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("scheduler opens");
        #[cfg(feature = "sqlite")]
        let scheduler = {
            apalis_sqlite::SqliteStorage::setup(&scheduler_pool)
                .await
                .expect("scheduler setup");
            cc_lb_scheduler::migrations::apply_post_setup_migrations(&scheduler_pool)
                .await
                .expect("scheduler migrations");
            Some(cc_lb_scheduler::admin::SchedulerAdminHandle::new(
                cc_lb_scheduler::worker::SchedulerBackend::Sqlite(
                    cc_lb_scheduler::worker::SqliteSchedulerBackend::new(
                        scheduler_pool.clone(),
                        clock_handle.clone(),
                    ),
                ),
            ))
        };
        #[cfg(not(feature = "sqlite"))]
        let scheduler = None;
        let state = AdminState {
            config_path: None,
            build: admin_test_common::test_build_info(),
            startup_config_overrides: Default::default(),
            storage: Some(storage.clone()),
            key_store: Some(admin_test_common::key_store(storage.clone())),
            aead: aead.clone(),
            limit_engine: admin_test_common::limit_engine_with_clock(clock_handle.clone()),
            lifecycle: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            dynamic_view: admin_test_common::dynamic_view_holder(&config),
            dynamic_view_rebinder: None,
            config: Arc::new(config),
            scheduler,
            admin_auth: admin_test_common::static_token_auth("test-token"),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: cc_lb_admin::events::storage_tail_channel(),
            clock: clock_handle,
            anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
                .expect("Anthropic origin"),
        };
        Self {
            _dir: dir,
            app: router(state),
            storage,
            aead,
            clock,
            #[cfg(feature = "sqlite")]
            scheduler_pool,
        }
    }

    async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
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
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("JSON response")
        };
        (status, body)
    }

    async fn get(&self, uri: &str) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        Ok((status, serde_json::from_slice(&body)?))
    }

    async fn start(&self, provider: &str) -> Value {
        let (status, body) = self
            .post("/admin/v1/oauth/draft/start", json!({"provider": provider}))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    async fn complete(&self, start: &Value, code: Option<&str>) -> (StatusCode, Value) {
        self.post(
            "/admin/v1/oauth/draft/complete",
            json!({"state_token": start["state_token"], "code": code}),
        )
        .await
    }

    async fn create(&self, start: &Value, name: &str) -> UpstreamRecord {
        let (status, body) = self
            .post(
                "/admin/v1/upstreams/from-oauth-draft",
                json!({"state_token": start["state_token"], "name": name}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        self.storage
            .get_by_id(body["id"].as_str().expect("id").parse().expect("uuid"))
            .await
            .expect("storage")
            .expect("upstream")
    }

    fn bundle(&self, upstream: &UpstreamRecord) -> OAuthTokenBundle {
        upstream
            .oauth_credentials
            .as_ref()
            .expect("credentials")
            .decrypt(&self.aead, upstream.id.as_bytes())
            .expect("decrypt")
    }

    async fn existing(&self, kind: UpstreamKind, account: &str) -> UpstreamRecord {
        let id = Uuid::new_v4();
        let bundle = OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: "old-access".to_owned(),
            refresh_token: "old-refresh".to_owned(),
            expires_at_unix_secs: NOW - 1,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some(account.to_owned()),
            project_id: (kind == UpstreamKind::GeminiOauth).then(|| "old-project".to_owned()),
            scopes: Vec::new(),
            never_refresh: false,
        };
        let tokens =
            AeadEncryptedField::encrypt(&self.aead, &bundle, id.as_bytes()).expect("encrypt");
        self.storage
            .create(UpstreamCreate {
                id,
                name: format!("existing-{id}"),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens,
                    never_refresh: false,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            })
            .await
            .expect("upstream")
    }

    #[cfg(feature = "sqlite")]
    async fn assert_bootstrap_jobs(&self, upstream: &UpstreamRecord) {
        let jobs: Vec<Vec<u8>> = sqlx::query_scalar("SELECT job FROM Jobs")
            .fetch_all(&self.scheduler_pool)
            .await
            .expect("jobs");
        let jobs: Vec<Value> = jobs
            .iter()
            .map(|body| serde_json::from_slice(body).expect("job JSON"))
            .collect();
        assert!(
            jobs.iter().any(|job| job["type"] == "oauth_refresh"
                && job["payload"]["upstream_id"] == upstream.id.to_string()),
            "refresh seeded: {jobs:?}"
        );
        assert!(
            jobs.iter().any(|job| job["type"] == "oauth_usage_poll"),
            "usage poll seeded: {jobs:?}"
        );
    }
}
#[tokio::test]
async fn openai_device_local_expiry_does_not_poll_provider() {
    let mock = MockServer::start().await;
    device_start(&mock).await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("openai").await;
    fixture.clock.advance_secs(120);
    let (status, body) = fixture.complete(&start, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "device_code_expired");
    let requests = mock.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/device/token")
    );
}

async fn device_start(mock: &MockServer) {
    Mock::given(method("POST")).and(path("/device/usercode")).and(body_json(json!({"client_id": cc_lb_oauth_protocol::OPENAI_CODEX_CLIENT_ID}))).respond_with(ResponseTemplate::new(200).set_body_json(json!({"device_auth_id": "device-id", "user_code": "ABCD-EFGH", "interval": 5, "expires_in": 120}))).mount(mock).await;
}

async fn device_authorized(mock: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/device/token"))
        .and(body_json(
            json!({"device_auth_id": "device-id", "user_code": "ABCD-EFGH"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"authorization_code": "device-code", "code_verifier": "provider-verifier"}),
        ))
        .mount(mock)
        .await;
}

async fn openai_token(mock: &MockServer, account: &str) {
    let claims = URL_SAFE_NO_PAD.encode(
        json!({"https://api.openai.com/auth": {"chatgpt_account_id": account}}).to_string(),
    );
    Mock::given(method("POST")).and(path("/openai/token")).and(header("content-type", "application/x-www-form-urlencoded")).respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "new-openai-access", "id_token": format!("header.{claims}.signature"), "refresh_token": "new-refresh", "expires_in": 3600}))).mount(mock).await;
}

async fn gemini_credentials(mock: &MockServer, email: &str) {
    Mock::given(method("POST")).and(path("/gemini/token")).and(header("content-type", "application/x-www-form-urlencoded")).respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "new-gemini-access", "refresh_token": "new-refresh", "expires_in": 3600}))).mount(mock).await;
    Mock::given(method("GET"))
        .and(path("/userinfo"))
        .and(header("authorization", "Bearer new-gemini-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"email": email})))
        .mount(mock)
        .await;
}

async fn gemini_token(mock: &MockServer, email: &str, load: Value, onboard: Option<Value>) {
    gemini_credentials(mock, email).await;
    Mock::given(method("POST"))
        .and(path("/v1internal:loadCodeAssist"))
        .respond_with(ResponseTemplate::new(200).set_body_json(load))
        .mount(mock)
        .await;
    if let Some(onboard) = onboard {
        Mock::given(method("POST"))
            .and(path("/v1internal:onboardUser"))
            .respond_with(ResponseTemplate::new(200).set_body_json(onboard))
            .mount(mock)
            .await;
    }
}

#[tokio::test]
async fn openai_device_start_returns_verification_and_expiry() {
    let mock = MockServer::start().await;
    device_start(&mock).await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("openai").await;
    assert_eq!(start["provider"], "openai");
    assert_eq!(start["flow"], "device_code");
    assert!(start["authorize_url"].is_null());
    assert_eq!(
        start["verification_url"],
        "https://auth.openai.com/codex/device"
    );
    assert_eq!(start["user_code"], "ABCD-EFGH");
    assert_eq!(start["poll_interval_secs"], 5);
    assert_eq!(start["expires_at_unix_secs"], NOW + 120);
}

#[tokio::test]
async fn openai_browser_start_returns_codex_authorize_url_and_pkce_params() {
    let mock = MockServer::start().await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture
        .post(
            "/admin/v1/oauth/draft/start",
            json!({"provider": "openai", "flow": "browser"}),
        )
        .await;
    assert_eq!(start.0, StatusCode::OK, "{}", start.1);
    assert_eq!(start.1["provider"], "openai");
    assert_eq!(start.1["flow"], "paste_code");
    for key in [
        "verification_url",
        "user_code",
        "poll_interval_secs",
        "expires_at_unix_secs",
    ] {
        assert!(start.1.get(key).is_none(), "{key} should be omitted");
    }
    let url = Url::parse(start.1["authorize_url"].as_str().expect("authorize URL"))
        .expect("authorize URL");
    let query = url
        .query_pairs()
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    for (key, expected) in [
        ("response_type", "code"),
        ("client_id", cc_lb_oauth_protocol::OPENAI_CODEX_CLIENT_ID),
        ("redirect_uri", "http://localhost:1455/auth/callback"),
        ("scope", "openid profile email offline_access"),
        ("code_challenge_method", "S256"),
        ("id_token_add_organizations", "true"),
        ("codex_cli_simplified_flow", "true"),
    ] {
        assert_eq!(query.get(key).map(String::as_str), Some(expected), "{key}");
    }
    assert_eq!(
        query.get("state").map(String::as_str),
        start.1["state_token"].as_str()
    );
    assert_eq!(query.get("code_challenge").map(String::len), Some(43));
    assert!(!query.contains_key("code"));
}

#[tokio::test]
async fn openai_browser_callback_url_exchanges_form_and_rejects_state_mismatch() {
    let mock = MockServer::start().await;
    openai_token(&mock, "browser-account").await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture
        .post(
            "/admin/v1/oauth/draft/start",
            json!({"provider": "openai", "flow": "browser"}),
        )
        .await;
    assert_eq!(start.0, StatusCode::OK, "{}", start.1);
    let callback = format!(
        "http://localhost:1455/auth/callback?code=browser-code&scope=openid&state={}",
        start.1["state_token"].as_str().expect("state")
    );
    let (status, complete) = fixture
        .post(
            "/admin/v1/oauth/draft/complete",
            json!({"state_token": start.1["state_token"], "code": callback}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    assert_eq!(complete["account_id"], "browser-account");
    let requests = mock.received_requests().await.expect("requests");
    let exchange = requests
        .iter()
        .find(|request| request.url.path() == "/openai/token")
        .expect("exchange");
    let form = url::form_urlencoded::parse(&exchange.body)
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        form.get("grant_type").map(String::as_str),
        Some("authorization_code")
    );
    assert_eq!(form.get("code").map(String::as_str), Some("browser-code"));
    assert_eq!(
        form.get("redirect_uri").map(String::as_str),
        Some("http://localhost:1455/auth/callback")
    );
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some(cc_lb_oauth_protocol::OPENAI_CODEX_CLIENT_ID)
    );
    assert!(
        form.get("code_verifier")
            .is_some_and(|value| value.len() >= 43)
    );

    let mismatch = fixture
        .post(
            "/admin/v1/oauth/draft/start",
            json!({"provider": "openai", "flow": "browser"}),
        )
        .await;
    let (status, body) = fixture
        .post(
            "/admin/v1/oauth/draft/complete",
            json!({
                "state_token": mismatch.1["state_token"],
                "code": "http://localhost:1455/auth/callback?code=bad&state=wrong"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "state_mismatch");
    assert!(
        body["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("different connection"))
    );
}

#[tokio::test]
async fn explicit_browser_flow_is_rejected_for_non_gemini_providers() {
    let mock = MockServer::start().await;
    let fixture = Fixture::new(&mock).await;
    let (status, body) = fixture
        .post(
            "/admin/v1/oauth/draft/start",
            json!({"provider": "anthropic", "flow": "browser"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"error": "unsupported_flow"}));
    let (status, body) = fixture
        .post(
            "/admin/v1/oauth/draft/start",
            json!({"provider": "gemini", "flow": "browser"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["flow"], "browser");
}

#[tokio::test]
async fn openai_device_start_accepts_live_string_interval_and_rfc3339_expiry() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/device/usercode"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_auth_id": "device-id",
            "user_code": "ABCD-EFGH",
            "interval": "5",
            "expires_at": "2023-11-14T22:15:20.000000+00:00"
        })))
        .mount(&mock)
        .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("openai").await;
    assert_eq!(start["poll_interval_secs"], 5);
    assert_eq!(start["expires_at_unix_secs"], NOW + 120);
}

#[tokio::test]
async fn openai_device_start_404_is_unavailable_without_fallback() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/device/usercode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock)
        .await;
    let fixture = Fixture::new(&mock).await;
    let (status, body) = fixture
        .post("/admin/v1/oauth/draft/start", json!({"provider": "openai"}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "device_login_unavailable");
}

#[tokio::test]
async fn openai_device_pending_without_code_keeps_draft_pollable() {
    for provider_status in [403, 404] {
        let mock = MockServer::start().await;
        device_start(&mock).await;
        Mock::given(method("POST"))
            .and(path("/device/token"))
            .respond_with(ResponseTemplate::new(provider_status))
            .mount(&mock)
            .await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start("openai").await;
        for _ in 0..2 {
            let (status, pending) = fixture.complete(&start, None).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{pending}");
            assert_eq!(
                pending,
                json!({"status": "pending", "poll_interval_secs": 5})
            );
        }
    }
}

#[tokio::test]
async fn openai_device_slow_down_expired_and_denied_are_stable_errors() {
    for (provider_status, error, expected_status, expected_error) in [
        (429, "slow_down", StatusCode::TOO_MANY_REQUESTS, "slow_down"),
        (
            400,
            "expired_token",
            StatusCode::BAD_REQUEST,
            "device_code_expired",
        ),
        (
            403,
            "access_denied",
            StatusCode::BAD_REQUEST,
            "access_denied",
        ),
    ] {
        let mock = MockServer::start().await;
        device_start(&mock).await;
        Mock::given(method("POST"))
            .and(path("/device/token"))
            .respond_with(
                ResponseTemplate::new(provider_status).set_body_json(json!({"error": error})),
            )
            .mount(&mock)
            .await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start("openai").await;
        let (status, body) = fixture.complete(&start, None).await;
        assert_eq!(status, expected_status, "{body}");
        assert_eq!(body["error"], expected_error);
    }
}

#[tokio::test]
async fn openai_device_success_form_exchanges_and_creates_account_with_jobs() {
    let mock = MockServer::start().await;
    device_start(&mock).await;
    device_authorized(&mock).await;
    openai_token(&mock, "chatgpt-account").await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("openai").await;
    let (status, complete) = fixture.complete(&start, None).await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    assert_eq!(complete["account_id"], "chatgpt-account");
    let created = fixture.create(&start, "openai-device").await;
    assert_eq!(created.kind, UpstreamKind::OpenaiOauth);
    assert!(created.warmup_enabled);
    assert_eq!(
        created.base_url.as_ref().map(Url::as_str),
        Some("https://chatgpt.com/backend-api/codex")
    );
    assert_eq!(
        fixture.bundle(&created).account_id.as_deref(),
        Some("chatgpt-account")
    );
    let requests = mock.received_requests().await.expect("requests");
    let exchange = requests
        .iter()
        .find(|request| request.url.path() == "/openai/token")
        .expect("exchange");
    let form = url::form_urlencoded::parse(&exchange.body)
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        form.get("grant_type").map(String::as_str),
        Some("authorization_code")
    );
    assert_eq!(form.get("code").map(String::as_str), Some("device-code"));
    assert_eq!(
        form.get("code_verifier").map(String::as_str),
        Some("provider-verifier")
    );
    assert_eq!(
        form.get("redirect_uri").map(String::as_str),
        Some("https://auth.openai.com/deviceauth/callback")
    );
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some(cc_lb_oauth_protocol::OPENAI_CODEX_CLIENT_ID)
    );
    assert!(!form.contains_key("client_secret"));
    #[cfg(feature = "sqlite")]
    fixture.assert_bootstrap_jobs(&created).await;
}

#[tokio::test]
async fn gemini_start_has_pkce_offline_consent_and_required_scopes() {
    let mock = MockServer::start().await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    assert_eq!(start["flow"], "browser");
    let url = Url::parse(start["authorize_url"].as_str().expect("authorize URL")).expect("URL");
    assert_eq!(url.host_str(), Some("accounts.google.com"));
    let query = url
        .query_pairs()
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    for (key, expected) in [
        ("response_type", "code"),
        ("access_type", "offline"),
        ("prompt", "consent"),
        ("code_challenge_method", "S256"),
        ("redirect_uri", "http://localhost:51121/oauth-callback"),
        ("client_id", ANTIGRAVITY_CLIENT_ID),
    ] {
        assert_eq!(query.get(key).map(String::as_str), Some(expected));
    }
    assert_eq!(
        query.get("state").map(String::as_str),
        start["state_token"].as_str()
    );
    assert_eq!(query.get("code_challenge").map(String::len), Some(43));
    assert_eq!(
        query
            .get("scope")
            .expect("scopes")
            .split_whitespace()
            .collect::<Vec<_>>(),
        vec![
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/cclog",
            "https://www.googleapis.com/auth/experimentsandconfigs"
        ]
    );
    assert!(!query.contains_key("code"));
}

#[tokio::test]
async fn gemini_complete_rejects_callback_state_mismatch_before_exchange() {
    let mock = MockServer::start().await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, body) = fixture
        .complete(
            &start,
            Some("http://localhost:51121/oauth-callback?code=google-code&state=wrong"),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "state_mismatch");
    assert!(mock.received_requests().await.expect("requests").is_empty());
}

#[tokio::test]
async fn gemini_complete_form_and_load_code_assist_create_project_with_jobs() {
    let mock = MockServer::start().await;
    gemini_token(
        &mock,
        "alice@example.com",
        json!({
            "currentTier": {"id": "free-tier"},
            "cloudaicompanionProject": "gemini-project",
        }),
        None,
    )
    .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let callback = format!(
        "http://localhost:51121/oauth-callback?code=google-code&state={}",
        start["state_token"].as_str().expect("state"),
    );
    let (status, complete) = fixture.complete(&start, Some(&callback)).await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    assert_eq!(complete["account_email"], "alice@example.com");
    assert_eq!(complete["project_id"], "gemini-project");
    assert_eq!(complete["plan"], "free-tier");
    assert_eq!(complete["suggested_name"], "alice");
    let created = fixture.create(&start, "gemini-paste").await;
    assert_eq!(created.kind, UpstreamKind::GeminiOauth);
    assert!(created.warmup_enabled);
    let bundle = fixture.bundle(&created);
    assert_eq!(bundle.project_id.as_deref(), Some("gemini-project"));
    assert_eq!(bundle.account_id.as_deref(), Some("alice@example.com"));
    assert_eq!(
        bundle.oauth_profile,
        cc_lb_aead::GeminiOAuthProfile::Antigravity
    );
    let requests = mock.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/v1internal:onboardUser")
    );
    let load = requests
        .iter()
        .find(|request| request.url.path() == "/v1internal:loadCodeAssist")
        .expect("load request");
    assert_eq!(
        serde_json::from_slice::<Value>(&load.body).expect("load JSON"),
        json!({"metadata": {"ideType": "ANTIGRAVITY"}}),
    );
    let exchange = requests
        .iter()
        .find(|request| request.url.path() == "/gemini/token")
        .expect("exchange");
    let form = url::form_urlencoded::parse(&exchange.body)
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    for (key, expected) in [
        ("grant_type", "authorization_code"),
        ("code", "google-code"),
        ("redirect_uri", "http://localhost:51121/oauth-callback"),
        ("client_id", ANTIGRAVITY_CLIENT_ID),
        ("client_secret", ANTIGRAVITY_CLIENT_SECRET),
    ] {
        assert_eq!(form.get(key).map(String::as_str), Some(expected));
    }
    assert!(
        form.get("code_verifier")
            .is_some_and(|value| value.len() >= 43)
    );
    #[cfg(feature = "sqlite")]
    fixture.assert_bootstrap_jobs(&created).await;
}

#[tokio::test]
async fn gemini_complete_onboards_free_tier_without_project() {
    let mock = MockServer::start().await;
    gemini_token(
        &mock,
        "alice@example.com",
        json!({
            "currentTier": null,
            "allowedTiers": [
                {"id": "standard-tier"},
                {"id": "free-tier", "isDefault": true},
            ],
        }),
        Some(json!({
            "done": true,
            "response": {"cloudaicompanionProject": {"id": "managed-project"}},
        })),
    )
    .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, complete) = fixture.complete(&start, Some("google-code")).await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    assert_eq!(complete["project_id"], "managed-project");
    assert_eq!(complete["account_email"], "alice@example.com");
    assert_eq!(complete["plan"], "free-tier");
    let requests = mock.received_requests().await.expect("requests");
    let request = requests
        .iter()
        .find(|request| request.url.path() == "/v1internal:onboardUser")
        .expect("onboard request");
    let body: Value = serde_json::from_slice(&request.body).expect("onboard JSON");
    assert_eq!(body["tier_id"], "free-tier");
    assert!(body.get("cloudaicompanionProject").is_none());
    assert_eq!(
        body["metadata"],
        json!({
            "ide_type": "ANTIGRAVITY",
            "ide_version": "2.24.0",
            "ide_name": "antigravity",
        })
    );
}

#[tokio::test]
async fn gemini_complete_missing_project_rejects_without_creating_upstream() {
    let mock = MockServer::start().await;
    gemini_token(
        &mock,
        "alice@example.com",
        json!({}),
        Some(json!({"done": true, "response": {}})),
    )
    .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, complete) = fixture.complete(&start, Some("google-code")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{complete}");
    assert_eq!(complete["error"], "missing_project_id");
    let requests = mock.received_requests().await.expect("requests");
    let onboard = requests
        .iter()
        .find(|request| request.url.path() == "/v1internal:onboardUser")
        .expect("legacy onboarding request");
    assert_eq!(
        serde_json::from_slice::<Value>(&onboard.body).expect("onboard JSON")["tier_id"],
        "free-tier",
    );
    let (status, body) = fixture
        .post(
            "/admin/v1/upstreams/from-oauth-draft",
            json!({"state_token": start["state_token"], "name": "must-not-create"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn gemini_complete_polls_pending_operation_until_done_without_repeated_name() {
    let _poll_delay = cc_lb_admin::v1::oauth::set_gemini_operation_poll_delay_for_tests(
        std::time::Duration::from_millis(10),
    );
    for (initial, operation_path) in [
        (
            json!({"name": "operations/onboard"}),
            "/v1internal/operations/onboard",
        ),
        (
            json!({
                "name": "projects/example/locations/global/operations/onboard",
                "done": false,
            }),
            "/v1internal/projects/example/locations/global/operations/onboard",
        ),
    ] {
        let mock = MockServer::start().await;
        gemini_token(
            &mock,
            "alice@example.com",
            json!({"allowedTiers": [{"id": "free-tier", "isDefault": true}]}),
            Some(initial),
        )
        .await;
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_polls = polls.clone();
        Mock::given(method("GET"))
            .and(path(operation_path))
            .and(header("authorization", "Bearer new-gemini-access"))
            .respond_with(move |_: &wiremock::Request| {
                let poll = observed_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(if poll == 0 {
                    json!({"done": false})
                } else {
                    json!({
                        "done": true,
                        "response": {"cloudaicompanionProject": {"id": "polled-project"}},
                    })
                })
            })
            .expect(2)
            .mount(&mock)
            .await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start("gemini").await;
        let (status, complete) = fixture.complete(&start, Some("google-code")).await;
        assert_eq!(status, StatusCode::OK, "{complete}");
        assert_eq!(complete["project_id"], "polled-project");
        assert_eq!(complete["plan"], "free-tier");
        assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn gemini_complete_current_tier_without_project_onboards_selected_tier() {
    let mock = MockServer::start().await;
    gemini_token(
        &mock,
        "alice@example.com",
        json!({"currentTier": {"id": "standard-tier"}}),
        Some(json!({
            "done": true,
            "response": {"cloudaicompanionProject": "managed-project"},
        })),
    )
    .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, complete) = fixture.complete(&start, Some("google-code")).await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    assert_eq!(complete["project_id"], "managed-project");
    assert_eq!(complete["plan"], "standard-tier");
    let requests = mock.received_requests().await.expect("requests");
    let request = requests
        .iter()
        .find(|request| request.url.path() == "/v1internal:onboardUser")
        .expect("onboard request");
    let body: Value = serde_json::from_slice(&request.body).expect("onboard JSON");
    assert_eq!(body["tier_id"], "standard-tier");
}

#[tokio::test]
async fn gemini_complete_current_tier_prefers_paid_tier_and_defaults_missing_id() {
    for (load, expected_plan) in [
        (
            json!({
                "currentTier": {"id": "free-tier"},
                "paidTier": {"id": "google-one-paid"},
                "cloudaicompanionProject": "existing-project",
            }),
            "google-one-paid",
        ),
        (
            json!({
                "currentTier": {},
                "cloudaicompanionProject": "existing-project",
            }),
            "free-tier",
        ),
    ] {
        let mock = MockServer::start().await;
        gemini_token(&mock, "alice@example.com", load, None).await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start("gemini").await;
        let (status, complete) = fixture.complete(&start, Some("google-code")).await;
        assert_eq!(status, StatusCode::OK, "{complete}");
        assert_eq!(complete["plan"], expected_plan);
        assert_eq!(complete["project_id"], "existing-project");
    }
}

#[tokio::test]
async fn gemini_complete_onboarding_uses_free_tier_fallback() {
    for (load, expected_tier, expected_plan) in [
        (
            json!({"allowedTiers": [{"id": "free-tier", "isDefault": false}]}),
            Some("free-tier"),
            "free-tier",
        ),
        (
            json!({"allowedTiers": [{"isDefault": true}]}),
            Some("free-tier"),
            "free-tier",
        ),
    ] {
        let mock = MockServer::start().await;
        gemini_token(
            &mock,
            "alice@example.com",
            load,
            Some(json!({
                "done": true,
                "response": {"cloudaicompanionProject": {"id": "managed-project"}},
            })),
        )
        .await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start("gemini").await;
        let (status, complete) = fixture.complete(&start, Some("google-code")).await;
        assert_eq!(status, StatusCode::OK, "{complete}");
        assert_eq!(complete["plan"], expected_plan);
        let requests = mock.received_requests().await.expect("requests");
        let onboard = requests
            .iter()
            .find(|request| request.url.path() == "/v1internal:onboardUser")
            .expect("onboard request");
        let body: Value = serde_json::from_slice(&onboard.body).expect("onboard JSON");
        assert_eq!(body.get("tier_id").and_then(Value::as_str), expected_tier);
        assert!(body.get("cloudaicompanionProject").is_none());
        assert!(body["metadata"].get("duetProject").is_none());
    }
}

#[tokio::test]
async fn gemini_complete_surfaces_ineligible_tier_reasons_without_tokens() {
    for current_tier in [Value::Null, json!({"id": "standard-tier"})] {
        let mock = MockServer::start().await;
        gemini_token(
            &mock,
            "alice@example.com",
            json!({
                "currentTier": current_tier,
                "ineligibleTiers": [{
                    "reasonCode": "UNSUPPORTED_LOCATION",
                    "reasonMessage": "Free usage is unavailable in this location: new-gemini-access",
                    "validationErrorMessage": "Use an eligible account, not new-refresh",
                }],
            }),
            Some(json!({"done": true, "response": {}})),
        )
        .await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start("gemini").await;
        let (status, complete) = fixture.complete(&start, Some("google-code")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{complete}");
        assert_eq!(complete["error"], "ineligible_tier");
        let detail = complete["detail"].as_str().expect("detail");
        assert!(detail.contains("Free usage is unavailable in this location"));
        assert!(detail.contains("Use an eligible account"));
        assert!(!complete.to_string().contains("new-gemini-access"));
        assert!(!complete.to_string().contains("new-refresh"));
    }
}

#[tokio::test]
async fn gemini_complete_validation_required_surfaces_reason_without_onboarding() {
    let mock = MockServer::start().await;
    gemini_token(
        &mock,
        "alice@example.com",
        json!({
            "ineligibleTiers": [{
                "reasonCode": "VALIDATION_REQUIRED",
                "reasonMessage": "Verify your Google account before using Code Assist.",
                "validationErrorMessage": "Account verification is incomplete.",
                "validationUrl": "https://accounts.google.com/verify",
            }],
        }),
        None,
    )
    .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, complete) = fixture.complete(&start, Some("google-code")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{complete}");
    assert_eq!(complete["error"], "account_validation_required");
    assert_eq!(complete["stage"], "load_code_assist");
    let detail = complete["detail"].as_str().expect("detail");
    assert!(detail.contains("Verify your Google account"));
    assert!(detail.contains("Account verification is incomplete"));
    assert!(
        mock.received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.url.path() != "/v1internal:onboardUser")
    );
}

#[tokio::test]
async fn reconnect_openai_browser_flow_updates_same_account() {
    let mock = MockServer::start().await;
    openai_token(&mock, "same-account").await;
    let fixture = Fixture::new(&mock).await;
    let upstream = fixture
        .existing(UpstreamKind::OpenaiOauth, "same-account")
        .await;
    let (status, start) = fixture
        .post(
            &format!("/admin/v1/upstreams/{}/oauth/start", upstream.id),
            json!({"flow": "browser"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{start}");
    assert_eq!(start["provider"], "openai");
    assert_eq!(start["flow"], "paste_code");
    let callback = format!(
        "http://localhost:1455/auth/callback?code=browser-reconnect&state={}",
        start["state_token"].as_str().expect("state")
    );
    let (status, complete) = fixture
        .post(
            &format!("/admin/v1/upstreams/{}/oauth/complete", upstream.id),
            json!({"state_token": start["state_token"], "code": callback}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    let updated = fixture
        .storage
        .get_by_id(upstream.id)
        .await
        .expect("storage")
        .expect("upstream");
    assert_eq!(fixture.bundle(&updated).access_token, "new-openai-access");
}

#[tokio::test]
async fn reconnect_openai_same_account_writes_existing_and_mismatch_preserves_tokens() {
    for (account, expected_status) in [
        ("same-account", StatusCode::OK),
        ("other-account", StatusCode::CONFLICT),
    ] {
        let mock = MockServer::start().await;
        device_start(&mock).await;
        device_authorized(&mock).await;
        openai_token(&mock, account).await;
        let fixture = Fixture::new(&mock).await;
        let upstream = fixture
            .existing(UpstreamKind::OpenaiOauth, "same-account")
            .await;
        let (status, start) = fixture
            .post(
                &format!("/admin/v1/upstreams/{}/oauth/start", upstream.id),
                json!({}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{start}");
        assert_eq!(start["flow"], "device_code");
        let (status, complete) = fixture
            .post(
                &format!("/admin/v1/upstreams/{}/oauth/complete", upstream.id),
                json!({"state_token": start["state_token"]}),
            )
            .await;
        assert_eq!(status, expected_status, "{complete}");
        let updated = fixture
            .storage
            .get_by_id(upstream.id)
            .await
            .expect("storage")
            .expect("upstream");
        assert_eq!(
            fixture.bundle(&updated).access_token,
            if status == StatusCode::OK {
                "new-openai-access"
            } else {
                "old-access"
            }
        );
        if status == StatusCode::CONFLICT {
            assert_eq!(complete["error"], "account_mismatch");
        }
    }
}

#[tokio::test]
async fn reconnect_gemini_same_email_writes_existing_and_mismatch_preserves_tokens() {
    for (email, expected_status) in [
        ("Alice@example.com", StatusCode::OK),
        ("other@example.com", StatusCode::CONFLICT),
    ] {
        let mock = MockServer::start().await;
        gemini_token(
            &mock,
            email,
            json!({
                "currentTier": {"id": "free-tier"},
                "cloudaicompanionProject": "new-project",
            }),
            None,
        )
        .await;
        let fixture = Fixture::new(&mock).await;
        let upstream = fixture
            .existing(UpstreamKind::GeminiOauth, "alice@example.com")
            .await;
        let (status, start) = fixture
            .post(
                &format!("/admin/v1/upstreams/{}/oauth/start", upstream.id),
                json!({}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{start}");
        assert_eq!(start["flow"], "browser");
        let (status, complete) = fixture
            .post(
                &format!("/admin/v1/upstreams/{}/oauth/complete", upstream.id),
                json!({"state_token": start["state_token"], "code": "google-code"}),
            )
            .await;
        assert_eq!(status, expected_status, "{complete}");
        let updated = fixture
            .storage
            .get_by_id(upstream.id)
            .await
            .expect("storage")
            .expect("upstream");
        let bundle = fixture.bundle(&updated);
        assert_eq!(
            bundle.access_token,
            if status == StatusCode::OK {
                "new-gemini-access"
            } else {
                "old-access"
            }
        );
        assert_eq!(
            bundle.project_id.as_deref(),
            Some(if status == StatusCode::OK {
                "new-project"
            } else {
                "old-project"
            })
        );
        if status == StatusCode::CONFLICT {
            assert_eq!(complete["error"], "account_mismatch");
        }
    }
}

async fn identity_token(mock: &MockServer, account: &str, email: &str, plan: &str) {
    let claims = URL_SAFE_NO_PAD.encode(
        json!({
            "email": email,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": account,
                "chatgpt_plan_type": plan,
            },
        })
        .to_string(),
    );
    Mock::given(method("POST"))
        .and(path("/openai/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "identity-access",
            "refresh_token": "identity-refresh",
            "expires_in": 3600,
            "id_token": format!("header.{claims}.signature"),
        })))
        .mount(mock)
        .await;
}

#[tokio::test]
async fn openai_identity_survives_create_and_updates_on_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let mock = MockServer::start().await;
    device_start(&mock).await;
    device_authorized(&mock).await;
    identity_token(&mock, "team-account", "alice@example.com", "team").await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("openai").await;
    let (status, complete) = fixture.complete(&start, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(complete["account_email"], "alice@example.com");
    assert_eq!(complete["account_id"], "team-account");
    assert_eq!(complete["plan"], "team");
    assert_eq!(complete["project_id"], Value::Null);
    let created = fixture.create(&start, "openai-identity").await;
    for surface in ["subscription-metadata", "oauth/status"] {
        let (status, body) = fixture
            .get(&format!("/admin/v1/upstreams/{}/{surface}", created.id))
            .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["account_email"], "alice@example.com");
        assert_eq!(body["account_id"], "team-account");
        assert_eq!(body["plan"], "team");
        assert!(!body.to_string().contains("identity-access"));
        assert!(!body.to_string().contains("identity-refresh"));
    }
    mock.reset().await;
    device_start(&mock).await;
    device_authorized(&mock).await;
    identity_token(
        &mock,
        "team-account",
        "alice-updated@example.com",
        "business",
    )
    .await;
    let (_, reconnect) = fixture
        .post(
            &format!("/admin/v1/upstreams/{}/oauth/start", created.id),
            json!({}),
        )
        .await;
    let (status, _) = fixture
        .post(
            &format!("/admin/v1/upstreams/{}/oauth/complete", created.id),
            json!({ "state_token": reconnect["state_token"] }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = fixture
        .get(&format!(
            "/admin/v1/upstreams/{}/subscription-metadata",
            created.id
        ))
        .await?;
    assert_eq!(body["account_email"], "alice-updated@example.com");
    assert_eq!(body["plan"], "business");
    assert_eq!(body["account_id"], "team-account");
    Ok(())
}

#[tokio::test]
async fn gemini_identity_tier_and_project_survive_create_and_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let mock = MockServer::start().await;
    gemini_token(
        &mock,
        "alice@example.com",
        json!({
            "cloudaicompanionProject": "first-project",
            "currentTier": { "id": "free-tier" },
        }),
        None,
    )
    .await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, complete) = fixture.complete(&start, Some("google-code")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(complete["account_email"], "alice@example.com");
    assert_eq!(complete["plan"], "free-tier");
    assert_eq!(complete["project_id"], "first-project");
    let created = fixture.create(&start, "gemini-identity").await;
    for surface in ["subscription-metadata", "oauth/status"] {
        let (_, body) = fixture
            .get(&format!("/admin/v1/upstreams/{}/{surface}", created.id))
            .await?;
        assert_eq!(body["account_email"], "alice@example.com");
        assert_eq!(body["plan"], "free-tier");
        assert_eq!(body["project_id"], "first-project");
        assert_eq!(body["account_id"], Value::Null);
    }
    mock.reset().await;
    gemini_token(
        &mock,
        "Alice@example.com",
        json!({
            "cloudaicompanionProject": "paid-project",
            "currentTier": { "id": "standard-tier" },
        }),
        None,
    )
    .await;
    let (_, reconnect) = fixture
        .post(
            &format!("/admin/v1/upstreams/{}/oauth/start", created.id),
            json!({}),
        )
        .await;
    let (status, _) = fixture
        .post(
            &format!("/admin/v1/upstreams/{}/oauth/complete", created.id),
            json!({ "state_token": reconnect["state_token"], "code": "new-google-code" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = fixture
        .get(&format!(
            "/admin/v1/upstreams/{}/subscription-metadata",
            created.id
        ))
        .await?;
    assert_eq!(body["account_email"], "Alice@example.com");
    assert_eq!(body["plan"], "standard-tier");
    assert_eq!(body["project_id"], "paid-project");
    Ok(())
}

#[tokio::test]
async fn provider_token_rejections_expose_stable_sanitized_details()
-> Result<(), Box<dyn std::error::Error>> {
    for provider in ["openai", "gemini"] {
        let mock = MockServer::start().await;
        if provider == "openai" {
            device_start(&mock).await;
            device_authorized(&mock).await;
        }
        let secret_code = if provider == "openai" {
            "device-code"
        } else {
            "google-code"
        };
        Mock::given(method("POST"))
            .and(path(format!("/{provider}/token")))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "invalid_grant",
                "error_description": format!("Authorization code was rejected: {secret_code}"),
            })))
            .mount(&mock)
            .await;
        let fixture = Fixture::new(&mock).await;
        let start = fixture.start(provider).await;
        let (status, body) = fixture.complete(&start, Some(secret_code)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "token_exchange_rejected");
        assert_eq!(body["provider_error"], "invalid_grant");
        assert_eq!(body["provider_status"], 400);
        assert_eq!(body["stage"], "token_exchange");
        let detail = body["detail"].as_str().ok_or("missing detail")?;
        assert!(detail.contains("Authorization code was rejected"));
        assert!(!body.to_string().contains(secret_code));
    }
    Ok(())
}

#[tokio::test]
async fn gemini_load_code_assist_failure_reports_status_and_redacts_access_token()
-> Result<(), Box<dyn std::error::Error>> {
    let mock = MockServer::start().await;
    gemini_credentials(&mock, "alice@example.com").await;
    Mock::given(method("POST"))
        .and(path("/v1internal:loadCodeAssist"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "status": "PERMISSION_DENIED", "message": "Access denied for new-gemini-access" },
        })))
        .mount(&mock).await;
    let fixture = Fixture::new(&mock).await;
    let start = fixture.start("gemini").await;
    let (status, body) = fixture.complete(&start, Some("google-code")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"], "load_code_assist_failed");
    assert_eq!(body["provider_error"], "PERMISSION_DENIED");
    assert_eq!(body["provider_status"], 403);
    assert!(
        body["detail"]
            .as_str()
            .ok_or("missing detail")?
            .contains("loadCodeAssist")
    );
    assert!(!body.to_string().contains("new-gemini-access"));
    Ok(())
}
