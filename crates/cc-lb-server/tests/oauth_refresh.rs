use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::Form;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::{DynamicViewHolder, Lifecycle, LifecycleConfig};
use cc_lb_plugin_api::{
    RequestContext, ShapedRequest, Upstream, UpstreamDialect, shape_request, sign_request,
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::refresh::{LazyRefresher, OAuthRefresher};
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSignerFactory, AnthropicOAuthSignerFactoryWithLazyRefresh,
};
use cc_lb_storage_api::{
    AuditStore, BackendKind, MetaStore, PrincipalCreate, PrincipalKind, UpstreamCreate,
    UpstreamStore,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::Request;
use http::header::{AUTHORIZATION, LOCATION};
use http_body_util::BodyExt;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();

fn prometheus() -> &'static PrometheusHandle {
    PROMETHEUS.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("prometheus recorder")
    })
}

struct Fixture {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    fake_base: String,
}

impl Fixture {
    async fn new() -> Self {
        let fake_addr = spawn_fake_anthropic().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!("sqlite://{}", dir.path().join("oauth.sqlite").display());
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url)
                .await
                .expect("storage"),
        );
        storage.initialize(BackendKind::Sqlite).await.unwrap();
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage.clone()),
            plugin_registry_repo: None,
        });
        let aead = Arc::new(AeadService::from_master_key([31; 32]));
        let fake_base = format!("http://{fake_addr}");
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse(&format!("{fake_base}/oauth/authorize")).expect("auth url"),
            token_url: Url::parse(&format!("{fake_base}/oauth/token")).expect("token url"),
            redirect_uri: Url::parse("http://localhost/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        Self {
            _dir: dir,
            storage,
            stores,
            aead,
            oauth_cfg,
            fake_base,
        }
    }

    async fn create_oauth_upstream(&self, name: &str, expires_at: u64) -> Uuid {
        let tokens = initial_tokens(&self.fake_base).await;
        self.create_oauth_upstream_with_tokens(name, expires_at, tokens, None)
            .await
    }

    async fn create_oauth_upstream_with_tokens(
        &self,
        name: &str,
        expires_at: u64,
        tokens: InitialTokens,
        base_url: Option<Url>,
    ) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url,
                api_key_ciphertext: None,
                warmup_enabled: false,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        let encrypted = encrypted(
            &self.aead,
            record.id,
            &OAuthTokenBundle {
                access_token: tokens.access_token,
                refresh_token: tokens.refresh_token,
                expires_at_unix_secs: expires_at,
                scopes: vec!["messages".to_owned()],
            },
        );
        self.storage
            .store_oauth_tokens(record.id, record.revision, encrypted)
            .await
            .expect("tokens stored");
        record.id
    }

    async fn create_principal(&self, name: &str) {
        cc_lb_storage_api::PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            now_secs(),
        )
        .await
        .expect("principal created");
    }

    fn refresher(&self, replica_id: Uuid, cancel: CancellationToken) -> Arc<OAuthRefresher> {
        Arc::new(OAuthRefresher::new(
            self.stores.clone(),
            self.aead.clone(),
            self.oauth_cfg.clone(),
            replica_id,
            None,
            cancel,
        ))
    }
}

#[tokio::test]
async fn oauth_refresh_against_fake_anthropic() {
    let fixture = Fixture::new().await;
    let upstream_id = fixture
        .create_oauth_upstream("happy", now_secs() + 60)
        .await;

    fixture
        .refresher(Uuid::new_v4(), CancellationToken::new())
        .sweep_once()
        .await
        .expect("sweep");

    let bundle = bundle(&fixture, upstream_id).await;
    assert!(bundle.access_token.starts_with("sk-ant-oat01-"));
    assert!(bundle.expires_at_unix_secs > now_secs() + 3_000);
    assert_eq!(refresh_history_len(&fixture.fake_base).await, 1);
}

#[tokio::test]
async fn refresh_token_rotation_preserved_when_provider_rotates() {
    let fixture = Fixture::new().await;
    let upstream_id = fixture.create_oauth_upstream("rotation", now_secs()).await;
    let before = bundle(&fixture, upstream_id).await.refresh_token;

    fixture
        .refresher(Uuid::new_v4(), CancellationToken::new())
        .sweep_once()
        .await
        .expect("sweep");

    let after = bundle(&fixture, upstream_id).await.refresh_token;
    assert_ne!(before, after);
}

#[tokio::test]
async fn expired_before_sweep_lazy_fires_and_retry_succeeds() {
    let fixture = Fixture::new().await;
    let upstream_id = fixture.create_oauth_upstream("lazy", now_secs()).await;
    let replica_id = Uuid::new_v4();
    let lazy = Arc::new(LazyRefresher::new(
        fixture.stores.clone(),
        fixture.aead.clone(),
        fixture.oauth_cfg.clone(),
        replica_id,
        None,
        CancellationToken::new(),
    ));
    let base = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "lazy",
    );
    let factory = AnthropicOAuthSignerFactoryWithLazyRefresh::new(base, lazy, upstream_id);
    let signer = cc_lb_plugin_api::SignerFactory::build(
        &factory,
        &Upstream::AnthropicDirect { base_url: None },
    )
    .await
    .expect("signer");

    let signed = sign_request(signer.as_ref(), shaped_request())
        .await
        .expect("signed after lazy refresh");

    assert!(signed.headers().get(AUTHORIZATION).is_some());
    assert_eq!(refresh_history_len(&fixture.fake_base).await, 1);
}

#[tokio::test]
async fn expired_oauth_upstream_selected_by_router_choice_refreshes_during_message_request() {
    let fixture = Fixture::new().await;
    fixture.create_principal("oauth-principal").await;
    let tokens = initial_tokens(&fixture.fake_base).await;
    fixture
        .create_oauth_upstream_with_tokens(
            "oauth-target",
            now_secs().saturating_sub(1),
            tokens,
            Some(Url::parse(&fixture.fake_base).expect("fake url")),
        )
        .await;
    let cancel = CancellationToken::new();
    let replica_id = Uuid::new_v4();
    let lazy = Arc::new(LazyRefresher::new(
        fixture.stores.clone(),
        fixture.aead.clone(),
        fixture.oauth_cfg.clone(),
        replica_id,
        None,
        cancel,
    ));
    let runtime = ExtismRuntime::new();
    let view = build_dynamic_view(
        fixture.stores.as_ref(),
        fixture.oauth_cfg.as_ref(),
        fixture.aead.clone(),
        Some(lazy),
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
    )
    .await
    .expect("dynamic view builds");
    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "oauth-principal".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicOAuth,
            }),
            None,
        )),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    );

    let response = lifecycle
        .handle(message_request())
        .await
        .expect("lifecycle response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(refresh_history_len(&fixture.fake_base).await, 1);
}

#[tokio::test]
async fn two_replicas_race_only_one_calls_token_endpoint() {
    let fixture = Fixture::new().await;
    fixture.create_oauth_upstream("race", now_secs()).await;
    let first = fixture.refresher(Uuid::new_v4(), CancellationToken::new());
    let second = fixture.refresher(Uuid::new_v4(), CancellationToken::new());

    let (left, right) = tokio::join!(first.sweep_once(), second.sweep_once());

    left.expect("left sweep");
    right.expect("right sweep");
    assert_eq!(refresh_history_len(&fixture.fake_base).await, 1);
}

#[tokio::test]
async fn failed_refresh_clears_lease_after_failure_marker() {
    let fixture = Fixture::new().await;
    let record = fixture
        .storage
        .create(UpstreamCreate {
            name: "failure".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            warmup_enabled: false,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        })
        .await
        .expect("upstream created");
    fixture
        .storage
        .store_oauth_tokens(
            record.id,
            record.revision,
            encrypted(
                &fixture.aead,
                record.id,
                &OAuthTokenBundle {
                    access_token: "sk-ant-oat01-old".to_owned(),
                    refresh_token: "sk-ant-ort01-missing".to_owned(),
                    expires_at_unix_secs: now_secs(),
                    scopes: Vec::new(),
                },
            ),
        )
        .await
        .expect("tokens stored");
    let replica_id = Uuid::new_v4();

    fixture
        .refresher(replica_id, CancellationToken::new())
        .sweep_once()
        .await
        .expect("sweep");

    let updated = fixture
        .storage
        .get_by_id(record.id)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(updated.refresh_lease_holder, None);
    assert_eq!(updated.refresh_lease_until_unix_secs, None);
    assert!(
        updated
            .last_apply_error
            .as_deref()
            .is_some_and(|reason| reason.starts_with("status_"))
    );
}

#[tokio::test]
async fn cancel_during_refresh_returns_within_one_second() {
    let slow_addr = spawn_slow_token_server().await;
    let fixture = Fixture::new().await;
    fixture.create_oauth_upstream("cancel", now_secs()).await;
    let cancel = CancellationToken::new();
    let mut cfg = (*fixture.oauth_cfg).clone();
    cfg.token_url = Url::parse(&format!("http://{slow_addr}/oauth/token")).expect("slow url");
    let refresher = Arc::new(OAuthRefresher::new(
        fixture.stores.clone(),
        fixture.aead.clone(),
        Arc::new(cfg),
        Uuid::new_v4(),
        None,
        cancel.clone(),
    ));
    let task = tokio::spawn(async move { refresher.sweep_once().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("returns within one second")
        .expect("join")
        .expect("sweep result");
}

#[tokio::test]
async fn metric_counter_increments_per_outcome() {
    let metrics = prometheus();
    let fixture = Fixture::new().await;
    let before_success = counter(metrics, "metric-success", "success");
    let before_failure = counter(metrics, "metric-failure", "failure");
    fixture
        .create_oauth_upstream("metric-success", now_secs())
        .await;
    let failed = fixture
        .storage
        .create(UpstreamCreate {
            name: "metric-failure".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            warmup_enabled: false,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        })
        .await
        .expect("upstream created");
    fixture
        .storage
        .store_oauth_tokens(
            failed.id,
            failed.revision,
            encrypted(
                &fixture.aead,
                failed.id,
                &OAuthTokenBundle {
                    access_token: "sk-ant-oat01-old".to_owned(),
                    refresh_token: "missing".to_owned(),
                    expires_at_unix_secs: now_secs(),
                    scopes: Vec::new(),
                },
            ),
        )
        .await
        .expect("tokens stored");

    fixture
        .refresher(Uuid::new_v4(), CancellationToken::new())
        .sweep_once()
        .await
        .expect("sweep");

    assert!(counter(metrics, "metric-success", "success") > before_success);
    assert!(counter(metrics, "metric-failure", "failure") > before_failure);
}

#[tokio::test]
async fn audit_redaction_clean_no_token_literals_in_audit_db() {
    let fixture = Fixture::new().await;
    fixture
        .create_oauth_upstream("audit-success", now_secs())
        .await;
    let failed = fixture
        .storage
        .create(UpstreamCreate {
            name: "audit-failure".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            warmup_enabled: false,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        })
        .await
        .expect("upstream created");
    fixture
        .storage
        .store_oauth_tokens(
            failed.id,
            failed.revision,
            encrypted(
                &fixture.aead,
                failed.id,
                &OAuthTokenBundle {
                    access_token: "sk-ant-oat01-secretliteral".to_owned(),
                    refresh_token: "sk-ant-ort01-secretliteral".to_owned(),
                    expires_at_unix_secs: now_secs(),
                    scopes: Vec::new(),
                },
            ),
        )
        .await
        .expect("tokens stored");

    fixture
        .refresher(Uuid::new_v4(), CancellationToken::new())
        .sweep_once()
        .await
        .expect("sweep");

    let entries = fixture
        .storage
        .query_audit(None, 0, u64::MAX, 100)
        .await
        .expect("audit query");
    let rendered = entries
        .iter()
        .map(|entry| format!("{:?}", entry))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("upstream_oauth_refresh_success"));
    assert!(rendered.contains("upstream_oauth_refresh_failure"));
    assert!(!rendered.contains("sk-ant-oat01"));
    assert!(!rendered.contains("sk-ant-ort01"));
}

#[derive(Deserialize)]
struct InitialTokens {
    access_token: String,
    refresh_token: String,
}

async fn initial_tokens(base: &str) -> InitialTokens {
    let verifier = "verifier";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut authorize_url = Url::parse(&format!("{base}/oauth/authorize")).expect("authorize url");
    authorize_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", "test-client")
        .append_pair("redirect_uri", "http://localhost/callback")
        .append_pair("code_challenge", challenge.as_str())
        .append_pair("code_challenge_method", "S256");
    let authorize = raw_http("GET", authorize_url.as_str(), &[], &[])
        .await
        .expect("authorize");
    assert!(
        authorize.status.is_redirection(),
        "authorize status {}",
        authorize.status
    );
    let location = authorize
        .headers
        .get(LOCATION)
        .expect("location")
        .to_str()
        .expect("location str");
    let code = Url::parse(location)
        .expect("location url")
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .expect("code");
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "authorization_code");
    serializer.append_pair("client_id", "test-client");
    serializer.append_pair("redirect_uri", "http://localhost/callback");
    serializer.append_pair("code", &code);
    serializer.append_pair("code_verifier", verifier);
    let body = serializer.finish();
    let token = raw_http(
        "POST",
        &format!("{base}/oauth/token"),
        &[("content-type", "application/x-www-form-urlencoded")],
        body.as_bytes(),
    )
    .await
    .expect("token");
    assert!(token.status.is_success(), "token status {}", token.status);
    serde_json::from_slice(&token.body).expect("token json")
}

struct RawHttpResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

async fn raw_http(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<RawHttpResponse> {
    let url = Url::parse(url).expect("test url");
    let host = url.host_str().expect("test url host");
    let port = url.port_or_known_default().expect("test url port");
    let mut target = url.path().to_owned();
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    let authority = if url.port().is_some() {
        format!("{host}:{port}")
    } else {
        host.to_owned()
    };
    let mut request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let mut stream = TcpStream::connect((host, port)).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    parse_raw_response(&bytes)
}

fn parse_raw_response(bytes: &[u8]) -> std::io::Result<RawHttpResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing headers"))?;
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid status"))?;
    let mut headers = HeaderMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = http::header::HeaderName::from_bytes(name.trim().as_bytes())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            let value = http::HeaderValue::from_str(value.trim())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            headers.insert(name, value);
        }
    }
    let body = &bytes[header_end + 4..];
    let body = if headers
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(body)?
    } else {
        body.to_vec()
    };
    Ok(RawHttpResponse {
        status,
        headers,
        body: Bytes::from(body),
    })
}

fn decode_chunked(mut bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk size"))?;
        let size_text = std::str::from_utf8(&bytes[..line_end])
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        bytes = &bytes[line_end + 2..];
        if size == 0 {
            return Ok(decoded);
        }
        if bytes.len() < size + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk body",
            ));
        }
        decoded.extend_from_slice(&bytes[..size]);
        bytes = &bytes[size + 2..];
    }
}

async fn spawn_fake_anthropic() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake");
    let addr = listener.local_addr().expect("fake addr");
    tokio::spawn(async move {
        axum::serve(listener, fake_anthropic_app(AppConfig::default()))
            .await
            .expect("fake server")
    });
    addr
}

async fn spawn_slow_token_server() -> SocketAddr {
    async fn slow(
        Form(_form): Form<std::collections::HashMap<String, String>>,
    ) -> impl IntoResponse {
        tokio::time::sleep(Duration::from_secs(60)).await;
        StatusCode::OK
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind slow");
    let addr = listener.local_addr().expect("slow addr");
    let app = axum::Router::new().route("/oauth/token", post(slow));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("slow server") });
    addr
}

async fn refresh_history_len(base: &str) -> usize {
    let response = raw_http("GET", &format!("{base}/__refresh_history"), &[], &[])
        .await
        .expect("history");
    let body: Value = serde_json::from_slice(&response.body).expect("history json");
    body["refreshes"].as_array().expect("refreshes").len()
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

async fn bundle(fixture: &Fixture, upstream_id: Uuid) -> OAuthTokenBundle {
    fixture
        .storage
        .get_by_id(upstream_id)
        .await
        .expect("get")
        .expect("record")
        .oauth_credentials
        .expect("tokens")
        .decrypt(&fixture.aead, upstream_id.as_bytes())
        .expect("decrypt")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn counter(handle: &PrometheusHandle, upstream: &str, outcome: &str) -> f64 {
    handle
        .render()
        .lines()
        .find(|line| {
            line.starts_with("cclb_oauth_refresh_total{")
                && line.contains(&format!(r#"upstream="{upstream}""#))
                && line.contains(&format!(r#"outcome="{outcome}""#))
        })
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0)
}

fn shaped_request() -> ShapedRequest {
    let ctx = RequestContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
    };
    let principal = cc_lb_plugin_api::Principal {
        id: "principal".to_owned(),
        kind: cc_lb_plugin_api::PrincipalKind::OAuthSubject,
        claims: serde_json::Map::new(),
    };
    shape_request(
        &DirectDialect,
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .expect("shape")
}

fn message_request() -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .expect("request builds")
}

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &cc_lb_plugin_api::Principal,
        builder: &mut cc_lb_plugin_api::ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        Ok(builder.shaped_request(
            Url::parse("https://api.anthropic.com/v1/messages").expect("url"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}
