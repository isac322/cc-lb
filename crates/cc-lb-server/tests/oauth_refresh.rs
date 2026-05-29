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
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_plugin_api::{
    RequestContext, ShapedRequest, Upstream, UpstreamDialect, shape_request, sign_request,
};
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::refresh::{LazyRefresher, OAuthRefresher};
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSignerFactory, AnthropicOAuthSignerFactoryWithLazyRefresh,
};
use cc_lb_storage_api::{UpstreamCreate, UpstreamKind, UpstreamStore};
use cc_lb_storage_redb::Storage;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::header::AUTHORIZATION;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
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
        let storage =
            Arc::new(Storage::open(&dir.path().join("oauth.redb"), [31; 32]).expect("storage"));
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            audit: Some(storage.clone()),
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
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
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

    fn refresher(&self, replica_id: Uuid, cancel: CancellationToken) -> Arc<OAuthRefresher> {
        Arc::new(
            OAuthRefresher::new(
                self.stores.clone(),
                self.aead.clone(),
                self.oauth_cfg.clone(),
                replica_id,
                cancel,
            )
            .expect("refresher"),
        )
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
    let lazy = Arc::new(
        LazyRefresher::new(
            fixture.stores.clone(),
            fixture.aead.clone(),
            fixture.oauth_cfg.clone(),
            replica_id,
            CancellationToken::new(),
        )
        .expect("lazy refresher"),
    );
    let base = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "lazy",
    );
    let factory = AnthropicOAuthSignerFactoryWithLazyRefresh::new(base, lazy, upstream_id);
    let signer = cc_lb_plugin_api::SignerFactory::build(&factory, &Upstream::AnthropicDirect)
        .await
        .expect("signer");

    let signed = sign_request(signer.as_ref(), shaped_request())
        .await
        .expect("signed after lazy refresh");

    assert!(signed.headers().get(AUTHORIZATION).is_some());
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
async fn failed_refresh_holds_lease_for_full_ttl_acting_as_backoff() {
    let fixture = Fixture::new().await;
    let record = fixture
        .storage
        .create(UpstreamCreate {
            name: "failure".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
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
    assert_eq!(updated.refresh_lease_holder, Some(replica_id));
    assert!(
        updated
            .refresh_lease_until_unix_secs
            .is_some_and(|until| until > now_secs() + 80)
    );
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
    let refresher = Arc::new(
        OAuthRefresher::new(
            fixture.stores.clone(),
            fixture.aead.clone(),
            Arc::new(cfg),
            Uuid::new_v4(),
            cancel.clone(),
        )
        .expect("refresher"),
    );
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
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");
    let verifier = "verifier";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let authorize = client
        .get(format!("{base}/oauth/authorize"))
        .query(&[
            ("response_type", "code"),
            ("client_id", "test-client"),
            ("redirect_uri", "http://localhost/callback"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
        ])
        .send()
        .await
        .expect("authorize")
        .error_for_status()
        .expect("authorize status");
    let location = authorize
        .headers()
        .get(http::header::LOCATION)
        .expect("location")
        .to_str()
        .expect("location str");
    let code = Url::parse(location)
        .expect("location url")
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .expect("code");
    client
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", "test-client"),
            ("redirect_uri", "http://localhost/callback"),
            ("code", &code),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .expect("token")
        .error_for_status()
        .expect("token status")
        .json()
        .await
        .expect("token json")
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
    let body: Value = reqwest::get(format!("{base}/__refresh_history"))
        .await
        .expect("history")
        .json()
        .await
        .expect("history json");
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
    };
    let principal = cc_lb_plugin_api::Principal {
        id: "principal".to_owned(),
        kind: cc_lb_plugin_api::PrincipalKind::OAuthSubject,
        claims: serde_json::Map::new(),
    };
    shape_request(&DirectDialect, &ctx, &Upstream::AnthropicDirect, &principal).expect("shape")
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
