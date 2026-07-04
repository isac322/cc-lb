use std::sync::{Arc, OnceLock};

use axum::body::Bytes;
use axum::http::{HeaderMap, Method};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_plugin_api::{
    Principal, PrincipalKind, RequestContext, ShapedRequest, SignerError, SignerFactory, Upstream,
    UpstreamDialect, shape_request, sign_request,
};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{BackendKind, MetaStore, UpstreamCreate, UpstreamStore};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
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
    _stores: Arc<Stores>,
    aead: Arc<AeadService>,
    _oauth_cfg: Arc<AnthropicOAuthConfig>,
    _fake_base: String,
}

impl Fixture {
    async fn new() -> Self {
        let fake_addr = spawn_fake_anthropic().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!("sqlite://{}", dir.path().join("composite.sqlite").display());
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_core::SystemClock))
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
            upstream_subscription_metadata: storage.clone(),
            organization_metadata: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage.clone()),
        });
        let aead = Arc::new(AeadService::from_master_key([32; 32]));
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
            _stores: stores,
            aead,
            _oauth_cfg: oauth_cfg,
            _fake_base: fake_base,
        }
    }

    async fn create_oauth_upstream(&self, name: &str) -> Uuid {
        self.create_oauth_upstream_with_access_token(
            name,
            "sk-ant-oat01-test-access-token-123456789",
        )
        .await
    }

    async fn create_oauth_upstream_with_access_token(
        &self,
        name: &str,
        access_token: &str,
    ) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        let encrypted = encrypted(
            &self.aead,
            record.id,
            &OAuthTokenBundle {
                access_token: access_token.to_owned(),
                refresh_token: format!("sk-ant-ort01-{name}-refresh-token-123456789"),
                expires_at_unix_secs: now_secs() + 3600,
                scopes: vec!["messages".to_owned()],
            },
        );
        self.storage
            .store_oauth_tokens(record.id, record.revision, encrypted)
            .await
            .expect("tokens stored");
        record.id
    }

    async fn create_apikey_upstream(&self, name: &str) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(b"test-key-ciphertext".to_vec()),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        record.id
    }

    async fn create_oauth_upstream_no_credentials(&self, name: &str) -> Uuid {
        let record = self
            .storage
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        record.id
    }
}

#[tokio::test]
async fn oauth_upstream_routes_to_oauth_signer() {
    let _ = prometheus();
    let fixture = Fixture::new().await;
    let _upstream_id = fixture.create_oauth_upstream("oauth-test").await;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "oauth-test",
        Arc::new(cc_lb_core::SystemClock),
    );

    let signer = factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await
        .expect("signer built for oauth upstream");

    let shaped = shaped_request();
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("signed request");

    let auth_header = signed
        .headers()
        .get("authorization")
        .expect("authorization header present");
    assert_eq!(
        auth_header.to_str().expect("header to str"),
        "Bearer sk-ant-oat01-test-access-token-123456789"
    );
}

#[tokio::test]
async fn apikey_upstream_routes_to_key_signer() {
    let fixture = Fixture::new().await;
    let _upstream_id = fixture.create_apikey_upstream("apikey-test").await;

    let factory = cc_lb_signer_anthropic_key::AnthropicKeySignerFactory::new(
        "sk-ant-test-api-key-12345".to_owned(),
    );

    let signer = factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await
        .expect("signer built for apikey upstream");

    let shaped = shaped_request();
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("signed request");

    let auth_header = signed
        .headers()
        .get("x-api-key")
        .expect("x-api-key header present");
    assert_eq!(
        auth_header.to_str().expect("header to str"),
        "sk-ant-test-api-key-12345"
    );
}

#[tokio::test]
async fn router_choice_selects_matching_oauth_upstream() {
    let _ = prometheus();
    let fixture = Fixture::new().await;
    fixture
        .create_oauth_upstream_with_access_token("oauth-alice", "sk-ant-oat01-alice-token")
        .await;
    fixture
        .create_oauth_upstream_with_access_token("oauth-bob", "sk-ant-oat01-bob-token")
        .await;
    let runtime = std::sync::Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let view = build_dynamic_view(
        fixture._stores.as_ref(),
        fixture._oauth_cfg.as_ref(),
        fixture.aead.clone(),
        None,
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
    .await
    .expect("dynamic view builds");

    let signer_factory = view
        .signer_factory
        .with_router_choice("sk-ant-downstream".to_owned(), "oauth-bob".to_owned());
    let signer = signer_factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await
        .expect("signer built for router choice");
    let signed = sign_request(signer.as_ref(), shaped_request())
        .await
        .expect("signed request");

    let auth_header = signed
        .headers()
        .get("authorization")
        .expect("authorization header present");
    assert_eq!(
        auth_header.to_str().expect("header to str"),
        "Bearer sk-ant-oat01-bob-token"
    );
}

#[tokio::test]
async fn empty_router_choice_errors() {
    let fixture = Fixture::new().await;
    fixture.create_oauth_upstream("oauth-only").await;
    let runtime = std::sync::Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let view = build_dynamic_view(
        fixture._stores.as_ref(),
        fixture._oauth_cfg.as_ref(),
        fixture.aead.clone(),
        None,
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
    .await
    .expect("dynamic view builds");

    let signer_factory = view
        .signer_factory
        .with_router_choice("sk-ant-downstream".to_owned(), String::new());
    let result = signer_factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await;

    match result {
        Err(SignerError::MissingCredentials { reason }) => {
            assert!(
                reason.contains("upstream not present"),
                "error message should indicate no matching router choice: {}",
                reason
            );
        }
        Ok(_) => panic!("expected MissingCredentials error, got Ok"),
        Err(other) => panic!("expected MissingCredentials error, got: {:?}", other),
    }
}

#[tokio::test]
async fn missing_oauth_credentials_returns_proper_signer_error() {
    let fixture = Fixture::new().await;
    let _upstream_id = fixture
        .create_oauth_upstream_no_credentials("oauth-missing")
        .await;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "oauth-missing",
        Arc::new(cc_lb_core::SystemClock),
    );

    let result = factory
        .build(&Upstream::AnthropicDirect { base_url: None })
        .await;

    match result {
        Err(SignerError::MissingCredentials { reason }) => {
            assert!(
                reason.contains("oauth")
                    || reason.contains("credentials")
                    || reason.contains("token"),
                "error message should indicate missing oauth credentials: {}",
                reason
            );
        }
        Ok(_) => panic!("expected MissingCredentials error, got Ok"),
        Err(other) => panic!("expected MissingCredentials error, got: {:?}", other),
    }
}

async fn spawn_fake_anthropic() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let app = fake_anthropic_app(AppConfig::default());
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    addr.to_string()
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
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
    let principal = Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::OAuthSubject,
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

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut cc_lb_plugin_api::ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        Ok(builder.shaped_request(
            Url::parse("https://api.anthropic.com/v1/messages").expect("url"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        ))
    }
}

fn now_secs() -> u64 {
    use cc_lb_core::Clock as _;

    let clock = cc_lb_core::SystemClock;
    clock
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
