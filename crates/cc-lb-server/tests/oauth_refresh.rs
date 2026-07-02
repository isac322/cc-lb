use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::http::{HeaderMap, Method, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::{Clock, ClockHandle, DynamicViewHolder, Lifecycle, LifecycleConfig, TestClock};
use cc_lb_oauth_protocol::{
    ExistingTokenParts, TokenEndpointResponse, parse_token_endpoint_response,
    refresh_token_form_body, refreshed_token_parts,
};
use cc_lb_plugin_api::{
    RequestContext, ShapedRequest, Upstream, UpstreamDialect, shape_request, sign_request,
};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshJob, OAuthRefreshJobHandler, OAuthRefreshUpstreams, RefreshedOAuthTokens,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerCtx, SchedulerPushTask};
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::refresh::{LazyRefresher, LazyRefresherDeps, LazyRefresherParams};
use cc_lb_server::scheduler_factory::{SchedulerBackend, SqliteSchedulerStorage};
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSignerFactory, AnthropicOAuthSignerFactoryWithLazyRefresh,
};
use cc_lb_storage_api::{
    BackendKind, MetaStore, PrincipalCreate, PrincipalKind, UpstreamCreate, UpstreamRecord,
    UpstreamStore,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::Request;
use http::header::{AUTHORIZATION, LOCATION};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

struct Fixture {
    _dir: tempfile::TempDir,
    clock: Arc<TestClock>,
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    fake_base: String,
    scheduler_backend: SchedulerBackend,
    scheduler_cancel: CancellationToken,
    scheduler_task: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.scheduler_cancel.cancel();
        self.scheduler_task.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        let fake_addr = spawn_fake_anthropic().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!("sqlite://{}", dir.path().join("oauth.sqlite").display());
        // Pin the test clock at real wall-clock "now" so the apalis scheduler's
        // real-time "due check" agrees with timestamps the SUT writes through
        // this TestClock. Using a far-past epoch (e.g. 1_700_000_000) makes
        // every scheduler-enqueued OAuth refresh job look overdue and fire a
        // second time, which is not the behavior we want to assert.
        let clock = Arc::new(TestClock::new_at(SystemTime::now()));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("storage"),
        );
        storage.initialize(BackendKind::Sqlite).await.unwrap();
        let scheduler_backend = sqlite_scheduler_backend(clock.clone()).await;
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
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
        let (scheduler_cancel, scheduler_task) = spawn_oauth_refresh_worker(
            scheduler_backend.clone(),
            storage.clone(),
            aead.clone(),
            oauth_cfg.clone(),
            clock.clone(),
        );
        Self {
            _dir: dir,
            clock,
            storage,
            stores,
            aead,
            oauth_cfg,
            fake_base,
            scheduler_backend,
            scheduler_cancel,
            scheduler_task,
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
            now_secs(self.clock.as_ref()),
        )
        .await
        .expect("principal created");
    }
}

#[tokio::test]
async fn expired_before_sweep_lazy_fires_and_retry_succeeds() {
    let fixture = Fixture::new().await;
    let upstream_id = fixture
        .create_oauth_upstream("lazy", now_secs(fixture.clock.as_ref()))
        .await;
    let replica_id = Uuid::new_v4();
    let lazy = Arc::new(LazyRefresher::new(LazyRefresherParams {
        deps: LazyRefresherDeps {
            stores: fixture.stores.clone(),
            aead: fixture.aead.clone(),
            oauth_cfg: fixture.oauth_cfg.clone(),
            clock: fixture.clock.clone(),
        },
        replica_id,
        metadata_hook: None,
        cancel: CancellationToken::new(),
        apalis_handle: fixture.scheduler_backend.clone(),
    }));
    let base = AnthropicOAuthSignerFactory::for_upstream_name(
        fixture.storage.clone(),
        fixture.aead.clone(),
        "lazy",
        fixture.clock.clone(),
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
            now_secs(fixture.clock.as_ref()).saturating_sub(1),
            tokens,
            Some(Url::parse(&fixture.fake_base).expect("fake url")),
        )
        .await;
    let cancel = CancellationToken::new();
    let replica_id = Uuid::new_v4();
    let lazy = Arc::new(LazyRefresher::new(LazyRefresherParams {
        deps: LazyRefresherDeps {
            stores: fixture.stores.clone(),
            aead: fixture.aead.clone(),
            oauth_cfg: fixture.oauth_cfg.clone(),
            clock: fixture.clock.clone(),
        },
        replica_id,
        metadata_hook: None,
        cancel,
        apalis_handle: fixture.scheduler_backend.clone(),
    }));
    let runtime = std::sync::Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
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
        fixture.clock.clone(),
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
            fixture.clock.clone(),
        )),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
        fixture.clock.clone(),
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

async fn refresh_history_len(base: &str) -> usize {
    let response = raw_http("GET", &format!("{base}/__refresh_history"), &[], &[])
        .await
        .expect("history");
    let body: Value = serde_json::from_slice(&response.body).expect("history json");
    body["refreshes"].as_array().expect("refreshes").len()
}

async fn sqlite_scheduler_backend(clock: ClockHandle) -> SchedulerBackend {
    let pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("scheduler sqlite opens");
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .expect("scheduler sqlite initializes");
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .expect("scheduler post-setup migrations apply");
    SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::new_in_queue(
            &pool,
            cc_lb_scheduler::worker::ADAPTIVE_QUEUE,
        ),
        clock,
    })
}

fn spawn_oauth_refresh_worker(
    backend: SchedulerBackend,
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    clock: ClockHandle,
) -> (CancellationToken, JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let handler_clock = clock.clone();
    let worker = cc_lb_scheduler::worker::build_adaptive_worker(
        &backend,
        SchedulerCtx::new(
            cc_lb_config::SchedulerConfig::default(),
            Arc::new({
                let backend = backend.clone();
                move |job| {
                    let backend = backend.clone();
                    let storage = storage.clone();
                    let aead = aead.clone();
                    let oauth_cfg = oauth_cfg.clone();
                    let clock = handler_clock.clone();
                    Box::pin(async move {
                        dispatch_oauth_refresh_job(backend, storage, aead, oauth_cfg, clock, job)
                            .await
                    })
                }
            }),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            clock,
        ),
    )
    .expect("entity worker builds");
    let worker_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        if let Err(error) = worker.run_until_cancelled(worker_cancel).await {
            panic!("oauth refresh worker failed: {error}");
        }
    });
    (cancel, task)
}

async fn dispatch_oauth_refresh_job(
    backend: SchedulerBackend,
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    clock: ClockHandle,
    job: AdaptiveJob,
) -> SchedulerResult<JobOutcome> {
    let AdaptiveJob::OAuthRefresh(job) = job else {
        return Ok(JobOutcome::Done);
    };
    let now = now_secs(clock.as_ref());
    let refresh_clock = clock.clone();
    OAuthRefreshJobHandler::new(TestOAuthRefreshUpstreams { storage }, Uuid::new_v4())
        .handle(
            job,
            now,
            move |upstream| refresh_tokens(aead, oauth_cfg, refresh_clock, upstream),
            {
                let backend = backend.clone();
                move |metadata_job| enqueue_metadata_refresh(backend.clone(), metadata_job)
            },
            move |upstream_id, expires_at_unix_secs| {
                enqueue_next_oauth_refresh(backend, upstream_id, expires_at_unix_secs)
            },
        )
        .await
}

#[derive(Clone)]
struct TestOAuthRefreshUpstreams {
    storage: Arc<Storage>,
}

impl OAuthRefreshUpstreams for TestOAuthRefreshUpstreams {
    async fn get_by_id(&self, id: Uuid) -> SchedulerResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(self.storage.as_ref(), id)
            .await
            .map_err(storage_scheduler_error)
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> SchedulerResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(self.storage.as_ref(), id, holder, tokens)
            .await
            .map_err(storage_scheduler_error)
    }

    async fn read_oauth_token_generation(&self, id: Uuid) -> SchedulerResult<Option<u64>> {
        UpstreamStore::read_oauth_token_generation(self.storage.as_ref(), id)
            .await
            .map_err(storage_scheduler_error)
    }
}

async fn refresh_tokens(
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    clock: ClockHandle,
    upstream: UpstreamRecord,
) -> SchedulerResult<RefreshedOAuthTokens> {
    let previous = upstream
        .oauth_credentials
        .as_ref()
        .ok_or_else(|| SchedulerError::Job("missing oauth credentials".to_owned()))?
        .decrypt(aead.as_ref(), upstream.id.as_bytes())
        .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let response = request_refresh(oauth_cfg.as_ref(), previous.refresh_token.as_str()).await?;
    let refreshed = refreshed_token_parts(
        ExistingTokenParts {
            refresh_token: previous.refresh_token,
            scopes: previous.scopes,
        },
        response,
        now_secs(clock.as_ref()),
    );
    let expires_at_unix_secs = refreshed.expires_at_unix_secs;
    let encrypted_tokens = EncryptedOAuthTokens::encrypt(
        aead.as_ref(),
        &OAuthTokenBundle {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            expires_at_unix_secs,
            scopes: refreshed.scopes,
        },
        upstream.id.as_bytes(),
    )
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    Ok(RefreshedOAuthTokens {
        encrypted_tokens,
        expires_at_unix_secs,
    })
}

async fn request_refresh(
    oauth_cfg: &AnthropicOAuthConfig,
    refresh_token: &str,
) -> SchedulerResult<TokenEndpointResponse> {
    let body = refresh_token_form_body(oauth_cfg.client_id.as_str(), refresh_token);
    let response = raw_http(
        "POST",
        oauth_cfg.token_url.as_str(),
        &[("content-type", "application/x-www-form-urlencoded")],
        body.as_bytes(),
    )
    .await
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    if !response.status.is_success() {
        return Err(SchedulerError::Job(format!(
            "token endpoint returned {}",
            response.status
        )));
    }
    parse_token_endpoint_response(&response.body)
        .map_err(|error| SchedulerError::Job(error.to_string()))
}

async fn enqueue_metadata_refresh(
    backend: SchedulerBackend,
    job: MetadataRefreshJob,
) -> SchedulerResult<()> {
    backend.push_job(AdaptiveJob::MetadataRefresh(job)).await
}

async fn enqueue_next_oauth_refresh(
    backend: SchedulerBackend,
    upstream_id: Uuid,
    expires_at_unix_secs: u64,
) -> SchedulerResult<()> {
    let job = OAuthRefreshJob::new(upstream_id);
    let task = SchedulerPushTask {
        args: AdaptiveJob::OAuthRefresh(job),
        idempotency_key: Some(
            OAuthRefreshJob::new(upstream_id).idempotency_key(expires_at_unix_secs),
        ),
        run_at_unix_secs: Some(OAuthRefreshJob::run_at_for_expires_at(expires_at_unix_secs)),
    };
    match backend.push_adaptive_task(task).await {
        Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn storage_scheduler_error(error: cc_lb_storage_api::StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

fn now_secs(clock: &dyn Clock) -> u64 {
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
