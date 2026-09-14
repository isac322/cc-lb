use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::resume_unwind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{
    AnthropicOAuthConfig, Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
    StorageConfig, TlsConfig,
};
use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::{ClockHandle, TestClock};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog};
use cc_lb_server::app::build_app_with_storage;
use cc_lb_server::{BuildError, signal::SignalHandle};
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind, PrincipalStore};
use cc_lb_storage_api::types::{
    Limit as KeyLimit, LimitKind as KeyLimitKind, PrincipalKindLite,
    UpstreamKind as KeyUpstreamKind,
};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamStore};
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, MetaStore, RequestEvent, RequestEventStore, Storage,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::{HeaderMap, StatusCode};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

pub const ADMIN_TOKEN: &str = "t4-admin-token";
pub const MODEL: &str = "claude-3-5-sonnet-20241022";
pub const FIXED_UNIX_SECS: u64 = 1_700_000_000;
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const READY_TIMEOUT: Duration = Duration::from_secs(30);

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy)]
pub enum AuthKind {
    ApiKey,
    OAuth,
}

pub struct TestAppOptions {
    pub fake_config: AppConfig,
    pub upstream_addr: Option<SocketAddr>,
    pub auth_kind: AuthKind,
    pub upstream_total_secs: u64,
    pub downstream_api_key_auth: bool,
    pub downstream_cost_cap_micros: Option<i64>,
    pub tls: bool,
}

impl TestAppOptions {
    pub fn fake(_label: &'static str) -> Self {
        Self {
            fake_config: AppConfig::default(),
            upstream_addr: None,
            auth_kind: AuthKind::ApiKey,
            upstream_total_secs: 30,
            downstream_api_key_auth: false,
            downstream_cost_cap_micros: None,
            tls: false,
        }
    }
}

pub struct TestApp {
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub storage: Arc<SqliteStorage>,
    pub downstream_api_key: Option<String>,
    pub downstream_key_id: Option<String>,
    pub cert_path: Option<PathBuf>,
    signal: SignalHandle,
    app_task: Option<JoinHandle<Result<(), BuildError>>>,
    fake_task: Option<JoinHandle<Result<(), std::io::Error>>>,
    raw_tasks: Mutex<Vec<JoinHandle<Result<(), std::io::Error>>>>,
    _dir: tempfile::TempDir,
}

impl TestApp {
    pub async fn run<T, F, Fut>(options: TestAppOptions, test: F) -> TestResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = TestResult<T>> + Send + 'static,
    {
        let app = Arc::new(Self::start(options).await?);
        let outcome = tokio::spawn(test(app.clone())).await;
        let app = Arc::try_unwrap(app)
            .map_err(|_| "T4 test retained a TestApp reference after its body completed")?;
        let cleanup = app.shutdown().await;

        match outcome {
            Ok(result) => {
                cleanup?;
                result
            }
            Err(error) if error.is_panic() => {
                if let Err(cleanup_error) = cleanup {
                    eprintln!("T4 cleanup after panic failed: {cleanup_error}");
                }
                resume_unwind(error.into_panic())
            }
            Err(error) => {
                cleanup?;
                Err(format!("T4 test body task was cancelled: {error}").into())
            }
        }
    }

    pub async fn start(options: TestAppOptions) -> TestResult<Self> {
        install_price_catalog_fixture();
        let dir = tempfile::tempdir()?;
        let sqlite_path = dir.path().join("cc-lb.sqlite");
        let config_path = dir.path().join("cc-lb.toml");
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(FIXED_UNIX_SECS));
        let storage = open_test_storage(&sqlite_path, clock.clone()).await?;

        let (fake_addr, fake_task) = spawn_fake(options.fake_config).await?;
        let upstream_addr = options.upstream_addr.unwrap_or(fake_addr);
        let upstream_kind = match options.auth_kind {
            AuthKind::ApiKey => UpstreamKind::AnthropicApiKey,
            AuthKind::OAuth => UpstreamKind::AnthropicOauth,
        };
        let upstream = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "t4-upstream".to_owned(),
                kind: upstream_kind,
                base_url: Some(Url::parse(&format!("http://{upstream_addr}"))?),
                api_key_ciphertext: matches!(options.auth_kind, AuthKind::ApiKey).then(Vec::new),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await?;
        PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: "t4-principal".to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: vec![],
                allowed_upstreams: vec![],
                default_limits: vec![],
                cache_keepalive: None,
            },
            FIXED_UNIX_SECS,
        )
        .await?;
        let (downstream_api_key, downstream_key_id) = if options.downstream_api_key_auth {
            let limit_overrides = options
                .downstream_cost_cap_micros
                .map(|cap_micros| {
                    vec![KeyLimit {
                        kind: KeyLimitKind::CostUsd,
                        window_secs: 60 * 60,
                        cap_micros,
                    }]
                })
                .unwrap_or_default();
            let key_store = KeyStore::new(storage.clone());
            let (record, plaintext) = key_store
                .create(
                    "t4-principal",
                    CreateParams {
                        upstream_kind: match options.auth_kind {
                            AuthKind::ApiKey => KeyUpstreamKind::AnthropicKey,
                            AuthKind::OAuth => KeyUpstreamKind::AnthropicOAuth,
                        },
                        label: "t4-downstream".to_owned(),
                        description: None,
                        expires_at_unix_secs: None,
                        limit_overrides,
                        principal_kind: PrincipalKindLite::Machine,
                    },
                )
                .await?;
            let key_id = key_store
                .list_all()
                .await?
                .into_iter()
                .find_map(|(principal_id, key_id, stored)| {
                    (principal_id == "t4-principal" && stored.index_hash == record.index_hash)
                        .then_some(key_id)
                })
                .ok_or("created T4 downstream key is missing from storage")?;
            (Some(plaintext.expose().to_owned()), Some(key_id))
        } else {
            (None, None)
        };

        if matches!(options.auth_kind, AuthKind::OAuth) {
            seed_oauth_tokens(storage.as_ref(), upstream.id, fake_addr).await?;
        }

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
        let admin_listener = TcpListener::bind("127.0.0.1:0").await?;
        let proxy_addr = proxy_listener.local_addr()?;
        let admin_addr = admin_listener.local_addr()?;

        let mut config = Config::default();
        config.listener.proxy_addr = proxy_addr;
        config.listener.admin_addr = admin_addr;
        config.listener.metrics_addr = "127.0.0.1:0".parse()?;
        config.storage = StorageConfig::Sqlite { path: sqlite_path };
        config.runtime.data_dir = Some(dir.path().join("data"));
        config.aead.key_env = "__CC_LB_T4_EXPLICIT_KEY__".to_owned();
        config.admin.token = Some(ADMIN_TOKEN.to_owned());
        if options.downstream_api_key_auth {
            config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
            config.downstream_auth.none_mode = None;
        } else {
            config.downstream_auth.mode = DownstreamAuthMode::None;
            config.downstream_auth.none_mode = Some(NoneModeConfig {
                principal_id: "t4-principal".to_owned(),
                upstream_kind: match options.auth_kind {
                    AuthKind::ApiKey => NoneModeUpstreamKind::AnthropicKey,
                    AuthKind::OAuth => NoneModeUpstreamKind::AnthropicOAuth,
                },
            });
        }
        config.timeouts.upstream_total_secs = options.upstream_total_secs;
        config.api_keys.price_catalog.url = format!("http://{fake_addr}/prices");
        config.api_keys.price_catalog.refresh_interval = Duration::from_secs(24 * 60 * 60);
        config.api_keys.price_catalog.cache_path = dir.path().join("prices.json");
        for job in config.scheduler.recurring_jobs.values_mut() {
            job.enabled = false;
        }
        if matches!(options.auth_kind, AuthKind::OAuth) {
            config.oauth.anthropic = Some(AnthropicOAuthConfig {
                client_id: "t4-client".to_owned(),
                auth_url: Url::parse(&format!("http://{fake_addr}/oauth/authorize"))?,
                token_url: Url::parse(&format!("http://{fake_addr}/oauth/token"))?,
                redirect_uri: Url::parse("http://localhost/callback")?,
                scopes: vec!["messages".to_owned()],
            });
        }
        let cert_path = if options.tls {
            let cert_path = dir.path().join("server-cert.pem");
            let key_path = dir.path().join("server-key.pem");
            std::fs::write(
                &cert_path,
                include_bytes!("../../../crates/cc-lb-server/tests/fixtures/tls/cert-a.pem"),
            )?;
            std::fs::write(
                &key_path,
                include_bytes!("../../../crates/cc-lb-server/tests/fixtures/tls/key-a.pem"),
            )?;
            config.listener.tls = Some(TlsConfig {
                cert_path: Some(cert_path.clone()),
                key_path: Some(key_path),
                reload_on_sighup: true,
            });
            Some(cert_path)
        } else {
            None
        };
        std::fs::write(&config_path, toml::to_string_pretty(&config)?)?;

        let managed_store: Arc<dyn ManagedKeyStore> = storage.clone();
        let storage_handle: Arc<dyn Storage> = storage.clone();
        let aead = Arc::new(AeadService::from_master_key([0x44; 32]));
        let app = build_app_with_storage(
            config,
            Some(&config_path),
            managed_store,
            storage_handle,
            aead,
            clock,
        )
        .await?;
        let signal = app.signal_handle();
        let app_task = tokio::spawn(async move {
            app.start_with_listeners(proxy_listener, admin_listener)
                .await
        });
        let mut running = Self {
            proxy_addr,
            admin_addr,
            storage,
            cert_path,
            downstream_api_key,
            downstream_key_id,
            signal,
            app_task: Some(app_task),
            fake_task: Some(fake_task),
            raw_tasks: Mutex::new(Vec::new()),
            _dir: dir,
        };
        if let Err(error) = running.wait_ready(options.tls).await {
            if let Err(cleanup_error) = running.shutdown().await {
                return Err(format!("{error}; cleanup also failed: {cleanup_error}").into());
            }
            return Err(error);
        }
        Ok(running)
    }

    async fn wait_ready(&mut self, tls: bool) -> TestResult {
        let timeout = READY_TIMEOUT;
        let admin = tokio::time::timeout(
            timeout,
            raw_http(self.admin_addr, "GET", "/admin/health", &[], &[]),
        )
        .await
        .map_err(|_| format!("admin listener was not ready within {timeout:?}"))??;
        if admin.status != StatusCode::OK {
            return Err(format!("admin readiness returned {}", admin.status).into());
        }
        if !tls {
            let proxy = tokio::time::timeout(
                timeout,
                raw_http(self.proxy_addr, "GET", "/healthz", &[], &[]),
            )
            .await
            .map_err(|_| format!("proxy listener was not ready within {timeout:?}"))??;
            if proxy.status != StatusCode::OK {
                return Err(format!("proxy readiness returned {}", proxy.status).into());
            }
        }
        Ok(())
    }

    pub async fn event(&self, request_id: &str) -> RequestEvent {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                let rows = self
                    .storage
                    .query_request_events(0, u64::MAX, 256)
                    .await
                    .expect("query request events");
                match rows
                    .into_iter()
                    .find(|row| row.request_id == request_id && row.event_id.is_some())
                {
                    Some(row) => return row,
                    None => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("request event was not persisted within 15s")
    }

    pub fn track_raw_task(&self, task: JoinHandle<Result<(), std::io::Error>>) {
        self.raw_tasks
            .lock()
            .expect("raw task lock poisoned")
            .push(task);
    }

    pub async fn shutdown(mut self) -> TestResult {
        self.signal.start_shutdown();
        let mut cleanup_error = None;

        if let Some(mut task) = self.app_task.take() {
            match tokio::time::timeout(WAIT_TIMEOUT, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    cleanup_error = Some(format!("App returned an error during shutdown: {error}"))
                }
                Ok(Err(error)) => {
                    cleanup_error = Some(format!("App task failed during shutdown: {error}"))
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    cleanup_error = Some("App did not shut down within 5s".to_owned());
                }
            }
        }
        if let Some(task) = self.fake_task.take()
            && let Err(error) = abort_and_join(task).await
            && cleanup_error.is_none()
        {
            cleanup_error = Some(format!("fake cleanup failed: {error}"));
        }
        let raw_tasks = std::mem::take(self.raw_tasks.get_mut().expect("raw task lock poisoned"));
        for task in raw_tasks {
            if let Err(error) = abort_and_join(task).await
                && cleanup_error.is_none()
            {
                cleanup_error = Some(format!("raw upstream cleanup failed: {error}"));
            }
        }
        self.storage.pool().close().await;

        match cleanup_error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.signal.start_shutdown();
        if let Some(task) = self.app_task.take() {
            task.abort();
        }
        if let Some(task) = self.fake_task.take() {
            task.abort();
        }
        if let Ok(tasks) = self.raw_tasks.get_mut() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

async fn abort_and_join(mut task: JoinHandle<Result<(), std::io::Error>>) -> TestResult {
    if !task.is_finished() {
        task.abort();
    }
    match tokio::time::timeout(WAIT_TIMEOUT, &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(error.into()),
        Ok(Err(error)) if error.is_cancelled() => Ok(()),
        Ok(Err(error)) => Err(format!("T4 background task panicked: {error}").into()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("T4 background task did not cancel within 5s".into())
        }
    }
}

async fn spawn_fake(
    config: AppConfig,
) -> TestResult<(SocketAddr, JoinHandle<Result<(), std::io::Error>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, fake_anthropic_app(config)).await });
    match request(
        addr,
        "GET",
        "/v1/models",
        &[("x-api-key", "sk-ant-readiness")],
        &[],
    )
    .await
    {
        Ok(response) if response.status == StatusCode::OK => Ok((addr, task)),
        Ok(response) => {
            task.abort();
            let _ = task.await;
            Err(format!("fake readiness returned {}", response.status).into())
        }
        Err(error) => {
            task.abort();
            let _ = task.await;
            Err(format!("fake readiness failed: {error}").into())
        }
    }
}

async fn open_test_storage(path: &Path, clock: ClockHandle) -> TestResult<Arc<SqliteStorage>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = Arc::new(open_sqlite(&database_url, clock).await?);
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

async fn seed_oauth_tokens(
    storage: &SqliteStorage,
    upstream_id: Uuid,
    fake_addr: SocketAddr,
) -> TestResult {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let verifier = "t4-verifier";
    use base64::Engine as _;
    use sha2::Digest as _;
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(sha2::Sha256::digest(verifier.as_bytes()));
    let authorize = client
        .get(format!("http://{fake_addr}/oauth/authorize"))
        .query(&[
            ("response_type", "code"),
            ("client_id", "t4-client"),
            ("redirect_uri", "http://localhost/callback"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("scope", "messages"),
        ])
        .send()
        .await?;
    assert_eq!(authorize.status(), reqwest::StatusCode::FOUND);
    let location = authorize
        .headers()
        .get(reqwest::header::LOCATION)
        .ok_or("missing OAuth redirect")?
        .to_str()?;
    let code = Url::parse(location)?
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .ok_or("missing OAuth code")?;
    let token: Value = client
        .post(format!("http://{fake_addr}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", "t4-client"),
            ("code", code.as_str()),
            ("code_verifier", verifier),
            ("redirect_uri", "http://localhost/callback"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let bundle = OAuthTokenBundle {
        access_token: token["access_token"]
            .as_str()
            .ok_or("missing access token")?
            .to_owned(),
        refresh_token: token["refresh_token"]
            .as_str()
            .ok_or("missing refresh token")?
            .to_owned(),
        expires_at_unix_secs: FIXED_UNIX_SECS + 3600,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["messages".to_owned()],
    };
    let aead = AeadService::from_master_key([0x44; 32]);
    let encrypted = EncryptedOAuthTokens::encrypt(&aead, &bundle, upstream_id.as_bytes())?;
    let record = UpstreamStore::get_by_id(storage, upstream_id)
        .await?
        .ok_or("missing OAuth upstream")?;
    UpstreamStore::store_oauth_tokens(storage, upstream_id, record.revision, encrypted).await?;
    Ok(())
}

pub fn install_price_catalog_fixture() {
    let models = HashMap::from([(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            by_tier: Default::default(),
        },
    )]);
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "t4-price-catalog".to_owned(),
        fetched_at_ms: FIXED_UNIX_SECS * 1_000,
        models,
        raw_json: serde_json::to_vec(&json!({
            MODEL: {
                "input_cost_per_token": 0.000003,
                "output_cost_per_token": 0.000015,
                "mode": "chat",
                "max_tokens": 8192
            }
        }))
        .expect("price catalog serializes"),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

pub fn message_body(stream: bool) -> Vec<u8> {
    let mut body = json!({
        "model": MODEL,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}]
    });
    if stream {
        body["stream"] = Value::Bool(true);
    }
    serde_json::to_vec(&body).expect("message body serializes")
}

pub struct TestResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

pub async fn proxy_request(
    app: &TestApp,
    request_id: &str,
    stream: bool,
    extra_headers: &[(&str, &str)],
) -> TestResult<TestResponse> {
    let mut headers = vec![
        ("content-type", "application/json"),
        ("request-id", request_id),
    ];
    if let Some(key) = app.downstream_api_key.as_deref() {
        headers.push(("x-api-key", key));
    }
    if stream {
        headers.push(("accept", "text/event-stream"));
    }
    headers.extend_from_slice(extra_headers);
    request(
        app.proxy_addr,
        "POST",
        "/v1/messages",
        &headers,
        &message_body(stream),
    )
    .await
}

pub async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> TestResult<TestResponse> {
    tokio::time::timeout(WAIT_TIMEOUT, raw_http(addr, method, path, headers, body))
        .await
        .map_err(|_| format!("HTTP request timed out after 5s: {method} {path}"))??
        .pipe(Ok)
}

pub async fn raw_http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<TestResponse> {
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    let stream = TcpStream::connect(addr).await?;
    let (mut reader, mut writer) = stream.into_split();
    writer.write_all(request.as_bytes()).await?;
    writer.write_all(body).await?;
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if response_is_complete(&bytes)? {
            break;
        }
    }
    parse_raw_response(&bytes)
}

fn response_is_complete(bytes: &[u8]) -> std::io::Result<bool> {
    let header_end = match bytes.windows(4).position(|window| window == b"\r\n\r\n") {
        Some(header_end) => header_end,
        None => return Ok(false),
    };
    let head = std::str::from_utf8(&bytes[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let body = &bytes[header_end + 4..];
    if head.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding")
                && value.trim().eq_ignore_ascii_case("chunked")
        })
    }) {
        return Ok(body.ends_with(b"0\r\n\r\n") || body.ends_with(b"0\r\n\r\n\r\n"));
    }
    let content_length = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    });
    Ok(content_length.is_some_and(|length| body.len() >= length))
}

pub fn parse_raw_response(bytes: &[u8]) -> std::io::Result<TestResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing headers"))?;
    let head = std::str::from_utf8(&bytes[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
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
    let encoded_body = &bytes[header_end + 4..];
    let body = if headers
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(encoded_body)?
    } else {
        encoded_body.to_vec()
    };
    Ok(TestResponse {
        status,
        headers,
        body,
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

pub async fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request ended before headers",
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = std::str::from_utf8(&bytes[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

pub fn happy_message_body() -> Value {
    json!({
        "id": "msg_fake_000000000000000000000000",
        "type": "message",
        "role": "assistant",
        "model": MODEL,
        "content": [{"type": "text", "text": "fake anthropic fixture response HELLO"}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 100, "output_tokens": 50}
    })
}

pub fn expected_sse_body() -> String {
    let events = [
        (
            "message_start",
            json!({"type":"message_start","message":{"id":"msg_fake_000000000000000000000000","type":"message","role":"assistant","model":MODEL,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":100,"output_tokens":0}}}),
        ),
        (
            "content_block_start",
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        ),
        (
            "content_block_delta",
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"fake anthropic fixture response HELLO "}}),
        ),
        (
            "content_block_stop",
            json!({"type":"content_block_stop","index":0}),
        ),
        (
            "message_delta",
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":100,"output_tokens":50}}),
        ),
        ("message_stop", json!({"type":"message_stop"})),
    ];
    let mut body = String::new();
    for (event, data) in events {
        body.push_str("event: ");
        body.push_str(event);
        body.push('\n');
        body.push_str("data: ");
        body.push_str(&data.to_string());
        body.push_str("\n\n");
    }
    body
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}
