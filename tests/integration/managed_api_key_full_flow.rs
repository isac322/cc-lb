use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use http::{HeaderMap, StatusCode};

use cc_lb_config::{
    Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, StorageConfig,
};
use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_pricing::{
    CatalogSnapshot, CatalogStatus, Pricing, UpstreamKind as PricingUpstreamKind, UsdPerMillion,
    global_catalog,
};
use cc_lb_server::{BuildError, build_app, signal::SignalHandle};
use cc_lb_storage_api::{
    BackendKind, MetaStore,
    principal::{
        Limit as PrincipalLimit, LimitKind as PrincipalLimitKind, PrincipalCreate, PrincipalKind,
        PrincipalStore,
    },
    types::{
        Limit as KeyLimit, LimitKind as KeyLimitKind, PrincipalKindLite,
        UpstreamKind as KeyUpstreamKind,
    },
    upstream::{UpstreamCreate, UpstreamKind, UpstreamStore},
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const ADMIN_TOKEN: &str = "task-31-admin-token";
const MASTER_KEY_ENV: &str = "CC_LB_TASK_31_MASTER_KEY";
const MASTER_KEY_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const MODEL: &str = "claude-3-5-sonnet-20241022";

#[tokio::test(flavor = "multi_thread")]
async fn managed_api_key_full_flow() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(evidence_dir())?;
    let dir = tempfile::tempdir()?;
    unsafe {
        std::env::set_var(MASTER_KEY_ENV, MASTER_KEY_HEX);
    }

    let client = TestClient::new(Duration::from_secs(10));
    let litellm = MockServer::start().await;
    let usage_tokens = Arc::new(AtomicU64::new(20));
    let upstream = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(price_catalog_fixture()))
        .mount(&litellm)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(UsageResponder {
            output_tokens: usage_tokens.clone(),
        })
        .mount(&upstream)
        .await;

    let storage_path = dir.path().join("managed-api-key.sqlite");
    let initial_config = base_config(
        DownstreamAuthMode::ApiKey,
        None,
        storage_path.clone(),
        litellm.uri(),
    );
    let (plaintext_key, key_id) = seed_runtime_state(&storage_path, upstream.uri(), "u1").await?;
    let server = StartedServer::start(initial_config.clone()).await?;
    wait_for_price_catalog().await?;
    append_step(
        1,
        "setup complete: tempdir storage, wiremock LiteLLM/upstream, build_app server started",
    )?;

    append_step(
        2,
        "principal u1, upstream, and API key seeded through runtime storage",
    )?;
    append_step(3, &format!("issued key {key_id} for principal u1"))?;

    let happy = send_message(&client, &server.proxy_url, Some(&plaintext_key)).await?;
    assert_eq!(
        happy.status(),
        StatusCode::OK,
        "happy response: {}",
        happy.text().await?
    );
    for header in [
        "anthropic-ratelimit-requests-remaining",
        "anthropic-ratelimit-requests-limit",
        "anthropic-ratelimit-requests-reset",
        "anthropic-ratelimit-tokens-remaining",
        "anthropic-ratelimit-tokens-limit",
        "anthropic-ratelimit-tokens-reset",
    ] {
        assert!(
            happy.headers().contains_key(header),
            "missing rate-limit header {header}"
        );
    }
    append_step(
        4,
        "happy /v1/messages returned 200 with request and token rate-limit headers",
    )?;

    let usage = wait_for_usage(&client, &server.admin_url, &key_id).await?;
    append_step(
        5,
        "usage rollup observed at least one request event for issued key",
    )?;
    let first_series = usage["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|series| series["request_count"].as_u64().unwrap_or(0) > 0)
        .expect("usage series with request count");
    assert!(first_series["cost_usd_micros"].as_i64().unwrap_or(0) > 0);
    assert!(first_series["request_count"].as_u64().unwrap_or(0) > 0);
    append_step(
        6,
        "usage rollup has positive request_count and virtual_cost_micros",
    )?;

    usage_tokens.store(10_000, Ordering::SeqCst);
    let mut rejected = None;
    for _ in 0..90 {
        let response = send_message(&client, &server.proxy_url, Some(&plaintext_key)).await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            rejected = Some(response);
            break;
        }
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "pre-cap response: {}",
            response.text().await?
        );
    }
    let rejected = rejected.expect("cost cap should reject within bounded attempts");
    assert!(rejected.headers().contains_key("retry-after"));
    let rejected_body: Value = rejected.json().await?;
    assert_eq!(rejected_body["error"]["limit_kind"], "cost_usd");
    append_step(
        7,
        "cost cap reached and subsequent /v1/messages returned 429 with Retry-After",
    )?;

    let disable_response = client
        .request(
            "POST",
            &format!(
                "{}/admin/principals/u1/keys/{key_id}/disable",
                server.admin_url
            ),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
    assert_eq!(
        disable_response.status(),
        StatusCode::OK,
        "disable response: {}",
        disable_response.text().await?
    );
    let disabled = send_message(&client, &server.proxy_url, Some(&plaintext_key)).await?;
    assert_eq!(
        disabled.status(),
        StatusCode::FORBIDDEN,
        "disabled response: {}",
        disabled.text().await?
    );
    append_step(8, "disabled key rejects /v1/messages with 403")?;

    let enable_response = client
        .request(
            "POST",
            &format!(
                "{}/admin/principals/u1/keys/{key_id}/enable",
                server.admin_url
            ),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
    assert_eq!(
        enable_response.status(),
        StatusCode::OK,
        "enable response: {}",
        enable_response.text().await?
    );
    let key_response = client
        .request(
            "GET",
            &format!("{}/admin/principals/u1/keys/{key_id}", server.admin_url),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
    assert_eq!(
        key_response.status(),
        StatusCode::OK,
        "get key response: {}",
        key_response.text().await?
    );
    let key_body: Value = key_response.json().await?;
    assert_eq!(key_body["status"], "active");
    append_step(9, "enabled key reports Active through GET key endpoint")?;

    let revoke_response = client
        .request(
            "POST",
            &format!(
                "{}/admin/principals/u1/keys/{key_id}/revoke",
                server.admin_url
            ),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
    assert_eq!(
        revoke_response.status(),
        StatusCode::OK,
        "revoke response: {}",
        revoke_response.text().await?
    );
    let revoked = send_message(&client, &server.proxy_url, Some(&plaintext_key)).await?;
    assert_eq!(
        revoked.status(),
        StatusCode::UNAUTHORIZED,
        "revoked response: {}",
        revoked.text().await?
    );
    append_step(10, "revoked key rejects /v1/messages with 401")?;
    server.shutdown().await;

    usage_tokens.store(20, Ordering::SeqCst);
    let none_storage_path = dir.path().join("managed-api-key-none.sqlite");
    let none_config = base_config(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: "anon".to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        }),
        none_storage_path.clone(),
        litellm.uri(),
    );
    seed_runtime_state(&none_storage_path, upstream.uri(), "anon").await?;
    let none_server = StartedServer::start(none_config).await?;
    wait_for_price_catalog().await?;
    let none_response = send_message(&client, &none_server.proxy_url, None).await?;
    assert_eq!(
        none_response.status(),
        StatusCode::OK,
        "mode none response: {}",
        none_response.text().await?
    );
    none_server.shutdown().await;
    assert_ne!(storage_path, none_storage_path);
    append_step(
        11,
        "mode=None server accepted request without x-api-key on isolated storage path",
    )?;

    Ok(())
}

#[derive(Clone)]
struct UsageResponder {
    output_tokens: Arc<AtomicU64>,
}

impl Respond for UsageResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg_task31",
            "type": "message",
            "role": "assistant",
            "model": MODEL,
            "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 10,
                "output_tokens": self.output_tokens.load(Ordering::SeqCst),
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0
            }
        }))
    }
}

struct TestClient {
    timeout: Duration,
}

struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl TestClient {
    fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    async fn get_status(&self, url: &str) -> std::io::Result<StatusCode> {
        self.request("GET", url, &[], &[])
            .await
            .map(|response| response.status)
    }

    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> std::io::Result<TestResponse> {
        tokio::time::timeout(self.timeout, raw_http(method, url, headers, body))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "request timed out"))?
    }
}

impl TestResponse {
    fn status(&self) -> StatusCode {
        self.status
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn text(&self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.body.clone())
    }

    async fn json(&self) -> serde_json::Result<Value> {
        serde_json::from_slice(&self.body)
    }
}

async fn raw_http(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<TestResponse> {
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

fn parse_raw_response(bytes: &[u8]) -> std::io::Result<TestResponse> {
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

struct StartedServer {
    proxy_url: String,
    admin_url: String,
    signal: SignalHandle,
    task: Option<JoinHandle<Result<(), BuildError>>>,
}

impl StartedServer {
    async fn start(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let proxy_addr = config.listener.proxy_addr;
        let admin_addr = config.listener.admin_addr;
        let clock: cc_lb_engine::ClockHandle = std::sync::Arc::new(cc_lb_engine::SystemClock);
        let app = build_app(config, clock).await?;
        let signal = app.signal_handle();
        let task = tokio::spawn(async move { app.start().await });
        let server = Self {
            proxy_url: format!("http://{proxy_addr}"),
            admin_url: format!("http://{admin_addr}"),
            signal,
            task: Some(task),
        };
        server.wait_ready().await?;
        Ok(server)
    }

    async fn wait_ready(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = TestClient::new(Duration::from_secs(1));
        let ready_secs = std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(60);
        let deadline = Instant::now() + Duration::from_secs(ready_secs);
        loop {
            let proxy_ok = client
                .get_status(&format!("{}/healthz", self.proxy_url))
                .await
                .is_ok_and(|status| status == StatusCode::OK);
            let admin_ok = client
                .get_status(&format!("{}/admin/health", self.admin_url))
                .await
                .is_ok_and(|status| status == StatusCode::OK);
            if proxy_ok && admin_ok {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("server did not become ready".into());
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(mut self) {
        self.signal.start_shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for StartedServer {
    fn drop(&mut self) {
        self.signal.start_shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn send_message(
    client: &TestClient,
    proxy_url: &str,
    key: Option<&str>,
) -> Result<TestResponse, Box<dyn std::error::Error>> {
    let body = serde_json::to_vec(&json!({
        "model": MODEL,
        "max_tokens": 100,
        "messages": [{"role": "user", "content": "hi"}]
    }))?;
    let mut headers = vec![("content-type", "application/json")];
    if let Some(key) = key {
        headers.push(("x-api-key", key));
    }
    client
        .request("POST", &format!("{proxy_url}/v1/messages"), &headers, &body)
        .await
        .map_err(Into::into)
}

async fn wait_for_usage(
    client: &TestClient,
    admin_url: &str,
    key_id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(65);
    loop {
        let response = client
            .request(
                "GET",
                &format!("{admin_url}/admin/principals/u1/keys/{key_id}/usage?range=1h&step=1h"),
                &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
                &[],
            )
            .await?;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "usage response: {}",
            response.text().await?
        );
        let body: Value = response.json().await?;
        let observed = body["series"].as_array().is_some_and(|series| {
            !series.is_empty()
                && series
                    .iter()
                    .any(|entry| entry["request_count"].as_u64().unwrap_or(0) >= 1)
        });
        if observed {
            return Ok(body);
        }
        if Instant::now() >= deadline {
            return Err("usage was not observed within 65s".into());
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn wait_for_price_catalog() -> Result<(), Box<dyn std::error::Error>> {
    seed_price_catalog();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if global_catalog()
            .lookup(MODEL, Some(PricingUpstreamKind::AnthropicKey), None)
            .is_some()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("price catalog was not loaded".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn seed_price_catalog() {
    let mut models = std::collections::HashMap::new();
    models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            by_tier: Default::default(),
        },
    );
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "test-fixture-hash".to_owned(),
        fetched_at_ms: now_secs() * 1000,
        models,
        raw_json: serde_json::to_vec(&price_catalog_fixture()).expect("price fixture serializes"),
        cache_creation_per_million_usd: std::collections::HashMap::new(),
        cache_read_per_million_usd: std::collections::HashMap::new(),
        cache_creation_per_million_usd_by_tier: std::collections::HashMap::new(),
        cache_read_per_million_usd_by_tier: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn append_step(step: u8, message: &str) -> std::io::Result<()> {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("task-31-step-{step}.log"));
    std::fs::write(path, format!("{} {message}\n", now_secs()))
}

fn evidence_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("integration crate lives under tests/integration")
        .join(".omo/evidence")
}

fn now_secs() -> u64 {
    use cc_lb_engine::Clock as _;
    let clock = cc_lb_engine::SystemClock;
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn price_catalog_fixture() -> Value {
    json!({
        MODEL: {
            "input_cost_per_token": 0.000003,
            "output_cost_per_token": 0.000015,
            "mode": "chat",
            "max_tokens": 8192
        }
    })
}

async fn seed_runtime_state(
    sqlite_path: &std::path::Path,
    upstream_url: String,
    principal_name: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let storage = sqlite_storage(sqlite_path).await?;
    UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "anthropic-wiremock".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse(&upstream_url)?),
            api_key_ciphertext: Some(Vec::new()),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await?;
    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: principal_name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["claude-3-5-sonnet-*".to_owned()],
            allowed_upstreams: vec![],
            default_limits: vec![
                PrincipalLimit {
                    kind: PrincipalLimitKind::CostUsd,
                    window_secs: 60 * 60,
                    cap_micros: 1_000_000,
                },
                PrincipalLimit {
                    kind: PrincipalLimitKind::Requests,
                    window_secs: 60 * 60,
                    cap_micros: 1_000,
                },
                PrincipalLimit {
                    kind: PrincipalLimitKind::TotalTokens,
                    window_secs: 60 * 60,
                    cap_micros: 1_000_000,
                },
            ],
            cache_keepalive: None,
        },
        now_secs(),
    )
    .await?;
    let (_record, plaintext) = KeyStore::new(storage)
        .create(
            principal_name,
            CreateParams {
                upstream_kind: KeyUpstreamKind::AnthropicKey,
                label: "prod".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![KeyLimit {
                    kind: KeyLimitKind::CostUsd,
                    window_secs: 60 * 60,
                    cap_micros: 1_000_000,
                }],
                principal_kind: PrincipalKindLite::Machine,
            },
        )
        .await?;
    let (key_id, _) = cc_lb_engine::api_keys::secret::parse(plaintext.expose())?;
    Ok((plaintext.expose().to_owned(), key_id))
}

fn base_config(
    mode: DownstreamAuthMode,
    none_mode: Option<NoneModeConfig>,
    sqlite_path: std::path::PathBuf,
    litellm_url: String,
) -> Config {
    let mut config = Config::default();
    config.listener.proxy_addr = free_addr();
    config.listener.admin_addr = free_addr();
    config.listener.metrics_addr = free_addr();
    config.timeouts.upstream_total_secs = 10;
    config.downstream_auth.mode = mode;
    config.downstream_auth.none_mode = none_mode;
    config.storage = StorageConfig::Sqlite { path: sqlite_path };
    config.admin.token = Some(ADMIN_TOKEN.to_owned());
    config.aead.key_env = MASTER_KEY_ENV.to_owned();
    config.api_keys.price_catalog.url = format!("{litellm_url}/prices");
    config.api_keys.price_catalog.refresh_interval = Duration::from_secs(60 * 60);
    config.api_keys.price_catalog.cache_path = tempfile::tempdir()
        .expect("price cache tempdir")
        .keep()
        .join("prices.json");
    config
}

async fn sqlite_storage(
    path: &std::path::Path,
) -> Result<Arc<SqliteStorage>, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(
        &database_url,
        std::sync::Arc::new(cc_lb_engine::SystemClock),
    )
    .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(Arc::new(storage))
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free addr")
}
