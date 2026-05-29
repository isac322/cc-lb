use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_config::{
    Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, StorageConfig,
};
use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_pricing::{UpstreamKind as PricingUpstreamKind, global_catalog};
use cc_lb_server::{BuildError, build_app, signal::SignalHandle};
use cc_lb_storage_api::{
    Limit as PrincipalLimit, LimitKind as PrincipalLimitKind, PrincipalCreate, PrincipalKind,
    PrincipalStore, UpstreamCreate, UpstreamKind, UpstreamStore,
};
use cc_lb_storage_redb::{
    Limit as KeyLimit, LimitKind as KeyLimitKind, PrincipalKindLite, Storage,
    UpstreamKind as KeyUpstreamKind,
};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
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
        std::env::set_var("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN);
    }

    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
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

    let storage_path = dir.path().join("managed-api-key.redb");
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

    append_step(2, "principal u1, upstream, and API key seeded through runtime storage")?;
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

    server.shutdown().await;


    usage_tokens.store(20, Ordering::SeqCst);
    let none_storage_path = dir.path().join("managed-api-key-none.redb");
    let none_config = base_config(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: "anon".to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            upstream_credential_ref: "k1".to_owned(),
        }),
        none_storage_path.clone(),
        litellm.uri(),
    );
    seed_runtime_state(&none_storage_path, upstream.uri(), "anon").await?;
    let none_server = StartedServer::start(none_config).await?;
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
        let app = build_app(config).await?;
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
        let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let proxy_ok = client
                .get(format!("{}/healthz", self.proxy_url))
                .send()
                .await
                .is_ok_and(|response| response.status() == StatusCode::OK);
            let admin_ok = client
                .get(format!("{}/admin/health", self.admin_url))
                .send()
                .await
                .is_ok_and(|response| response.status() == StatusCode::OK);
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
            let _ = timeout(Duration::from_secs(5), task).await;
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
    client: &Client,
    proxy_url: &str,
    key: Option<&str>,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut request = client
        .post(format!("{proxy_url}/v1/messages"))
        .header("content-type", "application/json")
        .json(&json!({
            "model": MODEL,
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "hi"}]
        }));
    if let Some(key) = key {
        request = request.header("x-api-key", key);
    }
    request.send().await
}

async fn wait_for_price_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if global_catalog()
            .lookup(MODEL, Some(PricingUpstreamKind::AnthropicKey))
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
    SystemTime::now()
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
    redb_path: &std::path::Path,
    upstream_url: String,
    principal_name: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let storage = Arc::new(Storage::open(redb_path, [0x11; 32])?);
    UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "anthropic-wiremock".to_owned(),
            kind: UpstreamKind::Custom,
            base_url: Some(Url::parse(&upstream_url)?),
            api_key_ciphertext: None,
        },
    )
    .await?;
    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: principal_name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["claude-3-5-sonnet-*".to_owned()],
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
        },
        now_secs(),
    )
    .await?;
    let (_record, plaintext) = KeyStore::new(storage).create(
        principal_name,
        CreateParams {
            upstream_kind: KeyUpstreamKind::AnthropicKey,
            upstream_credential_ref: "anthropic-wiremock".to_owned(),
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
    )?;
    let (key_id, _) = cc_lb_core::api_keys::secret::parse(plaintext.expose())?;
    Ok((plaintext.expose().to_owned(), key_id))
}

fn base_config(
    mode: DownstreamAuthMode,
    none_mode: Option<NoneModeConfig>,
    redb_path: std::path::PathBuf,
    litellm_url: String,
) -> Config {
    let mut config = Config::default();
    config.listener.proxy_addr = free_addr();
    config.listener.admin_addr = free_addr();
    config.listener.metrics_addr = free_addr();
    config.timeouts.upstream_total_secs = 10;
    config.downstream_auth.mode = mode;
    config.downstream_auth.none_mode = none_mode;
    config.storage = StorageConfig::Redb { path: redb_path };
    config.aead.key_env = MASTER_KEY_ENV.to_owned();
    config.api_keys.price_catalog.url = format!("{litellm_url}/prices");
    config.api_keys.price_catalog.refresh_interval = Duration::from_secs(60 * 60);
    config.api_keys.price_catalog.cache_path = tempfile::tempdir()
        .expect("price cache tempdir")
        .keep()
        .join("prices.json");
    config
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free addr")
}
