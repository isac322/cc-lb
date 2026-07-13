#![allow(dead_code)]

use std::io::Read;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle as ThreadJoinHandle;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_storage_api::{
    BUILTIN_CACHE_AFFINITY_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BackendKind, MetaStore,
    PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate, PrincipalKind,
    PrincipalStore, SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate, UpstreamStore,
    UpstreamSubscriptionQuotaStore,
    principal::Limit,
    types::{PrincipalKindLite, UpstreamKind as ManagedUpstreamKind},
};

use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::open_sqlite;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

pub struct TestProcess {
    child: Child,
    stderr: Arc<Mutex<Vec<u8>>>,
    stderr_thread: Option<ThreadJoinHandle<()>>,
    stdout_thread: Option<ThreadJoinHandle<()>>,
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(stderr_thread) = self.stderr_thread.take() {
            let _ = stderr_thread.join();
        }
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
    }
}

impl TestProcess {
    pub fn graceful_shutdown(&mut self) {
        #[cfg(unix)]
        {
            let pid = self.child.id();
            let mut kill_cmd = Command::new("kill")
                .arg("-15")
                .arg(pid.to_string())
                .spawn()
                .expect("spawn kill command");
            let _ = kill_cmd.wait();
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }

    fn spawn(config_path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
            .arg("serve")
            .arg("--config")
            .arg(config_path)
            .env(
                "CC_LB_MASTER_KEY",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .env("CC_LB_ADMIN_TOKEN", "admin-token")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cc-lb binary");
        let mut stderr_pipe = child.stderr.take().expect("child stderr pipe");
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_sink = Arc::clone(&stderr);
        let stderr_thread = std::thread::spawn(move || {
            let mut stderr_bytes = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut stderr_bytes);
            if let Ok(mut captured) = stderr_sink.lock() {
                captured.extend_from_slice(&stderr_bytes);
            }
        });
        let mut stdout_pipe = child.stdout.take().expect("child stdout pipe");
        let stdout_sink = Arc::clone(&stderr);
        let stdout_thread = std::thread::spawn(move || {
            let mut stdout_bytes = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut stdout_bytes);
            if let Ok(mut captured) = stdout_sink.lock() {
                captured.extend_from_slice(&stdout_bytes);
            }
        });

        Self {
            child,
            stderr,
            stderr_thread: Some(stderr_thread),
            stdout_thread: Some(stdout_thread),
        }
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    pub fn finish_stderr(&mut self) -> String {
        if let Some(stderr_thread) = self.stderr_thread.take() {
            let _ = stderr_thread.join();
        }
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
        self.stderr
            .lock()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default()
    }
}

pub struct TestServer {
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub sqlite_path: PathBuf,
    pub managed_key: Option<ManagedTestKey>,
    pub _fake: JoinHandle<Result<(), std::io::Error>>,
    pub _config_dir: TempDir,
    pub _process: TestProcess,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._fake.abort();
    }
}

impl TestServer {
    pub fn graceful_shutdown(&mut self) {
        self._process.graceful_shutdown();
    }

    pub fn finish_stderr(&mut self) -> String {
        self._process.finish_stderr()
    }
}

#[derive(Clone, Debug)]
pub struct ManagedTestKey {
    pub key_id: String,
    pub plaintext: String,
}

pub async fn spawn_test_server() -> TestServer {
    spawn_test_server_with_extra_config("").await
}

pub async fn spawn_test_server_with_extra_config(extra_toml: &str) -> TestServer {
    spawn_test_server_with_options(extra_toml, TestServerOptions::default()).await
}

pub async fn spawn_capture_test_server_with_extra_config(extra_toml: &str) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            proxy_readiness: ProxyReadiness::Tcp,
            ..TestServerOptions::default()
        },
    )
    .await
}

pub async fn spawn_test_server_with_fake_config(
    extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            fake_config,
            ..TestServerOptions::default()
        },
    )
    .await
}

pub async fn spawn_capture_test_server_with_fake_config(
    extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            fake_config,
            proxy_readiness: ProxyReadiness::Tcp,
            ..TestServerOptions::default()
        },
    )
    .await
}

pub async fn spawn_test_server_with_two_upstreams(
    extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            fake_config,
            topology: TestTopology::CacheAffinityPair,
            ..TestServerOptions::default()
        },
    )
    .await
}

pub async fn spawn_test_server_with_apikey_mode(
    extra_toml: &str,
    principal_limits: Vec<Limit>,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            fake_config,
            auth_config: AuthConfig::ApiKey,
            principal_limits,
            ..TestServerOptions::default()
        },
    )
    .await
}

pub async fn spawn_capture_test_server_with_apikey_mode(
    extra_toml: &str,
    principal_limits: Vec<Limit>,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            fake_config,
            auth_config: AuthConfig::ApiKey,
            principal_limits,
            proxy_readiness: ProxyReadiness::Tcp,
            ..TestServerOptions::default()
        },
    )
    .await
}

pub async fn spawn_capture_test_server_with_oauth_upstreams(
    extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        TestServerOptions {
            fake_config,
            auth_config: AuthConfig::NoneModeOauth,
            topology: TestTopology::SubscriptionPreferencePair,
            proxy_readiness: ProxyReadiness::Tcp,
            ..TestServerOptions::default()
        },
    )
    .await
}

enum AuthConfig {
    NoneMode,
    NoneModeOauth,
    ApiKey,
}

enum ProxyReadiness {
    RoutedModels,
    Tcp,
}

#[derive(Clone, Copy)]
enum TestTopology {
    Single,
    CacheAffinityPair,
    SubscriptionPreferencePair,
}

struct TestServerOptions {
    fake_config: AppConfig,
    auth_config: AuthConfig,
    principal_limits: Vec<Limit>,
    topology: TestTopology,
    proxy_readiness: ProxyReadiness,
}

impl Default for TestServerOptions {
    fn default() -> Self {
        Self {
            fake_config: AppConfig::default(),
            auth_config: AuthConfig::NoneMode,
            principal_limits: Vec::new(),
            topology: TestTopology::Single,
            proxy_readiness: ProxyReadiness::RoutedModels,
        }
    }
}

impl TestTopology {
    fn upstream_names(self) -> &'static [&'static str] {
        match self {
            Self::Single => &["fake_anthropic"],
            Self::CacheAffinityPair => &["fake_anthropic", "fake_anthropic_secondary"],
            Self::SubscriptionPreferencePair => {
                &["fake_anthropic_oauth", "fake_anthropic_oauth_secondary"]
            }
        }
    }

    fn messages_cap_bytes(self) -> u64 {
        match self {
            Self::Single => 256,
            Self::CacheAffinityPair | Self::SubscriptionPreferencePair => 131_072,
        }
    }

    const fn upstream_kind(self) -> UpstreamKind {
        match self {
            Self::Single | Self::CacheAffinityPair => UpstreamKind::AnthropicApiKey,
            Self::SubscriptionPreferencePair => UpstreamKind::AnthropicOauth,
        }
    }

    fn extra_config(self, fake_addr: SocketAddr) -> String {
        match self {
            Self::Single | Self::CacheAffinityPair => String::new(),
            Self::SubscriptionPreferencePair => format!(
                r#"[oauth.anthropic]
client_id = "capture-matrix-client"
auth_url = "http://{fake_addr}/oauth/authorize"
token_url = "http://{fake_addr}/oauth/token"
redirect_uri = "http://localhost/callback"
scopes = ["messages"]
"#
            ),
        }
    }
}

struct SpawnedTestServer {
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    metrics_addr: SocketAddr,
    sqlite_path: PathBuf,
    managed_key: Option<ManagedTestKey>,
    config_dir: TempDir,
    process: TestProcess,
}

struct StartupFailure {
    message: String,
    address_in_use: bool,
}

const SERVER_START_ATTEMPTS: usize = 4;

async fn spawn_test_server_with_options(
    extra_toml: &str,
    options: TestServerOptions,
) -> TestServer {
    let TestServerOptions {
        fake_config,
        auth_config,
        principal_limits,
        topology,
        proxy_readiness,
    } = options;
    let fake_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let fake_addr = fake_listener.local_addr().expect("fake local addr");
    let fake =
        tokio::spawn(
            async move { axum::serve(fake_listener, fake_anthropic_app(fake_config)).await },
        );

    for attempt in 1..=SERVER_START_ATTEMPTS {
        match spawn_test_server_attempt(
            extra_toml,
            fake_addr,
            &auth_config,
            principal_limits.clone(),
            topology,
            &proxy_readiness,
        )
        .await
        {
            Ok(spawned) => {
                return TestServer {
                    proxy_addr: spawned.proxy_addr,
                    admin_addr: spawned.admin_addr,
                    metrics_addr: spawned.metrics_addr,
                    sqlite_path: spawned.sqlite_path,
                    managed_key: spawned.managed_key,
                    _fake: fake,
                    _config_dir: spawned.config_dir,
                    _process: spawned.process,
                };
            }
            Err(error) if error.address_in_use && attempt < SERVER_START_ATTEMPTS => {
                eprintln!(
                    "cc-lb test server hit address-in-use during reserved-port handoff; retrying with fresh ports ({attempt}/{SERVER_START_ATTEMPTS})\n{}",
                    error.message
                );
            }
            Err(error) => panic!("{}", error.message),
        }
    }

    unreachable!("server start attempts loop always returns or panics")
}

async fn spawn_test_server_attempt(
    extra_toml: &str,
    fake_addr: SocketAddr,
    auth_config: &AuthConfig,
    principal_limits: Vec<Limit>,
    topology: TestTopology,
    proxy_readiness: &ProxyReadiness,
) -> Result<SpawnedTestServer, StartupFailure> {
    let proxy_listener = reserve_addr();
    let admin_listener = reserve_addr();
    let metrics_listener = reserve_addr();
    let proxy_addr = proxy_listener.local_addr().expect("proxy local addr");
    let admin_addr = admin_listener.local_addr().expect("admin local addr");
    let metrics_addr = metrics_listener.local_addr().expect("metrics local addr");
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let config_path = config_dir.path().join("cc-lb.toml");
    let sqlite_path = config_path.with_file_name("cc-lb.sqlite");
    let topology_config = topology.extra_config(fake_addr);
    let combined_extra = match (extra_toml.is_empty(), topology_config.is_empty()) {
        (true, true) => String::new(),
        (false, true) => extra_toml.to_owned(),
        (true, false) => topology_config,
        (false, false) => format!("{extra_toml}\n\n{topology_config}"),
    };
    write_config_with_extra(
        &config_path,
        proxy_addr,
        admin_addr,
        metrics_addr,
        &combined_extra,
        auth_config,
        topology.messages_cap_bytes(),
    );
    let managed_key = seed_storage(
        &sqlite_path,
        fake_addr,
        auth_config,
        principal_limits,
        topology,
    )
    .await;

    drop((proxy_listener, admin_listener, metrics_listener));

    let mut process = TestProcess::spawn(&config_path);

    match proxy_readiness {
        ProxyReadiness::RoutedModels => {
            wait_for_proxy_ready_or_exit(&mut process, proxy_addr, managed_key.as_ref()).await?;
        }
        ProxyReadiness::Tcp => wait_for_tcp_ready_or_exit(&mut process, proxy_addr).await?,
    }
    wait_for_status_or_exit(&mut process, admin_addr, "/admin/health", 200).await?;

    Ok(SpawnedTestServer {
        proxy_addr,
        admin_addr,
        metrics_addr,
        sqlite_path,
        managed_key,
        config_dir,
        process,
    })
}

pub fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free local addr")
}

fn reserve_addr() -> StdTcpListener {
    StdTcpListener::bind("127.0.0.1:0").expect("reserve free port")
}

fn write_config(
    path: &Path,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    metrics_addr: SocketAddr,
) {
    write_config_with_extra(
        path,
        proxy_addr,
        admin_addr,
        metrics_addr,
        "",
        &AuthConfig::NoneMode,
        256,
    )
}

fn write_config_with_extra(
    path: &Path,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    metrics_addr: SocketAddr,
    extra_toml: &str,
    auth_config: &AuthConfig,
    messages_cap_bytes: u64,
) {
    let storage_path = path.with_file_name("cc-lb.sqlite");
    let data_dir = path.parent().expect("config path has parent");
    let storage_path = storage_path.display();
    let data_dir = data_dir.display();
    // Extra TOML goes at the top so bare top-level keys attach to the root
    // table instead of the last-declared section (which would happen if
    // extra_toml were appended after e.g. `[egress]`).
    let extra_prefix = if extra_toml.is_empty() {
        String::new()
    } else {
        format!("{}\n\n", extra_toml.trim())
    };
    let downstream_auth = match auth_config {
        AuthConfig::NoneMode | AuthConfig::NoneModeOauth => {
            let upstream_kind = match auth_config {
                AuthConfig::NoneMode => "anthropic_key",
                AuthConfig::NoneModeOauth => "anthropic_o_auth",
                AuthConfig::ApiKey => unreachable!("API-key auth is handled separately"),
            };
            format!(
                r#"[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "api-key"
upstream_kind = "{upstream_kind}"
"#
            )
        }
        AuthConfig::ApiKey => String::from(
            r#"[downstream_auth]
mode = "api_key"
"#,
        ),
    };
    let config = format!(
        r#"{extra_prefix}
[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "{admin_addr}"
metrics_addr = "{metrics_addr}"

[body]
messages_cap_bytes = {messages_cap_bytes}
files_cap_bytes = 1048576

[timeouts]
request_header_secs = 10
request_body_chunk_secs = 30
idle_secs = 300
upstream_total_secs = 30
drain_secs = 5

[runtime]
data_dir = "{data_dir}"

{downstream_auth}

[storage]
kind = "sqlite"
path = "{storage_path}"

[aead]
key_env = "CC_LB_MASTER_KEY"

[observability]
tracing_level = "info"
log_redaction = true
user_prompt_redaction = false


[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[circuit_breaker]
failures_to_open = 5
window_secs = 10
half_open_after_secs = 30

[bulkhead]
max_conns_per_upstream = 50
semaphore_per_upstream = 100

[dns]
cache_ttl_floor_secs = 30
cache_ttl_ceiling_secs = 300

[egress]
"#
    );
    std::fs::write(path, config).expect("write config");
}

async fn seed_storage(
    storage_path: &Path,
    upstream_addr: SocketAddr,
    auth_config: &AuthConfig,
    principal_limits: Vec<Limit>,
    topology: TestTopology,
) -> Option<ManagedTestKey> {
    let database_url = format!("sqlite://{}", storage_path.display());
    let storage = std::sync::Arc::new(
        open_sqlite(
            &database_url,
            std::sync::Arc::new(cc_lb_engine::SystemClock),
        )
        .await
        .expect("test storage opens"),
    );
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("test storage initializes");
    for (ordinal, name) in topology.upstream_names().iter().enumerate() {
        let record = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: (*name).to_owned(),
                kind: topology.upstream_kind(),
                base_url: Some(
                    Url::parse(&format!("http://{upstream_addr}"))
                        .expect("fake upstream URL parses"),
                ),
                api_key_ciphertext: matches!(
                    topology.upstream_kind(),
                    UpstreamKind::AnthropicApiKey
                )
                .then(|| vec![0; 32]),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("seed upstream");
        if matches!(topology, TestTopology::SubscriptionPreferencePair) {
            seed_oauth_credentials(storage.as_ref(), &record, upstream_addr).await;
            seed_subscription_quota(storage.as_ref(), record.id, ordinal).await;
        }
    }
    let principal = PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "api-key".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: principal_limits,
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("seed principal");
    if matches!(topology, TestTopology::CacheAffinityPair) {
        PluginRegistryStore::insert_chain_entry(
            storage.as_ref(),
            PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Router,
                order: 1_000,
                wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID,
                config: serde_json::json!({}),
                sse_per_event: false,
                batched_events_per_flush: 1,
                batched_flush_ms: 100,
            },
        )
        .await
        .expect("seed cache-affinity router filter");
    }
    if matches!(topology, TestTopology::SubscriptionPreferencePair) {
        PluginRegistryStore::insert_chain_entry(
            storage.as_ref(),
            PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Router,
                order: 1_000,
                wasm_registry_id: BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
                config: serde_json::json!({}),
                sse_per_event: false,
                batched_events_per_flush: 1,
                batched_flush_ms: 100,
            },
        )
        .await
        .expect("seed subscription-preference router filter");
    }
    match auth_config {
        AuthConfig::NoneMode | AuthConfig::NoneModeOauth => None,
        AuthConfig::ApiKey => {
            let key_store = KeyStore::new(storage.clone());
            let (_record, plaintext) = key_store
                .create(
                    "api-key",
                    CreateParams {
                        upstream_kind: ManagedUpstreamKind::AnthropicKey,
                        label: "live-qa".to_owned(),
                        description: None,
                        expires_at_unix_secs: None,
                        limit_overrides: Vec::new(),
                        principal_kind: PrincipalKindLite::Machine,
                    },
                )
                .await
                .expect("seed managed key");
            let (key_id, _) = cc_lb_engine::api_keys::secret::parse(plaintext.expose())
                .expect("generated key parses");
            Some(ManagedTestKey {
                key_id,
                plaintext: plaintext.expose().to_owned(),
            })
        }
    }
}

async fn seed_oauth_credentials(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    record: &cc_lb_storage_api::UpstreamRecord,
    fake_addr: SocketAddr,
) {
    let tokens = issue_fake_oauth_tokens(fake_addr).await;
    let bundle = OAuthTokenBundle {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        expires_at_unix_secs: unix_now_secs().saturating_add(3_600),
        scopes: vec!["messages".to_owned()],
    };
    let encrypted = EncryptedOAuthTokens::encrypt(
        &AeadService::from_master_key([0; 32]),
        &bundle,
        record.id.as_bytes(),
    )
    .expect("encrypt fake OAuth tokens");
    UpstreamStore::store_oauth_tokens(storage, record.id, record.revision, encrypted)
        .await
        .expect("store fake OAuth tokens");
}

async fn seed_subscription_quota(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    upstream_id: uuid::Uuid,
    ordinal: usize,
) {
    let now_secs = unix_now_secs();
    let now_millis = now_secs.saturating_mul(1_000);
    let utilization = if ordinal == 0 { 0.20 } else { 0.65 };
    let samples = [
        subscription_quota_sample(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            utilization,
            now_secs.saturating_add(9_000),
            now_millis,
        ),
        subscription_quota_sample(
            upstream_id,
            SubscriptionQuotaWindow::SevenDay,
            utilization,
            now_secs.saturating_add(302_400),
            now_millis,
        ),
    ];
    storage
        .record_subscription_quota_samples(&samples)
        .await
        .expect("store subscription quota samples");
}

fn subscription_quota_sample(
    upstream_id: uuid::Uuid,
    window: SubscriptionQuotaWindow,
    utilization: f64,
    resets_at_unix_secs: u64,
    observed_at_unix_millis: u64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: uuid::Uuid::new_v4(),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(resets_at_unix_secs),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

#[derive(serde::Deserialize)]
struct IssuedOAuthTokens {
    access_token: String,
    refresh_token: String,
}

async fn issue_fake_oauth_tokens(fake_addr: SocketAddr) -> IssuedOAuthTokens {
    let verifier = "capture-matrix-verifier";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build fake OAuth client");
    let authorize = client
        .get(format!("http://{fake_addr}/oauth/authorize"))
        .query(&[
            ("response_type", "code"),
            ("client_id", "capture-matrix-client"),
            ("redirect_uri", "http://localhost/callback"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("scope", "messages"),
        ])
        .send()
        .await
        .expect("authorize fake OAuth token");
    let location = authorize
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .expect("fake OAuth authorization redirect");
    let code = Url::parse(location)
        .expect("parse fake OAuth redirect")
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .expect("fake OAuth authorization code");
    client
        .post(format!("http://{fake_addr}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", "capture-matrix-client"),
            ("redirect_uri", "http://localhost/callback"),
            ("code", code.as_str()),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .expect("exchange fake OAuth authorization code")
        .error_for_status()
        .expect("fake OAuth token exchange succeeds")
        .json()
        .await
        .expect("decode fake OAuth tokens")
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn ready_timeout(default: std::time::Duration) -> std::time::Duration {
    std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(default)
}

pub async fn wait_for_status(addr: SocketAddr, path: &str, status: u16) {
    wait_for_status_with_request(
        addr,
        path,
        status,
        &format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
}

async fn wait_for_proxy_ready(addr: SocketAddr, managed_key: Option<&ManagedTestKey>) {
    let api_key = managed_key
        .map(|key| key.plaintext.as_str())
        .unwrap_or("sk-ant-test");
    wait_for_status_with_request(
        addr,
        "/v1/models",
        200,
        &format!(
            "GET /v1/models HTTP/1.1\r\nHost: {addr}\r\nx-api-key: {api_key}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
}

async fn wait_for_proxy_ready_or_exit(
    process: &mut TestProcess,
    addr: SocketAddr,
    managed_key: Option<&ManagedTestKey>,
) -> Result<(), StartupFailure> {
    let api_key = managed_key
        .map(|key| key.plaintext.as_str())
        .unwrap_or("sk-ant-test");
    wait_for_status_with_request_or_exit(
        process,
        addr,
        "/v1/models",
        200,
        &format!(
            "GET /v1/models HTTP/1.1\r\nHost: {addr}\r\nx-api-key: {api_key}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await
}

async fn wait_for_tcp_ready_or_exit(
    process: &mut TestProcess,
    addr: SocketAddr,
) -> Result<(), StartupFailure> {
    let deadline = std::time::Instant::now() + ready_timeout(std::time::Duration::from_secs(60));
    loop {
        if let Some(exit_status) = process.try_wait().expect("poll cc-lb child") {
            let stderr = process.finish_stderr();
            return Err(startup_failure_for_exit(exit_status, stderr));
        }
        if TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(StartupFailure {
                message: format!("server did not accept TCP connections at {addr}"),
                address_in_use: false,
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn wait_for_status_or_exit(
    process: &mut TestProcess,
    addr: SocketAddr,
    path: &str,
    status: u16,
) -> Result<(), StartupFailure> {
    wait_for_status_with_request_or_exit(
        process,
        addr,
        path,
        status,
        &format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nConnection: close\r\n\r\n"
        ),
    )
    .await
}

async fn wait_for_status_with_request_or_exit(
    process: &mut TestProcess,
    addr: SocketAddr,
    path: &str,
    status: u16,
    request: &str,
) -> Result<(), StartupFailure> {
    let deadline = std::time::Instant::now() + ready_timeout(std::time::Duration::from_secs(60));
    loop {
        if let Some(exit_status) = process.try_wait().expect("poll cc-lb child") {
            let stderr = process.finish_stderr();
            return Err(startup_failure_for_exit(exit_status, stderr));
        }

        let last = match raw_http(addr, request).await {
            Ok(response) => {
                let last = format!("status={} body={}", response.status, response.body);
                if response.status == status {
                    return Ok(());
                }
                last
            }
            Err(error) => format!("error={error}"),
        };
        if std::time::Instant::now() >= deadline {
            return Err(StartupFailure {
                message: format!(
                    "server did not become ready at http://{addr}{path}; expected status {status}; last {last}"
                ),
                address_in_use: false,
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn startup_failure_for_exit(exit_status: ExitStatus, stderr: String) -> StartupFailure {
    let address_in_use =
        stderr.contains("Address already in use") || stderr.contains("os error 98");
    StartupFailure {
        message: format!(
            "cc-lb child exited before becoming ready: {exit_status}\nstderr:\n{stderr}"
        ),
        address_in_use,
    }
}

async fn wait_for_status_with_request(addr: SocketAddr, path: &str, status: u16, request: &str) {
    let deadline = std::time::Instant::now() + ready_timeout(std::time::Duration::from_secs(60));
    loop {
        let last = match raw_http(addr, request).await {
            Ok(response) => {
                let last = format!("status={} body={}", response.status, response.body);
                if response.status == status {
                    return;
                }
                last
            }
            Err(error) => format!("error={error}"),
        };
        if std::time::Instant::now() >= deadline {
            eprintln!(
                "server did not become ready at http://{addr}{path}; expected status {status}; last {}",
                last
            );
            panic!(
                "server did not become ready at http://{addr}{path}; expected status {status}; last {}",
                last
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub struct RawResponse {
    pub status: u16,
    pub headers: String,
    pub body: String,
}

pub async fn http_get(addr: SocketAddr, path: &str) -> std::io::Result<RawResponse> {
    raw_http(
        addr,
        &format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nConnection: close\r\n\r\n"
        ),
    )
    .await
}

pub async fn http_delete(addr: SocketAddr, path: &str) -> std::io::Result<RawResponse> {
    raw_http(
        addr,
        &format!(
            "DELETE {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nConnection: close\r\n\r\n"
        ),
    )
    .await
}

pub async fn http_post(
    addr: SocketAddr,
    path: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<RawResponse> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ncontent-length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    raw_http(addr, &request).await
}

pub async fn http_post_with_api_key(
    addr: SocketAddr,
    path: &str,
    api_key: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<RawResponse> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: {api_key}\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ncontent-length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    raw_http(addr, &request).await
}

async fn raw_http(addr: SocketAddr, request: &str) -> std::io::Result<RawResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(request.as_bytes()).await?;

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let (headers, body) = split_response(&text);
    Ok(RawResponse {
        status,
        headers,
        body,
    })
}

fn split_response(text: &str) -> (String, String) {
    match text.split_once("\r\n\r\n") {
        Some((headers, body)) => (headers.to_owned(), body.to_owned()),
        _ => (text.to_owned(), String::new()),
    }
}

#[allow(dead_code)]
pub fn config_path(dir: &TempDir) -> PathBuf {
    dir.path().join("cc-lb.toml")
}
