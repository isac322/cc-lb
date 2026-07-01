#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};

use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_storage_api::{
    BackendKind, ConfigStore, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore,
    UpstreamCreate, UpstreamStore,
    principal::Limit,
    types::{PrincipalKindLite, UpstreamKind as ManagedUpstreamKind},
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::open_sqlite;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

pub fn install_prometheus() -> &'static PrometheusHandle {
    static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();
    PROMETHEUS.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("prometheus recorder")
    })
}

pub struct TestProcess {
    child: Child,
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub struct TestServer {
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub sqlite_path: std::path::PathBuf,
    pub managed_key: Option<ManagedTestKey>,
    pub _fake: JoinHandle<Result<(), std::io::Error>>,
    pub _config_dir: TempDir,
    pub _process: TestProcess,
}

#[derive(Clone, Debug)]
pub struct ManagedTestKey {
    pub key_id: String,
    pub plaintext: String,
}

#[derive(Clone, Copy)]
enum TestTopology {
    Single,
    CacheAffinityPair,
}

impl TestTopology {
    fn upstream_names(self) -> &'static [&'static str] {
        match self {
            Self::Single => &["fake_anthropic"],
            Self::CacheAffinityPair => &["fake_anthropic", "fake_anthropic_secondary"],
        }
    }

    fn messages_cap_bytes(self) -> u64 {
        match self {
            Self::Single => 256,
            Self::CacheAffinityPair => 131_072,
        }
    }

    fn prompt_cache_shadow_enabled(self) -> bool {
        matches!(self, Self::CacheAffinityPair)
    }
}

pub async fn spawn_test_server() -> TestServer {
    spawn_test_server_inner(
        AppConfig::default(),
        false,
        Vec::new(),
        TestTopology::Single,
    )
    .await
}

pub async fn spawn_test_server_with_extra_config(_extra_toml: &str) -> TestServer {
    spawn_test_server().await
}

pub async fn spawn_test_server_with_fake_config(
    _extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_inner(fake_config, false, Vec::new(), TestTopology::Single).await
}

pub async fn spawn_test_server_with_two_upstreams(
    _extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_inner(
        fake_config,
        false,
        Vec::new(),
        TestTopology::CacheAffinityPair,
    )
    .await
}

pub async fn spawn_test_server_with_apikey_mode(
    _extra_toml: &str,
    principal_limits: Vec<Limit>,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_inner(fake_config, true, principal_limits, TestTopology::Single).await
}

async fn spawn_test_server_inner(
    fake_config: AppConfig,
    api_key_mode: bool,
    principal_limits: Vec<Limit>,
    topology: TestTopology,
) -> TestServer {
    let fake_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let fake_addr = fake_listener.local_addr().expect("fake local addr");
    let fake =
        tokio::spawn(
            async move { axum::serve(fake_listener, fake_anthropic_app(fake_config)).await },
        );

    let proxy_addr = free_addr();
    let admin_addr = free_addr();
    let metrics_addr = free_addr();
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let storage_path = config_dir.path().join("cc-lb.sqlite");
    let managed_key = seed_storage(
        &storage_path,
        fake_addr,
        api_key_mode,
        principal_limits,
        topology,
    )
    .await;
    seed_runtime_overlay(&storage_path, api_key_mode, topology).await;

    let child = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("serve")
        .env("CC_LB_LISTENER__PROXY_ADDR", proxy_addr.to_string())
        .env("CC_LB_LISTENER__ADMIN_ADDR", admin_addr.to_string())
        .env("CC_LB_LISTENER__METRICS_ADDR", metrics_addr.to_string())
        .env("CC_LB_STORAGE__KIND", "sqlite")
        .env("CC_LB_STORAGE__PATH", storage_path.display().to_string())
        .env("CC_LB_DATA_DIR", config_dir.path().display().to_string())
        .env(
            "CC_LB_MASTER_KEY",
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .env("CC_LB_ADMIN_TOKEN", "admin-token")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn cc-lb binary");
    let process = TestProcess { child };

    wait_for_proxy_ready(proxy_addr, managed_key.as_ref()).await;
    wait_for_status(admin_addr, "/admin/health", 200).await;

    TestServer {
        proxy_addr,
        admin_addr,
        metrics_addr,
        sqlite_path: storage_path,
        managed_key,
        _fake: fake,
        _config_dir: config_dir,
        _process: process,
    }
}

pub fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free local addr")
}

async fn seed_runtime_overlay(storage_path: &Path, api_key_mode: bool, topology: TestTopology) {
    let database_url = format!("sqlite://{}", storage_path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
        .await
        .expect("test storage opens for overlay seed");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("test storage initializes for overlay seed");
    let downstream_auth = if api_key_mode {
        serde_json::json!({ "mode": "api_key" })
    } else {
        serde_json::json!({
            "mode": "none",
            "none_mode": {
                "principal_id": "api-key",
                "upstream_kind": "anthropic_key"
            }
        })
    };
    let mut overlay = serde_json::json!({
        "downstream_auth": downstream_auth,
        "body": { "messages_cap_bytes": topology.messages_cap_bytes(), "files_cap_bytes": 1048576 },
        "timeouts": { "upstream_total_secs": 30, "drain_secs": 5 },
    });
    if topology.prompt_cache_shadow_enabled() {
        overlay["prompt_cache_shadow"] =
            serde_json::json!({ "enabled": true, "refresh_debounce_secs": 0 });
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    storage
        .put_effective_config(0, overlay, now)
        .await
        .expect("seed effective_config");
}

async fn seed_storage(
    storage_path: &Path,
    upstream_addr: SocketAddr,
    api_key_mode: bool,
    principal_limits: Vec<Limit>,
    topology: TestTopology,
) -> Option<ManagedTestKey> {
    let database_url = format!("sqlite://{}", storage_path.display());
    let storage = open_sqlite(
        &database_url,
        std::sync::Arc::new(cc_lb_engine::SystemClock),
    )
    .await
    .expect("test storage opens");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("test storage initializes");
    for name in topology.upstream_names() {
        UpstreamStore::create(
            &storage,
            UpstreamCreate {
                name: (*name).to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: Some(
                    Url::parse(&format!("http://{upstream_addr}"))
                        .expect("fake upstream URL parses"),
                ),
                api_key_ciphertext: Some(vec![0; 32]),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("seed upstream");
    }
    let principal = PrincipalStore::create(
        &storage,
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
    if !api_key_mode {
        return None;
    }
    let key_store = KeyStore::new(Arc::new(storage));
    let (_record, plaintext) = key_store
        .create(
            &principal.name,
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
    let (key_id, _) =
        cc_lb_engine::api_keys::secret::parse(plaintext.expose()).expect("generated key parses");
    Some(ManagedTestKey {
        key_id,
        plaintext: plaintext.expose().to_owned(),
    })
}

fn ready_timeout(default: std::time::Duration) -> std::time::Duration {
    std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(default)
}

pub async fn wait_for_status(addr: SocketAddr, path: &str, status: u16) {
    let deadline = std::time::Instant::now() + ready_timeout(std::time::Duration::from_secs(60));
    loop {
        let last = match http_get(addr, path).await {
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

pub async fn wait_for_proxy_ready(addr: SocketAddr, managed_key: Option<&ManagedTestKey>) {
    let api_key = managed_key
        .map(|key| key.plaintext.as_str())
        .unwrap_or("sk-ant-test");
    let deadline = std::time::Instant::now() + ready_timeout(std::time::Duration::from_secs(60));
    loop {
        let last = match http_get_with_key(addr, "/v1/models", api_key).await {
            Ok(response) => {
                let last = format!("status={} body={}", response.status, response.body);
                if response.status == 200 {
                    return;
                }
                last
            }
            Err(error) => format!("error={error}"),
        };
        if std::time::Instant::now() >= deadline {
            eprintln!(
                "proxy did not become ready at http://{addr}/v1/models; expected status 200; last {}",
                last
            );
            panic!(
                "proxy did not become ready at http://{addr}/v1/models; expected status 200; last {}",
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
    http_get_with_key(addr, path, "sk-ant-test").await
}

pub async fn http_get_with_key(
    addr: SocketAddr,
    path: &str,
    api_key: &str,
) -> std::io::Result<RawResponse> {
    raw_http(
        addr,
        &format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: {api_key}\r\nConnection: close\r\n\r\n"
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
        None => (text.to_owned(), String::new()),
    }
}
