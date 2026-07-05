#![allow(dead_code)]

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use cc_lb_storage_api::{
    BackendKind, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate,
    UpstreamStore,
    principal::Limit,
    types::{PrincipalKindLite, UpstreamKind as ManagedUpstreamKind},
};

use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::open_sqlite;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

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
    pub sqlite_path: PathBuf,
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

pub async fn spawn_test_server() -> TestServer {
    spawn_test_server_with_extra_config("").await
}

pub async fn spawn_test_server_with_extra_config(extra_toml: &str) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        AppConfig::default(),
        AuthConfig::NoneMode,
        Vec::new(),
    )
    .await
}

pub async fn spawn_test_server_with_fake_config(
    extra_toml: &str,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(extra_toml, fake_config, AuthConfig::NoneMode, Vec::new()).await
}

pub async fn spawn_test_server_with_apikey_mode(
    extra_toml: &str,
    principal_limits: Vec<Limit>,
    fake_config: AppConfig,
) -> TestServer {
    spawn_test_server_with_options(
        extra_toml,
        fake_config,
        AuthConfig::ApiKey,
        principal_limits,
    )
    .await
}

enum AuthConfig {
    NoneMode,
    ApiKey,
}

async fn spawn_test_server_with_options(
    extra_toml: &str,
    fake_config: AppConfig,
    auth_config: AuthConfig,
    principal_limits: Vec<Limit>,
) -> TestServer {
    let fake_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let fake_addr = fake_listener.local_addr().expect("fake local addr");
    let fake =
        tokio::spawn(
            async move { axum::serve(fake_listener, fake_anthropic_app(fake_config)).await },
        );

    let proxy_listener = reserve_addr();
    let admin_listener = reserve_addr();
    let metrics_listener = reserve_addr();
    let proxy_addr = proxy_listener.local_addr().expect("proxy local addr");
    let admin_addr = admin_listener.local_addr().expect("admin local addr");
    let metrics_addr = metrics_listener.local_addr().expect("metrics local addr");
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let config_path = config_dir.path().join("cc-lb.toml");
    let sqlite_path = config_path.with_file_name("cc-lb.sqlite");
    write_config_with_extra(
        &config_path,
        proxy_addr,
        admin_addr,
        metrics_addr,
        extra_toml,
        &auth_config,
    );
    let managed_key = seed_storage(&sqlite_path, fake_addr, &auth_config, principal_limits).await;

    drop((proxy_listener, admin_listener, metrics_listener));

    let child = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("serve")
        .arg("--config")
        .arg(&config_path)
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
        sqlite_path,
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
    )
}

fn write_config_with_extra(
    path: &Path,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    metrics_addr: SocketAddr,
    extra_toml: &str,
    auth_config: &AuthConfig,
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
        AuthConfig::NoneMode => {
            r#"[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "api-key"
upstream_kind = "anthropic_key"
"#
        }
        AuthConfig::ApiKey => {
            r#"[downstream_auth]
mode = "api_key"
"#
        }
    };
    let config = format!(
        r#"{extra_prefix}
[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "{admin_addr}"
metrics_addr = "{metrics_addr}"

[body]
messages_cap_bytes = 256
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
    UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "fake_anthropic".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(
                Url::parse(&format!("http://{upstream_addr}")).expect("fake upstream URL parses"),
            ),
            api_key_ciphertext: Some(vec![0; 32]),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("seed upstream");
    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "api-key".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: principal_limits,
        },
        1,
    )
    .await
    .expect("seed principal");
    match auth_config {
        AuthConfig::NoneMode => None,
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
        None => (text.to_owned(), String::new()),
    }
}

#[allow(dead_code)]
pub fn config_path(dir: &TempDir) -> PathBuf {
    dir.path().join("cc-lb.toml")
}
