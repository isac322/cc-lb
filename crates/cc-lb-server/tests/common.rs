#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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
    pub _fake: JoinHandle<Result<(), std::io::Error>>,
    pub _config_dir: TempDir,
    pub _process: TestProcess,
}

pub async fn spawn_test_server() -> TestServer {
    let fake_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let fake_addr = fake_listener.local_addr().expect("fake local addr");
    let fake = tokio::spawn(async move {
        axum::serve(fake_listener, fake_anthropic_app(AppConfig::default())).await
    });

    let proxy_addr = free_addr();
    let admin_addr = free_addr();
    let metrics_addr = free_addr();
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let config_path = config_dir.path().join("cc-lb.toml");
    write_config(
        &config_path,
        proxy_addr,
        admin_addr,
        metrics_addr,
        fake_addr,
    );

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

    wait_for_status(proxy_addr, "/v1/models", 200).await;
    wait_for_status(admin_addr, "/admin/health", 200).await;

    TestServer {
        proxy_addr,
        admin_addr,
        metrics_addr,
        _fake: fake,
        _config_dir: config_dir,
        _process: process,
    }
}

pub fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free local addr")
}

fn write_config(
    path: &Path,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    metrics_addr: SocketAddr,
    upstream_addr: SocketAddr,
) {
    let storage_path = path.with_file_name("cc-lb.redb");
    let data_dir = path.parent().expect("config path has parent");
    let bootstrap = format!(
        r#"
[[upstreams]]
name = "fake_anthropic"
kind = "custom"
base_url = "http://{upstream_addr}"

[[principals]]
name = "api-key"
kind = "machine"
"#,
    );
    std::fs::write(data_dir.join("bootstrap.toml"), bootstrap).expect("write bootstrap");
    let storage_path = storage_path.display();
    let data_dir = data_dir.display();
    let config = format!(
        r#"
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

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "api-key"
upstream_kind = "anthropic_key"
upstream_credential_ref = "fake_anthropic"

[storage]
kind = "redb"
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

pub async fn wait_for_status(addr: SocketAddr, path: &str, status: u16) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
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
