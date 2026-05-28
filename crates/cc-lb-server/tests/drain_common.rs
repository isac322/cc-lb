#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cc_lb_config::{
    AuthStrategy, Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, StorageConfig,
    UpstreamKind, UpstreamSpec,
};
use cc_lb_server::app::{BuildError, build_app_with_path};
use cc_lb_server::drain::DrainController;
use cc_lb_server::signal::SignalHandle;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

pub const STREAM_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#;

pub struct RunningApp {
    pub proxy_addr: SocketAddr,
    pub controller: DrainController,
    pub signals: SignalHandle,
    pub server: JoinHandle<Result<(), BuildError>>,
    pub _fake: JoinHandle<Result<(), std::io::Error>>,
    pub _config_dir: TempDir,
}

pub struct RunningRouter {
    pub proxy_addr: SocketAddr,
    pub controller: DrainController,
    pub server: JoinHandle<Result<(), std::io::Error>>,
    pub _fake: JoinHandle<Result<(), std::io::Error>>,
}

pub async fn start_app(drain_secs: u64, fake_config: AppConfig) -> RunningApp {
    let (upstream_addr, fake) = spawn_fake(fake_config).await;
    let proxy_addr = free_addr();
    let admin_addr = free_addr();
    let metrics_addr = free_addr();
    let config_dir = tempfile::tempdir().expect("config dir");
    let config_path = config_dir.path().join("cc-lb.toml");
    let mut config = config_for_upstream(upstream_addr, drain_secs);
    config.listener.proxy_addr = proxy_addr;
    config.listener.admin_addr = admin_addr;
    config.listener.metrics_addr = metrics_addr;

    let app = build_app_with_path(config, Some(&config_path)).expect("build app");
    let controller = app.drain_controller();
    let signals = app.signal_handle();
    let server = tokio::spawn(async move { app.start().await });

    wait_for_status(proxy_addr, "/v1/models", 200).await;

    RunningApp {
        proxy_addr,
        controller,
        signals,
        server,
        _fake: fake,
        _config_dir: config_dir,
    }
}

pub async fn start_router(drain_secs: u64, fake_config: AppConfig) -> RunningRouter {
    let (upstream_addr, fake) = spawn_fake(fake_config).await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy listener");
    let proxy_addr = listener.local_addr().expect("proxy addr");
    let mut config = config_for_upstream(upstream_addr, drain_secs);
    config.listener.proxy_addr = proxy_addr;

    let app = build_app_with_path(config, None).expect("build app");
    let controller = app.drain_controller();
    let router = app.router;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    wait_for_status(proxy_addr, "/v1/models", 200).await;

    RunningRouter {
        proxy_addr,
        controller,
        server,
        _fake: fake,
    }
}

pub async fn wait_for_in_flight(controller: &DrainController, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if controller.in_flight() >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "in-flight count did not reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn wait_for_force_closed(controller: &DrainController) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if controller.force_closed_total() > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "force-close counter did not increment"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn wait_for_status(addr: SocketAddr, path: &str, status: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = String::new();
    loop {
        if let Ok(response) = http_get(addr, path).await {
            last = format!("status={} body={}", response.status, response.body);
            if response.status == status {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "server did not become ready; last {last}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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

pub async fn http_post(
    addr: SocketAddr,
    path: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<RawResponse> {
    raw_post(addr, path, body, extra_headers, true).await
}

pub async fn http_post_without_api_key(
    addr: SocketAddr,
    path: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<RawResponse> {
    raw_post(addr, path, body, extra_headers, false).await
}

pub fn default_fake_config() -> AppConfig {
    AppConfig {
        slow_mode_bps: 1_048_576,
        files_cap_bytes: 104_857_600,
    }
}

pub fn slow_fake_config() -> AppConfig {
    AppConfig {
        slow_mode_bps: 8_192,
        files_cap_bytes: 104_857_600,
    }
}

pub fn very_slow_fake_config() -> AppConfig {
    AppConfig {
        slow_mode_bps: 1,
        files_cap_bytes: 104_857_600,
    }
}

fn configure_builtin_auth(config: &mut Config) {
    unsafe {
        std::env::set_var(
            "CC_LB_MASTER_KEY",
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
    }
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: "api-key".to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        upstream_credential_ref: "fake_anthropic".to_owned(),
    });
    config.storage = StorageConfig::Redb {
        path: unique_redb_path("cc-lb-drain"),
    };
}

fn unique_redb_path(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.redb", std::process::id()))
}

fn config_for_upstream(upstream_addr: SocketAddr, drain_secs: u64) -> Config {
    let mut config = Config::default();
    configure_builtin_auth(&mut config);
    config.timeouts.drain_secs = drain_secs;
    config.timeouts.upstream_total_secs = 30;
    config.upstreams.insert(
        "fake".to_owned(),
        UpstreamSpec {
            kind: UpstreamKind::Custom,
            base_url: Some(Url::parse(&format!("http://{upstream_addr}")).expect("upstream url")),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
    config
}

async fn spawn_fake(config: AppConfig) -> (SocketAddr, JoinHandle<Result<(), std::io::Error>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let addr = listener.local_addr().expect("fake addr");
    let task = tokio::spawn(async move { axum::serve(listener, fake_anthropic_app(config)).await });
    (addr, task)
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free addr")
}

async fn raw_post(
    addr: SocketAddr,
    path: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
    include_api_key: bool,
) -> std::io::Result<RawResponse> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ncontent-length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if include_api_key {
        request.push_str("x-api-key: sk-ant-test\r\n");
    }
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

pub struct RawResponse {
    pub status: u16,
    pub headers: String,
    pub body: String,
}

impl RawResponse {
    pub fn retry_after_is_60(&self) -> bool {
        self.headers
            .to_ascii_lowercase()
            .lines()
            .any(|line| line.trim() == "retry-after: 60")
    }
}
