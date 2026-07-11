use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use http::{HeaderMap, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use cc_lb_config::{Config, DownstreamAuthMode, NoneModeConfig, StorageConfig};
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
use wiremock::{Mock, MockServer, ResponseTemplate};

const ADMIN_TOKEN: &str = "terminal-observation-admin-token";
const MASTER_KEY_ENV: &str = "CC_LB_TERMINAL_OBS_MASTER_KEY";
const MASTER_KEY_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const MODEL: &str = "claude-3-5-sonnet-20241022";

#[tokio::test(flavor = "multi_thread")]
async fn terminal_body_too_large() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _key_id) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let big_body = vec![b'x'; 33 * 1024 * 1024];
    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                ("x-api-key", &plaintext_key),
            ],
            &big_body,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("body_too_large"));
    assert_eq!(row.status, 413);
    assert!(row.event_id.is_some(), "event_id must be populated");

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_authentication_failed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (_key, _key_id) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                (
                    "x-api-key",
                    "sk-cclb-invalid_bogusbogusbogusbogusbogusbogusbogusbogusbogus",
                ),
            ],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("authentication_failed"));
    assert_eq!(row.status, 401);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_4xx() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "type": "error",
            "error": {"type": "invalid_request_error", "message": "bad model"}
        })))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(row.status, 400);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_5xx() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(529).set_body_json(json!({
            "type": "error",
            "error": {"type": "overloaded_error", "message": "overloaded"}
        })))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(
        response.status().is_server_error() || response.status() == StatusCode::from_u16(529)?,
        "expected 5xx, got {}",
        response.status()
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_5xx"));
    assert!(row.status >= 500);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_dispatch_failed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let closed_addr = free_addr();
    let closed_url = format!("http://{closed_addr}");
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, closed_url, "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(
        response.status().is_server_error(),
        "expected 5xx from dispatch failure, got {}",
        response.status()
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_dispatch_failed"));
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_route_no_upstream_after_filter() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let bogus_upstream_id = uuid::Uuid::nil();
    let (plaintext_key, _) = seed_runtime_state_upstream_restricted(
        &sqlite_path,
        upstream.uri(),
        "u1",
        vec![bogus_upstream_id],
    )
    .await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "expected non-2xx, got {}",
        response.status()
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(
        row.error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_limit_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) =
        seed_runtime_state_tight_limit(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let first = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(first.status(), StatusCode::OK);

    let second = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

    let row = wait_for_request_event_status(&sqlite_path, 429).await?;
    assert_eq!(row.error_code.as_deref(), Some("limit_rejected"));
    assert_eq!(row.status, 429);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_success_non_stream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert!(
        row.error_code.is_none(),
        "error_code must be NULL on success"
    );
    assert_eq!(row.status, 200);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_success_stream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = ChunkedSseMock::start(happy_sse_stream()).await?;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, true).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert!(
        row.error_code.is_none(),
        "SSE happy path must have NULL error_code; got {:?}, row={:?}",
        row.error_code,
        row,
    );
    assert_eq!(row.status, 200);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    upstream.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_stream_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("content-type", "text/event-stream")
                .set_body_string(mid_stream_error_sse()),
        )
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, true).await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "upstream returned 200 before injecting mid-stream error"
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(row.status, 200);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_tower_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(happy_response())
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let mut config = base_config(sqlite_path.clone(), litellm.uri());
    config.timeouts.upstream_total_secs = 1;
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("tower_timeout"));
    assert_eq!(row.status, 504);
    assert!(row.event_id.is_some(), "event_id must be populated");

    server.shutdown().await;
    Ok(())
}

fn happy_sse_stream() -> String {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n",
        "\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":12}}\n",
        "\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n",
        "\n",
    )
    .to_owned()
}

fn mid_stream_error_sse() -> String {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n",
        "\n",
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"stream aborted by upstream\"}}\n",
        "\n",
    )
    .to_owned()
}

fn sample_request_body(stream: bool) -> Vec<u8> {
    let mut body = serde_json::Map::new();
    body.insert("model".to_owned(), Value::String(MODEL.to_owned()));
    body.insert("max_tokens".to_owned(), json!(64));
    body.insert(
        "messages".to_owned(),
        json!([{"role": "user", "content": "hi"}]),
    );
    if stream {
        body.insert("stream".to_owned(), Value::Bool(true));
    }
    serde_json::to_vec(&Value::Object(body)).expect("sample body serializes")
}

async fn send_messages(
    server: &StartedServer,
    key: &str,
    stream: bool,
) -> std::io::Result<TestResponse> {
    let client = TestClient::new(Duration::from_secs(15));
    client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[("content-type", "application/json"), ("x-api-key", key)],
            &sample_request_body(stream),
        )
        .await
}

fn ensure_env() {
    unsafe {
        std::env::set_var(MASTER_KEY_ENV, MASTER_KEY_HEX);
        std::env::set_var("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN);
    }
}

async fn start_price_mock() -> MockServer {
    let litellm = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(price_catalog_fixture()))
        .mount(&litellm)
        .await;
    litellm
}

fn happy_response() -> Value {
    json!({
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": MODEL,
        "content": [{"type": "text", "text": "ok"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 5, "output_tokens": 3}
    })
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
        let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
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
        let deadline = Instant::now() + Duration::from_secs(10);
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

struct TestClient {
    timeout: Duration,
}

struct TestResponse {
    status: StatusCode,
    #[allow(dead_code)]
    headers: HeaderMap,
    #[allow(dead_code)]
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
    let body = bytes[header_end + 4..].to_vec();
    Ok(TestResponse {
        status,
        headers,
        body,
    })
}

#[derive(Debug)]
struct RequestEventRow {
    event_id: Option<String>,
    error_code: Option<String>,
    status: u16,
}

async fn wait_for_request_event(
    sqlite_path: &Path,
) -> Result<RequestEventRow, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(row) = query_latest_request_event(sqlite_path).await? {
            return Ok(row);
        }
        if Instant::now() >= deadline {
            return Err("no request_event row was persisted within 5s".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

// Multi-request tests must poll until the row with the *expected* status
// is the latest one — the async request_event writer commits after the
// downstream response returns, so a naive `wait_for_request_event` right
// after the second request can race and pick up the FIRST request's row.
// Callers that only ever fire one request should keep using
// `wait_for_request_event`.
async fn wait_for_request_event_status(
    sqlite_path: &Path,
    expected_status: u16,
) -> Result<RequestEventRow, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_seen: Option<RequestEventRow> = None;
    loop {
        if let Some(row) = query_latest_request_event(sqlite_path).await?
            && row.status == expected_status
        {
            return Ok(row);
        } else if let Some(row) = query_latest_request_event(sqlite_path).await? {
            last_seen = Some(row);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "no request_event row with status={expected_status} within 10s (last_seen={last_seen:?})",
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn query_latest_request_event(
    sqlite_path: &Path,
) -> Result<Option<RequestEventRow>, Box<dyn std::error::Error>> {
    use sqlx::Row;
    use sqlx::sqlite::SqlitePoolOptions;

    let database_url = format!("sqlite://{}", sqlite_path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    let row_opt = sqlx::query(
        "SELECT event_id, error_code, json_extract(payload, '$.status') AS status \
         FROM request_events_v1 ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&pool)
    .await?;
    pool.close().await;

    let Some(row) = row_opt else {
        return Ok(None);
    };
    let event_id: Option<String> = row.try_get("event_id")?;
    let error_code: Option<String> = row.try_get("error_code")?;
    let status: i64 = row.try_get("status")?;
    Ok(Some(RequestEventRow {
        event_id,
        error_code,
        status: status as u16,
    }))
}

async fn wait_for_price_catalog() -> Result<(), Box<dyn std::error::Error>> {
    seed_price_catalog();
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

fn seed_price_catalog() {
    let mut models = std::collections::HashMap::new();
    models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
        },
    );
    global_catalog().install_snapshot(CatalogSnapshot {
        fetched_at_ms: now_secs() * 1000,
        models,
        raw_json: serde_json::to_vec(&price_catalog_fixture()).expect("price fixture serializes"),
        cache_creation_per_million_usd: std::collections::HashMap::new(),
        cache_read_per_million_usd: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
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
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    seed_runtime_state_full(
        sqlite_path,
        upstream_url,
        principal_name,
        vec!["claude-3-5-sonnet-*".to_owned()],
        default_limits(),
    )
    .await
}

async fn seed_runtime_state_upstream_restricted(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
    allowed_upstreams: Vec<cc_lb_storage_api::UpstreamRecordId>,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    seed_runtime_state_full_v2(
        sqlite_path,
        upstream_url,
        principal_name,
        vec!["claude-3-5-sonnet-*".to_owned()],
        allowed_upstreams,
        default_limits(),
    )
    .await
}

async fn seed_runtime_state_tight_limit(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let tight = vec![
        PrincipalLimit {
            kind: PrincipalLimitKind::CostUsd,
            window_secs: 60 * 60,
            cap_micros: 1_000_000,
        },
        PrincipalLimit {
            kind: PrincipalLimitKind::Requests,
            window_secs: 60 * 60,
            cap_micros: 1,
        },
        PrincipalLimit {
            kind: PrincipalLimitKind::TotalTokens,
            window_secs: 60 * 60,
            cap_micros: 1_000_000,
        },
    ];
    seed_runtime_state_full(
        sqlite_path,
        upstream_url,
        principal_name,
        vec!["claude-3-5-sonnet-*".to_owned()],
        tight,
    )
    .await
}

fn default_limits() -> Vec<PrincipalLimit> {
    vec![
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
    ]
}

async fn seed_runtime_state_full(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
    allowed_models: Vec<String>,
    default_limits: Vec<PrincipalLimit>,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    seed_runtime_state_full_v2(
        sqlite_path,
        upstream_url,
        principal_name,
        allowed_models,
        vec![],
        default_limits,
    )
    .await
}

async fn seed_runtime_state_full_v2(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
    allowed_models: Vec<String>,
    allowed_upstreams: Vec<cc_lb_storage_api::UpstreamRecordId>,
    default_limits: Vec<PrincipalLimit>,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let storage = sqlite_storage(sqlite_path).await?;
    UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "anthropic-mock".to_owned(),
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
            allowed_models,
            allowed_upstreams,
            default_limits,
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

fn base_config(sqlite_path: std::path::PathBuf, litellm_url: String) -> Config {
    base_config_with_mode(DownstreamAuthMode::ApiKey, None, sqlite_path, litellm_url)
}

fn base_config_with_mode(
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
    config.aead.key_env = MASTER_KEY_ENV.to_owned();
    config.api_keys.price_catalog.url = format!("{litellm_url}/prices");
    config.api_keys.price_catalog.refresh_interval = Duration::from_secs(60 * 60);
    config.api_keys.price_catalog.cache_path = tempfile::tempdir()
        .expect("price cache tempdir")
        .keep()
        .join("prices.json");
    config
}

async fn sqlite_storage(path: &Path) -> Result<Arc<SqliteStorage>, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock)).await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(Arc::new(storage))
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free addr")
}

/// TCP-level mock that reproduces Anthropic's chunked SSE streaming shape.
/// `wiremock::set_body_raw` collapses this into a single Content-Length blob
/// which races with cc-lb's async_stream tail `finish()` — see PR #241.
struct ChunkedSseMock {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ChunkedSseMock {
    async fn start(sse_body: String) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(chunked_sse_accept_loop(listener, shutdown_rx, sse_body));
        Ok(Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    fn uri(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ChunkedSseMock {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn chunked_sse_accept_loop(
    listener: TcpListener,
    mut shutdown: oneshot::Receiver<()>,
    sse_body: String,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            accept = listener.accept() => {
                let Ok((socket, _)) = accept else { return };
                let body = sse_body.clone();
                tokio::spawn(chunked_sse_handle(socket, body));
            }
        }
    }
}

async fn chunked_sse_handle(mut socket: tokio::net::TcpStream, sse_body: String) {
    if drain_http_request(&mut socket).await.is_err() {
        return;
    }
    let head = "HTTP/1.1 200 OK\r\n\
                Content-Type: text/event-stream\r\n\
                Transfer-Encoding: chunked\r\n\
                Connection: close\r\n\
                \r\n";
    if socket.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    for frame in sse_body.split_inclusive("\n\n") {
        let framed = format!("{:x}\r\n{}\r\n", frame.len(), frame);
        if socket.write_all(framed.as_bytes()).await.is_err() {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    let _ = socket.write_all(b"0\r\n\r\n").await;
    let _ = socket.shutdown().await;
}

async fn drain_http_request(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    let mut acc: Vec<u8> = Vec::new();
    let mut content_length: Option<usize> = None;
    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        acc.extend_from_slice(&buf[..n]);
        if let Some(header_end) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
            if content_length.is_none() {
                let head = std::str::from_utf8(&acc[..header_end]).unwrap_or_default();
                for line in head.split("\r\n") {
                    if let Some(rest) = strip_header_prefix(line, "content-length") {
                        content_length = rest.trim().parse::<usize>().ok();
                        break;
                    }
                }
            }
            let body_start = header_end + 4;
            let body_end = body_start + content_length.unwrap_or(0);
            if acc.len() >= body_end {
                return Ok(());
            }
        }
    }
}

fn strip_header_prefix<'a>(line: &'a str, name_lower: &str) -> Option<&'a str> {
    let colon = line.find(':')?;
    let (name, rest) = line.split_at(colon);
    if name.trim().eq_ignore_ascii_case(name_lower) {
        Some(&rest[1..])
    } else {
        None
    }
}
