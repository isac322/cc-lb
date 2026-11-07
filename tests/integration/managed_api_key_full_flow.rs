use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, UNIX_EPOCH};

use http::{HeaderMap, StatusCode};

use cc_lb_aead::AeadService;
use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog};
use cc_lb_server::{BuildError, build_app, signal::SignalHandle};
use cc_lb_storage_api::{
    Limit as KeyLimit, LimitKind as KeyLimitKind, MetaStore,
    principal::{
        Limit as PrincipalLimit, LimitKind as PrincipalLimitKind, PrincipalCreate, PrincipalKind,
        PrincipalStore,
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
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const ADMIN_TOKEN: &str = "task-31-admin-token";
const ADMIN_TOKEN_ENV: &str = "CC_LB_TASK_31_ADMIN_TOKEN";
const MASTER_KEY_ENV: &str = "CC_LB_TASK_31_MASTER_KEY";
const MASTER_KEY_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const MODEL: &str = "claude-3-5-sonnet-20241022";
const QA_TEXT_INPUT_TOKENS: u64 = 4_002;
const QA_TEXT_COST_MICROS: u64 = 12_021;

static TEST_ENV: Once = Once::new();

fn ensure_test_env() {
    TEST_ENV.call_once(|| unsafe {
        std::env::set_var(ADMIN_TOKEN_ENV, ADMIN_TOKEN);
        std::env::set_var(MASTER_KEY_ENV, MASTER_KEY_HEX);
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_api_key_full_flow() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(evidence_dir())?;
    let dir = tempfile::tempdir()?;
    ensure_test_env();

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
            model: MODEL,
        })
        .mount(&upstream)
        .await;

    let storage_path = dir.path().join("managed-api-key.sqlite");
    let initial_config = base_config(storage_path.clone(), litellm.uri());
    let (plaintext_key, key_id) = seed_runtime_state(&storage_path, upstream.uri(), "u1").await?;
    let server = StartedServer::start(initial_config).await?;
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

    let happy = send_message(&client, &server.proxy_url, &plaintext_key).await?;
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

    let (request_count, cost_usd_micros) = wait_for_usage(&storage_path, &key_id).await?;
    append_step(
        5,
        "persisted request events observed at least one request for issued key",
    )?;
    assert!(cost_usd_micros > 0);
    assert!(request_count > 0);
    append_step(
        6,
        "persisted request events have positive request count and cost",
    )?;

    usage_tokens.store(10_000, Ordering::SeqCst);
    let mut rejected = None;
    for _ in 0..90 {
        let response = send_message(&client, &server.proxy_url, &plaintext_key).await?;
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
    let revoke_response = client
        .request(
            "POST",
            &format!(
                "{}/admin/v1/principals/u1/keys/{key_id}/revoke",
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
    let revoked = send_message(&client, &server.proxy_url, &plaintext_key).await?;
    assert_eq!(
        revoked.status(),
        StatusCode::UNAUTHORIZED,
        "revoked response: {}",
        revoked.text().await?
    );
    append_step(8, "revoked key rejects /v1/messages with 401")?;
    server.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_image_admission_running_proxy_sqlite_regression()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_test_env();

    let client = TestClient::new(Duration::from_secs(10));
    let litellm = MockServer::start().await;
    let upstream = MockServer::start().await;
    let request_journal = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let usage_tokens = Arc::new(AtomicU64::new(0));

    // Use the canonical shared catalog fixture so concurrent integration
    // suites observe the same model and rates through the global catalog.
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(price_catalog_fixture()))
        .mount(&litellm)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(JournalResponder {
            output_tokens: usage_tokens,
            model: MODEL,
            request_journal: Arc::clone(&request_journal),
        })
        .mount(&upstream)
        .await;

    let storage_path = dir.path().join("qa1005-28.sqlite");
    let keys = seed_qa1005_keys(&storage_path, upstream.uri()).await?;
    let server = StartedServer::start(base_config(storage_path, litellm.uri())).await?;
    wait_for_price_catalog().await?;
    let text_body = qa_text_control_body();
    let [text_key, image_key, input_key, local_key, rejected_key] = keys.as_slice() else {
        return Err("QA1005-28 key fixture count changed".into());
    };
    let text_response = send_request_body(&client, &server.proxy_url, text_key, &text_body).await?;
    assert_eq!(
        text_response.status(),
        StatusCode::OK,
        "text control: {}",
        text_response.text().await?
    );

    let image_data = synthetic_png_base64(1024, 1024);
    assert!(
        image_data.len() > 1_000_000,
        "the regression fixture must be a large encoded PNG"
    );
    let image_body = qa_image_body(&image_data);
    let image_response =
        send_request_body(&client, &server.proxy_url, image_key, &image_body).await?;
    assert_eq!(
        image_response.status(),
        StatusCode::OK,
        "large native image: {}",
        image_response.text().await?
    );

    let input_response =
        send_request_body(&client, &server.proxy_url, input_key, &image_body).await?;
    assert_eq!(
        input_response.status(),
        StatusCode::OK,
        "image input-token allowance: {}",
        input_response.text().await?
    );

    let local_body = qa_local_prefix_body();
    let local_response =
        send_request_body(&client, &server.proxy_url, local_key, &local_body).await?;
    assert_eq!(
        local_response.status(),
        StatusCode::OK,
        "local prefix affordable at target boundary: {}",
        local_response.text().await?
    );

    let before_rejected = request_journal
        .lock()
        .expect("upstream request journal lock")
        .len();
    let rejected_response =
        send_request_body(&client, &server.proxy_url, rejected_key, &local_body).await?;
    assert_eq!(
        rejected_response.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "local prefix deterministic target-minus-one rejection: {}",
        rejected_response.text().await?
    );
    let after_rejected = request_journal
        .lock()
        .expect("upstream request journal lock")
        .len();
    assert_eq!(
        after_rejected, before_rejected,
        "a pre-dispatch 429 must not reach the upstream"
    );

    {
        let journal = request_journal
            .lock()
            .expect("upstream request journal lock");
        assert!(
            journal.iter().any(|body| body == &text_body),
            "the text control body must be forwarded unchanged"
        );
        assert!(
            journal.iter().any(|body| body == &image_body),
            "the native image body must be forwarded unchanged"
        );
        assert!(
            journal.iter().any(|body| body == &local_body),
            "the local-prefix body must be forwarded unchanged"
        );
        assert_eq!(
            journal.len(),
            4,
            "only admitted requests reach the upstream"
        );
    }
    server.shutdown().await;
    Ok(())
}

#[derive(Clone)]
struct JournalResponder {
    output_tokens: Arc<AtomicU64>,
    model: &'static str,
    request_journal: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Respond for JournalResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        self.request_journal
            .lock()
            .expect("upstream request journal lock")
            .push(request.body.clone());
        ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg_qa1005_28",
            "type": "message",
            "role": "assistant",
            "model": self.model,
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

#[derive(Clone, Copy)]
struct QaKeySpec {
    principal: &'static str,
    limits: &'static [KeyLimit],
}

// Admitted keys store 10x the target so exact-boundary requests fit in both
// cold (10%) and warm (100%) durable-usage states. The reject key intentionally
// stores the raw target-minus-one so it rejects in either state.
const QA_COST_TEXT: &[KeyLimit] = &[KeyLimit {
    kind: KeyLimitKind::CostUsd,
    window_secs: 60,
    cap_micros: 120_210, // cold target: 12,021; warm budget: 120,210
}];
const QA_COST_IMAGE: &[KeyLimit] = &[KeyLimit {
    kind: KeyLimitKind::CostUsd,
    window_secs: 60,
    cap_micros: 143_850, // cold target: 14,385; warm budget: 143,850
}];
const QA_INPUT_IMAGE: &[KeyLimit] = &[KeyLimit {
    kind: KeyLimitKind::InputTokens,
    window_secs: 60,
    cap_micros: 47_900, // cold target: 4,790; warm budget: 47,900
}];
const QA_COST_LOCAL: &[KeyLimit] = &[KeyLimit {
    kind: KeyLimitKind::CostUsd,
    window_secs: 60,
    cap_micros: 420_300, // cold target: 42,030; warm budget: 420,300
}];
const QA_COST_LOCAL_REJECT: &[KeyLimit] = &[KeyLimit {
    kind: KeyLimitKind::CostUsd,
    window_secs: 60,
    cap_micros: 42_029, // raw target-minus-one; rejects in cold and warm states
}];

const QA_KEY_SPECS: [QaKeySpec; 5] = [
    QaKeySpec {
        principal: "qa1005-28-text",
        limits: QA_COST_TEXT,
    },
    QaKeySpec {
        principal: "qa1005-28-image-cost",
        limits: QA_COST_IMAGE,
    },
    QaKeySpec {
        principal: "qa1005-28-image-input",
        limits: QA_INPUT_IMAGE,
    },
    QaKeySpec {
        principal: "qa1005-28-local",
        limits: QA_COST_LOCAL,
    },
    QaKeySpec {
        principal: "qa1005-28-local-reject",
        limits: QA_COST_LOCAL_REJECT,
    },
];

async fn send_request_body(
    client: &TestClient,
    proxy_url: &str,
    key: &str,
    body: &[u8],
) -> Result<TestResponse, Box<dyn std::error::Error>> {
    client
        .request(
            "POST",
            &format!("{proxy_url}/v1/messages"),
            &[("content-type", "application/json"), ("x-api-key", key)],
            body,
        )
        .await
        .map_err(Into::into)
}

fn qa_text_control_body() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "model": MODEL,
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "x".repeat(16_004)}]
    }))
    .expect("text control JSON serializes")
}

fn qa_image_body(data: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "model": MODEL,
        "max_tokens": 1,
        "messages": [{
            "role": "user",
            "content": [{
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": data
                }
            }]
        }]
    }))
    .expect("image JSON serializes")
}

fn qa_local_prefix_body() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "model": MODEL,
        "max_tokens": 1,
        "system": [{
            "type": "text",
            "text": "p".repeat(16_000),
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }],
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": "t".repeat(40_000)}]
        }]
    }))
    .expect("local-prefix JSON serializes")
}

fn synthetic_png_base64(width: u32, height: u32) -> String {
    fn crc32(bytes: &[u8]) -> u32 {
        let mut crc = !0_u32;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }

    fn append_chunk(png: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
        png.extend_from_slice(
            &u32::try_from(data.len())
                .expect("test PNG chunk fits in u32")
                .to_be_bytes(),
        );
        png.extend_from_slice(kind);
        png.extend_from_slice(data);
        let mut crc_input = Vec::with_capacity(kind.len() + data.len());
        crc_input.extend_from_slice(kind);
        crc_input.extend_from_slice(data);
        png.extend_from_slice(&crc32(&crc_input).to_be_bytes());
    }

    let row_len = usize::try_from(width).expect("test PNG width fits") + 1;
    let raw_len = row_len
        .checked_mul(usize::try_from(height).expect("test PNG height fits"))
        .expect("test PNG raster fits");
    let raw = vec![0_u8; raw_len];
    let mut zlib = vec![0x78, 0x01];
    let mut offset = 0;
    while offset < raw.len() {
        let remaining = raw.len() - offset;
        let block_len = remaining.min(65_535);
        let final_block = offset + block_len == raw.len();
        zlib.push(u8::from(final_block));
        let length = u16::try_from(block_len).expect("stored block fits");
        zlib.extend_from_slice(&length.to_le_bytes());
        zlib.extend_from_slice(&(!length).to_le_bytes());
        zlib.extend_from_slice(&raw[offset..offset + block_len]);
        offset += block_len;
    }
    let mut adler_a = 1_u32;
    let mut adler_b = 0_u32;
    for byte in &raw {
        adler_a = (adler_a + u32::from(*byte)) % 65_521;
        adler_b = (adler_b + adler_a) % 65_521;
    }
    zlib.extend_from_slice(&(adler_b << 16 | adler_a).to_be_bytes());

    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.extend_from_slice(&[8, 0, 0, 0, 0]);
    append_chunk(&mut png, b"IHDR", &ihdr);
    append_chunk(&mut png, b"IDAT", &zlib);
    append_chunk(&mut png, b"IEND", &[]);
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(png.len().div_ceil(3) * 4);
    for chunk in png.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = chunk.get(1).copied().map(u32::from).unwrap_or(0);
        let third = chunk.get(2).copied().map(u32::from).unwrap_or(0);
        let value = first << 16 | second << 8 | third;
        encoded.push(char::from(TABLE[((value >> 18) & 0x3f) as usize]));
        encoded.push(char::from(TABLE[((value >> 12) & 0x3f) as usize]));
        encoded.push(if chunk.len() > 1 {
            char::from(TABLE[((value >> 6) & 0x3f) as usize])
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            char::from(TABLE[(value & 0x3f) as usize])
        } else {
            '='
        });
    }
    encoded
}

#[derive(Clone)]
struct UsageResponder {
    output_tokens: Arc<AtomicU64>,
    model: &'static str,
}

impl Respond for UsageResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg_task31",
            "type": "message",
            "role": "assistant",
            "model": self.model,
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

struct ReservedConfig {
    config: Config,
    listener_reservations: [std::net::TcpListener; 2],
}

struct StartedServer {
    proxy_url: String,
    admin_url: String,
    signal: SignalHandle,
    task: Option<JoinHandle<Result<(), BuildError>>>,
}

impl StartedServer {
    async fn start(reserved: ReservedConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let ReservedConfig {
            config,
            listener_reservations,
        } = reserved;
        let proxy_addr = config.listener.proxy_addr;
        let admin_addr = config.listener.admin_addr;
        let clock: cc_lb_clock::ClockHandle = std::sync::Arc::new(cc_lb_clock::SystemClock);
        let app = build_app(config, clock).await?;
        let signal = app.signal_handle();
        // Keep all selected ports reserved while build_app performs its async setup.
        // App::start owns the real listeners immediately after this handoff.
        drop(listener_reservations);
        let task = tokio::spawn(async move { app.start().await });
        let mut server = Self {
            proxy_url: format!("http://{proxy_addr}"),
            admin_url: format!("http://{admin_addr}"),
            signal,
            task: Some(task),
        };
        server.wait_ready().await?;
        Ok(server)
    }

    async fn wait_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let client = TestClient::new(Duration::from_secs(1));
        let ready_secs = std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(60);
        let deadline = Instant::now() + Duration::from_secs(ready_secs);
        let proxy_url = self.proxy_url.clone();
        let admin_url = self.admin_url.clone();
        let task = self.task.as_mut().expect("server task must exist");
        loop {
            tokio::select! {
                result = &mut *task => {
                    let message = match result {
                        Ok(Ok(())) => "server exited before becoming ready".to_owned(),
                        Ok(Err(error)) => format!("server failed before becoming ready: {error}"),
                        Err(error) => format!("server task failed before becoming ready: {error}"),
                    };
                    return Err(std::io::Error::other(message).into());
                }
                (proxy_ok, admin_ok) = async {
                    let proxy_ok = client
                        .get_status(&format!("{proxy_url}/healthz"))
                        .await
                        .is_ok_and(|status| status == StatusCode::OK);
                    let admin_ok = client
                        .get_status(&format!("{admin_url}/admin/health"))
                        .await
                        .is_ok_and(|status| status == StatusCode::OK);
                    (proxy_ok, admin_ok)
                } => {
                    if proxy_ok && admin_ok {
                        return Ok(());
                    }
                }
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
    key: &str,
) -> Result<TestResponse, Box<dyn std::error::Error>> {
    let body = serde_json::to_vec(&json!({
        "model": MODEL,
        "max_tokens": 100,
        "messages": [{"role": "user", "content": "hi"}]
    }))?;
    client
        .request(
            "POST",
            &format!("{proxy_url}/v1/messages"),
            &[("content-type", "application/json"), ("x-api-key", key)],
            &body,
        )
        .await
        .map_err(Into::into)
}

/// Test-only read of persisted request events for one key, straight from the
/// SQLite payload column. Returns (request_count, summed cost_usd_micros).
async fn wait_for_usage(
    sqlite_path: &std::path::Path,
    key_id: &str,
) -> Result<(u64, i64), Box<dyn std::error::Error>> {
    use sqlx::Row;
    use sqlx::sqlite::SqlitePoolOptions;

    let database_url = format!("sqlite://{}", sqlite_path.display());
    let deadline = Instant::now() + Duration::from_secs(65);
    loop {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let rows =
            sqlx::query("SELECT list_cost_usd_micros FROM request_events_v1 WHERE key_id = ?")
                .bind(key_id)
                .fetch_all(&pool)
                .await?;
        pool.close().await;

        let mut request_count = 0_u64;
        let mut cost_usd_micros = 0_i64;
        for row in rows {
            request_count += 1;
            let cost: Option<i64> = row.try_get("list_cost_usd_micros")?;
            cost_usd_micros += cost.unwrap_or(0);
        }
        if request_count >= 1 {
            return Ok((request_count, cost_usd_micros));
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
        let ready = global_catalog().lookup(MODEL, None).is_some_and(|pricing| {
            pricing.input_per_million_usd == UsdPerMillion::from_whole_usd(3)
                && pricing.output_per_million_usd == UsdPerMillion::from_whole_usd(15)
        }) && global_catalog().estimate_max_with_cache(
            MODEL,
            QA_TEXT_INPUT_TOKENS,
            1,
            0,
            false,
            None,
        ) == Some(QA_TEXT_COST_MICROS);
        if ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("price catalog was not loaded with expected 3/15 prices".into());
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
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
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
        above_tokens_rates: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn append_step(step: u8, message: &str) -> std::io::Result<()> {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("step-{step}.log"));
    std::fs::write(path, format!("{} {message}\n", now_secs()))
}

fn evidence_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("integration crate lives under tests/integration")
        .join("target")
        .join("test-evidence")
        .join("managed-api-key-full-flow")
}

fn now_secs() -> u64 {
    use cc_lb_clock::Clock as _;
    let clock = cc_lb_clock::SystemClock;
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
    let created = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "anthropic-wiremock".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse(&upstream_url)?),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await?;
    // Must match the 32 bytes decoded from MASTER_KEY_HEX, which the server
    // loads via config.aead.key_env.
    let aead = AeadService::from_master_key([0x11; 32]);
    let ciphertext = aead.encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())?;
    UpstreamStore::update_api_key_secret(storage.as_ref(), created.id, Some(ciphertext)).await?;
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
                label: "prod".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![KeyLimit {
                    kind: KeyLimitKind::CostUsd,
                    window_secs: 60 * 60,
                    cap_micros: 1_000_000,
                }],
            },
        )
        .await?;
    let (key_id, _) = cc_lb_control::api_keys::secret::parse(plaintext.expose())?;
    Ok((plaintext.expose().to_owned(), key_id))
}

async fn seed_qa1005_keys(
    sqlite_path: &std::path::Path,
    upstream_url: String,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let storage = sqlite_storage(sqlite_path).await?;
    let created = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "qa1005-28-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse(&upstream_url)?),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await?;
    let aead = AeadService::from_master_key([0x11; 32]);
    let ciphertext = aead.encrypt(b"sk-ant-qa1005-28", created.id.as_bytes())?;
    UpstreamStore::update_api_key_secret(storage.as_ref(), created.id, Some(ciphertext)).await?;
    let mut keys = Vec::with_capacity(QA_KEY_SPECS.len());
    for spec in QA_KEY_SPECS {
        PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: spec.principal.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: vec!["claude-3-5-sonnet-*".to_owned()],
                allowed_upstreams: vec![],
                default_limits: vec![
                    PrincipalLimit {
                        kind: PrincipalLimitKind::Requests,
                        window_secs: 60,
                        cap_micros: 10,
                    },
                    PrincipalLimit {
                        kind: PrincipalLimitKind::TotalTokens,
                        window_secs: 60,
                        cap_micros: 1_000_000,
                    },
                ],
                cache_keepalive: None,
            },
            now_secs(),
        )
        .await?;
        let (_record, plaintext) = KeyStore::new(storage.clone())
            .create(
                spec.principal,
                CreateParams {
                    label: "qa1005-28".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: spec.limits.to_vec(),
                },
            )
            .await?;
        keys.push(plaintext.expose().to_owned());
    }
    Ok(keys)
}

fn base_config(sqlite_path: std::path::PathBuf, litellm_url: String) -> ReservedConfig {
    let proxy_reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve proxy port");
    let admin_reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve admin port");
    let metrics_addr = free_addr();
    let mut config = Config::default();
    config.listener.proxy_addr = proxy_reservation.local_addr().expect("proxy addr");
    config.listener.admin_addr = admin_reservation.local_addr().expect("admin addr");
    config.listener.metrics_addr = metrics_addr;
    config.timeouts.upstream_total_secs = 10;
    config.storage = StorageConfig::Sqlite { path: sqlite_path };
    config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
        id: "task-31".to_owned(),
        token_env: ADMIN_TOKEN_ENV.to_owned(),
    }];
    config.aead.key_env = MASTER_KEY_ENV.to_owned();
    config.price_catalog.url = format!("{litellm_url}/prices");
    config.price_catalog.cache_path = tempfile::tempdir()
        .expect("price cache tempdir")
        .keep()
        .join("prices.json");
    ReservedConfig {
        config,
        listener_reservations: [proxy_reservation, admin_reservation],
    }
}
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free addr")
}

async fn sqlite_storage(
    path: &std::path::Path,
) -> Result<Arc<SqliteStorage>, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, std::sync::Arc::new(cc_lb_clock::SystemClock)).await?;
    storage.initialize().await?;
    Ok(Arc::new(storage))
}
