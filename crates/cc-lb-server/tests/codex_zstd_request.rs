use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use reqwest::Client;
use serde_json::json;
use sqlx::{Row, sqlite::SqlitePoolOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::common;
use fake_anthropic::AppConfig;

#[derive(Clone, Debug)]
struct RecordedRequest {
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Clone, Default)]
struct FakeState(Arc<Mutex<Vec<RecordedRequest>>>);

impl FakeState {
    fn snapshot(&self) -> Vec<RecordedRequest> {
        self.0.lock().expect("fake request lock").clone()
    }
}

struct FakeOpenAi {
    base_url: String,
    state: FakeState,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for FakeOpenAi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake_openai(State(state): State<FakeState>, request: Request) -> Response {
    let headers = request.headers().clone();
    let path = request.uri().path().to_owned();
    let body = to_bytes(request.into_body(), 1_048_576)
        .await
        .expect("fake OpenAI request body")
        .to_vec();
    state
        .0
        .lock()
        .expect("fake request lock")
        .push(RecordedRequest {
            headers,
            body: body.clone(),
        });

    if path != "/v1/responses" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unexpected path"})),
        )
            .into_response();
    }
    let request_json =
        serde_json::from_slice::<serde_json::Value>(&body).expect("fake OpenAI JSON request");
    if !request_json["stream"].as_bool().unwrap_or(false) {
        return Json(json!({
            "id": "resp_zstd",
            "object": "response",
            "status": "completed",
            "model": "gpt-5",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        }))
        .into_response();
    }
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from(concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_zstd\",\"status\":\"in_progress\",\"usage\":null}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_zstd\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
        )),
    )
        .into_response()
}

async fn spawn_fake_openai() -> FakeOpenAi {
    let state = FakeState::default();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake OpenAI listener");
    let addr = listener.local_addr().expect("fake OpenAI address");
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(fake_openai).with_state(task_state),
        )
        .await
    });
    FakeOpenAi {
        base_url: format!("http://{addr}/"),
        state,
        task,
    }
}

async fn configure_openai(server: &common::TestServer, fake: &FakeOpenAi) {
    let client = Client::new();
    let response = client
        .post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth("admin-token")
        .json(&json!({
            "name": "codex-zstd-openai",
            "kind": "openai_api_key",
            "base_url": fake.base_url,
            "api_key_value": "sk-codex-zstd-test",
            "skip_preflight": true
        }))
        .send()
        .await
        .expect("create fake OpenAI upstream");
    assert_eq!(response.status(), StatusCode::CREATED);
    let _ = response
        .text()
        .await
        .expect("read create upstream response");
}

async fn wait_for_malformed_event(server: &common::TestServer) -> (i64, Option<String>) {
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .expect("open request event database");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query(
            "SELECT COUNT(*) AS count, MAX(error_code) AS error_code
             FROM request_events_v1
             WHERE error_code = 'body_read_failed'",
        )
        .fetch_one(&pool)
        .await
        .expect("query malformed request event");
        let count = row.get::<i64, _>("count");
        let error_code = row.get::<Option<String>, _>("error_code");
        if count == 1 {
            return (count, error_code);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for exactly one malformed request event; count={count}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn request_body() -> Vec<u8> {
    br#"{"model":"gpt-5","input":"zstd-regression","stream":true}"#.to_vec()
}

async fn send_responses(
    server: &common::TestServer,
    body: Vec<u8>,
    encoding: Option<&str>,
) -> reqwest::Response {
    let client = Client::new();
    let mut request = client
        .post(format!("http://{}/openai/v1/responses", server.proxy_addr))
        .header("x-api-key", &server.managed_key.plaintext)
        .header("content-type", "application/json")
        .header("originator", "codex_cli_rs")
        .header("chatgpt-account-id", "codex-test-account")
        .header("user-agent", "codex-cli/1.0")
        .header("x-client-request-id", "codex-zstd-request-id")
        .body(body);
    if let Some(encoding) = encoding {
        request = request.header("content-encoding", encoding);
    }
    request.send().await.expect("proxy Responses request")
}

fn assert_forwarded_body(record: &RecordedRequest, original: &[u8], compressed: &[u8]) {
    match record.body.as_slice() {
        body if body == original => assert!(
            record.headers.get(header::CONTENT_ENCODING).is_none(),
            "decoded upstream body must not retain stale content-encoding"
        ),
        body if body == compressed => assert_eq!(
            record.headers.get(header::CONTENT_ENCODING),
            Some(&header::HeaderValue::from_static("zstd")),
            "forwarded compressed body must retain content-encoding: zstd"
        ),
        _ => panic!("fake OpenAI received mismatched request body"),
    }
}

fn assert_client_metadata(record: &RecordedRequest) {
    assert_eq!(record.headers["originator"], "codex_cli_rs");
    assert_eq!(record.headers["chatgpt-account-id"], "codex-test-account");
    assert_eq!(record.headers["user-agent"], "codex-cli/1.0");
    assert_eq!(
        record.headers["x-client-request-id"],
        "codex-zstd-request-id"
    );
}

#[tokio::test]
async fn authenticated_responses_accepts_uncompressed_and_zstd_requests() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server().await;
    let fake = spawn_fake_openai().await;
    configure_openai(&server, &fake).await;

    let original = request_body();
    let control = send_responses(&server, original.clone(), None).await;
    assert_eq!(control.status(), StatusCode::OK);
    let control_body = control
        .text()
        .await
        .expect("read uncompressed SSE response");
    assert!(control_body.contains("event: response.completed"));
    assert!(control_body.contains("\"status\":\"completed\""));
    let compressed = zstd::bulk::compress(&original, 1).expect("compress request body");
    let encoded = send_responses(&server, compressed.clone(), Some("zstd")).await;
    assert_eq!(encoded.status(), StatusCode::OK);
    let encoded_body = encoded.text().await.expect("read zstd SSE response");
    assert!(encoded_body.contains("event: response.completed"));
    assert!(encoded_body.contains("\"status\":\"completed\""));

    let requests = fake.state.snapshot();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body, original);
    assert_client_metadata(&requests[0]);
    assert_forwarded_body(&requests[1], &request_body(), &compressed);
    assert_client_metadata(&requests[1]);
}

async fn advertised_body_without_auth(
    server: &common::TestServer,
    api_key: Option<&str>,
) -> Vec<u8> {
    let mut stream = TcpStream::connect(server.proxy_addr)
        .await
        .expect("connect proxy for auth-before-body check");
    let auth = api_key
        .map(|key| format!("x-api-key: {key}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST /openai/v1/responses HTTP/1.1\r\nHost: {}\r\n{auth}content-type: application/json\r\ncontent-encoding: zstd\r\ncontent-length: 1048576\r\nConnection: close\r\n\r\n",
        server.proxy_addr
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write header-only request");
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
        .await
        .expect("auth response before advertised body")
        .expect("read auth response");
    response.extend_from_slice(&chunk[..read]);
    response
}

#[tokio::test]
async fn advertised_zstd_body_is_rejected_before_authentication_reads_body() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server().await;
    let fake = spawn_fake_openai().await;
    configure_openai(&server, &fake).await;
    let before = fake.state.snapshot().len();

    for api_key in [None, Some("invalid-principal")] {
        let response = advertised_body_without_auth(&server, api_key).await;
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 401"));
        assert_eq!(fake.state.snapshot().len(), before);
    }
}

#[tokio::test]
async fn malformed_zstd_body_is_rejected_without_upstream_request() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server().await;
    let fake = spawn_fake_openai().await;
    configure_openai(&server, &fake).await;
    let before = fake.state.snapshot().len();

    let response = send_responses(&server, b"not-a-zstd-frame".to_vec(), Some("zstd")).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response
        .json::<serde_json::Value>()
        .await
        .expect("malformed zstd error JSON");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    let (event_count, error_code) = wait_for_malformed_event(&server).await;
    assert_eq!(event_count, 1);
    assert_eq!(error_code.as_deref(), Some("body_read_failed"));
    assert_eq!(fake.state.snapshot().len(), before);
}

#[tokio::test]
async fn decoded_zstd_body_obeys_messages_cap_without_upstream_request() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_principal_limits_and_messages_cap(
        "",
        vec![],
        AppConfig::default(),
        128,
    )
    .await;
    let fake = spawn_fake_openai().await;
    configure_openai(&server, &fake).await;
    let before = fake.state.snapshot().len();

    let original = json!({
        "model": "gpt-5",
        "input": "x".repeat(256),
        "stream": true
    })
    .to_string()
    .into_bytes();
    assert!(original.len() > 128);
    let compressed = zstd::bulk::compress(&original, 1).expect("compress oversized request body");
    assert!(compressed.len() <= 128);
    let response = send_responses(&server, compressed, Some("zstd")).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(fake.state.snapshot().len(), before);
}

#[tokio::test]
async fn valid_large_window_zstd_body_is_accepted_with_small_messages_cap() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_principal_limits_and_messages_cap(
        "",
        vec![],
        AppConfig::default(),
        256,
    )
    .await;
    let fake = spawn_fake_openai().await;
    configure_openai(&server, &fake).await;

    let mut plain =
        br#"{"model":"gpt-5","input":"zstd-small-cap-control","stream":false}"#.to_vec();
    plain.resize(166, b' ');
    let mut encoded = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x31, 0x05, 0x00];
    encoded.extend_from_slice(&plain);
    assert_eq!(plain.len(), 166);
    assert_eq!(encoded.len(), 175);
    assert_eq!(
        zstd::bulk::decompress(&encoded, 256).expect("decode large-window fixture"),
        plain
    );

    let response = send_responses(&server, encoded, Some("zstd")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let requests = fake.state.snapshot();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body, plain);
    assert!(requests[0].headers.get(header::CONTENT_ENCODING).is_none());
}
