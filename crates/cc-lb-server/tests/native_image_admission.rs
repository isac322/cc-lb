//! Real-binary admission contracts: image encoding size is not visual input quantity.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::Engine as _;
use cc_lb_storage_api::principal::{Limit, LimitKind};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use serde_json::{Value, json};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::common;

const DEADLINE: Duration = Duration::from_secs(20);
const BODY_CAP: u64 = 8 * 1024 * 1024;
const WINDOW_SECS: u64 = 3_600;
const INPUT_CAP: i64 = 100_000;
const ACTUAL_INPUT: i64 = 1_500;

#[derive(Clone, Copy, Debug)]
enum Provider {
    OpenAi,
    Gemini,
}

impl Provider {
    fn name(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Gemini => "gemini",
        }
    }

    fn model(self) -> &'static str {
        match self {
            Self::OpenAi => "gpt-4.1",
            Self::Gemini => "gemini-2.5-flash",
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::OpenAi => "/openai/v1/responses",
            Self::Gemini => "/gemini/v1beta/models/gemini-2.5-flash:generateContent",
        }
    }

    fn provider_path(self) -> &'static str {
        match self {
            Self::OpenAi => "/v1/responses",
            Self::Gemini => "/v1beta/models/gemini-2.5-flash:generateContent",
        }
    }

    fn text_request(self, text: &str) -> Value {
        match self {
            Self::OpenAi => json!({
                "model": self.model(), "max_output_tokens": 1, "store": false,
                "input": [{"role": "user", "content": [{"type": "input_text", "text": text}]}]
            }),
            Self::Gemini => json!({
                "contents": [{"role": "user", "parts": [{"text": text}]}],
                "generationConfig": {"maxOutputTokens": 1}
            }),
        }
    }

    fn image_request(self, encoded: &str) -> Value {
        let mut request = self.text_request("describe");
        match self {
            Self::OpenAi => request["input"][0]["content"]
                .as_array_mut()
                .expect("Responses content")
                .push(json!({
                    "type": "input_image", "detail": "low",
                    "image_url": format!("data:image/png;base64,{encoded}")
                })),
            Self::Gemini => request["contents"][0]["parts"]
                .as_array_mut()
                .expect("Gemini parts")
                .push(json!({"inlineData": {"mimeType": "image/png", "data": encoded}})),
        }
        request
    }

    fn reference_request(self) -> Value {
        // No pixels are available from these references. The supported model's
        // conservative per-image quantity, accumulated across 200 images, must
        // exceed 100,000. These are not network resources for the estimator.
        let images: Vec<_> = (0..200)
            .map(|index| match self {
                Self::OpenAi => json!({
                    "type": "input_image", "detail": "high",
                    "image_url": format!("https://images.invalid/{index}.png")
                }),
                Self::Gemini => json!({"fileData": {
                    "mimeType": "image/png", "fileUri": format!("gs://missing-images/{index}.png")
                }}),
            })
            .collect();
        let mut request = self.text_request("describe");
        match self {
            Self::OpenAi => request["input"][0]["content"] = json!(images),
            Self::Gemini => request["contents"][0]["parts"] = json!(images),
        }
        request
    }

    fn image_payload(self, request: &Value) -> &Value {
        match self {
            Self::OpenAi => &request["input"][0]["content"][1],
            Self::Gemini => &request["contents"][0]["parts"][1],
        }
    }

    fn response(self) -> Value {
        match self {
            Self::OpenAi => json!({
                "id": "resp_image_admission", "object": "response", "status": "completed",
                "model": self.model(), "store": false,
                "output": [{
                    "type": "message", "id": "msg_image_admission", "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "ok", "annotations": []}]
                }],
                "usage": {"input_tokens": ACTUAL_INPUT, "output_tokens": 1, "total_tokens": 1501}
            }),
            Self::Gemini => json!({
                "candidates": [{"content": {"role": "model", "parts": [{"text": "ok"}]}, "finishReason": "STOP"}],
                "usageMetadata": {"promptTokenCount": ACTUAL_INPUT, "candidatesTokenCount": 1, "totalTokenCount": 1501},
                "modelVersion": self.model()
            }),
        }
    }
}

#[derive(Clone)]
struct FixtureState {
    provider: Provider,
    catalog: Value,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl FixtureState {
    fn request_count(&self) -> usize {
        self.requests
            .lock()
            .expect("image fixture request lock")
            .len()
    }

    fn assert_forwarded_image(&self, ordinal: usize, request: &Value) {
        let requests = self.requests.lock().expect("image fixture request lock");
        assert_eq!(requests.len(), ordinal + 1, "exactly one provider dispatch");
        assert_eq!(
            self.provider.image_payload(&requests[ordinal]),
            self.provider.image_payload(request),
            "forward the original image payload unchanged"
        );
    }
}

async fn fixture_handler(State(state): State<FixtureState>, request: Request) -> Response {
    if request.uri().path() == "/catalog.json" {
        return Json(state.catalog).into_response();
    }
    assert_eq!(request.method(), axum::http::Method::POST);
    assert_eq!(request.uri().path(), state.provider.provider_path());
    let body = to_bytes(request.into_body(), BODY_CAP as usize)
        .await
        .expect("read image provider body");
    let body = serde_json::from_slice(&body).expect("image provider JSON");
    state
        .requests
        .lock()
        .expect("image fixture request lock")
        .push(body);
    Json(state.provider.response()).into_response()
}

struct Harness {
    server: common::TestServer,
    provider: Provider,
    state: FixtureState,
    pool: SqlitePool,
    client: reqwest::Client,
    _catalog_dir: tempfile::TempDir,
    fixture_task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.fixture_task.abort();
    }
}

impl Harness {
    async fn start(provider: Provider) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let state = FixtureState {
            provider,
            catalog: json!({provider.model(): {
                "litellm_provider": provider.name(),
                "input_cost_per_token": 0.000002,
                "output_cost_per_token": 0.000010
            }}),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind image fixture");
        let base_url = format!(
            "http://{}/",
            listener.local_addr().expect("image fixture address")
        );
        let router = Router::new()
            .fallback(fixture_handler)
            .with_state(state.clone());
        let fixture_task = tokio::spawn(async move { axum::serve(listener, router).await });
        let catalog_dir = tempfile::tempdir().expect("image catalog directory");
        let catalog_path = catalog_dir.path().join("catalog.json");
        std::fs::write(&catalog_path, state.catalog.to_string()).expect("seed image catalog");
        let config = format!(
            "[price_catalog]\nurl = \"{base_url}catalog.json\"\ncache_path = \"{}\"\n",
            catalog_path.display()
        );
        let server = common::spawn_test_server_with_principal_limits_and_messages_cap(
            &config,
            Vec::new(),
            fake_anthropic::AppConfig::default(),
            BODY_CAP,
        )
        .await;
        let deadline = tokio::time::Instant::now() + DEADLINE;
        while !String::from_utf8_lossy(&server._process.stdout_snapshot())
            .contains("installed latest local LiteLLM price catalog")
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "image catalog installation timed out"
            );
            tokio::task::yield_now().await;
        }
        let client = reqwest::Client::builder()
            .timeout(DEADLINE)
            .build()
            .expect("image test HTTP client");
        let response = client.post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
            .bearer_auth("admin-token")
            .json(&json!({
                "name": "native-image-admission", "kind": format!("{}_api_key", provider.name()),
                "base_url": base_url, "api_key_value": "loopback-image-contract", "skip_preflight": true
            })).send().await.expect("create image upstream");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::CREATED,
            "{}",
            response.text().await.expect("upstream response")
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&format!("sqlite://{}", server.sqlite_path.display()))
            .await
            .expect("open image observation database");
        Self {
            server,
            provider,
            state,
            pool,
            client,
            _catalog_dir: catalog_dir,
            fixture_task,
        }
    }

    async fn key(&self, label: &str, cap: i64) -> common::ManagedTestKey {
        let limit = Limit {
            kind: LimitKind::InputTokens,
            window_secs: WINDOW_SECS,
            cap_micros: cap,
        };
        let response = self
            .client
            .post(format!(
                "http://{}/admin/v1/principals",
                self.server.admin_addr
            ))
            .bearer_auth("admin-token")
            .json(&json!({
                "name": label, "kind": "machine", "allowed_models": [],
                "default_limits": [limit]
            }))
            .send()
            .await
            .expect("create image principal");
        let status = response.status();
        let principal: Value = response.json().await.expect("image principal JSON");
        assert_eq!(status, reqwest::StatusCode::CREATED, "{principal}");
        let id = principal["id"].as_str().expect("image principal id");
        let response = self
            .client
            .post(format!(
                "http://{}/admin/v1/principals/{id}/keys",
                self.server.admin_addr
            ))
            .bearer_auth("admin-token")
            .json(&json!({"label": label}))
            .send()
            .await
            .expect("issue image managed key");
        let status = response.status();
        let key: Value = response.json().await.expect("image managed key JSON");
        assert_eq!(status, reqwest::StatusCode::CREATED, "{key}");
        common::ManagedTestKey {
            key_id: key["key_id"].as_str().expect("image key id").to_owned(),
            plaintext: key["plaintext_key"]
                .as_str()
                .expect("image key plaintext")
                .to_owned(),
        }
    }

    async fn send(&self, key: &common::ManagedTestKey, body: &Value, expected: u16) {
        let response = self
            .client
            .post(format!(
                "http://{}{}",
                self.server.proxy_addr,
                self.provider.path()
            ))
            .bearer_auth(&key.plaintext)
            .json(body)
            .send()
            .await
            .expect("native image request");
        let status = response.status().as_u16();
        let response = response
            .text()
            .await
            .expect("consume native image response");
        assert_eq!(status, expected, "{:?}: {response}", self.provider);
        if expected == 429 {
            let response: Value =
                serde_json::from_str(&response).expect("admission rejection JSON");
            assert_eq!(response["error"]["type"], "rate_limit_error", "{response}");
        }
    }

    async fn assert_settled_once(&self, key: &common::ManagedTestKey) {
        // Request events and usage buckets have separate async writers. Poll
        // their exact positive completion conditions, never a quiet-period timer.
        let deadline = tokio::time::Instant::now() + DEADLINE;
        loop {
            let events = sqlx::query("SELECT input_tokens, output_tokens, list_status, model FROM request_events_v1 WHERE key_id = ? AND upstream_name = 'native-image-admission' AND list_status = 200")
                .bind(&key.key_id).fetch_all(&self.pool).await.expect("read image request events");
            let bucket = sqlx::query("SELECT COALESCE(SUM(requests), 0) AS requests, COALESCE(SUM(input_tokens), 0) AS input_tokens, COALESCE(SUM(output_tokens), 0) AS output_tokens FROM api_key_usage_buckets_v1 WHERE key_id = ? AND bucket_width_secs = 60")
                .bind(&key.key_id).fetch_one(&self.pool).await.expect("read image usage buckets");
            let requests = bucket.get::<i64, _>("requests");
            assert!(events.len() <= 1, "image request event duplicated");
            assert!(requests <= 1, "image usage settled more than once");
            if events.len() == 1 && requests == 1 {
                let event = &events[0];
                assert_eq!(event.get::<i64, _>("input_tokens"), ACTUAL_INPUT);
                assert_eq!(event.get::<i64, _>("output_tokens"), 1);
                assert_eq!(event.get::<i64, _>("list_status"), 200);
                assert_eq!(event.get::<String, _>("model"), self.provider.model());
                assert_eq!(
                    bucket.get::<i64, _>("input_tokens"),
                    ACTUAL_INPUT,
                    "settle provider actual, not the admission estimate"
                );
                assert_eq!(bucket.get::<i64, _>("output_tokens"), 1);
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "image durable settlement timed out"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn hydrate_empty_key(&self, key: &common::ManagedTestKey) {
        let before = self.state.request_count();
        // Registers this key's durable window, but cannot fit even a full cap.
        // A rejection makes no reservation and dispatches no provider request.
        self.send(key, &self.provider.text_request(&"x".repeat(500_000)), 429)
            .await;
        assert_eq!(self.state.request_count(), before);
        // Migration 0073 also installs the all-zero compaction writer. It has
        // an infinite lease and never flushes, so explicitly exclude it.
        let writer_epoch: String = sqlx::query_scalar(
            "SELECT writer_epoch FROM api_key_usage_writers_v1 WHERE writer_epoch != '00000000-0000-0000-0000-000000000000'",
        )
        .fetch_one(&self.pool)
        .await
        .expect("read serving process usage writer");
        let writer_sql =
            "SELECT last_flush_id FROM api_key_usage_writers_v1 WHERE writer_epoch = ?";
        let mut previous: Option<String> = sqlx::query_scalar(writer_sql)
            .bind(&writer_epoch)
            .fetch_one(&self.pool)
            .await
            .expect("read image usage writer");
        let deadline = tokio::time::Instant::now() + DEADLINE;
        let mut advances = 0;
        while advances < 2 {
            let current: Option<String> = sqlx::query_scalar(writer_sql)
                .bind(&writer_epoch)
                .fetch_one(&self.pool)
                .await
                .expect("poll image usage writer");
            if current.is_some() && current != previous {
                advances += 1;
                previous = current;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "empty-key durable hydration timed out"
            );
            tokio::task::yield_now().await;
        }
        // The writer refreshes registered keys after each flush. Observing the
        // following flush proves a prior refresh completed, so the 10% cold
        // allowance cannot turn the exact 4,000 admission into a false 429.
    }
}

fn png_chunk(png: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    png.extend_from_slice(
        &u32::try_from(data.len())
            .expect("fixture chunk length")
            .to_be_bytes(),
    );
    png.extend_from_slice(kind);
    png.extend_from_slice(data);
    let mut crc = !0_u32;
    for byte in kind.iter().chain(data) {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320_u32 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    png.extend_from_slice(&(!crc).to_be_bytes());
}

fn png_pair() -> [String; 2] {
    // Valid 1024x1024 RGB, 8-bit, noninterlaced PNGs. Every scanline uses
    // filter zero and identical black pixels. Only zlib compression differs.
    let pixels = vec![0_u8; 1024 * (1 + 1024 * 3)];
    let pair = [Compression::best(), Compression::none()].map(|compression| {
        let mut encoder = ZlibEncoder::new(Vec::new(), compression);
        encoder.write_all(&pixels).expect("compress RGB scanlines");
        let compressed = encoder.finish().expect("finish RGB zlib stream");
        let mut decoded = Vec::new();
        ZlibDecoder::new(compressed.as_slice())
            .read_to_end(&mut decoded)
            .expect("decode RGB fixture");
        assert_eq!(
            decoded, pixels,
            "both PNG encodings contain identical RGB pixels"
        );
        let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
        let mut ihdr = Vec::new();
        ihdr.extend_from_slice(&1024_u32.to_be_bytes());
        ihdr.extend_from_slice(&1024_u32.to_be_bytes());
        ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);
        png_chunk(&mut png, b"IHDR", &ihdr);
        png_chunk(&mut png, b"IDAT", &compressed);
        png_chunk(&mut png, b"IEND", &[]);
        base64::engine::general_purpose::STANDARD.encode(png)
    });
    assert!(
        pair[0].len() * 100 < pair[1].len(),
        "materially different encoded sizes"
    );
    assert!(
        pair[1].len() > 4_000_000,
        "reproduce encoded-byte overestimation"
    );
    pair
}

async fn encoding_contract(provider: Provider) {
    let harness = Harness::start(provider).await;
    let control = harness.key("healthy-text", INPUT_CAP).await;
    harness
        .send(&control, &provider.text_request("hello"), 200)
        .await;
    harness.assert_settled_once(&control).await;
    assert_eq!(harness.state.request_count(), 1, "healthy text dispatch");
    for (encoding, encoded) in ["compact", "uncompressed"].into_iter().zip(png_pair()) {
        let key = harness.key(encoding, INPUT_CAP).await;
        let request = provider.image_request(&encoded);
        let before = harness.state.request_count();
        harness.send(&key, &request, 200).await;
        harness.state.assert_forwarded_image(before, &request);
        harness.assert_settled_once(&key).await;
    }
}

#[tokio::test]
async fn openai_responses_admits_identical_rgb_pixels_independent_of_png_encoding() {
    encoding_contract(Provider::OpenAi).await;
}

#[tokio::test]
async fn gemini_generate_content_admits_identical_rgb_pixels_independent_of_png_encoding() {
    encoding_contract(Provider::Gemini).await;
}

#[tokio::test]
async fn native_images_preserve_exact_four_thousand_input_floor() {
    let compact = &png_pair()[0];
    for provider in [Provider::OpenAi, Provider::Gemini] {
        let harness = Harness::start(provider).await;
        let rejected = harness.key("floor-3999", 3_999).await;
        harness
            .send(&rejected, &provider.image_request(compact), 429)
            .await;
        assert_eq!(
            harness.state.request_count(),
            0,
            "below-floor rejection is pre-dispatch"
        );
        let admitted = harness.key("floor-4000", 4_000).await;
        harness.hydrate_empty_key(&admitted).await;
        harness
            .send(&admitted, &provider.image_request(compact), 200)
            .await;
        assert_eq!(
            harness.state.request_count(),
            1,
            "exact floor dispatches once"
        );
        harness.assert_settled_once(&admitted).await;
    }
}

#[tokio::test]
async fn multiple_unavailable_pixel_references_exceed_cap_before_provider_dispatch() {
    for provider in [Provider::OpenAi, Provider::Gemini] {
        let harness = Harness::start(provider).await;
        let key = harness.key("reference-images", INPUT_CAP).await;
        harness.hydrate_empty_key(&key).await;
        harness.send(&key, &provider.reference_request(), 429).await;
        assert_eq!(
            harness.state.request_count(),
            0,
            "reference images must accumulate without provider calls"
        );
        // A rejected image request must not poison the key's healthy text path.
        harness
            .send(&key, &provider.text_request("hello"), 200)
            .await;
        assert_eq!(harness.state.request_count(), 1);
        harness.assert_settled_once(&key).await;
    }
}

#[tokio::test]
async fn native_image_routes_authenticate_before_reading_a_body() {
    for provider in [Provider::OpenAi, Provider::Gemini] {
        let harness = Harness::start(provider).await;
        let mut socket = TcpStream::connect(harness.server.proxy_addr)
            .await
            .expect("connect auth probe");
        // Deliberately send no body, despite a length exceeding the body cap.
        // Waiting for or validating the image body before auth would hang or 413.
        let headers = format!(
            "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            provider.path(),
            BODY_CAP + 1
        );
        socket
            .write_all(headers.as_bytes())
            .await
            .expect("send unauthenticated headers");
        let mut reader = BufReader::new(socket);
        let mut response = String::new();
        tokio::time::timeout(DEADLINE, reader.read_line(&mut response))
            .await
            .expect("authentication must not wait for a body")
            .expect("read auth response status line");
        assert!(
            response.starts_with("HTTP/1.1 401"),
            "{provider:?}: {response}"
        );
        assert_eq!(
            harness.state.request_count(),
            0,
            "unauthenticated requests never dispatch"
        );
    }
}
