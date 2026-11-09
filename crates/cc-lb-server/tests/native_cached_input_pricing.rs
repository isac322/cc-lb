//! Loopback provider contracts: real server, catalog loader, usage projection and durable limits.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cc_lb_storage_api::principal::{Limit, LimitKind};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as ClientMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::common;

const DEADLINE: Duration = Duration::from_secs(20);
const WINDOW_SECS: u64 = 3_600;
const BUCKET_WIDTH_SECS: i64 = 60;
// Native short requests reserve the existing 4,000-input floor and one output token:
// 4,000 * $2/M + 1 * $10/M = 8,010 micros. Unknown actual pricing must retain this.
const ADMITTED_ESTIMATE: i64 = 8_010;

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
            Self::OpenAi => "gpt-5.6",
            Self::Gemini => "gemini-2.5-flash",
        }
    }

    fn probe_model(self) -> &'static str {
        match self {
            Self::OpenAi => "gpt-5.6-probe",
            Self::Gemini => "gemini-2.5-flash-probe",
        }
    }

    fn path(self, transport: Transport, model: &str) -> String {
        match self {
            Self::OpenAi => "/openai/v1/responses".to_owned(),
            Self::Gemini => {
                let operation = match transport {
                    Transport::Json => "generateContent",
                    Transport::Sse => "streamGenerateContent?alt=sse",
                    Transport::WebSocket => unreachable!("Gemini is not a Responses WebSocket"),
                };
                format!("/gemini/v1beta/models/{model}:{operation}")
            }
        }
    }

    fn request(self, transport: Transport, model: &str, scenario: &str) -> Value {
        match self {
            Self::OpenAi => {
                let mut request = json!({
                    "model": model,
                    // A supported modern implicit boundary, without an opaque
                    // cache key/resource that changes admission's input bound.
                    "input": [{"role": "user", "content": scenario}],
                    "max_output_tokens": 1,
                    "store": false
                });
                match transport {
                    Transport::Json => {}
                    Transport::Sse => request["stream"] = json!(true),
                    Transport::WebSocket => request["type"] = json!("response.create"),
                }
                request
            }
            Self::Gemini => json!({
                "contents": [{"role": "user", "parts": [{"text": scenario}]}],
                "generationConfig": {"maxOutputTokens": 1}
            }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Transport {
    Json,
    Sse,
    WebSocket,
}

impl Transport {
    fn event_status(self) -> i64 {
        match self {
            Self::Json | Self::Sse => 200,
            // A successful WebSocket turn retains the upgraded transport status.
            Self::WebSocket => 101,
        }
    }
}

const CONTRACTS: [(Provider, Transport); 5] = [
    (Provider::OpenAi, Transport::Json),
    (Provider::OpenAi, Transport::Sse),
    (Provider::OpenAi, Transport::WebSocket),
    (Provider::Gemini, Transport::Json),
    (Provider::Gemini, Transport::Sse),
];

#[derive(Clone)]
struct FixtureState {
    provider: Provider,
    requests: Arc<Mutex<Vec<Value>>>,
    catalog: Value,
}

impl FixtureState {
    fn record_response(&self, request: Value) -> Value {
        let scenario = match self.provider {
            Provider::OpenAi => request["input"][0]["content"].as_str(),
            Provider::Gemini => request["contents"][0]["parts"][0]["text"].as_str(),
        }
        .expect("fixture requires the scenario in the native request shape");
        let (input, cached) = match scenario {
            "cached" => (21_000, 20_000),
            "uncached" => (21_000, 0),
            "probe" => (1, 0),
            unexpected => panic!("unexpected native pricing fixture scenario: {unexpected}"),
        };
        let mut requests = self.requests.lock().expect("fixture request lock");
        let id = format!("resp_native_cache_{}", requests.len());
        let response = match self.provider {
            Provider::OpenAi => json!({
                "id": id,
                "object": "response",
                "status": "completed",
                "store": false,
                "model": request["model"],
                "output": [{
                    "type": "message", "id": format!("msg_{id}"), "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "ok", "annotations": []}]
                }],
                "usage": {
                    "input_tokens": input,
                    "input_tokens_details": {
                        "cached_tokens": cached,
                        "cache_write_tokens": 0,
                        "cache_write_tokens_details": {
                            "ttl_30m_tokens": 0,
                            "ttl_12h_tokens": 0
                        }
                    },
                    "output_tokens": 1,
                    "total_tokens": input + 1
                }
            }),
            Provider::Gemini => json!({
                "candidates": [{
                    "content": {"parts": [{"text": "ok"}], "role": "model"},
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": input,
                    "cachedContentTokenCount": cached,
                    "candidatesTokenCount": 1,
                    "totalTokenCount": input + 1
                },
                "modelVersion": self.provider.model()
            }),
        };
        requests.push(request);
        response
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("fixture request lock").len()
    }
}

struct Fixture {
    base_url: String,
    state: FixtureState,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture_http(State(state): State<FixtureState>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    if path == "/catalog.json" {
        return Json(state.catalog).into_response();
    }
    let body = to_bytes(request.into_body(), 1_048_576)
        .await
        .expect("native fixture request body");
    let request: Value = serde_json::from_slice(&body).expect("native fixture JSON");
    let stream =
        request["stream"].as_bool() == Some(true) || path.ends_with(":streamGenerateContent");
    let response = state.record_response(request);
    if !stream {
        return Json(response).into_response();
    }
    let body = match state.provider {
        Provider::OpenAi => format!(
            "event: response.created\ndata: {}\n\nevent: response.completed\ndata: {}\n\n",
            json!({"type": "response.created", "response": {
                "id": response["id"], "status": "in_progress", "store": false, "usage": null
            }}),
            json!({"type": "response.completed", "response": response}),
        ),
        Provider::Gemini => format!("data: {response}\n\n"),
    };
    (
        [(header::CONTENT_TYPE, "text/event-stream")],
        Body::from(body),
    )
        .into_response()
}

async fn fixture_websocket(
    State(state): State<FixtureState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .on_upgrade(move |socket| fixture_websocket_session(socket, state))
        .into_response()
}

async fn fixture_websocket_session(mut socket: WebSocket, state: FixtureState) {
    let mut completed = false;
    while let Some(message) = socket.next().await {
        let message = match message {
            Ok(message) => message,
            // The proxy drops its provider connection after downstream teardown.
            Err(_) if completed => return,
            Err(error) => panic!("fixture WebSocket frame before completion: {error}"),
        };
        match message {
            Message::Text(text) => {
                let request: Value = serde_json::from_str(&text).expect("response.create JSON");
                assert_eq!(request["type"], "response.create");
                let response = state.record_response(request);
                for event in [
                    json!({"type": "response.created", "response": {
                        "id": response["id"], "status": "in_progress", "store": false, "usage": null
                    }}),
                    json!({"type": "response.completed", "response": response}),
                ] {
                    if socket
                        .send(Message::Text(event.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                completed = true;
            }
            Message::Close(_) => return,
            Message::Ping(bytes) => {
                if socket.send(Message::Pong(bytes)).await.is_err() {
                    return;
                }
            }
            _ => {}
        }
    }
}

async fn fixture(provider: Provider, read_rate: Option<f64>) -> Fixture {
    let mut price = json!({
        "litellm_provider": provider.name(),
        "input_cost_per_token": 0.000002,
        "output_cost_per_token": 0.000010
    });
    if let Some(rate) = read_rate {
        price["cache_read_input_token_cost"] = json!(rate);
    }
    let catalog = json!({
        provider.model(): price,
        // A one-output-token probe reserves exactly 100,000 micros, independently
        // of the 4,000-input floor. Its body and requested generation are tiny.
        provider.probe_model(): {
            "litellm_provider": provider.name(),
            "input_cost_per_token": 0.0,
            "output_cost_per_token": 0.1,
            "cache_read_input_token_cost": 0.0
        }
    });
    let state = FixtureState {
        provider,
        requests: Arc::new(Mutex::new(Vec::new())),
        catalog,
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind native pricing loopback fixture");
    let address = listener.local_addr().expect("fixture address");
    let router = Router::new()
        .route("/v1/responses", get(fixture_websocket).post(fixture_http))
        .fallback(fixture_http)
        .with_state(state.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    Fixture {
        base_url: format!("http://{address}/"),
        state,
        task,
    }
}

struct Harness {
    server: common::TestServer,
    fixture: Fixture,
    pool: SqlitePool,
    _catalog_dir: tempfile::TempDir,
}

impl Harness {
    async fn start(provider: Provider, read_rate: Option<f64>, cap: i64) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let fixture = fixture(provider, read_rate).await;
        let catalog_dir = tempfile::tempdir().expect("native catalog directory");
        let catalog_path = catalog_dir.path().join("catalog.json");
        std::fs::write(&catalog_path, fixture.state.catalog.to_string())
            .expect("install synthetic catalog before server startup");
        let config = format!(
            "[price_catalog]\nurl = \"{}catalog.json\"\ncache_path = \"{}\"\n",
            fixture.base_url,
            catalog_path.display()
        );
        // WebSocket admission includes parsed JSON and per-turn bookkeeping,
        // not just frame bytes; the shared 256-byte oversize-test default is too small.
        let server = common::spawn_test_server_with_principal_limits_and_messages_cap(
            &config,
            vec![
                Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: WINDOW_SECS,
                    cap_micros: cap,
                },
                Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 0,
                    cap_micros: 1,
                },
            ],
            fake_anthropic::AppConfig::default(),
            1_048_576,
        )
        .await;
        // Startup exposes listeners while the catalog installer task is finishing.
        // The observed successful install is a barrier, not an arbitrary startup sleep.
        let deadline = tokio::time::Instant::now() + DEADLINE;
        loop {
            if String::from_utf8_lossy(&server._process.stdout_snapshot())
                .contains("installed latest local LiteLLM price catalog")
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "catalog installation barrier timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let client = reqwest::Client::new();
        let response = client
            .post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
            .bearer_auth("admin-token")
            .json(&json!({
                "name": "native-cached-input-contract",
                "kind": format!("{}_api_key", provider.name()),
                "base_url": fixture.base_url,
                "api_key_value": "loopback-native-contract-key",
                "skip_preflight": true
            }))
            .send()
            .await
            .expect("create native upstream");
        let status = response.status();
        let body = response
            .text()
            .await
            .expect("create upstream response body");
        assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
        // The admin mutation awaits the local dynamic-view publication.
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&format!("sqlite://{}", server.sqlite_path.display()))
            .await
            .expect("open native pricing observation database");
        Self {
            server,
            fixture,
            pool,
            _catalog_dir: catalog_dir,
        }
    }

    async fn send(
        &self,
        provider: Provider,
        transport: Transport,
        model: &str,
        scenario: &str,
    ) -> u16 {
        let body = provider.request(transport, model, scenario);
        let path = provider.path(transport, model);
        if matches!(transport, Transport::WebSocket) {
            let mut request = format!("ws://{}{path}", self.server.proxy_addr)
                .into_client_request()
                .expect("Responses WebSocket request");
            request.headers_mut().insert(
                "x-api-key",
                self.server
                    .managed_key
                    .plaintext
                    .parse()
                    .expect("managed key header"),
            );
            let (mut socket, handshake) =
                tokio::time::timeout(DEADLINE, tokio_tungstenite::connect_async(request))
                    .await
                    .expect("Responses WebSocket handshake deadline")
                    .expect("Responses WebSocket handshake");
            assert_eq!(handshake.status().as_u16(), 101);
            socket
                .send(ClientMessage::Text(body.to_string().into()))
                .await
                .expect("response.create");
            let status = tokio::time::timeout(DEADLINE, async {
                while let Some(frame) = socket.next().await {
                    let frame = frame.expect("Responses WebSocket frame");
                    if let ClientMessage::Text(text) = frame {
                        let event: Value = serde_json::from_str(&text).expect("Responses event");
                        match event["type"].as_str() {
                            Some("response.completed") => {
                                assert!(event["response"]["usage"].is_object(), "{event}");
                                return 200;
                            }
                            Some("error") => {
                                let status =
                                    event["status"].as_u64().expect("WebSocket error status")
                                        as u16;
                                assert_eq!(
                                    status, 429,
                                    "{provider:?}/{transport:?} {model}/{scenario}: {event}"
                                );
                                return status;
                            }
                            _ => {}
                        }
                    }
                }
                panic!("Responses WebSocket closed before terminal event");
            })
            .await
            .expect("Responses terminal event deadline");
            let _ = socket.close(None).await;
            return status;
        }
        let response = reqwest::Client::new()
            .post(format!("http://{}{path}", self.server.proxy_addr))
            .bearer_auth(&self.server.managed_key.plaintext)
            .json(&body)
            .timeout(DEADLINE)
            .send()
            .await
            .expect("native provider request");
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .expect("consume complete native response");
        assert!(
            matches!(status, 200 | 429),
            "{provider:?}/{transport:?} {model}/{scenario}: HTTP {status}: {body}"
        );
        if status == 200 {
            match transport {
                Transport::Json => {
                    let response: Value =
                        serde_json::from_str(&body).expect("native response JSON");
                    assert!(
                        response
                            .get("usage")
                            .or_else(|| response.get("usageMetadata"))
                            .is_some(),
                        "{response}"
                    );
                }
                Transport::Sse => assert!(body.contains("data:"), "native SSE response: {body}"),
                Transport::WebSocket => unreachable!(),
            }
        }
        status
    }

    async fn event(&self, transport: Transport, ordinal: i64) -> sqlx::sqlite::SqliteRow {
        let deadline = tokio::time::Instant::now() + DEADLINE;
        loop {
            let events = sqlx::query(
                "SELECT * FROM request_events_v1 WHERE key_id = ? AND upstream_name = 'native-cached-input-contract' AND list_status = ? AND input_tokens IS NOT NULL ORDER BY id",
            )
            .bind(&self.server.managed_key.key_id)
            .bind(transport.event_status())
            .fetch_all(&self.pool)
            .await
            .expect("query persisted native usage events");
            if events.len() >= ordinal as usize {
                return events
                    .into_iter()
                    .nth(ordinal as usize - 1)
                    .expect("event ordinal");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "persisted normalized usage event {ordinal} timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn bucket(&self, requests: i64) -> sqlx::sqlite::SqliteRow {
        let deadline = tokio::time::Instant::now() + DEADLINE;
        loop {
            let row = sqlx::query(
                "SELECT COALESCE(SUM(requests), 0) AS requests, COALESCE(SUM(input_tokens), 0) AS input_tokens, COALESCE(SUM(output_tokens), 0) AS output_tokens, COALESCE(SUM(cost_usd_micros), 0) AS cost_usd_micros FROM api_key_usage_buckets_v1 WHERE key_id = ? AND bucket_width_secs = ?",
            )
            .bind(&self.server.managed_key.key_id)
            .bind(BUCKET_WIDTH_SECS)
            .fetch_one(&self.pool)
            .await
            .expect("query durable native usage buckets");
            if row.get::<i64, _>("requests") >= requests {
                assert_eq!(
                    row.get::<i64, _>("requests"),
                    requests,
                    "native usage must settle exactly once"
                );
                return row;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "durable settled request-count {requests} timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn next_writer_flush(&self) {
        let writer_sql = "SELECT w.last_flush_id FROM api_key_usage_writers_v1 w JOIN api_key_usage_buckets_v1 b ON b.writer_epoch = w.writer_epoch WHERE b.key_id = ? LIMIT 1";
        let previous: String = sqlx::query_scalar(writer_sql)
            .bind(&self.server.managed_key.key_id)
            .fetch_one(&self.pool)
            .await
            .expect("settlement writer flush id");
        let deadline = tokio::time::Instant::now() + DEADLINE;
        loop {
            let current: String = sqlx::query_scalar(writer_sql)
                .bind(&self.server.managed_key.key_id)
                .fetch_one(&self.pool)
                .await
                .expect("query next writer flush id");
            if current != previous {
                // Refresh follows each flush; seeing the next flush proves the prior
                // refresh completed, so the cold-start 10% allowance cannot mask admission.
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "next durable writer flush barrier timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn assert_usage(event: &sqlx::sqlite::SqliteRow, transport: Transport, input: i64, cache: i64) {
    assert_eq!(
        event.get::<i64, _>("input_tokens"),
        input,
        "persist ordinary input separately from provider-inclusive cached input"
    );
    assert_eq!(event.get::<i64, _>("cache_read_input_tokens"), cache);
    assert_eq!(event.get::<i64, _>("output_tokens"), 1);
    assert_eq!(event.get::<i64, _>("list_status"), transport.event_status());
}

fn assert_cost(event: &sqlx::sqlite::SqliteRow, input: i64, cache: i64, total: i64) {
    for (column, expected) in [
        ("list_cost_input_micros", input),
        ("list_cost_output_micros", 10),
        ("list_cost_cache_read_micros", cache),
        ("list_cost_usd_micros", total),
    ] {
        assert_eq!(
            event.get::<Option<i64>, _>(column),
            Some(expected),
            "persisted {column}"
        );
    }
}

async fn known_rate_contract(provider: Provider, transport: Transport) {
    let harness = Harness::start(provider, Some(0.0000001), 1_000_000_000).await;
    assert_eq!(
        harness
            .send(provider, transport, provider.model(), "cached")
            .await,
        200,
        "{provider:?}/{transport:?}"
    );
    let cached = harness.event(transport, 1).await;
    assert_usage(&cached, transport, 1_000, 20_000);
    // Baseline fails here with 2,010 / zero cache, not a fixture/setup failure.
    assert_cost(&cached, 2_000, 2_000, 4_010);
    assert_eq!(
        harness.bucket(1).await.get::<i64, _>("cost_usd_micros"),
        4_010
    );

    assert_eq!(
        harness
            .send(provider, transport, provider.model(), "uncached")
            .await,
        200
    );
    let uncached = harness.event(transport, 2).await;
    assert_usage(&uncached, transport, 21_000, 0);
    assert_cost(&uncached, 42_000, 0, 42_010);
    let bucket = harness.bucket(2).await;
    assert_eq!(bucket.get::<i64, _>("cost_usd_micros"), 46_020);
    assert_eq!(bucket.get::<i64, _>("input_tokens"), 22_000);
    assert_eq!(bucket.get::<i64, _>("output_tokens"), 2);
    assert_eq!(harness.fixture.state.request_count(), 2);
}

#[tokio::test]
async fn openai_responses_json_cached_input_cost_contract() {
    known_rate_contract(Provider::OpenAi, Transport::Json).await;
}

#[tokio::test]
async fn openai_responses_sse_cached_input_cost_contract() {
    known_rate_contract(Provider::OpenAi, Transport::Sse).await;
}

#[tokio::test]
async fn openai_responses_websocket_cached_input_cost_contract() {
    known_rate_contract(Provider::OpenAi, Transport::WebSocket).await;
}

#[tokio::test]
async fn gemini_generate_content_json_cached_input_cost_contract() {
    known_rate_contract(Provider::Gemini, Transport::Json).await;
}

#[tokio::test]
async fn gemini_generate_content_sse_cached_input_cost_contract() {
    known_rate_contract(Provider::Gemini, Transport::Sse).await;
}

#[tokio::test]
async fn missing_native_cache_rate_keeps_projection_unknown_and_settles_admitted_liability() {
    for (provider, transport) in CONTRACTS {
        let harness = Harness::start(provider, None, 1_000_000_000).await;
        assert_eq!(
            harness
                .send(provider, transport, provider.model(), "cached")
                .await,
            200
        );
        let bucket = harness.bucket(1).await;
        assert_eq!(
            bucket.get::<i64, _>("cost_usd_micros"),
            ADMITTED_ESTIMATE,
            "unknown actual must finalize the admitted estimate, not zero or ordinary-only cost: {provider:?}/{transport:?}"
        );
        assert_eq!(bucket.get::<i64, _>("input_tokens"), 1_000);
        assert_eq!(bucket.get::<i64, _>("output_tokens"), 1);
        let event = harness.event(transport, 1).await;
        assert_usage(&event, transport, 1_000, 20_000);
        for column in [
            "list_cost_usd_micros",
            "list_cost_input_micros",
            "list_cost_output_micros",
            "list_cost_cache_read_micros",
            "list_cost_cache_write_micros",
            "list_cost_cache_write_30m_micros",
            "list_cost_cache_write_12h_micros",
        ] {
            assert_eq!(
                event.get::<Option<i64>, _>(column),
                None,
                "unknown projection must not invent {column}"
            );
        }
        // No cached usage needs no cache rate. Concurrency=1 also proves the
        // unknown-priced request finalized instead of holding its reservation open.
        assert_eq!(
            harness
                .send(provider, transport, provider.model(), "uncached")
                .await,
            200
        );
        let uncached = harness.event(transport, 2).await;
        assert_usage(&uncached, transport, 21_000, 0);
        assert_cost(&uncached, 42_000, 0, 42_010);
        let bucket = harness.bucket(2).await;
        assert_eq!(
            bucket.get::<i64, _>("cost_usd_micros"),
            ADMITTED_ESTIMATE + 42_010
        );
        assert_eq!(bucket.get::<i64, _>("input_tokens"), 22_000);
        assert_eq!(bucket.get::<i64, _>("output_tokens"), 2);
    }
}

#[tokio::test]
async fn explicit_zero_native_cache_rate_is_known_not_missing() {
    for (provider, transport) in CONTRACTS {
        let harness = Harness::start(provider, Some(0.0), 1_000_000_000).await;
        assert_eq!(
            harness
                .send(provider, transport, provider.model(), "cached")
                .await,
            200
        );
        let cached = harness.event(transport, 1).await;
        assert_usage(&cached, transport, 1_000, 20_000);
        assert_cost(&cached, 2_000, 0, 2_010);
        assert_eq!(
            harness.bucket(1).await.get::<i64, _>("cost_usd_micros"),
            2_010
        );
        assert_eq!(
            harness
                .send(provider, transport, provider.model(), "uncached")
                .await,
            200
        );
        assert_cost(&harness.event(transport, 2).await, 42_000, 0, 42_010);
        assert_eq!(
            harness.bucket(2).await.get::<i64, _>("cost_usd_micros"),
            44_020
        );
    }
}

#[tokio::test]
async fn corrected_native_cache_settlement_prevents_near_cap_followup_admission() {
    for (provider, transport) in CONTRACTS {
        // Initial 8,010 fits even the 10,300 cold-start allowance. After hydration,
        // a 100,000-micro one-token probe fits with the buggy 2,010 settlement
        // (102,010 <= 103,000), but not the corrected 4,010 (104,010 > 103,000).
        let harness = Harness::start(provider, Some(0.0000001), 103_000).await;
        assert_eq!(
            harness
                .send(provider, transport, provider.model(), "cached")
                .await,
            200
        );
        let bucket = harness.bucket(1).await;
        assert_eq!(bucket.get::<i64, _>("cost_usd_micros"), 4_010);
        harness.next_writer_flush().await;
        assert_eq!(
            harness
                .send(provider, transport, provider.probe_model(), "probe")
                .await,
            429,
            "corrected settled cost must govern the next admission: {provider:?}/{transport:?}"
        );
        assert_eq!(
            harness.fixture.state.request_count(),
            1,
            "rejected budget probe must never reach the provider"
        );
        // The rejected probe must not strand a live reservation or block a
        // normal valid cache hit whose admission estimate is only 8,010.
        assert_eq!(
            harness
                .send(provider, transport, provider.model(), "cached")
                .await,
            200
        );
        let allowed = harness.event(transport, 2).await;
        assert_usage(&allowed, transport, 1_000, 20_000);
        assert_cost(&allowed, 2_000, 2_000, 4_010);
        assert_eq!(
            harness.bucket(2).await.get::<i64, _>("cost_usd_micros"),
            8_020
        );
        assert_eq!(harness.fixture.state.request_count(), 2);
    }
}
