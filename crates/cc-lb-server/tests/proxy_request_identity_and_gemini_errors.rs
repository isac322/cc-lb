//! Public-surface regressions: this suite also runs unchanged against pre-fix master.

use std::collections::HashSet;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use cc_lb_aead::AeadService;
use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_server::App;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::{
    MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, RequestEvent, RequestEventKind,
    RequestEventStore, RequestEventStreamFilters, StorageResult, UpstreamCreate, UpstreamKind,
    UpstreamStore,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message, client::IntoClientRequest};
use tower::ServiceExt;
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const GEMINI_HTTP: &str = "/gemini/v1beta/models/gemini-2.5-flash:generateContent";
const GEMINI_LIVE: &str =
    "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";
const QUERY_SECRET: &str = "AIzaQuerySecret";
const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
enum ProviderMode {
    Fast,
    Held,
    RejectHandshake,
}

struct CountedListener {
    listener: TcpListener,
    accepts: Arc<AtomicUsize>,
}

impl axum::serve::Listener for CountedListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let connection = self.listener.accept().await.expect("mock upstream accepts");
        self.accepts.fetch_add(1, Ordering::SeqCst);
        connection
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

#[derive(Clone)]
struct ProviderState {
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

async fn http_provider(State(state): State<ProviderState>, request: Request) -> Response {
    let gemini = request.uri().path().starts_with("/v1beta/models/");
    let _body = request
        .into_body()
        .collect()
        .await
        .expect("mock receives body");
    let release = state.release.lock().await.take();
    if let Some(release) = release {
        let _ = release.await;
    }
    if gemini {
        Json(json!({
            "candidates": [{"content": {"parts": [{"text": "ok"}], "role": "model"},
                "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1,
                "totalTokenCount": 2},
            "modelVersion": "gemini-2.5-flash"
        }))
        .into_response()
    } else {
        Json(json!({
            "id": "resp_request_identity", "object": "response", "status": "completed",
            "model": "gpt-5", "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        }))
        .into_response()
    }
}

struct Harness {
    app: App,
    address: SocketAddr,
    key: String,
    storage: Arc<SqliteStorage>,
    upstream_accepts: Arc<AtomicUsize>,
    release: Option<oneshot::Sender<()>>,
    tasks: Vec<JoinHandle<()>>,
    _directory: tempfile::TempDir,
}

impl Drop for Harness {
    fn drop(&mut self) {
        // The slow provider waits for this signal, never for an arbitrary delay.
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Harness {
    async fn new(mode: ProviderMode) -> TestResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("storage.sqlite");
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage =
            Arc::new(open_sqlite(&format!("sqlite://{}", path.display()), clock.clone()).await?);
        storage.initialize().await?;
        let aead = Arc::new(AeadService::from_master_key([0; 32]));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let upstream_address = listener.local_addr()?;
        let upstream_accepts = Arc::new(AtomicUsize::new(0));
        let (release, receiver) = if matches!(mode, ProviderMode::Held) {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let accepts = upstream_accepts.clone();
        let provider_task = match mode {
            ProviderMode::Fast | ProviderMode::Held => {
                let router = Router::new()
                    .fallback(http_provider)
                    .with_state(ProviderState {
                        release: Arc::new(Mutex::new(receiver)),
                    });
                tokio::spawn(async move {
                    axum::serve(CountedListener { listener, accepts }, router)
                        .await
                        .expect("mock HTTP provider serves");
                })
            }
            ProviderMode::RejectHandshake => tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("provider accepts handshake");
                accepts.fetch_add(1, Ordering::SeqCst);
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(stream.read_u8().await.expect("provider receives handshake"));
                    assert!(headers.len() < 64 * 1024, "bounded handshake headers");
                }
                stream
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .expect("provider rejects handshake");
                stream.shutdown().await.expect("provider closes handshake");
            }),
        };
        for (name, kind) in [
            ("openai", UpstreamKind::OpenaiApiKey),
            ("gemini", UpstreamKind::GeminiApiKey),
        ] {
            let upstream = UpstreamStore::create(
                storage.as_ref(),
                UpstreamCreate {
                    id: Uuid::new_v4(),
                    name: name.to_owned(),
                    kind,
                    base_url: Some(format!("http://{upstream_address}/").parse()?),
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                },
            )
            .await?;
            let ciphertext = aead.encrypt(b"provider-fixture-secret", upstream.id.as_bytes())?;
            UpstreamStore::update_api_key_secret(storage.as_ref(), upstream.id, Some(ciphertext))
                .await?;
        }
        PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: "identity-principal".to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            cc_lb_clock::unix_secs(SystemTime::now()),
        )
        .await?;
        let (_, secret) = KeyStore::new(storage.clone())
            .create(
                "identity-principal",
                CreateParams {
                    label: "identity-fixture".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await?;
        let key = secret.expose().to_owned();
        let mut config = Config {
            storage: StorageConfig::Sqlite { path },
            ..Config::default()
        };
        config.runtime.data_dir = Some(directory.path().to_path_buf());
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "fixture".to_owned(),
            token_env: "CARGO_PKG_NAME".to_owned(),
        }];
        config.timeouts.upstream_total_secs = 1;
        let app =
            build_app_with_storage(config, None, storage.clone(), storage.clone(), aead, clock)
                .await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = app.router.clone();
        let proxy_task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("proxy serves");
        });
        Ok(Self {
            app,
            address,
            key,
            storage,
            upstream_accepts,
            release,
            tasks: vec![provider_task, proxy_task],
            _directory: directory,
        })
    }

    fn post(&self, path: &str, authenticate: bool) -> TestResult<Request> {
        let body = if path.contains("/v1beta/") {
            json!({"contents": [{"parts": [{"text": "hello"}]}]})
        } else {
            json!({"model": "gpt-5", "input": "hello", "stream": false})
        };
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body)?))?;
        if authenticate {
            self.authenticate(request.headers_mut())?;
        }
        Ok(request)
    }

    fn authenticate(&self, headers: &mut HeaderMap) -> TestResult<()> {
        headers.insert("authorization", format!("Bearer {}", self.key).parse()?);
        headers.insert("x-api-key", self.key.parse()?);
        headers.insert("x-goog-api-key", self.key.parse()?);
        Ok(())
    }

    async fn send(&self, request: Request) -> TestResult<CapturedResponse> {
        let response = timeout(WAIT, self.app.router.clone().oneshot(request)).await??;
        CapturedResponse::from_http(response).await
    }

    async fn admin_request_id(&self, blank: bool) -> TestResult<String> {
        let mut request = Request::builder()
            .uri("/admin/health")
            .body(Body::empty())?;
        if blank {
            request
                .headers_mut()
                .insert("request-id", HeaderValue::from_static(""));
        }
        let response = self.app.admin_router.clone().oneshot(request).await?;
        Ok(response.headers()["request-id"].to_str()?.to_owned())
    }

    fn websocket_request(&self, path: &str) -> TestResult<http::Request<()>> {
        let mut request = format!("ws://{}{path}", self.address).into_client_request()?;
        self.authenticate(request.headers_mut())?;
        Ok(request)
    }

    async fn reject_websocket(
        &self,
        path: &str,
        authenticate: bool,
    ) -> TestResult<CapturedResponse> {
        let mut request = format!("ws://{}{path}", self.address).into_client_request()?;
        if authenticate {
            self.authenticate(request.headers_mut())?;
        }
        let error = timeout(WAIT, connect_async(request))
            .await?
            .expect_err("local rejection must happen before upgrading");
        let WebSocketError::Http(response) = error else {
            panic!("expected an HTTP rejection, got {error}");
        };
        let (parts, body) = response.into_parts();
        Ok(CapturedResponse {
            status: parts.status,
            headers: parts.headers,
            body: Bytes::from(body.expect("rejection includes a body")),
        })
    }

    async fn disable_gemini(&self) -> TestResult<()> {
        let upstream = UpstreamStore::list(self.storage.as_ref(), None, 10)
            .await?
            .into_iter()
            .find(|record| record.kind == UpstreamKind::GeminiApiKey)
            .expect("fixture Gemini upstream");
        let path = format!("/admin/v1/upstreams/{}", upstream.id);
        let response = self
            .app
            .admin_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header(
                        "authorization",
                        format!("Bearer {}", env!("CARGO_PKG_NAME")),
                    )
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response.headers()["etag"].clone();
        let response = self
            .app
            .admin_router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{path}/disable"))
                    .header(
                        "authorization",
                        format!("Bearer {}", env!("CARGO_PKG_NAME")),
                    )
                    .header("if-match", etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        Ok(())
    }

    fn assert_no_upstream_accepts(&self) {
        assert_eq!(self.upstream_accepts.load(Ordering::SeqCst), 0);
    }
}

struct CapturedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl CapturedResponse {
    async fn from_http(response: Response) -> TestResult<Self> {
        let (parts, body) = response.into_parts();
        Ok(Self {
            status: parts.status,
            headers: parts.headers,
            body: timeout(WAIT, body.collect()).await??.to_bytes(),
        })
    }

    fn request_id(&self) -> &str {
        self.headers["request-id"]
            .to_str()
            .expect("ASCII request ID")
    }

    fn json(&self) -> Value {
        sonic_rs::from_slice(&self.body).expect("provider-shaped JSON")
    }
}

async fn stored_request_events(storage: &SqliteStorage) -> StorageResult<Vec<RequestEvent>> {
    let cursor = storage.current_request_event_cursor().await?;
    Ok(storage
        .query_request_events_between_cursors(0, cursor, 500, &RequestEventStreamFilters::default())
        .await?
        .into_iter()
        .map(|(_, event)| event)
        .collect())
}

async fn wait_for_row_count(harness: &Harness, expected: usize) -> TestResult<Vec<RequestEvent>> {
    let deadline = Instant::now() + WAIT;
    let mut poll = interval(Duration::from_millis(20));
    loop {
        let rows = stored_request_events(harness.storage.as_ref()).await?;
        if rows.len() >= expected {
            assert_eq!(rows.len(), expected, "unexpected additional request events");
            return Ok(rows);
        }
        assert!(
            Instant::now() < deadline,
            "request events did not commit: expected {expected} rows, got {rows:?}"
        );
        poll.tick().await;
    }
}

async fn wait_for_rows(
    harness: &Harness,
    expected: &[(&str, u16)],
) -> TestResult<Vec<RequestEvent>> {
    let rows = wait_for_row_count(harness, expected.len()).await?;
    for (request_id, status) in expected {
        assert!(
            rows.iter()
                .any(|row| row.request_id == *request_id && row.status == *status),
            "missing persisted request ID/status pair: {request_id}/{status}; rows: {rows:?}"
        );
    }
    Ok(rows)
}

fn assert_admin_id(id: &str) {
    let suffix = id.strip_prefix("req_admin_").expect("admin counter prefix");
    assert!(!suffix.is_empty());
    assert!(suffix.bytes().all(|byte| byte.is_ascii_digit()));
}

fn assert_no_secrets(harness: &Harness, response: &CapturedResponse, rows: &[RequestEvent]) {
    let serialized = serde_json::to_string(rows).expect("request rows serialize");
    let body = String::from_utf8_lossy(&response.body);
    for secret in [
        harness.key.as_str(),
        QUERY_SECRET,
        "provider-fixture-secret",
    ] {
        assert!(
            !body.contains(secret),
            "response must not expose credentials"
        );
        assert!(
            !serialized.contains(secret),
            "persisted row must not expose credentials"
        );
    }
}

#[tokio::test]
async fn generated_request_ids_are_distinct_across_mounts_and_restarts() -> TestResult<()> {
    let mut ids = HashSet::new();
    for _ in 0..2 {
        let harness = Harness::new(ProviderMode::Fast).await?;
        for path in [
            "/v1/messages",
            "/anthropic/v1/messages",
            "/openai/v1/responses",
            GEMINI_HTTP,
        ] {
            let response = harness.send(harness.post(path, false)?).await?;
            assert_eq!(response.status, StatusCode::UNAUTHORIZED);
            let id = response.request_id();
            assert!(id.starts_with("req_server_"));
            assert!(id.is_ascii());
            assert!(
                ids.insert(id.to_owned()),
                "request IDs collide across mounts or app restarts"
            );
        }
    }
    assert_eq!(ids.len(), 8);
    Ok(())
}

#[tokio::test]
async fn scoped_and_unscoped_openai_responses_persist_distinct_request_ids() -> TestResult<()> {
    let harness = Harness::new(ProviderMode::Fast).await?;
    let first = harness.send(harness.post("/v1/responses", true)?).await?;
    let second = harness
        .send(harness.post("/openai/v1/responses", true)?)
        .await?;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(second.status, StatusCode::OK);
    assert_ne!(first.request_id(), second.request_id());
    let rows = wait_for_rows(
        &harness,
        &[(first.request_id(), 200), (second.request_id(), 200)],
    )
    .await?;
    assert_ne!(rows[0].request_id, rows[1].request_id);
    Ok(())
}

#[tokio::test]
async fn blank_or_non_ascii_request_ids_fall_through_or_regenerate() -> TestResult<()> {
    let harness = Harness::new(ProviderMode::Fast).await?;
    let mut fallback = harness.post("/v1/responses", false)?;
    fallback
        .headers_mut()
        .insert("request-id", HeaderValue::from_static(""));
    fallback.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_static("valid-fallback-id"),
    );
    let response = harness.send(fallback).await?;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(response.request_id(), "valid-fallback-id");
    let mut ids = vec![response.request_id().to_owned()];
    for (header, value) in [
        ("request-id", HeaderValue::from_static("   ")),
        ("x-request-id", HeaderValue::from_static("\t")),
        ("request-id", HeaderValue::from_bytes(b"\xff")?),
    ] {
        let mut request = harness.post("/v1/responses", false)?;
        request.headers_mut().insert(header, value);
        let response = harness.send(request).await?;
        assert_eq!(response.status, StatusCode::UNAUTHORIZED);
        let id = response.request_id();
        assert!(id.starts_with("req_server_"));
        assert_ne!(id, "req_server_unknown");
        assert!(id.is_ascii());
        assert!(!ids.iter().any(|previous| previous == id));
        ids.push(id.to_owned());
    }
    let expected: Vec<_> = ids.iter().map(|id| (id.as_str(), 401)).collect();
    let rows = wait_for_rows(&harness, &expected).await?;
    assert!(
        rows.iter()
            .all(|row| row.error_code.as_deref() == Some("authentication_failed"))
    );
    assert_admin_id(&harness.admin_request_id(true).await?);
    Ok(())
}

#[tokio::test]
async fn gemini_http_timeouts_emit_deadline_exceeded_json_while_controls_hold() -> TestResult<()> {
    for (path, id) in [
        (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            "gemini-unscoped-timeout",
        ),
        (GEMINI_HTTP, "gemini-scoped-timeout"),
    ] {
        let harness = Harness::new(ProviderMode::Held).await?;
        let mut request = harness.post(&format!("{path}?key={QUERY_SECRET}"), true)?;
        request.headers_mut().insert("request-id", id.parse()?);
        let response = harness.send(request).await?;
        assert_eq!(response.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(response.request_id(), id);
        assert_eq!(
            response.headers["content-type"],
            "application/json; charset=utf-8"
        );
        let body = response.json();
        assert_eq!(body["error"]["code"], 504);
        assert_eq!(body["error"]["status"], "DEADLINE_EXCEEDED");
        assert_eq!(body["error"]["type"], "timeout_error");
        let rows = wait_for_rows(&harness, &[(id, 504)]).await?;
        assert_eq!(rows[0].error_code.as_deref(), Some("tower_timeout"));
        assert_eq!(harness.upstream_accepts.load(Ordering::SeqCst), 1);
        assert_no_secrets(&harness, &response, &rows);
    }
    Ok(())
}

#[tokio::test]
async fn gemini_live_unsupported_uri_persists_operation_not_supported_row() -> TestResult<()> {
    let harness = Harness::new(ProviderMode::Fast).await?;
    let response = harness
        .reject_websocket(&format!("/gemini/ws/unsupported?key={QUERY_SECRET}"), true)
        .await?;
    assert_eq!(response.status, StatusCode::NOT_FOUND);
    // Check the missing-row defect before the secondary canonical-path message contract.
    let rows = wait_for_row_count(&harness, 1).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, 404);
    assert_eq!(
        rows[0].error_code.as_deref(),
        Some("operation_not_supported")
    );
    assert_eq!(rows[0].event_kind, Some(RequestEventKind::Other));
    assert!(rows[0].upstream_id.is_none());
    let body = response.json();
    assert_eq!(body["error"]["code"], 404);
    assert_eq!(body["error"]["status"], "NOT_FOUND");
    assert_eq!(body["error"]["type"], "operation_not_supported");
    assert_eq!(
        body["error"]["message"],
        "GET /ws/unsupported is not supported by cc-lb yet"
    );
    harness.assert_no_upstream_accepts();
    assert_no_secrets(&harness, &response, &rows);
    Ok(())
}

#[tokio::test]
async fn gemini_live_without_eligible_key_returns_gemini_503_and_local_route_error()
-> TestResult<()> {
    let harness = Harness::new(ProviderMode::Fast).await?;
    harness.disable_gemini().await?;
    let response = harness
        .reject_websocket(&format!("{GEMINI_LIVE}?key={QUERY_SECRET}"), true)
        .await?;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response.headers["content-type"]
            .to_str()?
            .starts_with("application/json")
    );
    let body = response.json();
    assert_eq!(body["error"]["code"], 503);
    assert_eq!(body["error"]["status"], "UNAVAILABLE");
    assert_eq!(body["error"]["type"], "route_no_upstream_after_filter");
    let rows = wait_for_row_count(&harness, 1).await?;
    assert_eq!(rows[0].status, 503);
    assert_eq!(
        rows[0].error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    harness.assert_no_upstream_accepts();
    assert_no_secrets(&harness, &response, &rows);
    Ok(())
}

#[tokio::test]
async fn client_request_id_precedence_and_admin_counter_format_are_preserved() -> TestResult<()> {
    let harness = Harness::new(ProviderMode::Fast).await?;
    let mut request = harness.post("/v1/responses", true)?;
    request.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_static("client-healthy-control"),
    );
    let response = harness.send(request).await?;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.request_id(), "client-healthy-control");
    let mut request = harness.post("/openai/v1/responses", true)?;
    request
        .headers_mut()
        .insert("request-id", HeaderValue::from_static("primary-id"));
    request
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_static("secondary-id"));
    let response = harness.send(request).await?;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.request_id(), "primary-id");
    wait_for_rows(
        &harness,
        &[("client-healthy-control", 200), ("primary-id", 200)],
    )
    .await?;
    assert_admin_id(&harness.admin_request_id(false).await?);
    Ok(())
}

#[tokio::test]
async fn gemini_http_fast_and_non_gemini_timeout_controls_hold() -> TestResult<()> {
    let harness = Harness::new(ProviderMode::Fast).await?;
    let response = harness.send(harness.post(GEMINI_HTTP, true)?).await?;
    assert_eq!(response.status, StatusCode::OK);
    assert!(
        response.headers["content-type"]
            .to_str()?
            .starts_with("application/json")
    );
    assert_eq!(
        response.json()["candidates"][0]["content"]["parts"][0]["text"],
        "ok"
    );
    let fast_id = response.request_id().to_owned();
    let response = harness.send(harness.post(GEMINI_HTTP, false)?).await?;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(response.json()["error"]["status"], "UNAUTHENTICATED");
    let rows = wait_for_rows(
        &harness,
        &[(fast_id.as_str(), 200), (response.request_id(), 401)],
    )
    .await?;
    assert_eq!(
        rows.iter()
            .find(|row| row.status == 401)
            .unwrap()
            .error_code
            .as_deref(),
        Some("authentication_failed")
    );

    let slow = Harness::new(ProviderMode::Held).await?;
    let response = slow.send(slow.post("/v1/responses", true)?).await?;
    assert_eq!(response.status, StatusCode::GATEWAY_TIMEOUT);
    assert!(response.headers.get("content-type").is_none());
    assert_eq!(response.body.as_ref(), b"request timed out");
    let rows = wait_for_rows(&slow, &[(response.request_id(), 504)]).await?;
    assert_eq!(rows[0].error_code.as_deref(), Some("tower_timeout"));

    let unmounted = Harness::new(ProviderMode::Fast).await?;
    let response = unmounted
        .reject_websocket(
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            true,
        )
        .await?;
    assert_eq!(response.status, StatusCode::NOT_FOUND);
    // A committed subsequent auth failure is the barrier, not a fixed wait for zero rows.
    let sentinel = unmounted
        .reject_websocket("/gemini/ws/unsupported", false)
        .await?;
    assert_eq!(sentinel.status, StatusCode::UNAUTHORIZED);
    let rows = wait_for_row_count(&unmounted, 1).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, 401);
    assert_eq!(rows[0].error_code.as_deref(), Some("authentication_failed"));
    unmounted.assert_no_upstream_accepts();
    Ok(())
}

#[tokio::test]
async fn gemini_live_provider_503_and_healthy_upgrade_controls_hold() -> TestResult<()> {
    let rejecting = Harness::new(ProviderMode::RejectHandshake).await?;
    let (mut client, response) = timeout(
        WAIT,
        connect_async(rejecting.websocket_request(GEMINI_LIVE)?),
    )
    .await??;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    client
        .send(Message::Text(
            r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
        ))
        .await?;
    let message = timeout(WAIT, client.next())
        .await?
        .expect("provider error frame")?;
    let Message::Text(message) = message else {
        panic!("provider rejection must be a JSON frame");
    };
    let body: Value = sonic_rs::from_slice(message.as_bytes())?;
    assert_eq!(body["status"], 503);
    // Live upgrades downstream first; finish_handshake records the provider's later 503.
    let rows = wait_for_row_count(&rejecting, 1).await?;
    assert_eq!(rows[0].status, 503);
    assert_eq!(rows[0].error_code.as_deref(), Some("upstream_5xx"));
    assert_eq!(rejecting.upstream_accepts.load(Ordering::SeqCst), 1);

    let healthy = Harness::new(ProviderMode::Fast).await?;
    let (mut client, response) =
        timeout(WAIT, connect_async(healthy.websocket_request(GEMINI_LIVE)?)).await??;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    client.close(None).await?;
    let rows = wait_for_row_count(&healthy, 1).await?;
    assert_eq!(rows[0].status, 101);
    assert_eq!(rows[0].error_code.as_deref(), Some("client_closed_request"));
    healthy.assert_no_upstream_accepts();
    Ok(())
}
