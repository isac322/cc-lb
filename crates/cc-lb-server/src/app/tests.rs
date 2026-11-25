mod realtime_websocket_tests {
    include!("../app_realtime_websocket_tests.rs");
}
mod websocket_handshake_tests {
    include!("../app_websocket_handshake_tests.rs");
}
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use crate::state_machine::ServerStateHandle;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use cc_lb_config::Config;
use cc_lb_control::api_keys::{
    builtin_authn::BuiltinAuthn,
    concurrent_guard::KeyConcurrencyManager,
    key_store::KeyStore,
    limit_engine::LimitEngine,
    principal_view::{DialectCache, PrincipalView, RouterPipelineCache},
    secret,
};
use cc_lb_control::{DynamicViewHolder, RequestEventBus};
use cc_lb_domain::TerminalStrategy;
use cc_lb_engine::{
    BreakerRegistry, BreakerRuntimeConfig, BulkheadRegistry, BulkheadRuntimeConfig, Lifecycle,
    LifecycleConfig, make_default_dispatcher,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{
    ApiKeyMutation, IssueParams, KeyStatus, ManagedKeyStore, MetaStore as _, Storage,
    StorageResult, StoredApiKeyRecord,
};
use cc_lb_upstream::{
    ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use futures_util::{SinkExt, StreamExt, stream};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback as ServerHandshakeCallback, ErrorResponse as ServerHandshakeErrorResponse,
    Request as ServerHandshakeRequest, Response as ServerHandshakeResponse,
};
use tokio_tungstenite::{accept_async, connect_async};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

use super::*;

fn with_static_limit_subject(
    lifecycle: cc_lb_engine::Lifecycle,
    limit_engine: Arc<LimitEngine>,
    principal_id: String,
    key_id: String,
    record: StoredApiKeyRecord,
) -> cc_lb_engine::Lifecycle {
    struct StaticTestLimitSubjectProvider(cc_lb_engine::AuthLimitSubject);
    #[async_trait]
    impl cc_lb_engine::LimitSubjectProvider for StaticTestLimitSubjectProvider {
        async fn limit_subject(
            &self,
            _ctx: &cc_lb_engine::RequestContext,
            _principal: &cc_lb_domain::Principal,
            _authn_success: &cc_lb_control::api_keys::builtin_authn::AuthnSuccess,
        ) -> Option<cc_lb_engine::AuthLimitSubject> {
            Some(self.0.clone())
        }
    }
    lifecycle.with_limit_engine(
        limit_engine,
        Arc::new(StaticTestLimitSubjectProvider(
            cc_lb_engine::AuthLimitSubject {
                principal_id,
                key_id,
                record,
            },
        )),
    )
}

const TIER_PRICING_TEST_MODEL: &str = "tier-pricing-test-model";

#[test]
fn proxy_route_template_supports_all_gemini_model_operations() {
    assert_eq!(
        proxy_route_template("/v1beta/models/gemini-2.5-pro:generateContent"),
        Some("/v1beta/models/{model}:generateContent")
    );
    assert_eq!(
        proxy_route_template("/v1beta/models/gemini-2.5-pro:streamGenerateContent"),
        Some("/v1beta/models/{model}:streamGenerateContent")
    );
    assert_eq!(
        proxy_route_template("/v1beta/models/gemini-2.5-pro:countTokens"),
        Some("/v1beta/models/{model}:countTokens")
    );
}

struct CaptureHandshakeTarget {
    sender: Option<tokio::sync::oneshot::Sender<String>>,
}

impl ServerHandshakeCallback for CaptureHandshakeTarget {
    fn on_request(
        self,
        request: &ServerHandshakeRequest,
        response: ServerHandshakeResponse,
    ) -> Result<ServerHandshakeResponse, ServerHandshakeErrorResponse> {
        if let Some(sender) = self.sender {
            let target = request
                .uri()
                .path_and_query()
                .expect("provider handshake has a request target")
                .as_str()
                .to_owned();
            let _ = sender.send(target);
        }
        Ok(response)
    }
}

fn pricing_limit_cost_estimator(
    by_tier: BTreeMap<String, cc_lb_pricing::TierRate>,
) -> PricingLimitCostEstimator {
    let catalog = cc_lb_pricing::PriceCatalog::new_empty();
    let mut snapshot = cc_lb_pricing::CatalogSnapshot::empty_cost_disabled();
    snapshot.status = cc_lb_pricing::CatalogStatus::Ok;
    snapshot.models.insert(
        TIER_PRICING_TEST_MODEL.to_owned(),
        cc_lb_pricing::Pricing {
            native: None,
            model: TIER_PRICING_TEST_MODEL.to_owned(),
            input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(8),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier,
        },
    );
    catalog.install_snapshot(snapshot);
    PricingLimitCostEstimator { catalog }
}

async fn panic_when_polled() -> Result<Bytes, Infallible> {
    panic!("request body must not be polled")
}

fn controlled_body() -> (
    tokio::sync::mpsc::UnboundedSender<Result<Bytes, io::Error>>,
    tokio::sync::mpsc::UnboundedReceiver<()>,
    Body,
) {
    let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
    let (poll_tx, poll_rx) = tokio::sync::mpsc::unbounded_channel();
    let stream = stream::unfold((chunk_rx, poll_tx), |(mut chunk_rx, poll_tx)| async move {
        let _ = poll_tx.send(());
        chunk_rx
            .recv()
            .await
            .map(|chunk| (chunk, (chunk_rx, poll_tx)))
    });
    (chunk_tx, poll_rx, Body::from_stream(stream))
}

fn assert_measured(value: Option<f64>, field: &str) -> f64 {
    let value = value.unwrap_or_else(|| panic!("{field} should be measured"));
    assert!(value.is_finite(), "{field} should be finite: {value}");
    assert!(value >= 0.0, "{field} should be nonnegative: {value}");
    value
}

const TEST_PRINCIPAL: &str = "principal-test";

struct TestKey {
    plaintext: String,
    key_id: String,
    record: StoredApiKeyRecord,
}

static TEST_KEY: LazyLock<TestKey> = LazyLock::new(|| {
    let generated = secret::generate_new();
    TestKey {
        plaintext: generated.plaintext.expose().to_owned(),
        key_id: generated.key_id,
        record: StoredApiKeyRecord {
            label: "body-read test key".to_owned(),
            verify_hash: generated.verify_hash,
            secret_salt: generated.secret_salt,
            status: KeyStatus::Active,
            last_4: generated.last_4,
            index_hash: generated.index_hash,
            ..StoredApiKeyRecord::default()
        },
    }
});

/// In-memory `ManagedKeyStore` resolving exactly the fixture key for
/// `TEST_PRINCIPAL` (mirrors the engine `InMemoryManagedKeyStore`).
struct FixtureKeyStore;

#[async_trait]
impl ManagedKeyStore for FixtureKeyStore {
    async fn issue(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        Ok(TEST_KEY.record.clone())
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        Ok(
            (principal_id == TEST_PRINCIPAL && key_id == TEST_KEY.key_id)
                .then(|| TEST_KEY.record.clone()),
        )
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        Ok((index_hash == &TEST_KEY.record.index_hash).then(|| {
            (
                TEST_PRINCIPAL.to_owned(),
                TEST_KEY.key_id.clone(),
                TEST_KEY.record.clone(),
            )
        }))
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        Ok((principal_id == TEST_PRINCIPAL)
            .then(|| TEST_KEY.record.clone())
            .into_iter()
            .collect())
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        Ok(vec![(
            TEST_PRINCIPAL.to_owned(),
            TEST_KEY.key_id.clone(),
            TEST_KEY.record.clone(),
        )])
    }

    async fn update(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn revoke_zero_secrets(&self, _principal_id: &str, _key_id: &str) -> StorageResult<()> {
        Ok(())
    }
}

struct CountingFixtureKeyStore {
    lookups: Arc<AtomicU64>,
}

#[async_trait]
impl ManagedKeyStore for CountingFixtureKeyStore {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        FixtureKeyStore.issue(principal_id, key_id, params).await
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        FixtureKeyStore.get(principal_id, key_id).await
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        FixtureKeyStore.lookup_by_index_hash(index_hash).await
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        FixtureKeyStore.list_by_principal(principal_id).await
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        FixtureKeyStore.list_all().await
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        FixtureKeyStore.update(principal_id, key_id, mutation).await
    }

    async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()> {
        FixtureKeyStore
            .revoke_zero_secrets(principal_id, key_id)
            .await
    }
}

async fn counted_unexpected_body_poll(counter: Arc<AtomicU64>) -> Result<Bytes, Infallible> {
    counter.fetch_add(1, Ordering::SeqCst);
    panic!("pre-auth drain must never poll the request body")
}

struct WebSocketTestLimitEstimator;

impl cc_lb_engine::LimitCostEstimator for WebSocketTestLimitEstimator {
    fn estimate_max(
        &self,
        _model: &str,
        _max_input: u64,
        _max_output: u64,
        _service_tier: Option<&str>,
    ) -> Option<i64> {
        Some(200)
    }
    fn estimate_max_with_local_cache(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.estimate_max(
            model,
            input
                .uncached_input_tokens
                .saturating_add(input.cache_prefix_5m_tokens)
                .saturating_add(input.cache_prefix_1h_tokens),
            max_output,
            service_tier,
        )
    }
}

struct PendingWebSocketSendDrop(Arc<AtomicU64>);

impl Drop for PendingWebSocketSendDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Runs the real authentication path so `read_request_body` receives a
/// genuine `Authenticated` proof token.
async fn authenticate_test_request(headers: &HeaderMap) -> cc_lb_engine::Authenticated {
    let storage: Arc<dyn ManagedKeyStore> = Arc::new(FixtureKeyStore);
    let authn = BuiltinAuthn::new(
        Arc::new(KeyStore::new(storage)),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let view = PrincipalView::for_tests(
        TEST_PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::new(),
    );
    let mut headers = headers.clone();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(TEST_KEY.plaintext.as_str())
            .expect("fixture key is a valid header value"),
    );
    cc_lb_engine::authenticate_first(&authn, &headers, &view)
        .await
        .expect("fixture key should authenticate")
}

async fn read_with_timings(
    headers: HeaderMap,
    body: Body,
    cap: usize,
) -> (
    Result<Bytes, RequestBodyReadError>,
    cc_lb_lifecycle::RequestIoTimings,
) {
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let observer = cc_lb_engine::LifecycleContext::new(
        "measured-ingress".to_owned(),
        bus as Arc<dyn RequestEventBus>,
        &clock,
    );
    observer.mark_authn_reached();
    let auth = authenticate_test_request(&headers).await;
    let result = read_request_body(&headers, body, cap, Some(observer.clone()), &auth).await;
    drop(observer);
    let LifecycleEvent::RequestTerminated { io_timings, .. } = events
        .recv()
        .await
        .expect("terminal event should be emitted")
    else {
        panic!("expected terminal event");
    };
    (result, io_timings)
}

fn assert_ms(actual: Option<f64>, expected: f64, field: &str) {
    let actual = actual.unwrap_or_else(|| panic!("{field} should be measured"));
    assert!(
        (actual - expected).abs() < 1e-9,
        "{field}: expected {expected}, got {actual}"
    );
}

#[test]
fn timing_snapshot_partitions_completed_elapsed_time_without_overlap() {
    let ended = Instant::now();
    let started = ended.checked_sub(Duration::from_millis(30)).unwrap();
    let first_chunk_received = ended.checked_sub(Duration::from_millis(20));
    let process_started = ended.checked_sub(Duration::from_millis(6)).unwrap();
    let timings = RequestBodyTimingGuard {
        observer: None,
        started,
        first_chunk_received,
        accumulated_wait: Duration::from_millis(12),
        active_wait_started: None,
        process_started,
        process: Duration::from_millis(12),
        chunk_count: 2,
        published: false,
    }
    .snapshot(ended);

    assert_ms(timings.request_body_first_chunk_ms, 10.0, "first chunk");
    assert_ms(timings.request_body_receive_ms, 20.0, "receive");
    assert_ms(timings.request_body_wait_ms, 12.0, "wait");
    assert_ms(timings.request_body_process_ms, 18.0, "process");
    assert_eq!(timings.request_body_chunk_count, Some(2));
}

#[test]
fn timing_snapshot_counts_an_in_flight_cancelled_await_as_wait_only() {
    let ended = Instant::now();
    let started = ended.checked_sub(Duration::from_millis(30)).unwrap();
    let first_chunk_received = ended.checked_sub(Duration::from_millis(20));
    let active_wait_started = ended.checked_sub(Duration::from_millis(4));
    let timings = RequestBodyTimingGuard {
        observer: None,
        started,
        first_chunk_received,
        accumulated_wait: Duration::from_millis(8),
        active_wait_started,
        process_started: active_wait_started.unwrap(),
        process: Duration::from_millis(18),
        chunk_count: 1,
        published: false,
    }
    .snapshot(ended);

    assert_ms(timings.request_body_first_chunk_ms, 10.0, "first chunk");
    assert_ms(timings.request_body_receive_ms, 20.0, "receive");
    assert_ms(timings.request_body_wait_ms, 12.0, "wait");
    assert_ms(timings.request_body_process_ms, 18.0, "process");
    assert_eq!(timings.request_body_chunk_count, Some(1));
}

#[tokio::test]
async fn oversized_content_length_rejects_without_polling_body() {
    // Given
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("6"));
    let body = Body::from_stream(stream::once(panic_when_polled()));

    // When
    let (result, io_timings) = read_with_timings(headers, body, 5).await;

    // Then
    assert!(matches!(result, Err(RequestBodyReadError::TooLarge)));
    assert_eq!(io_timings.request_body_first_chunk_ms, None);
    assert_eq!(io_timings.request_body_receive_ms, None);
    assert_eq!(io_timings.request_body_wait_ms, Some(0.0));
    assert_measured(
        io_timings.request_body_process_ms,
        "request_body_process_ms",
    );
    assert_eq!(io_timings.request_body_chunk_count, Some(0));
}

#[tokio::test]
async fn single_frame_body_remains_zero_copy_at_cap() {
    // Given
    let input = Bytes::from_static(b"single frame");
    let input_ptr = input.as_ptr();

    // When
    let (output, io_timings) =
        read_with_timings(HeaderMap::new(), Body::from(input.clone()), input.len()).await;
    let output = output.expect("body at cap should collect");

    // Then
    assert_eq!(output, input);
    assert_eq!(output.as_ptr(), input_ptr);
    assert_eq!(io_timings.request_body_chunk_count, Some(1));
}

#[tokio::test]
async fn delayed_first_and_subsequent_frames_preserve_bytes_and_split_waits() {
    // Given
    let (chunk_tx, mut poll_rx, body) = controlled_body();
    let read = tokio::spawn(async move {
        let (output, io_timings) = read_with_timings(HeaderMap::new(), body, 6).await;
        (output.expect("controlled body should collect"), io_timings)
    });

    poll_rx
        .recv()
        .await
        .expect("first frame await should begin");
    assert!(!read.is_finished(), "first frame should remain pending");
    chunk_tx
        .send(Ok(Bytes::from_static(b"abc")))
        .expect("send first frame");

    poll_rx
        .recv()
        .await
        .expect("subsequent frame await should begin");
    assert!(
        !read.is_finished(),
        "subsequent frame should remain pending"
    );
    chunk_tx
        .send(Ok(Bytes::from_static(b"def")))
        .expect("send subsequent frame");
    drop(chunk_tx);

    // When
    let (output, io_timings) = read.await.expect("body reader task should complete");

    // Then
    assert_eq!(output, Bytes::from_static(b"abcdef"));
    assert!(
        assert_measured(
            io_timings.request_body_first_chunk_ms,
            "request_body_first_chunk_ms",
        ) > 0.0
    );
    assert!(
        assert_measured(
            io_timings.request_body_receive_ms,
            "request_body_receive_ms",
        ) > 0.0
    );
    assert!(assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms") > 0.0);
    assert_measured(
        io_timings.request_body_process_ms,
        "request_body_process_ms",
    );
    assert_eq!(io_timings.request_body_chunk_count, Some(2));
}

#[tokio::test]
async fn empty_data_frames_do_not_start_data_markers_or_increment_count() {
    let (chunk_tx, mut poll_rx, body) = controlled_body();
    let read = tokio::spawn(async move { read_with_timings(HeaderMap::new(), body, 6).await });
    poll_rx
        .recv()
        .await
        .expect("first frame await should begin");
    chunk_tx
        .send(Ok(Bytes::new()))
        .expect("send empty DATA frame");
    poll_rx
        .recv()
        .await
        .expect("reader should await after empty DATA");
    assert!(!read.is_finished(), "empty DATA must not finish collection");
    drop(chunk_tx);

    // When
    let (output, io_timings) = read.await.expect("body reader task should complete");
    let output = output.expect("empty body should collect");

    // Then
    assert!(output.is_empty());
    assert_eq!(io_timings.request_body_first_chunk_ms, None);
    assert_eq!(io_timings.request_body_receive_ms, None);
    assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms");
    assert_measured(
        io_timings.request_body_process_ms,
        "request_body_process_ms",
    );
    assert_eq!(io_timings.request_body_chunk_count, Some(0));
}

#[tokio::test]
async fn multi_frame_body_collects_into_contiguous_bytes_at_cap() {
    // Given
    let body = Body::from_stream(stream::iter([
        Ok::<_, Infallible>(Bytes::from_static(b"abc")),
        Ok::<_, Infallible>(Bytes::from_static(b"def")),
    ]));

    // When
    let (output, io_timings) = read_with_timings(HeaderMap::new(), body, 6).await;
    let output = output.expect("body at cap should collect");

    // Then
    assert_eq!(output, Bytes::from_static(b"abcdef"));
    assert_eq!(io_timings.request_body_chunk_count, Some(2));
}

#[tokio::test]
async fn streamed_cap_error_preserves_completed_ingress_measurements() {
    // Given
    let headers = HeaderMap::new();
    let body = Body::from_stream(stream::iter([
        Ok::<_, Infallible>(Bytes::from_static(b"abc")),
        Ok::<_, Infallible>(Bytes::from_static(b"def")),
    ]));

    // When
    let (result, io_timings) = read_with_timings(headers, body, 5).await;

    // Then
    assert!(matches!(result, Err(RequestBodyReadError::TooLarge)));
    assert_measured(
        io_timings.request_body_first_chunk_ms,
        "request_body_first_chunk_ms",
    );
    assert_measured(
        io_timings.request_body_receive_ms,
        "request_body_receive_ms",
    );
    assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms");
    assert_measured(
        io_timings.request_body_process_ms,
        "request_body_process_ms",
    );
    assert_eq!(io_timings.request_body_chunk_count, Some(1));
}

#[tokio::test]
async fn transport_error_preserves_completed_ingress_measurements() {
    // Given
    let body = Body::from_stream(stream::iter([
        Ok(Bytes::from_static(b"abc")),
        Err(io::Error::other("transport failed")),
    ]));

    // When
    let (result, io_timings) = read_with_timings(HeaderMap::new(), body, 6).await;

    // Then
    let Err(RequestBodyReadError::Read(source)) = result else {
        panic!("transport failure should remain a read error");
    };
    assert!(source.to_string().contains("transport failed"));
    assert_measured(
        io_timings.request_body_first_chunk_ms,
        "request_body_first_chunk_ms",
    );
    assert_measured(
        io_timings.request_body_receive_ms,
        "request_body_receive_ms",
    );
    assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms");
    assert_measured(
        io_timings.request_body_process_ms,
        "request_body_process_ms",
    );
    assert_eq!(io_timings.request_body_chunk_count, Some(1));
}

#[tokio::test]
async fn cancelled_body_read_publishes_active_wait_before_terminal_finish() {
    // Given
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let observer = cc_lb_engine::LifecycleContext::new(
        "cancelled-ingress".to_owned(),
        bus as Arc<dyn RequestEventBus>,
        &clock,
    );
    observer.mark_authn_reached();
    let (chunk_tx, mut poll_rx, body) = controlled_body();
    let read_observer = observer.clone();
    let auth = authenticate_test_request(&HeaderMap::new()).await;
    let read = tokio::spawn(async move {
        read_request_body(&HeaderMap::new(), body, 6, Some(read_observer), &auth).await
    });
    poll_rx
        .recv()
        .await
        .expect("first frame await should begin");
    chunk_tx
        .send(Ok(Bytes::from_static(b"abc")))
        .expect("send first frame");
    poll_rx
        .recv()
        .await
        .expect("subsequent frame await should begin");

    // When
    read.abort();
    let error = read.await.expect_err("body reader should be cancelled");
    assert!(error.is_cancelled());
    drop(observer);

    // Then
    let LifecycleEvent::RequestTerminated { io_timings, .. } = events
        .recv()
        .await
        .expect("terminal event should be emitted")
    else {
        panic!("expected terminal event");
    };
    assert_measured(
        io_timings.request_body_first_chunk_ms,
        "request_body_first_chunk_ms",
    );
    assert!(
        assert_measured(
            io_timings.request_body_receive_ms,
            "request_body_receive_ms",
        ) > 0.0
    );
    assert!(assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms",) > 0.0);
    assert_measured(
        io_timings.request_body_process_ms,
        "request_body_process_ms",
    );
    assert_eq!(io_timings.request_body_chunk_count, Some(1));
}

#[test]
fn request_body_caps_select_files_and_messages_independently() {
    // Given
    let caps = RequestBodyCaps {
        messages: 3,
        files: 7,
    };

    // When / Then
    assert_eq!(caps.for_path("/v1/messages"), 3);
    assert_eq!(caps.for_path("/v1/files"), 7);
    assert_eq!(caps.for_path("/v1/files/id/content"), 7);
    for path in [
        "/v1/audio/transcriptions",
        "/v1/audio/translations",
        "/v1/images/edits",
        "/v1/images/variations",
    ] {
        assert_eq!(caps.for_path(path), 7, "{path}");
    }
    assert_eq!(caps.for_path("/v1/models"), 3);
}

#[tokio::test]
async fn body_too_large_response_reuses_anthropic_error_shape() {
    // Given / When
    let response = body_too_large_response();
    let status = response.status();
    let content_type = response.headers()[axum::http::header::CONTENT_TYPE].clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("error body should collect")
        .to_bytes();

    // Then
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(content_type, "application/json; charset=utf-8");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("error body should be json"),
        serde_json::json!({
            "type": "error",
            "error": {
                "type": "body_too_large",
                "message": "request body exceeds configured cap",
            },
        })
    );
}

#[tokio::test]
async fn readiness_reports_undecryptable_openai_and_gemini_oauth_as_degraded_when_api_key_is_ready()
{
    for kind in [
        cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
        cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth,
    ] {
        let api_key = websocket_upstream(
            401,
            "ready-api-key",
            Url::parse("http://api-key.example").expect("api-key URL"),
        );
        let mut oauth = websocket_upstream(
            402,
            "broken-oauth",
            Url::parse("http://oauth.example").expect("oauth URL"),
        );
        oauth.kind = kind;
        let statuses = cc_lb_control::UpstreamStatusSnapshot {
            entries: HashMap::from([
                (
                    api_key.name.clone(),
                    cc_lb_control::UpstreamStatusEntry {
                        status: cc_lb_control::ApplyStatus::Active,
                        last_apply_error: None,
                        last_apply_at_unix_secs: 0,
                    },
                ),
                (
                    oauth.name.clone(),
                    cc_lb_control::UpstreamStatusEntry {
                        status: cc_lb_control::ApplyStatus::Active,
                        last_apply_error: None,
                        last_apply_at_unix_secs: 0,
                    },
                ),
            ]),
            ..Default::default()
        };
        let state = websocket_test_state(
            vec![api_key, oauth],
            TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(WebSocketTestSignerFactory),
            Arc::new(FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        state.server_state.transition_to_ready();
        let view = state.dynamic_view.load();
        state.dynamic_view.store(
            cc_lb_control::DynamicViewBuilder::from_view(&view)
                .upstream_status_snapshot(Arc::new(statuses))
                .build(),
        );
        assert!(matches!(
            readiness(&state),
            Readiness::Ready {
                degraded: Some("oauth_credentials_not_decryptable")
            }
        ));
    }
}

#[tokio::test]
async fn qa_ready_1_terminal_vertex_credential_is_unusable_without_probe_decrypt() {
    let mut terminal = websocket_upstream(
        404,
        "terminal-vertex",
        Url::parse("http://vertex.example").expect("vertex URL"),
    );
    terminal.kind = UpstreamKind::VertexCredential;
    terminal.api_key_ciphertext = Some(vec![0xff]);
    terminal.last_apply_error =
        Some(cc_lb_domain::vertex::VERTEX_CREDENTIAL_INVALID_CLIENT.to_owned());
    let statuses = cc_lb_control::UpstreamStatusSnapshot {
        entries: HashMap::from([(
            terminal.name.clone(),
            cc_lb_control::UpstreamStatusEntry {
                status: cc_lb_control::ApplyStatus::Active,
                last_apply_error: terminal.last_apply_error.clone(),
                last_apply_at_unix_secs: 0,
            },
        )]),
        ..Default::default()
    };
    let state = websocket_test_state(
        vec![terminal],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.server_state.transition_to_ready();
    let view = state.dynamic_view.load();
    state.dynamic_view.store(
        cc_lb_control::DynamicViewBuilder::from_view(&view)
            .upstream_status_snapshot(Arc::new(statuses))
            .build(),
    );
    assert!(matches!(
        readiness(&state),
        Readiness::NotReady("no_ready_upstream")
    ));
}
#[tokio::test]
async fn qa_ready_1_healthy_upstream_keeps_terminal_vertex_mix_ready() {
    let healthy = websocket_upstream(
        405,
        "healthy-api-key",
        Url::parse("http://api-key.example").expect("api key URL"),
    );
    let mut terminal = websocket_upstream(
        406,
        "terminal-vertex-mix",
        Url::parse("http://vertex.example").expect("vertex URL"),
    );
    terminal.kind = UpstreamKind::VertexCredential;
    terminal.api_key_ciphertext = Some(vec![0xff]);
    terminal.last_apply_error =
        Some(cc_lb_domain::vertex::VERTEX_CREDENTIAL_INVALID_GRANT.to_owned());
    let statuses = cc_lb_control::UpstreamStatusSnapshot {
        entries: HashMap::from([
            (
                healthy.name.clone(),
                cc_lb_control::UpstreamStatusEntry {
                    status: cc_lb_control::ApplyStatus::Active,
                    last_apply_error: None,
                    last_apply_at_unix_secs: 0,
                },
            ),
            (
                terminal.name.clone(),
                cc_lb_control::UpstreamStatusEntry {
                    status: cc_lb_control::ApplyStatus::Active,
                    last_apply_error: terminal.last_apply_error.clone(),
                    last_apply_at_unix_secs: 0,
                },
            ),
        ]),
        ..Default::default()
    };
    let state = websocket_test_state(
        vec![healthy, terminal],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.server_state.transition_to_ready();
    let view = state.dynamic_view.load();
    state.dynamic_view.store(
        cc_lb_control::DynamicViewBuilder::from_view(&view)
            .upstream_status_snapshot(Arc::new(statuses))
            .build(),
    );
    assert!(matches!(readiness(&state), Readiness::Ready { .. }));
}

#[tokio::test]
async fn readiness_blocks_when_only_openai_or_gemini_oauth_is_undecryptable() {
    for kind in [
        cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
        cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth,
    ] {
        let mut oauth = websocket_upstream(
            403,
            "only-broken-oauth",
            Url::parse("http://oauth.example").expect("oauth URL"),
        );
        oauth.kind = kind;
        let statuses = cc_lb_control::UpstreamStatusSnapshot {
            entries: HashMap::from([(
                oauth.name.clone(),
                cc_lb_control::UpstreamStatusEntry {
                    status: cc_lb_control::ApplyStatus::Active,
                    last_apply_error: None,
                    last_apply_at_unix_secs: 0,
                },
            )]),
            ..Default::default()
        };
        let state = websocket_test_state(
            vec![oauth],
            TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(WebSocketTestSignerFactory),
            Arc::new(FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        state.server_state.transition_to_ready();
        let view = state.dynamic_view.load();
        state.dynamic_view.store(
            cc_lb_control::DynamicViewBuilder::from_view(&view)
                .upstream_status_snapshot(Arc::new(statuses))
                .build(),
        );
        assert!(matches!(
            readiness(&state),
            Readiness::NotReady("no_ready_upstream")
        ));
    }
}

#[test]
fn default_fallback_populates_empty_catalog() {
    let catalog = cc_lb_pricing::PriceCatalog::new_empty();
    assert!(matches!(
        catalog.status(),
        cc_lb_pricing::CatalogStatus::CostDisabled
    ));

    let clock = cc_lb_clock::SystemClock;
    install_default_fallback_if_uninitialized(&catalog, &clock);

    assert!(matches!(catalog.status(), cc_lb_pricing::CatalogStatus::Ok));
    assert!(catalog.lookup("claude-opus-4-5", None).is_some());
}

#[test]
fn pricing_limit_cost_estimator_uses_requested_priority_tier() {
    // Given an estimator with a distinct priority price.
    let estimator = pricing_limit_cost_estimator(BTreeMap::from([(
        "priority".to_owned(),
        cc_lb_pricing::TierRate {
            input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(12),
        },
    )]));

    // When maximum cost is estimated for a priority request.
    let estimate = cc_lb_engine::LimitCostEstimator::estimate_max(
        &estimator,
        TIER_PRICING_TEST_MODEL,
        1_000_000,
        1_000_000,
        Some("priority"),
    );

    // Then the explicit priority rates are reserved.
    assert_eq!(estimate, Some(15_000_000));
}

#[test]
fn pricing_limit_cost_estimator_uses_batch_fallback() {
    // Given an estimator whose catalog has only base prices.
    let estimator = pricing_limit_cost_estimator(BTreeMap::new());

    // When maximum cost is estimated for a batch request.
    let estimate = cc_lb_engine::LimitCostEstimator::estimate_max(
        &estimator,
        TIER_PRICING_TEST_MODEL,
        1_000_000,
        1_000_000,
        Some("batch"),
    );

    // Then the catalog's exact-half batch fallback is reserved.
    assert_eq!(estimate, Some(5_000_000));
}

#[test]
fn default_fallback_contains_current_anthropic_pricing() {
    let catalog = cc_lb_pricing::PriceCatalog::new_empty();
    let clock = cc_lb_clock::SystemClock;

    install_default_fallback_if_uninitialized(&catalog, &clock);

    let expected = [
        (
            "claude-fable-5",
            10_000_000,
            12_500_000,
            20_000_000,
            1_000_000,
            50_000_000,
        ),
        (
            "claude-mythos-5",
            10_000_000,
            12_500_000,
            20_000_000,
            1_000_000,
            50_000_000,
        ),
        (
            "claude-opus-5-5",
            4_000_000,
            5_000_000,
            8_000_000,
            200_000,
            20_000_000,
        ),
        (
            "claude-opus-5",
            5_000_000,
            6_250_000,
            10_000_000,
            500_000,
            25_000_000,
        ),
        (
            "claude-opus-4-8",
            5_000_000,
            6_250_000,
            10_000_000,
            500_000,
            25_000_000,
        ),
        (
            "claude-opus-4-7",
            5_000_000,
            6_250_000,
            10_000_000,
            500_000,
            25_000_000,
        ),
        (
            "claude-opus-4-6",
            5_000_000,
            6_250_000,
            10_000_000,
            500_000,
            25_000_000,
        ),
        (
            "claude-opus-4-5",
            5_000_000,
            6_250_000,
            10_000_000,
            500_000,
            25_000_000,
        ),
        (
            "claude-sonnet-5-5",
            2_000_000,
            2_500_000,
            4_000_000,
            100_000,
            10_000_000,
        ),
        (
            "claude-sonnet-5",
            3_000_000,
            3_750_000,
            6_000_000,
            300_000,
            15_000_000,
        ),
        (
            "claude-sonnet-4-6",
            3_000_000,
            3_750_000,
            6_000_000,
            300_000,
            15_000_000,
        ),
        (
            "claude-sonnet-4-5",
            3_000_000,
            3_750_000,
            6_000_000,
            300_000,
            15_000_000,
        ),
        (
            "claude-haiku-5-5",
            100_000,
            125_000,
            200_000,
            10_000,
            500_000,
        ),
        (
            "claude-haiku-4-5",
            1_000_000,
            1_250_000,
            2_000_000,
            100_000,
            5_000_000,
        ),
    ];

    for (model, input, cache_5m, cache_1h, cache_read, output) in expected {
        let pricing = catalog
            .lookup(model, None)
            .unwrap_or_else(|| panic!("{model} fallback pricing exists"));
        assert_eq!(pricing.input_per_million_usd.as_micros_usd(), input);
        assert_eq!(pricing.output_per_million_usd.as_micros_usd(), output);

        let cache_pricing = catalog
            .routing_cache_pricing(model, None)
            .unwrap_or_else(|| panic!("{model} fallback cache pricing exists"));
        assert_eq!(
            cache_pricing
                .cache_creation_5m_per_million_usd
                .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
            Some(cache_5m)
        );
        assert_eq!(
            cache_pricing
                .cache_creation_1h_per_million_usd
                .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
            Some(cache_1h)
        );
        assert_eq!(
            cache_pricing
                .cache_read_per_million_usd
                .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
            Some(cache_read)
        );
    }

    let snapshot = catalog.current();
    assert_eq!(
        snapshot
            .models
            .keys()
            .filter(|model| model.starts_with("claude-fable"))
            .count(),
        1
    );
    assert!(!snapshot.above_tokens_rates.contains_key("claude-opus-5-5"));
    assert!(
        !snapshot
            .above_tokens_rates
            .contains_key("claude-sonnet-5-5")
    );
}

#[test]
fn default_fallback_haiku_5_5_applies_5x_band_above_100k() {
    let catalog = cc_lb_pricing::PriceCatalog::new_empty();
    let clock = cc_lb_clock::SystemClock;

    install_default_fallback_if_uninitialized(&catalog, &clock);

    let snapshot = catalog.current();
    let bands = snapshot
        .above_tokens_rates
        .get("claude-haiku-5-5")
        .expect("claude-haiku-5-5 threshold bands exist");
    let band = bands
        .get(&100_000)
        .expect("claude-haiku-5-5 100k threshold band exists");
    assert_eq!(
        band.default
            .input_per_million_usd
            .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
        Some(500_000)
    );
    assert_eq!(
        band.default
            .output_per_million_usd
            .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
        Some(2_500_000)
    );
    assert_eq!(
        band.default
            .cache_creation_per_million_usd
            .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
        Some(625_000)
    );
    assert_eq!(
        band.default
            .cache_read_per_million_usd
            .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
        Some(50_000)
    );

    // At 100,000 gross input tokens: base rates ($0.10 in / $0.50 out per MTok).
    assert_eq!(
        catalog.estimate_max("claude-haiku-5-5", 100_000, 0, None),
        Some(10_000)
    );
    assert_eq!(
        catalog.estimate_max("claude-haiku-5-5", 100_000, 1_000_000, None),
        Some(510_000)
    );

    // Strictly above 100,000 gross input tokens: 5x rates ($0.50 in / $2.50 out per MTok).
    assert_eq!(
        catalog.estimate_max("claude-haiku-5-5", 100_001, 0, None),
        Some(50_000)
    );
    assert_eq!(
        catalog.estimate_max("claude-haiku-5-5", 120_000, 0, None),
        Some(60_000)
    );
    assert_eq!(
        catalog.estimate_max("claude-haiku-5-5", 100_001, 1_000_000, None),
        Some(2_550_000)
    );

    // Cache-bearing prompts cross the 100,000 threshold based on gross input tokens.
    assert_eq!(
        catalog.estimate_max_with_cache("claude-haiku-5-5", 80_000, 0, 20_000, false, None),
        Some(10_500)
    );
    assert_eq!(
        catalog.estimate_max_with_cache("claude-haiku-5-5", 100_000, 0, 20_000, false, None),
        Some(62_500)
    );
    assert_eq!(
        catalog.estimate_max_with_local_cache(
            "claude-haiku-5-5",
            cc_lb_pricing::LocalCacheInputEstimate {
                uncached_input_tokens: 0,
                cache_prefix_5m_tokens: 100_000,
                cache_prefix_1h_tokens: 0,
            },
            0,
            None,
        ),
        Some(12_500)
    );
    assert_eq!(
        catalog.estimate_max_with_local_cache(
            "claude-haiku-5-5",
            cc_lb_pricing::LocalCacheInputEstimate {
                uncached_input_tokens: 1,
                cache_prefix_5m_tokens: 100_000,
                cache_prefix_1h_tokens: 0,
            },
            0,
            None,
        ),
        Some(62_500)
    );
    assert_eq!(
        catalog.estimate_max_with_local_cache(
            "claude-haiku-5-5",
            cc_lb_pricing::LocalCacheInputEstimate {
                uncached_input_tokens: 0,
                cache_prefix_5m_tokens: 0,
                cache_prefix_1h_tokens: 100_000,
            },
            0,
            None,
        ),
        Some(20_000)
    );
    assert_eq!(
        catalog.estimate_max_with_local_cache(
            "claude-haiku-5-5",
            cc_lb_pricing::LocalCacheInputEstimate {
                uncached_input_tokens: 1,
                cache_prefix_5m_tokens: 0,
                cache_prefix_1h_tokens: 100_000,
            },
            0,
            None,
        ),
        Some(100_000)
    );
}

#[test]
fn default_fallback_preserves_existing_ok_snapshot() {
    use std::collections::HashMap;

    let catalog = cc_lb_pricing::PriceCatalog::new_empty();
    let mut models = HashMap::new();
    models.insert(
        "operator-model-a".to_owned(),
        cc_lb_pricing::Pricing {
            native: None,
            model: "operator-model-a".to_owned(),
            input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(8),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: Default::default(),
        },
    );
    catalog.install_snapshot(cc_lb_pricing::CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 0,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: cc_lb_pricing::CatalogStatus::Ok,
    });

    let clock = cc_lb_clock::SystemClock;
    install_default_fallback_if_uninitialized(&catalog, &clock);

    assert!(catalog.lookup("operator-model-a", None).is_some());
    assert!(catalog.lookup("claude-opus-4-5", None).is_none());
}

#[test]
fn lifecycle_config_uses_startup_upstream_affinity_ttl() {
    let mut config = Config::default();
    config.upstream_affinity.ttl_days = 14;
    config.upstream_affinity.unknown_openai_reasoning =
        cc_lb_config::UnknownOpenaiReasoningPolicy::FailClosed;

    let lifecycle_config = super::lifecycle_config_from_config(
        &config,
        super::RequestBodyCaps {
            messages: 1024,
            files: 2048,
        },
        None,
    );

    assert_eq!(
        lifecycle_config.upstream_affinity_ttl,
        std::time::Duration::from_secs(14 * 86_400)
    );
    assert_eq!(
        lifecycle_config.unknown_openai_reasoning,
        cc_lb_config::UnknownOpenaiReasoningPolicy::FailClosed
    );
}

#[test]
fn wasmtime_config_overrides_hot_engine_memory_profile() {
    let mut config = Config::default();
    config.runtime.wasmtime.allocation_strategy = cc_lb_config::WasmtimeAllocationStrategy::Pooling;
    config.runtime.wasmtime.memory_max_pages = Some(4096);
    config.runtime.wasmtime.memory_reservation_bytes = Some(512 * 1024 * 1024);
    config.runtime.wasmtime.memory_guard_bytes = Some(128 * 1024 * 1024);
    config.runtime.wasmtime.pool_total_memories = Some(8);
    config.runtime.wasmtime.pool_total_core_instances = Some(12);

    let hot_engine_cfg = super::hot_engine_config_from_config(&config);

    assert_eq!(
        hot_engine_cfg.allocation_strategy,
        cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::Pooling,
    );
    assert_eq!(hot_engine_cfg.memory_max_pages, 4096);
    assert_eq!(hot_engine_cfg.memory_reservation_bytes, 512 * 1024 * 1024);
    assert_eq!(hot_engine_cfg.memory_guard_bytes, 128 * 1024 * 1024);
    assert_eq!(hot_engine_cfg.pool_total_memories, 8);
    assert_eq!(hot_engine_cfg.pool_total_core_instances, 12);
}

#[test]
fn wasmtime_config_defaults_to_on_demand_hot_engine() {
    let hot_engine_cfg = super::hot_engine_config_from_config(&Config::default());

    assert_eq!(
        hot_engine_cfg.allocation_strategy,
        cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::OnDemand,
    );
}

/// QA-06: timeout responses use the route's provider dialect without
/// polling request bodies. Lifecycle events stay silent before authn and
/// record a typed timeout once the handler marks authn reached.
#[tokio::test]
async fn pre_authn_timeout_emits_no_lifecycle_events() {
    struct PendingSignerFactory;

    impl cc_lb_upstream::ApiKeyAwareSignerFactory for PendingSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn cc_lb_upstream::SignerFactory> {
            unreachable!("pre-authn timeout never signs")
        }
    }

    struct PendingDispatch;

    #[async_trait]
    impl UpstreamDispatch for PendingDispatch {
        async fn dispatch(
            &self,
            _request: SignedRequest,
        ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
            unreachable!("pre-authn timeout never dispatches")
        }
    }

    async fn pending_handler() -> Response<Body> {
        std::future::pending::<()>().await;
        unreachable!("pre-authn timeout never reaches the handler")
    }

    async fn authenticated_pending_handler(
        Extension(observer): Extension<cc_lb_engine::LifecycleContext>,
    ) -> Response<Body> {
        observer.mark_authn_reached();
        std::future::pending::<Response<Body>>().await
    }

    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let storage = cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
        .await
        .expect("in-memory storage opens");
    let builtin_authn = Arc::new(BuiltinAuthn::new(
        Arc::new(KeyStore::new(
            Arc::new(FixtureKeyStore) as Arc<dyn ManagedKeyStore>
        )),
        clock.clone(),
    ));
    let view = cc_lb_control::DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(PendingSignerFactory))
        .principal_view(Arc::new(PrincipalView::for_tests(
            TEST_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        )))
        .upstream_records(Vec::new())
        .build();
    let dynamic_view = Arc::new(DynamicViewHolder::new(view));
    let lifecycle = Arc::new(
        Lifecycle::new_with_dynamic_view(
            builtin_authn.clone(),
            dynamic_view.clone(),
            Arc::new(PendingDispatch),
            LifecycleConfig::default(),
            clock.clone(),
        )
        .with_event_bus(bus as Arc<dyn RequestEventBus>),
    );
    let state = ProxyState {
        lifecycle,
        body_caps: RequestBodyCaps {
            messages: 1024,
            files: 1024,
        },
        server_state: Arc::new(ServerStateHandle::new_starting()),
        start_time: Instant::now(),
        drain_controller: DrainController::new(),
        aead: Arc::new(AeadService::from_master_key([0; 32])),
        storage: Arc::new(storage),
        dynamic_view,
        websocket_bulkheads: Arc::new(BulkheadRegistry::new()),
        websocket_bulkhead_config: BulkheadRuntimeConfig {
            max_conns_per_upstream: 50,
            semaphore_permits: 100,
            acquire_timeout: Duration::from_secs(1),
        },
        builtin_authn,
        clock,
        breaker_registry: Arc::new(BreakerRegistry::new()),
        breaker_config: BreakerRuntimeConfig::default(),
    };
    // Pause after real-clock setup so sqlite worker threads do not race
    // the deterministic middleware deadline.
    tokio::time::pause();
    let cases = [
        (None, "/v1/messages", false),
        (
            None,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            true,
        ),
        (
            None,
            "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
            true,
        ),
        (None, "/v1beta/models/gemini-2.5-flash:countTokens", true),
        (
            Some(cc_lb_domain::Provider::Gemini),
            "/v1beta/models/gemini-2.5-flash:generateContent",
            true,
        ),
        (
            Some(cc_lb_domain::Provider::Gemini),
            "/v1beta/cachedContents",
            true,
        ),
        (Some(cc_lb_domain::Provider::Openai), "/v1/responses", false),
        (
            Some(cc_lb_domain::Provider::Anthropic),
            "/v1/messages",
            false,
        ),
    ];
    for (provider, path, expect_gemini) in cases {
        let router = wrap_scoped_proxy_routes(
            Router::new().route(path, post(pending_handler)),
            state.clone(),
            RequestIdState::uuid_v7("req_server_"),
            1,
            provider,
        );
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("request-id", "timeout-request")
            .body(Body::from_stream(stream::once(panic_when_polled())))
            .expect("request builds");
        let response = router.oneshot(request).await.expect("router responds");

        assert_eq!(
            response.status(),
            StatusCode::GATEWAY_TIMEOUT,
            "{provider:?} {path}"
        );
        assert_eq!(response.headers()["request-id"], "timeout-request");
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .map(|value| value.to_str().expect("valid content type")),
            expect_gemini.then_some("application/json; charset=utf-8"),
            "{provider:?} {path}",
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("timeout body collects")
            .to_bytes();
        if expect_gemini {
            let json: Value = serde_json::from_slice(&body).expect("Gemini timeout is JSON");
            assert_eq!(
                json,
                serde_json::json!({
                    "error": {
                        "code": 504,
                        "message": "request timed out",
                        "status": "DEADLINE_EXCEEDED",
                        "type": "timeout_error",
                    }
                })
            );
        } else {
            assert_eq!(&body[..], b"request timed out", "{provider:?} {path}");
        }
        assert!(
            matches!(
                events.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "pre-authn timeout must emit no lifecycle events: {provider:?} {path}",
        );
    }

    let router = wrap_scoped_proxy_routes(
        Router::new().route("/v1/messages", post(authenticated_pending_handler)),
        state.clone(),
        RequestIdState::uuid_v7("req_server_"),
        1,
        None,
    );
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .body(Body::from_stream(stream::once(panic_when_polled())))
                .expect("request builds"),
        )
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(matches!(
        events
            .try_recv()
            .expect("post-authn timeout emits request start"),
        LifecycleEvent::RequestStarted { .. }
    ));
    let LifecycleEvent::RequestTerminated {
        client_status,
        reason,
        internal_errors,
        ..
    } = events
        .try_recv()
        .expect("post-authn timeout emits terminal event")
    else {
        panic!("expected RequestTerminated");
    };
    assert_eq!(client_status, 504);
    assert!(matches!(
        reason,
        cc_lb_lifecycle::TerminationReason::ErrorCode(code) if code == "tower_timeout"
    ));
    assert_eq!(
        internal_errors,
        vec![cc_lb_domain::InternalError {
            stage: cc_lb_domain::InternalErrorStage::Relay,
            kind: cc_lb_domain::InternalErrorKind::Timeout,
            message: Some("request timed out".to_owned()),
        }],
    );
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn proxy_timeout_middleware_does_not_rewrite_started_streaming_response() {
    async fn streaming_handler() -> Response<Body> {
        Response::new(Body::from_stream(stream::once(async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok::<_, Infallible>(Bytes::from_static(b"after deadline"))
        })))
    }

    let router = Router::new()
        .route("/v1/messages", post(streaming_handler))
        .layer(middleware::from_fn_with_state(
            ProxyTimeoutState {
                timeout: Duration::from_secs(1),
                provider: None,
            },
            proxy_timeout_middleware,
        ));
    tokio::time::pause();
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .extensions()
            .get::<cc_lb_engine::InternalFailure>()
            .is_none()
    );

    let (parts, body) = response.into_parts();
    let mut stream = body.into_data_stream();
    let first_chunk = stream.next();
    tokio::pin!(first_chunk);
    assert!(futures_util::poll!(&mut first_chunk).is_pending());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        first_chunk
            .await
            .expect("stream yields one chunk")
            .expect("chunk succeeds"),
        Bytes::from_static(b"after deadline"),
    );
    assert_eq!(parts.status, StatusCode::OK);
    assert!(
        parts
            .extensions
            .get::<cc_lb_engine::InternalFailure>()
            .is_none()
    );
}

struct WebSocketTestSignerFactory;

impl ApiKeyAwareSignerFactory for WebSocketTestSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl SignerFactory for WebSocketTestSignerFactory {
    async fn build(
        &self,
        _upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(WebSocketTestSigner))
    }
}

struct WebSocketTestSigner;

#[async_trait]
impl Signer for WebSocketTestSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

#[derive(Clone)]
struct PendingWebSocketSigner {
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
}

struct PendingWebSocketSigningDrop(Arc<tokio::sync::Notify>);

impl Drop for PendingWebSocketSigningDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl ApiKeyAwareSignerFactory for PendingWebSocketSigner {
    fn with_router_choice(&self, _upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl SignerFactory for PendingWebSocketSigner {
    async fn build(
        &self,
        _upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl Signer for PendingWebSocketSigner {
    async fn sign(
        &self,
        _shaped: ShapedRequest,
        _capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let _drop_signal = PendingWebSocketSigningDrop(self.dropped.clone());
        self.started.notify_one();
        std::future::pending().await
    }

    async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

fn websocket_upstream(id: u128, name: &str, base_url: Url) -> UpstreamRecord {
    UpstreamRecord {
        id: Uuid::from_u128(id),
        name: name.to_owned(),
        kind: UpstreamKind::OpenaiApiKey,
        base_url: Some(base_url),
        enabled: true,
        revision: 1,
        ..UpstreamRecord::default()
    }
}

async fn websocket_test_router(records: Vec<UpstreamRecord>, terminal: TerminalStrategy) -> Router {
    websocket_test_router_with_allowed_models(records, terminal, vec!["*".to_owned()]).await
}

async fn websocket_test_router_with_affinity_store(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
) -> (tempfile::TempDir, Router) {
    let dir = tempfile::TempDir::new().expect("tempdir creates");
    let db_url = format!("sqlite://{}", dir.path().join("affinity.sqlite").display());
    let state = websocket_test_state_with_affinity_store(
        records,
        terminal,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
        Some(db_url),
    )
    .await;
    (dir, proxy_router(state, 30))
}

async fn websocket_test_router_with_allowed_models(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    allowed_models: Vec<String>,
) -> Router {
    websocket_test_router_with_drain_and_allowed_models(
        records,
        terminal,
        DrainController::new(),
        allowed_models,
    )
    .await
}

async fn websocket_test_router_with_drain(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    drain_controller: DrainController,
) -> Router {
    websocket_test_router_with_drain_and_allowed_models(
        records,
        terminal,
        drain_controller,
        vec!["*".to_owned()],
    )
    .await
}

async fn websocket_test_router_with_drain_and_allowed_models(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    drain_controller: DrainController,
    allowed_models: Vec<String>,
) -> Router {
    websocket_test_router_with_signer_and_allowed_models(
        records,
        terminal,
        drain_controller,
        Arc::new(WebSocketTestSignerFactory),
        allowed_models,
    )
    .await
}

async fn websocket_test_router_with_signer(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    drain_controller: DrainController,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
) -> Router {
    websocket_test_router_with_signer_and_allowed_models(
        records,
        terminal,
        drain_controller,
        signer_factory,
        vec!["*".to_owned()],
    )
    .await
}

async fn websocket_test_router_with_signer_and_allowed_models(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    drain_controller: DrainController,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    allowed_models: Vec<String>,
) -> Router {
    let state = websocket_test_state(
        records,
        terminal,
        drain_controller,
        signer_factory,
        Arc::new(FixtureKeyStore),
        allowed_models,
    )
    .await;
    proxy_router(state, 30)
}

async fn websocket_test_state(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    drain_controller: DrainController,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    managed_key_store: Arc<dyn ManagedKeyStore>,
    allowed_models: Vec<String>,
) -> ProxyState {
    websocket_test_state_with_affinity_store(
        records,
        terminal,
        drain_controller,
        signer_factory,
        managed_key_store,
        allowed_models,
        None,
    )
    .await
}

async fn websocket_test_state_with_affinity_store(
    records: Vec<UpstreamRecord>,
    terminal: TerminalStrategy,
    drain_controller: DrainController,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    managed_key_store: Arc<dyn ManagedKeyStore>,
    allowed_models: Vec<String>,
    affinity_db_url: Option<String>,
) -> ProxyState {
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let with_affinity_store = affinity_db_url.is_some();
    let db_url = affinity_db_url.unwrap_or_else(|| "sqlite::memory:".to_owned());
    let sqlite = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&db_url, clock.clone())
            .await
            .expect("in-memory storage opens"),
    );
    if with_affinity_store {
        sqlite
            .initialize()
            .await
            .expect("affinity test storage initializes");
        for record in &records {
            let created = cc_lb_storage_api::UpstreamStore::create(
                &*sqlite,
                cc_lb_storage_api::UpstreamCreate {
                    id: record.id,
                    name: record.name.clone(),
                    kind: record.kind,
                    base_url: record.base_url.clone(),
                    api_key_ciphertext: record.api_key_ciphertext.clone(),
                    oauth_tokens: None,
                    warmup_enabled: record.warmup_enabled,
                    warmup_dialect_plugin: record.warmup_dialect_plugin.clone(),
                    workspace_id: record.workspace_id.clone(),
                    vertex: record.vertex.clone(),
                    vertex_credential_type: record.vertex_credential_type,
                    vertex_quota_project_id: record.vertex_quota_project_id.clone(),
                    api_key_access: None,
                },
            )
            .await
            .expect("seed upstream spec in affinity sqlite");
            if !record.enabled {
                cc_lb_storage_api::UpstreamStore::update(
                    &*sqlite,
                    created.id,
                    created.revision,
                    cc_lb_storage_api::UpstreamUpdate {
                        enabled: Some(false),
                        ..cc_lb_storage_api::UpstreamUpdate::default()
                    },
                )
                .await
                .expect("disable seeded upstream in affinity sqlite");
            }
        }
    }
    let storage: Arc<dyn Storage> = Arc::clone(&sqlite) as Arc<dyn Storage>;
    let builtin_authn = Arc::new(BuiltinAuthn::new(
        Arc::new(KeyStore::new(managed_key_store)),
        clock.clone(),
    ));
    let principal_view = Arc::new(PrincipalView::for_tests(
        TEST_PRINCIPAL,
        true,
        allowed_models,
        Vec::new(),
        HashMap::from([(
            TEST_PRINCIPAL.to_owned(),
            (
                Some(Arc::new(RouterPipelineCache {
                    user_filters: Vec::new(),
                    terminal,
                    instantiation_error: None,
                    provider_filters: None,
                })),
                DialectCache::Inherit,
            ),
        )]),
    ));
    let dynamic_view = Arc::new(DynamicViewHolder::new(
        cc_lb_control::dynamic_view::DynamicViewBuilder::new(0)
            .signer_factory(signer_factory)
            .principal_view(principal_view)
            .upstream_records(records)
            .build(),
    ));
    let mut lifecycle = Lifecycle::new_with_dynamic_view(
        builtin_authn.clone(),
        dynamic_view.clone(),
        make_default_dispatcher(50),
        LifecycleConfig::default(),
        clock.clone(),
    );
    if with_affinity_store {
        lifecycle = lifecycle.with_upstream_affinity_store(
            sqlite as Arc<dyn cc_lb_storage_api::UpstreamAffinityStore>,
        );
    }
    let lifecycle = Arc::new(lifecycle);
    ProxyState {
        lifecycle,
        body_caps: RequestBodyCaps {
            messages: 1024 * 1024,
            files: 1024 * 1024,
        },
        server_state: Arc::new(ServerStateHandle::new_starting()),
        start_time: Instant::now(),
        drain_controller,
        aead: Arc::new(AeadService::from_master_key([0; 32])),
        storage,
        dynamic_view,
        builtin_authn,
        clock,
        breaker_registry: Arc::new(BreakerRegistry::new()),
        breaker_config: BreakerRuntimeConfig::default(),
        websocket_bulkheads: Arc::new(BulkheadRegistry::new()),
        websocket_bulkhead_config: BulkheadRuntimeConfig {
            max_conns_per_upstream: 50,
            semaphore_permits: 100,
            acquire_timeout: Duration::from_secs(1),
        },
    }
}

fn replace_websocket_upstreams(state: &ProxyState, records: Vec<UpstreamRecord>) {
    let view = state.dynamic_view.load();
    state.dynamic_view.store(
        cc_lb_control::DynamicViewBuilder::from_view(&view)
            .upstream_records(records)
            .build(),
    );
}

async fn spawn_proxy_router(router: Router) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proxy listener binds");
    let address = listener.local_addr().expect("proxy listener has address");
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("proxy server remains healthy");
    });
    (address, task)
}

fn websocket_request(address: SocketAddr) -> http::Request<()> {
    let mut request = format!("ws://{address}/v1/responses")
        .into_client_request()
        .expect("WebSocket request builds");
    request.headers_mut().insert(
        "x-api-key",
        HeaderValue::from_str(TEST_KEY.plaintext.as_str())
            .expect("fixture key is a valid header value"),
    );
    request
}

#[test]
fn websocket_provider_message_limits_preserve_the_configured_cap_including_zero() {
    for message_cap in [0, 1, 1024 * 1024] {
        let config = responses_websocket_config(message_cap);
        assert_eq!(config.max_message_size, Some(message_cap));
        assert_eq!(config.max_frame_size, Some(message_cap));
    }
}

#[test]
fn websocket_provider_request_preserves_headers_and_refreshes_upgrade_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer provider-token"),
    );
    headers.insert(
        "openai-beta",
        HeaderValue::from_static("responses_websockets=2025-02-01"),
    );
    headers.insert(
        "x-client-metadata",
        HeaderValue::from_static("original-metadata"),
    );
    headers.insert(HOST, HeaderValue::from_static("downstream.example"));
    headers.insert(
        SEC_WEBSOCKET_KEY,
        HeaderValue::from_static("downstream-key"),
    );
    headers.insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("downstream-only"),
    );
    let request = responses_websocket_request(
        Url::parse("ws://provider.example/v1/responses").expect("provider URL parses"),
        headers,
    )
    .expect("provider handshake request builds");
    assert_eq!(request.headers()["authorization"], "Bearer provider-token");
    assert_eq!(
        request.headers()["openai-beta"],
        "responses_websockets=2025-02-01"
    );
    assert_eq!(request.headers()["x-client-metadata"], "original-metadata");
    assert_eq!(request.headers()[HOST], "provider.example");
    assert_ne!(request.headers()[SEC_WEBSOCKET_KEY], "downstream-key");
    assert!(!request.headers().contains_key(SEC_WEBSOCKET_PROTOCOL));
}

#[tokio::test]
async fn websocket_pending_send_cancellation_releases_turn_once() {
    use cc_lb_control::api_keys::limit_engine::RejectReason;
    use cc_lb_storage_api::principal::{Limit, LimitKind};

    for start_drain in [false, true] {
        let drain_controller = DrainController::new();
        let state = websocket_test_state(
            vec![websocket_upstream(
                1,
                "openai",
                Url::parse("http://127.0.0.1:1/").expect("unused provider URL parses"),
            )],
            TerminalStrategy::FirstPick,
            drain_controller.clone(),
            Arc::new(WebSocketTestSignerFactory),
            Arc::new(FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), state.clock.clone());
        let record = StoredApiKeyRecord {
            key_hash_b64: "ws-pending-send".to_owned(),
            limit_overrides: vec![
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: 10_000,
                },
                Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 0,
                    cap_micros: 1,
                },
            ],
            ..TEST_KEY.record.clone()
        };
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let reconcile = cc_lb_event_pipeline::spawn_lifecycle_limit_reconcile_subscriber(
            bus.attach_lifecycle_limit_reconcile(128),
            engine.clone(),
        );
        let lifecycle = Arc::try_unwrap(state.lifecycle)
            .unwrap_or_else(|_| panic!("private fixture lifecycle must have one owner"));
        let lifecycle = Arc::new(
            with_static_limit_subject(
                lifecycle,
                engine.clone(),
                TEST_PRINCIPAL.to_owned(),
                TEST_KEY.key_id.clone(),
                record.clone(),
            )
            .with_limit_cost_estimator(Arc::new(WebSocketTestLimitEstimator))
            .with_event_bus(bus),
        );
        let headers = websocket_request("127.0.0.1:1".parse().unwrap())
            .headers()
            .clone();
        let auth = lifecycle
            .authenticate(&headers)
            .await
            .expect("fixture authenticates");
        let mut gateway = ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1_000_000, 8);
        let auth = gateway.authenticate().await.expect("create authenticates");
        let dispatch = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-4.1","store":false,"max_output_tokens":100,"input":[]}"#,
                ),
            )
            .await
            .expect("real gateway admits the create")
            .expect("create is dispatchable");
        let candidate = &dispatch.candidates[0];
        gateway.assign_connection(
            dispatch.turn_id,
            1,
            candidate.upstream_id,
            &candidate.upstream_name,
        );
        gateway
            .validate_send(dispatch.turn_id)
            .await
            .expect("real send validation succeeds");
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);
        let view = state.dynamic_view.load();
        assert!(
            matches!(
                engine.reserve(
                    &view.principal_view,
                    &record,
                    TEST_PRINCIPAL,
                    "gpt-4.1",
                    Some(0),
                    0,
                    Some(1_000),
                ),
                Err(RejectReason::CostRateLimit)
            ),
            "the admitted turn must hold its real cost reservation"
        );
        let tokens_before = engine
            .headers_for(&record.key_hash_b64, TEST_PRINCIPAL)
            .into_iter()
            .find(|(name, _)| name == "anthropic-ratelimit-tokens-remaining")
            .expect("token quota is observable")
            .1
            .parse::<i64>()
            .expect("remaining token quota is numeric");
        assert!(
            tokens_before < 10_000,
            "the admitted turn must reserve token quota"
        );

        let started = tokio::sync::Notify::new();
        let attempts = AtomicU64::new(0);
        let completed = AtomicU64::new(0);
        let dropped = Arc::new(AtomicU64::new(0));
        let client_closed = CancellationToken::new();
        let pending_send = async {
            let _drop_signal = PendingWebSocketSendDrop(dropped.clone());
            attempts.fetch_add(1, Ordering::SeqCst);
            started.notify_one();
            std::future::pending::<()>().await;
            completed.fetch_add(1, Ordering::SeqCst);
            let _ = gateway.sent(dispatch.turn_id);
        };
        let cancel = async {
            started.notified().await;
            if start_drain {
                drain_controller.trigger();
            } else {
                client_closed.cancel();
            }
        };
        let (draining, ()) = tokio::join!(
            run_responses_websocket_until_cancelled(
                pending_send,
                &client_closed,
                &drain_controller,
            ),
            cancel,
        );
        assert_eq!(draining, start_drain);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "no send retry or replay"
        );
        assert_eq!(
            completed.load(Ordering::SeqCst),
            0,
            "the cancelled write never completes"
        );
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "pending send is dropped exactly once"
        );
        if draining {
            gateway.drain();
        } else {
            gateway.close();
        }
        gateway.close();
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
        reconcile.shutdown().await;
        let mut started_ids = Vec::new();
        let mut terminated_ids = Vec::new();
        let mut reservation_ids = Vec::new();
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::RequestStarted { event_id, .. } => started_ids.push(event_id),
                LifecycleEvent::RequestTerminated { event_id, .. } => terminated_ids.push(event_id),
                LifecycleEvent::LimitDecision {
                    decision: cc_lb_lifecycle::LimitDecisionKind::Reserved { reservation_id, .. },
                    ..
                } => reservation_ids.push(reservation_id),
                _ => {}
            }
        }
        assert_eq!(started_ids.len(), 1);
        assert_eq!(
            terminated_ids, started_ids,
            "the one admitted turn terminates exactly once"
        );
        assert_eq!(reservation_ids.len(), 1);
        assert!(!engine.refund_by_id(&reservation_ids[0]));
        assert!(!engine.reconcile_by_id(&reservation_ids[0], 0, 0, Some(0)));
        let headers = engine.headers_for(&record.key_hash_b64, TEST_PRINCIPAL);
        assert!(
            headers.iter().any(|(name, value)| {
                name == "anthropic-ratelimit-tokens-remaining" && value == "10000"
            }),
            "all reserved token quota must be refunded"
        );
        let full_budget = engine
            .reserve(
                &view.principal_view,
                &record,
                TEST_PRINCIPAL,
                "gpt-4.1",
                Some(0),
                0,
                Some(1_000),
            )
            .expect("all cost quota and concurrency capacity must be restored");
        drop(full_budget);
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
    }
}

#[tokio::test]
async fn websocket_pre_auth_drain_is_silent_and_never_authenticates_or_connects() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let drain_controller = DrainController::new();
    let lookups = Arc::new(AtomicU64::new(0));
    let body_polls = Arc::new(AtomicU64::new(0));
    let mut state = websocket_test_state(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain_controller.clone(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(CountingFixtureKeyStore {
            lookups: lookups.clone(),
        }),
        vec!["*".to_owned()],
    )
    .await;
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let lifecycle = Arc::try_unwrap(state.lifecycle)
        .unwrap_or_else(|_| panic!("private fixture lifecycle must have one owner"));
    state.lifecycle = Arc::new(lifecycle.with_event_bus(bus));
    let observed_body_polls = body_polls.clone();
    let router = proxy_router(state, 30).layer(middleware::from_fn(
        move |mut request: Request<Body>, next: Next| {
            let body_polls = observed_body_polls.clone();
            async move {
                *request.body_mut() =
                    Body::from_stream(stream::once(counted_unexpected_body_poll(body_polls)));
                next.run(request).await
            }
        },
    ));
    drain_controller.trigger();
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let error = connect_async(websocket_request(proxy_address))
        .await
        .expect_err("pre-auth drain must reject the real WebSocket upgrade");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("pre-auth drain must return an HTTP response");
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[RETRY_AFTER], "60");
    assert_eq!(
        lookups.load(Ordering::SeqCst),
        0,
        "authentication store must stay untouched"
    );
    assert_eq!(
        body_polls.load(Ordering::SeqCst),
        0,
        "request body must remain unpolled"
    );
    assert_eq!(drain_controller.in_flight(), 0);
    assert!(
        listener.accept().now_or_never().is_none(),
        "provider TCP connections must stay zero"
    );
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "pre-auth drain must emit no lifecycle rows"
    );
    proxy_task.abort();
}

#[tokio::test]
async fn websocket_client_close_or_drain_cancels_pending_signing() {
    for start_drain in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("provider listener binds");
        let address = listener
            .local_addr()
            .expect("provider address is available");
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let drain_controller = DrainController::new();
        let router = websocket_test_router_with_signer(
            vec![websocket_upstream(
                1,
                "openai",
                Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
            )],
            TerminalStrategy::FirstPick,
            drain_controller.clone(),
            Arc::new(PendingWebSocketSigner {
                started: started.clone(),
                dropped: dropped.clone(),
            }),
        )
        .await;
        let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
        let (mut client, response) = connect_async(websocket_request(proxy_address))
            .await
            .expect("downstream handshake succeeds before signing");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("client create sends");
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("the provider signing future starts");
        if start_drain {
            drain_controller.trigger();
        } else {
            client.close(None).await.expect("client close sends");
        }
        tokio::time::timeout(Duration::from_secs(5), dropped.notified())
            .await
            .expect("client closure or drain drops pending provider signing");
        assert!(
            !drain_controller.await_drained(Duration::from_secs(5)).await,
            "the session must release its drain guard after signing cancellation",
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "cancelled pending signing must not open a provider connection",
        );
        let _ = client.close(None).await;
        proxy_task.abort();
    }
}

#[tokio::test]
async fn websocket_client_close_or_drain_cancels_a_pending_provider_handshake() {
    use tokio::io::AsyncReadExt as _;

    for start_drain in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("upstream listener binds");
        let address = listener
            .local_addr()
            .expect("upstream address is available");
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let provider_task = tokio::spawn(async move {
            let (mut socket, _) = listener
                .accept()
                .await
                .expect("provider TCP connection arrives");
            let _ = accepted_tx.send(());
            // Never answer the provider handshake: client closure or drain
            // must close this TCP socket without waiting for its response.
            let mut buffer = [0_u8; 1024];
            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        let drain_controller = DrainController::new();
        let router = websocket_test_router_with_drain(
            vec![websocket_upstream(
                1,
                "openai",
                Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
            )],
            TerminalStrategy::FirstPick,
            drain_controller.clone(),
        )
        .await;
        let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
        let (mut client, _) = connect_async(websocket_request(proxy_address))
            .await
            .expect("downstream handshake succeeds");
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("client create sends");
        tokio::time::timeout(Duration::from_secs(5), accepted_rx)
            .await
            .expect("provider connection starts")
            .expect("provider connection signal arrives");
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","stream_id":"queued","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("a second client frame fits the bounded reader queue");
        if start_drain {
            drain_controller.trigger();
        } else {
            client.close(None).await.expect("client close sends");
        }
        tokio::time::timeout(Duration::from_secs(5), provider_task)
            .await
            .expect("close or drain cancels the stalled provider handshake")
            .expect("provider task completes");
        assert!(
            !drain_controller.await_drained(Duration::from_secs(5)).await,
            "the WebSocket drain guard must be released",
        );
        let _ = client.close(None).await;
        proxy_task.abort();
    }
}

#[tokio::test]
async fn websocket_drain_closes_an_idle_downstream_without_connecting_a_provider() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let drain_controller = DrainController::new();
    let router = websocket_test_router_with_drain(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain_controller.clone(),
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("idle downstream handshake succeeds");
    assert_eq!(drain_controller.in_flight(), 1);
    drain_controller.trigger();
    let ended = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("drain closes the idle downstream");
    assert!(matches!(
        ended,
        Some(Ok(UpstreamMessage::Close(_))) | Some(Err(_)) | None
    ));
    assert!(
        !drain_controller.await_drained(Duration::from_secs(5)).await,
        "idle WebSocket drain guard must be released",
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "idle drain must not open a provider connection",
    );
    proxy_task.abort();
}

#[tokio::test]
async fn websocket_codex_provider_receives_raw_query_for_native_and_custom_paths() {
    const QUERY: &str = "trace=a%2Fb&repeat=one&repeat=two&empty=&space=a+b&encoded=%252F";
    for (base_suffix, expected_path) in [
        ("/backend-api/codex/", "/backend-api/codex/responses"),
        ("/codex-proxy/?stale=1", "/codex-proxy/responses"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("provider listener binds");
        let address = listener
            .local_addr()
            .expect("provider address is available");
        let (target_tx, target_rx) = tokio::sync::oneshot::channel();
        let provider_task = tokio::spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("provider connection arrives");
            let mut websocket = tokio_tungstenite::accept_hdr_async(
                socket,
                CaptureHandshakeTarget {
                    sender: Some(target_tx),
                },
            )
            .await
            .expect("provider handshake succeeds");
            let create = websocket
                .next()
                .await
                .expect("provider receives create")
                .expect("provider create decodes");
            assert!(
                create
                    .to_text()
                    .expect("create is text")
                    .contains("response.create"),
            );
            let _ = websocket.close(None).await;
        });
        let mut record = websocket_upstream(
            1,
            "codex",
            Url::parse(&format!("http://{address}{base_suffix}"))
                .expect("Codex provider base parses"),
        );
        record.kind = UpstreamKind::OpenaiOauth;
        let router = websocket_test_router(vec![record], TerminalStrategy::FirstPick).await;
        let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
        let mut request = websocket_request(proxy_address);
        *request.uri_mut() = format!("ws://{proxy_address}/v1/responses?{QUERY}")
            .parse()
            .expect("downstream request target parses");
        let (mut client, response) = connect_async(request)
            .await
            .expect("downstream handshake succeeds");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("client create sends");
        let target = tokio::time::timeout(Duration::from_secs(5), target_rx)
            .await
            .expect("provider handshake arrives")
            .expect("provider target is captured");
        assert_eq!(target, format!("{expected_path}?{QUERY}"));
        tokio::time::timeout(Duration::from_secs(5), provider_task)
            .await
            .expect("provider receives the create")
            .expect("provider task completes");
        let _ = client.close(None).await;
        proxy_task.abort();
    }
}

#[tokio::test]
async fn websocket_oauth_codex_frames_preserve_lanes_and_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("address available");
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let provider = tokio::spawn(async move {
        // Keep every lane socket open until the client is done, so a
        // provider hang-up cannot interleave close notices with lane frames.
        let mut sockets = Vec::new();
        for _lane in ["a", "b"] {
            let (socket, _) = listener.accept().await.expect("provider accepts");
            let mut ws = tokio_tungstenite::accept_async(socket)
                .await
                .expect("provider handshake");
            let frame = ws
                .next()
                .await
                .expect("provider frame")
                .expect("frame decodes");
            let value: Value = serde_json::from_slice(&frame.into_data()).expect("JSON frame");
            assert!(value.get("stream_id").is_none());
            ws.send(UpstreamMessage::Text(
                r#"{"type":"response.output_text.delta","delta":"ok"}"#.into(),
            ))
            .await
            .expect("provider response sends");
            ws.send(UpstreamMessage::Text(
                r#"{"type":"response.metadata","x":1}"#.into(),
            ))
            .await
            .expect("provider metadata sends");
            sockets.push(ws);
        }
        let _ = done_rx.await;
        drop(sockets);
    });
    let mut record = websocket_upstream(
        1,
        "oauth",
        Url::parse(&format!("http://{address}/")).unwrap(),
    );
    record.kind = UpstreamKind::OpenaiOauth;
    let router = websocket_test_router(vec![record], TerminalStrategy::FirstPick).await;
    let (proxy, task) = spawn_proxy_router(router).await;
    let mut request = websocket_request(proxy);
    *request.uri_mut() = format!("ws://{proxy}/openai/v1/responses").parse().unwrap();
    let (mut client, _) = connect_async(request).await.expect("client handshake");
    for lane in ["a", "b"] {
        client.send(UpstreamMessage::Text(
            format!(r#"{{"type":"response.create","stream_id":"{lane}","model":"gpt-4.1","input":[],"store":false,"stream":true}}"#).into(),
        )).await.expect("client frame sends");
        let response = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("response arrives")
            .expect("response exists")
            .expect("response decodes");
        let value: Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
        assert_eq!(value["stream_id"], lane);
        assert_eq!(value["type"], "response.output_text.delta");
        let metadata = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("metadata arrives")
            .expect("metadata exists")
            .expect("metadata decodes");
        let metadata: Value = serde_json::from_str(metadata.to_text().unwrap()).unwrap();
        assert_eq!(metadata["type"], "response.metadata");
    }
    let _ = done_tx.send(());
    provider.await.expect("provider task");
    let _ = client.close(None).await;
    task.abort();
}

#[tokio::test]
async fn websocket_oauth_cross_lane_previous_response_is_rejected_without_provider_contact() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("address available");
    let provider = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("provider accepts");
        let mut ws = tokio_tungstenite::accept_async(socket)
            .await
            .expect("provider handshake");
        let first = ws
            .next()
            .await
            .expect("first frame")
            .expect("first frame decodes");
        assert!(!first.to_text().unwrap().contains("stream_id"));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            ws.next().now_or_never().is_none(),
            "rejected frame reaches no provider"
        );
    });
    let mut record = websocket_upstream(
        1,
        "oauth",
        Url::parse(&format!("http://{address}/")).unwrap(),
    );
    record.kind = UpstreamKind::OpenaiOauth;
    let router = websocket_test_router(vec![record], TerminalStrategy::FirstPick).await;
    let (proxy, task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy)).await.unwrap();
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","stream_id":"a","model":"gpt-4.1","input":[]}"#.into(),
        ))
        .await
        .unwrap();
    client.send(UpstreamMessage::Text(
        r#"{"type":"response.create","stream_id":"b","previous_response_id":"a","model":"gpt-4.1","input":[]}"#.into(),
    )).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let error: Value = serde_json::from_str(error.to_text().unwrap()).unwrap();
    assert_eq!(error["error"]["code"], "previous_response_not_found");
    assert_eq!(error["status"], 400);
    provider.await.unwrap();
    let _ = client.close(None).await;
    task.abort();
}

#[tokio::test]
async fn websocket_oauth_reconnect_previous_response_returns_400_with_anthropic_upstream_and_codex_backend()
 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("address available");

    // 1. Public /openai/v1/responses with OpenaiOauth + AnthropicApiKey
    let mut oauth = websocket_upstream(
        1,
        "oauth",
        Url::parse(&format!("http://{address}/")).expect("provider URL"),
    );
    oauth.kind = UpstreamKind::OpenaiOauth;
    let mut anthropic = websocket_upstream(
        2,
        "anthropic",
        Url::parse(&format!("http://{address}/")).expect("provider URL"),
    );
    anthropic.kind = UpstreamKind::AnthropicApiKey;
    let (_public_dir, public_router) = websocket_test_router_with_affinity_store(
        vec![oauth.clone(), anthropic.clone()],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (public_proxy, public_task) = spawn_proxy_router(public_router).await;
    let mut public_request = websocket_request(public_proxy);
    *public_request.uri_mut() = format!("ws://{public_proxy}/openai/v1/responses")
        .parse()
        .expect("public WS URI parses");
    let (mut public_client, _) = connect_async(public_request)
        .await
        .expect("public handshake succeeds");
    public_client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"previous_response_id":"resp_closed","input":[]}"#.into(),
        ))
        .await
        .expect("public create sends");
    let public_error = tokio::time::timeout(Duration::from_secs(5), public_client.next())
        .await
        .expect("public error arrives")
        .expect("public frame exists")
        .expect("public frame decodes");
    let public_error: Value = serde_json::from_str(public_error.to_text().expect("error is text"))
        .expect("error is JSON");
    assert_eq!(public_error["status"], 400);
    assert_eq!(public_error["error"]["code"], "previous_response_not_found");
    let _ = public_client.close(None).await;
    public_task.abort();

    // 2. Native /openai/backend-api/codex/responses with OpenaiOauth + OpenaiApiKey + AnthropicApiKey
    let mut openai_api_key = websocket_upstream(
        3,
        "openai-api-key",
        Url::parse(&format!("http://{address}/")).expect("provider URL"),
    );
    openai_api_key.kind = UpstreamKind::OpenaiApiKey;
    let (_codex_dir, codex_router) = websocket_test_router_with_affinity_store(
        vec![oauth, openai_api_key, anthropic],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (codex_proxy, codex_task) = spawn_proxy_router(codex_router).await;
    let mut codex_request = websocket_request(codex_proxy);
    *codex_request.uri_mut() = format!("ws://{codex_proxy}/openai/backend-api/codex/responses")
        .parse()
        .expect("codex WS URI parses");
    let (mut codex_client, _) = connect_async(codex_request)
        .await
        .expect("codex handshake succeeds");
    codex_client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"previous_response_id":"resp_closed","input":[]}"#.into(),
        ))
        .await
        .expect("codex create sends");
    let codex_error = tokio::time::timeout(Duration::from_secs(5), codex_client.next())
        .await
        .expect("codex error arrives")
        .expect("codex frame exists")
        .expect("codex frame decodes");
    let codex_error: Value =
        serde_json::from_str(codex_error.to_text().expect("error is text")).expect("error is JSON");
    assert_eq!(codex_error["status"], 400);
    assert_eq!(codex_error["error"]["code"], "previous_response_not_found");
    let _ = codex_client.close(None).await;
    codex_task.abort();

    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "rejected previous_response_not_found turns must not contact any upstream",
    );
}

#[tokio::test]
async fn websocket_oauth_public_create_adds_store_false_and_strips_lane() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("address available");
    let (frame_tx, frame_rx) = tokio::sync::oneshot::channel();
    let provider_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("provider accepts");
        let mut websocket = tokio_tungstenite::accept_async(socket)
            .await
            .expect("provider handshake");
        let frame = websocket
            .next()
            .await
            .expect("provider frame")
            .expect("frame ok");
        let _ = frame_tx.send(frame.into_data());
        websocket
            .send(UpstreamMessage::Text(
                r#"{"type":"response.metadata","x":1}"#.into(),
            ))
            .await
            .expect("provider metadata sends");
    });
    let mut record = websocket_upstream(
        1,
        "oauth",
        Url::parse(&format!("http://{address}/")).expect("provider URL"),
    );
    record.kind = UpstreamKind::OpenaiOauth;
    let router = websocket_test_router(vec![record], TerminalStrategy::FirstPick).await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","input":"hello","stream_id":"lane"}"#
                .into(),
        ))
        .await
        .expect("create sends");
    let frame = tokio::time::timeout(Duration::from_secs(5), frame_rx)
        .await
        .expect("provider receives frame")
        .expect("frame channel");
    let body: serde_json::Value = serde_json::from_slice(&frame).expect("JSON frame");
    assert_eq!(body["store"], false);
    assert!(body.get("stream_id").is_none());
    // The provider's response.metadata frame is not delivered to a
    // public-shape client; any frame that does arrive (for example the
    // close notice when the fake provider hangs up) must not be it.
    if let Ok(Some(Ok(message))) =
        tokio::time::timeout(Duration::from_millis(250), client.next()).await
        && let Ok(text) = message.to_text()
    {
        assert!(!text.contains("response.metadata"), "{text}");
    }
    provider_task.await.expect("provider task");
    let _ = client.close(None).await;
    proxy_task.abort();
}

#[tokio::test]
async fn websocket_handshake_and_invalid_client_frames_do_not_connect_upstream() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("upstream listener binds");
    let address = listener
        .local_addr()
        .expect("upstream address is available");
    let router = websocket_test_router(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, response) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds without provider connection");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "downstream upgrade must not connect a provider",
    );
    client
        .send(UpstreamMessage::Text("not-json".into()))
        .await
        .expect("malformed client message sends");
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("protocol error arrives")
        .expect("protocol error keeps WebSocket open")
        .expect("protocol error decodes");
    let error: Value = serde_json::from_str(error.to_text().expect("error is text"))
        .expect("protocol error is JSON");
    assert_eq!(error["error"]["code"], "invalid_json");
    client
        .send(UpstreamMessage::Binary(Bytes::from_static(b"unsupported")))
        .await
        .expect("binary client message sends");
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("binary error arrives")
        .expect("binary error keeps WebSocket open")
        .expect("binary error decodes");
    let error: Value = serde_json::from_str(error.to_text().expect("error is text"))
        .expect("binary error is JSON");
    assert_eq!(error["error"]["code"], "invalid_request_error");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "rejected client messages must not connect a provider",
    );
    let _ = client.close(None).await;
    proxy_task.abort();
}

async fn spawn_holding_upstream() -> (SocketAddr, CancellationToken, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("upstream listener binds");
    let address = listener
        .local_addr()
        .expect("upstream listener has address");
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = task_cancellation.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else {
                        break;
                    };
                    let connection_cancellation = task_cancellation.clone();
                    tokio::spawn(async move {
                        let Ok(mut websocket) = accept_async(stream).await else {
                            return;
                        };
                        if !matches!(websocket.next().await, Some(Ok(UpstreamMessage::Text(_)))) {
                            return;
                        }
                        let created = r#"{"type":"response.created","response":{"id":"holding","store":false}}"#;
                        if websocket.send(UpstreamMessage::Text(created.into())).await.is_err() {
                            return;
                        }
                        connection_cancellation.cancelled().await;
                        let _ = websocket.close(None).await;
                    });
                }
            }
        }
    });
    (address, cancellation, task)
}

/// WS-RED-02: all WebSocket connections for one upstream share the
/// configured bulkhead, so a full upstream is rejected with overload
/// semantics before another upstream connection is opened.
#[tokio::test]
async fn websocket_connection_uses_shared_upstream_bulkhead() {
    const MAX_CONNECTIONS: usize = 50;
    let (upstream_address, upstream_cancel, upstream_task) = spawn_holding_upstream().await;
    let router = websocket_test_router(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{upstream_address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let mut clients = Vec::with_capacity(MAX_CONNECTIONS);
    for _ in 0..MAX_CONNECTIONS {
        let connected = tokio::time::timeout(
            Duration::from_secs(5),
            connect_async(websocket_request(proxy_address)),
        )
        .await
        .expect("bulkhead slot connection should not time out")
        .expect("configured upstream should accept the connection");
        let mut client = connected.0;
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("client create sends");
        let created = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("provider admission does not time out")
            .expect("provider create event arrives")
            .expect("provider create event decodes");
        assert!(
            created
                .to_text()
                .expect("event is text")
                .contains("response.created")
        );
        clients.push(client);
    }

    let (mut client, handshake) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake does not consume an upstream slot");
    assert_eq!(handshake.status(), StatusCode::SWITCHING_PROTOCOLS);
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("overloaded client create sends");
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("bulkhead error arrives")
        .expect("WebSocket remains open for overload error")
        .expect("overload error decodes");
    let error: Value = serde_json::from_str(error.to_text().expect("error is text"))
        .expect("overload error is JSON");
    assert_eq!(error["status"], 503);
    assert_eq!(error["error"]["code"], "overloaded_error");
    let _ = client.close(None).await;

    upstream_cancel.cancel();
    for mut client in clients {
        let _ = client.close(None).await;
    }
    proxy_task.abort();
    upstream_task.abort();
}

/// WS-RED-03: a failed first connection must continue with the next
/// eligible OpenAI upstream and preserve the normal relay lifecycle.
#[tokio::test]
async fn websocket_connection_fails_over_to_next_eligible_upstream() {
    let first_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("first upstream listener binds");
    let first_address = first_listener
        .local_addr()
        .expect("first upstream listener has address");
    let first_task = tokio::spawn(async move {
        let Ok((stream, _)) = first_listener.accept().await else {
            return;
        };
        drop(stream);
    });

    let second_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second upstream listener binds");
    let second_address = second_listener
        .local_addr()
        .expect("second upstream listener has address");
    let second_task = tokio::spawn(async move {
        let (stream, _) = second_listener
            .accept()
            .await
            .expect("fallback connection arrives");
        let mut websocket = accept_async(stream)
            .await
            .expect("fallback WebSocket handshake succeeds");
        let create = websocket
            .next()
            .await
            .expect("fallback receives create")
            .expect("create decodes");
        assert!(
            create
                .to_text()
                .expect("create is text")
                .contains("response.create")
        );
        websocket
            .send(UpstreamMessage::Text(
                r#"{"type":"response.created","response":{"id":"fallback","store":false}}"#.into(),
            ))
            .await
            .expect("fallback upstream sends a relay frame");
        while websocket.next().await.is_some() {}
    });

    let router = websocket_test_router(
        vec![
            websocket_upstream(
                1,
                "first-openai",
                Url::parse(&format!("http://{first_address}/")).expect("first upstream URL parses"),
            ),
            websocket_upstream(
                2,
                "second-openai",
                Url::parse(&format!("http://{second_address}/"))
                    .expect("second upstream URL parses"),
            ),
        ],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(websocket_request(proxy_address)),
    )
    .await
    .expect("WebSocket failover should not time out")
    .expect("the next eligible upstream should complete the handshake");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("client create sends");
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("relayed frame should not time out")
        .expect("fallback relay should remain open")
        .expect("fallback relay frame should decode");
    assert_eq!(
        message,
        UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"fallback","store":false}}"#.into(),
        ),
    );
    client.close(None).await.expect("client close succeeds");

    let _ = tokio::time::timeout(Duration::from_secs(5), second_task)
        .await
        .expect("fallback upstream observes the close");
    let _ = first_task.await;
    proxy_task.abort();
}

#[tokio::test]
async fn websocket_connection_limit_preserves_native_error_without_generated_duplicates() {
    const NATIVE_ERROR: &str = r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"websocket_connection_limit_reached","message":"Maximum connection duration reached","param":null}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let provider_task = tokio::spawn(async move {
        let (socket, _) = listener
            .accept()
            .await
            .expect("provider connection arrives");
        let mut websocket = accept_async(socket)
            .await
            .expect("provider handshake succeeds");
        let _ = websocket
            .next()
            .await
            .expect("provider receives create")
            .expect("provider create decodes");
        websocket
            .send(UpstreamMessage::Text(NATIVE_ERROR.into()))
            .await
            .expect("provider sends native connection-limit error");
        while websocket.next().await.is_some() {}
    });
    let router = websocket_test_router(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("client create sends");
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("native error arrives")
        .expect("native error keeps downstream open")
        .expect("native error decodes");
    assert_eq!(error, UpstreamMessage::Text(NATIVE_ERROR.into()));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client.next())
            .await
            .is_err(),
        "native connection-limit error must not be followed by generated wire errors",
    );
    tokio::time::timeout(Duration::from_secs(5), provider_task)
        .await
        .expect("the failed provider connection closes")
        .expect("provider task completes");
    let _ = client.close(None).await;
    proxy_task.abort();
}

async fn ws1296_assert_client_close_echo(code: u16, completed_turn: bool) {
    use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let router = websocket_test_router(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds");
    let provider_task = if completed_turn {
        let task = tokio::spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("provider connection arrives");
            let mut websocket = accept_async(socket)
                .await
                .expect("provider handshake succeeds");
            websocket
                .next()
                .await
                .expect("provider receives create")
                .expect("create decodes");
            for event in [
                r#"{"type":"response.created","response":{"id":"resp-close","store":false}}"#,
                r#"{"type":"response.completed","response":{"id":"resp-close","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#,
            ] {
                websocket
                    .send(UpstreamMessage::Text(event.into()))
                    .await
                    .expect("provider event sends");
            }
            while websocket.next().await.is_some() {}
        });
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("client create sends");
        for kind in ["response.created", "response.completed"] {
            let event = tokio::time::timeout(Duration::from_secs(5), client.next())
                .await
                .expect("turn event arrives")
                .expect("downstream remains open")
                .expect("turn event decodes");
            let event: Value = serde_json::from_str(event.to_text().expect("event is text"))
                .expect("event is JSON");
            assert_eq!(event["type"], kind);
        }
        Some(task)
    } else {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "an idle downstream must not connect a provider",
        );
        None
    };
    client
        .close(Some(CloseFrame {
            code: CloseCode::from(code),
            reason: "".into(),
        }))
        .await
        .expect("client close sends");
    let reply = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("close acknowledgement arrives")
        .expect("close acknowledgement must be a frame, not EOF")
        .expect("close handshake must not reset the connection");
    assert_eq!(
        reply,
        UpstreamMessage::Close(Some(CloseFrame {
            code: CloseCode::from(code),
            reason: "".into(),
        })),
        "the peer must echo the client's close code",
    );
    if let Some(task) = provider_task {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("provider connection retires on client close")
            .expect("provider task completes");
    }
    proxy_task.abort();
}

#[tokio::test]
async fn ws1296_idle_client_close_echoes_1000_and_1001() {
    for code in [1000, 1001] {
        ws1296_assert_client_close_echo(code, false).await;
    }
}

#[tokio::test]
async fn ws1296_completed_turn_client_close_echoes_1000_and_1001() {
    for code in [1000, 1001] {
        ws1296_assert_client_close_echo(code, true).await;
    }
}

#[tokio::test]
async fn ws1296_peer_close_flush_discards_cancelled_send_slot() {
    use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

    let (queued_tx, mut queued_rx) = tokio::sync::mpsc::channel(1);
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel(1);
    let router = Router::new().route(
        "/ws",
        get(move |upgrade: WebSocketUpgrade| {
            let queued_tx = queued_tx.clone();
            let closed_tx = closed_tx.clone();
            async move {
                upgrade.on_upgrade(move |socket| async move {
                    let (mut sink, mut stream) = socket.split();
                    // feed fills the real futures SplitSink slot without
                    // flushing it, reproducing a cancelled send's state.
                    sink.feed(Message::Text("must-not-follow-peer-close".into()))
                        .await
                        .expect("application message enters the split sink slot");
                    queued_tx.send(()).await.expect("queued slot is signalled");
                    let peer_close = stream
                        .next()
                        .await
                        .expect("peer close arrives")
                        .expect("peer close decodes");
                    assert!(matches!(
                        peer_close,
                        Message::Close(Some(frame)) if frame.code == 1000
                    ));
                    flush_responses_peer_close(
                        sink,
                        stream,
                        tokio::time::Instant::now() + Duration::from_secs(1),
                    )
                    .await;
                    closed_tx.send(()).await.expect("shutdown is signalled");
                })
            }
        }),
    );
    let (address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(format!("ws://{address}/ws"))
        .await
        .expect("downstream handshake succeeds");
    tokio::time::timeout(Duration::from_secs(5), queued_rx.recv())
        .await
        .expect("split sink slot is populated")
        .expect("slot signal arrives");
    client
        .close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        }))
        .await
        .expect("client close sends");
    let reply = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("close acknowledgement arrives")
        .expect("close acknowledgement must not be EOF")
        .expect("retained application message must not prevent the close reply");
    assert_eq!(
        reply,
        UpstreamMessage::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })),
        "flush the queued automatic close reply, not the cancelled application send",
    );
    tokio::time::timeout(Duration::from_secs(5), closed_rx.recv())
        .await
        .expect("close cleanup completes")
        .expect("close cleanup signal arrives");
    proxy_task.abort();
}

#[tokio::test]
async fn ws1296_drain_shutdown_rechecks_peer_close_before_dropping_reader() {
    use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

    let router = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async move {
            upgrade.on_upgrade(|socket| async move {
                let (sink, mut stream) = socket.split();
                let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
                let reader = WsDownstreamReader {
                    task: tokio::spawn(async move {
                        let close = stream.next().await.unwrap().unwrap();
                        assert!(matches!(close, Message::Close(Some(frame)) if frame.code == 1000));
                        observed_tx.send(()).unwrap();
                        (true, stream)
                    }),
                };
                // The drain branch has already been selected. The reader
                // consumes Close before its shutdown send reaches the socket.
                observed_rx.await.unwrap();
                assert!(reader.task.is_finished());
                close_responses_downstream(sink, reader).await;
            })
        }),
    );
    let (address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(format!("ws://{address}/ws")).await.unwrap();
    client
        .close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        }))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("drain close acknowledgement arrives")
        .expect("drain must not discard the reader's close reply")
        .expect("drain must complete the peer close handshake");
    assert!(
        matches!(reply, UpstreamMessage::Close(Some(frame)) if frame.code == CloseCode::Normal)
    );
    proxy_task.abort();
}

#[tokio::test]
async fn ws1296_eof_and_read_error_do_not_synthesize_normal_close() {
    use tokio::io::AsyncWriteExt as _;

    for malformed_frame in [false, true] {
        let router = websocket_test_router(Vec::new(), TerminalStrategy::FirstPick).await;
        let (address, proxy_task) = spawn_proxy_router(router).await;
        let (mut client, _) = connect_async(websocket_request(address))
            .await
            .expect("downstream handshake succeeds");
        if malformed_frame {
            // A client data frame must be masked; this produces a read error.
            client.get_mut().write_all(&[0x81, 0x00]).await.unwrap();
        } else {
            client.get_mut().shutdown().await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("abnormal client termination releases the socket");
        assert!(
            !matches!(&result, Some(Ok(UpstreamMessage::Close(Some(frame))))
                if u16::from(frame.code) == 1000),
            "EOF and protocol errors must not be acknowledged as normal close"
        );
        proxy_task.abort();
    }
}

#[tokio::test]
async fn ws1296_unattributed_native_error_retires_two_named_lanes_without_duplicates() {
    const NATIVE_ERROR: &str = r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"Unsupported parameter: stream_id"}}"#;
    const RECOVERED: &str = r#"{"type":"response.completed","stream_id":"a","response":{"id":"resp-recovered","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let (retired_tx, retired_rx) = tokio::sync::oneshot::channel();
    let provider_task = tokio::spawn(async move {
        let (socket, _) = listener
            .accept()
            .await
            .expect("first provider connection arrives");
        let mut websocket = accept_async(socket)
            .await
            .expect("provider handshake succeeds");
        for lane in ["a", "b"] {
            let create = websocket
                .next()
                .await
                .expect("named create arrives")
                .expect("create decodes");
            let create: Value = serde_json::from_str(create.to_text().expect("create is text"))
                .expect("create is JSON");
            assert_eq!(create["stream_id"], lane);
            let created = serde_json::json!({
                "type": "response.created", "stream_id": lane,
                "response": {"id": format!("resp-{lane}"), "store": false},
            });
            websocket
                .send(UpstreamMessage::Text(created.to_string().into()))
                .await
                .expect("created sends");
        }
        websocket
            .send(UpstreamMessage::Text(NATIVE_ERROR.into()))
            .await
            .expect("native error sends");
        let retired = tokio::time::timeout(Duration::from_secs(5), websocket.next())
            .await
            .expect("unattributed error must retire the provider connection");
        assert!(
            matches!(
                retired,
                None | Some(Err(_)) | Some(Ok(UpstreamMessage::Close(_)))
            ),
            "no create may be replayed or sent on the failed provider connection",
        );
        retired_tx.send(()).expect("retirement is observed");
        let (socket, _) = listener
            .accept()
            .await
            .expect("new turn opens a fresh provider connection");
        let mut websocket = accept_async(socket)
            .await
            .expect("recovery handshake succeeds");
        let create = websocket
            .next()
            .await
            .expect("recovery create arrives")
            .expect("create decodes");
        let create: Value = serde_json::from_str(create.to_text().expect("create is text"))
            .expect("create is JSON");
        assert_eq!(create["stream_id"], "a");
        websocket.send(UpstreamMessage::Text(
            r#"{"type":"response.created","stream_id":"a","response":{"id":"resp-recovered","store":false}}"#.into(),
        )).await.expect("recovery created sends");
        websocket
            .send(UpstreamMessage::Text(RECOVERED.into()))
            .await
            .expect("recovery completion sends");
        while websocket.next().await.is_some() {}
    });
    let router = websocket_test_router(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds");
    for lane in ["a", "b"] {
        let create = serde_json::json!({
            "type": "response.create", "stream_id": lane,
            "model": "gpt-4.1", "store": false, "input": [],
        });
        client
            .send(UpstreamMessage::Text(create.to_string().into()))
            .await
            .expect("named create sends");
        let created = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("created arrives")
            .expect("downstream stays open")
            .expect("created decodes");
        let created: Value = serde_json::from_str(created.to_text().expect("created is text"))
            .expect("created is JSON");
        assert_eq!(created["type"], "response.created");
        assert_eq!(created["stream_id"], lane);
    }
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("native error arrives")
        .expect("downstream stays open")
        .expect("error decodes");
    assert_eq!(
        error,
        UpstreamMessage::Text(NATIVE_ERROR.into()),
        "native error bytes must be unchanged"
    );
    tokio::time::timeout(Duration::from_secs(5), retired_rx)
        .await
        .expect("provider retirement arrives")
        .expect("provider retirement is confirmed");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client.next())
            .await
            .is_err(),
        "one native connection error must not produce per-lane duplicates or a downstream close",
    );
    client.send(UpstreamMessage::Text(
        r#"{"type":"response.create","stream_id":"a","model":"gpt-4.1","store":false,"input":[]}"#.into(),
    )).await.expect("downstream accepts a new turn on the released lane");
    for expected in ["response.created", "response.completed"] {
        let event = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("recovery event arrives")
            .expect("downstream remains usable")
            .expect("event decodes");
        let value: Value =
            serde_json::from_str(event.to_text().expect("event is text")).expect("event is JSON");
        assert_eq!(value["type"], expected);
        if expected == "response.completed" {
            assert_eq!(event, UpstreamMessage::Text(RECOVERED.into()));
        }
    }
    let _ = client.close(None).await;
    tokio::time::timeout(Duration::from_secs(5), provider_task)
        .await
        .expect("provider task ends")
        .expect("provider assertions pass");
    proxy_task.abort();
}

#[tokio::test]
async fn ws1296_attributed_error_keeps_sibling_lane_and_provider_connection_usable() {
    const ATTRIBUTED_ERROR: &str = r#"{"type":"error","stream_id":"a","response_id":"resp-a","status":400,"error":{"type":"invalid_request_error","message":"Invalid input"}}"#;
    const SIBLING_COMPLETED: &str = r#"{"type":"response.completed","stream_id":"b","response":{"id":"resp-b","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#;
    const RECOVERED: &str = r#"{"type":"response.completed","stream_id":"a","response":{"id":"resp-recovered","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let provider_task = tokio::spawn(async move {
        let (socket, _) = listener
            .accept()
            .await
            .expect("provider connection arrives");
        let mut websocket = accept_async(socket)
            .await
            .expect("provider handshake succeeds");
        for lane in ["a", "b"] {
            let create = websocket
                .next()
                .await
                .expect("named create arrives")
                .expect("create decodes");
            let create: Value = serde_json::from_str(create.to_text().expect("create is text"))
                .expect("create is JSON");
            assert_eq!(create["stream_id"], lane);
            let created = serde_json::json!({
                "type": "response.created", "stream_id": lane,
                "response": {"id": format!("resp-{lane}"), "store": false},
            });
            websocket
                .send(UpstreamMessage::Text(created.to_string().into()))
                .await
                .expect("created sends");
        }
        websocket
            .send(UpstreamMessage::Text(ATTRIBUTED_ERROR.into()))
            .await
            .expect("attributed error sends");
        websocket
            .send(UpstreamMessage::Text(ATTRIBUTED_ERROR.into()))
            .await
            .expect("duplicate attributed error sends");
        websocket
            .send(UpstreamMessage::Text(SIBLING_COMPLETED.into()))
            .await
            .expect("sibling completion sends");
        let create = tokio::time::timeout(Duration::from_secs(5), websocket.next())
            .await
            .expect("same connection receives recovery create")
            .expect("attributed error must not retire the provider connection")
            .expect("recovery create decodes");
        let create: Value = serde_json::from_str(create.to_text().expect("create is text"))
            .expect("create is JSON");
        assert_eq!(create["type"], "response.create");
        assert_eq!(create["stream_id"], "a");
        websocket.send(UpstreamMessage::Text(
            r#"{"type":"response.created","stream_id":"a","response":{"id":"resp-recovered","store":false}}"#.into(),
        )).await.expect("recovery created sends");
        websocket
            .send(UpstreamMessage::Text(RECOVERED.into()))
            .await
            .expect("recovery completion sends");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "an attributed error must not force a fresh provider connection",
        );
        while websocket.next().await.is_some() {}
    });
    let router = websocket_test_router(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds");
    for lane in ["a", "b"] {
        let create = serde_json::json!({
            "type": "response.create", "stream_id": lane,
            "model": "gpt-4.1", "store": false, "input": [],
        });
        client
            .send(UpstreamMessage::Text(create.to_string().into()))
            .await
            .expect("named create sends");
        let created = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("created arrives")
            .expect("downstream stays open")
            .expect("created decodes");
        let created: Value = serde_json::from_str(created.to_text().expect("created is text"))
            .expect("created is JSON");
        assert_eq!(created["type"], "response.created");
        assert_eq!(created["stream_id"], lane);
    }
    for expected in [ATTRIBUTED_ERROR, SIBLING_COMPLETED] {
        let event = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("lane event arrives")
            .expect("downstream stays open")
            .expect("event decodes");
        assert_eq!(event, UpstreamMessage::Text(expected.into()));
    }
    client.send(UpstreamMessage::Text(
        r#"{"type":"response.create","stream_id":"a","model":"gpt-4.1","store":false,"input":[]}"#.into(),
    )).await.expect("failed lane accepts a new turn");
    for expected in ["response.created", "response.completed"] {
        let event = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("recovery event arrives")
            .expect("downstream remains usable")
            .expect("event decodes");
        let value: Value =
            serde_json::from_str(event.to_text().expect("event is text")).expect("event is JSON");
        assert_eq!(value["type"], expected);
        if expected == "response.completed" {
            assert_eq!(event, UpstreamMessage::Text(RECOVERED.into()));
        }
    }
    let _ = client.close(None).await;
    tokio::time::timeout(Duration::from_secs(5), provider_task)
        .await
        .expect("provider task ends")
        .expect("provider assertions pass");
    proxy_task.abort();
}

#[tokio::test]
async fn ws1296_reasoning_bind_failure_preserves_503_on_item_done_and_completed() {
    const ITEM_DONE: &str = r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"rs-bind-failure","type":"reasoning","summary":[],"encrypted_content":"synthetic-ws1296-bind-failure"}}"#;
    const COMPLETED: &str = r#"{"type":"response.completed","response":{"id":"resp-bind-failure","status":"completed","store":false,"output":[{"id":"rs-bind-failure","type":"reasoning","summary":[],"encrypted_content":"synthetic-ws1296-bind-failure"}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#;
    for (unbound_event, queued) in [
        (ITEM_DONE, false),
        (COMPLETED, false),
        (ITEM_DONE, true),
        (COMPLETED, true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("provider listener binds");
        let address = listener
            .local_addr()
            .expect("provider address is available");
        let (release_failure, failure_ready) = tokio::sync::oneshot::channel();
        let provider_task = tokio::spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("provider connection arrives");
            let mut websocket = accept_async(socket)
                .await
                .expect("provider handshake succeeds");
            websocket
                .next()
                .await
                .expect("provider receives create")
                .expect("create decodes");
            websocket
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp-bind-failure","store":false,"output":[]}}"#.into(),
                ))
                .await
                .expect("created sends");
            failure_ready
                .await
                .expect("client finishes admission checks");
            websocket
                .send(UpstreamMessage::Text(unbound_event.into()))
                .await
                .expect("reasoning event sends");
            while websocket.next().await.is_some() {}
        });
        // This existing fixture deliberately has no affinity store: binding
        // durable reasoning must fail closed without changing its status.
        let router = websocket_test_router(
            vec![websocket_upstream(
                1,
                "openai",
                Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
            )],
            TerminalStrategy::FirstPick,
        )
        .await;
        let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
        let (mut client, _) = connect_async(websocket_request(proxy_address))
            .await
            .expect("downstream handshake succeeds");
        client
            .send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
            ))
            .await
            .expect("client create sends");
        let created = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("created arrives")
            .expect("downstream stays open")
            .expect("created decodes");
        let created: Value = serde_json::from_str(created.to_text().expect("created is text"))
            .expect("created is JSON");
        assert_eq!(created["type"], "response.created");
        if queued {
            client.send(UpstreamMessage::Text(
                r#"{"type":"response.create","model":"gpt-4.1","store":false,"previous_response_id":"resp-bind-failure","input":[]}"#.into(),
            )).await.expect("same-lane continuation queues");
            // A rejected control is an owner-loop barrier after the queued
            // create; an automatic Pong would only prove reader progress.
            client
                .send(UpstreamMessage::Binary(Bytes::new()))
                .await
                .unwrap();
            let barrier = tokio::time::timeout(Duration::from_secs(5), client.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let barrier: Value = serde_json::from_str(barrier.to_text().unwrap()).unwrap();
            assert_eq!(barrier["error"]["code"], "invalid_request_error");
        }
        release_failure
            .send(())
            .expect("provider waits for admission");
        let error = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("binding error arrives")
            .expect("downstream stays open")
            .expect("binding error decodes");
        let error: Value =
            serde_json::from_str(error.to_text().expect("error is text")).expect("error is JSON");
        assert_eq!(
            error["type"], "error",
            "unbound reasoning must not be forwarded"
        );
        assert_eq!(
            error["status"], 503,
            "binding failure is not a provider disconnect"
        );
        assert_eq!(error["error"]["code"], "upstream_affinity_unavailable");
        if queued {
            let cancelled = tokio::time::timeout(Duration::from_secs(5), client.next())
                .await
                .expect("queued continuation receives its own failure")
                .expect("downstream remains open")
                .expect("queued failure decodes");
            let cancelled: Value = serde_json::from_str(cancelled.to_text().unwrap()).unwrap();
            assert_eq!(cancelled["status"], 502);
            assert_eq!(cancelled["error"]["code"], "upstream_connection_closed");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), client.next())
                .await
                .is_err(),
            "binding failure must not forward unbound data or emit duplicate errors",
        );
        let _ = client.close(None).await;
        tokio::time::timeout(Duration::from_secs(5), provider_task)
            .await
            .expect("provider task ends")
            .expect("provider assertions pass");
        proxy_task.abort();
    }
}

/// A provider close fails the admitted turn without replaying its create
/// on a different provider. The downstream remains usable for new turns.
#[tokio::test]
async fn websocket_session_does_not_hop_upstreams_after_handshake() {
    let first_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("first upstream listener binds");
    let first_address = first_listener
        .local_addr()
        .expect("first upstream listener has address");
    let first_task = tokio::spawn(async move {
        let (stream, _) = first_listener
            .accept()
            .await
            .expect("pinned connection arrives");
        let mut websocket = accept_async(stream)
            .await
            .expect("pinned WebSocket handshake succeeds");
        let _ = websocket
            .next()
            .await
            .expect("first provider receives create");
        websocket
            .send(UpstreamMessage::Text(
                r#"{"type":"response.created","response":{"id":"first","store":false}}"#.into(),
            ))
            .await
            .expect("pinned upstream sends a relay frame");
        let _ = websocket.close(None).await;
    });
    let second_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second upstream listener binds");
    let second_address = second_listener
        .local_addr()
        .expect("second upstream listener has address");

    let router = websocket_test_router(
        vec![
            websocket_upstream(
                1,
                "first-openai",
                Url::parse(&format!("http://{first_address}/")).expect("first upstream URL parses"),
            ),
            websocket_upstream(
                2,
                "second-openai",
                Url::parse(&format!("http://{second_address}/"))
                    .expect("second upstream URL parses"),
            ),
        ],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(websocket_request(proxy_address)),
    )
    .await
    .expect("WebSocket handshake should not time out")
    .expect("the selected upstream should complete the handshake");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("client create sends");
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("relayed frame should not time out")
        .expect("pinned relay should deliver the first frame")
        .expect("pinned relay frame should decode");
    assert_eq!(
        message,
        UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"first","store":false}}"#.into(),
        ),
    );
    let error = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("provider closure error arrives")
        .expect("downstream stays open")
        .expect("provider closure error decodes");
    let error: Value = serde_json::from_str(error.to_text().expect("error is text"))
        .expect("provider closure error is JSON");
    assert_eq!(error["error"]["code"], "upstream_connection_closed");

    assert!(
        tokio::time::timeout(Duration::from_millis(300), second_listener.accept())
            .await
            .is_err(),
        "the session must not reconnect to another upstream after the handshake"
    );
    let _ = first_task.await;
    proxy_task.abort();
}

#[derive(Clone, Copy)]
enum FakeEncryptedBehavior {
    RejectForeignInvalidEncrypted,
    AcceptAll,
}

#[derive(Clone)]
struct RecordedUpstreamRequest {
    transport: &'static str,
    body: Value,
}

struct FakeCodexServer {
    address: SocketAddr,
    requests: Arc<std::sync::Mutex<Vec<RecordedUpstreamRequest>>>,
    task: JoinHandle<()>,
}

fn fake_invalid_encrypted_error(item_id: &str) -> Value {
    serde_json::json!({
        "error": {
            "type": "invalid_request_error",
            "code": "invalid_encrypted_content",
            "message": format!(
                "The encrypted content for item {item_id} could not be verified. Reason: Encrypted content organization_id did not match the target organization"
            ),
        }
    })
}

fn fake_completed_response(id: &str, output_reasoning: Option<(&str, &str)>) -> Value {
    let mut output = Vec::new();
    if let Some((item_id, ciphertext)) = output_reasoning {
        output.push(serde_json::json!({
            "type": "reasoning",
            "id": item_id,
            "summary": [],
            "encrypted_content": ciphertext,
        }));
    }
    serde_json::json!({
        "id": id,
        "object": "response",
        "model": "gpt-5-codex",
        "status": "completed",
        "store": false,
        "output": output,
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    })
}

fn first_unknown_reasoning_id(body: &Value, allowed_known: &[&str]) -> Option<String> {
    body.get("input")?.as_array()?.iter().find_map(|item| {
        let obj = item.as_object()?;
        if obj.get("type").and_then(Value::as_str) != Some("reasoning") {
            return None;
        }
        let enc = obj.get("encrypted_content").and_then(Value::as_str)?;
        if allowed_known.contains(&enc) {
            return None;
        }
        Some(
            obj.get("id")
                .and_then(Value::as_str)
                .unwrap_or("rs_unknown")
                .to_owned(),
        )
    })
}

async fn spawn_fake_codex_upstream(
    behavior: FakeEncryptedBehavior,
    allowed_known: Vec<&'static str>,
    response_prefix: &'static str,
    output_reasoning: Option<(&'static str, &'static str)>,
    gzip_400: bool,
) -> FakeCodexServer {
    use axum::routing::get;
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reqs_http = requests.clone();
    let reqs_ws = requests.clone();
    let allowed_http = allowed_known.clone();
    let allowed_ws = allowed_known;
    let seq = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seq_http = seq.clone();
    let seq_ws = seq;

    let router = axum::Router::new().route(
        "/responses",
        get(move |upgrade: WebSocketUpgrade| {
            let reqs_ws = reqs_ws.clone();
            let allowed_ws = allowed_ws.clone();
            let seq_ws = seq_ws.clone();
            async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    while let Some(Ok(Message::Text(frame))) = socket.next().await {
                        let body: Value =
                            serde_json::from_str(&frame).expect("WS provider frame is JSON");
                        reqs_ws
                            .lock()
                            .expect("requests lock")
                            .push(RecordedUpstreamRequest {
                                transport: "ws",
                                body: body.clone(),
                            });
                        let stream_id = body
                            .get("stream_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                        if matches!(
                            behavior,
                            FakeEncryptedBehavior::RejectForeignInvalidEncrypted
                        ) && let Some(item_id) = first_unknown_reasoning_id(&body, &allowed_ws)
                        {
                            let mut error = serde_json::json!({
                                "type": "error",
                                "status": 400,
                                "error": fake_invalid_encrypted_error(&item_id)["error"],
                            });
                            if let Some(stream_id) = stream_id {
                                error["stream_id"] = Value::String(stream_id);
                            }
                            let _ = socket.send(Message::Text(error.to_string().into())).await;
                            continue;
                        }
                        let n = seq_ws.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        let resp_id = format!("{response_prefix}_ws_{n}");
                        let created = serde_json::json!({
                            "type": "response.created",
                            "sequence_number": 0,
                            "response": {
                                "id": resp_id,
                                "object": "response",
                                "model": "gpt-5-codex",
                                "status": "in_progress",
                                "store": false,
                                "output": [],
                                "usage": null,
                            }
                        });
                        let _ = socket.send(Message::Text(created.to_string().into())).await;
                        if let Some((item_id, ciphertext)) = output_reasoning {
                            let item_done = serde_json::json!({
                                "type": "response.output_item.done",
                                "sequence_number": 1,
                                "output_index": 0,
                                "item": {
                                    "type": "reasoning",
                                    "id": item_id,
                                    "summary": [],
                                    "encrypted_content": ciphertext,
                                }
                            });
                            let _ = socket
                                .send(Message::Text(item_done.to_string().into()))
                                .await;
                        }
                        let completed = serde_json::json!({
                            "type": "response.completed",
                            "sequence_number": 2,
                            "response": fake_completed_response(&resp_id, output_reasoning),
                        });
                        let _ = socket
                            .send(Message::Text(completed.to_string().into()))
                            .await;
                    }
                })
            }
        })
        .post(move |bytes: Bytes| {
            let reqs_http = reqs_http.clone();
            let allowed_http = allowed_http.clone();
            let seq_http = seq_http.clone();
            async move {
                let body: Value =
                    serde_json::from_slice(&bytes).expect("HTTP provider body is JSON");
                reqs_http
                    .lock()
                    .expect("requests lock")
                    .push(RecordedUpstreamRequest {
                        transport: "http",
                        body: body.clone(),
                    });
                if matches!(
                    behavior,
                    FakeEncryptedBehavior::RejectForeignInvalidEncrypted
                ) && let Some(item_id) = first_unknown_reasoning_id(&body, &allowed_http)
                {
                    let payload = serde_json::to_vec(&fake_invalid_encrypted_error(&item_id))
                        .expect("error json");
                    if gzip_400 {
                        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                        encoder.write_all(&payload).expect("gzip writes");
                        let compressed = encoder.finish().expect("gzip finishes");
                        return axum::response::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(http::header::CONTENT_TYPE, "application/json")
                            .header(http::header::CONTENT_ENCODING, "gzip")
                            .body(Body::from(compressed))
                            .expect("gzip 400 builds");
                    }
                    return axum::response::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("400 builds");
                }
                let n = seq_http.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                let resp_id = format!("{response_prefix}_http_{n}");
                axum::response::Response::builder()
                    .status(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&fake_completed_response(&resp_id, output_reasoning))
                            .expect("ok json"),
                    ))
                    .expect("200 builds")
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake codex binds");
    let address = listener.local_addr().expect("fake codex addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("fake codex serves");
    });
    FakeCodexServer {
        address,
        requests,
        task,
    }
}

fn codex_foreign_history_payload(
    is_ws: bool,
    reasoning_items: &[(&str, &str)],
    user_text: &str,
) -> Value {
    let mut input = Vec::new();
    for (item_id, enc) in reasoning_items {
        input.push(serde_json::json!({
            "type": "reasoning",
            "id": item_id,
            "summary": [],
            "encrypted_content": enc,
        }));
    }
    input.push(serde_json::json!({
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": user_text}],
    }));
    let mut payload = serde_json::json!({
        "model": "gpt-5-codex",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": input,
    });
    if is_ws {
        payload["type"] = Value::String("response.create".to_owned());
    }
    payload
}

async fn read_ws_until_completed(
    client: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("WS response frame arrives in time")
            .expect("WS stream stays open")
            .expect("WS frame decodes");
        let text = msg.to_text().expect("WS frame is text");
        let value: Value = serde_json::from_str(text).expect("WS frame is JSON");
        let event_type = value["type"].as_str().unwrap_or_default().to_owned();
        events.push(value);
        if event_type == "response.completed" || event_type == "error" {
            break;
        }
    }
    events
}

#[tokio::test]
async fn codex_unknown_reasoning_e2e_multi_and_single_upstream() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();
    let foreign_ciphertext = "gAAAAABcliproxy_foreign";

    // Scenario A: Multi-upstream pool (2 OpenAI OAuth upstreams) on empty DB
    // must strip foreign reasoning upfront and succeed without 1/N guessing.
    let fake_a = spawn_fake_codex_upstream(
        FakeEncryptedBehavior::RejectForeignInvalidEncrypted,
        vec![],
        "resp_a",
        Some(("rs_cclb_new_a", "gAAAAABcclb_new_a")),
        false,
    )
    .await;
    let fake_b = spawn_fake_codex_upstream(
        FakeEncryptedBehavior::RejectForeignInvalidEncrypted,
        vec![],
        "resp_b",
        Some(("rs_cclb_new_b", "gAAAAABcclb_new_b")),
        false,
    )
    .await;
    let mut up_a = websocket_upstream(
        1,
        "oauth-a",
        Url::parse(&format!("http://{}/", fake_a.address)).unwrap(),
    );
    up_a.kind = UpstreamKind::OpenaiOauth;
    let mut up_b = websocket_upstream(
        2,
        "oauth-b",
        Url::parse(&format!("http://{}/", fake_b.address)).unwrap(),
    );
    up_b.kind = UpstreamKind::OpenaiOauth;
    let (_dir_a, router_a) =
        websocket_test_router_with_affinity_store(vec![up_a, up_b], TerminalStrategy::FirstPick)
            .await;
    let (proxy_a, task_a) = spawn_proxy_router(router_a).await;

    let http_res = http
        .post(format!(
            "http://{proxy_a}/openai/backend-api/codex/responses"
        ))
        .header("x-api-key", TEST_KEY.plaintext.as_str())
        .json(&codex_foreign_history_payload(
            false,
            &[("rs_cliproxy_1", foreign_ciphertext)],
            "continue after cliproxyapi",
        ))
        .send()
        .await
        .expect("HTTP multi-upstream sends");
    let http_status = http_res.status();
    let http_body: Value = http_res.json().await.expect("HTTP json");
    assert_eq!(
        http_status,
        StatusCode::OK,
        "Scenario A HTTP must succeed: {http_body}"
    );

    let mut ws_req = websocket_request(proxy_a);
    *ws_req.uri_mut() = format!("ws://{proxy_a}/openai/backend-api/codex/responses")
        .parse()
        .unwrap();
    let (mut ws_client, _) = connect_async(ws_req).await.expect("WS connects");
    ws_client
        .send(UpstreamMessage::Text(
            codex_foreign_history_payload(
                true,
                &[("rs_cliproxy_1", foreign_ciphertext)],
                "continue ws after cliproxyapi",
            )
            .to_string()
            .into(),
        ))
        .await
        .expect("WS create sends");
    let ws_events = read_ws_until_completed(&mut ws_client).await;
    assert_eq!(
        ws_events.last().and_then(|v| v["type"].as_str()),
        Some("response.completed"),
        "Scenario A WS must succeed: {ws_events:?}"
    );
    let _ = ws_client.close(None).await;

    let seen_a = fake_a.requests.lock().unwrap().clone();
    let seen_b = fake_b.requests.lock().unwrap().clone();
    assert_eq!(
        seen_a.len(),
        2,
        "multi-upstream should strip upfront without retry (1 HTTP + 1 WS)"
    );
    assert_eq!(seen_a[0].transport, "http");
    assert_eq!(seen_a[1].transport, "ws");
    assert!(seen_b.is_empty());
    for req in &seen_a {
        let items = req.body["input"].as_array().expect("input array");
        assert!(items.iter().all(|item| item["type"] != "reasoning"));
        assert_eq!(items.len(), 1);
        assert_eq!(
            req.body["include"],
            serde_json::json!(["reasoning.encrypted_content"])
        );
    }
    task_a.abort();
    fake_a.task.abort();
    fake_b.task.abort();

    // Scenario B: Single eligible upstream accepts foreign reasoning verbatim and learns it.
    let fake_match = spawn_fake_codex_upstream(
        FakeEncryptedBehavior::AcceptAll,
        vec![],
        "resp_match",
        None,
        false,
    )
    .await;
    let mut up_match = websocket_upstream(
        10,
        "oauth-match",
        Url::parse(&format!("http://{}/", fake_match.address)).unwrap(),
    );
    up_match.kind = UpstreamKind::OpenaiOauth;
    let (_dir_b, router_b) =
        websocket_test_router_with_affinity_store(vec![up_match], TerminalStrategy::FirstPick)
            .await;
    let (proxy_b, task_b) = spawn_proxy_router(router_b).await;

    let http_res_b = http
        .post(format!(
            "http://{proxy_b}/openai/backend-api/codex/responses"
        ))
        .header("x-api-key", TEST_KEY.plaintext.as_str())
        .json(&codex_foreign_history_payload(
            false,
            &[("rs_cliproxy_1", "gAAAAABmatch_http")],
            "hello single http",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(http_res_b.status(), StatusCode::OK);

    let mut ws_req_b = websocket_request(proxy_b);
    *ws_req_b.uri_mut() = format!("ws://{proxy_b}/openai/backend-api/codex/responses")
        .parse()
        .unwrap();
    let (mut ws_client_b, _) = connect_async(ws_req_b).await.unwrap();
    ws_client_b
        .send(UpstreamMessage::Text(
            codex_foreign_history_payload(
                true,
                &[("rs_cliproxy_1", "gAAAAABmatch_ws")],
                "hello single ws",
            )
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let ws_events_b = read_ws_until_completed(&mut ws_client_b).await;
    assert_eq!(
        ws_events_b.last().and_then(|v| v["type"].as_str()),
        Some("response.completed"),
        "Scenario B WS must succeed: {ws_events_b:?}"
    );
    let _ = ws_client_b.close(None).await;

    let seen_match = fake_match.requests.lock().unwrap().clone();
    assert_eq!(seen_match.len(), 2);
    assert_eq!(
        seen_match[0].body["input"][0]["encrypted_content"],
        "gAAAAABmatch_http"
    );
    assert_eq!(
        seen_match[1].body["input"][0]["encrypted_content"],
        "gAAAAABmatch_ws"
    );
    task_b.abort();
    fake_match.task.abort();

    // Scenario C: Single eligible upstream rejects with 400 invalid_encrypted_content
    // (including gzip-encoded HTTP 400), retries once stripped on same upstream, and succeeds.
    let fake_retry = spawn_fake_codex_upstream(
        FakeEncryptedBehavior::RejectForeignInvalidEncrypted,
        vec![],
        "resp_retry",
        None,
        true,
    )
    .await;
    let mut up_retry = websocket_upstream(
        20,
        "oauth-retry",
        Url::parse(&format!("http://{}/", fake_retry.address)).unwrap(),
    );
    up_retry.kind = UpstreamKind::OpenaiOauth;
    let (_dir_c, router_c) =
        websocket_test_router_with_affinity_store(vec![up_retry], TerminalStrategy::FirstPick)
            .await;
    let (proxy_c, task_c) = spawn_proxy_router(router_c).await;

    let http_res_c = http
        .post(format!(
            "http://{proxy_c}/openai/backend-api/codex/responses"
        ))
        .header("x-api-key", TEST_KEY.plaintext.as_str())
        .json(&codex_foreign_history_payload(
            false,
            &[("rs_cliproxy_1", "gAAAAABmismatch_http")],
            "hello retry http",
        ))
        .send()
        .await
        .unwrap();
    let status_c = http_res_c.status();
    let body_c: Value = http_res_c.json().await.unwrap();
    assert_eq!(
        status_c,
        StatusCode::OK,
        "Scenario C HTTP retry must succeed: {body_c}"
    );

    let mut ws_req_c = websocket_request(proxy_c);
    *ws_req_c.uri_mut() = format!("ws://{proxy_c}/openai/backend-api/codex/responses")
        .parse()
        .unwrap();
    let (mut ws_client_c, _) = connect_async(ws_req_c).await.unwrap();
    ws_client_c
        .send(UpstreamMessage::Text(
            codex_foreign_history_payload(
                true,
                &[("rs_cliproxy_1", "gAAAAABmismatch_ws")],
                "hello retry ws",
            )
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let ws_events_c = read_ws_until_completed(&mut ws_client_c).await;
    assert_eq!(
        ws_events_c.last().and_then(|v| v["type"].as_str()),
        Some("response.completed"),
        "Scenario C WS retry must succeed: {ws_events_c:?}"
    );
    let _ = ws_client_c.close(None).await;

    let seen_retry = fake_retry.requests.lock().unwrap().clone();
    assert_eq!(
        seen_retry.len(),
        4,
        "Scenario C should record raw attempt + stripped retry for both HTTP and WS"
    );
    assert_eq!(
        seen_retry[0].body["input"][0]["encrypted_content"],
        "gAAAAABmismatch_http"
    );
    assert_eq!(seen_retry[1].body["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        seen_retry[2].body["input"][0]["encrypted_content"],
        "gAAAAABmismatch_ws"
    );
    assert_eq!(seen_retry[3].body["input"].as_array().unwrap().len(), 1);

    task_c.abort();
    fake_retry.task.abort();
}

#[tokio::test]
async fn codex_unknown_reasoning_e2e_mixed_turn2_history() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();
    let owned_http = "gAAAAABcclb_turn1_http";
    let owned_ws = "gAAAAABcclb_turn1_ws";
    let foreign = "gAAAAABcliproxy_legacy";

    let fake_a = spawn_fake_codex_upstream(
        FakeEncryptedBehavior::RejectForeignInvalidEncrypted,
        vec![owned_http, owned_ws],
        "resp_a",
        Some(("rs_cclb_1", owned_http)),
        false,
    )
    .await;
    let fake_b = spawn_fake_codex_upstream(
        FakeEncryptedBehavior::RejectForeignInvalidEncrypted,
        vec![],
        "resp_b",
        Some(("rs_b_1", "gAAAAABunused_b")),
        false,
    )
    .await;

    let mut up_a = websocket_upstream(
        1,
        "oauth-a",
        Url::parse(&format!("http://{}/", fake_a.address)).unwrap(),
    );
    up_a.kind = UpstreamKind::OpenaiOauth;
    let mut up_b = websocket_upstream(
        2,
        "oauth-b",
        Url::parse(&format!("http://{}/", fake_b.address)).unwrap(),
    );
    up_b.kind = UpstreamKind::OpenaiOauth;

    let (_dir, router) =
        websocket_test_router_with_affinity_store(vec![up_a, up_b], TerminalStrategy::FirstPick)
            .await;
    let (proxy, proxy_task) = spawn_proxy_router(router).await;

    // Turn 1 HTTP establishes affinity for `owned_http` on oauth-a.
    let turn1_http = http
        .post(format!("http://{proxy}/openai/backend-api/codex/responses"))
        .header("x-api-key", TEST_KEY.plaintext.as_str())
        .json(&codex_foreign_history_payload(false, &[], "turn 1 http"))
        .send()
        .await
        .unwrap();
    assert_eq!(turn1_http.status(), StatusCode::OK);

    // Turn 2 HTTP mixes foreign legacy reasoning + owned Turn 1 reasoning.
    let turn2_http = http
        .post(format!("http://{proxy}/openai/backend-api/codex/responses"))
        .header("x-api-key", TEST_KEY.plaintext.as_str())
        .json(&codex_foreign_history_payload(
            false,
            &[("rs_foreign", foreign), ("rs_cclb_1", owned_http)],
            "turn 2 http mixed",
        ))
        .send()
        .await
        .unwrap();
    let turn2_status = turn2_http.status();
    let turn2_body: Value = turn2_http.json().await.unwrap();
    assert_eq!(
        turn2_status,
        StatusCode::OK,
        "Scenario D HTTP Turn 2 mixed history must succeed: {turn2_body}"
    );

    // Now verify WS Turn 2 with mixed history where WS response binds `owned_ws` on Turn 1.
    let mut ws_req1 = websocket_request(proxy);
    *ws_req1.uri_mut() = format!("ws://{proxy}/openai/backend-api/codex/responses")
        .parse()
        .unwrap();
    let (mut ws1, _) = connect_async(ws_req1).await.unwrap();
    ws1.send(UpstreamMessage::Text(
        codex_foreign_history_payload(true, &[], "turn 1 ws")
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let ws1_events = read_ws_until_completed(&mut ws1).await;
    assert_eq!(
        ws1_events.last().and_then(|v| v["type"].as_str()),
        Some("response.completed")
    );
    let _ = ws1.close(None).await;

    // Reconnect WS (fresh socket, DB affinity for `owned_http` / Turn 1 item) and send mixed Turn 2.
    let mut ws_req2 = websocket_request(proxy);
    *ws_req2.uri_mut() = format!("ws://{proxy}/openai/backend-api/codex/responses")
        .parse()
        .unwrap();
    let (mut ws2, _) = connect_async(ws_req2).await.unwrap();
    ws2.send(UpstreamMessage::Text(
        codex_foreign_history_payload(
            true,
            &[("rs_foreign", foreign), ("rs_cclb_1", owned_http)],
            "turn 2 ws mixed",
        )
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let ws2_events = read_ws_until_completed(&mut ws2).await;
    assert_eq!(
        ws2_events.last().and_then(|v| v["type"].as_str()),
        Some("response.completed"),
        "Scenario D WS Turn 2 mixed history must succeed: {ws2_events:?}"
    );
    let _ = ws2.close(None).await;

    // Check recorded upstream requests on oauth-a:
    // Index 0: Turn 1 HTTP (no reasoning)
    // Index 1: Turn 2 HTTP raw (both foreign + owned_http)
    // Index 2: Turn 2 HTTP retry (ONLY owned_http kept!)
    // Index 3: Turn 1 WS (no reasoning)
    // Index 4: Turn 2 WS raw (both foreign + owned_http)
    // Index 5: Turn 2 WS retry (ONLY owned_http kept!)
    let seen_a = fake_a.requests.lock().unwrap().clone();
    assert_eq!(
        seen_a.len(),
        6,
        "unexpected request count: {}",
        seen_a.len()
    );
    assert_eq!(seen_a[1].body["input"].as_array().unwrap().len(), 3);
    assert_eq!(seen_a[2].body["input"].as_array().unwrap().len(), 2);
    assert_eq!(seen_a[2].body["input"][0]["encrypted_content"], owned_http);
    assert_eq!(seen_a[4].body["input"].as_array().unwrap().len(), 3);
    assert_eq!(seen_a[5].body["input"].as_array().unwrap().len(), 2);
    assert_eq!(seen_a[5].body["input"][0]["encrypted_content"], owned_http);
    assert!(fake_b.requests.lock().unwrap().is_empty());

    // Sending ONLY `foreign` again across `[oauth-a, oauth-b]` must NOT be pinned
    // to `oauth-a` (multi-upstream still strips `foreign` instead of binding it).
    let post_check = http
        .post(format!("http://{proxy}/openai/backend-api/codex/responses"))
        .header("x-api-key", TEST_KEY.plaintext.as_str())
        .json(&codex_foreign_history_payload(
            false,
            &[("rs_foreign", foreign)],
            "foreign should remain unbound",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(post_check.status(), StatusCode::OK);
    let seen_a_after = fake_a.requests.lock().unwrap().clone();
    assert_eq!(seen_a_after.len(), 7);
    assert_eq!(
        seen_a_after[6].body["input"].as_array().unwrap().len(),
        1,
        "foreign reasoning must never be bound during Turn 2 recovery"
    );

    proxy_task.abort();
    fake_a.task.abort();
    fake_b.task.abort();
}
