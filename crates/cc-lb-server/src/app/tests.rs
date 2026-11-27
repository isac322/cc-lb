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
use std::sync::{Arc, LazyLock, Mutex};

use crate::state_machine::ServerStateHandle;
use axum::body::Body;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
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

pub(super) struct TestKey {
    pub(super) plaintext: String,
    key_id: String,
    record: StoredApiKeyRecord,
}

pub(super) static TEST_KEY: LazyLock<TestKey> = LazyLock::new(|| {
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
pub(super) struct FixtureKeyStore;

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
pub(super) async fn authenticate_test_request(headers: &HeaderMap) -> cc_lb_engine::Authenticated {
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
    let response = body_too_large_response(cc_lb_domain::Provider::Anthropic);
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
    for model in [
        "claude-fable-5-1",
        "claude-fable-5-2",
        "claude-mythos-5-1",
        "claude-mythos-5-2",
    ] {
        assert!(
            catalog.lookup(model, None).is_none(),
            "{model} must stay unavailable in the built-in bootstrap snapshot until speed and inference_geo pricing modifiers are modeled"
        );
    }
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
    config.upstream_affinity.unavailable_openai_reasoning_owner =
        cc_lb_config::UnavailableOpenaiReasoningOwnerPolicy::FailClosed;

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
    assert_eq!(
        lifecycle_config.unavailable_openai_reasoning_owner,
        cc_lb_config::UnavailableOpenaiReasoningOwnerPolicy::FailClosed
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
        clock,
        breaker_registry: Arc::new(BreakerRegistry::new()),
        breaker_config: BreakerRuntimeConfig::default(),
        dns_resolver: cc_lb_engine::MetricDnsResolver::new(
            &cc_lb_engine::dns_cache::DnsResolverConfig::default(),
        )
        .expect("default DNS resolver builds"),
        upstream_connect_timeout: Duration::from_secs(30),
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

pub(super) struct WebSocketTestSignerFactory;

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

pub(super) fn websocket_upstream(id: u128, name: &str, base_url: Url) -> UpstreamRecord {
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

pub(super) async fn websocket_test_state(
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
        clock,
        breaker_registry: Arc::new(BreakerRegistry::new()),
        breaker_config: BreakerRuntimeConfig::default(),
        websocket_bulkheads: Arc::new(BulkheadRegistry::new()),
        websocket_bulkhead_config: BulkheadRuntimeConfig {
            max_conns_per_upstream: 50,
            semaphore_permits: 100,
            acquire_timeout: Duration::from_secs(1),
        },
        dns_resolver: cc_lb_engine::MetricDnsResolver::new(
            &cc_lb_engine::dns_cache::DnsResolverConfig::default(),
        )
        .expect("default DNS resolver builds"),
        upstream_connect_timeout: Duration::from_secs(30),
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

#[test]
fn websocket_provider_request_preserves_shaped_cloudflare_access_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer provider-token"),
    );
    headers.insert(
        "cf-access-client-id",
        HeaderValue::from_static("shape-client-id"),
    );
    headers.insert(
        "cf-access-client-secret",
        HeaderValue::from_static("shape-client-secret"),
    );
    let request = responses_websocket_request(
        Url::parse("ws://provider.example/v1/responses").expect("provider URL parses"),
        headers,
    )
    .expect("provider handshake request builds");
    assert_eq!(
        request
            .headers()
            .get("cf-access-client-id")
            .and_then(|v| v.to_str().ok()),
        Some("shape-client-id"),
    );
    assert_eq!(
        request
            .headers()
            .get("cf-access-client-secret")
            .and_then(|v| v.to_str().ok()),
        Some("shape-client-secret"),
    );
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
    const MAX_PERMITS: usize = 4;
    let (upstream_address, upstream_cancel, upstream_task) = spawn_holding_upstream().await;
    let mut state = websocket_test_state(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{upstream_address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 50,
        semaphore_permits: MAX_PERMITS as u32,
        acquire_timeout: Duration::from_millis(50),
    };
    let router = proxy_router(state, 30);
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let mut clients = Vec::with_capacity(MAX_PERMITS);
    for _ in 0..MAX_PERMITS {
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

#[tokio::test(flavor = "current_thread")]
async fn responses_request_body_normalize_offloads_zstd_async() {
    let mut base_headers = HeaderMap::new();
    base_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let auth = authenticate_test_request(&base_headers).await;

    let raw = br#"{"model":"gpt-5","input":"zstd-async"}"#;
    let compressed = zstd::bulk::compress(raw, 1).expect("compress zstd request");
    let mut zstd_req = Request::builder()
        .method(Method::POST)
        .uri("/v1/responses")
        .header(CONTENT_TYPE, "application/json")
        .header("content-encoding", "zstd")
        .header(CONTENT_LENGTH, compressed.len().to_string())
        .body(Bytes::from(compressed.clone()))
        .unwrap();
    zstd_req
        .extensions_mut()
        .insert(ProviderScope(cc_lb_domain::Provider::Openai));

    let normalized = super::responses_request_body::normalize(zstd_req, 1024, &auth, None)
        .await
        .expect("zstd normalize succeeds");
    assert!(normalized.headers().get("content-encoding").is_none());
    assert!(normalized.headers().get(CONTENT_LENGTH).is_none());
    assert_eq!(normalized.body().as_ref(), raw);

    let mut over_cap = Request::builder()
        .method(Method::POST)
        .uri("/v1/responses")
        .header("content-encoding", "zstd")
        .body(Bytes::from(compressed))
        .unwrap();
    over_cap
        .extensions_mut()
        .insert(ProviderScope(cc_lb_domain::Provider::Openai));
    assert_eq!(
        super::responses_request_body::normalize(over_cap, raw.len() - 1, &auth, None)
            .await
            .unwrap_err(),
        super::responses_request_body::ResponsesRequestBodyError::TooLarge
    );

    let invalid = Request::builder()
        .method(Method::POST)
        .uri("/v1/responses")
        .header("content-encoding", "zstd")
        .body(Bytes::from_static(b"not-valid-zstd"))
        .unwrap();
    assert_eq!(
        super::responses_request_body::normalize(invalid, 1024, &auth, None)
            .await
            .unwrap_err(),
        super::responses_request_body::ResponsesRequestBodyError::InvalidEncoding
    );
}

#[tokio::test]
async fn responses_websocket_routes_emit_trace_layer_http_spans() {
    use tower::ServiceExt as _;
    use tracing_subscriber::layer::{Context as LayerContext, Layer, SubscriberExt as _};
    use tracing_subscriber::registry::LookupSpan;

    #[derive(Clone, Default)]
    struct RecordedHttpSpans(Arc<Mutex<Vec<Option<String>>>>);

    #[derive(Default)]
    struct RouteVisitor(Option<String>);

    impl tracing::field::Visit for RouteVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "http.route" {
                self.0 = Some(value.to_owned());
            }
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "http.route" && self.0.is_none() {
                self.0 = Some(format!("{value:?}").trim_matches('"').to_owned());
            }
        }
    }

    impl<S> Layer<S> for RecordedHttpSpans
    where
        S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            ctx: LayerContext<'_, S>,
        ) {
            if let Some(span) = ctx.span(id)
                && span.metadata().name() == "proxy.request"
            {
                let mut visitor = RouteVisitor::default();
                values.record(&mut visitor);
                if visitor.0.is_some() {
                    self.0.lock().unwrap().push(visitor.0);
                }
            }
        }
    }

    let router = websocket_test_router_with_allowed_models(
        vec![],
        TerminalStrategy::FirstPick,
        vec!["*".to_owned()],
    )
    .await;
    let recorded = RecordedHttpSpans::default();
    let subscriber = tracing_subscriber::Registry::default().with(recorded.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    for path in [
        "/v1/responses",
        "/openai/v1/responses",
        "/openai/backend-api/codex/responses",
    ] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.expect("route responds");
        // Missing WebSocket upgrade headers -> 426 Upgrade Required after trace_layer enters.
        assert!(!resp.status().is_success(), "unexpected 2xx on {path}");
    }

    let spans = recorded.0.lock().unwrap().clone();
    assert_eq!(
        spans,
        vec![
            Some("/v1/responses".to_owned()),
            Some("/v1/responses".to_owned()),
            Some("/v1/responses".to_owned()),
        ],
        "expected all 3 Responses WS routes to emit HTTP request trace_layer spans with http.route=/v1/responses"
    );
}

#[tokio::test]
async fn codex_backend_root_non_upgrade_returns_503_when_draining_with_zero_events() {
    use tower::ServiceExt as _;

    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let drain_controller = DrainController::new();
    drain_controller.trigger();

    let mut state = websocket_test_state(
        Vec::new(),
        TerminalStrategy::FirstPick,
        drain_controller,
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let lifecycle = Arc::try_unwrap(state.lifecycle)
        .unwrap_or_else(|_| panic!("test lifecycle has single owner"))
        .with_event_bus(bus);
    state.lifecycle = Arc::new(lifecycle);
    let router = proxy_router(state, 30);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/openai/backend-api/codex")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.expect("route responds");
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        resp.headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("60")
    );
    assert!(
        events.try_recv().is_err(),
        "draining pre-auth 503 on codex backend root must emit 0 request events"
    );
}

#[tokio::test]
async fn dispatcher_isolates_same_host_upstreams_by_id_and_evicts_deleted_upstreams() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    // Unreachable peer triggers Transport error inside CircuitBreakerDispatch without waiting on network.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind port");
    let closed_addr = listener.local_addr().expect("local addr");
    drop(listener);

    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let mut config = Config::default();
    config.circuit_breaker.failures_to_open = 1;
    let parts = dispatcher(&config, clock.clone());

    let up_a = websocket_upstream(
        1,
        "shared-host-a",
        Url::parse(&format!("http://{closed_addr}/")).expect("url parses"),
    );
    let up_b = websocket_upstream(
        2,
        "shared-host-b",
        Url::parse(&format!("http://{closed_addr}/")).expect("url parses"),
    );
    assert_eq!(up_a.base_url, up_b.base_url);
    assert_ne!(up_a.id, up_b.id);

    async fn signed_for(upstream: &UpstreamRecord, url: &str) -> SignedRequest {
        struct FixedDialect(Url);
        impl cc_lb_upstream::UpstreamDialect for FixedDialect {
            fn shape(
                &self,
                context: &cc_lb_upstream::DialectShapeContext,
                _upstream: &cc_lb_domain::Upstream,
                _principal: &cc_lb_domain::Principal,
                builder: &mut cc_lb_upstream::ShapedRequestBuilder,
            ) -> Result<ShapedRequest, cc_lb_upstream::DialectError> {
                Ok(builder.shaped_request(
                    self.0.clone(),
                    context.method.clone(),
                    context.downstream_headers.clone(),
                    context.body_bytes.clone(),
                ))
            }
        }
        struct PassSigner;
        #[async_trait]
        impl Signer for PassSigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        let ctx = cc_lb_upstream::DialectShapeContext {
            request_id: "req-iso".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(b"{}"),
        };
        let principal = cc_lb_domain::Principal {
            id: TEST_PRINCIPAL.to_owned(),
            kind: cc_lb_domain::PrincipalKind::ApiKey,
        };
        let dom_up = cc_lb_domain::Upstream::AnthropicDirect { base_url: None };
        let mut shaped = cc_lb_upstream::shape_request(
            &FixedDialect(Url::parse(url).unwrap()),
            &ctx,
            &dom_up,
            &principal,
        )
        .unwrap();
        shaped.set_upstream_id(upstream.id);
        cc_lb_upstream::sign_request(&PassSigner, shaped)
            .await
            .unwrap()
    }

    // Dispatch once to up_a (fails on closed port -> opens up_a breaker only).
    let url = format!("http://{closed_addr}/v1/messages");
    let _ = parts
        .dispatcher
        .dispatch(signed_for(&up_a, &url).await)
        .await;
    let breaker_a = parts
        .breaker_registry
        .get(&up_a.id.to_string())
        .expect("up_a breaker registered by UUID");
    assert_eq!(breaker_a.current_state(), cc_lb_engine::BreakerState::Open);
    assert!(parts.breaker_registry.get(&up_b.id.to_string()).is_none());

    // Live OK server at the same URL format on another socket? Even on the SAME URL,
    // up_b stays healthy/independent in its own breaker + bulkhead entry.
    let ok_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ok");
    let ok_addr = ok_listener.local_addr().expect("ok addr");
    let ok_task = tokio::spawn(async move {
        if let Ok((mut stream, _)) = ok_listener.accept().await {
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
        }
    });
    let ok_url = format!("http://{ok_addr}/v1/messages");
    let _ = parts.breaker_registry.breaker(
        websocket_breaker_key(up_a.id),
        parts.breaker_config,
        clock.clone(),
    );
    let _ = parts.breaker_registry.breaker(
        websocket_breaker_key(up_b.id),
        parts.breaker_config,
        clock.clone(),
    );
    let _ = parts.websocket_bulkheads.semaphore_only_bulkhead(
        websocket_breaker_key(up_a.id),
        parts.websocket_bulkhead_config,
    );
    let _ = parts.websocket_bulkheads.semaphore_only_bulkhead(
        websocket_breaker_key(up_b.id),
        parts.websocket_bulkhead_config,
    );
    let ok_resp = parts
        .dispatcher
        .dispatch(signed_for(&up_b, &ok_url).await)
        .await
        .expect("up_b unaffected by up_a breaker");
    assert_eq!(ok_resp.status(), StatusCode::OK);
    ok_task.abort();

    // Wiring DynamicViewHolder eviction removes deleted up_a from breaker, HTTP bulkhead, and WS bulkhead.
    let principal_view = Arc::new(PrincipalView::for_tests(
        TEST_PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::new(),
    ));
    let initial = cc_lb_control::DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(WebSocketTestSignerFactory))
        .principal_view(Arc::clone(&principal_view))
        .upstream_records(vec![up_a.clone(), up_b.clone()])
        .build();
    let holder = wire_dynamic_view_eviction(
        DynamicViewHolder::new(initial),
        Arc::clone(&parts.breaker_registry),
        Arc::clone(&parts.http_bulkheads),
        Arc::clone(&parts.websocket_bulkheads),
    );
    holder.store(
        cc_lb_control::DynamicViewBuilder::new(1)
            .signer_factory(Arc::new(WebSocketTestSignerFactory))
            .principal_view(principal_view)
            .upstream_records(vec![up_b.clone()])
            .build(),
    );

    assert!(parts.breaker_registry.get(&up_a.id.to_string()).is_none());
    assert!(
        parts
            .breaker_registry
            .get(&websocket_breaker_key(up_a.id))
            .is_none()
    );
    assert!(parts.http_bulkheads.get(&up_a.id.to_string()).is_none());
    assert!(
        parts
            .websocket_bulkheads
            .get(&websocket_breaker_key(up_a.id))
            .is_none()
    );

    assert!(parts.breaker_registry.get(&up_b.id.to_string()).is_some());
    assert!(
        parts
            .breaker_registry
            .get(&websocket_breaker_key(up_b.id))
            .is_some()
    );
    assert!(parts.http_bulkheads.get(&up_b.id.to_string()).is_some());
    assert!(
        parts
            .websocket_bulkheads
            .get(&websocket_breaker_key(up_b.id))
            .is_some()
    );
}

#[tokio::test]
async fn responses_websocket_stalled_connect_on_one_lane_does_not_block_other_lane() {
    let fast_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fast");
    let fast_addr = fast_listener.local_addr().expect("fast addr");
    let release_fast = Arc::new(tokio::sync::Notify::new());
    let release_fast_task = Arc::clone(&release_fast);
    let fast_task = tokio::spawn(async move {
        let Ok((socket, _)) = fast_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(socket).await else {
            return;
        };
        let Some(Ok(_)) = ws.next().await else {
            return;
        };
        let _ = ws
            .send(UpstreamMessage::Text(
                r#"{"type":"response.created","stream_id":"lane-fast","response":{"id":"resp_fast","store":false}}"#.into(),
            ))
            .await;
        release_fast_task.notified().await;
        let _ = ws
            .send(UpstreamMessage::Text(
                r#"{"type":"response.completed","stream_id":"lane-fast","response":{"id":"resp_fast","store":false,"usage":{"input_tokens":1,"output_tokens":1}}}"#.into(),
            ))
            .await;
    });

    // Slow upstream accepts TCP socket and holds before completing HTTP 101 WS handshake.
    let slow_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind slow");
    let slow_addr = slow_listener.local_addr().expect("slow addr");
    let slow_connected = Arc::new(tokio::sync::Notify::new());
    let slow_connected_task = Arc::clone(&slow_connected);
    let release_slow = Arc::new(tokio::sync::Notify::new());
    let release_slow_task = Arc::clone(&release_slow);
    let slow_task = tokio::spawn(async move {
        let Ok((_held_socket, _)) = slow_listener.accept().await else {
            return;
        };
        slow_connected_task.notify_one();
        release_slow_task.notified().await;
    });

    let up_fast = websocket_upstream(
        1,
        "openai-fast",
        Url::parse(&format!("http://{fast_addr}/")).unwrap(),
    );
    let up_slow = websocket_upstream(
        2,
        "openai-slow",
        Url::parse(&format!("http://{slow_addr}/")).unwrap(),
    );

    let state = websocket_test_state(
        vec![up_fast],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let state_for_swap = state.clone();
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Start lane-fast and receive its first event.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-fast","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let created = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("fast created does not time out")
        .unwrap()
        .unwrap();
    assert!(
        created.to_text().unwrap().contains("resp_fast"),
        "expected resp_fast, got {created:?}"
    );

    // Route next new lane to slow_upstream, then trigger stalled connect on lane-slow.
    replace_websocket_upstreams(&state_for_swap, vec![up_slow]);
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-slow","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), slow_connected.notified())
        .await
        .expect("slow upstream accepted TCP and is holding handshake");

    // While lane-slow connect is stalled, ping/pong AND active lane-fast output MUST NOT block.
    client
        .send(UpstreamMessage::Ping(Bytes::from_static(b"nonblocking")))
        .await
        .unwrap();
    release_fast.notify_one();

    let mut saw_pong = false;
    let mut saw_completed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while (!saw_pong || !saw_completed) && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), client.next()).await {
            Ok(Some(Ok(UpstreamMessage::Pong(p)))) if p.as_ref() == b"nonblocking" => {
                saw_pong = true;
            }
            Ok(Some(Ok(UpstreamMessage::Text(t)))) if t.contains("response.completed") => {
                saw_completed = true;
            }
            other => panic!("unexpected frame while stalled connect in flight: {other:?}"),
        }
    }
    assert!(saw_pong, "pong must arrive while slow connect is stalled");
    assert!(
        saw_completed,
        "fast lane frame must arrive while slow connect is stalled"
    );

    release_slow.notify_one();
    let _ = client.close(None).await;
    proxy_task.abort();
    fast_task.abort();
    slow_task.abort();
}

#[tokio::test]
async fn responses_websocket_saturated_bulkhead_fails_over_to_next_candidate() {
    let (busy_addr, busy_cancel, busy_task) = spawn_holding_upstream().await;
    let healthy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ok");
    let healthy_addr = healthy_listener.local_addr().expect("ok addr");
    let healthy_task = tokio::spawn(async move {
        let Ok((socket, _)) = healthy_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(socket).await else {
            return;
        };
        if let Some(Ok(_)) = ws.next().await {
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp_failover","store":false}}"#
                        .into(),
                ))
                .await;
        }
    });

    let mut state = websocket_test_state(
        vec![
            websocket_upstream(
                1,
                "openai-busy",
                Url::parse(&format!("http://{busy_addr}/")).unwrap(),
            ),
            websocket_upstream(
                2,
                "openai-healthy",
                Url::parse(&format!("http://{healthy_addr}/")).unwrap(),
            ),
        ],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 50,
        semaphore_permits: 1,
        acquire_timeout: Duration::from_millis(25),
    };
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;

    // Client 1 saturates openai-busy's 1-slot bulkhead.
    let (mut client1, _) = connect_async(websocket_request(proxy_address))
        .await
        .unwrap();
    client1
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let created1 = tokio::time::timeout(Duration::from_secs(2), client1.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(created1.to_text().unwrap().contains("response.created"));

    // Client 2 cannot acquire openai-busy's bulkhead, so it fails over to openai-healthy.
    let (mut client2, _) = connect_async(websocket_request(proxy_address))
        .await
        .unwrap();
    client2
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let created2 = tokio::time::timeout(Duration::from_secs(2), client2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        created2.to_text().unwrap().contains("resp_failover"),
        "expected failover to healthy candidate, got {created2:?}"
    );

    busy_cancel.cancel();
    let _ = client1.close(None).await;
    let _ = client2.close(None).await;
    proxy_task.abort();
    busy_task.abort();
    healthy_task.abort();
}

#[tokio::test]
async fn responses_websocket_half_open_probe_429_releases_slot_without_closing_breaker() {
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let attempt = Arc::new(AtomicUsize::new(0));
    let attempt_task = Arc::clone(&attempt);
    let provider_task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let idx = attempt_task.fetch_add(1, Ordering::SeqCst);
            if idx < 2 {
                let mut req = Vec::new();
                let mut buf = [0u8; 512];
                while let Ok(n) = socket.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    req.extend_from_slice(&buf[..n]);
                    if req.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let status_line = if idx == 0 {
                    "HTTP/1.1 500 Internal Server Error"
                } else {
                    "HTTP/1.1 429 Too Many Requests"
                };
                let reply =
                    format!("{status_line}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                let _ = socket.write_all(reply.as_bytes()).await;
            } else if let Ok(mut ws) = accept_async(socket).await
                && let Some(Ok(_)) = ws.next().await
            {
                let _ = ws
                    .send(UpstreamMessage::Text(
                        r#"{"type":"response.created","response":{"id":"resp_recovered","store":false}}"#.into(),
                    ))
                    .await;
            }
        }
    });

    let up = websocket_upstream(
        1,
        "openai",
        Url::parse(&format!("http://{addr}/")).expect("url parses"),
    );
    let up_id = up.id;
    let test_clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_000));
    let mut state = websocket_test_state(
        vec![up],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.clock = test_clock.clone();
    state.breaker_config = BreakerRuntimeConfig {
        failures_to_open: 1,
        failure_window: Duration::from_secs(10),
        half_open_after: Duration::from_secs(5),
        half_open_max_in_flight: 1,
    };
    let breaker_registry = Arc::clone(&state.breaker_registry);
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .unwrap();

    // 1. Provider 500 opens the WS breaker.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let err1: Value =
        serde_json::from_str(client.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(err1["status"], 500);
    let breaker = breaker_registry
        .get(&websocket_breaker_key(up_id))
        .expect("WS breaker exists");
    assert_eq!(breaker.current_state(), cc_lb_engine::BreakerState::Open);

    // 2. Advance clock to half-open, receive HTTP 429: slot is released without closing breaker.
    test_clock.advance_secs(5);
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let err2: Value =
        serde_json::from_str(client.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(err2["status"], 429);
    assert_eq!(
        breaker.current_state(),
        cc_lb_engine::BreakerState::HalfOpen
    );
    assert_eq!(breaker.half_open_in_flight(), 0);

    // 3. Subsequent half-open probe succeeds and closes the breaker.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let ok = client.next().await.unwrap().unwrap();
    assert!(ok.to_text().unwrap().contains("resp_recovered"));
    assert_eq!(breaker.current_state(), cc_lb_engine::BreakerState::Closed);

    let _ = client.close(None).await;
    proxy_task.abort();
    provider_task.abort();
}

#[test]
fn websocket_provider_request_strips_connection_listed_hop_by_hop_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONNECTION,
        HeaderValue::from_static("x-hop-secret, keep-alive"),
    );
    headers.insert("x-hop-secret", HeaderValue::from_static("secret-value"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
    headers.insert("x-end-to-end", HeaderValue::from_static("preserved"));
    let request = responses_websocket_request(
        Url::parse("ws://provider.example/v1/responses").unwrap(),
        headers,
    )
    .expect("provider request builds");
    assert!(!request.headers().contains_key("x-hop-secret"));
    assert!(!request.headers().contains_key("keep-alive"));
    assert_eq!(request.headers()["x-end-to-end"], "preserved");
    assert!(request.headers().contains_key(CONNECTION));
    assert!(request.headers().contains_key(UPGRADE));
}

#[tokio::test]
async fn websocket_ingress_strips_connection_authorization_before_auth() {
    let router = websocket_test_router(Vec::new(), TerminalStrategy::FirstPick).await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let bearer = format!("Bearer {}", TEST_KEY.plaintext.as_str());

    let mut valid_request = websocket_request(proxy_address);
    valid_request.headers_mut().remove("x-api-key");
    valid_request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&bearer).expect("valid bearer header"),
    );
    let (mut client, _) = connect_async(valid_request)
        .await
        .expect("Bearer WS auth succeeds");
    let _ = client.close(None).await;

    let mut stripped_request = websocket_request(proxy_address);
    stripped_request.headers_mut().remove("x-api-key");
    stripped_request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&bearer).expect("valid bearer header"),
    );
    stripped_request.headers_mut().insert(
        CONNECTION,
        HeaderValue::from_static("Upgrade, Authorization"),
    );
    let err = connect_async(stripped_request)
        .await
        .expect_err("Connection: Authorization must be stripped before auth and fail with 401");
    assert!(err.to_string().contains("401"), "unexpected error: {err}");
    proxy_task.abort();
}

#[tokio::test]
async fn responses_websocket_concurrent_initial_oauth_lanes_pin_to_single_account() {
    use std::sync::atomic::AtomicUsize;

    let a_hits = Arc::new(AtomicUsize::new(0));
    let b_hits = Arc::new(AtomicUsize::new(0));
    let accepted_first_a = Arc::new(tokio::sync::Notify::new());
    let release_first_a = Arc::new(tokio::sync::Notify::new());

    let listener_a = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth-a");
    let addr_a = listener_a.local_addr().unwrap();
    let a_hits_task = Arc::clone(&a_hits);
    let accepted_task = Arc::clone(&accepted_first_a);
    let release_task = Arc::clone(&release_first_a);
    let task_a = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener_a.accept().await else {
                break;
            };
            let hit = a_hits_task.fetch_add(1, Ordering::SeqCst);
            if hit == 0 {
                accepted_task.notify_one();
                release_task.notified().await;
            }
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(socket).await else {
                    return;
                };
                if let Some(Ok(_)) = ws.next().await {
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            format!(
                                r#"{{"type":"response.created","response":{{"id":"resp_a_{hit}","store":false}}}}"#
                            )
                            .into(),
                        ))
                        .await;
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            format!(
                                r#"{{"type":"response.completed","response":{{"id":"resp_a_{hit}","store":false,"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                            )
                            .into(),
                        ))
                        .await;
                }
            });
        }
    });

    let listener_b = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth-b");
    let addr_b = listener_b.local_addr().unwrap();
    let b_hits_task = Arc::clone(&b_hits);
    let task_b = tokio::spawn(async move {
        while let Ok((socket, _)) = listener_b.accept().await {
            b_hits_task.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });

    let mut up_a = websocket_upstream(
        1,
        "oauth-a",
        Url::parse(&format!("http://{addr_a}/")).unwrap(),
    );
    up_a.kind = UpstreamKind::OpenaiOauth;
    let mut up_b = websocket_upstream(
        2,
        "oauth-b",
        Url::parse(&format!("http://{addr_b}/")).unwrap(),
    );
    up_b.kind = UpstreamKind::OpenaiOauth;

    #[derive(Default)]
    struct SwapQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for SwapQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .expect("swap quota cache lock")
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    let up_a_id = up_a.id;
    let up_b_id = up_b.id;
    let mut state = websocket_test_state(
        vec![up_a, up_b],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let now_secs = cc_lb_clock::unix_secs(state.clock.now());
    let quota_snapshot =
        |window: &str, utilization: f64| cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("oauth-pin-test".to_owned()),
            utilization: Some(utilization),
            status: Some("allowed".to_owned()),
            resets_at_unix_secs: Some(now_secs + 10_000),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(now_secs * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        };
    let quota_cache = Arc::new(SwapQuotaCache::default());
    {
        let mut guard = quota_cache.snapshots.lock().expect("swap quota cache lock");
        guard.insert(
            up_a_id,
            vec![quota_snapshot("5h", 0.10), quota_snapshot("7d", 0.10)],
        );
        guard.insert(
            up_b_id,
            vec![quota_snapshot("5h", 0.80), quota_snapshot("7d", 0.80)],
        );
    }
    let filter: Arc<dyn cc_lb_routing::FilterPlugin> = Arc::new(
        cc_lb_routing_eval::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
            state.clock.clone(),
        ),
    );
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let principal_view = Arc::new(PrincipalView::for_tests(
        TEST_PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::from([(
            TEST_PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        )]),
    ));
    let initial_view = state.dynamic_view.load();
    state.dynamic_view.store(
        cc_lb_control::DynamicViewBuilder::from_view(&initial_view)
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .build(),
    );

    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let lifecycle = Arc::try_unwrap(state.lifecycle)
        .unwrap_or_else(|_| panic!("private fixture lifecycle must have one owner"));
    state.lifecycle = Arc::new(lifecycle.with_event_bus(bus));
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Lane 1 starts initial unpinned OAuth handshake on oauth-a (0.10 vs 0.80 utilization).
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","stream_id":"lane-1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), accepted_first_a.notified())
        .await
        .expect("lane-1 dials oauth-a");
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("lane-1 route event")
            .expect("lifecycle bus open");
        if let LifecycleEvent::RouteCompleted {
            result: Ok(route), ..
        } = ev
        {
            assert_eq!(route.upstream_id, up_a_id, "lane-1 initially picks oauth-a");
            break;
        }
    }

    // Flip subscription quota snapshots so an unpinned lane-2 prefers oauth-b over oauth-a.
    // Wait for both lane-2's RouteCompleted event (proving initial choice was oauth-b) and an
    // invalid-event error response (proving the relay loop ran advance_one_dispatch and parked
    // lane-2 in `deferred_for_session_owner`) BEFORE releasing oauth-a's handshake.
    {
        let mut guard = quota_cache.snapshots.lock().expect("swap quota cache lock");
        guard.insert(
            up_a_id,
            vec![quota_snapshot("5h", 0.80), quota_snapshot("7d", 0.80)],
        );
        guard.insert(
            up_b_id,
            vec![quota_snapshot("5h", 0.10), quota_snapshot("7d", 0.10)],
        );
    }
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","stream_id":"lane-2","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("lane-2 route event")
            .expect("lifecycle bus open");
        if let LifecycleEvent::RouteCompleted {
            result: Ok(route), ..
        } = ev
        {
            assert_eq!(
                route.upstream_id, up_b_id,
                "fixture must select oauth-b for lane 2 before session pin takes effect"
            );
            break;
        }
    }
    client
        .send(UpstreamMessage::Text(r#"{"type":"invalid.sync"}"#.into()))
        .await
        .unwrap();
    let sync_reply = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("sync error returns after lane-2 admission")
        .unwrap()
        .unwrap();
    let sync_text = sync_reply.to_text().unwrap();
    assert!(
        sync_text.contains("\"invalid_request_error\""),
        "expected invalid_request_error sync sentinel, got {sync_text}"
    );
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        0,
        "lane-2 must wait for initial OAuth owner instead of dialing oauth-b"
    );

    release_first_a.notify_one();
    let mut completed = 0usize;
    while completed < 2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("both OAuth lanes complete")
            .unwrap()
            .unwrap();
        if let UpstreamMessage::Text(text) = msg {
            if text.contains("\"type\":\"error\"") {
                panic!("unexpected error frame while awaiting OAuth lanes: {text}");
            }
            if text.contains("\"response.completed\"") {
                completed += 1;
            }
        }
    }
    assert_eq!(
        a_hits.load(Ordering::SeqCst),
        2,
        "both lanes must pin to oauth-a"
    );
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        0,
        "oauth-b must never be dialed"
    );

    let _ = client.close(None).await;
    proxy_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test]
async fn responses_websocket_concurrent_shareable_lanes_coalesce_in_flight_dial() {
    use std::sync::atomic::AtomicUsize;

    let tcp_accepts = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind api-key");
    let addr = listener.local_addr().unwrap();
    let accepts_task = Arc::clone(&tcp_accepts);
    let accepted_task = Arc::clone(&accepted);
    let release_task = Arc::clone(&release);
    let up_task = tokio::spawn(async move {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        accepts_task.fetch_add(1, Ordering::SeqCst);
        accepted_task.notify_one();
        release_task.notified().await;
        let Ok(mut ws) = accept_async(socket).await else {
            return;
        };
        let mut count = 0usize;
        while let Some(Ok(msg)) = ws.next().await {
            if let UpstreamMessage::Text(req_text) = msg {
                count += 1;
                let req_json: Value = serde_json::from_str(&req_text).expect("valid create json");
                let stream_id = req_json
                    .get("stream_id")
                    .and_then(Value::as_str)
                    .expect("request includes stream_id");
                let _ = ws
                    .send(UpstreamMessage::Text(
                        format!(
                            r#"{{"type":"response.created","stream_id":"{stream_id}","response":{{"id":"resp_shared_{count}","store":false}}}}"#
                        )
                        .into(),
                    ))
                    .await;
                let _ = ws
                    .send(UpstreamMessage::Text(
                        format!(
                            r#"{{"type":"response.completed","stream_id":"{stream_id}","response":{{"id":"resp_shared_{count}","store":false,"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                        )
                        .into(),
                    ))
                    .await;
            }
        }
    });

    let up = websocket_upstream(
        1,
        "openai-shared",
        Url::parse(&format!("http://{addr}/")).unwrap(),
    );
    let mut state = websocket_test_state(
        vec![up.clone()],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 2,
        semaphore_permits: 2,
        acquire_timeout: Duration::from_secs(1),
    };
    // Hold 1 of 2 bulkhead permits so only 1 permit remains for Turn 1 + Turn 2 together.
    let _held_guard = state
        .websocket_bulkheads
        .semaphore_only_bulkhead(
            crate::app::responses_websocket::websocket_breaker_key(up.id),
            state.websocket_bulkhead_config,
        )
        .acquire_owned()
        .await
        .expect("acquire initial bulkhead permit");
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Turn 1 opens in-flight shareable dial using the sole bulkhead permit.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), accepted.notified())
        .await
        .expect("dial 1 accepted");

    // Turn 2 arrives before dial 1 upgrades; Ping/Pong ensures relay processed Turn 2 and
    // coalesced it onto the in-flight dial without opening a duplicate socket or failing 503.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-2","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    client
        .send(UpstreamMessage::Ping(Bytes::from_static(b"coalesce")))
        .await
        .unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(pong, UpstreamMessage::Pong(_)),
        "expected Pong after Turn 2 coalesced, got {pong:?}"
    );

    release.notify_one();
    let mut completed = 0usize;
    let mut seen_frames = Vec::new();
    while completed < 2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .unwrap_or_else(|_| {
                panic!("both coalesced turns complete; seen_frames={seen_frames:?}")
            })
            .unwrap()
            .unwrap();
        if let UpstreamMessage::Text(text) = msg {
            assert!(
                !text.contains("\"type\":\"error\""),
                "unexpected error frame during coalesced dispatch: {text}"
            );
            seen_frames.push(text.to_string());
            if text.contains("\"response.completed\"") {
                completed += 1;
            }
        }
    }
    assert_eq!(
        tcp_accepts.load(Ordering::SeqCst),
        1,
        "must coalesce onto 1 TCP/WS dial"
    );

    let _ = client.close(None).await;
    proxy_task.abort();
    up_task.abort();
}

#[tokio::test]
async fn responses_websocket_registers_connection_before_early_provider_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind oauth");
    let addr = listener.local_addr().unwrap();
    let up_task = tokio::spawn(async move {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(socket).await else {
            return;
        };
        // Push an immediate unsolicited frame as soon as the WebSocket upgrade finishes.
        let _ = ws
            .send(UpstreamMessage::Text(
                r#"{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":42.0,"window_minutes":300,"reset_at":1900000000},"secondary":{"used_percent":10.0,"window_minutes":10080,"reset_at":1900500000}},"credits":{"has_credits":true,"unlimited":false,"balance":"12.00"}}"#.into(),
            ))
            .await;
        if let Some(Ok(_)) = ws.next().await {
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp_early","store":false}}"#
                        .into(),
                ))
                .await;
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_early","store":false,"usage":{"input_tokens":1,"output_tokens":1}}}"#.into(),
                ))
                .await;
        }
    });

    let mut up = websocket_upstream(
        1,
        "oauth-early",
        Url::parse(&format!("http://{addr}/")).unwrap(),
    );
    up.kind = UpstreamKind::OpenaiOauth;
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut quota_events = bus.attach_lifecycle_subscription_quota(16);
    let mut state = websocket_test_state(
        vec![up.clone()],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let lifecycle = Arc::new(
        Lifecycle::new_with_dynamic_view(
            Arc::new(BuiltinAuthn::new(
                Arc::new(KeyStore::new(Arc::new(FixtureKeyStore))),
                state.clock.clone(),
            )),
            state.dynamic_view.clone(),
            make_default_dispatcher(50),
            LifecycleConfig::default(),
            state.clock.clone(),
        )
        .with_event_bus(bus),
    );
    state.lifecycle = lifecycle;
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("response arrives")
            .unwrap()
            .unwrap();
        if let UpstreamMessage::Text(text) = msg
            && text.contains("\"response.completed\"")
        {
            break;
        }
    }
    let mut observed_early_quota = false;
    while let Ok(event) = quota_events.try_recv() {
        if matches!(
            event,
            cc_lb_lifecycle::LifecycleEvent::UpstreamQuotaFrameObserved { upstream_id, .. }
                if upstream_id == up.id
        ) {
            observed_early_quota = true;
            break;
        }
    }
    assert!(
        observed_early_quota,
        "early codex.rate_limits frame emitted right after upgrade must be observed on upstream {}",
        up.id
    );

    let _ = client.close(None).await;
    proxy_task.abort();
    up_task.abort();
}

#[tokio::test]
async fn responses_websocket_slow_signer_refresh_does_not_block_active_lane() {
    #[derive(Clone)]
    struct SelectableDelayedSignerFactory {
        delayed_upstream_name: String,
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl ApiKeyAwareSignerFactory for SelectableDelayedSignerFactory {
        fn with_router_choice(
            &self,
            router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(SelectableDelayedSignerFactoryChoice {
                inner: self.clone(),
                choice: router_chosen_upstream_name,
            })
        }
    }

    #[derive(Clone)]
    struct SelectableDelayedSignerFactoryChoice {
        inner: SelectableDelayedSignerFactory,
        choice: String,
    }

    #[async_trait]
    impl SignerFactory for SelectableDelayedSignerFactoryChoice {
        async fn build(
            &self,
            _upstream: &cc_lb_domain::Upstream,
        ) -> Result<Arc<dyn Signer>, SignerError> {
            if self.choice == self.inner.delayed_upstream_name {
                self.inner.started.notify_one();
                self.inner.release.notified().await;
            }
            Ok(Arc::new(WebSocketTestSigner))
        }
    }

    let fast_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fast");
    let fast_addr = fast_listener.local_addr().unwrap();
    let release_fast = Arc::new(tokio::sync::Notify::new());
    let release_fast_task = Arc::clone(&release_fast);
    let fast_task = tokio::spawn(async move {
        let Ok((socket, _)) = fast_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(socket).await else {
            return;
        };
        if let Some(Ok(_)) = ws.next().await {
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","stream_id":"lane-fast","response":{"id":"resp_fast_sign","store":false}}"#.into(),
                ))
                .await;
            release_fast_task.notified().await;
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.completed","stream_id":"lane-fast","response":{"id":"resp_fast_sign","store":false,"usage":{"input_tokens":1,"output_tokens":1}}}"#.into(),
                ))
                .await;
        }
    });

    let slow_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind slow");
    let slow_addr = slow_listener.local_addr().unwrap();
    let up_fast = websocket_upstream(
        1,
        "openai-fast",
        Url::parse(&format!("http://{fast_addr}/")).unwrap(),
    );
    let mut up_slow = websocket_upstream(
        2,
        "openai-slow-signer",
        Url::parse(&format!("http://{slow_addr}/")).unwrap(),
    );
    up_slow.kind = UpstreamKind::OpenaiOauth;

    let signer_started = Arc::new(tokio::sync::Notify::new());
    let signer_release = Arc::new(tokio::sync::Notify::new());
    let state = websocket_test_state(
        vec![up_fast],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(SelectableDelayedSignerFactory {
            delayed_upstream_name: "openai-slow-signer".to_owned(),
            started: Arc::clone(&signer_started),
            release: Arc::clone(&signer_release),
        }),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let state_for_swap = state.clone();
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Start fast lane and receive created frame.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-fast","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    let created = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(created.to_text().unwrap().contains("resp_fast_sign"));

    // Swap dynamic view to slow-signer upstream and start a slow lane.
    replace_websocket_upstreams(&state_for_swap, vec![up_slow]);
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","stream_id":"lane-slow","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), signer_started.notified())
        .await
        .expect("slow signer entered build() off relay loop");

    // While slow signer blocks off-loop, ping/pong and lane-fast completion proceed unblocked.
    client
        .send(UpstreamMessage::Ping(Bytes::from_static(
            b"unblocked-by-sign",
        )))
        .await
        .unwrap();
    release_fast.notify_one();

    let mut saw_pong = false;
    let mut saw_completed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while (!saw_pong || !saw_completed) && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), client.next()).await {
            Ok(Some(Ok(UpstreamMessage::Pong(p)))) if p.as_ref() == b"unblocked-by-sign" => {
                saw_pong = true;
            }
            Ok(Some(Ok(UpstreamMessage::Text(t)))) if t.contains("response.completed") => {
                saw_completed = true;
            }
            other => panic!("unexpected frame while signer build blocked: {other:?}"),
        }
    }
    assert!(saw_pong && saw_completed);

    signer_release.notify_one();
    let _ = client.close(None).await;
    proxy_task.abort();
    fast_task.abort();
}

#[tokio::test]
async fn responses_websocket_coalesced_prepare_failure_only_fails_affected_turn() {
    use std::sync::atomic::AtomicUsize;

    let tcp_accepts = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind api-key");
    let addr = listener.local_addr().unwrap();
    let accepts_task = Arc::clone(&tcp_accepts);
    let up_task = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            accepts_task.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(socket).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if let UpstreamMessage::Text(req_text) = msg {
                        let req_json: Value =
                            serde_json::from_str(&req_text).expect("valid create json");
                        let stream_id = req_json
                            .get("stream_id")
                            .and_then(Value::as_str)
                            .expect("request includes stream_id");
                        let _ = ws
                            .send(UpstreamMessage::Text(
                                format!(
                                    r#"{{"type":"response.created","stream_id":"{stream_id}","response":{{"id":"resp_{stream_id}","store":false}}}}"#
                                )
                                .into(),
                            ))
                            .await;
                        let _ = ws
                            .send(UpstreamMessage::Text(
                                format!(
                                    r#"{{"type":"response.completed","stream_id":"{stream_id}","response":{{"id":"resp_{stream_id}","store":false,"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                                )
                                .into(),
                            ))
                            .await;
                    }
                }
            });
        }
    });

    let up = websocket_upstream(
        1,
        "openai-shared-prepare-isolation",
        Url::parse(&format!("http://{addr}/")).unwrap(),
    );
    let mut state = websocket_test_state(
        vec![up.clone()],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 2,
        semaphore_permits: 2,
        acquire_timeout: Duration::from_secs(5),
    };
    let bulkhead = state.websocket_bulkheads.semaphore_only_bulkhead(
        crate::app::responses_websocket::websocket_breaker_key(up.id),
        state.websocket_bulkhead_config,
    );
    let held_guard_1 = bulkhead
        .acquire_owned()
        .await
        .expect("hold first bulkhead permit");
    let held_guard_2 = bulkhead
        .acquire_owned()
        .await
        .expect("hold second bulkhead permit");
    let dynamic_view = Arc::clone(&state.dynamic_view);
    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Lane 1 (`gpt-4.1`) starts the pending dial and blocks waiting for the sole bulkhead permit.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    client
        .send(UpstreamMessage::Text(r#"{"type":"invalid.sync"}"#.into()))
        .await
        .unwrap();
    let sync1 = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        sync1
            .to_text()
            .unwrap()
            .contains("\"invalid_request_error\"")
    );

    // Lane 2 (`gpt-5`) coalesces onto Lane 1's PendingConnect while bulkhead permit is still held.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","stream_id":"lane-2","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    client
        .send(UpstreamMessage::Text(r#"{"type":"invalid.sync"}"#.into()))
        .await
        .unwrap();
    let sync2 = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        sync2
            .to_text()
            .unwrap()
            .contains("\"invalid_request_error\"")
    );

    // Restrict the principal allowlist to `gpt-5` only before releasing the bulkhead permit so
    // Lane 1 fails inside `PreparedWsConnectJob::run()` with `model_not_allowed` while Lane 2 stays allowed.
    let updated_pv = Arc::new(
        cc_lb_control::api_keys::principal_view::PrincipalView::for_tests(
            TEST_PRINCIPAL,
            true,
            vec!["gpt-5".to_owned()],
            Vec::new(),
            HashMap::from([(
                TEST_PRINCIPAL.to_owned(),
                (
                    Some(Arc::new(
                        cc_lb_control::api_keys::principal_view::RouterPipelineCache {
                            user_filters: Vec::new(),
                            terminal: TerminalStrategy::FirstPick,
                            instantiation_error: None,
                            provider_filters: None,
                        },
                    )),
                    cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
                ),
            )]),
        ),
    );
    let view = dynamic_view.load();
    dynamic_view.store(
        cc_lb_control::DynamicViewBuilder::from_view(&view)
            .principal_view(updated_pv)
            .build(),
    );

    drop((held_guard_1, held_guard_2));

    let mut lane1_failed_403 = false;
    let mut lane2_completed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while (!lane1_failed_403 || !lane2_completed) && tokio::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("turns settle")
            .unwrap()
            .unwrap();
        if let UpstreamMessage::Text(text) = msg {
            if text.contains("\"lane-1\"") && text.contains("\"model_not_allowed\"") {
                lane1_failed_403 = true;
            } else if text.contains("\"lane-2\"") && text.contains("\"response.completed\"") {
                lane2_completed = true;
            } else if text.contains("\"lane-2\"") && text.contains("\"type\":\"error\"") {
                panic!("lane-2 waiter must not inherit lane-1 prepare failure: {text}");
            }
        }
    }
    assert!(lane1_failed_403, "lane-1 must fail with model_not_allowed");
    assert!(lane2_completed, "lane-2 must revalidate and succeed");
    assert_eq!(tcp_accepts.load(Ordering::SeqCst), 1);

    let _ = client.close(None).await;
    proxy_task.abort();
    up_task.abort();
}

#[tokio::test]
async fn responses_websocket_deferred_explicit_affinity_preserves_target_upstream() {
    use std::sync::atomic::AtomicUsize;

    let a_hits = Arc::new(AtomicUsize::new(0));
    let b_hits = Arc::new(AtomicUsize::new(0));
    let accepted_first_a = Arc::new(tokio::sync::Notify::new());
    let release_first_a = Arc::new(tokio::sync::Notify::new());

    let listener_a = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth-a");
    let addr_a = listener_a.local_addr().unwrap();
    let a_hits_task = Arc::clone(&a_hits);
    let accepted_task = Arc::clone(&accepted_first_a);
    let release_task = Arc::clone(&release_first_a);
    let task_a = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener_a.accept().await else {
                break;
            };
            let hit = a_hits_task.fetch_add(1, Ordering::SeqCst);
            if hit == 0 {
                accepted_task.notify_one();
                release_task.notified().await;
            }
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(socket).await else {
                    return;
                };
                if let Some(Ok(_)) = ws.next().await {
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            format!(
                                r#"{{"type":"response.created","stream_id":"lane-1","response":{{"id":"resp_a_{hit}","store":false}}}}"#
                            )
                            .into(),
                        ))
                        .await;
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            format!(
                                r#"{{"type":"response.completed","stream_id":"lane-1","response":{{"id":"resp_a_{hit}","store":false,"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                            )
                            .into(),
                        ))
                        .await;
                }
            });
        }
    });

    let listener_b = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth-b");
    let addr_b = listener_b.local_addr().unwrap();
    let b_hits_task = Arc::clone(&b_hits);
    let task_b = tokio::spawn(async move {
        while let Ok((socket, _)) = listener_b.accept().await {
            let hit = b_hits_task.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(socket).await else {
                    return;
                };
                if let Some(Ok(_)) = ws.next().await {
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            format!(
                                r#"{{"type":"response.created","stream_id":"lane-2","response":{{"id":"resp_b_{hit}","store":false}}}}"#
                            )
                            .into(),
                        ))
                        .await;
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            format!(
                                r#"{{"type":"response.completed","stream_id":"lane-2","response":{{"id":"resp_b_{hit}","store":false,"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                            )
                            .into(),
                        ))
                        .await;
                }
            });
        }
    });

    let mut up_a = websocket_upstream(
        1,
        "oauth-a-explicit",
        Url::parse(&format!("http://{addr_a}/")).unwrap(),
    );
    up_a.kind = UpstreamKind::OpenaiOauth;
    let mut up_b = websocket_upstream(
        2,
        "oauth-b-explicit",
        Url::parse(&format!("http://{addr_b}/")).unwrap(),
    );
    up_b.kind = UpstreamKind::OpenaiOauth;

    let temp_dir = tempfile::tempdir().expect("tempdir for affinity sqlite");
    let db_path = temp_dir.path().join("affinity.sqlite");
    let db_url = format!("sqlite://{}", db_path.display());

    let up_a_id = up_a.id;
    let up_b_id = up_b.id;
    let state = websocket_test_state_with_affinity_store(
        vec![up_a, up_b],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
        Some(db_url),
    )
    .await;

    let owned_cipher = "gAAAAAB_durable_b_owned_cipher";
    let key = {
        use sha2::Digest as _;
        cc_lb_storage_api::UpstreamAffinityKey {
            principal_id: TEST_PRINCIPAL.to_owned(),
            provider: "openai".to_owned(),
            kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
            value_sha256: sha2::Sha256::digest(owned_cipher.as_bytes()).into(),
        }
    };
    let now_secs = cc_lb_clock::unix_secs(state.clock.now());
    state
        .storage
        .bind_upstream_affinities(
            &[cc_lb_storage_api::UpstreamAffinityBinding {
                key,
                upstream_id: up_b_id,
                owner_epoch: 0,
                observed_at_unix_secs: now_secs,
                expires_at_unix_secs: Some(now_secs + 3600),
            }],
            now_secs,
            3600,
        )
        .await
        .expect("seed durable affinity to oauth-b");

    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Lane 1 starts initial OAuth handshake on oauth-a (first pick).
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","stream_id":"lane-1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), accepted_first_a.notified())
        .await
        .expect("lane-1 dials oauth-a");

    // Lane 2 has durable explicit affinity to oauth-b; while deferred behind Lane 1's handshake,
    // releasing Lane 1's handshake to oauth-a must preserve Lane 2's explicit target oauth-b.
    client
        .send(UpstreamMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-2",
                "store": false,
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_durable_b",
                        "summary": [],
                        "content": [],
                        "encrypted_content": owned_cipher
                    },
                    {"role": "user", "content": "hi"}
                ]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    client
        .send(UpstreamMessage::Text(r#"{"type":"invalid.sync"}"#.into()))
        .await
        .unwrap();
    let sync_reply = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("sync sentinel after lane-2 deferral")
        .unwrap()
        .unwrap();
    let sync_text = sync_reply.to_text().unwrap();
    assert!(
        sync_text.contains("\"invalid_request_error\"") && sync_text.contains("invalid.sync"),
        "expected invalid.sync reply, got: {sync_text}"
    );
    assert_eq!(b_hits.load(Ordering::SeqCst), 0);

    release_first_a.notify_one();
    let mut completed = 0usize;
    while completed < 2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("both explicit and session lanes complete")
            .unwrap()
            .unwrap();
        if let UpstreamMessage::Text(text) = msg {
            if text.contains("\"upstream_affinity_unavailable\"")
                || text.contains("\"type\":\"error\"")
            {
                panic!("explicit affinity deferred lane must not fail: {text}");
            }
            if text.contains("\"response.completed\"") {
                completed += 1;
            }
        }
    }
    assert_eq!(a_hits.load(Ordering::SeqCst), 1, "lane-1 dials oauth-a");
    assert_eq!(b_hits.load(Ordering::SeqCst), 1, "lane-2 dials oauth-b");
    let _ = up_a_id;

    let _ = client.close(None).await;
    proxy_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test]
async fn responses_websocket_deferred_unestablished_reasoning_preserves_ciphertext() {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    let a_hits = Arc::new(AtomicUsize::new(0));
    let b_hits = Arc::new(AtomicUsize::new(0));
    let lane2_saw_verbatim_first = Arc::new(AtomicBool::new(false));
    let accepted_first_a = Arc::new(tokio::sync::Notify::new());
    let release_first_a = Arc::new(tokio::sync::Notify::new());
    let deferred_cipher = "gAAAAAB_deferred_lane2_cipher";

    let listener_a = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth-a");
    let addr_a = listener_a.local_addr().unwrap();
    let a_hits_task = Arc::clone(&a_hits);
    let accepted_task = Arc::clone(&accepted_first_a);
    let release_task = Arc::clone(&release_first_a);
    let verbatim_task = Arc::clone(&lane2_saw_verbatim_first);
    let task_a = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener_a.accept().await else {
                break;
            };
            let hit = a_hits_task.fetch_add(1, Ordering::SeqCst);
            if hit == 0 {
                accepted_task.notify_one();
                release_task.notified().await;
            }
            let verbatim_conn = Arc::clone(&verbatim_task);
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(socket).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if let UpstreamMessage::Text(req_text) = msg {
                        let req_stream = if hit == 0 { "lane-1" } else { "lane-2" };
                        if req_text.contains(deferred_cipher) {
                            verbatim_conn.store(true, Ordering::SeqCst);
                            let _ = ws
                                .send(UpstreamMessage::Text(
                                    r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":"cannot verify encrypted reasoning"}}"#.into(),
                                ))
                                .await;
                            continue;
                        }
                        let _ = ws
                            .send(UpstreamMessage::Text(
                                format!(
                                    r#"{{"type":"response.created","stream_id":"{req_stream}","response":{{"id":"resp_a_{hit}_{req_stream}","store":false}}}}"#
                                )
                                .into(),
                            ))
                            .await;
                        let _ = ws
                            .send(UpstreamMessage::Text(
                                format!(
                                    r#"{{"type":"response.completed","stream_id":"{req_stream}","response":{{"id":"resp_a_{hit}_{req_stream}","store":false,"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                                )
                                .into(),
                            ))
                            .await;
                    }
                }
            });
        }
    });

    let listener_b = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth-b");
    let addr_b = listener_b.local_addr().unwrap();
    let b_hits_task = Arc::clone(&b_hits);
    let task_b = tokio::spawn(async move {
        while let Ok((socket, _)) = listener_b.accept().await {
            b_hits_task.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });

    let mut up_a = websocket_upstream(
        1,
        "oauth-a-reason",
        Url::parse(&format!("http://{addr_a}/")).unwrap(),
    );
    up_a.kind = UpstreamKind::OpenaiOauth;
    let mut up_b = websocket_upstream(
        2,
        "oauth-b-reason",
        Url::parse(&format!("http://{addr_b}/")).unwrap(),
    );
    up_b.kind = UpstreamKind::OpenaiOauth;

    #[derive(Default)]
    struct SwapQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for SwapQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .expect("swap quota cache lock")
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    let temp_dir = tempfile::tempdir().expect("tempdir for unestablished reasoning sqlite");
    let db_path = temp_dir.path().join("unestablished-reasoning.sqlite");
    let db_url = format!("sqlite://{}", db_path.display());

    let up_a_id = up_a.id;
    let up_b_id = up_b.id;
    let state = websocket_test_state_with_affinity_store(
        vec![up_a, up_b],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
        Some(db_url),
    )
    .await;
    let now_secs = cc_lb_clock::unix_secs(state.clock.now());
    let quota_snapshot =
        |window: &str, utilization: f64| cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("oauth-reason-test".to_owned()),
            utilization: Some(utilization),
            status: Some("allowed".to_owned()),
            resets_at_unix_secs: Some(now_secs + 10_000),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(now_secs * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        };
    let quota_cache = Arc::new(SwapQuotaCache::default());
    {
        let mut guard = quota_cache.snapshots.lock().expect("swap quota cache lock");
        guard.insert(
            up_a_id,
            vec![quota_snapshot("5h", 0.10), quota_snapshot("7d", 0.10)],
        );
        guard.insert(
            up_b_id,
            vec![quota_snapshot("5h", 0.80), quota_snapshot("7d", 0.80)],
        );
    }
    let filter: Arc<dyn cc_lb_routing::FilterPlugin> = Arc::new(
        cc_lb_routing_eval::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
            state.clock.clone(),
        ),
    );
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let principal_view = Arc::new(PrincipalView::for_tests(
        TEST_PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::from([(
            TEST_PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        )]),
    ));
    let initial_view = state.dynamic_view.load();
    state.dynamic_view.store(
        cc_lb_control::DynamicViewBuilder::from_view(&initial_view)
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .build(),
    );

    let (proxy_address, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("client WS connects");

    // Lane 1 begins initial OAuth handshake on oauth-a.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5","stream_id":"lane-1","store":false,"input":[]}"#.into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), accepted_first_a.notified())
        .await
        .expect("lane-1 dials oauth-a");

    // Swap quota so Lane 2 initially picks [oauth-b, oauth-a] at admit time before pinning to oauth-a.
    {
        let mut guard = quota_cache.snapshots.lock().expect("swap quota cache lock");
        guard.insert(
            up_a_id,
            vec![quota_snapshot("5h", 0.80), quota_snapshot("7d", 0.80)],
        );
        guard.insert(
            up_b_id,
            vec![quota_snapshot("5h", 0.10), quota_snapshot("7d", 0.10)],
        );
    }
    client
        .send(UpstreamMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-2",
                "store": false,
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_deferred",
                        "summary": [],
                        "content": [],
                        "encrypted_content": deferred_cipher
                    },
                    {"role": "user", "content": "hi"}
                ]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    client
        .send(UpstreamMessage::Text(r#"{"type":"invalid.sync"}"#.into()))
        .await
        .unwrap();
    let sync_reply = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let sync_text = sync_reply.to_text().unwrap();
    assert!(
        sync_text.contains("\"invalid_request_error\""),
        "expected invalid.sync reply, got: {sync_text}"
    );

    release_first_a.notify_one();
    let mut completed = 0usize;
    while completed < 2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("both OAuth lanes complete")
            .unwrap()
            .unwrap();
        if let UpstreamMessage::Text(text) = msg {
            if text.contains("\"type\":\"error\"") {
                panic!("unexpected error frame while awaiting OAuth lanes: {text}");
            }
            if text.contains("\"response.completed\"") {
                completed += 1;
            }
        }
    }
    assert!(
        lane2_saw_verbatim_first.load(Ordering::SeqCst),
        "pinned deferred lane-2 must send original encrypted_content verbatim first and strip only after 400"
    );
    assert_eq!(a_hits.load(Ordering::SeqCst), 2);
    assert_eq!(b_hits.load(Ordering::SeqCst), 0);

    let _ = client.close(None).await;
    proxy_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test]
async fn responses_websocket_active_turn_finishes_during_drain_and_force_closes_on_deadline() {
    // Phase 1: an in-flight turn completes normally past `drain.trigger()` while
    // new `response.create` frames on the same socket are rejected with 503 draining.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let (delta_sent_tx, delta_sent_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let provider_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("provider accepts");
        let mut ws = accept_async(stream).await.expect("provider handshake");
        let frame = ws
            .next()
            .await
            .expect("provider receives create")
            .expect("valid text");
        assert!(frame.to_text().expect("text").contains("response.create"));
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"resp-1","store":false}}"#.into(),
        ))
        .await
        .expect("provider sends created");
        let _ = delta_sent_tx.send(());
        let _ = finish_rx.await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.completed","response":{"id":"resp-1","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#
                .into(),
        ))
        .await
        .expect("provider sends completed");
        let _ = ws.close(None).await;
    });

    let drain = DrainController::new();
    let router = websocket_test_router_with_drain(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain.clone(),
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("send initial create");

    tokio::time::timeout(Duration::from_secs(5), delta_sent_rx)
        .await
        .expect("provider sends created")
        .expect("created signal");
    let first_created = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives created")
        .expect("created frame")
        .expect("valid ws frame");
    assert!(
        first_created
            .to_text()
            .expect("text")
            .contains("response.created")
    );

    drain.trigger();
    assert_eq!(drain.in_flight(), 1);

    // New turn creation during drain must be rejected cleanly with `draining`.
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","stream_id":"during-drain","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("send second create");
    let reject_frame = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives drain error")
        .expect("reject frame")
        .expect("valid ws frame");
    let reject_text = reject_frame.to_text().expect("error text");
    assert!(
        reject_text.contains("\"draining\"") && reject_text.contains("\"during-drain\""),
        "expected 503 draining error for new turn during drain, got: {reject_text}"
    );
    assert_eq!(drain.in_flight(), 1);

    // Let active turn complete: connection finishes and releases drain guard automatically.
    let _ = finish_tx.send(());
    let completed = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives completion")
        .expect("completed frame")
        .expect("valid ws frame");
    assert!(
        completed
            .to_text()
            .expect("text")
            .contains("response.completed")
    );
    tokio::time::timeout(Duration::from_secs(5), provider_task)
        .await
        .expect("provider finishes")
        .expect("provider task");
    assert!(!drain.await_drained(Duration::from_secs(5)).await);
    let _ = client.close(None).await;
    proxy_task.abort();

    // Phase 2: a stalled active turn survives `trigger()` and closes on `mark_force_closed()`.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let (stalled_tx, stalled_rx) = tokio::sync::oneshot::channel();
    let provider_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("provider accepts");
        let mut ws = accept_async(stream).await.expect("provider handshake");
        let _ = ws.next().await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"resp-stalled","store":false}}"#.into(),
        ))
        .await
        .expect("provider sends created");
        let _ = stalled_tx.send(());
        // Hold open until force close drops the upstream connection.
        while ws.next().await.is_some() {}
    });

    let drain = DrainController::new();
    let router = websocket_test_router_with_drain(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain.clone(),
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("send stalled turn create");
    tokio::time::timeout(Duration::from_secs(5), stalled_rx)
        .await
        .expect("provider sends stalled delta")
        .expect("stalled signal");
    let _ = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives stalled delta");

    drain.trigger();
    assert_eq!(drain.in_flight(), 1);
    assert_eq!(drain.mark_force_closed(), 1);
    assert!(!drain.await_drained(Duration::from_secs(5)).await);
    let _ = tokio::time::timeout(Duration::from_secs(5), provider_task).await;
    proxy_task.abort();
}

#[tokio::test]
async fn responses_websocket_pending_handshake_rechecks_drain_before_first_write() {
    use std::sync::atomic::AtomicUsize;

    let provider_writes = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let accepted_task = Arc::clone(&accepted);
    let release_task = Arc::clone(&release);
    let writes_task = Arc::clone(&provider_writes);
    let provider_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("provider accepts TCP");
        accepted_task.notify_one();
        release_task.notified().await;
        if let Ok(mut ws) = accept_async(stream).await
            && let Ok(Some(Ok(_))) =
                tokio::time::timeout(Duration::from_millis(250), ws.next()).await
        {
            writes_task.fetch_add(1, Ordering::SeqCst);
        }
    });

    let drain = DrainController::new();
    let router = websocket_test_router_with_drain(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain.clone(),
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake");

    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","stream_id":"race-drain","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("send create");
    tokio::time::timeout(Duration::from_secs(2), accepted.notified())
        .await
        .expect("pending connect accepted TCP before completing WS handshake");

    // Trigger drain, then immediately release the provider WS upgrade so `connect_tasks`
    // can complete right alongside the drain notification.
    drain.trigger();
    release.notify_one();

    let reject = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives drain rejection")
        .expect("frame present")
        .expect("valid WS frame");
    let text = reject.to_text().expect("error text");
    assert!(
        text.contains("\"draining\"") && text.contains("\"race-drain\""),
        "expected 503 draining error, got: {text}"
    );
    assert!(
        !drain.await_drained(Duration::from_secs(2)).await,
        "relay must release drain guard cleanly once pending turn settles"
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), provider_task).await;
    assert_eq!(
        provider_writes.load(Ordering::SeqCst),
        0,
        "provider must not receive a first write after drain triggered"
    );

    let _ = client.close(None).await;
    proxy_task.abort();
}

#[tokio::test]
async fn responses_websocket_drain_waits_for_pending_error_terminal_response_failed() {
    // Part 1: attributable `error` arrives after `drain.trigger()` (exercises main loop exit gate).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let (send_error_tx, send_error_rx) = tokio::sync::oneshot::channel();
    let (send_failed_tx, send_failed_rx) = tokio::sync::oneshot::channel();
    let provider_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("provider accepts");
        let mut ws = accept_async(stream).await.expect("provider handshake");
        let _ = ws.next().await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"resp-err-drain","store":false}}"#
                .into(),
        ))
        .await
        .expect("provider sends created");
        let _ = send_error_rx.await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"error","response_id":"resp-err-drain","status":503,"error":{"code":"server_is_overloaded","message":"Overloaded"}}"#
                .into(),
        ))
        .await
        .expect("provider sends attributable error");
        let _ = send_failed_rx.await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.failed","response":{"id":"resp-err-drain","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"},"usage":{"input_tokens":12,"output_tokens":3}}}"#
                .into(),
        ))
        .await
        .expect("provider sends trailing response.failed");
        let _ = ws.close(None).await;
    });

    let drain = DrainController::new();
    let router = websocket_test_router_with_drain(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain.clone(),
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("send create");

    let created = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives created")
        .expect("created frame")
        .expect("valid WS frame");
    assert!(
        created
            .to_text()
            .expect("text")
            .contains("response.created")
    );

    drain.trigger();
    assert_eq!(drain.in_flight(), 1);
    let _ = send_error_tx.send(());

    let error_frame = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives error")
        .expect("error frame")
        .expect("valid WS frame");
    assert!(
        error_frame
            .to_text()
            .expect("text")
            .contains("\"server_is_overloaded\"")
    );
    // Give the relay a tick to re-evaluate loop exit guards while response.failed is pending.
    tokio::task::yield_now().await;
    assert_eq!(
        drain.in_flight(),
        1,
        "relay must stay open during drain while pending error terminal awaits response.failed"
    );

    let _ = send_failed_tx.send(());
    let failed_frame = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("client receives trailing response.failed during drain")
        .expect("failed frame")
        .expect("valid WS frame");
    assert!(
        failed_frame
            .to_text()
            .expect("text")
            .contains("response.failed")
    );
    tokio::time::timeout(Duration::from_secs(5), provider_task)
        .await
        .expect("provider finishes")
        .expect("provider task");
    assert!(
        !drain.await_drained(Duration::from_secs(5)).await,
        "relay must exit cleanly once trailing response.failed finishes"
    );
    let _ = client.close(None).await;
    proxy_task.abort();

    // Part 2: `drain.trigger()` arrives after `error` is forwarded (exercises `wait_for_draining` exit gate).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider address is available");
    let (send_failed_tx2, send_failed_rx2) = tokio::sync::oneshot::channel();
    let provider_task2 = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("provider accepts");
        let mut ws = accept_async(stream).await.expect("provider handshake");
        let _ = ws.next().await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"resp-err-before-drain","store":false}}"#
                .into(),
        ))
        .await
        .expect("provider sends created");
        ws.send(UpstreamMessage::Text(
            r#"{"type":"error","response_id":"resp-err-before-drain","status":503,"error":{"code":"server_is_overloaded","message":"Overloaded"}}"#
                .into(),
        ))
        .await
        .expect("provider sends attributable error");
        let _ = send_failed_rx2.await;
        ws.send(UpstreamMessage::Text(
            r#"{"type":"response.failed","response":{"id":"resp-err-before-drain","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"},"usage":{"input_tokens":7,"output_tokens":2}}}"#
                .into(),
        ))
        .await
        .expect("provider sends trailing response.failed");
        let _ = ws.close(None).await;
    });

    let drain2 = DrainController::new();
    let router2 = websocket_test_router_with_drain(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse(&format!("http://{address}/")).expect("upstream URL parses"),
        )],
        TerminalStrategy::FirstPick,
        drain2.clone(),
    )
    .await;
    let (proxy_address2, proxy_task2) = spawn_proxy_router(router2).await;
    let (mut client2, _) = connect_async(websocket_request(proxy_address2))
        .await
        .expect("downstream handshake");
    client2
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("send create");

    let created2 = tokio::time::timeout(Duration::from_secs(5), client2.next())
        .await
        .expect("client receives created")
        .expect("created frame")
        .expect("valid WS frame");
    assert!(
        created2
            .to_text()
            .expect("text")
            .contains("response.created")
    );

    let error_frame2 = tokio::time::timeout(Duration::from_secs(5), client2.next())
        .await
        .expect("client receives error before drain")
        .expect("error frame")
        .expect("valid WS frame");
    assert!(
        error_frame2
            .to_text()
            .expect("text")
            .contains("\"server_is_overloaded\"")
    );

    drain2.trigger();
    tokio::task::yield_now().await;
    assert_eq!(
        drain2.in_flight(),
        1,
        "wait_for_draining branch must keep relay open while pending error terminal awaits response.failed"
    );

    let _ = send_failed_tx2.send(());
    let failed_frame2 = tokio::time::timeout(Duration::from_secs(5), client2.next())
        .await
        .expect("client receives trailing response.failed after drain")
        .expect("failed frame")
        .expect("valid WS frame");
    assert!(
        failed_frame2
            .to_text()
            .expect("text")
            .contains("response.failed")
    );
    tokio::time::timeout(Duration::from_secs(5), provider_task2)
        .await
        .expect("provider2 finishes")
        .expect("provider2 task");
    assert!(
        !drain2.await_drained(Duration::from_secs(5)).await,
        "relay must release drain guard cleanly after trailing response.failed"
    );
    let _ = client2.close(None).await;
    proxy_task2.abort();
}

#[tokio::test]
async fn ws_relay_signing_failure_triggers_owner_failover_to_replacement() {
    use cc_lb_storage_api::upstream::UpstreamKind as StorageRecordKind;
    use sha2::{Digest, Sha256};

    let _ = rustls::crypto::ring::default_provider().install_default();

    let b_seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let b_seen_srv = Arc::clone(&b_seen);
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let _task_b = tokio::spawn(async move {
        let (stream, _) = listener_b.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        if let Some(Ok(UpstreamMessage::Text(text))) = ws.next().await {
            let val: Value = serde_json::from_str(&text).unwrap();
            b_seen_srv.lock().unwrap().push(val);
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp_sign_b","store":false}}"#
                        .into(),
                ))
                .await;
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_sign_b","status":"completed","store":false,"usage":{"input_tokens":5,"output_tokens":2}}}"#
                        .into(),
                ))
                .await;
        }
    });

    let up_a = Uuid::from_u128(0xAA010);
    let up_b = Uuid::from_u128(0xBB020);
    struct FailASignerFactory {
        fail_name: &'static str,
        chosen: Option<String>,
    }
    impl ApiKeyAwareSignerFactory for FailASignerFactory {
        fn with_router_choice(
            &self,
            router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self {
                fail_name: self.fail_name,
                chosen: Some(router_chosen_upstream_name),
            })
        }
    }
    #[async_trait]
    impl SignerFactory for FailASignerFactory {
        async fn build(
            &self,
            _upstream: &cc_lb_domain::Upstream,
        ) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(FailASigner {
                fail: self.chosen.as_deref() == Some(self.fail_name),
            }))
        }
    }
    struct FailASigner {
        fail: bool,
    }
    #[async_trait]
    impl Signer for FailASigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            if self.fail {
                return Err(SignerError::SigningFailed {
                    reason: "simulated signing failure on A".to_owned(),
                });
            }
            Ok(SignedRequest::from_shaped(shaped, capability))
        }
        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("sign-fail.sqlite");
    let db_url = format!("sqlite://{}", db_path.display());
    let rec_a = cc_lb_storage_api::upstream::UpstreamRecord {
        id: up_a,
        name: "api-a".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        base_url: Some(Url::parse("http://127.0.0.1:9/").unwrap()),
        enabled: true,
        ..Default::default()
    };
    let rec_b = cc_lb_storage_api::upstream::UpstreamRecord {
        id: up_b,
        name: "api-b".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        base_url: Some(Url::parse(&format!("http://{addr_b}/")).unwrap()),
        enabled: true,
        ..Default::default()
    };
    let state = websocket_test_state_with_affinity_store(
        vec![rec_a, rec_b],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(FailASignerFactory {
            fail_name: "api-a",
            chosen: None,
        }),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
        Some(db_url),
    )
    .await;

    let cipher_a = "gAAAAAB_relay_sign_fail_a";
    let now_secs = cc_lb_clock::unix_secs(state.clock.now());
    state
        .storage
        .bind_upstream_affinities(
            &[cc_lb_storage_api::UpstreamAffinityBinding {
                key: cc_lb_storage_api::UpstreamAffinityKey {
                    principal_id: TEST_PRINCIPAL.to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: Sha256::digest(cipher_a.as_bytes()).into(),
                },
                upstream_id: up_a,
                owner_epoch: 0,
                observed_at_unix_secs: now_secs,
                expires_at_unix_secs: None,
            }],
            now_secs,
            3600,
        )
        .await
        .unwrap();

    let (proxy_addr, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_addr)).await.unwrap();
    client
        .send(UpstreamMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "store": false,
                "include": ["reasoning.encrypted_content"],
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_sign_fail",
                        "summary": [],
                        "encrypted_content": cipher_a
                    },
                    {"role": "user", "content": "failover after sign err"}
                ]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    let created = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        created.to_text().unwrap().contains("resp_sign_b"),
        "{created:?}"
    );
    let completed = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        completed.to_text().unwrap().contains("response.completed"),
        "{completed:?}"
    );
    {
        let recorded = b_seen.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0]["input"].as_array().unwrap().len(), 1);
        assert_eq!(
            recorded[0]["input"][0]["content"],
            "failover after sign err"
        );
    }
    let _ = client.close(None).await;
    proxy_task.abort();
}

#[tokio::test]
async fn ws_relay_slot_cap_one_reclaims_idle_socket_on_live_429_and_eviction_replay() {
    use cc_lb_storage_api::upstream::UpstreamKind as StorageRecordKind;
    use sha2::{Digest, Sha256};

    let _ = rustls::crypto::ring::default_provider().install_default();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let _task_a = tokio::spawn(async move {
        while let Ok((stream, _)) = listener_a.accept().await {
            tokio::spawn(async move {
                let mut ws = accept_async(stream).await.unwrap();
                while let Some(Ok(UpstreamMessage::Text(_))) = ws.next().await {
                    let _ = ws
                        .send(UpstreamMessage::Text(
                            r#"{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"429"}}"#
                                .into(),
                        ))
                        .await;
                }
            });
        }
    });

    let b_seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let b_seen_srv = Arc::clone(&b_seen);
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let _task_b = tokio::spawn(async move {
        let (stream, _) = listener_b.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        if let Some(Ok(UpstreamMessage::Text(text))) = ws.next().await {
            let val: Value = serde_json::from_str(&text).unwrap();
            b_seen_srv.lock().unwrap().push(val);
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp_slot1_b","store":false}}"#
                        .into(),
                ))
                .await;
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_slot1_b","status":"completed","store":false,"usage":{"input_tokens":5,"output_tokens":2}}}"#
                        .into(),
                ))
                .await;
        }
    });

    let up_a = Uuid::from_u128(0xAA030);
    let up_b = Uuid::from_u128(0xBB040);
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("slot1.sqlite");
    let db_url = format!("sqlite://{}", db_path.display());
    let rec_a = cc_lb_storage_api::upstream::UpstreamRecord {
        id: up_a,
        name: "api-a".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        base_url: Some(Url::parse(&format!("http://{addr_a}/")).unwrap()),
        enabled: true,
        ..Default::default()
    };
    let rec_b = cc_lb_storage_api::upstream::UpstreamRecord {
        id: up_b,
        name: "api-b".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        base_url: Some(Url::parse(&format!("http://{addr_b}/")).unwrap()),
        enabled: true,
        ..Default::default()
    };
    let state_arg_url = Some(db_url);
    let mut state = websocket_test_state_with_affinity_store(
        vec![rec_a, rec_b],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
        state_arg_url,
    )
    .await;
    state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 1,
        semaphore_permits: 1,
        acquire_timeout: Duration::from_secs(1),
    };

    let cipher_a = "gAAAAAB_slot1_relay_a";
    let now_secs = cc_lb_clock::unix_secs(state.clock.now());
    state
        .storage
        .bind_upstream_affinities(
            &[cc_lb_storage_api::UpstreamAffinityBinding {
                key: cc_lb_storage_api::UpstreamAffinityKey {
                    principal_id: TEST_PRINCIPAL.to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: Sha256::digest(cipher_a.as_bytes()).into(),
                },
                upstream_id: up_a,
                owner_epoch: 0,
                observed_at_unix_secs: now_secs,
                expires_at_unix_secs: None,
            }],
            now_secs,
            3600,
        )
        .await
        .unwrap();

    let (proxy_addr, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
    let (mut client, _) = connect_async(websocket_request(proxy_addr)).await.unwrap();
    client
        .send(UpstreamMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "store": false,
                "include": ["reasoning.encrypted_content"],
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_s1_relay",
                        "summary": [],
                        "encrypted_content": cipher_a
                    },
                    {"role": "user", "content": "slot1 live 429"}
                ]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    let created = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        created.to_text().unwrap().contains("resp_slot1_b"),
        "{created:?}"
    );
    let completed = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        completed.to_text().unwrap().contains("response.completed"),
        "{completed:?}"
    );
    {
        let recorded = b_seen.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0]["input"].as_array().unwrap().len(), 1);
    }
    let _ = client.close(None).await;
    proxy_task.abort();

    // Part 2: Codex WS 1-slot pre-dispatch eviction (`400 previous_response_not_found`) leaves idle
    // socket A open; full-history replay on the same client WS reclaims A via
    // `reclaim_idle_connection_for_failover` and completes on OAuth B without 503.
    let listener_codex_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_codex_a = listener_codex_a.local_addr().unwrap();
    let cipher_codex_a = "gAAAAAB_codex_slot1_a";
    let _task_codex_a = tokio::spawn(async move {
        let (stream, _) = listener_codex_a.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        if let Some(Ok(UpstreamMessage::Text(_))) = ws.next().await {
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp_codex_slot1_a","status":"in_progress","store":false}}"#
                        .into(),
                ))
                .await;
            let _ = ws
                .send(UpstreamMessage::Text(
                    format!(
                        r#"{{"type":"response.completed","response":{{"id":"resp_codex_slot1_a","status":"completed","store":false,"output":[{{"type":"reasoning","id":"rs_ca","summary":[],"encrypted_content":"{cipher_codex_a}"}}],"usage":{{"input_tokens":5,"output_tokens":2}}}}}}"#
                    )
                    .into(),
                ))
                .await;
        }
        // Keep socket A open so the relay still sees `connections.len() == 1` until it reclaims A.
        let _ = ws.next().await;
    });

    let codex_b_seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let codex_b_seen_srv = Arc::clone(&codex_b_seen);
    let listener_codex_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_codex_b = listener_codex_b.local_addr().unwrap();
    let _task_codex_b = tokio::spawn(async move {
        let (stream, _) = listener_codex_b.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        if let Some(Ok(UpstreamMessage::Text(text))) = ws.next().await {
            let val: Value = serde_json::from_str(&text).unwrap();
            codex_b_seen_srv.lock().unwrap().push(val);
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"resp_codex_slot1_b","status":"in_progress","store":false}}"#
                        .into(),
                ))
                .await;
            let _ = ws
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_codex_slot1_b","status":"completed","store":false,"usage":{"input_tokens":5,"output_tokens":2}}}"#
                        .into(),
                ))
                .await;
        }
    });

    let mut oauth_a = websocket_upstream(
        1,
        "oauth-slot1-a",
        Url::parse(&format!("http://{addr_codex_a}/")).unwrap(),
    );
    oauth_a.kind = UpstreamKind::OpenaiOauth;
    oauth_a.oauth_never_refresh = true;
    let mut oauth_b = websocket_upstream(
        2,
        "oauth-slot1-b",
        Url::parse(&format!("http://{addr_codex_b}/")).unwrap(),
    );
    oauth_b.kind = UpstreamKind::OpenaiOauth;
    oauth_b.oauth_never_refresh = true;

    let db_dir2 = tempfile::tempdir().unwrap();
    let db_url2 = format!(
        "sqlite://{}",
        db_dir2.path().join("codex-slot1.sqlite").display()
    );
    let mut codex_state = websocket_test_state_with_affinity_store(
        vec![oauth_a.clone(), oauth_b.clone()],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
        Some(db_url2),
    )
    .await;
    codex_state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 1,
        semaphore_permits: 1,
        acquire_timeout: Duration::from_secs(1),
    };
    let codex_state_ref = codex_state.clone();
    let (codex_proxy, codex_task) = spawn_proxy_router(proxy_router(codex_state, 30)).await;
    let mut codex_req = websocket_request(codex_proxy);
    *codex_req.uri_mut() = format!("ws://{codex_proxy}/openai/backend-api/codex/responses")
        .parse()
        .unwrap();
    let (mut codex_client, _) = connect_async(codex_req).await.unwrap();

    codex_client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5.4","store":false,"include":["reasoning.encrypted_content"],"input":[{"role":"user","content":"turn1"}]}"#
                .into(),
        ))
        .await
        .unwrap();
    for expected in ["response.created", "response.completed"] {
        let frame = tokio::time::timeout(Duration::from_secs(5), codex_client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(frame.to_text().unwrap().contains(expected), "{frame:?}");
    }

    // Disable `oauth-slot1-a` so Turn 2 `previous_response_id` evicts A with `400 previous_response_not_found`,
    // leaving idle socket A open on the relay.
    oauth_a.enabled = false;
    replace_websocket_upstreams(&codex_state_ref, vec![oauth_a, oauth_b]);

    codex_client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp_codex_slot1_a","store":false,"input":[{"role":"user","content":"turn2"}]}"#
                .into(),
        ))
        .await
        .unwrap();
    let evict_err = tokio::time::timeout(Duration::from_secs(5), codex_client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let evict_json: Value = serde_json::from_str(evict_err.to_text().unwrap()).unwrap();
    assert_eq!(evict_json["status"], 400, "{evict_json}");
    assert_eq!(
        evict_json["error"]["code"], "previous_response_not_found",
        "{evict_json}"
    );

    // Replay full history on the SAME client WS under slot_cap = 1: relay must reclaim idle socket A
    // and dispatch to B without failing 503.
    codex_client
        .send(UpstreamMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "store": false,
                "include": ["reasoning.encrypted_content"],
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_ca",
                        "summary": [],
                        "encrypted_content": cipher_codex_a
                    },
                    {"role": "user", "content": "replay turn"}
                ]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let replay_created = tokio::time::timeout(Duration::from_secs(5), codex_client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        replay_created
            .to_text()
            .unwrap()
            .contains("resp_codex_slot1_b"),
        "{replay_created:?}"
    );
    let replay_completed = tokio::time::timeout(Duration::from_secs(5), codex_client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        replay_completed
            .to_text()
            .unwrap()
            .contains("response.completed"),
        "{replay_completed:?}"
    );
    {
        let codex_recorded = codex_b_seen.lock().unwrap();
        assert_eq!(codex_recorded.len(), 1);
        assert_eq!(codex_recorded[0]["input"].as_array().unwrap().len(), 1);
        assert_eq!(codex_recorded[0]["input"][0]["content"], "replay turn");
    }
    let _ = codex_client.close(None).await;
    codex_task.abort();
}
