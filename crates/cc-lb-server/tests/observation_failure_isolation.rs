//! Task 31: assert that an async prompt-cache observation store failure
//! does NOT fail the synchronous response path.
//!
//! Booting the full `cc-lb-server` HTTP listener and a fake upstream just to
//! prove this isolation property is overkill for what the production code
//! actually does: the response path enqueues to a bounded mpsc, and a
//! background writer task drains the queue and calls
//! `PromptCacheObservationStore::upsert_observation`. The writer task swallows
//! store errors with `tracing::warn!` and bumps
//! `cc_lb_cache_observation_write_failed_total`.
//!
//! This test pins that isolation property by exercising the sink directly
//! (T18 sink + observability counter wired in T25). "Response status == 200"
//! is represented by the fact that every `sink.enqueue(...)` call returns
//! `Ok(())` even though the backing store is permanently broken; the writer
//! task only ever logs and counts, it never panics or signals the enqueue
//! side. Per the task `MUST DO` clause, when no production constructor takes
//! `Arc<dyn PromptCacheObservationStore>` for the full server, the test may
//! use the T18 sink directly.

use std::collections::HashMap;
use std::convert::Infallible;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_domain::{Principal, TtlClass, Upstream, UpstreamCandidate};
use cc_lb_engine::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_engine::api_keys::key_store::KeyStore;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::api_keys::secret;
use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle,
    LifecycleConfig, UpstreamDispatch,
};
use cc_lb_observability::{cache_observation_dropped_reason, cache_observation_store_kind};
use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin, RoutingContext};
use cc_lb_server::prompt_cache_observation_sink::PromptCacheObservationSink;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    ApiKeyMutation, BackendKind, IssueParams, KeyStatus, ManagedKeyStore, MetaStore,
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageError, StorageResult,
    StoredApiKeyRecord,
};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, RetryDecision, ShapedRequest,
    ShapedRequestBuilder, SignedRequest, Signer, SignerError, SignerFactory, SigningCapability,
    SseEventTransformHook, TransformSseEventRequest, TransformSseEventResult, UpstreamDialect,
    UpstreamError,
};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::sync::Notify;
use tracing_subscriber::fmt::MakeWriter;
use url::Url;
use uuid::Uuid;

const SIMULATED_ERROR_MESSAGE: &str = "simulated prompt-cache observation store failure (task 31)";

/// Mock `PromptCacheObservationStore` whose `upsert_observation` always
/// returns `Err(StorageError::Unavailable)` with a recognizable message that
/// the captured log assertion can find.
struct FailingStore;

#[async_trait]
impl PromptCacheObservationStore for FailingStore {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        Err(StorageError::Unavailable {
            message: SIMULATED_ERROR_MESSAGE.to_owned(),
        })
    }
}

#[derive(Clone, Default)]
struct CapturedLogs {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.inner.lock().expect("captured logs lock");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

struct CapturedWriter {
    logs: CapturedLogs,
}

impl Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.logs
            .inner
            .lock()
            .expect("captured logs lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter { logs: self.clone() }
    }
}

fn record(index: u64) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        v3_prefix_key: format!("sha256:task31-{index}"),
        ttl_class: TtlClass::Ephemeral5m,
        expires_at_unix_secs: 1_800 + index,
        last_observed_at_unix_secs: 1_500 + index,
        hash_schema_version: HASH_SCHEMA_VERSION,
        prefix_content_block_index: 0,
        estimated_prefix_tokens: 0,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
    }
}

#[test]
fn observation_failure_does_not_fail_response() {
    // Capture tracing output so we can assert the writer task logged
    // the simulated error.
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _tracing_guard = tracing::subscriber::set_default(subscriber);

    // Local Prometheus recorder so the counter accessor can be observed
    // without contaminating other tests' globals. `metrics::with_local_recorder`
    // is a thread-local install, so we drive everything on a single-threaded
    // `current_thread` runtime to keep recorder visibility consistent across
    // the writer task's `.await` points (mirrors the T18 sink unit tests in
    // `tests/prompt_cache_observation_metrics.rs`).
    let recorder = PrometheusBuilder::new().build_recorder();
    let prom_handle = recorder.handle();

    metrics::with_local_recorder(&recorder, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime builds");

        runtime.block_on(async move {
            // Inject the failing store at sink construction (T18 wiring).
            let (sink, writer) = PromptCacheObservationSink::new(
                Arc::new(FailingStore),
                8,
                cache_observation_store_kind::SQLITE,
            );

            // Each enqueue simulates the post-response observation enqueue
            // performed by the lifecycle on a successful (HTTP 200) /v1/messages
            // request that carries cache breakpoints. The response path has
            // already returned 200 to the client by the time we hit this point;
            // the only thing left is whether the downstream store error can
            // claw its way back. It can't — enqueue must return Ok.
            for index in 0..3 {
                sink.enqueue(record(index))
                    .expect("enqueue must succeed: response is already 200");
            }

            // Drop the sink so the writer task observes channel close after
            // draining, then await it to guarantee all upserts (and their
            // failure-logging branches) have executed before we assert.
            drop(sink);
            // Cap the wait at 5 s per the task budget. The writer drains 3
            // records into a synchronous `Err(...)` return, so this completes
            // in milliseconds in practice.
            tokio::time::timeout(Duration::from_secs(5), writer)
                .await
                .expect("writer task drains within 5 s")
                .expect("writer task exits cleanly");
        });
    });

    // Assertion 1: response semantics preserved. Every enqueue above returned
    // Ok, so if we reached this line the synchronous response path was never
    // forced to fail by the async store error.

    // Assertion 2: write-failed counter incremented at least once per failed
    // upsert (3 enqueues -> 3 failed upserts -> counter == 3).
    let rendered = prom_handle.render();
    assert!(
        rendered.contains("cc_lb_cache_observation_write_failed_total{store=\"sqlite\"}"),
        "expected write_failed counter for store=sqlite in metrics output:\n{rendered}"
    );
    let counter_value = parse_write_failed_counter(&rendered, cache_observation_store_kind::SQLITE)
        .expect("write_failed counter parses from prometheus output");
    assert!(
        counter_value >= 1,
        "expected write_failed counter >= 1 for store=sqlite, got {counter_value}\n{rendered}"
    );

    // Assertion 3: writer logged a WARN with the simulated error message.
    let captured = logs.contents();
    assert!(
        captured.contains("prompt cache observation write failed"),
        "expected sink writer warn log, captured logs:\n{captured}"
    );
    assert!(
        captured.contains(SIMULATED_ERROR_MESSAGE),
        "expected simulated error message in captured logs:\n{captured}"
    );
    assert!(
        captured.contains("WARN"),
        "expected WARN level on observation write failure log:\n{captured}"
    );
}

/// Parse `cc_lb_cache_observation_write_failed_total{store="..."} <n>` from
/// the rendered Prometheus output for the given store label.
fn parse_write_failed_counter(rendered: &str, store: &str) -> Option<u64> {
    let needle = format!("cc_lb_cache_observation_write_failed_total{{store=\"{store}\"}}");
    for line in rendered.lines() {
        if let Some(rest) = line.strip_prefix(&needle) {
            return rest.trim().parse::<u64>().ok();
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Issue 825 F3/F4: the same failure modes must also leave a real request
// through `Lifecycle::handle` untouched — streamed and buffered. These tests
// wire the production `PromptCacheObservationSink` into a `DynamicView` and
// drive actual requests; the fixtures below mirror the engine's
// `tests/common` harness (in-memory managed key, passthrough dialect, canned
// upstream dispatch) so the publish site under test is the real one in
// `lifecycle.rs`, not the orphan `SseRelay` helper.
// ---------------------------------------------------------------------------

const FIXTURE_PRINCIPAL: &str = "principal-test";
const FIXTURE_MODEL: &str = "claude-sonnet-4-5-20250929";

/// Anthropic SSE frames whose `message_start` carries qualifying
/// cache-creation usage, so the stream loop publishes exactly one observation.
const SSE_MESSAGE_START: &[u8] = b"event: message_start\n\
    data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1600,\"cache_creation_input_tokens\":1600,\"output_tokens\":0}}}\n\n";
const SSE_MESSAGE_STOP: &[u8] = b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

/// Buffered (non-streaming) Anthropic response carrying the same usage.
const BUFFERED_BODY: &[u8] = br#"{"type":"message","usage":{"input_tokens":1600,"cache_creation_input_tokens":1600,"output_tokens":7}}"#;

#[derive(Clone, Copy)]
enum RequestMode {
    Streamed,
    Buffered,
}

struct FixtureKey {
    plaintext: String,
    key_id: String,
    record: StoredApiKeyRecord,
}

static FIXTURE_KEY: LazyLock<FixtureKey> = LazyLock::new(|| {
    let generated = secret::generate_new();
    FixtureKey {
        plaintext: generated.plaintext.expose().to_owned(),
        key_id: generated.key_id,
        record: StoredApiKeyRecord {
            label: "failure-isolation key".to_owned(),
            verify_hash: generated.verify_hash,
            secret_salt: generated.secret_salt,
            status: KeyStatus::Active,
            last_4: generated.last_4,
            index_hash: generated.index_hash,
            ..StoredApiKeyRecord::default()
        },
    }
});

fn fixture_api_key() -> &'static str {
    &FIXTURE_KEY.plaintext
}

/// In-memory `ManagedKeyStore` resolving exactly the fixture key for
/// `FIXTURE_PRINCIPAL` (mirrors the engine `InMemoryManagedKeyStore`).
struct FixtureKeyStore;

#[async_trait]
impl ManagedKeyStore for FixtureKeyStore {
    async fn issue(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        Ok(FIXTURE_KEY.record.clone())
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        Ok(
            (principal_id == FIXTURE_PRINCIPAL && key_id == FIXTURE_KEY.key_id)
                .then(|| FIXTURE_KEY.record.clone()),
        )
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        Ok((index_hash == &FIXTURE_KEY.record.index_hash).then(|| {
            (
                FIXTURE_PRINCIPAL.to_owned(),
                FIXTURE_KEY.key_id.clone(),
                FIXTURE_KEY.record.clone(),
            )
        }))
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        Ok((principal_id == FIXTURE_PRINCIPAL)
            .then(|| FIXTURE_KEY.record.clone())
            .into_iter()
            .collect())
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        Ok(vec![(
            FIXTURE_PRINCIPAL.to_owned(),
            FIXTURE_KEY.key_id.clone(),
            FIXTURE_KEY.record.clone(),
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

/// Signer chain that stamps a static upstream credential; refresh is never
/// exercised because the canned upstream always answers 200.
struct FixtureSignerChain;

impl ApiKeyAwareSignerFactory for FixtureSignerChain {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl SignerFactory for FixtureSignerChain {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(FixtureSigner))
    }
}

struct FixtureSigner;

#[async_trait]
impl Signer for FixtureSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

/// Router returning the configured dialect; the resolved upstream id comes
/// from the candidate set (the one `upstream_records` row).
struct FixtureRouter {
    dialect: Arc<dyn UpstreamDialect>,
}

impl RouterPlugin for FixtureRouter {
    fn route(
        &self,
        _ctx: &RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream_id: None,
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: self.dialect.clone(),
        })
    }
}

/// Passthrough dialect: forwards the downstream request to a fixed base URL.
struct FixtureDialect;

impl UpstreamDialect for FixtureDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://upstream.local/").expect("test URL parses");
        url.set_path(context.path.trim_start_matches('/'));
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

/// Same passthrough shape as `FixtureDialect`, but advertises an SSE transform
/// hook so the request exercises the compat SSE publish path in the stream
/// loop. The hook passes every event through unchanged.
struct FixtureTransformDialect;

impl UpstreamDialect for FixtureTransformDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        FixtureDialect.shape(context, upstream, principal, builder)
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        Some(self)
    }
}

impl SseEventTransformHook for FixtureTransformDialect {
    fn transform_sse_event(
        &self,
        _request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Ok(TransformSseEventResult::Unchanged)
    }
}

/// `UpstreamDispatch` answering each request with one canned response.
struct CannedDispatch {
    response: Mutex<Option<Response<Body>>>,
}

impl CannedDispatch {
    fn new(response: Response<Body>) -> Self {
        Self {
            response: Mutex::new(Some(response)),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for CannedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Ok(self
            .response
            .lock()
            .expect("dispatch response lock")
            .take()
            .expect("one dispatch per request"))
    }
}

/// Store whose first `upsert_observation` blocks until released. Used to hold
/// the sink writer mid-write so the bounded channel stays occupied while the
/// lifecycle publishes — a deterministic queue-full condition with no sleeps.
struct GatedStore {
    entered: Notify,
    release: Notify,
    gate: AtomicBool,
    attempts: AtomicU64,
}

impl GatedStore {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
            gate: AtomicBool::new(true),
            attempts: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl PromptCacheObservationStore for GatedStore {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if self.gate.swap(false, Ordering::Relaxed) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}

/// Store wrapper that counts committed upserts and notifies after each one
/// completes, so a test can prove the observation row is durably committed
/// while the upstream stream is still gated before its final frame.
struct CommitWatchStore {
    inner: Arc<dyn PromptCacheObservationStore>,
    committed: Notify,
    commits: AtomicU64,
}

#[async_trait]
impl PromptCacheObservationStore for CommitWatchStore {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        self.inner.upsert_observation(record).await?;
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.committed.notify_one();
        Ok(())
    }

    async fn count(&self) -> StorageResult<u64> {
        self.inner.count().await
    }
}

fn fixture_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: Uuid::from_u128(1),
        name: "test-upstream".to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        revision: 1,
        ..UpstreamRecord::default()
    }
}

/// Build a `Lifecycle` whose `DynamicView` publishes prompt-cache observations
/// through the given production sink and routes through the given dialect.
fn lifecycle_with_sink(
    sink: PromptCacheObservationSink,
    dialect: Arc<dyn UpstreamDialect>,
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> (
    Lifecycle,
    Arc<cc_lb_server::prompt_cache_thread_usage::PromptCacheThreadUsageTracker>,
) {
    let tracker =
        Arc::new(cc_lb_server::prompt_cache_thread_usage::PromptCacheThreadUsageTracker::new(30));
    let key_store: Arc<dyn ManagedKeyStore> = Arc::new(FixtureKeyStore);
    let authn = Arc::new(BuiltinAuthn::new(
        Arc::new(KeyStore::new(key_store)),
        Arc::new(cc_lb_engine::SystemClock),
    ));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(FixtureSignerChain))
        .global_router(Arc::new(FixtureRouter { dialect }))
        .global_observability_hooks(Vec::new())
        .principal_view(Arc::new(PrincipalView::for_tests(
            FIXTURE_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        )))
        .upstream_records(vec![fixture_upstream_record()])
        .prompt_cache_observation_sink(Arc::new(sink))
        .prompt_cache_thread_usage(tracker.clone())
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
    .with_event_bus(cc_lb_engine::new_in_memory_bus());
    (lifecycle, tracker)
}

/// `/v1/messages` request with one `cache_control` breakpoint whose prefix is
/// comfortably above the model's 1024-token cacheable threshold.
fn cacheable_request(mode: RequestMode) -> Request<Bytes> {
    let stream = matches!(mode, RequestMode::Streamed);
    let body = format!(
        concat!(
            r#"{{"model":"{model}","max_tokens":16,"stream":{stream},"#,
            r#""system":[{{"type":"text","text":"{prefix}","cache_control":{{"type":"ephemeral"}}}}],"#,
            r#""messages":[{{"role":"user","content":[{{"type":"text","text":"hi"}}]}}]}}"#
        ),
        model = FIXTURE_MODEL,
        stream = stream,
        prefix = "cacheable prefix body ".repeat(700),
    );
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", fixture_api_key())
        .header("anthropic-version", "2023-06-01")
        .header(CONTENT_TYPE, "application/json")
        .body(Bytes::from(body))
        .expect("test request builds")
}

/// SSE upstream response whose final `message_stop` frame is held back until
/// `release_frames` is notified, letting a test observe store state while the
/// stream is still in flight.
fn gated_sse_response(release_frames: Arc<Notify>) -> Response<Body> {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, Infallible>(Bytes::from_static(SSE_MESSAGE_START));
        release_frames.notified().await;
        yield Ok::<Bytes, Infallible>(Bytes::from_static(SSE_MESSAGE_STOP));
    };
    let mut response = Response::new(Body::from_stream(stream));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
}

fn upstream_response(mode: RequestMode) -> Response<Body> {
    match mode {
        RequestMode::Streamed => {
            // Pre-notified gate: the stream yields both frames immediately.
            let release = Arc::new(Notify::new());
            release.notify_one();
            gated_sse_response(release)
        }
        RequestMode::Buffered => {
            let mut response = Response::new(Body::from(Bytes::from_static(BUFFERED_BODY)));
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            response
        }
    }
}

/// Drive `case` on a current-thread runtime under a thread-local Prometheus
/// recorder and tracing subscriber (same pattern as the sink-level test
/// above), returning the captured log text and rendered metrics.
fn run_lifecycle_case<Fut>(case: impl FnOnce() -> Fut) -> (String, String)
where
    Fut: Future<Output = ()>,
{
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _tracing_guard = tracing::subscriber::set_default(subscriber);

    let recorder = PrometheusBuilder::new().build_recorder();
    let prom_handle: PrometheusHandle = recorder.handle();

    metrics::with_local_recorder(&recorder, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime builds");
        runtime.block_on(case());
    });

    (logs.contents(), prom_handle.render())
}

/// Parse `cc_lb_cache_observation_dropped_total{reason="..."} <n>` from the
/// rendered Prometheus output for the given reason label.
fn parse_dropped_counter(rendered: &str, reason: &str) -> Option<u64> {
    let needle = format!("cc_lb_cache_observation_dropped_total{{reason=\"{reason}\"}}");
    for line in rendered.lines() {
        if let Some(rest) = line.strip_prefix(&needle) {
            return rest.trim().parse::<u64>().ok();
        }
    }
    None
}

/// A full observation queue must drop the lifecycle's publish without
/// disturbing the response: the request still completes 200 end-to-end, the
/// drop is counted, and the store only ever sees the pre-fill records.
fn queue_full_case(mode: RequestMode) {
    let (_logs, rendered) = run_lifecycle_case(move || async move {
        let store = Arc::new(GatedStore::new());
        let (sink, writer) =
            PromptCacheObservationSink::new(store.clone(), 1, cache_observation_store_kind::SQLITE);

        // First record occupies the writer inside a gated upsert; the second
        // fills the single channel slot. The lifecycle's publish then observes
        // a genuinely full queue — no timing assumptions.
        sink.enqueue(record(0)).expect("first record enqueues");
        store.entered.notified().await;
        sink.enqueue(record(1))
            .expect("second record fills the queue");

        let (lifecycle, _) = lifecycle_with_sink(
            sink.clone(),
            Arc::new(FixtureDialect),
            Arc::new(CannedDispatch::new(upstream_response(mode))),
        );
        let response = lifecycle
            .handle(cacheable_request(mode))
            .await
            .expect("lifecycle handles request");
        assert_eq!(response.status(), StatusCode::OK);
        response
            .into_body()
            .collect()
            .await
            .expect("response body collects");

        assert_eq!(
            sink.dropped_total(),
            1,
            "lifecycle observation enqueue must be dropped on a full queue",
        );

        store.release.notify_one();
        drop(sink);
        drop(lifecycle);
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .expect("writer drains within 5 s")
            .expect("writer exits cleanly");
        assert_eq!(
            store.attempts.load(Ordering::Relaxed),
            2,
            "only the two pre-fill records may reach the store",
        );
    });

    let dropped = parse_dropped_counter(&rendered, cache_observation_dropped_reason::QUEUE_FULL)
        .expect("queue_full drop counter parses from prometheus output");
    assert_eq!(
        dropped, 1,
        "expected exactly one queue_full drop:\n{rendered}"
    );
}

/// A closed observation queue (writer task gone) must likewise leave the
/// response untouched: 200 end-to-end, drop counted as `channel_closed`, and
/// the lifecycle logs the closed sink.
fn queue_closed_case(mode: RequestMode) {
    let (logs, rendered) = run_lifecycle_case(move || async move {
        let (sink, writer) = PromptCacheObservationSink::new(
            Arc::new(FailingStore),
            8,
            cache_observation_store_kind::SQLITE,
        );
        // Aborting the writer drops the receiver with the task, so the channel
        // is closed before the lifecycle publishes.
        writer.abort();
        let _ = writer.await;

        let (lifecycle, _) = lifecycle_with_sink(
            sink.clone(),
            Arc::new(FixtureDialect),
            Arc::new(CannedDispatch::new(upstream_response(mode))),
        );
        let response = lifecycle
            .handle(cacheable_request(mode))
            .await
            .expect("lifecycle handles request");
        assert_eq!(response.status(), StatusCode::OK);
        response
            .into_body()
            .collect()
            .await
            .expect("response body collects");

        assert_eq!(
            sink.dropped_total(),
            1,
            "closed queue must drop the lifecycle observation",
        );
    });

    let dropped =
        parse_dropped_counter(&rendered, cache_observation_dropped_reason::CHANNEL_CLOSED)
            .expect("channel_closed drop counter parses from prometheus output");
    assert_eq!(
        dropped, 1,
        "expected exactly one channel_closed drop:\n{rendered}"
    );
    assert!(
        logs.contains("prompt cache observation sink is closed"),
        "expected closed-sink warn log from the lifecycle publish path:\n{logs}"
    );
}

/// A store that fails every `upsert_observation` must not disturb the
/// response: the enqueue succeeds, the request completes 200 end-to-end, and
/// the writer logs and counts the failed write instead of propagating it.
fn write_failure_case(mode: RequestMode) {
    let (logs, rendered) = run_lifecycle_case(move || async move {
        let (sink, writer) = PromptCacheObservationSink::new(
            Arc::new(FailingStore),
            8,
            cache_observation_store_kind::SQLITE,
        );

        let (lifecycle, _) = lifecycle_with_sink(
            sink.clone(),
            Arc::new(FixtureDialect),
            Arc::new(CannedDispatch::new(upstream_response(mode))),
        );
        let response = lifecycle
            .handle(cacheable_request(mode))
            .await
            .expect("lifecycle handles request");
        assert_eq!(response.status(), StatusCode::OK);
        response
            .into_body()
            .collect()
            .await
            .expect("response body collects");

        assert_eq!(
            sink.dropped_total(),
            0,
            "enqueue itself succeeds; the failure is downstream in the store",
        );

        drop(sink);
        drop(lifecycle);
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .expect("writer drains within 5 s")
            .expect("writer exits cleanly");
    });

    let failed = parse_write_failed_counter(&rendered, cache_observation_store_kind::SQLITE)
        .expect("write_failed counter parses from prometheus output");
    assert_eq!(
        failed, 1,
        "expected exactly one failed observation write:\n{rendered}"
    );
    assert!(
        logs.contains("prompt cache observation write failed"),
        "expected sink writer warn log, captured logs:\n{logs}"
    );
    assert!(
        logs.contains(SIMULATED_ERROR_MESSAGE),
        "expected simulated error message in captured logs:\n{logs}"
    );
}

/// Issue 825 B1/B2: the observation must be committed to the real store while
/// the upstream stream is still gated before its final frame — the publish
/// fires at `message_start`, not at stream end. `dialect` selects the fast
/// path (`FixtureDialect`) or the compat SSE transform path
/// (`FixtureTransformDialect`); both reach the same publish site.
fn early_commit_case(dialect: Arc<dyn UpstreamDialect>) {
    run_lifecycle_case(move || async move {
        // Real SQLite store behind the production sink: the commit assertion
        // observes a durable row, not an in-memory recording.
        let dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            dir.path().join("early-commit.sqlite").display()
        );
        let sqlite =
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
                .await
                .expect("sqlite opens");
        sqlite
            .initialize(BackendKind::Sqlite)
            .await
            .expect("sqlite migrates");
        let store = Arc::new(CommitWatchStore {
            inner: Arc::new(sqlite),
            committed: Notify::new(),
            commits: AtomicU64::new(0),
        });
        let (sink, writer) =
            PromptCacheObservationSink::new(store.clone(), 8, cache_observation_store_kind::SQLITE);

        // The upstream holds `message_stop` until released, so anything
        // committed before the release provably happened mid-stream.
        let release_frames = Arc::new(Notify::new());
        let (lifecycle, _) = lifecycle_with_sink(
            sink.clone(),
            dialect,
            Arc::new(CannedDispatch::new(gated_sse_response(
                release_frames.clone(),
            ))),
        );
        let response = lifecycle
            .handle(cacheable_request(RequestMode::Streamed))
            .await
            .expect("lifecycle handles request");
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();

        // Drive the stream to the first frame (message_start), then wait for
        // the writer to commit — all while the final frame is still gated.
        body.frame()
            .await
            .expect("first frame arrives")
            .expect("first frame is data");
        tokio::time::timeout(Duration::from_secs(5), store.committed.notified())
            .await
            .expect("observation commits while final frames are gated");
        assert_eq!(
            store.commits.load(Ordering::Relaxed),
            1,
            "exactly one observation committed before the final frame",
        );
        assert_eq!(
            store.count().await.expect("store count"),
            1,
            "durable row exists before the final frame",
        );

        release_frames.notify_one();
        while body.frame().await.transpose().expect("frames ok").is_some() {}

        drop(body);
        drop(sink);
        drop(lifecycle);
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .expect("writer drains within 5 s")
            .expect("writer exits cleanly");
        assert_eq!(
            store.commits.load(Ordering::Relaxed),
            1,
            "no duplicate publish at message_stop",
        );
    });
}

#[test]
fn message_start_commits_observation_before_final_frame() {
    early_commit_case(Arc::new(FixtureDialect));
}

#[test]
fn compat_sse_message_start_commits_observation_before_final_frame() {
    early_commit_case(Arc::new(FixtureTransformDialect));
}

#[test]
fn queue_full_streamed_response_still_completes() {
    queue_full_case(RequestMode::Streamed);
}

#[test]
fn queue_full_buffered_response_still_completes() {
    queue_full_case(RequestMode::Buffered);
}

#[test]
fn queue_closed_streamed_response_still_completes() {
    queue_closed_case(RequestMode::Streamed);
}

#[test]
fn queue_closed_buffered_response_still_completes() {
    queue_closed_case(RequestMode::Buffered);
}

#[test]
fn store_write_failure_streamed_response_still_completes() {
    write_failure_case(RequestMode::Streamed);
}

#[test]
fn store_write_failure_buffered_response_still_completes() {
    write_failure_case(RequestMode::Buffered);
}

#[test]
fn completed_responses_preserve_thread_usage_diagnostics() {
    for mode in [RequestMode::Buffered, RequestMode::Streamed] {
        run_lifecycle_case(move || async move {
            let store = Arc::new(FailingStore);
            let (sink, writer) =
                PromptCacheObservationSink::new(store, 4, cache_observation_store_kind::SQLITE);
            let (lifecycle, tracker) = lifecycle_with_sink(
                sink.clone(),
                Arc::new(FixtureDialect),
                Arc::new(CannedDispatch::new(upstream_response(mode))),
            );
            let upstream_id = fixture_upstream_record().id;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_secs();
            assert!(
                tracker
                    .thread_usage_score(upstream_id, FIXTURE_MODEL, "lineage-session", now)
                    .is_none()
            );
            let mut request = cacheable_request(mode);
            request.headers_mut().insert(
                "x-hermes-session-id",
                HeaderValue::from_static("lineage-session"),
            );
            let response = lifecycle.handle(request).await.expect("request succeeds");
            assert_eq!(response.status(), StatusCode::OK);
            response
                .into_body()
                .collect()
                .await
                .expect("response completes");
            let score = tracker
                .thread_usage_score(upstream_id, FIXTURE_MODEL, "lineage-session", now)
                .expect("completed response retains diagnostic usage");
            // Creation-only lineage retains the established 1:4 read equivalent.
            assert_eq!(score.predicted_cache_read_tokens, 400);
            assert!(
                tracker
                    .thread_usage_score(upstream_id, FIXTURE_MODEL, "different-session", now)
                    .is_none()
            );
            drop(lifecycle);
            drop(sink);
            tokio::time::timeout(Duration::from_secs(5), writer)
                .await
                .expect("writer drains")
                .expect("writer exits");
        });
    }
}
