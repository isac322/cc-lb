#![allow(dead_code)]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_control::api_keys::key_store::KeyStore;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::api_keys::secret;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    ApiKeyMutation, IssueParams, KeyStatus, ManagedKeyStore, StorageResult, StoredApiKeyRecord,
};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, RetryDecision, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, UpstreamDialect,
    UpstreamError, shape_request, sign_request,
};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use tokio::sync::Notify;
use url::Url;

#[derive(Clone)]
pub struct TestState {
    pub upstream_calls: Arc<AtomicU64>,
    pub refresh_count: Arc<AtomicU64>,
    pub stream_dropped: Arc<AtomicBool>,
    pub stream_drop_notify: Arc<Notify>,
}

impl Default for TestState {
    fn default() -> Self {
        Self {
            upstream_calls: Arc::new(AtomicU64::new(0)),
            refresh_count: Arc::new(AtomicU64::new(0)),
            stream_dropped: Arc::new(AtomicBool::new(false)),
            stream_drop_notify: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone)]
pub struct TestAuthn {
    pub authn: Arc<BuiltinAuthn>,
    pub principal_view: Arc<PrincipalView>,
    pub state: TestState,
    pub refresh_allowed: bool,
}

impl TestAuthn {
    pub fn new(state: TestState) -> Self {
        Self::with_principal_view(state, default_principal_view())
    }

    pub fn with_principal_view(state: TestState, view: Arc<PrincipalView>) -> Self {
        Self {
            authn: managed_authn("principal-test"),
            principal_view: view,
            state,
            refresh_allowed: true,
        }
    }
}

struct ManagedKeyFixture {
    plaintext: String,
    key_id: String,
    record: StoredApiKeyRecord,
}

static MANAGED_KEY_FIXTURE: LazyLock<ManagedKeyFixture> = LazyLock::new(|| {
    let generated = secret::generate_new();
    ManagedKeyFixture {
        plaintext: generated.plaintext.expose().to_owned(),
        key_id: generated.key_id,
        record: StoredApiKeyRecord {
            label: "engine test key".to_owned(),
            verify_hash: generated.verify_hash,
            secret_salt: generated.secret_salt,
            status: KeyStatus::Active,
            last_4: generated.last_4,
            index_hash: generated.index_hash,
            ..StoredApiKeyRecord::default()
        },
    }
});

pub fn managed_api_key() -> &'static str {
    &MANAGED_KEY_FIXTURE.plaintext
}

pub fn managed_key_id() -> &'static str {
    &MANAGED_KEY_FIXTURE.key_id
}

pub fn managed_authn(principal_id: &str) -> Arc<BuiltinAuthn> {
    let storage: Arc<dyn ManagedKeyStore> = Arc::new(InMemoryManagedKeyStore {
        principal_id: principal_id.to_owned(),
    });
    Arc::new(BuiltinAuthn::new(
        Arc::new(KeyStore::new(storage)),
        Arc::new(cc_lb_clock::SystemClock),
    ))
}

struct InMemoryManagedKeyStore {
    principal_id: String,
}

#[async_trait]
impl ManagedKeyStore for InMemoryManagedKeyStore {
    async fn issue(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        Ok(MANAGED_KEY_FIXTURE.record.clone())
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        Ok(
            (principal_id == self.principal_id && key_id == MANAGED_KEY_FIXTURE.key_id)
                .then(|| MANAGED_KEY_FIXTURE.record.clone()),
        )
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        Ok(
            (index_hash == &MANAGED_KEY_FIXTURE.record.index_hash).then(|| {
                (
                    self.principal_id.clone(),
                    MANAGED_KEY_FIXTURE.key_id.clone(),
                    MANAGED_KEY_FIXTURE.record.clone(),
                )
            }),
        )
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        Ok((principal_id == self.principal_id)
            .then(|| MANAGED_KEY_FIXTURE.record.clone())
            .into_iter()
            .collect())
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        Ok(vec![(
            self.principal_id.clone(),
            MANAGED_KEY_FIXTURE.key_id.clone(),
            MANAGED_KEY_FIXTURE.record.clone(),
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

fn default_principal_view() -> Arc<PrincipalView> {
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        std::collections::HashMap::new(),
    ))
}

impl ApiKeyAwareSignerFactory for TestAuthn {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(TestSignerFactory {
            state: self.state.clone(),
            refresh_allowed: self.refresh_allowed,
        })
    }
}

pub struct PassthroughDialect {
    pub base_url: Url,
}

impl UpstreamDialect for PassthroughDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let _ = upstream;
        let mut url = self.base_url.clone();
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

pub struct TestSignerFactory {
    state: TestState,
    refresh_allowed: bool,
}

#[async_trait]
impl SignerFactory for TestSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(TestSigner {
            state: self.state.clone(),
            refresh_allowed: self.refresh_allowed,
            key: "sk-ant-initial".to_owned(),
        }))
    }
}

pub struct TestSigner {
    state: TestState,
    refresh_allowed: bool,
    key: String,
}

#[async_trait]
impl Signer for TestSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let key =
            HeaderValue::from_str(&self.key).map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })?;
        shaped.headers_mut().insert("x-api-key", key);
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        if !self.refresh_allowed {
            return RetryDecision::Fail;
        }
        self.state.refresh_count.fetch_add(1, Ordering::Relaxed);
        RetryDecision::Refresh {
            new_signer: Arc::new(TestSigner {
                state: self.state.clone(),
                refresh_allowed: false,
                key: "sk-ant-refreshed".to_owned(),
            }),
        }
    }
}

#[derive(Clone)]
pub enum DispatchMode {
    StreamingOk,
    Statuses(Arc<Mutex<VecDeque<StatusCode>>>),
    HeadersOk(HeaderMap),
    Raw {
        status: StatusCode,
        headers: HeaderMap,
        body: Bytes,
    },
}

#[derive(Clone)]
pub struct MockDispatch {
    pub state: TestState,
    pub mode: DispatchMode,
}

#[async_trait]
impl UpstreamDispatch for MockDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let call = self.state.upstream_calls.fetch_add(1, Ordering::Relaxed) + 1;
        println!("upstream_call={call}");

        match &self.mode {
            DispatchMode::StreamingOk => Ok(streaming_response(self.state.clone())),
            DispatchMode::Statuses(statuses) => {
                let status = statuses
                    .lock()
                    .map(|mut statuses| statuses.pop_front().unwrap_or(StatusCode::OK))
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                Ok(json_response(status, default_json_for_status(status)))
            }
            DispatchMode::HeadersOk(headers) => {
                let mut response = json_response(
                    StatusCode::OK,
                    json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}}),
                );
                *response.headers_mut() = headers.clone();
                Ok(response)
            }
            DispatchMode::Raw {
                status,
                headers,
                body,
            } => {
                let mut response = Response::new(Body::from(body.clone()));
                *response.status_mut() = *status;
                *response.headers_mut() = headers.clone();
                Ok(response)
            }
        }
    }
}

pub fn lifecycle_with(authn: TestAuthn, dispatcher: MockDispatch) -> Lifecycle {
    lifecycle_with_parts(authn, Arc::new(dispatcher), LifecycleConfig::default())
}

pub fn lifecycle_with_parts(
    authn: TestAuthn,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
) -> Lifecycle {
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![default_upstream_record()])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        config,
        Arc::new(cc_lb_clock::SystemClock),
    )
}

pub fn lifecycle_with_cache(
    authn: TestAuthn,
    dispatcher: MockDispatch,
    cache: Arc<parking_lot::RwLock<cc_lb_control::UpstreamRateLimitCache>>,
) -> Lifecycle {
    let dispatcher = Arc::new(dispatcher);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![default_upstream_record()])
        .upstream_rate_limit_cache(cache)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}

fn default_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
            .expect("default upstream id parses"),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

pub fn messages_request(body: Bytes) -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("anthropic-version", "2023-06-01")
        .body(body)
        .expect("test request builds")
}

pub async fn collect_body(response: Response<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("test body collects")
        .to_bytes();
    (status, headers, body)
}

/// Deterministically drain the detached keepalive persist task the proxy path
/// spawns post-response (current-thread runtime: these yields run it to done).
pub async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

pub async fn signed_request(base_url: &str) -> SignedRequest {
    let upstream = Upstream::AnthropicDirect { base_url: None };
    let ctx = DialectShapeContext {
        request_id: "test-request".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
    };
    let principal = Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::ApiKey,
    };
    let shaped = shape_request(
        &PassthroughDialect {
            base_url: Url::parse(base_url).expect("test URL parses"),
        },
        &ctx,
        &upstream,
        &principal,
    )
    .expect("test request shapes");
    sign_request(&NoopSigner, shaped)
        .await
        .expect("test request signs")
}

struct NoopSigner;

#[async_trait]
impl Signer for NoopSigner {
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

fn streaming_response(state: TestState) -> Response<Body> {
    let stream = async_stream::stream! {
        let _guard = StreamDropGuard { state: state.clone() };
        for index in 0..54_u64 {
            let frame = format!("event: content_block_delta\ndata: {{\"index\":{index}}}\n\n");
            yield Ok::<Bytes, Infallible>(Bytes::from(frame));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        yield Ok::<Bytes, Infallible>(Bytes::from_static(
            b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":7,\"output_tokens\":42}}\n\n",
        ));
    };
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
}

struct StreamDropGuard {
    state: TestState,
}

impl Drop for StreamDropGuard {
    fn drop(&mut self) {
        self.state.stream_dropped.store(true, Ordering::Relaxed);
        self.state.stream_drop_notify.notify_waiters();
    }
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn default_json_for_status(status: StatusCode) -> serde_json::Value {
    if status.is_success() {
        json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}})
    } else {
        json!({"type":"error","error":{"type":"authentication_error","message":"forced"}})
    }
}

pub struct TestLifecycleBus {
    pub bus: Arc<cc_lb_control::InMemoryBus>,
    _assembler: Option<cc_lb_engine::RequestEventAssemblerHandle>,
    _rate_limit_header: Option<cc_lb_engine::RateLimitHeaderSubscriberHandle>,
}

impl TestLifecycleBus {
    pub fn new() -> Self {
        Self {
            bus: Arc::new(cc_lb_control::InMemoryBus::new()),
            _assembler: None,
            _rate_limit_header: None,
        }
    }

    pub fn with_assembler(mut self, storage: Arc<dyn cc_lb_storage_api::Storage>) -> Self {
        let rx = self.bus.attach_lifecycle_assembler(
            cc_lb_control::event_bus::DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY,
        );
        let bus_arc: Arc<dyn cc_lb_control::RequestEventBus> = self.bus.clone();
        self._assembler = Some(cc_lb_engine::spawn_request_event_assembler(
            rx,
            storage as Arc<dyn cc_lb_storage_api::RequestEventStore>,
            Some(bus_arc),
            Arc::new(cc_lb_observability::NoopMetricsHook),
        ));
        self
    }

    pub fn with_rate_limit_header_subscriber(
        mut self,
        sink: cc_lb_engine::UpstreamRateLimitSink,
    ) -> Self {
        let rx = self.bus.attach_lifecycle_rate_limit_header(
            cc_lb_control::event_bus::DEFAULT_LIFECYCLE_RATE_LIMIT_HEADER_CAPACITY,
        );
        let cache = Arc::new(parking_lot::RwLock::new(
            cc_lb_control::UpstreamRateLimitCache::default(),
        ));
        self._rate_limit_header = Some(cc_lb_engine::spawn_lifecycle_rate_limit_header_subscriber(
            rx,
            cache,
            Some(sink),
        ));
        self
    }

    pub fn with_rate_limit_header_subscriber_and_cache(
        mut self,
        sink: cc_lb_engine::UpstreamRateLimitSink,
        cache: Arc<parking_lot::RwLock<cc_lb_control::UpstreamRateLimitCache>>,
    ) -> Self {
        let rx = self.bus.attach_lifecycle_rate_limit_header(
            cc_lb_control::event_bus::DEFAULT_LIFECYCLE_RATE_LIMIT_HEADER_CAPACITY,
        );
        self._rate_limit_header = Some(cc_lb_engine::spawn_lifecycle_rate_limit_header_subscriber(
            rx,
            cache,
            Some(sink),
        ));
        self
    }

    pub fn bus_arc(&self) -> Arc<dyn cc_lb_control::RequestEventBus> {
        self.bus.clone()
    }
}

impl Default for TestLifecycleBus {
    fn default() -> Self {
        Self::new()
    }
}
