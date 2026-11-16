//! Focused HTTP proofs for the owner-bound Codex Files routes.
//!
//! These tests deliberately exercise `app_router`: the real authentication,
//! registered-purpose gate, session owner fence, body reader, Files handler,
//! selected-owner signer, and an ordinary local HTTP provider recorder. They
//! do not call the Files storage API directly; durable ledger behavior belongs
//! to `cc-lb-engine/tests/codex_files.rs`.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use axum::{Router, routing::any};
use bytes::Bytes;
use cc_lb_clock::{ClockHandle, SystemClock, unix_secs};
use cc_lb_control::{DynamicViewHolder, InMemoryBus, RequestEventBus};
use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::api_keys::secret;
use cc_lb_control::api_keys::session_token::{self, Claims};
use cc_lb_engine::{
    BulkheadRegistry, BulkheadRuntimeConfig, BreakerRegistry, BreakerRuntimeConfig, Lifecycle,
    LifecycleConfig, make_default_dispatcher,
};
use cc_lb_storage_api::account_owner::{AccountOwnerBinding, AccountOwnerStore};
use cc_lb_storage_api::codex_sessions::{
    CodexSessionStore, CredentialPurpose, IssueSession, SessionCredential, SessionFamily,
};
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind, PrincipalStore};
use cc_lb_storage_api::upstream::{
    UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStore,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_storage_api::{ManagedKeyStore, MetaStore};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use cc_lb_upstream::{
    ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, UpstreamError,
};
use futures_util::stream;
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

use super::{DrainController, ProxyState, RequestBodyCaps, ServerStateHandle, app_router};

const PROVIDER_BODY_CAP: usize = 4 * 1024 * 1024;
const OWNER_A: &str = "owner-a";
const OWNER_B: &str = "owner-b";
const WORKSPACE_A: &str = "workspace-a";
const WORKSPACE_B: &str = "workspace-b";

#[derive(Clone, Debug)]
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Reply {
    fn new(status: StatusCode, body: &'static [u8]) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Bytes::from_static(body),
        }
    }

    fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.insert(name, HeaderValue::from_static(value));
        self
    }

    fn with_body(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: body.into(),
        }
    }
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
struct RecorderState {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
}

async fn recorder_handler(
    State(state): State<RecorderState>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, PROVIDER_BODY_CAP)
        .await
        .expect("provider recorder body fits");
    state
        .requests
        .lock()
        .expect("provider request lock")
        .push(CapturedRequest {
            method: parts.method,
            path: parts.uri.path_and_query().expect("provider target").as_str().to_owned(),
            headers: parts.headers,
            body,
        });
    let reply = state
        .replies
        .lock()
        .expect("provider reply lock")
        .pop_front()
        .unwrap_or_else(|| Reply::new(StatusCode::INTERNAL_SERVER_ERROR, b"unexpected provider call"));
    let mut response = Response::new(Body::from(reply.body));
    *response.status_mut() = reply.status;
    *response.headers_mut() = reply.headers;
    response
}

struct Recorder {
    url: Url,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Recorder {
    async fn spawn(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind provider recorder");
        let address = listener.local_addr().expect("provider recorder address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = RecorderState {
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
            requests: requests.clone(),
        };
        let router = Router::new()
            .fallback(any(recorder_handler))
            .with_state(state);
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        Self {
            url: Url::parse(&format!("http://{address}/")).expect("provider recorder URL"),
            requests,
            task,
        }
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.requests.lock().expect("provider request lock").clone()
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum SigningMutation {
    RevokeSession(Arc<SqliteStorage>),
    RemoveWorkspace(Arc<SqliteStorage>, AccountOwnerBinding),
}

#[derive(Clone)]
struct VendorFactory {
    choices: Arc<Mutex<Vec<String>>>,
    mutation: Arc<Mutex<Option<SigningMutation>>>,
    unauthorized_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl VendorFactory {
    fn new() -> Self {
        Self {
            choices: Arc::new(Mutex::new(Vec::new())),
            mutation: Arc::new(Mutex::new(None)),
            unauthorized_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn choices(&self) -> Vec<String> {
        self.choices.lock().expect("signer choice lock").clone()
    }
}

impl ApiKeyAwareSignerFactory for VendorFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        self.choices
            .lock()
            .expect("signer choice lock")
            .push(router_chosen_upstream_name.clone());
        Arc::new(VendorSignerFactory {
            owner: router_chosen_upstream_name,
            mutation: self.mutation.clone(),
            unauthorized_calls: self.unauthorized_calls.clone(),
        })
    }
}

struct VendorSignerFactory {
    owner: String,
    mutation: Arc<Mutex<Option<SigningMutation>>>,
    unauthorized_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl SignerFactory for VendorSignerFactory {
    async fn build(
        &self,
        _upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(VendorSigner {
            token: format!("vendor-token-{}", self.owner),
            mutation: self.mutation.clone(),
            unauthorized_calls: self.unauthorized_calls.clone(),
        }))
    }
}

struct VendorSigner {
    token: String,
    mutation: Arc<Mutex<Option<SigningMutation>>>,
    unauthorized_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl Signer for VendorSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        for name in [
            "authorization",
            "x-api-key",
            "cookie",
            "chatgpt-account-id",
            "cc-lb-error-origin",
            "cc-lb-auth-reason",
        ] {
            assert!(
                shaped.headers().get(name).is_none(),
                "untrusted {name} must be stripped before vendor signing"
            );
        }
        shaped.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.token))
                .expect("vendor token header"),
        );
        let mutation = self.mutation.lock().expect("signing mutation lock").take();
        match mutation {
            Some(SigningMutation::RevokeSession(storage)) => {
                storage.revoke_session_family(
                    "family-a", "renewal-a", unix_secs(std::time::SystemTime::now()),
                ).await.expect("revoke session while signer is awaited");
            }
            Some(SigningMutation::RemoveWorkspace(storage, mut binding)) => {
                let revision = binding.revision;
                binding.revision += 1;
                binding.allowed_workspaces.retain(|workspace| workspace != WORKSPACE_A);
                storage.compare_and_replace_owner_binding(binding, revision)
                    .await.expect("remove selected workspace while signer is awaited");
            }
            None => {}
        }
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
        self.unauthorized_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        RetryDecision::Fail
    }
}

struct SessionSeed {
    access: String,
    renewal: String,
}

struct Fixture {
    router: Router,
    storage: Arc<SqliteStorage>,
    session_a: SessionSeed,
    session_b: SessionSeed,
    recorder_a: Recorder,
    recorder_b: Recorder,
    signer_factory: VendorFactory,
    upstream_a: UpstreamRecord,
    owner_a: AccountOwnerBinding,
    dynamic_view: Arc<DynamicViewHolder>,
    bus: Arc<InMemoryBus>,
}

impl Fixture {
    async fn new(reply_a: Vec<Reply>, reply_b: Vec<Reply>) -> Self {
        let recorder_a = Recorder::spawn(reply_a).await;
        let recorder_b = Recorder::spawn(reply_b).await;
        let clock: ClockHandle = Arc::new(SystemClock);
        let storage = Arc::new(
            open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open files HTTP sqlite storage"),
        );
        storage.initialize().await.expect("initialize files HTTP storage");
        let principal = PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: "files-http-principal".to_owned(),
                kind: PrincipalKind::Human,
                allowed_models: vec!["*".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            unix_secs(clock.now()),
        )
        .await
        .expect("create files HTTP principal");
        let upstream_a = create_upstream(storage.as_ref(), "files-owner-a", &recorder_a.url).await;
        let upstream_b = create_upstream(storage.as_ref(), "files-owner-b", &recorder_b.url).await;
        let owner_a = create_binding(&storage, principal.id, OWNER_A, WORKSPACE_A, upstream_a.id).await;
        let owner_b = create_binding(&storage, principal.id, OWNER_B, WORKSPACE_B, upstream_b.id).await;
        let key_store = KeyStore::new(storage.clone());
        let (_, parent_secret) = key_store
            .create(
                "files-http-principal",
                CreateParams {
                    label: "files-http-parent".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("create files HTTP parent key");
        let (parent_key_id, _) = secret::parse(parent_secret.expose()).expect("parent key id");
        let session_a = issue_session(
            storage.as_ref(),
            &principal.id.to_string(),
            &parent_key_id,
            &owner_a,
            WORKSPACE_A,
            "family-a",
            "access-a",
            "renewal-a",
            unix_secs(clock.now()),
        )
        .await;
        let session_b = issue_session(
            storage.as_ref(),
            &principal.id.to_string(),
            &parent_key_id,
            &owner_b,
            WORKSPACE_B,
            "family-b",
            "access-b",
            "renewal-b",
            unix_secs(clock.now()),
        )
        .await;
        let signer_factory = VendorFactory::new();
        let principal_view = Arc::new(PrincipalView::from_db(&[principal], HashMap::new()));
        let dynamic_view = Arc::new(DynamicViewHolder::new(
            cc_lb_control::dynamic_view::DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(signer_factory.clone()))
                .principal_view(principal_view)
                .upstream_records(vec![upstream_a.clone(), upstream_b.clone()])
                .build(),
        ));
        let builtin_authn = Arc::new(
            BuiltinAuthn::new(
                Arc::new(KeyStore::new(storage.clone() as Arc<dyn ManagedKeyStore>)),
                clock.clone(),
            )
            .with_session_store(storage.clone()),
        );
        let bus = Arc::new(InMemoryBus::new());
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                builtin_authn.clone(),
                dynamic_view.clone(),
                make_default_dispatcher(50),
                LifecycleConfig::default(),
                clock.clone(),
            )
            .with_event_bus(bus.clone()),
        );
        let server_state = Arc::new(ServerStateHandle::new_starting());
        server_state.transition_to_ready();
        let state = ProxyState {
            codex_sessions: Arc::new(cc_lb_engine::codex_sessions::CodexSessionService::new(
                storage.clone(),
                storage.clone(),
                clock.clone(),
            )),
            lifecycle,
            body_caps: RequestBodyCaps {
                messages: 4 * 1024 * 1024,
                files: 4 * 1024 * 1024,
            },
            server_state,
            start_time: std::time::Instant::now(),
            drain_controller: DrainController::new(),
            aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
            storage: storage.clone(),
            dynamic_view: dynamic_view.clone(),
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
        };
        let router = app_router(state, 30);
        Self {
            router,
            storage,
            dynamic_view,
            bus,
            session_a,
            session_b,
            recorder_a,
            recorder_b,
            signer_factory,
            upstream_a,
            owner_a,
        }
    }

    async fn request(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        body: Body,
    ) -> (StatusCode, HeaderMap, Bytes) {
        self.request_with_headers(method, uri, token, HeaderMap::new(), body)
            .await
    }

    async fn request_with_headers(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        headers: HeaderMap,
        body: Body,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .expect("files HTTP request");
        request.headers_mut().extend(headers);
        if let Some(token) = token {
            request.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).expect("session bearer header"),
            );
        }
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("files HTTP router response");
        let (parts, body) = response.into_parts();
        let body = body.collect().await.expect("files HTTP response body").to_bytes();
        (parts.status, parts.headers, body)
    }
}

async fn create_upstream(storage: &SqliteStorage, name: &str, base_url: &Url) -> UpstreamRecord {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::OpenaiOauth,
            base_url: Some(base_url.clone()),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create files HTTP upstream")
}

async fn create_binding(
    storage: &SqliteStorage,
    principal_id: Uuid,
    owner_id: &str,
    workspace_id: &str,
    upstream_id: Uuid,
) -> AccountOwnerBinding {
    let binding = AccountOwnerBinding {
        binding_id: format!("binding-{owner_id}"),
        principal_id: principal_id.to_string(),
        provider: "openai".to_owned(),
        owner_id: owner_id.to_owned(),
        upstream_id,
        owner_generation: 1,
        local_user_id: format!("local-{owner_id}"),
        allowed_workspaces: vec![workspace_id.to_owned(), format!("{workspace_id}-other")],
        provider_workspace_id: format!("provider-{workspace_id}"),
        outbound_region: None,
        revision: 1,
        revoked_at_unix_secs: None,
    };
    storage
        .create_owner_binding(binding.clone())
        .await
        .expect("create files HTTP owner binding");
    binding
}

async fn issue_session(
    storage: &SqliteStorage,
    principal_id: &str,
    parent_key_id: &str,
    binding: &AccountOwnerBinding,
    workspace_id: &str,
    family_id: &str,
    access_id: &str,
    renewal_id: &str,
    now: u64,
) -> SessionSeed {
    let access = session_token::mint_access(&Claims {
        issuer: "https://chatgpt.com".to_owned(),
        local_user_id: binding.local_user_id.clone(),
        workspace_id: workspace_id.to_owned(),
        family_id: family_id.to_owned(),
        access_id: access_id.to_owned(),
        issued_at: now,
        expires_at: now + 3_600,
        plan: "plus".to_owned(),
    })
    .expect("mint access token");
    let renewal = session_token::mint_renewal();
    let family = SessionFamily {
        family_id: family_id.to_owned(),
        principal_id: principal_id.to_owned(),
        parent_key_id: parent_key_id.to_owned(),
        owner_binding_id: binding.binding_id.clone(),
        provider: "openai".to_owned(),
        owner_generation: binding.owner_generation,
        binding_revision: binding.revision,
        workspace_id: workspace_id.to_owned(),
        workspace_generation: 1,
        access_generation: 1,
        local_user_id: binding.local_user_id.clone(),
        plan_type: "plus".to_owned(),
        origin: "https://chatgpt.com".to_owned(),
        access_ttl_secs: 3_600,
        created_at: now,
        expires_at: now + 7_200,
        revoked_at: None,
    };
    storage
        .issue_session(IssueSession {
            family,
            access: SessionCredential {
                credential_id: access_id.to_owned(),
                family_id: family_id.to_owned(),
                purpose: CredentialPurpose::SessionAccess,
                index_hash: access.index_hash,
                verify_hash: access.verify_hash,
                salt: access.secret_salt,
                workspace_generation: 1,
                access_generation: 1,
                issued_at: now,
                expires_at: now + 3_600,
                revoked_at: None,
            },
            renewal: SessionCredential {
                credential_id: renewal_id.to_owned(),
                family_id: family_id.to_owned(),
                purpose: CredentialPurpose::SessionRenewal,
                index_hash: renewal.index_hash,
                verify_hash: renewal.verify_hash,
                salt: renewal.secret_salt,
                workspace_generation: 1,
                access_generation: 1,
                issued_at: now,
                expires_at: now + 7_200,
                revoked_at: None,
            },
        })
        .await
        .expect("issue files HTTP session");
    SessionSeed {
        access: access.plaintext.expose().to_owned(),
        renewal: renewal.plaintext.expose().to_owned(),
    }
}

fn panic_body() -> Body {
    Body::from_stream(stream::once(async {
        panic!("rejected credential body must not be polled");
        #[allow(unreachable_code)]
        Ok::<Bytes, Infallible>(Bytes::new())
    }))
}

fn assert_one_auth_rejection(events: &mut tokio::sync::broadcast::Receiver<LifecycleEvent>) {
    let mut started = 0;
    let mut failed_auth = 0;
    let mut terminated = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            LifecycleEvent::RequestStarted { .. } => started += 1,
            LifecycleEvent::AuthCompleted { result: Err(_), .. } => failed_auth += 1,
            LifecycleEvent::RequestTerminated { client_status, .. } => {
                assert_eq!(client_status, 401);
                terminated += 1;
            }
            _ => {}
        }
    }
    assert_eq!((started, failed_auth, terminated), (1, 1, 1));
}

async fn usage_row_counts(storage: &SqliteStorage) -> (i64, i64) {
    (
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_rollups_v2")
            .fetch_one(storage.pool())
            .await
            .expect("count metered usage rows"),
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_api_usage_v1")
            .fetch_one(storage.pool())
            .await
            .expect("count provider usage rows"),
    )
}

#[tokio::test]
async fn invalid_principal_and_wrong_purpose_never_poll_body_or_sign() {
    let fixture = Fixture::new(vec![], vec![]).await;
    for token in [None, Some("invalid-principal"), Some(fixture.session_a.renewal.as_str())] {
        let mut events = fixture.bus.subscribe_lifecycle();
        let (status, headers, _) = fixture
            .request(Method::POST, "/backend-api/files", token, panic_body())
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers["cc-lb-error-origin"], "principal");
        assert_one_auth_rejection(&mut events);
    }
    let mut events = fixture.bus.subscribe_lifecycle();
    let (status, headers, _) = fixture
        .request(
            Method::POST,
            "/oauth/token",
            Some(&fixture.session_a.access),
            panic_body(),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers["cc-lb-error-origin"], "renewal");
    assert_one_auth_rejection(&mut events);
    assert!(fixture.recorder_a.requests().is_empty());
    assert!(fixture.recorder_b.requests().is_empty());
    assert!(fixture.signer_factory.choices().is_empty());
}

#[tokio::test]
async fn create_duplicate_finalize_and_pdf_reservation_preserve_raw_bodies_without_usage() {
    const CREATE_BODY: &[u8] = b" { \"file_id\":\"pdf-file\", \"upload_url\":\"https://signed.invalid/put\", \"opaque\": [1,2,3] }\n";
    const PDF_PENDING: &[u8] = br#"{"status":"pending","retry":true,"file_id":"pdf-file"}"#;
    const FINALIZE_BODY: &[u8] = br#" { "status":"success", "download_url":"https://signed.invalid/pdf" } "#;
    let fixture = Fixture::new(
        vec![
            Reply::new(StatusCode::CREATED, CREATE_BODY)
                .with_header("content-type", "application/json"),
            Reply::new(StatusCode::ACCEPTED, PDF_PENDING)
                .with_header("retry-after", "3"),
            Reply::new(StatusCode::OK, FINALIZE_BODY),
            Reply::new(StatusCode::OK, FINALIZE_BODY),
        ],
        vec![],
    )
    .await;
    let before_usage = usage_row_counts(&fixture.storage).await;
    let mut events = fixture.bus.subscribe_lifecycle();
    const CREATE_REQUEST: &[u8] = br#"{"file_name":"paper.pdf","mime_type":"application/pdf","opaque":"same-id"}"#;
    let (status, _, body) = fixture
        .request(
            Method::POST,
            "/backend-api/files",
            Some(&fixture.session_a.access),
            Body::from(CREATE_REQUEST),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(&body[..], CREATE_BODY);

    const UPLOADED_REQUEST: &[u8] = br#"{"bytes":4,"sha256":"abc"}"#;
    for (expected_status, expected_body) in [
        (StatusCode::ACCEPTED, PDF_PENDING),
        (StatusCode::OK, FINALIZE_BODY),
        (StatusCode::OK, FINALIZE_BODY),
    ] {
        let (status, headers, body) = fixture
            .request(
                Method::POST,
                "/backend-api/files/pdf-file/uploaded",
                Some(&fixture.session_a.access),
                Body::from(UPLOADED_REQUEST),
            )
            .await;
        assert_eq!(status, expected_status);
        assert_eq!(&body[..], expected_body);
        let ledger_state: String = sqlx::query_scalar(
            "SELECT state FROM codex_files_v1 WHERE file_id = 'pdf-file'",
        ).fetch_one(fixture.storage.pool()).await.expect("HTTP-created file state");
        assert_eq!(
            ledger_state,
            if status == StatusCode::ACCEPTED { "created" } else { "finalized" },
            "PDF reservation responses must not finalize the HTTP-created resource",
        );
        if status == StatusCode::ACCEPTED {
            assert_eq!(headers[header::RETRY_AFTER], "3");
        }
    }
    assert_eq!(usage_row_counts(&fixture.storage).await, before_usage);
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event, LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. }),
            "Files HTTP lifecycle must not invent token usage or a priced completion",
        );
    }
    let calls = fixture.recorder_a.requests();
    assert_eq!(calls.len(), 4, "each client retry is exactly one provider call");
    assert_eq!(calls[0].method, Method::POST);
    assert_eq!(calls[0].path, "/backend-api/files");
    assert_eq!(&calls[0].body[..], CREATE_REQUEST);
    for call in &calls[1..] {
        assert_eq!(call.path, "/backend-api/files/pdf-file/uploaded");
        assert_eq!(&call.body[..], UPLOADED_REQUEST);
    }
    assert!(fixture.recorder_b.requests().is_empty());
    assert_eq!(fixture.signer_factory.choices(), vec!["files-owner-a"; 4]);
}

#[tokio::test]
async fn pdf_create_retry_status_body_and_headers_are_not_converted_to_success() {
    const RETRY_BODY: &[u8] = br#"{"error":"pdf reservation pending","retry":true}"#;
    let fixture = Fixture::new(
        vec![Reply::new(StatusCode::TOO_MANY_REQUESTS, RETRY_BODY).with_header("retry-after", "7")],
        vec![],
    )
    .await;
    let (status, headers, body) = fixture
        .request(
            Method::POST,
            "/backend-api/files",
            Some(&fixture.session_a.access),
            Body::from(r#"{"file_name":"paper.pdf","mime_type":"application/pdf"}"#),
        )
        .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers[header::RETRY_AFTER], "7");
    assert_eq!(&body[..], RETRY_BODY);
    assert_eq!(fixture.recorder_a.requests().len(), 1);
    assert!(fixture.recorder_b.requests().is_empty());
}

#[tokio::test]
async fn unobserved_native_and_encoded_ids_reach_selected_owner_preserving_404() {
    const MISSING: &[u8] = br#"{"error":{"message":"native file missing","type":"not_found"}}"#;
    const SUCCESS: &[u8] = br#"{"status":"success","opaque":"same-id"}"#;
    let fixture = Fixture::new(
        vec![
            Reply::new(StatusCode::NOT_FOUND, MISSING),
            Reply::new(StatusCode::OK, SUCCESS),
        ],
        vec![],
    )
    .await;
    for (path, expected_status, expected_body) in [
        ("/backend-api/files/native-file/uploaded", StatusCode::NOT_FOUND, MISSING),
        ("/backend-api/files/native%20id/uploaded?purpose=pdf&opaque=a%2Fb", StatusCode::OK, SUCCESS),
    ] {
        let (status, _, body) = fixture
            .request(Method::POST, path, Some(&fixture.session_a.access), Body::from("{}"))
            .await;
        assert_eq!(status, expected_status);
        assert_eq!(&body[..], expected_body);
    }
    let calls = fixture.recorder_a.requests();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].path, "/backend-api/files/native-file/uploaded");
    assert_eq!(calls[1].path, "/backend-api/files/native%20id/uploaded?purpose=pdf&opaque=a%2Fb");
    assert!(fixture.recorder_b.requests().is_empty());
}

#[tokio::test]
async fn equal_ids_remain_owner_scoped_and_only_vendor_credentials_are_forwarded() {
    const CREATE: &[u8] = br#"{"file_id":"same-id"}"#;
    let fixture = Fixture::new(
        vec![Reply::new(StatusCode::OK, CREATE), Reply::new(StatusCode::OK, b"{}")],
        vec![Reply::new(StatusCode::OK, CREATE)],
    )
    .await;
    // Both owners have actually created this same native ID through the router.
    for token in [&fixture.session_b.access, &fixture.session_a.access] {
        let (status, _, _) = fixture
            .request(Method::POST, "/backend-api/files", Some(token), Body::from("{}"))
            .await;
        assert_eq!(status, StatusCode::OK);
    }
    let owner_b_calls = fixture.recorder_b.requests().len();
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("openai-api-key", "caller-vendor-key"),
        ("cookie", "caller-session=secret"),
        ("chatgpt-account-id", "caller-other-account"),
        ("cc-lb-error-origin", "principal"),
        ("cc-lb-auth-reason", "expired"),
        ("x-requested-with", "files-http-test"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(&fixture.session_a.access).expect("session API key header"),
    );
    let (status, _, _) = fixture
        .request_with_headers(
            Method::POST,
            "/backend-api/files/same-id/uploaded",
            Some(&fixture.session_a.access),
            headers,
            Body::from(r#"{"text":"same-id appears here too"}"#),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.recorder_b.requests().len(), owner_b_calls, "no other-owner lookup or failover");
    let calls = fixture.recorder_a.requests();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].headers[header::AUTHORIZATION], "Bearer vendor-token-files-owner-a");
    assert_eq!(calls[1].headers["x-requested-with"], "files-http-test");
    for name in ["x-api-key", "cookie", "chatgpt-account-id", "cc-lb-error-origin", "cc-lb-auth-reason"] {
        assert!(calls[1].headers.get(name).is_none(), "{name} leaked");
    }
    assert_eq!(fixture.signer_factory.choices(), vec!["files-owner-b", "files-owner-a", "files-owner-a"]);
}

#[tokio::test]
async fn upstream_401_is_raw_and_provenance_cannot_trigger_refresh_or_owner_switch() {
    const ERROR: &[u8] = br#" { "error": { "message":"vendor rejected", "code":"invalid_api_key" } } "#;
    let fixture = Fixture::new(
        vec![Reply::new(StatusCode::UNAUTHORIZED, ERROR)
            .with_header("cc-lb-error-origin", "principal")
            .with_header("cc-lb-auth-reason", "expired")
            .with_header("www-authenticate", "Bearer error=\"invalid_token\"")],
        vec![],
    )
    .await;
    let (status, headers, body) = fixture
        .request(
            Method::POST,
            "/backend-api/files/native-file/uploaded",
            Some(&fixture.session_a.access),
            Body::from("{}"),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(&body[..], ERROR);
    assert_eq!(headers["cc-lb-error-origin"], "upstream");
    assert!(headers.get("cc-lb-auth-reason").is_none());
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer error=\"invalid_token\"");
    assert_eq!(
        fixture.signer_factory.unauthorized_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "provider401 must not invoke credential refresh",
    );
    assert_eq!(fixture.recorder_a.requests().len(), 1);
    assert!(fixture.recorder_b.requests().is_empty());
    assert_eq!(fixture.signer_factory.choices(), vec!["files-owner-a"]);
}

#[tokio::test]
async fn disabled_deleted_or_missing_selected_upstream_is_503_without_failover() {
    for state in ["disabled", "deleted", "missing-from-view"] {
        let fixture = Fixture::new(vec![], vec![]).await;
        match state {
            "disabled" => {
                UpstreamStore::set_enabled(
                    fixture.storage.as_ref(), fixture.upstream_a.id, fixture.upstream_a.revision, false,
                ).await.expect("disable selected upstream");
            }
            "deleted" => {
                UpstreamStore::soft_delete(
                    fixture.storage.as_ref(), fixture.upstream_a.id, fixture.upstream_a.revision,
                ).await.expect("delete selected upstream");
            }
            _ => {
                let view = fixture.dynamic_view.load();
                let records = view.upstreams_snapshot().iter()
                    .filter(|record| record.id != fixture.upstream_a.id).cloned().collect();
                fixture.dynamic_view.store(
                    cc_lb_control::DynamicViewBuilder::from_view(&view)
                        .upstream_records(records).build(),
                );
            }
        }
        let (status, _, body) = fixture
            .request(
                Method::POST,
                "/backend-api/files/unavailable/uploaded",
                Some(&fixture.session_a.access),
                Body::from("{}"),
            )
            .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{state}");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("unavailable JSON");
        assert_eq!(payload["error"], "upstream_affinity_unavailable", "{state}");
        assert!(fixture.recorder_a.requests().is_empty(), "{state}");
        assert!(fixture.recorder_b.requests().is_empty(), "{state}");
        assert!(fixture.signer_factory.choices().is_empty(), "{state}");
    }
}

#[tokio::test]
async fn empty_file_id_route_never_dispatches() {
    let fixture = Fixture::new(vec![], vec![]).await;
    let (status, _, _) = fixture
        .request(
            Method::POST, "/backend-api/files//uploaded",
            Some(&fixture.session_a.access), Body::from("{}"),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(fixture.recorder_a.requests().is_empty());
    assert!(fixture.recorder_b.requests().is_empty());
    assert!(fixture.signer_factory.choices().is_empty());
}

#[tokio::test]
async fn signed_urls_are_returned_verbatim_without_any_proxy_connection() {
    // A connection trap counts even a failed TLS handshake, unlike an HTTP
    // recorder that would miss attempts to fetch an HTTPS signed-storage URL.
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await.expect("bind URL trap");
    let address = listener.local_addr().expect("URL trap address");
    let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted = connections.clone();
    let trap = tokio::spawn(async move {
        while let Ok((_socket, _)) = listener.accept().await {
            accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    struct AbortTrap(JoinHandle<()>);
    impl Drop for AbortTrap {
        fn drop(&mut self) { self.0.abort(); }
    }
    let _trap = AbortTrap(trap);
    let create_body = format!(
        "{{ \"file_id\":\"url-file\", \"upload_url\":\"https://{address}/put?sig=secret\" }}"
    );
    let finalize_body = format!(
        "{{ \"download_url\":\"https://{address}/get?sig=secret\", \"opaque\":true }}"
    );
    let fixture = Fixture::new(
        vec![
            Reply::with_body(StatusCode::OK, create_body.clone()),
            Reply::with_body(StatusCode::OK, finalize_body.clone()),
        ],
        vec![],
    ).await;
    for (path, expected) in [
        ("/backend-api/files", create_body),
        ("/backend-api/files/url-file/uploaded", finalize_body),
    ] {
        let (status, _, body) = fixture.request(
            Method::POST, path, Some(&fixture.session_a.access), Body::from("{}"),
        ).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], expected.as_bytes());
    }
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(fixture.recorder_a.requests().len(), 2);
    assert!(fixture.recorder_b.requests().is_empty());
}

#[tokio::test]
async fn awaited_vendor_signing_rechecks_revocation_and_workspace_access_before_dispatch() {
    for revoke_family in [true, false] {
        let fixture = Fixture::new(vec![], vec![]).await;
        *fixture.signer_factory.mutation.lock().expect("signing mutation lock") =
            Some(if revoke_family {
                SigningMutation::RevokeSession(fixture.storage.clone())
            } else {
                SigningMutation::RemoveWorkspace(fixture.storage.clone(), fixture.owner_a.clone())
            });
        let (status, _, _) = fixture.request(
            Method::POST,
            "/backend-api/files/native-file/uploaded",
            Some(&fixture.session_a.access),
            Body::from("{}"),
        ).await;
        assert_eq!(
            status,
            if revoke_family { StatusCode::UNAUTHORIZED } else { StatusCode::BAD_REQUEST },
        );
        assert_eq!(fixture.signer_factory.choices(), vec!["files-owner-a"]);
        assert!(
            fixture.signer_factory.mutation.lock().expect("signing mutation lock").is_none(),
            "mutation ran during the awaited real vendor-signing boundary",
        );
        assert!(fixture.recorder_a.requests().is_empty(), "stale signed request was dispatched");
        assert!(fixture.recorder_b.requests().is_empty(), "owner changed after invalidation");
    }
}

#[tokio::test]
async fn request_cap_and_body_read_failure_emit_one_typed_terminal_without_dispatch() {
    let fixture = Fixture::new(vec![], vec![]).await;
    for exceeds_cap in [true, false] {
        let mut events = fixture.bus.subscribe_lifecycle();
        let mut headers = HeaderMap::new();
        let body = if exceeds_cap {
            headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&(PROVIDER_BODY_CAP + 1).to_string()).expect("body cap header"),
            );
            panic_body()
        } else {
            Body::from_stream(stream::once(async {
                Err::<Bytes, _>(std::io::Error::other("fixture ingress read failure"))
            }))
        };
        let (status, headers, _) = fixture.request_with_headers(
            Method::POST, "/backend-api/files", Some(&fixture.session_a.access), headers, body,
        ).await;
        assert_eq!(
            status,
            if exceeds_cap { StatusCode::PAYLOAD_TOO_LARGE } else { StatusCode::BAD_REQUEST },
        );
        assert_eq!(headers["cc-lb-error-origin"], "proxy");
        let mut terminated = 0;
        while let Ok(event) = events.try_recv() {
            if let LifecycleEvent::RequestTerminated {
                client_status, reason, internal_errors, ..
            } = event {
                terminated += 1;
                assert_eq!(client_status, status.as_u16());
                let expected_code = if exceeds_cap { "body_too_large" } else { "body_read_failed" };
                assert!(
                    matches!(reason, cc_lb_lifecycle::TerminationReason::ErrorCode(code) if code == expected_code),
                    "ingress rejection must keep its typed terminal reason",
                );
                assert_eq!(internal_errors.len(), 1);
                assert_eq!(internal_errors[0].stage, cc_lb_domain::InternalErrorStage::Ingress);
            }
        }
        assert_eq!(terminated, 1);
        assert!(fixture.recorder_a.requests().is_empty());
        assert!(fixture.recorder_b.requests().is_empty());
        assert!(fixture.signer_factory.choices().is_empty());
    }
}

#[tokio::test]
async fn oversized_provider_response_emits_one_relay_failure_without_exposing_success() {
    let fixture = Fixture::new(
        vec![Reply::with_body(StatusCode::OK, vec![b'x'; PROVIDER_BODY_CAP + 1])],
        vec![],
    ).await;
    let mut events = fixture.bus.subscribe_lifecycle();
    let (status, headers, body) = fixture.request(
        Method::POST, "/backend-api/files", Some(&fixture.session_a.access), Body::from("{}"),
    ).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(headers["cc-lb-error-origin"], "proxy");
    let error: serde_json::Value = serde_json::from_slice(&body).expect("bounded relay error");
    assert_eq!(error["error"], "upstream_dispatch_failed");
    let mut terminated = 0;
    while let Ok(event) = events.try_recv() {
        if let LifecycleEvent::RequestTerminated { client_status, internal_errors, .. } = event {
            terminated += 1;
            assert_eq!(client_status, 502);
            assert_eq!(internal_errors.len(), 1);
            assert_eq!(internal_errors[0].stage, cc_lb_domain::InternalErrorStage::Relay);
        }
    }
    assert_eq!(terminated, 1);
    assert_eq!(fixture.recorder_a.requests().len(), 1);
    assert!(fixture.recorder_b.requests().is_empty());
}
