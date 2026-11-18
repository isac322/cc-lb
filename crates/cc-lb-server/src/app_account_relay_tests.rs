use super::*;
use cc_lb_control::api_keys::session_token::{self, Claims, MintedToken};
use cc_lb_domain::Upstream;
use cc_lb_storage_api::account_owner::{AccountOwnerBinding, AccountOwnerStore};
use cc_lb_storage_api::codex_sessions::{
    CodexSessionStore, CredentialPurpose, IssueSession, SessionCredential, SessionFamily,
};
use cc_lb_upstream::{ShapedRequest, SigningCapability};
use std::sync::Mutex;
use tokio::sync::broadcast::Receiver;

const ACCOUNT_PRINCIPAL: &str = "00000000-0000-0000-0000-000000000011";
const OWNER_A: &str = "00000000-0000-0000-0000-000000000001";
const OWNER_B: &str = "00000000-0000-0000-0000-000000000002";
const CHECK: &str = "/backend-api/wham/accounts/check";

struct RelayFactory {
    choices: Arc<Mutex<Vec<String>>>,
    revoke_during_sign: Option<cc_lb_storage_sqlite::SqliteStorage>,
}

impl ApiKeyAwareSignerFactory for RelayFactory {
    fn with_router_choice(&self, name: String) -> Arc<dyn SignerFactory> {
        self.choices.lock().unwrap().push(name.clone());
        Arc::new(RelaySigner {
            name,
            revoke_during_sign: self.revoke_during_sign.clone(),
        })
    }
}

struct RelaySigner {
    name: String,
    revoke_during_sign: Option<cc_lb_storage_sqlite::SqliteStorage>,
}

#[async_trait]
impl SignerFactory for RelaySigner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(Self {
            name: self.name.clone(),
            revoke_during_sign: self.revoke_during_sign.clone(),
        }))
    }
}

#[async_trait]
impl Signer for RelaySigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        // The real storage revoke is deliberately between the first owner fence
        // and the final fence, not an artificial pre-admission auth failure.
        if let Some(storage) = &self.revoke_during_sign {
            storage
                .revoke_session_family("relay-family", "renewal", 100)
                .await
                .unwrap();
        }
        assert!(shaped.headers().get("authorization").is_none());
        assert!(shaped.headers().get("chatgpt-account-id").is_none());
        shaped.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer provider-{}", self.name)).unwrap(),
        );
        shaped.headers_mut().insert(
            "chatgpt-account-id",
            capability
                .workspace()
                .expect("fenced workspace supplied")
                .clone(),
        );
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(
        &self,
        _error: &cc_lb_upstream::UpstreamError,
    ) -> cc_lb_upstream::RetryDecision {
        panic!("account controls must not enter retry/refresh handling")
    }
}

struct RelayDispatch {
    sent: Mutex<Vec<SignedRequest>>,
    status: StatusCode,
    headers: Mutex<HeaderMap>,
    body: Bytes,
    fail: bool,
}

#[async_trait]
impl UpstreamDispatch for RelayDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
        self.sent.lock().unwrap().push(request);
        if self.fail {
            return Err(cc_lb_engine::DispatchError::Transport {
                source: Box::new(io::Error::other("fixture transport failure")),
            });
        }
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.lock().unwrap().clone();
        Ok(response)
    }
}

struct RelayFixture {
    router: Router,
    storage: cc_lb_storage_sqlite::SqliteStorage,
    view: Arc<DynamicViewHolder>,
    access: MintedToken,
    renewal: MintedToken,
    dispatch: Arc<RelayDispatch>,
    choices: Arc<Mutex<Vec<String>>>,
    events: Receiver<LifecycleEvent>,
    admission_state: ProxyState,
}

fn relay_credential(
    id: &str,
    purpose: CredentialPurpose,
    token: &MintedToken,
) -> SessionCredential {
    SessionCredential {
        credential_id: id.into(),
        family_id: "relay-family".into(),
        purpose,
        index_hash: token.index_hash,
        verify_hash: token.verify_hash,
        salt: token.secret_salt,
        workspace_generation: 1,
        access_generation: 1,
        issued_at: 100,
        expires_at: 200,
        revoked_at: None,
    }
}

impl RelayFixture {
    async fn new(
        selected: &str,
        status: StatusCode,
        body: Bytes,
        fail: bool,
        revoke_during_sign: bool,
        region: Option<&str>,
    ) -> Self {
        let clock: ClockHandle = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let storage = cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
            .await
            .unwrap();
        sqlx::migrate!("../cc-lb-storage-sqlite/migrations")
            .run(storage.pool())
            .await
            .unwrap();
        for statement in [
            "INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000011','relay',100,100)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000001','owner-a','openai_oauth',100,100)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000002','owner-b','openai_oauth',100,100)",
            "INSERT INTO managed_keys_v1 (secret_hash,created_at,status,principal_id,key_id,label,verify_hash,secret_salt,limit_overrides,last_4,index_hash,updated_at) VALUES ('parent-hash',100,'active','00000000-0000-0000-0000-000000000011','parent','parent-label',zeroblob(32),zeroblob(16),'[]','1234',zeroblob(32),100)",
        ] {
            sqlx::query(statement)
                .execute(storage.pool())
                .await
                .unwrap();
        }
        for (suffix, upstream) in [("a", OWNER_A), ("b", OWNER_B)] {
            storage
                .create_owner_binding(AccountOwnerBinding {
                    binding_id: format!("binding-{suffix}"),
                    principal_id: ACCOUNT_PRINCIPAL.into(),
                    provider: "openai".into(),
                    owner_id: format!("owner-{suffix}"),
                    upstream_id: upstream.parse().unwrap(),
                    owner_generation: 1,
                    local_user_id: "local-user".into(),
                    allowed_workspaces: vec![format!("local-{suffix}")],
                    provider_workspace_id: format!("provider-workspace-{suffix}"),
                    outbound_region: region.map(str::to_owned),
                    revision: 1,
                    revoked_at_unix_secs: None,
                })
                .await
                .unwrap();
        }
        let access = session_token::mint_access(&Claims {
            issuer: "https://proxy.example.com".into(),
            local_user_id: "local-user".into(),
            workspace_id: format!("local-{selected}"),
            family_id: "relay-family".into(),
            access_id: "access".into(),
            issued_at: 100,
            expires_at: 200,
            plan: "unknown".into(),
        })
        .unwrap();
        let renewal = session_token::mint_renewal();
        storage
            .issue_session(IssueSession {
                family: SessionFamily {
                    family_id: "relay-family".into(),
                    principal_id: ACCOUNT_PRINCIPAL.into(),
                    parent_key_id: "parent".into(),
                    owner_binding_id: format!("binding-{selected}"),
                    provider: "openai".into(),
                    owner_generation: 1,
                    binding_revision: 1,
                    workspace_id: format!("local-{selected}"),
                    workspace_generation: 1,
                    access_generation: 1,
                    local_user_id: "local-user".into(),
                    plan_type: "unknown".into(),
                    origin: "https://proxy.example.com".into(),
                    access_ttl_secs: 100,
                    created_at: 100,
                    expires_at: 200,
                    revoked_at: None,
                },
                access: relay_credential("access", CredentialPurpose::SessionAccess, &access),
                renewal: relay_credential("renewal", CredentialPurpose::SessionRenewal, &renewal),
            })
            .await
            .unwrap();
        let authn = Arc::new(
            BuiltinAuthn::new(
                Arc::new(KeyStore::new(
                    Arc::new(FixtureKeyStore) as Arc<dyn ManagedKeyStore>
                )),
                clock.clone(),
            )
            .with_session_store(Arc::new(storage.clone())),
        );
        let choices = Arc::new(Mutex::new(Vec::new()));
        let view = Arc::new(DynamicViewHolder::new(
            cc_lb_control::DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(RelayFactory {
                    choices: choices.clone(),
                    revoke_during_sign: revoke_during_sign.then(|| storage.clone()),
                }))
                .principal_view(Arc::new(PrincipalView::for_tests(
                    ACCOUNT_PRINCIPAL,
                    true,
                    vec!["*".into()],
                    Vec::new(),
                    HashMap::new(),
                )))
                .upstream_records(vec![
                    UpstreamRecord {
                        id: OWNER_A.parse().unwrap(),
                        name: "owner-a".into(),
                        kind: cc_lb_storage_api::UpstreamKind::OpenaiOauth,
                        enabled: true,
                        ..Default::default()
                    },
                    UpstreamRecord {
                        id: OWNER_B.parse().unwrap(),
                        name: "owner-b".into(),
                        kind: cc_lb_storage_api::UpstreamKind::OpenaiOauth,
                        enabled: true,
                        ..Default::default()
                    },
                ])
                .build(),
        ));
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("17"));
        headers.insert(
            "location",
            HeaderValue::from_static("https://provider.example/never-follow"),
        );
        headers.insert("cc-lb-error-origin", HeaderValue::from_static("principal"));
        headers.insert("cc-lb-auth-reason", HeaderValue::from_static("expired"));
        headers.insert(
            "x-request-id",
            HeaderValue::from_static("account-provider-request"),
        );
        headers.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("37"),
        );
        headers.insert(
            "x-ratelimit-remaining-requests",
            HeaderValue::from_static("12"),
        );
        let dispatch = Arc::new(RelayDispatch {
            sent: Mutex::new(Vec::new()),
            status,
            headers: Mutex::new(headers),
            body,
            fail,
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let events = bus.subscribe_lifecycle();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                view.clone(),
                dispatch.clone(),
                LifecycleConfig::default(),
                clock.clone(),
            )
            .with_event_bus(bus as Arc<dyn RequestEventBus>),
        );
        let state = ProxyState {
            codex_sessions: Arc::new(cc_lb_engine::codex_sessions::CodexSessionService::new(
                Arc::new(storage.clone()),
                Arc::new(storage.clone()),
                clock.clone(),
            )),
            lifecycle,
            account_dispatcher: dispatch.clone(),
            body_caps: RequestBodyCaps {
                messages: 1024,
                files: 1024,
            },
            server_state: Arc::new(ServerStateHandle::new_starting()),
            start_time: Instant::now(),
            drain_controller: DrainController::new(),
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            storage: Arc::new(storage.clone()),
            dynamic_view: view.clone(),
            builtin_authn: authn,
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
        let admission_state = state.clone();
        let router = scoped_proxy_routes(state, RequestIdState::uuid_v7("relay_"), 30, None, false);
        Self {
            router,
            storage,
            view,
            access,
            renewal,
            dispatch,
            choices,
            events,
            admission_state,
        }
    }

    async fn request(&self, method: http::Method, path: &str, body: Body) -> Response<Body> {
        self.request_with_bearer(method, path, body, self.access.plaintext.expose())
            .await
    }

    async fn request_with_bearer(
        &self,
        method: http::Method,
        path: &str,
        body: Body,
        bearer: &str,
    ) -> Response<Body> {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", format!("Bearer {bearer}"))
                    .header("chatgpt-account-id", "injected-workspace")
                    .header("cookie", "provider-cookie=must-not-forward")
                    .header("host", "attacker.invalid")
                    .header("forwarded", "host=attacker.invalid;proto=http")
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["cache-control"], "no-store");
        response
    }

    fn lifecycle_events(&mut self) -> Vec<LifecycleEvent> {
        let mut result = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            result.push(event);
        }
        result
    }
}

#[tokio::test]
async fn account_revoked_while_waiting_for_capacity_never_dispatches() {
    let fixture = RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, false, None).await;
    let gate = fixture
        .admission_state
        .websocket_bulkheads
        .bulkhead_with_dispatcher(
            format!("account-control:{OWNER_A}"),
            BulkheadRuntimeConfig {
                max_conns_per_upstream: 1,
                semaphore_permits: 1,
                acquire_timeout: Duration::from_secs(1),
            },
            fixture.dispatch.clone(),
        );
    let permit = gate.acquire_owned().await.unwrap();
    let body_read = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = body_read.clone();
    let body = Body::from_stream(stream::once(async move {
        observed.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok::<_, std::io::Error>(Bytes::from_static(b"{}"))
    }));
    let response = fixture.request(http::Method::GET, "/backend-api/wham/usage", body);
    tokio::pin!(response);
    futures_util::future::poll_fn(|cx| {
        assert!(
            response.as_mut().poll(cx).is_pending(),
            "request bypassed occupied capacity"
        );
        if body_read.load(std::sync::atomic::Ordering::SeqCst) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
    fixture
        .storage
        .revoke_session_family("relay-family", "renewal", 100)
        .await
        .unwrap();
    drop(permit);
    let response = response.await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn account_capacity_is_held_until_response_body_is_dropped() {
    let fixture = RelayFixture::new(
        "a",
        StatusCode::OK,
        Bytes::from_static(b"opaque response"),
        false,
        false,
        None,
    )
    .await;
    let gate = fixture
        .admission_state
        .websocket_bulkheads
        .bulkhead_with_dispatcher(
            format!("account-control:{OWNER_A}"),
            BulkheadRuntimeConfig {
                max_conns_per_upstream: 1,
                semaphore_permits: 1,
                acquire_timeout: Duration::from_secs(1),
            },
            fixture.dispatch.clone(),
        );
    let response = fixture
        .request(http::Method::GET, "/backend-api/wham/usage", Body::empty())
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        gate.active(),
        1,
        "response must retain selected-owner capacity"
    );
    drop(response);
    assert_eq!(gate.active(), 0, "dropping the body must release capacity");
}

#[tokio::test]
async fn saturated_account_owner_does_not_block_another_owner() {
    let a = RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, false, None).await;
    let mut b = RelayFixture::new("b", StatusCode::OK, Bytes::new(), false, false, None).await;
    b.admission_state.websocket_bulkheads = a.admission_state.websocket_bulkheads.clone();
    b.router = scoped_proxy_routes(
        b.admission_state.clone(),
        RequestIdState::uuid_v7("relay_"),
        30,
        None,
        false,
    );
    let gate = a
        .admission_state
        .websocket_bulkheads
        .bulkhead_with_dispatcher(
            format!("account-control:{OWNER_A}"),
            BulkheadRuntimeConfig {
                max_conns_per_upstream: 1,
                semaphore_permits: 1,
                acquire_timeout: Duration::ZERO,
            },
            a.dispatch.clone(),
        );
    let _permit = gate.acquire_owned().await.unwrap();
    let response = a
        .request(http::Method::GET, "/backend-api/wham/usage", Body::empty())
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(a.dispatch.sent.lock().unwrap().is_empty());
    let response = b
        .request(http::Method::GET, "/backend-api/wham/usage", Body::empty())
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(b.dispatch.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn selected_owner_a_and_b_each_dispatch_once_with_raw_control_request() {
    for (selected, expected_id) in [("a", OWNER_A), ("b", OWNER_B)] {
        let mut fixture = RelayFixture::new(
            selected,
            StatusCode::OK,
            Bytes::from_static(br#"{"usage":{"input_tokens":123,"output_tokens":456}}"#),
            false,
            false,
            None,
        )
        .await;
        let query = "key=retain%2Fraw&key=&cursor=a+b%20c&repeat=1&repeat=2";
        let path = format!("/backend-api/wham/usage/thread_usage/query_v2?{query}");
        let opaque = Bytes::from_static(b"opaque\0not-json\xff");
        let response = fixture
            .request(http::Method::POST, &path, Body::from(opaque.clone()))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cc-lb-error-origin"], "upstream");
        assert!(response.headers().get("cc-lb-auth-reason").is_none());
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
            fixture.dispatch.body
        );
        {
            let sent = fixture.dispatch.sent.lock().unwrap();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].url().as_str(), format!("https://chatgpt.com{path}"));
            assert_eq!(sent[0].method(), http::Method::POST);
            assert_eq!(sent[0].body(), &opaque);
            assert_eq!(
                sent[0].headers()["chatgpt-account-id"].to_str().unwrap(),
                format!("provider-workspace-{selected}")
            );
            assert_eq!(
                sent[0].headers()["authorization"].to_str().unwrap(),
                format!("Bearer provider-owner-{selected}")
            );
            for name in ["x-api-key", "x-goog-api-key", "cookie", "host"] {
                assert!(sent[0].headers().get(name).is_none());
            }
        }
        assert_eq!(
            *fixture.choices.lock().unwrap(),
            vec![format!("owner-{selected}")]
        );
        let events = fixture.lifecycle_events();
        assert_eq!(events.iter().filter(|event| matches!(event, LifecycleEvent::UpstreamAttempt { upstream_id, attempt_num: 1, .. } if upstream_id.to_string() == expected_id)).count(), 1);
        assert!(events.iter().any(|event| matches!(event, LifecycleEvent::RouteCompleted { result: Ok(route), .. } if route.upstream_id.to_string() == expected_id && route.model.is_none())));
        assert!(events.iter().any(|event| matches!(
            event,
            LifecycleEvent::UpstreamResponseStarted { status: 200, .. }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::RequestTerminated { .. }))
                .count(),
            1
        );
        assert!(events.iter().any(|event| matches!(
            event,
            LifecycleEvent::RequestTerminated { request_body_bytes: Some(bytes), request_body_read_ms: Some(_), .. }
                if *bytes == opaque.len() as u64
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            LifecycleEvent::UpstreamResponseStarted { headers, .. }
                if headers.request_id.as_deref() == Some("account-provider-request")
                    && headers.provider_headers.get("x-codex-primary-used-percent").map(String::as_str) == Some("37")
                    && headers.provider_headers.get("x-ratelimit-remaining-requests").map(String::as_str) == Some("12")
        )));
    }
}

#[tokio::test]
async fn provider_non_success_status_body_and_retry_after_are_exact() {
    for status in [302, 400, 401, 403, 429, 500] {
        let body = Bytes::from_static(b"opaque provider error\0\xff");
        let fixture = RelayFixture::new(
            "a",
            StatusCode::from_u16(status).unwrap(),
            body.clone(),
            false,
            false,
            None,
        )
        .await;
        let response = fixture
            .request(http::Method::GET, CHECK, Body::empty())
            .await;
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["retry-after"], "17");
        assert_eq!(
            response.headers()["location"],
            "https://provider.example/never-follow"
        );
        assert_eq!(response.headers()["cc-lb-error-origin"], "upstream");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
            body
        );
        assert_eq!(fixture.dispatch.sent.lock().unwrap().len(), 1);
        assert_eq!(fixture.choices.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn accounts_check_projects_only_selected_provider_workspace_using_trusted_family() {
    let accounts = serde_json::json!({"accounts": [
        {"id":"foreign", "can_access_with_session":true},
        {"id":"provider-workspace-b", "can_access_with_session":true, "plan_type":"team", "workspace_backend_origin":"https://provider.invalid", "account_routing_override":"us_cr"}
    ], "default_account_id":"foreign", "secret":"provider-only"});
    let fixture = RelayFixture::new(
        "b",
        StatusCode::OK,
        Bytes::from(serde_json::to_vec(&accounts).unwrap()),
        false,
        false,
        None,
    )
    .await;
    let response = fixture
        .request(http::Method::GET, CHECK, Body::empty())
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let projected: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(projected["default_account_id"], "local-b");
    assert_eq!(projected["accounts"].as_array().unwrap().len(), 1);
    assert_eq!(projected["accounts"][0]["id"], "local-b");
    assert_eq!(
        projected["accounts"][0]["workspace_backend_origin"],
        "https://proxy.example.com"
    );
    assert_eq!(
        projected["accounts"][0]["account_routing_override"],
        "NO_CONSTRAINT"
    );
    assert!(!String::from_utf8_lossy(&bytes).contains("provider-workspace"));
    assert!(!String::from_utf8_lossy(&bytes).contains("provider.invalid"));
    assert!(projected.get("secret").is_none());
    assert_eq!(fixture.dispatch.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn revoked_during_signing_never_dispatches_or_selects_another_owner() {
    let fixture = RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, true, None).await;
    let response = fixture
        .request(
            http::Method::POST,
            "/backend-api/wham/rate-limit-reset-credits/consume",
            Body::from("opaque"),
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(*fixture.choices.lock().unwrap(), vec!["owner-a"]);
    assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn selected_owner_loss_and_unresolved_residency_fail_closed_before_signing() {
    for region in [None, Some("us")] {
        let fixture =
            RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, false, region).await;
        if region.is_none() {
            let view = fixture.view.load();
            fixture.view.store(
                cc_lb_control::DynamicViewBuilder::from_view(&view)
                    .upstream_records(
                        view.upstreams_snapshot()
                            .iter()
                            .filter(|record| record.id.to_string() != OWNER_A)
                            .cloned()
                            .collect(),
                    )
                    .build(),
            );
        }
        let response = fixture
            .request(http::Method::GET, "/backend-api/wham/usage", Body::empty())
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
        assert!(fixture.choices.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn consumption_and_nudge_transport_failures_are_never_retried() {
    for path in [
        "/backend-api/wham/rate-limit-reset-credits/consume",
        "/backend-api/wham/accounts/send_add_credits_nudge_email",
    ] {
        let mut fixture =
            RelayFixture::new("a", StatusCode::OK, Bytes::new(), true, false, None).await;
        let response = fixture
            .request(http::Method::POST, path, Body::from("not-json"))
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(fixture.dispatch.sent.lock().unwrap().len(), 1);
        assert_eq!(fixture.choices.lock().unwrap().len(), 1);
        assert!(
            !fixture
                .lifecycle_events()
                .iter()
                .any(|event| matches!(event, LifecycleEvent::UpstreamResponseStarted { .. }))
        );
    }
}

#[tokio::test]
async fn principal_and_renewal_credentials_cannot_read_account_bodies_even_on_head() {
    let fixture = RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, false, None).await;
    for method in [http::Method::GET, http::Method::HEAD] {
        for bearer in [
            TEST_KEY.plaintext.as_str(),
            fixture.renewal.plaintext.expose(),
        ] {
            let response = fixture
                .request_with_bearer(
                    method.clone(),
                    CHECK,
                    Body::from_stream(stream::once(panic_when_polled())),
                    bearer,
                )
                .await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
    assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
    assert!(fixture.choices.lock().unwrap().is_empty());
}

#[tokio::test]
async fn disabled_owner_rejects_before_reading_the_account_body() {
    let fixture = RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, false, None).await;
    sqlx::query("UPDATE upstream_spec_v1 SET enabled = 0 WHERE id = ?")
        .bind(OWNER_A)
        .execute(fixture.storage.pool())
        .await
        .unwrap();
    let response = fixture
        .request(
            http::Method::POST,
            "/backend-api/wham/usage/thread_usage/query",
            Body::from_stream(stream::once(panic_when_polled())),
        )
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
    assert!(fixture.choices.lock().unwrap().is_empty());
}

#[tokio::test]
async fn projected_accounts_drop_invalidated_body_headers() {
    let fixture = RelayFixture::new(
        "a",
        StatusCode::OK,
        Bytes::from_static(
            br#"{"accounts":[{"id":"provider-workspace-a","can_access_with_session":true}]}"#,
        ),
        false,
        false,
        None,
    )
    .await;
    {
        let mut headers = fixture.dispatch.headers.lock().unwrap();
        for name in [
            "content-length",
            "etag",
            "last-modified",
            "content-md5",
            "digest",
            "content-digest",
            "repr-digest",
        ] {
            headers.insert(
                http::HeaderName::from_static(name),
                HeaderValue::from_static("old-provider-validator"),
            );
        }
        headers.insert("content-encoding", HeaderValue::from_static("identity"));
    }
    let response = fixture
        .request(http::Method::GET, CHECK, Body::empty())
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    for name in [
        "content-encoding",
        "etag",
        "last-modified",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
    ] {
        assert!(
            response.headers().get(name).is_none(),
            "{name} no longer describes the projected bytes"
        );
    }
    let length = response.headers().get("content-length").map(|value| {
        value
            .to_str()
            .unwrap()
            .parse::<usize>()
            .expect("only a newly computed length may remain")
    });
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    if let Some(length) = length {
        assert_eq!(length, body.len());
    }
    assert!(String::from_utf8_lossy(&body).contains("local-a"));
}

#[tokio::test]
async fn compressed_success_fails_closed_but_compressed_provider_error_is_opaque() {
    use std::io::Write as _;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(br#"{"accounts":[{"id":"provider-workspace-a"}]}"#)
        .unwrap();
    let compressed = Bytes::from(encoder.finish().unwrap());
    for status in [StatusCode::OK, StatusCode::UNAUTHORIZED] {
        let fixture = RelayFixture::new("a", status, compressed.clone(), false, false, None).await;
        fixture
            .dispatch
            .headers
            .lock()
            .unwrap()
            .insert("content-encoding", HeaderValue::from_static("gzip"));
        let response = fixture
            .request(http::Method::GET, CHECK, Body::empty())
            .await;
        let expected = if status.is_success() {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            status
        };
        assert_eq!(response.status(), expected);
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        if status.is_success() {
            assert!(!String::from_utf8_lossy(&bytes).contains("provider-workspace"));
        } else {
            assert_eq!(bytes, compressed);
        }
        assert_eq!(fixture.dispatch.sent.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn access_head_is_owner_bound_and_does_not_try_to_parse_empty_accounts_json() {
    let fixture = RelayFixture::new("b", StatusCode::OK, Bytes::new(), false, false, None).await;
    let response = fixture
        .request(http::Method::HEAD, CHECK, Body::empty())
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty()
    );
    let sent = fixture.dispatch.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].method(), http::Method::HEAD);
    assert_eq!(
        sent[0].headers()["chatgpt-account-id"],
        "provider-workspace-b"
    );
}

#[tokio::test]
async fn account_request_size_cap_never_starts_a_signer_or_dispatch() {
    let fixture = RelayFixture::new("a", StatusCode::OK, Bytes::new(), false, false, None).await;
    let response = fixture
        .request(
            http::Method::POST,
            "/backend-api/wham/usage/thread_usage/query",
            Body::from(vec![b'x'; 1025]),
        )
        .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(fixture.choices.lock().unwrap().is_empty());
    assert!(fixture.dispatch.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn account_credentials_are_removed_from_projected_and_opaque_responses() {
    const CREDENTIAL_HEADERS: &[&str] = &[
        "set-cookie",
        "set-cookie2",
        "cookie",
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "api-key",
        "x-goog-api-key",
    ];
    for (path, status, body) in [
        (
            CHECK,
            StatusCode::OK,
            Bytes::from_static(
                br#"{"accounts":[{"id":"provider-workspace-a","can_access_with_session":true}]}"#,
            ),
        ),
        (
            "/backend-api/wham/usage",
            StatusCode::OK,
            Bytes::from_static(b"opaque success"),
        ),
        (
            CHECK,
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(b"opaque refusal"),
        ),
    ] {
        let fixture = RelayFixture::new("a", status, body.clone(), false, false, None).await;
        {
            let mut headers = fixture.dispatch.headers.lock().unwrap();
            for name in CREDENTIAL_HEADERS {
                headers.insert(
                    http::HeaderName::from_static(name),
                    HeaderValue::from_static("synthetic-provider-credential"),
                );
            }
        }
        let response = fixture
            .request(http::Method::GET, path, Body::empty())
            .await;
        assert_eq!(response.status(), status);
        for name in CREDENTIAL_HEADERS {
            assert!(
                !response.headers().contains_key(*name),
                "credential leaked: {name}"
            );
        }
        assert_eq!(response.headers()["cc-lb-error-origin"], "upstream");
        assert!(!response.headers().contains_key("cc-lb-auth-reason"));
        assert_eq!(response.headers()["retry-after"], "17");
        assert_eq!(response.headers()["x-codex-primary-used-percent"], "37");
        let received = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        if path == CHECK && status.is_success() {
            assert!(String::from_utf8_lossy(&received).contains("local-a"));
        } else {
            assert_eq!(received, body);
        }
        assert_eq!(fixture.dispatch.sent.lock().unwrap().len(), 1);
    }
}
