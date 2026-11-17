use super::*;
use cc_lb_control::api_keys::session_token::{self, Claims, MintedToken};
use cc_lb_storage_api::account_owner::{AccountOwnerBinding, AccountOwnerStore};
use cc_lb_storage_api::codex_sessions::{
    CodexSessionStore, CredentialPurpose, IssueSession, SessionCredential, SessionFamily,
};
use http::Method;
use tokio::sync::broadcast::{Receiver, error::TryRecvError};

const PRINCIPAL: &str = "00000000-0000-0000-0000-000000000011";
const OTHER_PRINCIPAL: &str = "00000000-0000-0000-0000-000000000012";
const UPSTREAM: &str = "00000000-0000-0000-0000-000000000001";
const UPSTREAM_2: &str = "00000000-0000-0000-0000-000000000002";
const PATH: &str = "/backend-api/cc-lb/workspaces";

struct NoProviderSigner;

impl ApiKeyAwareSignerFactory for NoProviderSigner {
    fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
        panic!("workspace discovery must not prepare a provider signer")
    }
}

struct NoProviderDispatch;

#[async_trait]
impl UpstreamDispatch for NoProviderDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
        panic!("workspace discovery must not dispatch to a provider")
    }
}

enum Bearer {
    Access,
    Renewal,
    Switched,
    Renewed,
    Invalid,
}

struct DiscoveryFixture {
    router: Router,
    storage: cc_lb_storage_sqlite::SqliteStorage,
    events: Receiver<LifecycleEvent>,
    access: MintedToken,
    renewal: MintedToken,
    switched_access: Option<String>,
    renewed_access: Option<String>,
}

fn credential(id: &str, purpose: CredentialPurpose, token: &MintedToken) -> SessionCredential {
    SessionCredential {
        credential_id: id.into(),
        family_id: "family".into(),
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

impl DiscoveryFixture {
    async fn new() -> Self {
        let clock: ClockHandle = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let storage = cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
            .await
            .expect("real in-memory SQLite opens");
        sqlx::migrate!("../cc-lb-storage-sqlite/migrations")
            .run(storage.pool())
            .await
            .expect("real storage migrations apply");
        for statement in [
            "INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000011','discovery',100,100)",
            "INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000012','other-principal',100,100)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000001','owner','openai_oauth',100,100)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000002','owner-2','openai_oauth',100,100)",
            "INSERT INTO managed_keys_v1 (secret_hash,created_at,status,principal_id,key_id,label,verify_hash,secret_salt,limit_overrides,last_4,index_hash,updated_at) VALUES ('parent-hash',100,'active','00000000-0000-0000-0000-000000000011','parent','parent-label',zeroblob(32),zeroblob(16),'[]','1234',zeroblob(32),100)",
        ] {
            sqlx::query(statement)
                .execute(storage.pool())
                .await
                .expect("seed session lineage");
        }
        for (binding_id, binding_principal, owner_id, upstream_id, local_user_id, workspaces) in [
            (
                "binding",
                PRINCIPAL,
                "owner",
                UPSTREAM,
                "user",
                vec!["workspace", "other"],
            ),
            (
                "binding-2",
                PRINCIPAL,
                "owner-2",
                UPSTREAM_2,
                "user",
                vec!["workspace-2"],
            ),
            (
                "foreign",
                PRINCIPAL,
                "foreign-owner",
                UPSTREAM_2,
                "foreign-user",
                vec!["foreign-workspace"],
            ),
            (
                "wrong-principal",
                OTHER_PRINCIPAL,
                "other-principal-owner",
                UPSTREAM_2,
                "user",
                vec!["principal-workspace"],
            ),
        ] {
            storage
                .create_owner_binding(AccountOwnerBinding {
                    binding_id: binding_id.into(),
                    principal_id: binding_principal.into(),
                    provider: "openai".into(),
                    owner_id: owner_id.into(),
                    upstream_id: upstream_id.parse().expect("fixture upstream UUID"),
                    owner_generation: 1,
                    local_user_id: local_user_id.into(),
                    allowed_workspaces: workspaces.into_iter().map(str::to_owned).collect(),
                    provider_workspace_id: format!("provider-{owner_id}"),
                    outbound_region: None,
                    revision: 1,
                    revoked_at_unix_secs: None,
                })
                .await
                .expect("reserve real owner aliases");
        }
        let access = session_token::mint_access(&Claims {
            issuer: "https://proxy.example.com".into(),
            local_user_id: "user".into(),
            workspace_id: "workspace".into(),
            family_id: "family".into(),
            access_id: "access".into(),
            issued_at: 100,
            expires_at: 200,
            plan: "unknown".into(),
        })
        .expect("mint local access token");
        let renewal = session_token::mint_renewal();
        storage
            .issue_session(IssueSession {
                family: SessionFamily {
                    family_id: "family".into(),
                    principal_id: PRINCIPAL.into(),
                    parent_key_id: "parent".into(),
                    owner_binding_id: "binding".into(),
                    provider: "openai".into(),
                    owner_generation: 1,
                    binding_revision: 1,
                    workspace_id: "workspace".into(),
                    workspace_generation: 1,
                    access_generation: 1,
                    local_user_id: "user".into(),
                    plan_type: "unknown".into(),
                    origin: "https://proxy.example.com".into(),
                    access_ttl_secs: 100,
                    created_at: 100,
                    expires_at: 200,
                    revoked_at: None,
                },
                access: credential("access", CredentialPurpose::SessionAccess, &access),
                renewal: credential("renewal", CredentialPurpose::SessionRenewal, &renewal),
            })
            .await
            .expect("persist minted token verifiers in the real session store");
        let builtin_authn = Arc::new(
            BuiltinAuthn::new(
                Arc::new(KeyStore::new(
                    Arc::new(FixtureKeyStore) as Arc<dyn ManagedKeyStore>
                )),
                clock.clone(),
            )
            .with_session_store(Arc::new(storage.clone())),
        );
        let dynamic_view = Arc::new(DynamicViewHolder::new(
            cc_lb_control::DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(NoProviderSigner))
                .principal_view(Arc::new(PrincipalView::for_tests(
                    PRINCIPAL,
                    true,
                    vec!["*".into()],
                    Vec::new(),
                    HashMap::new(),
                )))
                .upstream_records(Vec::new())
                .build(),
        ));
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let events = bus.subscribe_lifecycle();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                builtin_authn.clone(),
                dynamic_view.clone(),
                Arc::new(NoProviderDispatch),
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
            account_dispatcher: Arc::new(NoProviderDispatch),
            body_caps: RequestBodyCaps {
                messages: 1024,
                files: 1024,
            },
            server_state: Arc::new(ServerStateHandle::new_starting()),
            start_time: Instant::now(),
            drain_controller: DrainController::new(),
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            storage: Arc::new(storage.clone()),
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
        let router = scoped_proxy_routes(
            state,
            RequestIdState::uuid_v7("req_server_"),
            30,
            None,
            false,
        );
        Self {
            router,
            storage,
            events,
            access,
            renewal,
            switched_access: None,
            renewed_access: None,
        }
    }

    async fn disable_owner(&self) {
        sqlx::query("UPDATE upstream_spec_v1 SET enabled = 0 WHERE id = ?")
            .bind(UPSTREAM)
            .execute(self.storage.pool())
            .await
            .expect("disable fixture owner");
    }

    async fn request(
        &mut self,
        method: Method,
        path: &str,
        bearer: Bearer,
        expected_status: StatusCode,
        authenticated: bool,
    ) -> Bytes {
        self.request_with_body(
            method,
            path,
            bearer,
            Body::from_stream(stream::once(panic_when_polled())),
            expected_status,
            authenticated,
            None,
        )
        .await
    }

    async fn request_with_body(
        &mut self,
        method: Method,
        path: &str,
        bearer: Bearer,
        body: Body,
        expected_status: StatusCode,
        authenticated: bool,
        expected_body_bytes: Option<u64>,
    ) -> Bytes {
        let bearer = match bearer {
            Bearer::Access => self.access.plaintext.expose().to_owned(),
            Bearer::Renewal => self.renewal.plaintext.expose().to_owned(),
            Bearer::Switched => self
                .switched_access
                .as_deref()
                .expect("switch response stores an access token")
                .to_owned(),
            Bearer::Renewed => self
                .renewed_access
                .as_deref()
                .expect("renewal response stores an access token")
                .to_owned(),
            Bearer::Invalid => "not-a-local-session".into(),
        };
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(http::header::AUTHORIZATION, format!("Bearer {bearer}"))
            .body(body)
            .expect("discovery request builds");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("router responds");
        assert_eq!(response.status(), expected_status);
        assert_eq!(response.headers()[http::header::CACHE_CONTROL], "no-store");
        if !authenticated {
            assert_eq!(response.headers()["cc-lb-error-origin"], "principal");
        } else if expected_status.is_client_error() || expected_status.is_server_error() {
            assert_eq!(response.headers()["cc-lb-error-origin"], "proxy");
        }
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body collects")
            .to_bytes();
        let mut events = Vec::new();
        loop {
            match self.events.try_recv() {
                Ok(event) => events.push(event),
                Err(TryRecvError::Empty) => break,
                Err(error) => panic!("lifecycle event loss: {error}"),
            }
        }
        assert_local_events(&events, expected_status, authenticated, expected_body_bytes);
        body
    }
}

fn assert_local_events(
    events: &[LifecycleEvent],
    status: StatusCode,
    authenticated: bool,
    expected_body_bytes: Option<u64>,
) {
    // An exact event count also rules out parsing, routing and UpstreamAttempt events.
    assert_eq!(
        events.len(),
        if authenticated { 4 } else { 3 },
        "{events:#?}"
    );
    let Some(start_index) = events
        .iter()
        .position(|event| matches!(event, LifecycleEvent::RequestStarted { .. }))
    else {
        panic!("expected a single request start: {events:#?}");
    };
    let Some(LifecycleEvent::RequestStarted {
        event_id,
        stream,
        source_kind,
        event_kind,
        ..
    }) = events.get(start_index)
    else {
        unreachable!("request start index points to request start");
    };
    assert!(!stream);
    assert_eq!(source_kind.as_deref(), Some("proxy"));
    assert_eq!(
        *event_kind,
        Some(cc_lb_request_log::RequestEventKind::Other)
    );
    let mut auth_events = 0;
    let mut authentication_events = 0;
    for (index, event) in events.iter().enumerate() {
        if index == start_index || index == events.len() - 1 {
            continue;
        }
        match event {
            LifecycleEvent::AuthCompleted {
                event_id: id,
                result,
            } => {
                assert_eq!(id, event_id);
                auth_events += 1;
                if authenticated {
                    assert_eq!(
                        result
                            .as_ref()
                            .expect("authenticated attribution")
                            .principal_id,
                        PRINCIPAL
                    );
                } else {
                    assert!(matches!(
                        result,
                        Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed {
                            http_status,
                            ..
                        }) if *http_status == status.as_u16()
                    ));
                }
            }
            LifecycleEvent::AuthenticationCompleted {
                event_id: id,
                principal_id,
                ..
            } => {
                assert_eq!(id, event_id);
                assert_eq!(principal_id, PRINCIPAL);
                authentication_events += 1;
            }
            other => panic!("unexpected local discovery event: {other:?}"),
        }
    }
    assert_eq!(auth_events, 1);
    assert_eq!(authentication_events, usize::from(authenticated));
    let Some(LifecycleEvent::RequestTerminated {
        event_id: id,
        client_status,
        event_kind,
        request_body_bytes,
        ..
    }) = events.last()
    else {
        panic!("expected a single terminal event: {events:#?}");
    };
    assert_eq!(id, event_id);
    assert_eq!(*client_status, status.as_u16());
    assert_eq!(
        *event_kind,
        Some(cc_lb_request_log::RequestEventKind::Other)
    );
    match expected_body_bytes {
        Some(expected) => assert_eq!(*request_body_bytes, Some(expected)),
        None => assert!(
            request_body_bytes.is_none(),
            "request body must remain unread for pre-auth/local routes",
        ),
    }
}

#[tokio::test]
async fn discovery_returns_exact_local_alias_wire_shape() {
    let mut fixture = DiscoveryFixture::new().await;
    let body = fixture
        .request(Method::GET, PATH, Bearer::Access, StatusCode::OK, true)
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("discovery JSON");
    assert_eq!(
        wire,
        serde_json::json!({
            "session_id": "family",
            "workspace_generation": 1,
            "selected_workspace_id": "workspace",
            "workspaces": [
                {"workspace_id": "other"},
                {"workspace_id": "workspace"},
                {"workspace_id": "workspace-2"},
            ],
        })
    );
}

#[tokio::test]
async fn discovery_rejects_invalid_bearer_before_owner_discovery() {
    let mut fixture = DiscoveryFixture::new().await;
    fixture.disable_owner().await;
    let body = fixture
        .request(
            Method::GET,
            PATH,
            Bearer::Invalid,
            StatusCode::UNAUTHORIZED,
            false,
        )
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("authentication JSON");
    assert_eq!(wire["error"], "authentication_error");
}

#[tokio::test]
async fn discovery_rejects_renewal_purpose_before_owner_discovery() {
    let mut fixture = DiscoveryFixture::new().await;
    fixture.disable_owner().await;
    let body = fixture
        .request(
            Method::GET,
            PATH,
            Bearer::Renewal,
            StatusCode::UNAUTHORIZED,
            false,
        )
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("authentication JSON");
    assert_eq!(wire["error"], "authentication_error");
}

#[tokio::test]
async fn discovery_rejects_query_before_unavailable_owner_read() {
    let mut fixture = DiscoveryFixture::new().await;
    fixture.disable_owner().await;
    let body = fixture
        .request(
            Method::GET,
            "/backend-api/cc-lb/workspaces?workspace_id=other",
            Bearer::Access,
            StatusCode::BAD_REQUEST,
            true,
        )
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("query error JSON"),
        serde_json::json!({"error": "invalid_session_request"})
    );
}

#[tokio::test]
async fn discovery_reports_disabled_owner_without_provider_dispatch() {
    let mut fixture = DiscoveryFixture::new().await;
    fixture.disable_owner().await;
    let body = fixture
        .request(
            Method::GET,
            PATH,
            Bearer::Access,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
        )
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("owner error JSON"),
        serde_json::json!({"error": "session_unavailable"})
    );
}

#[tokio::test]
async fn discovery_rejects_disabled_principal_before_owner_discovery() {
    let mut fixture = DiscoveryFixture::new().await;
    fixture.disable_owner().await;
    sqlx::query("UPDATE principals_v1 SET enabled = 0 WHERE id = ?")
        .bind(PRINCIPAL)
        .execute(fixture.storage.pool())
        .await
        .expect("disable fixture principal");
    let body = fixture
        .request(
            Method::GET,
            PATH,
            Bearer::Access,
            StatusCode::FORBIDDEN,
            false,
        )
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("authentication JSON");
    assert_eq!(wire["error"], "authentication_error");
}

#[tokio::test]
async fn discovery_head_inherits_get_authentication_and_strips_response_body() {
    // This documents Axum's existing get(...)-derived HEAD behavior, not a new HEAD policy.
    let mut fixture = DiscoveryFixture::new().await;
    for (bearer, status, authenticated) in [
        (Bearer::Access, StatusCode::OK, true),
        (Bearer::Renewal, StatusCode::UNAUTHORIZED, false),
        (Bearer::Invalid, StatusCode::UNAUTHORIZED, false),
    ] {
        let body = fixture
            .request(Method::HEAD, PATH, bearer, status, authenticated)
            .await;
        assert!(body.is_empty(), "HEAD must strip the GET response body");
    }
    fixture.disable_owner().await;
    let body = fixture
        .request(
            Method::HEAD,
            PATH,
            Bearer::Access,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
        )
        .await;
    assert!(
        body.is_empty(),
        "HEAD retains GET owner fencing but strips its error body"
    );
}
#[tokio::test]
async fn account_routes_authenticate_before_provider_dispatch() {
    let mut fixture = DiscoveryFixture::new().await;
    let routes = [
        (Method::GET, "/backend-api/wham/accounts/check"),
        (Method::GET, "/backend-api/wham/usage"),
        (Method::GET, "/backend-api/wham/rate-limit-reset-credits"),
        (
            Method::POST,
            "/backend-api/wham/rate-limit-reset-credits/consume",
        ),
        (Method::GET, "/backend-api/wham/profiles/me"),
        (
            Method::POST,
            "/backend-api/wham/accounts/send_add_credits_nudge_email",
        ),
        (Method::GET, "/backend-api/wham/config/bundle"),
        (Method::GET, "/backend-api/wham/settings/user"),
        (Method::GET, "/backend-api/wham/workspace-messages"),
        (Method::GET, "/backend-api/wham/security-setup"),
        (
            Method::POST,
            "/backend-api/wham/usage/thread-estimates/query",
        ),
        (Method::POST, "/backend-api/wham/usage/thread_usage/query"),
        (
            Method::POST,
            "/backend-api/wham/usage/thread_usage/query_v2",
        ),
        (Method::GET, "/backend-api/wham/usage/plan_limit_history"),
        (
            Method::GET,
            "/backend-api/wham/usage/daily-token-usage-breakdown",
        ),
        (Method::GET, "/backend-api/wham/usage/credit-usage-events"),
        (
            Method::GET,
            "/backend-api/wham/usage/daily-workspace-user-token-usage-breakdown",
        ),
        (
            Method::GET,
            "/backend-api/wham/usage/daily-workspace-user-credit-usage",
        ),
        (
            Method::GET,
            "/backend-api/wham/analytics/daily-workspace-usage-counts",
        ),
        (
            Method::GET,
            "/backend-api/wham/analytics/daily-plugin-usage-metrics",
        ),
        (
            Method::GET,
            "/backend-api/wham/analytics/daily-skill-usage-metrics",
        ),
        (Method::GET, "/backend-api/accounts/verified_access"),
        (Method::POST, "/backend-api/accounts/switch-workspace-token"),
    ];
    for (method, path) in routes {
        let body = fixture
            .request(
                method.clone(),
                path,
                Bearer::Invalid,
                StatusCode::UNAUTHORIZED,
                false,
            )
            .await;
        assert_eq!(
            serde_json::from_slice::<Value>(&body).expect("authentication JSON")["error"],
            "authentication_error",
            "route must be registered and authenticate before provider dispatch: {method} {path}"
        );
    }
}
#[tokio::test]
async fn workspace_switch_fences_old_access_and_renews_selected_owner() {
    let mut fixture = DiscoveryFixture::new().await;
    let switch_body = r#"{"workspace_id":"workspace-2"}"#;
    let body = fixture
        .request_with_body(
            Method::POST,
            "/backend-api/accounts/switch-workspace-token",
            Bearer::Access,
            Body::from(switch_body),
            StatusCode::OK,
            true,
            Some(switch_body.len() as u64),
        )
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("workspace switch JSON");
    assert_eq!(wire["session_id"], "family");
    assert_eq!(wire["workspace_generation"], 2);
    assert_eq!(wire["account_id"], "workspace-2");
    assert_eq!(wire["user_id"], "user");
    assert_eq!(wire["token_type"], "Bearer");
    assert_eq!(wire["origin"], "https://proxy.example.com");
    let switched_access = wire["access_token"]
        .as_str()
        .expect("workspace switch access token")
        .to_owned();
    assert_ne!(switched_access, fixture.access.plaintext.expose());
    fixture.switched_access = Some(switched_access);

    let body = fixture
        .request(Method::GET, PATH, Bearer::Switched, StatusCode::OK, true)
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("switched discovery JSON"),
        serde_json::json!({
            "session_id": "family",
            "workspace_generation": 2,
            "selected_workspace_id": "workspace-2",
            "workspaces": [
                {"workspace_id": "other"},
                {"workspace_id": "workspace"},
                {"workspace_id": "workspace-2"},
            ],
        })
    );

    for (method, path) in [
        (Method::GET, PATH),
        (Method::GET, "/backend-api/wham/accounts/check"),
    ] {
        let body = fixture
            .request(
                method,
                path,
                Bearer::Access,
                StatusCode::UNAUTHORIZED,
                false,
            )
            .await;
        assert_eq!(
            serde_json::from_slice::<Value>(&body).expect("old access authentication JSON")["error"],
            "authentication_error",
        );
    }
    let body = fixture
        .request(
            Method::POST,
            "/backend-api/accounts/switch-workspace-token",
            Bearer::Access,
            StatusCode::UNAUTHORIZED,
            false,
        )
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("old switch authentication JSON")["error"],
        "authentication_error",
    );

    for target in ["foreign-workspace", "principal-workspace"] {
        let target_body = format!(r#"{{"workspace_id":"{target}"}}"#);
        let body = fixture
            .request_with_body(
                Method::POST,
                "/backend-api/accounts/switch-workspace-token",
                Bearer::Switched,
                Body::from(target_body.clone()),
                StatusCode::BAD_REQUEST,
                true,
                Some(target_body.len() as u64),
            )
            .await;
        assert_eq!(
            serde_json::from_slice::<Value>(&body).expect("owner conflict JSON"),
            serde_json::json!({"error": "owner_conflict"}),
        );
    }

    let renewal_old_generation_body =
        r#"{"grant_type":"refresh_token","expected_workspace_generation":1}"#;
    let body = fixture
        .request_with_body(
            Method::POST,
            "/oauth/token",
            Bearer::Renewal,
            Body::from(renewal_old_generation_body),
            StatusCode::BAD_REQUEST,
            true,
            Some(renewal_old_generation_body.len() as u64),
        )
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("stale renewal JSON"),
        serde_json::json!({"error": "invalid_session_request"}),
    );

    let renewal_body = r#"{"grant_type":"refresh_token","expected_workspace_generation":2}"#;
    let body = fixture
        .request_with_body(
            Method::POST,
            "/oauth/token",
            Bearer::Renewal,
            Body::from(renewal_body),
            StatusCode::OK,
            true,
            Some(renewal_body.len() as u64),
        )
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("renewal JSON");
    assert_eq!(wire["session_id"], "family");
    assert_eq!(wire["workspace_generation"], 2);
    assert_eq!(wire["account_id"], "workspace-2");
    assert_eq!(wire["user_id"], "user");
    assert_eq!(wire["origin"], "https://proxy.example.com");
    let renewed_access = wire["access_token"]
        .as_str()
        .expect("renewal access token")
        .to_owned();
    fixture.renewed_access = Some(renewed_access);

    let body = fixture
        .request(Method::GET, PATH, Bearer::Renewed, StatusCode::OK, true)
        .await;
    let wire: Value = serde_json::from_slice(&body).expect("renewed discovery JSON");
    assert_eq!(wire["session_id"], "family");
    assert_eq!(wire["workspace_generation"], 2);
    assert_eq!(wire["selected_workspace_id"], "workspace-2");
    assert_eq!(
        wire["workspaces"]
            .as_array()
            .expect("renewed workspace list")
            .iter()
            .filter(|workspace| workspace["workspace_id"] == "workspace-2")
            .count(),
        1,
    );
}
