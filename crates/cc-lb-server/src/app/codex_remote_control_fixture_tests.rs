use super::*;
use cc_lb_storage_api::codex_sessions::{
    CodexSessionStore, CredentialPurpose, IssueSession, SessionCredential, SessionFamily,
};
use cc_lb_storage_api::RelayEnrollmentStore;
use cc_lb_control::api_keys::{relay_authn::StoredRelayVerifier, session_token};

// Contract-faithful HTTP transport fixture only. This is not provider evidence
// and does not replace real-account CLI or real-socket WS fidelity QA.
struct RemoteDispatch {
    requests: std::sync::Mutex<Vec<(String, HeaderMap)>>,
    pending_started: tokio::sync::Notify,
}

#[async_trait]
impl UpstreamDispatch for RemoteDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
        let path = request.url().path().to_owned();
        self.requests.lock().unwrap().push((path.clone(), request.headers().clone()));
        if request.url().query() == Some("pending=1") {
            self.pending_started.notify_one();
            return std::future::pending().await;
        }
        let mut response = if request.url().query() == Some("reject=1") {
            let mut response = Response::new(Body::from("unaltered provider refusal"));
            *response.status_mut() = StatusCode::UNAUTHORIZED;
            response
        } else if request.url().query() == Some("reject=500") {
            let mut response = Response::new(Body::from("unaltered provider failure"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        } else if request.url().query() == Some("body-error=1") {
            Response::new(Body::from_stream(stream::once(async {
                Err::<Bytes,io::Error>(io::Error::other("synthetic response body failure"))
            })))
        } else if path.ends_with("/enroll") || path.ends_with("/refresh") {
            Response::new(Body::from(serde_json::json!({
                "server_id":"server", "environment_id":"environment",
                "remote_control_token": if path.ends_with("/refresh") { "synthetic-rotated-vendor-relay" } else { "synthetic-vendor-relay" },
                "expires_at":"1970-01-01T00:03:00Z", "future":{"preserved":true}
            }).to_string()))
        } else if path.ends_with("/pair/status") {
            Response::new(Body::from(r#"{"claimed":false}"#))
        } else if path.ends_with("/pair") {
            Response::new(Body::from(r#"{"pairing_code":"synthetic-pairing","manual_pairing_code":null,"server_id":"server","environment_id":"environment","expires_at":"1970-01-01T00:03:00Z"}"#))
        } else if request.method() == http::Method::DELETE {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NO_CONTENT;
            response
        } else {
            Response::new(Body::from(r#"{"items":[{"client_id":"client"}],"cursor":null}"#))
        };
        // Reserved provider provenance must never become local expiry evidence.
        response.headers_mut().insert("cc-lb-error-origin", HeaderValue::from_static("principal"));
        response.headers_mut().insert("cc-lb-auth-reason", HeaderValue::from_static("expired"));
        response.headers_mut().insert("x-codex-primary-used-percent", HeaderValue::from_static("17.5"));
        response.headers_mut().insert("set-cookie", HeaderValue::from_static("synthetic-cookie=must-not-capture"));
        Ok(response)
    }
}

struct RemoteFixture {
    state: ProxyState,
    router: Router,
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    dispatch: Arc<RemoteDispatch>,
    dynamic_view: Arc<DynamicViewHolder>,
    bus: Arc<cc_lb_control::InMemoryBus>,
    drain: DrainController,
    access: session_token::RedactedToken,
    renewal: session_token::RedactedToken,
}

async fn remote_fixture() -> RemoteFixture {
    remote_fixture_at(Url::parse("https://chatgpt.com/backend-api/codex").unwrap()).await
}

async fn remote_fixture_at(origin: Url) -> RemoteFixture {
    remote_fixture_config(origin,30).await
}

async fn remote_fixture_config(origin: Url, timeout_secs:u64) -> RemoteFixture {
    let clock: ClockHandle = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
    let storage = Arc::new(cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone()).await.unwrap());
    sqlx::migrate!("../cc-lb-storage-sqlite/migrations").run(storage.pool()).await.unwrap();
    sqlx::query("INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES (?, 'rc-runtime-principal',100,100)")
        .bind(TEST_PRINCIPAL).execute(storage.pool()).await.unwrap();
    sqlx::query("INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000001','rc-runtime-owner','openai_oauth',100,100)")
        .execute(storage.pool()).await.unwrap();
    sqlx::query("INSERT INTO account_owner_bindings_v1 (binding_id,principal_id,provider,owner_id,upstream_id,owner_generation,local_user_id,allowed_workspaces_json,provider_workspace_id,revision) VALUES ('binding',?,'openai','owner','00000000-0000-0000-0000-000000000001',1,'user','[\"workspace\"]','provider-workspace',1)")
        .bind(TEST_PRINCIPAL).execute(storage.pool()).await.unwrap();
    sqlx::query("INSERT INTO managed_keys_v1 (secret_hash,created_at,status,principal_id,key_id,label,verify_hash,secret_salt,limit_overrides,last_4,index_hash,updated_at) VALUES ('rc-runtime-parent',100,'active',?,'parent','rc-runtime-parent',zeroblob(32),zeroblob(16),'[]','1234',zeroblob(32),100)")
        .bind(TEST_PRINCIPAL).execute(storage.pool()).await.unwrap();
    let access = session_token::mint_access(&session_token::Claims {
        issuer:"https://proxy.example.com".into(), local_user_id:"user".into(), workspace_id:"workspace".into(),
        family_id:"family".into(), access_id:"access".into(), issued_at:100, expires_at:180, plan:"unknown".into(),
    }).unwrap();
    let renewal = session_token::mint_renewal();
    let credential = |token: &session_token::MintedToken, id: &str, purpose| SessionCredential {
        credential_id:id.into(), family_id:"family".into(), purpose, index_hash:token.index_hash,
        verify_hash:token.verify_hash, salt:token.secret_salt, workspace_generation:1, access_generation:1,
        issued_at:100, expires_at:if purpose == CredentialPurpose::SessionAccess {180} else {200}, revoked_at:None,
    };
    storage.issue_session(IssueSession {
        family:SessionFamily { family_id:"family".into(), principal_id:TEST_PRINCIPAL.into(), parent_key_id:"parent".into(),
            owner_binding_id:"binding".into(), provider:"openai".into(), owner_generation:1, binding_revision:1,
            workspace_id:"workspace".into(), workspace_generation:1, access_generation:1, local_user_id:"user".into(),
            plan_type:"unknown".into(), origin:"https://proxy.example.com".into(), access_ttl_secs:80,
            created_at:100, expires_at:200, revoked_at:None },
        access:credential(&access,"access",CredentialPurpose::SessionAccess),
        renewal:credential(&renewal,"renewal",CredentialPurpose::SessionRenewal),
    }).await.unwrap();
    let mut state = websocket_test_state(Vec::new(), TerminalStrategy::FirstPick, DrainController::new(),
        Arc::new(WebSocketTestSignerFactory), Arc::new(FixtureKeyStore), vec!["*".into()]).await;
    let aead = state.aead.clone();
    let mut record = websocket_upstream(1, "rc-runtime-owner", origin);
    record.kind = UpstreamKind::OpenaiOauth;
    record.oauth_credentials = Some(cc_lb_aead::EncryptedOAuthTokens::encrypt(&aead, &cc_lb_aead::OAuthTokenBundle {
        access_token:"synthetic-owner-access".into(), refresh_token:"synthetic-owner-refresh".into(), expires_at_unix_secs:500,
        refresh_token_expires_at_unix_secs:None, account_id:Some("provider-original-workspace".into()),
        project_id:None, oauth_profile:Default::default(), scopes:Vec::new(), never_refresh:true,
    }, record.id.as_bytes()).unwrap());
    let view = state.dynamic_view.load();
    state.dynamic_view.store(cc_lb_control::DynamicViewBuilder::from_view(&view).upstream_records(vec![record]).build());
    let builtin_authn = Arc::new(BuiltinAuthn::new(Arc::new(KeyStore::new(Arc::new(FixtureKeyStore))), clock.clone())
        .with_session_store(storage.clone())
        .with_relay_verifier(Arc::new(StoredRelayVerifier::new(storage.clone(), clock.clone()))));
    let dispatch = Arc::new(RemoteDispatch { requests:std::sync::Mutex::new(Vec::new()),pending_started:tokio::sync::Notify::new() });
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    state.lifecycle = Arc::new(Lifecycle::new_with_dynamic_view(builtin_authn.clone(), state.dynamic_view.clone(),
        dispatch.clone(), LifecycleConfig::default(), clock.clone()).with_event_bus(bus.clone()));
    state.builtin_authn = builtin_authn;
    let remote_dispatch = dispatch.clone();
    state.remote_control_dispatch = Arc::new(BulkheadDispatch::with_dispatcher_factory(
        Arc::new(BulkheadRegistry::new()), state.websocket_bulkhead_config,
        Arc::new(|request| request.url().host_str().unwrap_or("unknown").to_owned()),
        Arc::new(move |_| remote_dispatch.clone()),
    ));
    state.codex_sessions = Arc::new(cc_lb_engine::codex_sessions::CodexSessionService::new(storage.clone(),storage.clone(),clock.clone()));
    state.storage = storage.clone();
    state.clock = clock;
    RemoteFixture { dynamic_view:state.dynamic_view.clone(),drain:state.drain_controller.clone(),
        router:proxy_router(state.clone(),timeout_secs),state,storage,dispatch,bus,access:access.plaintext,renewal:renewal.plaintext }
}

fn remote_request(method: &str, path: &str, credential: &str, body: Body) -> Request<Body> {
    Request::builder().method(method).uri(path).header("authorization",format!("Bearer {credential}"))
        .header("x-codex-installation-id","installation").header("content-type","application/json").body(body).unwrap()
}

async fn enroll(fixture: &RemoteFixture) -> String {
    let response = fixture.router.clone().oneshot(remote_request("POST",codex_remote_control::ENROLL,fixture.access.expose(),
        Body::from(r#"{"name":"machine","os":"linux","arch":"aarch64","app_server_version":"0.169.0","installation_id":"installation"}"#))).await.unwrap();
    assert_eq!(response.status(),StatusCode::OK);
    assert_eq!(response.headers()["cache-control"],"no-store");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let response:Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(response["future"]["preserved"],true);
    let token = response["remote_control_token"].as_str().unwrap().to_owned();
    assert!(token.starts_with("cclb-relay-v1."));
    assert_ne!(token,"synthetic-vendor-relay");
    token
}

#[tokio::test]
async fn remote_runtime_enrollment_refresh_and_account_client_scope() {
    let fixture = remote_fixture().await;
    let relay = enroll(&fixture).await;
    let first = fixture.storage.lookup_relay_credential(&secret::compute_index_hash(relay.as_bytes())).await.unwrap().unwrap();
    assert_eq!(first.credential.identity.server_id,"server");
    let initial_count = fixture.dispatch.requests.lock().unwrap().len();
    let wrong = fixture.router.clone().oneshot(remote_request("GET",
        "/backend-api/wham/remote/control/environments/other/clients",fixture.access.expose(),Body::empty())).await.unwrap();
    assert_eq!(wrong.status(),StatusCode::BAD_REQUEST);
    assert_eq!(fixture.dispatch.requests.lock().unwrap().len(),initial_count);
    let refreshed = fixture.router.clone().oneshot(remote_request("POST",codex_remote_control::REFRESH,
        fixture.access.expose(),Body::from(r#"{"server_id":"server","installation_id":"installation"}"#))).await.unwrap();
    assert_eq!(refreshed.status(),StatusCode::OK);
    for (method,path,body) in [
        ("POST",codex_remote_control::PAIR,Body::from(r#"{"manual_code":true}"#)),
        ("POST",codex_remote_control::PAIR_STATUS,Body::from(r#"{"pairing_code":"synthetic-pairing"}"#)),
        ("GET","/backend-api/wham/remote/control/environments/environment/clients",Body::empty()),
        ("DELETE","/backend-api/wham/remote/control/environments/environment/clients/client",Body::empty()),
    ] {
        let credential = if method == "POST" {relay.as_str()} else {fixture.access.expose()};
        let response = fixture.router.clone().oneshot(remote_request(method,path,credential,body)).await.unwrap();
        assert_eq!(response.status(),if method == "DELETE" {StatusCode::NO_CONTENT} else {StatusCode::OK});
        assert_eq!(response.headers()["cc-lb-error-origin"],"upstream");
        assert!(!response.headers().contains_key("cc-lb-auth-reason"));
    }
    let requests = fixture.dispatch.requests.lock().unwrap();
    assert_eq!(requests.len(),6);
    assert_eq!(requests[0].1["authorization"],"Bearer synthetic-owner-access");
    assert_eq!(requests[0].1["chatgpt-account-id"],"provider-workspace");
    assert_eq!(requests[2].1["authorization"],"Bearer synthetic-rotated-vendor-relay");
    assert!(!requests[2].1.contains_key("chatgpt-account-id"));
}

#[tokio::test]
async fn remote_runtime_cross_purpose_and_revocation_never_poll_or_dispatch() {
    let fixture = remote_fixture().await;
    let relay = enroll(&fixture).await;
    for (path,credential) in [
        (codex_remote_control::PAIR,fixture.access.expose()),
        (codex_remote_control::ENROLL,relay.as_str()),
        (codex_remote_control::PAIR,fixture.renewal.expose()),
    ] {
        let response = fixture.router.clone().oneshot(remote_request("POST",path,credential,
            Body::from_stream(stream::once(panic_when_polled())))).await.unwrap();
        assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
    }
    fixture.storage.revoke_session_family("family","renewal",100).await.unwrap();
    let response = fixture.router.clone().oneshot(remote_request("POST",codex_remote_control::PAIR,&relay,
        Body::from_stream(stream::once(panic_when_polled())))).await.unwrap();
    assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.dispatch.requests.lock().unwrap().len(),1);
}

#[tokio::test]
async fn remote_runtime_preserves_provider_401_without_local_auth_recovery() {
    let fixture = remote_fixture().await;
    let relay = enroll(&fixture).await;
    let response = fixture.router.clone().oneshot(remote_request("POST",
        &format!("{}?reject=1",codex_remote_control::PAIR),&relay,Body::from(r#"{"manual_code":false}"#))).await.unwrap();
    assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["cc-lb-error-origin"],"upstream");
    assert!(!response.headers().contains_key("cc-lb-auth-reason"));
    assert!(!response.headers().contains_key("set-cookie"));
    assert_eq!(response.into_body().collect().await.unwrap().to_bytes(),"unaltered provider refusal");
    assert_eq!(fixture.dispatch.requests.lock().unwrap().len(),2);
}


#[tokio::test]
async fn remote_http_revocation_while_queued_prevents_provider_send() {
    let fixture = remote_fixture().await;
    let relay = enroll(&fixture).await;
    let registry = Arc::new(BulkheadRegistry::new());
    let mut config = fixture.state.websocket_bulkhead_config;
    config.semaphore_permits = 1;
    let provider = fixture.dispatch.clone();
    let bulkhead = registry.bulkhead_with_dispatcher("queued-owner", config, provider.clone());
    let permit = bulkhead.acquire_owned().await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered_for_dispatch = entered.clone();
    let queued = Arc::new(BulkheadDispatch::with_dispatcher_factory(
        registry, config, Arc::new(move |_| {
            entered_for_dispatch.notify_one();
            "queued-owner".to_owned()
        }), Arc::new(move |_| provider.clone()),
    ));
    let mut state = fixture.state.clone();
    state.remote_control_dispatch = queued.clone();
    let request = proxy_router(state, 30).oneshot(remote_request(
        "POST", codex_remote_control::PAIR, &relay, Body::from(r#"{"manual_code":false}"#),
    ));
    tokio::pin!(request);
    tokio::select! {
        response = &mut request => panic!("request escaped occupied bulkhead: {:?}", response.unwrap().status()),
        _ = entered.notified() => {}
    }
    fixture.storage.revoke_session_family("family", "renewal", 100).await.unwrap();
    drop(permit);
    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.dispatch.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn remote_runtime_malformed_body_methods_and_owner_loss_fail_before_send() {
    let fixture = remote_fixture().await;
    for body in ["{malformed",
        r#"{"name":"machine","os":"linux","arch":"aarch64","app_server_version":"0.169.0","installation_id":"other"}"#] {
        let response = fixture.router.clone().oneshot(remote_request("POST",codex_remote_control::ENROLL,
            fixture.access.expose(),Body::from(body))).await.unwrap();
        assert_eq!(response.status(),StatusCode::BAD_REQUEST);
    }
    let response = fixture.router.clone().oneshot(remote_request("HEAD",
        "/backend-api/wham/remote/control/environments/environment/clients",
        fixture.access.expose(),Body::empty())).await.unwrap();
    assert_eq!(response.status(),StatusCode::METHOD_NOT_ALLOWED);
    sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 100 WHERE binding_id = 'binding'")
        .execute(fixture.storage.pool()).await.unwrap();
    let response = fixture.router.clone().oneshot(remote_request("POST",codex_remote_control::ENROLL,
        fixture.access.expose(),Body::from(r#"{"name":"machine","os":"linux","arch":"aarch64","app_server_version":"0.169.0","installation_id":"installation"}"#))).await.unwrap();
    assert_eq!(response.status(),StatusCode::SERVICE_UNAVAILABLE);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let response:Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(response["error"],"upstream_affinity_unavailable");
    assert!(fixture.dispatch.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn remote_runtime_rejects_ws_identity_before_upgrade_or_provider_connect() {
    let fixture = remote_fixture().await;
    let relay = enroll(&fixture).await;
    let response = fixture.router.clone().oneshot(remote_request("POST",codex_remote_control::PAIR,&relay,
        Body::from(r#"{"manual_code":false,"server_id":"other"}"#))).await.unwrap();
    assert_eq!(response.status(),StatusCode::BAD_REQUEST);
    let mut request = remote_request("GET",codex_remote_control::SERVER,&relay,Body::empty());
    for (name,value) in [("x-codex-server-id","other"),("x-codex-name","bWFjaGluZQ=="),("x-codex-protocol-version","1")] {
        request.headers_mut().insert(http::HeaderName::from_bytes(name.as_bytes()).unwrap(),HeaderValue::from_static(value));
    }
    let response = fixture.router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(),StatusCode::BAD_REQUEST);
    assert!(!response.headers().contains_key("upgrade"));
    assert_eq!(fixture.dispatch.requests.lock().unwrap().len(),1);
}

struct SocketTask(JoinHandle<()>);
impl Drop for SocketTask {
    fn drop(&mut self) { self.0.abort(); }
}

fn remote_ws_request(address:SocketAddr, relay:&str) -> http::Request<()> {
    let mut request = format!("ws://{address}{}",codex_remote_control::SERVER).into_client_request().unwrap();
    for (field,value) in [
        ("authorization",format!("Bearer {relay}")),
        ("x-codex-server-id","server".into()),
        ("x-codex-installation-id","installation".into()),
        ("x-codex-name","bWFjaGluZQ==".into()),
        ("x-codex-protocol-version","1".into()),
        ("x-codex-subscribe-cursor","opaque-cursor".into()),
        ("x-codex-host-device-kind","desktop".into()),
    ] {
        request.headers_mut().insert(http::HeaderName::from_bytes(field.as_bytes()).unwrap(),HeaderValue::from_str(&value).unwrap());
    }
    request
}

#[tokio::test]
async fn remote_ws_provider_rejection_is_preserved_before_downstream_101() {
    use tokio::io::{AsyncReadExt,AsyncWriteExt};
    const REJECT_BODY:&str = "provider remote-control refusal";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = Url::parse(&format!("http://{}",listener.local_addr().unwrap())).unwrap();
    let fixture = remote_fixture_at(origin).await;
    let relay = enroll(&fixture).await;
    let mut provider_task = SocketTask(tokio::spawn(async move {
        let (mut stream,_) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let mut chunk = [0;512];
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0 && request.len()+n <= 8192);
            request.extend_from_slice(&chunk[..n]);
        }
        let response = format!("HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nCc-Lb-Error-Origin: principal\r\nCc-Lb-Auth-Reason: expired\r\nConnection: close\r\n\r\n{REJECT_BODY}",REJECT_BODY.len());
        stream.write_all(response.as_bytes()).await.unwrap();
    }));
    let (address,task) = spawn_proxy_router(fixture.router.clone()).await;
    let _proxy_task = SocketTask(task);
    let error = tokio::time::timeout(Duration::from_secs(5),connect_async(remote_ws_request(address,&relay)))
        .await.unwrap().expect_err("provider rejection must not become downstream 101");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected original HTTP handshake rejection");
    };
    assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["cc-lb-error-origin"],"upstream");
    assert!(!response.headers().contains_key("cc-lb-auth-reason"));
    assert_eq!(response.body().as_deref(),Some(REJECT_BODY.as_bytes()));
    tokio::time::timeout(Duration::from_secs(5),&mut provider_task.0).await.unwrap().unwrap();
}

#[derive(Clone,Copy,PartialEq,Eq)]
enum WsInvalidation { PrincipalView, Family, Owner, Drain }

#[tokio::test]
async fn remote_ws_reauthenticates_view_family_owner_and_cancels_on_drain() {
    for invalidation in [WsInvalidation::PrincipalView,WsInvalidation::Family,WsInvalidation::Owner,WsInvalidation::Drain] {
        remote_ws_invalidation_case(invalidation).await;
    }
}

async fn remote_ws_invalidation_case(invalidation:WsInvalidation) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = Url::parse(&format!("http://{}",listener.local_addr().unwrap())).unwrap();
    let fixture = remote_fixture_at(origin).await;
    let relay = enroll(&fixture).await;
    let mut receiver = fixture.bus.subscribe_lifecycle();
    let (pong_tx,pong_rx) = tokio::sync::oneshot::channel();
    let mut provider_task = SocketTask(tokio::spawn(async move {
        let (stream,_) = listener.accept().await.unwrap();
        let mut provider = tokio_tungstenite::accept_hdr_async(stream,
            |request:&ServerHandshakeRequest,response:ServerHandshakeResponse| {
                assert_eq!(request.uri().path(),codex_remote_control::SERVER);
                assert_eq!(request.headers()["authorization"],"Bearer synthetic-vendor-relay");
                for (field,value) in [
                    ("x-codex-server-id","server"),("x-codex-installation-id","installation"),
                    ("x-codex-name","bWFjaGluZQ=="),("x-codex-protocol-version","1"),
                    ("x-codex-subscribe-cursor","opaque-cursor"),("x-codex-host-device-kind","desktop"),
                ] { assert_eq!(request.headers()[field],value); }
                Ok(response)
            }).await.unwrap();
        assert!(matches!(provider.next().await,Some(Ok(UpstreamMessage::Text(text))) if text.as_str() == "client-one"));
        for text in [r#"{"sequence":1,"cursor":"one"}"#,r#"{"sequence":2,"cursor":"two"}"#] {
            provider.send(UpstreamMessage::Text(text.into())).await.unwrap();
        }
        provider.send(UpstreamMessage::Ping(Bytes::from_static(b"probe"))).await.unwrap();
        assert!(matches!(provider.next().await,Some(Ok(UpstreamMessage::Pong(bytes))) if bytes.as_ref() == b"probe"));
        pong_tx.send(()).unwrap();
        // A second pong or the post-disable application frame is a failure.
        assert!(matches!(provider.next().await,Some(Ok(UpstreamMessage::Close(_)))));
        let _ = provider.flush().await;
    }));
    let (address,task) = spawn_proxy_router(fixture.router.clone()).await;
    let _proxy_task = SocketTask(task);
    let (mut client,response) = tokio::time::timeout(Duration::from_secs(5),connect_async(remote_ws_request(address,&relay)))
        .await.unwrap().unwrap();
    assert_eq!(response.status(),StatusCode::SWITCHING_PROTOCOLS);
    client.send(UpstreamMessage::Ping(Bytes::from_static(b"client-probe"))).await.unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(5),client.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(pong,UpstreamMessage::Pong(bytes) if bytes.as_ref() == b"client-probe"));
    client.send(UpstreamMessage::Text("client-one".into())).await.unwrap();
    for expected in [r#"{"sequence":1,"cursor":"one"}"#,r#"{"sequence":2,"cursor":"two"}"#] {
        let message = tokio::time::timeout(Duration::from_secs(5),client.next()).await.unwrap().unwrap().unwrap();
        assert!(matches!(message,UpstreamMessage::Text(text) if text.as_str() == expected));
    }
    tokio::time::timeout(Duration::from_secs(5),pong_rx).await.unwrap().unwrap();
    match invalidation {
        WsInvalidation::PrincipalView => {
            // Durable authority remains valid: only full live-view reauth denies.
            let view = fixture.dynamic_view.load();
            fixture.dynamic_view.store(cc_lb_control::DynamicViewBuilder::from_view(&view)
                .principal_view(Arc::new(PrincipalView::for_tests(TEST_PRINCIPAL,false,vec!["*".into()],Vec::new(),HashMap::new())))
                .build());
        }
        WsInvalidation::Family => fixture.storage.revoke_session_family("family","renewal",100).await.unwrap(),
        WsInvalidation::Owner => {
            sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 100 WHERE binding_id = 'binding'")
                .execute(fixture.storage.pool()).await.unwrap();
        }
        WsInvalidation::Drain => fixture.drain.trigger(),
    }
    if invalidation != WsInvalidation::Drain {
        client.send(UpstreamMessage::Text("must-not-reach-provider".into())).await.unwrap();
    }
    let message = tokio::time::timeout(Duration::from_secs(5),client.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(message,UpstreamMessage::Close(_)));
    let _ = client.flush().await;
    tokio::time::timeout(Duration::from_secs(5),&mut provider_task.0).await.unwrap().unwrap();
    let events = lifecycle_until_terminal(&mut receiver).await;
    let expected = match invalidation {
        WsInvalidation::PrincipalView | WsInvalidation::Family => "authentication_failed",
        WsInvalidation::Owner => "upstream_affinity_unavailable",
        WsInvalidation::Drain => "drain_rejected",
    };
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::RequestTerminated {..})).count(),1);
    assert!(events.iter().any(|event| matches!(event,LifecycleEvent::RequestTerminated {client_status:101,
        reason:cc_lb_lifecycle::TerminationReason::ErrorCode(code),..} if code == expected)));
    assert!(events.iter().any(|event| matches!(event,LifecycleEvent::StreamCompleted {result:Err(_),..})));
    assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::UsageObserved {..} | LifecycleEvent::Priced {..})));
}

fn drain_lifecycle_events(receiver:&mut tokio::sync::broadcast::Receiver<LifecycleEvent>) -> Vec<LifecycleEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() { events.push(event); }
    events
}

fn assert_remote_accounting(events:&[LifecycleEvent], provider_status:u16, client_status:u16, code:Option<&str>) {
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::RouteCompleted {..})).count(),1);
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::UpstreamAttempt {..})).count(),1);
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::UpstreamResponseStarted {..})).count(),1);
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::RequestTerminated {..})).count(),1);
    assert!(events.iter().any(|event| matches!(event,LifecycleEvent::RouteCompleted {result:Ok(route),..}
        if route.upstream_id == Uuid::from_u128(1) && route.upstream_name == "rc-runtime-owner" && route.model.is_none())));
    assert!(events.iter().any(|event| matches!(event,LifecycleEvent::UpstreamAttempt {attempt_num:1,upstream_id,..}
        if *upstream_id == Uuid::from_u128(1))));
    assert!(events.iter().any(|event| matches!(event,LifecycleEvent::UpstreamResponseStarted {status,headers,..}
        if *status == provider_status && headers.provider_headers.get("x-codex-primary-used-percent").map(String::as_str) == Some("17.5"))));
    let terminal = events.iter().find(|event| matches!(event,LifecycleEvent::RequestTerminated {..})).unwrap();
    let LifecycleEvent::RequestTerminated {client_status:actual,reason,..} = terminal else { unreachable!() };
    assert_eq!(*actual,client_status);
    if let Some(expected) = code {
        assert!(matches!(reason,cc_lb_lifecycle::TerminationReason::ErrorCode(actual) if actual == expected));
    }
    assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::UsageObserved {..} | LifecycleEvent::Priced {..})));
    let serialized = serde_json::to_string(events).unwrap();
    assert!(!serialized.contains("synthetic-cookie=must-not-capture"));
}

#[tokio::test]
async fn remote_http_events_keep_owner_quota_and_provider_401_500_until_body_eof() {
    for (query,status,code) in [("reject=1",401,"upstream_4xx"),("reject=500",500,"upstream_5xx")] {
        let fixture = remote_fixture().await;
        let _relay = enroll(&fixture).await;
        let mut receiver = fixture.bus.subscribe_lifecycle();
        let response = fixture.router.clone().oneshot(remote_request("GET",
            &format!("/backend-api/wham/remote/control/environments/environment/clients?{query}"),
            fixture.access.expose(),Body::empty())).await.unwrap();
        assert_eq!(response.status().as_u16(),status);
        let mut events = drain_lifecycle_events(&mut receiver);
        assert!(events.iter().any(|event| matches!(event,LifecycleEvent::UpstreamResponseStarted {status:actual,..} if *actual == status)));
        assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::RequestTerminated {..})),
            "provider headers must not eagerly terminate a nonempty response body");
        response.into_body().collect().await.unwrap();
        events.extend(drain_lifecycle_events(&mut receiver));
        assert_remote_accounting(&events,status,status,Some(code));
    }
}

#[tokio::test]
async fn remote_http_events_distinguish_body_error_drop_and_known_empty_204() {
    let fixture = remote_fixture().await;
    let _relay = enroll(&fixture).await;
    for mode in ["body-error","drop","empty"] {
        let mut receiver = fixture.bus.subscribe_lifecycle();
        let path = if mode == "body-error" {
            "/backend-api/wham/remote/control/environments/environment/clients?body-error=1"
        } else if mode == "empty" {
            "/backend-api/wham/remote/control/environments/environment/clients/client"
        } else { "/backend-api/wham/remote/control/environments/environment/clients" };
        let response = fixture.router.clone().oneshot(remote_request(if mode == "empty" {"DELETE"} else {"GET"},
            path,fixture.access.expose(),Body::empty())).await.unwrap();
        let mut events = drain_lifecycle_events(&mut receiver);
        if mode != "empty" {
            assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::RequestTerminated {..})));
        }
        match mode {
            "body-error" => { assert!(response.into_body().collect().await.is_err()); }
            "drop" => drop(response),
            _ => { response.into_body().collect().await.unwrap(); }
        }
        events.extend(drain_lifecycle_events(&mut receiver));
        match mode {
            "body-error" => assert_remote_accounting(&events,200,200,Some("upstream_stream_error")),
            "drop" => assert_remote_accounting(&events,200,499,Some("client_closed_request")),
            _ => assert_remote_accounting(&events,204,204,None),
        }
    }
}

#[tokio::test]
async fn remote_http_pending_attempt_timeout_records_504_not_499() {
    let fixture = remote_fixture_config(Url::parse("https://chatgpt.com/backend-api/codex").unwrap(),1).await;
    let _relay = enroll(&fixture).await;
    let mut receiver = fixture.bus.subscribe_lifecycle();
    let notified = fixture.dispatch.pending_started.notified();
    let request = remote_request("GET",
        "/backend-api/wham/remote/control/environments/environment/clients?pending=1",
        fixture.access.expose(),Body::empty());
    let router = fixture.router.clone();
    let request_task = tokio::spawn(async move { router.oneshot(request).await.unwrap() });
    // Pause only after SQLite/auth/fencing has completed and the actual pinned
    // dispatch is pending, so external database workers cannot race auto-time.
    tokio::time::timeout(Duration::from_secs(5),notified).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    let response = request_task.await.unwrap();
    assert_eq!(response.status(),StatusCode::GATEWAY_TIMEOUT);
    response.into_body().collect().await.unwrap();
    let events = drain_lifecycle_events(&mut receiver);
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::RouteCompleted {result:Ok(route),..}
        if route.upstream_id == Uuid::from_u128(1))).count(),1);
    assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::UpstreamAttempt {upstream_id,..}
        if *upstream_id == Uuid::from_u128(1))).count(),1);
    assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::UpstreamResponseStarted {..})));
    let terminals:Vec<_> = events.iter().filter(|event| matches!(event,LifecycleEvent::RequestTerminated {..})).collect();
    assert_eq!(terminals.len(),1);
    assert!(matches!(terminals[0],LifecycleEvent::RequestTerminated {client_status:504,reason:cc_lb_lifecycle::TerminationReason::ErrorCode(code),..}
        if code == "tower_timeout"));
    assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::UsageObserved {..} | LifecycleEvent::Priced {..})));
}

async fn lifecycle_until_terminal(receiver:&mut tokio::sync::broadcast::Receiver<LifecycleEvent>) -> Vec<LifecycleEvent> {
    tokio::time::timeout(Duration::from_secs(5),async {
        let mut events = Vec::new();
        loop {
            let event = receiver.recv().await.unwrap();
            let terminal = matches!(event,LifecycleEvent::RequestTerminated {..});
            events.push(event);
            if terminal { break; }
        }
        events.extend(drain_lifecycle_events(receiver));
        events
    }).await.unwrap()
}

#[tokio::test]
async fn remote_ws_provider_error_close_records_failure_without_losing_data_or_close_frame() {
    for (code,failed) in [(1000,false),(1011,true),(1008,true),(4013,false)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = Url::parse(&format!("http://{}",listener.local_addr().unwrap())).unwrap();
        let fixture = remote_fixture_at(origin).await;
        let relay = enroll(&fixture).await;
        let mut receiver = fixture.bus.subscribe_lifecycle();
        let reason = "distinctive untrusted provider close detail";
        let mut provider_task = SocketTask(tokio::spawn(async move {
            let (stream,_) = listener.accept().await.unwrap();
            let mut provider = accept_async(stream).await.unwrap();
            provider.send(UpstreamMessage::Text("final-before-close".into())).await.unwrap();
            provider.send(UpstreamMessage::Binary(Bytes::from_static(b"final-binary-before-close"))).await.unwrap();
            provider.send(UpstreamMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code:code.into(),reason:reason.into(),
            }))).await.unwrap();
            assert!(matches!(provider.next().await,Some(Ok(UpstreamMessage::Close(_)))));
            let _ = provider.flush().await;
        }));
        let (address,task) = spawn_proxy_router(fixture.router.clone()).await;
        let _proxy_task = SocketTask(task);
        let (mut client,response) = tokio::time::timeout(Duration::from_secs(5),connect_async(remote_ws_request(address,&relay)))
            .await.unwrap().unwrap();
        assert_eq!(response.status(),StatusCode::SWITCHING_PROTOCOLS);
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5),client.next()).await.unwrap().unwrap().unwrap(),
            UpstreamMessage::Text(text) if text.as_str() == "final-before-close"));
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5),client.next()).await.unwrap().unwrap().unwrap(),
            UpstreamMessage::Binary(bytes) if bytes.as_ref() == b"final-binary-before-close"));
        let close = tokio::time::timeout(Duration::from_secs(5),client.next()).await.unwrap().unwrap().unwrap();
        assert!(matches!(close,UpstreamMessage::Close(Some(frame)) if u16::from(frame.code) == code && frame.reason.as_str() == reason));
        let _ = client.flush().await;
        tokio::time::timeout(Duration::from_secs(5),&mut provider_task.0).await.unwrap().unwrap();
        let events = lifecycle_until_terminal(&mut receiver).await;
        assert_eq!(events.iter().filter(|event| matches!(event,LifecycleEvent::RequestTerminated {..})).count(),1);
        assert!(events.iter().any(|event| matches!(event,LifecycleEvent::UpstreamResponseStarted {status:101,..})));
        if failed {
            assert!(events.iter().any(|event| matches!(event,LifecycleEvent::RequestTerminated {client_status:101,
                reason:cc_lb_lifecycle::TerminationReason::ErrorCode(error),..} if error == "upstream_stream_error")));
            assert!(events.iter().any(|event| matches!(event,LifecycleEvent::StreamCompleted {result:Err(error),..}
                if error.upstream_error_code.as_deref() == Some(code.to_string().as_str()))));
        } else {
            assert!(events.iter().any(|event| matches!(event,LifecycleEvent::StreamCompleted {result:Ok(_),..})));
        }
        assert!(!serde_json::to_string(&events).unwrap().contains(reason));
        assert!(!events.iter().any(|event| matches!(event,LifecycleEvent::UsageObserved {..} | LifecycleEvent::Priced {..})));
    }
}
