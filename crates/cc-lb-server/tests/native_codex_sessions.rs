//! Native Codex credentials through the real App, SQLite and loopback provider.
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State, WebSocketUpgrade};
use axum::extract::ws::Message;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cc_lb_aead::AeadService;
use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_server::App;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::{AccountOwnerBinding, AccountOwnerStore, ManagedKeyStore, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamKind, UpstreamStore};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{Message as ClientMessage, client::IntoClientRequest};
use tower::ServiceExt;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(20);
const NATIVE: &str = "/backend-api/codex/responses";
const SERVER_TOKEN: &str = "synthetic-server-owned-token";
const WORKSPACE: &str = "local-workspace";
type Records = Arc<Mutex<Vec<(String, String, Value)>>>;

#[derive(Default)]
struct WsBarrier {
    hold_first: AtomicBool,
    handshakes: AtomicUsize,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[derive(Clone)]
struct ProviderState {
    records: Records,
    barrier: Arc<WsBarrier>,
}

fn response_body() -> Value {
    json!({"id":"resp_native_session", "object":"response", "status":"completed", "model":"gpt-5", "output":[], "usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}})
}

async fn provider_http(State(state): State<ProviderState>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let authorization = request.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
    let body = to_bytes(request.into_body(), 1024 * 1024).await.expect("provider body");
    let body: Value = serde_json::from_slice(&body).expect("provider JSON");
    state.records.lock().expect("records").push((path, authorization, body));
    (StatusCode::OK, [("x-native-provider", "selected-owner")], Json(response_body())).into_response()
}

async fn provider_ws(State(state): State<ProviderState>, ws: WebSocketUpgrade, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let authorization = request.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
    state.barrier.handshakes.fetch_add(1, Ordering::SeqCst);
    ws.on_upgrade(move |mut socket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            if let Message::Text(text) = message {
                let body: Value = serde_json::from_str(&text).expect("provider WS JSON");
                let first = {
                    let mut records = state.records.lock().expect("records");
                    records.push((path.clone(), authorization.clone(), body));
                    records.len() == 1
                };
                let mut response = response_body();
                response["status"] = json!("in_progress");
                let created = json!({"type":"response.created", "response":response});
                if socket.send(Message::Text(created.to_string().into())).await.is_err() { break; }
                if first && state.barrier.hold_first.load(Ordering::SeqCst) {
                    state.barrier.started.notify_one();
                    state.barrier.release.notified().await;
                }
                let event = json!({"type":"response.completed", "response":response_body()});
                if socket.send(Message::Text(event.to_string().into())).await.is_err() { break; }
            }
        }
    }).into_response()
}

struct Fixture {
    _dir: tempfile::TempDir,
    app: App,
    tasks: Vec<JoinHandle<()>>,
    proxy: std::net::SocketAddr,
    records: Records,
    alternate: Records,
    access: String,
    renewal: String,
    parent: String,
    owner: Uuid,
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    barrier: Arc<WsBarrier>,
    principal_id: String,
    parent_key_id: String,
}

impl Drop for Fixture {
    fn drop(&mut self) { for task in &self.tasks { task.abort(); } }
}

impl Fixture {
    async fn new() -> Self {
        Self::with_limits(Vec::new()).await
    }

    async fn with_limits(limits: Vec<cc_lb_storage_api::Limit>) -> Self {
        let dir = tempfile::tempdir().expect("fixture directory");
        let path = dir.path().join("sessions.sqlite");
        let storage = Arc::new(cc_lb_storage_sqlite::open_sqlite(&format!("sqlite://{}", path.display()), Arc::new(cc_lb_clock::SystemClock)).await.expect("open SQLite"));
        storage.initialize().await.expect("initialize SQLite");
        let aead = Arc::new(AeadService::from_master_key([0;32]));
        let mut tasks = Vec::new();
        let records: Records = Arc::default();
        let alternate: Records = Arc::default();
        let barrier = Arc::new(WsBarrier::default());
        let mut owners = Vec::new();
        for (name, recorder) in [("selected", records.clone()), ("alternate", alternate.clone())] {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("provider bind");
            let addr = listener.local_addr().expect("provider address");
            let state = ProviderState {
                records: recorder,
                barrier: if name == "selected" { barrier.clone() } else { Arc::default() },
            };
            let router = Router::new().fallback(get(provider_ws).post(provider_http)).with_state(state);
            tasks.push(tokio::spawn(async move { axum::serve(listener, router).await.expect("serve provider"); }));
            let id = Uuid::new_v4();
            UpstreamStore::create(storage.as_ref(), UpstreamCreate {
                id, name:name.to_owned(), kind:UpstreamKind::OpenaiApiKey,
                base_url:Some(format!("http://{addr}/").parse().expect("provider URL")),
                api_key_ciphertext:Some(aead.encrypt(SERVER_TOKEN.as_bytes(), id.as_bytes()).expect("encrypt synthetic token")),
                oauth_tokens:None, warmup_enabled:false, warmup_dialect_plugin:None,
                workspace_id:None, api_key_access:None,
            }).await.expect("seed upstream");
            owners.push(id);
        }
        let now = cc_lb_clock::unix_secs(SystemTime::now());
        let principal = PrincipalStore::create(storage.as_ref(), PrincipalCreate {
            name:"native-session-parent".to_owned(), kind:PrincipalKind::Machine,
            allowed_models:Vec::new(), allowed_upstreams:owners.clone(), default_limits:limits, cache_keepalive:None,
        }, now).await.expect("seed principal");
        let managed: Arc<dyn ManagedKeyStore> = storage.clone();
        let keys = KeyStore::new(managed.clone());
        let (_, parent) = keys.create(&principal.id.to_string(), CreateParams {
            label:"session parent".to_owned(), description:None, expires_at_unix_secs:None, limit_overrides:Vec::new(),
        }).await.expect("issue parent key");
        let key_id = keys.list_all().await.expect("list key identities").into_iter().next().expect("parent identity").1;
        AccountOwnerStore::create_owner_binding(storage.as_ref(), AccountOwnerBinding {
            binding_id:"native-owner".to_owned(), principal_id:principal.id.to_string(), provider:"openai".to_owned(),
            owner_id:"synthetic-owner".to_owned(), upstream_id:owners[0], owner_generation:1,
            local_user_id:"local-user".to_owned(), allowed_workspaces:vec![WORKSPACE.to_owned()],
            provider_workspace_id:"provider-workspace".to_owned(), outbound_region:None, revision:1, revoked_at_unix_secs:None,
        }).await.expect("bind owner");
        let mut config = Config { storage:StorageConfig::Sqlite { path }, ..Config::default() };
        config.runtime.data_dir = Some(dir.path().to_owned());
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken { id:"test".to_owned(), token_env:crate::common::TEST_NONEMPTY_ENV.to_owned() }];
        let app = build_app_with_storage(config, None, managed, storage.clone(), aead, Arc::new(cc_lb_clock::SystemClock)).await.expect("build real App");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let proxy = listener.local_addr().expect("proxy address");
        let router = app.router.clone();
        tasks.push(tokio::spawn(async move { axum::serve(listener, router).await.expect("serve App"); }));
        let request = Request::builder().method("POST").uri(format!("/admin/v1/principals/{}/codex-sessions", principal.id))
            .header(header::AUTHORIZATION, format!("Bearer {}", env!("CARGO_PKG_NAME")))
            .header(header::CONTENT_TYPE,"application/json")
            .body(Body::from(json!({"parent_key_id":key_id,"owner_binding_id":"native-owner","workspace_id":WORKSPACE,
                "origin":"https://proxy.example.test","access_ttl_seconds":300,"session_ttl_seconds":600}).to_string())).expect("issuance request");
        let response = app.admin_router.clone().oneshot(request).await.expect("admin issuance");
        assert_eq!(response.status(), StatusCode::CREATED, "native session bootstrap must be mounted");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let grant: Value = serde_json::from_slice(&to_bytes(response.into_body(),1024*1024).await.expect("grant bytes")).expect("grant JSON");
        assert_eq!(grant["workspace_generation"],1);
        Self { _dir:dir, app, tasks, proxy, records, alternate,
            access:grant["access_token"].as_str().expect("access token").to_owned(),
            renewal:grant["renewal_token"].as_str().expect("renewal token").to_owned(),
            parent:parent.expose().to_owned(), owner:owners[0], storage, barrier,
            principal_id:principal.id.to_string(), parent_key_id:key_id }
    }

    async fn post(&self, path: &str, token: &str, body: Value) -> reqwest::Response {
        reqwest::Client::new().post(format!("http://{}{path}",self.proxy)).bearer_auth(token).json(&body).timeout(WAIT).send().await.expect("native request")
    }

    async fn prebody_unauthorized(&self, path: &str, token: &str) {
        let stream = futures_util::stream::pending::<Result<bytes::Bytes,std::io::Error>>();
        let request = Request::builder().method("POST").uri(path)
            .header(header::AUTHORIZATION,format!("Bearer {token}"))
            .header(header::CONTENT_TYPE,"application/json")
            .body(Body::from_stream(stream)).expect("never-ending request");
        let response = tokio::time::timeout(WAIT,self.app.router.clone().oneshot(request)).await.expect("authentication must precede body polling").expect("router response");
        assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
    }

    fn no_dispatch(&self) {
        assert!(self.records.lock().expect("records").is_empty());
        assert!(self.alternate.lock().expect("alternate").is_empty());
    }
}

fn turn() -> Value { json!({"model":"gpt-5","input":[{"role":"user","content":"native session contract"}],"store":false,"max_output_tokens":8}) }

#[tokio::test]
async fn native_session_inherits_parent_request_budget_without_pool_fallback() {
    let fixture = Fixture::with_limits(vec![cc_lb_storage_api::Limit {
        kind: cc_lb_storage_api::LimitKind::Requests,
        window_secs: 3600,
        cap_micros: 1,
    }]).await;
    assert_eq!(fixture.post(NATIVE,&fixture.access,turn()).await.status(),StatusCode::OK);
    assert_eq!(fixture.post(NATIVE,&fixture.access,turn()).await.status(),StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(fixture.records.lock().expect("records").len(),1);
    assert!(fixture.alternate.lock().expect("alternate").is_empty());
}

#[tokio::test]
async fn invalid_and_wrong_purpose_credentials_reject_before_body() {
    let fixture = Fixture::new().await;
    for token in ["invalid-native-session", fixture.renewal.as_str(), fixture.parent.as_str()] {
        fixture.prebody_unauthorized(NATIVE,token).await;
    }
    fixture.prebody_unauthorized("/oauth/token",&fixture.access).await;
    fixture.prebody_unauthorized("/oauth/revoke",&fixture.access).await;
    fixture.no_dispatch();
}

#[tokio::test]
async fn native_http_uses_only_bound_owner_and_server_credential() {
    let fixture = Fixture::new().await;
    let request = turn();
    let response = fixture.post(NATIVE,&fixture.access,request.clone()).await;
    assert_eq!(response.status(),StatusCode::OK);
    assert_eq!(response.headers()["x-native-provider"],"selected-owner");
    assert_eq!(response.json::<Value>().await.expect("response body"),response_body());
    let records = fixture.records.lock().expect("records");
    assert_eq!(records.len(),1);
    assert!(records[0].0.ends_with("/responses"));
    assert_eq!(records[0].1,format!("Bearer {SERVER_TOKEN}"));
    assert_eq!(records[0].2,request);
    assert!(fixture.alternate.lock().expect("alternate").is_empty());
}

#[tokio::test]
async fn renewal_requires_explicit_generation_and_revoke_invalidates_next_request() {
    let fixture = Fixture::new().await;
    for body in [json!({"grant_type":"refresh_token"}),json!({"grant_type":"refresh_token","expected_workspace_generation":2})] {
        assert_eq!(fixture.post("/oauth/token",&fixture.renewal,body).await.status(),StatusCode::BAD_REQUEST);
    }
    let response = fixture.post("/oauth/token",&fixture.renewal,json!({"grant_type":"refresh_token","expected_workspace_generation":1})).await;
    assert_eq!(response.status(),StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL],"no-store");
    let renewed: Value = response.json().await.expect("renewed grant");
    assert_eq!(renewed["workspace_generation"],1);
    let access = renewed["access_token"].as_str().expect("renewed access");
    assert_eq!(fixture.post("/oauth/revoke",&fixture.renewal,json!({})).await.status(),StatusCode::NO_CONTENT);
    fixture.prebody_unauthorized(NATIVE,access).await;
    fixture.prebody_unauthorized(NATIVE,&fixture.access).await;
    fixture.no_dispatch();
}

#[tokio::test]
async fn disabled_bound_owner_never_falls_back_to_other_eligible_upstream() {
    let fixture = Fixture::new().await;
    let request = Request::builder().method("POST").uri(format!("/admin/v1/upstreams/{}/disable",fixture.owner))
        .header(header::AUTHORIZATION,format!("Bearer {}",env!("CARGO_PKG_NAME")))
        .header(header::IF_MATCH,"W/\"1\"").body(Body::empty()).expect("disable request");
    let response = fixture.app.admin_router.clone().oneshot(request).await.expect("disable owner");
    assert!(response.status().is_success(),"owner disable status: {}",response.status());
    let response = fixture.post(NATIVE,&fixture.access,turn()).await;
    assert_eq!(response.status(),StatusCode::SERVICE_UNAVAILABLE);
    fixture.no_dispatch();
}

#[tokio::test]
async fn websocket_rechecks_session_after_revoke_before_next_logical_turn() {
    let fixture = Fixture::new().await;
    let mut request = format!("ws://{}{NATIVE}",fixture.proxy).into_client_request().expect("WS request");
    request.headers_mut().insert(header::AUTHORIZATION,format!("Bearer {}",fixture.access).parse().expect("bearer header"));
    let (mut socket,handshake) = tokio::time::timeout(WAIT,tokio_tungstenite::connect_async(request)).await.expect("WS handshake deadline").expect("native WS handshake");
    assert_eq!(handshake.status(),StatusCode::SWITCHING_PROTOCOLS);
    let mut body = turn(); body["type"] = json!("response.create");
    socket.send(ClientMessage::Text(body.to_string().into())).await.expect("first turn");
    tokio::time::timeout(WAIT,async {
        while let Some(frame) = socket.next().await {
            if let ClientMessage::Text(text) = frame.expect("WS frame") {
                let event: Value = serde_json::from_str(&text).expect("WS JSON");
                assert_ne!(event["type"],"error","first turn rejected: {event}");
                if event["type"] == "response.completed" { return; }
            }
        }
        panic!("socket ended before first completed turn");
    }).await.expect("first completed turn deadline");
    assert_eq!(fixture.records.lock().expect("records").len(),1);
    assert_eq!(fixture.post("/oauth/revoke",&fixture.renewal,json!({})).await.status(),StatusCode::NO_CONTENT);
    socket.send(ClientMessage::Text(body.to_string().into())).await.expect("second turn");
    tokio::time::timeout(WAIT,async {
        while let Some(frame) = socket.next().await {
            match frame.expect("rejected WS frame") {
                ClientMessage::Text(text) => {
                    let event: Value = serde_json::from_str(&text).expect("rejected WS JSON");
                    if event["type"] == "error" { assert_eq!(event["status"],401); return; }
                    assert_ne!(event["type"],"response.completed","revoked session dispatched");
                }
                ClientMessage::Close(_) => panic!("session rejection closed without an explicit authentication error"),
                _ => {}
            }
        }
        panic!("socket ended before explicit session rejection");
    }).await.expect("revoked turn deadline");
    assert_eq!(fixture.records.lock().expect("records").len(),1);
    assert!(fixture.alternate.lock().expect("alternate").is_empty());
}

#[tokio::test]
async fn renewal_never_accepts_body_credentials_or_implicit_workspace_generation() {
    let fixture = Fixture::new().await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/oauth/token",fixture.proxy))
        .json(&json!({"grant_type":"refresh_token","refresh_token":fixture.renewal,"expected_workspace_generation":1}))
        .timeout(WAIT).send().await.expect("body-only renewal");
    assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
    let response = fixture.post("/oauth/token",&fixture.renewal,
        json!({"grant_type":"refresh_token","refresh_token":"body-token-must-not-be-authority","expected_workspace_generation":1})).await;
    assert_eq!(response.status(),StatusCode::BAD_REQUEST);
    fixture.no_dispatch();
}

#[derive(Clone, Copy)]
enum QueuedMutation {
    OwnerRevision,
    FamilyRevoke,
    ParentRevoke,
}

async fn queued_turn_revalidates_authority(mutation: QueuedMutation) {
    let fixture = Fixture::new().await;
    fixture.barrier.hold_first.store(true, Ordering::SeqCst);
    let mut request = format!("ws://{}{NATIVE}",fixture.proxy).into_client_request().expect("queued WS request");
    request.headers_mut().insert(header::AUTHORIZATION,format!("Bearer {}",fixture.access).parse().expect("access header"));
    let (mut socket, handshake) = tokio::time::timeout(WAIT,tokio_tungstenite::connect_async(request))
        .await.expect("queued handshake deadline").expect("queued handshake");
    assert_eq!(handshake.status(),StatusCode::SWITCHING_PROTOCOLS);
    let mut first = turn();
    first["type"] = json!("response.create");
    first["input"][0]["content"] = json!("first held turn");
    socket.send(ClientMessage::Text(first.to_string().into())).await.expect("send held first turn");
    tokio::time::timeout(WAIT,fixture.barrier.started.notified()).await.expect("first turn reached provider");
    assert_eq!(fixture.records.lock().expect("records").len(),1);
    let mut second = turn();
    second["type"] = json!("response.create");
    second["input"][0]["content"] = json!("second queued turn");
    socket.send(ClientMessage::Text(second.to_string().into())).await.expect("send queued turn");
    socket.send(ClientMessage::Binary(bytes::Bytes::from_static(b"queue-admission-barrier")))
        .await.expect("queue barrier binary frame");
    // Unlike Ping (which tungstenite may answer automatically), this binary
    // protocol error is emitted by the serial gateway loop only after the
    // preceding response.create admission has completed. It does not cancel
    // active or queued turns. The first provider response remains held.
    tokio::time::timeout(WAIT,async {
        while let Some(frame) = socket.next().await {
            match frame.expect("queue barrier frame") {
                ClientMessage::Text(text) => {
                    let event: Value = serde_json::from_str(&text).expect("queue barrier JSON");
                    if event["type"] == "error" {
                        assert_eq!(event["status"],400,"queue admission failed before mutation: {event}");
                        assert_eq!(event["error"]["message"],"Binary WebSocket messages are not supported");
                        return;
                    }
                    assert_ne!(event["type"],"response.completed","first response escaped hold");
                }
                ClientMessage::Close(_) => panic!("socket closed before queue barrier"),
                _ => {}
            }
        }
        panic!("socket ended before queue barrier");
    }).await.expect("queue admission barrier deadline");
    assert_eq!(fixture.records.lock().expect("records").len(),1,"second turn dispatched while first was active");
    assert_eq!(fixture.barrier.handshakes.load(Ordering::SeqCst),1);
    match mutation {
        QueuedMutation::OwnerRevision => {
            let result = sqlx::query("UPDATE account_owner_bindings_v1 SET revision = revision + 1 WHERE binding_id = ?")
                .bind("native-owner").execute(fixture.storage.pool()).await.expect("advance live owner binding revision");
            assert_eq!(result.rows_affected(),1);
        }
        QueuedMutation::FamilyRevoke => {
            assert_eq!(fixture.post("/oauth/revoke",&fixture.renewal,json!({})).await.status(),StatusCode::NO_CONTENT);
        }
        QueuedMutation::ParentRevoke => {
            KeyStore::new(fixture.storage.clone())
                .revoke(&fixture.principal_id,&fixture.parent_key_id).await.expect("revoke parent in SQLite");
        }
    }
    fixture.barrier.release.notify_one();
    tokio::time::timeout(WAIT,async {
        let mut completed = false;
        while let Some(frame) = socket.next().await {
            if let ClientMessage::Text(text) = frame.expect("post-mutation frame") {
                let event: Value = serde_json::from_str(&text).expect("post-mutation JSON");
                match event["type"].as_str() {
                    Some("response.completed") => {
                        assert!(!completed,"queued turn completed after authority invalidation");
                        completed = true;
                    }
                    Some("error") => {
                        assert!(completed,"active first turn must finish before queued rejection");
                        match mutation {
                            QueuedMutation::OwnerRevision => {
                                assert_eq!(event["status"],400,"owner revision must fail locally: {event}");
                                assert_eq!(event["error"]["code"],"owner_conflict","owner conflict must retain provenance: {event}");
                            }
                            QueuedMutation::FamilyRevoke | QueuedMutation::ParentRevoke => {
                                assert_eq!(event["status"],401,"revoked queued authority must fail locally: {event}");
                            }
                        }
                        return;
                    }
                    _ => {}
                }
            }
        }
        panic!("socket ended without a local queued-turn rejection");
    }).await.expect("queued-turn rejection deadline");
    assert_eq!(fixture.records.lock().expect("records").len(),1,"invalidated queued turn reached provider");
    assert_eq!(fixture.barrier.handshakes.load(Ordering::SeqCst),1,"invalidated queued turn opened another provider connection");
    assert!(fixture.alternate.lock().expect("alternate").is_empty(),"invalidated owner fell back to another upstream");
}

#[tokio::test]
async fn queued_websocket_turn_owner_revision_is_refenced_before_dispatch() {
    queued_turn_revalidates_authority(QueuedMutation::OwnerRevision).await;
}

#[tokio::test]
async fn queued_websocket_turn_family_revoke_is_refenced_before_dispatch() {
    queued_turn_revalidates_authority(QueuedMutation::FamilyRevoke).await;
}

#[tokio::test]
async fn queued_websocket_turn_parent_revoke_is_refenced_before_dispatch() {
    queued_turn_revalidates_authority(QueuedMutation::ParentRevoke).await;
}

#[tokio::test]
async fn global_oauth_usage_rejects_session_credentials() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let fixture = Fixture::new().await;
    // The ordinary managed parent retains this existing global endpoint.
    let response = reqwest::Client::new()
        .get(format!("http://{}/api/oauth/usage",fixture.proxy))
        .bearer_auth(&fixture.parent).timeout(WAIT).send().await.expect("parent quota request");
    assert_eq!(response.status(),StatusCode::OK,"ordinary parent quota behavior changed");

    for token in [&fixture.access,&fixture.renewal] {
        // Send a real HTTP GET with a declared body that never arrives.
        // Receiving 401 before writing any body bytes proves the purpose
        // gate runs before a body read, rather than timing out on input.
        let mut stream = tokio::net::TcpStream::connect(fixture.proxy).await.expect("usage TCP connection");
        let request = format!(
            "GET /api/oauth/usage HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n",
            fixture.proxy,token,
        );
        stream.write_all(request.as_bytes()).await.expect("usage request headers");
        let status = tokio::time::timeout(WAIT,async {
            let mut headers = Vec::new();
            let mut byte = [0_u8;1];
            while !headers.ends_with(b"\r\n\r\n") {
                assert!(headers.len() < 32_768,"usage response headers exceeded bound");
                assert_ne!(stream.read(&mut byte).await.expect("usage response byte"),0,"usage connection ended before response headers");
                headers.push(byte[0]);
            }
            let line = headers.split(|byte| *byte == b'\n').next().expect("HTTP status line");
            let line = std::str::from_utf8(line).expect("ASCII HTTP status line");
            line.split_ascii_whitespace().nth(1).expect("HTTP status").parse::<u16>().expect("numeric HTTP status")
        }).await.expect("usage purpose rejection must precede body polling");
        assert_eq!(status,401,"session credential must not expose global OAuth quota");
    }
    fixture.no_dispatch();
    assert_eq!(fixture.barrier.handshakes.load(Ordering::SeqCst),0);
}
