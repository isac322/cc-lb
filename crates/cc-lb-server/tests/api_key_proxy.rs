//! API-key upstreams on the proxy path: the signer's credential and workspace
//! headers, the OAuth strip, routing that ignores the stored access snapshot,
//! readiness, and stored base URLs that predate write-time validation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::Config;
use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_control::{DynamicViewHolder, InMemoryBus, RequestEventBus};
use cc_lb_engine::{Lifecycle, LifecycleConfig, RequestEventAssemblerHandle};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyInference, ManagedKeyStore,
    MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, RemediationCode, RequestEvent,
    RequestEventStore, RequestEventStreamFilters, Storage as StorageTrait, UpstreamCreate,
    UpstreamRecord, UpstreamStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use fake_anthropic::{AppConfig, MessageScript, RecordedRequest, RequestRecorder, app};
use http_body_util::BodyExt;
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

/// Stored upstream key; no 12-byte window of it may reach a stored sink.
pub(crate) const KEY: &str = "sk-ant-api03-CANARYc4n4ry-proxy0123456789abcdef-AAAA";
const WORKSPACE: &str = "wrkspc_cfg";
const OAUTH_ACCESS_TOKEN: &str = "sk-ant-oat01-fixture-access-token";
const STORAGE_FILE: &str = "proxy.sqlite";
pub(crate) const MESSAGE_BODY: &[u8] = br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#;

/// fake-anthropic on a loopback port plus a SQLite store; every test builds
/// the real dynamic view and lifecycle over it.
pub(crate) struct ProxyHarness {
    dir: TempDir,
    pub(crate) storage: Arc<SqliteStorage>,
    aead: Arc<AeadService>,
    pub(crate) fake_url: Url,
    pub(crate) script: MessageScript,
    pub(crate) recorder: RequestRecorder,
}

impl ProxyHarness {
    pub(crate) async fn new() -> Self {
        let script = MessageScript::new();
        let recorder = RequestRecorder::default();
        let fake_addr = spawn_fake(AppConfig {
            message_script: Some(script.clone()),
            recorder: recorder.clone(),
            ..AppConfig::default()
        })
        .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!("sqlite://{}", dir.path().join(STORAGE_FILE).display());
        let storage =
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .expect("storage opens");
        storage.initialize().await.expect("storage initializes");
        Self {
            dir,
            storage: Arc::new(storage),
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            fake_url: Url::parse(&format!("http://{fake_addr}/")).expect("fake url"),
            script,
            recorder,
        }
    }

    pub(crate) fn storage_path(&self) -> std::path::PathBuf {
        self.dir.path().join(STORAGE_FILE)
    }

    pub(crate) async fn api_key_upstream(
        &self,
        name: &str,
        workspace_id: Option<&str>,
        access: Option<ApiKeyAccessWrite>,
        base_url: Url,
    ) -> UpstreamRecord {
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: Some(base_url),
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: workspace_id.map(str::to_owned),
                api_key_access: access,
            },
        )
        .await
        .expect("api-key upstream created");
        let ciphertext = self
            .aead
            .encrypt(KEY.as_bytes(), record.id.as_bytes())
            .expect("api key encrypts");
        UpstreamStore::update_api_key_secret(self.storage.as_ref(), record.id, Some(ciphertext))
            .await
            .expect("api key stored")
    }

    async fn oauth_upstream(&self, name: &str) -> UpstreamRecord {
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(self.fake_url.clone()),
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("oauth upstream created");
        let tokens = EncryptedOAuthTokens::encrypt(
            &self.aead,
            &OAuthTokenBundle {
                access_token: OAUTH_ACCESS_TOKEN.to_owned(),
                refresh_token: "oauth-refresh".to_owned(),
                account_id: None,
                project_id: None,
                expires_at_unix_secs: u64::MAX / 2,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            record.id.as_bytes(),
        )
        .expect("oauth tokens encrypt");
        UpstreamStore::store_oauth_tokens(
            self.storage.as_ref(),
            record.id,
            record.revision,
            tokens,
            false,
        )
        .await
        .expect("oauth tokens stored")
    }

    /// Creates the principal and managed key, then builds the lifecycle over
    /// the current storage state.
    pub(crate) async fn proxy(&self, upstreams: &[&UpstreamRecord]) -> Proxy {
        PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: "api-key-proxy".to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: upstreams.iter().map(|upstream| upstream.id).collect(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await
        .expect("principal created");
        let key_store = Arc::new(KeyStore::new(self.storage.clone()));
        let (_record, secret) = key_store
            .create(
                "api-key-proxy",
                CreateParams {
                    label: "api-key-proxy".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("managed key created");
        let stores = Stores {
            upstreams: self.storage.clone(),
            principals: self.storage.clone(),
            plugin_registry: self.storage.clone(),
            upstream_rate_limits: self.storage.clone(),
            upstream_subscription_quotas: self.storage.clone(),
            upstream_subscription_metadata: self.storage.clone(),
            organization_metadata: self.storage.clone(),
            plan_tiers: self.storage.clone(),
            prompt_cache_observations: self.storage.clone(),
            anthropic_compatibility_kv: self.storage.clone(),
            audit: Some(self.storage.clone()),
        };
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("wasm runtime"));
        let view = build_dynamic_view(
            &stores,
            self.aead.clone(),
            None,
            0,
            &runtime,
            self.dir.path(),
            Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
            30,
            Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
            1800,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("dynamic view builds");
        let lifecycle = Lifecycle::new_with_dynamic_view(
            Arc::new(BuiltinAuthn::new(
                key_store,
                Arc::new(cc_lb_clock::SystemClock),
            )),
            Arc::new(DynamicViewHolder::new(view)),
            cc_lb_engine::make_default_dispatcher(50),
            LifecycleConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        );
        Proxy {
            lifecycle,
            managed_key: secret.expose().to_owned(),
            events: None,
        }
    }
}

/// A lifecycle plus the managed key that authenticates to it.
pub(crate) struct Proxy {
    lifecycle: Lifecycle,
    pub(crate) managed_key: String,
    events: Option<EventCapture>,
}

/// Persists request events from the lifecycle bus and reports when the
/// final row for a request has landed.
struct EventCapture {
    _assembler: RequestEventAssemblerHandle,
    updates: tokio::sync::broadcast::Receiver<cc_lb_request_log::RequestEventUpdate>,
}

impl Proxy {
    /// Persists this lifecycle's request events into `storage`.
    pub(crate) fn recording(mut self, storage: Arc<SqliteStorage>) -> Self {
        let bus = Arc::new(InMemoryBus::new());
        let rx = bus.attach_lifecycle_assembler(
            cc_lb_control::event_bus::DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY,
        );
        let bus_arc: Arc<dyn RequestEventBus> = bus.clone();
        let assembler = cc_lb_engine::spawn_request_event_assembler(
            rx,
            storage as Arc<dyn RequestEventStore>,
            Some(bus_arc.clone()),
            Arc::new(cc_lb_observability::NoopMetricsHook),
        );
        self.events = Some(EventCapture {
            _assembler: assembler,
            updates: bus.subscribe(),
        });
        self.lifecycle = self.lifecycle.with_event_bus(bus_arc);
        self
    }

    pub(crate) async fn send(
        &mut self,
        body: &'static [u8],
        extra_headers: &[(&str, &str)],
    ) -> (StatusCode, Bytes) {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("x-api-key", self.managed_key.as_str())
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json");
        for (name, value) in extra_headers {
            builder = builder.header(*name, *value);
        }
        let request = builder
            .body(Bytes::from_static(body))
            .expect("request builds");
        let auth = self
            .lifecycle
            .authenticate(request.headers())
            .await
            .expect("managed key authenticates");
        let response = self
            .lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle responds");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        if let Some(events) = self.events.as_mut() {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !matches!(
                    events.updates.recv().await.expect("request update"),
                    cc_lb_request_log::RequestEventUpdate::Final(_)
                ) {}
            })
            .await
            .expect("final request event lands");
        }
        (status, body)
    }
}

pub(crate) async fn stored_request_events(storage: &SqliteStorage) -> Vec<RequestEvent> {
    let cursor = storage
        .current_request_event_cursor()
        .await
        .expect("request event cursor");
    storage
        .query_request_events_between_cursors(0, cursor, 500, &RequestEventStreamFilters::default())
        .await
        .expect("request events")
        .into_iter()
        .map(|(_, event)| event)
        .collect()
}

async fn spawn_fake(config: AppConfig) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake");
    let addr = listener.local_addr().expect("fake addr");
    tokio::spawn(async move { axum::serve(listener, app(config)).await });
    addr
}

fn messages(recorder: &RequestRecorder) -> Vec<RecordedRequest> {
    recorder
        .snapshot()
        .into_iter()
        .filter(|request| request.path == "/v1/messages")
        .collect()
}

/// Access snapshot for a key whose last check said `inference`.
fn access(verified: bool, inference: ApiKeyInference) -> ApiKeyAccessWrite {
    ApiKeyAccessWrite {
        verified,
        key_class: ApiKeyClass::Unknown,
        key_hint: "sk-ant-api03-...AAAA".to_owned(),
        api_key_id: None,
        needs_workspace: false,
        capabilities: ApiKeyCapabilities {
            inference,
            ..ApiKeyCapabilities::not_checked()
        },
        remediation: vec![RemediationCode::KeyForbidden],
        usage_report_state: None,
        last_error: Some("Anthropic refused the key".to_owned()),
        checked_at_unix_secs: verified.then_some(1_700_000_000),
        last_attempt_at_unix_secs: 1_700_000_000,
    }
}

/// SGN-01 + SGN-02 (b): a workspace upstream sends exactly one configured
/// key and one configured workspace header. The client's workspace and
/// managed key never reach the upstream.
#[tokio::test]
async fn sgn_01_workspace_upstream_sends_configured_key_and_workspace() {
    let harness = ProxyHarness::new().await;
    let upstream = harness
        .api_key_upstream(
            "workspace-key",
            Some(WORKSPACE),
            None,
            harness.fake_url.clone(),
        )
        .await;
    assert_eq!(upstream.workspace_id.as_deref(), Some(WORKSPACE));
    let mut proxy = harness.proxy(&[&upstream]).await;
    let managed_key = proxy.managed_key.clone();
    let bearer = format!("Bearer {managed_key}");

    let (status, body) = proxy
        .send(
            MESSAGE_BODY,
            &[
                ("anthropic-workspace-id", "wrkspc_EVIL"),
                ("authorization", bearer.as_str()),
            ],
        )
        .await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let [request] = messages(&harness.recorder)
        .try_into()
        .expect("one upstream request");
    assert_eq!(
        request.headers.get("x-api-key").map(String::as_str),
        Some(KEY)
    );
    assert_eq!(
        request
            .headers
            .get("anthropic-workspace-id")
            .map(String::as_str),
        Some(WORKSPACE)
    );
    assert_eq!(request.headers.get("authorization"), None);
    for value in request.headers.values() {
        assert!(
            !value.contains(&managed_key),
            "managed key forwarded: {value}"
        );
    }
}

/// SGN-02 (a): an upstream without a workspace strips the client's header.
#[tokio::test]
async fn sgn_02_upstream_without_workspace_strips_client_workspace() {
    let harness = ProxyHarness::new().await;
    let upstream = harness
        .api_key_upstream("plain-key", None, None, harness.fake_url.clone())
        .await;
    let mut proxy = harness.proxy(&[&upstream]).await;

    let (status, _body) = proxy
        .send(MESSAGE_BODY, &[("anthropic-workspace-id", "wrkspc_EVIL")])
        .await;

    assert_eq!(status, StatusCode::OK);
    let [request] = messages(&harness.recorder)
        .try_into()
        .expect("one upstream request");
    assert_eq!(
        request.headers.get("x-api-key").map(String::as_str),
        Some(KEY)
    );
    assert_eq!(request.headers.get("anthropic-workspace-id"), None);
    assert_eq!(request.headers.get("authorization"), None);
}

/// OAU-04: an OAuth upstream never forwards a client workspace header or
/// the managed key; it sends only its bearer token.
#[tokio::test]
async fn oau_04_oauth_upstream_strips_client_workspace() {
    let harness = ProxyHarness::new().await;
    let upstream = harness.oauth_upstream("oauth").await;
    let mut proxy = harness.proxy(&[&upstream]).await;

    let (status, body) = proxy
        .send(MESSAGE_BODY, &[("anthropic-workspace-id", "wrkspc_EVIL")])
        .await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let [request] = messages(&harness.recorder)
        .try_into()
        .expect("one upstream request");
    assert_eq!(request.headers.get("anthropic-workspace-id"), None);
    assert_eq!(request.headers.get("x-api-key"), None);
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some(format!("Bearer {OAUTH_ACCESS_TOKEN}").as_str())
    );
}

/// ACC-07: the stored access snapshot never gates routing. A key Anthropic
/// denied, one saved unverified, and one never checked all still serve.
#[tokio::test]
async fn acc_07_access_snapshot_never_gates_routing() {
    for access in [
        Some(access(false, ApiKeyInference::Denied)),
        Some(access(false, ApiKeyInference::Unverified)),
        None,
    ] {
        let label = format!("{access:?}");
        let harness = ProxyHarness::new().await;
        let upstream = harness
            .api_key_upstream("key", None, access, harness.fake_url.clone())
            .await;
        let mut proxy = harness.proxy(&[&upstream]).await;

        let (status, _body) = proxy.send(MESSAGE_BODY, &[]).await;

        assert_eq!(status, StatusCode::OK, "{label}");
        assert_eq!(messages(&harness.recorder).len(), 1, "{label}");
    }
}

/// FO-15: an upstream 401 on an API key is relayed, and nothing marks the
/// key reconnect-required (that predicate is OAuth-only).
#[tokio::test]
async fn fo_15_api_key_401_is_relayed_and_not_persisted() {
    let harness = ProxyHarness::new().await;
    let upstream = harness
        .api_key_upstream("key", None, None, harness.fake_url.clone())
        .await;
    harness
        .script
        .push_response(fake_anthropic::ScriptedMessageResponse::error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "invalid x-api-key",
        ));
    let mut proxy = harness.proxy(&[&upstream]).await;

    let (status, _body) = proxy.send(MESSAGE_BODY, &[]).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let stored = UpstreamStore::get_by_id(harness.storage.as_ref(), upstream.id)
        .await
        .expect("upstream read")
        .expect("upstream exists");
    assert_eq!(stored.last_apply_error, None);
}

/// ORG-17: a stored `base_url` that the new write-time rule would reject
/// (userinfo) still builds the view and keeps serving. Validation applies
/// only when `base_url` is written.
#[tokio::test]
async fn org_17_stored_base_url_violating_new_rule_still_serves() {
    let harness = ProxyHarness::new().await;
    let mut legacy_url = harness.fake_url.clone();
    legacy_url
        .set_username("legacy")
        .expect("userinfo settable on http url");
    let upstream = harness
        .api_key_upstream("legacy-url", None, None, legacy_url.clone())
        .await;
    assert_eq!(upstream.base_url.as_ref(), Some(&legacy_url));
    let mut proxy = harness.proxy(&[&upstream]).await;

    let (status, body) = proxy.send(MESSAGE_BODY, &[]).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(messages(&harness.recorder).len(), 1);
}

/// One readiness probe over the full app built from `storage`.
async fn readyz(harness: &ProxyHarness) -> (StatusCode, Value) {
    PrincipalStore::create(
        harness.storage.as_ref(),
        PrincipalCreate {
            name: "readiness-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("principal created");
    let managed_store: Arc<dyn ManagedKeyStore> = harness.storage.clone();
    let storage: Arc<dyn StorageTrait> = harness.storage.clone();
    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Sqlite {
            path: harness.storage_path(),
        },
        ..Default::default()
    };
    config.runtime.data_dir = Some(harness.dir.path().to_path_buf());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    let app = build_app_with_storage(
        config,
        None,
        managed_store,
        storage,
        harness.aead.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("app builds");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .expect("readyz request"),
        )
        .await
        .expect("readyz responds");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("readyz body")
        .to_bytes();
    (status, serde_json::from_slice(&body).expect("readyz json"))
}

/// RDY-01 (a): an API-key upstream alone makes the proxy ready. Cases (b)
/// to (e) live in `proxy_error_fallbacks.rs`.
#[tokio::test]
async fn rdy_01_api_key_only_is_ready() {
    let harness = ProxyHarness::new().await;
    harness
        .api_key_upstream("key", Some(WORKSPACE), None, harness.fake_url.clone())
        .await;

    let (status, json) = readyz(&harness).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["ready"], true);
    assert!(json.get("reason").is_none(), "{json}");
}

/// RDY-01 (f): disabled or deleted API-key upstreams are not ready ones.
#[tokio::test]
async fn rdy_01_disabled_or_deleted_api_key_only_is_not_ready() {
    for deleted in [false, true] {
        let harness = ProxyHarness::new().await;
        let upstream = harness
            .api_key_upstream("key", None, None, harness.fake_url.clone())
            .await;
        if deleted {
            UpstreamStore::soft_delete(harness.storage.as_ref(), upstream.id, upstream.revision)
                .await
                .expect("soft delete");
        } else {
            UpstreamStore::set_enabled(
                harness.storage.as_ref(),
                upstream.id,
                upstream.revision,
                false,
            )
            .await
            .expect("disable");
        }

        let (status, json) = readyz(&harness).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "deleted={deleted}");
        assert_eq!(json["reason"], "no_ready_upstream", "deleted={deleted}");
    }
}

/// RDY-02: the access snapshot is advisory; a key that was rejected, saved
/// unverified, or never checked keeps the proxy ready.
#[tokio::test]
async fn rdy_02_access_snapshot_does_not_affect_readiness() {
    for access in [
        Some(access(false, ApiKeyInference::Denied)),
        Some(access(false, ApiKeyInference::Unverified)),
        None,
    ] {
        let label = format!("{access:?}");
        let harness = ProxyHarness::new().await;
        harness
            .api_key_upstream("key", None, access, harness.fake_url.clone())
            .await;

        let (status, json) = readyz(&harness).await;

        assert_eq!(status, StatusCode::OK, "{label}: {json}");
        assert!(json.get("reason").is_none(), "{label}: {json}");
    }
}

/// RDY-03: an API key whose ciphertext cannot be decrypted is not ready on
/// its own; beside a usable upstream the proxy is ready with no reason.
#[tokio::test]
async fn rdy_03_undecryptable_api_key_is_not_ready_alone() {
    for with_usable in [false, true] {
        let harness = ProxyHarness::new().await;
        let broken = harness
            .api_key_upstream("broken-key", None, None, harness.fake_url.clone())
            .await;
        let foreign = AeadService::from_master_key([9; 32])
            .encrypt(KEY.as_bytes(), broken.id.as_bytes())
            .expect("foreign ciphertext");
        UpstreamStore::update_api_key_secret(harness.storage.as_ref(), broken.id, Some(foreign))
            .await
            .expect("corrupt ciphertext stored");
        if with_usable {
            harness
                .api_key_upstream("usable-key", None, None, harness.fake_url.clone())
                .await;
        }

        let (status, json) = readyz(&harness).await;

        if with_usable {
            assert_eq!(status, StatusCode::OK, "{json}");
            assert!(json.get("reason").is_none(), "{json}");
        } else {
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{json}");
            assert_eq!(json["reason"], "no_ready_upstream");
        }
    }
}
