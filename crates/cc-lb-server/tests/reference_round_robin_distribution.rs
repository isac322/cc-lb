use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, HeaderValue, Request, Response, StatusCode, header};
use axum::response::IntoResponse;
use cc_lb_admin::{AdminState, CurrentConfig, DynamicViewRebinder, router as admin_router};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{AnthropicOAuthConfig, Config, DownstreamAuthMode};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::key_store::KeyStore;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::{
    DispatchError, DynamicView, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
    UpstreamDispatch, UpstreamRateLimitSink, start_upstream_rate_limit_writer,
};
use cc_lb_plugin_api::SignedRequest;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore,
    Storage as StorageTrait, UpstreamCreate, UpstreamRateLimitObservationRecord,
    UpstreamRateLimitStateStore, UpstreamStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const ADMIN_TOKEN: &str = "round-robin-admin-token";
const TEAM_PRINCIPAL: &str = "team-X";
const MESSAGE_BODY: &[u8] = br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#;

#[tokio::test]
#[ignore = "builds and loads the wasm32-wasip1 reference router plugin"]
async fn reference_round_robin_distribution() -> TestResult<()> {
    let Some(wasm) = build_round_robin_wasm()? else {
        return Ok(());
    };
    let harness = Harness::new().await?;

    let registry_id = harness.upload_wasm(&wasm).await?;
    harness.attach_router(registry_id).await?;
    let api_key = harness.issue_downstream_key().await?;

    for _ in 0..30 {
        harness.proxy_messages(&api_key).await?;
    }

    harness.assert_approximately_equal_distribution();
    let records = harness.wait_for_rate_limit_rows().await?;
    harness.assert_rate_limit_rows_per_upstream(&records);

    Ok(())
}

struct Harness {
    admin: axum::Router,
    lifecycle: Arc<Lifecycle>,
    storage: Arc<SqliteStorage>,
    principal_id: Uuid,
    upstreams: Vec<SeededUpstream>,
    rate_limit_writer: JoinHandle<()>,
}

impl Harness {
    async fn new() -> TestResult<Self> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("data");
        let key = [26; 32];
        let storage_path = dir.path().join("round-robin.sqlite");
        let database_url = format!("sqlite://{}", storage_path.display());
        let storage = cc_lb_storage_sqlite::open_sqlite(&database_url).await?;
        storage.initialize(BackendKind::Sqlite).await?;
        let storage = Arc::new(storage);
        let aead = Arc::new(AeadService::from_master_key(key));
        let principal = PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: TEAM_PRINCIPAL.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            now_secs(),
        )
        .await?;

        let upstreams = seed_oauth_upstreams(storage.as_ref(), aead.as_ref()).await?;
        let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(CountingDispatch::new(&upstreams));
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage.clone()),
            plugin_registry_repo: None,
        });
        let runtime = Arc::new(cc_lb_runtime_extism::ExtismRuntime::new());
        let oauth_cfg = Arc::new(AnthropicOAuthConfig::default());
        let initial_view = rebuild_test_view(
            &stores,
            oauth_cfg.as_ref(),
            aead.clone(),
            runtime.as_ref(),
            &data_dir,
            dispatcher.clone(),
            0,
        )
        .await?;
        let dynamic_view = Arc::new(DynamicViewHolder::new(initial_view));
        let key_store = Arc::new(KeyStore::new(storage.clone()));
        let authn = Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::ApiKey,
            None,
            Some(key_store.clone()),
        ));
        let limit_engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()));
        let storage_trait: Arc<dyn StorageTrait> = storage.clone();
        let (rate_limit_sink, rate_limit_receiver) = UpstreamRateLimitSink::new();
        let rate_limit_writer =
            start_upstream_rate_limit_writer(storage_trait.clone(), rate_limit_receiver);
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                dynamic_view.clone(),
                LifecycleConfig::default(),
            )
            .with_limit_engine(limit_engine.clone(), authn)
            .with_upstream_rate_limit_sink(rate_limit_sink),
        );
        let mut config = Config::default();
        config.runtime.data_dir = Some(data_dir.clone());
        config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
        config.downstream_auth.none_mode = None;
        config.admin.token = Some(ADMIN_TOKEN.to_owned());
        let rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(TestRebinder {
            stores,
            oauth_cfg,
            runtime,
            aead: aead.clone(),
            data_dir,
            dispatcher,
        });
        let current_config = Arc::new(TestCurrentConfig {
            config: Arc::new(config.clone()),
            rebinder,
        });
        let admin = admin_router(AdminState {
            storage: Some(storage_trait),
            key_store: Some(key_store),
            aead,
            limit_engine,
            lifecycle: Some(lifecycle.clone()),
            audit_sink: None,
            dynamic_view,
            config: current_config,
            admin_token: Some(ADMIN_TOKEN.to_owned()),
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            subscription_metadata_hook: None,
            start_time: Instant::now(),
        });
        std::mem::forget(dir);

        Ok(Self {
            admin,
            lifecycle,
            storage,
            principal_id: principal.id,
            upstreams,
            rate_limit_writer,
        })
    }

    async fn upload_wasm(&self, wasm: &[u8]) -> TestResult<Uuid> {
        let boundary = format!("boundary-{}", Uuid::new_v4());
        let body = multipart_body(&boundary, "round-robin", "round-robin.wasm", wasm);
        let response = self
            .admin_response(
                Request::builder()
                    .method("POST")
                    .uri("/admin/v1/plugins/wasm")
                    .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))?,
            )
            .await?;
        assert_eq!(response.status, StatusCode::CREATED, "{}", response.json);
        json_uuid(&response.json, "id")
    }

    async fn attach_router(&self, registry_id: Uuid) -> TestResult<()> {
        let response = self
            .admin_json(
                "POST",
                &format!("/admin/v1/principals/{}/plugin-chain", self.principal_id),
                json!({
                    "slot": "Router",
                    "wasm_registry_id": registry_id,
                    "config": {}
                }),
            )
            .await?;
        assert_eq!(response.status, StatusCode::CREATED, "{}", response.json);
        assert!(
            response.headers.get("x-cc-lb-rebind-status").is_none(),
            "dynamic rebind deferred after plugin-chain attach: {:?}",
            response.headers
        );
        assert!(
            response.headers.get("x-cc-lb-generation").is_some(),
            "plugin-chain attach did not report a dynamic view generation"
        );
        Ok(())
    }

    async fn issue_downstream_key(&self) -> TestResult<String> {
        let response = self
            .admin_json(
                "POST",
                &format!("/admin/v1/principals/{TEAM_PRINCIPAL}/keys"),
                json!({ "label": "round-robin acceptance" }),
            )
            .await?;
        assert_eq!(response.status, StatusCode::CREATED, "{}", response.json);
        json_string(&response.json, "plaintext_key")
    }

    async fn proxy_messages(&self, api_key: &str) -> TestResult<()> {
        let response = self
            .lifecycle
            .handle(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", api_key)
                    .header("anthropic-version", "2023-06-01")
                    .header("content-type", "application/json")
                    .body(Bytes::from_static(MESSAGE_BODY))?,
            )
            .await?;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(json["type"], "message");
        Ok(())
    }

    fn assert_approximately_equal_distribution(&self) {
        let counts = self
            .upstreams
            .iter()
            .map(|upstream| {
                (
                    upstream.name.clone(),
                    upstream.count.load(Ordering::Acquire),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(counts.values().sum::<usize>(), 30, "{counts:?}");
        for (name, count) in counts {
            assert!(
                (9..=11).contains(&count),
                "upstream {name} received {count} requests, expected 10 +/- 1"
            );
        }
    }

    async fn wait_for_rate_limit_rows(
        &self,
    ) -> TestResult<Vec<UpstreamRateLimitObservationRecord>> {
        let upstream_ids = self.upstream_ids();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let records = self.storage.list_for_upstream_ids(&upstream_ids).await?;
            if upstream_ids.iter().all(|upstream_id| {
                records
                    .iter()
                    .any(|record| record.upstream_id == *upstream_id)
            }) {
                return Ok(records);
            }
            if Instant::now() >= deadline {
                return Ok(records);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn assert_rate_limit_rows_per_upstream(&self, records: &[UpstreamRateLimitObservationRecord]) {
        for upstream in &self.upstreams {
            assert!(
                records
                    .iter()
                    .any(|record| record.upstream_id == upstream.id),
                "missing upstream_rate_limit_state_v1 row for {}: {records:?}",
                upstream.name
            );
        }
    }

    fn upstream_ids(&self) -> Vec<Uuid> {
        self.upstreams.iter().map(|upstream| upstream.id).collect()
    }

    async fn admin_json(&self, method: &str, uri: &str, body: Value) -> TestResult<TestResponse> {
        self.admin_response(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await
    }

    async fn admin_response(&self, request: Request<Body>) -> TestResult<TestResponse> {
        let response = self.admin.clone().oneshot(request).await?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await?.to_bytes();
        let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if !status.is_success() {
            eprintln!("status={status} body={}", String::from_utf8_lossy(&body));
        }
        Ok(TestResponse {
            status,
            headers,
            json,
        })
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.rate_limit_writer.abort();
    }
}

struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    json: Value,
}

#[derive(Clone)]
struct SeededUpstream {
    id: Uuid,
    name: String,
    access_token: String,
    count: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CountingDispatch {
    counters_by_token: HashMap<String, Arc<AtomicUsize>>,
}

impl CountingDispatch {
    fn new(upstreams: &[SeededUpstream]) -> Self {
        Self {
            counters_by_token: upstreams
                .iter()
                .map(|upstream| (upstream.access_token.clone(), upstream.count.clone()))
                .collect(),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for CountingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(ToOwned::to_owned);
        let Some(counter) = token
            .as_ref()
            .and_then(|token| self.counters_by_token.get(token))
        else {
            let mut response = axum::Json(json!({
                "type": "error",
                "error": {
                    "type": "authentication_error",
                    "message": "missing test OAuth token"
                }
            }))
            .into_response();
            *response.status_mut() = StatusCode::UNAUTHORIZED;
            return Ok(response);
        };

        counter.fetch_add(1, Ordering::AcqRel);
        let mut response = axum::Json(json!({
            "id": "msg_round_robin_acceptance",
            "type": "message",
            "role": "assistant",
            "model": "claude-3-5-sonnet-20241022",
            "content": [{ "type": "text", "text": "ok" }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50
            }
        }))
        .into_response();
        response
            .headers_mut()
            .insert("request-id", HeaderValue::from_static("req_round_robin"));
        response.headers_mut().insert(
            "anthropic-ratelimit-requests-remaining",
            HeaderValue::from_static("999"),
        );
        response.headers_mut().insert(
            "anthropic-ratelimit-tokens-remaining",
            HeaderValue::from_static("999000"),
        );
        Ok(response)
    }
}

struct TestCurrentConfig {
    config: Arc<Config>,
    rebinder: Arc<dyn DynamicViewRebinder>,
}

impl CurrentConfig for TestCurrentConfig {
    fn current_config(&self) -> Arc<Config> {
        self.config.clone()
    }

    fn dynamic_view_rebinder(&self) -> Option<Arc<dyn DynamicViewRebinder>> {
        Some(self.rebinder.clone())
    }
}

struct TestRebinder {
    stores: Arc<Stores>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    runtime: Arc<cc_lb_runtime_extism::ExtismRuntime>,
    aead: Arc<AeadService>,
    data_dir: PathBuf,
    dispatcher: Arc<dyn UpstreamDispatch>,
}

#[async_trait]
impl DynamicViewRebinder for TestRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        let view = rebuild_test_view(
            &self.stores,
            self.oauth_cfg.as_ref(),
            self.aead.clone(),
            self.runtime.as_ref(),
            &self.data_dir,
            self.dispatcher.clone(),
            current_generation,
        )
        .await
        .map_err(|source| anyhow::anyhow!(source.to_string()))?;
        Ok(view)
    }
}

async fn rebuild_test_view(
    stores: &Stores,
    oauth_cfg: &AnthropicOAuthConfig,
    aead: Arc<AeadService>,
    runtime: &cc_lb_runtime_extism::ExtismRuntime,
    data_dir: &std::path::Path,
    dispatcher: Arc<dyn UpstreamDispatch>,
    current_generation: u64,
) -> TestResult<Arc<DynamicView>> {
    let view = build_dynamic_view(
        stores,
        oauth_cfg,
        aead,
        None,
        current_generation,
        runtime,
        data_dir,
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
    )
    .await?;
    Ok(DynamicViewBuilder::from_view(&view)
        .dispatcher(dispatcher)
        .build())
}

async fn seed_oauth_upstreams(
    storage: &SqliteStorage,
    aead: &AeadService,
) -> TestResult<Vec<SeededUpstream>> {
    let mut upstreams = Vec::new();
    for name in ["oauth-a", "oauth-b", "oauth-c"] {
        let access_token = format!("sk-ant-oat01-{name}-token");
        let record = UpstreamStore::create(
            storage,
            UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse(&format!("http://{name}.invalid"))?),
                api_key_ciphertext: None,
                warmup_enabled: false,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            },
        )
        .await?;
        let encrypted = EncryptedOAuthTokens::encrypt(
            aead,
            &OAuthTokenBundle {
                access_token: access_token.clone(),
                refresh_token: format!("sk-ant-ort01-{name}-refresh"),
                expires_at_unix_secs: now_secs() + 3600,
                scopes: vec!["messages".to_owned()],
            },
            record.id.as_bytes(),
        )?;
        UpstreamStore::store_oauth_tokens(storage, record.id, record.revision, encrypted).await?;
        upstreams.push(SeededUpstream {
            id: record.id,
            name: name.to_owned(),
            access_token,
            count: Arc::new(AtomicUsize::new(0)),
        });
    }
    Ok(upstreams)
}

fn build_round_robin_wasm() -> TestResult<Option<Vec<u8>>> {
    let root = repo_root();
    let status = Command::new("cargo")
        .current_dir(&root)
        .args([
            "build",
            "--manifest-path",
            "plugins/router/round-robin/Cargo.toml",
            "--target",
            "wasm32-wasip1",
            "--release",
        ])
        .status()?;
    if !status.success() {
        if !wasm32_wasip1_target_installed() {
            eprintln!("skipped: wasm32-wasip1 toolchain missing");
            return Ok(None);
        }
        return Err(error(format!(
            "round-robin wasm build failed with {status}"
        )));
    }

    for path in round_robin_wasm_candidates(&root) {
        if path.exists() {
            return Ok(Some(std::fs::read(path)?));
        }
    }

    Err(error("round-robin wasm artifact was not produced"))
}

fn round_robin_wasm_candidates(root: &std::path::Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(
            PathBuf::from(target_dir)
                .join("wasm32-wasip1")
                .join("release")
                .join("cc_lb_router_round_robin.wasm"),
        );
    }
    candidates.push(
        root.join("plugins")
            .join("router")
            .join("round-robin")
            .join("target")
            .join("wasm32-wasip1")
            .join("release")
            .join("cc_lb_router_round_robin.wasm"),
    );
    candidates
}

fn wasm32_wasip1_target_installed() -> bool {
    Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.trim() == "wasm32-wasip1")
        })
        .unwrap_or(false)
}

fn multipart_body(boundary: &str, name: &str, original_filename: &str, bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    push_text_part(&mut body, boundary, "name", name.as_bytes());
    push_text_part(
        &mut body,
        boundary,
        "original_filename",
        original_filename.as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bytes\"; filename=\"plugin.wasm\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/wasm\r\n\r\n");
    body.extend_from_slice(bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn push_text_part(body: &mut Vec<u8>, boundary: &str, name: &str, value: &[u8]) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(value);
    body.extend_from_slice(b"\r\n");
}

fn json_uuid(value: &Value, field: &str) -> TestResult<Uuid> {
    Ok(Uuid::parse_str(&json_string(value, field)?)?)
}

fn json_string(value: &Value, field: &str) -> TestResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| error(format!("response missing {field}: {value}")))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("cc-lb-server lives under crates/")
        .to_path_buf()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
