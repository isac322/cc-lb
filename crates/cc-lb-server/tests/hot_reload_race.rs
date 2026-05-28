mod reload_common;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_config::{Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::api_keys::key_store::KeyStore;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewHolder, Lifecycle, LifecycleConfig,
    UpstreamDispatch,
};
use cc_lb_plugin_api::{
    ObservabilityError, ObservabilityHook, ObserveEvent, RetryDecision, ShapedRequest,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, Upstream,
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::builtins::BuiltinRouter;
use cc_lb_server::reload::ConfigWatcher;
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::Barrier;

const PRINCIPAL_COUNT: usize = 10;
const PLUGINS_PER_PRINCIPAL: usize = 3;
const CLIENT_TASKS: usize = 50;
const REQUESTS_PER_CLIENT: usize = 10;
const RELOAD_COUNT: usize = 5;
const EVENTS_PER_REQUEST: usize = 4;
const EXPECTED_REQUESTS: usize = CLIENT_TASKS * REQUESTS_PER_CLIENT;
const EXPECTED_OBSERVE_EVENTS: usize = EXPECTED_REQUESTS * EVENTS_PER_REQUEST;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_reload_race_keeps_requests_successful_and_observed()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = StubWasms::new()?;
    let config_dir = tempfile::tempdir()?;
    let config_path = config_dir.path().join("cc-lb.toml");
    write_config(&config_path, &fixture, 0)?;

    let initial_config = reload_common::load_config(&config_path);
    let runtime = Arc::new(ExtismRuntime::new());
    let dynamic_view = reload_common::dynamic_view_holder(&initial_config);
    let watcher = Arc::new(ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        runtime.clone(),
        Some(dynamic_view.clone()),
    ));
    watcher.reload_now()?;
    assert_eq!(
        runtime.registered_slot_keys().len(),
        PRINCIPAL_COUNT * PLUGINS_PER_PRINCIPAL
    );

    let observed_events = Arc::new(AtomicUsize::new(0));
    let lifecycle = Arc::new(lifecycle_with_view(
        watcher.current_config(),
        dynamic_view,
        observed_events.clone(),
    )?);
    let start = Arc::new(Barrier::new(CLIENT_TASKS + 2));

    let mut clients = Vec::with_capacity(CLIENT_TASKS);
    for client_id in 0..CLIENT_TASKS {
        let lifecycle = lifecycle.clone();
        let start = start.clone();
        clients.push(tokio::spawn(async move {
            start.wait().await;
            let mut non_2xx = 0_usize;
            for request_index in 0..REQUESTS_PER_CLIENT {
                let response = lifecycle
                    .handle(messages_request(client_id, request_index))
                    .await
                    .expect("lifecycle handles request");
                let status = response.status();
                let _body = response
                    .into_body()
                    .collect()
                    .await
                    .expect("response body collects")
                    .to_bytes();
                if !status.is_success() {
                    non_2xx += 1;
                }
                tokio::task::yield_now().await;
            }
            non_2xx
        }));
    }

    let reloads = tokio::spawn({
        let start = start.clone();
        let watcher = watcher.clone();
        let config_path = config_path.clone();
        let fixture = fixture.clone_paths();
        async move {
            start.wait().await;
            for generation in 1..=RELOAD_COUNT {
                write_config_from_paths(&config_path, &fixture, generation)
                    .expect("reload config writes");
                let watcher = watcher.clone();
                tokio::task::spawn_blocking(move || watcher.reload_now().map(|_| ()))
                    .await
                    .expect("reload task joins")
                    .expect("reload succeeds");
            }
        }
    });

    start.wait().await;
    let mut non_2xx = 0_usize;
    for client in clients {
        non_2xx += client.await.expect("client task does not panic");
    }
    reloads.await.expect("reload task does not panic");

    assert_eq!(non_2xx, 0);
    assert_eq!(
        observed_events.load(Ordering::Acquire),
        EXPECTED_OBSERVE_EVENTS
    );
    assert_eq!(
        runtime.registered_slot_keys().len(),
        PRINCIPAL_COUNT * PLUGINS_PER_PRINCIPAL
    );

    Ok(())
}

fn lifecycle_with_view(
    config: Arc<Config>,
    dynamic_view: Arc<DynamicViewHolder>,
    observed_events: Arc<AtomicUsize>,
) -> Result<Lifecycle, Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(
        &storage_dir.path().join("hot-reload-race.redb"),
        [22; 32],
    )?);
    let _storage_dir = Box::leak(Box::new(storage_dir));
    let authn = Arc::new(BuiltinAuthn::new(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: request_principal().to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            upstream_credential_ref: "test-upstream".to_owned(),
        }),
        Arc::new(KeyStore::new(Arc::new(RedbManagedKeyStore::new(storage)))),
    ));
    Ok(Lifecycle::new(
        authn,
        dynamic_view.load().principal_view.clone(),
        Arc::new(NoopApiKeySignerFactory),
        Arc::new(BuiltinRouter::new(&config)?),
        Arc::new(OkDispatch),
        vec![Arc::new(CountingHook { observed_events })],
        LifecycleConfig::default(),
    ))
}

#[derive(Clone)]
struct CountingHook {
    observed_events: Arc<AtomicUsize>,
}

impl ObservabilityHook for CountingHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        self.observed_events.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct OkDispatch;

#[async_trait]
impl UpstreamDispatch for OkDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        let mut response = Response::new(Body::from(Bytes::from_static(br#"{}"#)));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }
}

fn messages_request(client_id: usize, request_index: usize) -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("request-id", format!("race-{client_id}-{request_index}"))
        .header("x-api-key", "sk-ant-test")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"max_tokens":8}"#,
        ))
        .expect("request builds")
}

struct StubWasms {
    _dir: tempfile::TempDir,
    router_path: PathBuf,
    observe_path: PathBuf,
}

impl StubWasms {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let router_path = dir.path().join("router.wasm");
        let observe_path = dir.path().join("observe.wasm");
        fs::write(&router_path, wat::parse_str(router_module())?)?;
        fs::write(&observe_path, wat::parse_str(observe_module())?)?;
        Ok(Self {
            _dir: dir,
            router_path,
            observe_path,
        })
    }

    fn clone_paths(&self) -> StubWasmPaths {
        StubWasmPaths {
            router_path: self.router_path.clone(),
            observe_path: self.observe_path.clone(),
        }
    }
}

struct StubWasmPaths {
    router_path: PathBuf,
    observe_path: PathBuf,
}

fn write_config(path: &Path, fixture: &StubWasms, generation: usize) -> io::Result<()> {
    write_config_from_paths(path, &fixture.clone_paths(), generation)
}

fn write_config_from_paths(
    path: &Path,
    fixture: &StubWasmPaths,
    generation: usize,
) -> io::Result<()> {
    let mut config = format!(
        r#"[listener]
proxy_addr = "127.0.0.1:18080"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[upstreams.fake]
kind = "custom"
base_url = "http://upstream.local/"
auth_strategy = "api_key"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "{}"
upstream_kind = "anthropic_key"
upstream_credential_ref = "test-upstream"

"#,
        request_principal()
    );
    let router_path = reload_common::toml_path(&fixture.router_path);
    let observe_path = reload_common::toml_path(&fixture.observe_path);
    for principal_index in 0..PRINCIPAL_COUNT {
        config.push_str(&format!(
            r#"[principals.principal_{principal_index:02}]
allowed_models = ["*"]

[principals.principal_{principal_index:02}.router_plugin]
name = "router-g{generation}"
wasm_path = "{router_path}"

[[principals.principal_{principal_index:02}.observability_hooks]]
name = "observe-a-g{generation}"
wasm_path = "{observe_path}"

[[principals.principal_{principal_index:02}.observability_hooks]]
name = "observe-b-g{generation}"
wasm_path = "{observe_path}"

"#
        ));
    }
    fs::write(path, config)
}

fn request_principal() -> &'static str {
    "request-principal"
}

fn router_module() -> &'static str {
    r#"(module (func (export "route") (result i32) (i32.const 0)))"#
}

fn observe_module() -> &'static str {
    r#"(module (func (export "observe") (result i32) (i32.const 0)))"#
}

struct NoopApiKeySignerFactory;

impl ApiKeyAwareSignerFactory for NoopApiKeySignerFactory {
    fn with_api_key(&self, _api_key: String) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

struct NoopSignerFactory;

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(NoopSigner))
    }
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

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}
