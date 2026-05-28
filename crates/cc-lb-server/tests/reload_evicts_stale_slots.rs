mod reload_common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

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
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, Upstream,
};
use cc_lb_server::builtins::BuiltinRouter;
use cc_lb_server::reload::ConfigWatcher;
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage};
use http::header::CONTENT_TYPE;
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use tokio::sync::Notify;

#[test]
fn reload_evicts_stale_slots() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let observe_path = dir.path().join("observe.wasm");
    reload_common::write_bytes(&observe_path, reload_common::OBSERVE_WASM);
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    write_config_with_alice_hook(&config_path, proxy_addr, &observe_path);

    let initial_config = reload_common::load_config(&config_path);
    let runtime = Arc::new(cc_lb_runtime_extism::ExtismRuntime::new());
    let dynamic_view = reload_common::dynamic_view_holder(&initial_config);
    let watcher = ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        runtime.clone(),
        Some(dynamic_view),
    );

    watcher.reload_now().expect("initial reload stages slots");
    let mut before = runtime.registered_slot_keys();
    before.sort();
    assert_eq!(
        before,
        vec![
            ("__global__".to_owned(), "global-hook".to_owned()),
            ("alice".to_owned(), "alice-hook".to_owned()),
        ]
    );

    write_config_without_alice_hook(&config_path, proxy_addr, &observe_path);
    let new_config = watcher.reload_now().expect("reload succeeds");

    let mut after = runtime.registered_slot_keys();
    after.sort();
    assert_eq!(
        after,
        vec![("__global__".to_owned(), "global-hook".to_owned())]
    );
    assert_eq!(
        runtime.registered_slot_keys().len(),
        expected_referenced_slot_count(&new_config)
    );
}

#[tokio::test]
async fn reload_inflight_request_unaffected_by_eviction() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let config_path = dir.path().join("cc-lb.toml");
    let observe_path = dir.path().join("observe.wasm");
    reload_common::write_bytes(&observe_path, reload_common::OBSERVE_WASM);
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse()?;
    write_config_with_alice_hook(&config_path, proxy_addr, &observe_path);

    let initial_config = reload_common::load_config(&config_path);
    let runtime = Arc::new(cc_lb_runtime_extism::ExtismRuntime::new());
    let dynamic_view = reload_common::dynamic_view_holder(&initial_config);
    let watcher = Arc::new(ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        runtime.clone(),
        Some(dynamic_view.clone()),
    ));
    watcher.reload_now()?;

    let lifecycle = lifecycle_with_view(
        watcher.current_config(),
        dynamic_view.clone(),
        BlockedDispatch::default(),
    )?;
    let request_entered_dispatch = lifecycle.dispatch.request_entered_dispatch.clone();
    let resume_request = lifecycle.dispatch.resume_request.clone();
    let handle = tokio::spawn({
        let lifecycle = lifecycle.lifecycle;
        async move { lifecycle.handle(messages_request()).await }
    });
    request_entered_dispatch.notified().await;

    write_config_without_alice_hook(&config_path, proxy_addr, &observe_path);
    watcher.reload_now()?;
    assert_eq!(
        runtime.registered_slot_keys(),
        vec![("__global__".to_owned(), "global-hook".to_owned())]
    );

    resume_request.notify_one();
    let response = handle.await??;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();

    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("message"));
    Ok(())
}

fn write_config_with_alice_hook(path: &Path, proxy_addr: SocketAddr, observe_path: &Path) {
    let observe_path = reload_common::toml_path(observe_path);
    let config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[upstreams.fake]
kind = "custom"
base_url = "http://upstream.local/"
auth_strategy = "api_key"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "alice"
upstream_kind = "anthropic_key"
upstream_credential_ref = "test-upstream"

[[plugins.observability_hooks]]
name = "global-hook"
wasm_path = "{observe_path}"

[principals.alice]
allowed_models = ["*"]

[[principals.alice.observability_hooks]]
name = "alice-hook"
wasm_path = "{observe_path}"
"#
    );
    std::fs::write(path, config).unwrap();
}

fn write_config_without_alice_hook(path: &Path, proxy_addr: SocketAddr, observe_path: &Path) {
    let observe_path = reload_common::toml_path(observe_path);
    let config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[upstreams.fake]
kind = "custom"
base_url = "http://upstream.local/"
auth_strategy = "api_key"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "alice"
upstream_kind = "anthropic_key"
upstream_credential_ref = "test-upstream"

[[plugins.observability_hooks]]
name = "global-hook"
wasm_path = "{observe_path}"

[principals.alice]
allowed_models = ["*"]
"#
    );
    std::fs::write(path, config).unwrap();
}

fn expected_referenced_slot_count(config: &Config) -> usize {
    let global_router = usize::from(config.plugins.router_plugin.is_some());
    let global_hooks = config.plugins.observability_hooks.len();
    let principal_slots = config.principals.values().map(|spec| {
        usize::from(spec.router_plugin.is_some())
            + spec.observability_hooks.as_ref().map_or(0, Vec::len)
    });
    global_router + global_hooks + principal_slots.sum::<usize>()
}

#[derive(Clone, Default)]
struct BlockedDispatch {
    request_entered_dispatch: Arc<Notify>,
    resume_request: Arc<Notify>,
}

#[async_trait]
impl UpstreamDispatch for BlockedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.request_entered_dispatch.notify_one();
        self.resume_request.notified().await;
        let mut response = Response::new(Body::from(Bytes::from(
            json!({
                "type": "message",
                "usage": {"input_tokens": 1, "output_tokens": 1}
            })
            .to_string(),
        )));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

struct LifecycleHarness {
    lifecycle: Lifecycle,
    dispatch: BlockedDispatch,
}

fn lifecycle_with_view(
    config: Arc<Config>,
    dynamic_view: Arc<DynamicViewHolder>,
    dispatch: BlockedDispatch,
) -> Result<LifecycleHarness, Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(
        &storage_dir.path().join("auth.redb"),
        [7; 32],
    )?);
    let _storage_dir = Box::leak(Box::new(storage_dir));
    let authn = Arc::new(BuiltinAuthn::new(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: "alice".to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            upstream_credential_ref: "test-upstream".to_owned(),
        }),
        Arc::new(KeyStore::new(Arc::new(RedbManagedKeyStore::new(storage)))),
    ));
    let lifecycle = Lifecycle::new(
        authn,
        dynamic_view.load().principal_view.clone(),
        Arc::new(NoopSignerFactory),
        Arc::new(BuiltinRouter::new(&config)?),
        Arc::new(dispatch.clone()),
        Vec::new(),
        LifecycleConfig::default(),
    );
    Ok(LifecycleHarness {
        lifecycle,
        dispatch,
    })
}

fn messages_request() -> http::Request<Bytes> {
    http::Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-test")
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(br#"{"model":"claude","messages":[]}"#))
        .expect("request builds")
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_api_key(&self, _api_key: String) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

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
