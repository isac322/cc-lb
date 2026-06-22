//! Regression test for the cc-lb extism v2 shape plugin path.
//!
//! Sibling of `dispatch_uses_resolved_upstream_base_url.rs`, which exercises the
//! non-extism `AnthropicDirectDialect` path. That test verified the lifecycle
//! rebuilds the dialect using the resolved upstream's `base_url`. This test
//! covers the analogous correctness property when a custom shape **plugin** is
//! attached to a principal: when an operator sets `base_url` on an upstream
//! record, the host must surface that override to the plugin via the v2
//! `ShapeRequest.upstream_base_url` envelope field.
//!
//! Before the fix, `ExtismDialectPlugin` cached a `base_url: Option<String>`
//! field on the wrapper struct and hard-coded it to `None` in the only
//! constructor — so v2 shape plugins always received `upstream_base_url:
//! None` regardless of the operator's configuration, and any plugin that
//! sensibly falls back to `https://api.anthropic.com` when the field is `None`
//! would silently misroute traffic.
//!
//! The fix moves `base_url` onto the host-side `Upstream::AnthropicDirect`
//! payload, populated at upstream resolution time, and has the Extism shape
//! wrapper read it from the `&Upstream` argument and serialize it into
//! `ShapeRequestV2.upstream_base_url`.
//!
//! The plugin used by this test inspects its raw extism input bytes for the
//! exact substring `"upstream_base_url":"http://target.invalid/"` (the
//! trailing slash is from `url::Url::to_string` normalization); when found,
//! it emits a shape response targeting `http://target.invalid/v1/messages`,
//! otherwise it falls back to the canonical Anthropic URL. So:
//!
//! * Before the fix: the marker is absent → plugin emits the Anthropic URL →
//!   the recording dispatcher captures `host_str() == "api.anthropic.com"` →
//!   assertion fails.
//! * After the fix: the marker is present → plugin emits the override URL →
//!   dispatcher captures `host_str() == "target.invalid"` → assertion passes.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Bytes;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use cc_lb_aead::AeadService;
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::{
    Body, DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
    UpstreamDispatch,
};
use cc_lb_plugin_api::SignedRequest;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::SubscriptionQuotaCache;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginChainEntryInput, PluginRegistryStore, PluginSlot,
    PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamStore, WasmBlob,
    WasmRegistryEntryInput,
};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn extism_shape_plugin_receives_resolved_upstream_base_url_via_envelope() {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("extism-shape-base-url.sqlite").display()
    );
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url)
        .await
        .expect("storage opens");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("storage initializes");
    let storage = Arc::new(storage);

    let target = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "bbb-target".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse("http://target.invalid").expect("target base_url parses")),
            api_key_ciphertext: Some(vec![1, 2, 3]),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("target upstream created");

    let principal = PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "test-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: vec![target.id],
            default_limits: Vec::new(),
        },
        now_secs(),
    )
    .await
    .expect("principal created");

    attach_shape_plugin(storage.as_ref(), principal.id)
        .await
        .expect("shape plugin attached");

    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: Some(storage.clone()),
        plugin_registry_repo: None,
        prompt_cache_observations: storage.clone(),
    });

    let aead = Arc::new(AeadService::from_master_key([33; 32]));
    let oauth_cfg = Arc::new(AnthropicOAuthConfig {
        client_id: "unused-by-this-test".to_owned(),
        auth_url: Url::parse("http://unused.invalid/authorize").expect("auth url"),
        token_url: Url::parse("http://unused.invalid/token").expect("token url"),
        redirect_uri: Url::parse("http://unused.invalid/callback").expect("redirect url"),
        scopes: vec!["messages".to_owned()],
    });
    let runtime = ExtismRuntime::new();
    let config = cc_lb_config::Config::default();
    let view = build_dynamic_view(
        stores.as_ref(),
        oauth_cfg.as_ref(),
        aead.clone(),
        None,
        0,
        &runtime,
        dir.path(),
        Arc::new(SubscriptionQuotaCache::new()),
        1800,
        &config,
    )
    .await
    .expect("dynamic view builds");

    let captured: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
    let recording = Arc::new(RecordingDispatcher {
        captured: captured.clone(),
    }) as Arc<dyn UpstreamDispatch>;
    let view = DynamicViewBuilder::from_view(&view)
        .dispatcher(recording)
        .build();
    let holder = Arc::new(DynamicViewHolder::new(view));

    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "test-principal".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
            None,
        )),
        holder,
        LifecycleConfig::default(),
    );

    let response = lifecycle
        .handle(message_request())
        .await
        .expect("lifecycle response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();

    let urls = captured.lock().expect("captured lock").clone();
    assert_eq!(
        urls.len(),
        1,
        "expected exactly one upstream dispatch; got {urls:?} status={status} body={body:?}",
    );
    let url = &urls[0];
    assert_eq!(
        url.host_str(),
        Some("target.invalid"),
        "Extism shape plugin must receive upstream_base_url override via the v2 wire envelope. \
         Plugin emitted Anthropic fallback URL ({url}) which means \
         ShapeRequestV2.upstream_base_url was None - the host failed to populate it from \
         Upstream::AnthropicDirect.base_url. \
         status={status} body={body:?}",
    );
    assert_eq!(
        url.path(),
        "/v1/messages",
        "dispatched path must be /v1/messages, got {url}",
    );

    let _ = target;
}

async fn attach_shape_plugin(storage: &Storage, principal_id: Uuid) -> Result<(), String> {
    let wat = shape_wat_with_marker(
        br#""upstream_base_url":"http://target.invalid/""#,
        &shape_response_json("http://target.invalid/v1/messages"),
        &shape_response_json("https://api.anthropic.com/v1/messages"),
    );
    let bytes = wat::parse_str(&wat).map_err(|err| format!("wat parses: {err}"))?;
    let sha = sha256(&bytes);
    let (entry, _) = PluginRegistryStore::persist_wasm_upload(
        storage,
        WasmBlob {
            sha256: sha,
            size_bytes: bytes.len() as u64,
            bytes,
            parse_validated_at_unix_secs: now_secs(),
        },
        WasmRegistryEntryInput {
            name: "test-shape-base-url".to_owned(),
            original_filename: "test-shape-base-url.wasm".to_owned(),
            label: None,
            uploaded_at_unix_secs: now_secs(),
            uploaded_by_admin_id: Uuid::new_v4(),
            wire_version: 2,
            supported_slots: Vec::new(),
        },
    )
    .await
    .map_err(|err| format!("persist_wasm_upload: {err}"))?;
    PluginRegistryStore::insert_chain_entry(
        storage,
        PluginChainEntryInput {
            principal_id,
            slot: PluginSlot::Shape,
            order: 0,
            wasm_registry_id: entry.id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            wire_version: Some(2),
        },
    )
    .await
    .map_err(|err| format!("insert_chain_entry: {err}"))?;
    Ok(())
}

fn shape_response_json(url: &str) -> String {
    json!({
        "_v": 1,
        "url": url,
        "method": "POST",
        "headers": [],
        "body_base64": BASE64.encode(b"{}"),
    })
    .to_string()
}

fn shape_wat_with_marker(marker: &[u8], success_output: &str, fallback_output: &str) -> String {
    let success_helper = bytes_helper("success_out", success_output.as_bytes());
    let fallback_helper = bytes_helper("fallback_out", fallback_output.as_bytes());
    let mut comparisons = String::new();
    for (offset, byte) in marker.iter().enumerate() {
        comparisons.push_str(&format!(
            "      (if (i32.ne (call $input_load_u8 (i64.add (local.get $i) (i64.const {offset}))) (i32.const {byte}))\n        (then (local.set $matched (i32.const 0))))\n"
        ));
    }
    let marker_len = marker.len();
    let success_len = success_output.len();
    let fallback_len = fallback_output.len();
    format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (import "extism:host/env" "input_length" (func $input_length (result i64)))
  (import "extism:host/env" "input_load_u8" (func $input_load_u8 (param i64) (result i32)))
{success_helper}
{fallback_helper}
  (func $contains_marker (result i32)
    (local $input_len i64)
    (local $i i64)
    (local $matched i32)
    (local.set $input_len (call $input_length))
    (if (i64.lt_u (local.get $input_len) (i64.const {marker_len}))
      (then (return (i32.const 0))))
    (loop $scan
      (local.set $matched (i32.const 1))
{comparisons}
      (if (local.get $matched)
        (then (return (i32.const 1))))
      (local.set $i (i64.add (local.get $i) (i64.const 1)))
      (br_if $scan (i64.le_u (i64.add (local.get $i) (i64.const {marker_len})) (local.get $input_len))))
    (i32.const 0))
  (func (export "shape") (result i32)
    (local $out i64)
    (local $out_len i64)
    (local.set $out (call $fallback_out))
    (local.set $out_len (i64.const {fallback_len}))
    (if (call $contains_marker)
      (then
        (local.set $out (call $success_out))
        (local.set $out_len (i64.const {success_len}))))
    (call $output_set (local.get $out) (local.get $out_len))
    (i32.const 0))
)
"#
    )
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
        len = bytes.len()
    )
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

struct RecordingDispatcher {
    captured: Arc<Mutex<Vec<Url>>>,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.captured
            .lock()
            .expect("captured lock")
            .push(request.url().clone());
        let body = Body::new(Full::from(Bytes::from_static(
            br#"{"type":"message","content":[]}"#,
        )));
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(body)
            .map_err(|err| DispatchError::RequestBuild {
                reason: err.to_string(),
            })
    }
}

fn message_request() -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .expect("request builds")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
