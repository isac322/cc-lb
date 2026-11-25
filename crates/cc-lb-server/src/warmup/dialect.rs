use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_plugin_wire::schema::ProviderFamily;
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime, WasmtimeRuntimeError,
};

use crate::wasm_host::WasmtimeUpstreamDialect;
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSignerFactory, AnthropicOAuthSignerFactoryWithLazyRefresh, LazyRefreshHandle,
};
use cc_lb_storage_api::{
    PluginSlotKind, StorageError, UpstreamKind, UpstreamRecord, WarmupLane, WasmRegistryEntry,
};
use cc_lb_upstream::{
    DialectShapeContext, SignerFactory, UpstreamDialect, shape_request, sign_request,
};
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::dynamic_view_builder::{Stores, read_verified_wasm, upstream_identity};
use crate::refresh::LazyRefresher;
use crate::warmup::request::{
    ProviderWarmupDispatchParams, WARMUP_MAX_TOKENS, WARMUP_MODEL, WARMUP_RESPONSE_TIMEOUT,
    WarmupHttpClient, build_provider_warmup_request,
};

pub struct WarmupDispatchOutcome {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

pub struct WarmupDialectDispatchParams<'a> {
    pub runtime: &'a WasmtimeRuntime,
    pub stores: &'a Stores,
    pub data_dir: &'a Path,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Option<Arc<LazyRefresher>>,
    pub upstream: &'a UpstreamRecord,
    pub lane: WarmupLane,
    pub http: &'a WarmupHttpClient,
    pub clock: cc_lb_clock::ClockHandle,
    pub config: &'a Config,
}

#[derive(Debug, Error)]
pub enum WarmupDispatchError {
    #[error("upstream has no warmup_dialect_plugin configured")]
    MissingPlugin,
    #[error("wasm registry entry {0} not found")]
    RegistryNotFound(Uuid),
    #[error(
        "wasm registry entry {wasm_registry_id} ({plugin_name}) does not support slot {slot:?}"
    )]
    RegistryUnsupportedSlot {
        wasm_registry_id: Uuid,
        plugin_name: String,
        slot: PluginSlotKind,
    },
    #[error("wasm registry entry {0} does not export the provider's warmup shape hook")]
    RegistryUnsupportedProvider(Uuid),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("wasm materialize failed: {0}")]
    Materialize(String),
    #[error("wasmtime plugin register failed: {0}")]
    Register(#[from] WasmtimeRuntimeError),
    #[error("warmup body serialize failed: {0}")]
    BodySerialize(#[from] serde_json::Error),
    #[error("shape failed: {0}")]
    Shape(String),
    #[error("signer failed: {0}")]
    Signer(String),
    #[error("request build failed: {0}")]
    RequestBuild(#[from] http::Error),
    #[error("http dispatch failed: {0}")]
    Http(String),
}

impl WarmupDispatchError {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Storage(_) | Self::Http(_) | Self::Materialize(_)
        )
    }
}

pub(crate) fn warmup_dialect_slot_key(sha256: [u8; 32], plugin_name: &str) -> RuntimeSlotKey {
    use std::fmt::Write as _;

    let mut synth = String::with_capacity("__warmup_sha256__".len() + 64);
    synth.push_str("__warmup_sha256__");
    for byte in sha256 {
        let _ = write!(&mut synth, "{byte:02x}");
    }
    RuntimeSlotKey::new(synth, plugin_name.to_owned(), RuntimeSlotRole::Shape)
}

pub async fn dispatch_warmup_with_dialect(
    params: WarmupDialectDispatchParams<'_>,
) -> Result<WarmupDispatchOutcome, WarmupDispatchError> {
    if !WarmupLane::lanes_for_kind(params.upstream.kind).contains(&params.lane) {
        return Err(WarmupDispatchError::Shape(format!(
            "warmup lane {} is not available for {} upstreams",
            params.lane.as_str(),
            params.upstream.kind.as_str()
        )));
    }
    let plugin_ref = params
        .upstream
        .warmup_dialect_plugin
        .as_ref()
        .ok_or(WarmupDispatchError::MissingPlugin)?;

    let registry_entry = params
        .stores
        .plugin_registry
        .get_registry_entry_by_id(plugin_ref.wasm_registry_id)
        .await?
        .ok_or(WarmupDispatchError::RegistryNotFound(
            plugin_ref.wasm_registry_id,
        ))?;
    let slot = PluginSlotKind::Shape;
    if registry_entry_unsupported_slot(&registry_entry, slot) {
        return Err(WarmupDispatchError::RegistryUnsupportedSlot {
            wasm_registry_id: registry_entry.id,
            plugin_name: registry_entry.name.clone(),
            slot,
        });
    }

    let slot_key = warmup_dialect_slot_key(registry_entry.sha256, &registry_entry.name);
    let slot = match params.runtime.get_slot(&slot_key) {
        Some(slot) => slot,
        None => {
            let (_wasm_path, wasm_bytes) =
                read_verified_wasm(params.stores, params.data_dir, registry_entry.sha256)
                    .await
                    .map_err(|error| WarmupDispatchError::Materialize(error.to_string()))?;
            params
                .runtime
                .register_shape(slot_key, registry_entry.name.clone(), &wasm_bytes)?
        }
    };
    let provider = match params.upstream.kind {
        UpstreamKind::OpenaiOauth => ProviderFamily::Openai,
        UpstreamKind::GeminiOauth => ProviderFamily::Gemini,
        _ => ProviderFamily::Anthropic,
    };
    let mut provider_bundle = if provider != ProviderFamily::Anthropic {
        Some(
            params
                .upstream
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| WarmupDispatchError::Signer("missing oauth credentials".to_owned()))?
                .decrypt(params.aead.as_ref(), params.upstream.id.as_bytes())
                .map_err(|_| WarmupDispatchError::Signer("oauth decrypt failed".to_owned()))?,
        )
    } else {
        None
    };
    let identity = upstream_identity(params.upstream, provider_bundle.as_ref());
    let dispatch = WasmPluginWireDispatch::from_slot(slot, params.runtime.config_arc());
    let native = cc_lb_engine::lifecycle::native_route_dialect(params.upstream);
    let selected = dispatch.for_provider(provider);
    if provider != ProviderFamily::Anthropic
        && selected
            .as_ref()
            .is_none_or(|dispatch| dispatch.shape_wire_version().is_none())
    {
        return Err(WarmupDispatchError::RegistryUnsupportedProvider(
            registry_entry.id,
        ));
    }
    let dialect: Arc<dyn UpstreamDialect> = match selected {
        Some(dispatch) => Arc::new(
            WasmtimeUpstreamDialect::new(Arc::new(dispatch))
                .with_native_request(native)
                .with_upstream_identity(identity),
        ),
        None => native,
    };
    if provider != ProviderFamily::Anthropic {
        let mut bundle = provider_bundle
            .take()
            .ok_or_else(|| WarmupDispatchError::Signer("missing oauth credentials".to_owned()))?;
        if !bundle.never_refresh
            && bundle.expires_at_unix_secs.saturating_sub(60)
                <= cc_lb_clock::unix_secs(params.clock.now())
            && let Some(refresher) = params.lazy_refresher.as_ref()
        {
            refresher
                .refresh_one(params.upstream.id)
                .await
                .map_err(|_| {
                    WarmupDispatchError::Signer("warmup OAuth refresh failed".to_owned())
                })?;
            let refreshed = params
                .stores
                .upstreams
                .get_by_id(params.upstream.id)
                .await?
                .ok_or_else(|| {
                    WarmupDispatchError::Signer(
                        "upstream missing after warmup OAuth refresh".to_owned(),
                    )
                })?;
            bundle = refreshed
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| {
                    WarmupDispatchError::Signer(
                        "missing oauth credentials after refresh".to_owned(),
                    )
                })?
                .decrypt(params.aead.as_ref(), params.upstream.id.as_bytes())
                .map_err(|_| WarmupDispatchError::Signer("oauth decrypt failed".to_owned()))?;
        }
        let (request, signer) = build_provider_warmup_request(
            &ProviderWarmupDispatchParams {
                http: params.http,
                upstream: params.upstream,
                lane: params.lane,
                aead: params.aead.as_ref(),
                config: params.config,
                clock: params.clock,
            },
            &bundle,
            dialect.as_ref(),
        )
        .await
        .map_err(WarmupDispatchError::Shape)?;
        return send_and_drain_warmup_request(params.http, request, signer.as_ref()).await;
    }

    let body_json = json!({
        "model": WARMUP_MODEL,
        "max_tokens": WARMUP_MAX_TOKENS,
        "messages": [{"role": "user", "content": "."}],
    });
    let context = DialectShapeContext {
        request_id: Uuid::new_v4().to_string(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from(serde_json::to_vec(&body_json)?),
    };
    let principal = Principal {
        id: params.upstream.id.to_string(),
        kind: PrincipalKind::ApiKey,
    };
    let upstream_api = Upstream::AnthropicDirect {
        base_url: params.upstream.base_url.clone(),
    };

    let shaped = shape_request(dialect.as_ref(), &context, &upstream_api, &principal)
        .map_err(|error| WarmupDispatchError::Shape(error.to_string()))?;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        params.stores.upstreams.clone(),
        params.aead,
        params.upstream.name.clone(),
        params.clock,
    )
    .allow_disabled_upstream(true);
    let signer = if let Some(lazy_refresher) = params.lazy_refresher {
        let refresh_handle: Arc<dyn LazyRefreshHandle> = lazy_refresher;
        AnthropicOAuthSignerFactoryWithLazyRefresh::new(factory, refresh_handle, params.upstream.id)
            .with_shared_refresh_state(
                params
                    .stores
                    .vertex_state
                    .shared_signer_state
                    .refresh_locks
                    .clone(),
                params
                    .stores
                    .vertex_state
                    .shared_signer_state
                    .anthropic_forced_generations
                    .clone(),
            )
            .build(&upstream_api)
            .await
    } else {
        factory.build(&upstream_api).await
    }
    .map_err(|error| WarmupDispatchError::Signer(error.to_string()))?;

    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .map_err(|error| WarmupDispatchError::Signer(error.to_string()))?;

    let mut request_builder = Request::builder()
        .method(signed.method().clone())
        .uri(signed.url().as_str());
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    let request = request_builder.body(Full::new(signed.body().clone()))?;

    send_and_drain_warmup_request(params.http, request, signer.as_ref()).await
}

async fn send_and_drain_warmup_request(
    http: &WarmupHttpClient,
    request: Request<Full<Bytes>>,
    signer: &dyn cc_lb_upstream::Signer,
) -> Result<WarmupDispatchOutcome, WarmupDispatchError> {
    send_and_drain_warmup_request_with_timeout(http, request, signer, WARMUP_RESPONSE_TIMEOUT).await
}

async fn send_and_drain_warmup_request_with_timeout(
    http: &WarmupHttpClient,
    request: Request<Full<Bytes>>,
    signer: &dyn cc_lb_upstream::Signer,
    timeout: std::time::Duration,
) -> Result<WarmupDispatchOutcome, WarmupDispatchError> {
    tokio::time::timeout(timeout, async {
        let response = http.request(request).await.map_err(|error| {
            WarmupDispatchError::Http(signer.scrub_secrets(&error.to_string()).into_owned())
        })?;
        let outcome = WarmupDispatchOutcome {
            status: response.status(),
            headers: response.headers().clone(),
        };
        let mut body = response.into_body();
        while let Some(frame) = body.frame().await {
            frame.map_err(|error| {
                WarmupDispatchError::Http(signer.scrub_secrets(&error.to_string()).into_owned())
            })?;
        }
        Ok(outcome)
    })
    .await
    .map_err(|_| WarmupDispatchError::Http("warmup response timed out".to_owned()))?
}

fn registry_entry_unsupported_slot(
    registry_entry: &WasmRegistryEntry,
    slot: PluginSlotKind,
) -> bool {
    !registry_entry.is_builtin && !registry_entry.supported_slots.contains(&slot)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cc_lb_upstream::{
        RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SigningCapability,
        UpstreamError,
    };
    use http::Request;
    use http_body_util::Full;
    use hyper_rustls::HttpsConnectorBuilder;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    use super::{WarmupDispatchError, send_and_drain_warmup_request_with_timeout};

    struct TestScrubSigner {
        secret: String,
    }

    #[async_trait]
    impl Signer for TestScrubSigner {
        async fn sign(
            &self,
            request: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(request, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }

        fn scrub_secrets<'a>(&self, message: &'a str) -> Cow<'a, str> {
            if message.contains(&self.secret) {
                Cow::Owned(message.replace(&self.secret, "[REDACTED]"))
            } else {
                Cow::Borrowed(message)
            }
        }
    }

    fn test_http_client() -> crate::warmup::request::WarmupHttpClient {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        Client::builder(TokioExecutor::new()).build(connector)
    }

    #[tokio::test]
    async fn warmup_request_helper_drains_body_and_enforces_single_timeout() {
        let drained = Arc::new(AtomicBool::new(false));
        let drained_flag = drained.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
                .await
                .expect("write headers and first chunk");
            drained_flag.store(true, Ordering::SeqCst);
            stream
                .write_all(b"6\r\n world\r\n0\r\n\r\n")
                .await
                .expect("write final chunk");
        });

        let req = Request::post(format!("http://{addr}/v1/messages"))
            .body(Full::new(Bytes::new()))
            .expect("req");
        let signer = TestScrubSigner {
            secret: "sk-secret".to_owned(),
        };
        let outcome = send_and_drain_warmup_request_with_timeout(
            &test_http_client(),
            req,
            &signer,
            Duration::from_secs(2),
        )
        .await
        .expect("drained response succeeds");
        assert_eq!(outcome.status, http::StatusCode::OK);
        assert!(drained.load(Ordering::SeqCst));

        // Body stall must fail under the single timeout budget even after headers arrived.
        let stall_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let stall_addr = stall_listener.local_addr().expect("addr");
        let hold = Arc::new(Notify::new());
        let hold_srv = hold.clone();
        tokio::spawn(async move {
            let (mut stream, _) = stall_listener.accept().await.expect("accept");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial")
                .await
                .expect("write incomplete body");
            hold_srv.notified().await;
        });

        let stall_req = Request::post(format!("http://{stall_addr}/v1/messages"))
            .body(Full::new(Bytes::new()))
            .expect("req");
        let err = send_and_drain_warmup_request_with_timeout(
            &test_http_client(),
            stall_req,
            &signer,
            Duration::from_millis(40),
        )
        .await
        .err()
        .expect("stalled body times out under single budget");
        hold.notify_one();
        assert!(matches!(err, WarmupDispatchError::Http(msg) if msg.contains("timed out")));
    }

    #[tokio::test]
    async fn warmup_request_helper_scrubs_secrets_on_body_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nINVALID\r\n")
                .await
                .expect("write invalid chunk");
        });

        let req = Request::post(format!("http://{addr}/v1/messages"))
            .body(Full::new(Bytes::new()))
            .expect("req");
        let signer = TestScrubSigner {
            secret: "chunk".to_owned(),
        };
        let err = send_and_drain_warmup_request_with_timeout(
            &test_http_client(),
            req,
            &signer,
            Duration::from_secs(2),
        )
        .await
        .err()
        .expect("corrupt chunked body fails");
        match err {
            WarmupDispatchError::Http(msg) => {
                assert!(
                    !msg.contains("chunk"),
                    "unscrubbed error leaked token: {msg}"
                );
            }
            other => panic!("expected Http error, got {other:?}"),
        }
    }
}
