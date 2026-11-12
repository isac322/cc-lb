use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_plugin_wire::schema::ProviderFamily;
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime, WasmtimeRuntimeError,
};

use crate::PluginManifest;
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

use crate::dynamic_view_builder::{Stores, materialize_wasm, upstream_identity};
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

    let wasm_path = materialize_wasm(params.stores, params.data_dir, registry_entry.sha256)
        .await
        .map_err(|error| WarmupDispatchError::Materialize(error.to_string()))?;
    let manifest = PluginManifest {
        name: registry_entry.name,
        artifact: wasm_path.to_string_lossy().into_owned(),
    };

    let synth_name = format!("__warmup__{}", params.upstream.id);

    let wasm_bytes = tokio::fs::read(&manifest.artifact)
        .await
        .map_err(|error| WarmupDispatchError::Materialize(error.to_string()))?;
    let slot = params.runtime.register_shape(
        RuntimeSlotKey::new(synth_name.clone(), manifest.name.clone()),
        manifest.name.clone(),
        &wasm_bytes,
    )?;
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
        let response = tokio::time::timeout(WARMUP_RESPONSE_TIMEOUT, params.http.request(request))
            .await
            .map_err(|_| WarmupDispatchError::Http("warmup response timed out".to_owned()))?
            .map_err(|error| WarmupDispatchError::Http(error.to_string()))?;
        let outcome = WarmupDispatchOutcome {
            status: response.status(),
            headers: response.headers().clone(),
        };
        let mut body = response.into_body();
        tokio::time::timeout(WARMUP_RESPONSE_TIMEOUT, async {
            while let Some(frame) = body.frame().await {
                frame.map_err(|error| {
                    WarmupDispatchError::Http(signer.scrub_secrets(&error.to_string()).into_owned())
                })?;
            }
            Ok::<(), WarmupDispatchError>(())
        })
        .await
        .map_err(|_| WarmupDispatchError::Http("warmup response timed out".to_owned()))??;
        return Ok(outcome);
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

    let response = params
        .http
        .request(request)
        .await
        .map_err(|error| WarmupDispatchError::Http(error.to_string()))?;

    Ok(WarmupDispatchOutcome {
        status: response.status(),
        headers: response.headers().clone(),
    })
}

fn registry_entry_unsupported_slot(
    registry_entry: &WasmRegistryEntry,
    slot: PluginSlotKind,
) -> bool {
    !registry_entry.is_builtin && !registry_entry.supported_slots.contains(&slot)
}
