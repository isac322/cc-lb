use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_plugin_api::{
    PluginManifest, Principal, PrincipalKind, RequestContext, RuntimeError, SignerFactory,
    Upstream, shape_request, sign_request,
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSignerFactory, AnthropicOAuthSignerFactoryWithLazyRefresh, LazyRefreshHandle,
};
use cc_lb_storage_api::{PluginSlot, StorageError, UpstreamRecord, WasmRegistryEntry};
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::Full;
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::dynamic_view_builder::{Stores, bridged_metadata, materialize_wasm};
use crate::refresh::LazyRefresher;
use crate::warmup::request::{WARMUP_MAX_TOKENS, WARMUP_MODEL, WarmupHttpClient};

pub struct WarmupDispatchOutcome {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

pub struct WarmupDialectDispatchParams<'a> {
    pub runtime: &'a ExtismRuntime,
    pub stores: &'a Stores,
    pub data_dir: &'a Path,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Arc<LazyRefresher>,
    pub upstream: &'a UpstreamRecord,
    pub http: &'a WarmupHttpClient,
    pub clock: cc_lb_core::ClockHandle,
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
        slot: PluginSlot,
    },
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("wasm materialize failed: {0}")]
    Materialize(String),
    #[error("plugin instantiate failed: {0}")]
    Instantiate(#[from] RuntimeError),
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
    let slot = PluginSlot::Shape;
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
        wire_version: plugin_ref.wire_version,
        config: plugin_ref.config.clone(),
        metadata: bridged_metadata(
            params.stores.plugin_registry_repo.as_ref(),
            registry_entry.sha256,
        )
        .await,
    };

    let synth_name = format!("__warmup__{}", params.upstream.id);
    let (dialect, staged) =
        params
            .runtime
            .instantiate_dialect_for_principal(&synth_name, &manifest.name, &manifest)?;
    params.runtime.commit_staged(vec![staged])?;

    let body_json = json!({
        "model": WARMUP_MODEL,
        "max_tokens": WARMUP_MAX_TOKENS,
        "messages": [{"role": "user", "content": "."}],
    });
    let ctx = RequestContext {
        request_id: Uuid::new_v4().to_string(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from(serde_json::to_vec(&body_json)?),
        cache_breakpoints: Vec::new(),
        canonical_model_id: WARMUP_MODEL.to_owned(),
    };
    let principal = Principal {
        id: params.upstream.id.to_string(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let upstream_api = Upstream::AnthropicDirect {
        base_url: params.upstream.base_url.clone(),
    };

    let shaped = shape_request(dialect.as_ref(), &ctx, &upstream_api, &principal)
        .map_err(|error| WarmupDispatchError::Shape(error.to_string()))?;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        params.stores.upstreams.clone(),
        params.aead,
        params.upstream.name.clone(),
        params.clock,
    )
    .allow_disabled_upstream(true);
    let refresh_handle: Arc<dyn LazyRefreshHandle> = params.lazy_refresher;
    let factory_with_refresh = AnthropicOAuthSignerFactoryWithLazyRefresh::new(
        factory,
        refresh_handle,
        params.upstream.id,
    );
    let signer = factory_with_refresh
        .build(&upstream_api)
        .await
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

fn registry_entry_unsupported_slot(registry_entry: &WasmRegistryEntry, slot: PluginSlot) -> bool {
    !registry_entry.is_builtin
        && !registry_entry.supported_slots.is_empty()
        && !registry_entry.supported_slots.contains(&slot)
}
