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
use cc_lb_storage_api::{StorageError, UpstreamRecord};
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

#[derive(Debug, Error)]
pub enum WarmupDispatchError {
    #[error("upstream has no warmup_dialect_plugin configured")]
    MissingPlugin,
    #[error("wasm registry entry {0} not found")]
    RegistryNotFound(Uuid),
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

pub async fn dispatch_warmup_with_dialect(
    runtime: &ExtismRuntime,
    stores: &Stores,
    data_dir: &Path,
    aead: Arc<AeadService>,
    lazy_refresher: Arc<LazyRefresher>,
    upstream: &UpstreamRecord,
    http: &WarmupHttpClient,
) -> Result<WarmupDispatchOutcome, WarmupDispatchError> {
    let plugin_ref = upstream
        .warmup_dialect_plugin
        .as_ref()
        .ok_or(WarmupDispatchError::MissingPlugin)?;

    let registry_entry = stores
        .plugin_registry
        .get_registry_entry_by_id(plugin_ref.wasm_registry_id)
        .await?
        .ok_or(WarmupDispatchError::RegistryNotFound(
            plugin_ref.wasm_registry_id,
        ))?;

    let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256)
        .await
        .map_err(|error| WarmupDispatchError::Materialize(error.to_string()))?;
    let manifest = PluginManifest {
        name: registry_entry.name,
        artifact: wasm_path.to_string_lossy().into_owned(),
        wire_version: plugin_ref.wire_version,
        config: plugin_ref.config.clone(),
        metadata: bridged_metadata(stores.plugin_registry_repo.as_ref(), registry_entry.sha256)
            .await,
    };

    let synth_name = format!("__warmup__{}", upstream.id);
    let (dialect, staged) =
        runtime.instantiate_dialect_for_principal(&synth_name, &manifest.name, &manifest)?;
    runtime.commit_staged(vec![staged])?;

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
        id: upstream.id.to_string(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let upstream_api = Upstream::AnthropicDirect {
        base_url: upstream.base_url.clone(),
    };

    let shaped = shape_request(dialect.as_ref(), &ctx, &upstream_api, &principal)
        .map_err(|error| WarmupDispatchError::Shape(error.to_string()))?;

    let factory = AnthropicOAuthSignerFactory::for_upstream_name(
        stores.upstreams.clone(),
        aead,
        upstream.name.clone(),
    );
    let refresh_handle: Arc<dyn LazyRefreshHandle> = lazy_refresher;
    let factory_with_refresh =
        AnthropicOAuthSignerFactoryWithLazyRefresh::new(factory, refresh_handle, upstream.id);
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

    let response = http
        .request(request)
        .await
        .map_err(|error| WarmupDispatchError::Http(error.to_string()))?;

    Ok(WarmupDispatchOutcome {
        status: response.status(),
        headers: response.headers().clone(),
    })
}
