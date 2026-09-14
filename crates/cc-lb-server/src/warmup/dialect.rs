use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime, WasmtimeRuntimeError,
};

use crate::wasm_host::WasmtimeUpstreamDialect;
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSignerFactory, AnthropicOAuthSignerFactoryWithLazyRefresh, LazyRefreshHandle,
};
use cc_lb_storage_api::{PluginSlotKind, StorageError, UpstreamRecord, WasmRegistryEntry};
use cc_lb_upstream::{
    DialectShapeContext, SignerFactory, UpstreamDialect, shape_request, sign_request,
};
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::Full;
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::PluginManifest;
use crate::dynamic_view_builder::{Stores, materialize_wasm};
use crate::refresh::LazyRefresher;
use crate::warmup::request::{WARMUP_MAX_TOKENS, WARMUP_MODEL, WarmupHttpClient};

pub struct WarmupDispatchOutcome {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

pub struct WarmupDialectDispatchParams<'a> {
    pub runtime: &'a WasmtimeRuntime,
    pub stores: &'a Stores,
    pub data_dir: &'a Path,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Arc<LazyRefresher>,
    pub upstream: &'a UpstreamRecord,
    pub http: &'a WarmupHttpClient,
    pub clock: cc_lb_engine::ClockHandle,
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

#[async_trait::async_trait]
trait WarmupDialectHttpDispatch: Send + Sync {
    async fn dispatch(
        &self,
        request: Request<Full<Bytes>>,
    ) -> Result<WarmupDispatchOutcome, String>;
}

#[async_trait::async_trait]
impl WarmupDialectHttpDispatch for WarmupHttpClient {
    async fn dispatch(
        &self,
        request: Request<Full<Bytes>>,
    ) -> Result<WarmupDispatchOutcome, String> {
        let response = self
            .request(request)
            .await
            .map_err(|error| error.to_string())?;
        Ok(WarmupDispatchOutcome {
            status: response.status(),
            headers: response.headers().clone(),
        })
    }
}

async fn dispatch_shaped_warmup(
    dialect: &dyn UpstreamDialect,
    signer_factory: &dyn SignerFactory,
    http: &dyn WarmupDialectHttpDispatch,
    upstream: &Upstream,
    principal: &Principal,
    request_id: &str,
) -> Result<WarmupDispatchOutcome, WarmupDispatchError> {
    let body_json = json!({
        "model": WARMUP_MODEL,
        "max_tokens": WARMUP_MAX_TOKENS,
        "messages": [{"role": "user", "content": "."}],
    });
    let context = DialectShapeContext {
        request_id: request_id.to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from(serde_json::to_vec(&body_json)?),
    };
    let shaped = shape_request(dialect, &context, upstream, principal)
        .map_err(|error| WarmupDispatchError::Shape(error.to_string()))?;
    let signer = signer_factory
        .build(upstream)
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

    http.dispatch(request)
        .await
        .map_err(WarmupDispatchError::Http)
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
        pure: true,
        name: registry_entry.name,
        artifact: wasm_path.to_string_lossy().into_owned(),
        wire_version: None,
        config: plugin_ref.config.clone(),
        metadata: std::collections::BTreeMap::new(),
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
    let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        params.runtime.config_arc(),
    ));
    let dialect: Arc<dyn UpstreamDialect> = Arc::new(WasmtimeUpstreamDialect::new(dispatch));

    let principal = Principal {
        id: params.upstream.id.to_string(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let upstream_api = Upstream::AnthropicDirect {
        base_url: params.upstream.base_url.clone(),
    };
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
    let request_id = Uuid::new_v4().to_string();

    dispatch_shaped_warmup(
        dialect.as_ref(),
        &factory_with_refresh,
        params.http,
        &upstream_api,
        &principal,
        &request_id,
    )
    .await
}

fn registry_entry_unsupported_slot(
    registry_entry: &WasmRegistryEntry,
    slot: PluginSlotKind,
) -> bool {
    !registry_entry.is_builtin
        && !registry_entry.supported_slots.is_empty()
        && !registry_entry.supported_slots.contains(&slot)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use std::sync::Mutex;

    use cc_lb_upstream::{
        DialectError, RetryDecision, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer,
        SignerError, SigningCapability, UpstreamError,
    };
    use http::HeaderValue;
    use http::header::AUTHORIZATION;
    use http_body_util::BodyExt;
    use url::Url;

    use super::*;

    struct TransformingDialect;

    impl UpstreamDialect for TransformingDialect {
        fn shape(
            &self,
            context: &DialectShapeContext,
            upstream: &Upstream,
            _principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            let Upstream::AnthropicDirect {
                base_url: Some(base_url),
            } = upstream
            else {
                return Err(DialectError::UpstreamMismatch {
                    reason: "test requires an explicit Anthropic base URL".to_owned(),
                });
            };
            let mut body: serde_json::Value =
                serde_json::from_slice(&context.body_bytes).map_err(|error| {
                    DialectError::UnsupportedRequest {
                        reason: error.to_string(),
                    }
                })?;
            body["messages"][0]["content"] = json!("dialect-shaped");
            body["request_id"] = json!(context.request_id);

            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            Ok(builder.shaped_request(
                base_url.join("v1/messages")?,
                context.method.clone(),
                headers,
                Bytes::from(serde_json::to_vec(&body).map_err(|error| {
                    DialectError::UnsupportedRequest {
                        reason: error.to_string(),
                    }
                })?),
            ))
        }
    }

    struct BearerSignerFactory;

    #[async_trait::async_trait]
    impl SignerFactory for BearerSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(BearerSigner))
        }
    }

    struct BearerSigner;

    #[async_trait::async_trait]
    impl Signer for BearerSigner {
        async fn sign(
            &self,
            mut shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            shaped.headers_mut().insert(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer oauth-access-token"),
            );
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    struct CapturedRequest {
        method: Method,
        uri: http::Uri,
        headers: HeaderMap,
        body: Bytes,
    }

    #[derive(Default)]
    struct RecordingHttp {
        captured: Mutex<Option<CapturedRequest>>,
    }

    #[async_trait::async_trait]
    impl WarmupDialectHttpDispatch for RecordingHttp {
        async fn dispatch(
            &self,
            request: Request<Full<Bytes>>,
        ) -> Result<WarmupDispatchOutcome, String> {
            let (parts, body) = request.into_parts();
            let body = body
                .collect()
                .await
                .map_err(|error| error.to_string())?
                .to_bytes();
            *self.captured.lock().expect("captured request lock") = Some(CapturedRequest {
                method: parts.method,
                uri: parts.uri,
                headers: parts.headers,
                body,
            });
            Ok(WarmupDispatchOutcome {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
            })
        }
    }

    #[tokio::test]
    async fn t2__warmup_dialect_dispatch_shapes_signs_and_sends_fixed_request() {
        let http = RecordingHttp::default();
        let upstream = Upstream::AnthropicDirect {
            base_url: Some(Url::parse("https://api.example.test/").expect("test base URL")),
        };
        let principal = Principal {
            id: "warmup-principal".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };

        let outcome = dispatch_shaped_warmup(
            &TransformingDialect,
            &BearerSignerFactory,
            &http,
            &upstream,
            &principal,
            "warmup-request-fixed",
        )
        .await
        .expect("shaped and signed warmup request dispatches");

        assert_eq!(outcome.status, StatusCode::OK);
        let captured = http
            .captured
            .lock()
            .expect("captured request lock")
            .take()
            .expect("HTTP dispatch captured one request");
        assert_eq!(captured.method, Method::POST);
        assert_eq!(
            captured.uri,
            "https://api.example.test/v1/messages"
                .parse::<http::Uri>()
                .expect("expected request URI")
        );
        assert_eq!(
            captured.headers.get(AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer oauth-access-token"))
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&captured.body)
                .expect("captured body is JSON"),
            json!({
                "model": WARMUP_MODEL,
                "max_tokens": WARMUP_MAX_TOKENS,
                "messages": [{"role": "user", "content": "dialect-shaped"}],
                "request_id": "warmup-request-fixed",
            })
        );
    }
}
