use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bytes::Bytes;
use cc_lb_plugin_api::{
    AuthnError, AuthnOutcome, DialectError, Principal, PrincipalQuotas, RequestContext,
    RetryDecision, RouteDecision, RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, Upstream,
    UpstreamDialect, UpstreamError,
};
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;

use crate::{PluginCallError, PluginSlot, SignerFactoryResolver};

#[derive(Clone)]
pub(crate) struct ExtismAuthnPlugin {
    slot: Arc<PluginSlot>,
    signer_factory_resolver: Option<SignerFactoryResolver>,
}

impl ExtismAuthnPlugin {
    pub(crate) fn new(
        slot: Arc<PluginSlot>,
        signer_factory_resolver: Option<SignerFactoryResolver>,
    ) -> Self {
        Self {
            slot,
            signer_factory_resolver,
        }
    }
}

#[async_trait]
impl cc_lb_plugin_api::AuthnPlugin for ExtismAuthnPlugin {
    async fn authenticate(&self, ctx: &RequestContext) -> Result<AuthnOutcome, AuthnError> {
        let response = self
            .slot
            .call_value_async(
                "authenticate",
                json!({
                    "_version": 1,
                    "request": RequestContextWire::from(ctx),
                }),
            )
            .await
            .map_err(authn_runtime_error)?;
        let response: AuthnResponse = parse_versioned(response).map_err(authn_runtime_message)?;
        let signer_state = response.signer_state.unwrap_or(Value::Null);
        let signer_factory = response
            .signer_factory_ref
            .as_deref()
            .and_then(|factory_ref| {
                self.signer_factory_resolver
                    .as_ref()
                    .and_then(|resolver| resolver(factory_ref, &response.principal, &signer_state))
            })
            .unwrap_or_else(|| {
                Arc::new(ExtismSignerFactory::new(self.slot.clone(), signer_state))
                    as Arc<dyn SignerFactory>
            });
        Ok(AuthnOutcome {
            principal: response.principal,
            signer_factory,
            quotas: response.quotas.into(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct ExtismRouterPlugin {
    slot: Arc<PluginSlot>,
}

impl ExtismRouterPlugin {
    pub(crate) fn new(slot: Arc<PluginSlot>) -> Self {
        Self { slot }
    }
}

impl RouterPlugin for ExtismRouterPlugin {
    fn route(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
    ) -> Result<RouteDecision, RouteError> {
        let response = self
            .slot
            .call_value_sync(
                "route",
                json!({
                    "_version": 1,
                    "request": RequestContextWire::from(ctx),
                    "principal": principal,
                }),
            )
            .map_err(route_runtime_error)?;
        let response: RouteResponse = parse_versioned(response).map_err(route_runtime_message)?;
        let dialect: Box<dyn UpstreamDialect> = match response.dialect.unwrap_or_default() {
            PluginBinding::SelfPlugin => Box::new(ExtismDialectPlugin::new(self.slot.clone())),
        };
        Ok(RouteDecision {
            upstream: response.upstream,
            dialect,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ExtismDialectPlugin {
    slot: Arc<PluginSlot>,
}

impl ExtismDialectPlugin {
    pub(crate) fn new(slot: Arc<PluginSlot>) -> Self {
        Self { slot }
    }
}

impl UpstreamDialect for ExtismDialectPlugin {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let response = self
            .slot
            .call_value_sync(
                "shape",
                json!({
                    "_version": 1,
                    "request": RequestContextWire::from(ctx),
                    "upstream": upstream,
                    "principal": principal,
                }),
            )
            .map_err(dialect_runtime_error)?;
        let response: ShapeResponse = parse_versioned(response).map_err(dialect_runtime_message)?;
        let url =
            Url::parse(&response.url).map_err(|source| DialectError::InvalidUrl { source })?;
        let method = response.method.parse::<Method>().map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid method: {source}"),
            }
        })?;
        let headers = headers_from_wire(response.headers)
            .map_err(|reason| DialectError::UnsupportedRequest { reason })?;
        let body = BASE64.decode(response.body_base64).map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid body_base64: {source}"),
            }
        })?;
        Ok(builder.shaped_request(url, method, headers, Bytes::from(body)))
    }

    fn normalize_error(&self, status: StatusCode, body: &Bytes) -> Option<Bytes> {
        let response = self
            .slot
            .call_value_sync(
                "normalize_error",
                json!({
                    "_version": 1,
                    "status": status.as_u16(),
                    "body_base64": BASE64.encode(body),
                }),
            )
            .ok()?;
        let response: NormalizeErrorResponse = parse_versioned(response).ok()?;
        response
            .body_base64
            .and_then(|body| BASE64.decode(body).ok())
            .map(Bytes::from)
    }
}

#[derive(Clone)]
pub(crate) struct ExtismSignerFactory {
    slot: Arc<PluginSlot>,
    factory_state: Value,
}

impl ExtismSignerFactory {
    pub(crate) fn new(slot: Arc<PluginSlot>, factory_state: Value) -> Self {
        Self {
            slot,
            factory_state,
        }
    }
}

#[async_trait]
impl SignerFactory for ExtismSignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let response = self
            .slot
            .call_value_async(
                "build_signer",
                json!({
                    "_version": 1,
                    "upstream": upstream,
                    "factory_state": self.factory_state,
                }),
            )
            .await
            .map_err(signer_runtime_error)?;
        let response: BuildSignerResponse =
            parse_versioned(response).map_err(signer_runtime_message)?;
        Ok(Arc::new(ExtismSigner {
            slot: self.slot.clone(),
            signer_state: response.signer_state.unwrap_or(Value::Null),
        }))
    }
}

pub(crate) struct ExtismSigner {
    slot: Arc<PluginSlot>,
    signer_state: Value,
}

#[async_trait]
impl Signer for ExtismSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let response = self
            .slot
            .call_value_async(
                "sign",
                json!({
                    "_version": 1,
                    "shaped": ShapedRequestWire::from(&shaped),
                    "signer_state": self.signer_state,
                }),
            )
            .await
            .map_err(signer_runtime_error)?;
        let response: SignResponse = parse_versioned(response).map_err(signer_runtime_message)?;
        if let Some(url) = response.url {
            shaped.set_url(
                Url::parse(&url).map_err(|source| SignerError::SigningFailed {
                    reason: format!("plugin returned invalid url: {source}"),
                })?,
            );
        }
        if let Some(method) = response.method {
            shaped.set_method(method.parse::<Method>().map_err(|source| {
                SignerError::SigningFailed {
                    reason: format!("plugin returned invalid method: {source}"),
                }
            })?);
        }
        if let Some(headers) = response.headers {
            let headers = headers_from_wire(headers)
                .map_err(|reason| SignerError::SigningFailed { reason })?;
            shaped.headers_mut().clear();
            for (name, value) in headers {
                if let Some(name) = name {
                    shaped.headers_mut().append(name, value);
                }
            }
        }
        if let Some(body_base64) = response.body_base64 {
            let body = BASE64
                .decode(body_base64)
                .map_err(|source| SignerError::SigningFailed {
                    reason: format!("plugin returned invalid body_base64: {source}"),
                })?;
            shaped.set_body(Bytes::from(body));
        }
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, err: &UpstreamError) -> RetryDecision {
        let response = self
            .slot
            .call_value_async(
                "on_unauthorized",
                json!({
                    "_version": 1,
                    "error": UpstreamErrorWire::from(err),
                    "signer_state": self.signer_state,
                }),
            )
            .await;
        let Ok(response) = response else {
            return RetryDecision::Fail;
        };
        let Ok(response) = parse_versioned::<UnauthorizedResponse>(response) else {
            return RetryDecision::Fail;
        };
        match response.decision.as_str() {
            "refresh" => RetryDecision::Refresh {
                new_signer: Arc::new(ExtismSigner {
                    slot: self.slot.clone(),
                    signer_state: response
                        .signer_state
                        .unwrap_or_else(|| self.signer_state.clone()),
                }),
            },
            _ => RetryDecision::Fail,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RequestContextWire {
    request_id: String,
    headers: Vec<HeaderWire>,
    method: String,
    path: String,
    query: Option<String>,
    body_base64: String,
}

impl From<&RequestContext> for RequestContextWire {
    fn from(ctx: &RequestContext) -> Self {
        Self {
            request_id: ctx.request_id.clone(),
            headers: headers_to_wire(&ctx.downstream_headers),
            method: ctx.method.as_str().to_owned(),
            path: ctx.path.clone(),
            query: ctx.query.clone(),
            body_base64: BASE64.encode(&ctx.body_bytes),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ShapedRequestWire {
    url: String,
    method: String,
    headers: Vec<HeaderWire>,
    body_base64: String,
}

impl From<&ShapedRequest> for ShapedRequestWire {
    fn from(shaped: &ShapedRequest) -> Self {
        Self {
            url: shaped.url().to_string(),
            method: shaped.method().as_str().to_owned(),
            headers: headers_to_wire(shaped.headers()),
            body_base64: BASE64.encode(shaped.body()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HeaderWire {
    name: String,
    value_base64: String,
}

#[derive(Clone, Debug, Deserialize)]
struct AuthnResponse {
    principal: Principal,
    quotas: PrincipalQuotasWire,
    #[serde(default)]
    signer_factory_ref: Option<String>,
    #[serde(default)]
    signer_state: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct PrincipalQuotasWire {
    requests_per_window: u64,
    input_tokens_per_window: u64,
    output_tokens_per_window: u64,
    window_ms: u64,
    #[serde(default)]
    allowed_models: Vec<String>,
}

impl From<PrincipalQuotasWire> for PrincipalQuotas {
    fn from(value: PrincipalQuotasWire) -> Self {
        Self {
            requests_per_window: value.requests_per_window,
            input_tokens_per_window: value.input_tokens_per_window,
            output_tokens_per_window: value.output_tokens_per_window,
            window: Duration::from_millis(value.window_ms),
            allowed_models: value.allowed_models,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RouteResponse {
    upstream: Upstream,
    #[serde(default)]
    dialect: Option<PluginBinding>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum PluginBinding {
    #[default]
    #[serde(rename = "self")]
    SelfPlugin,
}

#[derive(Clone, Debug, Deserialize)]
struct ShapeResponse {
    url: String,
    method: String,
    #[serde(default)]
    headers: Vec<HeaderWire>,
    #[serde(default)]
    body_base64: String,
}

#[derive(Clone, Debug, Deserialize)]
struct NormalizeErrorResponse {
    body_base64: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct BuildSignerResponse {
    #[serde(default)]
    signer_state: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct SignResponse {
    url: Option<String>,
    method: Option<String>,
    headers: Option<Vec<HeaderWire>>,
    body_base64: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct UnauthorizedResponse {
    decision: String,
    signer_state: Option<Value>,
}

#[derive(Clone, Debug, Serialize)]
struct UpstreamErrorWire {
    status: u16,
    body_base64: Option<String>,
    category: &'static str,
}

impl From<&UpstreamError> for UpstreamErrorWire {
    fn from(value: &UpstreamError) -> Self {
        match value {
            UpstreamError::Unauthorized { status, body } => Self {
                status: status.as_u16(),
                body_base64: body.as_ref().map(|body| BASE64.encode(body)),
                category: "unauthorized",
            },
            UpstreamError::Retryable { status, body } => Self {
                status: status.as_u16(),
                body_base64: body.as_ref().map(|body| BASE64.encode(body)),
                category: "retryable",
            },
            UpstreamError::Failed { status, body } => Self {
                status: status.as_u16(),
                body_base64: body.as_ref().map(|body| BASE64.encode(body)),
                category: "failed",
            },
        }
    }
}

pub(crate) fn headers_to_wire(headers: &HeaderMap) -> Vec<HeaderWire> {
    headers
        .iter()
        .map(|(name, value)| HeaderWire {
            name: name.as_str().to_owned(),
            value_base64: BASE64.encode(value.as_bytes()),
        })
        .collect()
}

fn headers_from_wire(headers: Vec<HeaderWire>) -> Result<HeaderMap, String> {
    let mut out = HeaderMap::new();
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|source| format!("invalid header name {}: {source}", header.name))?;
        let value = BASE64.decode(header.value_base64).map_err(|source| {
            format!("invalid header value base64 for {}: {source}", header.name)
        })?;
        let value = HeaderValue::from_bytes(&value)
            .map_err(|source| format!("invalid header value for {}: {source}", header.name))?;
        out.append(name, value);
    }
    Ok(out)
}

pub(crate) fn parse_versioned<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, String> {
    match value.get("_version").and_then(Value::as_u64) {
        Some(1) => serde_json::from_value(value).map_err(|source| source.to_string()),
        Some(version) => Err(format!("unsupported plugin envelope version: {version}")),
        None => Err("missing plugin envelope _version".to_owned()),
    }
}

fn authn_runtime_error(source: PluginCallError) -> AuthnError {
    AuthnError::Runtime {
        reason: source.to_string(),
    }
}

fn authn_runtime_message(reason: String) -> AuthnError {
    AuthnError::Runtime { reason }
}

fn route_runtime_error(source: PluginCallError) -> RouteError {
    RouteError::Runtime {
        reason: source.to_string(),
    }
}

fn route_runtime_message(reason: String) -> RouteError {
    RouteError::Runtime { reason }
}

fn dialect_runtime_error(source: PluginCallError) -> DialectError {
    DialectError::UnsupportedRequest {
        reason: source.to_string(),
    }
}

fn dialect_runtime_message(reason: String) -> DialectError {
    DialectError::UnsupportedRequest { reason }
}

fn signer_runtime_error(source: PluginCallError) -> SignerError {
    SignerError::SigningFailed {
        reason: source.to_string(),
    }
}

fn signer_runtime_message(reason: String) -> SignerError {
    SignerError::SigningFailed { reason }
}
