use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, RetryDecision, RouteDecision, RouteError,
    RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, Upstream, UpstreamCandidate, UpstreamDialect,
    UpstreamError,
};
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::{PluginCallError, PluginSlot};

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
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        let response = self
            .slot
            .call_value_sync(
                "route",
                json!({
                    "_version": 1,
                    "request": RequestContextWire::from(ctx),
                    "principal": principal,
                    "candidates": candidates,
                }),
            )
            .map_err(route_runtime_error)?;
        let response: RouteResponse = parse_versioned(response).map_err(route_runtime_message)?;
        let dialect: Arc<dyn UpstreamDialect> = match response.dialect.unwrap_or_default() {
            PluginBinding::SelfPlugin => Arc::new(ExtismDialectPlugin::new(self.slot.clone())),
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
        let headers = headers_from_wire(response.headers).map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: source.to_string(),
            }
        })?;
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
            let headers =
                headers_from_wire(headers).map_err(|source| SignerError::SigningFailed {
                    reason: source.to_string(),
                })?;
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

fn headers_from_wire(headers: Vec<HeaderWire>) -> Result<HeaderMap, WireError> {
    let mut out = HeaderMap::new();
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|source| {
            WireError::InvalidHeaderName {
                name: header.name.clone(),
                source,
            }
        })?;
        let value = BASE64.decode(header.value_base64).map_err(|source| {
            WireError::InvalidHeaderValueBase64 {
                name: header.name.clone(),
                source,
            }
        })?;
        let value =
            HeaderValue::from_bytes(&value).map_err(|source| WireError::InvalidHeaderValue {
                name: header.name.clone(),
                source,
            })?;
        out.append(name, value);
    }
    Ok(out)
}

pub(crate) fn parse_versioned<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, WireError> {
    match value.get("_version").and_then(Value::as_u64) {
        Some(1) => {
            serde_json::from_value(value).map_err(|source| WireError::Deserialize { source })
        }
        Some(version) => Err(WireError::UnsupportedVersion { version }),
        None => Err(WireError::MissingVersion),
    }
}

#[derive(Debug)]
pub(crate) enum WireError {
    Deserialize {
        source: serde_json::Error,
    },
    UnsupportedVersion {
        version: u64,
    },
    MissingVersion,
    InvalidHeaderName {
        name: String,
        source: http::header::InvalidHeaderName,
    },
    InvalidHeaderValueBase64 {
        name: String,
        source: base64::DecodeError,
    },
    InvalidHeaderValue {
        name: String,
        source: http::header::InvalidHeaderValue,
    },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deserialize { source } => write!(f, "plugin envelope decode failed: {source}"),
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported plugin envelope version: {version}")
            }
            Self::MissingVersion => write!(f, "missing plugin envelope _version"),
            Self::InvalidHeaderName { name, source } => {
                write!(f, "invalid header name {name}: {source}")
            }
            Self::InvalidHeaderValueBase64 { name, source } => {
                write!(f, "invalid header value base64 for {name}: {source}")
            }
            Self::InvalidHeaderValue { name, source } => {
                write!(f, "invalid header value for {name}: {source}")
            }
        }
    }
}

impl std::error::Error for WireError {}

fn route_runtime_error(source: PluginCallError) -> RouteError {
    RouteError::Runtime {
        reason: source.to_string(),
    }
}

fn route_runtime_message(source: WireError) -> RouteError {
    RouteError::Runtime {
        reason: source.to_string(),
    }
}

fn dialect_runtime_error(source: PluginCallError) -> DialectError {
    DialectError::UnsupportedRequest {
        reason: source.to_string(),
    }
}

fn dialect_runtime_message(source: WireError) -> DialectError {
    DialectError::UnsupportedRequest {
        reason: source.to_string(),
    }
}

fn signer_runtime_error(source: PluginCallError) -> SignerError {
    SignerError::SigningFailed {
        reason: source.to_string(),
    }
}

fn signer_runtime_message(source: WireError) -> SignerError {
    SignerError::SigningFailed {
        reason: source.to_string(),
    }
}
