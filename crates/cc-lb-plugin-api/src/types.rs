//! Shared public data types for plugin boundaries.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::errors::{DialectError, SignerError};
use crate::traits::{Signer, SignerFactory, UpstreamDialect};

/// Authenticated caller identity used for quota, audit, and routing decisions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    /// Stable principal identifier, unique within the proxy deployment.
    pub id: String,
    /// Principal category inferred by the authentication plugin.
    pub kind: PrincipalKind,
    /// Plugin-provided claims available to router and observability layers.
    pub claims: serde_json::Map<String, serde_json::Value>,
}

/// Principal categories supported by first-party and custom auth plugins.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Principal authenticated by an Anthropic-compatible API key.
    ApiKey,
    /// Principal authenticated as an OAuth subject.
    OAuthSubject,
    /// Principal authenticated by an internal key managed by cc-lb.
    InternalKey,
    /// Principal authenticated through a workload identity mechanism.
    WorkloadIdentity,
    /// Principal authenticated by a Claude subscription bearer token.
    SubscriptionBearer,
}

/// Upstream backends supported by the proxy routing contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Upstream {
    /// Direct Anthropic API endpoint.
    AnthropicDirect,
    /// AWS Bedrock runtime endpoint for Anthropic models.
    BedrockRuntime {
        /// AWS region containing the Bedrock runtime endpoint.
        region: String,
    },
    /// AWS Bedrock mantle Anthropic-compatible endpoint.
    BedrockMantle {
        /// AWS region containing the mantle endpoint.
        region: String,
    },
    /// Google Vertex AI Anthropic publisher endpoint.
    Vertex {
        /// GCP project identifier.
        project: String,
        /// Vertex AI region.
        region: String,
    },
    /// Custom gateway that already speaks the Anthropic Messages wire shape.
    CustomAnthropicSpec {
        /// Base URL for the custom Anthropic-compatible gateway.
        base_url: Url,
    },
}

/// Credential strategy expected by a selected upstream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStrategy {
    /// Anthropic-style `x-api-key` signing.
    ApiKey,
    /// Anthropic-style OAuth bearer signing.
    OAuth,
    /// AWS Signature Version 4 signing.
    AwsSigV4,
    /// GCP OAuth bearer signing.
    GcpOAuth,
    /// Forward an internal credential supplied by upstream configuration.
    InternalForwarded,
}

/// Parsed downstream request data passed into plugins.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Stable request identifier used for logs, audit rows, and upstream traceability.
    pub request_id: String,
    /// Downstream request headers after hop-by-hop stripping.
    pub downstream_headers: HeaderMap,
    /// Downstream HTTP method.
    pub method: Method,
    /// Downstream request path, such as `/v1/messages`.
    pub path: String,
    /// Raw downstream query string without the leading `?`.
    pub query: Option<String>,
    /// Buffered request body bytes, required by SigV4 and other signing schemes.
    pub body_bytes: Bytes,
}

/// Request produced by an upstream dialect before credentials are applied.
#[derive(Clone, Debug)]
pub struct ShapedRequest {
    url: Url,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
    _seal: crate::private::Seal,
}

impl ShapedRequest {
    /// Returns the destination URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Replaces the destination URL.
    pub fn set_url(&mut self, url: Url) {
        self.url = url;
    }

    /// Returns the HTTP method.
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Replaces the HTTP method.
    pub fn set_method(&mut self, method: Method) {
        self.method = method;
    }

    /// Returns the request headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns mutable request headers for signer-owned changes.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Returns the request body.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Replaces the request body.
    pub fn set_body(&mut self, body: Bytes) {
        self.body = body;
    }
}

/// Dialect-facing capability used to construct shaped requests.
///
/// Values of this type are created only by [`shape_request`]. Dialect
/// implementations receive a mutable reference while their
/// [`crate::UpstreamDialect::shape`] method is executing, which lets them return
/// a shaped request without exposing an unrestricted public constructor.
#[derive(Debug)]
pub struct ShapedRequestBuilder {
    _seal: crate::private::Seal,
}

impl ShapedRequestBuilder {
    /// Creates a shaped request from dialect-owned parts.
    pub fn shaped_request(
        &mut self,
        url: Url,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> ShapedRequest {
        ShapedRequest {
            url,
            method,
            headers,
            body,
            _seal: crate::private::Seal,
        }
    }
}

/// Invokes an upstream dialect with a temporary shaped-request capability.
pub fn shape_request(
    dialect: &dyn UpstreamDialect,
    ctx: &RequestContext,
    upstream: &Upstream,
    principal: &Principal,
) -> Result<ShapedRequest, DialectError> {
    let mut builder = ShapedRequestBuilder {
        _seal: crate::private::Seal,
    };
    dialect.shape(ctx, upstream, principal, &mut builder)
}

/// Request after a signer has consumed and sealed a shaped request.
#[derive(Clone, Debug)]
pub struct SignedRequest {
    url: Url,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
    _seal: crate::private::Seal,
}

impl SignedRequest {
    /// Seals an owned shaped request after the signer has applied credentials.
    pub fn from_shaped(shaped: ShapedRequest, _capability: &mut SigningCapability) -> Self {
        Self {
            url: shaped.url,
            method: shaped.method,
            headers: shaped.headers,
            body: shaped.body,
            _seal: crate::private::Seal,
        }
    }

    /// Returns the destination URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the HTTP method.
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Returns the signed request headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns the signed request body.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Consumes the signed request into relay-ready parts.
    pub fn into_parts(self) -> (Url, Method, HeaderMap, Bytes) {
        (self.url, self.method, self.headers, self.body)
    }
}

/// Signer-facing capability used to seal shaped requests.
///
/// Values of this type are created only by [`sign_request`]. Signer
/// implementations receive a mutable reference while their [`crate::Signer::sign`]
/// method is executing, which keeps arbitrary crates from sealing shaped
/// requests outside the signer boundary.
#[derive(Debug)]
pub struct SigningCapability {
    _seal: crate::private::Seal,
}

/// Invokes a signer with a temporary signing capability.
pub async fn sign_request(
    signer: &dyn Signer,
    shaped: ShapedRequest,
) -> Result<SignedRequest, SignerError> {
    let mut capability = SigningCapability {
        _seal: crate::private::Seal,
    };
    signer.sign(shaped, &mut capability).await
}

/// Router output selecting both an upstream and its dialect boundary object.
pub struct RouteDecision {
    /// Upstream selected for the request.
    pub upstream: Upstream,
    /// Dialect plugin that shapes the request for the selected upstream.
    pub dialect: Box<dyn UpstreamDialect>,
}

/// Authentication output used by routing, quota, and signer construction.
#[derive(Clone)]
pub struct AuthnOutcome {
    /// Authenticated principal.
    pub principal: Principal,
    /// Factory that builds signers for routed upstreams.
    pub signer_factory: Arc<dyn SignerFactory>,
    /// Quotas and model gates attached to the principal.
    pub quotas: PrincipalQuotas,
}

/// Per-principal quota window and model allow-list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalQuotas {
    /// Maximum request count allowed within `window`.
    pub requests_per_window: u64,
    /// Maximum input token count allowed within `window`.
    pub input_tokens_per_window: u64,
    /// Maximum output token count allowed within `window`.
    pub output_tokens_per_window: u64,
    /// Quota window duration.
    pub window: Duration,
    /// Glob-style model names allowed for this principal.
    pub allowed_models: Vec<String>,
}

/// Observability events emitted by the lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserveEvent {
    /// Downstream request has entered the proxy.
    RequestStarted {
        /// Request identifier.
        request_id: String,
        /// Downstream user-agent header, when present.
        downstream_user_agent: Option<String>,
    },
    /// Authentication completed successfully.
    AuthnComplete {
        /// Authenticated principal identifier.
        principal_id: String,
        /// Authenticated principal kind.
        kind: PrincipalKind,
    },
    /// Router selected an upstream.
    UpstreamChosen {
        /// Selected upstream.
        upstream: Upstream,
    },
    /// A batch of streamed events passed through the relay.
    Chunk {
        /// Monotonic batch index within the response stream.
        batch_index: u64,
        /// Number of SSE events in the batch.
        event_count: usize,
        /// Total bytes in the batch.
        total_bytes: usize,
    },
    /// Request finished successfully or with an upstream HTTP error.
    RequestFinished {
        /// Final HTTP status code.
        status: StatusCode,
        /// Input token count reported by the upstream, when known.
        input_tokens: Option<u64>,
        /// Output token count reported by the upstream, when known.
        output_tokens: Option<u64>,
        /// End-to-end request duration in milliseconds.
        duration_ms: u64,
    },
    /// Lifecycle or plugin error was observed.
    Error {
        /// Stable error code.
        code: String,
        /// Redacted human-readable message.
        message: String,
        /// Error source component.
        source: String,
    },
}

/// Decision returned by a signer after receiving an unauthorized upstream error.
#[derive(Clone)]
pub enum RetryDecision {
    /// Retry with a refreshed signer.
    Refresh {
        /// Signer containing refreshed credentials.
        new_signer: Arc<dyn Signer>,
    },
    /// Do not retry the request.
    Fail,
}

/// Plugin manifest passed to runtime adapters when instantiating plugins.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Plugin name from configuration.
    pub name: String,
    /// Filesystem path or runtime-specific locator for the plugin artifact.
    pub artifact: String,
    /// Runtime configuration provided to the plugin.
    pub config: serde_json::Value,
    /// Runtime-specific metadata not interpreted by the core API contract.
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}
