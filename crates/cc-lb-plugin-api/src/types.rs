//! Shared public data types for plugin boundaries.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::errors::{DialectError, SignerError};
use crate::traits::{Signer, UpstreamDialect};

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
    /// Custom gateway that already speaks the Anthropic Messages wire shape.
    CustomAnthropicSpec {
        /// Base URL for the custom Anthropic-compatible gateway.
        base_url: Url,
    },
}

/// Upstream record kind exposed to router plugins for candidate selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    /// Anthropic API-key upstream.
    AnthropicApiKey,
    /// Anthropic OAuth upstream.
    AnthropicOauth,
    /// Custom Anthropic-compatible upstream.
    Custom,
}

impl UpstreamKind {
    /// Returns the stable snake_case wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicApiKey => "anthropic_api_key",
            Self::AnthropicOauth => "anthropic_oauth",
            Self::Custom => "custom",
        }
    }
}

/// Upstream rate-limit metric kind observed from upstream responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitKind {
    /// Request-count rate limit.
    Requests,
    /// Aggregate token rate limit.
    Tokens,
    /// Input-token rate limit.
    InputTokens,
    /// Output-token rate limit.
    OutputTokens,
}

impl RateLimitKind {
    /// Returns the stable snake_case wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

/// Latest upstream rate-limit observation exposed to router plugins.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RateLimitObservation {
    /// Observed rate-limit kind.
    pub kind: RateLimitKind,
    /// Provider-defined rate-limit window label.
    pub window: String,
    /// Optional maximum quota for the window.
    pub limit: Option<u64>,
    /// Optional remaining quota for the window.
    pub remaining: Option<u64>,
    /// Optional provider reset timestamp or duration string.
    pub reset: Option<String>,
}

/// Available upstream candidate for routing decisions.
///
/// The router receives a list of available upstream candidates sorted by
/// `upstream_id` in ascending order (Uuid byte order). This stable ordering
/// allows plugins to implement deterministic routing algorithms.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpstreamCandidate {
    /// Stable upstream identifier.
    pub upstream_id: Uuid,
    /// Operator-facing upstream name.
    pub name: String,
    /// Upstream kind used to select compatible routing strategies.
    pub kind: UpstreamKind,
    /// Latest rate-limit observations for this candidate.
    pub observed_rate_limits: Vec<RateLimitObservation>,
    /// Unix timestamp in seconds for the candidate observation snapshot.
    pub observed_at_unix_secs: u64,
}

/// Credential strategy expected by a selected upstream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStrategy {
    /// Anthropic-style `x-api-key` signing.
    ApiKey,
    /// Anthropic-style OAuth bearer signing.
    OAuth,
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
    /// Stable upstream identifier selected by the router, when provided by the plugin.
    pub upstream_id: Option<Uuid>,
    /// Upstream selected for the request.
    pub upstream: Upstream,
    /// Dialect plugin that shapes the request for the selected upstream.
    pub dialect: Arc<dyn UpstreamDialect>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shaped_request_accessors_and_mutators_preserve_parts() {
        let mut builder = ShapedRequestBuilder {
            _seal: crate::private::Seal,
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-test", "one".parse().unwrap());
        let mut shaped = builder.shaped_request(
            "https://example.test/v1/messages".parse().unwrap(),
            Method::POST,
            headers,
            Bytes::from_static(b"first"),
        );

        assert_eq!(shaped.url().as_str(), "https://example.test/v1/messages");
        assert_eq!(shaped.method(), Method::POST);
        assert_eq!(shaped.headers()["x-test"], "one");
        assert_eq!(shaped.body(), &Bytes::from_static(b"first"));

        shaped.set_url("https://example.test/v1/complete".parse().unwrap());
        shaped.set_method(Method::PUT);
        shaped
            .headers_mut()
            .insert("x-test", "two".parse().unwrap());
        shaped.set_body(Bytes::from_static(b"second"));

        assert_eq!(shaped.url().path(), "/v1/complete");
        assert_eq!(shaped.method(), Method::PUT);
        assert_eq!(shaped.headers()["x-test"], "two");
        assert_eq!(shaped.body(), &Bytes::from_static(b"second"));
    }

    #[test]
    fn signed_request_exposes_and_consumes_signed_parts() {
        let mut builder = ShapedRequestBuilder {
            _seal: crate::private::Seal,
        };
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        let shaped = builder.shaped_request(
            "https://api.example.test/v1/messages".parse().unwrap(),
            Method::POST,
            headers,
            Bytes::from_static(b"{}"),
        );
        let mut capability = SigningCapability {
            _seal: crate::private::Seal,
        };

        let signed = SignedRequest::from_shaped(shaped, &mut capability);
        assert_eq!(signed.url().host_str(), Some("api.example.test"));
        assert_eq!(signed.method(), Method::POST);
        assert_eq!(signed.headers()["authorization"], "Bearer token");
        assert_eq!(signed.body(), &Bytes::from_static(b"{}"));

        let (url, method, headers, body) = signed.into_parts();
        assert_eq!(url.as_str(), "https://api.example.test/v1/messages");
        assert_eq!(method, Method::POST);
        assert_eq!(headers["authorization"], "Bearer token");
        assert_eq!(body, Bytes::from_static(b"{}"));
    }

    #[test]
    fn upstream_and_manifest_serde_round_trip() {
        let upstreams = vec![
            Upstream::AnthropicDirect,
            Upstream::CustomAnthropicSpec {
                base_url: "https://gateway.example.test".parse().unwrap(),
            },
        ];

        for upstream in upstreams {
            let json = serde_json::to_string(&upstream).unwrap();
            let decoded: Upstream = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, upstream);
        }

        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "name": "authn",
            "artifact": "plugin.wasm",
            "config": {"enabled": true}
        }))
        .unwrap();
        assert_eq!(manifest.name, "authn");
        assert!(manifest.metadata.is_empty());
    }

    #[test]
    fn public_enums_cover_all_current_variants() {
        let principal_kinds = [
            PrincipalKind::ApiKey,
            PrincipalKind::OAuthSubject,
            PrincipalKind::InternalKey,
            PrincipalKind::WorkloadIdentity,
            PrincipalKind::SubscriptionBearer,
        ];
        assert_eq!(principal_kinds.len(), 5);

        let strategies = [
            CredentialStrategy::ApiKey,
            CredentialStrategy::OAuth,
            CredentialStrategy::InternalForwarded,
        ];
        assert_eq!(strategies.len(), 3);
    }

    #[test]
    fn observe_event_variants_are_equatable() {
        let events = vec![
            ObserveEvent::RequestStarted {
                request_id: "req".to_owned(),
                downstream_user_agent: Some("ua".to_owned()),
            },
            ObserveEvent::AuthnComplete {
                principal_id: "principal".to_owned(),
                kind: PrincipalKind::InternalKey,
            },
            ObserveEvent::UpstreamChosen {
                upstream: Upstream::AnthropicDirect,
            },
            ObserveEvent::Chunk {
                batch_index: 1,
                event_count: 2,
                total_bytes: 3,
            },
            ObserveEvent::RequestFinished {
                status: StatusCode::OK,
                input_tokens: Some(4),
                output_tokens: Some(5),
                duration_ms: 6,
            },
            ObserveEvent::Error {
                code: "E".to_owned(),
                message: "redacted".to_owned(),
                source: "plugin".to_owned(),
            },
        ];

        assert_eq!(events, events.clone());
    }
}
