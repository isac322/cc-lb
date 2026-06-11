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
}

/// Upstream record kind exposed to router plugins for candidate selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    /// Anthropic API-key upstream.
    AnthropicApiKey,
    /// Anthropic OAuth upstream.
    AnthropicOauth,
}

impl UpstreamKind {
    /// Returns the stable snake_case wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicApiKey => "anthropic_api_key",
            Self::AnthropicOauth => "anthropic_oauth",
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

/// Freshness state for subscription quota data exposed to router plugins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionQuotaDataState {
    /// Data is inside the configured routing freshness window.
    Fresh,
    /// Data exists but is older than the configured routing freshness window.
    Stale,
    /// No usable subscription quota data exists for the candidate/window.
    Missing,
}

/// Latest subscription quota snapshot for one candidate/window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaCandidateSnapshot {
    /// Subscription quota window label.
    pub window: String,
    /// Freshness state for this window snapshot.
    pub state: SubscriptionQuotaDataState,
    /// Data source label, such as header, api, or merged.
    pub source: Option<String>,
    /// Provider-reported utilization fraction.
    pub utilization: Option<f64>,
    /// Provider-reported quota status.
    pub status: Option<String>,
    /// Provider reset timestamp in Unix seconds.
    pub resets_at_unix_secs: Option<u64>,
    /// Whether the provider reported a crossed warning threshold.
    pub surpassed_threshold: Option<bool>,
    /// Representative claim used for provenance/debugging.
    pub representative_claim: Option<String>,
    /// Provider reason the quota window is disabled.
    pub disabled_reason: Option<String>,
    /// Whether provider extra usage is enabled.
    pub extra_usage_enabled: Option<bool>,
    /// Provider extra-usage monthly credit limit.
    pub extra_usage_monthly_limit: Option<f64>,
    /// Provider extra-usage consumed credits.
    pub extra_usage_used_credits: Option<f64>,
    /// Observation timestamp in Unix milliseconds.
    pub observed_at_unix_millis: Option<u64>,
    /// Configured maximum age before this snapshot becomes stale.
    pub max_staleness_secs: u64,
}

/// Prompt cache TTL class: immutable after entry creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum TtlClass {
    /// 5-minute TTL cache entry.
    #[default]
    Ephemeral5m,
    /// 1-hour TTL cache entry.
    Ephemeral1h,
}

/// Origin of a cache breakpoint: whether explicitly requested or auto-inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum BreakpointOrigin {
    /// Explicit cache breakpoint requested by the user or application.
    Explicit,
    /// Auto-inferred cache breakpoint from proxy analysis.
    AutoCacheInferred,
}

/// Source of a cache breakpoint within the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum CacheBreakpointSource {
    /// Breakpoint from tools in the request.
    Tools,
    /// Breakpoint from system content.
    System,
    /// Breakpoint from message content.
    Message,
}

/// Cache breakpoint position in the request, for prompt cache optimization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct CacheBreakpoint {
    /// Block index of the breakpoint.
    pub block_index: u32,
    /// Source of the breakpoint.
    pub source: CacheBreakpointSource,
    /// Dot-separated path (e.g., "messages.0.content.1") within request.
    pub path: String,
    /// Message index if this breakpoint is within a message, None for system content.
    pub message_index: Option<u32>,
    /// Content hash of the prefix up to this breakpoint.
    pub prefix_hash: String,
    /// Token count of the prefix up to this breakpoint.
    pub prefix_token_count: u64,
    /// Requested TTL class for this breakpoint.
    pub requested_ttl: TtlClass,
    /// Origin of this breakpoint.
    pub origin: BreakpointOrigin,
}

/// Warm cache entry eligible for reuse in upstream requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct WarmCacheEntry {
    /// Content hash of the cached prefix.
    pub prefix_hash: String,
    /// Unix timestamp in seconds when this entry expires.
    pub expires_at_unix_secs: u64,
    /// TTL class of this cache entry.
    pub ttl_class: TtlClass,
    /// Last observed usage time in Unix seconds.
    pub last_observed_at_unix_secs: u64,
}

/// Cache utility prediction for routing decisions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct CacheScore {
    /// Predicted input tokens that can be read from cache.
    pub predicted_cache_read_tokens: u32,
    /// Predicted input tokens written to 5-minute cache.
    pub predicted_cache_creation_tokens_5m: u32,
    /// Predicted input tokens written to 1-hour cache.
    pub predicted_cache_creation_tokens_1h: u32,
    /// Predicted input tokens not read from cache.
    pub predicted_uncached_input_tokens: u32,
    /// Predicted Unix timestamp when the cache entry will expire, None if permanent.
    pub predicted_expires_at_unix_secs: Option<u64>,
    /// Index of the matched cache breakpoint if one was selected, None otherwise.
    pub matched_breakpoint_index: Option<u32>,
    /// Confidence score for this prediction (0.0 to 1.0).
    pub confidence: f32,
    /// Optional explanation for ambiguous or low-confidence predictions.
    pub ambiguity_reason: Option<String>,
}

/// Available upstream candidate for routing decisions.
///
/// The router receives a list of available upstream candidates sorted by
/// `upstream_id` in ascending order (Uuid byte order). This stable ordering
/// allows plugins to implement deterministic routing algorithms.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpstreamCandidate {
    /// Stable upstream identifier.
    pub upstream_id: Uuid,
    /// Operator-facing upstream name.
    pub name: String,
    /// Upstream kind used to select compatible routing strategies.
    pub kind: UpstreamKind,
    /// Latest rate-limit observations for this candidate.
    pub observed_rate_limits: Vec<RateLimitObservation>,
    /// Latest subscription quota snapshots for this candidate.
    #[serde(default)]
    pub subscription_quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
    /// Unix timestamp in seconds for the candidate observation snapshot.
    pub observed_at_unix_secs: u64,
    /// Predicted cache utility for this candidate, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_score: Option<CacheScore>,
    /// Resolved upstream base URL. Populated by the host so that v2 plugins
    /// that self-reference for shape can construct the dispatch URL against
    /// the configured upstream instead of hardcoding api.anthropic.com.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
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
    /// Cache breakpoints extracted from the request for prompt cache optimization.
    pub cache_breakpoints: Vec<CacheBreakpoint>,
    /// Canonical model identifier resolved from the request.
    pub canonical_model_id: String,
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
        /// Cache write token count reported by the upstream, when known.
        cache_creation_input_tokens: Option<u64>,
        /// Cache read token count reported by the upstream, when known.
        cache_read_input_tokens: Option<u64>,
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
    /// Preferred plugin wire envelope version. Omitted manifests default to v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
    /// Runtime configuration provided to the plugin.
    pub config: serde_json::Value,
    /// Runtime-specific metadata not interpreted by the core API contract.
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

/// Maximum bytes allowed for a routing stage reason.
pub const STAGE_REASON_MAX_BYTES: usize = 256;
/// Maximum bytes allowed for an encoded routing trace.
pub const ROUTING_TRACE_MAX_BYTES: usize = 4 * 1024;
/// Maximum routing stages recorded in a trace.
pub const STAGE_COUNT_MAX: usize = 16;
/// Maximum number of internal errors recorded on a request event.
pub const INTERNAL_ERRORS_MAX_COUNT: usize = 8;
/// Maximum bytes allowed for an internal error message.
pub const INTERNAL_ERROR_MESSAGE_MAX_BYTES: usize = 256;

/// Reason for a passthrough routing decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PassthroughCause {
    /// Upstream is healthy and available.
    HealthyUpstream,
    /// No alternative upstream available.
    NoAlternative,
    /// Plugin returned passthrough decision.
    PluginDecision,
}

/// Per-candidate filter decision reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerCandidateReason {
    /// Upstream candidate identifier.
    pub upstream_id: Uuid,
    /// Whether the candidate survived the filter.
    pub kept: bool,
    /// Human-readable candidate-specific reason.
    pub reason: String,
}

/// Strategy for selecting a terminal upstream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TerminalStrategy {
    /// Select first available upstream.
    #[default]
    FirstPick,
    /// Select a router plugin at random.
    Random,
}

/// Decision made at a single routing stage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageDecision {
    /// Name of the routing stage.
    pub stage_name: String,
    /// Upstream candidate identifier if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    /// Reason for this stage's decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Time spent executing this routing stage, in microseconds.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub duration_us: u64,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Terminal routing decision selecting an upstream.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalDecision {
    /// Selected upstream identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    /// Strategy used for selection.
    pub strategy: TerminalStrategy,
}

/// Complete routing trace for a request through all decision stages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RoutingTrace {
    /// Sequence of stage decisions made during routing.
    #[serde(default)]
    pub stages: Vec<StageDecision>,
    /// Final terminal routing decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<TerminalDecision>,
}

/// Stage where an internal error occurred.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InternalErrorStage {
    /// Authentication stage.
    Authn,
    /// Routing stage.
    #[default]
    Router,
    /// Router filter stage.
    RouterFilter,
    /// Request shaping stage.
    Shape,
    /// Request signing stage.
    Signer,
    /// Request relay stage.
    Relay,
}

/// Kind of internal error that occurred.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InternalErrorKind {
    /// Plugin crashed or returned an error.
    #[default]
    PluginError,
    /// Plugin returned invalid output.
    InvalidOutput,
    /// Plugin trapped during execution.
    Trap,
    /// Configuration error.
    ConfigError,
    /// Timeout error.
    Timeout,
    /// Resource unavailable.
    Unavailable,
    /// Other intentional internal error classification.
    Other,
}

/// Internal error information with stage and kind details.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalError {
    /// Stage where the error occurred.
    pub stage: InternalErrorStage,
    /// Kind of error.
    pub kind: InternalErrorKind,
    /// Optional error message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
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
        let upstreams = vec![Upstream::AnthropicDirect];

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
        assert_eq!(manifest.wire_version, None);
        assert!(manifest.metadata.is_empty());

        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "name": "cache-aware",
            "artifact": "plugin.wasm",
            "wire_version": 2,
            "config": {}
        }))
        .unwrap();
        assert_eq!(manifest.wire_version, Some(2));
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
                cache_creation_input_tokens: Some(6),
                cache_read_input_tokens: Some(7),
                duration_ms: 8,
            },
            ObserveEvent::Error {
                code: "E".to_owned(),
                message: "redacted".to_owned(),
                source: "plugin".to_owned(),
            },
        ];

        assert_eq!(events, events.clone());
    }

    #[test]
    fn cache_types_roundtrip() {
        let ttl_class = TtlClass::Ephemeral1h;
        let json = serde_json::to_string(&ttl_class).unwrap();
        let decoded: TtlClass = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ttl_class);

        let origin = BreakpointOrigin::AutoCacheInferred;
        let json = serde_json::to_string(&origin).unwrap();
        let decoded: BreakpointOrigin = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, origin);

        let source = CacheBreakpointSource::System;
        let json = serde_json::to_string(&source).unwrap();
        let decoded: CacheBreakpointSource = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, source);

        let breakpoint = CacheBreakpoint {
            block_index: 0,
            source: CacheBreakpointSource::Message,
            path: "messages.0.content.1".to_owned(),
            message_index: Some(0),
            prefix_hash: "abc123".to_owned(),
            prefix_token_count: 100,
            requested_ttl: TtlClass::Ephemeral5m,
            origin: BreakpointOrigin::Explicit,
        };
        let json = serde_json::to_string(&breakpoint).unwrap();
        let decoded: CacheBreakpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, breakpoint);

        let warm_entry = WarmCacheEntry {
            prefix_hash: "def456".to_owned(),
            expires_at_unix_secs: 1700000000,
            ttl_class: TtlClass::Ephemeral1h,
            last_observed_at_unix_secs: 1699999000,
        };
        let json = serde_json::to_string(&warm_entry).unwrap();
        let decoded: WarmCacheEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, warm_entry);

        let cache_score = CacheScore {
            predicted_cache_read_tokens: 50,
            predicted_cache_creation_tokens_5m: 100,
            predicted_cache_creation_tokens_1h: 200,
            predicted_uncached_input_tokens: 25,
            predicted_expires_at_unix_secs: Some(1700000000),
            matched_breakpoint_index: Some(0),
            confidence: 0.95,
            ambiguity_reason: None,
        };
        let json = serde_json::to_string(&cache_score).unwrap();
        let decoded: CacheScore = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded.predicted_cache_read_tokens,
            cache_score.predicted_cache_read_tokens
        );
        assert_eq!(
            decoded.predicted_cache_creation_tokens_5m,
            cache_score.predicted_cache_creation_tokens_5m
        );
        assert_eq!(
            decoded.predicted_cache_creation_tokens_1h,
            cache_score.predicted_cache_creation_tokens_1h
        );
        assert_eq!(
            decoded.predicted_uncached_input_tokens,
            cache_score.predicted_uncached_input_tokens
        );
        assert_eq!(
            decoded.predicted_expires_at_unix_secs,
            cache_score.predicted_expires_at_unix_secs
        );
        assert_eq!(
            decoded.matched_breakpoint_index,
            cache_score.matched_breakpoint_index
        );
        assert!((decoded.confidence - cache_score.confidence).abs() < 0.0001);
        assert_eq!(decoded.ambiguity_reason, cache_score.ambiguity_reason);
    }

    #[test]
    fn upstream_candidate_cache_score_roundtrip() {
        let candidate_no_cache = UpstreamCandidate {
            upstream_id: Uuid::new_v4(),
            name: "test-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: vec![],
            subscription_quotas: vec![],
            observed_at_unix_secs: 1700000000,
            cache_score: None,
            base_url: None,
        };
        let json = serde_json::to_string(&candidate_no_cache).unwrap();
        let decoded: UpstreamCandidate = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.upstream_id, candidate_no_cache.upstream_id);
        assert_eq!(decoded.name, candidate_no_cache.name);
        assert_eq!(decoded.cache_score, None);

        let cache_score = CacheScore {
            predicted_cache_read_tokens: 50,
            predicted_cache_creation_tokens_5m: 100,
            predicted_cache_creation_tokens_1h: 200,
            predicted_uncached_input_tokens: 25,
            predicted_expires_at_unix_secs: Some(1700000000),
            matched_breakpoint_index: Some(0),
            confidence: 0.95,
            ambiguity_reason: None,
        };
        let candidate_with_cache = UpstreamCandidate {
            upstream_id: Uuid::new_v4(),
            name: "test-upstream-cached".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: vec![],
            subscription_quotas: vec![],
            observed_at_unix_secs: 1700000000,
            cache_score: Some(cache_score),
            base_url: None,
        };
        let json = serde_json::to_string(&candidate_with_cache).unwrap();
        let decoded: UpstreamCandidate = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.upstream_id, candidate_with_cache.upstream_id);
        assert_eq!(decoded.name, candidate_with_cache.name);
        assert!(decoded.cache_score.is_some());
        assert_eq!(decoded.cache_score.unwrap().predicted_cache_read_tokens, 50);
    }

    #[test]
    fn request_context_cache_fields_roundtrip() {
        let ctx_empty = RequestContext {
            request_id: "req-1".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::new(),
            cache_breakpoints: Vec::new(),
            canonical_model_id: String::new(),
        };
        assert_eq!(ctx_empty.cache_breakpoints.len(), 0);
        assert_eq!(ctx_empty.canonical_model_id, "");

        let breakpoint = CacheBreakpoint {
            block_index: 1,
            source: CacheBreakpointSource::Message,
            path: "messages.0.content.0".to_owned(),
            message_index: Some(0),
            prefix_hash: "hash123".to_owned(),
            prefix_token_count: 150,
            requested_ttl: TtlClass::Ephemeral1h,
            origin: BreakpointOrigin::Explicit,
        };
        let ctx_populated = RequestContext {
            request_id: "req-2".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: Some("param=value".to_owned()),
            body_bytes: Bytes::from_static(b"test"),
            cache_breakpoints: vec![breakpoint],
            canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        };
        assert_eq!(ctx_populated.cache_breakpoints.len(), 1);
        assert_eq!(
            ctx_populated.canonical_model_id,
            "claude-sonnet-4-5-20250929"
        );
    }

    #[test]
    fn upstream_candidate_deserialize_without_cache_score_field() {
        let json = r#"{
            "upstream_id": "00000000-0000-0000-0000-000000000001",
            "name": "legacy-upstream",
            "kind": "anthropic_api_key",
            "observed_rate_limits": [],
            "subscription_quotas": [],
            "observed_at_unix_secs": 1700000000
        }"#;
        let candidate: UpstreamCandidate = serde_json::from_str(json).unwrap();
        assert!(candidate.cache_score.is_none());
        assert_eq!(candidate.name, "legacy-upstream");
    }

    #[test]
    fn request_context_cache_breakpoints_default_on_missing_fields() {
        let ctx = RequestContext {
            request_id: "test".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::GET,
            path: "/test".to_owned(),
            query: None,
            body_bytes: Bytes::new(),
            cache_breakpoints: Vec::new(),
            canonical_model_id: String::new(),
        };
        assert!(ctx.cache_breakpoints.is_empty());
        assert!(ctx.canonical_model_id.is_empty());
    }
}
