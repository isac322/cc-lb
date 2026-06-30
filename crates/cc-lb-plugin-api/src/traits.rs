//! Object-safe plugin traits for each proxy lifecycle boundary.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use uuid::Uuid;

use crate::errors::{
    DialectError, ObservabilityError, RouteError, RuntimeError, SignerError, UpstreamError,
};
use crate::types::{
    ObserveEvent, PerCandidateReason, PluginManifest, Principal, RequestContext, RetryDecision,
    RouteDecision, ShapedRequest, ShapedRequestBuilder, SignedRequest, SigningCapability, SlotKey,
    Upstream, UpstreamCandidate,
};

/// Filter plugin output containing upstream selection results and per-candidate reasons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterOutput {
    /// Upstream IDs that passed the filter.
    pub kept_upstream_ids: Vec<Uuid>,
    /// Human-readable reason for the filtering decision.
    pub reason: String,
    /// Per-candidate filtering reasons.
    pub per_candidate_reasons: Vec<PerCandidateReason>,
}

/// Filter plugin errors returned by [`FilterPlugin`].
#[derive(Debug)]
pub enum FilterError {
    /// Runtime error during filtering.
    Runtime {
        /// Redacted runtime error reason.
        reason: String,
    },
    /// Trap error (plugin crashed or returned invalid state).
    Trap {
        /// Redacted trap error reason.
        reason: String,
    },
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterError::Runtime { reason } => write!(f, "filter runtime error: {}", reason),
            FilterError::Trap { reason } => write!(f, "filter trap error: {}", reason),
        }
    }
}

impl std::error::Error for FilterError {}

/// Filter plugin boundary for upstream candidate filtering and decision-making.
///
/// Filter plugins evaluate requests against custom criteria and decide which upstream
/// candidates are acceptable. They have access to the request context, authenticated
/// principal, and list of available upstream candidates, and return a filtered set
/// of acceptable upstreams along with per-candidate reasoning.
pub trait FilterPlugin: Send + Sync {
    /// Filters upstream candidates based on request and principal.
    ///
    /// Returns a [`FilterOutput`] containing the kept upstream IDs and per-candidate
    /// reasons, or a [`FilterError`] if filtering fails.
    fn filter(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError>;

    /// Returns the stable plugin identifier.
    fn plugin_id(&self) -> Uuid;

    /// Returns the human-readable plugin name.
    fn plugin_name(&self) -> &str;

    /// Returns the composite key identifying this filter slot — the
    /// principal it is bound to and the plugin name. Callers use this
    /// to address cache eviction, hot reload, and per-slot metrics
    /// uniformly across runtime implementations.
    ///
    /// The default impl returns a global slot key derived from
    /// [`Self::plugin_name`]. Runtime impls bound to a specific
    /// principal (e.g. wasmtime) override this to return the
    /// principal × plugin pair they were instantiated with.
    fn slot_key(&self) -> SlotKey {
        SlotKey::global(self.plugin_name())
    }
}

/// Router plugin boundary.
#[deprecated(since = "0.2.0", note = "Use FilterPlugin via wire v3")]
pub trait RouterPlugin: Send + Sync {
    /// Selects the upstream and dialect for an authenticated request.
    ///
    /// The `candidates` parameter provides the list of available upstreams that can be
    /// selected. Candidates are sorted by `upstream_id` ascending (Uuid byte order) to enable
    /// deterministic routing algorithms.
    fn route(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError>;
}

/// Upstream dialect boundary for request shaping and error normalization.
pub trait UpstreamDialect: Send + Sync {
    /// Shapes a downstream Anthropic-compatible request for the selected upstream.
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError>;

    /// Normalizes an upstream error body to Anthropic error shape when possible.
    fn normalize_error(&self, status: StatusCode, body: &Bytes) -> Option<Bytes>;
}

/// Signer boundary for applying credentials to shaped requests.
#[async_trait]
pub trait Signer: Send + Sync {
    /// Consumes a shaped request and returns a sealed signed request.
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError>;

    /// Handles an unauthorized upstream response, optionally refreshing credentials.
    async fn on_unauthorized(&self, err: &UpstreamError) -> RetryDecision;
}

/// Factory that builds upstream-specific signers.
#[async_trait]
pub trait SignerFactory: Send + Sync {
    /// Builds a signer for the selected upstream.
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError>;
}

/// Factory extension that binds signer construction to the router-selected upstream.
pub trait ApiKeyAwareSignerFactory: Send + Sync {
    /// Returns a signer factory using the downstream API key and router-selected upstream name.
    fn with_router_choice(
        &self,
        api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory>;
}

/// Non-blocking observability hook boundary.
pub trait ObservabilityHook: Send + Sync {
    /// Observes a lifecycle event.
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError>;
}

/// Runtime abstraction for concrete plugin systems (currently wasmtime).
pub trait PluginRuntime: Send + Sync {
    /// Instantiates a router plugin.
    #[allow(deprecated)]
    fn instantiate_router(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn RouterPlugin>, RuntimeError>;

    /// Instantiates an upstream dialect plugin.
    fn instantiate_dialect(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn UpstreamDialect>, RuntimeError>;

    /// Instantiates a signer factory plugin.
    ///
    /// Signer plugin extension is no longer supported — cc-lb-server uses
    /// built-in `AnthropicKeySigner` / `AnthropicOAuthSigner` directly.
    /// New runtime impls should leave the default `unimplemented!()` body
    /// in place. The trait method is retained as a deprecated shim so
    /// that out-of-tree runtimes do not see a hard trait-shape break;
    /// it is scheduled for removal in the next semver-major bump.
    fn instantiate_signer_factory(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn SignerFactory>, RuntimeError> {
        unimplemented!(
            "signer plugin extension dropped; use built-in \
             AnthropicKeySigner / AnthropicOAuthSigner instead"
        )
    }

    /// Instantiates an observability hook plugin.
    fn instantiate_observability(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn ObservabilityHook>, RuntimeError>;
}
