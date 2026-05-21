//! Object-safe plugin traits for each proxy lifecycle boundary.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;

use crate::errors::{
    AuthnError, DialectError, ObservabilityError, RouteError, RuntimeError, SignerError,
    UpstreamError,
};
use crate::types::{
    AuthnOutcome, ObserveEvent, PluginManifest, Principal, RequestContext, RetryDecision,
    RouteDecision, ShapedRequest, ShapedRequestBuilder, SignedRequest, SigningCapability, Upstream,
};

/// Authentication plugin boundary.
#[async_trait]
pub trait AuthnPlugin: Send + Sync {
    /// Authenticates a parsed downstream request.
    async fn authenticate(&self, ctx: &RequestContext) -> Result<AuthnOutcome, AuthnError>;
}

/// Router plugin boundary.
pub trait RouterPlugin: Send + Sync {
    /// Selects the upstream and dialect for an authenticated request.
    fn route(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
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

/// Non-blocking observability hook boundary.
pub trait ObservabilityHook: Send + Sync {
    /// Observes a lifecycle event.
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError>;
}

/// Runtime abstraction for concrete plugin systems such as Extism.
pub trait PluginRuntime: Send + Sync {
    /// Instantiates an authentication plugin.
    fn instantiate(&self, manifest: &PluginManifest) -> Result<Arc<dyn AuthnPlugin>, RuntimeError>;

    /// Instantiates a router plugin.
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
    fn instantiate_signer_factory(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn SignerFactory>, RuntimeError>;

    /// Instantiates an observability hook plugin.
    fn instantiate_observability(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn ObservabilityHook>, RuntimeError>;
}
