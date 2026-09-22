use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_domain::{Principal, Upstream};

use crate::transform::{ResponseTransformHook, SseEventTransformHook};
use crate::{
    DialectError, DialectShapeContext, RetryDecision, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, SignerError, SigningCapability, UpstreamError,
};

/// Upstream dialect boundary for request shaping and error normalization.
pub trait UpstreamDialect: Send + Sync {
    /// Shapes a downstream Anthropic-compatible request for the selected upstream.
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError>;

    /// Buffered response transform hook carried by this dialect, if any.
    fn response_transform_hook(&self) -> Option<&dyn ResponseTransformHook> {
        None
    }

    /// Per-event SSE response transform hook carried by this dialect, if any.
    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        None
    }
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
    async fn on_unauthorized(&self, error: &UpstreamError) -> RetryDecision;
}

/// Factory that builds upstream-specific signers.
#[async_trait]
pub trait SignerFactory: Send + Sync {
    /// Builds a signer for the selected upstream.
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError>;
}

/// Factory extension that binds signer construction to the router-selected upstream.
pub trait ApiKeyAwareSignerFactory: Send + Sync {
    /// Returns a signer factory for the router-selected upstream. Upstream
    /// credentials come from storage, never from the caller.
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory>;
}
