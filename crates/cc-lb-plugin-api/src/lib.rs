//! Public plugin contract for the `cc-lb` proxy lifecycle.
//!
//! The core proxy owns the wire-level server loop and drives requests through
//! `parse -> authenticate -> route -> shape -> sign -> relay_stream`.
//! This crate defines the stable types and object-safe traits used at those
//! boundaries: authentication plugins identify a [`Principal`], router plugins
//! choose an [`Upstream`] and [`UpstreamDialect`], dialects build a
//! [`ShapedRequest`], signers consume it into a [`SignedRequest`], and the core
//! relays the signed request while emitting [`ObserveEvent`] values to
//! observability hooks.
//!
//! The crate deliberately uses `http`, `bytes`, and `url` types instead of
//! exposing the server's transport implementation. Request body validation,
//! semantic JSON checks, and concrete HTTP client behavior live in downstream
//! crates, not in this API contract.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod errors;
mod traits;
mod types;

pub use errors::{
    DialectError, ObservabilityError, RouteError, RuntimeError, SignerError, UpstreamError,
};
pub use traits::{
    ObservabilityHook, PluginRuntime, RouterPlugin, Signer, SignerFactory, UpstreamDialect,
};
pub use types::{
    shape_request, sign_request, AuthStrategy, ObserveEvent, PluginManifest, Principal,
    PrincipalKind, PrincipalQuotas, RequestContext, RetryDecision, RouteDecision, ShapedRequest,
    ShapedRequestBuilder, SignedRequest, SigningCapability, Upstream,
};

mod private {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct Seal;
}
