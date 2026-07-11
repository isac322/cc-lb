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
pub mod types;

pub use errors::{ObservabilityError, RouteError};
pub use traits::{FilterError, FilterOutput, FilterPlugin, ObservabilityHook, RouterPlugin};
pub use types::{
    CachePricingSummary, CandidateUrgency, CredentialStrategy, GLOBAL_PRINCIPAL, InternalError,
    InternalErrorKind, InternalErrorStage, ObserveEvent, PerCandidateReason, PluginManifest,
    PluginSlot, Principal, PrincipalKind, PrincipalQuotas, RateLimitKind, RateLimitObservation,
    RequestContext, RouteDecision, RoutingTrace, SlotKey, SubscriptionPreferenceTrace,
    SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, SubscriptionTier,
    TerminalStrategy, Upstream, UpstreamCandidate, UpstreamKind, default_pure,
};

#[doc(hidden)]
pub use cc_lb_domain::{
    BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_NAME, BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
    BUILTIN_SUBSCRIPTION_PREFERENCE_NAME,
};

#[doc(hidden)]
pub use cc_lb_upstream::{
    ApiKeyAwareSignerFactory, DialectError, DialectShapeContext, ResponseTransformError,
    ResponseTransformHook, RetryDecision, ShapedRequest, ShapedRequestBuilder, SignedRequest,
    Signer, SignerError, SignerFactory, SigningCapability, SseEvent, SseEventTransformHook,
    TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult, UpstreamDialect, UpstreamError, shape_request, sign_request,
};
