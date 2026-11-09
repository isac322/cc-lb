//! Host-side upstream request shaping, signing, and response transform SPI.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod context;
mod error;
mod request;
mod retry;
mod scrub;
mod traits;
mod transform;
mod url;

pub use context::DialectShapeContext;
pub use error::{DialectError, ResponseTransformError, SignerError, UpstreamError};
pub use request::{
    ShapedRequest, ShapedRequestBuilder, SignedRequest, SigningCapability, shape_request,
    sign_request,
};
pub use retry::RetryDecision;
pub use scrub::scrub_secret;
pub use traits::{ApiKeyAwareSignerFactory, Signer, SignerFactory, UpstreamDialect};
pub use transform::{
    GeminiEnvelopeHook, GeminiEnvelopeRequest, GeminiEnvelopeResult, RealtimeFrameDirection,
    RealtimeFrameKind, RealtimeFrameTransformHook, ResponseTransformHook, SseEvent,
    SseEventTransformHook, TransformRealtimeFrameRequest, TransformRealtimeFrameResult,
    TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult,
};
pub use url::{scrub_query_api_key, scrub_url_query_api_key, upstream_request_url};

mod private {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct Seal;
}
