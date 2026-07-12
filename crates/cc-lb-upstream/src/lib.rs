//! Host-side upstream request shaping, signing, and response transform SPI.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod context;
mod error;
mod request;
mod retry;
mod traits;
mod transform;

pub use context::DialectShapeContext;
pub use error::{DialectError, ResponseTransformError, SignerError, UpstreamError};
pub use request::{
    ShapedRequest, ShapedRequestBuilder, SignedRequest, SigningCapability, shape_request,
    sign_request,
};
pub use retry::RetryDecision;
pub use traits::{ApiKeyAwareSignerFactory, Signer, SignerFactory, UpstreamDialect};
pub use transform::{
    ResponseTransformHook, SseEvent, SseEventTransformHook, TransformResponseRequest,
    TransformResponseResult, TransformSseEventRequest, TransformSseEventResult,
};

mod private {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct Seal;
}
