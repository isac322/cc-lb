use bytes::Bytes;
use cc_lb_domain::{Principal, Upstream};
use http::{HeaderMap, Method, StatusCode};

use crate::ResponseTransformError;

/// Buffered upstream response transform input.
#[derive(Clone, Debug, PartialEq)]
pub struct TransformResponseRequest {
    /// Stable request identifier.
    pub request_id: String,
    /// Authenticated caller identity.
    pub principal: Principal,
    /// Selected upstream that produced the response.
    pub upstream: Upstream,
    /// Original downstream request method.
    pub request_method: Method,
    /// Original downstream request path.
    pub request_path: String,
    /// Canonical model identifier resolved from the request.
    pub canonical_model_id: String,
    /// Upstream response status.
    pub response_status: StatusCode,
    /// Upstream response headers after host-owned trimming/decoding.
    pub response_headers: HeaderMap,
    /// Decoded response body bytes.
    pub body: Bytes,
}

/// Buffered upstream response transform output.
#[derive(Clone, Debug, PartialEq)]
pub enum TransformResponseResult {
    /// Return the upstream response unchanged.
    Unchanged,
    /// Replace selected downstream-visible response parts.
    Replace {
        /// Optional replacement status.
        status: Option<StatusCode>,
        /// Optional replacement end-to-end headers.
        headers: Option<HeaderMap>,
        /// Optional replacement decoded body.
        body: Option<Bytes>,
    },
}

/// One parsed SSE event delivered to response-transform plugins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SseEvent {
    /// SSE event type, empty for unnamed `data:` events.
    pub event: String,
    /// Concatenated SSE data payload bytes.
    pub data: Bytes,
}

/// Per-event SSE response transform input.
#[derive(Clone, Debug, PartialEq)]
pub struct TransformSseEventRequest {
    /// Stable request identifier.
    pub request_id: String,
    /// Authenticated caller identity.
    pub principal: Principal,
    /// Selected upstream that produced the response.
    pub upstream: Upstream,
    /// Original downstream request method.
    pub request_method: Method,
    /// Original downstream request path.
    pub request_path: String,
    /// Canonical model identifier resolved from the request.
    pub canonical_model_id: String,
    /// Upstream response status.
    pub response_status: StatusCode,
    /// Upstream response headers after host-owned trimming/decoding.
    pub response_headers: HeaderMap,
    /// Complete parsed SSE event.
    pub event: SseEvent,
}

/// Per-event SSE response transform output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransformSseEventResult {
    /// Emit the input event unchanged.
    Unchanged,
    /// Replace the input event with zero or more events.
    Replace {
        /// Replacement events emitted in order.
        events: Vec<SseEvent>,
    },
    /// Drop the input event.
    Drop,
}

/// Buffered response transform boundary.
pub trait ResponseTransformHook: Send + Sync {
    /// Transforms one buffered upstream response.
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError>;
}

/// SSE event response transform boundary.
pub trait SseEventTransformHook: Send + Sync {
    /// Transforms one complete parsed SSE event.
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError>;
}
