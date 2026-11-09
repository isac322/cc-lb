use bytes::Bytes;
use cc_lb_domain::{Principal, Upstream};
use http::{HeaderMap, Method, StatusCode};
use url::Url;

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
/// Direction of a provider realtime/live frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeFrameDirection {
    /// Initial session configuration frame sent before media or events flow.
    Setup,
    /// Frame sent from the client toward the provider.
    ClientToProvider,
    /// Frame sent from the provider toward the client.
    ProviderToClient,
}

/// Payload encoding of a provider realtime/live frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeFrameKind {
    /// UTF-8 JSON payload.
    Json,
    /// UTF-8 text payload that is not JSON.
    Text,
    /// Opaque binary payload.
    Binary,
}

/// Provider-qualified realtime/live frame transform input.
#[derive(Clone, Debug, PartialEq)]
pub struct TransformRealtimeFrameRequest {
    /// Proxy-assigned request identifier for logging and correlation.
    pub request_id: String,
    /// Authenticated principal that owns the realtime session.
    pub principal: Principal,
    /// Upstream the frame is exchanged with.
    pub upstream: Upstream,
    /// HTTP method of the original realtime handshake request.
    pub request_method: Method,
    /// Request path of the original realtime handshake.
    pub request_path: String,
    /// Canonical model identifier the session is bound to.
    pub canonical_model_id: String,
    /// Direction of this frame relative to the provider.
    pub direction: RealtimeFrameDirection,
    /// Payload encoding of `payload`.
    pub kind: RealtimeFrameKind,
    /// Raw frame payload bytes.
    pub payload: Bytes,
}

/// Provider-qualified realtime/live frame transform output.
#[derive(Clone, Debug, PartialEq)]
pub enum TransformRealtimeFrameResult {
    /// Forward the original frame unmodified.
    Unchanged,
    /// Forward a replacement frame with the given encoding and payload.
    Replace {
        /// Payload encoding of the replacement frame.
        kind: RealtimeFrameKind,
        /// Replacement payload bytes.
        payload: Bytes,
    },
    /// Discard the frame without forwarding it.
    Drop,
}

/// Provider-qualified realtime/live frame response transform boundary.
pub trait RealtimeFrameTransformHook: Send + Sync {
    /// Transforms one realtime frame, returning whether to keep, replace, or drop it.
    fn transform_realtime_frame(
        &self,
        request: TransformRealtimeFrameRequest,
    ) -> Result<TransformRealtimeFrameResult, ResponseTransformError>;
}

/// Final Code Assist envelope, before the signer applies stored credentials.
#[derive(Clone, Copy, Debug)]
pub struct GeminiEnvelopeRequest<'a> {
    /// Host-owned destination. Envelope hooks cannot replace this URL.
    pub url: &'a Url,
    /// Request headers before signer-owned credential replacement.
    pub headers: &'a HeaderMap,
    /// Serialized Code Assist envelope after native request wrapping.
    pub envelope_body: &'a Bytes,
}

/// Optional replacements for a final Code Assist envelope.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GeminiEnvelopeResult {
    /// Replacement serialized envelope, or `None` to preserve the original.
    pub envelope_body: Option<Bytes>,
    /// Replacement headers, or `None` to preserve the original.
    pub headers: Option<HeaderMap>,
}

/// Optional hook invoked after Code Assist wrapping and before signing.
pub trait GeminiEnvelopeHook: Send + Sync {
    /// Replaces envelope parts without changing the host-owned destination.
    fn shape_envelope_gemini(
        &self,
        request: GeminiEnvelopeRequest<'_>,
    ) -> Result<GeminiEnvelopeResult, crate::SignerError>;
}

impl std::fmt::Debug for dyn GeminiEnvelopeHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GeminiEnvelopeHook")
    }
}
