use bytes::Bytes;
use cc_lb_domain::CachePricingSummary;
use http::{HeaderMap, Method};

/// Request data available to routing filters and routers.
#[derive(Clone, Debug)]
pub struct RoutingContext {
    /// Stable request identifier used for routing and wire-backed filters.
    pub request_id: String,
    /// Conversation identifier used by session-aware routing filters.
    pub thread_id: Option<String>,
    /// Raw service tier requested in the downstream request body.
    pub requested_service_tier: Option<String>,
    /// Downstream headers after hop-by-hop stripping.
    pub downstream_headers: HeaderMap,
    /// Downstream HTTP method.
    pub method: Method,
    /// Downstream request path.
    pub path: String,
    /// Raw downstream query string without the leading question mark.
    pub query: Option<String>,
    /// Buffered downstream request body.
    pub body_bytes: Bytes,
    /// Canonical model identifier resolved from the request.
    pub canonical_model_id: String,
    /// Model pricing summary used by routing filters.
    pub cache_pricing: CachePricingSummary,
}
