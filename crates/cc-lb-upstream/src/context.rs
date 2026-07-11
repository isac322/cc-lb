use bytes::Bytes;
use http::{HeaderMap, Method};

/// Request data available to upstream dialect shaping implementations.
#[derive(Clone, Debug)]
pub struct DialectShapeContext {
    /// Stable request identifier used by wire-backed dialects.
    pub request_id: String,
    /// Downstream request headers after hop-by-hop stripping.
    pub downstream_headers: HeaderMap,
    /// Downstream HTTP method.
    pub method: Method,
    /// Downstream request path, such as `/v1/messages`.
    pub path: String,
    /// Raw downstream query string without the leading `?`.
    pub query: Option<String>,
    /// Buffered downstream request body.
    pub body_bytes: Bytes,
}
