use bytes::Bytes;
use cc_lb_domain::{CacheBreakpoint, CachePricingSummary};
use cc_lb_routing::RoutingContext;
use cc_lb_upstream::DialectShapeContext;
use http::{HeaderMap, Method};

/// Parsed downstream request data passed into plugins.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Stable request identifier used for logs, audit rows, and upstream traceability.
    pub request_id: String,
    /// Session / thread identifier that stays constant across multiple requests
    /// belonging to the same conversation. Populated at request parse time
    /// from `x-claude-code-session-id` and friends. `None` for stateless
    /// requests that do not carry a session header.
    ///
    /// Filters that need cache-affinity (e.g. subscription routing) MUST
    /// prefer this over `request_id` as their per-session hash input so that
    /// all requests belonging to the same conversation land on the same
    /// upstream and reuse the Anthropic prompt cache.
    pub thread_id: Option<String>,
    /// Downstream request headers after hop-by-hop stripping.
    pub downstream_headers: HeaderMap,
    /// Downstream HTTP method.
    pub method: Method,
    /// Downstream request path, such as `/v1/messages`.
    pub path: String,
    /// Raw downstream query string without the leading `?`.
    pub query: Option<String>,
    /// Buffered request body bytes, required by SigV4 and other signing schemes.
    pub body_bytes: Bytes,
    /// Cache breakpoints extracted from the request for prompt cache optimization.
    pub cache_breakpoints: Vec<CacheBreakpoint>,
    /// Canonical model identifier resolved from the request.
    pub canonical_model_id: String,
    /// Pricing summary for the canonical model, loaded by the host from the
    /// in-memory price catalog before router plugins run.
    pub cache_pricing: CachePricingSummary,
}

impl RequestContext {
    /// Starts a construction flow that remains source-compatible as fields are added.
    pub fn builder() -> ParsedRequestBuilder {
        ParsedRequestBuilder::new()
    }

    /// Projects the request data required by dialect shaping and signing.
    pub fn dialect_shape_context(&self) -> DialectShapeContext {
        DialectShapeContext {
            request_id: self.request_id.clone(),
            downstream_headers: self.downstream_headers.clone(),
            method: self.method.clone(),
            path: self.path.clone(),
            query: self.query.clone(),
            body_bytes: self.body_bytes.clone(),
        }
    }

    /// Projects the request data available to routing filters and routers.
    pub fn routing_context(&self) -> RoutingContext {
        RoutingContext {
            request_id: self.request_id.clone(),
            thread_id: self.thread_id.clone(),
            downstream_headers: self.downstream_headers.clone(),
            method: self.method.clone(),
            path: self.path.clone(),
            query: self.query.clone(),
            body_bytes: self.body_bytes.clone(),
            canonical_model_id: self.canonical_model_id.clone(),
            cache_pricing: self.cache_pricing.clone(),
        }
    }
}

/// Forward-compatible constructor for the engine's parsed downstream request.
pub struct ParsedRequestBuilder {
    request_id: String,
    thread_id: Option<String>,
    downstream_headers: HeaderMap,
    method: Method,
    path: String,
    query: Option<String>,
    body_bytes: Bytes,
    cache_breakpoints: Vec<CacheBreakpoint>,
    canonical_model_id: String,
    cache_pricing: CachePricingSummary,
}

impl ParsedRequestBuilder {
    /// Creates an empty builder whose defaults preserve the parsed-request empty state.
    pub fn new() -> Self {
        Self {
            request_id: String::new(),
            thread_id: None,
            downstream_headers: HeaderMap::new(),
            method: Method::GET,
            path: String::new(),
            query: None,
            body_bytes: Bytes::new(),
            cache_breakpoints: Vec::new(),
            canonical_model_id: String::new(),
            cache_pricing: CachePricingSummary::default(),
        }
    }

    /// Sets the stable request identifier.
    pub fn request_id(mut self, request_id: String) -> Self {
        self.request_id = request_id;
        self
    }

    /// Sets the optional conversation identifier.
    pub fn thread_id(mut self, thread_id: Option<String>) -> Self {
        self.thread_id = thread_id;
        self
    }

    /// Sets the stripped downstream headers.
    pub fn downstream_headers(mut self, downstream_headers: HeaderMap) -> Self {
        self.downstream_headers = downstream_headers;
        self
    }

    /// Sets the downstream HTTP method.
    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    /// Sets the downstream request path.
    pub fn path(mut self, path: String) -> Self {
        self.path = path;
        self
    }

    /// Sets the raw query string.
    pub fn query(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    /// Sets the buffered request body.
    pub fn body_bytes(mut self, body_bytes: Bytes) -> Self {
        self.body_bytes = body_bytes;
        self
    }

    /// Sets prompt-cache breakpoints.
    pub fn cache_breakpoints(mut self, cache_breakpoints: Vec<CacheBreakpoint>) -> Self {
        self.cache_breakpoints = cache_breakpoints;
        self
    }

    /// Sets the canonical model identifier.
    pub fn canonical_model_id(mut self, canonical_model_id: String) -> Self {
        self.canonical_model_id = canonical_model_id;
        self
    }

    /// Sets the model pricing summary.
    pub fn cache_pricing(mut self, cache_pricing: CachePricingSummary) -> Self {
        self.cache_pricing = cache_pricing;
        self
    }

    /// Builds the parsed request context.
    pub fn build(self) -> RequestContext {
        RequestContext {
            request_id: self.request_id,
            thread_id: self.thread_id,
            downstream_headers: self.downstream_headers,
            method: self.method,
            path: self.path,
            query: self.query,
            body_bytes: self.body_bytes,
            cache_breakpoints: self.cache_breakpoints,
            canonical_model_id: self.canonical_model_id,
            cache_pricing: self.cache_pricing,
        }
    }
}

impl Default for ParsedRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}
