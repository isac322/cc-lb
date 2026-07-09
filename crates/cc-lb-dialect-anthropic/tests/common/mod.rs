#![allow(dead_code)]

use bytes::Bytes;
use cc_lb_plugin_api::{Principal, PrincipalKind, RequestContext};
use http::{HeaderMap, HeaderValue, Method};

pub fn principal() -> Principal {
    Principal {
        id: "principal-direct".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: Default::default(),
    }
}

pub fn request_context(
    method: Method,
    path: &str,
    query: Option<&str>,
    body_bytes: Bytes,
    downstream_headers: HeaderMap,
) -> RequestContext {
    RequestContext {
        request_id: "req-direct-identity".to_owned(),
        thread_id: None,
        downstream_headers,
        method,
        path: path.to_owned(),
        query: query.map(ToOwned::to_owned),
        body_bytes,
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
        cache_pricing: cc_lb_plugin_api::CachePricingSummary::default(),
    }
}

pub fn anthropic_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("oauth-2025-04-20,files-api-2025-04-14"),
    );
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("user-agent", HeaderValue::from_static("claude-cli/2.1.75"));
    headers.insert(
        "anthropic-dangerous-direct-browser-access",
        HeaderValue::from_static("true"),
    );
    headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer sk-ant-oat01-test"),
    );
    headers.insert("x-foo", HeaderValue::from_static("bar"));
    headers
}
