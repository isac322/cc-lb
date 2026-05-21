#![allow(dead_code)]

use bytes::Bytes;
use cc_lb_plugin_api::{Principal, PrincipalKind, RequestContext, Upstream};
use http::{HeaderMap, HeaderValue, Method};

pub fn principal() -> Principal {
    Principal {
        id: "principal-vertex".to_owned(),
        kind: PrincipalKind::WorkloadIdentity,
        claims: Default::default(),
    }
}

pub fn vertex_upstream() -> Upstream {
    Upstream::Vertex {
        project: "p".to_owned(),
        region: "us-central1".to_owned(),
    }
}

pub fn request_context(body_bytes: Bytes, downstream_headers: HeaderMap) -> RequestContext {
    RequestContext {
        request_id: "req-vertex-test".to_owned(),
        downstream_headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes,
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
    headers.insert("accept", HeaderValue::from_static("application/json"));
    headers.insert("user-agent", HeaderValue::from_static("claude-cli/2.1.75"));
    headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer sk-ant-test"),
    );
    headers.insert("x-forwarded-for", HeaderValue::from_static("127.0.0.1"));
    headers
}

pub fn messages_body(stream: bool) -> Bytes {
    Bytes::from(format!(
        r#"{{"model":"claude-3-5-sonnet@20240620","max_tokens":64,"messages":[],"stream":{stream},"anthropic_version":"2023-06-01","anthropic_beta":["fine-grained-tool-streaming-2025-05-14"]}}"#
    ))
}
