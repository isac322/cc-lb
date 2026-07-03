use axum::body::Body;
use bytes::Bytes;
use cc_lb_plugin_api::Upstream;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use serde_json::{Value, json};
use thiserror::Error;

use crate::sse_error_frame::make_error_frame_from_json;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UpstreamKind {
    AnthropicDirect,
}

impl From<&Upstream> for UpstreamKind {
    fn from(upstream: &Upstream) -> Self {
        match upstream {
            Upstream::AnthropicDirect { .. } => Self::AnthropicDirect,
        }
    }
}

#[derive(Debug, Error)]
pub enum NormalizerError {
    #[error("upstream error body was not valid JSON")]
    InvalidJson,
    #[error("upstream error body did not contain a recognizable error shape")]
    UnsupportedShape,
}

#[derive(Clone, Default)]
pub struct ErrorNormalizer;

impl ErrorNormalizer {
    pub fn new() -> Self {
        Self
    }

    pub fn normalize_http_error(
        &self,
        _kind: UpstreamKind,
        _status: StatusCode,
        body: &Bytes,
    ) -> Bytes {
        body.clone()
    }

    pub fn build_http_error_response(
        &self,
        kind: UpstreamKind,
        status: StatusCode,
        body: &Bytes,
        original_headers: &HeaderMap,
    ) -> Response<Body> {
        let body = self.normalize_http_error(kind, status, body);
        response_from_error_body(status, body, original_headers)
    }

    pub fn normalize_sse_error_frame(
        &self,
        kind: UpstreamKind,
        raw_event_data_json: &Bytes,
    ) -> Bytes {
        let error_json = match kind {
            UpstreamKind::AnthropicDirect => anthropic_sse_error_json(raw_event_data_json),
        };
        make_error_frame_from_json(&error_json)
    }
}

fn response_from_error_body(
    status: StatusCode,
    body: Bytes,
    original_headers: &HeaderMap,
) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    preserve_protocol_headers(original_headers, response.headers_mut());
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn preserve_protocol_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source.iter() {
        if is_protocol_header(name) {
            target.append(name.clone(), value.clone());
        }
    }
}

fn is_protocol_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    name == "request-id"
        || name == "x-request-id"
        || name == "retry-after"
        || name.starts_with("anthropic-")
}

fn anthropic_sse_error_json(raw_event_data_json: &Bytes) -> Value {
    match serde_json::from_slice::<Value>(raw_event_data_json) {
        Ok(value) => match canonical_anthropic_error_value(&value) {
            Some(error) => error,
            None => fallback_error_value(),
        },
        Err(_source) => fallback_error_value(),
    }
}

fn canonical_anthropic_error_value(value: &Value) -> Option<Value> {
    if value.get("type").and_then(Value::as_str) != Some("error") {
        return None;
    }

    let error = value.get("error")?;
    let error_type = error.get("type").and_then(Value::as_str)?;
    let message = error.get("message").and_then(Value::as_str)?;
    Some(json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": message,
        }
    }))
}

fn fallback_error_value() -> Value {
    json!({
        "type": "error",
        "error": {
            "type": "api_error",
            "message": "upstream error",
        }
    })
}
