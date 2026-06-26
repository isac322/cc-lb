use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use axum::body::Body;
use bytes::Bytes;
use cc_lb_plugin_api::{Upstream, UpstreamDialect};
use flate2::read::GzDecoder;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use serde_json::{Value, json};
use thiserror::Error;

use crate::sse_error_frame::make_error_frame_from_json;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

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
pub struct ErrorNormalizer {
    dialects: HashMap<UpstreamKind, Arc<dyn UpstreamDialect>>,
}

impl ErrorNormalizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_dialect(mut self, kind: UpstreamKind, dialect: Arc<dyn UpstreamDialect>) -> Self {
        self.register_dialect(kind, dialect);
        self
    }

    pub fn register_dialect(
        &mut self,
        kind: UpstreamKind,
        dialect: Arc<dyn UpstreamDialect>,
    ) -> &mut Self {
        self.dialects.insert(kind, dialect);
        self
    }

    pub fn normalize_http_error(
        &self,
        kind: UpstreamKind,
        status: StatusCode,
        body: &Bytes,
    ) -> Bytes {
        self.normalize_http_error_with_dialect(kind, status, body, None)
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

    pub(crate) fn build_http_error_response_with_dialect(
        &self,
        kind: UpstreamKind,
        status: StatusCode,
        body: &Bytes,
        original_headers: &HeaderMap,
        fallback_dialect: Option<&dyn UpstreamDialect>,
    ) -> Response<Body> {
        let body = self.normalize_http_error_with_dialect(kind, status, body, fallback_dialect);
        response_from_error_body(status, body, original_headers)
    }

    fn normalize_http_error_with_dialect(
        &self,
        kind: UpstreamKind,
        status: StatusCode,
        body: &Bytes,
        fallback_dialect: Option<&dyn UpstreamDialect>,
    ) -> Bytes {
        let body = decoded_gzip_error_body(body);
        if let Some(dialect) = self.dialects.get(&kind) {
            if let Some(normalized) = dialect.normalize_error(status, &body) {
                return normalized;
            }
            return body;
        }

        if let Some(dialect) = fallback_dialect
            && let Some(normalized) = dialect.normalize_error(status, &body)
        {
            return normalized;
        }

        body
    }
}

fn decoded_gzip_error_body(body: &Bytes) -> Bytes {
    if !body.starts_with(&GZIP_MAGIC) {
        return body.clone();
    }

    let mut decoder = GzDecoder::new(body.as_ref());
    let mut decoded = Vec::new();
    match decoder.read_to_end(&mut decoded) {
        Ok(_) => Bytes::from(decoded),
        Err(source) => {
            tracing::warn!(%source, "failed to decode gzip upstream error body");
            body.clone()
        }
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
