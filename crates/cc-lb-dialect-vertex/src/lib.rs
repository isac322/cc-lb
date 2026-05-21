//! Vertex AI Anthropic publisher dialect.

#![forbid(unsafe_code)]

pub mod error_map;

use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::{Map, Value};
use url::Url;

pub use error_map::{map_vertex_error_status, vertex_error_to_anthropic_json};

const VERTEX_ANTHROPIC_VERSION: &str = "vertex-2023-10-16";
const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Debug, Default)]
pub struct VertexDialect {
    base_url: Option<Url>,
}

impl VertexDialect {
    pub fn with_base_url(base_url: Option<Url>) -> Self {
        Self { base_url }
    }
}

impl UpstreamDialect for VertexDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let Upstream::Vertex { project, region } = upstream else {
            return Err(DialectError::UpstreamMismatch {
                reason: "VertexDialect requires Upstream::Vertex".to_owned(),
            });
        };

        let mut body = parse_body_object(&ctx.body_bytes)?;
        let streaming = wants_stream(&body, &ctx.downstream_headers);
        let model = take_model(&mut body)?;
        body.insert(
            "anthropic_version".to_owned(),
            Value::String(VERTEX_ANTHROPIC_VERSION.to_owned()),
        );
        let body_bytes = serde_json::to_vec(&Value::Object(body)).map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: format!("failed to serialize Vertex request body: {source}"),
            }
        })?;

        let url = vertex_url(self.base_url.as_ref(), project, region, &model, streaming)?;
        let mut headers = shaped_headers(&ctx.downstream_headers);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));

        Ok(builder.shaped_request(url, ctx.method.clone(), headers, Bytes::from(body_bytes)))
    }

    fn normalize_error(&self, status: StatusCode, body: &Bytes) -> Option<Bytes> {
        error_map::normalize_vertex_error(status, body)
    }
}

/// Returns Vertex SSE bytes without parsing or rewriting them.
pub fn passthrough_sse_bytes(bytes: Bytes) -> Bytes {
    bytes
}

fn parse_body_object(body: &[u8]) -> Result<Map<String, Value>, DialectError> {
    let value = serde_json::from_slice::<Value>(body).map_err(|source| {
        DialectError::UnsupportedRequest {
            reason: format!("Vertex requires a JSON object body: {source}"),
        }
    })?;

    match value {
        Value::Object(object) => Ok(object),
        _ => Err(DialectError::UnsupportedRequest {
            reason: "Vertex requires a JSON object body".to_owned(),
        }),
    }
}

fn take_model(body: &mut Map<String, Value>) -> Result<String, DialectError> {
    let Some(model) = body.remove("model") else {
        return Err(DialectError::UnsupportedRequest {
            reason: "Vertex request body must include string field `model`".to_owned(),
        });
    };

    match model {
        Value::String(model) => Ok(model),
        _ => Err(DialectError::UnsupportedRequest {
            reason: "Vertex request body field `model` must be a string".to_owned(),
        }),
    }
}

fn wants_stream(body: &Map<String, Value>, headers: &HeaderMap) -> bool {
    body.get("stream").and_then(Value::as_bool).unwrap_or(false)
        || headers
            .get_all(ACCEPT)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .any(is_text_event_stream)
}

fn is_text_event_stream(value: &str) -> bool {
    value.split(',').any(|part| {
        part.trim()
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.eq_ignore_ascii_case("text/event-stream"))
    })
}

fn shaped_headers(headers: &HeaderMap) -> HeaderMap {
    let mut shaped = headers.clone();
    shaped.remove("anthropic-version");
    shaped.remove("anthropic-beta");
    shaped.remove("x-api-key");
    shaped.remove(AUTHORIZATION);
    shaped.remove(CONTENT_LENGTH);
    shaped
}

fn vertex_url(
    base_url: Option<&Url>,
    project: &str,
    region: &str,
    model: &str,
    streaming: bool,
) -> Result<Url, DialectError> {
    let mut url = match base_url {
        Some(base_url) => base_url.clone(),
        None => {
            let mut base =
                String::with_capacity("https://-aiplatform.googleapis.com".len() + region.len());
            base.push_str("https://");
            base.push_str(region);
            base.push_str("-aiplatform.googleapis.com");
            Url::parse(&base).map_err(|source| DialectError::InvalidUrl { source })?
        }
    };
    let suffix = if streaming {
        "streamRawPredict"
    } else {
        "rawPredict"
    };
    let mut model_endpoint = String::with_capacity(model.len() + 1 + suffix.len());
    model_endpoint.push_str(model);
    model_endpoint.push(':');
    model_endpoint.push_str(suffix);
    {
        let mut segments =
            url.path_segments_mut()
                .map_err(|_| DialectError::UnsupportedRequest {
                    reason: "Vertex base URL cannot be a base".to_owned(),
                })?;
        segments
            .push("v1")
            .push("projects")
            .push(project)
            .push("locations")
            .push(region)
            .push("publishers")
            .push("anthropic")
            .push("models")
            .push(&model_endpoint);
    }
    Ok(url)
}
