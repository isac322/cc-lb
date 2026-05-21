use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::{Map, Value};
use url::Url;

use crate::error_map::normalize_bedrock_error;

const BEDROCK_ANTHROPIC_VERSION: &str = "bedrock-2023-05-31";
const EVENTSTREAM_ACCEPT: &str = "application/vnd.amazon.eventstream";
const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Debug, Default)]
pub struct BedrockRuntimeDialect {
    base_url: Option<Url>,
}

impl BedrockRuntimeDialect {
    pub fn with_base_url(base_url: Option<Url>) -> Self {
        Self { base_url }
    }
}

#[derive(Clone, Debug, Default)]
pub struct BedrockBodyTransform;

impl BedrockBodyTransform {
    pub fn rewrite_in(input: &[u8]) -> Vec<u8> {
        input.to_vec()
    }

    pub fn restore_response_shape(input: &[u8]) -> Vec<u8> {
        input.to_vec()
    }
}

impl UpstreamDialect for BedrockRuntimeDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let Upstream::BedrockRuntime { region } = upstream else {
            return Err(DialectError::UnsupportedRequest {
                reason: "BedrockRuntimeDialect requires Upstream::BedrockRuntime".to_owned(),
            });
        };

        let mut body = parse_body_object(&ctx.body_bytes)?;
        let streaming = wants_stream(&body, &ctx.downstream_headers);
        let model = take_model(&mut body)?;
        body.insert(
            "anthropic_version".to_owned(),
            Value::String(BEDROCK_ANTHROPIC_VERSION.to_owned()),
        );
        apply_beta_header(&mut body, &ctx.downstream_headers);
        let body_bytes = serde_json::to_vec(&Value::Object(body)).map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: format!("failed to serialize Bedrock request body: {source}"),
            }
        })?;

        let url = runtime_url(self.base_url.as_ref(), region, &model, streaming)?;
        let mut headers = shaped_headers(&ctx.downstream_headers);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
        headers.insert(
            ACCEPT,
            if streaming {
                HeaderValue::from_static(EVENTSTREAM_ACCEPT)
            } else {
                HeaderValue::from_static(JSON_CONTENT_TYPE)
            },
        );

        Ok(builder.shaped_request(url, ctx.method.clone(), headers, Bytes::from(body_bytes)))
    }

    fn normalize_error(&self, status: StatusCode, body: &Bytes) -> Option<Bytes> {
        normalize_bedrock_error(status, body)
    }
}

fn parse_body_object(body: &[u8]) -> Result<Map<String, Value>, DialectError> {
    let value = serde_json::from_slice::<Value>(body).map_err(|source| {
        DialectError::UnsupportedRequest {
            reason: format!("Bedrock runtime requires a JSON object body: {source}"),
        }
    })?;

    match value {
        Value::Object(object) => Ok(object),
        _ => Err(DialectError::UnsupportedRequest {
            reason: "Bedrock runtime requires a JSON object body".to_owned(),
        }),
    }
}

fn take_model(body: &mut Map<String, Value>) -> Result<String, DialectError> {
    let Some(model) = body.remove("model") else {
        return Err(DialectError::UnsupportedRequest {
            reason: "Bedrock runtime request body must include string field `model`".to_owned(),
        });
    };

    match model {
        Value::String(model) => Ok(model),
        _ => Err(DialectError::UnsupportedRequest {
            reason: "Bedrock runtime request body field `model` must be a string".to_owned(),
        }),
    }
}

fn wants_stream(body: &Map<String, Value>, headers: &HeaderMap) -> bool {
    body.get("stream").and_then(Value::as_bool).unwrap_or(false)
        || headers
            .get(ACCEPT)
            .and_then(|value| value.to_str().ok())
            .map(crate::is_text_event_stream)
            .unwrap_or(false)
}

fn apply_beta_header(body: &mut Map<String, Value>, headers: &HeaderMap) {
    if let Some(beta) = headers
        .get("anthropic-beta")
        .and_then(|value| value.to_str().ok())
    {
        let values = beta
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| Value::String(value.to_owned()))
            .collect::<Vec<_>>();
        if !values.is_empty() {
            body.insert("anthropic_beta".to_owned(), Value::Array(values));
        }
    }
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

fn runtime_url(
    base_url: Option<&Url>,
    region: &str,
    model: &str,
    streaming: bool,
) -> Result<Url, DialectError> {
    let mut url = match base_url {
        Some(base_url) => base_url.clone(),
        None => {
            let mut base = String::with_capacity(
                "https://bedrock-runtime..amazonaws.com".len() + region.len(),
            );
            base.push_str("https://bedrock-runtime.");
            base.push_str(region);
            base.push_str(".amazonaws.com");
            Url::parse(&base).map_err(|source| DialectError::InvalidUrl { source })?
        }
    };
    {
        let mut segments =
            url.path_segments_mut()
                .map_err(|_| DialectError::UnsupportedRequest {
                    reason: "Bedrock runtime base URL cannot be a base".to_owned(),
                })?;
        segments.push("model");
        segments.push(model);
        segments.push(if streaming {
            "invoke-with-response-stream"
        } else {
            "invoke"
        });
    }
    Ok(url)
}
