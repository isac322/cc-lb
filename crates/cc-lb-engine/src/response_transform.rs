use std::sync::Arc;

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde_json::Value;

use cc_lb_domain::{Principal, Upstream};
use cc_lb_upstream::{
    ResponseTransformError, ResponseTransformHook, SseEvent, SseEventTransformHook,
    TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult, UpstreamDialect,
};

use crate::lifecycle::RequestEventContext;
use crate::provider_capability::ProviderCapability;
use crate::sse_error_frame::{
    ResponsesSseErrorContext, make_provider_error_frame, make_responses_failed_frame,
};
use crate::strip_hop_by_hop;

#[derive(Clone)]
pub(crate) struct ResponseTransformContext {
    pub(crate) principal: Principal,
    pub(crate) upstream: Upstream,
    pub(crate) request_method: Method,
    pub(crate) request_path: String,
    pub(crate) dialect: Arc<dyn UpstreamDialect>,
    pub(crate) gemini_oauth: bool,
    pub(crate) vertex: bool,
    pub(crate) vertex_location: Option<String>,
}

pub(crate) struct BufferedTransformParts {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

pub(crate) enum SseTransformOutcome {
    Emit(Bytes),
    EmitTransformed(Bytes),
    Drop,
    FailOpen,
    Error(ResponseTransformError),
}
pub(crate) struct GeminiOauthResponseTransform;

impl ResponseTransformHook for GeminiOauthResponseTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        let Some(body) = unwrap_gemini_code_assist_body(&request.body)? else {
            return Ok(TransformResponseResult::Unchanged);
        };
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: None,
            body: Some(body),
        })
    }
}

impl SseEventTransformHook for GeminiOauthResponseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let Some(data) = unwrap_gemini_code_assist_body(&request.event.data)? else {
            return Ok(TransformSseEventResult::Unchanged);
        };
        Ok(TransformSseEventResult::Replace {
            events: vec![SseEvent {
                event: request.event.event,
                data,
            }],
        })
    }
}

pub(crate) fn unwrap_gemini_code_assist_body(
    body: &[u8],
) -> Result<Option<Bytes>, ResponseTransformError> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Ok(None);
    };
    let Some(response) = value.get("response") else {
        return Ok(None);
    };
    serde_json::to_vec(response)
        .map(Bytes::from)
        .map(Some)
        .map_err(|error| ResponseTransformError::Runtime {
            reason: format!("failed to encode Gemini response: {error}"),
        })
}
pub(crate) static GEMINI_OAUTH_RESPONSE_TRANSFORM: GeminiOauthResponseTransform =
    GeminiOauthResponseTransform;

pub(crate) struct ComposedSseResponseTransformHook<'a> {
    pub(crate) gemini: &'a dyn SseEventTransformHook,
    pub(crate) custom: &'a dyn SseEventTransformHook,
}

impl SseEventTransformHook for ComposedSseResponseTransformHook<'_> {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let TransformSseEventRequest {
            request_id,
            principal,
            upstream,
            request_method,
            request_path,
            canonical_model_id,
            response_status,
            response_headers,
            event,
        } = request;
        let gemini_result = self.gemini.transform_sse_event(TransformSseEventRequest {
            request_id: request_id.clone(),
            principal: principal.clone(),
            upstream: upstream.clone(),
            request_method: request_method.clone(),
            request_path: request_path.clone(),
            canonical_model_id: canonical_model_id.clone(),
            response_status,
            response_headers: response_headers.clone(),
            event: event.clone(),
        })?;
        let events = match gemini_result {
            TransformSseEventResult::Unchanged => vec![event],
            TransformSseEventResult::Replace { events } => events,
            TransformSseEventResult::Drop => return Ok(TransformSseEventResult::Drop),
        };
        let mut transformed = Vec::new();
        for event in events {
            match self.custom.transform_sse_event(TransformSseEventRequest {
                request_id: request_id.clone(),
                principal: principal.clone(),
                upstream: upstream.clone(),
                request_method: request_method.clone(),
                request_path: request_path.clone(),
                canonical_model_id: canonical_model_id.clone(),
                response_status,
                response_headers: response_headers.clone(),
                event: event.clone(),
            })? {
                TransformSseEventResult::Unchanged => {
                    transformed.push(event);
                }
                TransformSseEventResult::Replace { events } => transformed.extend(events),
                TransformSseEventResult::Drop => {}
            }
        }
        if transformed.is_empty() {
            Ok(TransformSseEventResult::Drop)
        } else {
            Ok(TransformSseEventResult::Replace {
                events: transformed,
            })
        }
    }
}
/// Translate the buffered Vertex response into the Gemini Developer API shape.
///
/// This must run before usage parsing, affinity extraction, response hooks, and
/// delivery so all downstream observers see the same client-visible resource.
pub(crate) fn translate_vertex_response(
    body: &[u8],
    request_path: &str,
    capability: ProviderCapability,
    configured_location: Option<&str>,
) -> Result<Option<Bytes>, &'static str> {
    let is_cached = request_path.contains("/cachedContents");
    let is_catalog = capability == ProviderCapability::GeminiModels
        && request_path.starts_with("/v1")
        && request_path.contains("/models");
    if !is_cached && !is_catalog {
        return Ok(None);
    }
    let mut value = sonic_rs::from_slice::<Value>(body).map_err(|_| "invalid Vertex JSON")?;
    if is_cached {
        let object = value
            .as_object_mut()
            .ok_or("invalid cached content response")?;
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or("invalid cached content name")?;
        let (prefix, id) = name
            .rsplit_once("/cachedContents/")
            .ok_or("invalid cached content name")?;
        let mut prefix_parts = prefix.split('/');
        if prefix_parts.next() != Some("projects")
            || prefix_parts.next().is_none()
            || prefix_parts.next() != Some("locations")
        {
            return Err("invalid cached content name");
        }
        let location = prefix_parts.next().ok_or("invalid cached content name")?;
        if prefix_parts.next().is_some()
            || configured_location.is_some_and(|expected| expected != location)
            || id.is_empty()
            || id.contains('/')
        {
            return Err("invalid cached content name");
        }
        object.insert(
            "name".to_owned(),
            Value::String(format!("cachedContents/{id}")),
        );
        if let Some(model) = object.get("model").and_then(Value::as_str)
            && let Some(model_id) = model
                .strip_prefix("projects/")
                .and_then(|rest| rest.split_once("/locations/"))
                .and_then(|(_, rest)| rest.split_once("/publishers/google/models/"))
                .map(|(_, id)| id)
                .filter(|id| !id.is_empty() && !id.contains('/'))
        {
            object.insert(
                "model".to_owned(),
                Value::String(format!("models/{model_id}")),
            );
        }
    } else {
        let object = value.as_object().ok_or("invalid Vertex model catalog")?;
        let model_get = request_path
            .split_once("/models/")
            .is_some_and(|(_, id)| !id.is_empty() && !id.contains('/'));
        if model_get {
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.strip_prefix("publishers/google/models/"))
                .filter(|id| !id.is_empty() && !id.contains('/'))
                .ok_or("invalid Vertex model catalog")?;
            let mut projected = serde_json::Map::new();
            projected.insert("name".to_owned(), Value::String(format!("models/{name}")));
            if let Some(version) = object.get("versionId") {
                projected.insert("version".to_owned(), version.clone());
            }
            value = Value::Object(projected);
        } else {
            let publisher_models = object
                .get("publisherModels")
                .and_then(Value::as_array)
                .ok_or("invalid Vertex model catalog")?;
            let mut models = Vec::with_capacity(publisher_models.len());
            for model in publisher_models {
                let model = model.as_object().ok_or("invalid Vertex model catalog")?;
                let name = model
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(|name| name.strip_prefix("publishers/google/models/"))
                    .filter(|id| !id.is_empty() && !id.contains('/'))
                    .ok_or("invalid Vertex model catalog")?;
                let mut projected = serde_json::Map::new();
                projected.insert("name".to_owned(), Value::String(format!("models/{name}")));
                if let Some(version) = model.get("versionId") {
                    projected.insert("version".to_owned(), version.clone());
                }
                models.push(Value::Object(projected));
            }
            let mut projected = serde_json::Map::new();
            projected.insert("models".to_owned(), Value::Array(models));
            if let Some(token) = object.get("nextPageToken") {
                projected.insert("nextPageToken".to_owned(), token.clone());
            }
            value = Value::Object(projected);
        }
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map(Some)
        .map_err(|_| "failed to encode Vertex response")
}
pub(crate) fn apply_buffered_transform_result(
    upstream_status: StatusCode,
    mut upstream_headers: HeaderMap,
    upstream_body: Bytes,
    result: TransformResponseResult,
) -> BufferedTransformParts {
    match result {
        TransformResponseResult::Unchanged => BufferedTransformParts {
            status: upstream_status,
            headers: upstream_headers,
            body: upstream_body,
        },
        TransformResponseResult::Replace {
            status,
            headers,
            body,
        } => {
            let (body, content_encoding) = match body {
                Some(body) => (body, None),
                None => {
                    let content_encoding = upstream_headers.get(CONTENT_ENCODING).cloned();
                    (upstream_body, content_encoding)
                }
            };
            let mut headers = match headers {
                Some(mut headers) => {
                    // A replacement map is plugin-owned. Only the original,
                    // non-hop-by-hop unified values may reach the client.
                    strip_hop_by_hop(&mut upstream_headers);
                    sanitize_host_owned_headers(&mut headers);
                    for (name, value) in &upstream_headers {
                        if is_upstream_unified_header(name.as_str()) {
                            headers.append(name.clone(), value.clone());
                        }
                    }
                    headers
                }
                None => {
                    sanitize_client_output_headers(&mut upstream_headers);
                    upstream_headers
                }
            };
            set_downstream_response_framing(&mut headers, body.len(), content_encoding);
            BufferedTransformParts {
                status: status.unwrap_or(upstream_status),
                headers,
                body,
            }
        }
    }
}

pub(crate) fn sanitized_response_headers_for_plugin(headers: &HeaderMap) -> HeaderMap {
    let mut sanitized = headers.clone();
    sanitize_host_owned_headers(&mut sanitized);
    sanitized
}

pub(crate) fn sanitize_downstream_response_headers(
    mut headers: HeaderMap,
    body_len: usize,
    content_encoding: Option<HeaderValue>,
) -> HeaderMap {
    sanitize_client_output_headers(&mut headers);
    set_downstream_response_framing(&mut headers, body_len, content_encoding);
    headers
}

fn set_downstream_response_framing(
    headers: &mut HeaderMap,
    body_len: usize,
    content_encoding: Option<HeaderValue>,
) {
    if let Some(content_encoding) = content_encoding {
        headers.insert(CONTENT_ENCODING, content_encoding);
    }
    if let Ok(value) = HeaderValue::from_str(&body_len.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
}

pub(crate) fn sanitize_downstream_stream_headers(headers: &mut HeaderMap) {
    sanitize_client_output_headers(headers);
}

fn sanitize_host_owned_headers(headers: &mut HeaderMap) {
    sanitize_response_headers_except(headers, |_| false);
}

fn sanitize_client_output_headers(headers: &mut HeaderMap) {
    // These are original upstream headers, unlike a plugin replacement map.
    // Keep unified values in place rather than cloning and reattaching them.
    sanitize_response_headers_except(headers, is_upstream_unified_header);
}

fn is_upstream_unified_header(name: &str) -> bool {
    name.starts_with("anthropic-ratelimit-unified-")
}

fn sanitize_response_headers_except(headers: &mut HeaderMap, preserve: impl Fn(&str) -> bool) {
    strip_hop_by_hop(headers);
    headers.remove(CONTENT_ENCODING);
    headers.remove(CONTENT_LENGTH);
    let remove_names = headers
        .keys()
        .filter(|name| {
            let name = name.as_str();
            !preserve(name)
                && (name.starts_with("x-cc-lb-")
                    || name.starts_with("anthropic-ratelimit-")
                    || name.starts_with("x-ratelimit-")
                    || name == "authorization"
                    || name == "proxy-authorization"
                    || name == "x-api-key")
        })
        .cloned()
        .collect::<Vec<_>>();
    for name in remove_names {
        headers.remove(name);
    }
}

pub(crate) fn transform_sse_event_bytes(
    hook: Option<&dyn SseEventTransformHook>,
    transform_ctx: &ResponseTransformContext,
    event_ctx: &RequestEventContext,
    response_status: StatusCode,
    response_headers: &HeaderMap,
    raw: Bytes,
) -> SseTransformOutcome {
    let Some(hook) = hook else {
        return SseTransformOutcome::FailOpen;
    };
    let Some(event) = parse_sse_event(raw.clone()) else {
        return SseTransformOutcome::FailOpen;
    };
    match hook.transform_sse_event(TransformSseEventRequest {
        request_id: event_ctx.request_id.clone(),
        principal: transform_ctx.principal.clone(),
        upstream: transform_ctx.upstream.clone(),
        request_method: transform_ctx.request_method.clone(),
        request_path: transform_ctx.request_path.clone(),
        canonical_model_id: event_ctx.canonical_model_id.clone(),
        response_status,
        response_headers: response_headers.clone(),
        event: event.clone(),
    }) {
        Ok(TransformSseEventResult::Unchanged) => SseTransformOutcome::Emit(raw),
        Ok(TransformSseEventResult::Replace { events }) => match format_sse_events(&events) {
            Ok(bytes) => SseTransformOutcome::EmitTransformed(bytes),
            Err(error) => SseTransformOutcome::Error(error),
        },
        Ok(TransformSseEventResult::Drop) => SseTransformOutcome::Drop,
        Err(error) => SseTransformOutcome::Error(error),
    }
}

fn parse_sse_event(raw: Bytes) -> Option<SseEvent> {
    let text = std::str::from_utf8(&raw).ok()?;
    let mut event = String::new();
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(name) = line.strip_prefix("event:") {
            event = name.trim_start().to_owned();
            continue;
        }
        if let Some(payload) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push(b'\n');
            }
            data.extend_from_slice(payload.trim_start().as_bytes());
        }
    }
    Some(SseEvent {
        event,
        data: Bytes::from(data),
    })
}

fn format_sse_events(events: &[SseEvent]) -> Result<Bytes, ResponseTransformError> {
    let mut output = Vec::new();
    for event in events {
        if !event.event.is_empty() {
            if event
                .event
                .bytes()
                .any(|byte| matches!(byte, b'\r' | b'\n'))
            {
                return Err(ResponseTransformError::Runtime {
                    reason: "plugin returned SSE event name containing CR/LF".to_owned(),
                });
            }
            output.extend_from_slice(b"event: ");
            output.extend_from_slice(event.event.as_bytes());
            output.extend_from_slice(b"\n");
        }
        for line in event.data.split(|byte| *byte == b'\n') {
            output.extend_from_slice(b"data: ");
            output.extend_from_slice(line);
            output.extend_from_slice(b"\n");
        }
        output.extend_from_slice(b"\n");
    }
    Ok(Bytes::from(output))
}

pub(crate) fn make_response_transform_error_frame(
    capability: ProviderCapability,
    context: &ResponsesSseErrorContext,
    error: &ResponseTransformError,
) -> Bytes {
    if matches!(
        capability,
        ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiResponsesCompact
    ) {
        make_responses_failed_frame(context, "response_transform_error", &error.to_string())
    } else {
        make_provider_error_frame(
            capability,
            http::StatusCode::BAD_GATEWAY,
            "response_transform_error",
            &error.to_string(),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::translate_vertex_response;
    use crate::provider_capability::ProviderCapability;

    #[test]
    fn qa_resp_2_translates_cached_content_before_hooks() {
        let body = br#"{"name":"projects/123456/locations/us-central1/cachedContents/abc","model":"projects/demo/locations/us-central1/publishers/google/models/gemini-2.5-flash","expireTime":"2026-12-01T00:00:00Z"}"#;
        let translated = translate_vertex_response(
            body,
            "/v1beta/cachedContents",
            ProviderCapability::GeminiCachedContentsCreate,
            Some("us-central1"),
        )
        .expect("valid cached response")
        .expect("translation");
        let value: serde_json::Value = sonic_rs::from_slice(&translated).expect("json");
        assert_eq!(value["name"], "cachedContents/abc");
        assert_eq!(value["model"], "models/gemini-2.5-flash");
        assert_eq!(value["expireTime"], "2026-12-01T00:00:00Z");
    }
    #[test]
    fn qa_resp_1_vertex_cached_response_preserves_expiry_and_client_model() {
        let translated = translate_vertex_response(
            br#"{"name":"projects/p/locations/us/cachedContents/id","model":"projects/p/locations/us/publishers/google/models/gemini-2.5-flash","expireTime":"2026-12-01T00:00:00Z"}"#,
            "/v1beta/cachedContents",
            ProviderCapability::GeminiCachedContentsCreate,
            Some("us"),
        )
        .expect("valid response")
        .expect("Vertex response is translated");
        let value: serde_json::Value = sonic_rs::from_slice(&translated).expect("json");
        assert_eq!(value["name"], "cachedContents/id");
        assert_eq!(value["model"], "models/gemini-2.5-flash");
        assert_eq!(value["expireTime"], "2026-12-01T00:00:00Z");
    }

    #[test]
    fn qa_resp_3_streaming_and_non_vertex_responses_are_untouched() {
        assert!(
            translate_vertex_response(
                br#"{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}"#,
                "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
                ProviderCapability::GeminiGenerateContent,
                Some("us"),
            )
            .expect("streaming response")
            .is_none()
        );
        assert!(
            translate_vertex_response(
                br#"{"name":"cachedContents/client-id"}"#,
                "/v1beta/models/gemini-2.5-flash:generateContent",
                ProviderCapability::GeminiGenerateContent,
                None,
            )
            .expect("non-Vertex response")
            .is_none()
        );
    }

    #[test]
    fn qa_resp_4_unexpected_cached_response_shape_fails_closed() {
        assert!(
            translate_vertex_response(
                br#"{"name":"publishers/google/models/gemini-2.5-flash"}"#,
                "/v1beta/cachedContents",
                ProviderCapability::GeminiCachedContentsCreate,
                Some("us"),
            )
            .is_err()
        );
    }

    #[test]
    fn qa_aff_3_rejects_unexpected_cached_content_location() {
        let body = br#"{"name":"projects/123456/locations/eu/cachedContents/abc"}"#;
        assert!(
            translate_vertex_response(
                body,
                "/v1beta/cachedContents",
                ProviderCapability::GeminiCachedContentsCreate,
                Some("us-central1"),
            )
            .is_err()
        );
    }

    #[test]
    fn qa_model_1_projects_publisher_models_with_pagination() {
        let body = br#"{"publisherModels":[{"name":"publishers/google/models/gemini-2.5-flash","versionId":"001"}],"nextPageToken":"next"}"#;
        let translated = translate_vertex_response(
            body,
            "/v1beta/models",
            ProviderCapability::GeminiModels,
            Some("us-central1"),
        )
        .expect("valid catalog")
        .expect("translation");
        let value: serde_json::Value = sonic_rs::from_slice(&translated).expect("json");
        assert_eq!(value["models"][0]["name"], "models/gemini-2.5-flash");
        assert_eq!(value["models"][0]["version"], "001");
        assert_eq!(value["nextPageToken"], "next");
    }

    #[test]
    fn qa_resp_4_rejects_malformed_catalog() {
        assert!(
            translate_vertex_response(
                br#"{"publisherModels":[{"name":"publishers/google/models/x/y"}]}"#,
                "/v1beta/models",
                ProviderCapability::GeminiModels,
                None,
            )
            .is_err()
        );
    }
    #[test]
    fn qa_model_2_projects_publisher_model_get() {
        let translated = translate_vertex_response(
            br#"{"name":"publishers/google/models/gemini-2.5-flash","versionId":"001"}"#,
            "/v1beta/models/gemini-2.5-flash",
            ProviderCapability::GeminiModels,
            None,
        )
        .expect("valid model")
        .expect("translation");
        let value: serde_json::Value = sonic_rs::from_slice(&translated).expect("json");
        assert_eq!(value["name"], "models/gemini-2.5-flash");
        assert_eq!(value["version"], "001");
    }
}
