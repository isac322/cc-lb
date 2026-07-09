use std::sync::Arc;

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http::{HeaderMap, HeaderValue, Method, StatusCode};

use cc_lb_plugin_api::{
    Principal, ResponseTransformError, ResponseTransformHook, SseEvent, SseEventTransformHook,
    TransformResponseResult, TransformSseEventRequest, TransformSseEventResult, Upstream,
};

use crate::hop_by_hop::strip_hop_by_hop;
use crate::lifecycle::RequestEventContext;
use crate::sse_error_frame::make_error_frame;

#[derive(Clone)]
pub(crate) struct ResponseTransformContext {
    pub(crate) principal: Principal,
    pub(crate) upstream: Upstream,
    pub(crate) request_method: Method,
    pub(crate) request_path: String,
    pub(crate) response_transform_hook: Option<Arc<dyn ResponseTransformHook>>,
    pub(crate) sse_event_transform_hook: Option<Arc<dyn SseEventTransformHook>>,
}

pub(crate) struct BufferedTransformParts {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

pub(crate) enum SseTransformOutcome {
    Emit(Bytes),
    Drop,
    FailOpen,
    Error(ResponseTransformError),
}

pub(crate) fn apply_buffered_transform_result(
    upstream_status: StatusCode,
    upstream_headers: HeaderMap,
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
            let headers = headers.unwrap_or(upstream_headers);
            BufferedTransformParts {
                status: status.unwrap_or(upstream_status),
                headers: sanitize_downstream_response_headers(
                    headers,
                    body.len(),
                    content_encoding,
                ),
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

fn sanitize_downstream_response_headers(
    mut headers: HeaderMap,
    body_len: usize,
    content_encoding: Option<HeaderValue>,
) -> HeaderMap {
    sanitize_host_owned_headers(&mut headers);
    if let Some(content_encoding) = content_encoding {
        headers.insert(CONTENT_ENCODING, content_encoding);
    }
    if let Ok(value) = HeaderValue::from_str(&body_len.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
    headers
}

pub(crate) fn sanitize_downstream_stream_headers(headers: &mut HeaderMap) {
    sanitize_host_owned_headers(headers);
}

fn sanitize_host_owned_headers(headers: &mut HeaderMap) {
    strip_hop_by_hop(headers);
    headers.remove(CONTENT_ENCODING);
    headers.remove(CONTENT_LENGTH);
    let remove_names = headers
        .keys()
        .filter(|name| {
            let name = name.as_str();
            name.starts_with("x-cc-lb-")
                || name.starts_with("anthropic-ratelimit-")
                || name.starts_with("x-ratelimit-")
                || name == "authorization"
                || name == "proxy-authorization"
                || name == "x-api-key"
        })
        .cloned()
        .collect::<Vec<_>>();
    for name in remove_names {
        headers.remove(name);
    }
}

pub(crate) fn transform_sse_event_bytes(
    hook: Option<&Arc<dyn SseEventTransformHook>>,
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
        Ok(TransformSseEventResult::Replace { events }) => {
            SseTransformOutcome::Emit(format_sse_events(&events))
        }
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

fn format_sse_events(events: &[SseEvent]) -> Bytes {
    let mut output = Vec::new();
    for event in events {
        if !event.event.is_empty() {
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
    Bytes::from(output)
}

pub(crate) fn make_response_transform_error_frame(error: &ResponseTransformError) -> Bytes {
    make_error_frame("response_transform_error", &error.to_string())
}
