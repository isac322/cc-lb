use axum::body::Body;
use bytes::{Bytes, BytesMut};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use std::convert::Infallible;

use crate::lifecycle::compat_request::{CompatResponseKind, CompatResponsePlan};
use crate::openai_compat;

/// Maximum number of bytes retained while collecting a response for a
/// compatibility conversion. The caller supplies the configured request cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CollectError {
    BodyTooLarge,
    BodyRead,
}

/// Collect an upstream body without allowing an unbounded compatibility buffer.
pub(crate) async fn collect_body(body: Body, cap: usize) -> Result<Bytes, CollectError> {
    let mut body = body;
    let mut bytes = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| CollectError::BodyRead)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.len() > cap.saturating_sub(bytes.len()) {
            return Err(CollectError::BodyTooLarge);
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes.freeze())
}

/// Aggregate a Responses SSE body into one public Response JSON body.
///
/// The upstream status is retained for non-SSE error bodies; an SSE terminal
/// failure supplies its own public status and envelope. A missing terminal is
/// deliberately surfaced as a gateway error rather than a partial 200.
pub(crate) async fn aggregate_response(
    response: Response<Body>,
    echo_metadata: Option<Value>,
    cap: usize,
) -> Result<Response<Body>, CollectError> {
    let (parts, body) = response.into_parts();
    let status = parts.status;
    let headers = parts.headers;
    let body = collect_body(body, cap).await?;

    if !is_sse_content_type(&headers) {
        if !status.is_success() {
            if let Some(public) =
                openai_compat::response::public_error_from_detail(status.as_u16(), &body)
            {
                return Ok(json_response(status, public));
            }
        }
        return Ok(json_response(status, body.to_vec()));
    }

    let mut aggregator = openai_compat::response::SseAggregator::new(echo_metadata);
    for payload in sse_payloads(&body) {
        if payload == b"[DONE]" {
            continue;
        }
        let Ok(event) = sonic_rs::from_slice::<Value>(&payload) else {
            continue;
        };
        aggregator.push(&event);
    }
    match aggregator.finish() {
        openai_compat::response::AggregateOutcome::Response(value) => {
            let body = serde_json::to_vec(&value).map_err(|_| CollectError::BodyRead)?;
            Ok(json_response(StatusCode::OK, body))
        }
        openai_compat::response::AggregateOutcome::Error { status, body } => Ok(json_response(
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            body,
        )),
        openai_compat::response::AggregateOutcome::Truncated => Err(CollectError::BodyRead),
    }
}
/// Inspect only the prefix before the first output event. Once output starts,
/// the held prefix and all remaining upstream frames are relayed incrementally.
pub(crate) async fn inspect_truncation_response(
    response: Response<Body>,
    cap: usize,
) -> Result<(Response<Body>, bool), CollectError> {
    if !is_sse_content_type(response.headers()) {
        return Ok((response, false));
    }
    let (parts, body) = response.into_parts();
    let mut body = body;
    let mut held = Vec::new();
    let mut scan = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| CollectError::BodyRead)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        held.extend_from_slice(&data);
        scan.extend_from_slice(&data);
        let mut output_started = false;
        while let Some(end) = sse_event_end(&scan) {
            let event = scan.drain(..end).collect::<Vec<_>>();
            let Some(payload) = sse_data(&event) else {
                continue;
            };
            if payload == b"[DONE]" {
                continue;
            }
            let Ok(event) = sonic_rs::from_slice::<Value>(&payload) else {
                continue;
            };
            match openai_compat::response::classify_early_event(&event) {
                openai_compat::response::EarlyEvent::Output => {
                    output_started = true;
                    break;
                }
                openai_compat::response::EarlyEvent::ContextOverflow => {
                    return Ok((Response::from_parts(parts, Body::from(held)), true));
                }
                openai_compat::response::EarlyEvent::OtherTerminal
                | openai_compat::response::EarlyEvent::Pending => {}
            }
        }
        if output_started {
            let prefix = std::mem::take(&mut held);
            let stream = async_stream::stream! {
                if !prefix.is_empty() {
                    yield Ok::<Bytes, Infallible>(Bytes::from(prefix));
                }
                while let Some(frame) = body.frame().await {
                    let Ok(frame) = frame else { break };
                    let Ok(data) = frame.into_data() else { continue };
                    if !data.is_empty() {
                        yield Ok::<Bytes, Infallible>(data);
                    }
                }
            };
            return Ok((
                Response::from_parts(parts, Body::from_stream(stream)),
                false,
            ));
        }
        if held.len() > cap {
            return Ok((Response::from_parts(parts, Body::from(held)), true));
        }
    }
    Ok((Response::from_parts(parts, Body::from(held)), false))
}

fn chat_from_sse(
    plan: &crate::openai_compat::chat::ChatPlan,
    body: &[u8],
) -> Result<Vec<u8>, CollectError> {
    let mut id = "chatcmpl-compat".to_owned();
    let mut created = 0_i64;
    for payload in sse_payloads(body) {
        if payload == b"[DONE]" {
            continue;
        }
        let Ok(event) = sonic_rs::from_slice::<Value>(&payload) else {
            continue;
        };
        if let Some(response) = event.get("response") {
            if let Some(value) = response.get("id").and_then(Value::as_str) {
                id = value.to_owned();
            }
            if let Some(value) = response.get("created_at").and_then(Value::as_i64) {
                created = value;
            }
        }
    }
    let mut translator =
        crate::openai_compat::chat::ChatStreamTranslator::new(plan, id.clone(), created, 0);
    let mut stream_chunks = Vec::new();
    for payload in sse_payloads(body) {
        if payload == b"[DONE]" {
            continue;
        }
        let Ok(event) = sonic_rs::from_slice::<Value>(&payload) else {
            continue;
        };
        for chunk in translator.push(&event) {
            stream_chunks.push(chunk);
        }
    }
    if !plan.stream {
        let _ = translator.finish();
        let output = crate::openai_compat::chat::chat_completion_from_streams(
            plan,
            id,
            created,
            vec![translator],
        );
        return serde_json::to_vec(&output).map_err(|_| CollectError::BodyRead);
    }
    let usage = translator.usage();
    stream_chunks.extend(translator.finish());
    if plan.include_usage {
        if let Some(usage) = usage {
            stream_chunks.push(crate::openai_compat::chat::usage_chunk(
                &id,
                created,
                &plan.model,
                &usage,
            ));
        }
    }
    let mut output = Vec::new();
    for chunk in stream_chunks {
        let data = serde_json::to_vec(&chunk).map_err(|_| CollectError::BodyRead)?;
        output.extend_from_slice(b"data: ");
        output.extend_from_slice(&data);
        output.extend_from_slice(b"\n\n");
    }
    output.extend_from_slice(b"data: [DONE]\n\n");
    Ok(output)
}

/// Convert a translated Responses SSE stream to the public Chat Completions
/// shape. The response is buffered so translator state (stop sequences,
/// tool-call fragments, and usage) is request-scoped and deterministic.
pub(crate) fn convert_buffered_plan(
    plan: &CompatResponsePlan,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, CollectError> {
    let (status, body, content_type) = match &plan.kind {
        CompatResponseKind::ModelsList => {
            if !status.is_success() {
                let converted =
                    openai_compat::response::public_error_from_detail(status.as_u16(), &body)
                        .unwrap_or_else(|| body.to_vec());
                return Ok(response_with_headers(status, headers, converted, None));
            }
            let value = sonic_rs::from_slice::<Value>(&body).map_err(|_| CollectError::BodyRead)?;
            let Some(public) = openai_compat::models::public_model_list(&value) else {
                return Err(CollectError::BodyRead);
            };
            (
                status,
                serde_json::to_vec(&public).map_err(|_| CollectError::BodyRead)?,
                "application/json",
            )
        }
        CompatResponseKind::ModelsGet { id } => {
            if !status.is_success() {
                let converted =
                    openai_compat::response::public_error_from_detail(status.as_u16(), &body)
                        .unwrap_or_else(|| body.to_vec());
                return Ok(response_with_headers(status, headers, converted, None));
            }
            let value = sonic_rs::from_slice::<Value>(&body).map_err(|_| CollectError::BodyRead)?;
            let Some(public) = openai_compat::models::public_model(&value, id) else {
                let body = openai_compat::models::model_not_found_body(id);
                return Ok(response_with_headers(
                    StatusCode::NOT_FOUND,
                    headers,
                    body,
                    None,
                ));
            };
            (
                status,
                serde_json::to_vec(&public).map_err(|_| CollectError::BodyRead)?,
                "application/json",
            )
        }
        CompatResponseKind::Chat { plan } => {
            if !status.is_success() {
                let body =
                    openai_compat::response::public_error_from_detail(status.as_u16(), &body)
                        .unwrap_or_else(|| body.to_vec());
                return Ok(response_with_headers(status, headers, body, None));
            }
            let body = chat_from_sse(plan, &body)?;
            let content_type = if plan.stream {
                "text/event-stream"
            } else {
                "application/json"
            };
            (StatusCode::OK, body, content_type)
        }
        CompatResponseKind::ImagesStream { kind } => {
            if !status.is_success() {
                let body =
                    openai_compat::response::public_error_from_detail(status.as_u16(), &body)
                        .unwrap_or_else(|| body.to_vec());
                return Ok(response_with_headers(status, headers, body, None));
            }
            let value = sonic_rs::from_slice::<Value>(&body).map_err(|_| CollectError::BodyRead)?;
            (
                status,
                openai_compat::images::image_json_to_sse(*kind, &value),
                "text/event-stream",
            )
        }
        CompatResponseKind::Transcription { format } => {
            let Some((content_type, body)) =
                openai_compat::transcription::transcription_response(*format, &body)
            else {
                return Err(CollectError::BodyRead);
            };
            (status, body, content_type)
        }
        _ => {
            if !status.is_success() {
                let body =
                    openai_compat::response::public_error_from_detail(status.as_u16(), &body)
                        .unwrap_or_else(|| body.to_vec());
                return Ok(response_with_headers(status, headers, body, None));
            }
            return Ok(response_with_headers(status, headers, body.to_vec(), None));
        }
    };
    Ok(response_with_headers(
        status,
        headers,
        body,
        Some(content_type),
    ))
}

/// Apply the stream-side compatibility rewrite to one complete SSE event.
/// `None` means the event is intentionally suppressed.
pub(crate) fn transform_sse_event(
    raw: Bytes,
    echo_metadata: Option<&Value>,
    normalize_events: bool,
) -> Option<Bytes> {
    let (event_name, data) = parse_sse_event(&raw)?;
    if normalize_events && openai_compat::response::drop_sse_event(&event_name) {
        return None;
    }
    let Some(metadata) = echo_metadata else {
        return Some(raw);
    };
    let mut value = sonic_rs::from_slice::<Value>(&data).ok()?;
    openai_compat::response::echo_metadata_into(&mut value, metadata);
    let encoded = serde_json::to_vec(&value).ok()?;
    Some(format_sse_event(&event_name, &encoded))
}

pub(crate) fn normalize_detail_body(status: StatusCode, body: &[u8]) -> Option<Bytes> {
    openai_compat::response::public_error_from_detail(status.as_u16(), body).map(Bytes::from)
}

fn json_response(status: StatusCode, body: Vec<u8>) -> Response<Body> {
    response_with_headers(status, HeaderMap::new(), body, Some("application/json"))
}

fn response_with_headers(
    status: StatusCode,
    mut headers: HeaderMap,
    body: Vec<u8>,
    content_type: Option<&'static str>,
) -> Response<Body> {
    headers.remove(CONTENT_LENGTH);
    headers.remove(http::header::CONTENT_ENCODING);
    if let Some(content_type) = content_type {
        headers.insert(CONTENT_TYPE, http::HeaderValue::from_static(content_type));
    }
    headers.insert(
        CONTENT_LENGTH,
        http::HeaderValue::from_str(&body.len().to_string())
            .expect("body length is a valid header value"),
    );
    let mut builder = Response::builder().status(status);
    *builder.headers_mut().expect("response builder headers") = headers;
    builder
        .body(Body::from(body))
        .expect("response body construction")
}
pub(crate) fn compact_response_body(
    original_request: &Value,
    aggregated: &[u8],
) -> Result<Vec<u8>, CollectError> {
    let aggregated =
        sonic_rs::from_slice::<Value>(aggregated).map_err(|_| CollectError::BodyRead)?;
    let compact = openai_compat::compact::compaction_resource(original_request, &aggregated);
    serde_json::to_vec(&compact).map_err(|_| CollectError::BodyRead)
}

pub(crate) fn requires_buffered_response(plan: &CompatResponsePlan) -> bool {
    matches!(
        &plan.kind,
        CompatResponseKind::ResponsesAggregate { .. }
            | CompatResponseKind::Compact { .. }
            | CompatResponseKind::Chat { .. }
            | CompatResponseKind::ModelsList
            | CompatResponseKind::ModelsGet { .. }
            | CompatResponseKind::ImagesStream { .. }
            | CompatResponseKind::Transcription { .. }
            | CompatResponseKind::PassthroughNormalizeErrors
    )
}

pub(crate) fn stream_echo_metadata(plan: &CompatResponsePlan) -> Option<&Value> {
    match &plan.kind {
        CompatResponseKind::ResponsesStream { echo_metadata } => echo_metadata.as_ref(),
        _ => None,
    }
}

pub(crate) fn normalize_stream_events(plan: &CompatResponsePlan) -> bool {
    matches!(
        &plan.kind,
        CompatResponseKind::ResponsesStream { .. } | CompatResponseKind::PassthroughNormalizeErrors
    )
}
pub(crate) struct CompatSseTransformHook {
    pub(crate) echo_metadata: Option<Value>,
    pub(crate) normalize_events: bool,
}

impl cc_lb_upstream::SseEventTransformHook for CompatSseTransformHook {
    fn transform_sse_event(
        &self,
        request: cc_lb_upstream::TransformSseEventRequest,
    ) -> Result<cc_lb_upstream::TransformSseEventResult, cc_lb_upstream::ResponseTransformError>
    {
        if self.normalize_events && openai_compat::response::drop_sse_event(&request.event.event) {
            return Ok(cc_lb_upstream::TransformSseEventResult::Drop);
        }
        let Some(metadata) = self.echo_metadata.as_ref() else {
            return Ok(cc_lb_upstream::TransformSseEventResult::Unchanged);
        };
        let Ok(mut value) = sonic_rs::from_slice::<Value>(&request.event.data) else {
            return Ok(cc_lb_upstream::TransformSseEventResult::Unchanged);
        };
        openai_compat::response::echo_metadata_into(&mut value, metadata);
        let data = serde_json::to_vec(&value).map_err(|error| {
            cc_lb_upstream::ResponseTransformError::Runtime {
                reason: format!("failed to encode compatibility SSE event: {error}"),
            }
        })?;
        Ok(cc_lb_upstream::TransformSseEventResult::Replace {
            events: vec![cc_lb_upstream::SseEvent {
                event: request.event.event,
                data: Bytes::from(data),
            }],
        })
    }
}

fn is_sse_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"))
        })
}

fn sse_event_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| index + 2)
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
        })
}

fn sse_payloads(body: &[u8]) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    let mut start = 0;
    for (index, window) in body.windows(2).enumerate() {
        if window == b"\n\n" {
            if let Some(payload) = sse_data(&body[start..index]) {
                payloads.push(payload);
            }
            start = index + 2;
        }
    }
    if start < body.len() {
        if let Some(payload) = sse_data(&body[start..]) {
            payloads.push(payload);
        }
    }
    payloads
}

fn sse_data(frame: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(frame).ok()?;
    let mut data = Vec::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(value) = line.strip_prefix("data:") else {
            continue;
        };
        if !data.is_empty() {
            data.push(b'\n');
        }
        data.extend_from_slice(value.strip_prefix(' ').unwrap_or(value).as_bytes());
    }
    (!data.is_empty()).then_some(data)
}

fn parse_sse_event(raw: &[u8]) -> Option<(String, Vec<u8>)> {
    let text = std::str::from_utf8(raw).ok()?;
    let mut event = String::new();
    let mut data = Vec::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(value) = line.strip_prefix("event:") {
            event = value.trim_start().to_owned();
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push(b'\n');
            }
            data.extend_from_slice(value.strip_prefix(' ').unwrap_or(value).as_bytes());
        }
    }
    Some((event, data))
}

fn format_sse_event(event: &str, data: &[u8]) -> Bytes {
    let mut output = Vec::with_capacity(event.len() + data.len() + 16);
    if !event.is_empty() {
        output.extend_from_slice(b"event: ");
        output.extend_from_slice(event.as_bytes());
        output.extend_from_slice(b"\n");
    }
    for line in data.split(|byte| *byte == b'\n') {
        output.extend_from_slice(b"data: ");
        output.extend_from_slice(line);
        output.extend_from_slice(b"\n");
    }
    output.extend_from_slice(b"\n");
    Bytes::from(output)
}
