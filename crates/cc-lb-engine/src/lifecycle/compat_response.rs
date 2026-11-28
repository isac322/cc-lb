use axum::body::Body;
use bytes::{Bytes, BytesMut};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::convert::Infallible;

use crate::lifecycle::compat_request::{CompatResponseKind, CompatResponsePlan};
use crate::openai_compat;
use crate::openai_compat::chat::{sse_data, sse_event_end, sse_payloads};

pub(crate) const CHAT_STREAM_PREFIX_CAP_BYTES: usize = 1024 * 1024;

/// Maximum number of bytes retained while collecting a response for a
/// compatibility conversion. The caller supplies the configured request cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CollectError {
    BodyTooLarge,
    BodyRead,
    IncompleteStream,
}

impl CollectError {
    /// Public error message for a failed compatibility conversion.
    pub(crate) fn public_message(&self) -> &'static str {
        match self {
            Self::BodyTooLarge => "upstream response exceeded the compatibility buffer",
            Self::BodyRead => "upstream response body could not be read",
            Self::IncompleteStream => "upstream response ended before a terminal event",
        }
    }
}

/// Stack-local body collection timings for buffered compatibility paths (`ResponsesAggregate`
/// and `Compact`), avoiding per-frame locks and heartbeat `select!` polling on the proxy path.
pub(crate) struct CompatBodyTiming {
    pub(crate) collect_started: std::time::Instant,
    wait_elapsed: std::time::Duration,
    wait_started: bool,
    active_wait_started: Option<std::time::Instant>,
    process_elapsed: std::time::Duration,
    process_started: bool,
    pub(crate) first_body_chunk_at: Option<std::time::Instant>,
    pub(crate) body_chunk_count: u64,
}

impl CompatBodyTiming {
    pub(crate) fn start() -> Self {
        Self {
            collect_started: std::time::Instant::now(),
            wait_elapsed: std::time::Duration::ZERO,
            wait_started: false,
            active_wait_started: None,
            process_elapsed: std::time::Duration::ZERO,
            process_started: false,
            first_body_chunk_at: None,
            body_chunk_count: 0,
        }
    }

    pub(crate) fn snapshot(&self, now: std::time::Instant) -> cc_lb_lifecycle::RequestIoTimings {
        let mut wait_elapsed = self.wait_elapsed;
        if let Some(started_at) = self.active_wait_started {
            wait_elapsed = wait_elapsed.saturating_add(now.saturating_duration_since(started_at));
        }
        cc_lb_lifecycle::RequestIoTimings {
            response_body_wait_ms: self
                .wait_started
                .then_some(wait_elapsed.as_secs_f64() * 1_000.0),
            response_body_process_ms: self
                .process_started
                .then_some(self.process_elapsed.as_secs_f64() * 1_000.0),
            ..Default::default()
        }
    }
}

/// Collect an upstream body without allowing an unbounded compatibility buffer.
pub(crate) async fn collect_body_with_timing(
    body: Body,
    cap: usize,
    mut timing: Option<&mut CompatBodyTiming>,
) -> Result<Bytes, CollectError> {
    let mut body = body;
    let mut bytes = BytesMut::new();
    loop {
        let (frame, frame_ready_at) = match timing.as_deref_mut() {
            Some(stats) => {
                let wait_started_at = std::time::Instant::now();
                stats.wait_started = true;
                stats.active_wait_started = Some(wait_started_at);
                let frame = body.frame().await;
                let ready_at = std::time::Instant::now();
                stats.active_wait_started = None;
                stats.wait_elapsed = stats
                    .wait_elapsed
                    .saturating_add(ready_at.saturating_duration_since(wait_started_at));
                (frame, Some(ready_at))
            }
            None => (body.frame().await, None),
        };
        let Some(frame) = frame else {
            break;
        };
        let result = match frame {
            Ok(frame) => match frame.into_data() {
                Ok(data) => {
                    if let (Some(stats), Some(ready_at)) = (timing.as_deref_mut(), frame_ready_at) {
                        stats.first_body_chunk_at.get_or_insert(ready_at);
                        stats.body_chunk_count = stats.body_chunk_count.saturating_add(1);
                    }
                    if data.len() > cap.saturating_sub(bytes.len()) {
                        Err(CollectError::BodyTooLarge)
                    } else {
                        bytes.extend_from_slice(&data);
                        Ok(())
                    }
                }
                Err(_) => Ok(()),
            },
            Err(_) => Err(CollectError::BodyRead),
        };
        if let (Some(stats), Some(ready_at)) = (timing.as_deref_mut(), frame_ready_at) {
            stats.process_started = true;
            stats.process_elapsed = stats.process_elapsed.saturating_add(ready_at.elapsed());
        }
        result?;
    }
    Ok(bytes.freeze())
}

pub(crate) async fn collect_body(body: Body, cap: usize) -> Result<Bytes, CollectError> {
    collect_body_with_timing(body, cap, None).await
}

pub(crate) struct AggregatedResponse {
    pub(crate) response: Response<Body>,
    pub(crate) observed_usage: Option<crate::usage_parser::UsageCounts>,
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
    mut timing: Option<&mut CompatBodyTiming>,
    tier_policy: crate::usage_parser::ServiceTierMergePolicy,
) -> Result<AggregatedResponse, CollectError> {
    let (parts, body) = response.into_parts();
    let status = parts.status;
    let headers = parts.headers;
    let body = collect_body_with_timing(body, cap, timing.as_deref_mut()).await?;

    let process_started_at = timing.as_ref().map(|_| std::time::Instant::now());
    let aggregated = finish_aggregate_response(status, &headers, &body, echo_metadata, tier_policy);
    if let (Some(stats), Some(started_at)) = (timing, process_started_at) {
        stats.process_started = true;
        stats.process_elapsed = stats.process_elapsed.saturating_add(started_at.elapsed());
    }
    aggregated
}

fn finish_aggregate_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
    echo_metadata: Option<Value>,
    tier_policy: crate::usage_parser::ServiceTierMergePolicy,
) -> Result<AggregatedResponse, CollectError> {
    if !is_sse_content_type(headers) {
        if !status.is_success()
            && let Some(public) =
                openai_compat::response::public_error_from_detail(status.as_u16(), body)
        {
            return Ok(AggregatedResponse {
                response: json_response(status, public),
                observed_usage: None,
            });
        }
        return Ok(AggregatedResponse {
            response: json_response(status, body.to_vec()),
            observed_usage: None,
        });
    }

    let mut aggregator = openai_compat::response::SseAggregator::new(echo_metadata);
    let mut sse_usage = crate::usage_parser::UsageCounts::default();
    for payload in sse_payloads(body) {
        if payload == b"[DONE]" {
            continue;
        }
        let Ok(event) = sonic_rs::from_slice::<Value>(&payload) else {
            continue;
        };
        crate::usage_parser::accumulate_sse_usage_value_for_shape_with_policy(
            &event,
            None,
            &mut sse_usage,
            crate::provider_capability::UsageShape::OpenaiResponses,
            tier_policy,
        );
        aggregator.push(&event);
    }
    match aggregator.finish() {
        openai_compat::response::AggregateOutcome::Response(value) => {
            let body = serde_json::to_vec(&value).map_err(|_| CollectError::BodyRead)?;
            Ok(AggregatedResponse {
                response: json_response(StatusCode::OK, body),
                observed_usage: sse_usage.present.then_some(sse_usage),
            })
        }
        openai_compat::response::AggregateOutcome::Error { status, body } => {
            Ok(AggregatedResponse {
                response: json_response(
                    StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                    body,
                ),
                observed_usage: None,
            })
        }
        openai_compat::response::AggregateOutcome::Truncated => Err(CollectError::IncompleteStream),
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
        let remaining_cap = cap.saturating_sub(held.len());
        let inspect_len = data.len().min(remaining_cap);
        let inspect_slice = &data[..inspect_len];
        held.extend_from_slice(&data);
        scan.extend_from_slice(inspect_slice);
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
            return Err(CollectError::BodyTooLarge);
        }
    }
    Ok((Response::from_parts(parts, Body::from(held)), false))
}
/// Result of preflighting a translated Chat Completions stream.
pub(crate) enum ChatStreamInspection {
    /// The stream produced output (or a terminal success event); relay it
    /// incrementally after the held prefix.
    Ready(Response<Body>),
    /// The stream failed before producing any output; return this public error
    /// response instead of committing the translated HTTP 200 headers.
    Failed(Response<Body>),
}

/// Hold a translated Chat stream until its first output-bearing event or a
/// terminal event. This is the only point where a provider failure can still
/// become an HTTP error: once the response headers are sent, later SSE errors
/// must remain in-band.
pub(crate) async fn inspect_chat_stream_response(
    response: Response<Body>,
    cap: usize,
) -> Result<ChatStreamInspection, CollectError> {
    let (parts, body) = response.into_parts();
    let mut body = body;
    // Held frames keep their original `Bytes` (no copy). Only the unparsed
    // tail is copied into `scan`, and the budget is checked before a frame is
    // retained, so the prefix never holds more than `cap` bytes plus one frame.
    let mut held: Vec<Bytes> = Vec::new();
    let mut held_len = 0usize;
    let mut scan = Vec::new();
    let mut decided = false;
    while !decided {
        let Some(frame) = body.frame().await else {
            break;
        };
        let frame = frame.map_err(|_| CollectError::BodyRead)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        held_len += data.len();
        if held_len > cap {
            // Over budget: relay this frame unscanned and stop holding.
            held.push(data);
            break;
        }
        scan.extend_from_slice(&data);
        held.push(data);
        while let Some(end) = sse_event_end(&scan) {
            let event = scan.drain(..end).collect::<Vec<_>>();
            let Some(payload) = sse_data(&event) else {
                continue;
            };
            if payload == b"[DONE]" {
                decided = true;
                break;
            }
            let Ok(event) = sonic_rs::from_slice::<Value>(&payload) else {
                continue;
            };
            let kind = event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if matches!(kind, "response.failed" | "error") {
                let (status, body) = chat_failure(&event).unwrap_or((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::to_vec(&json!({
                        "error": {
                            "message": "The upstream request failed.",
                            "type": "server_error",
                            "param": null,
                            "code": "server_error"
                        }
                    }))
                    .unwrap_or_default(),
                ));
                return Ok(ChatStreamInspection::Failed(json_response(status, body)));
            }
            if matches!(
                kind,
                "response.output_text.delta"
                    | "response.refusal.delta"
                    | "response.function_call_arguments.delta"
            ) || (kind == "response.output_item.added"
                && event
                    .get("item")
                    .and_then(|item| item.get("type"))
                    .and_then(Value::as_str)
                    == Some("function_call"))
            {
                decided = true;
                break;
            }
            if matches!(kind, "response.completed" | "response.incomplete") {
                decided = true;
                break;
            }
        }
    }
    let stream = async_stream::stream! {
        for data in held {
            if !data.is_empty() {
                yield Ok::<Bytes, Infallible>(data);
            }
        }
        while let Some(frame) = body.frame().await {
            let Ok(frame) = frame else { break };
            let Ok(data) = frame.into_data() else { continue };
            if !data.is_empty() {
                yield Ok::<Bytes, Infallible>(data);
            }
        }
    };
    Ok(ChatStreamInspection::Ready(Response::from_parts(
        parts,
        Body::from_stream(stream),
    )))
}

#[derive(Debug)]
enum ChatConversionError {
    Collect(CollectError),
    Public { status: StatusCode, body: Vec<u8> },
}

fn chat_failure(error_event: &Value) -> Option<(StatusCode, Vec<u8>)> {
    let mut aggregator = openai_compat::response::SseAggregator::new(None);
    aggregator.push(error_event);
    match aggregator.finish() {
        openai_compat::response::AggregateOutcome::Error { status, body } => Some((
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            body,
        )),
        _ => None,
    }
}

fn chat_from_sse(
    plan: &crate::openai_compat::chat::ChatPlan,
    body: &[u8],
) -> Result<Vec<u8>, ChatConversionError> {
    let (id, created) = crate::openai_compat::chat::new_identity();
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
        stream_chunks.extend(translator.push(&event));
        if translator.failure().is_some() {
            let (status, body) = chat_failure(&event).unwrap_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::to_vec(&json!({
                    "error": {
                        "message": "The upstream request failed.",
                        "type": "server_error",
                        "param": null,
                        "code": "server_error"
                    }
                }))
                .unwrap_or_default(),
            ));
            return Err(ChatConversionError::Public { status, body });
        }
    }
    if !translator.terminal_seen() && !translator.stopped() {
        return Err(ChatConversionError::Collect(CollectError::IncompleteStream));
    }
    if !plan.stream {
        translator.finish();
        let output = crate::openai_compat::chat::chat_completion_from_streams(
            plan,
            id,
            created,
            vec![translator],
        );
        return serde_json::to_vec(&output)
            .map_err(|_| ChatConversionError::Collect(CollectError::BodyRead));
    }
    let usage = translator.usage();
    stream_chunks.extend(translator.finish());
    if plan.include_usage
        && !translator.stopped()
        && let Some(usage) = usage
    {
        stream_chunks.push(crate::openai_compat::chat::usage_chunk(
            &id,
            created,
            &plan.model,
            &usage,
        ));
    }
    let mut output = Vec::new();
    for chunk in stream_chunks {
        let data = serde_json::to_vec(&chunk)
            .map_err(|_| ChatConversionError::Collect(CollectError::BodyRead))?;
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
    is_model_allowed: &dyn Fn(&str) -> bool,
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
            let Some(public) = openai_compat::models::public_model_list(&value, is_model_allowed)
            else {
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
            if !is_model_allowed(id) {
                let body = openai_compat::models::model_not_found_body(id);
                return Ok(response_with_headers(
                    StatusCode::NOT_FOUND,
                    headers,
                    body,
                    None,
                ));
            }
            let value = sonic_rs::from_slice::<Value>(&body).map_err(|_| CollectError::BodyRead)?;
            let Some(public) = openai_compat::models::public_model(&value, id, is_model_allowed)
            else {
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
            let body = match chat_from_sse(plan, &body) {
                Ok(body) => body,
                Err(ChatConversionError::Public { status, body }) => {
                    return Ok(response_with_headers(status, headers, body, None));
                }
                Err(ChatConversionError::Collect(error)) => return Err(error),
            };
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
            if !status.is_success() {
                let body =
                    openai_compat::response::public_error_from_detail(status.as_u16(), &body)
                        .unwrap_or_else(|| body.to_vec());
                return Ok(response_with_headers(status, headers, body, None));
            }
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
pub(crate) fn compact_response_body(aggregated: &[u8]) -> Result<Vec<u8>, CollectError> {
    let aggregated =
        sonic_rs::from_slice::<Value>(aggregated).map_err(|_| CollectError::BodyRead)?;
    let compact = openai_compat::compact::compaction_resource(&aggregated);
    serde_json::to_vec(&compact).map_err(|_| CollectError::BodyRead)
}

pub(crate) fn requires_buffered_response(plan: &CompatResponsePlan) -> bool {
    match &plan.kind {
        CompatResponseKind::ResponsesAggregate { .. }
        | CompatResponseKind::Compact
        | CompatResponseKind::ModelsList
        | CompatResponseKind::ModelsGet { .. }
        | CompatResponseKind::ImagesStream { .. }
        | CompatResponseKind::Transcription { .. } => true,
        CompatResponseKind::Chat { plan } => !plan.stream || plan.n > 1,
        CompatResponseKind::ResponsesStream { .. } => false,
    }
}

pub(crate) fn stream_echo_metadata(plan: &CompatResponsePlan) -> Option<&Value> {
    match &plan.kind {
        CompatResponseKind::ResponsesStream { echo_metadata } => echo_metadata.as_ref(),
        _ => None,
    }
}

pub(crate) fn normalize_stream_events(plan: &CompatResponsePlan) -> bool {
    matches!(&plan.kind, CompatResponseKind::ResponsesStream { .. })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn chat_plan(stream: bool) -> crate::openai_compat::chat::ChatPlan {
        crate::openai_compat::chat::ChatPlan {
            n: 1,
            stop: Vec::new(),
            stream,
            include_usage: false,
            model: "m".to_owned(),
        }
    }

    #[test]
    fn sse_payloads_accept_crlf_delimiters() {
        let body = b"data: {\"type\":\"response.completed\"}\r\n\r\n\
data: [DONE]\r\n\r\n";
        let payloads = sse_payloads(body);
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0], br#"{"type":"response.completed"}"#);
        assert_eq!(payloads[1], b"[DONE]");
    }

    #[test]
    fn buffered_chat_failure_maps_context_overflow_to_400() {
        let body = br#"data: {"type":"response.failed","response":{"error":{"code":"context_length_exceeded","message":"too long"}}}

"#;
        let error = chat_from_sse(&chat_plan(false), body).expect_err("failure");
        let ChatConversionError::Public { status, body } = error else {
            panic!("expected public error");
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let value: Value = sonic_rs::from_slice(&body).expect("error JSON");
        assert_eq!(value["error"]["code"], "context_length_exceeded");
    }

    #[test]
    fn buffered_chat_truncation_is_not_a_partial_success() {
        let body = br#"data: {"type":"response.output_text.delta","delta":"partial"}

"#;
        let error = chat_from_sse(&chat_plan(false), body).expect_err("truncated");
        assert!(matches!(
            error,
            ChatConversionError::Collect(CollectError::IncompleteStream)
        ));
    }

    #[tokio::test]
    async fn chat_stream_prefix_failure_becomes_public_http_error() {
        let response = Response::builder()
            .body(Body::from(
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"too long\"}}}\n\n",
            ))
            .expect("response");
        let result = inspect_chat_stream_response(response, 4096)
            .await
            .expect("inspection");
        let ChatStreamInspection::Failed(response) = result else {
            panic!("expected public failure response");
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = sonic_rs::from_slice(&body).expect("JSON error");
        assert_eq!(value["error"]["code"], "context_length_exceeded");
    }
    #[tokio::test]
    async fn chat_stream_prefix_over_cap_commits_headers_and_relay() {
        let response = Response::builder()
            .body(Body::from(format!(
                "data: {{\"type\":\"response.created\",\"padding\":\"{}\"}}\n\n",
                "x".repeat(32)
            )))
            .expect("response");
        let result = inspect_chat_stream_response(response, 8)
            .await
            .expect("inspection");
        let ChatStreamInspection::Ready(response) = result else {
            panic!("expected committed stream");
        };
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn chat_stream_prefix_oversized_frame_is_relayed_without_buffering() {
        let frame = format!(
            "data: {{\"type\":\"error\",\"padding\":\"{}\"}}\n\n",
            "x".repeat(64)
        );
        let response = Response::builder()
            .body(Body::from(frame.clone()))
            .expect("response");
        let result = inspect_chat_stream_response(response, 16)
            .await
            .expect("inspection");
        // A frame past the hold budget is committed and relayed intact, not
        // copied in for scanning first.
        let ChatStreamInspection::Ready(response) = result else {
            panic!("expected committed stream");
        };
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body, Bytes::from(frame));
    }

    #[tokio::test]
    async fn chat_stream_prefix_output_remains_incremental() {
        let response = Response::builder()
            .body(Body::from_stream(async_stream::stream! {
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"data: {\"type\":\"response.created\",\"response\":{}}\n\n",
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n\
                       data: {\"type\":\"response.completed\",\"response\":{}}\n\n",
                ));
            }))
            .expect("response");
        let result = inspect_chat_stream_response(response, 4096)
            .await
            .expect("inspection");
        let ChatStreamInspection::Ready(response) = result else {
            panic!("expected ready stream");
        };
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert!(
            body.windows(b"response.output_text.delta".len())
                .any(|window| window == b"response.output_text.delta")
        );
    }

    #[tokio::test]
    async fn truncation_prefix_over_cap_is_body_error_not_retryable_overflow() {
        let response = Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(Body::from(
                "data: {\"type\":\"response.created\",\"response\":{}}\n\n",
            ))
            .expect("response");
        let error = inspect_truncation_response(response, 8)
            .await
            .expect_err("cap");
        assert_eq!(error, CollectError::BodyTooLarge);
    }

    #[tokio::test]
    async fn truncation_prefix_allows_coalesced_chunk_when_first_output_fits_cap() {
        let first_output = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n";
        let trailing = format!(
            "data: {{\"type\":\"response.output_text.delta\",\"delta\":\"{}\"}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{}}}}\n\n",
            "x".repeat(256)
        );
        let full_chunk = format!("{first_output}{trailing}");
        let cap = first_output.len() + 16;
        assert!(full_chunk.len() > cap);
        let response = Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(Body::from(full_chunk.clone()))
            .expect("response");
        let (relayed, overflow) = inspect_truncation_response(response, cap)
            .await
            .expect("first output within prefix cap should relay full chunk");
        assert!(!overflow);
        let collected = relayed
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        assert_eq!(collected, Bytes::from(full_chunk));
    }

    #[tokio::test]
    async fn disallowed_model_get_returns_model_not_found() {
        let plan = CompatResponsePlan {
            kind: CompatResponseKind::ModelsGet { id: "gpt-x".into() },
            truncation_emulate: false,
        };
        let response = convert_buffered_plan(
            &plan,
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from_static(br#"{"models":[{"slug":"gpt-x"}]}"#),
            &|_| false,
        )
        .expect("model response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = sonic_rs::from_slice(&body).expect("error JSON");
        assert_eq!(value["error"]["code"], "model_not_found");
    }

    #[tokio::test]
    async fn aggregate_response_preserves_earlier_oauth_priority_when_terminal_event_is_default_without_mutating_body()
     {
        use crate::usage_parser::ServiceTierMergePolicy;

        let sse = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_agg_prio\",\"object\":\"response\",\"status\":\"in_progress\",\"service_tier\":\"auto\"}}\n\n",
            "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_agg_prio\",\"object\":\"response\",\"status\":\"in_progress\",\"service_tier\":\"priority\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_agg_prio\",\"object\":\"response\",\"status\":\"completed\",\"service_tier\":\"default\",\"usage\":{\"input_tokens\":10,\"output_tokens\":8,\"total_tokens\":18}}}\n\n",
        );
        let response = Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(Body::from(sse))
            .expect("response");
        let AggregatedResponse {
            response: aggregated,
            observed_usage,
        } = aggregate_response(
            response,
            None,
            65_536,
            None,
            ServiceTierMergePolicy::OpenaiOauth,
        )
        .await
        .expect("aggregates");
        let usage = observed_usage.expect("observed usage");
        assert_eq!(usage.service_tier.as_deref(), Some("priority"));
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 8);

        let body = aggregated
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let client_json: Value = sonic_rs::from_slice(&body).expect("client JSON");
        assert_eq!(
            client_json["service_tier"].as_str(),
            Some("default"),
            "client-visible aggregated response body must remain untouched"
        );
    }
}
