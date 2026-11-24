use super::types::CollectedResponse;
use super::*;

pub(super) fn new_activity_heartbeat_interval() -> tokio::time::Interval {
    let period = cc_lb_lifecycle::DEFAULT_STREAM_ACTIVITY_HEARTBEAT_INTERVAL;
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

pub(super) fn maybe_emit_chunk_heartbeat(
    now: Instant,
    last_heartbeat_at: &mut Instant,
    heartbeat: &mut tokio::time::Interval,
    observer: Option<&LifecycleContext>,
) {
    if now.saturating_duration_since(*last_heartbeat_at)
        >= cc_lb_lifecycle::DEFAULT_STREAM_ACTIVITY_HEARTBEAT_INTERVAL
    {
        *last_heartbeat_at = now;
        heartbeat.reset();
        if let Some(o) = observer {
            o.emit_request_activity();
        }
    }
}

pub(super) async fn drive_with_activity_heartbeat<F: Future>(
    fut: F,
    observer: Option<&LifecycleContext>,
) -> F::Output {
    let Some(observer) = observer else {
        return fut.await;
    };
    let mut heartbeat = new_activity_heartbeat_interval();
    tokio::pin!(fut);
    loop {
        tokio::select! {
            biased;
            output = &mut fut => return output,
            _ = heartbeat.tick() => observer.emit_request_activity(),
        }
    }
}

pub(super) async fn timed_body_frame_with_heartbeat<B>(
    body: &mut B,
    timing: &BodyIoTiming,
    observer: Option<&LifecycleContext>,
    last_heartbeat_at: &mut Instant,
    heartbeat: &mut tokio::time::Interval,
) -> Option<Result<http_body::Frame<B::Data>, B::Error>>
where
    B: http_body::Body + Unpin,
{
    let frame_fut = timed_body_frame(body, timing);
    tokio::pin!(frame_fut);
    loop {
        tokio::select! {
            biased;
            frame = &mut frame_fut => return frame,
            _ = heartbeat.tick() => {
                *last_heartbeat_at = Instant::now();
                if let Some(o) = observer {
                    o.emit_request_activity();
                }
            }
        }
    }
}

pub(super) const DECOMPRESSION_OUTPUT_BUDGET_BODY_CAP_MULTIPLIER: usize = 4;
pub(super) const INCOMPLETE_SSE_EVENT_BUDGET_BODY_CAP_MULTIPLIER: usize = 1;
pub(super) const SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES: usize = 1024 * 1024;
pub(super) const SSE_BUFFER_SMALL_EVENT_CAPACITY_FRACTION: usize = 8;
// `upstream_response` names the proxy leg where decoding failed. It does not
// attribute the malformed bytes to the provider: transport corruption or a
// local decoder defect can produce the same classification.
pub(super) const UPSTREAM_RESPONSE_BODY_ERROR_TYPE: &str = "upstream_response_body_error";
pub(super) const UPSTREAM_RESPONSE_DECODE_ERROR_TYPE: &str = "upstream_response_decode_error";
pub(super) const UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE: &str = "upstream_response_framing_error";
pub(super) const UPSTREAM_RESPONSE_INCOMPLETE_ERROR_TYPE: &str = "upstream_response_incomplete";
pub(super) const UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE: &str = "api_error";
pub(super) const UPSTREAM_AFFINITY_ERROR_TYPE: &str = "upstream_affinity_error";
pub(super) const UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE: &str =
    "upstream affinity is unavailable for opaque request content";
pub(super) const UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE: &str =
    "too many upstream affinity keys in request";
pub(super) const UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE: &str =
    "the upstream that owns the referenced provider state is not eligible for this request";
pub(super) const UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE: &str =
    "upstream affinity is not established for opaque request content";
pub(super) const SINGLE_OWNER_AMBIGUOUS_MESSAGE: &str = "this OpenAI resource belongs to one API account, but \
     more than one OpenAI API-key upstream is eligible; restrict the API key to one";
pub(super) const UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE: &str =
    "upstream SSE response could not be safely processed";
pub(super) const UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE: &str =
    "upstream response body could not be read";

pub(super) fn is_upstream_response_decode_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::UnexpectedEof
    )
}

pub(super) const UPSTREAM_DIAGNOSTIC_HEADER_MAX_BYTES: usize = 256;

/// True when the unconsumed SSE bytes hold no partial event: only line
/// terminators remain, matching the end-of-stream framing check.
pub(super) fn sse_buffer_is_blank(buffer: &[u8]) -> bool {
    buffer.iter().all(|byte| matches!(byte, b'\r' | b'\n'))
}

/// Accept-with-warning predicate for an upstream SSE body that fails after
/// cc-lb already parsed the upstream `message_stop` (#663). The caller adds
/// the emitter-of-record and error-class conditions specific to its failure
/// site.
pub(super) fn upstream_end_after_message_stop_is_acceptable(
    status: StatusCode,
    semantic_message_stop_seen: bool,
    terminal_error_recorded: bool,
    provider_error_seen: bool,
    warning_recorded: bool,
    unparsed_sse: &[u8],
) -> bool {
    status.is_success()
        && semantic_message_stop_seen
        && !terminal_error_recorded
        && !provider_error_seen
        && !warning_recorded
        && sse_buffer_is_blank(unparsed_sse)
}

pub(super) fn bounded_diagnostic_header(value: Option<&HeaderValue>) -> Option<String> {
    let text = value?.to_str().ok()?;
    // `to_str` only accepts visible ASCII, so every index is a char boundary.
    text.get(..text.len().min(UPSTREAM_DIAGNOSTIC_HEADER_MAX_BYTES))
        .map(str::to_owned)
}

/// Upstream body-end evidence collected while relaying an SSE response.
/// Header values are kept as cheap `HeaderValue` clones and only rendered
/// into strings when the body ended abnormally.
pub(super) struct UpstreamBodyDiagnostics {
    pub(super) http_version: http::Version,
    pub(super) request_id: Option<HeaderValue>,
    pub(super) content_encoding: Option<HeaderValue>,
    pub(super) content_length: Option<u64>,
    pub(super) body_end: Option<cc_lb_lifecycle::UpstreamBodyEnd>,
    pub(super) error: Option<StreamErrorClassification>,
    pub(super) warning: Option<(&'static str, String)>,
}

impl UpstreamBodyDiagnostics {
    pub(super) fn from_parts(parts: &http::response::Parts) -> Self {
        Self {
            http_version: parts.version,
            request_id: parts.headers.get("request-id").cloned(),
            content_encoding: parts.headers.get(http::header::CONTENT_ENCODING).cloned(),
            content_length: parts
                .headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse().ok()),
            body_end: None,
            error: None,
            warning: None,
        }
    }

    /// Records how the upstream body ended. The first abnormal end wins, so a
    /// transport error followed by a decoder finish failure stays a transport
    /// error.
    pub(super) fn record_body_end(
        &mut self,
        body_end: cc_lb_lifecycle::UpstreamBodyEnd,
        classify: impl FnOnce() -> StreamErrorClassification,
    ) {
        if self.body_end.is_none() {
            self.body_end = Some(body_end);
            self.error = Some(classify());
        }
    }

    pub(super) fn record_warning(&mut self, warning_type: &'static str, message: String) {
        if self.warning.is_none() {
            self.warning = Some((warning_type, message));
        }
    }

    pub(super) fn warning_recorded(&self) -> bool {
        self.warning.is_some()
    }

    /// Renders the lifecycle payload, or `None` when the body ended cleanly.
    pub(super) fn into_lifecycle(
        self,
        body_bytes: u64,
        message_stop_ms: Option<u64>,
    ) -> Option<cc_lb_lifecycle::UpstreamStreamDiagnostics> {
        let body_end = self.body_end?;
        let (body_error_cause, body_error_io_kind, body_error_h2_reason) = match self.error {
            Some(error) => (
                Some(error.cause.as_str().to_owned()),
                error.io_kind,
                error.h2_reason,
            ),
            None => (None, None, None),
        };
        let (warning_type, warning_message) = match self.warning {
            Some((warning_type, message)) => (Some(warning_type.to_owned()), Some(message)),
            None => (None, None),
        };
        Some(cc_lb_lifecycle::UpstreamStreamDiagnostics {
            http_version: Some(format!("{:?}", self.http_version)),
            request_id: bounded_diagnostic_header(self.request_id.as_ref()),
            content_encoding: bounded_diagnostic_header(self.content_encoding.as_ref()),
            content_length: self.content_length,
            body_bytes,
            body_end: Some(body_end),
            body_error_cause,
            body_error_io_kind,
            body_error_h2_reason,
            message_stop_ms,
            warning_type,
            warning_message,
        })
    }
}

#[derive(Default)]
pub(super) struct SseDownstreamBoundaryTracker {
    pub(super) trailing: u32,
    pub(super) trailing_len: u8,
    pub(super) observed_bytes: bool,
    pub(super) at_event_boundary: bool,
}

impl SseDownstreamBoundaryTracker {
    pub(super) fn observe(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        if bytes.len() >= 4 {
            let tail = &bytes[bytes.len() - 4..];
            self.trailing = u32::from_be_bytes([tail[0], tail[1], tail[2], tail[3]]);
            self.trailing_len = 4;
        } else {
            for &byte in bytes {
                self.trailing = (self.trailing << 8) | u32::from(byte);
                self.trailing_len = self.trailing_len.saturating_add(1).min(4);
            }
        }
        self.observed_bytes = true;
        self.at_event_boundary = (self.trailing_len >= 2
            && matches!(self.trailing & 0xffff, 0x0a0a | 0x0d0d))
            || (self.trailing_len == 4 && self.trailing == 0x0d0a0d0a);
    }

    pub(super) const fn partial_event_pending(&self) -> bool {
        self.observed_bytes && !self.at_event_boundary
    }
}

pub(super) fn make_stream_error_frame(
    capability: ProviderCapability,
    context: &ResponsesSseErrorContext,
    error_type: &str,
    message: &str,
    separate_from_partial_event: bool,
) -> Bytes {
    let frame = if matches!(
        capability,
        ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiResponsesCompact
    ) {
        make_responses_failed_frame(context, error_type, message)
    } else {
        make_provider_error_frame(
            capability,
            StatusCode::BAD_GATEWAY,
            error_type,
            message,
            None,
        )
    };
    if !separate_from_partial_event {
        return frame;
    }

    let mut separated = BytesMut::with_capacity(2 + frame.len());
    separated.extend_from_slice(b"\n\n");
    separated.extend_from_slice(&frame);
    separated.freeze()
}

pub(super) const fn decompression_output_budget_bytes(body_cap_bytes: usize) -> usize {
    body_cap_bytes.saturating_mul(DECOMPRESSION_OUTPUT_BUDGET_BODY_CAP_MULTIPLIER)
}

pub(super) const fn incomplete_sse_event_budget_bytes(body_cap_bytes: usize) -> usize {
    body_cap_bytes.saturating_mul(INCOMPLETE_SSE_EVENT_BUDGET_BODY_CAP_MULTIPLIER)
}

pub(super) fn split_sse_event(buffer: &mut BytesMut, end: usize) -> Bytes {
    let raw = buffer.split_to(end).freeze();
    let retained_capacity = buffer.capacity();
    if retained_capacity > SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES
        && raw.len() < retained_capacity / SSE_BUFFER_SMALL_EVENT_CAPACITY_FRACTION
    {
        *buffer = BytesMut::from(buffer.as_ref());
    }
    raw
}

pub(super) fn semantic_body_bytes(body: &Bytes, plaintext: std::borrow::Cow<'_, [u8]>) -> Bytes {
    match plaintext {
        std::borrow::Cow::Owned(bytes) => Bytes::from(bytes),
        std::borrow::Cow::Borrowed(_) => body.clone(),
    }
}

/// Cap on the retained non-200 upstream error body: an admin-visible diagnostic
/// that can echo request text, so the byte cap bounds the blast radius.
pub(super) const REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES: usize = 8 * 1024;
/// Bytes of an upstream error body considered for a stored sink. The margin
/// over the store cap lets a secret that straddles the cap be masked whole
/// before truncation, so no fragment of it survives the cut.
pub(super) const REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES: usize =
    REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES + 512;
pub(super) const REQUEST_LOG_UPSTREAM_ERROR_BODY_TRUNCATION_MARKER: &str = "\n...[truncated]";

pub(super) fn append_bounded_upstream_error_body(
    buf: &mut Vec<u8>,
    chunk: &[u8],
    truncated: &mut bool,
) {
    if *truncated || chunk.is_empty() {
        return;
    }
    let remaining = REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES.saturating_sub(buf.len());
    if chunk.len() <= remaining {
        buf.extend_from_slice(chunk);
    } else {
        buf.extend_from_slice(&chunk[..remaining]);
        *truncated = true;
    }
}

/// Render bounded upstream error bytes as a lossy UTF-8 string, appending a
/// truncation marker when the body was capped. The result never exceeds the byte
/// cap and always ends on a UTF-8 char boundary.
pub(super) fn bounded_lossy_upstream_error_body(bytes: &[u8], truncated: bool) -> String {
    let text = String::from_utf8_lossy(bytes);
    let marker = REQUEST_LOG_UPSTREAM_ERROR_BODY_TRUNCATION_MARKER;
    let needs_marker = truncated || text.len() > REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES;
    let max_text_len = if needs_marker {
        REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES.saturating_sub(marker.len())
    } else {
        REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES
    };

    let mut end = text.len().min(max_text_len);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }

    let mut out = String::with_capacity(end + usize::from(needs_marker) * marker.len());
    out.push_str(&text[..end]);
    if needs_marker {
        out.push_str(marker);
    }
    out
}

/// Render an upstream error body for a stored sink. Only the first
/// `REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES` count; the signer masks its
/// secret in them before the store cap and truncation marker are applied.
pub(super) fn scrubbed_upstream_error_body(
    signer: &dyn Signer,
    bytes: &[u8],
    truncated: bool,
) -> String {
    let collected = bytes
        .get(..REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES)
        .unwrap_or(bytes);
    let truncated = truncated || collected.len() < bytes.len();
    let text = String::from_utf8_lossy(collected);
    let scrubbed = signer.scrub_secrets(&text);
    bounded_lossy_upstream_error_body(scrubbed.as_bytes(), truncated)
}

/// Render the request-log `upstream_error_message` for an abnormal upstream
/// `stop_reason` (`refusal`, `model_context_window_exceeded`): `category=<..>;
/// <explanation>` when `stop_details` is present, else the bare stop reason.
/// Bounded with the same helper used for upstream error bodies.
pub(crate) fn abnormal_stop_error_message(stop: &usage_parser::AbnormalStop) -> String {
    let message = match (&stop.category, &stop.explanation) {
        (Some(category), Some(explanation)) => {
            format!("category={category}; {explanation}")
        }
        (Some(category), None) => format!("category={category}"),
        (None, Some(explanation)) => explanation.clone(),
        (None, None) => stop.stop_reason.clone(),
    };
    bounded_lossy_upstream_error_body(message.as_bytes(), false)
}

pub(super) fn affinity_stream_error() -> cc_lb_lifecycle::StreamError {
    cc_lb_lifecycle::StreamError {
        error_type: UPSTREAM_AFFINITY_ERROR_TYPE.to_owned(),
        error_message: UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE.to_owned(),
        ..Default::default()
    }
}

/// Provider-owned state a successful Anthropic SSE event introduces when the
/// request carried no affinity hint. Only the events that can create state are
/// inspected, malformed event data carries no usable state, and keys already
/// bound for this response are skipped, so stateless events keep the exact
/// ungated passthrough.
pub(super) fn unsolicited_anthropic_sse_affinity_keys(
    event: &usage_parser::ParsedSseEvent<'_>,
    principal_id: &str,
    bound_keys: &HashSet<UpstreamAffinityKey>,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    if !matches!(
        event.event_type(),
        Some("content_block_start" | "message_start" | "message_delta")
    ) {
        return Ok(Vec::new());
    }
    let keys = match extract_response_affinity_keys_from_sse_event(
        ProviderCapability::AnthropicMessages,
        event,
        principal_id,
    ) {
        Ok(keys) => keys,
        Err(UpstreamAffinityExtractionError::InvalidSseData) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    Ok(keys
        .into_iter()
        .filter(|key| !bound_keys.contains(key))
        .collect())
}

/// JSON key fragments every provider-state field the unsolicited Anthropic
/// extractor binds must contain: `encrypted_content`, `container` (which also
/// covers the `container_upload` block type), and `file_id`. `\u` covers keys
/// spelled with JSON escapes. Event bytes holding none of them cannot introduce
/// provider state, so they keep the raw chunk passthrough.
pub(super) const ANTHROPIC_PROVIDER_STATE_MARKERS: [&[u8]; 4] =
    [b"encrypted_content", b"container", b"file_id", b"\\u"];

/// Already-forwarded bytes of a partial event re-scanned together with the
/// next chunk, so a marker split across upstream chunks is detected before
/// the bytes that follow it are forwarded.
pub(super) const ANTHROPIC_PROVIDER_STATE_MARKER_PROBE_BYTES: usize =
    b"encrypted_content".len() - 1;

/// Whether unforwarded SSE bytes may belong to an event that introduces
/// unsolicited Anthropic provider state and therefore must be held until the
/// event completes and its state is bound.
pub(super) fn sse_bytes_may_carry_anthropic_provider_state(bytes: &[u8]) -> bool {
    ANTHROPIC_PROVIDER_STATE_MARKERS
        .iter()
        .any(|marker| bytes.windows(marker.len()).any(|window| window == *marker))
}

/// Whether an SSE frame carries user-visible streamed content for its wire
/// protocol. This is deliberately provider-aware: relay/usage/terminal
/// processing still sees every parsed frame, while telemetry counts only
/// semantic text frames.
pub(super) fn is_semantic_content_frame(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
) -> bool {
    let value = event.value().ok().flatten();
    let non_empty_string = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
    };

    match capability.usage_shape() {
        crate::provider_capability::UsageShape::AnthropicMessages => {
            event.event_name() == Some(b"content_block_delta")
        }
        crate::provider_capability::UsageShape::OpenaiChatCompletions => value
            .and_then(|value| value.get("choices"))
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices.iter().any(|choice| {
                    let Some(delta) = choice.get("delta") else {
                        return false;
                    };
                    non_empty_string(delta.get("content"))
                        || delta
                            .get("tool_calls")
                            .and_then(Value::as_array)
                            .is_some_and(|calls| {
                                calls.iter().any(|call| {
                                    non_empty_string(call.pointer("/function/arguments"))
                                })
                            })
                })
            }),
        crate::provider_capability::UsageShape::OpenaiCompletions => value
            .and_then(|value| value.get("choices"))
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices
                    .iter()
                    .any(|choice| non_empty_string(choice.get("text")))
            }),
        crate::provider_capability::UsageShape::OpenaiResponses => {
            matches!(
                event.event_type(),
                Some(
                    "response.output_text.delta"
                        | "response.function_call_arguments.delta"
                        | "response.reasoning_summary_text.delta"
                        | "response.refusal.delta"
                )
            ) && non_empty_string(value.and_then(|value| value.get("delta")))
        }
        crate::provider_capability::UsageShape::GeminiGenerateContent => {
            let candidates = value
                .and_then(|value| value.get("candidates"))
                .or_else(|| value.and_then(|value| value.get("response")?.get("candidates")));
            candidates
                .and_then(Value::as_array)
                .is_some_and(|candidates| {
                    candidates.iter().any(|candidate| {
                        candidate
                            .get("content")
                            .and_then(|content| content.get("parts"))
                            .and_then(Value::as_array)
                            .is_some_and(|parts| {
                                parts.iter().any(|part| {
                                    !part
                                        .get("thought")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false)
                                        && (non_empty_string(part.get("text"))
                                            || part.get("functionCall").is_some())
                                })
                            })
                    })
                })
        }
        _ => false,
    }
}

/// Whether a successful SSE event is the provider's terminal success event,
/// before which request-referenced provider state still pending a binding must
/// be persisted. Gemini streams carry no terminal event, so their pending keys
/// are bound once the upstream stream completes cleanly.
pub(crate) fn sse_event_completes_response(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
) -> bool {
    match capability.usage_shape() {
        crate::provider_capability::UsageShape::AnthropicMessages => {
            event
                .value()
                .ok()
                .flatten()
                .and_then(|value| value.get("type"))
                .and_then(serde_json::Value::as_str)
                == Some("message_stop")
        }
        crate::provider_capability::UsageShape::OpenaiResponses => {
            event.value().ok().flatten().is_some_and(|_| {
                event.event_name() == Some(b"response.completed")
                    || event.event_name() == Some(b"response.incomplete")
            }) || event
                .value()
                .ok()
                .flatten()
                .and_then(|value| value.get("type"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|event_type| {
                    event_type == "response.completed" || event_type == "response.incomplete"
                })
        }
        crate::provider_capability::UsageShape::OpenaiChatCompletions
        | crate::provider_capability::UsageShape::OpenaiCompletions => {
            event.is_done() || event.has_finished_choices()
        }
        crate::provider_capability::UsageShape::GeminiGenerateContent
        | crate::provider_capability::UsageShape::GeminiEmbed
        | crate::provider_capability::UsageShape::OpenaiEmbeddings
        | crate::provider_capability::UsageShape::OpenaiAudio
        | crate::provider_capability::UsageShape::OpenaiImages
        | crate::provider_capability::UsageShape::None => false,
    }
}

pub(crate) fn keepalive_event_reaches_upstream_terminal(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
) -> bool {
    if event.value().ok().flatten().is_some_and(|value| {
        if value.get("object").and_then(Value::as_str) == Some("response")
            && value.get("status").and_then(Value::as_str) == Some("failed")
        {
            return true;
        }
        match value.get("type").and_then(Value::as_str) {
            Some("response.failed") => value
                .get("response")
                .filter(|response| response.is_object())
                .is_some_and(|response| {
                    response.get("error").is_none_or(|error| {
                        error.is_null()
                            || (error.get("code").is_some_and(Value::is_string)
                                && error.get("message").is_some_and(Value::is_string))
                    })
                }),
            Some("error") => {
                !matches!(
                    capability,
                    ProviderCapability::OpenaiResponses
                        | ProviderCapability::OpenaiResponsesCompact
                ) || (value.get("message").is_some_and(Value::is_string)
                    && value
                        .get("code")
                        .is_none_or(|code| code.is_null() || code.is_string()))
            }
            _ => false,
        }
    }) {
        return true;
    }
    match capability.usage_shape() {
        crate::provider_capability::UsageShape::OpenaiChatCompletions
        | crate::provider_capability::UsageShape::OpenaiCompletions => event.is_done(),
        _ => sse_event_completes_response(capability, event),
    }
}

pub(super) fn keepalive_http_end_is_terminal(
    capability: ProviderCapability,
    upstream_is_sse: bool,
    status: StatusCode,
    finished_choices_seen: bool,
) -> bool {
    !status.is_success()
        || !upstream_is_sse
        || (matches!(
            capability.usage_shape(),
            crate::provider_capability::UsageShape::OpenaiChatCompletions
                | crate::provider_capability::UsageShape::OpenaiCompletions
        ) && finished_choices_seen)
        || matches!(
            capability.usage_shape(),
            crate::provider_capability::UsageShape::GeminiGenerateContent
                | crate::provider_capability::UsageShape::GeminiEmbed
                | crate::provider_capability::UsageShape::OpenaiEmbeddings
                | crate::provider_capability::UsageShape::OpenaiAudio
                | crate::provider_capability::UsageShape::OpenaiImages
                | crate::provider_capability::UsageShape::None
        )
}

/// JSON `type` of the provider's terminal success SSE event, used by the #663
/// accept-with-warning path. Gemini streams have no terminal event.
pub(super) const fn semantic_terminal_event_type(
    capability: ProviderCapability,
) -> Option<&'static str> {
    match capability.usage_shape() {
        crate::provider_capability::UsageShape::AnthropicMessages => Some("message_stop"),
        crate::provider_capability::UsageShape::OpenaiResponses => Some("response.completed"),
        crate::provider_capability::UsageShape::OpenaiChatCompletions
        | crate::provider_capability::UsageShape::OpenaiCompletions
        | crate::provider_capability::UsageShape::OpenaiEmbeddings
        | crate::provider_capability::UsageShape::OpenaiAudio
        | crate::provider_capability::UsageShape::OpenaiImages
        | crate::provider_capability::UsageShape::GeminiGenerateContent
        | crate::provider_capability::UsageShape::GeminiEmbed
        | crate::provider_capability::UsageShape::None => None,
    }
}

pub(super) fn observe_sse_terminal_event(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
    usage_message_stop: bool,
    semantic_message_stop_seen: &mut bool,
    message_stop_at: &mut Option<Instant>,
    now: impl FnOnce() -> Instant,
) {
    // OpenAI Responses providers identify terminal events in the SSE event
    // name even when the JSON payload omits its redundant `type` field.
    // Anthropic keeps the stricter JSON-type-only rule so a named
    // `message_stop` event cannot turn malformed payloads into completion.
    let parsed_value = event.value().ok().flatten();
    let event_name_terminal = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::OpenaiResponses
    ) && (event.event_name() == Some(b"response.completed")
        || event.event_name() == Some(b"response.incomplete"))
        && parsed_value.is_some();
    let semantic_event_type = parsed_value
        .and_then(|value| value.get("type"))
        .and_then(serde_json::Value::as_str);
    let response_terminal_type = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::OpenaiResponses
    ) && semantic_event_type.is_some_and(|event_type| {
        event_type == "response.completed" || event_type == "response.incomplete"
    });
    *semantic_message_stop_seen |= event_name_terminal
        || response_terminal_type
        || semantic_terminal_event_type(capability)
            .is_some_and(|terminal| semantic_event_type == Some(terminal));
    let response_failed_stop = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::OpenaiResponses
    ) && parsed_value.is_some()
        && (event.event_name() == Some(b"response.failed")
            || semantic_event_type == Some("response.failed"));
    let openai_chat_stop = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::OpenaiChatCompletions
            | crate::provider_capability::UsageShape::OpenaiCompletions
    ) && (event.is_done()
        || parsed_value
            .and_then(|value| value.get("choices"))
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices.iter().any(|choice| {
                    choice
                        .get("finish_reason")
                        .is_some_and(|reason| !reason.is_null())
                })
            }));
    let gemini_stop = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::GeminiGenerateContent
    ) && parsed_value
        .and_then(|value| {
            value
                .get("candidates")
                .or_else(|| value.get("response")?.get("candidates"))
        })
        .and_then(Value::as_array)
        .is_some_and(|candidates| {
            candidates.iter().any(|candidate| {
                candidate
                    .get("finishReason")
                    .is_some_and(|reason| !reason.is_null())
            })
        });
    if event.event_name() == Some(b"message_stop")
        || ((usage_message_stop
            || event_name_terminal
            || response_terminal_type
            || response_failed_stop
            || openai_chat_stop
            || gemini_stop)
            && message_stop_at.is_none())
    {
        *message_stop_at = Some(now());
    }
}

pub(super) fn observe_sse_lifecycle_markers(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
    message_start_at: &mut Option<Instant>,
    content_block_start_at: &mut Option<Instant>,
    ping_count: &mut u64,
    downstream_drop_guard: Option<&mut DownstreamStreamDropGuard>,
    now: Instant,
) {
    match event.event_name() {
        Some(b"message_start") if message_start_at.is_none() => {
            *message_start_at = Some(now);
            if let Some(guard) = downstream_drop_guard {
                guard.set_keepalive_anchor_at(now);
            }
            return;
        }
        Some(b"content_block_start") if content_block_start_at.is_none() => {
            *content_block_start_at = Some(now);
            return;
        }
        Some(b"ping") => {
            *ping_count = ping_count.saturating_add(1);
            return;
        }
        _ => {}
    }

    let Some(value) = event.value().ok().flatten() else {
        return;
    };
    match capability.usage_shape() {
        crate::provider_capability::UsageShape::OpenaiResponses => match event.event_type() {
            Some("response.created" | "response.in_progress") if message_start_at.is_none() => {
                *message_start_at = Some(now);
                if let Some(guard) = downstream_drop_guard {
                    guard.set_keepalive_anchor_at(now);
                }
            }
            Some("response.output_item.added" | "response.content_part.added")
                if content_block_start_at.is_none() =>
            {
                *content_block_start_at = Some(now);
            }
            _ => {}
        },
        crate::provider_capability::UsageShape::OpenaiChatCompletions
        | crate::provider_capability::UsageShape::OpenaiCompletions => {
            if let Some(choices) = value.get("choices").and_then(Value::as_array) {
                if message_start_at.is_none() {
                    *message_start_at = Some(now);
                    if let Some(guard) = downstream_drop_guard {
                        guard.set_keepalive_anchor_at(now);
                    }
                }
                if content_block_start_at.is_none()
                    && choices.iter().any(|choice| {
                        choice.get("delta").is_some_and(|delta| {
                            delta.get("role").is_some()
                                || delta.get("content").is_some_and(|v| !v.is_null())
                                || delta.get("tool_calls").is_some()
                        }) || choice.get("text").is_some_and(|v| !v.is_null())
                    })
                {
                    *content_block_start_at = Some(now);
                }
            }
        }
        crate::provider_capability::UsageShape::GeminiGenerateContent => {
            let candidates = value.get("candidates").or_else(|| {
                value
                    .get("response")
                    .and_then(|resp| resp.get("candidates"))
            });
            if let Some(candidates) = candidates.and_then(Value::as_array) {
                if message_start_at.is_none() {
                    *message_start_at = Some(now);
                    if let Some(guard) = downstream_drop_guard {
                        guard.set_keepalive_anchor_at(now);
                    }
                }
                if content_block_start_at.is_none()
                    && candidates.iter().any(|candidate| {
                        candidate
                            .get("content")
                            .and_then(|content| content.get("parts"))
                            .and_then(Value::as_array)
                            .is_some_and(|parts| !parts.is_empty())
                    })
                {
                    *content_block_start_at = Some(now);
                }
            }
        }
        _ => {}
    }
}

/// Deadline for reading a discarded upstream error body to EOF. A stalled
/// body must not hold the refresh/failover decision (and with it the rest of
/// the request timeout) hostage; the bound is short because the terminal
/// relay regenerates a response when the body never arrives.
pub(super) const ERROR_BODY_COLLECT_DEADLINE: Duration = Duration::from_secs(10);
/// Byte cap for the same read. Bodies that cannot be relayed completely are
/// never relayed at all, so the cap only bounds how much memory a partial
/// read may hold before it is dropped: the stored-sink collection bound
/// already used for upstream error text.
pub(super) const ERROR_BODY_COLLECT_CAP_BYTES: usize =
    REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES;

pub(super) async fn collect_terminal_error_body(body: Body) -> Option<Bytes> {
    let collected = tokio::time::timeout(
        ERROR_BODY_COLLECT_DEADLINE,
        http_body_util::Limited::new(body, ERROR_BODY_COLLECT_CAP_BYTES).collect(),
    )
    .await
    .ok()?
    .ok()?;
    Some(collected.to_bytes())
}

pub(super) async fn collect_error_response(response: Response<Body>) -> CollectedResponse {
    let (parts, body) = response.into_parts();
    CollectedResponse {
        status: parts.status,
        headers: {
            let mut headers = parts.headers;
            strip_hop_by_hop(&mut headers);
            headers
        },
        body: collect_terminal_error_body(body).await,
    }
}

pub(super) fn response_from_collected(
    collected: CollectedResponse,
    capability: ProviderCapability,
) -> Response<Body> {
    let Some(body) = collected.body else {
        // A stalled, truncated, or failed upstream body is never relayed in
        // part; the relayed status stays honest instead of echoing the
        // upstream code next to proxy-generated bytes.
        return provider_error_response(
            capability,
            StatusCode::BAD_GATEWAY,
            "api_error",
            UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE,
            None,
        );
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = collected.status;
    *response.headers_mut() = collected.headers;
    response
}
/// Largest error body held as a JSON candidate. Provider JSON errors are a
/// few hundred bytes; past this the body is parsed as SSE, which bounds the
/// per-chunk revalidation of held bytes.
pub(super) const MAX_HELD_JSON_ERROR_BODY_BYTES: usize = 64 * 1024;

/// How an error-status body on a transformed SSE stream is read.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ErrorBodyClass {
    /// Only blank events so far; they take the normal SSE path while the
    /// next unconsumed bytes decide between JSON and SSE.
    Pending,
    /// Every byte so far is a valid JSON prefix; held unparsed.
    JsonCandidate,
    /// Parsed as SSE events.
    Sse,
}

/// Advances the error-body class before the next SSE event is split.
///
/// While `Pending`, completed blank events are parsed normally. After them,
/// unconsumed bytes whose first non-whitespace byte is `{` or `[` become a
/// JSON candidate (leading whitespace included); any other byte makes the
/// body SSE. A rewritten or dropped event also makes it SSE, so raw JSON
/// never follows transformed output. A candidate is
/// held only while it remains a valid JSON prefix: the first byte that breaks
/// JSON syntax (`[trace: x`, `{"extension:` followed by a raw newline)
/// returns it to SSE parsing before any event is split, so genuine SSE keeps
/// its normal handling.
pub(super) fn classify_error_body(class: ErrorBodyClass, buffer: &[u8]) -> ErrorBodyClass {
    let class = match class {
        ErrorBodyClass::Pending => match buffer.first() {
            None => return ErrorBodyClass::Pending,
            Some(b'{' | b'[') => ErrorBodyClass::JsonCandidate,
            Some(byte) if !byte.is_ascii_whitespace() => return ErrorBodyClass::Sse,
            Some(_) => match usage_parser::find_sse_event_end(buffer) {
                // A completed blank event is split and passed through normally.
                Some(end) if buffer[..end].iter().all(u8::is_ascii_whitespace) => {
                    return ErrorBodyClass::Pending;
                }
                // Otherwise the first non-whitespace byte decides (` {`,
                // `\n{`, ` {\n\n`). A held candidate keeps its leading
                // whitespace and goes back to SSE parsing untouched if it
                // breaks JSON.
                _ => match buffer.iter().find(|byte| !byte.is_ascii_whitespace()) {
                    None => return ErrorBodyClass::Pending,
                    Some(b'{' | b'[') => ErrorBodyClass::JsonCandidate,
                    Some(_) => return ErrorBodyClass::Sse,
                },
            },
        },
        class => class,
    };
    if class != ErrorBodyClass::JsonCandidate {
        return class;
    }
    if buffer.len() > MAX_HELD_JSON_ERROR_BODY_BYTES {
        return ErrorBodyClass::Sse;
    }
    if is_json_prefix(buffer) {
        ErrorBodyClass::JsonCandidate
    } else {
        ErrorBodyClass::Sse
    }
}

/// Whether `buffer` is a prefix of some valid JSON document.
///
/// `serde_json` reports a truncated document as EOF, with two exceptions
/// that are probed by completing them. A document cut inside a number
/// (`-`, `2.`, `2.5e+`) is a syntax error, and every unfinished number
/// accepts one more digit. A `\u` escape with fewer than four following
/// bytes is EOF even when those bytes cannot be hex (`"\u\n\n`), so the
/// escape is padded with `0`s, which is always legal (lone surrogates are
/// accepted here). `sonic_rs` reports syntax errors for ordinary
/// truncations, so it cannot drive this decision. Held error bodies only,
/// capped by `MAX_HELD_JSON_ERROR_BODY_BYTES`, so off the request hot path.
pub(super) fn is_json_prefix(buffer: &[u8]) -> bool {
    let unfinished = |bytes: &[u8]| match serde_json::from_slice::<serde::de::IgnoredAny>(bytes) {
        Ok(_) => true,
        Err(error) => error.is_eof(),
    };
    let padded = |padding: usize| {
        let mut probe = Vec::with_capacity(buffer.len() + padding);
        probe.extend_from_slice(buffer);
        probe.resize(buffer.len() + padding, b'0');
        unfinished(&probe)
    };
    if let Some(missing) = short_unicode_escape_missing_bytes(buffer) {
        return padded(missing);
    }
    unfinished(buffer) || padded(1)
}

/// Hex digits still missing when `buffer` ends inside a `\u` escape.
pub(super) fn short_unicode_escape_missing_bytes(buffer: &[u8]) -> Option<usize> {
    (0..4).find_map(|present| {
        let u = buffer.len().checked_sub(present + 1)?;
        if buffer[u] != b'u' {
            return None;
        }
        // The `u` follows an escaping backslash only after an odd run of them.
        let backslashes = buffer[..u]
            .iter()
            .rev()
            .take_while(|&&byte| byte == b'\\')
            .count();
        (backslashes % 2 == 1).then_some(4 - present)
    })
}

/// Whether a held error body is one complete JSON document.
pub(super) fn held_error_body_is_json(buffer: &[u8]) -> bool {
    serde_json::from_slice::<serde::de::IgnoredAny>(buffer).is_ok()
}

pub(super) fn has_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| {
            let mime = mime.trim();
            mime.eq_ignore_ascii_case("application/json")
                || mime
                    .rsplit_once('+')
                    .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case("json"))
        })
}

pub(super) fn is_sse_response(
    headers: &HeaderMap,
    stream_requested: bool,
    capability: ProviderCapability,
) -> bool {
    if !headers.contains_key(CONTENT_TYPE) {
        // ChatGPT's Responses backend can omit this header. An explicit
        // Content-Type, including JSON errors, always keeps its own semantics.
        return stream_requested
            && matches!(
                capability.usage_shape(),
                crate::provider_capability::UsageShape::OpenaiResponses
            );
    }
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type.trim().eq_ignore_ascii_case("text/event-stream")
            })
        })
}

#[cfg(test)]
mod upstream_error_scrub_tests {
    use cc_lb_signer_anthropic_key::AnthropicKeySigner;

    use super::*;

    const KEY: &str = "sk-ant-api03-CANARYc4n4ry0123456789abcdefABCDEF-AAAA";
    const MARKER: &str = REQUEST_LOG_UPSTREAM_ERROR_BODY_TRUNCATION_MARKER;

    fn signer() -> AnthropicKeySigner {
        AnthropicKeySigner::new(KEY, None).expect("signer")
    }

    fn assert_no_key_fragment(stored: &str) {
        for fragment in KEY.as_bytes().windows(12) {
            let fragment = std::str::from_utf8(fragment).expect("ascii key");
            assert!(
                !stored.contains(fragment),
                "stored text keeps key fragment {fragment:?}"
            );
        }
    }

    /// Feeds `body` through the streaming collector in small chunks, the way
    /// the relay does, and renders the stored text.
    fn collected_and_stored(body: &[u8]) -> String {
        let mut collected = Vec::new();
        let mut truncated = false;
        for chunk in body.chunks(97) {
            append_bounded_upstream_error_body(&mut collected, chunk, &mut truncated);
        }
        scrubbed_upstream_error_body(&signer(), &collected, truncated)
    }

    fn body_with_key_at(offset: usize, total: usize) -> Vec<u8> {
        let mut body = vec![b'x'; offset];
        body.extend_from_slice(KEY.as_bytes());
        body.resize(total.max(body.len()), b'y');
        body
    }

    /// SEC-13: a key at the start, middle, end, or straddling the store cap is
    /// masked before truncation, on both the buffered and streaming paths.
    #[test]
    fn sec_13_key_anywhere_before_cap_leaves_no_fragment() {
        let cap = REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES;
        let total = cap + 4 * 1024;
        for offset in [
            0,
            cap / 2,
            cap - MARKER.len() - 20,
            cap - 20,
            cap - 1,
            cap + 100,
        ] {
            let body = body_with_key_at(offset, total);
            for stored in [
                collected_and_stored(&body),
                scrubbed_upstream_error_body(&signer(), &body, false),
            ] {
                assert!(stored.len() <= cap, "offset {offset}: {}", stored.len());
                assert!(stored.ends_with(MARKER), "offset {offset}");
                assert_no_key_fragment(&stored);
            }
        }
    }

    /// SEC-13: a short error body that echoes the key twice keeps its text
    /// but loses both copies, with no truncation marker.
    #[test]
    fn sec_13_repeated_key_in_short_body_is_masked() {
        let body = format!(r#"{{"error":"bad key {KEY} (again: {KEY})"}}"#);

        let stored = collected_and_stored(body.as_bytes());

        assert_eq!(
            stored,
            r#"{"error":"bad key [REDACTED] (again: [REDACTED])"}"#
        );
    }

    /// SEC-14 (unit): provider text without the key is handed back unchanged,
    /// and a key in the structured message is masked.
    #[test]
    fn sec_14_structured_message_is_masked_and_clean_text_kept() {
        let clean = br#"{"error":{"type":"rate_limit_error","message":"rate limit exceeded"}}"#;
        let echoed = format!(r#"{{"error":{{"type":"auth","message":"echo {KEY}"}}}}"#);

        let clean = usage_parser::observe_http_error_body(clean)
            .map(|error| error.into_parts(&signer()))
            .unwrap_or_default();
        let echoed = usage_parser::observe_http_error_body(echoed.as_bytes())
            .map(|error| error.into_parts(&signer()))
            .unwrap_or_default();

        assert_eq!(clean.1.as_deref(), Some("rate limit exceeded"));
        assert_eq!(echoed.1.as_deref(), Some("echo [REDACTED]"));
    }

    fn message_with_key_at(offset: usize) -> String {
        let mut message = "m".repeat(offset);
        message.push_str(KEY);
        message.push_str(&"t".repeat(2048));
        message
    }

    /// SEC-13: a key inside a structured `error.message`, at or straddling
    /// the 1024-byte message cap, is masked before the cap on the buffered,
    /// unauthorized, and mid-stream paths.
    #[test]
    fn sec_13_structured_message_key_at_cap_leaves_no_fragment() {
        for offset in [0, 1000, 1010, 1015, 1020, 1021, 1023, 1030] {
            let message = message_with_key_at(offset);
            let body = serde_json::json!({
                "type": "error",
                "error": { "type": "invalid_request_error", "message": message },
            })
            .to_string();
            let (_, buffered, _) = usage_parser::observe_http_error_body(body.as_bytes())
                .map(|error| error.into_parts(&signer()))
                .unwrap_or_default();
            let raw = format!("event: error\ndata: {body}\n\n");
            let event = parse_sse_event(raw.as_bytes());
            let streamed = usage_parser::detect_mid_stream_error(&event, &signer())
                .and_then(|error| error.error_message);
            for stored in [buffered, streamed] {
                let stored = stored.expect("message extracted");
                assert!(stored.len() <= 1024, "offset {offset}: {}", stored.len());
                assert!(stored.ends_with("..."), "offset {offset}");
                assert_no_key_fragment(&stored);
            }
        }
    }
}

#[cfg(test)]
mod keepalive_terminal_proof_tests {
    use super::{
        ProviderCapability, StatusCode, keepalive_event_reaches_upstream_terminal,
        keepalive_http_end_is_terminal,
    };
    use crate::usage_parser::parse_sse_event;

    #[test]
    fn explicit_provider_failures_are_terminal_without_success() {
        assert!(keepalive_event_reaches_upstream_terminal(
            ProviderCapability::AnthropicMessages,
            &parse_sse_event(b"event: error\ndata: {\"type\":\"error\",\"error\":null}\n\n"),
        ));
        for raw in [
            b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n".as_slice(),
            b"data: {\"object\":\"response\",\"status\":\"failed\",\"error\":null}\n\n",
            b"event: error\ndata: {\"type\":\"error\",\"message\":\"native error\",\"code\":null}\n\n",
        ] {
            assert!(keepalive_event_reaches_upstream_terminal(
                ProviderCapability::OpenaiResponses,
                &parse_sse_event(raw),
            ));
        }
    }

    #[test]
    fn abnormal_hints_and_error_lookalikes_do_not_prove_completion() {
        for raw in [
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\"}}\n\n".as_slice(),
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"model_context_window_exceeded\"}}\n\n",
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"error\":{\"type\":\"api_error\",\"message\":\"not a terminal declaration\"}}\n\n",
            b"event: response.failed\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ordinary text\"}\n\n",
            b"event: error\ndata: {\"type\":\"message_delta\",\"delta\":{\"text\":\"ordinary text\"}}\n\n",
            b"event: error\ndata: \"ordinary text\"\n\n",
            b"event: error\ndata: {\"type\":\"error\"\n\n",
        ] {
            assert!(!keepalive_event_reaches_upstream_terminal(
                ProviderCapability::AnthropicMessages,
                &parse_sse_event(raw),
            ));
        }
        assert!(keepalive_event_reaches_upstream_terminal(
            ProviderCapability::AnthropicMessages,
            &parse_sse_event(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
        ));
    }

    #[test]
    fn responses_error_proof_requires_native_payload_fields() {
        for raw in [
            b"event: error\ndata: {\"type\":\"error\"}\n\n".as_slice(),
            b"event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"test\",\"message\":\"nested\"}}\n\n",
            b"event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n",
            b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":42}\n\n",
            b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":42}}\n\n",
        ] {
            assert!(!keepalive_event_reaches_upstream_terminal(
                ProviderCapability::OpenaiResponses, &parse_sse_event(raw),
            ));
        }
    }

    #[test]
    fn finished_chat_choice_requires_http_end_but_done_does_not() {
        let choice =
            parse_sse_event(b"data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\n");
        assert!(!keepalive_event_reaches_upstream_terminal(
            ProviderCapability::OpenaiChatCompletions,
            &choice,
        ));
        assert!(keepalive_http_end_is_terminal(
            ProviderCapability::OpenaiChatCompletions,
            true,
            StatusCode::OK,
            true,
        ));
        assert!(keepalive_event_reaches_upstream_terminal(
            ProviderCapability::OpenaiChatCompletions,
            &parse_sse_event(b"data: [DONE]\n\n"),
        ));
    }

    #[test]
    fn clean_http_end_only_completes_formats_with_an_eof_boundary() {
        assert!(!keepalive_http_end_is_terminal(
            ProviderCapability::AnthropicMessages,
            true,
            StatusCode::OK,
            false,
        ));
        assert!(!keepalive_http_end_is_terminal(
            ProviderCapability::OpenaiResponses,
            true,
            StatusCode::OK,
            false,
        ));
        assert!(!keepalive_http_end_is_terminal(
            ProviderCapability::OpenaiChatCompletions,
            true,
            StatusCode::OK,
            false,
        ));
        assert!(keepalive_http_end_is_terminal(
            ProviderCapability::AnthropicMessages,
            true,
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
        ));
        assert!(keepalive_http_end_is_terminal(
            ProviderCapability::AnthropicMessages,
            true,
            StatusCode::TEMPORARY_REDIRECT,
            false,
        ));
        assert!(keepalive_http_end_is_terminal(
            ProviderCapability::AnthropicMessages,
            false,
            StatusCode::OK,
            false,
        ));
        assert!(keepalive_http_end_is_terminal(
            ProviderCapability::OpenaiAudio,
            true,
            StatusCode::OK,
            false,
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drive_with_activity_heartbeat_skips_short_waits_and_pulses_long_silent_waits() {
        use std::{sync::Arc, time::Duration};

        use cc_lb_clock::ClockHandle;
        use cc_lb_control::RequestEventBus;

        use crate::lifecycle::{LifecycleContext, drive_with_activity_heartbeat};

        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut rx = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);

        let short_observer =
            LifecycleContext::new("req-short-wait".to_owned(), bus.clone(), &clock);
        short_observer.mark_authn_reached();
        drive_with_activity_heartbeat(std::future::ready(()), Some(&short_observer)).await;
        while let Ok(ev) = rx.try_recv() {
            assert!(!matches!(
                ev,
                cc_lb_lifecycle::LifecycleEvent::RequestActivity { .. }
            ));
        }

        let long_observer = LifecycleContext::new("req-long-wait".to_owned(), bus, &clock);
        long_observer.mark_authn_reached();
        let expected_id = long_observer.event_id().to_owned();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let drive = tokio::spawn(async move {
            drive_with_activity_heartbeat(done_rx, Some(&long_observer)).await
        });
        for _ in 0..3 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::task::yield_now().await;
        }
        let _ = done_tx.send(());
        let _ = drive.await.expect("drive task completes");

        let mut activity_count = 0usize;
        while let Ok(ev) = rx.try_recv() {
            if let cc_lb_lifecycle::LifecycleEvent::RequestActivity { event_id } = ev {
                assert_eq!(event_id, expected_id);
                activity_count += 1;
            }
        }
        assert_eq!(
            activity_count, 3,
            "180s silent wait must emit three 60s activity heartbeats"
        );
    }
}
