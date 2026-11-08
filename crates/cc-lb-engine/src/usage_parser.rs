//! Anthropic Messages API SSE usage parser, shared by the proxy lifecycle
//! and tests/fuzz/bench.
//!
//! Decodes streaming SSE events into [`UsageCounts`] with null-safe merge:
//! a field is overwritten only when the upstream event reports a present
//! AND non-null value. Absent or `null` fields preserve the prior cumulative
//! value, which matches Anthropic's contract that
//! `message_delta.usage.input_tokens` is optional.
//!
//! See `docs.anthropic.com/en/api/messages-streaming` for the event catalog
//! and `anthropic-sdk-typescript`/`MessageDeltaUsage` for the field schema.

use std::borrow::Cow;

use crate::provider_capability::UsageShape;
use cc_lb_upstream::Signer;
use serde_json::Value;

const UPSTREAM_ERROR_MESSAGE_MAX_BYTES: usize = 1024;
/// Message bytes kept before the signer scrubs them: the store cap plus room
/// for the longest accepted credential (512 bytes), so a secret straddling
/// the cap is still whole when it is masked.
const UPSTREAM_ERROR_MESSAGE_COLLECT_BYTES: usize = UPSTREAM_ERROR_MESSAGE_MAX_BYTES + 512;
const UPSTREAM_ERROR_TYPE_MAX_BYTES: usize = 256;
const TRUNCATION_MARKER: &str = "...";

/// Cumulative token + extended usage state extracted from a single request's
/// SSE stream (or non-stream JSON body).
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct UsageCounts {
    /// Progress-aware presence, including lossy thinking estimates.
    pub(crate) present: bool,
    /// Provider usage object presence, suitable for billing and reconciliation.
    pub(crate) billable_present: bool,
    // Internal parser marker: tracks a thinking progress frame even when its estimate is numerically zero.
    pub(crate) thinking_progress_present: bool,
    pub(crate) input_tokens: u64,
    pub(crate) gross_input_tokens: Option<u64>,
    pub(crate) input_includes_cached: bool,
    pub(crate) output_tokens: u64,
    pub(crate) cache_creation_input_tokens: u64,
    pub(crate) cache_creation_input_tokens_5m: u64,
    pub(crate) cache_creation_input_tokens_1h: u64,
    pub(crate) cache_read_input_tokens: u64,
    /// Numeric OpenAI image-token details, when reported by an eligible usage shape.
    pub(crate) input_image_tokens: Option<u64>,
    pub(crate) output_image_tokens: Option<u64>,
    /// Attribution evidence is separate from numeric accounting, which defaults
    /// absent or malformed cache counters to zero in some provider shapes.
    pub(crate) cache_creation_evidence: CacheTokenEvidence,
    pub(crate) cache_read_evidence: CacheTokenEvidence,
    pub(crate) thinking_tokens: u64,
    /// Gemini's cumulative prompt tokens attributed to tool use, when reported.
    ///
    /// This remains distinct from `input_tokens`: Gemini's `promptTokenCount`
    /// continues to drive input accounting, while this optional component is
    /// propagated for usage attribution.
    pub(crate) tool_use_prompt_tokens: Option<u64>,
    /// Last Gemini candidates snapshot, kept separately from billable output
    /// so partial updates can retain it without adding cumulative counts.
    pub(crate) last_candidates_tokens: u64,
    pub(crate) web_search_requests: u64,
    pub(crate) web_fetch_requests: u64,
    pub(crate) service_tier: Option<String>,
    pub(crate) inference_geo: Option<String>,
    pub(crate) iterations: Option<Value>,
    /// Accumulator for the per-frame `delta.estimated_tokens` field surfaced by
    /// the beta `thinking-token-count-2026-05-13` header on
    /// `content_block_delta` events whose `delta.type == "thinking_delta"`.
    ///
    /// This is a delta (must be summed), and the value is documented as a
    /// **lossy estimate** — `message_delta.usage.output_tokens` /
    /// `usage.output_tokens_details.thinking_tokens` remain authoritative for
    /// billing. We track it purely to drive live-progress broadcasts; it does
    /// not make `billable_present` true.
    pub(crate) estimated_thinking_progress: u64,
}

/// Evidence from a complete cumulative cache-counter representation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum CacheTokenEvidence {
    #[default]
    Unreported,
    Zero,
    Positive,
    Invalid,
}

impl CacheTokenEvidence {
    fn from_value(value: &Value) -> Self {
        match value.as_u64() {
            Some(0) => Self::Zero,
            Some(_) => Self::Positive,
            None => Self::Invalid,
        }
    }

    fn merge(&mut self, reported: Self) {
        // Missing/null SSE fields preserve earlier affirmative evidence.
        // A present malformed representation instead makes attribution unknown
        // until a later complete numeric snapshot replaces it.
        if reported != Self::Unreported {
            *self = reported;
        }
    }
}

impl UsageCounts {
    /// Whether authoritative provider usage is available for pricing and
    /// reconciliation. The fallback preserves existing internal constructors
    /// that predate `billable_present`; parser-created progress-only state
    /// carries `thinking_progress_present` and therefore remains non-billable.
    pub(crate) fn has_billable_usage(&self) -> bool {
        self.billable_present || (self.present && !self.thinking_progress_present)
    }

    pub(crate) fn has_affirmative_read_only_cache_usage(&self) -> bool {
        self.cache_read_evidence == CacheTokenEvidence::Positive
            && self.cache_creation_evidence == CacheTokenEvidence::Zero
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SseUsageUpdate {
    pub(crate) message_start_usage: bool,
    pub(crate) message_stop: bool,
}

/// Result of parsing an upstream-emitted `event: error` SSE frame.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct UpstreamStreamError {
    pub(crate) error_type: Option<String>,
    pub(crate) error_message: Option<String>,
}

/// An upstream `stop_reason` that ends the response abnormally even though the
/// HTTP status is 200 (`refusal`, `model_context_window_exceeded`). Normal
/// reasons (`end_turn`, `max_tokens`, `stop_sequence`, `tool_use`,
/// `pause_turn`, `compaction`) never produce this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AbnormalStop {
    pub(crate) stop_reason: String,
    pub(crate) category: Option<String>,
    pub(crate) explanation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundedErrorType(String);

impl BoundedErrorType {
    fn parse(value: &str) -> Option<Self> {
        (value.len() <= UPSTREAM_ERROR_TYPE_MAX_BYTES).then(|| Self(value.to_owned()))
    }

    fn into_string(self) -> String {
        self.0
    }
}

/// Raw `error.message` text bounded to the collect window (not yet the store
/// cap); `.1` records that the source exceeded the window. The store cap is
/// applied only after the signer scrubbed the text, in [`Self::finish`].
#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundedErrorMessage(String, bool);

impl BoundedErrorMessage {
    fn parse(value: &str) -> Self {
        if value.len() <= UPSTREAM_ERROR_MESSAGE_COLLECT_BYTES {
            return Self(value.to_owned(), false);
        }
        let mut end = UPSTREAM_ERROR_MESSAGE_COLLECT_BYTES;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        Self(value.get(..end).unwrap_or(value).to_owned(), true)
    }

    /// Masks the signer's secret, then applies the store cap and marker.
    fn finish(self, signer: Option<&dyn Signer>) -> String {
        let Self(text, source_truncated) = self;
        let mut text = scrub_string(signer, text);
        if !source_truncated && text.len() <= UPSTREAM_ERROR_MESSAGE_MAX_BYTES {
            return text;
        }
        let mut end = text
            .len()
            .min(UPSTREAM_ERROR_MESSAGE_MAX_BYTES - TRUNCATION_MARKER.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str(TRUNCATION_MARKER);
        text
    }
}

/// Applies the signer's scrubber, reusing `text` when nothing matched.
fn scrub_string(signer: Option<&dyn Signer>, text: String) -> String {
    let Some(signer) = signer else {
        return text;
    };
    let scrubbed = match signer.scrub_secrets(&text) {
        Cow::Owned(scrubbed) => Some(scrubbed),
        Cow::Borrowed(_) => None,
    };
    scrubbed.unwrap_or(text)
}

/// A structured upstream error extracted from an HTTP error response body.
///
/// `error_type` is `error.type`, or `error.code` when the provider emits no
/// `type` (non-Anthropic gateways). `error_message` is `error.message`. Either
/// field may be absent; present fields are never empty or fabricated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalUpstreamError {
    error_type: Option<BoundedErrorType>,
    error_message: Option<BoundedErrorMessage>,
    retry_after: Option<String>,
}

impl CanonicalUpstreamError {
    /// `(error_type, error_message)` with `signer`'s secret masked before the
    /// store cap is applied; each is `None` when the upstream body carried no
    /// usable value, so callers can fall back per field (e.g. a bounded raw
    /// body for a missing message) instead of emitting a zero-value fake
    /// message.
    pub(crate) fn into_parts(self, signer: &dyn Signer) -> (Option<String>, Option<String>) {
        self.into_parts_with(Some(signer))
    }

    pub(crate) fn retry_after(&self) -> Option<&str> {
        self.retry_after.as_deref()
    }

    fn into_parts_with(self, signer: Option<&dyn Signer>) -> (Option<String>, Option<String>) {
        (
            self.error_type
                .map(|error_type| scrub_string(signer, error_type.into_string())),
            self.error_message.map(|message| message.finish(signer)),
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NonStreamObservation {
    pub(crate) usage: UsageCounts,
    pub(crate) canonical_error: Option<CanonicalUpstreamError>,
}

/// Find the byte offset of the next SSE event terminator (`\n\n`, `\r\r`, or
/// `\r\n\r\n`) in `buffer`. Returns the index immediately after the terminator,
/// or `None` if the buffer does not yet contain a full event.
pub(crate) fn find_sse_event_end(buffer: &[u8]) -> Option<usize> {
    let mut index = 0;
    while index < buffer.len() {
        if buffer[index] == b'\n' && buffer.get(index + 1) == Some(&b'\n') {
            return Some(index + 2);
        }
        if buffer[index] == b'\r' {
            if buffer.get(index + 1) == Some(&b'\r') {
                return Some(index + 2);
            }
            if buffer.get(index + 1) == Some(&b'\n')
                && buffer.get(index + 2) == Some(&b'\r')
                && buffer.get(index + 3) == Some(&b'\n')
            {
                return Some(index + 4);
            }
        }
        index += 1;
    }
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SseEventParseError {
    InvalidData,
}

/// One SSE frame split into its borrowed event metadata and parsed JSON payload.
///
/// The joined payload is parsed once so usage, provider-error, affinity, and
/// keepalive observers can share the same JSON tree.
#[derive(Debug)]
pub(crate) struct ParsedSseEvent<'a> {
    event_name: Option<&'a [u8]>,
    named_error: bool,
    done: bool,
    value: Result<Option<Value>, SseEventParseError>,
}

impl ParsedSseEvent<'_> {
    pub(crate) fn event_name(&self) -> Option<&[u8]> {
        self.event_name
    }

    pub(crate) fn event_type(&self) -> Option<&str> {
        self.value
            .as_ref()
            .ok()
            .and_then(|value| value.as_ref())
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str)
            .or_else(|| {
                self.event_name
                    .and_then(|name| std::str::from_utf8(name).ok())
            })
    }

    pub(crate) fn value(&self) -> Result<Option<&Value>, SseEventParseError> {
        self.value
            .as_ref()
            .map(Option::as_ref)
            .map_err(|error| *error)
    }
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn has_finished_choices(&self) -> bool {
        self.value()
            .ok()
            .flatten()
            .and_then(|value| value.get("choices"))
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                !choices.is_empty()
                    && choices.iter().all(|choice| {
                        choice
                            .get("finish_reason")
                            .and_then(Value::as_str)
                            .is_some()
                    })
            })
    }
}

/// Parse one complete SSE frame.
///
/// A single `data:` line is parsed directly from the input buffer. Multiline
/// data allocates only for the required SSE newline joining.
pub(crate) fn parse_sse_event(raw_event: &[u8]) -> ParsedSseEvent<'_> {
    let Ok(text) = std::str::from_utf8(raw_event) else {
        return ParsedSseEvent {
            event_name: sse_event_name(raw_event),
            named_error: false,
            value: Err(SseEventParseError::InvalidData),
            done: false,
        };
    };
    let (event_name, named_error) = scan_sse_event_metadata(text);
    let mut data_lines = text.split(['\r', '\n']).filter_map(sse_data_line);
    let Some(first) = data_lines.next() else {
        return ParsedSseEvent {
            event_name,
            named_error,
            done: false,
            value: Ok(None),
        };
    };
    let Some(second) = data_lines.next() else {
        let value = if first.trim() == "[DONE]" {
            Ok(None)
        } else {
            sonic_rs::from_str::<Value>(first)
                .map(Some)
                .map_err(|_| SseEventParseError::InvalidData)
        };
        return ParsedSseEvent {
            event_name,
            named_error,
            done: first.trim() == "[DONE]",
            value,
        };
    };

    let mut payload = String::with_capacity(raw_event.len());
    payload.push_str(first);
    payload.push('\n');
    payload.push_str(second);
    for line in data_lines {
        payload.push('\n');
        payload.push_str(line);
    }
    let value = if payload.trim() == "[DONE]" {
        Ok(None)
    } else {
        sonic_rs::from_str::<Value>(&payload)
            .map(Some)
            .map_err(|_| SseEventParseError::InvalidData)
    };
    ParsedSseEvent {
        event_name,
        named_error,
        done: payload.trim() == "[DONE]",
        value,
    }
}

fn sse_data_line(line: &str) -> Option<&str> {
    line.strip_prefix("data:")
        .map(|data| data.strip_prefix(' ').unwrap_or(data))
}

fn scan_sse_event_metadata(text: &str) -> (Option<&[u8]>, bool) {
    let mut event_name = None;
    let mut named_error = false;
    for line in text.split(['\r', '\n']) {
        let Some(name) = line.strip_prefix("event:") else {
            continue;
        };
        event_name.get_or_insert(name.trim_ascii().as_bytes());
        named_error |= name.trim() == "error";
    }
    (event_name, named_error)
}

/// Extract the first raw `event: <name>` line from one SSE event.
pub(crate) fn sse_event_name(raw_event: &[u8]) -> Option<&[u8]> {
    raw_event
        .split(|byte| matches!(*byte, b'\r' | b'\n'))
        .filter_map(|line| line.strip_prefix(b"event:"))
        .map(|name| name.trim_ascii())
        .next()
}

/// Merge usage carried by one parsed SSE event into `usage`. Returns flags
/// for which boundary events were observed (so callers can record timing).
#[cfg(test)]
pub(crate) fn accumulate_sse_usage(
    event: &ParsedSseEvent<'_>,
    usage: &mut UsageCounts,
) -> SseUsageUpdate {
    accumulate_sse_usage_inner(event, usage, None)
}

pub(crate) fn accumulate_sse_usage_for_shape(
    event: &ParsedSseEvent<'_>,
    usage: &mut UsageCounts,
    shape: UsageShape,
) -> SseUsageUpdate {
    accumulate_sse_usage_inner(event, usage, Some(shape))
}

fn accumulate_sse_usage_inner(
    event: &ParsedSseEvent<'_>,
    usage: &mut UsageCounts,
    shape: Option<UsageShape>,
) -> SseUsageUpdate {
    let mut update = SseUsageUpdate::default();
    if let Ok(Some(value)) = event.value() {
        accumulate_sse_usage_value(value, event.event_name, usage, &mut update, shape);
    }
    update
}

fn accumulate_sse_usage_value(
    value: &Value,
    event_name: Option<&[u8]>,
    usage: &mut UsageCounts,
    update: &mut SseUsageUpdate,
    shape: Option<UsageShape>,
) {
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| event_name.and_then(|name| std::str::from_utf8(name).ok()));
    if event_type == Some("message_stop") {
        update.message_stop = true;
    }
    if event_type == Some("content_block_delta")
        && let Some(delta) = value.get("delta")
        && delta.get("type").and_then(Value::as_str) == Some("thinking_delta")
        && let Some(estimated) = delta.get("estimated_tokens").and_then(Value::as_u64)
    {
        usage.estimated_thinking_progress =
            usage.estimated_thinking_progress.saturating_add(estimated);
        usage.thinking_progress_present = true;
        usage.present = true;
    }
    let Some(reported) = usage_value_for_shape(value, shape).filter(|value| !value.is_null())
    else {
        return;
    };
    if event_type == Some("message_start") {
        update.message_start_usage = true;
    }
    usage.present = true;
    usage.billable_present = true;
    usage.thinking_progress_present = false;
    merge_usage_value_for_shape(usage, reported, shape);
    if usage.service_tier.is_none()
        && let Some(tier) = service_tier_from_envelope(value)
    {
        usage.service_tier = Some(tier.to_owned());
    }
}

fn usage_value_for_shape(value: &Value, shape: Option<UsageShape>) -> Option<&Value> {
    match shape {
        Some(UsageShape::None) => None,
        Some(UsageShape::GeminiGenerateContent) => value
            .get("usageMetadata")
            .or_else(|| {
                value
                    .get("response")
                    .and_then(|response| response.get("usageMetadata"))
            })
            .or_else(|| value.get("usage")),
        Some(UsageShape::GeminiEmbed) => value.get("usageMetadata").or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("usageMetadata"))
        }),
        Some(UsageShape::OpenaiResponses) => value.get("usage").or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("usage"))
        }),
        Some(UsageShape::OpenaiAudio) => value
            .get("usage")
            .filter(|usage| usage.get("type").and_then(Value::as_str) == Some("tokens")),
        Some(UsageShape::OpenaiImages) => value.get("usage"),
        Some(
            UsageShape::AnthropicMessages
            | UsageShape::OpenaiChatCompletions
            | UsageShape::OpenaiCompletions
            | UsageShape::OpenaiEmbeddings,
        ) => value.get("usage").or_else(|| {
            value
                .get("message")
                .and_then(|message| message.get("usage"))
        }),
        None => value
            .get("usage")
            .or_else(|| value.get("usageMetadata"))
            .or_else(|| {
                value
                    .get("response")
                    .and_then(|response| response.get("usage"))
            })
            .or_else(|| {
                value
                    .get("response")
                    .and_then(|response| response.get("usageMetadata"))
            })
            .or_else(|| {
                value
                    .get("message")
                    .and_then(|message| message.get("usage"))
            }),
    }
}

#[cfg(test)]
pub(crate) fn observe_non_stream_json_body(body: &[u8]) -> NonStreamObservation {
    let Ok(value) = sonic_rs::from_slice::<Value>(body) else {
        return NonStreamObservation::default();
    };
    observe_non_stream_json_value(&value)
}

pub(crate) fn observe_non_stream_json_value(value: &Value) -> NonStreamObservation {
    NonStreamObservation {
        usage: usage_from_value(value),
        canonical_error: canonical_upstream_error_from_value(value),
    }
}

pub(crate) fn observe_non_stream_json_value_for_shape(
    value: &Value,
    shape: UsageShape,
) -> NonStreamObservation {
    NonStreamObservation {
        usage: usage_from_value_for_shape(value, Some(shape)),
        canonical_error: canonical_upstream_error_from_value(value),
    }
}

fn usage_from_value(value: &Value) -> UsageCounts {
    usage_from_value_for_shape(value, None)
}

fn usage_from_value_for_shape(value: &Value, shape: Option<UsageShape>) -> UsageCounts {
    if matches!(shape, Some(UsageShape::GeminiGenerateContent))
        && let Value::Array(elements) = value
    {
        let mut usage = UsageCounts::default();
        for element in elements {
            let Some(usage_value) =
                usage_value_for_shape(element, shape).filter(|value| !value.is_null())
            else {
                continue;
            };
            usage.present = true;
            merge_usage_value_for_shape(&mut usage, usage_value, shape);
            if usage.service_tier.is_none()
                && let Some(tier) = service_tier_from_envelope(element)
            {
                usage.service_tier = Some(tier.to_owned());
            }
        }
        return usage;
    }

    let Some(usage_value) = usage_value_for_shape(value, shape).filter(|value| !value.is_null())
    else {
        return UsageCounts::default();
    };
    let mut usage = UsageCounts {
        present: true,
        billable_present: true,
        ..UsageCounts::default()
    };
    merge_usage_value_for_shape(&mut usage, usage_value, shape);
    if usage.service_tier.is_none()
        && let Some(tier) = service_tier_from_envelope(value)
    {
        usage.service_tier = Some(tier.to_owned());
    }
    usage
}

fn service_tier_from_envelope(value: &Value) -> Option<&str> {
    value
        .get("service_tier")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("service_tier"))
                .and_then(Value::as_str)
        })
}

pub(crate) fn merge_usage_value(usage: &mut UsageCounts, reported: &Value) {
    merge_usage_value_for_shape(usage, reported, None);
}

pub(crate) fn merge_usage_value_for_shape(
    usage: &mut UsageCounts,
    reported: &Value,
    shape: Option<UsageShape>,
) {
    let anthropic_cache_creation_evidence = cache_creation_evidence(reported);
    usage
        .cache_creation_evidence
        .merge(anthropic_cache_creation_evidence);
    if !has_anthropic_cache_creation_fields(reported) {
        usage
            .cache_creation_evidence
            .merge(provider_cache_creation_evidence(reported));
    }
    usage.cache_read_evidence.merge(
        reported
            .get("cache_read_input_tokens")
            .filter(|value| !value.is_null())
            .map_or(
                CacheTokenEvidence::Unreported,
                CacheTokenEvidence::from_value,
            ),
    );
    let prompt_token_count = reported
        .get("promptTokenCount")
        .or_else(|| reported.get("prompt_tokens"))
        .and_then(Value::as_u64);
    if reported
        .get("promptTokenCount")
        .and_then(Value::as_u64)
        .is_some()
    {
        usage.input_includes_cached = true;
    }
    if let Some(input_tokens) = reported
        .get("input_tokens")
        .and_then(Value::as_u64)
        .or(prompt_token_count)
    {
        usage.gross_input_tokens = Some(input_tokens);
        usage.input_tokens = input_tokens;
    }
    if let Some(output_tokens) = reported
        .get("output_tokens")
        .or_else(|| reported.get("completion_tokens"))
        .and_then(Value::as_u64)
    {
        usage.output_tokens = output_tokens;
    }
    if matches!(
        shape,
        Some(
            UsageShape::OpenaiResponses
                | UsageShape::OpenaiChatCompletions
                | UsageShape::OpenaiCompletions
                | UsageShape::OpenaiImages
        )
    ) {
        let input_image_tokens = reported
            .get("input_tokens_details")
            .or_else(|| reported.get("input_token_details"))
            .or_else(|| reported.get("prompt_tokens_details"))
            .and_then(|details| details.get("image_tokens"))
            .and_then(Value::as_u64);
        if let Some(image_tokens) = input_image_tokens {
            usage.input_image_tokens = Some(image_tokens);
        }
        let output_image_tokens = reported
            .get("output_tokens_details")
            .or_else(|| reported.get("completion_tokens_details"))
            .and_then(|details| details.get("image_tokens"))
            .and_then(Value::as_u64);
        if let Some(image_tokens) = output_image_tokens {
            usage.output_image_tokens = Some(image_tokens);
        }
    }
    let cached_tokens = reported
        .get("input_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .or_else(|| {
            reported
                .get("input_token_details")
                .and_then(|details| details.get("cached_tokens"))
        })
        .or_else(|| {
            reported
                .get("prompt_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
        })
        .and_then(Value::as_u64);
    if let Some(cached_tokens) = cached_tokens {
        usage.input_includes_cached = true;
        usage.cache_read_input_tokens = cached_tokens;
        usage.cache_read_evidence.merge(if cached_tokens == 0 {
            CacheTokenEvidence::Zero
        } else {
            CacheTokenEvidence::Positive
        });
    } else if let Some(cached_tokens) = reported.get("cachedContentTokenCount") {
        usage
            .cache_read_evidence
            .merge(CacheTokenEvidence::from_value(cached_tokens));
        if let Some(cached_tokens) = cached_tokens.as_u64() {
            if usage.gross_input_tokens.is_some() {
                usage.input_includes_cached = true;
            }
            usage.cache_read_input_tokens = cached_tokens;
        }
    }
    let (cc_total, cc_5m, cc_1h) = parse_cache_creation_split(reported);
    if cc_total > 0
        || reported.get("cache_creation").is_some()
        || reported.get("cache_creation_input_tokens").is_some()
    {
        usage.cache_creation_input_tokens = cc_total;
        usage.cache_creation_input_tokens_5m = cc_5m;
        usage.cache_creation_input_tokens_1h = cc_1h;
    } else if let Some(cache_write_tokens) = reported
        .get("input_tokens_details")
        .and_then(|details| details.get("cache_write_tokens"))
        .or_else(|| {
            reported
                .get("input_token_details")
                .and_then(|details| details.get("cache_write_tokens"))
        })
        .and_then(Value::as_u64)
        .or_else(|| {
            reported
                .get("cacheCreationTokenCount")
                .and_then(Value::as_u64)
        })
    {
        usage.cache_creation_input_tokens = cache_write_tokens;
        usage.input_includes_cached = true;
        usage.present = true;
    }
    if let Some(cache_read) = reported
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
    {
        usage.cache_read_input_tokens = cache_read;
    }
    if usage.input_includes_cached
        && let Some(gross) = usage.gross_input_tokens
    {
        usage.input_tokens = gross
            .saturating_sub(usage.cache_read_input_tokens)
            .saturating_sub(usage.cache_creation_input_tokens);
    }
    if let Some(reasoning_tokens) = reported
        .get("output_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
        .or_else(|| {
            reported
                .get("completion_tokens_details")
                .and_then(|details| details.get("reasoning_tokens"))
        })
        .or_else(|| reported.get("thoughtsTokenCount"))
        .and_then(Value::as_u64)
    {
        usage.thinking_tokens = reasoning_tokens;
    }
    if let Some(tool_use_prompt_tokens) = reported
        .get("toolUsePromptTokenCount")
        .and_then(Value::as_u64)
    {
        usage.tool_use_prompt_tokens = Some(tool_use_prompt_tokens);
    }
    if reported.get("output_tokens").is_none() {
        let candidates_tokens = reported.get("candidatesTokenCount").and_then(Value::as_u64);
        if let Some(candidates_tokens) = candidates_tokens {
            usage.last_candidates_tokens = candidates_tokens;
        }
        if candidates_tokens.is_some()
            || reported
                .get("thoughtsTokenCount")
                .and_then(Value::as_u64)
                .is_some()
        {
            // Gemini candidates exclude thoughts; other providers already
            // include their reasoning/thinking subset in output_tokens.
            usage.output_tokens = usage
                .last_candidates_tokens
                .saturating_add(usage.thinking_tokens);
        }
    }
    if let Some(thinking) = reported
        .get("output_tokens_details")
        .and_then(|v| v.get("thinking_tokens"))
        .and_then(Value::as_u64)
    {
        usage.thinking_tokens = thinking;
    }
    if let Some(server_tool_use) = reported.get("server_tool_use") {
        if let Some(ws) = server_tool_use
            .get("web_search_requests")
            .and_then(Value::as_u64)
        {
            usage.web_search_requests = ws;
        }
        if let Some(wf) = server_tool_use
            .get("web_fetch_requests")
            .and_then(Value::as_u64)
        {
            usage.web_fetch_requests = wf;
        }
    }
    if let Some(tier) = reported.get("service_tier").and_then(Value::as_str) {
        usage.service_tier = Some(tier.to_owned());
    }
    if let Some(geo) = reported.get("inference_geo").and_then(Value::as_str) {
        usage.inference_geo = Some(geo.to_owned());
    }
    if let Some(iterations) = reported.get("iterations").filter(|v| !v.is_null()) {
        usage.iterations = Some(iterations.clone());
    }
}

fn canonical_upstream_error_from_value(value: &Value) -> Option<CanonicalUpstreamError> {
    let is_native_failed_response = value.get("object").and_then(Value::as_str) == Some("response")
        && value.get("status").and_then(Value::as_str) == Some("failed");
    if is_native_failed_response {
        return observe_http_error_value(value);
    }
    if value.get("type").and_then(Value::as_str) != Some("error") {
        return None;
    }
    let (error_type, error_message) = bounded_upstream_error(value);
    Some(CanonicalUpstreamError {
        error_type: Some(error_type?),
        error_message: Some(error_message?),
        retry_after: None,
    })
}

/// Extract a structured upstream error from an already-parsed HTTP error
/// response body.
///
/// Unlike [`canonical_upstream_error_from_value`], this does not require the
/// Anthropic `{"type":"error", ...}` envelope: any JSON object carrying an
/// `error` object qualifies, with `error.type` preferred over `error.code`.
///
/// Callers MUST only invoke this behind an upstream non-success status; the
/// extractor performs no status check, so the status gate is what prevents a
/// 200 body that merely contains error-shaped content from being
/// misclassified.
///
/// Type and message are independent: a type-only body still yields `Some` so
/// `upstream_error_type` stays groupable, and a message-only body preserves
/// the message. `None` is returned only when neither field is usable, so the
/// caller keeps its bounded raw-body fallback.
pub(crate) fn observe_http_error_value(value: &Value) -> Option<CanonicalUpstreamError> {
    let error_obj = value.get("error")?;
    let error_type = ["type", "code", "status"]
        .iter()
        .filter_map(|key| error_obj.get(*key).and_then(Value::as_str))
        .filter(|candidate| !candidate.is_empty())
        .find_map(BoundedErrorType::parse)
        .or_else(|| {
            error_obj
                .get("code")
                .and_then(Value::as_u64)
                .and_then(|code| match code {
                    400 => Some("INVALID_ARGUMENT"),
                    401 => Some("UNAUTHENTICATED"),
                    403 => Some("PERMISSION_DENIED"),
                    404 => Some("NOT_FOUND"),
                    429 => Some("RESOURCE_EXHAUSTED"),
                    500 => Some("INTERNAL"),
                    503 => Some("UNAVAILABLE"),
                    _ => None,
                })
                .and_then(BoundedErrorType::parse)
        });
    let error_message = error_obj
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .map(BoundedErrorMessage::parse);
    let retry_after = error_obj
        .get("details")
        .and_then(Value::as_array)
        .and_then(|details| {
            details.iter().find_map(|detail| {
                (detail.get("@type").and_then(Value::as_str)
                    == Some("type.googleapis.com/google.rpc.RetryInfo"))
                .then(|| detail.get("retryDelay").and_then(Value::as_str))
                .flatten()
                .filter(|delay| valid_retry_delay(delay))
                .map(ToOwned::to_owned)
            })
        });
    if error_type.is_none() && error_message.is_none() && retry_after.is_none() {
        return None;
    }
    Some(CanonicalUpstreamError {
        error_type,
        error_message,
        retry_after,
    })
}
fn valid_retry_delay(delay: &str) -> bool {
    if delay.is_empty() || delay.len() > 64 {
        return false;
    }
    let Some(number) = ["ms", "s", "m", "h"]
        .iter()
        .find_map(|unit| delay.strip_suffix(unit))
    else {
        return false;
    };
    !number.is_empty()
        && number.bytes().any(|byte| byte.is_ascii_digit())
        && number.bytes().filter(|&byte| byte == b'.').count() <= 1
        && number
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
        && !number.starts_with('.')
        && !number.ends_with('.')
}

/// [`observe_http_error_value`] for a raw body buffer; parses once with
/// sonic-rs. Used by paths that never needed the body as JSON otherwise
/// (e.g. the streaming upstream-error fallback).
pub(crate) fn observe_http_error_body(body: &[u8]) -> Option<CanonicalUpstreamError> {
    let value = sonic_rs::from_slice::<Value>(body).ok()?;
    observe_http_error_value(&value)
}

/// Detect a mid-stream `event: error` (or `data: {"type":"error",...}`)
/// emission. Returns the upstream error's `type` and `message` if found, with
/// `signer`'s secret masked before the message cap is applied.
pub(crate) fn detect_mid_stream_error(
    event: &ParsedSseEvent<'_>,
    signer: &dyn Signer,
) -> Option<UpstreamStreamError> {
    detect_mid_stream_error_with(event, Some(signer))
}

fn detect_mid_stream_error_with(
    event: &ParsedSseEvent<'_>,
    signer: Option<&dyn Signer>,
) -> Option<UpstreamStreamError> {
    let value = event.value().ok().flatten()?;
    let type_str = value.get("type").and_then(Value::as_str);
    let top_level_error = value.get("error").filter(|error| !error.is_null());
    let is_response_failed = type_str == Some("response.failed")
        || event.event_name == Some(b"response.failed")
        || (value.get("object").and_then(Value::as_str) == Some("response")
            && value.get("status").and_then(Value::as_str) == Some("failed"));
    let nested_response_error = value
        .get("response")
        .and_then(|resp| resp.get("error"))
        .filter(|error| !error.is_null());
    if !event.named_error
        && type_str != Some("error")
        && top_level_error.is_none()
        && !is_response_failed
        && nested_response_error.is_none()
    {
        return None;
    }
    let error_source = if top_level_error.is_some() {
        value
    } else if let Some(response_obj) = value
        .get("response")
        .filter(|r| r.get("error").is_some_and(|error| !error.is_null()))
    {
        response_obj
    } else {
        value
    };
    let (mut error_type, error_message) = bounded_upstream_error(error_source);
    if error_type.is_none() {
        let error_obj = error_source.get("error").unwrap_or(error_source);
        let candidate_keys: &[&str] = if is_response_failed || nested_response_error.is_some() {
            &["code", "status"]
        } else {
            &["status"]
        };
        error_type = candidate_keys
            .iter()
            .filter_map(|key| error_obj.get(*key).and_then(Value::as_str))
            .filter(|candidate| !candidate.is_empty())
            .find_map(BoundedErrorType::parse);
    }
    if error_type.is_none() && is_response_failed {
        error_type = BoundedErrorType::parse("response_failed");
    }
    Some(UpstreamStreamError {
        error_type: error_type.map(|error_type| scrub_string(signer, error_type.into_string())),
        error_message: error_message.map(|message| message.finish(signer)),
    })
}

/// Detect an abnormal `stop_reason` on a `message_delta` SSE event. Returns
/// `Some` only for `refusal` and `model_context_window_exceeded`; every other
/// stop reason and every other event type returns `None`.
pub(crate) fn detect_abnormal_stop(event: &ParsedSseEvent<'_>) -> Option<AbnormalStop> {
    let value = event.value().ok().flatten()?;
    if value.get("type").and_then(Value::as_str) != Some("message_delta") {
        return None;
    }
    let delta = value.get("delta")?;
    abnormal_stop_from_fields(delta.get("stop_reason"), delta.get("stop_details"))
}

/// Detect an abnormal `stop_reason` on a buffered non-streaming Messages
/// response body. Same rule as [`detect_abnormal_stop`], reading the top-level
/// `stop_reason` / `stop_details` fields. Only a Messages-shaped body
/// (`type == "message"`) qualifies: the non-streaming path also serves
/// passthrough routes whose bodies may carry an unrelated `stop_reason`.
pub(crate) fn detect_abnormal_stop_non_stream(value: &Value) -> Option<AbnormalStop> {
    if value.get("type").and_then(Value::as_str) != Some("message") {
        return None;
    }
    abnormal_stop_from_fields(value.get("stop_reason"), value.get("stop_details"))
}

fn abnormal_stop_from_fields(
    stop_reason: Option<&Value>,
    stop_details: Option<&Value>,
) -> Option<AbnormalStop> {
    let stop_reason = stop_reason.and_then(Value::as_str)?;
    if !matches!(stop_reason, "refusal" | "model_context_window_exceeded") {
        return None;
    }
    Some(AbnormalStop {
        stop_reason: stop_reason.to_owned(),
        category: stop_details
            .and_then(|details| details.get("category"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        explanation: stop_details
            .and_then(|details| details.get("explanation"))
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn bounded_upstream_error(
    value: &Value,
) -> (Option<BoundedErrorType>, Option<BoundedErrorMessage>) {
    let error_obj = value.get("error").unwrap_or(value);
    (
        error_obj
            .get("type")
            .and_then(Value::as_str)
            .and_then(BoundedErrorType::parse),
        error_obj
            .get("message")
            .and_then(Value::as_str)
            .map(BoundedErrorMessage::parse),
    )
}

fn has_anthropic_cache_creation_fields(usage: &Value) -> bool {
    [
        "cache_creation_input_tokens",
        "cache_creation",
        "cache_creation_input_tokens_5m",
        "cache_creation_input_tokens_1h",
    ]
    .into_iter()
    .any(|field| usage.get(field).is_some())
}

fn provider_cache_creation_evidence(usage: &Value) -> CacheTokenEvidence {
    let values = [
        usage
            .get("input_tokens_details")
            .and_then(|details| details.get("cache_write_tokens")),
        usage
            .get("input_token_details")
            .and_then(|details| details.get("cache_write_tokens")),
        usage.get("cacheCreationTokenCount"),
    ];
    let mut invalid = false;
    for value in values.into_iter().flatten() {
        if value.is_null() {
            continue;
        }
        match CacheTokenEvidence::from_value(value) {
            CacheTokenEvidence::Unreported => unreachable!("value is present"),
            evidence @ (CacheTokenEvidence::Zero | CacheTokenEvidence::Positive) => {
                return evidence;
            }
            CacheTokenEvidence::Invalid => invalid = true,
        }
    }
    if invalid {
        CacheTokenEvidence::Invalid
    } else {
        CacheTokenEvidence::Unreported
    }
}

/// Parse Anthropic's cache-creation fields, which require a complete
/// representation when split buckets are used.
fn parse_cache_creation_split(usage: &Value) -> (u64, u64, u64) {
    let flat = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    cache_creation_split(flat, usage.get("cache_creation"))
}

/// Unlike the accounting split, inference requires complete numeric evidence.
/// Never fill missing split buckets with zero or prefer a contradictory total.
fn cache_creation_evidence(usage: &Value) -> CacheTokenEvidence {
    let flat = usage.get("cache_creation_input_tokens");
    let nested = usage.get("cache_creation");
    let suffix_5m = usage.get("cache_creation_input_tokens_5m");
    let suffix_1h = usage.get("cache_creation_input_tokens_1h");
    if flat.is_none_or(Value::is_null)
        && nested.is_none_or(Value::is_null)
        && suffix_5m.is_none()
        && suffix_1h.is_none()
    {
        return CacheTokenEvidence::Unreported;
    }
    let flat_total = match flat {
        Some(value) => match value.as_u64() {
            Some(total) => Some(total),
            None => return CacheTokenEvidence::Invalid,
        },
        None => None,
    };
    let nested_total = match nested {
        Some(value) => {
            let Some(total) = value
                .get("ephemeral_5m_input_tokens")
                .and_then(Value::as_u64)
                .zip(
                    value
                        .get("ephemeral_1h_input_tokens")
                        .and_then(Value::as_u64),
                )
                .and_then(|(five_minute, one_hour)| five_minute.checked_add(one_hour))
            else {
                return CacheTokenEvidence::Invalid;
            };
            Some(total)
        }
        None => None,
    };
    let suffix_total = match (suffix_5m, suffix_1h) {
        (None, None) => None,
        (Some(five_minute), Some(one_hour)) => {
            let Some(total) = five_minute
                .as_u64()
                .zip(one_hour.as_u64())
                .and_then(|(five_minute, one_hour)| five_minute.checked_add(one_hour))
            else {
                return CacheTokenEvidence::Invalid;
            };
            Some(total)
        }
        _ => return CacheTokenEvidence::Invalid,
    };
    let total = flat_total.or(nested_total).or(suffix_total);
    if nested_total.is_some_and(|nested| Some(nested) != total)
        || suffix_total.is_some_and(|suffix| Some(suffix) != total)
    {
        return CacheTokenEvidence::Invalid;
    }
    match total {
        Some(0) => CacheTokenEvidence::Zero,
        Some(_) => CacheTokenEvidence::Positive,
        None => CacheTokenEvidence::Unreported,
    }
}

/// Splits `cache_creation` into `(total, 5m, 1h)`.
///
/// The nested `cache_creation` object is authoritative when it carries numbers. The
/// `m5 == 0 && m1 == 0` branch is defensive hardening, not a documented provider shape: it keeps a
/// positive flat `cache_creation_input_tokens` from being dropped if the nested object ever
/// arrives present-but-empty. That path has to guess a bucket and picks 5m, the provider default
/// and the TTL Anthropic uses for its own server-tool breakpoints. If a 1h write ever reached us
/// this way it would be priced at 1.25x base input instead of 2.0x.
pub(crate) fn cache_creation_split(flat: u64, nested: Option<&Value>) -> (u64, u64, u64) {
    if let Some(cc) = nested.and_then(Value::as_object) {
        let m5 = cc
            .get("ephemeral_5m_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let m1 = cc
            .get("ephemeral_1h_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if m5 == 0 && m1 == 0 && flat > 0 {
            return (flat, flat, 0);
        }
        return (m5.saturating_add(m1), m5, m1);
    }
    (flat, flat, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_event(payload: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"event: message_delta\ndata: ");
        out.extend_from_slice(payload.as_bytes());
        out.extend_from_slice(b"\n\n");
        out
    }

    fn accumulate_raw_sse_usage(raw: &[u8], usage: &mut UsageCounts) -> SseUsageUpdate {
        let event = parse_sse_event(raw);
        accumulate_sse_usage(&event, usage)
    }

    fn detect_raw_mid_stream_error(raw: &[u8]) -> Option<UpstreamStreamError> {
        let event = parse_sse_event(raw);
        detect_mid_stream_error_with(&event, None)
    }

    #[test]
    fn gemini_cached_content_evidence_distinguishes_positive_zero_and_absent() {
        for (payload, expected) in [
            (serde_json::json!({}), CacheTokenEvidence::Unreported),
            (
                serde_json::json!({"cachedContentTokenCount": 0}),
                CacheTokenEvidence::Zero,
            ),
            (
                serde_json::json!({"cachedContentTokenCount": 17}),
                CacheTokenEvidence::Positive,
            ),
        ] {
            let usage = observe_non_stream_json_value_for_shape(
                &serde_json::json!({"usageMetadata": payload}),
                UsageShape::GeminiGenerateContent,
            )
            .usage;
            assert_eq!(usage.cache_read_evidence, expected);
        }
    }

    #[test]
    fn cache_creation_evidence_requires_complete_numeric_zero() {
        for (reported, expected_evidence, expected_counts) in [
            (r#"{}"#, CacheTokenEvidence::Unreported, (0, 0, 0)),
            (
                r#"{"cache_creation_input_tokens":null}"#,
                CacheTokenEvidence::Unreported,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":null}"#,
                CacheTokenEvidence::Unreported,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0}"#,
                CacheTokenEvidence::Zero,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                CacheTokenEvidence::Zero,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                CacheTokenEvidence::Zero,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":7}"#,
                CacheTokenEvidence::Positive,
                (7, 7, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}"#,
                CacheTokenEvidence::Positive,
                (5, 2, 3),
            ),
            (
                r#"{"cache_creation_input_tokens":null,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation":null}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":0}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_1h_input_tokens":0}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":null}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation":{}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0}}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}"#,
                CacheTokenEvidence::Invalid,
                (5, 2, 3),
            ),
            (
                r#"{"cache_creation_input_tokens":7,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                CacheTokenEvidence::Invalid,
                (7, 7, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":9,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}}"#,
                CacheTokenEvidence::Invalid,
                (5, 2, 3),
            ),
            (
                r#"{"cache_creation_input_tokens":"0"}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":false}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":-1}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0.5}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":[]}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":18446744073709551615,"ephemeral_1h_input_tokens":1}}"#,
                CacheTokenEvidence::Invalid,
                (u64::MAX, u64::MAX, 1),
            ),
        ] {
            let body = format!(r#"{{"usage":{reported}}}"#);
            let observation = observe_non_stream_json_body(body.as_bytes());
            let usage = observation.usage;
            assert_eq!(
                usage.cache_creation_evidence, expected_evidence,
                "{reported}"
            );
            assert_eq!(
                (
                    usage.cache_creation_input_tokens,
                    usage.cache_creation_input_tokens_5m,
                    usage.cache_creation_input_tokens_1h,
                ),
                expected_counts,
                "numeric accounting must remain unchanged: {reported}",
            );
        }
    }

    #[test]
    fn cache_creation_suffix_evidence_requires_complete_consistent_numeric_pair() {
        for (reported, expected_evidence, expected_counts) in [
            (
                r#"{"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Zero,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Zero,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":2,"cache_creation_input_tokens_1h":3}"#,
                CacheTokenEvidence::Positive,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3},"cache_creation_input_tokens_5m":2,"cache_creation_input_tokens_1h":3}"#,
                CacheTokenEvidence::Positive,
                (5, 2, 3),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":null,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":null}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":"0","cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":"0"}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":-1,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":0.5}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":false,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens_5m":18446744073709551615,"cache_creation_input_tokens_1h":1}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation_input_tokens_5m":1,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":1,"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (1, 1, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":1}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3},"cache_creation_input_tokens_5m":0,"cache_creation_input_tokens_1h":0}"#,
                CacheTokenEvidence::Invalid,
                (5, 2, 3),
            ),
            (
                r#"{"cache_creation_input_tokens":5,"cache_creation_input_tokens_5m":2,"cache_creation_input_tokens_1h":4}"#,
                CacheTokenEvidence::Invalid,
                (5, 5, 0),
            ),
            (
                r#"{"cache_creation_input_tokens":0,"cache_creation_input_tokens_5m":0}"#,
                CacheTokenEvidence::Invalid,
                (0, 0, 0),
            ),
        ] {
            let body = format!(r#"{{"usage":{reported}}}"#);
            let mut usage = observe_non_stream_json_body(body.as_bytes()).usage;
            merge_usage_value(
                &mut usage,
                &serde_json::json!({"cache_read_input_tokens": 2400}),
            );
            assert_eq!(
                usage.cache_creation_evidence, expected_evidence,
                "{reported}"
            );
            assert_eq!(
                usage.has_affirmative_read_only_cache_usage(),
                expected_evidence == CacheTokenEvidence::Zero,
                "{reported}"
            );
            assert_eq!(
                (
                    usage.cache_creation_input_tokens,
                    usage.cache_creation_input_tokens_5m,
                    usage.cache_creation_input_tokens_1h,
                ),
                expected_counts,
                "numeric accounting must remain unchanged: {reported}",
            );
        }
    }

    #[test]
    fn cache_creation_evidence_survives_missing_and_null_sse_updates() {
        for initial in [
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}"#,
            r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
        ] {
            let mut usage = UsageCounts::default();
            let start = format!(
                "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"usage\":{initial}}}}}\n\n"
            );
            assert!(accumulate_raw_sse_usage(start.as_bytes(), &mut usage).message_start_usage);
            assert!(usage.has_affirmative_read_only_cache_usage());
            for reported in [
                r#"{"output_tokens":17}"#,
                r#"{"cache_creation_input_tokens":null,"cache_read_input_tokens":null}"#,
                r#"{"cache_creation":null}"#,
            ] {
                let frame = raw_event(&format!(r#"{{"type":"message_delta","usage":{reported}}}"#));
                accumulate_raw_sse_usage(&frame, &mut usage);
                assert!(usage.has_affirmative_read_only_cache_usage(), "{reported}");
                assert_eq!(usage.cache_read_input_tokens, 2_400);
                assert_eq!(usage.cache_creation_input_tokens, 0);
                assert_eq!(usage.output_tokens, 17);
            }
        }
    }

    #[test]
    fn partial_nested_sse_snapshots_do_not_combine_into_zero_evidence() {
        let mut usage = UsageCounts::default();
        for reported in [
            r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0}}"#,
            r#"{"cache_creation":{"ephemeral_1h_input_tokens":0}}"#,
            r#"{"cache_creation_input_tokens":null}"#,
        ] {
            let frame = raw_event(&format!(r#"{{"type":"message_delta","usage":{reported}}}"#));
            accumulate_raw_sse_usage(&frame, &mut usage);
            assert_eq!(usage.cache_creation_input_tokens, 0);
            assert_eq!(usage.cache_creation_evidence, CacheTokenEvidence::Invalid);
            assert!(!usage.has_affirmative_read_only_cache_usage());
        }
    }

    #[test]
    fn complete_sse_cache_evidence_replaces_previous_cumulative_snapshot() {
        let mut usage = UsageCounts::default();
        for (reported, expected_creation, read_only) in [
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":9}"#,
                9,
                false,
            ),
            (r#"{"cache_creation_input_tokens":0}"#, 0, true),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":0}}"#,
                0,
                false,
            ),
            (
                r#"{"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                0,
                true,
            ),
            (r#"{"cache_read_input_tokens":"2400"}"#, 0, false),
            (r#"{"cache_read_input_tokens":2400}"#, 0, true),
            (r#"{"cache_read_input_tokens":0}"#, 0, false),
        ] {
            let frame = raw_event(&format!(r#"{{"type":"message_delta","usage":{reported}}}"#));
            accumulate_raw_sse_usage(&frame, &mut usage);
            assert_eq!(
                usage.cache_creation_input_tokens, expected_creation,
                "{reported}"
            );
            assert_eq!(
                usage.has_affirmative_read_only_cache_usage(),
                read_only,
                "{reported}"
            );
        }
    }

    #[test]
    fn parsed_event_joins_multiline_data_for_all_observers() {
        let raw = b"event: message_delta\r\ndata: {\"type\":\"message_delta\",\r\ndata: \"usage\":{\"output_tokens\":17}}\r\n\r\n";
        let event = parse_sse_event(raw);
        let mut usage = UsageCounts::default();

        let update = accumulate_sse_usage(&event, &mut usage);

        assert_eq!(event.event_name(), Some(b"message_delta".as_slice()));
        assert!(!update.message_stop);
        assert_eq!(usage.output_tokens, 17);
    }

    #[test]
    fn json_type_takes_precedence_over_sse_event_name() {
        let raw = b"event: message_stop\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":19}}\n\n";
        let event = parse_sse_event(raw);
        let mut usage = UsageCounts::default();

        let update = accumulate_sse_usage(&event, &mut usage);

        assert_eq!(event.event_type(), Some("message_delta"));
        assert!(!update.message_stop);
        assert_eq!(usage.output_tokens, 19);
    }

    #[test]
    fn malformed_payload_is_available_to_fail_closed_observers() {
        let event = parse_sse_event(b"event: content_block_start\ndata: {not-json}\n\n");

        assert_eq!(event.value(), Err(SseEventParseError::InvalidData));
    }

    #[test]
    fn later_error_event_name_classifies_mid_stream_error() {
        let raw = "event: message_delta\nevent:\u{a0}error\u{a0}\ndata: {\"type\":\"message_delta\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"later named error\"}}\n\n";
        let event = parse_sse_event(raw.as_bytes());

        let error = detect_mid_stream_error_with(&event, None).expect("later event:error detected");

        assert_eq!(event.event_name(), Some(b"message_delta".as_slice()));
        assert_eq!(error.error_type.as_deref(), Some("overloaded_error"));
        assert_eq!(error.error_message.as_deref(), Some("later named error"));
    }
    #[test]
    fn response_completed_null_error_is_not_mid_stream_error() {
        let raw = b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null}}\n\n";
        assert!(detect_raw_mid_stream_error(raw).is_none());
        let top_level = b"data: {\"type\":\"response.completed\",\"error\":null}\n\n";
        assert!(detect_raw_mid_stream_error(top_level).is_none());
    }

    #[test]
    fn merges_full_anthropic_usage_object() {
        let mut usage = UsageCounts::default();
        let raw = raw_event(
            r#"{"type":"message_delta","usage":{"input_tokens":120,"output_tokens":48,"cache_creation_input_tokens":40,"cache_creation":{"ephemeral_5m_input_tokens":25,"ephemeral_1h_input_tokens":15},"cache_read_input_tokens":10,"output_tokens_details":{"thinking_tokens":12},"server_tool_use":{"web_search_requests":3,"web_fetch_requests":1},"service_tier":"priority","inference_geo":"us-east"}}"#,
        );
        accumulate_raw_sse_usage(&raw, &mut usage);
        assert!(usage.present);
        assert_eq!(usage.input_tokens, 120);
        assert_eq!(usage.output_tokens, 48);
        assert_eq!(usage.cache_creation_input_tokens, 40);
        assert_eq!(usage.cache_creation_input_tokens_5m, 25);
        assert_eq!(usage.cache_creation_input_tokens_1h, 15);
        assert_eq!(usage.cache_read_input_tokens, 10);
        assert_eq!(usage.thinking_tokens, 12);
        assert_eq!(usage.web_search_requests, 3);
        assert_eq!(usage.web_fetch_requests, 1);
        assert_eq!(usage.service_tier.as_deref(), Some("priority"));
        assert_eq!(usage.inference_geo.as_deref(), Some("us-east"));
    }

    #[test]
    fn openai_cache_write_tokens_are_subtracted_from_gross_input() {
        let mut usage = UsageCounts::default();
        merge_usage_value(
            &mut usage,
            &serde_json::json!({
                "input_tokens": 100,
                "input_tokens_details": {
                    "cached_tokens": 20,
                    "cache_write_tokens": 30,
                },
            }),
        );
        assert_eq!(usage.cache_creation_input_tokens, 30);
        assert_eq!(usage.input_tokens, 50);
    }

    #[test]
    fn provider_cache_write_evidence_distinguishes_zero_positive_and_invalid() {
        for (reported, expected) in [
            (
                serde_json::json!({"input_tokens_details": {"cache_write_tokens": 0}}),
                CacheTokenEvidence::Zero,
            ),
            (
                serde_json::json!({"input_token_details": {"cache_write_tokens": 7}}),
                CacheTokenEvidence::Positive,
            ),
            (
                serde_json::json!({"cacheCreationTokenCount": 0}),
                CacheTokenEvidence::Zero,
            ),
            (
                serde_json::json!({"cacheCreationTokenCount": 11}),
                CacheTokenEvidence::Positive,
            ),
            (
                serde_json::json!({"cacheCreationTokenCount": "7"}),
                CacheTokenEvidence::Invalid,
            ),
            (serde_json::json!({}), CacheTokenEvidence::Unreported),
        ] {
            let mut usage = UsageCounts::default();
            merge_usage_value(&mut usage, &reported);
            assert_eq!(usage.cache_creation_evidence, expected, "{reported}");
        }
    }

    #[test]
    fn gemini_cache_creation_tokens_are_subtracted_from_gross_input() {
        let mut usage = UsageCounts::default();
        merge_usage_value(
            &mut usage,
            &serde_json::json!({
                "promptTokenCount": 100,
                "cachedContentTokenCount": 20,
                "cacheCreationTokenCount": 30,
            }),
        );
        assert_eq!(usage.cache_creation_input_tokens, 30);
        assert_eq!(usage.input_tokens, 50);
    }

    #[test]
    fn empty_nested_cache_creation_falls_back_to_positive_flat_total() {
        let usage = serde_json::json!({
            "cache_creation": {},
            "cache_creation_input_tokens": 5_000,
        });

        assert_eq!(parse_cache_creation_split(&usage), (5_000, 5_000, 0));
    }

    #[test]
    fn populated_nested_cache_creation_takes_precedence_over_flat_total() {
        let usage = serde_json::json!({
            "cache_creation": {
                "ephemeral_5m_input_tokens": 500,
                "ephemeral_1h_input_tokens": 500,
            },
            "cache_creation_input_tokens": 9_999,
        });

        assert_eq!(parse_cache_creation_split(&usage), (1_000, 500, 500));
    }

    #[test]
    fn null_safe_merge_preserves_message_start_input_tokens() {
        let mut usage = UsageCounts::default();
        let start = raw_event(
            r#"{"type":"message_start","message":{"usage":{"input_tokens":99,"output_tokens":1}}}"#,
        );
        accumulate_raw_sse_usage(&start, &mut usage);
        assert_eq!(usage.input_tokens, 99);

        // message_delta without input_tokens (null/absent). Must NOT zero out the 99.
        let delta = raw_event(r#"{"type":"message_delta","usage":{"output_tokens":50}}"#);
        accumulate_raw_sse_usage(&delta, &mut usage);
        assert_eq!(usage.input_tokens, 99);
        assert_eq!(usage.output_tokens, 50);
    }

    #[test]
    fn preserves_iterations_opaque() {
        let mut usage = UsageCounts::default();
        let raw = raw_event(
            r#"{"type":"message_delta","usage":{"output_tokens":10,"iterations":[{"type":"message","model":"claude-x","input_tokens":5,"output_tokens":10}]}}"#,
        );
        accumulate_raw_sse_usage(&raw, &mut usage);
        let preserved = usage.iterations.expect("iterations preserved");
        let arr = preserved.as_array().expect("iterations is array");
        assert_eq!(arr.len(), 1);
        assert_eq!(
            arr[0].get("model").and_then(Value::as_str),
            Some("claude-x")
        );
    }

    #[test]
    fn detect_mid_stream_error_from_named_event() {
        let raw = b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"upstream overloaded\"}}\n\n";
        let err = detect_raw_mid_stream_error(raw).expect("detected");
        assert_eq!(err.error_type.as_deref(), Some("overloaded_error"));
        assert_eq!(err.error_message.as_deref(), Some("upstream overloaded"));
    }

    #[test]
    fn detect_mid_stream_error_from_data_type_only() {
        let raw = b"data: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"too many\"}}\n\n";
        let err = detect_raw_mid_stream_error(raw).expect("detected");
        assert_eq!(err.error_type.as_deref(), Some("rate_limit_error"));
    }

    #[test]
    fn detect_mid_stream_error_ignores_normal_events() {
        let raw = raw_event(r#"{"type":"message_delta","usage":{"output_tokens":1}}"#);
        assert!(detect_raw_mid_stream_error(&raw).is_none());
    }

    #[test]
    fn detect_abnormal_stop_extracts_refusal_details() {
        let raw = raw_event(
            r#"{"type":"message_delta","delta":{"stop_reason":"refusal","stop_details":{"type":"refusal","category":"cyber","explanation":"declined","fallback_credit_token":null}},"usage":{"output_tokens":0}}"#,
        );
        let event = parse_sse_event(&raw);
        let stop = detect_abnormal_stop(&event).expect("abnormal stop");
        assert_eq!(stop.stop_reason, "refusal");
        assert_eq!(stop.category.as_deref(), Some("cyber"));
        assert_eq!(stop.explanation.as_deref(), Some("declined"));
    }

    #[test]
    fn detect_abnormal_stop_extracts_context_window_exceeded() {
        let raw = raw_event(
            r#"{"type":"message_delta","delta":{"stop_reason":"model_context_window_exceeded"},"usage":{"output_tokens":0}}"#,
        );
        let event = parse_sse_event(&raw);
        let stop = detect_abnormal_stop(&event).expect("abnormal stop");
        assert_eq!(stop.stop_reason, "model_context_window_exceeded");
        assert!(stop.category.is_none());
        assert!(stop.explanation.is_none());
    }

    #[test]
    fn detect_abnormal_stop_ignores_end_turn() {
        let raw = raw_event(
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}"#,
        );
        let event = parse_sse_event(&raw);
        assert!(detect_abnormal_stop(&event).is_none());
    }

    #[test]
    fn detect_abnormal_stop_ignores_missing_stop_reason() {
        let raw = raw_event(r#"{"type":"message_delta","usage":{"output_tokens":1}}"#);
        let event = parse_sse_event(&raw);
        assert!(detect_abnormal_stop(&event).is_none());
    }

    #[test]
    fn detect_abnormal_stop_non_stream_extracts_refusal_details() {
        let value = serde_json::json!({
            "type": "message",
            "stop_reason": "refusal",
            "stop_details": {
                "type": "refusal",
                "category": "reasoning_extraction",
                "explanation": "declined",
                "fallback_credit_token": null
            }
        });
        let stop = detect_abnormal_stop_non_stream(&value).expect("abnormal stop");
        assert_eq!(stop.stop_reason, "refusal");
        assert_eq!(stop.category.as_deref(), Some("reasoning_extraction"));
        assert_eq!(stop.explanation.as_deref(), Some("declined"));
    }

    #[test]
    fn detect_abnormal_stop_non_stream_ignores_non_message_bodies() {
        // Passthrough routes (count_tokens, models, catch-alls) can carry a
        // top-level `stop_reason` that is not a Messages stop reason.
        let value = serde_json::json!({
            "stop_reason": "refusal",
            "stop_details": {"category": "cyber", "explanation": "declined"}
        });
        assert!(detect_abnormal_stop_non_stream(&value).is_none());

        let value = serde_json::json!({
            "type": "error",
            "stop_reason": "refusal"
        });
        assert!(detect_abnormal_stop_non_stream(&value).is_none());

        let value = serde_json::json!({
            "type": "message",
            "stop_reason": "refusal"
        });
        let stop = detect_abnormal_stop_non_stream(&value).expect("abnormal stop");
        assert_eq!(stop.stop_reason, "refusal");
    }

    #[test]
    fn find_sse_event_end_lf_lf() {
        assert_eq!(find_sse_event_end(b"event: a\ndata: b\n\n"), Some(18));
    }

    #[test]
    fn find_sse_event_end_crlf_crlf() {
        assert_eq!(find_sse_event_end(b"event: a\r\ndata: b\r\n\r\n"), Some(21));
    }

    #[test]
    fn find_sse_event_end_incomplete() {
        assert_eq!(find_sse_event_end(b"event: a\ndata: b\n"), None);
    }

    #[test]
    fn stream_error_message_cap_includes_truncation_marker() {
        let long_msg = "x".repeat(UPSTREAM_ERROR_MESSAGE_MAX_BYTES + 200);
        let raw = format!(
            "event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"e\",\"message\":\"{long_msg}\"}}}}\n\n"
        );
        let err = detect_raw_mid_stream_error(raw.as_bytes()).expect("detected");
        let msg = err.error_message.expect("message present");
        assert!(msg.len() <= UPSTREAM_ERROR_MESSAGE_MAX_BYTES);
        assert!(msg.ends_with("..."));
    }

    #[test]
    fn canonical_error_body_extracts_structured_fields() {
        // Given
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"forced fake rate limit response"}}"#;

        // When
        let error = observe_non_stream_json_body(body)
            .canonical_error
            .expect("canonical error");

        // Then
        assert_eq!(error.error_type.expect("type").0, "rate_limit_error");
        assert_eq!(
            error.error_message.expect("message").0,
            "forced fake rate limit response"
        );
    }

    #[test]
    fn canonical_error_message_cap_includes_marker_on_utf8_boundary() {
        // Given
        let long_message = format!("{}🙂tail", "a".repeat(1023));
        let body = serde_json::json!({
            "type": "error",
            "error": {
                "type": "api_error",
                "message": long_message,
            }
        })
        .to_string();

        // When
        let error = observe_non_stream_json_body(body.as_bytes())
            .canonical_error
            .expect("canonical error with long message");

        // Then
        let message = error.error_message.expect("message").finish(None);
        assert_eq!(message.len(), UPSTREAM_ERROR_MESSAGE_MAX_BYTES);
        assert!(message.is_char_boundary(1021));
        assert_eq!(message, format!("{}...", "a".repeat(1021)));
    }

    #[test]
    fn canonical_error_message_preserves_exact_cap_and_bounds_ascii_and_emoji_excess() {
        // Given
        let cases = [
            ("a".repeat(1024), false),
            ("a".repeat(1025), true),
            ("🙂".repeat(256), false),
            ("🙂".repeat(257), true),
        ];

        // When / Then
        for (message, truncated) in cases {
            let body = serde_json::json!({
                "type": "error",
                "error": { "type": "api_error", "message": message },
            })
            .to_string();
            let error = observe_non_stream_json_body(body.as_bytes())
                .canonical_error
                .expect("canonical error");
            let extracted = error.error_message.expect("message").finish(None);
            assert!(extracted.len() <= UPSTREAM_ERROR_MESSAGE_MAX_BYTES);
            assert_eq!(extracted.ends_with("..."), truncated);
            if !truncated {
                assert_eq!(extracted, message);
            }
        }
    }

    #[test]
    fn canonical_error_type_accepts_256_bytes_and_rejects_oversized_values() {
        // Given
        let cases = [
            ("a".repeat(256), true),
            ("a".repeat(257), false),
            ("🙂".repeat(64), true),
            ("🙂".repeat(65), false),
        ];

        // When / Then
        for (error_type, accepted) in cases {
            let body = serde_json::json!({
                "type": "error",
                "error": { "type": error_type, "message": "bounded" },
            })
            .to_string();
            let error = observe_non_stream_json_body(body.as_bytes()).canonical_error;
            assert_eq!(error.is_some(), accepted, "type bytes={}", error_type.len());
        }
    }

    #[test]
    fn canonical_error_body_rejects_malformed_or_wrong_shapes() {
        // Given
        let controls: &[&[u8]] = &[
            b"",
            b"not-json",
            br#"{"type":"message","error":{"type":"rate_limit_error","message":"no"}}"#,
            br#"{"type":"error","error":{"message":"missing type"}}"#,
            br#"{"type":"error","error":{"type":"rate_limit_error"}}"#,
        ];

        // When / Then
        for body in controls {
            assert!(observe_non_stream_json_body(body).canonical_error.is_none());
        }
    }

    #[test]
    fn non_stream_observation_combines_usage_and_canonical_error() {
        // Given
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"bounded"},"usage":{"input_tokens":12,"output_tokens":3}}"#;

        // When
        let observation = observe_non_stream_json_body(body);

        // Then
        assert_eq!(observation.usage.input_tokens, 12);
        assert_eq!(observation.usage.output_tokens, 3);
        let error = observation.canonical_error.expect("canonical error");
        assert_eq!(error.error_type.expect("type").0, "rate_limit_error");
        assert_eq!(error.error_message.expect("message").0, "bounded");
    }

    #[test]
    fn http_error_body_does_not_require_outer_error_envelope() {
        // Given: non-Anthropic error bodies without `{"type":"error", ...}`.
        let bodies: &[&[u8]] = &[
            br#"{"error":{"code":"rate_limit_exceeded","message":"isolated upstream rejection"}}"#,
            br#"{"error":{"type":"rate_limit_error","message":"no envelope"}}"#,
            br#"{"type":"message","error":{"type":"api_error","message":"wrong outer type"}}"#,
        ];
        let expected = [
            ("rate_limit_exceeded", "isolated upstream rejection"),
            ("rate_limit_error", "no envelope"),
            ("api_error", "wrong outer type"),
        ];

        // When / Then
        for (body, (error_type, message)) in bodies.iter().zip(expected) {
            let error = observe_http_error_body(body).expect("upstream error extracted");
            assert_eq!(error.error_type.expect("type").0, error_type);
            assert_eq!(error.error_message.expect("message").0, message);
        }
    }

    #[test]
    fn http_error_body_prefers_type_over_code() {
        // Given
        let body =
            br#"{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"m"}}"#;

        // When
        let error = observe_http_error_body(body).expect("upstream error extracted");

        // Then
        assert_eq!(error.error_type.expect("type").0, "rate_limit_error");
    }

    #[test]
    fn http_error_body_falls_back_to_code_when_type_unusable() {
        // Given: `type` present but non-string or over the 256-byte bound.
        let long_type = "a".repeat(UPSTREAM_ERROR_TYPE_MAX_BYTES + 1);
        let bodies = [
            br#"{"error":{"type":429,"code":"rate_limit_exceeded","message":"m"}}"#.to_vec(),
            serde_json::json!({
                "error": {
                    "type": long_type,
                    "code": "rate_limit_exceeded",
                    "message": "m",
                }
            })
            .to_string()
            .into_bytes(),
        ];

        // When / Then
        for body in &bodies {
            let error = observe_http_error_body(body).expect("code fallback extracted");
            assert_eq!(error.error_type.expect("type").0, "rate_limit_exceeded");
        }
    }

    #[test]
    fn http_error_body_preserves_message_without_type_or_code() {
        // Given
        let body = br#"{"error":{"message":"bare upstream message"}}"#;

        // When
        let error = observe_http_error_body(body).expect("message preserved");

        // Then
        assert_eq!(
            error.into_parts_with(None),
            (None, Some("bare upstream message".to_owned()))
        );
    }

    #[test]
    fn http_error_body_preserves_type_without_message() {
        // Given: a type-only body still yields Some so `upstream_error_type`
        // stays groupable; the caller supplies its bounded raw body as the
        // message instead of a fabricated one.
        let bodies: &[&[u8]] = &[
            br#"{"error":{"type":"rate_limit_error"}}"#,
            br#"{"error":{"type":"rate_limit_error","message":""}}"#,
            br#"{"error":{"code":"rate_limit_exceeded","message":null}}"#,
        ];
        let expected = [
            "rate_limit_error",
            "rate_limit_error",
            "rate_limit_exceeded",
        ];

        // When / Then
        for (body, error_type) in bodies.iter().zip(expected) {
            let error = observe_http_error_body(body).expect("type preserved");
            assert_eq!(
                error.into_parts_with(None),
                (Some(error_type.to_owned()), None),
            );
        }
    }

    #[test]
    fn http_error_body_returns_none_without_usable_fields() {
        // Given: callers keep the bounded raw-body fallback for these.
        let bodies: &[&[u8]] = &[
            b"",
            b"not-json",
            br#"{"error":{"message":null}}"#,
            br#"{"error":{"type":"","message":""}}"#,
            br#"{"error":"rate limited"}"#,
            br#"{"message":"top-level only"}"#,
        ];

        // When / Then
        for body in bodies {
            assert!(observe_http_error_body(body).is_none());
        }
    }

    #[test]
    fn strict_non_stream_observer_still_requires_canonical_envelope() {
        // Given: the relaxed code fallback must not leak into the canonical
        // observer used for success-path usage extraction.
        let body = br#"{"error":{"code":"rate_limit_exceeded","message":"m"}}"#;

        // When / Then
        assert!(observe_non_stream_json_body(body).canonical_error.is_none());
        assert!(observe_http_error_body(body).is_some());
    }

    #[test]
    fn mid_stream_error_ignores_code_fallback() {
        // Given: SSE native semantics unchanged — `error.code` is not a
        // mid-stream error type.
        let raw = b"event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"m\"}}\n\n";

        // When
        let err = detect_raw_mid_stream_error(raw).expect("named error event");

        // Then
        assert!(err.error_type.is_none());
        assert_eq!(err.error_message.as_deref(), Some("m"));
    }

    #[test]
    fn non_stream_value_observation_matches_body_observation() {
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"bounded"},"usage":{"input_tokens":12,"output_tokens":3}}"#;
        let value = sonic_rs::from_slice::<Value>(body).expect("valid response JSON");

        assert_eq!(
            observe_non_stream_json_value(&value),
            observe_non_stream_json_body(body)
        );
    }

    #[test]
    fn non_stream_observation_rejects_malformed_error_shapes() {
        // Given
        let controls: &[&[u8]] = &[
            b"",
            b"not-json",
            br#"{"type":"message","error":{"type":"rate_limit_error","message":"no"}}"#,
            br#"{"type":"error","error":{"message":"missing type"}}"#,
            br#"{"type":"error","error":{"type":"rate_limit_error"}}"#,
        ];

        // When / Then
        for body in controls {
            let observation = observe_non_stream_json_body(body);
            assert_eq!(observation.usage, UsageCounts::default());
            assert!(observation.canonical_error.is_none());
        }
    }

    #[test]
    fn content_block_delta_thinking_delta_accumulates_estimated_tokens() {
        let mut usage = UsageCounts::default();
        let frame1 = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"estimated_tokens\":12}}\n\n".to_vec();
        let frame2 = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"estimated_tokens\":34}}\n\n".to_vec();
        let update1 = accumulate_raw_sse_usage(&frame1, &mut usage);
        let update2 = accumulate_raw_sse_usage(&frame2, &mut usage);
        assert!(!update1.message_start_usage && !update1.message_stop);
        assert!(!update2.message_start_usage && !update2.message_stop);
        assert!(usage.present);
        assert!(
            !usage.billable_present,
            "thinking estimates are progress-only until provider usage arrives"
        );
        assert_eq!(usage.estimated_thinking_progress, 46);
        assert_eq!(
            usage.thinking_tokens, 0,
            "authoritative field must remain 0"
        );
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 0);
    }

    #[test]
    fn content_block_delta_text_delta_does_not_touch_thinking_progress() {
        let mut usage = UsageCounts::default();
        let frame = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n".to_vec();
        accumulate_raw_sse_usage(&frame, &mut usage);
        assert!(!usage.present);
        assert_eq!(usage.estimated_thinking_progress, 0);
    }

    #[test]
    fn content_block_delta_null_estimated_tokens_is_noop() {
        let mut usage = UsageCounts::default();
        let frame = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"estimated_tokens\":null}}\n\n".to_vec();
        accumulate_raw_sse_usage(&frame, &mut usage);
        assert!(!usage.present);
        assert_eq!(usage.estimated_thinking_progress, 0);
    }

    #[test]
    fn thinking_delta_progress_does_not_overwrite_authoritative_thinking_tokens() {
        let mut usage = UsageCounts::default();
        let prog = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"estimated_tokens\":7}}\n\n".to_vec();
        accumulate_raw_sse_usage(&prog, &mut usage);
        let delta = raw_event(
            r#"{"type":"message_delta","usage":{"output_tokens":50,"output_tokens_details":{"thinking_tokens":42}}}"#,
        );
        accumulate_raw_sse_usage(&delta, &mut usage);
        assert_eq!(usage.estimated_thinking_progress, 7);
        assert_eq!(usage.thinking_tokens, 42);
        assert_eq!(usage.output_tokens, 50);
    }

    #[test]
    fn usage_from_json_body_extracts_all_fields() {
        let body = serde_json::json!({
            "id": "msg_x",
            "usage": {
                "input_tokens": 50,
                "output_tokens": 100,
                "cache_creation_input_tokens": 10,
                "cache_read_input_tokens": 5,
                "output_tokens_details": { "thinking_tokens": 8 },
                "server_tool_use": { "web_search_requests": 2 },
                "service_tier": "standard"
            }
        })
        .to_string();
        let usage = observe_non_stream_json_body(body.as_bytes()).usage;
        assert!(usage.present);
        assert!(usage.billable_present);
        assert_eq!(usage.input_tokens, 50);
        assert_eq!(usage.output_tokens, 100);
        assert_eq!(usage.thinking_tokens, 8);
        assert_eq!(usage.web_search_requests, 2);
        assert_eq!(usage.service_tier.as_deref(), Some("standard"));
    }
    #[test]
    fn oa_use_01_responses_usage_maps_nested_details() {
        let body = serde_json::json!({
            "usage": {
                "input_tokens": 120,
                "output_tokens": 48,
                "input_tokens_details": { "cached_tokens": 17 },
                "output_tokens_details": { "reasoning_tokens": 12 }
            }
        });

        let usage = observe_non_stream_json_value(&body).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 103);
        assert_eq!(usage.output_tokens, 48);
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 12);
        assert!(usage.thinking_tokens <= usage.output_tokens);
    }
    #[test]
    fn service_tier_falls_back_to_response_envelope() {
        let value = serde_json::json!({
            "service_tier": "priority",
            "usage": {"input_tokens": 3, "output_tokens": 5}
        });
        let usage = observe_non_stream_json_value(&value).usage;
        assert_eq!(usage.service_tier.as_deref(), Some("priority"));

        let nested = serde_json::json!({
            "response": {
                "service_tier": "scale",
                "usage": {"input_tokens": 7, "output_tokens": 11}
            }
        });
        let usage = observe_non_stream_json_value(&nested).usage;
        assert_eq!(usage.service_tier.as_deref(), Some("scale"));
    }

    #[test]
    fn gm_use_01_generate_content_usage_metadata_maps_usage() {
        let body = serde_json::json!({
            "usageMetadata": {
                "promptTokenCount": 120,
                "candidatesTokenCount": 48,
                "cachedContentTokenCount": 17,
                "thoughtsTokenCount": 12,
                "toolUsePromptTokenCount": 9
            }
        });

        let usage = observe_non_stream_json_value(&body).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 103);
        assert_eq!(usage.output_tokens, 60);
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 12);
        assert_eq!(usage.tool_use_prompt_tokens, Some(9));
    }

    #[test]
    fn gm_use_02_code_assist_response_envelope_usage_metadata_maps_usage() {
        let body = serde_json::json!({
            "response": {
                "candidates": [{
                    "content": {
                        "parts": [{"text": "hello"}]
                    }
                }],
                "usageMetadata": {
                    "promptTokenCount": 120,
                    "candidatesTokenCount": 48,
                    "cachedContentTokenCount": 17,
                    "thoughtsTokenCount": 12,
                    "toolUsePromptTokenCount": 9
                }
            }
        });

        let usage = observe_non_stream_json_value(&body).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 103);
        assert_eq!(usage.output_tokens, 60);
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 12);
        assert_eq!(usage.tool_use_prompt_tokens, Some(9));
    }
    #[test]
    fn gm_use_05_tool_use_prompt_tokens_non_stream_presence_states() {
        for (tool_use_prompt_tokens, expected) in
            [(Some(37), Some(37)), (None, None), (Some(0), Some(0))]
        {
            let usage_metadata = match tool_use_prompt_tokens {
                Some(value) => serde_json::json!({
                    "promptTokenCount": 120,
                    "candidatesTokenCount": 48,
                    "toolUsePromptTokenCount": value
                }),
                None => serde_json::json!({
                    "promptTokenCount": 120,
                    "candidatesTokenCount": 48
                }),
            };
            let usage = observe_non_stream_json_value(&serde_json::json!({
                "usageMetadata": usage_metadata
            }))
            .usage;
            assert_eq!(usage.tool_use_prompt_tokens, expected);
            assert_eq!(usage.input_tokens, 120);
            assert_eq!(usage.output_tokens, 48);
        }

        let null = observe_non_stream_json_value(&serde_json::json!({
            "usageMetadata": {
                "promptTokenCount": 120,
                "candidatesTokenCount": 48,
                "toolUsePromptTokenCount": null
            }
        }))
        .usage;
        assert_eq!(null.tool_use_prompt_tokens, None);
    }

    #[test]
    fn gm_sse_06_tool_use_prompt_tokens_replace_and_retain_cumulative_snapshots() {
        let mut usage = UsageCounts::default();
        for (payload, expected) in [
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":48,"toolUsePromptTokenCount":37}}"#,
                Some(37),
            ),
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":49}}"#,
                Some(37),
            ),
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":50,"toolUsePromptTokenCount":null}}"#,
                Some(37),
            ),
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":51,"toolUsePromptTokenCount":0}}"#,
                Some(0),
            ),
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":52,"toolUsePromptTokenCount":11}}"#,
                Some(11),
            ),
        ] {
            let frame = format!("data: {payload}\n\n");
            accumulate_raw_sse_usage(frame.as_bytes(), &mut usage);
            assert_eq!(usage.tool_use_prompt_tokens, expected, "{payload}");
        }
    }

    #[test]
    fn gm_use_03_raw_usage_metadata_oracle_includes_thoughts() {
        let body = br#"{"usageMetadata":{"promptTokenCount":20384,"candidatesTokenCount":387,"thoughtsTokenCount":1046,"totalTokenCount":21817}}"#;

        let usage = observe_non_stream_json_body(body).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 20_384);
        assert_eq!(usage.output_tokens, 1_433);
        assert_eq!(usage.thinking_tokens, 1_046);
        assert_eq!(usage.cache_read_input_tokens, 0);
        assert_eq!(usage.estimated_thinking_progress, 0);
    }

    #[test]
    fn gm_use_04_output_sum_saturates_without_losing_components() {
        let body = br#"{"usageMetadata":{"candidatesTokenCount":18446744073709551610,"thoughtsTokenCount":12}}"#;

        let usage = observe_non_stream_json_body(body).usage;

        assert!(usage.present);
        assert_eq!(usage.output_tokens, u64::MAX);
        assert_eq!(usage.thinking_tokens, 12);
        assert_eq!(usage.input_tokens, 0);

        let mut streamed = UsageCounts::default();
        accumulate_raw_sse_usage(
            b"data: {\"usageMetadata\":{\"candidatesTokenCount\":18446744073709551610,\"thoughtsTokenCount\":12}}\n\n",
            &mut streamed,
        );
        assert_eq!(streamed.output_tokens, u64::MAX);
        assert_eq!(streamed.thinking_tokens, 12);

        accumulate_raw_sse_usage(
            b"data: {\"usageMetadata\":{\"thoughtsTokenCount\":0}}\n\n",
            &mut streamed,
        );
        assert_eq!(streamed.output_tokens, u64::MAX - 5);
        assert_eq!(streamed.thinking_tokens, 0);
    }

    #[test]
    fn gm_use_05_generate_content_array_usage_metadata_maps_last_element() {
        let body = serde_json::json!([
            {
                "candidates": [{
                    "content": {"parts": [{"text": "partial"}]}
                }]
            },
            {
                "candidates": [{
                    "content": {"parts": [{"text": "complete"}]}
                }],
                "usageMetadata": {
                    "promptTokenCount": 110,
                    "candidatesTokenCount": 4,
                    "cachedContentTokenCount": 17,
                    "thoughtsTokenCount": 12
                }
            }
        ]);

        let usage =
            observe_non_stream_json_value_for_shape(&body, UsageShape::GeminiGenerateContent).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 93);
        assert_eq!(usage.gross_input_tokens, Some(110));
        assert_eq!(usage.output_tokens, 16);
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 12);
    }

    #[test]
    fn gm_use_06_code_assist_array_response_usage_metadata_maps_usage() {
        let body = serde_json::json!([
            {
                "response": {
                    "candidates": [{
                        "content": {"parts": [{"text": "partial"}]}
                    }]
                }
            },
            {
                "response": {
                    "candidates": [{
                        "content": {"parts": [{"text": "complete"}]}
                    }],
                    "usageMetadata": {
                        "promptTokenCount": 110,
                        "candidatesTokenCount": 4,
                        "cachedContentTokenCount": 17,
                        "thoughtsTokenCount": 12
                    }
                }
            }
        ]);

        let usage =
            observe_non_stream_json_value_for_shape(&body, UsageShape::GeminiGenerateContent).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 93);
        assert_eq!(usage.gross_input_tokens, Some(110));
        assert_eq!(usage.output_tokens, 16);
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 12);
    }

    #[test]
    fn gm_use_07_array_usage_merges_cumulative_snapshots_without_summing() {
        let body = serde_json::json!([
            {
                "usageMetadata": {
                    "promptTokenCount": 120,
                    "cachedContentTokenCount": 17,
                    "candidatesTokenCount": 10,
                    "thoughtsTokenCount": 2
                }
            },
            {
                "usageMetadata": {
                    "promptTokenCount": 120,
                    "candidatesTokenCount": 15
                }
            },
            {
                "usageMetadata": {
                    "thoughtsTokenCount": 4
                }
            }
        ]);

        let usage =
            observe_non_stream_json_value_for_shape(&body, UsageShape::GeminiGenerateContent).usage;

        assert!(usage.present);
        assert_eq!(usage.input_tokens, 103);
        assert_eq!(usage.gross_input_tokens, Some(120));
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 4);
        assert_eq!(usage.output_tokens, 19);
    }

    #[test]
    fn gm_use_08_empty_and_non_object_arrays_have_no_usage() {
        for body in [
            serde_json::json!([]),
            serde_json::json!([null]),
            serde_json::json!([{"candidates": []}]),
        ] {
            assert!(
                !observe_non_stream_json_value_for_shape(&body, UsageShape::GeminiGenerateContent,)
                    .usage
                    .present,
                "array without usage metadata must remain absent: {body}"
            );
        }

        let non_gemini = serde_json::json!([
            {"usage": {"input_tokens": 9, "output_tokens": 4}}
        ]);
        assert!(
            !observe_non_stream_json_value_for_shape(
                &non_gemini,
                UsageShape::OpenaiChatCompletions,
            )
            .usage
            .present,
            "top-level arrays must not change non-Gemini usage shapes"
        );
    }

    #[test]
    fn oa_err_01_openai_error_type_precedes_code() {
        let body =
            br#"{"error":{"type":"invalid_request_error","code":"invalid_prompt","message":"bad request"}}"#;

        let error = observe_http_error_body(body).expect("upstream error extracted");

        assert_eq!(
            error.into_parts_with(None),
            (
                Some("invalid_request_error".to_owned()),
                Some("bad request".to_owned())
            )
        );
    }

    #[test]
    fn oa_err_02_native_failed_response_maps_error_type_and_message() {
        let body = serde_json::json!({
            "id": "resp_failed",
            "object": "response",
            "status": "failed",
            "error": {
                "code": "server_error",
                "message": "the model failed before completion"
            }
        });

        let error = observe_non_stream_json_value(&body)
            .canonical_error
            .expect("native failed response error extracted");

        assert_eq!(
            error.into_parts_with(None),
            (
                Some("server_error".to_owned()),
                Some("the model failed before completion".to_owned())
            )
        );
    }

    #[test]
    fn gm_err_01_numeric_code_uses_status_and_keeps_message() {
        let body =
            br#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exceeded"}}"#;

        let error = observe_http_error_body(body).expect("upstream error extracted");

        assert_eq!(
            error.into_parts_with(None),
            (
                Some("RESOURCE_EXHAUSTED".to_owned()),
                Some("quota exceeded".to_owned())
            )
        );
    }

    #[test]
    fn gm_sse_01_stream_generate_content_reports_terminal_usage() {
        let first =
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n\n";
        let terminal = b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":120,\"candidatesTokenCount\":48,\"cachedContentTokenCount\":17,\"thoughtsTokenCount\":12,\"toolUsePromptTokenCount\":9}}\n\n";
        let mut usage = UsageCounts::default();

        let first_update = accumulate_raw_sse_usage(first, &mut usage);

        assert!(!first_update.message_stop);
        assert!(!usage.present);
        assert_eq!(usage, UsageCounts::default());

        let terminal_update = accumulate_raw_sse_usage(terminal, &mut usage);

        assert!(!terminal_update.message_stop);
        assert!(usage.present);
        assert_eq!(usage.input_tokens, 103);
        assert_eq!(usage.output_tokens, 60);
        assert_eq!(usage.cache_read_input_tokens, 17);
        assert_eq!(usage.thinking_tokens, 12);
        assert_eq!(usage.tool_use_prompt_tokens, Some(9));
        assert!(usage.thinking_tokens < usage.output_tokens);
    }

    #[test]
    fn gm_sse_02_error_only_chunk_observes_status_and_message() {
        let raw =
            b"event: error\ndata: {\"error\":{\"status\":\"RESOURCE_EXHAUSTED\",\"message\":\"quota exceeded\"}}\n\n";

        let error = detect_raw_mid_stream_error(raw).expect("Gemini SSE error detected");

        assert_eq!(error.error_type.as_deref(), Some("RESOURCE_EXHAUSTED"));
        assert_eq!(error.error_message.as_deref(), Some("quota exceeded"));
    }

    #[test]
    fn gm_sse_03_cumulative_snapshots_replace_instead_of_accumulating() {
        let mut usage = UsageCounts::default();
        let snapshots = [
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"cachedContentTokenCount":17,"candidatesTokenCount":5,"thoughtsTokenCount":7}}"#,
                12,
                7,
            ),
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"cachedContentTokenCount":17,"candidatesTokenCount":5,"thoughtsTokenCount":7}}"#,
                12,
                7,
            ),
            (
                r#"{"response":{"usageMetadata":{"promptTokenCount":120,"cachedContentTokenCount":17,"candidatesTokenCount":9,"thoughtsTokenCount":11}}}"#,
                20,
                11,
            ),
            (
                r#"{"usageMetadata":{"promptTokenCount":120,"cachedContentTokenCount":17,"candidatesTokenCount":3,"thoughtsTokenCount":4}}"#,
                7,
                4,
            ),
        ];

        for (payload, expected_output, expected_thinking) in snapshots {
            let frame = format!("data: {payload}\n\n");
            let update = accumulate_raw_sse_usage(frame.as_bytes(), &mut usage);

            assert!(usage.present);
            assert!(!update.message_start_usage);
            assert!(!update.message_stop);
            assert_eq!(usage.input_tokens, 103);
            assert_eq!(usage.cache_read_input_tokens, 17);
            assert_eq!(usage.output_tokens, expected_output, "{payload}");
            assert_eq!(usage.thinking_tokens, expected_thinking, "{payload}");
        }
    }

    #[test]
    fn gm_sse_04_partial_snapshots_preserve_missing_and_null_components() {
        let mut usage = UsageCounts::default();
        let snapshots = [
            (
                r#"{"usageMetadata":{"thoughtsTokenCount":1046}}"#,
                1_046,
                1_046,
            ),
            (
                r#"{"usageMetadata":{"candidatesTokenCount":null,"thoughtsTokenCount":null}}"#,
                1_046,
                1_046,
            ),
            (
                r#"{"usageMetadata":{"candidatesTokenCount":387}}"#,
                1_433,
                1_046,
            ),
            (r#"{"usageMetadata":{"thoughtsTokenCount":500}}"#, 887, 500),
            (r#"{"usageMetadata":{"candidatesTokenCount":0}}"#, 500, 500),
            (r#"{"usageMetadata":{"thoughtsTokenCount":0}}"#, 0, 0),
            (
                r#"{"usageMetadata":{"candidatesTokenCount":10,"thoughtsTokenCount":12}}"#,
                22,
                12,
            ),
            (r#"{"usageMetadata":{"promptTokenCount":50}}"#, 22, 12),
            (
                r#"{"usageMetadata":{"candidatesTokenCount":null,"thoughtsTokenCount":null}}"#,
                22,
                12,
            ),
            (r#"{"usageMetadata":{"candidatesTokenCount":5}}"#, 17, 12),
            (
                r#"{"usageMetadata":{"candidatesTokenCount":3,"thoughtsTokenCount":null}}"#,
                15,
                12,
            ),
            (r#"{"usageMetadata":{"thoughtsTokenCount":0}}"#, 3, 0),
        ];

        for (payload, expected_output, expected_thinking) in snapshots {
            let frame = format!("data: {payload}\n\n");
            accumulate_raw_sse_usage(frame.as_bytes(), &mut usage);

            assert!(usage.present);
            assert_eq!(usage.output_tokens, expected_output, "{payload}");
            assert_eq!(usage.thinking_tokens, expected_thinking, "{payload}");
        }

        let mut next_turn = UsageCounts::default();
        accumulate_raw_sse_usage(
            b"data: {\"usageMetadata\":{\"candidatesTokenCount\":3}}\n\n",
            &mut next_turn,
        );
        assert_eq!(next_turn.output_tokens, 3);
        assert_eq!(next_turn.thinking_tokens, 0);
    }

    #[test]
    fn gm_sse_05_other_provider_thinking_remains_an_output_subset() {
        for payload in [
            r#"{"type":"response.completed","response":{"usage":{"input_tokens":120,"output_tokens":48,"output_tokens_details":{"reasoning_tokens":12}}}}"#,
            r#"{"type":"message_delta","usage":{"input_tokens":120,"output_tokens":48,"output_tokens_details":{"thinking_tokens":12}}}"#,
        ] {
            let mut usage = UsageCounts::default();
            let frame = format!("data: {payload}\n\n");

            accumulate_raw_sse_usage(frame.as_bytes(), &mut usage);

            assert!(usage.present);
            assert_eq!(usage.input_tokens, 120);
            assert_eq!(usage.output_tokens, 48, "{payload}");
            assert_eq!(usage.thinking_tokens, 12, "{payload}");
            assert_eq!(usage.estimated_thinking_progress, 0);
        }
    }

    #[test]
    fn partial_cumulative_snapshots_retain_cached_subtraction() {
        let mut gemini = UsageCounts::default();
        accumulate_raw_sse_usage(
            b"data: {\"usageMetadata\":{\"promptTokenCount\":120,\"cachedContentTokenCount\":17}}\n\n",
            &mut gemini,
        );
        assert_eq!(gemini.input_tokens, 103);
        assert_eq!(gemini.cache_read_input_tokens, 17);

        accumulate_raw_sse_usage(
            b"data: {\"usageMetadata\":{\"promptTokenCount\":120,\"candidatesTokenCount\":10}}\n\n",
            &mut gemini,
        );
        assert_eq!(gemini.input_tokens, 103);
        assert_eq!(gemini.cache_read_input_tokens, 17);
        assert_eq!(gemini.output_tokens, 10);

        let mut openai = UsageCounts::default();
        accumulate_raw_sse_usage(
            b"data: {\"type\":\"response.in_progress\",\"response\":{\"usage\":{\"input_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":17}}}}\n\n",
            &mut openai,
        );
        assert_eq!(openai.input_tokens, 103);
        assert_eq!(openai.cache_read_input_tokens, 17);

        accumulate_raw_sse_usage(
            b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":120,\"output_tokens\":15}}}\n\n",
            &mut openai,
        );
        assert_eq!(openai.input_tokens, 103);
        assert_eq!(openai.cache_read_input_tokens, 17);
        assert_eq!(openai.output_tokens, 15);
    }

    #[test]
    fn extracts_bounded_gemini_retry_info_delay() {
        let body = br#"{"error":{"code":429,"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"1.5s"}]}}"#;
        let error = observe_http_error_body(body).expect("retry info extracted");
        assert_eq!(error.retry_after(), Some("1.5s"));
    }

    #[test]
    fn usage_shapes_decode_openai_chat_and_completion_payloads() {
        for shape in [
            UsageShape::OpenaiChatCompletions,
            UsageShape::OpenaiCompletions,
        ] {
            let value = serde_json::json!({
                "usage": {
                    "prompt_tokens": 120,
                    "completion_tokens": 48,
                    "prompt_tokens_details": {"cached_tokens": 17},
                    "completion_tokens_details": {"reasoning_tokens": 12}
                }
            });
            let usage = observe_non_stream_json_value_for_shape(&value, shape).usage;
            assert!(usage.present);
            assert_eq!(usage.input_tokens, 103);
            assert_eq!(usage.gross_input_tokens, Some(120));
            assert_eq!(usage.output_tokens, 48);
            assert_eq!(usage.cache_read_input_tokens, 17);
            assert_eq!(usage.thinking_tokens, 12);

            let mut streamed = UsageCounts::default();
            let event = parse_sse_event(
                br#"data: {"choices":[],"usage":{"prompt_tokens":120,"completion_tokens":48,"prompt_tokens_details":{"cached_tokens":17},"completion_tokens_details":{"reasoning_tokens":12}}}

"#,
            );
            accumulate_sse_usage_for_shape(&event, &mut streamed, shape);
            assert_eq!(streamed, usage);
            assert!(
                !observe_non_stream_json_value_for_shape(
                    &serde_json::json!({"choices":[{"message":{"content":"ok"}}]}),
                    shape,
                )
                .usage
                .present
            );
        }
    }

    #[test]
    fn usage_shapes_decode_openai_embeddings_and_gemini_embed() {
        let embeddings = serde_json::json!({
            "object": "list",
            "usage": {"prompt_tokens": 23, "total_tokens": 23}
        });
        let usage =
            observe_non_stream_json_value_for_shape(&embeddings, UsageShape::OpenaiEmbeddings)
                .usage;
        assert!(usage.present);
        assert_eq!(usage.input_tokens, 23);
        assert_eq!(usage.output_tokens, 0);

        let gemini = serde_json::json!({
            "embeddings": [{"values": [0.1, 0.2]}],
            "usageMetadata": {"promptTokenCount": 9, "totalTokenCount": 9}
        });
        let usage = observe_non_stream_json_value_for_shape(&gemini, UsageShape::GeminiEmbed).usage;
        assert!(usage.present);
        assert_eq!(usage.input_tokens, 9);
        assert_eq!(usage.output_tokens, 0);

        let embeddings_event = parse_sse_event(
            br#"data: {"usage":{"prompt_tokens":23,"total_tokens":23}}

"#,
        );
        let mut streamed = UsageCounts::default();
        accumulate_sse_usage_for_shape(
            &embeddings_event,
            &mut streamed,
            UsageShape::OpenaiEmbeddings,
        );
        assert_eq!(streamed.input_tokens, 23);

        let gemini_event = parse_sse_event(
            br#"data: {"usageMetadata":{"promptTokenCount":9,"totalTokenCount":9}}

"#,
        );
        let mut streamed = UsageCounts::default();
        accumulate_sse_usage_for_shape(&gemini_event, &mut streamed, UsageShape::GeminiEmbed);
        assert_eq!(streamed.input_tokens, 9);

        let missing = serde_json::json!({"embeddings": [{"values": [0.1]}]});
        assert!(
            !observe_non_stream_json_value_for_shape(&missing, UsageShape::GeminiEmbed)
                .usage
                .present
        );
    }

    #[test]
    fn usage_shapes_decode_openai_image_and_token_audio_responses() {
        let image = serde_json::json!({
            "created": 1,
            "data": [{"b64_json": "encoded"}],
            "usage": {
                "input_tokens": 11,
                "output_tokens": 3,
                "total_tokens": 14,
                "input_tokens_details": {"image_tokens": 7},
                "output_tokens_details": {"image_tokens": 2}
            }
        });
        let image_usage =
            observe_non_stream_json_value_for_shape(&image, UsageShape::OpenaiImages).usage;
        assert!(image_usage.present);
        assert_eq!(image_usage.input_tokens, 11);
        assert_eq!(image_usage.output_tokens, 3);
        assert_eq!(image_usage.input_image_tokens, Some(7));
        assert_eq!(image_usage.output_image_tokens, Some(2));

        let audio = serde_json::json!({
            "text": "transcribed",
            "usage": {
                "type": "tokens",
                "input_tokens": 11,
                "output_tokens": 3,
                "total_tokens": 14
            }
        });
        let audio_usage =
            observe_non_stream_json_value_for_shape(&audio, UsageShape::OpenaiAudio).usage;
        assert!(audio_usage.present);
        assert_eq!(audio_usage.input_tokens, 11);
        assert_eq!(audio_usage.output_tokens, 3);

        let duration_audio = serde_json::json!({
            "text": "transcribed",
            "usage": {"type": "duration", "seconds": 12.5}
        });
        assert!(
            !observe_non_stream_json_value_for_shape(&duration_audio, UsageShape::OpenaiAudio)
                .usage
                .present
        );
        let non_image = serde_json::json!({
            "usage": {
                "input_tokens": 11,
                "output_tokens": 3,
                "input_tokens_details": {"image_tokens": 7},
                "output_tokens_details": {"image_tokens": 2}
            }
        });
        let non_image_usage =
            observe_non_stream_json_value_for_shape(&non_image, UsageShape::OpenaiEmbeddings).usage;
        assert_eq!(non_image_usage.input_image_tokens, None);
        assert_eq!(non_image_usage.output_image_tokens, None);
    }
    #[test]
    fn openai_image_details_retain_only_numeric_cumulative_values() {
        let first = serde_json::json!({
            "usage": {
                "input_tokens": 20,
                "output_tokens": 8,
                "input_tokens_details": {"image_tokens": 6},
                "output_tokens_details": {"image_tokens": 3}
            }
        });
        let mut usage =
            observe_non_stream_json_value_for_shape(&first, UsageShape::OpenaiImages).usage;
        assert_eq!(usage.input_image_tokens, Some(6));
        assert_eq!(usage.output_image_tokens, Some(3));

        let second = serde_json::json!({
            "usage": {
                "input_tokens": 20,
                "output_tokens": 8,
                "input_tokens_details": null,
                "output_tokens_details": {"image_tokens": "3"}
            }
        });
        let second_value = second.get("usage").expect("usage object");
        merge_usage_value_for_shape(&mut usage, second_value, Some(UsageShape::OpenaiImages));
        assert_eq!(usage.input_image_tokens, Some(6));
        assert_eq!(usage.output_image_tokens, Some(3));

        let omitted = serde_json::json!({
            "usage": {"input_tokens": 20, "output_tokens": 8}
        });
        let omitted_value = omitted.get("usage").expect("usage object");
        merge_usage_value_for_shape(&mut usage, omitted_value, Some(UsageShape::OpenaiImages));
        assert_eq!(usage.input_image_tokens, Some(6));
        assert_eq!(usage.output_image_tokens, Some(3));
    }
    #[test]
    fn streamed_openai_image_details_retain_when_cumulative_frame_omits_them() {
        let first = parse_sse_event(
            br#"data: {"usage":{"input_tokens":20,"output_tokens":8,"input_tokens_details":{"image_tokens":6},"output_tokens_details":{"image_tokens":3}}}

"#,
        );
        let second = parse_sse_event(
            br#"data: {"usage":{"input_tokens":20,"output_tokens":8}}

"#,
        );
        let mut usage = UsageCounts::default();
        accumulate_sse_usage_for_shape(&first, &mut usage, UsageShape::OpenaiImages);
        accumulate_sse_usage_for_shape(&second, &mut usage, UsageShape::OpenaiImages);
        assert_eq!(usage.input_image_tokens, Some(6));
        assert_eq!(usage.output_image_tokens, Some(3));
    }

    #[test]
    fn image_and_audio_usage_shapes_decode_streamed_usage() {
        for (shape, usage_json) in [
            (
                UsageShape::OpenaiImages,
                r#"{"input_tokens":11,"output_tokens":3,"total_tokens":14}"#,
            ),
            (
                UsageShape::OpenaiAudio,
                r#"{"type":"tokens","input_tokens":11,"output_tokens":3,"total_tokens":14}"#,
            ),
        ] {
            let raw = format!("data: {{\"usage\":{usage_json}}}\n\n");
            let event = parse_sse_event(raw.as_bytes());
            let mut usage = UsageCounts::default();
            accumulate_sse_usage_for_shape(&event, &mut usage, shape);
            assert!(usage.present);
            assert_eq!(usage.input_tokens, 11);
            assert_eq!(usage.output_tokens, 3);
        }
    }

    #[test]
    fn openai_chat_sse_done_and_finished_choice_are_detectable() {
        let done = parse_sse_event(b"data: [DONE]\n\n");
        assert!(done.is_done());
        assert!(!done.has_finished_choices());

        let finished = parse_sse_event(
            br#"data: {"choices":[{"index":0,"finish_reason":"stop","delta":{}}]}

"#,
        );
        assert!(!finished.is_done());
        assert!(finished.has_finished_choices());
    }
}
