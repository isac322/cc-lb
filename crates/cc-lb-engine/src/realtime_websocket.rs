//! Connection-pinned OpenAI Realtime and Gemini Live admission/accounting.
use crate::completion_observer::{CompletionObserver, StreamCompletionLog, StreamLatency};
use crate::downstream_stream_drop_guard::record_stream_termination_metric;
use crate::responses_websocket::WebSocketCandidate;
use crate::terminal_observer::{
    InternalFailure, LifecycleContext, StreamErrorClassification, StreamTerminationCause,
    StreamTerminationOutcome, UpstreamErrorCode, classify_stream_error,
    record_websocket_terminal_span,
};
use crate::usage_parser::{UsageCounts, merge_usage_value, merge_usage_value_for_shape};
use bytes::Bytes;
use cc_lb_control::api_keys::limit_engine::{LimitEngine, Reservation};
use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage, Provider};
use cc_lb_lifecycle::{LifecycleEvent, StreamError, StreamSuccess, UsageSnapshot, UsageSource};
use cc_lb_observability::{RedactionPolicy, truncate_reason};
use cc_lb_upstream::{
    RealtimeFrameDirection, RealtimeFrameKind, TransformRealtimeFrameRequest,
    TransformRealtimeFrameResult,
};
use http::{HeaderMap, StatusCode};
use serde_json::Value;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Instant;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeWebSocketKind {
    Openai,
    Gemini,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeWebSocketTermination {
    Completed,
    ProviderClosed,
    ProxyRelayError,
    DownstreamCancelled,
}

/// Bounded transport diagnostics retained independently of provider frame bodies.
#[derive(Default)]
pub struct RealtimeWebSocketDiagnostics {
    close: Option<(u16, String)>,
    relay_error: Option<StreamErrorClassification>,
}

impl RealtimeWebSocketDiagnostics {
    pub fn with_close(mut self, code: u16, reason: &str) -> Self {
        self.close = Some((
            code,
            truncate_reason(&RedactionPolicy::default().redact_text(reason)),
        ));
        self
    }

    /// Classify the real relay error chain without inventing HTTP/2 attribution.
    pub fn with_relay_error(mut self, error: &(dyn StdError + 'static)) -> Self {
        let mut classified = classify_stream_error(error);
        let mut source = Some(error);
        let mut io_cause = None;
        for _ in 0..8 {
            let Some(error) = source else {
                break;
            };
            if let Some(error) = error.downcast_ref::<std::io::Error>() {
                io_cause = Some(classify_stream_error(error).cause);
                break;
            }
            source = error.source();
        }
        classified.cause = io_cause.unwrap_or(StreamTerminationCause::TransportError);
        classified.h2_reason = None;
        self.relay_error = Some(classified);
        self
    }

    fn message(&self) -> Option<String> {
        match (&self.close, &self.relay_error) {
            (Some((code, reason)), Some(error)) => Some(truncate_reason(&format!(
                "websocket close code={code} reason={reason}; {}",
                error.redacted_chain,
            ))),
            (Some((code, reason)), None) => Some(truncate_reason(&format!(
                "websocket close code={code} reason={reason}",
            ))),
            (None, Some(error)) => Some(error.redacted_chain.clone()),
            (None, None) => None,
        }
    }
}

/// Cheap cloneable handle passed to the usage worker, never the relay parser.
#[derive(Clone)]
pub struct RealtimeUsageObserver {
    observer: Option<LifecycleContext>,
}

fn realtime_stream_complete(
    termination: RealtimeWebSocketTermination,
    usage_complete: bool,
) -> bool {
    match termination {
        RealtimeWebSocketTermination::Completed => true,
        RealtimeWebSocketTermination::ProviderClosed => usage_complete,
        RealtimeWebSocketTermination::ProxyRelayError
        | RealtimeWebSocketTermination::DownstreamCancelled => false,
    }
}

fn realtime_terminal_outcome(
    termination: RealtimeWebSocketTermination,
    complete: bool,
) -> (StreamTerminationOutcome, StreamTerminationCause) {
    match termination {
        RealtimeWebSocketTermination::Completed => (
            StreamTerminationOutcome::Completed,
            StreamTerminationCause::None,
        ),
        RealtimeWebSocketTermination::ProviderClosed if complete => (
            StreamTerminationOutcome::Completed,
            StreamTerminationCause::None,
        ),
        RealtimeWebSocketTermination::ProviderClosed => (
            StreamTerminationOutcome::UpstreamError,
            StreamTerminationCause::ProviderError,
        ),
        RealtimeWebSocketTermination::ProxyRelayError => (
            StreamTerminationOutcome::ProxyError,
            StreamTerminationCause::TransportError,
        ),
        RealtimeWebSocketTermination::DownstreamCancelled => (
            StreamTerminationOutcome::ClientCancelled,
            StreamTerminationCause::None,
        ),
    }
}

#[derive(Debug)]
pub struct RealtimeStreamMetrics {
    started: Instant,
    first_frame_ms: Option<u64>,
    last_frame_ms: Option<u64>,
    event_count: u64,
    ping_count: u64,
    total_bytes: u64,
    provider_session_id: Option<String>,
    provider_connection_id: Option<String>,
    /// Completion state for the current provider turn. `None` means that no
    /// turn boundary has been observed on the relay path yet.
    turn_complete: Option<bool>,
}

impl RealtimeStreamMetrics {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            first_frame_ms: None,
            last_frame_ms: None,
            event_count: 0,
            ping_count: 0,
            total_bytes: 0,
            provider_session_id: None,
            provider_connection_id: None,
            turn_complete: None,
        }
    }

    pub fn observe_frame(&mut self, bytes: &[u8]) {
        let elapsed_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.first_frame_ms.get_or_insert(elapsed_ms);
        self.last_frame_ms = Some(elapsed_ms);
        self.event_count = self.event_count.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes.len() as u64);
    }

    /// Observe provider turn boundaries on the relay path, which sees every
    /// frame. Usage accounting is intentionally marker-filtered and therefore
    /// cannot observe OpenAI `response.created` frames reliably.
    pub fn observe_turn_boundary(&mut self, kind: RealtimeWebSocketKind, frame: &[u8]) {
        let has_boundary_marker = match kind {
            RealtimeWebSocketKind::Openai => {
                frame
                    .windows(b"response.created".len())
                    .any(|window| window == b"response.created")
                    || frame
                        .windows(b"response.done".len())
                        .any(|window| window == b"response.done")
            }
            RealtimeWebSocketKind::Gemini => {
                frame
                    .windows(b"modelTurn".len())
                    .any(|window| window == b"modelTurn")
                    || frame
                        .windows(b"turnComplete".len())
                        .any(|window| window == b"turnComplete")
            }
        };
        if !has_boundary_marker {
            return;
        }
        let Ok(value) = sonic_rs::from_slice::<Value>(frame) else {
            return;
        };
        let complete = match kind {
            RealtimeWebSocketKind::Openai => match value.get("type").and_then(Value::as_str) {
                Some("response.created") => Some(false),
                Some("response.done") => Some(true),
                _ => None,
            },
            RealtimeWebSocketKind::Gemini => {
                if value.pointer("/serverContent/modelTurn").is_some() {
                    Some(false)
                } else if value
                    .pointer("/serverContent/turnComplete")
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    Some(true)
                } else {
                    None
                }
            }
        };
        if let Some(complete) = complete {
            self.turn_complete = Some(complete);
        }
    }

    pub fn turn_complete(&self) -> Option<bool> {
        self.turn_complete
    }

    /// Capture bounded provider-native setup/session identifiers without
    /// retaining arbitrary frame payloads.
    pub fn observe_provider_metadata(&mut self, kind: RealtimeWebSocketKind, frame: &[u8]) {
        let Ok(value) = sonic_rs::from_slice::<Value>(frame) else {
            return;
        };
        let bounded = |value: Option<&Value>| {
            let value = value.and_then(Value::as_str)?;
            (!value.is_empty() && value.len() <= 512).then(|| value.to_owned())
        };
        match kind {
            RealtimeWebSocketKind::Openai
                if value.get("type").and_then(Value::as_str) == Some("session.created") =>
            {
                self.provider_session_id =
                    bounded(value.pointer("/session/id")).or(self.provider_session_id.take());
                self.provider_connection_id = bounded(value.pointer("/session/connection_id"))
                    .or(self.provider_connection_id.take());
            }
            RealtimeWebSocketKind::Gemini => {
                self.provider_session_id = bounded(value.pointer("/setupComplete/sessionId"))
                    .or_else(|| bounded(value.pointer("/setupComplete/session_id")))
                    .or_else(|| bounded(value.pointer("/sessionResumptionUpdate/newSessionId")))
                    .or(self.provider_session_id.take());
                self.provider_connection_id = bounded(value.pointer("/setupComplete/connectionId"))
                    .or_else(|| bounded(value.pointer("/setupComplete/connection_id")))
                    .or(self.provider_connection_id.take());
            }
            _ => {}
        }
    }

    pub fn observe_ping(&mut self) {
        self.ping_count = self.ping_count.saturating_add(1);
    }
    fn latency(&self, status: u16) -> StreamLatency {
        let stream_total_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let inter_output_avg_ms = match (self.first_frame_ms, self.last_frame_ms) {
            (Some(first), Some(last)) if self.event_count > 1 => {
                Some(last.saturating_sub(first) / (self.event_count - 1))
            }
            _ => None,
        };
        StreamLatency {
            status,
            stream_first_chunk_ms: self.first_frame_ms,
            stream_message_start_ms: None,
            stream_content_block_start_ms: None,
            stream_first_content_delta_ms: None,
            stream_last_content_delta_ms: None,
            stream_message_stop_ms: None,
            stream_last_chunk_ms: self.last_frame_ms,
            stream_first_output_ms: self.first_frame_ms,
            stream_last_output_ms: self.last_frame_ms,
            stream_event_count: self.event_count,
            stream_inter_output_avg_ms: inter_output_avg_ms,
            provider_session_id: self.provider_session_id.clone(),
            provider_connection_id: self.provider_connection_id.clone(),
            stream_total_ms,
            // Realtime and Gemini Live frames are not SSE events or
            // Anthropic content deltas. Keep those legacy counters at zero.
            sse_event_count: 0,
            content_delta_count: 0,
            ping_count: self.ping_count,
            inter_token_avg_ms: None,
            total_bytes: self.total_bytes,
        }
    }
}

impl Default for RealtimeStreamMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Provider endpoint details used when preparing a pinned WebSocket connection.
pub struct RealtimeWebSocketTarget<'a> {
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub model: Option<&'a str>,
}

impl RealtimeWebSocketKind {
    pub const fn provider(self) -> Provider {
        match self {
            Self::Openai => Provider::Openai,
            Self::Gemini => Provider::Gemini,
        }
    }

    /// A cheap marker check avoids parsing audio and other ordinary relay frames.
    pub fn may_report_usage(self, frame: &[u8]) -> bool {
        match self {
            Self::Openai => frame
                .windows(b"response.done".len())
                .any(|window| window == b"response.done"),
            Self::Gemini => ["usageMetadata", "turnComplete"].iter().any(|marker| {
                frame
                    .windows(marker.len())
                    .any(|window| window == marker.as_bytes())
            }),
        }
    }
}

pub struct RealtimeWebSocketObservation {
    pub(crate) observer: Option<LifecycleContext>,
    pub(crate) completion_observer: Arc<CompletionObserver>,
    pub(crate) request_id: String,
}

impl RealtimeWebSocketObservation {
    pub fn finish(self, status: StatusCode) {
        if let Some(observer) = &self.observer {
            if status.is_server_error() {
                observer.set_upstream_error(status, UpstreamErrorCode::Upstream5xx);
            } else {
                observer.set_success_status(status);
            }
            observer.finish();
        }
    }

    /// Finish an authenticated handshake whose client disconnected before
    /// sending the provider setup frame. This is a cancellation, not a
    /// successful WebSocket upgrade.
    pub fn finish_client_closed(self) {
        if let Some(observer) = &self.observer {
            observer.set_client_closed(StatusCode::SWITCHING_PROTOCOLS);
            observer.finish();
        }
    }
}

pub struct RealtimeWebSocketSession {
    pub candidate: WebSocketCandidate,
    pub url: Url,
    pub headers: HeaderMap,
    pub(crate) observer: Option<LifecycleContext>,
    pub(crate) completion_observer: Arc<CompletionObserver>,
    pub(crate) stream_span: tracing::Span,
    pub(crate) reservation: Option<Reservation>,
    pub(crate) limit_engine: Option<Arc<LimitEngine>>,
    pub(crate) model: String,
    pub(crate) principal: cc_lb_domain::Principal,
    pub(crate) upstream: cc_lb_domain::Upstream,
    pub(crate) path: String,
    pub(crate) dialect: Arc<dyn cc_lb_upstream::UpstreamDialect>,
    pub(crate) provider_request_id: Option<String>,
}

impl RealtimeWebSocketSession {
    pub fn has_frame_transform(&self) -> bool {
        self.dialect.realtime_frame_transform_hook().is_some()
    }

    /// Transforms a provider WebSocket frame using the configured upstream hook.
    pub fn transform_frame(
        &self,
        direction: RealtimeFrameDirection,
        kind: RealtimeFrameKind,
        payload: Bytes,
    ) -> Result<TransformRealtimeFrameResult, cc_lb_upstream::ResponseTransformError> {
        let Some(hook) = self.dialect.realtime_frame_transform_hook() else {
            return Ok(TransformRealtimeFrameResult::Unchanged);
        };
        hook.transform_realtime_frame(TransformRealtimeFrameRequest {
            request_id: self
                .observer
                .as_ref()
                .map(|observer| observer.event_id().to_owned())
                .unwrap_or_default(),
            principal: self.principal.clone(),
            upstream: self.upstream.clone(),
            request_method: http::Method::GET,
            request_path: self.path.clone(),
            canonical_model_id: self.model.clone(),
            direction,
            kind,
            payload,
        })
    }

    /// Publish the actual successful provider upgrade, not an HTTP body milestone.
    pub fn connected(&mut self, response_headers: &HeaderMap) {
        self.provider_request_id = ["x-request-id", "request-id", "x-goog-request-id"]
            .iter()
            .find_map(|name| {
                let value = response_headers.get(*name)?.to_str().ok()?;
                (!value.is_empty() && value.len() <= 512)
                    .then(|| truncate_reason(&RedactionPolicy::default().redact_text(value)))
            });
        if let Some(observer) = &self.observer {
            observer.set_response_stream_span(self.stream_span.clone());
            observer.emit_lifecycle(LifecycleEvent::UpstreamAttempt {
                event_id: observer.event_id().to_owned(),
                attempt_num: 1,
                upstream_id: self.candidate.upstream_id,
            });
            observer.set_success_status(StatusCode::SWITCHING_PROTOCOLS);
            observer.emit_lifecycle(LifecycleEvent::UpstreamResponseStarted {
                event_id: observer.event_id().to_owned(),
                status: StatusCode::SWITCHING_PROTOCOLS.as_u16(),
                headers: Default::default(),
                bulkhead_wait_ms: None,
                dns_ms: None,
                connect_ms: None,
                connection_reused: None,
                shape_ms: None,
                sign_ms: None,
                upstream_ttfb_ms: None,
            });
        }
    }

    /// Trace propagation belongs to the engine-created connection child span.
    pub fn inject_trace_context(&self, headers: &mut HeaderMap) {
        self.stream_span.in_scope(|| {
            cc_lb_observability::inject_current_trace_context(headers);
        });
    }

    pub fn usage_observer(&self) -> RealtimeUsageObserver {
        RealtimeUsageObserver {
            observer: self.observer.clone(),
        }
    }

    /// Called after the usage worker drains; emits one terminal event per connection.
    pub fn finish(
        mut self,
        usage: RealtimeUsage,
        metrics: RealtimeStreamMetrics,
        termination: RealtimeWebSocketTermination,
        diagnostics: RealtimeWebSocketDiagnostics,
    ) {
        let status = StatusCode::SWITCHING_PROTOCOLS;
        // A proxy relay error or downstream cancellation always wins over a
        // completion marker from an earlier turn on the same connection.
        // Completion is scoped to the terminal transport path, rather than
        // allowing sticky usage state to turn a later failure into success.
        let complete = realtime_stream_complete(
            termination,
            metrics.turn_complete().unwrap_or(usage.complete),
        );
        let (outcome, mut cause) = realtime_terminal_outcome(termination, complete);
        if !complete && let Some(error) = &diagnostics.relay_error {
            cause = error.cause;
        }
        record_stream_termination_metric(
            outcome,
            cause,
            &self.candidate.upstream_name,
            Some(&self.model),
        );
        let (error_type, error_message) = if complete {
            (None, None)
        } else {
            (
                Some(match termination {
                    RealtimeWebSocketTermination::DownstreamCancelled => "client_closed_request",
                    RealtimeWebSocketTermination::ProxyRelayError => "proxy_relay_error",
                    RealtimeWebSocketTermination::ProviderClosed
                    | RealtimeWebSocketTermination::Completed => "realtime_stream_incomplete",
                }),
                Some("realtime WebSocket stream ended before completion"),
            )
        };
        let diagnostic_message = diagnostics.message();
        let error_message = diagnostic_message.as_deref().or(error_message);
        if let Some((code, reason)) = &diagnostics.close {
            self.stream_span
                .record("websocket.close.code", i64::from(*code));
            self.stream_span
                .record("websocket.close.reason", reason.as_str());
        }
        if let Some(error) = &diagnostics.relay_error {
            self.stream_span
                .record("error.io.kind", error.io_kind.as_deref());
            self.stream_span
                .record("error.io.os_error", error.io_os_error);
        }
        record_websocket_terminal_span(
            &self.stream_span,
            outcome,
            cause,
            error_type,
            if complete { None } else { error_message },
        );
        if let Some(observer) = &self.observer {
            if let Some(counts) = usage.counts.as_ref() {
                observer.set_usage_counts(counts, complete);
                observer.emit_lifecycle(LifecycleEvent::UsageObserved {
                    event_id: observer.event_id().to_owned(),
                    usage: usage.snapshot(),
                    source: UsageSource::NonStreamBody,
                });
            }
            if !complete || diagnostics.close.is_some() || self.provider_request_id.is_some() {
                observer.emit_lifecycle(LifecycleEvent::UpstreamStreamDiagnosticsObserved {
                    event_id: observer.event_id().to_owned(),
                    diagnostics: cc_lb_lifecycle::UpstreamStreamDiagnostics {
                        // The upgraded WebSocket has no HTTP response body.
                        http_version: None,
                        request_id: self.provider_request_id.clone(),
                        body_bytes: metrics.total_bytes,
                        body_end: diagnostics
                            .relay_error
                            .as_ref()
                            .map(|_| cc_lb_lifecycle::UpstreamBodyEnd::TransportError),
                        body_error_cause: (!complete).then(|| cause.as_str().to_owned()),
                        body_error_io_kind: diagnostics
                            .relay_error
                            .as_ref()
                            .and_then(|error| error.io_kind.clone()),
                        warning_type: diagnostics
                            .close
                            .as_ref()
                            .map(|_| "websocket_close".to_owned()),
                        warning_message: diagnostics
                            .close
                            .as_ref()
                            .and_then(|_| diagnostic_message.clone()),
                        ..Default::default()
                    },
                });
            }
            let latency = metrics.latency(status.as_u16());
            latency.record_on_span(&self.stream_span);
            observer.emit_lifecycle(LifecycleEvent::StreamCompleted {
                event_id: observer.event_id().to_owned(),
                result: if complete {
                    Ok(StreamSuccess {
                        usage: usage.snapshot(),
                        usage_present: usage.counts.is_some(),
                        sse_event_count: latency.sse_event_count,
                        body_bytes: Some(latency.total_bytes),
                        // WebSocket output frames use the provider-neutral
                        // chunk count; never expose them as Anthropic SSE
                        // events or content deltas.
                        body_chunk_count: Some(latency.stream_event_count),
                        first_body_chunk_ms: latency.stream_first_output_ms,
                        stream_last_chunk_ms: latency.stream_last_output_ms,
                        stream_total_ms: Some(latency.stream_total_ms),
                        ping_count: Some(latency.ping_count),
                        stream_inter_output_avg_ms: latency.stream_inter_output_avg_ms,
                        provider_session_id: latency.provider_session_id.clone(),
                        provider_connection_id: latency.provider_connection_id.clone(),
                        ..Default::default()
                    })
                } else {
                    Err(StreamError {
                        error_type: match termination {
                            RealtimeWebSocketTermination::DownstreamCancelled => {
                                "client_closed_request"
                            }
                            RealtimeWebSocketTermination::ProxyRelayError => "proxy_relay_error",
                            _ => "realtime_stream_incomplete",
                        }
                        .to_owned(),
                        error_message: error_message
                            .unwrap_or("realtime WebSocket stream ended before completion")
                            .to_owned(),
                        body_bytes: Some(latency.total_bytes),
                        body_chunk_count: Some(latency.stream_event_count),
                        first_body_chunk_ms: latency.stream_first_output_ms,
                        stream_last_chunk_ms: latency.stream_last_chunk_ms,
                        stream_total_ms: Some(latency.stream_total_ms),
                        ping_count: Some(latency.ping_count),
                        stream_inter_output_avg_ms: latency.stream_inter_output_avg_ms,
                        provider_session_id: latency.provider_session_id.clone(),
                        provider_connection_id: latency.provider_connection_id.clone(),
                    })
                },
            });
            if let Some(reservation) = self.reservation.take() {
                reservation.handoff_after_response();
            }
            match termination {
                RealtimeWebSocketTermination::DownstreamCancelled => {
                    if complete {
                        observer.set_success_status(status);
                    } else {
                        observer.set_client_closed(status);
                    }
                }
                RealtimeWebSocketTermination::ProxyRelayError => {
                    observer.set_failure(InternalFailure {
                        status,
                        error_code: UpstreamErrorCode::StreamError.as_str(),
                        error: InternalError {
                            stage: InternalErrorStage::Relay,
                            kind: InternalErrorKind::Unavailable,
                            message: Some(
                                error_message
                                    .unwrap_or("realtime WebSocket relay failed before provider completion")
                                    .to_owned(),
                            ),
                        },
                    });
                }
                RealtimeWebSocketTermination::Completed => {
                    observer.set_success_status(status);
                }
                RealtimeWebSocketTermination::ProviderClosed if complete => {
                    observer.set_success_status(status);
                }
                RealtimeWebSocketTermination::ProviderClosed => {
                    observer.set_upstream_error(status, UpstreamErrorCode::StreamError);
                }
            }
            observer.finish();
            if let Some(dispatch) = tracing::enabled!(
                target: "cc_lb_engine::lifecycle",
                tracing::Level::INFO
            )
            .then(|| tracing::dispatcher::get_default(|dispatch| dispatch.clone()))
            {
                let _ = self.completion_observer.enqueue(StreamCompletionLog::new(
                    observer.event_id().to_owned(),
                    &self.stream_span,
                    latency,
                    dispatch,
                ));
            }
        } else if let (Some(engine), Some(reservation), Some(counts)) = (
            self.limit_engine.as_ref(),
            self.reservation.take(),
            usage.counts.as_ref(),
        ) {
            let cost = cc_lb_pricing::virtual_cost_micros_full_with_context_and_provider_writes(
                &self.model,
                counts.input_tokens,
                counts.output_tokens,
                counts.input_image_tokens.unwrap_or(0),
                counts.output_image_tokens.unwrap_or(0),
                counts.cache_creation_input_tokens,
                counts.cache_creation_input_tokens_5m,
                counts.cache_creation_input_tokens_1h,
                counts.cache_read_input_tokens,
                counts.cache_write_input_tokens,
                counts.cache_write_input_tokens_30m,
                counts.cache_write_input_tokens_12h,
                counts.service_tier.as_deref(),
            )
            .unwrap_or_else(cc_lb_pricing::ComputedCostBreakdown::unknown);
            engine.reconcile(
                reservation,
                counts.input_tokens,
                counts.output_tokens,
                cost.total_micros,
            );
        }
    }

    /// Finish a failed provider handshake without treating HTTP errors as a
    /// successful relay response.
    pub fn finish_handshake(mut self, status: StatusCode) {
        if let Some(observer) = &self.observer {
            if let Some(reservation) = self.reservation.take() {
                reservation.handoff_after_response();
            }
            if status.is_client_error() {
                observer.set_upstream_error(status, UpstreamErrorCode::Upstream4xx);
            } else if status.is_server_error() {
                observer.set_upstream_error(status, UpstreamErrorCode::Upstream5xx);
            } else {
                observer.set_success_status(status);
            }
            observer.finish();
        }
    }
}

#[derive(Default)]
pub struct RealtimeUsage {
    counts: Option<UsageCounts>,
    complete: bool,
    observer: Option<RealtimeUsageObserver>,
    published: Option<UsageSnapshot>,
}

impl RealtimeUsage {
    pub fn with_observer(observer: RealtimeUsageObserver) -> Self {
        Self {
            observer: Some(observer),
            ..Default::default()
        }
    }

    /// Run on the bounded usage worker, never on the transport relay task.
    pub fn observe(&mut self, kind: RealtimeWebSocketKind, frame: &Bytes) {
        let Ok(value) = sonic_rs::from_slice::<Value>(frame) else {
            return;
        };
        let number =
            |value: &Value, name: &str| value.get(name).and_then(Value::as_u64).unwrap_or(0);
        match kind {
            RealtimeWebSocketKind::Openai => {
                match value.get("type").and_then(Value::as_str) {
                    Some("response.created") => {
                        // A single OpenAI Realtime connection can carry
                        // multiple response turns. Completion belongs to the
                        // current turn, not the lifetime of the socket.
                        self.complete = false;
                        return;
                    }
                    Some("response.done") => {}
                    _ => return,
                }
                self.complete = true;
                let Some(usage) = value
                    .pointer("/response/usage")
                    .filter(|value| value.is_object())
                else {
                    return;
                };
                let mut normalized = UsageCounts {
                    present: true,
                    billable_present: true,
                    ..Default::default()
                };
                merge_usage_value_for_shape(
                    &mut normalized,
                    usage,
                    Some(crate::provider_capability::UsageShape::OpenaiResponses),
                );
                let counts = self.counts.get_or_insert_with(UsageCounts::default);
                counts.present = true;
                counts.billable_present = true;
                counts.input_tokens = counts.input_tokens.saturating_add(normalized.input_tokens);
                counts.output_tokens = counts
                    .output_tokens
                    .saturating_add(normalized.output_tokens);
                counts.cache_read_input_tokens = counts
                    .cache_read_input_tokens
                    .saturating_add(normalized.cache_read_input_tokens);
                if let Some(value) = normalized.cache_write_input_tokens {
                    counts.cache_write_input_tokens = Some(
                        counts
                            .cache_write_input_tokens
                            .unwrap_or_default()
                            .saturating_add(value),
                    );
                }
                if let Some(value) = normalized.cache_write_input_tokens_30m {
                    counts.cache_write_input_tokens_30m = Some(
                        counts
                            .cache_write_input_tokens_30m
                            .unwrap_or_default()
                            .saturating_add(value),
                    );
                }
                if let Some(value) = normalized.cache_write_input_tokens_12h {
                    counts.cache_write_input_tokens_12h = Some(
                        counts
                            .cache_write_input_tokens_12h
                            .unwrap_or_default()
                            .saturating_add(value),
                    );
                }
                if let Some(image_tokens) = normalized.input_image_tokens {
                    counts.input_image_tokens = Some(
                        counts
                            .input_image_tokens
                            .unwrap_or_default()
                            .saturating_add(image_tokens),
                    );
                }
                if let Some(image_tokens) = normalized.output_image_tokens {
                    counts.output_image_tokens = Some(
                        counts
                            .output_image_tokens
                            .unwrap_or_default()
                            .saturating_add(image_tokens),
                    );
                }
            }
            RealtimeWebSocketKind::Gemini => {
                if value.pointer("/serverContent/modelTurn").is_some() {
                    // Live turns can share one session; a new model turn
                    // invalidates the prior turn's completion marker.
                    self.complete = false;
                }
                if value
                    .pointer("/serverContent/turnComplete")
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    self.complete = true;
                }
                let Some(usage) = value.get("usageMetadata").filter(|value| value.is_object())
                else {
                    return;
                };
                let mut normalized = UsageCounts {
                    present: true,
                    billable_present: true,
                    ..Default::default()
                };
                merge_usage_value(&mut normalized, usage);
                // Live usageMetadata is cumulative for the session, unlike Realtime response.done.
                self.counts = Some(UsageCounts {
                    present: true,
                    billable_present: true,
                    input_tokens: normalized.input_tokens,
                    output_tokens: number(usage, "responseTokenCount")
                        .saturating_add(number(usage, "thoughtsTokenCount")),
                    cache_read_input_tokens: normalized.cache_read_input_tokens,
                    ..Default::default()
                });
            }
        }
        if let Some(observer) = self
            .observer
            .as_ref()
            .and_then(|handle| handle.observer.as_ref())
            && let Some(counts) = self.counts.as_ref()
        {
            let snapshot = self.snapshot();
            if self.published.as_ref() != Some(&snapshot) {
                // Active counters are cumulative replacements. Only finish can
                // declare completeness after the terminal relay path is known.
                observer.set_usage_counts(counts, false);
                observer.emit_lifecycle(LifecycleEvent::UsageObserved {
                    event_id: observer.event_id().to_owned(),
                    usage: snapshot.clone(),
                    source: UsageSource::NonStreamBody,
                });
                self.published = Some(snapshot);
            }
        }
    }
    fn snapshot(&self) -> UsageSnapshot {
        self.counts
            .as_ref()
            .map(|counts| UsageSnapshot {
                input_tokens: counts.input_tokens,
                output_tokens: counts.output_tokens,
                input_image_tokens: counts.input_image_tokens,
                output_image_tokens: counts.output_image_tokens,
                cache_read_input_tokens: counts.cache_read_input_tokens,
                cache_write_input_tokens: counts.cache_write_input_tokens,
                cache_write_input_tokens_30m: counts.cache_write_input_tokens_30m,
                cache_write_input_tokens_12h: counts.cache_write_input_tokens_12h,
                thinking_tokens: counts.thinking_tokens,
                ..Default::default()
            })
            .unwrap_or_default()
    }
}

/// Extract routing metadata only from the first authenticated Gemini setup frame.
pub fn gemini_setup_model(_auth: &crate::Authenticated, frame: &[u8]) -> Option<String> {
    let value: Value = sonic_rs::from_slice(frame).ok()?;
    value
        .pointer("/setup/model")
        .and_then(Value::as_str)
        .map(|model| model.strip_prefix("models/").unwrap_or(model).to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        RealtimeStreamMetrics, RealtimeUsage, RealtimeWebSocketKind, RealtimeWebSocketTermination,
        realtime_stream_complete, realtime_terminal_outcome, record_stream_termination_metric,
    };
    use crate::terminal_observer::{StreamTerminationCause, StreamTerminationOutcome};
    use bytes::Bytes;
    use http::StatusCode;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn realtime_diagnostics_redact_bound_and_classify_real_io() {
        let reason = format!("Bearer abcdef.ghijkl {}", "x".repeat(4096));
        let error = std::io::Error::from_raw_os_error(54);
        let diagnostics = super::RealtimeWebSocketDiagnostics::default()
            .with_close(1011, &reason)
            .with_relay_error(&error);
        let message = diagnostics.message().expect("native close retained");
        assert!(message.starts_with("websocket close code=1011 reason="));
        assert!(!message.contains("Bearer abcdef.ghijkl"));
        assert!(message.len() <= cc_lb_observability::ROUTING_REASON_MAX_BYTES);
        let io = diagnostics.relay_error.expect("typed I/O classification");
        assert_eq!(io.io_os_error, Some(54));
        assert_eq!(io.io_kind, Some(format!("{:?}", error.kind())));
        assert_eq!(io.h2_reason, None);
    }

    #[test]
    fn realtime_relay_io_causes_remain_typed_without_h2() {
        for (kind, expected) in [
            (
                std::io::ErrorKind::ConnectionReset,
                StreamTerminationCause::IoReset,
            ),
            (
                std::io::ErrorKind::TimedOut,
                StreamTerminationCause::IoTimeout,
            ),
            (
                std::io::ErrorKind::UnexpectedEof,
                StreamTerminationCause::UnexpectedEof,
            ),
        ] {
            let error = std::io::Error::new(kind, "relay failed");
            let diagnostics =
                super::RealtimeWebSocketDiagnostics::default().with_relay_error(&error);
            let io = diagnostics.relay_error.expect("real transport error");
            assert_eq!(io.cause, expected);
            assert_eq!(io.io_kind, Some(format!("{kind:?}")));
            assert_eq!(io.h2_reason, None);
        }
    }

    #[test]
    fn realtime_usage_publishes_changed_cumulative_snapshots_before_finish() {
        use cc_lb_clock::{ClockHandle, SystemClock};
        use cc_lb_control::RequestEventBus;
        use cc_lb_control::event_bus::InMemoryBus;
        use cc_lb_lifecycle::LifecycleEvent;
        use std::sync::Arc;

        for kind in [RealtimeWebSocketKind::Openai, RealtimeWebSocketKind::Gemini] {
            let bus = Arc::new(InMemoryBus::new());
            let mut events = bus.subscribe_lifecycle();
            let clock: ClockHandle = Arc::new(SystemClock);
            let observer = crate::terminal_observer::LifecycleContext::new(
                "realtime-usage".to_owned(),
                bus,
                &clock,
            );
            observer.mark_authn_reached();
            let mut usage = RealtimeUsage::with_observer(super::RealtimeUsageObserver {
                observer: Some(observer.clone()),
            });
            let frames = match kind {
                RealtimeWebSocketKind::Openai => [
                    br#"{"type":"response.done","response":{"usage":{"input_tokens":10,"output_tokens":3}}}"#.as_slice(),
                    br#"{"type":"response.done","response":{"usage":{"input_tokens":10,"output_tokens":4}}}"#.as_slice(),
                ],
                RealtimeWebSocketKind::Gemini => [
                    br#"{"usageMetadata":{"promptTokenCount":10,"responseTokenCount":3}}"#.as_slice(),
                    br#"{"usageMetadata":{"promptTokenCount":20,"responseTokenCount":7}}"#.as_slice(),
                ],
            };
            for (frame, expected) in frames.into_iter().zip([(10, 3), (20, 7)]) {
                usage.observe(kind, &Bytes::copy_from_slice(frame));
                let LifecycleEvent::UsageObserved {
                    usage: snapshot, ..
                } = events.try_recv().expect("active snapshot before close")
                else {
                    panic!("expected only a usage observation");
                };
                assert_eq!((snapshot.input_tokens, snapshot.output_tokens), expected);
                assert!(events.try_recv().is_err(), "no early terminal observation");
            }
            if kind == RealtimeWebSocketKind::Gemini {
                usage.observe(kind, &Bytes::copy_from_slice(frames[1]));
                assert!(
                    events.try_recv().is_err(),
                    "unchanged cumulative usage is deduplicated"
                );
            }
            usage.observe(kind, &Bytes::from_static(br#"{"ignored":true}"#));
            assert!(events.try_recv().is_err(), "ordinary frames are not usage");
            assert_eq!(
                (
                    usage.snapshot().input_tokens,
                    usage.snapshot().output_tokens
                ),
                (20, 7)
            );
            observer.finish();
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleEvent::RequestTerminated { .. })
            ));
        }
    }
    #[test]
    fn realtime_usage_sums_only_response_done() {
        let mut usage = RealtimeUsage::default();
        usage.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.created","response":{"usage":{"input_tokens":999}}}"#,
            ),
        );
        for _ in 0..2 {
            usage.observe(RealtimeWebSocketKind::Openai, &Bytes::from_static(br#"{"type":"response.done","response":{"usage":{"input_tokens":10,"output_tokens":3,"input_token_details":{"cached_tokens":4}}}}"#));
        }
        assert!(usage.complete);
        let counts = usage.counts.unwrap();
        assert!(counts.present);
        assert!(counts.billable_present);
        assert_eq!(
            (
                counts.input_tokens,
                counts.output_tokens,
                counts.cache_read_input_tokens
            ),
            (12, 6, 8)
        );
    }
    #[test]
    fn realtime_usage_completion_is_reset_for_new_turns() {
        let mut openai = RealtimeUsage::default();
        openai.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.done","response":{"usage":{"input_tokens":1,"output_tokens":1}}}"#,
            ),
        );
        assert!(openai.complete);
        openai.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(br#"{"type":"response.created"}"#),
        );
        assert!(!openai.complete);

        let mut gemini = RealtimeUsage::default();
        gemini.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(br#"{"serverContent":{"turnComplete":true}}"#),
        );
        assert!(gemini.complete);
        gemini.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(br#"{"serverContent":{"modelTurn":{"parts":[]}}}"#),
        );
        assert!(!gemini.complete);
    }
    #[test]
    fn relay_turn_boundaries_reset_completion_before_later_termination() {
        let mut metrics = RealtimeStreamMetrics::new();
        metrics.observe_turn_boundary(
            RealtimeWebSocketKind::Openai,
            br#"{"type":"response.done","response":{"usage":{"input_tokens":1}}}"#,
        );
        assert_eq!(metrics.turn_complete(), Some(true));
        metrics.observe_turn_boundary(
            RealtimeWebSocketKind::Openai,
            br#"{"type":"response.created","response":{"id":"later"}}"#,
        );
        assert_eq!(metrics.turn_complete(), Some(false));

        for (termination, expected) in [
            (
                RealtimeWebSocketTermination::ProviderClosed,
                StreamTerminationOutcome::UpstreamError,
            ),
            (
                RealtimeWebSocketTermination::ProxyRelayError,
                StreamTerminationOutcome::ProxyError,
            ),
            (
                RealtimeWebSocketTermination::DownstreamCancelled,
                StreamTerminationOutcome::ClientCancelled,
            ),
        ] {
            let complete =
                realtime_stream_complete(termination, metrics.turn_complete().unwrap_or_default());
            assert!(!complete);
            assert_eq!(realtime_terminal_outcome(termination, complete).0, expected);
        }
    }

    #[test]
    fn realtime_openai_usage_propagates_image_details_only_for_openai() {
        let mut openai = RealtimeUsage::default();
        openai.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.done","response":{"usage":{"input_tokens":100,"output_tokens":7,"input_token_details":{"image_tokens":12},"output_tokens_details":{"image_tokens":4}}}}"#,
            ),
        );
        let counts = openai.counts.unwrap();
        assert_eq!(counts.input_image_tokens, Some(12));
        assert_eq!(counts.output_image_tokens, Some(4));

        let mut gemini = RealtimeUsage::default();
        gemini.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(
                br#"{"usageMetadata":{"promptTokenCount":100,"responseTokenCount":7,"modalityTokenCount":[{"modality":"IMAGE","tokenCount":12}]}}"#,
            ),
        );
        let counts = gemini.counts.unwrap();
        assert_eq!(counts.input_image_tokens, None);
        assert_eq!(counts.output_image_tokens, None);
    }

    #[test]
    fn realtime_openai_usage_preserves_provider_cache_write_lifetimes() {
        let mut usage = RealtimeUsage::default();
        usage.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.done","response":{"usage":{"input_tokens":100,"output_tokens":7,"input_tokens_details":{"cache_write_tokens":30,"cache_write_tokens_details":{"ttl_30m_tokens":20,"ttl_12h_tokens":10}}}}}"#,
            ),
        );
        let counts = usage
            .counts
            .expect("provider cache writes should be present");
        assert_eq!(counts.cache_write_input_tokens, Some(30));
        assert_eq!(counts.cache_write_input_tokens_30m, Some(20));
        assert_eq!(counts.cache_write_input_tokens_12h, Some(10));
    }

    #[test]
    fn realtime_usage_subtracts_cached_input_tokens() {
        let mut openai = RealtimeUsage::default();
        openai.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.done","response":{"usage":{"input_tokens":1000,"output_tokens":7,"input_token_details":{"cached_tokens":900}}}}"#,
            ),
        );
        let counts = openai.counts.unwrap();
        assert_eq!(
            (
                counts.input_tokens,
                counts.output_tokens,
                counts.cache_read_input_tokens
            ),
            (100, 7, 900)
        );

        let mut gemini = RealtimeUsage::default();
        gemini.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(
                br#"{"usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":900,"responseTokenCount":7}}"#,
            ),
        );
        let counts = gemini.counts.unwrap();
        assert_eq!(
            (
                counts.input_tokens,
                counts.output_tokens,
                counts.cache_read_input_tokens
            ),
            (100, 7, 900)
        );
    }

    #[test]
    fn live_usage_uses_latest_cumulative_snapshot() {
        let mut usage = RealtimeUsage::default();
        usage.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(
                br#"{"usageMetadata":{"promptTokenCount":10,"responseTokenCount":3}}"#,
            ),
        );
        usage.observe(RealtimeWebSocketKind::Gemini, &Bytes::from_static(br#"{"usageMetadata":{"promptTokenCount":20,"responseTokenCount":7,"thoughtsTokenCount":2}}"#));
        usage.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(br#"{"serverContent":{"turnComplete":true}}"#),
        );
        assert!(usage.complete);
        let counts = usage.counts.unwrap();
        assert_eq!((counts.input_tokens, counts.output_tokens), (20, 9));
        assert!(
            RealtimeWebSocketKind::Gemini
                .may_report_usage(br#"{"serverContent":{"turnComplete":true}}"#)
        );
        assert!(
            !RealtimeWebSocketKind::Gemini
                .may_report_usage(br#"{"serverContent":{"audio":"abc"}}"#)
        );
    }

    #[test]
    fn realtime_usage_accounts_for_burst_beyond_relay_channel_capacity() {
        let mut usage = RealtimeUsage::default();
        let frame = Bytes::from_static(
            br#"{"type":"response.done","response":{"usage":{"input_tokens":1,"output_tokens":1}}}"#,
        );
        for _ in 0..64 {
            usage.observe(RealtimeWebSocketKind::Openai, &frame);
        }
        let counts = usage.counts.expect("usage should be present");
        assert_eq!((counts.input_tokens, counts.output_tokens), (64, 64));
    }

    #[test]
    fn realtime_stream_metrics_are_provider_neutral_and_bounded() {
        let mut metrics = RealtimeStreamMetrics::new();
        metrics.observe_frame(b"event");
        metrics.observe_ping();
        metrics.observe_frame(b"more");
        let latency = metrics.latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        assert_eq!(latency.sse_event_count, 0);
        assert_eq!(latency.content_delta_count, 0);
        assert_eq!(latency.stream_event_count, 2);
        assert!(latency.stream_first_output_ms.is_some());
        assert!(latency.stream_last_output_ms.is_some());
        assert!(latency.stream_inter_output_avg_ms.is_some());
        assert_eq!(latency.ping_count, 1);
        assert_eq!(latency.total_bytes, 9);
        assert_eq!(latency.status, 101);
    }

    #[test]
    fn realtime_provider_metadata_is_bounded_and_protocol_specific() {
        let mut metrics = RealtimeStreamMetrics::new();
        metrics.observe_provider_metadata(
            RealtimeWebSocketKind::Openai,
            br#"{"type":"session.created","session":{"id":"rt-session","connection_id":"rt-connection"}}"#,
        );
        let latency = metrics.latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        assert_eq!(latency.provider_session_id.as_deref(), Some("rt-session"));
        assert_eq!(
            latency.provider_connection_id.as_deref(),
            Some("rt-connection")
        );

        let mut gemini = RealtimeStreamMetrics::new();
        gemini.observe_provider_metadata(
            RealtimeWebSocketKind::Gemini,
            br#"{"setupComplete":{"sessionId":"live-session","connectionId":"live-connection"}}"#,
        );
        let latency = gemini.latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        assert_eq!(latency.provider_session_id.as_deref(), Some("live-session"));
        assert_eq!(
            latency.provider_connection_id.as_deref(),
            Some("live-connection")
        );

        let mut bounded = RealtimeStreamMetrics::new();
        bounded.observe_provider_metadata(
            RealtimeWebSocketKind::Openai,
            format!(
                r#"{{"type":"session.created","session":{{"id":"{}"}}}}"#,
                "x".repeat(513)
            )
            .as_bytes(),
        );
        assert!(
            bounded
                .latency(StatusCode::SWITCHING_PROTOCOLS.as_u16())
                .provider_session_id
                .is_none()
        );
    }
    #[test]
    fn realtime_terminal_outcomes_cover_every_transport_path() {
        let cases = [
            (
                RealtimeWebSocketTermination::Completed,
                false,
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
            (
                RealtimeWebSocketTermination::ProviderClosed,
                false,
                StreamTerminationOutcome::UpstreamError,
                StreamTerminationCause::ProviderError,
            ),
            (
                RealtimeWebSocketTermination::ProviderClosed,
                true,
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
            (
                RealtimeWebSocketTermination::ProxyRelayError,
                false,
                StreamTerminationOutcome::ProxyError,
                StreamTerminationCause::TransportError,
            ),
            (
                RealtimeWebSocketTermination::DownstreamCancelled,
                false,
                StreamTerminationOutcome::ClientCancelled,
                StreamTerminationCause::None,
            ),
        ];
        for (termination, complete, expected_outcome, expected_cause) in cases {
            assert_eq!(
                realtime_terminal_outcome(termination, complete),
                (expected_outcome, expected_cause)
            );
        }
    }

    #[test]
    fn realtime_completion_does_not_stick_to_later_failures() {
        assert!(realtime_stream_complete(
            RealtimeWebSocketTermination::ProviderClosed,
            true
        ));
        assert!(!realtime_stream_complete(
            RealtimeWebSocketTermination::ProxyRelayError,
            true
        ));
        assert!(!realtime_stream_complete(
            RealtimeWebSocketTermination::DownstreamCancelled,
            true
        ));
    }

    #[test]
    fn realtime_terminal_metric_is_bounded_and_emitted_once_per_outcome() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        for (outcome, cause) in [
            (
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
            (
                StreamTerminationOutcome::UpstreamError,
                StreamTerminationCause::ProviderError,
            ),
            (
                StreamTerminationOutcome::ProxyError,
                StreamTerminationCause::TransportError,
            ),
            (
                StreamTerminationOutcome::ClientCancelled,
                StreamTerminationCause::None,
            ),
        ] {
            record_stream_termination_metric(
                outcome,
                cause,
                "gemini-live-upstream",
                Some("gemini-live"),
            );
        }
        let samples = snapshotter.snapshot().into_vec();
        assert_eq!(samples.len(), 4);
        for (key, _, _, value) in samples {
            assert_eq!(key.key().name(), "cc_lb_stream_terminations_total");
            let labels = key.key().labels().collect::<Vec<_>>();
            assert_eq!(labels.len(), 4);
            assert!(labels.iter().all(|label| label.value().len() <= 128));
            assert!(matches!(value, DebugValue::Counter(1)));
        }
    }

    #[test]
    fn realtime_timing_keeps_native_live_markers_out_of_anthropic_counters() {
        let mut metrics = RealtimeStreamMetrics::new();
        metrics.observe_provider_metadata(
            RealtimeWebSocketKind::Gemini,
            br#"{"sessionResumptionUpdate":{"newSessionId":"live-session"}}"#,
        );
        metrics.observe_frame(br#"{"serverContent":{"modelTurn":{"parts":[{"text":"hi"}]}}}"#);
        metrics.observe_frame(br#"{"serverContent":{"turnComplete":true}}"#);
        let latency = metrics.latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        assert_eq!(latency.provider_session_id.as_deref(), Some("live-session"));
        assert_eq!(latency.stream_event_count, 2);
        assert!(latency.stream_first_output_ms.is_some());
        assert!(latency.stream_last_output_ms.is_some());
        assert_eq!(latency.sse_event_count, 0);
        assert_eq!(latency.content_delta_count, 0);
        assert_eq!(latency.ping_count, 0);
    }
}
