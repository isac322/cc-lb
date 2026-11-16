//! Single-attempt, non-generation observation for the C-RC runtime.
//! This handle records an already-authorized dispatch; it grants no authority.

use std::{pin::Pin, task::{Context, Poll}, time::Instant};

use axum::body::Body;
use bytes::Bytes;
use cc_lb_clock::{ClockHandle, unix_millis};
use cc_lb_lifecycle::{LifecycleEvent, RouteInfo, StreamError, StreamSuccess};
use cc_lb_storage_api::{UpstreamKind, UpstreamRecord};
use http::{HeaderMap, StatusCode};
use http_body::{Body as HttpBody, Frame, SizeHint};

use crate::{Authenticated, LifecycleContext, TerminalClassification};
use crate::terminal_observer::{InternalFailure, UpstreamErrorCode, error_codes};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteControlWebSocketTermination {
    Graceful,
    Authentication,
    OwnerChanged,
    Overloaded,
    Transport,
    WriteTimeout,
    Draining,
    DrainTimeout,
    PeerErrorClose(u16),
}

/// Owned by exactly one HTTP body or upgraded connection. Construct it only
/// with the authenticated proof and lifecycle context from the same handler.
/// No authentication, owner lookup, body parsing, or usage inference occurs here.
pub struct RemoteControlObservation {
    context: Option<LifecycleContext>,
    clock: Option<ClockHandle>,
    started: Option<Instant>,
    kind: UpstreamKind,
    response_status: Option<StatusCode>,
    finished: bool,
    cancellation_owned: bool,
}

impl RemoteControlObservation {
    pub fn begin(
        _auth: &Authenticated,
        context: Option<&LifecycleContext>,
        upstream: &UpstreamRecord,
        clock: &ClockHandle,
    ) -> Self {
        if let Some(context) = context {
            context.emit_lifecycle(LifecycleEvent::RouteCompleted {
                event_id: context.event_id().to_owned(),
                result: Ok(RouteInfo {
                    upstream_id: upstream.id, upstream_name: upstream.name.clone(), model: None,
                    upstream_kind: Some(upstream.kind.as_str().to_owned()), route_ms: None, routing_trace: None,
                    predicted_cache_read_tokens: None, matched_v3_cache_key: None,
                    breakpoint_content_block_index: None, matched_content_block_index: None,
                    lookback_distance: None, predicted_cache_creation_tokens_5m: None,
                    predicted_cache_creation_tokens_1h: None, token_estimate_source: None,
                    cache_breakpoints: None, cache_prefix_hash: None, cache_value_micros: None,
                    formula_winner_upstream_id: None, kept_upstream_id: None,
                    quota_urgency_5h: None, quota_urgency_7d: None, quota_urgency_combined: None,
                    quota_warning_multiplier: None,
                }),
                routing_trace: None,
            });
            context.emit_lifecycle(LifecycleEvent::UpstreamAttempt {
                event_id: context.event_id().to_owned(), attempt_num: 1, upstream_id: upstream.id,
            });
        }
        Self {
            context: context.cloned(), clock: context.map(|_| clock.clone()),
            started: context.map(|_| Instant::now()), kind: upstream.kind,
            response_status: None, finished: false, cancellation_owned: false,
        }
    }

    /// Record actual received provider headers once, before body rewriting.
    /// The existing engine whitelist carries quota evidence without auth/cookies.
    pub fn response_started(&mut self, status: StatusCode, headers: &HeaderMap) {
        if self.response_status.is_some() { return; }
        self.response_status = Some(status);
        let Some(context) = self.context.as_ref() else { return; };
        context.emit_lifecycle(LifecycleEvent::UpstreamResponseStarted {
            event_id: context.event_id().to_owned(), status: status.as_u16(),
            headers: crate::lifecycle::header_snapshot_from(headers, self.kind),
            bulkhead_wait_ms: None, dns_ms: None, connect_ms: None, connection_reused: None,
            shape_ms: None, sign_ms: None,
            upstream_ttfb_ms: self.started.map(|started| started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
            observed_at_unix_millis: self.clock.as_ref().map(|clock| unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64),
        });
    }

    /// Forward payload bytes unchanged. EOF/error/drop, not response headers,
    /// owns the terminal event. An already-empty body is complete immediately.
    pub fn wrap_body(mut self, body: Body) -> Body {
        if self.context.is_none() { return body; }
        self.cancellation_owned = true;
        if body.is_end_stream() {
            self.finish_upstream(Some(0), Some(0));
            return body;
        }
        Body::new(ObservedBody { body, observation: Some(self), bytes: 0, chunks: 0 })
    }

    /// A buffered response was rejected/rewritten locally after receiving the
    /// provider response. Preserve that provider milestone but record the local
    /// outcome exactly once; no reasonless upstream error is fabricated.
    pub fn complete_local(mut self, status: StatusCode) {
        self.finished = true;
        if let Some(context) = self.context.as_ref() {
            context.terminate_local(TerminalClassification::local(status));
        }
    }

    pub fn complete_transport_failure(mut self) {
        self.finished = true;
        if let Some(context) = self.context.as_ref() {
            context.terminate_failure(InternalFailure {
                status: StatusCode::BAD_GATEWAY,
                error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                error: cc_lb_domain::InternalError {
                    stage: cc_lb_domain::InternalErrorStage::Relay,
                    kind: cc_lb_domain::InternalErrorKind::Unavailable,
                    message: Some("remote control provider transport failed".to_owned()),
                },
            });
        }
    }

    /// The upgraded connection, rather than the empty 101 entity, owns finish.
    pub fn own_websocket(mut self) -> Self {
        self.cancellation_owned = true;
        self
    }

    pub fn complete_websocket(mut self, termination: RemoteControlWebSocketTermination) {
        use cc_lb_domain::{InternalErrorKind, InternalErrorStage};
        use RemoteControlWebSocketTermination as End;
        if termination == End::Graceful {
            self.finish_upstream(None, None);
            return;
        }
        let (code, stage, kind, message) = match termination {
            End::Authentication => (error_codes::AUTHENTICATION_FAILED, InternalErrorStage::Authn,
                InternalErrorKind::Unavailable, "remote control authentication no longer valid"),
            End::OwnerChanged => (error_codes::UPSTREAM_AFFINITY_UNAVAILABLE, InternalErrorStage::Storage,
                InternalErrorKind::Unavailable, "remote control creating owner no longer available"),
            End::Overloaded => (error_codes::UPSTREAM_DISPATCH_FAILED, InternalErrorStage::Relay,
                InternalErrorKind::Unavailable, "remote control relay buffer exhausted"),
            End::Transport => (error_codes::UPSTREAM_DISPATCH_FAILED, InternalErrorStage::Relay,
                InternalErrorKind::Unavailable, "remote control relay transport failed"),
            End::WriteTimeout => (error_codes::UPSTREAM_DISPATCH_FAILED, InternalErrorStage::Relay,
                InternalErrorKind::Timeout, "remote control relay write deadline exceeded"),
            End::Draining => (error_codes::DRAIN_REJECTED, InternalErrorStage::Relay,
                InternalErrorKind::Unavailable, "remote control server is draining"),
            End::DrainTimeout => (error_codes::UPSTREAM_DISPATCH_FAILED, InternalErrorStage::Relay,
                InternalErrorKind::Timeout, "remote control graceful drain deadline exceeded"),
            End::PeerErrorClose(_) => (error_codes::UPSTREAM_STREAM_ERROR, InternalErrorStage::Relay,
                InternalErrorKind::Unavailable, "remote control peer closed with an error"),
            End::Graceful => unreachable!("handled above"),
        };
        self.finished = true;
        let peer_code = match termination { End::PeerErrorClose(code) => Some(code.to_string()), _ => None };
        if let Some(context) = self.context.as_ref() {
            context.emit_lifecycle(LifecycleEvent::StreamCompleted {
                event_id: context.event_id().to_owned(),
                result: Err(StreamError { error_type: code.to_owned(), error_message: message.to_owned(),
                    upstream_error_code: peer_code, ..Default::default() }),
            });
            context.terminate_failure(InternalFailure {
                status: StatusCode::SWITCHING_PROTOCOLS, error_code: code,
                error: cc_lb_domain::InternalError { stage, kind, message: Some(message.to_owned()) },
            });
        }
    }

    fn finish_upstream(&mut self, bytes: Option<u64>, chunks: Option<u64>) {
        if self.finished { return; }
        self.finished = true;
        let Some(context) = self.context.as_ref() else { return; };
        let status = self.response_status.unwrap_or(StatusCode::BAD_GATEWAY);
        context.emit_lifecycle(LifecycleEvent::StreamCompleted {
            event_id: context.event_id().to_owned(),
            result: Ok(StreamSuccess { body_bytes: bytes, body_chunk_count: chunks,
                usage_present: false, ..Default::default() }),
        });
        if status.is_client_error() || status.is_server_error() {
            let code = if status.is_client_error() { UpstreamErrorCode::Upstream4xx } else { UpstreamErrorCode::Upstream5xx };
            context.emit_lifecycle(LifecycleEvent::RequestLogUpstreamErrorObserved {
                event_id: context.event_id().to_owned(), error_type: "http_error".to_owned(),
                error_message: "remote control provider returned an error".to_owned(),
                upstream_error_code: Some(status.as_str().to_owned()), retry_after: None,
            });
            context.set_upstream_error(status, code);
        } else {
            context.set_success_status(status);
        }
        context.finish();
    }

    fn body_failed(&mut self, bytes: u64, chunks: u64) {
        if self.finished { return; }
        self.finished = true;
        let Some(context) = self.context.as_ref() else { return; };
        let status = self.response_status.unwrap_or(StatusCode::BAD_GATEWAY);
        context.emit_lifecycle(LifecycleEvent::StreamCompleted {
            event_id: context.event_id().to_owned(),
            result: Err(StreamError { error_type: "upstream_stream_error".to_owned(),
                error_message: "remote control provider response body failed".to_owned(),
                body_bytes: Some(bytes), body_chunk_count: Some(chunks), ..Default::default() }),
        });
        context.set_upstream_error(status, UpstreamErrorCode::StreamError);
        context.finish();
    }
}

impl Drop for RemoteControlObservation {
    fn drop(&mut self) {
        // Pending handlers belong to the surrounding timeout/error middleware.
        // Only a transferred body/established connection owns client cancellation.
        if self.finished || !self.cancellation_owned { return; }
        self.finished = true;
        if let Some(context) = self.context.as_ref() {
            context.set_client_closed(if self.response_status == Some(StatusCode::SWITCHING_PROTOCOLS) {
                StatusCode::SWITCHING_PROTOCOLS
            } else { StatusCode::from_u16(499).expect("fixed client cancellation status") });
            context.finish();
        }
    }
}

struct ObservedBody {
    body: Body,
    observation: Option<RemoteControlObservation>,
    bytes: u64,
    chunks: u64,
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.bytes = self.bytes.saturating_add(data.len() as u64);
                    self.chunks = self.chunks.saturating_add(1);
                }
                if self.body.is_end_stream() {
                    if let Some(mut observation) = self.observation.take() {
                        observation.finish_upstream(Some(self.bytes), Some(self.chunks));
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                if let Some(mut observation) = self.observation.take() {
                    observation.finish_upstream(Some(self.bytes), Some(self.chunks));
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                if let Some(mut observation) = self.observation.take() {
                    observation.body_failed(self.bytes, self.chunks);
                }
                Poll::Ready(Some(Err(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool { self.body.is_end_stream() }
    fn size_hint(&self) -> SizeHint { self.body.size_hint() }
}

#[cfg(test)]
#[path = "codex_remote_control_observation_tests.rs"]
mod tests;
