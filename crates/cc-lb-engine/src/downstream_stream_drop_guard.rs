use std::time::{Duration, Instant};

use bytes::Bytes;
use cc_lb_observability::UNRESOLVED_ATTRIBUTION_LABEL;
use http::StatusCode;
use metrics::SharedString;
use tracing::Span;

use crate::attempt_rail::ResponseAccountingGuard;
use crate::body_io_timing::BodyIoTiming;
use crate::cache_keepalive::{
    LifecycleKeepalive, LifecycleKeepaliveContext, StreamingKeepaliveResponse,
};
use crate::terminal_observer::{
    LifecycleContext, StreamErrorClassification, StreamTerminationCause, StreamTerminationOutcome,
    UpstreamErrorCode, numeric_attribute,
};
use crate::usage_parser::ParsedSseEvent;

const CLIENT_CLOSED_STATUS: u16 = 499;

/// Routed upstream name and requested model attached to
/// `cc_lb_stream_terminations_total`, using the same values as the
/// lifecycle-attributed request metrics.
pub(crate) struct StreamTerminationAttribution {
    upstream: String,
    model: Option<String>,
}

impl StreamTerminationAttribution {
    pub(crate) fn new(upstream: &str, model: Option<&str>) -> Self {
        Self {
            upstream: upstream.to_owned(),
            model: model.map(str::to_owned),
        }
    }
}

/// Emit the shared terminal metric for transports that do not use the HTTP
/// body drop guard (currently realtime WebSockets). Callers own the terminal
/// state machine and must invoke this exactly once.
pub(crate) fn record_stream_termination_metric(
    outcome: StreamTerminationOutcome,
    cause: StreamTerminationCause,
    upstream: &str,
    model: Option<&str>,
) {
    let model = model.map(str::to_owned).map_or(
        SharedString::const_str(UNRESOLVED_ATTRIBUTION_LABEL),
        SharedString::from,
    );
    metrics::counter!(
        "cc_lb_stream_terminations_total",
        "outcome" => outcome.as_str(),
        "cause" => cause.as_str(),
        "upstream" => upstream.to_owned(),
        "model" => model
    )
    .increment(1);
}

struct StreamCompletionOwner {
    keepalive: LifecycleKeepalive,
    context: LifecycleKeepaliveContext,
    response: Option<StreamingKeepaliveResponse>,
    cache_anchor_at: Instant,
    runtime: tokio::runtime::Handle,
    cached_tokens: u64,
}

impl StreamCompletionOwner {
    fn new(keepalive: LifecycleKeepalive, context: LifecycleKeepaliveContext) -> Self {
        Self {
            cache_anchor_at: context.request_dispatched_at,
            runtime: tokio::runtime::Handle::current(),
            keepalive,
            context,
            response: Some(StreamingKeepaliveResponse::default()),
            cached_tokens: 0,
        }
    }

    fn observe(&mut self, event: &ParsedSseEvent<'_>) {
        if let Some(response) = self.response.as_mut() {
            response.observe(event);
        }
    }

    fn observe_emitted(&mut self, bytes: &Bytes, max_pending_sse_bytes: usize) {
        if let Some(response) = self.response.as_mut() {
            response.observe_emitted(bytes, max_pending_sse_bytes);
        }
    }

    fn set_cache_anchor_at(&mut self, at: Instant) {
        self.cache_anchor_at = at;
    }

    fn set_cached_tokens(&mut self, cached_tokens: u64) {
        self.cached_tokens = cached_tokens;
    }

    fn complete(&mut self) {
        let Some(response) = self
            .response
            .take()
            .and_then(StreamingKeepaliveResponse::into_value)
        else {
            return;
        };
        let runtime = self.runtime.clone();
        if tokio::runtime::Handle::try_current().is_err() {
            let _entered = runtime.enter();
            self.dispatch_completion(&response);
        } else {
            self.dispatch_completion(&response);
        }
    }

    fn dispatch_completion(&mut self, response: &serde_json::Value) {
        let context = &mut self.context;
        self.keepalive.on_response_completed(
            response,
            &context.principal,
            std::mem::take(&mut context.accounting_key_id),
            &context.cache_metadata,
            context.upstream_id,
            std::mem::take(&mut context.shaped_body),
            std::mem::take(&mut context.original_body),
            &context.downstream_headers,
            self.cache_anchor_at.elapsed(),
            self.cached_tokens,
            context.request_start_order,
        );
    }
}

/// Completion state shared by the normal EOF rail and the downstream-drop rail.
/// Its response snapshot is consumed at most once.
pub(crate) struct DownstreamStreamDropGuard {
    observer: Option<LifecycleContext>,
    timing_observer: Option<LifecycleContext>,
    span: Span,
    initial_upstream_status: StatusCode,
    pending_terminal: Option<(StreamTerminationOutcome, StreamTerminationCause)>,
    semantic_completed: bool,
    response_accounting_guard: Option<ResponseAccountingGuard>,
    abnormal_stop: Option<(String, String)>,
    abnormal_stop_recorded: bool,
    finalized: bool,
    relay_start: Instant,
    body_io_timing: BodyIoTiming,
    attribution: StreamTerminationAttribution,
    keepalive_completion: Option<StreamCompletionOwner>,
}

impl DownstreamStreamDropGuard {
    pub(crate) fn armed(
        observer: Option<LifecycleContext>,
        span: Span,
        initial_upstream_status: StatusCode,
        initial_upstream_error: Option<StreamTerminationCause>,
        relay_start: Instant,
        body_io_timing: BodyIoTiming,
        attribution: StreamTerminationAttribution,
    ) -> Self {
        Self {
            timing_observer: observer.clone(),
            observer,
            initial_upstream_status,
            span,
            pending_terminal: initial_upstream_error
                .map(|cause| (StreamTerminationOutcome::UpstreamError, cause)),
            semantic_completed: false,
            response_accounting_guard: None,
            abnormal_stop: None,
            abnormal_stop_recorded: false,
            finalized: false,
            relay_start,
            body_io_timing,
            attribution,
            keepalive_completion: None,
        }
    }

    pub(crate) fn with_keepalive_completion(
        mut self,
        keepalive: LifecycleKeepalive,
        context: Option<LifecycleKeepaliveContext>,
    ) -> Self {
        self.keepalive_completion =
            context.map(|context| StreamCompletionOwner::new(keepalive, context));
        self
    }

    pub(crate) fn has_keepalive_completion(&self) -> bool {
        self.keepalive_completion.is_some()
    }

    pub(crate) fn observe_keepalive(&mut self, event: &ParsedSseEvent<'_>) {
        if let Some(owner) = self.keepalive_completion.as_mut() {
            owner.observe(event);
        }
    }

    pub(crate) fn observe_keepalive_emitted(
        &mut self,
        bytes: &Bytes,
        max_pending_sse_bytes: usize,
    ) {
        if let Some(owner) = self.keepalive_completion.as_mut() {
            owner.observe_emitted(bytes, max_pending_sse_bytes);
        }
    }

    pub(crate) fn set_keepalive_anchor_at(&mut self, at: Instant) {
        if let Some(owner) = self.keepalive_completion.as_mut() {
            owner.set_cache_anchor_at(at);
        }
    }

    pub(crate) fn set_keepalive_cached_tokens(&mut self, cached_tokens: u64) {
        if let Some(owner) = self.keepalive_completion.as_mut() {
            owner.set_cached_tokens(cached_tokens);
        }
    }

    pub(crate) fn complete_keepalive(&mut self) {
        if self.pending_terminal.is_some()
            || self.abnormal_stop.is_some()
            || self.abnormal_stop_recorded
        {
            return;
        }
        if let Some(owner) = self.keepalive_completion.as_mut() {
            owner.complete();
        }
    }

    pub(crate) fn with_response_accounting_guard(
        mut self,
        guard: Option<ResponseAccountingGuard>,
    ) -> Self {
        self.response_accounting_guard = guard;
        self
    }

    pub(crate) fn handoff_response_accounting(&mut self) {
        if let Some(guard) = self.response_accounting_guard.take() {
            guard.forget();
        }
    }

    pub(crate) fn has_abnormal_stop(&self) -> bool {
        self.abnormal_stop.is_some()
    }

    pub(crate) fn mark_abnormal_stop(&mut self, error_type: String, error_message: String) {
        self.abnormal_stop
            .get_or_insert((error_type, error_message));
    }

    /// Apply only after the terminal outcome is known, so provider/proxy
    /// failures supersede a previously observed abnormal stop.
    pub(crate) fn record_abnormal_stop(&mut self, observer: &LifecycleContext) -> bool {
        if self.pending_terminal.is_some() {
            return false;
        }
        let Some((error_type, error_message)) = self.abnormal_stop.take() else {
            return false;
        };
        self.abnormal_stop_recorded = true;
        let code = if error_type == "refusal" {
            UpstreamErrorCode::Refusal
        } else {
            UpstreamErrorCode::ContextWindowExceeded
        };
        observer.emit_lifecycle(
            cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                event_id: observer.event_id().to_owned(),
                error_type,
                error_message,
                upstream_error_code: None,
                retry_after: None,
            },
        );
        observer.set_upstream_error(self.initial_upstream_status, code);
        true
    }

    pub(crate) fn mark_upstream_error(
        &mut self,
        cause: StreamTerminationCause,
        classification: Option<&StreamErrorClassification>,
    ) {
        self.mark_error(
            StreamTerminationOutcome::UpstreamError,
            cause,
            classification,
        );
    }

    pub(crate) fn mark_proxy_error(&mut self, cause: StreamTerminationCause) {
        self.mark_error(StreamTerminationOutcome::ProxyError, cause, None);
    }
    /// Record that the downstream has received the provider's semantic terminal
    /// event. A later downstream close is a successful completion, not a
    /// client cancellation, unless an upstream/proxy error is recorded first.
    pub(crate) fn mark_semantic_completion(&mut self) {
        self.semantic_completed = true;
    }

    pub(crate) fn detach_lifecycle_observer(&mut self) {
        self.observer = None;
    }

    fn mark_error(
        &mut self,
        outcome: StreamTerminationOutcome,
        cause: StreamTerminationCause,
        classification: Option<&StreamErrorClassification>,
    ) {
        let (_, effective_cause) = *self.pending_terminal.get_or_insert((outcome, cause));
        self.span.record("error.cause", effective_cause.as_str());
        if let Some(classification) = classification {
            self.span
                .record("error.chain", classification.redacted_chain.as_str());
            if let Some(io_kind) = classification.io_kind.as_deref() {
                self.span.record("error.io.kind", io_kind);
            }
            if let Some(io_os_error) = classification.io_os_error {
                self.span
                    .record("error.io.os_error", i64::from(io_os_error));
            }
            if let Some(h2_reason) = classification.h2_reason.as_deref() {
                self.span.record("error.h2.reason", h2_reason);
            }
        }
    }

    fn record_body_io_timings(&self) {
        if let Some(observer) = self.timing_observer.as_ref() {
            observer.set_io_timings(self.body_io_timing.snapshot());
        }
    }

    pub(crate) fn finish(&mut self, response_body_ms: u64, finalize_ms: u64) {
        if self.finalized {
            return;
        }
        self.record_body_io_timings();
        self.record_response_timings(response_body_ms, finalize_ms);
        match self.pending_terminal {
            Some((outcome, cause)) => self.record_terminal(outcome, cause),
            None => self.record_terminal(
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
        }
    }

    fn record_response_timings(&self, response_body_ms: u64, finalize_ms: u64) {
        self.span.record(
            "cc_lb.response_body_ms",
            numeric_attribute(Some(response_body_ms)).as_value(),
        );
        self.span.record(
            "cc_lb.request.finalize_ms",
            numeric_attribute(Some(finalize_ms)).as_value(),
        );
    }

    fn record_terminal(
        &mut self,
        outcome: StreamTerminationOutcome,
        cause: StreamTerminationCause,
    ) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        let response_status = match outcome {
            StreamTerminationOutcome::ClientCancelled => CLIENT_CLOSED_STATUS,
            StreamTerminationOutcome::Completed
            | StreamTerminationOutcome::UpstreamError
            | StreamTerminationOutcome::ProxyError => self.initial_upstream_status.as_u16(),
        };
        self.span
            .record("http.response.status_code", i64::from(response_status));
        self.span.record("stream.outcome", outcome.as_str());
        self.span.record("error.cause", cause.as_str());
        if matches!(
            outcome,
            StreamTerminationOutcome::UpstreamError | StreamTerminationOutcome::ProxyError
        ) {
            self.span.record("otel.status_code", "ERROR");
        }
        // `finalized` makes this the only emission for the guard, so the
        // attribution strings move into the label set instead of being cloned.
        record_stream_termination_metric(
            outcome,
            cause,
            &std::mem::take(&mut self.attribution.upstream),
            self.attribution.model.take().as_deref(),
        );
    }
}

impl Drop for DownstreamStreamDropGuard {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        self.record_body_io_timings();
        let cancellation_at = Instant::now();
        let response_body_ms =
            duration_to_ms(cancellation_at.saturating_duration_since(self.relay_start));
        if let Some((outcome, cause)) = self.pending_terminal {
            let finalize_ms = duration_to_ms(cancellation_at.elapsed());
            if let Some(observer) = self.observer.as_ref() {
                observer.set_upstream_body_ms_if_absent(response_body_ms);
                observer.set_finalize_ms(finalize_ms);
                if outcome == StreamTerminationOutcome::Completed {
                    observer.finish();
                }
            }
            self.record_response_timings(response_body_ms, finalize_ms);
            self.record_terminal(outcome, cause);
            return;
        }
        if self.semantic_completed {
            // Leave the reservation alive before RequestTerminated is
            // published; the subscriber reconciles success or refunds an
            // abnormal stop. Pre-terminal cancellation retains RAII refund.
            let complete_keepalive = self.abnormal_stop.is_none() && !self.abnormal_stop_recorded;
            self.handoff_response_accounting();
            let finalize_ms = duration_to_ms(cancellation_at.elapsed());
            if let Some(observer) = self.observer.take() {
                self.record_abnormal_stop(&observer);
                observer.set_upstream_body_ms_if_absent(response_body_ms);
                observer.set_finalize_ms(finalize_ms);
                observer.finish();
            }
            if complete_keepalive {
                self.complete_keepalive();
            }
            self.record_response_timings(response_body_ms, finalize_ms);
            self.record_terminal(
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            );
            return;
        }
        if let Some(observer) = self.observer.take()
            && let Ok(status) = StatusCode::from_u16(CLIENT_CLOSED_STATUS)
        {
            observer.set_upstream_body_ms_if_absent(response_body_ms);
            observer.set_client_closed(status);
            self.record_terminal(
                StreamTerminationOutcome::ClientCancelled,
                StreamTerminationCause::None,
            );
            let finalize_ms = duration_to_ms(cancellation_at.elapsed());
            observer.set_finalize_ms(finalize_ms);
            self.record_response_timings(response_body_ms, finalize_ms);
            return;
        }
        self.record_terminal(
            StreamTerminationOutcome::ClientCancelled,
            StreamTerminationCause::None,
        );
        let finalize_ms = duration_to_ms(cancellation_at.elapsed());
        self.record_response_timings(response_body_ms, finalize_ms);
    }
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::body_io_timing::BodyIoPhase;
    use crate::terminal_observer::{InternalFailure, UpstreamErrorCode, error_codes};
    use cc_lb_clock::{ClockHandle, SystemClock};
    use cc_lb_control::RequestEventBus;
    use cc_lb_control::event_bus::InMemoryBus;
    use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage};
    use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};
    use tracing_subscriber::{Layer, Registry};

    use super::*;

    #[derive(Clone, Default)]
    struct ResponseTimingLayer {
        span_ids: Arc<Mutex<Vec<Id>>>,
        values: Arc<Mutex<HashMap<String, Vec<u64>>>>,
    }

    impl<S> Layer<S> for ResponseTimingLayer
    where
        S: Subscriber,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: LayerContext<'_, S>) {
            if attrs.metadata().name() == "proxy.response_stream" {
                self.span_ids
                    .lock()
                    .expect("response timing span ids lock")
                    .push(id.clone());
                attrs.record(&mut ResponseTimingVisitor {
                    values: &self.values,
                });
            }
        }

        fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: LayerContext<'_, S>) {
            if self
                .span_ids
                .lock()
                .expect("response timing span ids lock")
                .contains(id)
            {
                values.record(&mut ResponseTimingVisitor {
                    values: &self.values,
                });
            }
        }
    }

    type RecordedResponseTimingValues = Arc<Mutex<HashMap<String, Vec<u64>>>>;

    struct ResponseTimingVisitor<'a> {
        values: &'a Arc<Mutex<HashMap<String, Vec<u64>>>>,
    }

    impl Visit for ResponseTimingVisitor<'_> {
        fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.record_u64(
                field,
                u64::try_from(value).expect("nonnegative response timing"),
            );
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            if matches!(
                field.name(),
                "http.response.status_code"
                    | "cc_lb.response_body_ms"
                    | "cc_lb.request.finalize_ms"
            ) {
                self.values
                    .lock()
                    .expect("response timing values lock")
                    .entry(field.name().to_owned())
                    .or_default()
                    .push(value);
            }
        }
    }

    fn response_stream_span() -> (Span, RecordedResponseTimingValues) {
        let layer = ResponseTimingLayer::default();
        let values = Arc::clone(&layer.values);
        let subscriber = Registry::default().with(layer);
        let span = tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!(
                "proxy.response_stream",
                http.response.status_code = tracing::field::Empty,
                cc_lb.response_body_ms = tracing::field::Empty,
                cc_lb.request.finalize_ms = tracing::field::Empty,
            )
        });
        (span, values)
    }

    fn counter_sample(
        exercise: impl FnOnce(&mut DownstreamStreamDropGuard),
    ) -> (HashMap<String, String>, u64) {
        counter_sample_with_span(Span::none(), exercise)
    }

    fn counter_sample_with_span(
        span: Span,
        exercise: impl FnOnce(&mut DownstreamStreamDropGuard),
    ) -> (HashMap<String, String>, u64) {
        counter_sample_with_status_and_span(StatusCode::OK, span, exercise)
    }

    fn test_attribution() -> StreamTerminationAttribution {
        StreamTerminationAttribution::new("upstream-a", Some("claude-test"))
    }

    fn counter_sample_with_status_and_span(
        initial_upstream_status: StatusCode,
        span: Span,
        exercise: impl FnOnce(&mut DownstreamStreamDropGuard),
    ) -> (HashMap<String, String>, u64) {
        counter_sample_with_attribution(initial_upstream_status, span, test_attribution(), exercise)
    }

    fn counter_sample_with_attribution(
        initial_upstream_status: StatusCode,
        span: Span,
        attribution: StreamTerminationAttribution,
        exercise: impl FnOnce(&mut DownstreamStreamDropGuard),
    ) -> (HashMap<String, String>, u64) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut guard = DownstreamStreamDropGuard::armed(
                None,
                span,
                initial_upstream_status,
                None,
                Instant::now(),
                BodyIoTiming::default(),
                attribution,
            );
            exercise(&mut guard);
            drop(guard);
        });

        let samples = snapshotter.snapshot().into_vec();
        assert_eq!(samples.len(), 1, "one terminal metric series expected");
        let (key, _, _, value) = samples.into_iter().next().expect("metric sample");
        assert_eq!(key.key().name(), "cc_lb_stream_terminations_total");
        let labels: HashMap<String, String> = key
            .key()
            .labels()
            .map(|label| (label.key().to_owned(), label.value().to_owned()))
            .collect();
        let mut label_keys = labels.keys().map(String::as_str).collect::<Vec<_>>();
        label_keys.sort_unstable();
        assert_eq!(label_keys, ["cause", "model", "outcome", "upstream"]);
        let DebugValue::Counter(value) = value else {
            panic!("stream termination metric must be a counter");
        };
        (labels, value)
    }

    #[test]
    fn termination_carries_routed_upstream_and_requested_model() {
        let (labels, value) = counter_sample(|guard| guard.finish(1, 1));

        assert_eq!(
            labels.get("upstream").map(String::as_str),
            Some("upstream-a")
        );
        assert_eq!(labels.get("model").map(String::as_str), Some("claude-test"));
        assert_eq!(value, 1);
    }

    #[test]
    fn termination_without_requested_model_uses_unresolved_label() {
        let (labels, value) = counter_sample_with_attribution(
            StatusCode::OK,
            Span::none(),
            StreamTerminationAttribution::new("upstream-a", None),
            |guard| guard.finish(1, 1),
        );

        assert_eq!(
            labels.get("upstream").map(String::as_str),
            Some("upstream-a")
        );
        assert_eq!(labels.get("model").map(String::as_str), Some("none"));
        assert_eq!(value, 1);
    }

    fn recorded_once(values: &HashMap<String, Vec<u64>>, field: &str) -> Option<u64> {
        let recorded = values.get(field)?;
        assert_eq!(recorded.len(), 1, "{field} must be recorded exactly once");
        recorded.first().copied()
    }

    #[test]
    fn finish_and_drop_increment_completed_once() {
        let (span, timings) = response_stream_span();
        let (labels, value) = counter_sample_with_span(span, |guard| {
            guard.finish(31, 7);
            guard.finish(99, 88);
        });

        assert_eq!(labels.get("outcome").map(String::as_str), Some("completed"));
        assert_eq!(labels.get("cause").map(String::as_str), Some("none"));
        assert_eq!(value, 1);
        let timings = timings.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&timings, "http.response.status_code"),
            Some(u64::from(StatusCode::OK.as_u16()))
        );
        assert_eq!(recorded_once(&timings, "cc_lb.response_body_ms"), Some(31));
        assert_eq!(
            recorded_once(&timings, "cc_lb.request.finalize_ms"),
            Some(7)
        );
    }

    #[test]
    fn marked_error_dropped_before_eos_is_not_client_cancelled() {
        let (span, values) = response_stream_span();
        let (labels, value) =
            counter_sample_with_status_and_span(StatusCode::BAD_GATEWAY, span, |guard| {
                guard.mark_upstream_error(StreamTerminationCause::H2Reset, None);
            });

        assert_eq!(
            labels.get("outcome").map(String::as_str),
            Some("upstream_error")
        );
        assert_eq!(labels.get("cause").map(String::as_str), Some("h2_reset"));
        assert_eq!(value, 1);
        let values = values.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&values, "http.response.status_code"),
            Some(u64::from(StatusCode::BAD_GATEWAY.as_u16()))
        );
        assert!(recorded_once(&values, "cc_lb.response_body_ms").is_some());
        assert!(recorded_once(&values, "cc_lb.request.finalize_ms").is_some());
    }

    #[tokio::test]
    async fn pending_error_drop_preserves_partial_body_and_finalize_timing() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "pending-error-drop".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        observer.set_upstream_error(StatusCode::BAD_GATEWAY, UpstreamErrorCode::Upstream5xx);
        let relay_start = Instant::now()
            .checked_sub(Duration::from_millis(25))
            .expect("relay start before error drop");
        let (span, span_timings) = response_stream_span();
        let mut guard = DownstreamStreamDropGuard::armed(
            Some(observer),
            span,
            StatusCode::BAD_GATEWAY,
            None,
            relay_start,
            BodyIoTiming::default(),
            test_attribution(),
        );
        guard.mark_upstream_error(StreamTerminationCause::H2Reset, None);

        drop(guard);

        let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            upstream_body_ms,
            finalize_ms,
            ..
        } = rx.recv().await.expect("terminal event delivered")
        else {
            panic!("expected request termination");
        };
        assert_eq!(
            reason,
            TerminationReason::ErrorCode(error_codes::UPSTREAM_5XX.to_owned())
        );
        assert_eq!(client_status, StatusCode::BAD_GATEWAY.as_u16());
        assert!(upstream_body_ms.is_some_and(|elapsed| elapsed >= 25));
        assert!(finalize_ms.is_some());
        let span_timings = span_timings.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&span_timings, "cc_lb.response_body_ms"),
            upstream_body_ms
        );
        assert_eq!(
            recorded_once(&span_timings, "cc_lb.request.finalize_ms"),
            finalize_ms
        );
    }

    #[test]
    fn proxy_error_uses_distinct_outcome_and_cause() {
        let (span, values) = response_stream_span();
        let (labels, value) = counter_sample_with_span(span, |guard| {
            guard.mark_proxy_error(StreamTerminationCause::AffinityError);
        });

        assert_eq!(
            labels.get("outcome").map(String::as_str),
            Some("proxy_error")
        );
        assert_eq!(
            labels.get("cause").map(String::as_str),
            Some("affinity_error")
        );
        assert_eq!(value, 1);
        let values = values.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&values, "http.response.status_code"),
            Some(u64::from(StatusCode::OK.as_u16()))
        );
    }

    #[test]
    fn detached_observer_drop_remains_client_cancelled_metric() {
        let (span, values) = response_stream_span();
        let (labels, value) = counter_sample_with_span(span, |guard| {
            guard.detach_lifecycle_observer();
        });

        assert_eq!(
            labels.get("outcome").map(String::as_str),
            Some("client_cancelled")
        );
        assert_eq!(labels.get("cause").map(String::as_str), Some("none"));
        assert_eq!(value, 1);
        let values = values.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&values, "http.response.status_code"),
            Some(u64::from(CLIENT_CLOSED_STATUS))
        );
    }

    #[test]
    fn unmarked_drop_is_client_cancelled_once() {
        let (labels, value) = counter_sample(|_| {});

        assert_eq!(
            labels.get("outcome").map(String::as_str),
            Some("client_cancelled")
        );
        assert_eq!(labels.get("cause").map(String::as_str), Some("none"));
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn client_cancelled_drop_preserves_partial_body_and_existing_timings() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "client-cancelled-drop".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        observer.set_request_body_timing(11, Some(123));
        observer.set_termination_timings(Some(17), None, None);
        let relay_start = Instant::now()
            .checked_sub(Duration::from_millis(25))
            .expect("relay start before cancellation");
        let (span, span_timings) = response_stream_span();
        let body_io_timing = BodyIoTiming::default();
        body_io_timing.transition(Some(BodyIoPhase::Process));
        let guard = DownstreamStreamDropGuard::armed(
            Some(observer),
            span,
            StatusCode::OK,
            None,
            relay_start,
            body_io_timing,
            test_attribution(),
        );

        drop(guard);

        let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            request_body_read_ms,
            request_body_bytes,
            proxy_setup_ms,
            upstream_body_ms,
            finalize_ms,
            io_timings,
            ..
        } = rx.recv().await.expect("terminal event delivered")
        else {
            panic!("expected request termination");
        };
        assert_eq!(
            reason,
            TerminationReason::ErrorCode(error_codes::CLIENT_CLOSED_REQUEST.to_owned())
        );
        assert_eq!(client_status, CLIENT_CLOSED_STATUS);
        assert_eq!(request_body_read_ms, Some(11));
        assert_eq!(request_body_bytes, Some(123));
        assert_eq!(proxy_setup_ms, Some(17));
        assert!(upstream_body_ms.is_some_and(|elapsed| elapsed >= 25));
        assert!(finalize_ms.is_some());
        assert!(io_timings.response_body_process_ms.is_some());
        assert_eq!(io_timings.response_body_wait_ms, None);
        assert_eq!(io_timings.response_body_downstream_poll_gap_ms, None);
        let span_timings = span_timings.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&span_timings, "http.response.status_code"),
            Some(u64::from(CLIENT_CLOSED_STATUS))
        );
        assert_eq!(
            recorded_once(&span_timings, "cc_lb.response_body_ms"),
            upstream_body_ms
        );
        assert_eq!(
            recorded_once(&span_timings, "cc_lb.request.finalize_ms"),
            finalize_ms
        );
        assert!(
            rx.try_recv().is_err(),
            "client-cancelled drop must not emit StreamCompleted"
        );
    }

    #[tokio::test]
    async fn tower_timeout_overrides_provisional_client_cancellation_after_guard_drop() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "tower-timeout-after-client-cancellation".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        observer.set_request_body_timing(13, Some(456));
        observer.set_termination_timings(Some(19), None, None);
        let relay_start = Instant::now()
            .checked_sub(Duration::from_millis(25))
            .expect("relay start before cancellation");
        let (span, span_timings) = response_stream_span();
        let guard = DownstreamStreamDropGuard::armed(
            Some(observer.clone()),
            span,
            StatusCode::OK,
            None,
            relay_start,
            BodyIoTiming::default(),
            test_attribution(),
        );

        drop(guard);

        assert!(
            rx.try_recv().is_err(),
            "guard drop must leave finalization to the remaining lifecycle observer"
        );
        observer.terminate_failure(InternalFailure::tower_timeout(InternalError {
            stage: InternalErrorStage::Relay,
            kind: InternalErrorKind::Timeout,
            message: Some("request timed out".to_owned()),
        }));

        // `terminate_failure` emits the otherwise-missing `RequestStarted` ahead of the
        // terminal event so the assembler has a partial to attach to; this test
        // asserts on the termination's fields, so skip past anything earlier.
        let terminal = loop {
            let event = rx.recv().await.expect("terminal event delivered");
            if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
                break event;
            }
        };
        let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            request_body_read_ms,
            request_body_bytes,
            proxy_setup_ms,
            upstream_body_ms,
            finalize_ms,
            ..
        } = terminal
        else {
            panic!("expected request termination");
        };
        assert_eq!(
            reason,
            TerminationReason::ErrorCode(error_codes::TOWER_TIMEOUT.to_owned())
        );
        assert_eq!(client_status, StatusCode::GATEWAY_TIMEOUT.as_u16());
        assert_eq!(request_body_read_ms, Some(13));
        assert_eq!(request_body_bytes, Some(456));
        assert_eq!(proxy_setup_ms, Some(19));
        assert!(upstream_body_ms.is_some_and(|elapsed| elapsed >= 25));
        assert!(finalize_ms.is_some());
        let span_timings = span_timings.lock().expect("response timing values lock");
        assert_eq!(
            recorded_once(&span_timings, "http.response.status_code"),
            Some(u64::from(CLIENT_CLOSED_STATUS))
        );
        assert_eq!(
            recorded_once(&span_timings, "cc_lb.response_body_ms"),
            upstream_body_ms
        );
        assert_eq!(
            recorded_once(&span_timings, "cc_lb.request.finalize_ms"),
            finalize_ms
        );
    }

    #[tokio::test]
    async fn detached_observer_is_not_reclassified_on_post_eos_drop() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "post-eos-drop".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        let body_io_timing = BodyIoTiming::default();
        body_io_timing.transition(Some(BodyIoPhase::Wait));
        let mut guard = DownstreamStreamDropGuard::armed(
            Some(observer.clone()),
            Span::none(),
            StatusCode::OK,
            None,
            Instant::now(),
            body_io_timing,
            test_attribution(),
        );
        guard.detach_lifecycle_observer();
        drop(guard);
        observer.set_success_status(StatusCode::OK);
        observer.finish();

        let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            io_timings,
            ..
        } = rx.recv().await.expect("terminal event delivered")
        else {
            panic!("expected request termination");
        };
        assert_eq!(reason, TerminationReason::Success);
        assert_eq!(client_status, StatusCode::OK.as_u16());
        assert!(io_timings.response_body_wait_ms.is_some());
        assert_eq!(io_timings.response_body_process_ms, None);
        assert_eq!(io_timings.response_body_downstream_poll_gap_ms, None);
    }
}
