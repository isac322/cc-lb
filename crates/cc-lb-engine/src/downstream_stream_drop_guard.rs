use http::StatusCode;
use tracing::Span;

use crate::terminal_observer::{
    LifecycleContext, StreamErrorClassification, StreamTerminationCause, StreamTerminationOutcome,
    error_codes,
};

const CLIENT_CLOSED_STATUS: u16 = 499;

pub(crate) struct DownstreamStreamDropGuard {
    observer: Option<LifecycleContext>,
    span: Span,
    pending_terminal: Option<(StreamTerminationOutcome, StreamTerminationCause)>,
    finalized: bool,
}

impl DownstreamStreamDropGuard {
    pub(crate) fn armed(
        observer: Option<LifecycleContext>,
        span: Span,
        initial_upstream_error: Option<StreamTerminationCause>,
    ) -> Self {
        Self {
            observer,
            span,
            pending_terminal: initial_upstream_error
                .map(|cause| (StreamTerminationOutcome::UpstreamError, cause)),
            finalized: false,
        }
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

    pub(crate) fn finish(&mut self) {
        match self.pending_terminal {
            Some((outcome, cause)) => self.record_terminal(outcome, cause),
            None => self.record_terminal(
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
        }
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
        self.span.record("stream.outcome", outcome.as_str());
        self.span.record("error.cause", cause.as_str());
        if matches!(
            outcome,
            StreamTerminationOutcome::UpstreamError | StreamTerminationOutcome::ProxyError
        ) {
            self.span.record("otel.status_code", "ERROR");
        }
        metrics::counter!(
            "cc_lb_stream_terminations_total",
            "outcome" => outcome.as_str(),
            "cause" => cause.as_str()
        )
        .increment(1);
        tracing::info!(
            parent: &self.span,
            outcome = outcome.as_str(),
            cause = cause.as_str(),
            "response_stream_terminated"
        );
    }
}

impl Drop for DownstreamStreamDropGuard {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        if let Some((outcome, cause)) = self.pending_terminal {
            self.record_terminal(outcome, cause);
            return;
        }
        if let Some(observer) = self.observer.take()
            && let Ok(status) = StatusCode::from_u16(CLIENT_CLOSED_STATUS)
        {
            observer.set_terminal(status, error_codes::CLIENT_CLOSED_REQUEST);
        }
        self.record_terminal(
            StreamTerminationOutcome::ClientCancelled,
            StreamTerminationCause::None,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cc_lb_control::{LifecycleBusReceiver, RequestEventBus};
    use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use crate::clock::{ClockHandle, SystemClock};
    use crate::event_bus::InMemoryBus;

    use super::*;

    fn counter_sample(
        exercise: impl FnOnce(&mut DownstreamStreamDropGuard),
    ) -> (HashMap<String, String>, u64) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut guard = DownstreamStreamDropGuard::armed(None, Span::none(), None);
            exercise(&mut guard);
            drop(guard);
        });

        let samples = snapshotter.snapshot().into_vec();
        assert_eq!(samples.len(), 1, "one terminal metric series expected");
        let (key, _, _, value) = samples.into_iter().next().expect("metric sample");
        assert_eq!(key.key().name(), "cc_lb_stream_terminations_total");
        let labels = key
            .key()
            .labels()
            .map(|label| (label.key().to_owned(), label.value().to_owned()))
            .collect();
        let DebugValue::Counter(value) = value else {
            panic!("stream termination metric must be a counter");
        };
        (labels, value)
    }

    #[test]
    fn finish_and_drop_increment_completed_once() {
        let (labels, value) = counter_sample(|guard| {
            guard.finish();
            guard.finish();
        });

        assert_eq!(labels.get("outcome").map(String::as_str), Some("completed"));
        assert_eq!(labels.get("cause").map(String::as_str), Some("none"));
        assert_eq!(value, 1);
    }

    #[test]
    fn marked_error_dropped_before_eos_is_not_client_cancelled() {
        let (labels, value) = counter_sample(|guard| {
            guard.mark_upstream_error(StreamTerminationCause::H2Reset, None);
        });

        assert_eq!(
            labels.get("outcome").map(String::as_str),
            Some("upstream_error")
        );
        assert_eq!(labels.get("cause").map(String::as_str), Some("h2_reset"));
        assert_eq!(value, 1);
    }

    #[test]
    fn proxy_error_uses_distinct_outcome_and_cause() {
        let (labels, value) = counter_sample(|guard| {
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
    }

    #[test]
    fn detached_observer_drop_remains_client_cancelled_metric() {
        let (labels, value) = counter_sample(DownstreamStreamDropGuard::detach_lifecycle_observer);

        assert_eq!(
            labels.get("outcome").map(String::as_str),
            Some("client_cancelled")
        );
        assert_eq!(labels.get("cause").map(String::as_str), Some("none"));
        assert_eq!(value, 1);
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
    async fn detached_observer_is_not_reclassified_on_post_eos_drop() {
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut rx) = bus.subscribe_lifecycle() else {
            panic!("expected in-memory lifecycle receiver");
        };
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "post-eos-drop".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        let mut guard =
            DownstreamStreamDropGuard::armed(Some(observer.clone()), Span::none(), None);
        guard.detach_lifecycle_observer();
        drop(guard);
        observer.set_success_status(StatusCode::OK);
        observer.finish();

        let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            ..
        } = rx.recv().await.expect("terminal event delivered")
        else {
            panic!("expected request termination");
        };
        assert_eq!(reason, TerminationReason::Success);
        assert_eq!(client_status, StatusCode::OK.as_u16());
    }
}
