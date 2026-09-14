use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use cc_lb_lifecycle::RequestIoTimings;
use futures_core::Stream;
use http_body::Body;
use hyper::body::Frame;

use crate::terminal_observer::LifecycleContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyIoPhase {
    Wait,
    Process,
    DownstreamPollGap,
}

#[derive(Clone, Default)]
pub(crate) struct BodyIoTiming {
    inner: Arc<Mutex<BodyIoTimingState>>,
}

#[derive(Default)]
struct BodyIoTimingState {
    wait: PhaseTotal,
    process: PhaseTotal,
    downstream_poll_gap: PhaseTotal,
    active: Option<(BodyIoPhase, Instant)>,
    stopped: bool,
}

#[derive(Clone, Copy, Default)]
struct PhaseTotal {
    elapsed: Duration,
    started: bool,
}

impl BodyIoTiming {
    pub(crate) fn transition(&self, phase: Option<BodyIoPhase>) {
        self.transition_at(phase, Instant::now());
    }

    pub(crate) fn stop(&self) {
        self.transition(None);
    }

    pub(crate) fn snapshot(&self) -> RequestIoTimings {
        self.snapshot_at(Instant::now())
    }

    fn transition_at(&self, phase: Option<BodyIoPhase>, now: Instant) {
        let mut state = self.lock_state();
        if state.stopped {
            return;
        }
        state.accrue_active(now);
        if let Some(phase) = phase {
            state.total_mut(phase).started = true;
            state.active = Some((phase, now));
        } else {
            state.stopped = true;
        }
    }

    #[cfg(test)]
    fn stop_at(&self, now: Instant) {
        self.transition_at(None, now);
    }

    fn snapshot_at(&self, now: Instant) -> RequestIoTimings {
        let state = self.lock_state();
        let mut wait = state.wait;
        let mut process = state.process;
        let mut downstream_poll_gap = state.downstream_poll_gap;
        if let Some((phase, started_at)) = state.active {
            let elapsed = now.saturating_duration_since(started_at);
            match phase {
                BodyIoPhase::Wait => wait.elapsed = wait.elapsed.saturating_add(elapsed),
                BodyIoPhase::Process => {
                    process.elapsed = process.elapsed.saturating_add(elapsed);
                }
                BodyIoPhase::DownstreamPollGap => {
                    downstream_poll_gap.elapsed =
                        downstream_poll_gap.elapsed.saturating_add(elapsed);
                }
            }
        }
        RequestIoTimings {
            response_body_wait_ms: phase_ms(wait),
            response_body_process_ms: phase_ms(process),
            response_body_downstream_poll_gap_ms: phase_ms(downstream_poll_gap),
            ..Default::default()
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, BodyIoTimingState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl BodyIoTimingState {
    fn accrue_active(&mut self, now: Instant) {
        let Some((phase, started_at)) = self.active.take() else {
            return;
        };
        let elapsed = now.saturating_duration_since(started_at);
        let total = self.total_mut(phase);
        total.elapsed = total.elapsed.saturating_add(elapsed);
    }

    fn total_mut(&mut self, phase: BodyIoPhase) -> &mut PhaseTotal {
        match phase {
            BodyIoPhase::Wait => &mut self.wait,
            BodyIoPhase::Process => &mut self.process,
            BodyIoPhase::DownstreamPollGap => &mut self.downstream_poll_gap,
        }
    }
}

fn phase_ms(total: PhaseTotal) -> Option<f64> {
    total
        .started
        .then_some(total.elapsed.as_secs_f64() * 1_000.0)
}

pub(crate) async fn timed_body_frame<B>(
    body: &mut B,
    timing: &BodyIoTiming,
) -> Option<Result<Frame<B::Data>, B::Error>>
where
    B: Body + Unpin,
{
    poll_fn(|cx| {
        timing.transition(Some(BodyIoPhase::Wait));
        match Pin::new(&mut *body).poll_frame(cx) {
            Poll::Ready(frame) => {
                timing.transition(Some(BodyIoPhase::Process));
                Poll::Ready(frame)
            }
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

pub(crate) struct TimedResponseStream<S> {
    inner: Pin<Box<S>>,
    timing: BodyIoTiming,
}

impl<S> TimedResponseStream<S> {
    pub(crate) fn new(inner: S, timing: BodyIoTiming) -> Self {
        Self {
            inner: Box::pin(inner),
            timing,
        }
    }
}

impl<S> Stream for TimedResponseStream<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.timing.transition(Some(BodyIoPhase::Process));
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.timing.transition(Some(BodyIoPhase::DownstreamPollGap));
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                this.timing.stop();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub(crate) struct BodyIoTimingObserverGuard {
    observer: Option<LifecycleContext>,
    timing: BodyIoTiming,
}

impl BodyIoTimingObserverGuard {
    pub(crate) fn new(observer: Option<LifecycleContext>, timing: BodyIoTiming) -> Self {
        Self { observer, timing }
    }

    pub(crate) fn record(&mut self) {
        if let Some(observer) = self.observer.take() {
            observer.set_io_timings(self.timing.snapshot());
        }
    }
}

impl Drop for BodyIoTimingObserverGuard {
    fn drop(&mut self) {
        self.record();
    }
}

#[cfg(test)]
mod tests {
    use super::{BodyIoPhase, BodyIoTiming, TimedResponseStream};
    use futures_core::Stream;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn phases_are_mutually_exclusive_and_snapshot_includes_active_time() {
        let timing = BodyIoTiming::default();
        let start = Instant::now().into_std();

        timing.transition_at(Some(BodyIoPhase::Wait), start);
        timing.transition_at(Some(BodyIoPhase::Process), start + Duration::from_millis(7));
        timing.transition_at(
            Some(BodyIoPhase::DownstreamPollGap),
            start + Duration::from_millis(12),
        );
        let snapshot = timing.snapshot_at(start + Duration::from_millis(23));

        assert_eq!(snapshot.response_body_wait_ms, Some(7.0));
        assert_eq!(snapshot.response_body_process_ms, Some(5.0));
        assert_eq!(snapshot.response_body_downstream_poll_gap_ms, Some(11.0));
    }

    #[tokio::test(start_paused = true)]
    async fn active_wait_is_preserved_without_terminal_transition() {
        let timing = BodyIoTiming::default();
        let start = Instant::now().into_std();
        timing.transition_at(Some(BodyIoPhase::Wait), start);

        let first = timing.snapshot_at(start + Duration::from_millis(13));
        let later = timing.snapshot_at(start + Duration::from_millis(19));

        assert_eq!(first.response_body_wait_ms, Some(13.0));
        assert_eq!(later.response_body_wait_ms, Some(19.0));
        assert_eq!(later.response_body_process_ms, None);
        assert_eq!(later.response_body_downstream_poll_gap_ms, None);
    }

    #[tokio::test(start_paused = true)]
    async fn entered_zero_duration_is_distinct_from_unmeasured() {
        let timing = BodyIoTiming::default();
        let start = Instant::now().into_std();
        timing.transition_at(Some(BodyIoPhase::Process), start);
        timing.stop_at(start);

        let snapshot = timing.snapshot_at(start);

        assert_eq!(snapshot.response_body_wait_ms, None);
        assert_eq!(snapshot.response_body_process_ms, Some(0.0));
        assert_eq!(snapshot.response_body_downstream_poll_gap_ms, None);
    }

    struct OneItemStream(bool);

    impl Stream for OneItemStream {
        type Item = ();

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let item = self.0.then_some(());
            self.0 = false;
            Poll::Ready(item)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stop_freezes_timing_across_late_wrapper_polls_and_phase_transitions() {
        let timing = BodyIoTiming::default();
        let start = Instant::now().into_std();
        timing.transition_at(Some(BodyIoPhase::Process), start);
        timing.transition_at(Some(BodyIoPhase::Wait), start + Duration::from_millis(4));
        timing.stop_at(start + Duration::from_millis(11));
        let stopped = timing.snapshot_at(start + Duration::from_millis(11));

        timing.transition_at(
            Some(BodyIoPhase::Process),
            start + Duration::from_millis(20),
        );
        timing.transition_at(
            Some(BodyIoPhase::DownstreamPollGap),
            start + Duration::from_millis(30),
        );
        let mut stream = Box::pin(TimedResponseStream::new(
            OneItemStream(true),
            timing.clone(),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(())));
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(None));
        timing.stop_at(start + Duration::from_millis(40));

        assert_eq!(
            timing.snapshot_at(start + Duration::from_millis(100)),
            stopped
        );
        assert_eq!(stopped.response_body_process_ms, Some(4.0));
        assert_eq!(stopped.response_body_wait_ms, Some(7.0));
        assert_eq!(stopped.response_body_downstream_poll_gap_ms, None);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_cancellation_is_accrued_before_stop() {
        let timing = BodyIoTiming::default();
        let start = Instant::now().into_std();
        timing.transition_at(Some(BodyIoPhase::Process), start);
        timing.transition_at(Some(BodyIoPhase::Wait), start + Duration::from_millis(2));

        timing.stop_at(start + Duration::from_millis(11));
        let snapshot = timing.snapshot_at(start + Duration::from_millis(100));

        assert_eq!(snapshot.response_body_process_ms, Some(2.0));
        assert_eq!(snapshot.response_body_wait_ms, Some(9.0));
        assert_eq!(snapshot.response_body_downstream_poll_gap_ms, None);
    }
}
