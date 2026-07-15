use http::StatusCode;

use crate::terminal_observer::{LifecycleContext, error_codes};

const CLIENT_CLOSED_STATUS: u16 = 499;

pub(crate) struct DownstreamStreamDropGuard {
    observer: Option<LifecycleContext>,
}

impl DownstreamStreamDropGuard {
    pub(crate) fn armed(observer: Option<LifecycleContext>) -> Self {
        Self { observer }
    }

    pub(crate) const fn disarmed() -> Self {
        Self { observer: None }
    }

    pub(crate) fn disarm(&mut self) {
        self.observer = None;
    }
}

impl Drop for DownstreamStreamDropGuard {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take()
            && let Ok(status) = StatusCode::from_u16(CLIENT_CLOSED_STATUS)
        {
            observer.set_terminal(status, error_codes::CLIENT_CLOSED_REQUEST);
            observer.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_control::{InMemoryBus, LifecycleBusReceiver, RequestEventBus};
    use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};

    use crate::clock::{ClockHandle, SystemClock};
    use crate::downstream_stream_drop_guard::DownstreamStreamDropGuard;
    use crate::terminal_observer::{LifecycleContext, error_codes};

    #[test]
    fn stream_drop_emits_client_closed_terminal_event_immediately() {
        // Given: a streaming response guard owns a clone while another observer
        // clone remains alive in the request task.
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut rx) = bus.subscribe_lifecycle() else {
            panic!("expected in-memory lifecycle receiver");
        };
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_client_closed".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        let expected_event_id = observer.event_id().to_owned();

        // When: the downstream response stream is dropped by a client disconnect.
        drop(DownstreamStreamDropGuard::armed(Some(observer.clone())));

        // Then: the terminal event is emitted without waiting for the last
        // LifecycleContext clone to drop during process teardown.
        let event = rx.try_recv().expect("client disconnect terminal event");
        let LifecycleEvent::RequestTerminated {
            event_id,
            reason,
            client_status,
            ..
        } = event
        else {
            panic!("expected RequestTerminated event");
        };
        assert_eq!(event_id, expected_event_id);
        assert_eq!(client_status, 499);
        assert!(
            matches!(reason, TerminationReason::ErrorCode(ref code) if code == error_codes::CLIENT_CLOSED_REQUEST)
        );

        drop(observer);
        assert!(
            rx.try_recv().is_err(),
            "final observer drop must not emit a duplicate terminal event",
        );
    }
}
