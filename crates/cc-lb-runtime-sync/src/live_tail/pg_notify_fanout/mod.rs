mod fanout;
#[cfg(feature = "live-tail-pg")]
mod listener;
mod metrics;
#[cfg(feature = "live-tail-pg")]
mod notifier;
#[cfg(feature = "live-tail-pg")]
mod protocol;

pub const DEFAULT_PG_NOTIFY_CHANNEL: &str = "cc_lb_events_partial";
pub const PARTIAL_NOTIFY_MPSC_CAPACITY: usize = 1024;

pub use fanout::PgNotifyFanout;
#[cfg(feature = "live-tail-pg")]
pub use listener::PgListener;
#[cfg(feature = "live-tail-pg")]
pub use notifier::PgNotifier;

#[cfg(test)]
mod tests {
    use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};
    use tokio::sync::mpsc;

    use super::*;
    use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus};

    #[tokio::test]
    async fn pg_notify_fanout_drops_when_notifier_queue_full_without_blocking() {
        let local_bus = InMemoryBus::with_capacity(16);
        let (tx, mut rx) = mpsc::channel(1024);
        let fanout = PgNotifyFanout::new(local_bus, tx);

        for i in 0..5000 {
            fanout.publish(RequestEventUpdate::Partial(RequestEventPartial {
                event_id: format!("event-{i}"),
                request_id: format!("req-{i}"),
                ..RequestEventPartial::default()
            }));
        }

        let mut queued = 0usize;
        while rx.try_recv().is_ok() {
            queued += 1;
        }
        assert_eq!(queued, 1024);
    }

    #[cfg(feature = "live-tail-pg")]
    #[test]
    fn notify_payload_size_boundary_matches_plan_limit() {
        assert!(notifier::notify_payload_fits_inline(7499));
        assert!(!notifier::notify_payload_fits_inline(7500));
        assert!(!notifier::notify_payload_fits_inline(7501));
    }
}
