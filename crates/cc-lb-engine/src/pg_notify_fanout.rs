mod fanout;
mod listener;
mod metrics;
mod notifier;
mod protocol;

pub use fanout::PgNotifyFanout;
pub use listener::PgListener;
pub use notifier::{DEFAULT_PG_NOTIFY_CHANNEL, PARTIAL_NOTIFY_MPSC_CAPACITY, PgNotifier};

#[cfg(test)]
mod tests {
    use cc_lb_storage_api::RequestEventPartial;
    use tokio::sync::mpsc;

    use super::*;
    use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus, RequestEventUpdate};

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

    #[test]
    fn notify_payload_size_boundary_matches_plan_limit() {
        assert!(notifier::notify_payload_fits_inline(7499));
        assert!(!notifier::notify_payload_fits_inline(7500));
        assert!(!notifier::notify_payload_fits_inline(7501));
    }
}
