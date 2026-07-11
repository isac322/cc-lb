use cc_lb_control::RequestEventBus;
use cc_lb_engine::PgNotifyFanout;
use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};

#[tokio::test]
async fn pg_notify_fanout_notifier_queue_overflow_drops_without_blocking() {
    let local_bus = cc_lb_engine::InMemoryBus::with_capacity(16);
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel(1024);
    let fanout = PgNotifyFanout::new(local_bus, notify_tx);

    for index in 0..5000 {
        fanout.publish(RequestEventUpdate::Partial(RequestEventPartial {
            event_id: format!("event-{index}"),
            request_id: format!("req-{index}"),
            ts: 1_700_000_000,
            ts_ms: 1_700_000_000_000,
            last_update_ms: 1_700_000_000_000,
            ..RequestEventPartial::default()
        }));
    }

    let mut queued = 0usize;
    while notify_rx.try_recv().is_ok() {
        queued += 1;
    }
    assert_eq!(queued, 1024);
}
