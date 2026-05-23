use cc_lb_observability::{
    BoundedChannelHook, ObservabilityHook, ObserveEvent, dropped_events_total,
};

#[tokio::test]
async fn bounded_hook_counts_overflow_without_panicking() {
    let before = dropped_events_total();
    let (hook, mut receiver) = BoundedChannelHook::with_capacity(10);
    let mut dropped = 0;

    for batch_index in 0..100 {
        let result = hook.observe(ObserveEvent::Chunk {
            batch_index,
            event_count: 1,
            total_bytes: 16,
        });

        if result.is_err() {
            dropped += 1;
        }
    }

    let mut received = 0;
    while receiver.try_recv().is_ok() {
        received += 1;
    }

    assert_eq!(received, 10);
    assert_eq!(dropped, 90);
    assert_eq!(dropped_events_total() - before, 90);

    println!("bounded_received={received}");
    println!("bounded_dropped={dropped}");
}
