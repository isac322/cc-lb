use cc_lb_core::{DashboardBroadcaster, record_dashboard_sse_lagged};
use cc_lb_observability::dropped_events_total;
use cc_lb_storage_api::{RequestEvent, RequestEventUpstream};
use tokio::sync::broadcast::error::RecvError;

#[tokio::test]
async fn subscriber_receives_published_event() {
    let broadcaster = DashboardBroadcaster::with_capacity(8);
    let mut receiver = broadcaster.subscribe();

    broadcaster.publish(event("req-1"));

    let received = receiver.recv().await.expect("event is delivered");
    assert_eq!(received.request_id, "req-1");
}

#[tokio::test]
async fn two_subscribers_receive_each_published_event() {
    let broadcaster = DashboardBroadcaster::with_capacity(8);
    let mut first = broadcaster.subscribe();
    let mut second = broadcaster.subscribe();

    broadcaster.publish(event("req-shared"));

    assert_eq!(first.recv().await.unwrap().request_id, "req-shared");
    assert_eq!(second.recv().await.unwrap().request_id, "req-shared");
}

#[test]
fn publish_without_subscribers_does_not_error() {
    let broadcaster = DashboardBroadcaster::with_capacity(8);

    broadcaster.publish(event("req-no-subscribers"));
}

#[tokio::test]
async fn slow_subscriber_observes_lag() {
    let broadcaster = DashboardBroadcaster::with_capacity(2);
    let mut slow_receiver = broadcaster.subscribe();

    for index in 0..5 {
        broadcaster.publish(event(&format!("req-{index}")));
    }

    match slow_receiver.recv().await {
        Err(RecvError::Lagged(skipped)) => assert!(skipped >= 1),
        other => panic!("expected lagged receiver, got {other:?}"),
    }
}

#[test]
fn lag_helper_increments_dropped_event_counter_by_skipped_count() {
    let before = dropped_events_total();

    record_dashboard_sse_lagged(3);

    assert_eq!(dropped_events_total() - before, 3);
}

fn event(request_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1,
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some("claude-test".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(2),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        cost_usd_micros: None,
        duration_ms: 3,
        error_code: None,
        ..Default::default()
    }
}
