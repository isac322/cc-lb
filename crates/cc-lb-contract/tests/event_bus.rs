use cc_lb_contract::{
    BusReceiver, FinalRequestEventUpdate, LifecycleBusReceiver, RequestEvent, RequestEventPartial,
    RequestEventPhase, RequestEventUpdate,
};

fn sample_event(request_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_700_000_000,
        request_id: request_id.to_owned(),
        status: 200,
        duration_ms: 42,
        event_id: Some(format!("event-{request_id}")),
        ..RequestEvent::default()
    }
}

fn sample_partial(request_id: &str) -> RequestEventPartial {
    RequestEventPartial {
        event_id: format!("event-{request_id}"),
        request_id: request_id.to_owned(),
        ts: 1_700_000_000,
        ts_ms: 1_700_000_000_000,
        last_update_ms: 1_700_000_000_010,
        elapsed_ms: 10,
        ..RequestEventPartial::default()
    }
}

#[test]
fn request_event_phase_as_str_is_stable() {
    assert_eq!(RequestEventPhase::Partial.as_str(), "partial");
    assert_eq!(RequestEventPhase::Final.as_str(), "final");
}

#[test]
fn request_event_update_constructors_set_phase() {
    let partial = RequestEventUpdate::partial(sample_partial("req-partial"));
    let final_update = RequestEventUpdate::final_(sample_event("req-final"), 7);

    assert_eq!(partial.phase(), RequestEventPhase::Partial);
    assert_eq!(partial.event_id(), "event-req-partial");
    assert_eq!(final_update.phase(), RequestEventPhase::Final);
    assert_eq!(final_update.event_id(), "event-req-final");
    assert!(matches!(
        final_update,
        RequestEventUpdate::Final(FinalRequestEventUpdate { cursor: 7, .. })
    ));
}

#[test]
fn request_event_update_is_final_matches_phase() {
    let partial = RequestEventUpdate::partial(sample_partial("req-partial"));
    let final_update = RequestEventUpdate::final_(sample_event("req-final"), 1);

    assert!(!partial.is_final());
    assert!(final_update.is_final());
}

#[test]
fn bus_receiver_variants_compile_with_tokio_channels() {
    let (_broadcast_tx, broadcast_rx) = tokio::sync::broadcast::channel(1);
    let (_request_tx, request_rx) = tokio::sync::mpsc::channel(1);
    let (_lifecycle_tx, lifecycle_rx) = tokio::sync::broadcast::channel(1);

    let _in_memory = BusReceiver::InMemory(broadcast_rx);
    let _remote = BusReceiver::Remote(request_rx);
    let _none = LifecycleBusReceiver::None;
    let _lifecycle = LifecycleBusReceiver::InMemory(lifecycle_rx);
}
