use cc_lb_request_log::RequestEvent;

const REQUEST_EVENT_FIXTURE: &str =
    include_str!("../../../tests/fixtures/reassembly/request_event.json");

#[test]
fn request_event_round_trips_fixture_bytes_when_domain_trace_is_populated() {
    // Given: the T0 persisted event fixture contains the populated domain trace and internal error.
    let fixture = REQUEST_EVENT_FIXTURE;

    // When: the persisted request-event read model deserializes and serializes it.
    let event: RequestEvent = serde_json::from_str(fixture).expect("fixture deserializes");
    let serialized = serde_json::to_string(&event).expect("event serializes");

    // Then: its persisted representation remains byte-identical.
    assert!(event.routing_trace.is_some());
    assert_eq!(event.internal_errors.len(), 1);
    assert_eq!(serialized, fixture);
}
