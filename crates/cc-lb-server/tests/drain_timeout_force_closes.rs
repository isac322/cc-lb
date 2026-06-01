#![cfg(any())]

// Retired legacy static-config scenario.
//
// Current shutdown semantics intentionally do not treat long-lived SSE response
// bodies as drain in-flight work. The active regression coverage for that
// contract lives in `cc_lb_core::drain::tests`:
// - `response_body_stream_does_not_hold_in_flight`
// - `request_processing_still_holds_in_flight`
