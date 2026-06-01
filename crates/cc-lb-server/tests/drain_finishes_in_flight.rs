#![cfg(any())]

// Retired legacy static-config scenario.
//
// SSE streams are response-body lifetime, not proxy request processing lifetime.
// Graceful drain waits for handlers to produce responses; clients are expected
// to reconnect if shutdown cuts an active SSE body.
