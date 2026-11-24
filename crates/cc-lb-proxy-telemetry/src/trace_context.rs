use http::HeaderMap;

/// Inject the active OpenTelemetry/tracing context into outbound upstream headers.
pub fn inject_current_trace_context(headers: &mut HeaderMap) {
    cc_lb_observability::inject_current_trace_context(headers);
}
