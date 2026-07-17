use http::HeaderMap;
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

struct HeaderMapExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

struct HeaderMapInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(name) = http::HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = http::HeaderValue::from_str(&value) else {
            return;
        };
        self.0.insert(name, value);
    }
}

pub fn parent_context_from_headers(headers: &HeaderMap) -> Option<Context> {
    let parent = TraceContextPropagator::new().extract(&HeaderMapExtractor(headers));
    parent.span().span_context().is_valid().then_some(parent)
}

pub fn inject_current_trace_context(headers: &mut HeaderMap) {
    let context = tracing::Span::current().context();
    if !context.span().span_context().is_valid() {
        return;
    }
    TraceContextPropagator::new().inject_context(&context, &mut HeaderMapInjector(headers));
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    };

    use super::*;

    fn remote_context() -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("b7ad6b7169203331").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
    }

    #[test]
    fn round_trips_trace_context_through_header_map() {
        let mut headers = HeaderMap::new();
        TraceContextPropagator::new()
            .inject_context(&remote_context(), &mut HeaderMapInjector(&mut headers));

        assert_eq!(
            headers.get("traceparent"),
            Some(&HeaderValue::from_static(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            ))
        );

        let parent = parent_context_from_headers(&headers).expect("valid parent context");
        assert_eq!(
            parent.span().span_context().trace_id(),
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
        );
    }

    #[test]
    fn rejects_missing_or_malformed_traceparent() {
        assert!(parent_context_from_headers(&HeaderMap::new()).is_none());

        let mut headers = HeaderMap::new();
        headers.insert("traceparent", HeaderValue::from_static("not-a-traceparent"));
        assert!(parent_context_from_headers(&headers).is_none());
    }

    #[test]
    fn inject_without_active_otel_span_leaves_headers_unchanged() {
        let mut headers = HeaderMap::new();
        inject_current_trace_context(&mut headers);
        assert!(headers.is_empty());
    }
}
