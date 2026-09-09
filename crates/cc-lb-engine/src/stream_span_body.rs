use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tracing::Span;

/// Keeps a response-stream span current for every body poll and for body drop.
pub(crate) struct StreamSpanBody {
    inner: Option<Body>,
    span: Option<Span>,
}

impl StreamSpanBody {
    pub(crate) fn new(inner: Body, span: Span) -> Self {
        Self {
            inner: Some(inner),
            span: Some(span),
        }
    }
}

impl HttpBody for StreamSpanBody {
    type Data = Bytes;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        if this.span.is_none() {
            return Poll::Ready(None);
        }
        let poll = {
            let _entered = this
                .span
                .as_ref()
                .expect("stream span missing before terminal poll")
                .enter();
            Pin::new(
                this.inner
                    .as_mut()
                    .expect("stream body polled after terminal frame"),
            )
            .poll_frame(cx)
        };
        if matches!(&poll, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.span.take();
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(HttpBody::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map_or_else(SizeHint::new, HttpBody::size_hint)
    }
}

impl Drop for StreamSpanBody {
    fn drop(&mut self) {
        if let Some(span) = self.span.as_ref() {
            let _entered = span.enter();
            drop(self.inner.take());
        } else {
            drop(self.inner.take());
        }
        self.span.take();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use http_body_util::BodyExt as _;
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::{Layer, Registry};

    use super::*;

    const TEST_EVENT_TARGET: &str = "cc_lb_engine::stream_span_body_test";

    #[derive(Clone, Default)]
    struct RecordingLayer {
        closed: Arc<AtomicUsize>,
        poll_event_in_stream_span: Arc<AtomicBool>,
    }

    impl<S> Layer<S> for RecordingLayer
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
            if event.metadata().target() != TEST_EVENT_TARGET {
                return;
            }
            let in_stream_span = ctx.event_scope(event).is_some_and(|scope| {
                scope
                    .from_root()
                    .any(|span| span.metadata().name() == "proxy.response_stream")
            });
            self.poll_event_in_stream_span
                .store(in_stream_span, Ordering::SeqCst);
        }

        fn on_close(&self, id: tracing::span::Id, ctx: LayerContext<'_, S>) {
            if ctx
                .span(&id)
                .is_some_and(|span| span.metadata().name() == "proxy.response_stream")
            {
                self.closed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    struct EventBody;

    impl HttpBody for EventBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            tracing::info!(target: TEST_EVENT_TARGET, "inner_body_polled");
            Poll::Ready(None)
        }
    }

    #[tokio::test]
    async fn body_poll_is_scoped_and_span_closes_at_eos() {
        let recording = RecordingLayer::default();
        let closed = Arc::clone(&recording.closed);
        let poll_event_in_stream_span = Arc::clone(&recording.poll_event_in_stream_span);
        let subscriber = Registry::default().with(recording);
        let _default = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!("proxy.response_stream");
        let inner = Body::new(EventBody);
        let mut body = StreamSpanBody::new(inner, span.clone());
        drop(span);

        assert_eq!(closed.load(Ordering::SeqCst), 0);
        assert!(body.frame().await.is_none());
        assert!(poll_event_in_stream_span.load(Ordering::SeqCst));
        assert_eq!(closed.load(Ordering::SeqCst), 1);

        drop(body);
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn body_drop_closes_unpolled_span_once() {
        let recording = RecordingLayer::default();
        let closed = Arc::clone(&recording.closed);
        let subscriber = Registry::default().with(recording);
        let _default = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!("proxy.response_stream");
        let body = StreamSpanBody::new(Body::empty(), span.clone());
        drop(span);

        assert_eq!(closed.load(Ordering::SeqCst), 0);
        drop(body);
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }
}
