#![allow(dead_code)]

use std::convert::Infallible;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::{SseBatchConfig, SseRelay};
use cc_lb_plugin_api::{
    DialectError, ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RequestContext,
    ShapedRequest, ShapedRequestBuilder, Upstream, UpstreamDialect,
};
use http::Response;
use http_body_util::BodyExt;
use tokio::sync::Notify;

#[derive(Default)]
pub struct RecordingHook {
    events: Mutex<Vec<ObserveEvent>>,
}

impl RecordingHook {
    pub fn events(&self) -> Vec<ObserveEvent> {
        self.events.lock().expect("recording hook lock").clone()
    }

    pub fn chunk_calls(&self) -> usize {
        self.events()
            .into_iter()
            .filter(|event| matches!(event, ObserveEvent::Chunk { .. }))
            .count()
    }
}

impl ObservabilityHook for RecordingHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        self.events.lock().expect("recording hook lock").push(event);
        Ok(())
    }
}

pub struct TestDialect;

impl UpstreamDialect for TestDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "test dialect is relay-only".to_owned(),
        })
    }
}

#[derive(Clone)]
pub struct DropSignal {
    closed: Arc<AtomicBool>,
    notify: Arc<Notify>,
    started: Instant,
}

impl DropSignal {
    pub fn new() -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            started: Instant::now(),
        }
    }

    pub async fn wait_closed_ms(&self) -> u128 {
        if !self.closed.load(Ordering::Relaxed) {
            self.notify.notified().await;
        }
        self.started.elapsed().as_millis()
    }
}

struct DropGuard {
    signal: DropSignal,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.signal.closed.store(true, Ordering::Relaxed);
        self.signal.notify.notify_waiters();
    }
}

pub fn relay_for(hook: Arc<RecordingHook>, batch: SseBatchConfig) -> SseRelay {
    SseRelay {
        obs: hook,
        dialect: Arc::new(TestDialect),
        batch,
        error_normalizer: None,
        upstream_kind: None,
        streaming_usage: Arc::new(Mutex::new(Default::default())),
        prompt_cache_observation_context: None,
        prompt_cache_observation_event_emitter: None,
    }
}

pub fn body_from_chunks(
    chunks: Vec<Bytes>,
    delay: Duration,
    drop_signal: Option<DropSignal>,
) -> Body {
    let stream = async_stream::stream! {
        let _guard = drop_signal.map(|signal| DropGuard { signal });
        for chunk in chunks {
            yield Ok::<Bytes, Infallible>(chunk);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
    };
    Body::from_stream(stream)
}

pub fn body_with_error_after(chunks: Vec<Bytes>, error_after: usize) -> Body {
    let stream = async_stream::stream! {
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index == error_after {
                yield Err::<Bytes, io::Error>(io::Error::new(io::ErrorKind::ConnectionReset, "forced upstream failure"));
                return;
            }
            yield Ok::<Bytes, io::Error>(chunk);
        }
        yield Err::<Bytes, io::Error>(io::Error::new(io::ErrorKind::ConnectionReset, "forced upstream failure"));
    };
    Body::from_stream(stream)
}

pub async fn collect_response_body(response: Response<Body>) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("response body collects")
        .to_bytes()
}

pub fn numbered_events(count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|index| {
            Bytes::from(format!(
                "event: content_block_delta\ndata: {{\"index\":{index}}}\n\n"
            ))
        })
        .collect()
}

pub fn fixture_1000_events() -> Bytes {
    let mut bytes = Vec::new();
    for index in 0..1000 {
        bytes.extend_from_slice(
            format!("event: content_block_delta\ndata: {{\"index\":{index},\"text\":\"token-{index}\"}}\n\n").as_bytes(),
        );
    }
    Bytes::from(bytes)
}

#[async_trait]
pub trait WaitForRelay {
    async fn collect_relay(self) -> Bytes;
}

#[async_trait]
impl WaitForRelay for Response<Body> {
    async fn collect_relay(self) -> Bytes {
        collect_response_body(self).await
    }
}
