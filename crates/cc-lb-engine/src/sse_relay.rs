use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant as StdInstant};

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use cc_lb_contract::RequestEventBus;
use cc_lb_lifecycle::{EventId, LifecycleEvent};
use cc_lb_observability::{ObservabilityHook, ObserveEvent};
use cc_lb_upstream::UpstreamDialect;
use eventsource_stream::{Event, EventStream, EventStreamError};
use futures_core::Stream;
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
use http_body_util::BodyStream;
use hyper::body::Frame;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant as TokioInstant, Sleep, sleep};

use crate::error_normalizer::{ErrorNormalizer, UpstreamKind};
use crate::lifecycle::{
    PromptCacheObservationContext, PromptCacheObservationDecodeResult, PromptCacheUsage,
    decode_prompt_cache_observations_pure, prompt_cache_observations_to_wire,
};
use crate::sse_error_frame::make_error_frame;

const CLIENT_DISCONNECTED_STATUS: u16 = 499;

#[derive(Clone)]
pub struct SseRelay {
    pub obs: Arc<dyn ObservabilityHook>,
    pub dialect: Arc<dyn UpstreamDialect>,
    pub batch: SseBatchConfig,
    pub error_normalizer: Option<Arc<ErrorNormalizer>>,
    pub upstream_kind: Option<UpstreamKind>,
    pub streaming_usage: Arc<Mutex<StreamingUsage>>,
    pub prompt_cache_observation_context: Option<PromptCacheObservationContext>,
    pub prompt_cache_observation_event_emitter: Option<PromptCacheObservationEventEmitter>,
}

#[derive(Clone)]
pub struct PromptCacheObservationEventEmitter {
    pub event_id: EventId,
    pub bus: Arc<dyn RequestEventBus>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamingUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SseBatchConfig {
    pub max_events: usize,
    pub max_age: Duration,
}

impl Default for SseBatchConfig {
    fn default() -> Self {
        Self {
            max_events: 32,
            max_age: Duration::from_millis(100),
        }
    }
}

#[derive(Debug)]
pub enum RelayError {
    UpstreamReadFailed { source: String },
    SizeCapExceeded { cap: usize },
    MalformedSse { source: String },
}

impl fmt::Display for RelayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UpstreamReadFailed { source } => {
                write!(f, "upstream read failed: {source}")
            }
            Self::SizeCapExceeded { cap } => {
                write!(f, "upstream body exceeded size cap {cap} bytes")
            }
            Self::MalformedSse { source } => write!(f, "malformed SSE stream: {source}"),
        }
    }
}

impl Error for RelayError {}

struct RelayRuntime {
    obs: Arc<dyn ObservabilityHook>,
    batch: SseBatchConfig,
    error_normalizer: Option<Arc<ErrorNormalizer>>,
    upstream_kind: Option<UpstreamKind>,
    streaming_usage: Arc<Mutex<StreamingUsage>>,
    prompt_cache_observation_context: Option<PromptCacheObservationContext>,
    prompt_cache_observation_event_emitter: Option<PromptCacheObservationEventEmitter>,
    started: StdInstant,
}

impl SseRelay {
    pub fn current_usage(&self) -> StreamingUsage {
        *self
            .streaming_usage
            .lock()
            .expect("streaming usage lock poisoned")
    }

    pub fn into_response(self, upstream_body: hyper::body::Incoming) -> Response<Body> {
        self.into_response_from_body(Body::new(upstream_body))
    }

    #[doc(hidden)]
    pub fn into_response_from_body(self, upstream_body: Body) -> Response<Body> {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (tx, mut rx) = mpsc::channel::<Bytes>(16);
        let runtime = self.into_runtime();
        let relay_task = tokio::spawn(async move {
            runtime
                .relay_stream_task(upstream_body, tx, cancel_rx)
                .await;
        });

        let stream = async_stream::stream! {
            let _guard = DownstreamCancelGuard { cancel_tx, relay_task };
            while let Some(bytes) = rx.recv().await {
                yield Ok::<Bytes, Infallible>(bytes);
            }
        };

        let mut response = Response::new(Body::from_stream(stream));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
        response
    }

    pub async fn relay_full(
        self,
        upstream_body: hyper::body::Incoming,
        size_cap: usize,
    ) -> Result<Response<Body>, RelayError> {
        self.relay_full_body(Body::new(upstream_body), size_cap)
            .await
    }

    #[doc(hidden)]
    pub async fn relay_full_body(
        self,
        upstream_body: Body,
        size_cap: usize,
    ) -> Result<Response<Body>, RelayError> {
        let runtime = self.into_runtime();
        let mut stream = BodyStream::new(upstream_body);
        let mut body = BytesMut::new();
        while let Some(frame) = next_body_frame(&mut stream).await {
            let frame = frame.map_err(|source| RelayError::UpstreamReadFailed {
                source: source.to_string(),
            })?;
            if let Ok(data) = frame.into_data() {
                if body.len().saturating_add(data.len()) > size_cap {
                    return Err(RelayError::SizeCapExceeded { cap: size_cap });
                }
                body.extend_from_slice(&data);
            }
        }

        let bytes = body.freeze();
        let usage = usage_from_json_bytes(&bytes);
        runtime.store_usage(usage);
        Ok(Response::new(Body::from(bytes)))
    }

    fn into_runtime(self) -> RelayRuntime {
        RelayRuntime {
            obs: self.obs,
            batch: self.batch.normalized(),
            error_normalizer: self.error_normalizer,
            upstream_kind: self.upstream_kind,
            streaming_usage: self.streaming_usage,
            prompt_cache_observation_context: self.prompt_cache_observation_context,
            prompt_cache_observation_event_emitter: self.prompt_cache_observation_event_emitter,
            started: StdInstant::now(),
        }
    }
}

impl StreamingUsage {
    pub fn observe_message_start(&mut self, usage_json: &Value) {
        if let Some(input_tokens) = usage_json.get("input_tokens").and_then(Value::as_u64) {
            self.input_tokens = input_tokens;
        }
        apply_cache_creation_split(self, usage_json);
        if let Some(cache_read_input_tokens) = usage_json
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
        {
            self.cache_read_input_tokens = cache_read_input_tokens;
        }
    }

    pub fn observe_message_delta(&mut self, usage_json: &Value) {
        if let Some(output_tokens) = usage_json.get("output_tokens").and_then(Value::as_u64) {
            self.output_tokens = output_tokens;
        }
        apply_cache_creation_split(self, usage_json);
        if let Some(cache_read_input_tokens) = usage_json
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
        {
            self.cache_read_input_tokens = cache_read_input_tokens;
        }
    }

    pub fn mark_complete(&mut self) {
        self.complete = true;
    }
}

fn apply_cache_creation_split(usage: &mut StreamingUsage, reported: &Value) {
    if let Some(cc) = reported.get("cache_creation").and_then(Value::as_object) {
        let m5 = cc
            .get("ephemeral_5m_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let m1 = cc
            .get("ephemeral_1h_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        usage.cache_creation_input_tokens_5m = m5;
        usage.cache_creation_input_tokens_1h = m1;
        usage.cache_creation_input_tokens = m5.saturating_add(m1);
        return;
    }
    if let Some(flat) = reported
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
    {
        usage.cache_creation_input_tokens_5m = flat;
        usage.cache_creation_input_tokens_1h = 0;
        usage.cache_creation_input_tokens = flat;
    }
}

impl SseBatchConfig {
    fn normalized(self) -> Self {
        Self {
            max_events: self.max_events.max(1),
            max_age: self.max_age,
        }
    }
}

impl RelayRuntime {
    async fn relay_stream_task(
        self,
        upstream_body: Body,
        tx: mpsc::Sender<Bytes>,
        mut cancel_rx: watch::Receiver<bool>,
    ) {
        let mut stream = BodyStream::new(upstream_body);
        let mut buffer = BytesMut::new();
        let mut batcher = SseBatcher::new(self.batch);
        let mut usage = self.current_usage();
        let mut prompt_cache_decode = PromptCacheObservationDecodeResult::default();
        let mut prompt_cache_observations_buffered = false;
        let mut deadline = Box::pin(sleep(self.batch.max_age));
        reset_deadline(&mut deadline, self.batch.max_age);

        loop {
            tokio::select! {
                changed = cancel_rx.changed() => {
                    if changed.is_ok() && *cancel_rx.borrow() {
                        batcher.flush(&self.obs);
                        self.observe_finished(client_disconnected_status(), None, None, None, None);
                        break;
                    }
                    if changed.is_err() {
                        batcher.flush(&self.obs);
                        self.observe_finished(client_disconnected_status(), None, None, None, None);
                        break;
                    }
                }
                _ = &mut deadline, if batcher.has_pending() => {
                    batcher.flush(&self.obs);
                    reset_deadline(&mut deadline, self.batch.max_age);
                }
                frame = next_body_frame(&mut stream) => {
                    match frame {
                        Some(Ok(frame)) => {
                            if let Ok(data) = frame.into_data() {
                                buffer.extend_from_slice(&data);
                                if self.drain_complete_events(
                                    &mut buffer,
                                    &tx,
                                    &mut batcher,
                                    &mut deadline,
                                    &mut usage,
                                    &mut prompt_cache_decode,
                                    &mut prompt_cache_observations_buffered,
                                ).await.is_err() {
                                    self.observe_finished(client_disconnected_status(), None, None, None, None);
                                    break;
                                }
                            }
                        }
                        Some(Err(source)) => {
                            batcher.flush(&self.obs);
                            let _sent = tx.send(self.error_frame_for_unknown_status(&source.to_string())).await;
                            self.observe_error("upstream_read_failed", &source.to_string());
                            self.observe_finished(
                                StatusCode::BAD_GATEWAY,
                                Some(usage.input_tokens),
                                Some(usage.output_tokens),
                                Some(usage.cache_creation_input_tokens),
                                Some(usage.cache_read_input_tokens),
                            );
                            break;
                        }
                        None => {
                            batcher.flush(&self.obs);
                            self.observe_finished(
                                StatusCode::OK,
                                Some(usage.input_tokens),
                                Some(usage.output_tokens),
                                Some(usage.cache_creation_input_tokens),
                                Some(usage.cache_read_input_tokens),
                            );
                            break;
                        }
                    }
                }
            }
        }
        if prompt_cache_observations_buffered && !prompt_cache_decode.observations.is_empty() {
            self.emit_prompt_cache_observations_produced(&prompt_cache_decode, true);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn drain_complete_events(
        &self,
        buffer: &mut BytesMut,
        tx: &mpsc::Sender<Bytes>,
        batcher: &mut SseBatcher,
        deadline: &mut Pin<Box<Sleep>>,
        usage: &mut StreamingUsage,
        prompt_cache_decode: &mut PromptCacheObservationDecodeResult,
        prompt_cache_observations_buffered: &mut bool,
    ) -> Result<(), ()> {
        while let Some(end) = crate::usage_parser::find_sse_event_end(buffer) {
            let raw = buffer.split_to(end).freeze();
            let mut outgoing = raw.clone();
            match parse_one_event(raw).await {
                Ok(Some(event)) => {
                    if event.event == "error" {
                        outgoing = self.error_frame_from_event(&event, outgoing);
                    } else {
                        let usage_update = update_usage_from_event(&event, usage);
                        self.store_usage(*usage);
                        if usage_update.message_start_usage
                            && !*prompt_cache_observations_buffered
                            && let Some(context) = self.prompt_cache_observation_context.as_ref()
                        {
                            let now_unix_secs = context.cache.clock_now_unix_secs();
                            *prompt_cache_decode = decode_prompt_cache_observations_pure(
                                context,
                                PromptCacheUsage {
                                    cache_creation_input_tokens: usage.cache_creation_input_tokens,
                                    cache_read_input_tokens: usage.cache_read_input_tokens,
                                },
                                now_unix_secs,
                            );
                            *prompt_cache_observations_buffered = true;
                        }
                        if usage_update.message_stop
                            && *prompt_cache_observations_buffered
                            && self.prompt_cache_observation_context.is_some()
                        {
                            self.emit_prompt_cache_observations_produced(
                                prompt_cache_decode,
                                false,
                            );
                            *prompt_cache_observations_buffered = false;
                        }
                    }
                }
                Ok(None) => {}
                Err(source) => {
                    let frame = self.error_frame_for_unknown_status(&source.to_string());
                    if tx.send(frame).await.is_err() {
                        return Err(());
                    }
                    self.observe_error("malformed_sse", &source.to_string());
                    return Err(());
                }
            }

            batcher.record_event(outgoing.len(), &self.obs, deadline);
            if tx.send(outgoing).await.is_err() {
                return Err(());
            }
        }
        Ok(())
    }

    fn emit_prompt_cache_observations_produced(
        &self,
        decode: &PromptCacheObservationDecodeResult,
        was_aborted: bool,
    ) {
        let (Some(context), Some(emitter)) = (
            self.prompt_cache_observation_context.as_ref(),
            self.prompt_cache_observation_event_emitter.as_ref(),
        ) else {
            return;
        };
        let dropped_aborted = if was_aborted {
            u32::try_from(decode.observations.len()).unwrap_or(u32::MAX)
        } else {
            0
        };
        emitter
            .bus
            .publish_lifecycle(LifecycleEvent::PromptCacheObservationsProduced {
                event_id: emitter.event_id.clone(),
                upstream_id: context.upstream_id,
                canonical_model_id: context.canonical_model_id.clone(),
                observations: if was_aborted {
                    Vec::new()
                } else {
                    prompt_cache_observations_to_wire(&decode.observations)
                },
                dropped_below_threshold: if was_aborted {
                    0
                } else {
                    decode.dropped_below_threshold
                },
                dropped_aborted,
            });
    }

    fn error_frame_from_event(&self, event: &Event, raw_fallback: Bytes) -> Bytes {
        let data = Bytes::from(event.data.clone());
        if let (Some(normalizer), Some(kind)) = (self.error_normalizer.as_ref(), self.upstream_kind)
        {
            return normalizer.normalize_sse_error_frame(kind, &data);
        }

        raw_fallback
    }

    fn error_frame_for_unknown_status(&self, message: &str) -> Bytes {
        make_error_frame("api_error", message)
    }

    fn current_usage(&self) -> StreamingUsage {
        *self
            .streaming_usage
            .lock()
            .expect("streaming usage lock poisoned")
    }

    fn store_usage(&self, usage: StreamingUsage) {
        *self
            .streaming_usage
            .lock()
            .expect("streaming usage lock poisoned") = usage;
    }

    fn observe_error(&self, code: &str, message: &str) {
        let _result = self.obs.observe(ObserveEvent::Error {
            code: code.to_owned(),
            message: message.to_owned(),
            source: "sse_relay".to_owned(),
        });
    }

    fn observe_finished(
        &self,
        status: StatusCode,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cache_creation_input_tokens: Option<u64>,
        cache_read_input_tokens: Option<u64>,
    ) {
        let _result = self.obs.observe(ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms: self
                .started
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        });
    }
}

struct SseBatcher {
    config: SseBatchConfig,
    batch_index: u64,
    event_count: usize,
    total_bytes: usize,
}

impl SseBatcher {
    fn new(config: SseBatchConfig) -> Self {
        Self {
            config,
            batch_index: 0,
            event_count: 0,
            total_bytes: 0,
        }
    }

    fn has_pending(&self) -> bool {
        self.event_count > 0
    }

    fn record_event(
        &mut self,
        bytes: usize,
        obs: &Arc<dyn ObservabilityHook>,
        deadline: &mut Pin<Box<Sleep>>,
    ) {
        if self.event_count == 0 {
            reset_deadline(deadline, self.config.max_age);
        }
        self.event_count = self.event_count.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        if self.event_count >= self.config.max_events {
            self.flush(obs);
            reset_deadline(deadline, self.config.max_age);
        }
    }

    fn flush(&mut self, obs: &Arc<dyn ObservabilityHook>) {
        if self.event_count == 0 {
            return;
        }
        let event = ObserveEvent::Chunk {
            batch_index: self.batch_index,
            event_count: self.event_count,
            total_bytes: self.total_bytes,
        };
        let _result = obs.observe(event);
        self.batch_index = self.batch_index.saturating_add(1);
        self.event_count = 0;
        self.total_bytes = 0;
    }
}

struct DownstreamCancelGuard {
    cancel_tx: watch::Sender<bool>,
    relay_task: JoinHandle<()>,
}

impl Drop for DownstreamCancelGuard {
    fn drop(&mut self) {
        let _sent = self.cancel_tx.send(true);
        if !self.relay_task.is_finished() {
            self.relay_task.abort();
        }
    }
}

async fn next_body_frame(
    stream: &mut BodyStream<Body>,
) -> Option<Result<Frame<Bytes>, axum::Error>> {
    poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

async fn parse_one_event(raw: Bytes) -> Result<Option<Event>, RelayError> {
    let source = SingleBytesStream { next: Some(raw) };
    let mut stream = EventStream::new(source);
    match poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
        Some(Ok(event)) => Ok(Some(event)),
        Some(Err(source)) => Err(RelayError::MalformedSse {
            source: event_stream_error_to_string(source),
        }),
        None => Ok(None),
    }
}

struct SingleBytesStream {
    next: Option<Bytes>,
}

impl Stream for SingleBytesStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.next.take().map(Ok))
    }
}

fn event_stream_error_to_string(error: EventStreamError<Infallible>) -> String {
    match error {
        EventStreamError::Utf8(source) => source.to_string(),
        EventStreamError::Parser(source) => source.to_string(),
        EventStreamError::Transport(source) => match source {},
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamingUsageUpdate {
    message_start_usage: bool,
    message_stop: bool,
}

fn update_usage_from_event(event: &Event, usage: &mut StreamingUsage) -> StreamingUsageUpdate {
    let mut update = StreamingUsageUpdate::default();
    let Ok(value) = sonic_rs::from_str::<Value>(&event.data) else {
        return update;
    };
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or(event.event.as_str());
    match event_type {
        "message_start" => {
            if let Some(usage_json) = value
                .get("message")
                .and_then(|message| message.get("usage"))
            {
                usage.observe_message_start(usage_json);
                update.message_start_usage = true;
            }
        }
        "message_delta" => {
            if let Some(usage_json) = value.get("usage") {
                usage.observe_message_delta(usage_json);
            }
        }
        "message_stop" => {
            update_usage_from_value(&value, usage);
            usage.mark_complete();
            update.message_stop = true;
        }
        _ => {}
    }
    update
}

fn usage_from_json_bytes(bytes: &Bytes) -> StreamingUsage {
    let mut usage = StreamingUsage::default();
    if let Ok(value) = sonic_rs::from_slice::<Value>(bytes) {
        update_usage_from_value(&value, &mut usage);
        usage.mark_complete();
    }
    usage
}

fn update_usage_from_value(value: &Value, usage: &mut StreamingUsage) {
    let reported_usage = value.get("usage").or_else(|| {
        value
            .get("message")
            .and_then(|message| message.get("usage"))
    });
    if let Some(input_tokens) = reported_usage
        .and_then(|usage| usage.get("input_tokens"))
        .and_then(Value::as_u64)
    {
        usage.input_tokens = input_tokens;
    }
    if let Some(output_tokens) = reported_usage
        .and_then(|usage| usage.get("output_tokens"))
        .and_then(Value::as_u64)
    {
        usage.output_tokens = output_tokens;
    }
    if let Some(reported) = reported_usage {
        apply_cache_creation_split(usage, reported);
    }
    if let Some(cache_read_input_tokens) = reported_usage
        .and_then(|usage| usage.get("cache_read_input_tokens"))
        .and_then(Value::as_u64)
    {
        usage.cache_read_input_tokens = cache_read_input_tokens;
    }
}

fn reset_deadline(deadline: &mut Pin<Box<Sleep>>, max_age: Duration) {
    deadline
        .as_mut()
        .reset(TokioInstant::now() + max_age.max(Duration::from_millis(1)));
}

fn client_disconnected_status() -> StatusCode {
    StatusCode::from_u16(CLIENT_DISCONNECTED_STATUS).unwrap_or(StatusCode::BAD_GATEWAY)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use cc_lb_observability::{ObservabilityError, ObservabilityHook, ObserveEvent};
    use cc_lb_plugin_api::types::{
        BreakpointOrigin, CacheBreakpoint, CacheBreakpointSource, TtlClass, WarmCacheEntry,
    };
    use cc_lb_plugin_api::{Principal, Upstream};
    use cc_lb_storage_api::PromptCacheObservationRecord;
    use cc_lb_upstream::{
        DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
    };
    use http_body_util::BodyExt;
    use uuid::Uuid;

    use crate::event_bus::InMemoryBus;
    use crate::lifecycle::{
        PromptCacheObservationCacheLike, PromptCacheObservationEnqueueError,
        PromptCacheObservationInput, PromptCacheObservationSinkLike,
    };
    use cc_lb_contract::LifecycleBusReceiver;

    use super::*;

    const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";

    #[tokio::test]
    async fn abort_skips_observation() {
        let cache = Arc::new(RecordingPromptCacheObservationCache::default());
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut events) = bus.subscribe_lifecycle() else {
            panic!("expected InMemory lifecycle receiver");
        };
        let relay = relay_with_context_with_emitter(cache, sink.clone(), bus, "sse-abort");

        let response = relay.into_response_from_body(Body::from(sse_event(
            "message_start",
            r#"{"type":"message_start","message":{"usage":{"input_tokens":1600,"cache_creation_input_tokens":1600}}}"#,
        )));
        let _body = response.into_body().collect().await.expect("body collects");

        assert!(sink.records().is_empty());
        let LifecycleEvent::PromptCacheObservationsProduced {
            observations,
            dropped_below_threshold,
            dropped_aborted,
            ..
        } = events.try_recv().expect("abort event is emitted")
        else {
            panic!("expected prompt-cache observation event");
        };
        assert!(observations.is_empty());
        assert_eq!(dropped_below_threshold, 0);
        assert_eq!(dropped_aborted, 1);
    }

    #[tokio::test]
    async fn abort_skips_in_memory_upsert() {
        let cache = Arc::new(RecordingPromptCacheObservationCache::default());
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let relay = relay_with_context(cache.clone(), sink);

        let response = relay.into_response_from_body(Body::from(sse_event(
            "message_start",
            r#"{"type":"message_start","message":{"usage":{"input_tokens":1600,"cache_creation_input_tokens":1600}}}"#,
        )));
        let _body = response.into_body().collect().await.expect("body collects");

        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000231").unwrap();
        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            TEST_MODEL,
            &[("write".to_owned(), TtlClass::Ephemeral5m)],
            cache.clock_now_unix_secs(),
        );
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn message_stop_emits_observation_event() {
        let cache = Arc::new(RecordingPromptCacheObservationCache::default());
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut events) = bus.subscribe_lifecycle() else {
            panic!("expected InMemory lifecycle receiver");
        };
        let relay = relay_with_context_with_emitter(cache, sink.clone(), bus, "sse-success");
        let body = format!(
            "{}{}",
            sse_event(
                "message_start",
                r#"{"type":"message_start","message":{"usage":{"input_tokens":1600,"cache_creation_input_tokens":1600}}}"#,
            ),
            sse_event("message_stop", r#"{"type":"message_stop"}"#),
        );

        let response = relay.into_response_from_body(Body::from(body));
        let _body = response.into_body().collect().await.expect("body collects");

        assert!(sink.records().is_empty());
        let LifecycleEvent::PromptCacheObservationsProduced {
            canonical_model_id,
            observations,
            dropped_below_threshold,
            dropped_aborted,
            ..
        } = events.try_recv().expect("success event is emitted")
        else {
            panic!("expected prompt-cache observation event");
        };
        assert_eq!(canonical_model_id, TEST_MODEL);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].prefix_hash, "write");
        assert_eq!(dropped_below_threshold, 0);
        assert_eq!(dropped_aborted, 0);
    }

    fn relay_with_context(
        cache: Arc<RecordingPromptCacheObservationCache>,
        sink: Arc<RecordingPromptCacheObservationSink>,
    ) -> SseRelay {
        relay_with_context_impl(cache, sink, None)
    }

    fn relay_with_context_with_emitter(
        cache: Arc<RecordingPromptCacheObservationCache>,
        sink: Arc<RecordingPromptCacheObservationSink>,
        bus: Arc<InMemoryBus>,
        event_id: &str,
    ) -> SseRelay {
        relay_with_context_impl(
            cache,
            sink,
            Some(PromptCacheObservationEventEmitter {
                event_id: event_id.to_owned(),
                bus,
            }),
        )
    }

    fn relay_with_context_impl(
        cache: Arc<RecordingPromptCacheObservationCache>,
        _sink: Arc<RecordingPromptCacheObservationSink>,
        prompt_cache_observation_event_emitter: Option<PromptCacheObservationEventEmitter>,
    ) -> SseRelay {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000231").unwrap();
        SseRelay {
            obs: Arc::new(NoopHook),
            dialect: Arc::new(TestDialect),
            batch: SseBatchConfig::default(),
            error_normalizer: None,
            upstream_kind: None,
            streaming_usage: Arc::new(Mutex::new(StreamingUsage::default())),
            prompt_cache_observation_context: Some(PromptCacheObservationContext {
                upstream_id,
                canonical_model_id: TEST_MODEL.to_owned(),
                cache_breakpoints: vec![cache_breakpoint(0, "write", 1_600, TtlClass::Ephemeral5m)],
                selected_match: None,
                cache,
            }),
            prompt_cache_observation_event_emitter,
        }
    }

    fn sse_event(event: &str, data: &str) -> String {
        format!("event: {event}\ndata: {data}\n\n")
    }

    fn cache_breakpoint(
        index: u32,
        prefix_hash: &str,
        prefix_token_count: u64,
        requested_ttl: TtlClass,
    ) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index: index,
            source: CacheBreakpointSource::Message,
            path: format!("messages[{index}]"),
            message_index: Some(index),
            prefix_hash: prefix_hash.to_owned(),
            prefix_token_count,
            requested_ttl,
            origin: BreakpointOrigin::Explicit,
            lookback_prefixes: vec![cc_lb_plugin_api::types::CacheLookbackPrefix {
                prefix_hash: prefix_hash.to_owned(),
                content_block_index: index,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("test".to_owned()),
        }
    }

    #[derive(Default)]
    struct RecordingPromptCacheObservationCache {
        upserts: Mutex<HashMap<String, u64>>,
    }

    impl PromptCacheObservationCacheLike for RecordingPromptCacheObservationCache {
        fn snapshot_for_upstream(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            request_breakpoint_hashes: &[(String, TtlClass)],
            now_unix_secs: u64,
        ) -> Vec<WarmCacheEntry> {
            let upserts = self.upserts.lock().expect("upserts lock");
            request_breakpoint_hashes
                .iter()
                .filter_map(|(prefix_hash, ttl_class)| {
                    upserts
                        .get(prefix_hash)
                        .copied()
                        .map(|expires_at_unix_secs| WarmCacheEntry {
                            prefix_hash: prefix_hash.clone(),
                            expires_at_unix_secs,
                            ttl_class: *ttl_class,
                            last_observed_at_unix_secs: now_unix_secs,
                            content_block_index: 0,
                            estimated_prefix_tokens: 0,
                            token_estimate_source: "local_tiktoken_v1".to_owned(),
                            hash_schema_version: 4,
                        })
                })
                .collect()
        }

        fn upsert_observation(&self, observation: PromptCacheObservationInput) {
            self.upserts
                .lock()
                .expect("upserts lock")
                .insert(observation.prefix_hash, observation.expires_at_unix_secs);
        }

        fn refresh_on_hit(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            _prefix_hash: &str,
            _ttl_class: TtlClass,
            _now_unix_secs: u64,
        ) -> bool {
            true
        }

        fn grace_margin_secs(&self) -> u64 {
            30
        }

        fn clock_now_unix_secs(&self) -> u64 {
            0
        }
    }

    #[derive(Default)]
    struct RecordingPromptCacheObservationSink {
        records: Mutex<Vec<PromptCacheObservationRecord>>,
    }

    impl RecordingPromptCacheObservationSink {
        fn records(&self) -> Vec<PromptCacheObservationRecord> {
            self.records.lock().expect("records lock").clone()
        }
    }

    impl PromptCacheObservationSinkLike for RecordingPromptCacheObservationSink {
        fn enqueue(
            &self,
            record: PromptCacheObservationRecord,
        ) -> Result<(), PromptCacheObservationEnqueueError> {
            self.records.lock().expect("records lock").push(record);
            Ok(())
        }
    }

    struct NoopHook;

    impl ObservabilityHook for NoopHook {
        fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
            Ok(())
        }
    }

    struct TestDialect;

    impl UpstreamDialect for TestDialect {
        fn shape(
            &self,
            _context: &DialectShapeContext,
            _upstream: &Upstream,
            _principal: &Principal,
            _builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            Err(DialectError::UnsupportedRequest {
                reason: "test dialect is relay-only".to_owned(),
            })
        }
    }

    fn parse_usage(json: &str) -> Value {
        serde_json::from_str(json).expect("test usage json")
    }

    #[test]
    fn observe_message_start_nested_cache_creation_split() {
        let mut usage = StreamingUsage::default();
        let v = parse_usage(
            r#"{"input_tokens":10,"cache_creation":{"ephemeral_5m_input_tokens":400,"ephemeral_1h_input_tokens":1200},"cache_read_input_tokens":50}"#,
        );

        usage.observe_message_start(&v);

        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.cache_creation_input_tokens_5m, 400);
        assert_eq!(usage.cache_creation_input_tokens_1h, 1200);
        assert_eq!(usage.cache_creation_input_tokens, 1600);
        assert_eq!(usage.cache_read_input_tokens, 50);
    }

    #[test]
    fn observe_message_start_legacy_flat_only() {
        let mut usage = StreamingUsage::default();
        let v = parse_usage(r#"{"cache_creation_input_tokens":1600}"#);

        usage.observe_message_start(&v);

        assert_eq!(usage.cache_creation_input_tokens_5m, 1600);
        assert_eq!(usage.cache_creation_input_tokens_1h, 0);
        assert_eq!(usage.cache_creation_input_tokens, 1600);
    }

    #[test]
    fn observe_message_start_nested_overrides_legacy() {
        let mut usage = StreamingUsage::default();
        let v = parse_usage(
            r#"{"cache_creation_input_tokens":9999,"cache_creation":{"ephemeral_5m_input_tokens":500,"ephemeral_1h_input_tokens":500}}"#,
        );

        usage.observe_message_start(&v);

        assert_eq!(usage.cache_creation_input_tokens_5m, 500);
        assert_eq!(usage.cache_creation_input_tokens_1h, 500);
        assert_eq!(usage.cache_creation_input_tokens, 1000);
    }

    #[test]
    fn observe_message_delta_nested_cache_creation_split() {
        let mut usage = StreamingUsage::default();
        let v = parse_usage(
            r#"{"output_tokens":7,"cache_creation":{"ephemeral_5m_input_tokens":11,"ephemeral_1h_input_tokens":22}}"#,
        );

        usage.observe_message_delta(&v);

        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.cache_creation_input_tokens_5m, 11);
        assert_eq!(usage.cache_creation_input_tokens_1h, 22);
        assert_eq!(usage.cache_creation_input_tokens, 33);
    }

    #[test]
    fn usage_from_json_bytes_nested_cache_creation_split() {
        let body = Bytes::from_static(
            br#"{"usage":{"input_tokens":3,"output_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":400,"ephemeral_1h_input_tokens":1200},"cache_read_input_tokens":7}}"#,
        );

        let usage = usage_from_json_bytes(&body);

        assert!(usage.complete);
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.cache_creation_input_tokens_5m, 400);
        assert_eq!(usage.cache_creation_input_tokens_1h, 1200);
        assert_eq!(usage.cache_creation_input_tokens, 1600);
        assert_eq!(usage.cache_read_input_tokens, 7);
    }

    #[test]
    fn usage_from_json_bytes_legacy_flat() {
        let body = Bytes::from_static(
            br#"{"message":{"usage":{"cache_creation_input_tokens":42,"cache_read_input_tokens":3}}}"#,
        );

        let usage = usage_from_json_bytes(&body);

        assert_eq!(usage.cache_creation_input_tokens_5m, 42);
        assert_eq!(usage.cache_creation_input_tokens_1h, 0);
        assert_eq!(usage.cache_creation_input_tokens, 42);
        assert_eq!(usage.cache_read_input_tokens, 3);
    }
}
