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
use cc_lb_plugin_api::{ObservabilityHook, ObserveEvent, UpstreamDialect};
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
use crate::sse_error_frame::{make_error_frame, make_error_frame_from_json};

const CLIENT_DISCONNECTED_STATUS: u16 = 499;

#[derive(Clone)]
pub struct SseRelay {
    pub obs: Arc<dyn ObservabilityHook>,
    pub dialect: Arc<dyn UpstreamDialect>,
    pub batch: SseBatchConfig,
    pub error_normalizer: Option<Arc<ErrorNormalizer>>,
    pub upstream_kind: Option<UpstreamKind>,
    pub streaming_usage: Arc<Mutex<StreamingUsage>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamingUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
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
    dialect: Arc<dyn UpstreamDialect>,
    batch: SseBatchConfig,
    error_normalizer: Option<Arc<ErrorNormalizer>>,
    upstream_kind: Option<UpstreamKind>,
    streaming_usage: Arc<Mutex<StreamingUsage>>,
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
        runtime.finish_usage(&usage).await;
        Ok(Response::new(Body::from(bytes)))
    }

    fn into_runtime(self) -> RelayRuntime {
        RelayRuntime {
            obs: self.obs,
            dialect: self.dialect,
            batch: self.batch.normalized(),
            error_normalizer: self.error_normalizer,
            upstream_kind: self.upstream_kind,
            streaming_usage: self.streaming_usage,
            started: StdInstant::now(),
        }
    }
}

impl StreamingUsage {
    pub fn observe_message_start(&mut self, usage_json: &Value) {
        if let Some(input_tokens) = usage_json.get("input_tokens").and_then(Value::as_u64) {
            self.input_tokens = input_tokens;
        }
        if let Some(cache_creation_input_tokens) = usage_json
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
        {
            self.cache_creation_input_tokens = cache_creation_input_tokens;
        }
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
        if let Some(cache_creation_input_tokens) = usage_json
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
        {
            self.cache_creation_input_tokens = cache_creation_input_tokens;
        }
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
        let mut deadline = Box::pin(sleep(self.batch.max_age));
        reset_deadline(&mut deadline, self.batch.max_age);

        loop {
            tokio::select! {
                changed = cancel_rx.changed() => {
                    if changed.is_ok() && *cancel_rx.borrow() {
                        batcher.flush(&self.obs);
                        self.observe_finished(client_disconnected_status(), None, None);
                        break;
                    }
                    if changed.is_err() {
                        batcher.flush(&self.obs);
                        self.observe_finished(client_disconnected_status(), None, None);
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
                                if self.drain_complete_events(&mut buffer, &tx, &mut batcher, &mut deadline, &mut usage).await.is_err() {
                                    self.observe_finished(client_disconnected_status(), None, None);
                                    break;
                                }
                            }
                        }
                        Some(Err(source)) => {
                            batcher.flush(&self.obs);
                            let _sent = tx.send(self.error_frame_for_unknown_status(&source.to_string())).await;
                            self.observe_error("upstream_read_failed", &source.to_string());
                            self.observe_finished(StatusCode::BAD_GATEWAY, Some(usage.input_tokens), Some(usage.output_tokens));
                            break;
                        }
                        None => {
                            batcher.flush(&self.obs);
                            self.finish_usage(&usage).await;
                            self.observe_finished(StatusCode::OK, Some(usage.input_tokens), Some(usage.output_tokens));
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn drain_complete_events(
        &self,
        buffer: &mut BytesMut,
        tx: &mpsc::Sender<Bytes>,
        batcher: &mut SseBatcher,
        deadline: &mut Pin<Box<Sleep>>,
        usage: &mut StreamingUsage,
    ) -> Result<(), ()> {
        while let Some(end) = find_sse_event_end(buffer) {
            let raw = buffer.split_to(end).freeze();
            let mut outgoing = raw.clone();
            match parse_one_event(raw).await {
                Ok(Some(event)) => {
                    if event.event == "error" {
                        outgoing = self.error_frame_from_event(&event, outgoing);
                    } else {
                        update_usage_from_event(&event, usage);
                        self.store_usage(*usage);
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

    fn error_frame_from_event(&self, event: &Event, raw_fallback: Bytes) -> Bytes {
        let data = Bytes::from(event.data.clone());
        if let (Some(normalizer), Some(kind)) = (self.error_normalizer.as_ref(), self.upstream_kind)
        {
            return normalizer.normalize_sse_error_frame(kind, &data);
        }

        match self
            .dialect
            .normalize_error(StatusCode::INTERNAL_SERVER_ERROR, &data)
        {
            Some(json) => match serde_json::from_slice::<Value>(&json) {
                Ok(value) => make_error_frame_from_json(&value),
                Err(_source) => raw_fallback,
            },
            None => raw_fallback,
        }
    }

    fn error_frame_for_unknown_status(&self, message: &str) -> Bytes {
        let empty = Bytes::new();
        self.dialect
            .normalize_error(StatusCode::BAD_GATEWAY, &empty)
            .map(|json| match serde_json::from_slice::<Value>(&json) {
                Ok(value) => crate::sse_error_frame::make_error_frame_from_json(&value),
                Err(_source) => make_error_frame("api_error", message),
            })
            .unwrap_or_else(|| make_error_frame("api_error", message))
    }

    async fn finish_usage(&self, usage: &StreamingUsage) {
        if usage.complete {
            return;
        }

        metrics::counter!(
            "cclb_streaming_usage_missing_total",
            "dialect" => streaming_usage_dialect_label(self.upstream_kind)
        )
        .increment(1);
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
    ) {
        let _result = self.obs.observe(ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            duration_ms: self
                .started
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        });
    }
}

fn streaming_usage_dialect_label(upstream_kind: Option<UpstreamKind>) -> &'static str {
    match upstream_kind {
        Some(UpstreamKind::BedrockRuntime | UpstreamKind::BedrockMantle) => "bedrock",
        Some(UpstreamKind::Vertex) => "vertex",
        Some(UpstreamKind::AnthropicDirect | UpstreamKind::CustomAnthropicSpec) | None => {
            "anthropic"
        }
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

fn update_usage_from_event(event: &Event, usage: &mut StreamingUsage) {
    let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
        return;
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
        }
        _ => {}
    }
}

pub(crate) fn usage_from_json_bytes(bytes: &Bytes) -> StreamingUsage {
    let mut usage = StreamingUsage::default();
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
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
    if let Some(cache_creation_input_tokens) = reported_usage
        .and_then(|usage| usage.get("cache_creation_input_tokens"))
        .and_then(Value::as_u64)
    {
        usage.cache_creation_input_tokens = cache_creation_input_tokens;
    }
    if let Some(cache_read_input_tokens) = reported_usage
        .and_then(|usage| usage.get("cache_read_input_tokens"))
        .and_then(Value::as_u64)
    {
        usage.cache_read_input_tokens = cache_read_input_tokens;
    }
}

pub(crate) fn find_sse_event_end(buffer: &[u8]) -> Option<usize> {
    let mut index = 0;
    while index < buffer.len() {
        if buffer[index] == b'\n' && buffer.get(index + 1) == Some(&b'\n') {
            return Some(index + 2);
        }
        if buffer[index] == b'\r' {
            if buffer.get(index + 1) == Some(&b'\r') {
                return Some(index + 2);
            }
            if buffer.get(index + 1) == Some(&b'\n')
                && buffer.get(index + 2) == Some(&b'\r')
                && buffer.get(index + 3) == Some(&b'\n')
            {
                return Some(index + 4);
            }
        }
        index += 1;
    }
    None
}

fn reset_deadline(deadline: &mut Pin<Box<Sleep>>, max_age: Duration) {
    deadline
        .as_mut()
        .reset(TokioInstant::now() + max_age.max(Duration::from_millis(1)));
}

fn client_disconnected_status() -> StatusCode {
    StatusCode::from_u16(CLIENT_DISCONNECTED_STATUS).unwrap_or(StatusCode::BAD_GATEWAY)
}
