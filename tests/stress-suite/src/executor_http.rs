use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::time::{Instant, timeout};
use tower::Service;

use crate::executor::{RequestSender, SendFuture};
use crate::executor_metrics::{ConnectionCounts, ExecutionTerminal, RequestObservation};
use crate::manifest::PlannedRequest;
use crate::sse_timing::{SseTiming, duration_ms};
use crate::traffic::SlowReaderPolicy;

const MAX_SSE_FRAME_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct HttpRequestSender {
    client: Client<CountingConnector, Full<Bytes>>,
    endpoint: Uri,
    request_timeout: Duration,
    connections: Arc<ConnectionTracker>,
}

impl HttpRequestSender {
    pub fn new(endpoint: Uri, request_timeout: Duration) -> Self {
        let connections = Arc::new(ConnectionTracker::default());
        let connector = CountingConnector::new(Arc::clone(&connections));
        Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
            endpoint,
            request_timeout,
            connections,
        }
    }
}

impl RequestSender for HttpRequestSender {
    fn send(&self, request: PlannedRequest) -> SendFuture {
        self.connections.requests.fetch_add(1, Ordering::Relaxed);
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        let request_timeout = self.request_timeout;
        Box::pin(async move {
            match timeout(request_timeout, send_request(client, endpoint, request)).await {
                Ok(observation) => observation,
                Err(_) => RequestObservation::timed_out(duration_ms(request_timeout)),
            }
        })
    }

    fn connection_counts(&self) -> ConnectionCounts {
        let new = self.connections.new.load(Ordering::Relaxed);
        ConnectionCounts {
            new,
            reused: self
                .connections
                .requests
                .load(Ordering::Relaxed)
                .saturating_sub(new),
        }
    }
}

async fn send_request(
    client: Client<CountingConnector, Full<Bytes>>,
    endpoint: Uri,
    planned: PlannedRequest,
) -> RequestObservation {
    let started_at = Instant::now();
    let mut builder = Request::post(endpoint).header("content-type", "application/json");
    if planned.stream {
        builder = builder.header("x-stress-stream", "true");
    }
    for (name, value) in &planned.provider_headers {
        builder = builder.header(name, value);
    }
    let request = match builder.body(Full::new(Bytes::from(planned.body))) {
        Ok(request) => request,
        Err(_) => return RequestObservation::transport_error(duration_ms(started_at.elapsed())),
    };
    let response = match client.request(request).await {
        Ok(response) => response,
        Err(_) => return RequestObservation::transport_error(duration_ms(started_at.elapsed())),
    };
    let status = response.status().as_u16();
    if planned.stream {
        read_stream(
            response.into_body(),
            status,
            planned.slow_reader_policy,
            started_at,
        )
        .await
    } else {
        read_json(response.into_body(), status, started_at).await
    }
}

async fn read_stream(
    mut body: Incoming,
    status: u16,
    policy: SlowReaderPolicy,
    started_at: Instant,
) -> RequestObservation {
    let mut parser = SseTiming::new(MAX_SSE_FRAME_BYTES);
    let mut ttfb_ms = None;
    let mut chunks = 0_u16;
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            return RequestObservation::transport_error(duration_ms(started_at.elapsed()));
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let elapsed = started_at.elapsed();
        ttfb_ms.get_or_insert_with(|| duration_ms(elapsed));
        chunks = chunks.saturating_add(1);
        parser.push(&data, elapsed);
        match policy {
            SlowReaderPolicy::Eager => {}
            SlowReaderPolicy::Paced { delay_ms } => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await
            }
            SlowReaderPolicy::CancelAfterChunks { chunks: limit } if chunks >= limit => {
                return RequestObservation {
                    status: Some(status),
                    ttfb_ms,
                    latency_ms: duration_ms(started_at.elapsed()),
                    terminal: ExecutionTerminal::ClientCancelled,
                    stream: None,
                };
            }
            SlowReaderPolicy::CancelAfterChunks { .. } => {}
        }
    }
    let summary = parser.finish();
    let terminal =
        if summary.malformed_sse || summary.malformed_json || summary.unexpected_truncation {
            ExecutionTerminal::MalformedResponse
        } else {
            ExecutionTerminal::Response(status)
        };
    RequestObservation {
        status: Some(status),
        ttfb_ms,
        latency_ms: duration_ms(started_at.elapsed()),
        terminal,
        stream: Some(summary),
    }
}

async fn read_json(mut body: Incoming, status: u16, started_at: Instant) -> RequestObservation {
    let mut bytes = Vec::new();
    let mut ttfb_ms = None;
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            return RequestObservation::transport_error(duration_ms(started_at.elapsed()));
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        ttfb_ms.get_or_insert_with(|| duration_ms(started_at.elapsed()));
        bytes.extend_from_slice(&data);
    }
    let terminal = if serde_json::from_slice::<serde_json::Value>(&bytes).is_ok() {
        ExecutionTerminal::Response(status)
    } else {
        ExecutionTerminal::MalformedResponse
    };
    RequestObservation {
        status: Some(status),
        ttfb_ms,
        latency_ms: duration_ms(started_at.elapsed()),
        terminal,
        stream: None,
    }
}

#[derive(Default)]
struct ConnectionTracker {
    new: AtomicU64,
    requests: AtomicU64,
}

#[derive(Clone)]
struct CountingConnector {
    inner: HttpConnector,
    tracker: Arc<ConnectionTracker>,
}

impl CountingConnector {
    fn new(tracker: Arc<ConnectionTracker>) -> Self {
        Self {
            inner: HttpConnector::new(),
            tracker,
        }
    }
}

impl Service<Uri> for CountingConnector {
    type Response = <HttpConnector as Service<Uri>>::Response;
    type Error = <HttpConnector as Service<Uri>>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let tracker = Arc::clone(&self.tracker);
        let future = self.inner.call(uri);
        Box::pin(async move {
            let result = future.await;
            if result.is_ok() {
                tracker.new.fetch_add(1, Ordering::Relaxed);
            }
            result
        })
    }
}
