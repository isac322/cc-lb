use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::header;
use axum::http::{Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use tower::{Layer, Service};

type ChaosFuture<T> = Pin<Box<DynChaosFuture<T>>>;
type DynChaosFuture<T> = dyn Future<Output = T> + Send;

const LATENCY_ENV: &str = "CC_LB_CHAOS_LATENCY_MS";
const DROP_ENV: &str = "CC_LB_CHAOS_DROP_PCT";
const RST_ENV: &str = "CC_LB_CHAOS_RST_AFTER_BYTES";
const TRUNCATE_ENV: &str = "CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS";

#[derive(Clone, Debug, Default)]
pub struct ChaosConfig {
    latency_ms: u64,
    drop_pct: u8,
    rst_after_bytes: u64,
    truncate_after_events: u64,
}

impl ChaosConfig {
    pub fn from_env() -> Self {
        Self {
            latency_ms: parse_u64_env(LATENCY_ENV),
            drop_pct: parse_drop_pct_env(),
            rst_after_bytes: parse_u64_env(RST_ENV),
            truncate_after_events: parse_u64_env(TRUNCATE_ENV),
        }
    }

    fn disabled(&self) -> bool {
        self.latency_ms == 0
            && self.drop_pct == 0
            && self.rst_after_bytes == 0
            && self.truncate_after_events == 0
    }

    fn should_drop(&self) -> bool {
        self.drop_pct > 0 && rand::random_range(0_u8..100) < self.drop_pct
    }
}

#[derive(Clone, Debug)]
pub struct ChaosLayer {
    config: ChaosConfig,
}

impl ChaosLayer {
    pub fn from_env() -> Self {
        Self {
            config: ChaosConfig::from_env(),
        }
    }
}

impl<S> Layer<S> for ChaosLayer {
    type Service = ChaosService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ChaosService {
            inner,
            config: self.config.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChaosService<S> {
    inner: S,
    config: ChaosConfig,
}

impl<S> Service<Request<Body>> for ChaosService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = ChaosFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let config = self.config.clone();
        if config.disabled() {
            let future = self.inner.call(request);
            return Box::pin(future);
        }
        if config.should_drop() {
            return Box::pin(async { Ok(drop_response()) });
        }

        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            if config.latency_ms > 0 {
                tokio::time::sleep(Duration::from_millis(config.latency_ms)).await;
            }
            Ok(apply_body_chaos(response, &config))
        })
    }
}

fn apply_body_chaos(response: Response<Body>, config: &ChaosConfig) -> Response<Body> {
    let is_sse = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.starts_with("text/event-stream"))
        .unwrap_or(false);

    if is_sse && config.truncate_after_events > 0 {
        return truncate_sse_response(response, config.truncate_after_events);
    }
    if config.rst_after_bytes > 0 {
        return truncate_bytes_response(response, config.rst_after_bytes);
    }
    response
}

fn drop_response() -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    response
}

fn truncate_bytes_response(response: Response<Body>, limit: u64) -> Response<Body> {
    let (parts, mut body) = response.into_parts();
    let stream = async_stream::stream! {
        let mut remaining = limit;
        loop {
            if remaining == 0 {
                yield Err::<Bytes, axum::Error>(chaos_body_error("chaos rst after bytes"));
                break;
            }
            let Some(frame) = body.frame().await else {
                break;
            };
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        let data_len = data.len() as u64;
                        if data_len <= remaining {
                            remaining -= data_len;
                            yield Ok::<Bytes, axum::Error>(data);
                        } else {
                            let take = usize::try_from(remaining).unwrap_or(usize::MAX);
                            if take > 0 {
                                yield Ok::<Bytes, axum::Error>(data.slice(..take));
                            }
                            yield Err::<Bytes, axum::Error>(chaos_body_error("chaos rst after bytes"));
                            break;
                        }
                    }
                }
                Err(source) => {
                    yield Err::<Bytes, axum::Error>(source);
                    break;
                }
            }
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

fn truncate_sse_response(response: Response<Body>, event_limit: u64) -> Response<Body> {
    let (parts, mut body) = response.into_parts();
    let stream = async_stream::stream! {
        let mut seen = 0_u64;
        let mut line_start = true;
        loop {
            if seen >= event_limit {
                break;
            }
            let Some(frame) = body.frame().await else {
                break;
            };
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        let (chunk, next_seen, next_line_start) = sse_prefix_through_events(
                            &data,
                            seen,
                            event_limit,
                            line_start,
                        );
                        seen = next_seen;
                        line_start = next_line_start;
                        if !chunk.is_empty() {
                            yield Ok::<Bytes, axum::Error>(chunk);
                        }
                    }
                }
                Err(source) => {
                    yield Err::<Bytes, axum::Error>(source);
                    break;
                }
            }
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

fn sse_prefix_through_events(
    data: &Bytes,
    mut seen: u64,
    limit: u64,
    mut line_start: bool,
) -> (Bytes, u64, bool) {
    let mut end = data.len();
    let bytes = data.as_ref();
    let mut index = 0_usize;
    while index < bytes.len() {
        if line_start && bytes[index..].starts_with(b"data:") {
            seen = seen.saturating_add(1);
            if seen > limit {
                end = index;
                break;
            }
        }
        line_start = bytes[index] == b'\n';
        index += 1;
    }
    (data.slice(..end), seen, line_start)
}

fn chaos_body_error(message: &'static str) -> axum::Error {
    axum::Error::new(std::io::Error::other(message))
}

fn parse_u64_env(name: &str) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

fn parse_drop_pct_env() -> u8 {
    std::env::var(DROP_ENV)
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|value| *value <= 100)
        .unwrap_or(0)
}
