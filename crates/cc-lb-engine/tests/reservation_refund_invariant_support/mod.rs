use std::convert::Infallible;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, UpstreamDispatch};
use cc_lb_plugin_api::SignedRequest;
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
use tokio::sync::Notify;

pub fn sse_dispatch(status: StatusCode, body: Body) -> Arc<dyn UpstreamDispatch> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    Arc::new(OneResponseDispatch::new(response))
}

pub fn json_dispatch(status: StatusCode, body: Bytes) -> Arc<dyn UpstreamDispatch> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Arc::new(OneResponseDispatch::new(response))
}

pub fn pending_sse(waiting: Arc<Notify>) -> Body {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, Infallible>(normal_sse_frame());
        waiting.notify_one();
        std::future::pending::<()>().await;
    };
    Body::from_stream(stream)
}

pub fn normal_sse_frame() -> Bytes {
    Bytes::from_static(
        b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0}\n\n",
    )
}

struct OneResponseDispatch {
    response: std::sync::Mutex<Option<Response<Body>>>,
}

impl OneResponseDispatch {
    fn new(response: Response<Body>) -> Self {
        Self {
            response: std::sync::Mutex::new(Some(response)),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for OneResponseDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.response
            .lock()
            .map_err(|_| DispatchError::Transport {
                reason: "test response lock poisoned".to_owned(),
            })?
            .take()
            .ok_or_else(|| DispatchError::Transport {
                reason: "test response already consumed".to_owned(),
            })
    }
}
