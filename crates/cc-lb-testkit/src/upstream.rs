use std::{
    collections::VecDeque,
    convert::Infallible,
    future::{Ready, ready},
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use tower::Service;

pub type ScriptedRequest = Request<Full<Bytes>>;
pub type ScriptedResponse = Response<Full<Bytes>>;

#[derive(Clone, Default)]
pub struct ScriptedUpstream {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    requests: Mutex<Vec<ScriptedRequest>>,
    responses: Mutex<VecDeque<ScriptedResponse>>,
}

impl ScriptedUpstream {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_response(&self, response: ScriptedResponse) {
        lock(&self.inner.responses).push_back(response);
    }

    #[must_use]
    pub fn requests(&self) -> Vec<ScriptedRequest> {
        lock(&self.inner.requests).clone()
    }

    #[must_use]
    pub fn pending_responses(&self) -> usize {
        lock(&self.inner.responses).len()
    }
}

impl Service<ScriptedRequest> for ScriptedUpstream {
    type Response = ScriptedResponse;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ScriptedRequest) -> Self::Future {
        lock(&self.inner.requests).push(request);
        let response = lock(&self.inner.responses).pop_front();
        ready(Ok(
            response.expect("ScriptedUpstream response queue exhausted")
        ))
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("ScriptedUpstream mutex poisoned")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::{Request, Response, StatusCode};
    use http_body_util::{BodyExt, Full};
    use tower::Service;

    use super::ScriptedUpstream;

    #[tokio::test]
    async fn responses_are_fifo_and_requests_are_recorded() {
        let mut upstream = ScriptedUpstream::new();
        upstream.push_response(
            Response::builder()
                .status(StatusCode::CREATED)
                .body(Full::new(Bytes::from_static(b"first")))
                .expect("first response"),
        );
        upstream.push_response(
            Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Full::new(Bytes::from_static(b"second")))
                .expect("second response"),
        );

        let first = upstream
            .call(
                Request::builder()
                    .uri("/first")
                    .body(Full::new(Bytes::from_static(b"one")))
                    .expect("first request"),
            )
            .await
            .expect("infallible");
        let second = upstream
            .call(
                Request::builder()
                    .uri("/second")
                    .body(Full::new(Bytes::from_static(b"two")))
                    .expect("second request"),
            )
            .await
            .expect("infallible");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(
            first.into_body().collect().await.expect("body").to_bytes(),
            Bytes::from_static(b"first")
        );
        assert_eq!(second.status(), StatusCode::ACCEPTED);
        assert_eq!(
            upstream
                .requests()
                .iter()
                .map(|request| request.uri().path())
                .collect::<Vec<_>>(),
            ["/first", "/second"]
        );
        assert_eq!(upstream.pending_responses(), 0);
    }
}
