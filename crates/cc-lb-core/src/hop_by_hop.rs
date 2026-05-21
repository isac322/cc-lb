use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use http::header::{
    CONNECTION, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderName, Request, Response};
use tower::{Layer, Service};

const HOP_BY_HOP_HEADERS: [HeaderName; 9] = [
    HeaderName::from_static("connection"),
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-authenticate"),
    HeaderName::from_static("proxy-authorization"),
    HeaderName::from_static("te"),
    HeaderName::from_static("trailer"),
    HeaderName::from_static("transfer-encoding"),
    HeaderName::from_static("upgrade"),
    HeaderName::from_static("proxy-connection"),
];

pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let connection_names = connection_listed_headers(headers);
    let mut removal_targets = Vec::new();

    for (name, _) in headers.iter() {
        if is_hop_by_hop(name, &connection_names) {
            removal_targets.push(name.clone());
        }
    }

    for name in removal_targets {
        headers.remove(name);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HopByHopStripLayer;

impl HopByHopStripLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for HopByHopStripLayer {
    type Service = HopByHopStripService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HopByHopStripService::new(inner)
    }
}

#[derive(Clone, Debug)]
pub struct HopByHopStripService<S> {
    inner: S,
}

impl<S> HopByHopStripService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for HopByHopStripService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        strip_hop_by_hop(request.headers_mut());
        let future = self.inner.call(request);

        Box::pin(async move {
            let mut response = future.await?;
            strip_hop_by_hop(response.headers_mut());
            Ok(response)
        })
    }
}

fn connection_listed_headers(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .filter_map(|token| HeaderName::from_lowercase(token.to_ascii_lowercase().as_bytes()).ok())
        .collect()
}

fn is_hop_by_hop(name: &HeaderName, connection_names: &HashSet<HeaderName>) -> bool {
    HOP_BY_HOP_HEADERS.contains(name)
        || connection_names.contains(name)
        || *name == TE
        || *name == TRAILER
        || *name == UPGRADE
        || *name == PROXY_AUTHENTICATE
        || *name == PROXY_AUTHORIZATION
        || *name == TRANSFER_ENCODING
}
