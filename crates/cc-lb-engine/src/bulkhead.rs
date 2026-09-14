use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use tokio::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_upstream::SignedRequest;
use dashmap::DashMap;
use http::{HeaderMap, Request, Response};
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::rt::TokioExecutor;
use metrics::Unit;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::dns_cache::{CachingDnsConnector, DnsResolverConfig};
use crate::instrumented_connector::InstrumentedHttpsConnector;
use crate::lifecycle::{Body, DispatchError, UpstreamDispatch};

const DEFAULT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

static REGISTER_BULKHEAD_METRICS: Once = Once::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BulkheadRuntimeConfig {
    pub max_conns_per_upstream: u32,
    pub semaphore_permits: u32,
    pub acquire_timeout: Duration,
}

impl From<cc_lb_config::BulkheadConfig> for BulkheadRuntimeConfig {
    fn from(config: cc_lb_config::BulkheadConfig) -> Self {
        Self {
            max_conns_per_upstream: config.max_conns_per_upstream,
            semaphore_permits: config.semaphore_per_upstream,
            acquire_timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum BulkheadError {
    #[error("bulkhead queue full; retry after {retry_after:?}")]
    QueueFull { retry_after: Duration },
}

#[derive(Debug, Error)]
pub enum ExecuteError {
    #[error("bulkhead queue full; retry after {0:?}")]
    BulkheadFull(Duration),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

pub struct Bulkhead {
    pub upstream_name: String,
    pub semaphore: Arc<Semaphore>,
    pub in_flight: AtomicU32,
    pub client: Arc<dyn UpstreamDispatch>,
    pub config: BulkheadRuntimeConfig,
}

impl Bulkhead {
    pub fn new(
        upstream_name: impl Into<String>,
        config: BulkheadRuntimeConfig,
        dispatcher: Arc<dyn UpstreamDispatch>,
    ) -> Arc<Self> {
        register_bulkhead_metrics();
        let bulkhead = Arc::new(Self {
            upstream_name: upstream_name.into(),
            semaphore: Arc::new(Semaphore::new(config.semaphore_permits as usize)),
            in_flight: AtomicU32::new(0),
            client: dispatcher,
            config,
        });
        bulkhead.emit_active(0);
        bulkhead
    }

    pub async fn execute(&self, signed: SignedRequest) -> Result<Response<Body>, ExecuteError> {
        let wait_start = Instant::now();
        let _guard = self.acquire().await.map_err(|source| match source {
            BulkheadError::QueueFull { retry_after } => ExecuteError::BulkheadFull(retry_after),
        })?;
        crate::request_timing::record_bulkhead_wait(wait_start.elapsed());
        self.client
            .dispatch(signed)
            .await
            .map_err(ExecuteError::Dispatch)
    }

    pub async fn acquire(&self) -> Result<BulkheadGuard<'_>, BulkheadError> {
        let permit = tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| BulkheadError::QueueFull {
            retry_after: self.config.acquire_timeout,
        })?
        .map_err(|_| BulkheadError::QueueFull {
            retry_after: self.config.acquire_timeout,
        })?;

        let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.emit_active(active);
        Ok(BulkheadGuard {
            bulkhead: self,
            _permit: permit,
        })
    }

    pub fn active(&self) -> u32 {
        self.in_flight.load(Ordering::SeqCst)
    }

    fn emit_active(&self, active: u32) {
        metrics::gauge!(
            "cc_lb_bulkhead_active",
            "upstream" => self.upstream_name.clone()
        )
        .set(f64::from(active));
    }
}

pub struct BulkheadGuard<'a> {
    bulkhead: &'a Bulkhead,
    _permit: OwnedSemaphorePermit,
}

impl Drop for BulkheadGuard<'_> {
    fn drop(&mut self) {
        let active = self
            .bulkhead
            .in_flight
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        self.bulkhead.emit_active(active);
    }
}

#[derive(Default)]
pub struct BulkheadRegistry {
    pub map: DashMap<String, Arc<Bulkhead>>,
}

impl BulkheadRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bulkhead(
        &self,
        upstream_name: impl Into<String>,
        config: BulkheadRuntimeConfig,
    ) -> Arc<Bulkhead> {
        self.bulkhead_with_factory(upstream_name, config, || {
            make_default_dispatcher(config.max_conns_per_upstream as usize)
        })
    }

    pub fn bulkhead_with_dispatcher(
        &self,
        upstream_name: impl Into<String>,
        config: BulkheadRuntimeConfig,
        dispatcher: Arc<dyn UpstreamDispatch>,
    ) -> Arc<Bulkhead> {
        self.bulkhead_with_factory(upstream_name, config, || dispatcher)
    }

    pub fn bulkhead_with_factory<F>(
        &self,
        upstream_name: impl Into<String>,
        config: BulkheadRuntimeConfig,
        dispatcher_factory: F,
    ) -> Arc<Bulkhead>
    where
        F: FnOnce() -> Arc<dyn UpstreamDispatch>,
    {
        let upstream_name = upstream_name.into();
        self.map
            .entry(upstream_name.clone())
            .or_insert_with(|| Bulkhead::new(upstream_name, config, dispatcher_factory()))
            .clone()
    }

    pub fn get(&self, upstream_name: &str) -> Option<Arc<Bulkhead>> {
        self.map
            .get(upstream_name)
            .map(|bulkhead| Arc::clone(bulkhead.value()))
    }
}

pub struct BulkheadDispatch {
    registry: Arc<BulkheadRegistry>,
    config: BulkheadRuntimeConfig,
    upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
    dispatcher_factory: Arc<dyn Fn(usize) -> Arc<dyn UpstreamDispatch> + Send + Sync>,
}

impl BulkheadDispatch {
    pub fn new(
        registry: Arc<BulkheadRegistry>,
        config: BulkheadRuntimeConfig,
        upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
    ) -> Self {
        Self::with_dispatcher_factory(
            registry,
            config,
            upstream_name,
            Arc::new(make_default_dispatcher),
        )
    }

    pub fn with_dispatcher_factory(
        registry: Arc<BulkheadRegistry>,
        config: BulkheadRuntimeConfig,
        upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
        dispatcher_factory: Arc<dyn Fn(usize) -> Arc<dyn UpstreamDispatch> + Send + Sync>,
    ) -> Self {
        Self {
            registry,
            config,
            upstream_name,
            dispatcher_factory,
        }
    }
}

#[async_trait]
impl UpstreamDispatch for BulkheadDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let upstream_name = (self.upstream_name)(&request);
        let config = self.config;
        let dispatcher_factory = Arc::clone(&self.dispatcher_factory);
        let bulkhead = self
            .registry
            .bulkhead_with_factory(upstream_name, config, || {
                dispatcher_factory(config.max_conns_per_upstream as usize)
            });
        bulkhead
            .execute(request)
            .await
            .map_err(dispatch_error_from_execute)
    }
}

#[async_trait]
impl UpstreamDispatch for Bulkhead {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.execute(request)
            .await
            .map_err(dispatch_error_from_execute)
    }
}

#[async_trait]
impl UpstreamDispatch for Arc<Bulkhead> {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.execute(request)
            .await
            .map_err(dispatch_error_from_execute)
    }
}

type DefaultHttpsConnector =
    InstrumentedHttpsConnector<hyper_rustls::HttpsConnector<CachingDnsConnector>>;
type DefaultHttpsClient = Client<DefaultHttpsConnector, Full<Bytes>>;

pub fn make_default_dispatcher(max_idle_per_host: usize) -> Arc<dyn UpstreamDispatch> {
    Arc::new(build_default_https_dispatcher(max_idle_per_host))
}

fn build_default_https_dispatcher(
    max_idle_per_host: usize,
) -> HttpsHyperDispatcher<DefaultHttpsClient> {
    let caching_http = CachingDnsConnector::new(&DnsResolverConfig::default())
        .expect("default DNS resolver builds");
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .wrap_connector(caching_http);
    let connector = InstrumentedHttpsConnector::new(connector);
    tracing::info!("dispatcher built with caching DNS + instrumented HTTPS connector");
    HttpsHyperDispatcher {
        client: build_client(connector, max_idle_per_host),
    }
}

#[async_trait]
trait DispatcherClient: Send + Sync {
    type ResponseBody: http_body::Body<Data = Bytes> + Send + 'static;
    type Error: std::fmt::Display + Send + Sync + 'static;

    async fn request(
        &self,
        request: Request<Full<Bytes>>,
    ) -> Result<Response<Self::ResponseBody>, Self::Error>;
}

#[async_trait]
impl<C> DispatcherClient for Client<C, Full<Bytes>>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    type ResponseBody = hyper::body::Incoming;
    type Error = hyper_util::client::legacy::Error;

    async fn request(
        &self,
        request: Request<Full<Bytes>>,
    ) -> Result<Response<Self::ResponseBody>, Self::Error> {
        Client::request(self, request).await
    }
}

#[derive(Clone)]
struct HttpsHyperDispatcher<C> {
    client: C,
}

#[async_trait]
impl<C> UpstreamDispatch for HttpsHyperDispatcher<C>
where
    C: DispatcherClient + 'static,
    <C::ResponseBody as http_body::Body>::Error: Into<axum::BoxError>,
{
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        dispatch_with_dispatcher_client(&self.client, request).await
    }
}

fn build_client<C>(connector: C, max_idle_per_host: usize) -> Client<C, Full<Bytes>>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    let mut builder = Client::builder(TokioExecutor::new());
    builder.pool_idle_timeout(DEFAULT_POOL_IDLE_TIMEOUT);
    builder.pool_max_idle_per_host(max_idle_per_host);
    builder.build(connector)
}

fn sanitized_url(url: &url::Url) -> String {
    let mut url = url.clone();
    url.set_query(None);
    url.set_fragment(None);
    url.into()
}

fn record_dispatch_failure(span: &tracing::Span, error_type: &str) {
    span.record("otel.status_code", "ERROR");
    span.record("error.type", error_type);
}

pub(crate) async fn dispatch_with_client<C>(
    client: &Client<C, Full<Bytes>>,
    request: SignedRequest,
) -> Result<Response<Body>, DispatchError>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    dispatch_with_dispatcher_client(client, request).await
}

#[tracing::instrument(
    name = "proxy.dispatch",
    skip_all,
    fields(
        otel.kind = "client",
        otel.name = %request.method(),
        otel.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        http.request.method = %request.method(),
        http.response.status_code = tracing::field::Empty,
        server.address = request.url().host_str().unwrap_or_default(),
        server.port = request.url().port_or_known_default().unwrap_or_default(),
        url.full = %sanitized_url(request.url()),
        url.path = request.url().path(),
    )
)]
async fn dispatch_with_dispatcher_client<C>(
    client: &C,
    request: SignedRequest,
) -> Result<Response<Body>, DispatchError>
where
    C: DispatcherClient,
    <C::ResponseBody as http_body::Body>::Error: Into<axum::BoxError>,
{
    let span = tracing::Span::current();
    let (url, method, headers, body) = request.into_parts();
    let uri = url.as_str().parse::<http::Uri>().map_err(|source| {
        record_dispatch_failure(&span, "invalid_uri");
        DispatchError::InvalidUri {
            reason: source.to_string(),
        }
    })?;

    let mut builder = Request::builder().method(method).uri(uri);
    copy_headers(headers, builder.headers_mut());
    if let Some(headers) = builder.headers_mut() {
        cc_lb_observability::inject_current_trace_context(headers);
    }
    let request = builder.body(Full::new(body)).map_err(|source| {
        record_dispatch_failure(&span, "request_build");
        DispatchError::RequestBuild {
            reason: source.to_string(),
        }
    })?;

    let response = client.request(request).await.map_err(|source| {
        record_dispatch_failure(&span, "transport");
        DispatchError::Transport {
            reason: source.to_string(),
        }
    })?;
    let status = response.status();
    span.record("http.response.status_code", u64::from(status.as_u16()));
    if status.is_client_error() || status.is_server_error() {
        record_dispatch_failure(&span, status.as_str());
    }
    let (parts, body) = response.into_parts();
    Ok(Response::from_parts(parts, Body::new(body)))
}

fn copy_headers(source: HeaderMap, target: Option<&mut HeaderMap>) {
    let Some(target) = target else {
        return;
    };

    for (name, value) in source {
        if let Some(name) = name {
            target.append(name, value);
        }
    }
}

fn dispatch_error_from_execute(error: ExecuteError) -> DispatchError {
    match error {
        ExecuteError::BulkheadFull(retry_after) => DispatchError::BulkheadFull { retry_after },
        ExecuteError::Dispatch(source) => source,
    }
}

fn register_bulkhead_metrics() {
    REGISTER_BULKHEAD_METRICS.call_once(|| {
        metrics::describe_gauge!(
            "cc_lb_bulkhead_active",
            Unit::Count,
            "Active in-flight upstream requests admitted by each bulkhead."
        );
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cc_lb_domain::{Principal, PrincipalKind, Upstream};
    use cc_lb_upstream::{
        DialectError, DialectShapeContext, RetryDecision, ShapedRequest, ShapedRequestBuilder,
        SignedRequest, Signer, SignerError, SigningCapability, UpstreamDialect, UpstreamError,
        shape_request, sign_request,
    };
    use http::{HeaderMap, Method, Response};
    use url::Url;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn no_contention_wait_ms_is_under_threshold() {
        use crate::request_timing::with_timings;

        let bulkhead = bulkhead_for_test(8);
        let (_, timings) = with_timings(async {
            bulkhead
                .execute(signed_request().await)
                .await
                .expect("bulkhead execute succeeds");
        })
        .await;

        assert_eq!(timings.bulkhead_wait_ms, Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn high_contention_forces_wait_ms() {
        use crate::request_timing::with_timings;

        let bulkhead = bulkhead_for_test(1);
        let permit_holder = bulkhead.acquire().await.expect("acquire");
        let execute = tokio::spawn({
            let bulkhead = Arc::clone(&bulkhead);
            async move {
                with_timings(async {
                    bulkhead
                        .execute(signed_request().await)
                        .await
                        .expect("bulkhead execute succeeds");
                })
                .await
            }
        });
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(
            !execute.is_finished(),
            "execute completed while the permit was still held"
        );
        drop(permit_holder);
        let (_, timings) = execute.await.expect("execute task");

        assert_eq!(timings.bulkhead_wait_ms, Some(100));
    }

    #[allow(non_snake_case)]
    #[tokio::test(start_paused = true)]
    async fn t1__bulkhead_concurrency_cap_and_queue_full_returns_error() {
        let bulkhead = bulkhead_for_test(3);
        let mut permits = Vec::new();
        for _ in 0..3 {
            permits.push(
                bulkhead
                    .acquire()
                    .await
                    .expect("configured permit is admitted"),
            );
        }
        assert_eq!(bulkhead.active(), 3);

        match bulkhead.acquire().await {
            Err(error) => assert_eq!(
                error,
                BulkheadError::QueueFull {
                    retry_after: Duration::from_secs(1),
                }
            ),
            Ok(_) => panic!("request beyond the concurrency cap must be rejected"),
        }

        drop(permits);
        assert_eq!(bulkhead.active(), 0);
        bulkhead
            .acquire()
            .await
            .expect("dropping guards releases the configured slots");
    }

    #[tokio::test(start_paused = true)]
    async fn bulkhead_outside_scope_does_not_panic() {
        let bulkhead = bulkhead_for_test(4);

        bulkhead
            .execute(signed_request().await)
            .await
            .expect("bulkhead execute succeeds");
    }

    fn bulkhead_for_test(semaphore_permits: u32) -> Arc<Bulkhead> {
        Bulkhead::new(
            "test-upstream",
            BulkheadRuntimeConfig {
                max_conns_per_upstream: semaphore_permits,
                semaphore_permits,
                acquire_timeout: Duration::from_secs(1),
            },
            Arc::new(NoopDispatch),
        )
    }

    struct NoopDispatch;

    #[async_trait]
    impl UpstreamDispatch for NoopDispatch {
        async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            Ok(Response::new(Body::from(Bytes::new())))
        }
    }

    #[allow(non_snake_case)]
    mod t2__dispatcher {
        use std::convert::Infallible;
        use std::sync::{Arc, Mutex};

        use http::header::{AUTHORIZATION, CONTENT_TYPE};
        use http::{HeaderValue, StatusCode};
        use http_body_util::BodyExt;
        use tower::{Service, service_fn};

        use super::*;

        struct CapturedRequest {
            uri: http::Uri,
            method: Method,
            headers: HeaderMap,
            body: Bytes,
        }

        struct TowerServiceClient<S> {
            service: tokio::sync::Mutex<S>,
        }

        impl<S> TowerServiceClient<S> {
            fn new(service: S) -> Self {
                Self {
                    service: tokio::sync::Mutex::new(service),
                }
            }
        }

        #[async_trait]
        impl<S> DispatcherClient for TowerServiceClient<S>
        where
            S: Service<Request<Full<Bytes>>, Response = Response<Full<Bytes>>, Error = Infallible>
                + Send,
            S::Future: Send,
        {
            type ResponseBody = Full<Bytes>;
            type Error = Infallible;

            async fn request(
                &self,
                request: Request<Full<Bytes>>,
            ) -> Result<Response<Self::ResponseBody>, Self::Error> {
                self.service.lock().await.call(request).await
            }
        }

        #[tokio::test(start_paused = true)]
        async fn signed_request_is_delivered_exactly_and_response_is_mapped() {
            let captured = Arc::new(Mutex::new(None));
            let captured_by_service = Arc::clone(&captured);
            let response_body = Bytes::from_static(b"mapped response body");
            let service_response_body = response_body.clone();
            let service = service_fn(move |request: Request<Full<Bytes>>| {
                let captured = Arc::clone(&captured_by_service);
                let response_body = service_response_body.clone();
                async move {
                    let (parts, body) = request.into_parts();
                    let body = body
                        .collect()
                        .await
                        .expect("full request body is infallible")
                        .to_bytes();
                    *captured.lock().expect("capture lock is available") = Some(CapturedRequest {
                        uri: parts.uri,
                        method: parts.method,
                        headers: parts.headers,
                        body,
                    });

                    let mut response = Response::new(Full::new(response_body));
                    *response.status_mut() = StatusCode::MULTI_STATUS;
                    response.headers_mut().insert(
                        "x-response-token",
                        HeaderValue::from_static("response-value"),
                    );
                    Ok::<_, Infallible>(response)
                }
            });
            let dispatcher = HttpsHyperDispatcher {
                client: TowerServiceClient::new(service),
            };

            let mut request_headers = HeaderMap::new();
            request_headers.insert(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer signed-secret"),
            );
            request_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            request_headers.insert("x-signed-header", HeaderValue::from_static("signed-value"));
            let request_body = Bytes::from_static(br#"{"dispatch":"exact"}"#);
            let signed = signed_request_with_parts(
                Url::parse("https://api.example.test/base").expect("test URL parses"),
                request_headers.clone(),
                Method::PATCH,
                "/v1/messages",
                Some("beta=dispatcher"),
                request_body.clone(),
            )
            .await;

            let response = dispatcher
                .dispatch(signed)
                .await
                .expect("tower service dispatch succeeds");

            let captured = captured
                .lock()
                .expect("capture lock is available")
                .take()
                .expect("service received the signed request");
            assert_eq!(
                captured.uri,
                "https://api.example.test/v1/messages?beta=dispatcher"
                    .parse::<http::Uri>()
                    .expect("expected URI parses")
            );
            assert_eq!(captured.method, Method::PATCH);
            assert_eq!(captured.headers, request_headers);
            assert_eq!(captured.body, request_body);
            assert_eq!(response.status(), StatusCode::MULTI_STATUS);
            assert_eq!(
                response.headers().get("x-response-token"),
                Some(&HeaderValue::from_static("response-value"))
            );
            assert_eq!(
                response
                    .into_body()
                    .collect()
                    .await
                    .expect("mapped response body reads")
                    .to_bytes(),
                response_body
            );
        }
    }

    #[test]
    fn default_builder_returns_dispatcher_with_exact_real_hyper_client() {
        fn accepts_exact_builder(_builder: fn(usize) -> HttpsHyperDispatcher<DefaultHttpsClient>) {}

        accepts_exact_builder(build_default_https_dispatcher);
    }

    async fn signed_request() -> SignedRequest {
        signed_request_for_url(Url::parse("http://upstream.local/").expect("test URL parses")).await
    }

    async fn signed_request_for_url(base_url: Url) -> SignedRequest {
        signed_request_with_parts(
            base_url,
            HeaderMap::new(),
            Method::POST,
            "/v1/messages",
            None,
            Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
        )
        .await
    }

    async fn signed_request_with_parts(
        base_url: Url,
        downstream_headers: HeaderMap,
        method: Method,
        path: &str,
        query: Option<&str>,
        body_bytes: Bytes,
    ) -> SignedRequest {
        let upstream = Upstream::AnthropicDirect { base_url: None };
        let context = DialectShapeContext {
            request_id: "test-request".to_owned(),
            downstream_headers,
            method,
            path: path.to_owned(),
            query: query.map(str::to_owned),
            body_bytes,
        };
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };
        let shaped = shape_request(
            &PassthroughDialect { base_url },
            &context,
            &upstream,
            &principal,
        )
        .expect("test request shapes");

        sign_request(&NoopSigner, shaped)
            .await
            .expect("test request signs")
    }

    struct PassthroughDialect {
        base_url: Url,
    }

    impl UpstreamDialect for PassthroughDialect {
        fn shape(
            &self,
            context: &DialectShapeContext,
            _upstream: &Upstream,
            _principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            let mut url = self.base_url.clone();
            url.set_path(context.path.trim_start_matches('/'));
            url.set_query(context.query.as_deref());
            Ok(builder.shaped_request(
                url,
                context.method.clone(),
                context.downstream_headers.clone(),
                context.body_bytes.clone(),
            ))
        }
    }

    struct NoopSigner;

    #[async_trait]
    impl Signer for NoopSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }
}
