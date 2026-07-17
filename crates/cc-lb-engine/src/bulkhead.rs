use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;
#[cfg(not(test))]
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_upstream::SignedRequest;
use dashmap::DashMap;
use http::{HeaderMap, Request, Response};
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connect, HttpConnector};
use hyper_util::rt::TokioExecutor;
use metrics::Unit;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[cfg(test)]
use tokio::time::Instant;

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

pub fn make_default_dispatcher(max_idle_per_host: usize) -> Arc<dyn UpstreamDispatch> {
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
    Arc::new(HttpsHyperDispatcher {
        client: build_client(connector, max_idle_per_host),
    })
}

pub fn make_http_dispatcher_with_connector(
    connector: HttpConnector,
    max_idle_per_host: usize,
) -> Arc<dyn UpstreamDispatch> {
    Arc::new(HttpHyperDispatcher {
        client: build_client(connector, max_idle_per_host),
    })
}

#[derive(Clone)]
struct HttpsHyperDispatcher {
    client: Client<
        InstrumentedHttpsConnector<hyper_rustls::HttpsConnector<CachingDnsConnector>>,
        Full<Bytes>,
    >,
}

#[derive(Clone)]
struct HttpHyperDispatcher {
    client: Client<HttpConnector, Full<Bytes>>,
}

#[async_trait]
impl UpstreamDispatch for HttpsHyperDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        dispatch_with_client(&self.client, request).await
    }
}

#[async_trait]
impl UpstreamDispatch for HttpHyperDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        dispatch_with_client(&self.client, request).await
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
pub(crate) async fn dispatch_with_client<C>(
    client: &Client<C, Full<Bytes>>,
    request: SignedRequest,
) -> Result<Response<Body>, DispatchError>
where
    C: Connect + Clone + Send + Sync + 'static,
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

    #[tokio::test]
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

        assert!(
            timings.bulkhead_wait_ms.unwrap_or(0) <= 10,
            "expected <=10ms, got {:?}",
            timings.bulkhead_wait_ms
        );
    }

    #[tokio::test(start_paused = true)]
    async fn high_contention_forces_wait_ms() {
        use crate::request_timing::with_timings;

        let bulkhead = bulkhead_for_test(1);
        let permit_holder = bulkhead.acquire().await.expect("acquire");
        let release_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let execute = with_timings(async {
            bulkhead
                .execute(signed_request().await)
                .await
                .expect("bulkhead execute succeeds");
        });
        tokio::pin!(execute);
        tokio::select! {
            release_result = release_handle => {
                release_result.expect("release task");
                drop(permit_holder);
            }
            _ = &mut execute => panic!("execute completed before permit release"),
        }
        let (_, timings) = execute.await;

        assert!(
            timings.bulkhead_wait_ms.unwrap_or(0) >= 90,
            "expected >=90ms wait, got {:?}",
            timings.bulkhead_wait_ms
        );
    }

    #[tokio::test]
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

    async fn signed_request() -> SignedRequest {
        let upstream = Upstream::AnthropicDirect { base_url: None };
        let context = DialectShapeContext {
            request_id: "test-request".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
        };
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };
        let shaped = shape_request(&PassthroughDialect, &context, &upstream, &principal)
            .expect("test request shapes");

        sign_request(&NoopSigner, shaped)
            .await
            .expect("test request signs")
    }

    struct PassthroughDialect;

    impl UpstreamDialect for PassthroughDialect {
        fn shape(
            &self,
            context: &DialectShapeContext,
            _upstream: &Upstream,
            _principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            let mut url = Url::parse("http://upstream.local/").expect("test URL parses");
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
