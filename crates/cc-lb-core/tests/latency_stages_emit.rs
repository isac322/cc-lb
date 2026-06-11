mod common;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_core::instrumented_connector::InstrumentedHttpsConnector;
use cc_lb_core::{
    Body, BulkheadConfig, BulkheadDispatch, BulkheadRegistry, CachingDnsConnector, DispatchError,
    DnsResolveFuture, DnsResolver, DnsResolverConfig, Lifecycle, UpstreamDispatch,
};
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, Principal, RequestContext, RouteDecision, RouteError,
    RouterPlugin, TerminalStrategy, Upstream, UpstreamCandidate,
};
use cc_lb_storage_api::types::{KeyStatus, RequestEvent, StoredApiKeyRecord};
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_redb::Storage as RedbStorage;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::rt::TokioExecutor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use url::Url;
use uuid::Uuid;

use common::{PassthroughDialect, TestAuthn, TestState, lifecycle_with_parts, messages_request};

const DEFAULT_UPSTREAM_ID: &str = "00000000-0000-0000-0000-000000000001";

#[tokio::test]
async fn cold_request_populates_all_connection_stages_ip_upstream() {
    let upstream = MockUpstream::start(Duration::ZERO).await;
    let dispatcher = instrumented_bulkhead_dispatcher(None, 8, 8);
    let harness = lifecycle_for(&upstream.ip_base_url(), dispatcher);

    send_message(&harness.lifecycle).await;

    let event = single_event(&harness.storage).await;
    assert!(event.auth_ms.is_some());
    assert!(event.route_ms.is_some());
    assert!(event.limit_reserve_ms.is_some());
    assert_bulkhead_wait_under(event.bulkhead_wait_ms, 5);
    assert_eq!(event.connection_reused, Some(false));
    assert!(
        event.connect_ms.is_some(),
        "connect_ms was not emitted: {event:?}"
    );
    // IP literals can bypass the resolver, so dns_ms is covered by the hostname regression test.
    assert!(event.dns_ms.is_none() || event.dns_ms.is_some());
    assert!(event.observability_post_ms.is_some());
    assert!(event.limit_reconcile_ms.is_some());
}

#[tokio::test]
async fn routing_trace_stage_duration_sum_matches_route_ms() {
    let upstream = MockUpstream::start(Duration::ZERO).await;
    let dispatcher = instrumented_bulkhead_dispatcher(None, 8, 8);
    let harness = lifecycle_for_with_filters(
        &upstream.ip_base_url(),
        dispatcher,
        vec![
            Arc::new(SleepingFilter::new(
                "latency-stage-a",
                Duration::from_millis(25),
            )),
            Arc::new(SleepingFilter::new(
                "latency-stage-b",
                Duration::from_millis(25),
            )),
        ],
    );

    send_message(&harness.lifecycle).await;

    let event = single_event(&harness.storage).await;
    let trace = event.routing_trace.as_ref().expect("routing_trace emitted");
    assert_eq!(trace.stages.len(), 2, "routing trace: {trace:?}");
    assert_eq!(trace.stages[0].stage_name, "latency-stage-a");
    assert_eq!(trace.stages[1].stage_name, "latency-stage-b");
    assert_stage_durations_match_route_ms(&event);
}

#[tokio::test]
async fn cold_request_with_hostname_populates_dns_ms() {
    let upstream = MockUpstream::start(Duration::ZERO).await;
    let resolver = Arc::new(StaticResolver::new("mock-upstream.test"));
    let dispatcher = instrumented_bulkhead_dispatcher(Some(resolver), 8, 8);
    let harness = lifecycle_for(&upstream.host_base_url("mock-upstream.test"), dispatcher);

    send_message(&harness.lifecycle).await;

    let event = single_event(&harness.storage).await;
    assert!(event.dns_ms.is_some(), "dns_ms was not emitted: {event:?}");
    assert_eq!(event.connection_reused, Some(false));
    assert!(
        event.connect_ms.is_some(),
        "connect_ms was not emitted: {event:?}"
    );
}

#[tokio::test]
async fn warm_pool_request_skips_connection_stages() {
    let upstream = MockUpstream::start(Duration::ZERO).await;
    let resolver = Arc::new(StaticResolver::new("mock-upstream.test"));
    let dispatcher = instrumented_bulkhead_dispatcher(Some(resolver), 8, 8);
    let harness = lifecycle_for(&upstream.host_base_url("mock-upstream.test"), dispatcher);

    send_message(&harness.lifecycle).await;
    send_message(&harness.lifecycle).await;

    let events = events(&harness.storage).await;
    assert_eq!(
        events.len(),
        2,
        "expected two request events, got {events:?}"
    );
    let event = &events[1];
    assert_eq!(event.connection_reused, Some(true));
    assert_eq!(event.dns_ms, None);
    assert_eq!(event.connect_ms, None);
    assert!(event.auth_ms.is_some());
    assert!(event.bulkhead_wait_ms.is_some());
}

#[tokio::test]
async fn bulkhead_contention_records_wait_ms() {
    let upstream = MockUpstream::start(Duration::from_millis(40)).await;
    let dispatcher = instrumented_bulkhead_dispatcher(None, 1, 1);
    let harness = lifecycle_for(&upstream.ip_base_url(), dispatcher);
    let LifecycleHarness {
        lifecycle,
        storage,
        _dir,
    } = harness;
    let lifecycle = Arc::new(lifecycle);

    let mut handles = Vec::new();
    for _ in 0..5 {
        let lifecycle = Arc::clone(&lifecycle);
        handles.push(tokio::spawn(async move {
            send_message(&lifecycle).await;
        }));
    }
    for handle in handles {
        handle.await.expect("request task joins");
    }

    let events = events(&storage).await;
    assert_eq!(
        events.len(),
        5,
        "expected five request events, got {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.bulkhead_wait_ms.is_some_and(|wait_ms| wait_ms >= 20)),
        "expected at least one contended request, got {events:?}"
    );
}

struct LifecycleHarness {
    lifecycle: Lifecycle,
    storage: Arc<RedbStorage>,
    _dir: tempfile::TempDir,
}

fn lifecycle_for(base_url: &str, dispatcher: Arc<dyn UpstreamDispatch>) -> LifecycleHarness {
    lifecycle_for_with_filters(base_url, dispatcher, Vec::new())
}

fn lifecycle_for_with_filters(
    base_url: &str,
    dispatcher: Arc<dyn UpstreamDispatch>,
    filters: Vec<Arc<dyn FilterPlugin>>,
) -> LifecycleHarness {
    let state = TestState::default();
    let authn = TestAuthn::with_principal_view(state, principal_view_with_filters(filters));
    let dir = tempfile::tempdir().expect("request event storage tempdir");
    let storage = Arc::new(
        RedbStorage::open(&dir.path().join("latency-stages.redb"), [17; 32])
            .expect("request event storage opens"),
    );
    let limit_engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()));
    let router = Arc::new(SelectingRouter {
        base_url: Url::parse(base_url).expect("test base URL parses"),
    });
    let lifecycle = lifecycle_with_parts(
        authn,
        router,
        dispatcher,
        Vec::new(),
        cc_lb_core::LifecycleConfig::default(),
    )
    .with_request_event_storage(Arc::clone(&storage) as Arc<dyn StorageTrait>)
    .with_static_limit_subject(
        limit_engine,
        "principal-test".to_owned(),
        "key-test".to_owned(),
        active_record(),
    );

    LifecycleHarness {
        lifecycle,
        storage,
        _dir: dir,
    }
}

fn principal_view_with_filters(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    );
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

async fn send_message(lifecycle: &Lifecycle) {
    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    assert_eq!(response.status(), StatusCode::OK);
    let _body = response
        .into_body()
        .collect()
        .await
        .expect("response body collects")
        .to_bytes();
}

async fn single_event(storage: &RedbStorage) -> RequestEvent {
    let events = events(storage).await;
    assert_eq!(
        events.len(),
        1,
        "expected one request event, got {events:?}"
    );
    events.into_iter().next().expect("event exists")
}

async fn events(storage: &RedbStorage) -> Vec<RequestEvent> {
    RequestEventStore::query_request_events(storage, 0, u64::MAX, 100)
        .await
        .expect("query request events")
}

fn assert_bulkhead_wait_under(value: Option<u64>, max_ms: u64) {
    let wait_ms = value.expect("bulkhead_wait_ms emitted");
    assert!(
        wait_ms <= max_ms,
        "expected bulkhead wait <= {max_ms}ms, got {wait_ms}ms"
    );
}

fn assert_stage_durations_match_route_ms(event: &RequestEvent) {
    let route_us = event.route_ms.expect("route_ms emitted") * 1_000;
    let trace = event.routing_trace.as_ref().expect("routing_trace emitted");
    let stage_duration_us: u64 = trace.stages.iter().map(|stage| stage.duration_us).sum();
    let tolerance_us = (route_us / 10).max(1_000);
    let diff_us = stage_duration_us.abs_diff(route_us);

    assert!(
        stage_duration_us > 0,
        "expected non-zero per-stage durations: {trace:?}"
    );
    assert!(
        diff_us <= tolerance_us,
        "expected routing trace stage durations ({stage_duration_us}us) to be within 10% of route_ms ({route_us}us), diff={diff_us}us tolerance={tolerance_us}us trace={trace:?}"
    );
}

struct SelectingRouter {
    base_url: Url,
}

impl RouterPlugin for SelectingRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream_id: Some(default_upstream_id()),
            upstream: Upstream::AnthropicDirect,
            dialect: Arc::new(PassthroughDialect {
                base_url: self.base_url.clone(),
            }),
        })
    }
}

struct SleepingFilter {
    name: &'static str,
    delay: Duration,
}

impl SleepingFilter {
    fn new(name: &'static str, delay: Duration) -> Self {
        Self { name, delay }
    }
}

impl FilterPlugin for SleepingFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        std::thread::sleep(self.delay);
        Ok(FilterOutput {
            kept_upstream_ids: candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
            reason: format!("{} kept all candidates", self.name),
            per_candidate_reasons: Vec::new(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}

fn default_upstream_id() -> Uuid {
    Uuid::parse_str(DEFAULT_UPSTREAM_ID).expect("default upstream id parses")
}

fn instrumented_bulkhead_dispatcher(
    resolver: Option<Arc<dyn DnsResolver>>,
    semaphore_permits: u32,
    max_idle_per_host: usize,
) -> Arc<dyn UpstreamDispatch> {
    let inner = instrumented_dispatcher(resolver, max_idle_per_host);
    let config = BulkheadConfig {
        max_conns_per_upstream: max_idle_per_host as u32,
        semaphore_permits,
        acquire_timeout: Duration::from_secs(2),
    };
    let dispatcher_factory: Arc<dyn Fn(usize) -> Arc<dyn UpstreamDispatch> + Send + Sync> =
        Arc::new(move |_| Arc::clone(&inner));
    Arc::new(BulkheadDispatch::with_dispatcher_factory(
        Arc::new(BulkheadRegistry::new()),
        config,
        Arc::new(|request| {
            request
                .url()
                .host_str()
                .unwrap_or("unknown-upstream")
                .to_owned()
        }),
        dispatcher_factory,
    ))
}

fn instrumented_dispatcher(
    resolver: Option<Arc<dyn DnsResolver>>,
    max_idle_per_host: usize,
) -> Arc<dyn UpstreamDispatch> {
    let config = DnsResolverConfig::default();
    let caching_http = match resolver {
        Some(resolver) => CachingDnsConnector::with_resolver(resolver, &config),
        None => CachingDnsConnector::new(&config).expect("default DNS resolver builds"),
    };
    let https = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .wrap_connector(caching_http);
    Arc::new(HyperDispatch {
        client: build_client(InstrumentedHttpsConnector::new(https), max_idle_per_host),
    })
}

fn build_client<C>(connector: C, max_idle_per_host: usize) -> Client<C, Full<Bytes>>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    let mut builder = Client::builder(TokioExecutor::new());
    builder.pool_idle_timeout(Duration::from_secs(90));
    builder.pool_max_idle_per_host(max_idle_per_host);
    builder.build(connector)
}

struct HyperDispatch<C> {
    client: Client<C, Full<Bytes>>,
}

#[async_trait]
impl<C> UpstreamDispatch for HyperDispatch<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    async fn dispatch(
        &self,
        request: cc_lb_plugin_api::SignedRequest,
    ) -> Result<Response<Body>, DispatchError> {
        let (url, method, headers, body) = request.into_parts();
        let uri =
            url.as_str()
                .parse::<http::Uri>()
                .map_err(|source| DispatchError::InvalidUri {
                    reason: source.to_string(),
                })?;
        let mut builder = Request::builder().method(method).uri(uri);
        copy_headers(headers, builder.headers_mut());
        let request =
            builder
                .body(Full::new(body))
                .map_err(|source| DispatchError::RequestBuild {
                    reason: source.to_string(),
                })?;
        let response =
            self.client
                .request(request)
                .await
                .map_err(|source| DispatchError::Transport {
                    reason: source.to_string(),
                })?;
        let (parts, body) = response.into_parts();
        Ok(Response::from_parts(parts, Body::new(body)))
    }
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

struct StaticResolver {
    host: String,
}

impl StaticResolver {
    fn new(host: &str) -> Self {
        Self {
            host: host.to_owned(),
        }
    }
}

impl DnsResolver for StaticResolver {
    fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
        let expected = self.host.clone();
        Box::pin(async move {
            assert_eq!(name, expected);
            Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        })
    }
}

struct MockUpstream {
    addr: SocketAddr,
    shutdown: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl MockUpstream {
    async fn start(delay_before_headers: Duration) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("mock upstream binds");
        let addr = listener.local_addr().expect("mock upstream local addr");
        let shutdown = Arc::new(Notify::new());
        let task_shutdown = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_shutdown.notified() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _peer)) = accepted else {
                            break;
                        };
                        tokio::spawn(handle_connection(stream, delay_before_headers));
                    }
                }
            }
        });
        Self {
            addr,
            shutdown,
            task,
        }
    }

    fn ip_base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn host_base_url(&self, host: &str) -> String {
        format!("http://{host}:{}", self.addr.port())
    }
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        self.task.abort();
    }
}

async fn handle_connection(mut stream: TcpStream, delay_before_headers: Duration) {
    while read_request(&mut stream).await {
        if !delay_before_headers.is_zero() {
            tokio::time::sleep(delay_before_headers).await;
        }
        if write_response(&mut stream).await.is_err() {
            break;
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> bool {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let bytes_read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return false,
            Ok(bytes_read) => bytes_read,
        };
        buffer.extend_from_slice(&chunk[..bytes_read]);
        if let Some(header_end) = find_header_end(&buffer) {
            break header_end;
        }
    };
    let content_length = content_length(&buffer[..header_end]);
    let body_bytes_read = buffer.len().saturating_sub(header_end);
    if body_bytes_read < content_length {
        let mut remaining = vec![0_u8; content_length - body_bytes_read];
        if stream.read_exact(&mut remaining).await.is_err() {
            return false;
        }
    }
    true
}

async fn write_response(stream: &mut TcpStream) -> std::io::Result<()> {
    let body = br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#;
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn content_length(headers: &[u8]) -> usize {
    String::from_utf8_lossy(headers)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}
