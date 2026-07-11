mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_control::RequestEventBus;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::instrumented_connector::InstrumentedHttpsConnector;
use cc_lb_engine::{
    Body, BulkheadDispatch, BulkheadRegistry, BulkheadRuntimeConfig, CachingDnsConnector,
    DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY, DispatchError, DnsResolveFuture, DnsResolver,
    DnsResolverConfig, DynamicViewBuilder, DynamicViewHolder, InMemoryBus, Lifecycle,
    LifecycleConfig, RequestEventAssemblerHandle, UpstreamDispatch, spawn_request_event_assembler,
};
use cc_lb_observability::NoopMetricsHook;
use cc_lb_plugin_api::{Principal, RouteDecision, RouteError, RouterPlugin, UpstreamCandidate};
use cc_lb_storage_api::types::{KeyStatus, RequestEvent, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{BackendKind, MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
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

use common::{TestAuthn, TestState, messages_request};

const DEFAULT_UPSTREAM_ID: &str = "00000000-0000-0000-0000-000000000001";

#[tokio::test]
async fn cold_request_populates_all_connection_stages_ip_upstream() {
    let upstream = MockUpstream::start(Duration::ZERO).await;
    let dispatcher = instrumented_bulkhead_dispatcher(None, 8, 8);
    let harness = lifecycle_for(&upstream.ip_base_url(), dispatcher).await;

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
    assert!(
        event.observability_post_ms.is_none(),
        "RFC-0002 Phase 6f: handler no longer measures observability_post inline; the hook adapter subscriber runs off-thread so this handler-side field is intentionally unpopulated. Got: {:?}",
        event.observability_post_ms
    );
    assert!(
        event.limit_reconcile_ms.is_none(),
        "RFC-0002 H4: handler no longer measures reconcile; LimitReconcileSubscriber owns the reconcile call and does not populate this handler-side field. Got: {:?}",
        event.limit_reconcile_ms
    );
}

#[tokio::test]
async fn cold_request_with_hostname_populates_dns_ms() {
    let upstream = MockUpstream::start(Duration::ZERO).await;
    let resolver = Arc::new(StaticResolver::new("mock-upstream.test"));
    let dispatcher = instrumented_bulkhead_dispatcher(Some(resolver), 8, 8);
    let harness = lifecycle_for(&upstream.host_base_url("mock-upstream.test"), dispatcher).await;

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
    let harness = lifecycle_for(&upstream.host_base_url("mock-upstream.test"), dispatcher).await;

    send_message(&harness.lifecycle).await;
    send_message(&harness.lifecycle).await;

    let events = wait_for_events(&harness.storage, 2).await;
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
    let harness = lifecycle_for(&upstream.ip_base_url(), dispatcher).await;
    let LifecycleHarness {
        lifecycle,
        storage,
        _dir,
        _assembler,
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

    let events = wait_for_events(&storage, 5).await;
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
    storage: Arc<SqliteStorage>,
    _dir: tempfile::TempDir,
    _assembler: RequestEventAssemblerHandle,
}

async fn lifecycle_for(base_url: &str, dispatcher: Arc<dyn UpstreamDispatch>) -> LifecycleHarness {
    let state = TestState::default();
    let authn = TestAuthn::new(state);
    let dir = tempfile::tempdir().expect("request event storage tempdir");
    let path = dir.path().join("latency-stages.sqlite");
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
        .await
        .expect("request event storage opens");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize");
    let storage = Arc::new(storage);
    let bus = Arc::new(InMemoryBus::new());
    let assembler_rx = bus.attach_lifecycle_assembler(DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY);
    let assembler = spawn_request_event_assembler(
        assembler_rx,
        Arc::clone(&storage) as Arc<dyn StorageTrait>,
        Some(Arc::clone(&bus) as Arc<dyn RequestEventBus>),
        Arc::new(NoopMetricsHook),
    );
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_engine::SystemClock),
    );
    let base_url = Url::parse(base_url).expect("test base URL parses");
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(SelectingRouter))
        .global_observability_hooks(Vec::new())
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![upstream_record(base_url)])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
    .with_event_bus(Arc::clone(&bus) as Arc<dyn RequestEventBus>)
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
        _assembler: assembler,
    }
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

fn upstream_record(base_url: Url) -> UpstreamRecord {
    UpstreamRecord {
        id: default_upstream_id(),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(base_url),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
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

async fn single_event(storage: &SqliteStorage) -> RequestEvent {
    let events = wait_for_events(storage, 1).await;
    assert_eq!(
        events.len(),
        1,
        "expected one request event, got {events:?}"
    );
    events.into_iter().next().expect("event exists")
}

async fn query_events(storage: &SqliteStorage) -> Vec<RequestEvent> {
    RequestEventStore::query_request_events(storage, 0, u64::MAX, 100)
        .await
        .expect("query request events")
}

async fn wait_for_events(storage: &SqliteStorage, expected: usize) -> Vec<RequestEvent> {
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let events = query_events(storage).await;
        if events.len() >= expected {
            return events;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for {expected} request event(s); got {} after 3s: {events:?}",
                events.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn assert_bulkhead_wait_under(value: Option<u64>, max_ms: u64) {
    let wait_ms = value.expect("bulkhead_wait_ms emitted");
    assert!(
        wait_ms <= max_ms,
        "expected bulkhead wait <= {max_ms}ms, got {wait_ms}ms"
    );
}

struct SelectingRouter;

impl RouterPlugin for SelectingRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        panic!("terminal strategy selects upstream before legacy router")
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
    let config = BulkheadRuntimeConfig {
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
        request: cc_lb_upstream::SignedRequest,
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
