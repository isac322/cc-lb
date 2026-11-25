use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{LookupIpStrategy, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::dns::Name;
use metrics::Unit;
use thiserror::Error;
use tower_service::Service;

pub type DnsResolveFuture<'a> = Pin<Box<DynDnsResolveFuture<'a>>>;
pub type DynDnsResolveFuture<'a> =
    dyn Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'a;
type MetricDnsFuture = Pin<Box<DynMetricDnsFuture>>;
type DynMetricDnsFuture =
    dyn Future<Output = Result<std::vec::IntoIter<SocketAddr>, DnsCacheError>> + Send;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DnsResolverConfig {
    pub cache_ttl_floor: Duration,
    pub cache_ttl_ceiling: Duration,
    pub max_concurrent: usize,
}

impl Default for DnsResolverConfig {
    fn default() -> Self {
        Self {
            cache_ttl_floor: Duration::from_secs(30),
            cache_ttl_ceiling: Duration::from_secs(300),
            max_concurrent: 64,
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DnsCacheError {
    #[error("failed to build hickory resolver: {message}")]
    Build { message: String },
    #[error("dns resolution failed for {host}: {message}")]
    Resolve { host: String, message: String },
}

pub trait DnsResolver: Send + Sync + 'static {
    fn resolve(&self, name: String) -> DnsResolveFuture<'_>;
}

pub fn make_resolver(config: &DnsResolverConfig) -> Result<Arc<TokioResolver>, DnsCacheError> {
    // Read /etc/resolv.conf (or platform equivalent) so the resolver honors the host's DNS
    // setup (split-horizon DNS, VPN/WARP local stubs at 127.0.x.x, corporate internal zones,
    // etc.). `ResolverConfig::default()` hardcodes Cloudflare public DNS which fails in any
    // environment where outbound DNS to 1.1.1.1 is blocked or rewritten.
    let resolver_config = hickory_resolver::system_conf::read_system_conf()
        .map(|(cfg, _)| cfg)
        .unwrap_or_else(|_| ResolverConfig::default());
    let opts = resolver_opts(config);
    let mut builder =
        TokioResolver::builder_with_config(resolver_config, TokioRuntimeProvider::default());
    *builder.options_mut() = opts;
    builder
        .build()
        .map(Arc::new)
        .map_err(|source| DnsCacheError::Build {
            message: source.to_string(),
        })
}

#[doc(hidden)]
pub fn make_resolver_with_factory<F, R>(config: &DnsResolverConfig, factory: F) -> R
where
    F: FnOnce(ResolverConfig, ResolverOpts, TokioRuntimeProvider) -> R,
{
    let opts = resolver_opts(config);
    factory(
        ResolverConfig::default(),
        opts,
        TokioRuntimeProvider::default(),
    )
}

fn resolver_opts(config: &DnsResolverConfig) -> ResolverOpts {
    let mut opts = ResolverOpts::default();
    opts.positive_min_ttl = Some(config.cache_ttl_floor);
    opts.positive_max_ttl = Some(config.cache_ttl_ceiling);
    opts.num_concurrent_reqs = config.max_concurrent;
    opts.ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
    opts
}

const DUAL_STACK_DNS_GRACE: Duration = Duration::from_millis(50);

fn interleave_pair<T: Copy>(first: Vec<T>, second: Vec<T>) -> Vec<T> {
    if first.is_empty() {
        return second;
    }
    if second.is_empty() {
        return first;
    }
    let mut ordered = Vec::with_capacity(first.len() + second.len());
    let mut first_iter = first.into_iter();
    let mut second_iter = second.into_iter();
    loop {
        match (first_iter.next(), second_iter.next()) {
            (Some(a), Some(b)) => {
                ordered.push(a);
                ordered.push(b);
            }
            (Some(a), None) => {
                ordered.push(a);
                ordered.extend(first_iter);
                break;
            }
            (None, Some(b)) => {
                ordered.push(b);
                ordered.extend(second_iter);
                break;
            }
            (None, None) => break,
        }
    }
    ordered
}

fn order_dual_stack_addrs(addrs: &[SocketAddr]) -> Vec<SocketAddr> {
    let v4 = addrs
        .iter()
        .copied()
        .filter(SocketAddr::is_ipv4)
        .collect::<Vec<_>>();
    let v6 = addrs
        .iter()
        .copied()
        .filter(SocketAddr::is_ipv6)
        .collect::<Vec<_>>();
    if v4.is_empty() || v6.is_empty() {
        return addrs.to_vec();
    }
    interleave_pair(v4, v6)
}

async fn resolve_dual_stack_with_grace<F4, F6>(
    ipv4: F4,
    ipv6: F6,
    grace: Duration,
) -> Result<Vec<IpAddr>, DnsCacheError>
where
    F4: Future<Output = Result<Vec<IpAddr>, DnsCacheError>>,
    F6: Future<Output = Result<Vec<IpAddr>, DnsCacheError>>,
{
    enum Family {
        V4,
        V6,
    }

    tokio::pin!(ipv4);
    tokio::pin!(ipv6);

    let (first_family, first_res) = tokio::select! {
        res = &mut ipv4 => (Family::V4, res),
        res = &mut ipv6 => (Family::V6, res),
    };

    match first_res {
        Ok(first_ips) if !first_ips.is_empty() => {
            let peer = async {
                match first_family {
                    Family::V4 => ipv6.await,
                    Family::V6 => ipv4.await,
                }
            };
            match tokio::time::timeout(grace, peer).await {
                Ok(Ok(second_ips)) if !second_ips.is_empty() => match first_family {
                    Family::V4 => Ok(interleave_pair(first_ips, second_ips)),
                    Family::V6 => Ok(interleave_pair(second_ips, first_ips)),
                },
                _ => Ok(first_ips),
            }
        }
        first_empty_or_err => {
            let second_res = match first_family {
                Family::V4 => ipv6.await,
                Family::V6 => ipv4.await,
            };
            match (first_empty_or_err, second_res) {
                (_, Ok(ips)) if !ips.is_empty() => Ok(ips),
                (Ok(_), Ok(ips)) => Ok(ips),
                (Ok(ips), Err(_)) => Ok(ips),
                (Err(_), Ok(ips)) => Ok(ips),
                (Err(err), Err(_)) => Err(err),
            }
        }
    }
}

/// Connect a TCP stream to resolved addresses within `deadline` using staggered
/// Happy-Eyeballs concurrency so one stalled family cannot starve an alternate
/// address before the shared deadline expires.
pub async fn connect_tcp_with_fallback(
    addrs: &[SocketAddr],
    deadline: tokio::time::Instant,
) -> io::Result<tokio::net::TcpStream> {
    connect_tcp_with_fallback_using(addrs, deadline, tokio::net::TcpStream::connect).await
}

async fn connect_tcp_with_fallback_using<F, Fut>(
    addrs: &[SocketAddr],
    deadline: tokio::time::Instant,
    connect: F,
) -> io::Result<tokio::net::TcpStream>
where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<tokio::net::TcpStream>> + Send + 'static,
{
    let ordered = order_dual_stack_addrs(addrs);
    if ordered.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no socket addresses to connect to",
        ));
    }
    if deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .is_zero()
    {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "TCP connect timed out",
        ));
    }
    let mut attempts = tokio::task::JoinSet::new();
    let mut next_idx = 0usize;
    let mut last_error: Option<io::Error> = None;

    loop {
        if next_idx < ordered.len() && attempts.is_empty() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connect timed out",
                ));
            }
            attempts.spawn(connect(ordered[next_idx]));
            next_idx += 1;
            continue;
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            attempts.abort_all();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TCP connect timed out",
            ));
        }

        if next_idx < ordered.len() {
            let remaining_attempts =
                u32::try_from(ordered.len() - next_idx + 1).unwrap_or(u32::MAX);
            let stagger = (remaining / remaining_attempts)
                .min(Duration::from_millis(250))
                .max(Duration::from_millis(1));
            let stagger_sleep = tokio::time::sleep(stagger.min(remaining));
            tokio::pin!(stagger_sleep);
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    attempts.abort_all();
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TCP connect timed out",
                    ));
                }
                _ = &mut stagger_sleep => {
                    attempts.spawn(connect(ordered[next_idx]));
                    next_idx += 1;
                }
                joined = attempts.join_next() => {
                    match joined {
                        Some(Ok(Ok(stream))) => {
                            attempts.abort_all();
                            return Ok(stream);
                        }
                        Some(Ok(Err(err))) => {
                            last_error = Some(err);
                            if next_idx < ordered.len() {
                                attempts.spawn(connect(ordered[next_idx]));
                                next_idx += 1;
                            }
                        }
                        Some(Err(join_err)) => {
                            last_error = Some(io::Error::other(join_err.to_string()));
                            if next_idx < ordered.len() {
                                attempts.spawn(connect(ordered[next_idx]));
                                next_idx += 1;
                            }
                        }
                        None => {}
                    }
                }
            }
        } else {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    attempts.abort_all();
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TCP connect timed out",
                    ));
                }
                joined = attempts.join_next() => {
                    match joined {
                        Some(Ok(Ok(stream))) => {
                            attempts.abort_all();
                            return Ok(stream);
                        }
                        Some(Ok(Err(err))) => last_error = Some(err),
                        Some(Err(join_err)) => {
                            last_error = Some(io::Error::other(join_err.to_string()));
                        }
                        None => break,
                    }
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "TCP connect exhausted all resolved addresses",
        )
    }))
}

impl DnsResolver for TokioResolver {
    fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
        Box::pin(async move {
            if let Ok(ip) = name
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
            {
                return Ok(vec![ip]);
            }
            let v4_host = name.clone();
            let v6_host = name.clone();
            let v4 = async move {
                self.ipv4_lookup(v4_host.clone())
                    .await
                    .map(|lookup| {
                        lookup
                            .answers()
                            .iter()
                            .filter_map(|record| record.data.ip_addr())
                            .collect()
                    })
                    .map_err(|source| DnsCacheError::Resolve {
                        host: v4_host,
                        message: source.to_string(),
                    })
            };
            let v6 = async move {
                self.ipv6_lookup(v6_host.clone())
                    .await
                    .map(|lookup| {
                        lookup
                            .answers()
                            .iter()
                            .filter_map(|record| record.data.ip_addr())
                            .collect()
                    })
                    .map_err(|source| DnsCacheError::Resolve {
                        host: v6_host,
                        message: source.to_string(),
                    })
            };
            resolve_dual_stack_with_grace(v4, v6, DUAL_STACK_DNS_GRACE).await
        })
    }
}

impl<T> DnsResolver for Arc<T>
where
    T: DnsResolver,
{
    fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
        (**self).resolve(name)
    }
}

#[derive(Clone)]
pub struct CachingDnsConnector {
    inner: HttpConnector<MetricDnsResolver>,
}

impl CachingDnsConnector {
    pub fn new(config: &DnsResolverConfig) -> Result<Self, DnsCacheError> {
        MetricDnsResolver::new(config).map(|resolver| Self::from_metric_resolver(resolver, None))
    }

    pub fn with_resolver(resolver: Arc<dyn DnsResolver>, config: &DnsResolverConfig) -> Self {
        Self::from_metric_resolver(MetricDnsResolver::with_resolver(resolver, config), None)
    }

    pub fn from_metric_resolver(
        metric_resolver: MetricDnsResolver,
        connect_timeout: Option<Duration>,
    ) -> Self {
        register_dns_metrics();
        let mut inner = HttpConnector::new_with_resolver(metric_resolver);
        inner.enforce_http(false);
        inner.set_connect_timeout(connect_timeout);
        Self { inner }
    }
}

impl Service<Uri> for CachingDnsConnector {
    type Response = <HttpConnector<MetricDnsResolver> as Service<Uri>>::Response;
    type Error = <HttpConnector<MetricDnsResolver> as Service<Uri>>::Error;
    type Future = <HttpConnector<MetricDnsResolver> as Service<Uri>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        self.inner.call(dst)
    }
}

#[derive(Clone)]
pub struct MetricDnsResolver {
    resolver: Arc<dyn DnsResolver>,
    metric_cache: Arc<DashMap<String, Instant>>,
    metric_ttl: Duration,
}

impl MetricDnsResolver {
    pub fn new(config: &DnsResolverConfig) -> Result<Self, DnsCacheError> {
        make_resolver(config).map(|resolver| Self::with_resolver(resolver, config))
    }

    pub fn with_resolver(resolver: Arc<dyn DnsResolver>, config: &DnsResolverConfig) -> Self {
        register_dns_metrics();
        Self {
            resolver,
            metric_cache: Arc::new(DashMap::new()),
            metric_ttl: config.cache_ttl_ceiling,
        }
    }

    /// Resolve a `host` and `port` into socket addresses through the shared DNS
    /// cache and instrumentation path. IP literals return immediately without a
    /// DNS lookup.
    pub async fn resolve_host(
        &self,
        host: &str,
        port: u16,
    ) -> Result<Vec<SocketAddr>, DnsCacheError> {
        if let Ok(ip) = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let ips = resolve_host_ips(
            Arc::clone(&self.resolver),
            Arc::clone(&self.metric_cache),
            self.metric_ttl,
            host.to_owned(),
        )
        .await?;
        Ok(ips
            .into_iter()
            .map(|ip| SocketAddr::new(ip, port))
            .collect())
    }
}

async fn resolve_host_ips(
    resolver: Arc<dyn DnsResolver>,
    metric_cache: Arc<DashMap<String, Instant>>,
    metric_ttl: Duration,
    host: String,
) -> Result<Vec<IpAddr>, DnsCacheError> {
    let start = Instant::now();
    let now = Instant::now();
    let was_hit = metric_cache
        .get(&host)
        .map(|entry| *entry.value() > now)
        .unwrap_or(false);

    match resolver.resolve(host.clone()).await {
        Ok(ips) => {
            crate::request_timing::record_dns(
                start.elapsed(),
                crate::request_timing::DnsResolutionOutcome::Resolved,
            );
            emit_dns_metric(if was_hit { "hit" } else { "miss" });
            metric_cache.insert(host, Instant::now() + metric_ttl);
            Ok(ips)
        }
        Err(err) => {
            crate::request_timing::record_dns(
                start.elapsed(),
                crate::request_timing::DnsResolutionOutcome::Failed,
            );
            emit_dns_metric("error");
            Err(err)
        }
    }
}

impl Service<Name> for MetricDnsResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = DnsCacheError;
    type Future = MetricDnsFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let host = name.as_str().to_owned();
        let resolver = Arc::clone(&self.resolver);
        let metric_cache = Arc::clone(&self.metric_cache);
        let metric_ttl = self.metric_ttl;

        Box::pin(async move {
            let ips = resolve_host_ips(resolver, metric_cache, metric_ttl, host).await?;
            Ok(ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, 0))
                .collect::<Vec<_>>()
                .into_iter())
        })
    }
}

fn emit_dns_metric(outcome: &'static str) {
    metrics::counter!("cc_lb_dns_resolve_total", "outcome" => outcome).increment(1);
}

fn register_dns_metrics() {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| {
        metrics::describe_counter!(
            "cc_lb_dns_resolve_total",
            Unit::Count,
            "DNS resolution outcomes for the caching connector. hit and miss are heuristic counts based on the local TTL shim; hickory remains authoritative for correctness."
        );
    });
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::Duration;

    use dashmap::DashMap;
    use hyper::Uri;
    use tokio::net::TcpListener;
    use tower_service::Service;

    use super::*;

    async fn with_timings<F, T>(
        fut: F,
    ) -> (
        T,
        cc_lb_proxy_telemetry::request_timing::RequestStageTimings,
    )
    where
        F: Future<Output = T>,
    {
        let ctx = Arc::new(std::sync::Mutex::new(
            cc_lb_proxy_telemetry::request_timing::RequestStageTimings::default(),
        ));
        let value = cc_lb_proxy_telemetry::request_timing::REQUEST_STAGE_TIMINGS
            .scope(ctx.clone(), fut)
            .await;
        let snapshot = ctx.lock().unwrap().clone();
        (value, snapshot)
    }

    struct StubResolver<F> {
        resolve: F,
    }

    impl<F> StubResolver<F> {
        fn new(resolve: F) -> Self {
            Self { resolve }
        }
    }

    impl<F, Fut> DnsResolver for StubResolver<F>
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'static,
    {
        fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
            Box::pin((self.resolve)(name))
        }
    }

    fn metric_resolver(resolver: impl DnsResolver) -> MetricDnsResolver {
        MetricDnsResolver {
            resolver: Arc::new(resolver),
            metric_cache: Arc::new(DashMap::new()),
            metric_ttl: Duration::from_secs(300),
        }
    }

    #[tokio::test]
    async fn dns_resolver_writes_dns_ms_inside_scope() {
        let stub = StubResolver::new(|_name| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(vec![IpAddr::from(Ipv4Addr::LOCALHOST)])
        });
        let mut resolver = metric_resolver(stub);

        let (_, timings) = with_timings(async {
            let name: Name = "example.com".parse().unwrap();
            let _ = resolver.call(name).await;
        })
        .await;

        assert!(
            timings.dns_ms.is_some_and(|dns_ms| dns_ms >= 100),
            "expected dns_ms >= 100, got {:?}",
            timings.dns_ms
        );
        assert_eq!(
            timings.dns_outcome,
            Some(cc_lb_proxy_telemetry::request_timing::DnsResolutionOutcome::Resolved)
        );
    }

    #[tokio::test]
    async fn dns_resolver_outside_scope_silent_noop() {
        let stub = StubResolver::new(|_name| async { Ok(vec![IpAddr::from(Ipv4Addr::LOCALHOST)]) });
        let mut resolver = metric_resolver(stub);
        let name: Name = "example.com".parse().unwrap();

        let _ = resolver.call(name).await;
    }

    #[tokio::test]
    async fn dns_resolver_failure_records_elapsed_and_failed_outcome() {
        let stub = StubResolver::new(|name| async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(DnsCacheError::Resolve {
                host: name,
                message: "boom".to_owned(),
            })
        });
        let mut resolver = metric_resolver(stub);

        let (_, timings) = with_timings(async {
            let name: Name = "example.com".parse().unwrap();
            let _ = resolver.call(name).await;
        })
        .await;

        assert!(
            timings.dns_ms.is_some_and(|dns_ms| dns_ms >= 20),
            "expected failed dns_ms >= 20, got {:?}",
            timings.dns_ms
        );
        assert_eq!(
            timings.dns_outcome,
            Some(cc_lb_proxy_telemetry::request_timing::DnsResolutionOutcome::Failed)
        );
        assert_eq!(timings.dns_attempts, 1);
    }

    #[tokio::test]
    async fn caching_dns_connector_accepts_https_uri() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind local listener");
        let port = listener.local_addr().expect("local addr").port();
        let mut connector = CachingDnsConnector::new(&DnsResolverConfig::default())
            .expect("build caching dns connector");
        let uri: Uri = format!("https://127.0.0.1:{port}")
            .parse()
            .expect("https URI");

        let result = connector.call(uri).await;

        if let Err(err) = result {
            let message = err.to_string();
            assert!(
                !message.contains("invalid URL")
                    && !message.contains("URL scheme")
                    && !message.contains("scheme is not http"),
                "connector rejected HTTPS scheme: {message}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_host_handles_ip_literals_hostnames_and_errors() {
        let stub = StubResolver::new(|name| async move {
            if name == "bad.invalid" {
                Err(DnsCacheError::Resolve {
                    host: name,
                    message: "nxdomain".to_owned(),
                })
            } else {
                Ok(vec![IpAddr::from(Ipv4Addr::new(127, 0, 0, 2))])
            }
        });
        let resolver =
            MetricDnsResolver::with_resolver(Arc::new(stub), &DnsResolverConfig::default());

        let v4 = resolver.resolve_host("127.0.0.1", 8443).await.unwrap();
        assert_eq!(v4, vec!["127.0.0.1:8443".parse::<SocketAddr>().unwrap()]);

        let v6 = resolver.resolve_host("[::1]", 443).await.unwrap();
        assert_eq!(v6, vec!["[::1]:443".parse::<SocketAddr>().unwrap()]);

        let (_, first_timings) = with_timings(async {
            let addrs = resolver
                .resolve_host("api.example.test", 443)
                .await
                .unwrap();
            assert_eq!(addrs, vec!["127.0.0.2:443".parse::<SocketAddr>().unwrap()]);
        })
        .await;
        assert_eq!(
            first_timings.dns_outcome,
            Some(cc_lb_proxy_telemetry::request_timing::DnsResolutionOutcome::Resolved)
        );

        let err = resolver.resolve_host("bad.invalid", 443).await.unwrap_err();
        assert!(matches!(err, DnsCacheError::Resolve { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn resolve_dual_stack_with_grace_returns_fast_family_without_waiting_for_stalled_peer() {
        let fast_v4 = vec![IpAddr::from(Ipv4Addr::new(192, 0, 2, 10))];
        let fast_v6 = vec![IpAddr::from(
            "2001:db8::10".parse::<std::net::Ipv6Addr>().unwrap(),
        )];

        // 1. IPv4 succeeds quickly while IPv6 stalls past the 50ms grace window.
        let started = tokio::time::Instant::now();
        let resolved = resolve_dual_stack_with_grace(
            {
                let v4 = fast_v4.clone();
                async move { Ok(v4) }
            },
            async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(vec![IpAddr::from(
                    "2001:db8::99".parse::<std::net::Ipv6Addr>().unwrap(),
                )])
            },
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("fast IPv4 returns after grace window");
        assert_eq!(resolved, fast_v4);
        assert_eq!(started.elapsed(), DUAL_STACK_DNS_GRACE);

        // 2. IPv6 succeeds quickly while IPv4 stalls past the 50ms grace window.
        let started = tokio::time::Instant::now();
        let resolved = resolve_dual_stack_with_grace(
            async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(vec![IpAddr::from(Ipv4Addr::new(192, 0, 2, 99))])
            },
            {
                let v6 = fast_v6.clone();
                async move { Ok(v6) }
            },
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("fast IPv6 returns after grace window");
        assert_eq!(resolved, fast_v6);
        assert_eq!(started.elapsed(), DUAL_STACK_DNS_GRACE);

        // 3. Both arrive inside grace window -> interleaved A and AAAA.
        let started = tokio::time::Instant::now();
        let resolved = resolve_dual_stack_with_grace(
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(vec![
                    IpAddr::from(Ipv4Addr::new(192, 0, 2, 1)),
                    IpAddr::from(Ipv4Addr::new(192, 0, 2, 2)),
                ])
            },
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(vec![IpAddr::from(
                    "2001:db8::1".parse::<std::net::Ipv6Addr>().unwrap(),
                )])
            },
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("both fast families interleave");
        assert_eq!(
            resolved,
            vec![
                IpAddr::from(Ipv4Addr::new(192, 0, 2, 1)),
                IpAddr::from("2001:db8::1".parse::<std::net::Ipv6Addr>().unwrap()),
                IpAddr::from(Ipv4Addr::new(192, 0, 2, 2)),
            ]
        );
        assert_eq!(started.elapsed(), Duration::from_millis(20));

        // 4. First family errors immediately, peer succeeds slower than grace -> still returns peer.
        let resolved = resolve_dual_stack_with_grace(
            async {
                Err(DnsCacheError::Resolve {
                    host: "example.test".to_owned(),
                    message: "no A record".to_owned(),
                })
            },
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(fast_v6.clone())
            },
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("slow healthy peer rescues first-family error");
        assert_eq!(resolved, fast_v6);
    }

    #[tokio::test]
    async fn tokio_resolver_resolves_localhost() {
        let resolver = make_resolver(&DnsResolverConfig::default()).expect("build system resolver");
        let addrs = DnsResolver::resolve(&*resolver, "localhost".to_owned())
            .await
            .expect("resolve localhost");
        assert!(
            addrs.iter().any(|ip| ip.is_loopback()),
            "expected loopback address for localhost, got {addrs:?}"
        );
    }

    #[tokio::test]
    async fn caching_dns_connector_from_metric_resolver_honors_connect_timeout() {
        // Separate controlled DNS step: caller deadline fires during a stalled DNS lookup.
        let slow_dns = StubResolver::new(|_| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(vec![IpAddr::from(Ipv4Addr::LOCALHOST)])
        });
        let slow_resolver =
            MetricDnsResolver::with_resolver(Arc::new(slow_dns), &DnsResolverConfig::default());
        let mut slow_connector = CachingDnsConnector::from_metric_resolver(
            slow_resolver,
            Some(Duration::from_millis(20)),
        );
        let slow_uri: Uri = "http://slow.example.test:8080/"
            .parse()
            .expect("URI parses");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), slow_connector.call(slow_uri))
                .await
                .is_err(),
            "stalled DNS lookup must be cut off by outer timeout"
        );

        // Separate controlled HTTP TCP connect step: resolve DNS immediately via StubResolver
        // to an unaccepted local listener whose accept backlog is saturated, so only
        // `HttpConnector::set_connect_timeout` can cut off the stalled TCP handshake cleanly.
        let listener =
            std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind backlog listener");
        let stall_addr = listener.local_addr().expect("backlog listener addr");
        let mut held_sockets = Vec::new();
        let mut backlog_stalls_connect = false;
        for _ in 0..512 {
            match std::net::TcpStream::connect_timeout(&stall_addr, Duration::from_millis(20)) {
                Ok(stream) => held_sockets.push(stream),
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                    backlog_stalls_connect = true;
                    break;
                }
                Err(_) => break,
            }
        }
        assert!(
            backlog_stalls_connect,
            "expected saturated local accept backlog to stall TCP connect ({} held)",
            held_sockets.len()
        );

        let fast_dns = StubResolver::new(|_| async { Ok(vec![IpAddr::from(Ipv4Addr::LOCALHOST)]) });
        let resolver =
            MetricDnsResolver::with_resolver(Arc::new(fast_dns), &DnsResolverConfig::default());
        let mut connector =
            CachingDnsConnector::from_metric_resolver(resolver, Some(Duration::from_millis(50)));
        let stall_uri: Uri = format!("http://stall.example.test:{}/", stall_addr.port())
            .parse()
            .expect("stall URI parses");
        let err = tokio::time::timeout(Duration::from_secs(2), connector.call(stall_uri))
            .await
            .expect("HttpConnector connect_timeout must fire well before 2s outer safety cap")
            .expect_err("stalled TCP connect must fail");
        let mut has_timeout_cause = false;
        let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(&err);
        while let Some(source) = cur {
            if let Some(io_err) = source.downcast_ref::<io::Error>()
                && io_err.kind() == io::ErrorKind::TimedOut
            {
                has_timeout_cause = true;
                break;
            }
            cur = source.source();
        }
        assert!(
            has_timeout_cause,
            "expected TimedOut source from HttpConnector connect_timeout, got {err:?}"
        );
        drop(held_sockets);
        drop(listener);
    }

    #[tokio::test(start_paused = true)]
    async fn connect_tcp_with_fallback_reaches_alternate_address_within_short_budget() {
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let healthy_addr = healthy_listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let _ = healthy_listener.accept().await;
        });
        let stalled_addr: SocketAddr = "192.0.2.1:443".parse().unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);

        let stream =
            connect_tcp_with_fallback_using(&[stalled_addr, healthy_addr], deadline, move |addr| {
                Box::pin(async move {
                    if addr == stalled_addr {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        Err(io::Error::new(io::ErrorKind::TimedOut, "stalled"))
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        tokio::net::TcpStream::connect(addr).await
                    }
                })
                    as Pin<Box<dyn Future<Output = io::Result<tokio::net::TcpStream>> + Send>>
            })
            .await
            .expect(
                "staggered dial must start and complete healthy second address inside 200ms budget",
            );
        assert_eq!(stream.peer_addr().unwrap(), healthy_addr);
        accept_task.abort();
    }

    #[tokio::test]
    async fn connect_tcp_with_fallback_recovers_from_failed_address_and_interleaves_families() {
        let v4: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let v6: SocketAddr = "[::1]:2".parse().unwrap();
        let v4_b: SocketAddr = "127.0.0.2:3".parse().unwrap();
        assert_eq!(order_dual_stack_addrs(&[v4, v4_b, v6]), vec![v4, v6, v4_b]);

        let closed_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let closed_v4 = closed_listener.local_addr().unwrap();
        drop(closed_listener);

        let working_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let working_addr = working_listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let _ = working_listener.accept().await;
        });

        let stream = connect_tcp_with_fallback(
            &[closed_v4, working_addr],
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .expect("fallback reaches healthy address after initial connect failure");
        assert_eq!(stream.peer_addr().unwrap(), working_addr);
        accept_task.abort();
    }
}
