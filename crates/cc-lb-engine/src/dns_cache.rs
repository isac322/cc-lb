use std::collections::{HashSet, VecDeque};
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
use hickory_resolver::proto::rr::domain::usage::ONION;
use hickory_resolver::proto::rr::{IntoName as _, Name as DnsName};
use hyper::Uri;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::rt::TokioIo;
use metrics::Unit;
use thiserror::Error;
use tower_service::Service;

pub type DnsResolveFuture<'a> = Pin<Box<DynDnsResolveFuture<'a>>>;
pub type DynDnsResolveFuture<'a> =
    dyn Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'a;
pub type ProgressiveDnsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProgressiveDnsResolution, DnsCacheError>> + Send + 'a>>;
pub type SocketAddrsResolveFuture =
    Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, DnsCacheError>> + Send + 'static>>;
type MetricDnsFuture = Pin<Box<DynMetricDnsFuture>>;
type DynMetricDnsFuture =
    dyn Future<Output = Result<std::vec::IntoIter<SocketAddr>, DnsCacheError>> + Send;

pub struct ProgressiveDnsResolution {
    pub initial: Vec<IpAddr>,
    pub pending_peer: Option<DnsResolveFuture<'static>>,
}

pub struct ProgressiveSocketAddrs {
    pub initial: Vec<SocketAddr>,
    pub pending_peer: Option<SocketAddrsResolveFuture>,
}

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

    fn resolve_progressive(self: Arc<Self>, name: String) -> ProgressiveDnsFuture<'static> {
        Box::pin(async move {
            let ips = self.resolve(name).await?;
            Ok(ProgressiveDnsResolution {
                initial: ips,
                pending_peer: None,
            })
        })
    }
}

fn system_conf_or_default() -> (ResolverConfig, ResolverOpts) {
    hickory_resolver::system_conf::read_system_conf()
        .unwrap_or_else(|_| (ResolverConfig::default(), ResolverOpts::default()))
}

pub fn make_resolver(config: &DnsResolverConfig) -> Result<Arc<TokioResolver>, DnsCacheError> {
    // Read /etc/resolv.conf (or platform equivalent) so the resolver honors the host's DNS
    // setup (split-horizon DNS, VPN/WARP local stubs at 127.0.x.x, corporate internal zones,
    // etc.). `ResolverConfig::default()` hardcodes Cloudflare public DNS which fails in any
    // environment where outbound DNS to 1.1.1.1 is blocked or rewritten.
    let (resolver_config, sys_opts) = system_conf_or_default();
    let opts = resolver_opts(config, sys_opts, LookupIpStrategy::Ipv4AndIpv6);
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
    let opts = resolver_opts(
        config,
        ResolverOpts::default(),
        LookupIpStrategy::Ipv4AndIpv6,
    );
    factory(
        ResolverConfig::default(),
        opts,
        TokioRuntimeProvider::default(),
    )
}

fn resolver_opts(
    config: &DnsResolverConfig,
    mut opts: ResolverOpts,
    ip_strategy: LookupIpStrategy,
) -> ResolverOpts {
    opts.positive_min_ttl = Some(config.cache_ttl_floor);
    opts.positive_max_ttl = Some(config.cache_ttl_ceiling);
    opts.num_concurrent_reqs = config.max_concurrent;
    opts.ip_strategy = ip_strategy;
    opts
}

const DUAL_STACK_DNS_GRACE: Duration = Duration::from_millis(50);

fn build_search_candidates(
    raw: &str,
    config: &ResolverConfig,
    opts: &ResolverOpts,
) -> Result<Vec<DnsName>, DnsCacheError> {
    let trimmed = raw.trim_start_matches('[').trim_end_matches(']');
    let name = trimmed
        .into_name()
        .map_err(|source| DnsCacheError::Resolve {
            host: raw.to_owned(),
            message: source.to_string(),
        })?;
    let is_onion_v3 = ONION.zone_of(&name)
        && name
            .trim_to(2)
            .iter()
            .next()
            .is_some_and(|label| label.len() == 56);
    if name.is_fqdn() || is_onion_v3 {
        let mut fqdn = name;
        fqdn.set_fqdn(true);
        return Ok(vec![fqdn]);
    }

    let raw_name_first = name.num_labels() as usize > opts.ndots || name.is_localhost();
    let mut fqdn = name.clone();
    fqdn.set_fqdn(true);

    let mut candidates = Vec::with_capacity(2 + config.search().len());
    let mut push_unique = |mut cand: DnsName| {
        cand.set_fqdn(true);
        if !candidates.contains(&cand) {
            candidates.push(cand);
        }
    };

    if raw_name_first {
        push_unique(fqdn.clone());
    }
    if let Some(domain) = config.domain()
        && let Ok(joined) = name.clone().append_domain(domain)
    {
        push_unique(joined);
    }
    for search in config.search() {
        if let Ok(joined) = name.clone().append_domain(search) {
            push_unique(joined);
        }
    }
    if !raw_name_first {
        push_unique(fqdn);
    }
    Ok(candidates)
}

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

async fn resolve_dual_stack_progressive(
    mut ipv4: DnsResolveFuture<'static>,
    mut ipv6: DnsResolveFuture<'static>,
    grace: Duration,
) -> Result<ProgressiveDnsResolution, DnsCacheError> {
    enum Family {
        V4,
        V6,
    }

    let (first_family, first_res) = tokio::select! {
        res = &mut ipv4 => (Family::V4, res),
        res = &mut ipv6 => (Family::V6, res),
    };

    let mut peer: DnsResolveFuture<'static> = match first_family {
        Family::V4 => ipv6,
        Family::V6 => ipv4,
    };

    match first_res {
        Ok(first_ips) if !first_ips.is_empty() => {
            tokio::select! {
                second_res = &mut peer => match second_res {
                    Ok(second_ips) if !second_ips.is_empty() => {
                        let initial = match first_family {
                            Family::V4 => interleave_pair(first_ips, second_ips),
                            Family::V6 => interleave_pair(second_ips, first_ips),
                        };
                        Ok(ProgressiveDnsResolution {
                            initial,
                            pending_peer: None,
                        })
                    }
                    _ => Ok(ProgressiveDnsResolution {
                        initial: first_ips,
                        pending_peer: None,
                    }),
                },
                () = tokio::time::sleep(grace) => Ok(ProgressiveDnsResolution {
                    initial: first_ips,
                    pending_peer: Some(peer),
                }),
            }
        }
        first_empty_or_err => {
            let second_res = peer.await;
            let ips = match (first_empty_or_err, second_res) {
                (_, Ok(ips)) if !ips.is_empty() => Ok(ips),
                (Ok(_), Ok(ips)) => Ok(ips),
                (Ok(ips), Err(_)) => Ok(ips),
                (Err(_), Ok(ips)) => Ok(ips),
                (Err(err), Err(_)) => Err(err),
            }?;
            Ok(ProgressiveDnsResolution {
                initial: ips,
                pending_peer: None,
            })
        }
    }
}

#[cfg(test)]
async fn resolve_dual_stack_with_grace<F4, F6>(
    ipv4: F4,
    ipv6: F6,
    grace: Duration,
) -> Result<Vec<IpAddr>, DnsCacheError>
where
    F4: Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'static,
    F6: Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'static,
{
    resolve_dual_stack_progressive(Box::pin(ipv4), Box::pin(ipv6), grace)
        .await
        .map(|res| res.initial)
}

async fn resolve_search_candidates_progressive<F4, F6, Fut4, Fut6>(
    raw: &str,
    config: &ResolverConfig,
    opts: &ResolverOpts,
    lookup_v4: F4,
    lookup_v6: F6,
    grace: Duration,
) -> Result<ProgressiveDnsResolution, DnsCacheError>
where
    F4: Fn(DnsName) -> Fut4,
    F6: Fn(DnsName) -> Fut6,
    Fut4: Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'static,
    Fut6: Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + 'static,
{
    if let Ok(ip) = raw
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    {
        return Ok(ProgressiveDnsResolution {
            initial: vec![ip],
            pending_peer: None,
        });
    }
    let candidates = build_search_candidates(raw, config, opts)?;
    let mut last_error: Option<DnsCacheError> = None;
    let mut saw_empty = false;
    for candidate in candidates {
        match resolve_dual_stack_progressive(
            Box::pin(lookup_v4(candidate.clone())),
            Box::pin(lookup_v6(candidate)),
            grace,
        )
        .await
        {
            Ok(res) if !res.initial.is_empty() => return Ok(res),
            Ok(_) => saw_empty = true,
            Err(err) => last_error = Some(err),
        }
    }
    if saw_empty {
        Ok(ProgressiveDnsResolution {
            initial: Vec::new(),
            pending_peer: None,
        })
    } else if let Some(err) = last_error {
        Err(err)
    } else {
        Ok(ProgressiveDnsResolution {
            initial: Vec::new(),
            pending_peer: None,
        })
    }
}

/// Connect a TCP stream to resolved addresses within `deadline` using staggered
/// Happy-Eyeballs concurrency so one stalled family cannot starve an alternate
/// address before the shared deadline expires.
pub async fn connect_tcp_with_fallback(
    addrs: &[SocketAddr],
    deadline: tokio::time::Instant,
) -> io::Result<tokio::net::TcpStream> {
    connect_tcp_with_progressive_fallback(
        ProgressiveSocketAddrs {
            initial: addrs.to_vec(),
            pending_peer: None,
        },
        Some(deadline),
    )
    .await
}

pub async fn connect_tcp_with_progressive_fallback(
    resolution: ProgressiveSocketAddrs,
    deadline: Option<tokio::time::Instant>,
) -> io::Result<tokio::net::TcpStream> {
    connect_tcp_with_progressive_fallback_using(
        resolution,
        deadline,
        tokio::net::TcpStream::connect,
    )
    .await
}

fn push_unseen_addrs(
    ordered: &mut VecDeque<SocketAddr>,
    seen: &mut HashSet<SocketAddr>,
    new_addrs: Vec<SocketAddr>,
) {
    let mut merged: Vec<SocketAddr> = ordered.drain(..).collect();
    for addr in new_addrs {
        if seen.insert(addr) {
            merged.push(addr);
        }
    }
    for addr in order_dual_stack_addrs(&merged) {
        ordered.push_back(addr);
    }
}

async fn connect_tcp_with_progressive_fallback_using<F, Fut>(
    resolution: ProgressiveSocketAddrs,
    deadline: Option<tokio::time::Instant>,
    connect: F,
) -> io::Result<tokio::net::TcpStream>
where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<tokio::net::TcpStream>> + Send + 'static,
{
    let mut seen = HashSet::new();
    let mut ordered = VecDeque::new();
    push_unseen_addrs(&mut ordered, &mut seen, resolution.initial);
    let mut pending_peer = resolution.pending_peer;

    if ordered.is_empty() && pending_peer.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no socket addresses to connect to",
        ));
    }
    if deadline.is_some_and(|d| {
        d.saturating_duration_since(tokio::time::Instant::now())
            .is_zero()
    }) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "TCP connect timed out",
        ));
    }
    let mut attempts = tokio::task::JoinSet::new();
    let mut last_error: Option<io::Error> = None;

    loop {
        if attempts.is_empty()
            && let Some(addr) = ordered.pop_front()
        {
            if deadline.is_some_and(|d| {
                d.saturating_duration_since(tokio::time::Instant::now())
                    .is_zero()
            }) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connect timed out",
                ));
            }
            attempts.spawn(connect(addr));
            continue;
        }

        if attempts.is_empty() && ordered.is_empty() && pending_peer.is_none() {
            break;
        }

        let now = tokio::time::Instant::now();
        if let Some(d) = deadline
            && d.saturating_duration_since(now).is_zero()
        {
            attempts.abort_all();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TCP connect timed out",
            ));
        }

        let stagger_at = if !ordered.is_empty() && !attempts.is_empty() {
            let remaining_attempts = u32::try_from(ordered.len() + 1).unwrap_or(u32::MAX);
            let base = deadline.map_or(Duration::from_millis(250), |d| {
                d.saturating_duration_since(now) / remaining_attempts
            });
            let stagger = base
                .min(Duration::from_millis(250))
                .max(Duration::from_millis(1));
            let target = now + stagger;
            Some(deadline.map_or(target, |d| target.min(d)))
        } else {
            None
        };

        tokio::select! {
            () = async {
                if let Some(d) = deadline {
                    tokio::time::sleep_until(d).await;
                }
            }, if deadline.is_some() => {
                attempts.abort_all();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connect timed out",
                ));
            }
            peer_res = async {
                pending_peer
                    .as_mut()
                    .expect("pending_peer guarded")
                    .await
            }, if pending_peer.is_some() => {
                pending_peer = None;
                match peer_res {
                    Ok(addrs) => push_unseen_addrs(&mut ordered, &mut seen, addrs),
                    Err(err) => {
                        last_error.get_or_insert_with(|| io::Error::other(err.to_string()));
                    }
                }
            }
            () = async {
                if let Some(at) = stagger_at {
                    tokio::time::sleep_until(at).await;
                }
            }, if stagger_at.is_some() => {
                if let Some(addr) = ordered.pop_front() {
                    attempts.spawn(connect(addr));
                }
            }
            joined = attempts.join_next(), if !attempts.is_empty() => {
                match joined {
                    Some(Ok(Ok(stream))) => {
                        attempts.abort_all();
                        return Ok(stream);
                    }
                    Some(Ok(Err(err))) => {
                        last_error = Some(err);
                        if let Some(addr) = ordered.pop_front() {
                            attempts.spawn(connect(addr));
                        }
                    }
                    Some(Err(join_err)) => {
                        last_error = Some(io::Error::other(join_err.to_string()));
                        if let Some(addr) = ordered.pop_front() {
                            attempts.spawn(connect(addr));
                        }
                    }
                    None => {}
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

#[cfg(test)]
async fn connect_tcp_with_fallback_using<F, Fut>(
    addrs: &[SocketAddr],
    deadline: tokio::time::Instant,
    connect: F,
) -> io::Result<tokio::net::TcpStream>
where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<tokio::net::TcpStream>> + Send + 'static,
{
    connect_tcp_with_progressive_fallback_using(
        ProgressiveSocketAddrs {
            initial: addrs.to_vec(),
            pending_peer: None,
        },
        Some(deadline),
        connect,
    )
    .await
}

struct DualStackSystemResolver {
    v4: Arc<TokioResolver>,
    v6: Arc<TokioResolver>,
    config: ResolverConfig,
    opts: ResolverOpts,
}

impl DualStackSystemResolver {
    fn new(config: &DnsResolverConfig) -> Result<Self, DnsCacheError> {
        let (resolver_config, sys_opts) = system_conf_or_default();
        let build_family = |strategy: LookupIpStrategy| {
            let mut builder = TokioResolver::builder_with_config(
                resolver_config.clone(),
                TokioRuntimeProvider::default(),
            );
            *builder.options_mut() = resolver_opts(config, sys_opts.clone(), strategy);
            builder
                .build()
                .map(Arc::new)
                .map_err(|source| DnsCacheError::Build {
                    message: source.to_string(),
                })
        };
        let v4 = build_family(LookupIpStrategy::Ipv4Only)?;
        let v6 = build_family(LookupIpStrategy::Ipv6Only)?;
        Ok(Self {
            v4,
            v6,
            config: resolver_config,
            opts: sys_opts,
        })
    }
}

async fn lookup_candidate_ips(
    resolver: Arc<TokioResolver>,
    candidate: DnsName,
) -> Result<Vec<IpAddr>, DnsCacheError> {
    let host = candidate.to_utf8();
    resolver
        .lookup_ip(candidate)
        .await
        .map(|lookup| lookup.iter().collect())
        .map_err(|source| DnsCacheError::Resolve {
            host,
            message: source.to_string(),
        })
}

impl DnsResolver for DualStackSystemResolver {
    fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
        let v4 = Arc::clone(&self.v4);
        let v6 = Arc::clone(&self.v6);
        let config = self.config.clone();
        let opts = self.opts.clone();
        Box::pin(async move {
            let progressive = resolve_search_candidates_progressive(
                &name,
                &config,
                &opts,
                move |cand| lookup_candidate_ips(Arc::clone(&v4), cand),
                move |cand| lookup_candidate_ips(Arc::clone(&v6), cand),
                DUAL_STACK_DNS_GRACE,
            )
            .await?;
            Ok(progressive.initial)
        })
    }

    fn resolve_progressive(self: Arc<Self>, name: String) -> ProgressiveDnsFuture<'static> {
        Box::pin(async move {
            let v4 = Arc::clone(&self.v4);
            let v6 = Arc::clone(&self.v6);
            resolve_search_candidates_progressive(
                &name,
                &self.config,
                &self.opts,
                move |cand| lookup_candidate_ips(Arc::clone(&v4), cand),
                move |cand| lookup_candidate_ips(Arc::clone(&v6), cand),
                DUAL_STACK_DNS_GRACE,
            )
            .await
        })
    }
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
            self.lookup_ip(name.clone())
                .await
                .map(|lookup| lookup.iter().collect())
                .map_err(|source| DnsCacheError::Resolve {
                    host: name,
                    message: source.to_string(),
                })
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

    fn resolve_progressive(self: Arc<Self>, name: String) -> ProgressiveDnsFuture<'static> {
        Arc::clone(&*self).resolve_progressive(name)
    }
}

#[cfg(test)]
type TestDialerFn = Arc<
    dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = io::Result<tokio::net::TcpStream>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct CachingDnsConnector {
    metric_resolver: MetricDnsResolver,
    connect_timeout: Option<Duration>,
    #[cfg(test)]
    dialer: Option<TestDialerFn>,
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
        Self {
            metric_resolver,
            connect_timeout,
            #[cfg(test)]
            dialer: None,
        }
    }

    #[cfg(test)]
    fn with_test_dialer<F, Fut>(mut self, dialer: F) -> Self
    where
        F: Fn(SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = io::Result<tokio::net::TcpStream>> + Send + 'static,
    {
        self.dialer = Some(Arc::new(move |addr| Box::pin(dialer(addr))));
        self
    }
}

impl Service<Uri> for CachingDnsConnector {
    type Response = TokioIo<tokio::net::TcpStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let default_port = match dst.scheme_str() {
            Some("https") => 443,
            Some("http") => 80,
            _ => {
                return Box::pin(std::future::ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid URL scheme: expected http or https",
                ))));
            }
        };
        let Some(host) = dst.host().filter(|h| !h.is_empty()).map(str::to_owned) else {
            return Box::pin(std::future::ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing URL host",
            ))));
        };
        let port = dst.port_u16().unwrap_or(default_port);
        let metric_resolver = self.metric_resolver.clone();
        let connect_timeout = self.connect_timeout;
        #[cfg(test)]
        let dialer = self.dialer.clone();

        Box::pin(async move {
            let addrs = metric_resolver
                .resolve_host_progressive(&host, port)
                .await
                .map_err(io::Error::other)?;
            let deadline = connect_timeout.map(|timeout| tokio::time::Instant::now() + timeout);
            #[cfg(test)]
            let stream = if let Some(dialer) = dialer {
                connect_tcp_with_progressive_fallback_using(addrs, deadline, move |addr| {
                    dialer(addr)
                })
                .await?
            } else {
                connect_tcp_with_progressive_fallback(addrs, deadline).await?
            };
            #[cfg(not(test))]
            let stream = connect_tcp_with_progressive_fallback(addrs, deadline).await?;
            let _ = stream.set_nodelay(true);
            Ok(TokioIo::new(stream))
        })
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
        DualStackSystemResolver::new(config)
            .map(|resolver| Self::with_resolver(Arc::new(resolver), config))
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
        let progressive = self.resolve_host_progressive(host, port).await?;
        Ok(progressive.initial)
    }

    /// Resolve a `host` and `port` into initial socket addresses plus an
    /// optional slower address family still in flight past `DUAL_STACK_DNS_GRACE`.
    pub async fn resolve_host_progressive(
        &self,
        host: &str,
        port: u16,
    ) -> Result<ProgressiveSocketAddrs, DnsCacheError> {
        if let Ok(ip) = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        {
            return Ok(ProgressiveSocketAddrs {
                initial: vec![SocketAddr::new(ip, port)],
                pending_peer: None,
            });
        }
        resolve_host_ips_progressive(
            Arc::clone(&self.resolver),
            Arc::clone(&self.metric_cache),
            self.metric_ttl,
            host.to_owned(),
            port,
        )
        .await
    }
}

async fn resolve_host_ips_progressive(
    resolver: Arc<dyn DnsResolver>,
    metric_cache: Arc<DashMap<String, Instant>>,
    metric_ttl: Duration,
    host: String,
    port: u16,
) -> Result<ProgressiveSocketAddrs, DnsCacheError> {
    let start = Instant::now();
    let now = Instant::now();
    let was_hit = metric_cache
        .get(&host)
        .map(|entry| *entry.value() > now)
        .unwrap_or(false);

    match Arc::clone(&resolver)
        .resolve_progressive(host.clone())
        .await
    {
        Ok(resolution) => {
            crate::request_timing::record_dns(
                start.elapsed(),
                crate::request_timing::DnsResolutionOutcome::Resolved,
            );
            emit_dns_metric(if was_hit { "hit" } else { "miss" });
            metric_cache.insert(host, Instant::now() + metric_ttl);
            let initial = resolution
                .initial
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect();
            let pending_peer = resolution.pending_peer.map(|fut| {
                Box::pin(async move {
                    let ips = fut.await?;
                    Ok(ips
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, port))
                        .collect())
                }) as SocketAddrsResolveFuture
            });
            Ok(ProgressiveSocketAddrs {
                initial,
                pending_peer,
            })
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

async fn resolve_host_ips(
    resolver: Arc<dyn DnsResolver>,
    metric_cache: Arc<DashMap<String, Instant>>,
    metric_ttl: Duration,
    host: String,
) -> Result<Vec<IpAddr>, DnsCacheError> {
    let progressive =
        resolve_host_ips_progressive(resolver, metric_cache, metric_ttl, host, 0).await?;
    Ok(progressive
        .initial
        .into_iter()
        .map(|addr| addr.ip())
        .collect())
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
        let peer_v6 = fast_v6.clone();
        let resolved = resolve_dual_stack_with_grace(
            async {
                Err(DnsCacheError::Resolve {
                    host: "example.test".to_owned(),
                    message: "no A record".to_owned(),
                })
            },
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(peer_v6)
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

    #[tokio::test(start_paused = true)]
    async fn progressive_dns_keeps_delayed_aaaa_available_when_ipv4_refused() {
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let healthy_v6_proxy: SocketAddr = "[::1]:8443".parse().unwrap();
        let healthy_local = healthy_listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let _ = healthy_listener.accept().await;
        });
        let refused_v4: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let resolution = resolve_dual_stack_progressive(
            Box::pin(async move { Ok(vec![refused_v4.ip()]) }),
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(70)).await;
                Ok(vec![healthy_v6_proxy.ip()])
            }),
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("initial A resolution succeeds");
        assert_eq!(resolution.initial, vec![refused_v4.ip()]);
        assert!(
            resolution.pending_peer.is_some(),
            "slower AAAA lookup past grace period must remain in pending_peer"
        );

        let socket_res = ProgressiveSocketAddrs {
            initial: resolution
                .initial
                .into_iter()
                .map(|ip| SocketAddr::new(ip, 8443))
                .collect(),
            pending_peer: resolution.pending_peer.map(|fut| {
                Box::pin(async move {
                    let ips = fut.await?;
                    Ok(ips
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, 8443))
                        .collect())
                }) as SocketAddrsResolveFuture
            }),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let stream =
            connect_tcp_with_progressive_fallback_using(socket_res, Some(deadline), move |addr| {
                Box::pin(async move {
                    if addr == refused_v4 {
                        Err(io::Error::new(io::ErrorKind::ConnectionRefused, "refused"))
                    } else {
                        tokio::net::TcpStream::connect(healthy_local).await
                    }
                })
                    as Pin<Box<dyn Future<Output = io::Result<tokio::net::TcpStream>> + Send>>
            })
            .await
            .expect("delayed AAAA must rescue refused IPv4 within deadline");
        assert_eq!(stream.peer_addr().unwrap(), healthy_local);
        accept_task.abort();
    }

    #[tokio::test]
    async fn search_candidates_preserve_hosts_overrides_without_cross_candidate_mixing() {
        let mut config = ResolverConfig::default();
        config.set_domain("internal.example".parse().unwrap());
        config.add_search("corp.local".parse().unwrap());
        let mut opts = ResolverOpts::default();
        opts.ndots = 1;

        let candidates = build_search_candidates("api", &config, &opts).unwrap();
        assert_eq!(
            candidates,
            vec![
                "api.internal.example.".parse::<DnsName>().unwrap(),
                "api.corp.local.".parse::<DnsName>().unwrap(),
                "api.".parse::<DnsName>().unwrap(),
            ]
        );
        assert!(candidates.iter().all(DnsName::is_fqdn));

        let multi_label = build_search_candidates("api.v2", &config, &opts).unwrap();
        assert_eq!(multi_label[0], "api.v2.".parse::<DnsName>().unwrap());

        let exact = build_search_candidates("api.example.com.", &config, &opts).unwrap();
        assert_eq!(exact, vec!["api.example.com.".parse::<DnsName>().unwrap()]);

        // Later search suffix has a hosts-only IPv4 entry while candidate-1 is empty; candidate-1
        // IPv4 must never combine with candidate-2 IPv6, and later suffix hosts entry must resolve.
        let v4_suffix2: IpAddr = "10.20.0.2".parse().unwrap();
        let v6_suffix2: IpAddr = "fd00::2".parse().unwrap();
        let v6_suffix1: IpAddr = "fd00::1".parse().unwrap();
        let res = resolve_search_candidates_progressive(
            "short-svc",
            &config,
            &opts,
            move |cand| {
                let s = cand.to_utf8();
                async move {
                    if s == "short-svc.corp.local." {
                        Ok(vec![v4_suffix2])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            move |cand| {
                let s = cand.to_utf8();
                async move {
                    if s == "short-svc.corp.local." {
                        Ok(vec![v6_suffix2])
                    } else if s == "short-svc.internal.example." {
                        // Candidate 1 only has v6: returns candidate 1 only without mixing with candidate 2's v4.
                        Ok(Vec::new())
                    } else {
                        let _ = v6_suffix1;
                        Ok(Vec::new())
                    }
                }
            },
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("later suffix hosts entry resolves");
        assert_eq!(res.initial, vec![v4_suffix2, v6_suffix2]);

        // Ensure candidate 1 returning only v4 does NOT pull v6 from candidate 2.
        let v4_cand1: IpAddr = "10.10.0.1".parse().unwrap();
        let res_no_mix = resolve_search_candidates_progressive(
            "split-svc",
            &config,
            &opts,
            move |cand| {
                let s = cand.to_utf8();
                async move {
                    if s == "split-svc.internal.example." {
                        Ok(vec![v4_cand1])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            move |cand| {
                let s = cand.to_utf8();
                async move {
                    if s == "split-svc.corp.local." {
                        Ok(vec![v6_suffix2])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            DUAL_STACK_DNS_GRACE,
        )
        .await
        .expect("candidate 1 resolves cleanly");
        assert_eq!(res_no_mix.initial, vec![v4_cand1]);
    }

    #[tokio::test(start_paused = true)]
    async fn caching_dns_connector_shares_one_tcp_deadline_across_families_and_progressive_peer() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_hook = Arc::clone(&attempts);
        let dual_dns = StubResolver::new(|_| async {
            Ok(vec![
                "192.0.2.1".parse::<IpAddr>().unwrap(),
                "2001:db8::1".parse::<IpAddr>().unwrap(),
            ])
        });
        let resolver =
            MetricDnsResolver::with_resolver(Arc::new(dual_dns), &DnsResolverConfig::default());
        let mut connector =
            CachingDnsConnector::from_metric_resolver(resolver, Some(Duration::from_secs(1)))
                .with_test_dialer(move |_addr| {
                    let count = Arc::clone(&attempts_hook);
                    async move {
                        count.fetch_add(1, AtomicOrdering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        Err(io::Error::new(io::ErrorKind::TimedOut, "stalled"))
                    }
                });
        let uri: Uri = "https://dual-stall.example.test/".parse().unwrap();
        let started = tokio::time::Instant::now();
        let err = connector
            .call(uri)
            .await
            .expect_err("stalled dual-stack HTTP connect must time out");
        let elapsed = started.elapsed();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(
            elapsed >= Duration::from_secs(1) && elapsed < Duration::from_millis(1100),
            "shared HTTP TCP deadline across IPv4+IPv6 must fire near 1.0s, took {elapsed:?}"
        );
        assert_eq!(
            attempts.load(AtomicOrdering::SeqCst),
            2,
            "staggered Happy Eyeballs must attempt both families inside the 1s budget"
        );

        // Also confirm CachingDnsConnector receives progressive delayed AAAA when A is refused.
        struct ProgressiveStub;
        impl DnsResolver for ProgressiveStub {
            fn resolve(&self, _name: String) -> DnsResolveFuture<'_> {
                Box::pin(async { Ok(vec!["127.0.0.1".parse().unwrap()]) })
            }

            fn resolve_progressive(
                self: Arc<Self>,
                _name: String,
            ) -> ProgressiveDnsFuture<'static> {
                Box::pin(async {
                    Ok(ProgressiveDnsResolution {
                        initial: vec!["127.0.0.1".parse().unwrap()],
                        pending_peer: Some(Box::pin(async {
                            tokio::time::sleep(Duration::from_millis(70)).await;
                            Ok(vec!["::1".parse().unwrap()])
                        })),
                    })
                })
            }
        }
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let healthy_local = healthy_listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let _ = healthy_listener.accept().await;
        });
        let prog_resolver = MetricDnsResolver::with_resolver(
            Arc::new(ProgressiveStub),
            &DnsResolverConfig::default(),
        );
        let mut prog_connector = CachingDnsConnector::from_metric_resolver(
            prog_resolver,
            Some(Duration::from_millis(500)),
        )
        .with_test_dialer(move |addr| async move {
            if addr.is_ipv4() {
                Err(io::Error::new(io::ErrorKind::ConnectionRefused, "refused"))
            } else {
                tokio::net::TcpStream::connect(healthy_local).await
            }
        });
        let io = prog_connector
            .call("http://prog.example.test:443/".parse().unwrap())
            .await
            .expect("HTTP connector must rescue refused IPv4 via delayed AAAA");
        assert_eq!(io.inner().peer_addr().unwrap(), healthy_local);
        accept_task.abort();
    }
}
