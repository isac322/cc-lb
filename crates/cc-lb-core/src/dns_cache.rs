use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hickory_resolver::config::{LookupIpStrategy, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::TokioResolver;
use hyper::Uri;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::client::legacy::connect::HttpConnector;
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
    make_resolver_with_factory(config, |resolver_config, opts, provider| {
        let mut builder = TokioResolver::builder_with_config(resolver_config, provider);
        *builder.options_mut() = opts;
        builder
            .build()
            .map(Arc::new)
            .map_err(|source| DnsCacheError::Build {
                message: source.to_string(),
            })
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
    opts.ip_strategy = LookupIpStrategy::Ipv4thenIpv6;
    opts
}

impl DnsResolver for TokioResolver {
    fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
        Box::pin(async move {
            let host = name.clone();
            let lookup = self
                .lookup_ip(name)
                .await
                .map_err(|source| DnsCacheError::Resolve {
                    host,
                    message: source.to_string(),
                })?;
            Ok(lookup.iter().collect())
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
        make_resolver(config).map(|resolver| Self::with_resolver(resolver, config))
    }

    pub fn with_resolver(resolver: Arc<dyn DnsResolver>, config: &DnsResolverConfig) -> Self {
        register_dns_metrics();
        let metric_resolver = MetricDnsResolver {
            resolver,
            metric_cache: Arc::new(DashMap::new()),
            metric_ttl: config.cache_ttl_ceiling,
        };
        Self {
            inner: HttpConnector::new_with_resolver(metric_resolver),
        }
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
            let now = Instant::now();
            let was_hit = metric_cache
                .get(&host)
                .map(|entry| *entry.value() > now)
                .unwrap_or(false);

            let ips = resolver.resolve(host.clone()).await;
            match ips {
                Ok(ips) => {
                    emit_dns_metric(if was_hit { "hit" } else { "miss" });
                    metric_cache.insert(host, Instant::now() + metric_ttl);
                    Ok(ips
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, 0))
                        .collect::<Vec<_>>()
                        .into_iter())
                }
                Err(err) => {
                    emit_dns_metric("error");
                    Err(err)
                }
            }
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
