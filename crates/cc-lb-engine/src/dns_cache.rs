use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;

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
        let metric_resolver = MetricDnsResolver::new(resolver, config);
        let mut inner = HttpConnector::new_with_resolver(metric_resolver);
        inner.enforce_http(false);
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
    fn new(resolver: Arc<dyn DnsResolver>, config: &DnsResolverConfig) -> Self {
        Self {
            resolver,
            metric_cache: Arc::new(DashMap::new()),
            metric_ttl: config.cache_ttl_ceiling,
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
        let start = Instant::now();

        Box::pin(async move {
            let now = Instant::now();
            let was_hit = metric_cache
                .get(&host)
                .map(|entry| *entry.value() > now)
                .unwrap_or(false);

            let ips = resolver.resolve(host.clone()).await;
            match ips {
                Ok(ips) => {
                    crate::request_timing::record_dns(start.elapsed());
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
            "DNS resolution outcomes for the caching connector. hit and miss are heuristic classifications based on time since the previous successful resolution; hickory remains authoritative for caching and correctness."
        );
    });
}

#[cfg(test)]
#[allow(non_snake_case)]
mod t2__cache_hit_metric {
    use std::collections::{HashMap, VecDeque};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use metrics::{Counter, CounterFn, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use tower_service::Service;

    use super::{DnsResolveFuture, DnsResolver, DnsResolverConfig, MetricDnsResolver};

    #[derive(Clone, Default)]
    struct CountingRecorder {
        counts: Arc<Mutex<HashMap<String, u64>>>,
    }

    #[derive(Clone)]
    struct CountingCounter {
        counts: Arc<Mutex<HashMap<String, u64>>>,
        key: String,
    }

    impl CounterFn for CountingCounter {
        fn increment(&self, value: u64) {
            let mut counts = self.counts.lock().expect("recorder lock");
            *counts.entry(self.key.clone()).or_insert(0) += value;
        }

        fn absolute(&self, value: u64) {
            let mut counts = self.counts.lock().expect("recorder lock");
            counts.insert(self.key.clone(), value);
        }
    }

    impl Recorder for CountingRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(CountingCounter {
                counts: Arc::clone(&self.counts),
                key: key.to_string(),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[derive(Clone)]
    struct RecordingResolver {
        calls: Arc<Mutex<Vec<String>>>,
        responses: Arc<Mutex<VecDeque<Vec<IpAddr>>>>,
    }

    impl RecordingResolver {
        fn new(responses: impl IntoIterator<Item = Vec<IpAddr>>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("resolver calls").clone()
        }
    }

    impl DnsResolver for RecordingResolver {
        fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
            self.calls.lock().expect("resolver calls").push(name);
            let response = self
                .responses
                .lock()
                .expect("resolver responses")
                .pop_front()
                .expect("scripted resolver response");
            Box::pin(async move { Ok(response) })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn resolver_results_are_not_re_cached_by_the_metrics_layer() {
        let recorder = CountingRecorder::default();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let config = DnsResolverConfig {
            cache_ttl_floor: Duration::from_secs(30),
            cache_ttl_ceiling: Duration::from_secs(300),
            max_concurrent: 64,
        };
        let first_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        let refreshed_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));
        let resolver = RecordingResolver::new([vec![first_ip], vec![refreshed_ip]]);
        let mut service = MetricDnsResolver::new(Arc::new(resolver.clone()), &config);

        let first: Vec<SocketAddr> = service
            .call("api.anthropic.com".parse().expect("DNS name"))
            .await
            .expect("first resolution")
            .collect();
        assert_eq!(first, vec![SocketAddr::new(first_ip, 0)]);

        tokio::time::advance(config.cache_ttl_floor).await;
        let refreshed: Vec<SocketAddr> = service
            .call("api.anthropic.com".parse().expect("DNS name"))
            .await
            .expect("refreshed resolution")
            .collect();
        assert_eq!(refreshed, vec![SocketAddr::new(refreshed_ip, 0)]);
        assert_eq!(
            resolver.calls(),
            vec![
                "api.anthropic.com".to_owned(),
                "api.anthropic.com".to_owned(),
            ]
        );

        let (dns_miss, dns_hit) = {
            let counts = recorder.counts.lock().expect("recorder counts");
            (
                counts
                    .get("Key(cc_lb_dns_resolve_total, [outcome = miss])")
                    .copied()
                    .unwrap_or(0),
                counts
                    .get("Key(cc_lb_dns_resolve_total, [outcome = hit])")
                    .copied()
                    .unwrap_or(0),
            )
        };
        assert_eq!(dns_miss, 1);
        assert_eq!(dns_hit, 1);
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::Duration;

    use tower_service::Service;

    use super::*;
    use crate::request_timing::with_timings;

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
        MetricDnsResolver::new(Arc::new(resolver), &DnsResolverConfig::default())
    }

    #[tokio::test(start_paused = true)]
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

        assert_eq!(timings.dns_ms, Some(100));
    }

    #[tokio::test]
    async fn dns_resolver_outside_scope_silent_noop() {
        let stub = StubResolver::new(|_name| async { Ok(vec![IpAddr::from(Ipv4Addr::LOCALHOST)]) });
        let mut resolver = metric_resolver(stub);
        let name: Name = "example.com".parse().unwrap();

        let _ = resolver.call(name).await;
    }

    #[tokio::test]
    async fn dns_resolver_failure_does_not_record() {
        let stub = StubResolver::new(|name| async move {
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

        assert_eq!(timings.dns_ms, None);
    }
}
