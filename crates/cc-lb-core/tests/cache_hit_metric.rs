use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

use cc_lb_core::{CachingDnsConnector, DnsCacheError, DnsResolver, DnsResolverConfig};
use hyper::Uri;
use metrics::{Counter, CounterFn, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use tokio::net::TcpListener;
use tower::ServiceExt;

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
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

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
struct FakeResolver {
    ips: Vec<IpAddr>,
}

impl FakeResolver {
    fn new(ips: Vec<IpAddr>) -> Self {
        Self { ips }
    }
}

impl DnsResolver for FakeResolver {
    fn resolve(
        &self,
        _name: String,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<IpAddr>, DnsCacheError>> + Send + '_>,
    > {
        let ips = self.ips.clone();
        Box::pin(async move { Ok(ips) })
    }
}

#[tokio::test]
async fn cache_hit_metric() {
    let recorder = CountingRecorder::default();
    metrics::set_global_recorder(recorder.clone()).expect("install recorder");

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let _ = listener.accept().await.expect("accept connection");
        }
    });

    let connector = CachingDnsConnector::with_resolver(
        Arc::new(FakeResolver::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])),
        &DnsResolverConfig::default(),
    );

    let uri: Uri = format!("http://api.anthropic.com:{}/", addr.port())
        .parse()
        .expect("uri");

    let _first = connector
        .clone()
        .oneshot(uri.clone())
        .await
        .expect("first connect");
    let _second = connector.oneshot(uri).await.expect("second connect");

    server.await.expect("server task");

    let counts = recorder.counts.lock().expect("recorder counts");
    let dns_miss = counts
        .get("Key(cc_lb_dns_resolve_total, [outcome = miss])")
        .copied()
        .unwrap_or(0);
    let dns_hit = counts
        .get("Key(cc_lb_dns_resolve_total, [outcome = hit])")
        .copied()
        .unwrap_or(0);
    println!("dns_hit={} dns_miss={}", dns_hit, dns_miss);
    assert_eq!(dns_miss, 1);
    assert_eq!(dns_hit, 1);
}
