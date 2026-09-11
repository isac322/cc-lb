use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

use cc_lb_engine::{DnsResolveFuture, DnsResolver, DnsResolverConfig, make_resolver_with_factory};

#[tokio::test]
async fn resolver_factory_supports_deterministic_mock_lookup() {
    let resolver = make_resolver_with_factory(&DnsResolverConfig::default(), |_, _, _| {
        Arc::new(RecordingResolver::new(vec![IpAddr::V4(
            Ipv4Addr::LOCALHOST,
        )]))
    });
    let lookup = resolver
        .resolve("api.anthropic.com".to_owned())
        .await
        .expect("dns lookup");

    assert!(!lookup.is_empty());
    assert_eq!(lookup, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
    assert_eq!(resolver.calls(), vec!["api.anthropic.com".to_owned()]);
}

#[derive(Clone)]
struct RecordingResolver {
    ips: Vec<IpAddr>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl RecordingResolver {
    fn new(ips: Vec<IpAddr>) -> Self {
        Self {
            ips,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("resolver calls").clone()
    }
}

impl DnsResolver for RecordingResolver {
    fn resolve(&self, name: String) -> DnsResolveFuture<'_> {
        self.calls.lock().expect("resolver calls").push(name);
        let ips = self.ips.clone();
        Box::pin(async move { Ok(ips) })
    }
}

#[tokio::test]
async fn resolves_anthropic_host_with_fake_resolver() {
    let resolver = RecordingResolver::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);

    let ips = resolver
        .resolve("api.anthropic.com".to_owned())
        .await
        .expect("fake dns lookup");

    assert!(!ips.is_empty());
    assert_eq!(resolver.calls(), vec!["api.anthropic.com".to_owned()]);
}
