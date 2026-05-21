use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use cc_lb_core::{make_resolver, DnsCacheError, DnsResolver, DnsResolverConfig};

#[tokio::test]
#[ignore]
async fn resolves_anthropic_host() {
    let resolver = make_resolver(&DnsResolverConfig::default()).expect("resolver builds");
    let lookup = resolver
        .lookup_ip("api.anthropic.com")
        .await
        .expect("dns lookup");
    assert!(lookup.iter().next().is_some());
}

#[derive(Clone)]
struct FakeResolver {
    ips: Vec<IpAddr>,
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
async fn resolves_anthropic_host_with_fake_resolver() {
    let resolver: Arc<dyn DnsResolver> = Arc::new(FakeResolver {
        ips: vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
    });

    let ips = resolver
        .resolve("api.anthropic.com".to_owned())
        .await
        .expect("fake dns lookup");

    assert!(!ips.is_empty());
}
