use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_engine::{CachingDnsConnector, DnsResolveFuture, DnsResolver, DnsResolverConfig};
use hyper::Uri;
use tokio::net::TcpListener;
use tower::ServiceExt;

#[derive(Clone)]
struct RecordingResolver {
    calls: Arc<Mutex<Vec<String>>>,
}

impl RecordingResolver {
    fn new() -> Self {
        Self {
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
        Box::pin(async { Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]) })
    }
}

#[tokio::test]
async fn t3__caching_dns_connector_accepts_https_uri() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind held listener");
    let address = listener.local_addr().expect("held listener address");
    let server = tokio::spawn(async move {
        let (_stream, peer) = listener.accept().await.expect("accept connector socket");
        peer
    });

    let resolver = RecordingResolver::new();
    let connector = CachingDnsConnector::with_resolver(
        Arc::new(resolver.clone()),
        &DnsResolverConfig::default(),
    );
    let uri: Uri = format!("https://api.anthropic.com:{}/", address.port())
        .parse()
        .expect("HTTPS URI");

    let result = tokio::time::timeout(Duration::from_secs(5), connector.oneshot(uri))
        .await
        .expect("connector completes");
    if let Err(error) = &result {
        let message = error.to_string();
        assert!(
            !message.contains("invalid URL")
                && !message.contains("URL scheme")
                && !message.contains("scheme is not http"),
            "connector rejected HTTPS scheme: {message}"
        );
    }
    let connection = result.expect("connector accepts HTTPS and opens the socket");
    let peer = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("listener accepts the connector socket")
        .expect("listener task completes");

    assert_eq!(resolver.calls(), vec!["api.anthropic.com".to_owned()]);
    assert_eq!(peer.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    drop(connection);
}
