use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_engine::{CachingDnsConnector, DnsResolveFuture, DnsResolver, DnsResolverConfig};
use hyper::Uri;
use tokio::net::TcpListener;
use tower::ServiceExt;

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

async fn connect_once(connector: CachingDnsConnector, uri: Uri, listener: &TcpListener) -> IpAddr {
    let connection = connector
        .oneshot(uri)
        .await
        .expect("connector opens the socket");
    let (_stream, peer) = listener
        .accept()
        .await
        .expect("listener accepts the connector socket");
    drop(connection);
    peer.ip()
}

#[tokio::test]
async fn caching_dns_connector_accepts_https_uri() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind held listener");
    let address = listener.local_addr().expect("held listener address");
    let resolver = RecordingResolver::new([vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]]);
    let connector = CachingDnsConnector::with_resolver(
        Arc::new(resolver.clone()),
        &DnsResolverConfig::default(),
    );
    let uri: Uri = format!("https://api.anthropic.com:{}/", address.port())
        .parse()
        .expect("HTTPS URI");

    let peer_ip = connect_once(connector, uri, &listener).await;

    assert_eq!(resolver.calls(), vec!["api.anthropic.com".to_owned()]);
    assert_eq!(peer_ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
}

#[tokio::test(start_paused = true)]
async fn ttl_ceiling_refreshes_dns_before_connector_failover() {
    let first_ip = Ipv4Addr::LOCALHOST;
    let refreshed_ip = Ipv6Addr::LOCALHOST;
    let first_listener = TcpListener::bind((first_ip, 0))
        .await
        .expect("bind first held listener");
    let port = first_listener
        .local_addr()
        .expect("first held listener address")
        .port();
    let refreshed_listener = TcpListener::bind((refreshed_ip, port))
        .await
        .expect("bind refreshed held listener");
    let config = DnsResolverConfig {
        cache_ttl_floor: Duration::from_secs(30),
        cache_ttl_ceiling: Duration::from_secs(300),
        max_concurrent: 64,
    };
    // The second identical answer models Hickory serving its cached record at age 299s.
    // The connector must ask again at the 300s ceiling instead of extending that answer locally.
    let resolver = RecordingResolver::new([
        vec![IpAddr::V4(first_ip)],
        vec![IpAddr::V4(first_ip)],
        vec![IpAddr::V6(refreshed_ip)],
    ]);
    let connector = CachingDnsConnector::with_resolver(Arc::new(resolver.clone()), &config);
    let uri: Uri = format!("https://api.anthropic.com:{port}/")
        .parse()
        .expect("HTTPS URI");

    assert_eq!(
        connect_once(connector.clone(), uri.clone(), &first_listener).await,
        IpAddr::V4(first_ip)
    );

    tokio::time::advance(config.cache_ttl_ceiling - Duration::from_secs(1)).await;
    assert_eq!(
        connect_once(connector.clone(), uri.clone(), &first_listener).await,
        IpAddr::V4(first_ip)
    );
    drop(first_listener);

    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        connect_once(connector, uri, &refreshed_listener).await,
        IpAddr::V6(refreshed_ip)
    );
    assert_eq!(
        resolver.calls(),
        vec![
            "api.anthropic.com".to_owned(),
            "api.anthropic.com".to_owned(),
            "api.anthropic.com".to_owned(),
        ]
    );
}
