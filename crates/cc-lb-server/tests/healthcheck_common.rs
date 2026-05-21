use std::net::SocketAddr;

use axum::{routing::any, Router};
use cc_lb_config::{AuthStrategy, Config, UpstreamKind, UpstreamSpec};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;

pub async fn spawn_upstream(
    status: axum::http::StatusCode,
) -> (SocketAddr, JoinHandle<Result<(), std::io::Error>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    let upstream = Router::new().fallback(any(move || async move { (status, "ok") }));
    let task = tokio::spawn(async move { axum::serve(listener, upstream).await });
    (addr, task)
}

pub fn config_for_upstream(upstream_addr: SocketAddr, failures_to_open: u32) -> Config {
    let mut config = Config::default();
    config.circuit_breaker.failures_to_open = failures_to_open;
    config.upstreams.insert(
        "primary".to_owned(),
        UpstreamSpec {
            kind: UpstreamKind::Custom,
            base_url: Some(Url::parse(&format!("http://{upstream_addr}")).expect("upstream url")),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
    config
}
