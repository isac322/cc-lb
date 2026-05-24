use std::net::SocketAddr;
use std::path::PathBuf;

use axum::{routing::any, Router};
use cc_lb_config::{
    AuthStrategy, Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, UpstreamKind,
    UpstreamSpec,
};
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

pub fn configure_builtin_auth(config: &mut Config) {
    std::env::set_var(
        "CC_LB_MASTER_KEY",
        "0000000000000000000000000000000000000000000000000000000000000000",
    );
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: "api-key".to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        upstream_credential_ref: "fake_anthropic".to_owned(),
    });
    config.storage.redb_path = Some(unique_redb_path("cc-lb-health"));
}

fn unique_redb_path(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.redb", std::process::id()))
}

pub fn config_for_upstream(upstream_addr: SocketAddr, failures_to_open: u32) -> Config {
    let mut config = Config::default();
    configure_builtin_auth(&mut config);
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
