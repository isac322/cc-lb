#![forbid(unsafe_code)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use fake_anthropic::{AppConfig, app};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 9080)]
    port: u16,
    #[arg(long, default_value_t = 1024)]
    slow_mode_bps: u64,
    #[arg(long, default_value_t = 104_857_600)]
    files_cap_bytes: usize,
    #[arg(long, default_value_t = 3600)]
    tokens_expire_in: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("fake-anthropic listening on http://{addr}");

    let app = app(AppConfig {
        slow_mode_bps: args.slow_mode_bps,
        files_cap_bytes: args.files_cap_bytes,
        tokens_expire_in: args.tokens_expire_in,
        ..AppConfig::default()
    });
    axum::serve(listener, app).await?;

    Ok(())
}
