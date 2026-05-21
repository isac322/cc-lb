#![forbid(unsafe_code)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use mock_anthropic_oauth_server::app;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 9085)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("mock-anthropic-oauth-server listening on http://{addr}");
    axum::serve(listener, app()).await?;
    Ok(())
}
