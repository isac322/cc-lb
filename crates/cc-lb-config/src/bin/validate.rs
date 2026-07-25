use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use cc_lb_config::{Config, ConfigOverrides, ListenerOverrides};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Validate a cc-lb config file")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long = "listener-proxy-addr")]
    listener_proxy_addr: Option<SocketAddr>,
    #[arg(long = "listener-admin-addr")]
    listener_admin_addr: Option<SocketAddr>,
    #[arg(long = "listener-metrics-addr")]
    listener_metrics_addr: Option<SocketAddr>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let (_config, warnings) =
        Config::load_with_overrides_and_warnings(&args.config, overrides(&args))?;
    print_warnings(&warnings);
    println!("valid");
    Ok(())
}
fn print_warnings(fields: &[String]) {
    for field in fields {
        eprintln!(
            "warning: {}",
            cc_lb_config::removed_prompt_cache_switch_warning(field)
        );
    }
}

fn overrides(args: &Args) -> ConfigOverrides {
    ConfigOverrides::from_listener(ListenerOverrides {
        proxy_addr: args.listener_proxy_addr,
        admin_addr: args.listener_admin_addr,
        metrics_addr: args.listener_metrics_addr,
    })
}
