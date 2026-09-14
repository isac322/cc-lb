use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use cc_lb_config::{Config, ConfigOverrides, ListenerOverrides};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Print the merged cc-lb config as JSON")]
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
    let (config, warnings) =
        Config::load_with_overrides_and_warnings(&args.config, overrides(&args))?;
    print_warnings(&warnings);

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, &config)?;
    writeln!(handle)?;
    Ok(())
}
fn print_warnings(warnings: &[String]) {
    for warning in warnings {
        eprintln!("warning: {}", cc_lb_config::config_warning_message(warning));
    }
}

fn overrides(args: &Args) -> ConfigOverrides {
    ConfigOverrides::from_listener(ListenerOverrides {
        proxy_addr: args.listener_proxy_addr,
        admin_addr: args.listener_admin_addr,
        metrics_addr: args.listener_metrics_addr,
    })
}
