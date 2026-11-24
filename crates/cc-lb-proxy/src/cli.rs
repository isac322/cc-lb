use std::path::PathBuf;

use cc_lb_server::ProxyRole;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "cc-lb-proxy",
    version,
    about = "cc-lb split proxy/worker data-plane binary"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Cli {
    pub fn command() -> clap::Command {
        let version: &'static str = Box::leak(crate::version::format_version().into_boxed_str());
        <Self as CommandFactory>::command().version(version)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum CliProxyRole {
    /// Serve proxy traffic and run background scheduler workers.
    ProxyWorker,
    /// Serve proxy traffic only; do not spawn background scheduler workers.
    Proxy,
    /// Run background scheduler workers and `/healthz`/`/readyz` only; disable proxy routes.
    Worker,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        strict_preflight: bool,
        #[arg(long, value_enum)]
        role: Option<CliProxyRole>,
        #[arg(long)]
        no_worker: bool,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Validate {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
    },
}

/// Resolves `--role` and `--no-worker` flags into a concrete [`ProxyRole`].
pub fn resolve_role(
    role: Option<CliProxyRole>,
    no_worker: bool,
) -> Result<ProxyRole, &'static str> {
    match (role, no_worker) {
        (None, false) | (Some(CliProxyRole::ProxyWorker), false) => Ok(ProxyRole::ProxyWorker),
        (None, true) | (Some(CliProxyRole::Proxy), _) => Ok(ProxyRole::ProxyOnly),
        (Some(CliProxyRole::Worker), false) => Ok(ProxyRole::WorkerOnly),
        (Some(CliProxyRole::ProxyWorker | CliProxyRole::Worker), true) => {
            Err("--no-worker cannot be combined with --role proxy-worker or --role worker")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_role_flags_resolve_expected_proxy_role() {
        let cases = [
            (
                ["cc-lb-proxy", "serve", "--config", "/tmp/c.toml"].as_slice(),
                Ok(ProxyRole::ProxyWorker),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--no-worker",
                ]
                .as_slice(),
                Ok(ProxyRole::ProxyOnly),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--role",
                    "proxy",
                ]
                .as_slice(),
                Ok(ProxyRole::ProxyOnly),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--role",
                    "proxy-worker",
                ]
                .as_slice(),
                Ok(ProxyRole::ProxyWorker),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--role",
                    "worker",
                ]
                .as_slice(),
                Ok(ProxyRole::WorkerOnly),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--role",
                    "proxy",
                    "--no-worker",
                ]
                .as_slice(),
                Ok(ProxyRole::ProxyOnly),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--role",
                    "worker",
                    "--no-worker",
                ]
                .as_slice(),
                Err("--no-worker cannot be combined with --role proxy-worker or --role worker"),
            ),
            (
                [
                    "cc-lb-proxy",
                    "serve",
                    "--config",
                    "/tmp/c.toml",
                    "--role",
                    "proxy-worker",
                    "--no-worker",
                ]
                .as_slice(),
                Err("--no-worker cannot be combined with --role proxy-worker or --role worker"),
            ),
        ];

        for (argv, expected) in cases {
            let cli = Cli::try_parse_from(argv).expect("cli parse should succeed");
            let Some(Command::Serve {
                role, no_worker, ..
            }) = cli.command
            else {
                panic!("expected serve command");
            };
            assert_eq!(resolve_role(role, no_worker), expected, "argv={argv:?}");
        }
    }
}
