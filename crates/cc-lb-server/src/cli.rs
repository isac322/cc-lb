use std::path::PathBuf;

use clap::{ArgAction, CommandFactory, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cc-lb",
    version,
    about = "Anthropic-compatible cc-lb reverse proxy"
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

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        strict_preflight: bool,
        #[arg(long, action = ArgAction::Set)]
        skip_handshake_if_fresh: Option<bool>,
        #[arg(long, action = ArgAction::Set)]
        force_handshake: Option<bool>,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_flags_are_none_so_config_can_provide_defaults() {
        let cli =
            Cli::try_parse_from(["cc-lb", "serve", "--config", "cc-lb.toml"]).expect("cli parses");

        let Some(Command::Serve {
            skip_handshake_if_fresh,
            force_handshake,
            ..
        }) = cli.command
        else {
            panic!("expected serve command");
        };

        assert!(skip_handshake_if_fresh.is_none());
        assert!(force_handshake.is_none());
    }

    #[test]
    fn explicit_flags_are_propagated() {
        let cli = Cli::try_parse_from([
            "cc-lb",
            "serve",
            "--config",
            "cc-lb.toml",
            "--skip-handshake-if-fresh",
            "false",
            "--force-handshake",
            "true",
        ])
        .expect("cli parses");

        let Some(Command::Serve {
            skip_handshake_if_fresh,
            force_handshake,
            ..
        }) = cli.command
        else {
            panic!("expected serve command");
        };

        assert_eq!(skip_handshake_if_fresh, Some(false));
        assert_eq!(force_handshake, Some(true));
    }
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Validate {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
    },
}
