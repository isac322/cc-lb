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
    Doctor {
        #[command(subcommand)]
        command: DoctorCommand,
    },
    /// Compact subscription quota history into change-only checkpoints.
    ///
    /// Replays raw observations to create sparse checkpoints with exact timestamps.
    /// This separates history from freshness, as the latest cache still updates on
    /// every observation. Source provenance (header and api) is preserved and merged
    /// at read time.
    CompactSubscriptionQuotaHistory {
        /// Path to the SQLite database file; required with --drop-raw-observations.
        #[arg(long, value_name = "PATH")]
        storage_path: Option<PathBuf>,
        /// Drop the raw observations table and VACUUM the database after validation.
        ///
        /// This is a destructive operation that reclaims disk space. It validates
        /// checkpoint parity before dropping any data. If validation fails, it aborts.
        /// This flag is rejected unless --storage-path is explicit.
        #[arg(long, action = ArgAction::SetTrue)]
        drop_raw_observations: bool,
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

#[derive(Debug, Subcommand)]
pub enum DoctorCommand {
    ListAbandonedChainEntries,
}
