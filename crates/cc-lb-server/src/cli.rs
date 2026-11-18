use std::path::PathBuf;

use clap::{Args, CommandFactory, Parser, Subcommand};

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
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Doctor {
        #[command(subcommand)]
        command: DoctorCommand,
    },
    Admin {
        #[arg(long, value_name = "URL")]
        base_url: String,
        #[command(subcommand)]
        command: AdminCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    Health,
    Upstreams {
        #[command(subcommand)]
        command: AdminUpstreamsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum AdminUpstreamsCommand {
    List,
    #[command(visible_alias = "import")]
    Credential(AdminCredentialArgs),
}

#[derive(Debug, Args)]
pub struct AdminCredentialArgs {
    /// Existing upstream id to update; omit to create a new upstream.
    #[arg(long)]
    pub id: Option<String>,
    /// JSON file containing the credential request body, or `-` for stdin.
    #[arg(long, value_name = "PATH")]
    pub input: PathBuf,
    /// Revision required when updating an existing upstream.
    #[arg(long)]
    pub revision: Option<u64>,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Validate {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum DoctorCommand {
    ListAbandonedChainEntries,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_health_command_parses() {
        let cli = Cli::try_parse_from([
            "cc-lb",
            "admin",
            "--base-url",
            "http://127.0.0.1:9090",
            "health",
        ])
        .expect("admin health command should parse");

        assert!(matches!(cli.command, Some(Command::Admin { .. })));
    }

    #[test]
    fn admin_upstreams_list_command_parses() {
        let cli = Cli::try_parse_from([
            "cc-lb",
            "admin",
            "--base-url",
            "http://127.0.0.1:9090",
            "upstreams",
            "list",
        ])
        .expect("admin upstreams list command should parse");

        assert!(matches!(cli.command, Some(Command::Admin { .. })));
    }
    #[test]
    fn admin_gemini_credential_import_parses_from_stdin() {
        let cli = Cli::try_parse_from([
            "cc-lb",
            "admin",
            "--base-url",
            "http://127.0.0.1:9090",
            "upstreams",
            "credential",
            "--input",
            "-",
        ])
        .expect("credential import command should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Admin {
                command: AdminCommand::Upstreams {
                    command: AdminUpstreamsCommand::Credential(_),
                },
                ..
            })
        ));
    }
}
