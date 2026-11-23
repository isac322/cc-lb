use std::path::PathBuf;

use clap::{CommandFactory, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cc-lb-admin",
    version,
    about = "cc-lb split admin control-plane binary"
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
    },
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn serve_command_parses_config_and_data_dir() {
        let cli = Cli::try_parse_from([
            "cc-lb-admin",
            "serve",
            "--config",
            "/etc/cc-lb/cc-lb.toml",
            "--data-dir",
            "/var/lib/cc-lb",
        ])
        .expect("admin serve command should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Serve {
                config,
                data_dir: Some(data_dir),
            }) if config == Path::new("/etc/cc-lb/cc-lb.toml")
                && data_dir == Path::new("/var/lib/cc-lb")
        ));
    }
}
