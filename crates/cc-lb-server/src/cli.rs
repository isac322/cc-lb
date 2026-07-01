use clap::{CommandFactory, Parser, Subcommand};

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
        #[arg(long)]
        strict_preflight: bool,
    },
    Doctor {
        #[command(subcommand)]
        command: DoctorCommand,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_parses_with_no_flags() {
        let cli = Cli::try_parse_from(["cc-lb", "serve"]).expect("cli parses");
        let Some(Command::Serve { strict_preflight }) = cli.command else {
            panic!("expected serve command");
        };
        assert!(!strict_preflight);
    }

    #[test]
    fn serve_strict_preflight_flag_propagates() {
        let cli =
            Cli::try_parse_from(["cc-lb", "serve", "--strict-preflight"]).expect("cli parses");
        let Some(Command::Serve { strict_preflight }) = cli.command else {
            panic!("expected serve command");
        };
        assert!(strict_preflight);
    }

    #[test]
    fn serve_rejects_removed_runtime_flags() {
        for arg in [
            "--data-dir",
            "--skip-handshake-if-fresh",
            "--force-handshake",
        ] {
            let err = Cli::try_parse_from(["cc-lb", "serve", arg, "x"])
                .expect_err(&format!("flag {arg} must be removed"));
            let rendered = err.to_string();
            assert!(
                rendered.contains("unexpected argument") || rendered.contains(arg),
                "expected {arg} rejection, got: {rendered}"
            );
        }
    }

    #[test]
    fn serve_rejects_removed_config_flag() {
        let err = Cli::try_parse_from(["cc-lb", "serve", "--config", "cc-lb.toml"])
            .expect_err("--config flag must be rejected after env+storage migration");
        let rendered = err.to_string();
        assert!(
            rendered.contains("unexpected argument") || rendered.contains("--config"),
            "expected --config rejection, got: {rendered}"
        );
    }

    #[test]
    fn config_subcommand_is_removed() {
        let err = Cli::try_parse_from(["cc-lb", "config", "validate"])
            .expect_err("`config` subcommand must be removed");
        let rendered = err.to_string();
        assert!(
            rendered.contains("unrecognized subcommand")
                || rendered.contains("invalid")
                || rendered.contains("did you mean"),
            "expected config subcommand rejection, got: {rendered}"
        );
    }
}

#[derive(Debug, Subcommand)]
pub enum DoctorCommand {
    ListAbandonedChainEntries,
}
