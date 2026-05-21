#![forbid(unsafe_code)]

use std::process::ExitCode;

use cc_lb_server::cli::{Cli, Command, ConfigCommand};
use cc_lb_server::{run_serve, validate};
use clap::FromArgMatches;

enum RunError {
    Validation(validate::ValidateError),
    Serve(cc_lb_server::app::ServeError),
    Other(Box<dyn std::error::Error>),
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(RunError::Validation(error)) => {
            eprintln!("validation: failed: {error}");
            ExitCode::from(2)
        }
        Err(RunError::Serve(cc_lb_server::app::ServeError::Preflight(error))) => {
            eprintln!("preflight: failed: {error}");
            ExitCode::from(2)
        }
        Err(RunError::Serve(error)) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
        Err(RunError::Other(error)) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), RunError> {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches).map_err(|error| RunError::Other(Box::new(error)))?;

    match cli.command {
        Some(Command::Serve { config }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|error| RunError::Other(Box::new(error)))?;
            runtime
                .block_on(run_serve(&config))
                .map_err(RunError::Serve)
        }
        Some(Command::Config {
            command: ConfigCommand::Validate { config },
        }) => validate::run(&config).map_err(RunError::Validation),
        None => {
            let mut command = Cli::command();
            command
                .print_help()
                .map_err(|error| RunError::Other(Box::new(error)))?;
            println!();
            Ok(())
        }
    }
}
