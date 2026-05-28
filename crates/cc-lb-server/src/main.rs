#![forbid(unsafe_code)]

use std::process::ExitCode;

use cc_lb_server::app::ServeError;
use cc_lb_server::cli::{Cli, Command, ConfigCommand};
use cc_lb_server::{run_serve, validate};
use clap::FromArgMatches;

enum RunError {
    Validation(validate::ValidateError),
    Serve(cc_lb_server::app::ServeError),
    Cli(clap::Error),
    Runtime(std::io::Error),
    Help(std::io::Error),
}

fn main() -> ExitCode {
    if version_requested() {
        println!("{}", cc_lb_server::version::compact_version());
        return ExitCode::SUCCESS;
    }

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(RunError::Validation(error)) => {
            eprintln!("validation: failed: {error}");
            ExitCode::from(2)
        }
        Err(RunError::Serve(ServeError::Preflight(error))) => {
            eprintln!("preflight: failed: {error}");
            ExitCode::from(2)
        }
        Err(RunError::Serve(error)) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
        Err(RunError::Cli(error)) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
        Err(RunError::Runtime(error)) => {
            eprintln!("failed to build tokio runtime: {error}");
            ExitCode::FAILURE
        }
        Err(RunError::Help(error)) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn version_requested() -> bool {
    std::env::args_os()
        .skip(1)
        .any(|arg| arg == "-V" || arg == "--version")
}

fn run() -> Result<(), RunError> {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches).map_err(RunError::Cli)?;

    match cli.command {
        Some(Command::Serve {
            config,
            data_dir,
            strict_preflight,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(RunError::Runtime)?;
            runtime
                .block_on(run_serve(&config, data_dir.as_deref(), strict_preflight))
                .map_err(RunError::Serve)
        }
        Some(Command::Config {
            command: ConfigCommand::Validate { config, .. },
        }) => validate::run(&config).map_err(RunError::Validation),
        None => {
            let mut command = Cli::command();
            command.print_help().map_err(RunError::Help)?;
            println!();
            Ok(())
        }
    }
}
