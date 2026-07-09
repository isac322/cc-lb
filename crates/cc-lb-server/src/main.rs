#![forbid(unsafe_code)]

use std::process::ExitCode;

use cc_lb_server::app::{BuildError, ServeError};
use cc_lb_server::cli::{Cli, Command, ConfigCommand, DoctorCommand};
use cc_lb_server::doctor::{self, DoctorError};
use cc_lb_server::subscription_quota_checkpoint_backfill::{
    self, SubscriptionQuotaBackfillCliError,
};
use cc_lb_server::{run_serve, validate};
use clap::FromArgMatches;

enum RunError {
    Validation(validate::ValidateError),
    Serve(cc_lb_server::app::ServeError),
    Cli(clap::Error),
    Doctor(DoctorError),
    SubscriptionQuotaBackfill(SubscriptionQuotaBackfillCliError),
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
            serve_error_exit_code(&error)
        }
        Err(RunError::Cli(error)) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
        Err(RunError::Doctor(DoctorError::StorageNotFound { path })) => {
            eprintln!(
                "{}",
                serde_json::json!({ "error": "storage not found", "path": path })
            );
            ExitCode::from(2)
        }
        Err(RunError::Doctor(error)) => {
            eprintln!("doctor: failed: {error}");
            ExitCode::FAILURE
        }
        Err(RunError::SubscriptionQuotaBackfill(
            SubscriptionQuotaBackfillCliError::MissingSqliteStoragePath { path },
        )) => {
            eprintln!(
                "{}",
                serde_json::json!({ "error": "storage not found", "path": path })
            );
            ExitCode::from(2)
        }
        Err(RunError::SubscriptionQuotaBackfill(error)) => {
            eprintln!("subscription quota checkpoint backfill: failed: {error}");
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

fn serve_error_exit_code(error: &ServeError) -> ExitCode {
    match error {
        ServeError::Build(BuildError::Storage(
            cc_lb_storage_api::StorageError::BackendKindMismatch { .. },
        ))
        | ServeError::Build(BuildError::StorageFactory(
            cc_lb_server::storage_factory::StorageFactoryError::BackendKindMismatch { .. },
        )) => ExitCode::from(2),
        ServeError::Build(BuildError::StorageFactory(
            cc_lb_server::storage_factory::StorageFactoryError::InitFailed { message },
        )) if message.contains("backend kind mismatch") => ExitCode::from(2),
        ServeError::Build(BuildError::StorageKeyMissing { .. }) => ExitCode::from(2),
        _ => ExitCode::FAILURE,
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
    let clock: cc_lb_engine::ClockHandle = std::sync::Arc::new(cc_lb_engine::SystemClock);

    match cli.command {
        Some(Command::Serve {
            config,
            data_dir,
            strict_preflight,
            skip_handshake_if_fresh,
            force_handshake,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(RunError::Runtime)?;
            runtime
                .block_on(run_serve(
                    &config,
                    data_dir.as_deref(),
                    strict_preflight,
                    skip_handshake_if_fresh,
                    force_handshake,
                    clock.clone(),
                ))
                .map_err(RunError::Serve)
        }
        Some(Command::Config {
            command: ConfigCommand::Validate { config, .. },
        }) => validate::run(&config, clock.clone()).map_err(RunError::Validation),
        Some(Command::Doctor {
            command: DoctorCommand::ListAbandonedChainEntries,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(RunError::Runtime)?;
            runtime
                .block_on(doctor::run_list_abandoned_chain_entries(clock))
                .map_err(RunError::Doctor)
        }
        Some(Command::CompactSubscriptionQuotaHistory {
            storage_path,
            drop_raw_observations,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(RunError::Runtime)?;
            runtime
                .block_on(subscription_quota_checkpoint_backfill::run(
                    clock,
                    storage_path,
                    drop_raw_observations,
                ))
                .map_err(RunError::SubscriptionQuotaBackfill)
        }
        None => {
            let mut command = Cli::command();
            command.print_help().map_err(RunError::Help)?;
            println!();
            Ok(())
        }
    }
}
