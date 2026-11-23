#![deny(unsafe_code)]

use std::process::ExitCode;

use cc_lb_admin_server::cli::{Cli, Command};
use cc_lb_admin_server::{AdminBuildError, AdminServeError, run_admin_serve};
use clap::FromArgMatches;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configures jemalloc to purge unused memory in the background after ten seconds.
///
/// jemalloc reads this exported, NUL-terminated string before allocator initialization.
#[allow(non_upper_case_globals)]
#[allow(
    unsafe_code,
    reason = "jemalloc requires this exported C-ABI configuration symbol"
)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000\0";

enum RunError {
    Serve(AdminServeError),
    Cli(clap::Error),
    Runtime(std::io::Error),
    Help(std::io::Error),
}

fn main() -> ExitCode {
    cc_lb_runtime_sync::build_meta::init(
        option_env!("CC_LB_GIT_SHA"),
        option_env!("CC_LB_BUILD_TIME"),
    );

    if version_requested() {
        println!("{}", cc_lb_admin_server::version::compact_version());
        return ExitCode::SUCCESS;
    }

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(RunError::Serve(error)) => {
            eprintln!("{error}");
            serve_error_exit_code(&error)
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

fn serve_error_exit_code(error: &AdminServeError) -> ExitCode {
    match error {
        AdminServeError::Config(_)
        | AdminServeError::Build(
            AdminBuildError::Config(_)
            | AdminBuildError::StorageKeyMissing { .. }
            | AdminBuildError::InvalidStorageKey,
        ) => ExitCode::from(2),
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
    let clock: cc_lb_clock::ClockHandle = std::sync::Arc::new(cc_lb_clock::SystemClock);

    match cli.command {
        Some(Command::Serve { config, data_dir }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(RunError::Runtime)?;
            runtime
                .block_on(run_admin_serve(&config, data_dir.as_deref(), clock))
                .map_err(RunError::Serve)
        }
        None => {
            let mut command = Cli::command();
            command.print_help().map_err(RunError::Help)?;
            println!();
            Ok(())
        }
    }
}
