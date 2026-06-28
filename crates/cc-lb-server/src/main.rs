#![forbid(unsafe_code)]

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use cc_lb_server::app::{BuildError, ServeError};
use cc_lb_server::cli::{Cli, Command, ConfigCommand, DoctorCommand};
use cc_lb_server::{run_serve, validate};
use cc_lb_storage_api::{BackendKind, MetaStore, PluginRegistryStore, PluginSlot, PrincipalStore};
use clap::FromArgMatches;
use serde::Serialize;
use thiserror::Error;

enum RunError {
    Validation(validate::ValidateError),
    Serve(cc_lb_server::app::ServeError),
    Cli(clap::Error),
    Doctor(DoctorError),
    Runtime(std::io::Error),
    Help(std::io::Error),
}

#[derive(Debug, Error)]
enum DoctorError {
    #[error("storage not found: {path}")]
    StorageNotFound { path: String },
    #[error("failed to open storage: {0}")]
    StorageOpen(#[source] cc_lb_storage_api::StorageError),
    #[error("storage query failed: {0}")]
    StorageQuery(#[source] cc_lb_storage_api::StorageError),
    #[error("failed to write JSON report: {0}")]
    WriteJson(#[source] serde_json::Error),
    #[error("failed to write stdout: {0}")]
    Stdout(#[source] std::io::Error),
}

#[derive(Debug, Serialize)]
struct AbandonedChainEntriesReport {
    abandoned_chain_entries: Vec<AbandonedChainEntryReport>,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Serialize)]
struct AbandonedChainEntryReport {
    principal_id: String,
    wasm_registry_id: String,
    wasm_registry_name: String,
    slot: String,
    order: i64,
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
    let clock: cc_lb_core::ClockHandle = std::sync::Arc::new(cc_lb_core::SystemClock);

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
                .block_on(run_list_abandoned_chain_entries(clock))
                .map_err(RunError::Doctor)
        }
        None => {
            let mut command = Cli::command();
            command.print_help().map_err(RunError::Help)?;
            println!();
            Ok(())
        }
    }
}

async fn run_list_abandoned_chain_entries(
    clock: cc_lb_core::ClockHandle,
) -> Result<(), DoctorError> {
    let path = doctor_storage_path();
    if !path.exists() {
        return Err(DoctorError::StorageNotFound {
            path: path.display().to_string(),
        });
    }

    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .map_err(DoctorError::StorageOpen)?;
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .map_err(DoctorError::StorageOpen)?;
    let mut abandoned_chain_entries = Vec::new();
    let principals = storage
        .list(0, usize::MAX, true)
        .await
        .map_err(DoctorError::StorageQuery)?;

    for principal in principals {
        for slot in [
            PluginSlot::Router,
            PluginSlot::ObservabilityHook,
            PluginSlot::Shape,
        ] {
            let chain_entries = storage
                .list_chain_for_principal(principal.id, slot)
                .await
                .map_err(DoctorError::StorageQuery)?;
            for chain_entry in chain_entries {
                let Some(registry_entry) = storage
                    .get_registry_entry_by_id(chain_entry.wasm_registry_id)
                    .await
                    .map_err(DoctorError::StorageQuery)?
                else {
                    continue;
                };
                if registry_entry.supported_slots.is_empty() && !registry_entry.is_builtin {
                    abandoned_chain_entries.push(AbandonedChainEntryReport {
                        principal_id: chain_entry.principal_id.to_string(),
                        wasm_registry_id: registry_entry.id.to_string(),
                        wasm_registry_name: registry_entry.name,
                        slot: chain_entry.slot.as_str().to_owned(),
                        order: chain_entry.order,
                    });
                }
            }
        }
    }

    abandoned_chain_entries.sort();
    let report = AbandonedChainEntriesReport {
        abandoned_chain_entries,
    };
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &report).map_err(DoctorError::WriteJson)?;
    writeln!(stdout).map_err(DoctorError::Stdout)
}

fn doctor_storage_path() -> PathBuf {
    match std::env::var_os("CC_LB_STORAGE_PATH") {
        Some(path) => PathBuf::from(path),
        None => default_doctor_storage_path(),
    }
}

fn default_doctor_storage_path() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("cc-lb")
            .join("storage.sqlite"),
        None => PathBuf::from("~/.local/share/cc-lb/storage.sqlite"),
    }
}
