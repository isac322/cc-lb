#![deny(unsafe_code)]

use std::io::{Read, Write};
use std::path::Path;
use std::process::ExitCode;

use cc_lb_server::app::{BuildError, ServeError};
use cc_lb_server::cli::{
    AdminCommand, AdminUpstreamsCommand, Cli, Command, ConfigCommand, DoctorCommand,
};
use cc_lb_server::doctor::{self, DoctorError};
use cc_lb_server::{run_serve, validate};
use clap::FromArgMatches;
use reqwest::{Method, StatusCode};
use serde_json::Value;
use thiserror::Error;

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
#[derive(Debug, Error)]
enum AdminCliError {
    #[error("missing required environment variable CC_LB_ADMIN_TOKEN")]
    MissingToken,
    #[error("admin request failed")]
    Request(#[source] reqwest::Error),
    #[error("admin API returned HTTP status {0}")]
    HttpStatus(StatusCode),
    #[error("failed to write admin response: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid credential JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    InvalidInput(&'static str),
}

enum RunError {
    Validation(validate::ValidateError),
    Serve(cc_lb_server::app::ServeError),
    Cli(clap::Error),
    Admin(AdminCliError),
    Doctor(DoctorError),
    Runtime(std::io::Error),
    Help(std::io::Error),
}

const ADMIN_TOKEN_ENV: &str = "CC_LB_ADMIN_TOKEN";

async fn run_admin(base_url: &str, command: AdminCommand) -> Result<(), AdminCliError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        .build()
        .map_err(AdminCliError::Request)?;
    let (method, path, body, revision, token) = match command {
        AdminCommand::Health => {
            let url = format!("{}/admin/health", base_url.trim_end_matches('/'));
            let response = client
                .get(url)
                .send()
                .await
                .map_err(AdminCliError::Request)?;
            return write_admin_response(response).await;
        }
        AdminCommand::Upstreams {
            command: AdminUpstreamsCommand::List,
        } => (
            Method::GET,
            "/admin/v1/upstreams".to_owned(),
            None,
            None,
            std::env::var(ADMIN_TOKEN_ENV).map_err(|_| AdminCliError::MissingToken)?,
        ),
        AdminCommand::Upstreams {
            command: AdminUpstreamsCommand::Credential(args),
        } => {
            let input = if args.input.as_os_str() == "-" {
                let mut input = String::new();
                std::io::stdin().read_to_string(&mut input)?;
                input
            } else {
                std::fs::read_to_string(Path::new(&args.input))?
            };
            if input.trim().is_empty() {
                return Err(AdminCliError::InvalidInput("credential input is empty"));
            }
            let body: Value = serde_json::from_str(&input)?;
            let token = std::env::var(ADMIN_TOKEN_ENV).map_err(|_| AdminCliError::MissingToken)?;
            if let Some(id) = args.id {
                let revision = args.revision.ok_or(AdminCliError::InvalidInput(
                    "--revision is required when updating an upstream",
                ))?;
                (
                    Method::PUT,
                    format!("/admin/v1/upstreams/{id}"),
                    Some(body),
                    Some(revision),
                    token,
                )
            } else {
                (
                    Method::POST,
                    "/admin/v1/upstreams".to_owned(),
                    Some(body),
                    None,
                    token,
                )
            }
        }
    };
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    let mut request = client.request(method, url).bearer_auth(token);
    if let Some(revision) = revision {
        request = request.header("If-Match", revision.to_string());
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.map_err(AdminCliError::Request)?;
    write_admin_response(response).await
}

async fn write_admin_response(response: reqwest::Response) -> Result<(), AdminCliError> {
    let status = response.status();
    if !status.is_success() {
        return Err(AdminCliError::HttpStatus(status));
    }
    let body = response.bytes().await.map_err(AdminCliError::Request)?;
    std::io::stdout().lock().write_all(&body)?;
    Ok(())
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
        Err(RunError::Admin(error)) => {
            eprintln!("admin: failed: {error}");
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
    let clock: cc_lb_clock::ClockHandle = std::sync::Arc::new(cc_lb_clock::SystemClock);

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
                .block_on(run_serve(
                    &config,
                    data_dir.as_deref(),
                    strict_preflight,
                    clock.clone(),
                ))
                .map_err(RunError::Serve)
        }
        Some(Command::Config {
            command: ConfigCommand::Validate { config },
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
        Some(Command::Admin { base_url, command }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(RunError::Runtime)?;
            runtime
                .block_on(run_admin(&base_url, command))
                .map_err(RunError::Admin)
        }
        None => {
            let mut command = Cli::command();
            command.print_help().map_err(RunError::Help)?;
            println!();
            Ok(())
        }
    }
}
