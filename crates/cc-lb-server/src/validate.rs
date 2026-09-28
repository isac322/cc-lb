use std::path::Path;

use cc_lb_config::Config;
use thiserror::Error;

use crate::preflight::{self, PreflightOptions, PreflightReport};

#[derive(Debug, Error)]
pub enum ValidateError {
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
    #[error(transparent)]
    Runtime(#[from] std::io::Error),
    #[error(transparent)]
    Preflight(#[from] preflight::PreflightError),
}

pub fn run(config_path: &Path, clock: cc_lb_clock::ClockHandle) -> Result<(), ValidateError> {
    let config = Config::load(config_path)?;
    let report = validate_preflight(&config, clock)?;
    println!("validation: ok");
    print_preflight_report(&report);
    Ok(())
}

fn validate_preflight(
    config: &Config,
    clock: cc_lb_clock::ClockHandle,
) -> Result<PreflightReport, ValidateError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime
        .block_on(preflight::run_offline(
            config,
            PreflightOptions { skip_bind: true },
            clock,
        ))
        .map_err(ValidateError::Preflight)
}

fn print_preflight_report(report: &PreflightReport) {
    println!("preflight: ok");
    for warning in &report.warnings {
        println!("preflight: warning: {warning}");
    }
}
