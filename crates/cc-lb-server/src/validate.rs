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

pub fn run(config_path: &Path, clock: cc_lb_engine::ClockHandle) -> Result<(), ValidateError> {
    let (config, warnings) = Config::load_with_warnings(config_path)?;
    for warning in warnings {
        eprintln!(
            "warning: {}",
            cc_lb_config::config_warning_message(&warning)
        );
    }
    let report = run_config(&config, clock)?;
    println!("validation: ok");
    print_preflight_report(&report);
    Ok(())
}

fn run_config(
    config: &Config,
    clock: cc_lb_engine::ClockHandle,
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

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    use cc_lb_testkit::fixed_clock;

    #[test]
    fn t2__validate_cli_inprocess_runner() {
        let config = Config {
            storage: cc_lb_config::StorageConfig::Sqlite {
                path: "/definitely/missing/cc-lb/validate.sqlite".into(),
            },
            ..Config::default()
        };
        let report = run_config(&config, fixed_clock(1_700_000_000))
            .expect("offline validation skips storage, listeners, and subprocesses");
        assert_eq!(report, PreflightReport::default());
    }
}
