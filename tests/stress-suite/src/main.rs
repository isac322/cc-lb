#![forbid(unsafe_code)]

use std::path::Path;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;

mod cli;
mod comparison_baseline;
mod comparison_cli;
mod comparison_engine;
mod comparison_metrics;
mod comparison_report;
#[cfg(test)]
mod comparison_tests;
mod docker;
mod elevated_preview;
pub mod evidence_environment;
mod evidence_metrics;
pub mod evidence_qdisc;
mod evidence_redaction;
mod evidence_schema;
mod evidence_self_check;
#[cfg(test)]
mod evidence_tests;
mod executor;
mod executor_evidence;
mod executor_http;
mod executor_metrics;
mod executor_self_check;
mod fabric;
mod fabric_commands;
mod fabric_inspect;
mod fabric_probe;
mod fabric_runtime;
mod fabric_state;
mod manifest;
mod multi_replica;
mod multi_replica_auth;
mod multi_replica_config;
mod multi_replica_evidence;
mod multi_replica_final;
mod multi_replica_http;
mod multi_replica_load;
mod multi_replica_load_timed;
mod multi_replica_runtime;
mod multi_replica_storage;
mod multi_replica_waves;
mod netem_render;
mod planner;
mod preflight;
mod preflight_cli;
mod preflight_evidence;
mod replay;
mod request_classifier;
mod sse_timing;
mod supervisor;
mod supervisor_process;
mod supervisor_runner;
mod t1_chaos;
mod t1_chaos_runtime;
mod topology_cli;
mod topology_decision;
mod topology_spec;
mod traffic;
mod traffic_catalog_data;
mod traffic_generator;
mod verdict;

use crate::cli::{Cli, Command, PlanArgs, ReplayArgs, RunArgs};

#[cfg(test)]
mod elevated_preview_tests;
#[cfg(test)]
mod executor_tests;
#[cfg(test)]
mod manifest_tests;
#[cfg(test)]
mod multi_replica_tests;
#[cfg(test)]
mod preflight_tests;
#[cfg(test)]
mod request_classifier_tests;
#[cfg(test)]
mod supervisor_tests;
#[cfg(test)]
mod topology_decision_tests;
#[cfg(test)]
mod topology_spec_tests;
#[cfg(test)]
mod traffic_tests;

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Plan(args) => run_plan(args),
        Command::Run(args) => run_stress(args),
        Command::Replay(args) => run_replay(args),
        Command::Preflight(args) => preflight_cli::run(args),
        Command::Compare(args) => comparison_cli::run_compare(args),
        Command::EstablishBaseline(args) => comparison_cli::run_establish_baseline(args),
    }
}

fn run_stress(args: RunArgs) -> ExitCode {
    match args.tier.as_deref() {
        Some("elevated-netns") => {
            eprintln!("elevated_execution_forbidden");
            ExitCode::FAILURE
        }
        Some(_) => unavailable("run"),
        None => multi_replica::run(args),
    }
}

fn run_plan(args: PlanArgs) -> ExitCode {
    match (args.output, args.emit_topology) {
        (Some(output), None) => materialize_plan(args.seed, &args.profile, &output),
        (None, Some(output)) => match u32::try_from(args.seed) {
            Ok(seed) => topology_cli::emit_topology(seed, &args.profile, &output),
            Err(_) => {
                eprintln!("--emit-topology requires a seed that fits in u32");
                ExitCode::from(2)
            }
        },
        (Some(_), Some(_)) => {
            eprintln!("plan accepts either --output or --emit-topology, not both");
            ExitCode::from(2)
        }
        (None, None) => {
            eprintln!("plan requires --output or --emit-topology");
            ExitCode::from(2)
        }
    }
}

fn materialize_plan(seed: u64, profile: &str, output: &Path) -> ExitCode {
    let profile = match profile.parse() {
        Ok(profile) => profile,
        Err(error) => {
            eprintln!("invalid plan profile: {error}");
            return ExitCode::from(2);
        }
    };
    let generated_at = match unix_millis() {
        Ok(generated_at) => generated_at,
        Err(error) => {
            eprintln!("failed to read plan timestamp: {error}");
            return ExitCode::FAILURE;
        }
    };
    let manifest = match planner::plan(planner::PlanInput::new(seed, profile, generated_at)) {
        Ok(manifest) => manifest,
        Err(error) => {
            eprintln!("failed to materialize manifest: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = write_manifest(output, &manifest) {
        eprintln!("failed to write manifest {}: {error}", output.display());
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run_replay(args: ReplayArgs) -> ExitCode {
    if !args.dry_run {
        eprintln!("replay currently requires --dry-run");
        return ExitCode::from(2);
    }
    match replay::read_and_verify(&args.manifest) {
        Ok(manifest) => {
            println!(
                "replay dry-run verified manifest integrity for {}",
                manifest.run_id
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn unix_millis() -> Result<u64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?;
    u64::try_from(duration.as_millis()).map_err(|error| error.to_string())
}

fn write_manifest(path: &Path, manifest: &manifest::Manifest) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create output directory: {error}"))?;
    }
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| format!("serialize manifest: {error}"))?;
    std::fs::write(path, bytes).map_err(|error| format!("write output file: {error}"))
}

fn unavailable(command: &str) -> ExitCode {
    eprintln!("{command} is not available in the initial scaffold");
    ExitCode::FAILURE
}
