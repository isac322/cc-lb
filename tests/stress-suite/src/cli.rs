use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "cc-lb-stress-suite",
    version,
    about = "Manual stress-suite orchestration for cc-lb"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Materialize a stress-test plan.
    Plan(PlanArgs),
    /// Execute a materialized stress-test plan.
    Run(RunArgs),
    /// Replay a completed stress-test run.
    Replay(ReplayArgs),
    /// Check local stress-test prerequisites.
    Preflight(PreflightArgs),
    /// Compare stress-test evidence.
    Compare(CompareArgs),
    /// Record a comparison baseline.
    EstablishBaseline(EstablishBaselineArgs),
}

#[derive(Debug, Args)]
pub struct PlanArgs {
    #[arg(long)]
    pub seed: u64,
    #[arg(long)]
    pub profile: String,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub emit_topology: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    #[arg(long)]
    pub tier: Option<String>,
    #[arg(long)]
    pub seed: Option<u64>,
    #[arg(long, conflicts_with = "seed")]
    pub manifest: Option<PathBuf>,
    #[arg(long)]
    pub profile: Option<String>,
    #[arg(long)]
    pub replicas: Option<usize>,
    #[arg(long)]
    pub run_id: Option<String>,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub only_wave: Option<String>,
    #[arg(
        long,
        env = "CC_LB_STRESS_FULL",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "1",
        default_value = "0"
    )]
    pub enable_full_profile: bool,
    #[arg(long, env = "CC_LB_STRESS_FULL_WINDOW_MS")]
    pub full_window_ms: Option<u64>,
    #[arg(
        long,
        env = "CC_LB_STRESS_TIMED_LOAD",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "1",
        default_value = "0"
    )]
    pub timed_load: bool,
    #[arg(long, env = "CC_LB_STRESS_LOAD_WORKERS")]
    pub load_workers: Option<u64>,
    #[arg(
        long,
        env = "CC_LB_STRESS_LOAD_RAMP",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "1",
        default_value = "0"
    )]
    pub load_ramp: bool,
}

#[derive(Debug, Args)]
pub struct ReplayArgs {
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Args)]
pub struct CompareArgs {
    #[arg(long)]
    pub baseline: PathBuf,
    #[arg(long, required_unless_present = "preflight_evidence")]
    pub evidence: Option<PathBuf>,
    #[arg(long, conflicts_with = "evidence")]
    pub preflight_evidence: Option<PathBuf>,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct EstablishBaselineArgs {
    #[arg(long)]
    pub evidence: PathBuf,
    #[arg(long, default_value = "tests/stress-suite/baseline.json")]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct PreflightArgs {
    #[arg(long, conflicts_with_all = ["check_topology", "self_check", "tier"])]
    pub fabric_proof: bool,
    #[arg(long, conflicts_with = "check_topology")]
    pub tier: Option<String>,
    #[arg(long)]
    pub run_id: Option<String>,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub check_topology: Option<PathBuf>,
    #[arg(long, value_enum, conflicts_with_all = ["check_topology", "fabric_proof", "run_id", "tier"])]
    pub self_check: Option<SelfCheck>,
    #[arg(long, value_enum, requires = "self_check")]
    pub dummy: Option<DummyMode>,
    #[arg(long, requires = "self_check")]
    pub input: Option<PathBuf>,
    #[arg(
        long,
        requires = "output",
        conflicts_with_all = [
            "fabric_proof",
            "tier",
            "run_id",
            "check_topology",
            "self_check",
            "dummy",
            "input"
        ]
    )]
    pub exercise_t1_chaos: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SelfCheck {
    Supervisor,
    Classify,
    Executor,
    ExecutorSse,

    EvidenceSchema,
    Redaction,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum DummyMode {
    Ok,
    Panic,
}

fn parse_switch(value: &str) -> Result<bool, String> {
    match value {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(format!("expected 0, 1, false, or true; got {other}")),
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::Cli;

    #[test]
    fn command_surface_contains_every_scaffolded_operation() {
        let command = Cli::command();

        for name in [
            "plan",
            "run",
            "replay",
            "preflight",
            "compare",
            "establish-baseline",
        ] {
            assert!(command.find_subcommand(name).is_some());
        }
    }

    #[test]
    fn cli_help_golden() {
        // Given: the public command surface.
        let command = Cli::command();

        // When: automation resolves the run and preflight subcommands.
        let run = command.find_subcommand("run").expect("run command exists");
        let preflight = command
            .find_subcommand("preflight")
            .expect("preflight command exists");

        // Then: materialized runs and the T1 exercise are discoverable from help.
        assert!(
            run.get_arguments()
                .any(|argument| argument.get_long() == Some("manifest"))
        );
        assert!(
            preflight
                .get_arguments()
                .any(|argument| argument.get_long() == Some("exercise-t1-chaos"))
        );
    }
}
