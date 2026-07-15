use std::path::Path;
use std::process::ExitCode;

use crate::cli::RunArgs;
use crate::multi_replica_evidence::RunEvidence;
use crate::verdict::Verdict;

pub fn run(args: RunArgs) -> ExitCode {
    let input = match RunInput::from_args(args) {
        Ok(input) => input,
        Err(error) => {
            eprintln!("invalid multi-replica run: {error}");
            return ExitCode::from(2);
        }
    };
    let _ = input.seed;
    let result = crate::multi_replica_runtime::execute(&input);
    match result {
        Ok(evidence) => finish(&input.output, evidence),
        Err(error) => {
            eprintln!("multi-replica orchestration failed: {error}");
            ExitCode::FAILURE
        }
    }
}

pub struct RunInput {
    pub seed: u64,
    pub profile: RunProfile,
    pub replicas: usize,
    pub run_id: String,
    pub output: std::path::PathBuf,
    pub only_wave: Option<String>,
}

impl RunInput {
    fn from_args(args: RunArgs) -> Result<Self, String> {
        let (seed, manifest_profile) = match (args.seed, args.manifest) {
            (Some(seed), None) => (seed, None),
            (None, Some(path)) => {
                let manifest = crate::replay::read_and_verify(&path)
                    .map_err(|error| format!("read manifest {}: {error}", path.display()))?;
                (manifest.seed, Some(manifest.profile.as_str()))
            }
            (None, None) => return Err("--seed or --manifest is required".to_owned()),
            (Some(_), Some(_)) => return Err("--seed conflicts with --manifest".to_owned()),
        };
        let profile = match (args.profile, manifest_profile) {
            (Some(profile), Some(manifest_profile)) if profile != manifest_profile => {
                return Err("--profile does not match --manifest".to_owned());
            }
            (Some(profile), _) => profile,
            (None, Some(manifest_profile)) => manifest_profile.to_owned(),
            (None, None) => return Err("--profile is required".to_owned()),
        };
        let profile = RunProfile::parse(&profile)?;
        if profile == RunProfile::Full && !full_profile_enabled() {
            return Err("--profile full requires CC_LB_STRESS_FULL=1".to_owned());
        }
        let replicas = args.replicas.unwrap_or(2);
        if replicas < 2 {
            return Err("--replicas must be at least 2".to_owned());
        }
        let only_wave = args.only_wave;
        if only_wave
            .as_deref()
            .is_some_and(|wave| wave != "storage-impairment")
        {
            return Err("--only-wave supports storage-impairment".to_owned());
        }
        Ok(Self {
            seed,
            profile,
            replicas,
            run_id: args
                .run_id
                .ok_or_else(|| "--run-id is required".to_owned())?,
            output: args
                .output
                .ok_or_else(|| "--output is required".to_owned())?,
            only_wave,
        })
    }
}

fn finish(output: &Path, evidence: RunEvidence) -> ExitCode {
    let verdict = evidence.verdict;
    match write_evidence(output, &evidence) {
        Ok(()) if matches!(verdict, Verdict::Pass | Verdict::NotComparable) => ExitCode::SUCCESS,
        Ok(()) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("write multi-replica evidence {}: {error}", output.display());
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunProfile {
    Smoke,
    Full,
}

const SMOKE_WINDOW_MS: u64 = 180_000;
const FULL_WINDOW_MS: u64 = 1_200_000;
const FULL_WINDOW_ENV: &str = "CC_LB_STRESS_FULL_WINDOW_MS";

impl RunProfile {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "full" => Ok(Self::Full),
            other => Err(format!("unsupported run profile {other}")),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
        }
    }

    pub const fn wave_count(self) -> usize {
        match self {
            Self::Smoke => 2,
            Self::Full => 6,
        }
    }

    pub const fn per_replica_allowed(self) -> u64 {
        match self {
            Self::Smoke => 250,
            Self::Full => 4_000,
        }
    }

    pub fn min_wave_execution_ms(self) -> u64 {
        match self {
            Self::Smoke => SMOKE_WINDOW_MS,
            Self::Full => full_window_ms(),
        }
    }

    pub const fn min_achieved_rps(self) -> f64 {
        match self {
            Self::Smoke => 1.0,
            Self::Full => 5.0,
        }
    }

    #[cfg(test)]
    pub const fn min_total_requests_default(self) -> u64 {
        match self {
            Self::Smoke => 180,
            Self::Full => 6_000,
        }
    }

    pub fn min_total_requests(self) -> u64 {
        requests_for_window(self.min_wave_execution_ms(), self.min_achieved_rps())
    }

    pub fn target_requests_per_wave(self) -> u64 {
        requests_for_window(self.wave_duration_ms(), self.min_achieved_rps()).max(1)
    }

    pub fn wave_duration_ms(self) -> u64 {
        self.min_wave_execution_ms()
            .checked_div(u64::try_from(self.wave_count()).unwrap_or(u64::MAX))
            .unwrap_or(1)
            .max(1)
    }

    pub fn validate_throughput(self, completed: u64, elapsed_ms: u64) -> Result<(), String> {
        if completed < self.min_total_requests() {
            return Err(format!(
                "completed {completed} requests below {} floor {}",
                self.as_str(),
                self.min_total_requests()
            ));
        }
        let achieved = completed as f64 / (elapsed_ms.max(1) as f64 / 1_000.0);
        if achieved < self.min_achieved_rps() {
            return Err(format!(
                "achieved RPS {achieved:.3} below {} floor {:.3}",
                self.as_str(),
                self.min_achieved_rps()
            ));
        }
        Ok(())
    }
}

fn full_window_ms() -> u64 {
    std::env::var(FULL_WINDOW_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value >= 1_000)
        .unwrap_or(FULL_WINDOW_MS)
}

fn requests_for_window(window_ms: u64, rps: f64) -> u64 {
    ((window_ms as f64 / 1_000.0) * rps).ceil() as u64
}

fn full_profile_enabled() -> bool {
    std::env::var("CC_LB_STRESS_FULL").as_deref() == Ok("1")
}

fn write_evidence(output: &Path, evidence: &RunEvidence) -> Result<(), String> {
    crate::evidence_redaction::write_redacted_json(&output.join("evidence.json"), evidence)
}

#[cfg(test)]
mod tests {
    use super::{RunArgs, RunInput};

    #[test]
    fn run_requires_seed_or_manifest_before_other_run_inputs() {
        // Given: a smoke run without either materialized input source.
        let args = RunArgs {
            tier: None,
            seed: None,
            manifest: None,
            profile: Some("smoke".to_owned()),
            replicas: None,
            run_id: None,
            output: None,
            only_wave: None,
        };

        // When: run input is parsed.
        let result = RunInput::from_args(args);

        // Then: the missing materialized input has the highest-priority diagnosis.
        assert!(matches!(result, Err(error) if error == "--seed or --manifest is required"));
    }

    #[test]
    fn full_profile_has_final_gate_shape() {
        // Given: the final validation profile used by Todo 15.
        let profile = super::RunProfile::Full;

        // When: its static shape is inspected.
        let wave_count = profile.wave_count();

        // Then: non-BLOCKED full evidence can satisfy the final jq contract.
        assert!(wave_count >= 6);
        assert_eq!(profile.as_str(), "full");
        assert_eq!(profile.per_replica_allowed(), 4_000);
        assert_eq!(profile.min_wave_execution_ms(), 1_200_000);
        assert_eq!(profile.min_total_requests_default(), 6_000);
        assert!(profile.min_achieved_rps() >= 1.0);
    }

    #[test]
    fn sub_one_rps_full_profile_cannot_pass() {
        let profile = super::RunProfile::Full;

        let result = profile.validate_throughput(7, 1_200_000);

        assert!(matches!(result, Err(error) if error.contains("below full floor")));
    }
}
