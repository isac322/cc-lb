use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::cli::{DummyMode, PreflightArgs, SelfCheck};

pub fn run(args: PreflightArgs) -> ExitCode {
    if args.exercise_t1_chaos {
        let Some(output) = args.output.as_deref() else {
            eprintln!("preflight --exercise-t1-chaos requires --output");
            return ExitCode::from(2);
        };
        return crate::t1_chaos::run(output);
    }
    if let Some(path) = args.check_topology {
        return crate::topology_cli::check_topology(&path);
    }
    if let Some(self_check) = args.self_check {
        return run_self_check(
            self_check,
            SelfCheckInputs {
                dummy: args.dummy,
                input: args.input.as_deref(),
                output: args.output.as_deref(),
            },
        );
    }
    if !args.fabric_proof {
        let Some(tier) = args.tier.as_deref() else {
            return unavailable();
        };
        return match tier {
            "docker-netem" => run_docker_netem(args),
            "elevated-netns" => run_elevated_preview(args.output),
            _ => {
                eprintln!("unsupported preflight tier: {tier}");
                ExitCode::from(2)
            }
        };
    }
    run_fabric_proof(args)
}

struct SelfCheckInputs<'a> {
    dummy: Option<DummyMode>,
    input: Option<&'a Path>,
    output: Option<&'a Path>,
}

fn run_self_check(self_check: SelfCheck, inputs: SelfCheckInputs<'_>) -> ExitCode {
    match self_check {
        SelfCheck::Supervisor => {
            let Some(dummy) = inputs.dummy else {
                eprintln!("preflight --self-check=supervisor requires --dummy");
                return ExitCode::from(2);
            };
            crate::supervisor_runner::run_self_check(dummy, inputs.output)
        }
        SelfCheck::Classify => {
            let Some(input) = inputs.input else {
                eprintln!("preflight --self-check=classify requires --input");
                return ExitCode::from(2);
            };
            let Some(output) = inputs.output else {
                eprintln!("preflight --self-check=classify requires --output");
                return ExitCode::from(2);
            };
            match crate::request_classifier::run(input, output) {
                Ok(report) if report.all_correct => ExitCode::SUCCESS,
                Ok(_) => ExitCode::FAILURE,
                Err(error) => {
                    eprintln!("classify self-check failed: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        SelfCheck::Executor => {
            let Some(output) = inputs.output else {
                eprintln!("preflight --self-check=executor requires --output");
                return ExitCode::from(2);
            };
            crate::executor_self_check::run_executor(output)
        }
        SelfCheck::ExecutorSse => {
            let Some(input) = inputs.input else {
                eprintln!("preflight --self-check=executor-sse requires --input");
                return ExitCode::from(2);
            };
            let Some(output) = inputs.output else {
                eprintln!("preflight --self-check=executor-sse requires --output");
                return ExitCode::from(2);
            };
            crate::executor_self_check::run_sse(input, output)
        }
        SelfCheck::EvidenceSchema => {
            let Some(output) = inputs.output else {
                eprintln!("preflight --self-check=evidence-schema requires --output");
                return ExitCode::from(2);
            };
            crate::evidence_self_check::run_evidence_schema(output)
        }
        SelfCheck::Redaction => {
            let Some(input) = inputs.input else {
                eprintln!("preflight --self-check=redaction requires --input");
                return ExitCode::from(2);
            };
            let Some(output) = inputs.output else {
                eprintln!("preflight --self-check=redaction requires --output");
                return ExitCode::from(2);
            };
            crate::evidence_self_check::run_redaction(input, output)
        }
    }
}

fn run_docker_netem(args: PreflightArgs) -> ExitCode {
    let Some(run_id) = args.run_id else {
        eprintln!("preflight --tier docker-netem requires --run-id");
        return ExitCode::from(2);
    };
    let Some(output) = args.output else {
        eprintln!("preflight --tier docker-netem requires --output");
        return ExitCode::from(2);
    };
    crate::preflight::run_docker_netem(&run_id, &output)
}

fn run_elevated_preview(output: Option<PathBuf>) -> ExitCode {
    let Some(output) = output else {
        eprintln!("preflight --tier elevated-netns requires --output");
        return ExitCode::from(2);
    };
    crate::elevated_preview::run_preview(&output)
}

fn run_fabric_proof(args: PreflightArgs) -> ExitCode {
    let Some(run_id) = args.run_id else {
        eprintln!("preflight --fabric-proof requires --run-id");
        return ExitCode::from(2);
    };
    let Some(output) = args.output else {
        eprintln!("preflight --fabric-proof requires --output");
        return ExitCode::from(2);
    };
    let decision = crate::fabric::prove(&run_id);
    if let Err(error) = write_decision(&output, &decision) {
        eprintln!(
            "failed to write fabric-proof output {}: {error}",
            output.display()
        );
        return ExitCode::FAILURE;
    }
    match decision.verdict {
        crate::topology_decision::Verdict::Pass => ExitCode::SUCCESS,
        crate::topology_decision::Verdict::Blocked => ExitCode::FAILURE,
    }
}

fn write_decision(
    path: &Path,
    decision: &crate::topology_decision::TopologyDecision,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create output directory: {error}"))?;
    }
    let bytes = serde_json::to_vec_pretty(decision)
        .map_err(|error| format!("serialize topology decision: {error}"))?;
    std::fs::write(path, bytes).map_err(|error| format!("write output file: {error}"))
}

fn unavailable() -> ExitCode {
    eprintln!("preflight is not available in the initial scaffold");
    ExitCode::FAILURE
}
