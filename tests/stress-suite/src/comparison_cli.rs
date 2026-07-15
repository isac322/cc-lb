use std::path::Path;
use std::process::ExitCode;

use crate::cli::{CompareArgs, EstablishBaselineArgs};
use crate::comparison_baseline::{BaselineDocument, read_baseline, read_evidence, write_baseline};
use crate::comparison_engine::compare;
use crate::comparison_report::ComparisonReport;
use crate::verdict::Verdict;

pub fn run_compare(args: CompareArgs) -> ExitCode {
    if let Some(path) = args.preflight_evidence {
        return run_preflight_compare(&path, &args.output);
    }
    let baseline = match read_baseline(&args.baseline) {
        Ok(baseline) => baseline,
        Err(error) => return fail(&error),
    };
    let Some(path) = args.evidence else {
        return fail("--evidence or --preflight-evidence is required");
    };
    let evidence = match read_evidence(&path) {
        Ok(evidence) => evidence,
        Err(error) => return fail(&error),
    };
    let report = compare(&baseline, &evidence);
    if let Err(error) = write_report(&args.output, &report) {
        return fail(&error);
    }
    match report.verdict {
        Verdict::Pass | Verdict::NotComparable => ExitCode::SUCCESS,
        Verdict::Fail | Verdict::Blocked => ExitCode::FAILURE,
    }
}

fn run_preflight_compare(path: &Path, output: &Path) -> ExitCode {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => {
            return fail(&format!(
                "read preflight evidence {}: {error}",
                path.display()
            ));
        }
    };
    let evidence = match serde_json::from_str(&text) {
        Ok(evidence) => evidence,
        Err(error) => {
            return fail(&format!(
                "parse preflight evidence {}: {error}",
                path.display()
            ));
        }
    };
    let report = crate::comparison_engine::compare_preflight(&evidence);
    if let Err(error) = write_report(output, &report) {
        return fail(&error);
    }
    ExitCode::FAILURE
}

pub fn run_establish_baseline(args: EstablishBaselineArgs) -> ExitCode {
    let evidence = match read_evidence(&args.evidence) {
        Ok(evidence) => evidence,
        Err(error) => return fail(&error),
    };
    let baseline = BaselineDocument::from_evidence(&evidence);
    match write_baseline(&args.output, &baseline) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => fail(&error),
    }
}

fn write_report(path: &Path, report: &ComparisonReport) -> Result<(), String> {
    crate::evidence_redaction::write_redacted_json(path, report)
}

fn fail(error: &str) -> ExitCode {
    eprintln!("comparison: {error}");
    ExitCode::FAILURE
}
