use crate::comparison_baseline::{
    BaselineDocument, BaselineWave, ComparisonPolicy, EnvironmentBounds, evidence_schema_matches,
};
use crate::comparison_metrics::{compare_metrics, compare_qdisc};
use crate::comparison_report::{
    ComparisonReport, ComparisonSummary, Delta, WaveComparison, delta, empty_comparison,
};
use crate::evidence_environment::EnvironmentManifest;
use crate::evidence_schema::{EvidenceDocument, WaveEvidence};
use crate::preflight_evidence::PreflightEvidence;
use crate::verdict::Verdict;

pub fn compare(baseline: &BaselineDocument, evidence: &EvidenceDocument) -> ComparisonReport {
    let mut reasons = status_reasons(evidence);
    if !evidence_schema_matches(evidence, baseline) {
        reasons.push("schema_mismatch".to_owned());
    }
    reasons.extend(environment_reasons(
        &baseline.environment,
        &evidence.environment,
        &baseline.policy,
    ));

    let mut comparisons = Vec::new();
    for baseline_wave in &baseline.waves {
        let Some(evidence_wave) = evidence
            .waves
            .iter()
            .find(|wave| wave.name == baseline_wave.name)
        else {
            reasons.push("wave_set_mismatch".to_owned());
            continue;
        };
        comparisons.push(compare_wave(
            baseline_wave,
            evidence_wave,
            &baseline.policy,
            &mut reasons,
        ));
    }
    if baseline.waves.len() != evidence.waves.len() {
        reasons.push("wave_set_mismatch".to_owned());
    }

    ComparisonReport {
        verdict: verdict_for(&reasons),
        reasons: unique(reasons),
        blocked_stage: None,
        blocked_reason: None,
        comparison: ComparisonSummary {
            throughput: total_throughput(&baseline.waves, &evidence.waves),
            waves: comparisons,
        },
    }
}

pub fn compare_preflight(evidence: &PreflightEvidence) -> ComparisonReport {
    let capability_missing = !evidence.checks.docker
        || !evidence.checks.cap_net_admin
        || !evidence.checks.tc
        || !evidence.checks.netem_seed;
    let reason = evidence
        .blocked_reason
        .as_deref()
        .filter(|reason| !reason.trim().is_empty());
    if evidence.verdict == Verdict::Blocked
        && evidence.blocked_stage == Some(crate::topology_decision::BlockedStage::Preflight)
        && capability_missing
        && let Some(reason) = reason
    {
        return ComparisonReport {
            verdict: Verdict::Blocked,
            reasons: vec!["preflight_capability_missing".to_owned()],
            blocked_stage: Some("preflight".to_owned()),
            blocked_reason: Some(reason.to_owned()),
            comparison: empty_comparison(),
        };
    }
    ComparisonReport {
        verdict: Verdict::Fail,
        reasons: vec!["invalid_preflight_blocked_evidence".to_owned()],
        blocked_stage: None,
        blocked_reason: None,
        comparison: empty_comparison(),
    }
}

fn compare_wave(
    baseline: &BaselineWave,
    evidence: &WaveEvidence,
    policy: &ComparisonPolicy,
    reasons: &mut Vec<String>,
) -> WaveComparison {
    if baseline.sample_count < policy.min_samples || evidence.sample_count < policy.min_samples {
        reasons.push("insufficient_samples".to_owned());
    }
    let throughput = delta(baseline.achieved_rps, evidence.rps.achieved);
    if throughput.delta_ratio.abs() > policy.rps_budget_ratio {
        reasons.push("achieved_rps_outside_budget".to_owned());
    }
    WaveComparison {
        name: baseline.name.clone(),
        throughput,
        latency_ms: compare_metrics(
            "latency_ms",
            &baseline.metrics.latency_ms,
            &evidence.metrics.latency_ms,
            policy,
            reasons,
        ),
        ttfb_ms: compare_metrics(
            "ttfb_ms",
            &baseline.metrics.ttfb_ms,
            &evidence.metrics.ttfb_ms,
            policy,
            reasons,
        ),
        ttft_ms: compare_metrics(
            "ttft_ms",
            &baseline.metrics.ttft_ms,
            &evidence.metrics.ttft_ms,
            policy,
            reasons,
        ),
        schedule_drift_ms: compare_metrics(
            "schedule_drift_ms",
            &baseline.metrics.schedule_drift_ms,
            &evidence.metrics.schedule_drift_ms,
            policy,
            reasons,
        ),
        qdisc: compare_qdisc(&baseline.qdisc, evidence, policy, reasons),
    }
}

fn environment_reasons(
    baseline: &EnvironmentBounds,
    evidence: &EnvironmentManifest,
    policy: &ComparisonPolicy,
) -> Vec<String> {
    let same = baseline.kernel == evidence.kernel
        && baseline.tc_version == evidence.tc_version
        && baseline.docker_version == evidence.docker_version
        && baseline.image_digests == evidence.image_digests
        && baseline.binary_hashes == evidence.binary_hashes
        && baseline.git_sha == evidence.git_sha
        && baseline.git_dirty == evidence.git_dirty
        && baseline.cpu_count == evidence.cpu.count
        && baseline.cpu_model == evidence.cpu.model
        && baseline.cgroup == evidence.limits.cgroup
        && baseline.memory_limit == evidence.limits.memory_limit
        && baseline.fd_soft_limit == evidence.limits.fd_soft_limit;
    let current_load = evidence
        .host_load_samples
        .iter()
        .map(|sample| sample.one_minute)
        .fold(0.0, f64::max);
    let load_limit = baseline.max_one_minute_load.max(1.0) * policy.max_host_load_ratio;
    if same && current_load <= load_limit {
        Vec::new()
    } else {
        vec!["environment_drift".to_owned()]
    }
}

fn status_reasons(evidence: &EvidenceDocument) -> Vec<String> {
    match evidence.verdict {
        Verdict::Pass => Vec::new(),
        Verdict::Fail => vec!["evidence_failed".to_owned()],
        Verdict::Blocked => vec!["post_preflight_blocked_evidence".to_owned()],
        Verdict::NotComparable => vec!["evidence_not_comparable".to_owned()],
    }
}

fn total_throughput(baseline: &[BaselineWave], evidence: &[WaveEvidence]) -> Delta {
    let baseline_total = baseline.iter().map(|wave| wave.achieved_rps).sum();
    let evidence_total = evidence.iter().map(|wave| wave.rps.achieved).sum();
    delta(baseline_total, evidence_total)
}

fn verdict_for(reasons: &[String]) -> Verdict {
    if reasons.iter().any(|reason| {
        reason.contains("regression")
            || reason.contains("failed")
            || reason.contains("post_preflight")
    }) {
        Verdict::Fail
    } else if reasons.is_empty() {
        Verdict::Pass
    } else {
        Verdict::NotComparable
    }
}

fn unique(mut reasons: Vec<String>) -> Vec<String> {
    reasons.sort();
    reasons.dedup();
    reasons
}
