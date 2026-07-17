#[test]
fn compare_pass_on_improved() {
    let baseline = baseline();
    let report = crate::comparison_engine::compare(&baseline, &improved_document());

    assert_eq!(
        report.verdict,
        crate::verdict::Verdict::Pass,
        "{:?}",
        report.reasons
    );
    assert!(report.comparison.throughput.delta_ratio > 0.0);
}

#[test]
fn compare_fail_on_regression_beyond_ci() {
    let baseline = baseline();
    let mut evidence = high_confidence_document();
    evidence.waves[0].metrics.latency_ms.p95 = 200;
    evidence.waves[0].metrics.latency_ms.p99 = 200;
    let report = crate::comparison_engine::compare(&baseline, &evidence);

    assert_eq!(report.verdict, crate::verdict::Verdict::Fail);
    assert!(
        report
            .reasons
            .iter()
            .any(|reason| reason == "latency_ms_p95_regression_beyond_confidence_band")
    );
}

#[test]
fn not_comparable_low_samples() {
    let baseline = baseline();
    let mut evidence = improved_document();
    evidence.waves[0].sample_count = 4;
    let report = crate::comparison_engine::compare(&baseline, &evidence);

    assert_eq!(report.verdict, crate::verdict::Verdict::NotComparable);
    assert!(
        report
            .reasons
            .iter()
            .any(|reason| reason == "insufficient_samples")
    );
}

#[test]
fn not_comparable_env_drift() {
    let baseline = baseline();
    let mut evidence = improved_document();
    evidence.environment.kernel = "different-kernel".to_owned();
    let report = crate::comparison_engine::compare(&baseline, &evidence);

    assert_eq!(report.verdict, crate::verdict::Verdict::NotComparable);
    assert!(
        report
            .reasons
            .iter()
            .any(|reason| reason == "environment_drift")
    );
}

#[test]
fn blocked_on_missing_preflight() {
    let evidence = crate::preflight_evidence::PreflightEvidence::capability_blocked(
        "test-run",
        crate::preflight_evidence::CapabilityFailure::docker("docker unavailable"),
        crate::topology_decision::CleanupReceipt::empty(),
    );
    let report = crate::comparison_engine::compare_preflight(&evidence);

    assert_eq!(report.verdict, crate::verdict::Verdict::Blocked);
    assert_eq!(report.blocked_stage.as_deref(), Some("preflight"));
    assert_eq!(report.blocked_reason.as_deref(), Some("docker unavailable"));
}

#[test]
fn post_preflight_blocked_evidence_is_fail() {
    let baseline = baseline();
    let mut evidence = high_confidence_document();
    evidence.verdict = crate::verdict::Verdict::Blocked;
    let report = crate::comparison_engine::compare(&baseline, &evidence);

    assert_eq!(report.verdict, crate::verdict::Verdict::Fail);
    assert!(
        report
            .reasons
            .iter()
            .any(|reason| reason == "post_preflight_blocked_evidence")
    );
}

fn baseline() -> crate::comparison_baseline::BaselineDocument {
    crate::comparison_baseline::BaselineDocument::from_evidence(&high_confidence_document())
}

fn improved_document() -> crate::evidence_schema::EvidenceDocument {
    let mut evidence = high_confidence_document();
    let wave = &mut evidence.waves[0];
    wave.rps.achieved = 5.25;
    improve_metric(&mut wave.metrics.latency_ms);
    improve_metric(&mut wave.metrics.ttfb_ms);
    improve_metric(&mut wave.metrics.ttft_ms);
    improve_metric(&mut wave.metrics.schedule_drift_ms);
    evidence
}

fn high_confidence_document() -> crate::evidence_schema::EvidenceDocument {
    let mut evidence =
        crate::evidence_self_check::synthetic_document().expect("synthetic evidence builds");
    let wave = &mut evidence.waves[0];
    wave.sample_count = 40;
    set_samples(&mut wave.metrics.latency_ms);
    set_samples(&mut wave.metrics.ttfb_ms);
    set_samples(&mut wave.metrics.ttft_ms);
    set_samples(&mut wave.metrics.schedule_drift_ms);
    evidence
}

fn set_samples(metric: &mut crate::evidence_metrics::MetricSummary) {
    metric.samples = 40;
    metric.dispersion.iqr = 1;
    metric.dispersion.mad = 1;
}

fn improve_metric(metric: &mut crate::evidence_metrics::MetricSummary) {
    metric.p95 /= 2;
    metric.p99 /= 2;
}
