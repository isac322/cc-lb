use crate::comparison_baseline::{
    ComparisonPolicy, ConfidenceBand, MetricBaseline, QdiscBaseline, ratio,
};
use crate::comparison_report::{BandComparison, PercentileComparison, QdiscComparison, ratio_f64};
use crate::evidence_metrics::MetricSummary;
use crate::evidence_schema::WaveEvidence;

pub fn compare_metrics(
    name: &str,
    baseline: &MetricBaseline,
    evidence: &MetricSummary,
    policy: &ComparisonPolicy,
    reasons: &mut Vec<String>,
) -> PercentileComparison {
    PercentileComparison {
        p95: compare_percentile(
            name,
            "p95",
            &baseline.p95,
            evidence.p95,
            evidence,
            policy,
            reasons,
        ),
        p99: compare_percentile(
            name,
            "p99",
            &baseline.p99,
            evidence.p99,
            evidence,
            policy,
            reasons,
        ),
    }
}

pub fn compare_qdisc(
    baseline: &[QdiscBaseline],
    evidence: &WaveEvidence,
    policy: &ComparisonPolicy,
    reasons: &mut Vec<String>,
) -> Vec<QdiscComparison> {
    baseline
        .iter()
        .filter_map(|expected| {
            let actual = evidence
                .netem
                .qdisc
                .iter()
                .find(|stat| stat.edge == expected.edge);
            let Some(actual) = actual else {
                reasons.push("qdisc_divergence".to_owned());
                return None;
            };
            let actual_drop_ratio = ratio(actual.dropped, actual.packets);
            let drop_ratio_delta = actual_drop_ratio - expected.drop_ratio;
            if actual.device != expected.device
                || actual.packets == 0
                || drop_ratio_delta.abs() > policy.qdisc_drop_ratio_delta
                || actual.schedule_drift_ms > policy.max_qdisc_schedule_drift_ms
                || actual.overlimits != expected.overlimits
                || actual.requeues != expected.requeues
            {
                reasons.push("qdisc_divergence".to_owned());
            }
            Some(QdiscComparison {
                edge: expected.edge.clone(),
                baseline_drop_ratio: expected.drop_ratio,
                evidence_drop_ratio: actual_drop_ratio,
                drop_ratio_delta,
            })
        })
        .collect()
}

fn compare_percentile(
    metric: &str,
    percentile: &str,
    baseline: &ConfidenceBand,
    value: u64,
    evidence: &MetricSummary,
    policy: &ComparisonPolicy,
    reasons: &mut Vec<String>,
) -> BandComparison {
    let evidence_band = robust_band(
        value,
        evidence.dispersion.iqr,
        evidence.dispersion.mad,
        evidence.samples,
    );
    let label = format!("{metric}_{percentile}");
    if band_too_wide(baseline, policy) || band_too_wide(&evidence_band, policy) {
        reasons.push(format!("{label}_confidence_band_too_wide"));
    } else if evidence_band.lower > baseline.upper {
        reasons.push(format!("{label}_regression_beyond_confidence_band"));
    } else if bands_overlap(baseline, &evidence_band) {
        reasons.push(format!("{label}_confidence_intervals_overlap"));
    }
    BandComparison {
        baseline: ConfidenceBand {
            lower: baseline.lower,
            upper: baseline.upper,
        },
        evidence: evidence_band,
        delta: value as f64 - midpoint(baseline),
        delta_ratio: ratio_f64(value as f64, midpoint(baseline)),
    }
}

fn robust_band(value: u64, iqr: u64, mad: u64, samples: u64) -> ConfidenceBand {
    let scale = iqr.max(mad).max(1) as f64;
    let half_width = 1.96 * scale / (samples.max(1) as f64).sqrt();
    ConfidenceBand {
        lower: (value as f64 - half_width).max(0.0),
        upper: value as f64 + half_width,
    }
}

fn band_too_wide(band: &ConfidenceBand, policy: &ComparisonPolicy) -> bool {
    band.upper - band.lower > (midpoint(band) * policy.max_confidence_band_ratio).max(5.0)
}

fn bands_overlap(left: &ConfidenceBand, right: &ConfidenceBand) -> bool {
    left.lower <= right.upper && right.lower <= left.upper
}

fn midpoint(band: &ConfidenceBand) -> f64 {
    (band.lower + band.upper) / 2.0
}
