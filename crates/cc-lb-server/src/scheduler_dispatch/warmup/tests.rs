use super::*;
use cc_lb_domain::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};

use super::scheduling::POST_RESET_GUARD_SECS;

const NOW_UNIX_SECS: i64 = 1_782_000_000;
const NOW_UNIX_MILLIS: u64 = 1_782_000_000_000;
const FIVE_HOURS: i64 = 5 * 60 * 60;
const SEVEN_DAYS: i64 = 7 * 24 * 60 * 60;

#[test]
fn next_warmup_task_schedules_after_reset_guard_and_jitter() {
    let upstream_id = uuid::Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);
    let resets_at_unix_secs = 1_782_414_000;

    let task = next_warmup_task(upstream_id, resets_at_unix_secs);

    let run_at = task.run_at_unix_secs.expect("test task has run_at");
    assert!(
        (resets_at_unix_secs + POST_RESET_GUARD_SECS..=resets_at_unix_secs + 59).contains(&run_at),
        "run_at={run_at}"
    );
    assert_eq!(
        task.idempotency_key.as_deref(),
        Some("adaptive:warmup:12345678-90ab-cdef-1234-567890abcdef:1782414000")
    );
}

#[test]
fn quota_preflight_skips_fresh_seven_day_exhausted_snapshot() {
    let snapshots = [quota_snapshot(
        SubscriptionQuotaWindow::SevenDay,
        SubscriptionQuotaDataState::Fresh,
        Some(1.0),
        Some(SubscriptionQuotaStatus::Rejected),
        Some(1_782_414_000),
    )];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: 1_782_414_000
        }
    );
}

#[test]
fn quota_preflight_skips_fresh_fable_weekly_exhausted_without_shared_seven_day() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(1_782_018_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDayFable,
            SubscriptionQuotaDataState::Fresh,
            Some(1.0),
            Some(SubscriptionQuotaStatus::Rejected),
            Some(1_782_414_000),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: 1_782_414_000
        }
    );
}

#[test]
fn quota_preflight_preserves_shared_seven_day_precedence() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDayFable,
            SubscriptionQuotaDataState::Fresh,
            Some(1.0),
            Some(SubscriptionQuotaStatus::Rejected),
            Some(1_782_500_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(1.0),
            Some(SubscriptionQuotaStatus::Rejected),
            Some(1_782_414_000),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: 1_782_414_000
        }
    );
}

#[test]
fn quota_preflight_prefers_seven_day_over_five_hour_snapshot() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(1_782_018_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(1.0),
            Some(SubscriptionQuotaStatus::Rejected),
            Some(1_782_414_000),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: 1_782_414_000
        }
    );
}

#[test]
fn quota_preflight_reports_active_window_for_fresh_five_hour_snapshot() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(1_782_018_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Absent,
            None,
            None,
            None,
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: 1_782_018_000
        }
    );
}

#[test]
fn quota_preflight_falls_through_for_stale_or_missing_snapshots() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Stale,
            Some(1.0),
            Some(SubscriptionQuotaStatus::Rejected),
            Some(1_782_414_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Unobserved,
            None,
            None,
            Some(1_782_018_000),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::Unknown
    );
}

#[test]
fn quota_preflight_sends_warmup_for_fresh_five_hour_inactive_snapshot() {
    let snapshots = [quota_snapshot(
        SubscriptionQuotaWindow::FiveHour,
        SubscriptionQuotaDataState::Fresh,
        Some(0.0),
        Some(SubscriptionQuotaStatus::Allowed),
        Some(1_781_982_000),
    )];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SendWarmup
    );
}

#[test]
fn quota_preflight_ignores_five_hour_snapshot_older_than_three_minutes() {
    let snapshots = [quota_snapshot_with_observed_at(
        SubscriptionQuotaWindow::FiveHour,
        SubscriptionQuotaDataState::Fresh,
        Some(0.2),
        Some(SubscriptionQuotaStatus::Allowed),
        Some(1_782_018_000),
        NOW_UNIX_MILLIS - 181_000,
    )];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::Unknown
    );
}

#[test]
fn weekly_only_anthropic_account_warms_on_seven_day_window() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Absent,
            None,
            None,
            None,
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SendWarmup
    );
    assert_eq!(
        next_warmup_evaluation_after_success(&snapshots, &[], NOW_UNIX_SECS),
        NOW_UNIX_SECS + SEVEN_DAYS
    );
}

#[test]
fn both_active_targets_skip_until_earliest_reset() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + 2_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + 1_000),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: NOW_UNIX_SECS + 1_000
        }
    );
}

#[test]
fn active_five_hour_with_unknown_weekly_target_skips_until_five_hour_reset() {
    let active_five_hour = quota_snapshot(
        SubscriptionQuotaWindow::FiveHour,
        SubscriptionQuotaDataState::Fresh,
        Some(0.2),
        Some(SubscriptionQuotaStatus::Allowed),
        Some(NOW_UNIX_SECS + 2_000),
    );
    for weekly in [
        Some(quota_snapshot_with_observed_at(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Stale,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + SEVEN_DAYS),
            NOW_UNIX_MILLIS,
        )),
        None,
    ] {
        let mut snapshots = vec![active_five_hour.clone()];
        if let Some(weekly) = weekly {
            snapshots.push(weekly);
        }
        assert_eq!(
            quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
            WarmupQuotaPreflightDecision::ActiveWindow {
                cycle_key: NOW_UNIX_SECS + 2_000
            }
        );
    }
}

#[test]
fn exhausted_weekly_window_skips_until_reset() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Absent,
            None,
            None,
            None,
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(1.0),
            Some(SubscriptionQuotaStatus::Rejected),
            Some(NOW_UNIX_SECS + 5_000),
        ),
    ];

    assert_eq!(
        quota_preflight_decision_from_snapshots(&snapshots, NOW_UNIX_SECS),
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: NOW_UNIX_SECS + 5_000
        }
    );
}

#[test]
fn success_uses_earliest_future_reset_from_response_or_snapshot() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + 2_000),
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + 3_000),
        ),
    ];
    let observations = [UnifiedQuotaObservation {
        window: SubscriptionQuotaWindow::SevenDay,
        resets_at_unix_secs: u64::try_from(NOW_UNIX_SECS + 1_000).ok(),
        ..UnifiedQuotaObservation::default()
    }];

    assert_eq!(
        next_warmup_evaluation_after_success(&snapshots, &observations, NOW_UNIX_SECS),
        NOW_UNIX_SECS + 1_000
    );
}

#[test]
fn success_with_stale_five_hour_snapshot_uses_short_cadence() {
    let snapshots = [
        quota_snapshot_with_observed_at(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaDataState::Stale,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + FIVE_HOURS),
            NOW_UNIX_MILLIS - 181_000,
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaDataState::Fresh,
            Some(0.2),
            Some(SubscriptionQuotaStatus::Allowed),
            Some(NOW_UNIX_SECS + SEVEN_DAYS),
        ),
    ];

    assert_eq!(
        next_warmup_evaluation_after_success(&snapshots, &[], NOW_UNIX_SECS),
        NOW_UNIX_SECS + FIVE_HOURS
    );
}

fn quota_snapshot(
    window: SubscriptionQuotaWindow,
    state: SubscriptionQuotaDataState,
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<i64>,
) -> SubscriptionQuotaCandidateSnapshot {
    quota_snapshot_with_observed_at(
        window,
        state,
        utilization,
        status,
        resets_at_unix_secs,
        NOW_UNIX_MILLIS,
    )
}

fn quota_snapshot_with_observed_at(
    window: SubscriptionQuotaWindow,
    state: SubscriptionQuotaDataState,
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<i64>,
    observed_at_unix_millis: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.as_str().to_owned(),
        state,
        source: Some("test".to_owned()),
        utilization,
        status: status.map(|value| value.as_str().to_owned()),
        resets_at_unix_secs: resets_at_unix_secs.and_then(|value| u64::try_from(value).ok()),
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: Some(observed_at_unix_millis),
        max_staleness_secs: 1_800,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}
