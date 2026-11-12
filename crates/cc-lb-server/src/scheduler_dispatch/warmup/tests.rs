use super::*;
use cc_lb_domain::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::upstream::UpstreamKind;
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
    let task = next_warmup_task(upstream_id, resets_at_unix_secs, WarmupLane::Primary);
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
fn idle_target_requests_warmup_even_when_the_other_target_is_active() {
    for (kind, short, weekly) in provider_target_windows() {
        for (idle, active_window) in [(short, weekly), (weekly, short)] {
            assert_decision(
                kind,
                &[inactive(idle), active(active_window, NOW_UNIX_SECS + 1_000)],
                WarmupQuotaPreflightDecision::SendWarmup,
            );
        }
    }
}

#[test]
fn both_active_targets_schedule_earliest_target_reset() {
    for (kind, short, weekly) in provider_target_windows() {
        for (short_reset, weekly_reset) in [
            (NOW_UNIX_SECS + 1_000, NOW_UNIX_SECS + 2_000),
            (NOW_UNIX_SECS + 2_000, NOW_UNIX_SECS + 1_000),
        ] {
            assert_decision(
                kind,
                &[active(short, short_reset), active(weekly, weekly_reset)],
                WarmupQuotaPreflightDecision::ActiveWindow {
                    cycle_key: NOW_UNIX_SECS + 1_000,
                },
            );
        }
    }
}

#[test]
fn absent_five_hour_targets_only_the_weekly_window_regardless_of_sample_age() {
    for (kind, short, weekly) in provider_target_windows() {
        for observed_at in [
            Some(NOW_UNIX_MILLIS),
            Some(NOW_UNIX_MILLIS - 1_800_000),
            None,
        ] {
            let mut absent_short = absent(short);
            absent_short.observed_at_unix_millis = observed_at;
            let mut snapshots = [absent_short, inactive(weekly)];
            assert_decision(kind, &snapshots, WarmupQuotaPreflightDecision::SendWarmup);
            snapshots[1] = active(weekly, NOW_UNIX_SECS + SEVEN_DAYS);
            assert_decision(
                kind,
                &snapshots,
                WarmupQuotaPreflightDecision::ActiveWindow {
                    cycle_key: NOW_UNIX_SECS + SEVEN_DAYS,
                },
            );
        }
    }
}

#[test]
fn openai_team_warms_each_present_window() {
    let mut snapshots = [
        active(SubscriptionQuotaWindow::FiveHour, NOW_UNIX_SECS + 1_000),
        inactive(SubscriptionQuotaWindow::SevenDay),
    ];
    assert_decision(
        UpstreamKind::OpenaiOauth,
        &snapshots,
        WarmupQuotaPreflightDecision::SendWarmup,
    );
    snapshots[1] = active(SubscriptionQuotaWindow::SevenDay, NOW_UNIX_SECS + 2_000);
    assert_decision(
        UpstreamKind::OpenaiOauth,
        &snapshots,
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: NOW_UNIX_SECS + 1_000,
        },
    );
}

#[test]
fn rolling_unused_windows_are_inactive_at_the_sixty_second_tolerance() {
    for (kind, window, other_window, duration) in [
        (
            UpstreamKind::OpenaiOauth,
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            FIVE_HOURS,
        ),
        (
            UpstreamKind::OpenaiOauth,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::FiveHour,
            SEVEN_DAYS,
        ),
        (
            UpstreamKind::GeminiOauth,
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::GeminiSevenDay,
            FIVE_HOURS,
        ),
        (
            UpstreamKind::GeminiOauth,
            SubscriptionQuotaWindow::GeminiSevenDay,
            SubscriptionQuotaWindow::GeminiFiveHour,
            SEVEN_DAYS,
        ),
    ] {
        for adjustment in [0, -60] {
            let snapshots = [
                quota_snapshot(
                    window,
                    SubscriptionQuotaDataState::Fresh,
                    Some(0.0),
                    Some(NOW_UNIX_SECS + duration + adjustment),
                ),
                active(other_window, NOW_UNIX_SECS + 1_000),
            ];
            assert_decision(kind, &snapshots, WarmupQuotaPreflightDecision::SendWarmup);
        }
        let snapshots = [
            quota_snapshot(
                window,
                SubscriptionQuotaDataState::Fresh,
                Some(0.0),
                Some(NOW_UNIX_SECS + duration - 61),
            ),
            active(other_window, NOW_UNIX_SECS + 1_000),
        ];
        assert_decision(
            kind,
            &snapshots,
            WarmupQuotaPreflightDecision::ActiveWindow {
                cycle_key: NOW_UNIX_SECS + 1_000,
            },
        );
    }
}

#[test]
fn rolling_unused_detection_uses_observed_time_not_current_time() {
    let mut rolling = quota_snapshot(
        SubscriptionQuotaWindow::GeminiFiveHour,
        SubscriptionQuotaDataState::Fresh,
        Some(0.0),
        Some(NOW_UNIX_SECS - 120 + FIVE_HOURS),
    );
    rolling.observed_at_unix_millis = Some(NOW_UNIX_MILLIS - 120_000);
    assert_decision(
        UpstreamKind::GeminiOauth,
        &[
            rolling,
            active(
                SubscriptionQuotaWindow::GeminiSevenDay,
                NOW_UNIX_SECS + 5_000,
            ),
        ],
        WarmupQuotaPreflightDecision::SendWarmup,
    );
}

#[test]
fn gemini_ignores_exhausted_and_unstarted_third_party_windows() {
    let mut snapshots = [
        active(
            SubscriptionQuotaWindow::GeminiFiveHour,
            NOW_UNIX_SECS + 1_000,
        ),
        active(
            SubscriptionQuotaWindow::GeminiSevenDay,
            NOW_UNIX_SECS + 2_000,
        ),
        inactive(SubscriptionQuotaWindow::ThirdPartyFiveHour),
        exhausted(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 5_000,
        ),
    ];
    assert_decision(
        UpstreamKind::GeminiOauth,
        &snapshots,
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: NOW_UNIX_SECS + 1_000,
        },
    );
    snapshots[0] = inactive(SubscriptionQuotaWindow::GeminiFiveHour);
    assert_decision(
        UpstreamKind::GeminiOauth,
        &snapshots,
        WarmupQuotaPreflightDecision::SendWarmup,
    );
}

#[test]
fn weekly_exhaustion_precedes_inactive_or_absent_targets() {
    for (kind, weekly, five_hour) in [
        (
            UpstreamKind::AnthropicOauth,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::FiveHour,
        ),
        (
            UpstreamKind::AnthropicOauth,
            SubscriptionQuotaWindow::SevenDayFable,
            SubscriptionQuotaWindow::FiveHour,
        ),
        (
            UpstreamKind::OpenaiOauth,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::FiveHour,
        ),
        (
            UpstreamKind::GeminiOauth,
            SubscriptionQuotaWindow::GeminiSevenDay,
            SubscriptionQuotaWindow::GeminiFiveHour,
        ),
    ] {
        for short_window in [inactive(five_hour), absent(five_hour)] {
            assert_decision(
                kind,
                &[short_window, exhausted(weekly, NOW_UNIX_SECS + 5_000)],
                WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
                    cycle_key: NOW_UNIX_SECS + 5_000,
                },
            );
        }
    }
}

#[test]
fn weekly_exhaustion_preserves_base_seven_day_precedence() {
    assert_decision(
        UpstreamKind::AnthropicOauth,
        &[
            exhausted(
                SubscriptionQuotaWindow::SevenDayFable,
                NOW_UNIX_SECS + 6_000,
            ),
            exhausted(SubscriptionQuotaWindow::SevenDay, NOW_UNIX_SECS + 5_000),
        ],
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: NOW_UNIX_SECS + 5_000,
        },
    );
}

#[test]
fn stale_missing_or_old_targets_are_unknown_without_a_known_active_target() {
    for (kind, short, _) in provider_target_windows() {
        assert_decision(kind, &[], WarmupQuotaPreflightDecision::Unknown);
        for state in [
            SubscriptionQuotaDataState::Stale,
            SubscriptionQuotaDataState::Unobserved,
        ] {
            assert_decision(
                kind,
                &[quota_snapshot(
                    short,
                    state,
                    Some(0.2),
                    Some(NOW_UNIX_SECS + 1_000),
                )],
                WarmupQuotaPreflightDecision::Unknown,
            );
        }
        let mut old = active(short, NOW_UNIX_SECS + 1_000);
        old.observed_at_unix_millis = Some(NOW_UNIX_MILLIS - 181_000);
        assert_decision(kind, &[old], WarmupQuotaPreflightDecision::Unknown);
    }
}

#[test]
fn expired_reset_is_inactive_and_stale_weekly_exhaustion_does_not_block() {
    let mut weekly = exhausted(SubscriptionQuotaWindow::SevenDay, NOW_UNIX_SECS + 1_000);
    weekly.state = SubscriptionQuotaDataState::Stale;
    assert_decision(
        UpstreamKind::AnthropicOauth,
        &[
            active(SubscriptionQuotaWindow::FiveHour, NOW_UNIX_SECS),
            weekly,
        ],
        WarmupQuotaPreflightDecision::SendWarmup,
    );
}

#[test]
fn success_without_known_reset_is_bounded_by_shortest_present_duration() {
    for (kind, short, weekly) in provider_target_windows() {
        assert_eq!(
            next_warmup_evaluation_after_success(
                kind,
                WarmupLane::Primary,
                &[inactive(short), inactive(weekly)],
                &[],
                NOW_UNIX_SECS
            ),
            NOW_UNIX_SECS + FIVE_HOURS,
        );
        assert_eq!(
            next_warmup_evaluation_after_success(
                kind,
                WarmupLane::Primary,
                &[absent(short), inactive(weekly)],
                &[],
                NOW_UNIX_SECS
            ),
            NOW_UNIX_SECS + SEVEN_DAYS,
        );
        assert_eq!(
            next_warmup_evaluation_after_success(
                kind,
                WarmupLane::Primary,
                &[],
                &[],
                NOW_UNIX_SECS
            ),
            NOW_UNIX_SECS + FIVE_HOURS,
        );
    }
}

#[test]
fn success_ignores_expired_stale_and_third_party_resets() {
    let mut stale = active(SubscriptionQuotaWindow::GeminiSevenDay, NOW_UNIX_SECS + 500);
    stale.state = SubscriptionQuotaDataState::Stale;
    let observations = [
        observation(SubscriptionQuotaWindow::GeminiFiveHour, NOW_UNIX_SECS),
        observation(
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            NOW_UNIX_SECS + 100,
        ),
        observation(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 200,
        ),
    ];
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::GeminiOauth,
            WarmupLane::Primary,
            &[stale],
            &observations,
            NOW_UNIX_SECS
        ),
        NOW_UNIX_SECS + FIVE_HOURS,
    );
}

#[test]
fn response_can_restore_a_previously_absent_target_after_success() {
    let observations = [observation(
        SubscriptionQuotaWindow::FiveHour,
        NOW_UNIX_SECS + 1_000,
    )];
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::OpenaiOauth,
            WarmupLane::Primary,
            &[absent(SubscriptionQuotaWindow::FiveHour)],
            &observations,
            NOW_UNIX_SECS
        ),
        NOW_UNIX_SECS + 1_000,
    );
}

#[test]
fn preflight_allows_all_oauth_kinds_to_reach_credentials_gate_and_rejects_api_keys() {
    for kind in [
        UpstreamKind::AnthropicOauth,
        UpstreamKind::OpenaiOauth,
        UpstreamKind::GeminiOauth,
    ] {
        let upstream = UpstreamRecord {
            kind,
            warmup_enabled: true,
            ..UpstreamRecord::default()
        };
        assert_eq!(
            warmup_preflight_reject(&upstream),
            Some(WarmupPreflightReject::PermanentFailure(
                WarmupPermanentFailureReason::OauthCredentialsMissing
            )),
        );
    }
    for kind in [
        UpstreamKind::AnthropicApiKey,
        UpstreamKind::OpenaiApiKey,
        UpstreamKind::GeminiApiKey,
    ] {
        let upstream = UpstreamRecord {
            kind,
            warmup_enabled: true,
            ..UpstreamRecord::default()
        };
        assert_eq!(
            warmup_preflight_reject(&upstream),
            Some(WarmupPreflightReject::Skip(
                WarmupSkipReason::UpstreamDisabled
            ))
        );
    }
}

#[test]
fn preflight_deleted_and_warmup_disabled_gates_take_precedence() {
    for kind in [
        UpstreamKind::AnthropicOauth,
        UpstreamKind::OpenaiOauth,
        UpstreamKind::GeminiOauth,
    ] {
        let mut upstream = UpstreamRecord {
            kind,
            warmup_enabled: true,
            deleted_at_unix_secs: Some(NOW_UNIX_SECS as u64),
            ..UpstreamRecord::default()
        };
        assert_eq!(
            warmup_preflight_reject(&upstream),
            Some(WarmupPreflightReject::Skip(
                WarmupSkipReason::UpstreamDeleted
            )),
        );
        upstream.deleted_at_unix_secs = None;
        upstream.warmup_enabled = false;
        assert_eq!(
            warmup_preflight_reject(&upstream),
            Some(WarmupPreflightReject::Skip(
                WarmupSkipReason::UpstreamDisabled
            )),
        );
    }
}

#[test]
fn next_warmup_task_keeps_the_lane_in_job_and_key() {
    let upstream_id = uuid::Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);
    let resets_at_unix_secs = 1_782_414_000;
    let task = next_warmup_task(upstream_id, resets_at_unix_secs, WarmupLane::ThirdParty);
    assert_eq!(
        task.idempotency_key.as_deref(),
        Some("adaptive:warmup_third_party:12345678-90ab-cdef-1234-567890abcdef:1782414000")
    );
    let cc_lb_scheduler::worker::AdaptiveJob::Warmup(job) = task.args else {
        panic!("expected warmup job");
    };
    assert_eq!(job.lane, WarmupLane::ThirdParty);
    assert_eq!(job.cycle_key, resets_at_unix_secs);
}

#[test]
fn third_party_lane_warms_its_idle_group_while_gemini_group_is_active() {
    let snapshots = [
        active(
            SubscriptionQuotaWindow::GeminiFiveHour,
            NOW_UNIX_SECS + 1_000,
        ),
        active(
            SubscriptionQuotaWindow::GeminiSevenDay,
            NOW_UNIX_SECS + 2_000,
        ),
        inactive(SubscriptionQuotaWindow::ThirdPartyFiveHour),
        active(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 3_000,
        ),
    ];
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::ThirdParty,
        &snapshots,
        WarmupQuotaPreflightDecision::SendWarmup,
    );
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::Primary,
        &snapshots,
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: NOW_UNIX_SECS + 1_000,
        },
    );
}

#[test]
fn third_party_lane_schedules_its_own_earliest_reset() {
    let snapshots = [
        inactive(SubscriptionQuotaWindow::GeminiFiveHour),
        active(
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            NOW_UNIX_SECS + 4_000,
        ),
        active(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 3_000,
        ),
    ];
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::ThirdParty,
        &snapshots,
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: NOW_UNIX_SECS + 3_000,
        },
    );
}

#[test]
fn third_party_lane_treats_rolling_unused_windows_as_idle() {
    let snapshots = [
        quota_snapshot(
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            SubscriptionQuotaDataState::Fresh,
            Some(0.0),
            Some(NOW_UNIX_SECS + FIVE_HOURS),
        ),
        active(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 1_000,
        ),
    ];
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::ThirdParty,
        &snapshots,
        WarmupQuotaPreflightDecision::SendWarmup,
    );
}

#[test]
fn weekly_exhaustion_blocks_only_its_own_lane() {
    let gemini_exhausted = [
        exhausted(
            SubscriptionQuotaWindow::GeminiSevenDay,
            NOW_UNIX_SECS + 5_000,
        ),
        inactive(SubscriptionQuotaWindow::ThirdPartyFiveHour),
        inactive(SubscriptionQuotaWindow::ThirdPartySevenDay),
    ];
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::Primary,
        &gemini_exhausted,
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: NOW_UNIX_SECS + 5_000,
        },
    );
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::ThirdParty,
        &gemini_exhausted,
        WarmupQuotaPreflightDecision::SendWarmup,
    );

    let third_party_exhausted = [
        exhausted(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 6_000,
        ),
        inactive(SubscriptionQuotaWindow::GeminiFiveHour),
        inactive(SubscriptionQuotaWindow::GeminiSevenDay),
    ];
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::ThirdParty,
        &third_party_exhausted,
        WarmupQuotaPreflightDecision::SevenDayQuotaExhausted {
            cycle_key: NOW_UNIX_SECS + 6_000,
        },
    );
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::Primary,
        &third_party_exhausted,
        WarmupQuotaPreflightDecision::SendWarmup,
    );
}

#[test]
fn every_absent_lane_target_is_targets_absent_for_that_lane_only() {
    let snapshots = [
        absent(SubscriptionQuotaWindow::ThirdPartyFiveHour),
        absent(SubscriptionQuotaWindow::ThirdPartySevenDay),
        inactive(SubscriptionQuotaWindow::GeminiFiveHour),
    ];
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::ThirdParty,
        &snapshots,
        WarmupQuotaPreflightDecision::TargetsAbsent,
    );
    assert_lane_decision(
        UpstreamKind::GeminiOauth,
        WarmupLane::Primary,
        &snapshots,
        WarmupQuotaPreflightDecision::SendWarmup,
    );
    assert_decision(
        UpstreamKind::GeminiOauth,
        &[
            absent(SubscriptionQuotaWindow::GeminiFiveHour),
            absent(SubscriptionQuotaWindow::GeminiSevenDay),
        ],
        WarmupQuotaPreflightDecision::TargetsAbsent,
    );
}

#[test]
fn anthropic_and_openai_with_every_target_absent_still_send() {
    for (kind, short, weekly) in [
        (
            UpstreamKind::AnthropicOauth,
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ),
        (
            UpstreamKind::OpenaiOauth,
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ),
    ] {
        assert_decision(
            kind,
            &[absent(short), absent(weekly)],
            WarmupQuotaPreflightDecision::Unknown,
        );
    }
}

#[test]
fn partially_absent_lane_targets_stay_unknown_and_send() {
    for other in [
        Some(quota_snapshot(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            SubscriptionQuotaDataState::Unobserved,
            None,
            None,
        )),
        Some(quota_snapshot(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            SubscriptionQuotaDataState::Stale,
            Some(0.2),
            Some(NOW_UNIX_SECS + 1_000),
        )),
        None,
    ] {
        let mut snapshots = vec![absent(SubscriptionQuotaWindow::ThirdPartyFiveHour)];
        snapshots.extend(other);
        assert_lane_decision(
            UpstreamKind::GeminiOauth,
            WarmupLane::ThirdParty,
            &snapshots,
            WarmupQuotaPreflightDecision::Unknown,
        );
    }
}

#[test]
fn success_evaluation_uses_only_the_lane_target_windows() {
    let snapshots = [
        active(
            SubscriptionQuotaWindow::GeminiFiveHour,
            NOW_UNIX_SECS + 1_000,
        ),
        active(
            SubscriptionQuotaWindow::GeminiSevenDay,
            NOW_UNIX_SECS + 2_000,
        ),
        active(
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            NOW_UNIX_SECS + 3_000,
        ),
        active(
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            NOW_UNIX_SECS + 4_000,
        ),
    ];
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::GeminiOauth,
            WarmupLane::ThirdParty,
            &snapshots,
            &[],
            NOW_UNIX_SECS,
        ),
        NOW_UNIX_SECS + 3_000
    );
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::GeminiOauth,
            WarmupLane::Primary,
            &snapshots,
            &[],
            NOW_UNIX_SECS,
        ),
        NOW_UNIX_SECS + 1_000
    );
    // A Gemini-group observation in the response does not move the
    // third-party cadence.
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::GeminiOauth,
            WarmupLane::ThirdParty,
            &snapshots,
            &[observation(
                SubscriptionQuotaWindow::GeminiFiveHour,
                NOW_UNIX_SECS + 500,
            )],
            NOW_UNIX_SECS,
        ),
        NOW_UNIX_SECS + 3_000
    );
}

#[test]
fn third_party_success_without_known_reset_falls_back_to_five_hours() {
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::GeminiOauth,
            WarmupLane::ThirdParty,
            &[
                active(
                    SubscriptionQuotaWindow::GeminiFiveHour,
                    NOW_UNIX_SECS + 1_000
                ),
                absent(SubscriptionQuotaWindow::ThirdPartySevenDay),
            ],
            &[],
            NOW_UNIX_SECS,
        ),
        NOW_UNIX_SECS + FIVE_HOURS
    );
}

fn assert_decision(
    kind: UpstreamKind,
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    expected: WarmupQuotaPreflightDecision,
) {
    assert_lane_decision(kind, WarmupLane::Primary, snapshots, expected);
}

fn assert_lane_decision(
    kind: UpstreamKind,
    lane: WarmupLane,
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    expected: WarmupQuotaPreflightDecision,
) {
    assert_eq!(
        quota_preflight_decision_from_snapshots(kind, lane, snapshots, NOW_UNIX_SECS),
        expected,
        "kind={kind:?} lane={lane:?}"
    );
}

fn active(window: SubscriptionQuotaWindow, reset: i64) -> SubscriptionQuotaCandidateSnapshot {
    quota_snapshot(
        window,
        SubscriptionQuotaDataState::Fresh,
        Some(0.2),
        Some(reset),
    )
}

fn inactive(window: SubscriptionQuotaWindow) -> SubscriptionQuotaCandidateSnapshot {
    quota_snapshot(window, SubscriptionQuotaDataState::Fresh, Some(0.0), None)
}

fn absent(window: SubscriptionQuotaWindow) -> SubscriptionQuotaCandidateSnapshot {
    quota_snapshot(window, SubscriptionQuotaDataState::Absent, None, None)
}

fn exhausted(window: SubscriptionQuotaWindow, reset: i64) -> SubscriptionQuotaCandidateSnapshot {
    let mut snapshot = quota_snapshot(
        window,
        SubscriptionQuotaDataState::Fresh,
        Some(1.0),
        Some(reset),
    );
    snapshot.status = Some(SubscriptionQuotaStatus::Rejected.as_str().to_owned());
    snapshot
}

fn observation(window: SubscriptionQuotaWindow, reset: i64) -> UnifiedQuotaObservation {
    UnifiedQuotaObservation {
        window,
        resets_at_unix_secs: u64::try_from(reset).ok(),
        ..UnifiedQuotaObservation::default()
    }
}

#[test]
fn known_active_target_survives_stale_old_unobserved_or_missing_other_target() {
    for (kind, short, weekly) in provider_target_windows() {
        for (active_window, unknown_window) in [(short, weekly), (weekly, short)] {
            let known_active = active(active_window, NOW_UNIX_SECS + 2_000);
            let mut stale = active(unknown_window, NOW_UNIX_SECS + SEVEN_DAYS);
            stale.state = SubscriptionQuotaDataState::Stale;
            let mut old = active(unknown_window, NOW_UNIX_SECS + SEVEN_DAYS);
            old.observed_at_unix_millis = Some(NOW_UNIX_MILLIS - 181_000);
            for unknown in [
                Some(stale),
                Some(old),
                Some(quota_snapshot(
                    unknown_window,
                    SubscriptionQuotaDataState::Unobserved,
                    None,
                    None,
                )),
                None,
            ] {
                let mut snapshots = vec![known_active.clone()];
                if let Some(unknown) = unknown {
                    snapshots.push(unknown);
                }
                assert_decision(
                    kind,
                    &snapshots,
                    WarmupQuotaPreflightDecision::ActiveWindow {
                        cycle_key: NOW_UNIX_SECS + 2_000,
                    },
                );
            }
        }
    }
}

#[test]
fn success_uses_earliest_future_reset_from_response_or_snapshot() {
    for (kind, short, weekly) in provider_target_windows() {
        let snapshots = [
            active(short, NOW_UNIX_SECS + 2_000),
            active(weekly, NOW_UNIX_SECS + 3_000),
        ];
        for (response_reset, expected) in [(1_000, 1_000), (4_000, 2_000)] {
            let observations = [observation(weekly, NOW_UNIX_SECS + response_reset)];
            assert_eq!(
                next_warmup_evaluation_after_success(
                    kind,
                    WarmupLane::Primary,
                    &snapshots,
                    &observations,
                    NOW_UNIX_SECS,
                ),
                NOW_UNIX_SECS + expected,
                "kind={kind:?}",
            );
        }
    }
}

#[test]
fn success_with_stale_five_hour_snapshot_uses_short_cadence() {
    for (kind, short, weekly) in provider_target_windows() {
        let mut stale_short = active(short, NOW_UNIX_SECS + FIVE_HOURS);
        stale_short.state = SubscriptionQuotaDataState::Stale;
        stale_short.observed_at_unix_millis = Some(NOW_UNIX_MILLIS - 181_000);
        assert_eq!(
            next_warmup_evaluation_after_success(
                kind,
                WarmupLane::Primary,
                &[stale_short, active(weekly, NOW_UNIX_SECS + SEVEN_DAYS)],
                &[],
                NOW_UNIX_SECS,
            ),
            NOW_UNIX_SECS + FIVE_HOURS,
            "kind={kind:?}",
        );
    }
}

#[test]
fn success_with_expired_or_missing_five_hour_reset_is_bounded_by_five_hours() {
    for (kind, short, weekly) in provider_target_windows() {
        for reset in [Some(NOW_UNIX_SECS - 1), None] {
            let snapshots = [
                quota_snapshot(short, SubscriptionQuotaDataState::Fresh, Some(0.2), reset),
                active(weekly, NOW_UNIX_SECS + SEVEN_DAYS),
            ];
            let observations = [observation(weekly, NOW_UNIX_SECS + SEVEN_DAYS)];
            assert_eq!(
                next_warmup_evaluation_after_success(
                    kind,
                    WarmupLane::Primary,
                    &snapshots,
                    &observations,
                    NOW_UNIX_SECS,
                ),
                NOW_UNIX_SECS + FIVE_HOURS,
                "kind={kind:?}",
            );
        }
    }
}

#[test]
fn success_with_missing_five_hour_target_is_bounded_by_short_cadence() {
    for (kind, _, weekly) in provider_target_windows() {
        assert_eq!(
            next_warmup_evaluation_after_success(
                kind,
                WarmupLane::Primary,
                &[active(weekly, NOW_UNIX_SECS + SEVEN_DAYS)],
                &[],
                NOW_UNIX_SECS,
            ),
            NOW_UNIX_SECS + FIVE_HOURS,
            "kind={kind:?}",
        );
    }
}

#[test]
fn weekly_only_success_preserves_absence_after_the_sample_becomes_old() {
    for (kind, short, weekly) in provider_target_windows() {
        let mut absent_short = absent(short);
        absent_short.observed_at_unix_millis = Some(NOW_UNIX_MILLIS - 604_800_000);
        let snapshots = [absent_short, active(weekly, NOW_UNIX_SECS + SEVEN_DAYS)];
        let observations = [observation(weekly, NOW_UNIX_SECS + SEVEN_DAYS)];
        assert_decision(
            kind,
            &snapshots,
            WarmupQuotaPreflightDecision::ActiveWindow {
                cycle_key: NOW_UNIX_SECS + SEVEN_DAYS,
            },
        );
        assert_eq!(
            next_warmup_evaluation_after_success(
                kind,
                WarmupLane::Primary,
                &snapshots,
                &observations,
                NOW_UNIX_SECS,
            ),
            NOW_UNIX_SECS + SEVEN_DAYS,
            "kind={kind:?}",
        );
    }
}

#[test]
fn success_response_reset_overrides_the_older_snapshot_reset() {
    for (kind, short, weekly) in provider_target_windows() {
        let snapshots = [active(short, NOW_UNIX_SECS + 1_000), absent(weekly)];
        for (reset, expected) in [
            (Some(NOW_UNIX_SECS + 5_000), NOW_UNIX_SECS + 5_000),
            (None, NOW_UNIX_SECS + FIVE_HOURS),
            (Some(NOW_UNIX_SECS - 1), NOW_UNIX_SECS + FIVE_HOURS),
        ] {
            let observations = [UnifiedQuotaObservation {
                window: short,
                resets_at_unix_secs: reset.and_then(|reset| u64::try_from(reset).ok()),
                ..UnifiedQuotaObservation::default()
            }];
            assert_eq!(
                next_warmup_evaluation_after_success(
                    kind,
                    WarmupLane::Primary,
                    &snapshots,
                    &observations,
                    NOW_UNIX_SECS,
                ),
                expected,
                "kind={kind:?}",
            );
        }
    }
}

#[test]
fn anthropic_zero_usage_with_future_reset_remains_active() {
    assert_decision(
        UpstreamKind::AnthropicOauth,
        &[
            quota_snapshot(
                SubscriptionQuotaWindow::FiveHour,
                SubscriptionQuotaDataState::Fresh,
                Some(0.0),
                Some(NOW_UNIX_SECS + FIVE_HOURS),
            ),
            absent(SubscriptionQuotaWindow::SevenDay),
        ],
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: NOW_UNIX_SECS + FIVE_HOURS,
        },
    );
}

fn provider_target_windows() -> [(
    UpstreamKind,
    SubscriptionQuotaWindow,
    SubscriptionQuotaWindow,
); 3] {
    [
        (
            UpstreamKind::AnthropicOauth,
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ),
        (
            UpstreamKind::OpenaiOauth,
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ),
        (
            UpstreamKind::GeminiOauth,
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::GeminiSevenDay,
        ),
    ]
}

#[test]
fn latest_absent_five_hour_sample_overrides_older_stale_sample_for_weekly_cadence() {
    use crate::subscription_quota_cache::SubscriptionQuotaCache;
    use cc_lb_storage_api::{
        SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    };

    let upstream_id = uuid::Uuid::new_v4();
    let cache = SubscriptionQuotaCache::new();
    let mut sample = SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: NOW_UNIX_MILLIS - 600_000,
        sample_id: uuid::Uuid::new_v4(),
        utilization: Some(0.2),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: u64::try_from(NOW_UNIX_SECS + FIVE_HOURS).ok(),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        credits: None,
        ingested_at_unix_millis: NOW_UNIX_MILLIS,
    };
    cache.upsert_observation(upstream_id, &sample);
    sample.source = SubscriptionQuotaSource::Api;
    sample.sample_kind = SubscriptionQuotaSampleKind::Absent;
    sample.observed_at_unix_millis = NOW_UNIX_MILLIS - 500_000;
    sample.utilization = None;
    sample.status = None;
    sample.resets_at_unix_secs = None;
    cache.upsert_observation(upstream_id, &sample);

    let snapshots = cache.snapshot_for_upstream(upstream_id, NOW_UNIX_MILLIS, 180);
    assert_eq!(
        snapshots
            .iter()
            .find(|snapshot| snapshot.window == "5h")
            .unwrap()
            .state,
        SubscriptionQuotaDataState::Absent,
    );
    let observations = [UnifiedQuotaObservation {
        window: SubscriptionQuotaWindow::SevenDay,
        resets_at_unix_secs: u64::try_from(NOW_UNIX_SECS + SEVEN_DAYS).ok(),
        ..UnifiedQuotaObservation::default()
    }];
    assert_eq!(
        next_warmup_evaluation_after_success(
            UpstreamKind::AnthropicOauth,
            WarmupLane::Primary,
            &snapshots,
            &observations,
            NOW_UNIX_SECS,
        ),
        NOW_UNIX_SECS + SEVEN_DAYS,
    );
}

fn quota_snapshot(
    window: SubscriptionQuotaWindow,
    state: SubscriptionQuotaDataState,
    utilization: Option<f64>,
    resets_at_unix_secs: Option<i64>,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.as_str().to_owned(),
        state,
        source: Some("test".to_owned()),
        utilization,
        status: Some(SubscriptionQuotaStatus::Allowed.as_str().to_owned()),
        resets_at_unix_secs: resets_at_unix_secs.and_then(|reset| u64::try_from(reset).ok()),
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        credits: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: Some(NOW_UNIX_MILLIS),
        max_staleness_secs: 1_800,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}
