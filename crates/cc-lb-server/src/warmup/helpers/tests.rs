use super::*;
use std::collections::HashSet;

fn five_hour_header(resets_at_unix_secs: u64) -> UnifiedQuotaObservation {
    UnifiedQuotaObservation {
        resets_at_unix_secs: Some(resets_at_unix_secs),
        ..Default::default()
    }
}

fn seven_day_header(
    resets_at_unix_secs: u64,
    status: Option<SubscriptionQuotaStatus>,
    utilization: Option<f64>,
) -> UnifiedQuotaObservation {
    UnifiedQuotaObservation {
        window: SubscriptionQuotaWindow::SevenDay,
        utilization,
        status,
        resets_at_unix_secs: Some(resets_at_unix_secs),
        ..Default::default()
    }
}
fn fable_weekly_header(
    resets_at_unix_secs: u64,
    status: Option<SubscriptionQuotaStatus>,
    utilization: Option<f64>,
) -> UnifiedQuotaObservation {
    UnifiedQuotaObservation {
        window: SubscriptionQuotaWindow::SevenDayFable,
        utilization,
        status,
        resets_at_unix_secs: Some(resets_at_unix_secs),
        ..Default::default()
    }
}

#[test]
fn jitter_is_deterministic() {
    let upstream_id = Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);
    let resets_at = 1_800_000_000;
    let jitter_ms = stable_jitter_ms(upstream_id, resets_at);

    for _ in 0..1_000 {
        assert_eq!(stable_jitter_ms(upstream_id, resets_at), jitter_ms);
    }
}

#[test]
fn jitter_is_under_30s() {
    for index in 0..1_000_u128 {
        let upstream_id = Uuid::from_u128(index + 1);
        let jitter_ms = stable_jitter_ms(upstream_id, 1_800_000_000 + index as u64);
        assert!(jitter_ms < 30_000, "jitter_ms={jitter_ms}");
    }
}

#[test]
fn jitter_distributes_across_upstreams() {
    let candidate_resets_at_unix_secs = 1_800_000_000;
    let distinct_jitters = (0..10)
        .map(|_| stable_jitter_ms(Uuid::new_v4(), candidate_resets_at_unix_secs))
        .collect::<HashSet<_>>();

    assert!(
        distinct_jitters.len() >= 7,
        "distinct_jitters={distinct_jitters:?}"
    );
}

#[test]
fn backoff_schedule_sequence() {
    let mut schedule = BackoffSchedule::default();

    let attempts = (0..6)
        .map(|_| {
            schedule
                .next()
                .expect("backoff schedule is infinite")
                .as_secs()
        })
        .collect::<Vec<_>>();
    assert_eq!(attempts, [60, 120, 300, 600, 600, 600]);

    schedule.reset();
    assert_eq!(schedule.next(), Some(Duration::from_secs(60)));
}

#[test]
fn classify_2xx_writes_success() {
    assert_eq!(
        classify_response(StatusCode::OK, &[], 1_800_000_000),
        WarmupResult::Success {
            cycle_key: 1_800_000_000,
        }
    );
}

#[test]
fn classify_2xx_success_uses_5h_header_when_present() {
    assert_eq!(
        classify_response(
            StatusCode::CREATED,
            &[five_hour_header(1_800_003_600)],
            1_800_000_000
        ),
        WarmupResult::Success {
            cycle_key: 1_800_003_600,
        }
    );
}

#[test]
fn classify_429_with_5h_active_header_writes_window_already_active() {
    assert_eq!(
        classify_response(
            StatusCode::TOO_MANY_REQUESTS,
            &[five_hour_header(1_800_003_600)],
            1_800_000_000,
        ),
        WarmupResult::WindowAlreadyActive {
            cycle_key: 1_800_003_600,
        }
    );
}

#[test]
fn classify_429_with_7d_reset_writes_seven_day_window_exhausted() {
    assert_eq!(
        classify_response(
            StatusCode::TOO_MANY_REQUESTS,
            &[seven_day_header(
                1_800_604_800,
                Some(SubscriptionQuotaStatus::Rejected),
                None,
            )],
            1_800_000_000,
        ),
        WarmupResult::SevenDayWindowExhausted {
            resets_at: 1_800_604_800,
        }
    );
}

#[test]
fn classify_429_with_fable_weekly_reset_writes_seven_day_window_exhausted() {
    assert_eq!(
        classify_response(
            StatusCode::TOO_MANY_REQUESTS,
            &[fable_weekly_header(
                1_800_604_800,
                Some(SubscriptionQuotaStatus::Rejected),
                None,
            )],
            1_800_000_000,
        ),
        WarmupResult::SevenDayWindowExhausted {
            resets_at: 1_800_604_800,
        }
    );
}

#[test]
fn classify_429_preserves_shared_seven_day_precedence() {
    assert_eq!(
        classify_response(
            StatusCode::TOO_MANY_REQUESTS,
            &[
                fable_weekly_header(1_800_700_000, Some(SubscriptionQuotaStatus::Rejected), None,),
                seven_day_header(1_800_604_800, Some(SubscriptionQuotaStatus::Rejected), None,),
            ],
            1_800_000_000,
        ),
        WarmupResult::SevenDayWindowExhausted {
            resets_at: 1_800_604_800,
        }
    );
}

#[test]
fn classify_429_with_allowed_7d_reset_uses_5h_window() {
    assert_eq!(
        classify_response(
            StatusCode::TOO_MANY_REQUESTS,
            &[
                five_hour_header(1_800_003_600),
                seven_day_header(
                    1_800_604_800,
                    Some(SubscriptionQuotaStatus::Allowed),
                    Some(0.25),
                ),
            ],
            1_800_000_000,
        ),
        WarmupResult::WindowAlreadyActive {
            cycle_key: 1_800_003_600,
        }
    );
}

#[test]
fn classify_429_without_5h_header_retryable() {
    assert_eq!(
        classify_response(StatusCode::TOO_MANY_REQUESTS, &[], 1_800_000_000),
        WarmupResult::RetryableTransient
    );
}

#[test]
fn classify_401_permanent_auth() {
    assert_eq!(
        classify_response(StatusCode::UNAUTHORIZED, &[], 1_800_000_000),
        WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::AuthFailed)
    );
}

#[test]
fn classify_403_permanent_forbidden() {
    assert_eq!(
        classify_response(StatusCode::FORBIDDEN, &[], 1_800_000_000),
        WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::Forbidden)
    );
}

#[test]
fn classify_400_permanent_bad_request() {
    assert_eq!(
        classify_response(StatusCode::BAD_REQUEST, &[], 1_800_000_000),
        WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::BadRequest)
    );
}

#[test]
fn classify_404_permanent_not_found() {
    assert_eq!(
        classify_response(StatusCode::NOT_FOUND, &[], 1_800_000_000),
        WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::NotFound)
    );
}

#[test]
fn classify_5xx_retryable() {
    assert_eq!(
        classify_response(StatusCode::BAD_GATEWAY, &[], 1_800_000_000),
        WarmupResult::RetryableTransient
    );
}

#[test]
fn classify_network_error_retryable() {
    assert_eq!(
        classify_response(StatusCode::BAD_GATEWAY, &[], 1_800_000_000),
        WarmupResult::RetryableTransient
    );
}

#[test]
fn classify_unknown_status_retryable_conservative() {
    assert_eq!(
        classify_response(StatusCode::IM_A_TEAPOT, &[], 1_800_000_000),
        WarmupResult::RetryableTransient
    );
}
