use std::hash::Hasher;
use std::time::Duration;

use cc_lb_core::UnifiedQuotaObservation;
use cc_lb_storage_api::{SubscriptionQuotaLatestRecord, SubscriptionQuotaWindow};
use http::StatusCode;
use siphasher::sip::SipHasher13;
use uuid::Uuid;

const JITTER_SPREAD_MS: u64 = 30_000;
const BACKOFF_STEPS_SECS: [u64; 4] = [60, 120, 300, 600];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmupAbandonReason {
    AuthFailed,
    Forbidden,
    BadRequest,
    NotFound,
    DialectPlugin,
}

impl WarmupAbandonReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AuthFailed => "auth_failed",
            Self::Forbidden => "forbidden",
            Self::BadRequest => "bad_request",
            Self::NotFound => "not_found",
            Self::DialectPlugin => "dialect_plugin_failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmupResult {
    Success { cycle_key: i64 },
    RetryableTransient,
    AbandonCyclePermanent(WarmupAbandonReason),
    WindowAlreadyActive { cycle_key: i64 },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackoffSchedule {
    attempt: u32,
}

impl BackoffSchedule {
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

impl Iterator for BackoffSchedule {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let step_index = (self.attempt as usize).min(BACKOFF_STEPS_SECS.len() - 1);
        self.attempt = self.attempt.saturating_add(1);
        Some(Duration::from_secs(BACKOFF_STEPS_SECS[step_index]))
    }
}

pub fn stable_jitter_ms(upstream_id: Uuid, candidate_resets_at_unix_secs: u64) -> u64 {
    const SIPHASH_K0: u64 = 0;
    const SIPHASH_K1: u64 = 0;
    let mut hasher = SipHasher13::new_with_keys(SIPHASH_K0, SIPHASH_K1);
    hasher.write(upstream_id.as_bytes());
    hasher.write(&candidate_resets_at_unix_secs.to_le_bytes());
    hasher.finish() % JITTER_SPREAD_MS
}

pub fn cycle_key_from_observation(latest: &SubscriptionQuotaLatestRecord) -> Option<i64> {
    latest
        .resets_at_unix_secs
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

pub fn classify_response(
    status: StatusCode,
    parsed_headers: &[UnifiedQuotaObservation],
    candidate_cycle_key: i64,
) -> WarmupResult {
    if status.is_success() {
        return WarmupResult::Success {
            cycle_key: five_hour_cycle_key(parsed_headers).unwrap_or(candidate_cycle_key),
        };
    }

    match status {
        StatusCode::TOO_MANY_REQUESTS => match five_hour_cycle_key(parsed_headers) {
            Some(cycle_key) if cycle_key > candidate_cycle_key => {
                WarmupResult::WindowAlreadyActive { cycle_key }
            }
            _ => WarmupResult::RetryableTransient,
        },
        StatusCode::UNAUTHORIZED => {
            WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::AuthFailed)
        }
        StatusCode::FORBIDDEN => {
            WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::Forbidden)
        }
        StatusCode::BAD_REQUEST => {
            WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::BadRequest)
        }
        StatusCode::NOT_FOUND => WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::NotFound),
        status if status.is_server_error() => WarmupResult::RetryableTransient,
        _ => WarmupResult::RetryableTransient,
    }
}

fn five_hour_cycle_key(parsed_headers: &[UnifiedQuotaObservation]) -> Option<i64> {
    parsed_headers
        .iter()
        .find(|observation| observation.window == SubscriptionQuotaWindow::FiveHour)
        .and_then(|observation| observation.resets_at_unix_secs)
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn five_hour_header(resets_at_unix_secs: u64) -> UnifiedQuotaObservation {
        UnifiedQuotaObservation {
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
}
