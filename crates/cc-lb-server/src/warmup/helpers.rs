use std::hash::Hasher;
use std::time::Duration;

use cc_lb_engine::UnifiedQuotaObservation;
use cc_lb_storage_api::{
    SubscriptionQuotaLatestRecord, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
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
    SevenDayWindowExhausted { resets_at: i64 },
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
        StatusCode::TOO_MANY_REQUESTS => {
            if let Some(resets_at) = seven_day_exhausted_cycle_key(parsed_headers)
                && resets_at > candidate_cycle_key
            {
                return WarmupResult::SevenDayWindowExhausted { resets_at };
            }

            match five_hour_cycle_key(parsed_headers) {
                Some(cycle_key) if cycle_key > candidate_cycle_key => {
                    WarmupResult::WindowAlreadyActive { cycle_key }
                }
                _ => WarmupResult::RetryableTransient,
            }
        }
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
    cycle_key_for_window(parsed_headers, SubscriptionQuotaWindow::FiveHour)
}

fn seven_day_exhausted_cycle_key(parsed_headers: &[UnifiedQuotaObservation]) -> Option<i64> {
    const SEVEN_DAY_WINDOWS: [SubscriptionQuotaWindow; 3] = [
        SubscriptionQuotaWindow::SevenDay,
        SubscriptionQuotaWindow::SevenDaySonnet,
        SubscriptionQuotaWindow::SevenDayOpus,
    ];

    SEVEN_DAY_WINDOWS
        .into_iter()
        .find_map(|window| exhausted_cycle_key_for_window(parsed_headers, window))
}

fn exhausted_cycle_key_for_window(
    parsed_headers: &[UnifiedQuotaObservation],
    window: SubscriptionQuotaWindow,
) -> Option<i64> {
    parsed_headers
        .iter()
        .find(|observation| {
            observation.window == window && quota_observation_is_exhausted(observation)
        })
        .and_then(|observation| observation.resets_at_unix_secs)
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

fn quota_observation_is_exhausted(observation: &UnifiedQuotaObservation) -> bool {
    observation.status == Some(SubscriptionQuotaStatus::Rejected)
        || observation
            .utilization
            .is_some_and(|utilization| utilization >= 1.0)
}

fn cycle_key_for_window(
    parsed_headers: &[UnifiedQuotaObservation],
    window: SubscriptionQuotaWindow,
) -> Option<i64> {
    parsed_headers
        .iter()
        .find(|observation| observation.window == window)
        .and_then(|observation| observation.resets_at_unix_secs)
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

#[cfg(test)]
mod tests;
