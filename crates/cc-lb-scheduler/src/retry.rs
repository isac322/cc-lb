//! Retry logic and backoff strategies for failed jobs.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tower::{Layer, Service};

use crate::error::SchedulerError;
use crate::middleware::JobOutcomeStatus;

type RetryFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

const JITTER_DENOMINATOR_PER_MILLE: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Probe,
    Adaptive,
    Maintenance,
}

impl RetryClass {
    pub const fn policy(self) -> RetryPolicy {
        match self {
            Self::Probe => RetryPolicy::new(3, 1, 5, JitterRatio::from_per_mille(100)),
            Self::Adaptive => RetryPolicy::new(5, 30, 600, JitterRatio::from_per_mille(100)),
            Self::Maintenance => RetryPolicy::new(1, 60, 60, JitterRatio::from_per_mille(100)),
        }
    }

    pub fn base_delay(self, attempt: u32) -> Option<Duration> {
        self.policy().base_delay(attempt)
    }

    pub fn next_delay_with_seed(self, attempt: u32, seed: u64) -> Option<Duration> {
        self.base_delay(attempt)
            .map(|base_delay| self.jitter_delay(base_delay, seed))
    }

    pub fn jitter_delay(self, base_delay: Duration, seed: u64) -> Duration {
        self.policy().jitter_delay(base_delay, seed)
    }

    pub const fn layer(self) -> RetryClassLayer {
        RetryClassLayer::new(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_attempts: u32,
    base_secs: u64,
    max_secs: u64,
    jitter_ratio: JitterRatio,
}

impl RetryPolicy {
    pub const fn new(
        max_attempts: u32,
        base_secs: u64,
        max_secs: u64,
        jitter_ratio: JitterRatio,
    ) -> Self {
        Self {
            max_attempts,
            base_secs,
            max_secs,
            jitter_ratio,
        }
    }

    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    pub const fn max_delay(self) -> Duration {
        Duration::from_secs(self.max_secs)
    }

    pub const fn jitter_ratio(self) -> JitterRatio {
        self.jitter_ratio
    }

    pub fn base_delay(self, attempt: u32) -> Option<Duration> {
        if attempt == 0 || attempt > self.max_attempts {
            return None;
        }

        let multiplier = 2_u64.saturating_pow(attempt.saturating_sub(1));
        let seconds = self.base_secs.saturating_mul(multiplier).min(self.max_secs);
        Some(Duration::from_secs(seconds))
    }

    pub fn jitter_delay(self, base_delay: Duration, seed: u64) -> Duration {
        let jitter = u64::from(self.jitter_ratio.per_mille());
        if jitter == 0 {
            return base_delay;
        }

        let width = jitter.saturating_mul(2).saturating_add(1);
        let slot = mixed_seed(seed) % width;
        let offset = i128::from(slot) - i128::from(jitter);
        let base_millis = i128::try_from(base_delay.as_millis()).unwrap_or(i128::MAX);
        let delta = base_millis.saturating_mul(offset) / i128::from(JITTER_DENOMINATOR_PER_MILLE);
        let jittered = base_millis.saturating_add(delta);

        if jittered <= 0 {
            return Duration::ZERO;
        }

        match u128::try_from(jittered) {
            Ok(millis) => duration_from_millis(millis),
            Err(_negative) => Duration::ZERO,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JitterRatio {
    per_mille: u16,
}

impl JitterRatio {
    pub const fn from_per_mille(per_mille: u16) -> Self {
        Self { per_mille }
    }

    pub const fn per_mille(self) -> u16 {
        self.per_mille
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobOutcome {
    Done,
    Skip,
    Noop,
    Retry { delay: Option<Duration> },
    DeadLetter,
}

impl JobOutcome {
    pub const fn metric_status(&self) -> JobOutcomeStatus {
        match self {
            Self::Done => JobOutcomeStatus::Done,
            Self::Skip => JobOutcomeStatus::Skip,
            Self::Noop => JobOutcomeStatus::Noop,
            Self::Retry { delay: _ } | Self::DeadLetter => JobOutcomeStatus::Retry,
        }
    }

    pub const fn is_terminal_failure(&self) -> bool {
        match self {
            Self::DeadLetter => true,
            Self::Done | Self::Skip | Self::Noop | Self::Retry { delay: _ } => false,
        }
    }
}

pub trait RetryPayload {
    fn attempt_count(&self) -> u32;

    fn allows_retry(&self) -> bool {
        true
    }

    fn retry_seed(&self) -> u64 {
        u64::from(self.attempt_count())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryClassLayer {
    class: RetryClass,
}

impl RetryClassLayer {
    pub const fn new(class: RetryClass) -> Self {
        Self { class }
    }
}

impl<S> Layer<S> for RetryClassLayer {
    type Service = RetryClassService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryClassService {
            inner,
            class: self.class,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetryClassService<S> {
    inner: S,
    class: RetryClass,
}

impl<S, P> Service<P> for RetryClassService<S>
where
    P: RetryPayload + Send + 'static,
    S: Service<P, Response = JobOutcome>,
    S::Error: From<SchedulerError>,
    S::Future: Send + 'static,
{
    type Response = JobOutcome;
    type Error = S::Error;
    type Future = RetryFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, payload: P) -> Self::Future {
        let attempt_count = payload.attempt_count();
        let allows_retry = payload.allows_retry();
        let retry_seed = payload.retry_seed();
        let ordinal = attempt_count.saturating_add(1);
        let class = self.class;
        let future = self.inner.call(payload);
        Box::pin(async move {
            match future.await? {
                JobOutcome::Retry {
                    delay: Some(explicit),
                } if explicit.is_zero() => Err(SchedulerError::InvalidRetryDelay.into()),
                JobOutcome::Retry { .. }
                    if !allows_retry || ordinal > class.policy().max_attempts() =>
                {
                    Ok(JobOutcome::DeadLetter)
                }
                JobOutcome::Retry {
                    delay: Some(explicit),
                } => Ok(JobOutcome::Retry {
                    delay: Some(explicit.min(class.policy().max_delay())),
                }),
                JobOutcome::Retry { delay: None } => {
                    match class.next_delay_with_seed(ordinal, retry_seed) {
                        Some(delay) => Ok(JobOutcome::Retry { delay: Some(delay) }),
                        None => Ok(JobOutcome::DeadLetter),
                    }
                }
                outcome @ (JobOutcome::Done
                | JobOutcome::Skip
                | JobOutcome::Noop
                | JobOutcome::DeadLetter) => Ok(outcome),
            }
        })
    }
}

fn mixed_seed(seed: u64) -> u64 {
    seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn duration_from_millis(millis: u128) -> Duration {
    match u64::try_from(millis) {
        Ok(value) => Duration::from_millis(value),
        Err(_overflow) => Duration::from_millis(u64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::JobOutcome;

    #[test]
    fn job_outcome_serde_wire_compatibility() {
        for (outcome, wire) in [
            (
                JobOutcome::Retry {
                    delay: Some(Duration::from_secs(30)),
                },
                serde_json::json!({"Retry": {"delay": {"secs": 30, "nanos": 0}}}),
            ),
            (
                JobOutcome::Retry { delay: None },
                serde_json::json!({"Retry": {"delay": null}}),
            ),
        ] {
            assert_eq!(
                serde_json::to_value(&outcome).expect("serialize outcome"),
                wire
            );
            assert_eq!(
                serde_json::from_value::<JobOutcome>(wire).expect("deserialize outcome"),
                outcome
            );
        }
    }
}
