use std::time::Duration;

use apalis_core::error::{AbortError, BoxDynError};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::retry::{JitterRatio, RetryClass};
use cc_lb_scheduler::retry::{JobOutcome, RetryPayload};
use proptest::prelude::*;
use tower::{Layer, ServiceExt, service_fn};

proptest! {
    #[test]
    fn shared_modules_retry_delay_is_monotonic_capped_and_jittered(
        class in retry_class_strategy(),
        seed in any::<u64>(),
    ) {
        let policy = class.policy();
        let mut previous = Duration::ZERO;

        for attempt in 1..=policy.max_attempts() {
            let base = class.base_delay(attempt).expect("attempt is retryable");
            let delay = class.next_delay_with_seed(attempt, seed).expect("attempt is retryable");
            let lower = jitter_lower_bound(base, policy.jitter_ratio());
            let upper = jitter_upper_bound(base, policy.jitter_ratio());

            prop_assert!(delay >= lower, "{delay:?} should be >= {lower:?}");
            prop_assert!(delay <= upper, "{delay:?} should be <= {upper:?}");
            prop_assert!(delay >= previous, "{delay:?} should be monotonic after {previous:?}");
            prop_assert!(base <= policy.max_delay());
            previous = delay;
        }

        prop_assert_eq!(class.next_delay_with_seed(policy.max_attempts() + 1, seed), None);
    }
}

#[test]
fn shared_modules_retry_curves_match_named_classes() {
    assert_eq!(
        base_curve(RetryClass::Probe, 4),
        vec![Some(1), Some(2), Some(4), None]
    );
    assert_eq!(
        base_curve(RetryClass::Adaptive, 6),
        vec![Some(30), Some(60), Some(120), Some(240), Some(480), None]
    );
    assert_eq!(base_curve(RetryClass::Maintenance, 2), vec![Some(60), None]);
}

#[test]
fn shared_modules_retry_entity_jitter_distribution_is_uniform_enough() {
    let base = Duration::from_secs(600);
    let mut buckets = [0_u32; 10];

    for seed in 0_u64..1_000 {
        let delay = RetryClass::Adaptive.jitter_delay(base, seed);
        assert!(delay >= Duration::from_secs(540));
        assert!(delay <= Duration::from_secs(660));

        let offset_ms = delay.as_millis().saturating_sub(540_000);
        let bucket = (offset_ms * 10 / 120_001).min(9);
        buckets[usize::try_from(bucket).expect("bucket fits usize")] += 1;
    }

    let expected = 100.0_f64;
    let chi_square = buckets.iter().fold(0.0_f64, |acc, count| {
        let delta = f64::from(*count) - expected;
        acc + (delta * delta / expected)
    });

    assert!(
        chi_square < 16.919,
        "expected p > 0.05 for 9 degrees of freedom, chi_square={chi_square}, buckets={buckets:?}"
    );
}

fn retry_class_strategy() -> impl Strategy<Value = RetryClass> {
    prop::sample::select(vec![
        RetryClass::Probe,
        RetryClass::Adaptive,
        RetryClass::Maintenance,
    ])
}

fn base_curve(class: RetryClass, attempts: u32) -> Vec<Option<u64>> {
    (1..=attempts)
        .map(|attempt| class.base_delay(attempt).map(|delay| delay.as_secs()))
        .collect()
}

fn jitter_lower_bound(base: Duration, ratio: JitterRatio) -> Duration {
    Duration::from_millis(
        u64::try_from(
            base.as_millis()
                .saturating_mul(u128::from(1_000 - ratio.per_mille()))
                / 1_000,
        )
        .expect("duration millis fits u64"),
    )
}

fn jitter_upper_bound(base: Duration, ratio: JitterRatio) -> Duration {
    Duration::from_millis(
        u64::try_from(
            base.as_millis()
                .saturating_mul(u128::from(1_000 + ratio.per_mille()))
                / 1_000,
        )
        .expect("duration millis fits u64"),
    )
}

#[derive(Clone, Copy)]
struct RetryRequest {
    attempt_count: u32,
    allows_retry: bool,
    seed: u64,
}

impl RetryPayload for RetryRequest {
    fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    fn allows_retry(&self) -> bool {
        self.allows_retry
    }

    fn retry_seed(&self) -> u64 {
        self.seed
    }
}

async fn retry_result(
    class: RetryClass,
    attempt_count: u32,
    allows_retry: bool,
    seed: u64,
    result: Result<JobOutcome, BoxDynError>,
) -> Result<JobOutcome, BoxDynError> {
    let mut result = Some(result);
    class
        .layer()
        .layer(service_fn(move |_request: RetryRequest| {
            std::future::ready(result.take().expect("inner service called once"))
        }))
        .oneshot(RetryRequest {
            attempt_count,
            allows_retry,
            seed,
        })
        .await
}

#[tokio::test]
async fn retry_class_service_uses_one_based_ordinal_and_honors_no_retry_override() {
    for class in [
        RetryClass::Maintenance,
        RetryClass::Probe,
        RetryClass::Adaptive,
    ] {
        for attempt in 0..=class.policy().max_attempts() {
            let outcome = retry_result(
                class,
                attempt,
                true,
                17,
                Ok(JobOutcome::Retry { delay: None }),
            )
            .await
            .expect("retry classification");
            if attempt < class.policy().max_attempts() {
                assert!(
                    matches!(outcome, JobOutcome::Retry { delay: Some(delay) } if !delay.is_zero())
                );
            } else {
                assert_eq!(outcome, JobOutcome::DeadLetter);
            }
        }
        for delay in [None, Some(Duration::from_secs(30))] {
            assert_eq!(
                retry_result(class, 0, false, 17, Ok(JobOutcome::Retry { delay }))
                    .await
                    .expect("single attempt"),
                JobOutcome::DeadLetter,
            );
        }
    }
}

#[tokio::test]
async fn retry_class_service_honors_and_clamps_explicit_positive_delay() {
    for (class, input, expected) in [
        (RetryClass::Maintenance, 30, 30),
        (RetryClass::Maintenance, 3_600, 60),
        (RetryClass::Adaptive, 7, 7),
        (RetryClass::Adaptive, 10_000, 600),
        (RetryClass::Probe, 10, 5),
    ] {
        for seed in [0, 1, u64::MAX] {
            assert_eq!(
                retry_result(
                    class,
                    0,
                    true,
                    seed,
                    Ok(JobOutcome::Retry {
                        delay: Some(Duration::from_secs(input))
                    })
                )
                .await
                .expect("explicit delay"),
                JobOutcome::Retry {
                    delay: Some(Duration::from_secs(expected))
                },
            );
        }
    }
}

#[tokio::test]
async fn retry_class_service_rejects_zero_retry_delay_as_error() {
    for class in [
        RetryClass::Maintenance,
        RetryClass::Probe,
        RetryClass::Adaptive,
    ] {
        for attempt in (0..=class.policy().max_attempts() + 1).chain([u32::MAX]) {
            for allows_retry in [false, true] {
                let error = retry_result(
                    class,
                    attempt,
                    allows_retry,
                    0,
                    Ok(JobOutcome::Retry {
                        delay: Some(Duration::ZERO),
                    }),
                )
                .await
                .expect_err("zero delay must always be rejected");
                assert!(matches!(
                    error.downcast_ref::<SchedulerError>(),
                    Some(SchedulerError::InvalidRetryDelay)
                ));
                assert_eq!(error.to_string(), "retry delay must be non-zero");
            }
        }
    }
}

#[tokio::test]
async fn retry_class_service_none_uses_jittered_backoff_and_preserves_passthrough() {
    for class in [
        RetryClass::Maintenance,
        RetryClass::Probe,
        RetryClass::Adaptive,
    ] {
        for attempt in 0..class.policy().max_attempts() {
            for seed in [0, 17, u64::MAX] {
                let outcome = retry_result(
                    class,
                    attempt,
                    true,
                    seed,
                    Ok(JobOutcome::Retry { delay: None }),
                )
                .await
                .expect("backoff delay");
                assert_eq!(
                    outcome,
                    JobOutcome::Retry {
                        delay: class.next_delay_with_seed(attempt + 1, seed)
                    }
                );
                let JobOutcome::Retry { delay: Some(delay) } = outcome else {
                    panic!("expected retry")
                };
                let base = class.base_delay(attempt + 1).expect("retryable");
                assert!(delay >= jitter_lower_bound(base, class.policy().jitter_ratio()));
                assert!(delay <= jitter_upper_bound(base, class.policy().jitter_ratio()));
            }
        }
        for outcome in [
            JobOutcome::Done,
            JobOutcome::Skip,
            JobOutcome::Noop,
            JobOutcome::DeadLetter,
        ] {
            assert_eq!(
                retry_result(class, u32::MAX, false, 0, Ok(outcome.clone()))
                    .await
                    .expect("passthrough"),
                outcome
            );
        }
        let error: BoxDynError = Box::new(AbortError::new(std::io::Error::other("abort identity")));
        let original = std::ptr::from_ref(error.downcast_ref::<AbortError>().expect("abort"));
        let returned = retry_result(class, 0, true, 0, Err(error))
            .await
            .expect_err("abort passthrough");
        assert_eq!(
            std::ptr::from_ref(
                returned
                    .downcast_ref::<AbortError>()
                    .expect("same error type")
            ),
            original
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..Default::default() })]

    #[test]
    fn prop_retry_class_service_invariants(
        class in retry_class_strategy(),
        attempt_count in prop_oneof![0_u32..8, any::<u32>(), Just(u32::MAX)],
        allows_retry in any::<bool>(),
        outcome_kind in 0_u8..7,
        delay_secs in 1_u64..20_000,
        seed in any::<u64>(),
    ) {
        let outcome = match outcome_kind {
            0 => JobOutcome::Retry { delay: Some(Duration::ZERO) },
            1 => JobOutcome::Retry { delay: Some(Duration::from_secs(delay_secs)) },
            2 => JobOutcome::Retry { delay: None },
            3 => JobOutcome::Done,
            4 => JobOutcome::Skip,
            5 => JobOutcome::Noop,
            _ => JobOutcome::DeadLetter,
        };
        let runtime = tokio::runtime::Builder::new_current_thread().build().expect("test runtime");
        let result = runtime.block_on(retry_result(class, attempt_count, allows_retry, seed, Ok(outcome.clone())));
        let ordinal = attempt_count.saturating_add(1);
        if outcome_kind == 0 {
            let error = result.expect_err("zero delay");
            prop_assert!(matches!(error.downcast_ref::<SchedulerError>(), Some(SchedulerError::InvalidRetryDelay)));
        } else if outcome_kind <= 2 && (!allows_retry || ordinal > class.policy().max_attempts()) {
            prop_assert_eq!(result.expect("exhausted retry"), JobOutcome::DeadLetter);
        } else if outcome_kind == 1 {
            prop_assert_eq!(result.expect("explicit retry"), JobOutcome::Retry {
                delay: Some(Duration::from_secs(delay_secs).min(class.policy().max_delay())),
            });
        } else if outcome_kind == 2 {
            let JobOutcome::Retry { delay: Some(delay) } = result.expect("backoff") else {
                return Err(TestCaseError::fail("expected positive concrete backoff"));
            };
            let (initial, cap) = match class {
                RetryClass::Probe => (1_u64, 5_u64),
                RetryClass::Adaptive => (30, 600),
                RetryClass::Maintenance => (60, 60),
            };
            let base_secs = (initial * (1_u64 << (ordinal - 1))).min(cap);
            prop_assert!(!delay.is_zero());
            prop_assert!(delay >= Duration::from_millis(base_secs * 900));
            prop_assert!(delay <= Duration::from_millis(base_secs * 1_100));
            prop_assert!(Duration::from_secs(base_secs) <= class.policy().max_delay());
        } else {
            prop_assert_eq!(result.expect("terminal passthrough"), outcome);
        }
    }
}
