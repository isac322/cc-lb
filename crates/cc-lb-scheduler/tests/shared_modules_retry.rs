use std::time::Duration;

use cc_lb_scheduler::retry::{JitterRatio, RetryClass};
use proptest::prelude::*;

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
