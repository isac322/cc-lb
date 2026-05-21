#![cfg(loom)]

use cc_lb_loom_tests::quota::{ConsumeResult, QuotaCounter};
use loom::sync::Arc;
use loom::thread;

#[test]
fn quota_no_double_count_or_missed_reject() {
    loom::model(|| {
        const LIMIT: u64 = 5;
        const AMOUNTS: [u64; 3] = [2, 2, 2];

        let counter = Arc::new(QuotaCounter::new());
        let mut handles = Vec::new();

        for amount in AMOUNTS {
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || counter.try_consume(amount, LIMIT)));
        }

        let mut accepted_total = 0;
        let mut rejected = 0;
        for handle in handles {
            match handle.join().unwrap() {
                ConsumeResult::Allowed { amount } => accepted_total += amount,
                ConsumeResult::Rejected {
                    observed,
                    amount,
                    limit,
                } => {
                    rejected += 1;
                    assert!(observed + amount > limit);
                }
            }
        }

        assert!(accepted_total <= LIMIT);
        assert_eq!(counter.usage(), accepted_total);
        assert_eq!(rejected, 1);
    });
}
