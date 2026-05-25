#![forbid(unsafe_code)]

use std::time::Duration;

use cc_lb_core::api_keys::types::{Limit, LimitKind};
use proptest::prelude::*;

fn arb_limit_kind() -> impl Strategy<Value = LimitKind> {
    prop_oneof![
        Just(LimitKind::Requests),
        Just(LimitKind::InputTokens),
        Just(LimitKind::OutputTokens),
        Just(LimitKind::TotalTokens),
        Just(LimitKind::CostUsd),
        Just(LimitKind::Concurrent),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn limit_subset_iff_kind_window_and_cap(
        child_kind in arb_limit_kind(),
        parent_kind in arb_limit_kind(),
        child_window_secs in 1u64..=86_400,
        parent_window_secs in 1u64..=86_400,
        child_cap in -1_000_000_000i64..=1_000_000_000,
        parent_cap in -1_000_000_000i64..=1_000_000_000,
    ) {
        let child = Limit {
            kind: child_kind,
            window: Duration::from_secs(child_window_secs),
            cap_micros: child_cap,
        };
        let parent = Limit {
            kind: parent_kind,
            window: Duration::from_secs(parent_window_secs),
            cap_micros: parent_cap,
        };

        prop_assert_eq!(
            child.is_subset_of(&parent),
            child_kind == parent_kind
                && child_window_secs == parent_window_secs
                && child_cap <= parent_cap,
        );
    }
}
