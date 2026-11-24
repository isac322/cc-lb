pub use cc_lb_storage_api::{KeyStatus, Limit, LimitKind};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_subset_of_accepts_same_kind_window_and_lower_cap() {
        let parent = Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 300,
            cap_micros: 1_000,
        };
        let child = Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 300,
            cap_micros: 750,
        };

        assert!(child.is_subset_of(&parent));
    }

    #[test]
    fn is_subset_of_rejects_larger_cap() {
        let parent = Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        };
        let child = Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 200,
        };

        assert!(!child.is_subset_of(&parent));
    }

    #[test]
    fn is_subset_of_rejects_different_kind() {
        let parent = Limit {
            kind: LimitKind::Concurrent,
            window_secs: 60,
            cap_micros: 1,
        };
        let child = Limit {
            kind: LimitKind::CostUsd,
            window_secs: 60,
            cap_micros: 1,
        };

        assert!(!child.is_subset_of(&parent));
    }
}
