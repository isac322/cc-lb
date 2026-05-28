//! Sparse integer ordering utility for plugin chain positioning.
//!
//! Maintains ordering with gaps (multiples of STEP) to allow insertions between
//! existing values without reordering the entire sequence. When gaps become too tight,
//! `rebalance` redistributes values evenly.

/// Gap size supporting ~10 consecutive inserts before rebalance needed.
pub const STEP: i64 = 1000;

/// Returns the next available position after existing values.
///
/// For an empty slice, returns STEP.
/// For non-empty input, returns max(existing) + STEP.
pub fn next_after(existing: &[i64]) -> i64 {
    existing.iter().max().copied().unwrap_or(0) + STEP
}

/// Returns a value strictly between lower and upper bounds.
///
/// In debug mode, panics if the gap is ≤1 (no space for a value between them).
/// In release mode, returns the midpoint without checking.
pub fn between(lower: i64, upper: i64) -> i64 {
    debug_assert!(
        upper - lower >= 2,
        "gap too tight: {} to {} (need gap ≥ 2 for between)",
        lower,
        upper
    );
    (lower + upper) / 2
}

/// Returns evenly-spaced values with STEP spacing, starting at STEP.
///
/// Rebalances a sequence by redistributing N values as [STEP, 2*STEP, 3*STEP, ..., N*STEP].
pub fn rebalance(existing: Vec<i64>) -> Vec<i64> {
    existing
        .iter()
        .enumerate()
        .map(|(idx, _)| STEP * (idx as i64 + 1))
        .collect()
}

/// Returns true if any adjacent pair has a gap smaller than 2.
///
/// A gap < 2 means no value can be inserted between them.
pub fn needs_rebalance(existing: &[i64]) -> bool {
    if existing.len() < 2 {
        return false;
    }

    let mut sorted = existing.to_vec();
    sorted.sort_unstable();

    sorted.windows(2).any(|pair| pair[1] - pair[0] < 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_after_empty() {
        assert_eq!(next_after(&[]), STEP);
    }

    #[test]
    fn next_after_grows_by_step() {
        assert_eq!(next_after(&[1000]), 2000);
        assert_eq!(next_after(&[1000, 2000, 3000]), 4000);
        assert_eq!(next_after(&[3000, 1000, 2000]), 4000); // order doesn't matter
    }

    #[test]
    fn between_midpoint() {
        assert_eq!(between(1000, 3000), 2000);
        assert_eq!(between(0, 10), 5);
        assert_eq!(between(100, 200), 150);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn between_too_tight_in_debug_panics() {
        // Gap of exactly 1 should panic in debug mode
        let result = std::panic::catch_unwind(|| between(1000, 1001));
        assert!(result.is_err(), "Should panic for gap < 2 in debug mode");
    }

    #[test]
    fn rebalance_idempotent() {
        let input = vec![1000, 2000, 3000, 4000];
        let rebalanced = rebalance(input);
        assert_eq!(rebalanced, vec![1000, 2000, 3000, 4000]);

        // Rebalancing again should be the same
        let rebalanced_again = rebalance(rebalanced);
        assert_eq!(rebalanced_again, vec![1000, 2000, 3000, 4000]);
    }

    #[test]
    fn rebalance_handles_unsorted_input() {
        let input = vec![3000, 1000, 2000];
        let rebalanced = rebalance(input);
        assert_eq!(rebalanced, vec![1000, 2000, 3000]);
    }

    #[test]
    fn needs_rebalance_detects_tight_gap() {
        // Gap of exactly 1 between adjacent values
        assert!(needs_rebalance(&[1000, 1001]));

        // Gap of 2 is sufficient (can fit one value in between: 1000.5, but we work with integers)
        assert!(!needs_rebalance(&[1000, 1002]));

        // Multiple gaps, one too tight
        assert!(needs_rebalance(&[1000, 2000, 2001, 3000]));

        // Empty and single element don't need rebalance
        assert!(!needs_rebalance(&[]));
        assert!(!needs_rebalance(&[1000]));

        // Unsorted input still detects tight gaps
        assert!(needs_rebalance(&[1001, 1000])); // sorted: [1000, 1001]
    }

    #[test]
    fn needs_rebalance_healthy_gaps() {
        // Typical well-spaced sequence
        assert!(!needs_rebalance(&[1000, 2000, 3000, 4000]));

        // Even after many inserts (simulated with manual values)
        assert!(!needs_rebalance(&[1000, 1500, 2000, 2500, 3000]));
    }
}
