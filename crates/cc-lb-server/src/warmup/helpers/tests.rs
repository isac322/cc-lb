use super::*;
use std::collections::HashSet;

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
