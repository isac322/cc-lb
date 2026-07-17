use crate::planner::{PlanInput, Profile, plan};
use crate::replay::{ReplayError, verify};
use crate::traffic::ExpectedLabel;
use crate::verdict::Verdict;

#[test]
fn manifest_determinism() {
    // Given: equivalent planning inputs with different informational timestamps.
    let first = plan(PlanInput::new(123, Profile::Smoke, 1_000)).expect("first plan builds");
    let second = plan(PlanInput::new(123, Profile::Smoke, 2_000)).expect("second plan builds");
    let different_seed =
        plan(PlanInput::new(456, Profile::Smoke, 1_000)).expect("different-seed plan builds");

    // When: the determinism boundary removes the generated timestamp.
    let first_json = first
        .normalized_canonical_json()
        .expect("first manifest normalizes");
    let second_json = second
        .normalized_canonical_json()
        .expect("second manifest normalizes");

    // Then: equivalent inputs are byte-identical and a different seed changes the schedule.
    assert_eq!(first_json, second_json);
    assert_ne!(first.schedule_hash, different_seed.schedule_hash);
}

#[test]
fn replay_refuses_tampered() {
    // Given: a materialized manifest with a valid integrity hash.
    let mut manifest = plan(PlanInput::new(123, Profile::Smoke, 1_000)).expect("plan builds");

    // When: a replay input changes an expected request label without resigning it.
    manifest.requests[0].expected_label = ExpectedLabel::ProviderError;
    let result = verify(&manifest);

    // Then: replay identifies the integrity boundary rather than accepting the altered plan.
    assert!(matches!(result, Err(ReplayError::Integrity { .. })));
}

#[test]
fn verdict_enum_roundtrip() {
    // Given: every evidence verdict value.
    let verdicts = [
        Verdict::Pass,
        Verdict::Fail,
        Verdict::Blocked,
        Verdict::NotComparable,
    ];

    // When: verdicts cross the JSON boundary.
    let json = serde_json::to_string(&verdicts).expect("verdicts serialize");
    let decoded: Vec<Verdict> = serde_json::from_str(&json).expect("verdicts parse");

    // Then: their stable, machine-consumed spellings survive unchanged.
    assert_eq!(json, "[\"PASS\",\"FAIL\",\"BLOCKED\",\"NOT_COMPARABLE\"]");
    assert_eq!(decoded, verdicts);
}
