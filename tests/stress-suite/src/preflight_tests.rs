use crate::fabric_state::{ResourceNames, cleanup_targets};
use crate::preflight_evidence::{CapabilityFailure, PreflightEvidence};
use crate::topology_decision::{BlockedStage, CleanupReceipt};
use crate::verdict::Verdict;

#[test]
fn preflight_verdicts() {
    // Given: each capability failure that can prevent Docker netem preflight.
    let cleanup = CleanupReceipt::empty();
    let cases = [
        CapabilityFailure::docker("Docker daemon is unreachable"),
        CapabilityFailure::tc("tc is unavailable in the netem helper"),
        CapabilityFailure::cap_net_admin("NET_ADMIN is unavailable"),
        CapabilityFailure::netem_seed("tc netem seed is unavailable"),
    ];

    // When: each failure is converted to preflight evidence.
    let evidence =
        cases.map(|failure| PreflightEvidence::capability_blocked("pf2", failure, cleanup.clone()));

    // Then: capability failures are explicit BLOCKED evidence with no fabric claim.
    for item in evidence {
        assert_eq!(item.verdict, Verdict::Blocked);
        assert_eq!(item.blocked_stage, Some(BlockedStage::Preflight));
        assert!(
            item.blocked_reason
                .as_deref()
                .is_some_and(|reason| !reason.is_empty())
        );
        assert!(item.chosen_fabric.is_none());
        assert!(item.mechanic.is_none());
        assert!(item.rendered_and_installed_edges.is_empty());
        assert!(item.cleanup.attempted);
        assert!(item.validate().is_ok());
    }
}

#[test]
fn cleanup_by_label() {
    // Given: a run ID and all resources managed by its private Docker fabric.
    let names = ResourceNames::new("pf2").expect("valid resource names");

    // When: the label-scoped cleanup target list is built.
    let targets = cleanup_targets(&names, "pf2");

    // Then: Docker lookup uses the run label and every named resource remains targeted.
    assert_eq!(targets.label, "com.cc-lb.stress.run=pf2");
    assert_eq!(targets.network, "ccstress_pf2");
    assert_eq!(targets.helper_image, "cc-lb-stress-netem:pf2");
    assert_eq!(targets.containers.len(), 6);
    assert!(
        targets
            .containers
            .contains(&"ccstress_pf2_probe".to_owned())
    );
    assert!(
        targets
            .containers
            .contains(&"ccstress_pf2_netem_cc_lb".to_owned())
    );
}

#[test]
fn t1_chaos_observation_requires_150ms_delta() {
    // Given: a control request duration and two chaos request durations.
    let control_ms = 100;

    // When: the latency delta falls below and reaches the T1 observation threshold.
    let below_threshold = crate::t1_chaos::chaos_observed(control_ms, 249);
    let at_threshold = crate::t1_chaos::chaos_observed(control_ms, 250);

    // Then: only the required 150ms delta is reported as observed.
    assert!(!below_threshold);
    assert!(at_threshold);
}
