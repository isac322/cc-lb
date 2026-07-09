use cc_lb_storage_api::{
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaObservationRecord,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSemanticFingerprint, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use uuid::Uuid;

fn semantic_checkpoint_base_record() -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id: Uuid::from_u128(1),
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: 1_700_000_000_000,
        sample_id: Uuid::from_u128(2),
        utilization: Some(0.5),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(1_700_018_000),
        surpassed_threshold: Some(0.8),
        representative_claim: Some("claim-a".to_owned()),
        fallback_percentage: Some(0.2),
        fallback_available: Some(true),
        overage_in_use: Some(false),
        overage_period_monthly_utilization: Some(0.3),
        upgrade_paths: Some(vec!["team".to_owned(), "enterprise".to_owned()]),
        disabled_reason: Some("none".to_owned()),
        extra_usage_enabled: Some(true),
        extra_usage_monthly_limit: Some(10_000.0),
        extra_usage_used_credits: Some(123.0),
        ingested_at_unix_millis: 1_700_000_000_100,
    }
}

fn semantic_checkpoint_fingerprint(
    record: &SubscriptionQuotaObservationRecord,
) -> SubscriptionQuotaSemanticFingerprint {
    SubscriptionQuotaSemanticFingerprint::from_observation(record)
}

#[test]
fn semantic_checkpoint_ignores_runtime_and_evidence_fields() {
    // Given: two observations with the same quota state but different sample evidence.
    let base = semantic_checkpoint_base_record();
    let mut noisy = base.clone();
    noisy.sample_kind = SubscriptionQuotaSampleKind::ProcessStart;
    noisy.observed_at_unix_millis += 60_000;
    noisy.ingested_at_unix_millis += 90_000;
    noisy.sample_id = Uuid::from_u128(3);
    noisy.representative_claim = Some("claim-b".to_owned());

    // When: both records are reduced to semantic checkpoint fingerprints.
    let base_fingerprint = semantic_checkpoint_fingerprint(&base);
    let noisy_fingerprint = semantic_checkpoint_fingerprint(&noisy);

    // Then: runtime/evidence-only changes do not change checkpoint identity.
    assert_eq!(base_fingerprint, noisy_fingerprint);
}

#[test]
fn semantic_checkpoint_uses_stable_expected_fingerprint() {
    // Given: a representative observation fixture for the semantic key format.
    let base = semantic_checkpoint_base_record();

    // When: the fixture is reduced to a persisted semantic fingerprint.
    let fingerprint = semantic_checkpoint_fingerprint(&base);

    // Then: the bytes stay pinned to the stable serialization and BLAKE3 hash.
    let expected = [
        212, 219, 0, 145, 156, 73, 55, 248, 104, 141, 74, 128, 82, 42, 172, 250, 97, 70, 216, 154,
        79, 146, 106, 68, 241, 11, 241, 47, 154, 152, 176, 3,
    ];
    assert_eq!(fingerprint.as_bytes(), &expected);
}

#[test]
fn semantic_checkpoint_changes_for_each_quota_semantic_field() {
    // Given: a fully populated quota observation.
    let base = semantic_checkpoint_base_record();
    let base_fingerprint = semantic_checkpoint_fingerprint(&base);
    let mut changed_records = Vec::new();

    let mut changed = base.clone();
    changed.utilization = Some(0.6);
    changed_records.push(("utilization", changed));

    let mut changed = base.clone();
    changed.status = Some(SubscriptionQuotaStatus::AllowedWarning);
    changed_records.push(("status", changed));

    let mut changed = base.clone();
    changed.resets_at_unix_secs = Some(1_700_019_000);
    changed_records.push(("resets_at_unix_secs", changed));

    let mut changed = base.clone();
    changed.surpassed_threshold = Some(0.9);
    changed_records.push(("surpassed_threshold", changed));

    let mut changed = base.clone();
    changed.fallback_percentage = Some(0.25);
    changed_records.push(("fallback_percentage", changed));

    let mut changed = base.clone();
    changed.fallback_available = Some(false);
    changed_records.push(("fallback_available", changed));

    let mut changed = base.clone();
    changed.overage_in_use = Some(true);
    changed_records.push(("overage_in_use", changed));

    let mut changed = base.clone();
    changed.overage_period_monthly_utilization = Some(0.35);
    changed_records.push(("overage_period_monthly_utilization", changed));

    let mut changed = base.clone();
    changed.upgrade_paths = Some(vec!["enterprise".to_owned()]);
    changed_records.push(("upgrade_paths", changed));

    let mut changed = base.clone();
    changed.disabled_reason = Some("quota_disabled".to_owned());
    changed_records.push(("disabled_reason", changed));

    let mut changed = base.clone();
    changed.extra_usage_enabled = Some(false);
    changed_records.push(("extra_usage_enabled", changed));

    let mut changed = base.clone();
    changed.extra_usage_monthly_limit = Some(20_000.0);
    changed_records.push(("extra_usage_monthly_limit", changed));

    let mut changed = base.clone();
    changed.extra_usage_used_credits = Some(124.0);
    changed_records.push(("extra_usage_used_credits", changed));

    // When/Then: every quota-semantic field changes checkpoint identity.
    for (field, changed) in changed_records {
        assert_ne!(
            base_fingerprint,
            semantic_checkpoint_fingerprint(&changed),
            "{field} must be part of checkpoint identity"
        );
    }
}

#[test]
fn semantic_checkpoint_distinguishes_exact_f64_changes_without_tolerance() {
    // Given: observations whose utilization differs by the smallest larger f64 bit pattern.
    let base = semantic_checkpoint_base_record();
    let mut tiny_change = base.clone();
    tiny_change.utilization = Some(f64::from_bits(0.5f64.to_bits() + 1));

    let mut decrease = base.clone();
    decrease.utilization = Some(0.4);

    // When: semantic checkpoint identities are computed.
    let base_fingerprint = semantic_checkpoint_fingerprint(&base);
    let tiny_change_fingerprint = semantic_checkpoint_fingerprint(&tiny_change);
    let decrease_fingerprint = semantic_checkpoint_fingerprint(&decrease);

    // Then: exact f64 bits are used; there is no tolerance or monotonic filtering.
    assert_ne!(base_fingerprint, tiny_change_fingerprint);
    assert_ne!(base_fingerprint, decrease_fingerprint);
}

#[test]
fn semantic_checkpoint_record_preserves_source_and_evidence_without_identity_collapse() {
    // Given: a header-sourced observation with evidence fields.
    let observation = semantic_checkpoint_base_record();

    // When: the observation is represented as a checkpoint record.
    let checkpoint = SubscriptionQuotaCheckpointRecord::from(&observation);

    // Then: source provenance and evidence survive outside semantic identity.
    assert_eq!(checkpoint.source, SubscriptionQuotaSource::Header);
    assert_eq!(checkpoint.sample_kind, observation.sample_kind);
    assert_eq!(checkpoint.sample_id, observation.sample_id);
    assert_eq!(
        checkpoint.representative_claim,
        observation.representative_claim
    );
    assert_eq!(
        checkpoint.changed_at_unix_millis,
        observation.observed_at_unix_millis
    );
    assert_eq!(
        checkpoint.semantic_fingerprint,
        observation.semantic_checkpoint_fingerprint()
    );
}
