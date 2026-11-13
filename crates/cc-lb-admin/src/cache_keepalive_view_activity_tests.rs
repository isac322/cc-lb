use std::collections::{BTreeMap, HashMap};

use cc_lb_pricing::PriceCatalog;
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion};
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveEnqueueState, CacheKeepaliveEvidenceRecord,
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem, CacheKeepaliveSessionRecord,
    CacheKeepaliveSessionStatus, CacheKeepaliveTerminalReason, CacheKeepaliveTurnRecord, CacheTtl,
};
use uuid::Uuid;

use crate::v1::principal_cache_keepalive::view::{
    CacheKeepaliveActivityBatch, activity_for_item, row_from_activity,
};

use super::status::CacheKeepaliveViewState;
use super::{CacheKeepaliveActivitySource, derive_activity_view};

fn item(source: CacheKeepaliveSessionEntrySource) -> CacheKeepaliveSessionListItem {
    CacheKeepaliveSessionListItem {
        id: "decision-only".to_owned(),
        source,
        session_key_hash: matches!(source, CacheKeepaliveSessionEntrySource::Session)
            .then(|| "session".to_owned()),
        cycle_id: None,
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::nil(),
        last_message_at_ms: 1_730_000_000_000,
        ttl: CacheTtl::Ttl5m,
        generation: 42,
        refresh_count: None,
        has_renewal_in_horizon: false,
        status: None,
        enqueue_state: None,
        terminal_reason: None,
        cancel_reason: None,
        request_incomplete: false,
        decision: Some("not_tracked".to_owned()),
        reason: "user turn (stop_reason=end_turn)".to_owned(),
        display_reason: None,
        error: None,
        config_snapshot: None,
    }
}

fn turn(source_ref_id: &str, ts: u64) -> CacheKeepaliveTurnRecord {
    CacheKeepaliveTurnRecord {
        source_ref_id: source_ref_id.to_owned(),
        session_key_hash: "session".to_owned(),
        principal_id: "principal".to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::nil(),
        cycle_id: None,
        model: "claude-sonnet-4-5".to_owned(),
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_creation_input_tokens_5m: 0,
        cache_creation_input_tokens_1h: 0,
        cache_read_input_tokens: 20_000,
        cost_micros: Some(0),
        hit_miss: "hit".to_owned(),
        ts,
    }
}

fn session() -> CacheKeepaliveSessionRecord {
    CacheKeepaliveSessionRecord {
        session_key_hash: "session".to_owned(),
        principal_id: "principal".to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::nil(),
        cycle_id: None,
        generation: 42,
        refresh_count: 0,
        first_scheduled_at_unix_secs: 1_730_000_000,
        cache_anchor_at_unix_secs: 1_730_000_000,
        last_message_at_ms: 1_730_000_000_000,
        run_at_unix_secs: 1_730_000_200,
        ttl: CacheTtl::Ttl5m,
        status: CacheKeepaliveSessionStatus::Active,
        enqueue_state: CacheKeepaliveEnqueueState::Pending,
        running_since_unix_secs: None,
        current_job_key: "cache_keepalive:session:42".to_owned(),
        encrypted_payload: b"must-not-be-loaded-for-list-summary".to_vec(),
        display_reason: "agent-in-turn".to_owned(),
        error: None,
        config_snapshot: None,
        terminal_reason: None,
        cancel_reason: None,
        expires_at_unix_secs: 1_730_000_300,
        created_at_unix_secs: 1_730_000_000,
        updated_at_unix_secs: 1_730_000_000,
    }
}

fn priced_catalog() -> std::sync::Arc<PriceCatalog> {
    let catalog = PriceCatalog::new_empty();
    let mut models = HashMap::new();
    models.insert(
        "claude-sonnet-4-5".to_owned(),
        Pricing {
            native: None,
            model: "claude-sonnet-4-5".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
        },
    );
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd.insert(
        "claude-sonnet-4-5".to_owned(),
        UsdPerMillion::from_micros_usd(3_750_000),
    );
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(
        "claude-sonnet-4-5".to_owned(),
        UsdPerMillion::from_micros_usd(300_000),
    );
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "activity-fixture".to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    });
    catalog
}

fn renewal_evidence(
    source_ref_id: &str,
    cycle_id: Uuid,
    request_start_ms: u64,
    ttl: CacheTtl,
    hit_miss: &str,
    prefix_hash: &str,
) -> CacheKeepaliveEvidenceRecord {
    CacheKeepaliveEvidenceRecord {
        evidence_id: format!("{source_ref_id}-evidence"),
        cycle_id: Some(cycle_id),
        evidence_kind: "renewal".to_owned(),
        request_start_ms,
        prefix_fingerprint: Some(format!("{source_ref_id}-fingerprint")),
        prefix_hash: Some(prefix_hash.to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::nil(),
        model: "claude-sonnet-4-5".to_owned(),
        ttl,
        hit_miss: Some(hit_miss.to_owned()),
        cache_read_input_tokens: Some(20_000),
        cost_micros: Some(0),
        request_ref: Some(format!("{source_ref_id}-request")),
        event_ref: Some(source_ref_id.to_owned()),
        session_key_hash: None,
        cycle_anchor_ms: Some(
            request_start_ms.saturating_sub(
                match ttl {
                    CacheTtl::Ttl5m | CacheTtl::Ttl30m => ttl.as_secs().saturating_sub(30),
                    CacheTtl::Ttl1h | CacheTtl::Ttl12h => ttl.as_secs().saturating_sub(300),
                }
                .saturating_mul(1_000),
            ),
        ),
    }
}

fn follow_up_evidence(
    request_start_ms: u64,
    ttl: CacheTtl,
    prefix_hash: &str,
) -> CacheKeepaliveEvidenceRecord {
    CacheKeepaliveEvidenceRecord {
        evidence_id: "follow-up-evidence".to_owned(),
        cycle_id: None,
        evidence_kind: "follow_up".to_owned(),
        request_start_ms,
        prefix_fingerprint: Some("follow-up-fingerprint".to_owned()),
        prefix_hash: Some(prefix_hash.to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::nil(),
        model: "claude-sonnet-4-5".to_owned(),
        ttl,
        hit_miss: Some("hit".to_owned()),
        cache_read_input_tokens: Some(20_000),
        cost_micros: Some(0),
        request_ref: Some("follow-up-request".to_owned()),
        event_ref: Some("follow-up".to_owned()),
        session_key_hash: None,
        cycle_anchor_ms: None,
    }
}

#[test]
fn unowned_legacy_groups_do_not_inherit_an_unidentified_current_schedule() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.last_message_at_ms = 1_000_000;
    let turns = [
        turn("legacy-a", 100),
        turn("legacy-b", 500),
        turn("legacy-c", 900),
    ];
    let mut scheduled = session();
    scheduled.cache_anchor_at_unix_secs = 1_000;
    scheduled.run_at_unix_secs = 1_270;
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: Some(&scheduled),
            turns: &turns,
            evidence: &[],
            now_ms: 1_010_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(
        activity
            .gaps
            .iter()
            .filter(|gap| { !gap.pulses.is_empty() && gap.ending_kind == "scheduled_waiting" })
            .count(),
        0,
        "unidentified history cannot inherit the current run_at"
    );
    assert_eq!(activity.total_renewals, 0);
    assert!(!activity.is_last_pending);
    assert!(activity.turns.iter().all(|turn| !turn.pending));
    assert_eq!(activity.retained_renewals, 3);
    assert_eq!(activity.gaps.len(), 4);
    assert_eq!(
        activity
            .gaps
            .iter()
            .filter(|gap| gap.pulses.is_empty() && gap.ending_kind == "scheduled_waiting")
            .count(),
        1
    );
}

#[test]
fn unowned_legacy_turns_never_displace_owned_current_cycle() {
    let cycle = Uuid::from_u128(78);
    let catalog = priced_catalog();
    for status in [
        CacheKeepaliveSessionStatus::Active,
        CacheKeepaliveSessionStatus::InRequest,
    ] {
        for owned_turn_cycle in [Some(cycle), None] {
            let mut item = item(CacheKeepaliveSessionEntrySource::Session);
            item.status = Some(status);
            item.cycle_id = Some(cycle);
            item.last_message_at_ms = 1_000_000;
            let mut owned = turn("owned", 1_000);
            owned.cycle_id = owned_turn_cycle;
            owned.cost_micros = Some(6_000);
            let turns = [
                owned,
                turn("unowned-before", 999),
                turn("unowned-boundary", 1_000),
                turn("unowned-after", 1_001),
            ];
            let evidence = [renewal_evidence(
                "owned",
                cycle,
                1_000_000,
                CacheTtl::Ttl5m,
                "hit",
                "owned-prefix",
            )];
            let activity = derive_activity_view(
                CacheKeepaliveActivitySource {
                    item: &item,
                    session: None,
                    turns: &turns,
                    evidence: &evidence,
                    now_ms: 1_050_000,
                    horizon_start_ms: None,
                },
                catalog.as_ref(),
            );
            assert_eq!(
                activity.total_renewals, 1,
                "status={status:?} owned={owned_turn_cycle:?}"
            );
            assert_eq!(activity.retained_renewals, 4);
            assert_eq!(activity.turns.len(), 4);
            assert!(activity.is_last_pending);
            assert!(
                activity
                    .turns
                    .iter()
                    .find(|turn| turn.source_ref_id == "owned")
                    .unwrap()
                    .pending
            );
            assert!(
                activity
                    .turns
                    .iter()
                    .filter(|turn| turn.source_ref_id != "owned")
                    .all(|turn| !turn.pending)
            );
            assert!(
                activity
                    .gaps
                    .iter()
                    .filter(|gap| gap.cycle_id.is_none())
                    .all(|gap| gap.ending_kind == "followup_unknown")
            );
        }
    }
}

#[test]
fn paused_current_cycle_pending_evidence_is_not_renewal_scheduling() {
    let cycle_id = Uuid::from_u128(70);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.cycle_id = Some(cycle_id);
    item.status = Some(CacheKeepaliveSessionStatus::InRequest);
    item.refresh_count = Some(1);
    let mut record = turn("paused-paid", 1_730_000_100);
    record.cycle_id = Some(cycle_id);
    record.cost_micros = Some(6_000);
    let evidence = [renewal_evidence(
        "paused-paid",
        cycle_id,
        1_730_000_100_000,
        CacheTtl::Ttl5m,
        "hit",
        "prefix",
    )];
    let mut session = session();
    session.cycle_id = Some(cycle_id);
    // A request marker's scheduling fields are not cache or request deadlines.
    session.run_at_unix_secs = 1_730_000_150;
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: Some(&session),
            turns: &[record],
            evidence: &evidence,
            now_ms: 1_730_000_200_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(activity.gaps[0].ending_kind, "followup_pending");
    assert_eq!(activity.gaps[0].ending_at_ms, 1_730_000_200_000);
    assert_eq!(activity.evidence_status, "pending");
    assert!(activity.is_last_pending);
    assert!(activity.turns[0].pending);
    assert!(!activity.is_active);
    assert_eq!(activity.next_renewal, None);
    assert_eq!(activity.total_spent_micros, 6_000);
    assert_eq!(activity.total_avoided_micros, Some(0));
}

#[test]
fn overdue_active_paid_renewal_is_not_pending_but_stays_scheduled() {
    let cycle = Uuid::from_u128(76);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.cycle_id = Some(cycle);
    item.refresh_count = Some(1);
    let mut paid = turn("overdue-active", 1_730_000_100);
    paid.cycle_id = Some(cycle);
    paid.cost_micros = Some(6_000);
    let evidence = [renewal_evidence(
        "overdue-active",
        cycle,
        1_730_000_100_000,
        CacheTtl::Ttl5m,
        "hit",
        "prefix",
    )];
    let mut session = session();
    session.cycle_id = Some(cycle);
    session.run_at_unix_secs = 1_730_000_370;
    let derive = |now_ms, evidence: &[CacheKeepaliveEvidenceRecord]| {
        derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: Some(&session),
                turns: std::slice::from_ref(&paid),
                evidence,
                now_ms,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        )
    };
    let activity = derive(1_730_000_400_000, &evidence);
    assert_eq!(activity.evidence_status, "observed");
    assert!(!activity.is_last_pending);
    assert!(!activity.turns[0].pending);
    assert_eq!(activity.gaps[0].ending_kind, "ttl_expired_no_followup");
    assert_eq!(activity.gaps[0].ending_at_ms, 1_730_000_400_000);
    assert!(activity.is_active);
    assert!(activity.next_renewal.is_some());
    assert_eq!(activity.total_spent_micros, 6_000);
    let live = derive(1_730_000_399_999, &evidence);
    assert!(live.is_last_pending);
    assert_eq!(live.evidence_status, "pending");
    assert_eq!(live.gaps[0].ending_kind, "scheduled_waiting");
    let overdue = derive(1_730_000_500_000, &evidence);
    assert!(!overdue.is_last_pending);
    assert_eq!(overdue.gaps[0].ending_at_ms, 1_730_000_400_000);
    let mut observed_evidence = evidence.to_vec();
    observed_evidence.push(follow_up_evidence(
        1_730_000_400_000,
        CacheTtl::Ttl5m,
        "prefix",
    ));
    let observed = derive(1_730_000_500_000, &observed_evidence);
    assert_eq!(observed.evidence_status, "observed");
    assert!(observed.turns[0].followed_up);
    assert_eq!(observed.gaps[0].ending_kind, "followup_hit");
    let unknown = derive(1_730_000_500_000, &[]);
    assert_eq!(unknown.evidence_status, "unknown");
    assert_ne!(unknown.gaps[0].ending_kind, "ttl_expired_no_followup");
}

#[test]
fn paused_evidence_lifetime_uses_last_renewal_not_request_marker_or_first_pulse() {
    let current_cycle = Uuid::from_u128(71);
    let historical_cycle = Uuid::from_u128(72);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.cycle_id = Some(current_cycle);
    item.last_message_at_ms = 5_000_000;
    let mut first = turn("first", 1_000);
    first.cycle_id = Some(current_cycle);
    let mut last = turn("last", 1_270);
    last.cycle_id = Some(current_cycle);
    let mut historical = turn("historical", 500);
    historical.cycle_id = Some(historical_cycle);
    let turns = [first, last, historical];
    let renewals = vec![
        renewal_evidence(
            "first",
            current_cycle,
            1_000_000,
            CacheTtl::Ttl5m,
            "hit",
            "current",
        ),
        renewal_evidence(
            "last",
            current_cycle,
            1_270_000,
            CacheTtl::Ttl1h,
            "hit",
            "current",
        ),
        renewal_evidence(
            "historical",
            historical_cycle,
            500_000,
            CacheTtl::Ttl5m,
            "hit",
            "old",
        ),
    ];
    let catalog = priced_catalog();
    for incomplete in [false, true] {
        item.status = Some(if incomplete {
            CacheKeepaliveSessionStatus::Terminal
        } else {
            CacheKeepaliveSessionStatus::InRequest
        });
        item.request_incomplete = incomplete;
        item.terminal_reason = incomplete.then_some(CacheKeepaliveTerminalReason::Stale);
        for (now_ms, pending) in [(1_600_000, true), (4_869_999, true), (4_870_000, false)] {
            let activity = derive_activity_view(
                CacheKeepaliveActivitySource {
                    item: &item,
                    session: None,
                    turns: &turns,
                    evidence: &renewals,
                    now_ms,
                    horizon_start_ms: None,
                },
                catalog.as_ref(),
            );
            assert!(!activity.is_active);
            assert_eq!(activity.next_renewal, None);
            assert_eq!(
                activity.is_last_pending, pending,
                "incomplete={incomplete} now={now_ms}"
            );
            assert_eq!(
                activity.turns.iter().filter(|turn| turn.pending).count(),
                usize::from(pending)
            );
            assert_eq!(
                activity.evidence_status,
                if pending { "pending" } else { "observed" }
            );
            let current = activity
                .gaps
                .iter()
                .find(|gap| gap.cycle_id == Some(current_cycle))
                .unwrap();
            assert_eq!(
                current.ending_kind,
                if pending {
                    "followup_pending"
                } else {
                    "ttl_expired_no_followup"
                }
            );
            assert_eq!(
                current.ending_at_ms,
                if pending { now_ms } else { 4_870_000 }
            );
            let history = activity
                .gaps
                .iter()
                .find(|gap| gap.cycle_id == Some(historical_cycle))
                .unwrap();
            assert_eq!(history.ending_kind, "ttl_expired_no_followup");
            assert_eq!(history.ending_at_ms, 500_000);
            assert!(
                !activity
                    .turns
                    .iter()
                    .find(|turn| turn.cycle_id == Some(historical_cycle))
                    .unwrap()
                    .pending
            );
        }
        let mut observed_evidence = renewals.clone();
        observed_evidence.push(follow_up_evidence(1_600_000, CacheTtl::Ttl1h, "current"));
        let observed = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: &turns,
                evidence: &observed_evidence,
                now_ms: 1_700_000,
                horizon_start_ms: None,
            },
            catalog.as_ref(),
        );
        assert!(!observed.is_last_pending);
        assert!(observed.turns[0].followed_up);
        assert_eq!(observed.evidence_status, "observed");
        assert_eq!(
            observed
                .gaps
                .iter()
                .find(|gap| gap.cycle_id == Some(current_cycle))
                .unwrap()
                .ending_kind,
            "followup_hit"
        );
    }
}

#[test]
fn paused_empty_cycle_does_not_borrow_history_or_fabricate_pending_evidence() {
    let current_cycle = Uuid::from_u128(73);
    let historical_cycle = Uuid::from_u128(74);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::InRequest);
    item.cycle_id = Some(current_cycle);
    item.last_message_at_ms = 1_000_000;
    let mut old = turn("old", 800);
    old.cycle_id = Some(historical_cycle);
    let legacy = turn("legacy-before-request", 900);
    let evidence = [renewal_evidence(
        "old",
        historical_cycle,
        800_000,
        CacheTtl::Ttl5m,
        "hit",
        "old",
    )];
    for turns in [&[][..], &[old, legacy][..]] {
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns,
                evidence: &evidence,
                now_ms: 1_010_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        assert_eq!(activity.total_renewals, 0);
        assert!(!activity.is_last_pending);
        assert!(!activity.is_active);
        assert_eq!(activity.next_renewal, None);
        assert!(activity.turns.iter().all(|turn| !turn.pending));
        assert!(
            activity
                .gaps
                .iter()
                .all(|gap| gap.ending_kind != "followup_pending"
                    && gap.ending_kind != "scheduled_waiting")
        );
    }
}

#[test]
fn paused_legacy_turn_requires_current_cycle_evidence_and_real_terminals_end_pending() {
    let cycle_id = Uuid::from_u128(75);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.cycle_id = Some(cycle_id);
    item.last_message_at_ms = 2_000_000;
    item.status = Some(CacheKeepaliveSessionStatus::InRequest);
    let legacy = turn("legacy-current", 1_900);
    let evidence = [renewal_evidence(
        "legacy-current",
        cycle_id,
        1_900_000,
        CacheTtl::Ttl5m,
        "hit",
        "current",
    )];
    let derive = |item: &CacheKeepaliveSessionListItem,
                  evidence: &[CacheKeepaliveEvidenceRecord]| {
        derive_activity_view(
            CacheKeepaliveActivitySource {
                item,
                session: None,
                turns: std::slice::from_ref(&legacy),
                evidence,
                now_ms: 2_010_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        )
    };
    let pending = derive(&item, &evidence);
    assert_eq!(pending.total_renewals, 1);
    assert!(pending.is_last_pending);
    assert!(pending.turns[0].pending);
    assert_eq!(pending.gaps[0].ending_kind, "followup_pending");
    let unknown = derive(&item, &[]);
    assert_eq!(
        unknown.total_renewals, 0,
        "unowned pre-request legacy history"
    );
    assert!(!unknown.is_last_pending);
    assert_eq!(unknown.evidence_status, "unknown");
    assert_eq!(unknown.gaps[0].ending_kind, "followup_unknown");
    for reason in [
        CacheKeepaliveTerminalReason::Cancelled,
        CacheKeepaliveTerminalReason::MaxRefreshes,
        CacheKeepaliveTerminalReason::MaxDuration,
        CacheKeepaliveTerminalReason::DispatchError,
    ] {
        item.status = Some(CacheKeepaliveSessionStatus::Terminal);
        item.terminal_reason = Some(reason);
        let terminal = derive(&item, &evidence);
        assert!(!terminal.is_last_pending);
        assert!(!terminal.turns[0].pending);
        assert_ne!(terminal.gaps[0].ending_kind, "followup_pending");
    }
}

#[test]
fn cache_keepalive_activity_orders_turns_newest_first_without_fabricating_decision_events() {
    // Given: a decision-only not-tracked row and unordered renewal turn records.
    let decision = item(CacheKeepaliveSessionEntrySource::Decision);
    let session = item(CacheKeepaliveSessionEntrySource::Session);
    let turns = [turn("older", 100), turn("newer", 200)];
    let catalog = PriceCatalog::new_empty();

    // When: the server derives view models for both persisted row sources.
    let decision_view = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &decision,
            session: None,
            turns: &[],
            evidence: &[],
            now_ms: 0,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );
    let session_view = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session,
            session: None,
            turns: &turns,
            evidence: &[],
            now_ms: 0,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );

    // Then: decisions stay event-free, numeric generations survive, and detail is newest first.
    assert_eq!(decision_view.state, CacheKeepaliveViewState::NotTracked);
    assert_eq!(decision_view.generation, 42);
    assert_eq!(decision_view.turns, []);
    assert_eq!(decision_view.pnl.map(|pnl| pnl.net_micros), Some(0));
    assert_eq!(decision_view.net_pnl_display, "$0.00");
    assert_eq!(session_view.turns[0].source_ref_id, "newer");
    assert_eq!(session_view.turns[1].source_ref_id, "older");
}

#[test]
fn batched_cache_keepalive_turns_preserve_serialized_row_behavior() {
    // Given: the same list item and turn records as the legacy per-session row path.
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let turns = vec![turn("older", 100), turn("newer", 200)];
    let catalog = PriceCatalog::new_empty();
    let session = session();
    let legacy_activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: Some(&session),
            turns: &turns,
            evidence: &[],
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );

    // When: the list row consumes those records from the shared batch without the encrypted session.
    let batch = CacheKeepaliveActivityBatch::from_turns(turns, Vec::new());
    let batched_activity = activity_for_item(
        catalog.as_ref(),
        1_730_000_100_000,
        &item,
        None,
        &batch,
        None,
    )
    .expect("batched activity");
    let legacy_row = row_from_activity(12, &item, &legacy_activity, Some("primary".to_owned()))
        .expect("legacy row");
    let batched_row = row_from_activity(12, &item, &batched_activity, Some("primary".to_owned()))
        .expect("batched row");

    // Then: every serialized field remains byte-for-byte identical.
    assert_eq!(
        serde_json::to_vec(&batched_row).expect("serialize batched row"),
        serde_json::to_vec(&legacy_row).expect("serialize legacy row"),
    );
}

#[test]
fn mixed_activity_ties_order_by_source_ref_id_ascending_and_only_newest_active_turn_pending() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.cycle_id = Some(Uuid::from_u128(79));
    let mut turns = [
        turn("z-turn", 200),
        turn("a-turn", 200),
        turn("older-turn", 199),
    ];
    for turn in &mut turns {
        turn.cycle_id = item.cycle_id;
    }
    let catalog = PriceCatalog::new_empty();

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            evidence: &[],
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );

    let ordered_ids = activity
        .turns
        .iter()
        .map(|turn| turn.source_ref_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ordered_ids, ["a-turn", "z-turn", "older-turn"]);
    assert!(activity.turns[0].pending);
    assert!(activity.turns[1..].iter().all(|turn| !turn.pending));
    assert_eq!(
        activity
            .turns
            .iter()
            .map(|turn| turn.turn_number)
            .collect::<Vec<_>>(),
        [2, 3, 1]
    );
}
#[test]
fn cache_keepalive_hit_without_followup_evidence_is_not_counted_as_followed_up() {
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let turns = [turn("renewal-without-followup", 200)];
    let catalog = PriceCatalog::new_empty();

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            evidence: &[],
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );

    assert!(!activity.turns[0].followed_up);
}
#[test]
fn cache_keepalive_activity_requires_matching_followup_evidence() {
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let turns = [turn("renewal-with-followup", 200)];
    let evidence = [
        CacheKeepaliveEvidenceRecord {
            evidence_id: "renewal-evidence".to_owned(),
            cycle_id: Some(Uuid::from_u128(1)),
            evidence_kind: "renewal".to_owned(),
            request_start_ms: 200_000,
            prefix_fingerprint: Some("fp".to_owned()),
            prefix_hash: Some("hash".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::nil(),
            model: "claude-sonnet-4-5".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(20_000),
            cost_micros: Some(1),
            request_ref: Some("request".to_owned()),
            event_ref: Some("renewal-with-followup".to_owned()),
            session_key_hash: None,
            cycle_anchor_ms: Some(0),
        },
        CacheKeepaliveEvidenceRecord {
            evidence_id: "followup-evidence".to_owned(),
            cycle_id: None,
            evidence_kind: "follow_up".to_owned(),
            request_start_ms: 320_000,
            prefix_fingerprint: Some("extended-conversation-fp".to_owned()),
            prefix_hash: Some("hash".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::nil(),
            model: "claude-sonnet-4-5".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(20_000),
            cost_micros: Some(2),
            request_ref: Some("followup".to_owned()),
            event_ref: Some("followup".to_owned()),
            session_key_hash: None,
            cycle_anchor_ms: None,
        },
    ];
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        PriceCatalog::new_empty().as_ref(),
    );
    assert!(activity.turns[0].followed_up);
    assert_eq!(activity.evidence_status, "observed");
}
#[test]
fn cache_keepalive_activity_credits_only_the_earliest_matching_renewal_in_a_cycle() {
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let turns = [turn("renewal-a", 200), turn("renewal-b", 240)];
    let evidence = [
        CacheKeepaliveEvidenceRecord {
            evidence_id: "renewal-a-evidence".to_owned(),
            cycle_id: Some(Uuid::from_u128(1)),
            evidence_kind: "renewal".to_owned(),
            request_start_ms: 200_000,
            prefix_fingerprint: Some("a".to_owned()),
            prefix_hash: Some("base-prefix".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::nil(),
            model: "claude-sonnet-4-5".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(20_000),
            cost_micros: Some(1),
            request_ref: Some("request-a".to_owned()),
            event_ref: Some("renewal-a".to_owned()),
            session_key_hash: None,
            cycle_anchor_ms: Some(0),
        },
        CacheKeepaliveEvidenceRecord {
            evidence_id: "renewal-b-evidence".to_owned(),
            cycle_id: Some(Uuid::from_u128(1)),
            evidence_kind: "renewal".to_owned(),
            request_start_ms: 240_000,
            prefix_fingerprint: Some("b".to_owned()),
            prefix_hash: Some("base-prefix".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::nil(),
            model: "claude-sonnet-4-5".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(20_000),
            cost_micros: Some(1),
            request_ref: Some("request-b".to_owned()),
            event_ref: Some("renewal-b".to_owned()),
            session_key_hash: None,
            cycle_anchor_ms: Some(0),
        },
        CacheKeepaliveEvidenceRecord {
            evidence_id: "followup-extension".to_owned(),
            cycle_id: None,
            evidence_kind: "follow_up".to_owned(),
            request_start_ms: 320_000,
            prefix_fingerprint: Some("new-extended-request".to_owned()),
            prefix_hash: Some("base-prefix".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::nil(),
            model: "claude-sonnet-4-5".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(20_000),
            cost_micros: Some(2),
            request_ref: Some("followup".to_owned()),
            event_ref: Some("followup".to_owned()),
            session_key_hash: None,
            cycle_anchor_ms: None,
        },
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        PriceCatalog::new_empty().as_ref(),
    );

    assert_eq!(activity.evidence_status, "observed");
    assert_eq!(activity.turns[0].source_ref_id, "renewal-b");
    assert!(!activity.turns[0].followed_up);
    assert_eq!(activity.turns[1].source_ref_id, "renewal-a");
    assert!(activity.turns[1].followed_up);
}
#[test]
fn active_same_cycle_miss_followup_resolves_pending_without_miss_credit() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    let cycle_id = Uuid::from_u128(2);
    item.cycle_id = Some(cycle_id);

    let mut hit = turn("same-cycle-hit", 200);
    hit.cycle_id = Some(cycle_id);
    hit.cost_micros = Some(6_000);

    let mut miss = turn("same-cycle-miss", 201);
    miss.cycle_id = Some(cycle_id);
    miss.hit_miss = "miss".to_owned();
    miss.cost_micros = Some(15_000);

    let mut miss_follow_up = follow_up_evidence(202_000, CacheTtl::Ttl5m, "shared-prefix");
    miss_follow_up.evidence_id = "same-cycle-miss-follow-up".to_owned();
    let evidence = [
        renewal_evidence(
            "same-cycle-hit",
            cycle_id,
            200_000,
            CacheTtl::Ttl5m,
            "hit",
            "shared-prefix",
        ),
        renewal_evidence(
            "same-cycle-miss",
            cycle_id,
            201_000,
            CacheTtl::Ttl5m,
            "miss",
            "shared-prefix",
        ),
        miss_follow_up,
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[hit, miss],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    assert_eq!(activity.evidence_status, "observed");
    assert_eq!(activity.total_renewals, 1);
    assert_eq!(activity.total_avoided_micros, Some(0));
    assert!(!activity.is_last_pending);
    assert_eq!(activity.turns[0].source_ref_id, "same-cycle-miss");
    assert!(!activity.turns[0].pending);
    assert!(!activity.turns[0].followed_up);
    assert_eq!(
        activity.turns[0].pnl.as_ref().map(|pnl| pnl.net_micros),
        Some(-15_000)
    );
    assert_eq!(activity.turns[1].source_ref_id, "same-cycle-hit");
    assert!(!activity.turns[1].followed_up);
    assert_eq!(
        activity.turns[1].pnl.as_ref().map(|pnl| pnl.net_micros),
        Some(-6_000)
    );
}

#[test]
fn terminal_complete_negative_evidence_is_observed_without_follow_up() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    item.terminal_reason = Some(CacheKeepaliveTerminalReason::MaxRefreshes);
    item.refresh_count = Some(12);

    let mut hit = turn("terminal-hit", 200);
    hit.cost_micros = Some(6_000);
    let mut miss = turn("terminal-miss", 100);
    miss.cost_micros = Some(15_000);
    miss.hit_miss = "miss".to_owned();
    let cycle_id = Uuid::from_u128(10);
    let evidence = [
        renewal_evidence(
            "terminal-hit",
            cycle_id,
            200_000,
            CacheTtl::Ttl5m,
            "hit",
            "terminal",
        ),
        renewal_evidence(
            "terminal-miss",
            cycle_id,
            210_000,
            CacheTtl::Ttl5m,
            "miss",
            "terminal",
        ),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[hit, miss],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    let pnl = activity.pnl.expect("priced terminal activity");
    assert_eq!(pnl.spent_micros, 21_000);
    assert_eq!(pnl.avoided_micros, 0);
    assert_eq!(pnl.net_micros, -21_000);
    assert_eq!(activity.total_spent_micros, 21_000);
    assert_eq!(activity.total_avoided_micros, Some(0));
    assert_eq!(activity.evidence_status, "observed");
    assert_eq!(activity.total_renewals, 1);
    assert!(
        activity
            .turns
            .iter()
            .all(|turn| !turn.pending && !turn.followed_up)
    );
}

#[test]
fn terminal_complete_hit_only_evidence_is_observed_without_avoided_credit() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    item.terminal_reason = Some(CacheKeepaliveTerminalReason::MaxRefreshes);
    item.refresh_count = Some(12);

    let mut hit = turn("terminal-hit-only", 200);
    hit.cost_micros = Some(6_000);
    let evidence = [renewal_evidence(
        "terminal-hit-only",
        Uuid::from_u128(11),
        200_000,
        CacheTtl::Ttl5m,
        "hit",
        "terminal-hit-only",
    )];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[hit],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    let pnl = activity.pnl.expect("priced terminal hit-only activity");
    assert_eq!(pnl.spent_micros, 6_000);
    assert_eq!(pnl.avoided_micros, 0);
    assert_eq!(pnl.net_micros, -6_000);
    assert_eq!(activity.total_spent_micros, 6_000);
    assert_eq!(activity.total_avoided_micros, Some(0));
    assert_eq!(activity.evidence_status, "observed");
    assert_eq!(activity.turns.len(), 1);
    assert!(!activity.turns[0].pending);
    assert!(!activity.turns[0].followed_up);
}

#[test]
fn legacy_null_cycle_turn_survives_without_owned_cycle_evidence() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    let c1 = Uuid::from_u128(19);
    let c2 = Uuid::from_u128(20);
    item.cycle_id = Some(c2);
    item.last_message_at_ms = 100_000;

    let legacy_turn = turn("legacy-turn", 200);
    let mut c1_turn = turn("c1-turn", 100);
    c1_turn.cycle_id = Some(c1);
    let mut unrelated_evidence =
        renewal_evidence("legacy-turn", c1, 200_000, CacheTtl::Ttl5m, "hit", "legacy");
    unrelated_evidence.principal_id = "other-principal".to_owned();

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[legacy_turn, c1_turn],
            evidence: &[unrelated_evidence],
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        PriceCatalog::new_empty().as_ref(),
    );

    assert_eq!(
        activity
            .turns
            .iter()
            .map(|turn| turn.source_ref_id.as_str())
            .collect::<Vec<_>>(),
        ["legacy-turn", "c1-turn"]
    );
}

#[test]
fn active_current_cycle_counters_exclude_unowned_legacy_rows() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.cycle_id = Some(Uuid::from_u128(21));
    item.last_message_at_ms = 100_000;
    let mut current = turn("current", 200);
    current.cycle_id = item.cycle_id;
    let legacy = turn("legacy", 99);

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[legacy, current],
            evidence: &[],
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        PriceCatalog::new_empty().as_ref(),
    );

    assert_eq!(
        activity
            .turns
            .iter()
            .map(|turn| turn.source_ref_id.as_str())
            .collect::<Vec<_>>(),
        ["current", "legacy"]
    );
    assert_eq!(activity.total_renewals, 1);
    assert_eq!(activity.retained_renewals, 2);
    assert!(activity.turns[0].pending);
    assert!(!activity.turns[1].pending);
}

#[test]
fn replacing_c1_with_empty_c2_retains_history_without_inheriting_pending() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    let c1 = Uuid::from_u128(11);
    let c2 = Uuid::from_u128(12);
    item.cycle_id = Some(c2);
    let active_session = session();

    let mut c1_turn = turn("c1-turn", 200);
    c1_turn.cost_micros = Some(21_000);
    c1_turn.cycle_id = Some(c1);
    let evidence = [
        renewal_evidence("c1-turn", c1, 200_000, CacheTtl::Ttl5m, "hit", "c1"),
        follow_up_evidence(320_000, CacheTtl::Ttl5m, "c1"),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: Some(&active_session),
            turns: &[c1_turn],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    let pnl = activity.pnl.expect("retained c1 activity");
    assert_eq!(pnl.spent_micros, 21_000);
    assert_eq!(pnl.avoided_micros, 69_000);
    assert_eq!(pnl.net_micros, 48_000);
    assert_eq!(activity.total_spent_micros, 21_000);
    assert_eq!(activity.total_avoided_micros, Some(69_000));
    assert_eq!(
        activity.turns.len(),
        1,
        "retained C1 table rows must match rail pulses"
    );
    assert_eq!(activity.turns[0].source_ref_id, "c1-turn");
    assert_eq!(activity.turns[0].cycle_id, Some(c1));
    assert_eq!(activity.turns[0].turn_number, 1);
    assert_eq!(activity.total_renewals, 0);
    assert!(!activity.is_last_pending);
    assert_eq!(activity.retained_renewals, 1);
    assert_eq!(activity.gaps.len(), 2);
    assert_eq!(activity.gaps[0].cycle_id, Some(c1));
    assert_eq!(activity.gaps[0].ending_kind, "followup_hit");
    let waiting = &activity.gaps[1];
    assert_eq!(waiting.cycle_id, Some(c2));
    assert_eq!(waiting.ending_kind, "scheduled_waiting");
    assert!(waiting.pulses.is_empty());
    assert_eq!(waiting.pause_at_ms, 1_730_000_000_000);
    assert_eq!(waiting.ending_at_ms, 1_730_000_200_000);
    assert_eq!(waiting.total_duration_sec, 200);
    assert_eq!(waiting.since_last_renewal_sec, None);
}

#[test]
fn persisted_zero_cost_is_real_zero_spend() {
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let mut turn = turn("zero-cost-hit", 200);
    turn.cost_micros = Some(0);
    let cycle_id = Uuid::from_u128(13);
    let evidence = [
        renewal_evidence(
            "zero-cost-hit",
            cycle_id,
            200_000,
            CacheTtl::Ttl5m,
            "hit",
            "zero",
        ),
        follow_up_evidence(320_000, CacheTtl::Ttl5m, "zero"),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[turn],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    let pnl = activity.pnl.expect("priced zero-cost activity");
    assert_eq!(pnl.spent_micros, 0);
    assert_eq!(pnl.avoided_micros, 69_000);
    assert_eq!(pnl.net_micros, 69_000);
    assert_eq!(activity.total_spent_micros, 0);
}

#[test]
fn unknown_cost_keeps_hit_history_without_claiming_zero_pnl() {
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let mut renewal = turn("unknown-cost-hit", 200);
    renewal.cost_micros = None;
    let evidence = [
        renewal_evidence(
            "unknown-cost-hit",
            Uuid::from_u128(19),
            200_000,
            CacheTtl::Ttl5m,
            "hit",
            "unknown-cost",
        ),
        follow_up_evidence(320_000, CacheTtl::Ttl5m, "unknown-cost"),
    ];
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[renewal],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    assert_eq!(activity.total_renewals, 1);
    assert_eq!(activity.turns[0].source_ref_id, "unknown-cost-hit");
    assert_eq!(activity.turns[0].renewal_tokens, 20_000);
    assert!(activity.turns[0].followed_up);
    assert_eq!(activity.turns[0].pnl, None);
    assert_eq!(activity.pnl, None);
    assert_eq!(activity.total_spent_micros, 0);
    let row = row_from_activity(12, &item, &activity, None).expect("row");
    assert_eq!(row.net_pnl, None);
}

#[test]
fn hit_miss_counts_include_hits_but_not_misses() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    let mut hit = turn("count-hit", 200);
    hit.cost_micros = Some(6_000);
    let mut miss = turn("count-miss", 100);
    miss.cost_micros = Some(15_000);
    miss.hit_miss = "miss".to_owned();
    let cycle_id = Uuid::from_u128(14);
    let evidence = [
        renewal_evidence(
            "count-hit",
            cycle_id,
            200_000,
            CacheTtl::Ttl5m,
            "hit",
            "count",
        ),
        renewal_evidence(
            "count-miss",
            cycle_id,
            210_000,
            CacheTtl::Ttl5m,
            "miss",
            "count",
        ),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[hit, miss],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    assert_eq!(
        activity
            .turns
            .iter()
            .map(|turn| turn.renewals)
            .collect::<Vec<_>>(),
        [1, 0]
    );
    assert_eq!(activity.total_renewals, 1);
    assert_eq!(activity.total_spent_micros, 21_000);
}

#[test]
fn mixed_batch_evidence_stays_scoped_to_each_session() {
    let mut observed_item = item(CacheKeepaliveSessionEntrySource::Session);
    observed_item.id = "observed".to_owned();
    let mut unknown_item = item(CacheKeepaliveSessionEntrySource::Session);
    unknown_item.id = "unknown".to_owned();
    unknown_item.session_key_hash = Some("session-unknown".to_owned());
    let mut none_item = item(CacheKeepaliveSessionEntrySource::Session);
    none_item.id = "none".to_owned();
    none_item.session_key_hash = Some("session-none".to_owned());

    let observed_turn = turn("observed-turn", 200);
    let mut unknown_turn = turn("unknown-turn", 200);
    unknown_turn.session_key_hash = "session-unknown".to_owned();
    let observed_cycle = Uuid::from_u128(15);
    let mut unknown_renewal = renewal_evidence(
        "unknown-turn",
        Uuid::from_u128(16),
        200_000,
        CacheTtl::Ttl5m,
        "hit",
        "unknown",
    );
    unknown_renewal.cycle_id = None;
    unknown_renewal.prefix_hash = None;
    unknown_renewal.prefix_policy_version = None;
    unknown_renewal.prefix_schema_version = None;
    let batch = CacheKeepaliveActivityBatch::from_turns(
        vec![observed_turn, unknown_turn],
        vec![
            renewal_evidence(
                "observed-turn",
                observed_cycle,
                200_000,
                CacheTtl::Ttl5m,
                "hit",
                "observed",
            ),
            follow_up_evidence(201_000, CacheTtl::Ttl5m, "observed"),
            unknown_renewal,
        ],
    );
    let catalog = PriceCatalog::new_empty();

    let observed = activity_for_item(
        catalog.as_ref(),
        1_730_000_100_000,
        &observed_item,
        None,
        &batch,
        None,
    )
    .expect("observed activity");
    let unknown = activity_for_item(
        catalog.as_ref(),
        1_730_000_100_000,
        &unknown_item,
        None,
        &batch,
        None,
    )
    .expect("unknown activity");
    let none = activity_for_item(
        catalog.as_ref(),
        1_730_000_100_000,
        &none_item,
        None,
        &batch,
        None,
    )
    .expect("empty activity");

    assert_eq!(observed.evidence_status, "observed");
    assert_eq!(unknown.evidence_status, "unknown");
    assert_eq!(unknown.unknown_cycles, 1);
    assert_eq!(none.evidence_status, "none");
}

#[test]
fn historical_turn_uses_its_persisted_ttl_for_avoided_cost() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.ttl = CacheTtl::Ttl1h;
    let mut historical_turn = turn("historical-5m", 200);
    historical_turn.cost_micros = Some(21_000);
    let cycle_id = Uuid::from_u128(17);
    let evidence = [
        renewal_evidence(
            "historical-5m",
            cycle_id,
            100_000,
            CacheTtl::Ttl5m,
            "hit",
            "historical",
        ),
        follow_up_evidence(399_999, CacheTtl::Ttl5m, "historical"),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[historical_turn],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    let pnl = activity.pnl.expect("priced historical activity");
    assert_eq!(pnl.avoided_micros, 69_000);
    assert_eq!(pnl.spent_micros, 21_000);
    assert_eq!(pnl.net_micros, 48_000);
}

#[test]
fn valid_follow_up_after_cap_is_attributed_once_within_historical_ttl() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    item.terminal_reason = Some(CacheKeepaliveTerminalReason::MaxRefreshes);
    item.refresh_count = Some(12);
    let mut turn = turn("capped-turn", 200);
    turn.cost_micros = Some(21_000);
    let cycle_id = Uuid::from_u128(18);
    let evidence = [
        renewal_evidence(
            "capped-turn",
            cycle_id,
            100_000,
            CacheTtl::Ttl5m,
            "hit",
            "capped",
        ),
        follow_up_evidence(399_999, CacheTtl::Ttl5m, "capped"),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[turn],
            evidence: &evidence,
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    assert_eq!(activity.evidence_status, "observed");
    assert_eq!(activity.total_renewals, 1);
    assert!(activity.turns[0].followed_up);
    assert!(!activity.turns[0].pending);
    assert_eq!(activity.gaps[0].ending_kind, "followup_hit");
    assert_eq!(activity.gaps[0].ending_at_ms, 399_999);
}

#[test]
fn idle_gaps_and_table_share_all_cycles_without_changing_current_counters_or_pnl() {
    let c1 = Uuid::from_u128(31);
    let c2 = Uuid::from_u128(32);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.cycle_id = Some(c1);
    let mut active_session = session();
    active_session.cycle_id = Some(c2);
    active_session.run_at_unix_secs = 5_100;

    let mut old_first = turn("c1-a", 1_000);
    old_first.cycle_id = Some(c1);
    old_first.cost_micros = Some(2_000);
    old_first.hit_miss = "HIT".to_owned();
    let mut old_last = turn("c1-b", 1_000);
    old_last.cycle_id = Some(c1);
    old_last.cost_micros = Some(3_000);
    old_last.cache_read_input_tokens = 30_000;
    let mut current = turn("c2", 5_000);
    current.cycle_id = Some(c2);
    current.cost_micros = Some(4_000);
    current.hit_miss = "miss".to_owned();
    let turns = [current, old_last, old_first];
    let evidence = [
        renewal_evidence("c2", c2, 5_000_000, CacheTtl::Ttl1h, "miss", "c2"),
        renewal_evidence("c1-b", c1, 1_005_000, CacheTtl::Ttl5m, "hit", "c1"),
        renewal_evidence("c1-a", c1, 1_000_000, CacheTtl::Ttl5m, "hit", "c1"),
        follow_up_evidence(1_007_000, CacheTtl::Ttl5m, "c1"),
        follow_up_evidence(1_006_000, CacheTtl::Ttl5m, "c1"),
        follow_up_evidence(5_001_000, CacheTtl::Ttl1h, "wrong-prefix"),
    ];
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: Some(&active_session),
            turns: &turns,
            evidence: &evidence,
            now_ms: 5_000_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    assert_eq!(activity.retained_renewals, 2);
    assert_eq!(activity.total_renewals, 0);
    assert_eq!(activity.turns.len(), 3);
    assert_eq!(
        activity
            .turns
            .iter()
            .map(|turn| (
                turn.source_ref_id.as_str(),
                turn.cycle_id,
                turn.turn_number,
                turn.pending
            ))
            .collect::<Vec<_>>(),
        [
            ("c2", Some(c2), 1, true),
            ("c1-a", Some(c1), 1, false),
            ("c1-b", Some(c1), 2, false),
        ]
    );
    assert!(activity.is_last_pending);
    let pnl = activity.pnl.expect("retained cycle pnl");
    assert_eq!(pnl.spent_micros, 9_000);
    assert_eq!(pnl.avoided_micros, 0);
    assert_eq!(pnl.net_micros, -9_000);
    let row = row_from_activity(12, &item, &activity, None).expect("row");
    assert_eq!(row.retained_renewals, 2);

    assert_eq!(activity.gaps.len(), 2);
    let previous = &activity.gaps[0];
    assert_eq!(previous.cycle_id, Some(c1));
    assert_eq!(previous.pause_at_ms, 730_000);
    assert_eq!(previous.cached_tokens, 30_000);
    assert_eq!(previous.ending_kind, "followup_before_expiry");
    assert_eq!(previous.ending_at_ms, 1_006_000);
    assert_eq!(previous.total_duration_sec, 276);
    assert_eq!(previous.since_last_renewal_sec, Some(1));
    assert_eq!(previous.spent_micros, 5_000);
    assert_eq!(previous.avoided_micros, Some(0));
    assert_eq!(previous.net_pnl_micros, Some(-5_000));
    assert_eq!(previous.pulses.len(), 2);
    assert_eq!(previous.pulses[0].pulse_number, 1);
    assert_eq!(previous.pulses[0].time_ms, 1_000_000);
    assert_eq!(previous.pulses[0].offset_sec, 270);
    assert_eq!(previous.pulses[0].since_prev_renewal_sec, None);
    assert_eq!(previous.pulses[0].spent_micros, Some(2_000));
    assert!(previous.pulses[0].hit);
    assert_eq!(previous.pulses[1].pulse_number, 2);
    assert_eq!(previous.pulses[1].time_ms, 1_005_000);
    assert_eq!(previous.pulses[1].offset_sec, 275);
    assert_eq!(previous.pulses[1].since_prev_renewal_sec, Some(5));
    let current = &activity.gaps[1];
    assert_eq!(current.cycle_id, Some(c2));
    assert_eq!(current.pause_at_ms, 1_700_000);
    assert_eq!(current.ending_kind, "scheduled_waiting");
    assert_eq!(current.ending_at_ms, 5_100_000);
    assert_eq!(current.total_duration_sec, 3_400);
    assert_eq!(current.since_last_renewal_sec, Some(100));
    assert!(!current.pulses[0].hit);
}

#[test]
fn idle_gap_terminal_endings_and_fallback_timing_preserve_unknown_cost() {
    let cycle_id = Uuid::from_u128(77);
    let proven = [renewal_evidence(
        "unknown",
        cycle_id,
        1_000_000,
        CacheTtl::Ttl5m,
        "hit",
        "hash",
    )];
    for (terminal_reason, max_attempts, evidence, expected) in [
        (None, 12, &proven[..], "ttl_expired_no_followup"),
        (None, 1, &proven[..], "capped_no_followup"),
        (
            Some(CacheKeepaliveTerminalReason::MaxRefreshes),
            12,
            &proven[..],
            "capped_no_followup",
        ),
        (
            Some(CacheKeepaliveTerminalReason::MaxDuration),
            12,
            &proven[..],
            "capped_no_followup",
        ),
        // Without renewal evidence nobody can tell whether a follow-up came.
        (
            Some(CacheKeepaliveTerminalReason::MaxRefreshes),
            12,
            &[][..],
            "followup_unknown",
        ),
    ] {
        let mut item = item(CacheKeepaliveSessionEntrySource::Session);
        item.status = Some(CacheKeepaliveSessionStatus::Terminal);
        item.terminal_reason = terminal_reason;
        item.config_snapshot = Some(CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 60,
            refresh_lead_time_1h_secs: 600,
            max_refreshes_per_session: max_attempts,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 8_388_608,
        });
        let mut historical = turn("unknown", 1_000);
        historical.cost_micros = None;
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: &[historical],
                evidence,
                now_ms: 2_000_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        let gap = &activity.gaps[0];
        assert_eq!(
            gap.cycle_id,
            (!evidence.is_empty()).then_some(cycle_id),
            "{expected}"
        );
        assert_eq!(
            gap.pause_at_ms,
            if evidence.is_empty() {
                760_000
            } else {
                730_000
            }
        );
        assert_eq!(gap.ending_kind, expected);
        assert_eq!(gap.ending_at_ms, 1_000_000);
        assert_eq!(
            gap.total_duration_sec,
            if evidence.is_empty() { 240 } else { 270 }
        );
        assert_eq!(gap.since_last_renewal_sec, Some(0));
        assert_eq!(gap.pulses[0].time_ms, 1_000_000);
        assert_eq!(gap.pulses[0].spent_micros, None);
        assert_eq!(gap.avoided_micros, None);
        assert_eq!(gap.net_pnl_micros, None);
    }
}

#[test]
fn null_cycle_gaps_split_by_ttl_or_max_attempts_and_price_unknown_or_pending_turns() {
    let mut session_item = item(CacheKeepaliveSessionEntrySource::Session);
    session_item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    session_item.config_snapshot = Some(CacheKeepaliveConfigSnapshot {
        refresh_lead_time_5m_secs: 60,
        refresh_lead_time_1h_secs: 600,
        max_refreshes_per_session: 2,
        max_total_duration_secs: 14_400,
        snapshot_max_bytes: 8_388_608,
    });
    let mut first = turn("first", 100);
    first.cost_micros = Some(1_000);
    let mut second = turn("second", 101);
    second.cost_micros = Some(2_000);
    let mut third = turn("third", 102);
    third.cost_micros = Some(3_000);
    let mut fourth = turn("fourth", 500);
    fourth.cost_micros = Some(4_000);
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session_item,
            session: None,
            turns: &[first, second, third, fourth],
            evidence: &[],
            now_ms: 1_730_000_100_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    assert_eq!(activity.evidence_status, "unknown");
    assert_eq!(activity.gaps.len(), 3);
    assert_eq!(
        activity
            .gaps
            .iter()
            .map(|gap| gap.pulses.len())
            .collect::<Vec<_>>(),
        [2, 1, 1]
    );
    assert_eq!(
        activity
            .gaps
            .iter()
            .map(|gap| gap.net_pnl_micros)
            .collect::<Vec<_>>(),
        [None, None, None]
    );

    let cycle_id = Uuid::from_u128(22);
    let mut pending_item = item(CacheKeepaliveSessionEntrySource::Session);
    pending_item.status = Some(CacheKeepaliveSessionStatus::Active);
    pending_item.cycle_id = Some(cycle_id);
    let mut pending_turn = turn("pending", 200);
    pending_turn.cycle_id = Some(cycle_id);
    pending_turn.cost_micros = Some(6_000);
    let pending_evidence = [renewal_evidence(
        "pending",
        cycle_id,
        200_000,
        CacheTtl::Ttl5m,
        "hit",
        "pending",
    )];
    let pending_activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &pending_item,
            session: None,
            turns: &[pending_turn],
            evidence: &pending_evidence,
            now_ms: 200_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(pending_activity.evidence_status, "pending");
    assert_eq!(pending_activity.gaps[0].net_pnl_micros, Some(-6_000));
    let pending_row = row_from_activity(12, &pending_item, &pending_activity, None).expect("row");
    let net_pnl = pending_row.net_pnl.expect("priced pending net_pnl");
    assert!((net_pnl + 0.006).abs() < 1e-12, "net_pnl={net_pnl}");
}

#[test]
fn empty_active_gap_uses_snapshot_leads_without_fabricating_renewals() {
    for (ttl, expected_duration) in [(CacheTtl::Ttl5m, 240), (CacheTtl::Ttl1h, 3_000)] {
        let mut item = item(CacheKeepaliveSessionEntrySource::Session);
        item.status = Some(CacheKeepaliveSessionStatus::Active);
        item.ttl = ttl;
        item.config_snapshot = Some(CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 60,
            refresh_lead_time_1h_secs: 600,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 8_388_608,
        });
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: &[],
                evidence: &[],
                now_ms: item.last_message_at_ms,
                horizon_start_ms: None,
            },
            PriceCatalog::new_empty().as_ref(),
        );
        assert_eq!(activity.retained_renewals, 0);
        assert_eq!(activity.total_renewals, 0);
        assert!(activity.turns.is_empty());
        let gap = &activity.gaps[0];
        assert_eq!(gap.pause_at_ms, item.last_message_at_ms);
        assert_eq!(
            gap.natural_expiry_ms,
            Some(item.last_message_at_ms + ttl.as_secs() * 1_000)
        );
        assert_eq!(gap.ending_kind, "scheduled_waiting");
        assert_eq!(gap.total_duration_sec, expected_duration);
        assert_eq!(
            gap.ending_at_ms,
            item.last_message_at_ms + expected_duration * 1_000
        );
        assert!(gap.pulses.is_empty());
        assert_eq!(gap.since_last_renewal_sec, None);
        assert_eq!(gap.spent_micros, 0);
        assert_eq!(gap.avoided_micros, None);
        assert_eq!(gap.net_pnl_micros, None);
    }
}

#[test]
fn idle_gaps_order_same_second_pulses_by_millisecond_evidence_timestamp() {
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    let cycle_id = Uuid::from_u128(99);
    item.cycle_id = Some(cycle_id);

    let mut later_lex_first = turn("a-later-ms", 1_000);
    later_lex_first.cycle_id = Some(cycle_id);
    let mut earlier_lex_second = turn("z-earlier-ms", 1_000);
    earlier_lex_second.cycle_id = Some(cycle_id);

    let evidence = [
        renewal_evidence(
            "a-later-ms",
            cycle_id,
            1_000_800,
            CacheTtl::Ttl5m,
            "hit",
            "same-sec",
        ),
        renewal_evidence(
            "z-earlier-ms",
            cycle_id,
            1_000_100,
            CacheTtl::Ttl5m,
            "hit",
            "same-sec",
        ),
    ];

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[later_lex_first, earlier_lex_second],
            evidence: &evidence,
            now_ms: 1_005_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );

    let pulses = &activity.gaps[0].pulses;
    assert_eq!(pulses.len(), 2);
    assert_eq!(pulses[0].pulse_number, 1);
    assert_eq!(pulses[0].time_ms, 1_000_100);
    assert_eq!(pulses[1].pulse_number, 2);
    assert_eq!(pulses[1].time_ms, 1_000_800);
}

#[test]
fn attempts_never_fall_below_recorded_renewals_in_the_current_cycle() {
    // Given: a session capped by an older release that stored 11 attempts
    // although its cycle recorded 12 renewals, plus one renewal from an
    // earlier cycle that must not count.
    let cycle_id = Uuid::from_u128(12);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    item.terminal_reason = Some(CacheKeepaliveTerminalReason::MaxRefreshes);
    item.cycle_id = Some(cycle_id);
    item.refresh_count = Some(11);
    let mut turns = (0..12_u64)
        .map(|index| {
            let mut turn = turn(&format!("cycle-{index}"), 1_000 + index * 270);
            turn.cycle_id = Some(cycle_id);
            turn
        })
        .collect::<Vec<_>>();
    let mut earlier = turn("earlier-cycle", 500);
    earlier.cycle_id = Some(Uuid::from_u128(11));
    turns.push(earlier);
    let derive = |item: &CacheKeepaliveSessionListItem, turns: &[CacheKeepaliveTurnRecord]| {
        derive_activity_view(
            CacheKeepaliveActivitySource {
                item,
                session: None,
                turns,
                evidence: &[],
                now_ms: 10_000_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        )
        .attempts
    };

    // Then: the displayed attempts match the renewals actually recorded.
    assert_eq!(derive(&item, &turns), Some(12));

    // And: a correct stored count is kept, and decision rows stay countless.
    item.refresh_count = Some(12);
    assert_eq!(derive(&item, &turns), Some(12));
    item.refresh_count = None;
    assert_eq!(derive(&item, &turns), None);
}

#[test]
fn session_follow_up_proves_cycle_and_credits_earliest_matching_renewal_once() {
    for (renewal_prefix, follow_up_prefix, follow_up_tokens) in [
        (Some("renewal-prefix"), None, 20_000),
        (Some("renewal-prefix"), Some("serving-prefix"), 25_000),
        (None, None, 20_000),
    ] {
        let cycle = Uuid::from_u128(90);
        let mut item = item(CacheKeepaliveSessionEntrySource::Session);
        item.status = Some(CacheKeepaliveSessionStatus::Active);
        item.cycle_id = Some(cycle);
        let mut turns = [turn("session-early", 200), turn("session-late", 240)];
        for turn in &mut turns {
            turn.cycle_id = Some(cycle);
            turn.cost_micros = Some(6_000);
        }
        let mut evidence = vec![
            renewal_evidence(
                "session-early",
                cycle,
                200_000,
                CacheTtl::Ttl5m,
                "hit",
                "renewal-prefix",
            ),
            renewal_evidence(
                "session-late",
                cycle,
                240_000,
                CacheTtl::Ttl5m,
                "hit",
                "renewal-prefix",
            ),
        ];
        for renewal in &mut evidence {
            renewal.prefix_hash = renewal_prefix.map(str::to_owned);
            renewal.session_key_hash = Some("session".to_owned());
        }
        let mut follow_up = follow_up_evidence(320_000, CacheTtl::Ttl5m, "unused");
        follow_up.prefix_hash = follow_up_prefix.map(str::to_owned);
        follow_up.session_key_hash = Some("session".to_owned());
        follow_up.cache_read_input_tokens = Some(follow_up_tokens);
        let derive = |evidence: &[CacheKeepaliveEvidenceRecord]| {
            derive_activity_view(
                CacheKeepaliveActivitySource {
                    item: &item,
                    session: None,
                    turns: &turns,
                    evidence,
                    now_ms: 310_000,
                    horizon_start_ms: None,
                },
                priced_catalog().as_ref(),
            )
        };
        assert!(derive(&evidence).is_last_pending);
        evidence.push(follow_up.clone());
        follow_up.evidence_id = "duplicate-session-follow-up".to_owned();
        evidence.push(follow_up);
        let activity = derive(&evidence);
        assert_eq!(activity.evidence_status, "observed");
        assert_eq!(activity.unknown_cycles, 0);
        assert!(!activity.is_last_pending);
        assert!(activity.turns.iter().all(|turn| !turn.pending));
        assert_eq!(activity.turns[0].source_ref_id, "session-late");
        assert!(!activity.turns[0].followed_up);
        assert!(activity.turns[1].followed_up);
        assert_eq!(activity.total_avoided_micros, Some(69_000));
        assert_eq!(activity.gaps[0].ending_kind, "followup_hit");
        assert_eq!(activity.gaps[0].avoided_micros, Some(69_000));
    }
}

#[test]
fn session_follow_up_requires_full_cache_read_and_same_route_within_ttl() {
    let cycle = Uuid::from_u128(91);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.cycle_id = Some(cycle);
    let mut paid = turn("session-paid", 200);
    paid.cycle_id = Some(cycle);
    paid.cost_micros = Some(6_000);
    let mut renewal = renewal_evidence(
        "session-paid",
        cycle,
        200_000,
        CacheTtl::Ttl5m,
        "hit",
        "renewal-prefix",
    );
    renewal.session_key_hash = Some("session".to_owned());
    let mut valid = follow_up_evidence(300_000, CacheTtl::Ttl5m, "different-prefix");
    valid.session_key_hash = Some("session".to_owned());
    assert!(super::follow_up_matches(&renewal, &valid));
    for invalid in 0..9 {
        let mut renewal = renewal.clone();
        let mut follow_up = valid.clone();
        match invalid {
            0 => follow_up.cache_read_input_tokens = Some(19_999),
            1 => follow_up.upstream_id = Uuid::from_u128(1),
            2 => follow_up.model = "claude-opus-4-6".to_owned(),
            3 => follow_up.principal_id = "other-principal".to_owned(),
            4 => follow_up.session_key_hash = Some("other-session".to_owned()),
            5 => follow_up.request_start_ms = 200_000,
            6 => follow_up.request_start_ms = 500_001,
            7 => renewal.cache_read_input_tokens = Some(0),
            8 => renewal.cache_read_input_tokens = None,
            _ => unreachable!(),
        }
        let evidence = [renewal, follow_up];
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: std::slice::from_ref(&paid),
                evidence: &evidence,
                now_ms: 310_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        assert!(activity.is_last_pending, "invalid case {invalid}");
        assert!(!activity.turns[0].followed_up, "invalid case {invalid}");
        assert_eq!(
            activity.total_avoided_micros,
            Some(0),
            "invalid case {invalid}"
        );
    }
    valid.request_start_ms = 500_000;
    assert!(super::follow_up_matches(&renewal, &valid));
}

#[test]
fn natural_expiry_classifies_early_boundary_and_late_followups() {
    let anchor = 1_000_000;
    let cycle_id = Uuid::from_u128(501);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    let mut paid = turn("expiry-renewal", (anchor + 270_000) / 1_000);
    paid.cycle_id = Some(cycle_id);
    paid.cost_micros = Some(6_000);
    for (offset, ending, avoided) in [
        (271_000, "followup_before_expiry", 0),
        (300_000, "followup_before_expiry", 0),
        (320_000, "followup_hit", 69_000),
    ] {
        let mut renewal = renewal_evidence(
            "expiry-renewal",
            cycle_id,
            anchor + 270_000,
            CacheTtl::Ttl5m,
            "hit",
            "expiry",
        );
        renewal.cycle_anchor_ms = Some(anchor);
        let evidence = [
            renewal,
            follow_up_evidence(anchor + offset, CacheTtl::Ttl5m, "expiry"),
        ];
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: std::slice::from_ref(&paid),
                evidence: &evidence,
                now_ms: anchor + 900_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        assert_eq!(activity.gaps[0].natural_expiry_ms, Some(anchor + 300_000));
        assert_eq!(activity.gaps[0].ending_kind, ending);
        assert_eq!(activity.total_avoided_micros, Some(avoided));
        assert_eq!(activity.pnl.unwrap().net_micros, avoided - 6_000);
        assert_eq!(activity.turns[0].followed_up, avoided > 0);
        assert_eq!(activity.gaps[0].net_pnl_micros, Some(avoided - 6_000));
        if avoided == 0 {
            assert_eq!(
                activity.turns[0].action,
                "follow-up arrived before cache expiry (renewal not needed)"
            );
        }
        let wire = serde_json::to_value(&activity.gaps[0]).unwrap();
        assert_eq!(wire["natural_expiry_ms"], anchor + 300_000);
        assert_eq!(wire["ending_kind"], ending);
        let row = row_from_activity(12, &item, &activity, None).unwrap();
        assert_eq!(row.total_avoided, Some(avoided as f64 / 1_000_000.0));
    }
}

#[test]
fn cycle_credits_earliest_matching_renewal_and_caps_followup_tokens_to_kept_tokens() {
    let anchor = 1_000_000;
    let cycle_id = Uuid::from_u128(502);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    let mut turns = Vec::new();
    let mut evidence = Vec::new();
    for (name, offset) in [("first", 270_000), ("second", 540_000), ("third", 810_000)] {
        let mut paid = turn(name, (anchor + offset) / 1_000);
        paid.cycle_id = Some(cycle_id);
        paid.cost_micros = Some(6_000);
        paid.cache_read_input_tokens = 10_000;
        paid.cache_creation_input_tokens = 5_000;
        turns.push(paid);
        let mut renewal = renewal_evidence(
            name,
            cycle_id,
            anchor + offset,
            CacheTtl::Ttl5m,
            "hit",
            "cycle",
        );
        renewal.cycle_anchor_ms = Some(anchor);
        evidence.push(renewal);
    }
    // A later retained renewal cannot steal the credit of the earliest matching
    // renewal, even when it falls after the first follow-up.
    for (followup_tokens, expected_tokens) in [(12_000, 12_000), (25_000, 15_000)] {
        let mut follow_up = follow_up_evidence(anchor + 551_000, CacheTtl::Ttl5m, "cycle");
        follow_up.cache_read_input_tokens = Some(followup_tokens);
        evidence.push(follow_up);
        evidence.push(follow_up_evidence(
            anchor + 850_000,
            CacheTtl::Ttl5m,
            "cycle",
        ));
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: &turns,
                evidence: &evidence,
                now_ms: anchor + 2_000_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        let avoided = expected_tokens * 345 / 100;
        assert_eq!(activity.total_avoided_micros, Some(avoided));
        assert_eq!(activity.total_spent_micros, 18_000);
        assert_eq!(activity.pnl.unwrap().net_micros, avoided - 18_000);
        assert_eq!(
            activity
                .turns
                .iter()
                .filter(|turn| turn.followed_up)
                .count(),
            1
        );
        for turn in &activity.turns {
            assert_eq!(turn.followed_up, turn.source_ref_id == "first");
            assert_eq!(
                turn.pnl.unwrap().net_micros,
                if turn.followed_up {
                    avoided - 6_000
                } else {
                    -6_000
                }
            );
        }
        evidence.truncate(3);
    }
}

#[test]
fn missing_anchor_is_unknown_until_storage_resolves_it() {
    let cycle_id = Uuid::from_u128(503);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    let mut paid = turn("legacy-anchor", 1_270);
    paid.cycle_id = Some(cycle_id);
    paid.cost_micros = Some(6_000);
    let mut renewal = renewal_evidence(
        "legacy-anchor",
        cycle_id,
        1_270_000,
        CacheTtl::Ttl5m,
        "hit",
        "legacy",
    );
    renewal.cycle_anchor_ms = None;
    let mut evidence = vec![
        renewal,
        follow_up_evidence(1_271_000, CacheTtl::Ttl5m, "legacy"),
    ];
    for with_resolved_anchor in [false, true] {
        evidence[0].cycle_anchor_ms = with_resolved_anchor.then_some(1_000_000);
        // An item snapshot must never substitute for storage-resolved evidence.
        item.config_snapshot = Some(CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        });
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: std::slice::from_ref(&paid),
                evidence: &evidence,
                now_ms: 2_000_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        assert_eq!(activity.total_spent_micros, 6_000);
        assert_eq!(activity.unknown_cycles, u64::from(!with_resolved_anchor));
        assert_eq!(
            activity.total_avoided_micros,
            with_resolved_anchor.then_some(0)
        );
        assert_eq!(
            activity.gaps[0].natural_expiry_ms,
            with_resolved_anchor.then_some(1_300_000)
        );
        assert_eq!(
            activity.gaps[0].ending_kind,
            if with_resolved_anchor {
                "followup_before_expiry"
            } else {
                "followup_unknown"
            }
        );
        assert_eq!(
            activity.pnl.map(|pnl| pnl.net_micros),
            with_resolved_anchor.then_some(-6_000)
        );
    }
    // Storage-resolved anchors carried by several rows use the minimum value.
    evidence[0].cycle_anchor_ms = Some(900_000);
    let mut later = evidence[0].clone();
    later.evidence_id = "later-anchor".to_owned();
    later.request_start_ms = 1_270_500;
    later.cycle_anchor_ms = Some(1_000_000);
    evidence.push(later);
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[paid],
            evidence: &evidence,
            now_ms: 2_000_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(activity.gaps[0].natural_expiry_ms, Some(1_200_000));
    assert!(activity.turns[0].followed_up);
}

#[test]
fn no_followup_cycle_is_pending_only_while_open_and_counts_unknown_once() {
    let cycle_id = Uuid::from_u128(504);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Active);
    item.cycle_id = Some(cycle_id);
    let mut turns = [
        turn("no-followup-first", 1_270),
        turn("no-followup-last", 1_540),
    ];
    let mut evidence = Vec::new();
    for turn in &mut turns {
        turn.cycle_id = Some(cycle_id);
        turn.cost_micros = Some(6_000);
        let mut renewal = renewal_evidence(
            &turn.source_ref_id,
            cycle_id,
            turn.ts * 1_000,
            CacheTtl::Ttl5m,
            "hit",
            "no-followup",
        );
        renewal.cycle_anchor_ms = Some(1_000_000);
        evidence.push(renewal);
    }
    for (now_ms, pending) in [(1_839_999, true), (1_840_000, false)] {
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: &turns,
                evidence: &evidence,
                now_ms,
                horizon_start_ms: None,
            },
            PriceCatalog::new_empty().as_ref(),
        );
        assert_eq!(activity.is_last_pending, pending);
        assert_eq!(
            activity.gaps[0].ending_kind,
            if pending {
                "scheduled_waiting"
            } else {
                "ttl_expired_no_followup"
            }
        );
        assert_eq!(activity.pnl.unwrap().net_micros, -12_000);
        assert_eq!(activity.total_avoided_micros, Some(0));
    }
    for renewal in &mut evidence {
        renewal.cycle_anchor_ms = None;
    }
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            evidence: &evidence,
            now_ms: 2_000_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(activity.unknown_cycles, 1);
    assert_eq!(activity.total_spent_micros, 12_000);
    assert_eq!(activity.pnl, None);
    assert_eq!(activity.gaps[0].net_pnl_micros, None);
}

#[test]
fn first_followup_before_expiry_prevents_credit_for_a_later_followup() {
    let cycle_id = Uuid::from_u128(801);
    let anchor = 1_000_000;
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.cycle_id = Some(cycle_id);
    let mut paid = turn("two-followups", 1_270);
    paid.cycle_id = Some(cycle_id);
    paid.cost_micros = Some(6_000);
    let evidence = [
        renewal_evidence(
            "two-followups",
            cycle_id,
            anchor + 270_000,
            CacheTtl::Ttl5m,
            "hit",
            "two-followups",
        ),
        follow_up_evidence(anchor + 271_000, CacheTtl::Ttl5m, "two-followups"),
        CacheKeepaliveEvidenceRecord {
            evidence_id: "second-followup".to_owned(),
            ..follow_up_evidence(anchor + 320_000, CacheTtl::Ttl5m, "two-followups")
        },
    ];
    for status in [
        CacheKeepaliveSessionStatus::Active,
        CacheKeepaliveSessionStatus::InRequest,
    ] {
        item.status = Some(status);
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: std::slice::from_ref(&paid),
                evidence: &evidence,
                now_ms: anchor + 330_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        assert_eq!(activity.gaps[0].ending_kind, "followup_before_expiry");
        assert_eq!(activity.gaps[0].ending_at_ms, anchor + 271_000);
        assert_eq!(activity.gaps[0].pause_at_ms, anchor);
        assert_eq!(activity.total_avoided_micros, Some(0));
        assert_eq!(activity.pnl.unwrap().net_micros, -6_000);
        assert!(!activity.is_last_pending);
        assert!(!activity.turns[0].pending);
        assert!(!activity.turns[0].followed_up);
        assert_eq!(activity.evidence_status, "observed");
    }
}

#[test]
fn saved_open_cycle_is_not_pending_and_horizon_follows_the_credited_turn() {
    let cycle_id = Uuid::from_u128(802);
    let anchor = 1_000_000;
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.cycle_id = Some(cycle_id);
    let mut turns = [turn("horizon-first", 1_270), turn("horizon-second", 1_540)];
    for turn in &mut turns {
        turn.cycle_id = Some(cycle_id);
        turn.cost_micros = Some(6_000);
    }
    let mut evidence = [
        renewal_evidence(
            "horizon-first",
            cycle_id,
            anchor + 270_000,
            CacheTtl::Ttl5m,
            "hit",
            "horizon",
        ),
        renewal_evidence(
            "horizon-second",
            cycle_id,
            anchor + 540_000,
            CacheTtl::Ttl5m,
            "hit",
            "horizon",
        ),
        follow_up_evidence(anchor + 551_000, CacheTtl::Ttl5m, "horizon"),
    ];
    for status in [
        CacheKeepaliveSessionStatus::Active,
        CacheKeepaliveSessionStatus::InRequest,
    ] {
        item.status = Some(status);
        for (horizon, avoided, spent, retained) in [
            (None, 69_000, 12_000, 2),
            (Some(anchor + 400_000), 0, 6_000, 1),
        ] {
            let activity = derive_activity_view(
                CacheKeepaliveActivitySource {
                    item: &item,
                    session: None,
                    turns: &turns,
                    evidence: &evidence,
                    now_ms: anchor + 560_000,
                    horizon_start_ms: horizon,
                },
                priced_catalog().as_ref(),
            );
            assert_eq!(activity.retained_renewals, retained);
            assert_eq!(activity.total_avoided_micros, Some(avoided));
            assert_eq!(activity.pnl.unwrap().net_micros, avoided - spent);
            assert_eq!(activity.gaps[0].pause_at_ms, anchor);
            assert_eq!(activity.gaps[0].natural_expiry_ms, Some(anchor + 300_000));
            assert_eq!(activity.gaps[0].ending_kind, "followup_hit");
            assert_eq!(activity.gaps[0].avoided_micros, Some(avoided));
            assert!(!activity.is_last_pending);
            assert!(activity.turns.iter().all(|turn| !turn.pending));
            assert_eq!(activity.evidence_status, "observed");
            assert_eq!(
                activity.turns[0].action,
                "redundant renewal (an earlier renewal kept the cache warm)"
            );
        }
    }
    // If the first renewal's window no longer contains F, the later renewal is
    // credited and the same horizon includes its saving.
    evidence[2].request_start_ms = anchor + 820_000;
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            evidence: &evidence,
            now_ms: anchor + 830_000,
            horizon_start_ms: Some(anchor + 400_000),
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(activity.total_avoided_micros, Some(69_000));
    assert!(activity.turns[0].followed_up);
    assert!(!activity.is_last_pending);
}

#[test]
fn renewal_credit_requires_positive_kept_tokens_and_respects_storage_resolved_anchor() {
    let cycle_id = Uuid::from_u128(803);
    let mut item = item(CacheKeepaliveSessionEntrySource::Session);
    item.status = Some(CacheKeepaliveSessionStatus::Terminal);
    let mut paid = turn("empty-renewal", 1_270);
    paid.cycle_id = Some(cycle_id);
    paid.hit_miss = "miss".to_owned();
    paid.cache_read_input_tokens = 0;
    paid.cost_micros = Some(6_000);
    let evidence = [
        renewal_evidence(
            "empty-renewal",
            cycle_id,
            1_270_000,
            CacheTtl::Ttl5m,
            "miss",
            "empty",
        ),
        follow_up_evidence(1_320_000, CacheTtl::Ttl5m, "empty"),
    ];
    for creation in [0, 10_000] {
        paid.cache_creation_input_tokens = creation;
        let activity = derive_activity_view(
            CacheKeepaliveActivitySource {
                item: &item,
                session: None,
                turns: std::slice::from_ref(&paid),
                evidence: &evidence,
                now_ms: 2_000_000,
                horizon_start_ms: None,
            },
            priced_catalog().as_ref(),
        );
        assert_eq!(activity.turns[0].followed_up, creation > 0);
        assert_eq!(
            activity.total_avoided_micros,
            Some(if creation > 0 { 34_500 } else { 0 })
        );
        assert_eq!(
            activity.gaps[0].ending_kind,
            if creation > 0 {
                "followup_hit"
            } else {
                "ttl_expired_no_followup"
            }
        );
    }
    let mut renewal = evidence[0].clone();
    renewal.cycle_anchor_ms = Some(1_269_000);
    item.config_snapshot = Some(CacheKeepaliveConfigSnapshot {
        refresh_lead_time_5m_secs: 300,
        refresh_lead_time_1h_secs: 3_600,
        max_refreshes_per_session: 12,
        max_total_duration_secs: 14_400,
        snapshot_max_bytes: 524_288,
    });
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &[paid],
            evidence: &[renewal],
            now_ms: 2_000_000,
            horizon_start_ms: None,
        },
        priced_catalog().as_ref(),
    );
    assert_eq!(activity.gaps[0].pause_at_ms, 1_269_000);
    assert_eq!(activity.gaps[0].natural_expiry_ms, Some(1_569_000));
}

#[test]
fn batched_activity_reuses_one_cached_economics_result_per_cycle() {
    let anchor = 1_000_000;
    let mut items = Vec::new();
    let mut turns = Vec::new();
    let mut evidence = Vec::new();
    for index in 0..500_u128 {
        let cycle_id = Uuid::from_u128(10_000 + index);
        let session_key = format!("batch-session-{index}");
        let prefix = format!("batch-prefix-{index}");
        let mut item = item(CacheKeepaliveSessionEntrySource::Session);
        item.id = session_key.clone();
        item.session_key_hash = Some(session_key.clone());
        item.status = Some(CacheKeepaliveSessionStatus::Terminal);
        item.cycle_id = Some(cycle_id);
        for (suffix, offset) in [("first", 270_000), ("second", 540_000)] {
            let source_ref = format!("{session_key}-{suffix}");
            let mut paid = turn(&source_ref, (anchor + offset) / 1_000);
            paid.session_key_hash = session_key.clone();
            paid.cycle_id = Some(cycle_id);
            paid.cost_micros = Some(6_000);
            turns.push(paid);
            let mut renewal = renewal_evidence(
                &source_ref,
                cycle_id,
                anchor + offset,
                CacheTtl::Ttl5m,
                "hit",
                &prefix,
            );
            renewal.cycle_anchor_ms = Some(anchor);
            evidence.push(renewal);
        }
        let mut follow_up = follow_up_evidence(anchor + 551_000, CacheTtl::Ttl5m, &prefix);
        follow_up.evidence_id = format!("{session_key}-followup");
        evidence.push(follow_up);
        items.push(item);
    }
    let batch = CacheKeepaliveActivityBatch::from_turns(turns, evidence);
    assert_eq!(batch.cached_cycle_count(), 500);
    let catalog = priced_catalog();
    for horizon_start in [None, Some(anchor + 400_000)] {
        for item in &items {
            let activity = activity_for_item(
                catalog.as_ref(),
                anchor + 560_000,
                item,
                None,
                &batch,
                horizon_start,
            )
            .expect("cached batch activity");
            assert_eq!(activity.evidence_status, "observed");
            assert_eq!(
                activity.total_avoided_micros,
                Some(if horizon_start.is_none() { 69_000 } else { 0 })
            );
            assert_eq!(activity.gaps[0].ending_kind, "followup_hit");
            assert_eq!(activity.gaps[0].natural_expiry_ms, Some(anchor + 300_000));
            assert_eq!(
                activity
                    .turns
                    .iter()
                    .filter(|turn| turn.followed_up)
                    .count(),
                usize::from(horizon_start.is_none())
            );
            assert!(!activity.is_last_pending);
        }
        assert_eq!(batch.cached_cycle_count(), 500);
    }
}
