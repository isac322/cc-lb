use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem,
    CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus, CacheKeepaliveTurnRecord, CacheTtl,
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
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::nil(),
        last_message_at_ms: 1_730_000_000_000,
        ttl: CacheTtl::Ttl5m,
        generation: 42,
        refresh_count: None,
        status: None,
        enqueue_state: None,
        terminal_reason: None,
        decision: Some("not_tracked".to_owned()),
        reason: "user turn (stop_reason=end_turn)".to_owned(),
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
        model: "claude-sonnet-4-5".to_owned(),
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_creation_input_tokens_5m: 0,
        cache_creation_input_tokens_1h: 0,
        cache_read_input_tokens: 20_000,
        cost_micros: 0,
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
        generation: 42,
        refresh_count: 0,
        first_scheduled_at_unix_secs: 1_730_000_000,
        cache_anchor_at_unix_secs: 1_730_000_000,
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
        expires_at_unix_secs: 1_730_000_300,
        created_at_unix_secs: 1_730_000_000,
        updated_at_unix_secs: 1_730_000_000,
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
            now_ms: 0,
        },
        catalog.as_ref(),
    );
    let session_view = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session,
            session: None,
            turns: &turns,
            now_ms: 0,
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
            now_ms: 1_730_000_100_000,
        },
        catalog.as_ref(),
    );

    // When: the list row consumes those records from the shared batch without the encrypted session.
    let batch = CacheKeepaliveActivityBatch::from_turns(turns);
    let batched_activity =
        activity_for_item(catalog.as_ref(), 1_730_000_100_000, &item, None, &batch)
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
    let item = item(CacheKeepaliveSessionEntrySource::Session);
    let turns = [
        turn("z-turn", 200),
        turn("a-turn", 200),
        turn("older-turn", 199),
    ];
    let catalog = PriceCatalog::new_empty();

    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &item,
            session: None,
            turns: &turns,
            now_ms: 1_730_000_100_000,
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
        [3, 2, 1]
    );
}
