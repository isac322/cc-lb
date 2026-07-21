use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem, CacheKeepaliveTurnRecord,
    CacheTtl,
};
use uuid::Uuid;

use super::status::CacheKeepaliveViewState;
use super::{CacheKeepaliveActivitySource, derive_activity_view};

fn item(source: CacheKeepaliveSessionEntrySource) -> CacheKeepaliveSessionListItem {
    CacheKeepaliveSessionListItem {
        id: "decision-only".to_owned(),
        source,
        session_key_hash: None,
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
