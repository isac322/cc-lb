pub mod economics;
pub mod status;

use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem,
    CacheKeepaliveSessionRecord, CacheKeepaliveTurnRecord, CacheTtl,
};
use serde::Serialize;
use uuid::Uuid;

use self::economics::{
    CacheKeepaliveSessionPnl, CacheKeepaliveTurnPnl, CacheKeepaliveTurnPnlInput, derive_turn_pnl,
    format_net_pnl, format_turn_pnl, rates_from_catalog, sum_turn_pnl,
};
use self::status::{
    CacheKeepaliveReasonDisplay, CacheKeepaliveViewState, next_renewal_display, reason_display,
    state_from_item, turn_action,
};

#[cfg(test)]
#[path = "cache_keepalive_view_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cache_keepalive_view_status_tests.rs"]
mod status_tests;

#[cfg(test)]
#[path = "cache_keepalive_view_activity_tests.rs"]
mod activity_tests;

pub struct CacheKeepaliveActivitySource<'a> {
    pub item: &'a CacheKeepaliveSessionListItem,
    pub session: Option<&'a CacheKeepaliveSessionRecord>,
    pub turns: &'a [CacheKeepaliveTurnRecord],
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveActivityView {
    pub id: String,
    pub source: CacheKeepaliveSessionEntrySource,
    pub state: CacheKeepaliveViewState,
    pub reason: CacheKeepaliveReasonDisplay,
    pub ttl: CacheTtl,
    pub attempts: Option<u32>,
    pub max_attempts: Option<u32>,
    pub generation: u64,
    pub upstream_id: Uuid,
    pub last_message_at_ms: u64,
    pub error: Option<String>,
    pub config_snapshot: Option<CacheKeepaliveConfigSnapshot>,
    pub next_renewal: Option<String>,
    pub pnl: Option<CacheKeepaliveSessionPnl>,
    pub net_pnl_display: String,
    pub total_renewals: u32,
    pub is_last_pending: bool,
    pub turns: Vec<CacheKeepaliveActivityTurn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveActivityTurn {
    pub source_ref_id: String,
    pub turn_number: u32,
    pub time_ms: u64,
    pub renewal_tokens: u64,
    pub renewals: u32,
    pub followed_up: bool,
    pub pending: bool,
    pub action: &'static str,
    pub pnl: Option<CacheKeepaliveTurnPnl>,
    pub pnl_display: String,
}

pub fn derive_activity_view(
    source: CacheKeepaliveActivitySource<'_>,
    catalog: &PriceCatalog,
) -> CacheKeepaliveActivityView {
    let state = state_from_item(source.item);
    let mut records = source.turns.iter().collect::<Vec<_>>();
    records.sort_by(|left, right| {
        right
            .ts
            .cmp(&left.ts)
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
    let is_active = matches!(
        state,
        CacheKeepaliveViewState::Renewed | CacheKeepaliveViewState::Scheduled
    );
    let turns = records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let turn_number =
                u32::try_from(records.len().saturating_sub(index)).unwrap_or(u32::MAX);
            activity_turn(
                record,
                turn_number,
                is_active && index == 0,
                catalog,
                source.item.ttl,
            )
        })
        .collect::<Vec<_>>();
    let priced_turns = turns
        .iter()
        .map(|turn| turn.pnl)
        .collect::<Option<Vec<_>>>();
    let pnl = match (state, priced_turns) {
        (CacheKeepaliveViewState::NotTracked, _) => Some(sum_turn_pnl(&[])),
        (_, Some(turns)) => Some(sum_turn_pnl(&turns)),
        (_, None) => None,
    };
    let next_renewal = source.session.and_then(|session| {
        next_renewal_display(
            state,
            session.run_at_unix_secs.saturating_mul(1_000),
            source.now_ms,
        )
    });
    CacheKeepaliveActivityView {
        id: source.item.id.clone(),
        source: source.item.source,
        state,
        reason: reason_display(&source.item.reason, source.item.error.as_deref()),
        ttl: source.item.ttl,
        attempts: source.item.refresh_count,
        max_attempts: source
            .item
            .config_snapshot
            .as_ref()
            .map(|config| config.max_refreshes_per_session),
        generation: source.item.generation,
        upstream_id: source.item.upstream_id,
        last_message_at_ms: source.item.last_message_at_ms,
        error: source.item.error.clone(),
        config_snapshot: source.item.config_snapshot.clone(),
        next_renewal,
        pnl,
        net_pnl_display: pnl.map_or_else(|| "-".to_owned(), |pnl| format_net_pnl(pnl.net_micros)),
        total_renewals: turns
            .iter()
            .fold(0_u32, |total, turn| total.saturating_add(turn.renewals)),
        is_last_pending: turns.first().is_some_and(|turn| turn.pending),
        turns,
    }
}

fn activity_turn(
    record: &CacheKeepaliveTurnRecord,
    turn_number: u32,
    pending: bool,
    catalog: &PriceCatalog,
    ttl: CacheTtl,
) -> CacheKeepaliveActivityTurn {
    let followed_up = !pending && record.hit_miss.eq_ignore_ascii_case("hit");
    let pnl = rates_from_catalog(catalog, &record.model, ttl).map(|rates| {
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: record.cache_read_input_tokens,
                renewals: 1,
                followed_up,
                pending,
            },
            rates,
        )
    });
    let pnl_display = pnl.map_or_else(|| "-".to_owned(), format_turn_pnl);
    CacheKeepaliveActivityTurn {
        source_ref_id: record.source_ref_id.clone(),
        turn_number,
        time_ms: record.ts.saturating_mul(1_000),
        renewal_tokens: record.cache_read_input_tokens,
        renewals: 1,
        followed_up,
        pending,
        action: turn_action(followed_up, pending),
        pnl,
        pnl_display,
    }
}
