pub mod economics;
pub mod status;

use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveEvidenceRecord, CacheKeepaliveSessionEntrySource,
    CacheKeepaliveSessionListItem, CacheKeepaliveSessionRecord, CacheKeepaliveTurnRecord, CacheTtl,
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
    pub evidence: &'a [CacheKeepaliveEvidenceRecord],
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
    pub is_active: bool,
    pub total_spent_micros: i64,
    pub total_avoided_micros: Option<i64>,
    pub evidence_status: String,
    pub unknown_cycles: u64,
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
    // The session row is the authoritative current-generation projection for
    // detail views. List items normally carry the same cycle id, but older
    // projections can omit or lag it while the session record is current.
    // Preserve null-cycle legacy turns when they have no owned cycle evidence.
    let cycle_id = source
        .session
        .and_then(|session| session.cycle_id)
        .or(source.item.cycle_id);
    let mut records = source
        .turns
        .iter()
        .filter(|turn| {
            let Some(cycle_id) = cycle_id else {
                return true;
            };
            if turn.cycle_id == Some(cycle_id) {
                return true;
            }
            if turn.cycle_id.is_some() {
                return false;
            }
            renewal_evidence_for_turn(turn, source.evidence)
                .and_then(|renewal| renewal.cycle_id)
                .is_none_or(|renewal_cycle_id| renewal_cycle_id == cycle_id)
        })
        .collect::<Vec<_>>();
    records.sort_by(|left, right| {
        right
            .ts
            .cmp(&left.ts)
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
    let is_active = matches!(
        source.item.status,
        Some(cc_lb_storage_api::CacheKeepaliveSessionStatus::Active)
    );
    let pending_source_ref_id = if is_active && cycle_id.is_some() {
        records
            .first()
            .filter(|turn| {
                renewal_evidence_for_turn(turn, source.evidence)
                    .and_then(|renewal| renewal.cycle_id)
                    == cycle_id
            })
            .map(|turn| turn.source_ref_id.as_str())
    } else {
        None
    };
    // A proven follow-up closes the active window for the newest turn. Keep
    // the newest turn pending only while its current-cycle renewal is unresolved.
    let turns = records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let pending = is_active
                && index == 0
                && (pending_source_ref_id.is_none()
                    || (pending_source_ref_id == Some(record.source_ref_id.as_str())
                        && !follow_up_proven(record, source.evidence)));
            let turn_number =
                u32::try_from(records.len().saturating_sub(index)).unwrap_or(u32::MAX);
            activity_turn(
                record,
                turn_number,
                pending,
                catalog,
                source.item.ttl,
                source.evidence,
            )
        })
        .collect::<Vec<_>>();
    let retained_turns = source
        .turns
        .iter()
        .enumerate()
        .map(|(index, record)| {
            activity_turn(
                record,
                u32::try_from(source.turns.len().saturating_sub(index)).unwrap_or(u32::MAX),
                false,
                catalog,
                source.item.ttl,
                source.evidence,
            )
        })
        .collect::<Vec<_>>();
    let priced_turns = retained_turns
        .iter()
        .map(|turn| turn.pnl)
        .collect::<Option<Vec<_>>>();
    let pnl = match (state, priced_turns) {
        (CacheKeepaliveViewState::NotTracked, _) => Some(sum_turn_pnl(&[])),
        (_, Some(turns)) => Some(sum_turn_pnl(&turns)),
        (_, None) => None,
    };
    let (evidence_status, unknown_cycles) =
        evidence_status(source.turns, source.evidence, pending_source_ref_id);
    let total_spent_micros = source
        .turns
        .iter()
        .fold(0_i64, |total, turn| total.saturating_add(turn.cost_micros));
    let total_avoided_micros = (evidence_status == "observed")
        .then(|| pnl.map(|value| value.avoided_micros))
        .flatten();
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
        is_active,
        total_spent_micros,
        total_avoided_micros,
        evidence_status,
        unknown_cycles,
        turns,
    }
}

fn evidence_status(
    turns: &[CacheKeepaliveTurnRecord],
    evidence: &[CacheKeepaliveEvidenceRecord],
    pending_source_ref_id: Option<&str>,
) -> (String, u64) {
    if turns.is_empty()
        && !evidence
            .iter()
            .any(|entry| entry.evidence_kind == "renewal")
    {
        return ("none".to_owned(), 0);
    }
    let mut unknown_cycles = 0_u64;
    let mut observed = false;
    let mut pending = false;
    for turn in turns {
        let Some(renewal) = renewal_evidence_for_turn(turn, evidence) else {
            unknown_cycles = unknown_cycles.saturating_add(1);
            continue;
        };
        if !renewal_evidence_known(renewal) {
            unknown_cycles = unknown_cycles.saturating_add(1);
        } else {
            observed = true;
            if pending_source_ref_id == Some(turn.source_ref_id.as_str())
                && !follow_up_proven(turn, evidence)
            {
                pending = true;
            }
        }
    }
    if unknown_cycles > 0 {
        ("unknown".to_owned(), unknown_cycles)
    } else if pending {
        ("pending".to_owned(), 0)
    } else if observed {
        ("observed".to_owned(), 0)
    } else {
        ("none".to_owned(), 0)
    }
}

fn renewal_evidence_for_turn<'a>(
    record: &CacheKeepaliveTurnRecord,
    evidence: &'a [CacheKeepaliveEvidenceRecord],
) -> Option<&'a CacheKeepaliveEvidenceRecord> {
    evidence.iter().find(|candidate| {
        candidate.evidence_kind == "renewal"
            && candidate
                .event_ref
                .as_deref()
                .is_some_and(|event_ref| event_ref == record.source_ref_id)
            && candidate.principal_id == record.principal_id
            && candidate.upstream_id == record.upstream_id
            && candidate.model == record.model
    })
}

fn renewal_evidence_known(evidence: &CacheKeepaliveEvidenceRecord) -> bool {
    evidence.cycle_id.is_some()
        && evidence.prefix_hash.is_some()
        && evidence.prefix_policy_version.is_some()
        && evidence.prefix_schema_version.is_some()
}

fn follow_up_matches(
    renewal: &CacheKeepaliveEvidenceRecord,
    follow_up: &CacheKeepaliveEvidenceRecord,
) -> bool {
    renewal_evidence_known(renewal)
        && follow_up.evidence_kind == "follow_up"
        && follow_up.principal_id == renewal.principal_id
        && follow_up.upstream_id == renewal.upstream_id
        && follow_up.model == renewal.model
        && follow_up
            .cache_read_input_tokens
            .is_some_and(|tokens| tokens > 0)
        && follow_up.request_start_ms > renewal.request_start_ms
        && follow_up.request_start_ms
            <= renewal
                .request_start_ms
                .saturating_add(renewal.ttl.as_secs().saturating_mul(1_000))
        && follow_up.prefix_hash.is_some()
        && follow_up.prefix_hash == renewal.prefix_hash
}

fn follow_up_proven(
    record: &CacheKeepaliveTurnRecord,
    evidence: &[CacheKeepaliveEvidenceRecord],
) -> bool {
    let Some(renewal) = renewal_evidence_for_turn(record, evidence) else {
        return false;
    };
    let Some(cycle_id) = renewal.cycle_id else {
        return false;
    };
    let cycle_renewals = evidence
        .iter()
        .filter(|candidate| {
            candidate.evidence_kind == "renewal"
                && candidate.cycle_id == Some(cycle_id)
                && candidate.principal_id == renewal.principal_id
                && candidate.upstream_id == renewal.upstream_id
                && candidate.model == renewal.model
        })
        .collect::<Vec<_>>();
    let Some(follow_up) = evidence
        .iter()
        .filter(|candidate| {
            cycle_renewals
                .iter()
                .any(|candidate_renewal| follow_up_matches(candidate_renewal, candidate))
        })
        .min_by_key(|candidate| (candidate.request_start_ms, &candidate.evidence_id))
    else {
        return false;
    };
    cycle_renewals
        .iter()
        .filter(|candidate| follow_up_matches(candidate, follow_up))
        .max_by_key(|candidate| (candidate.request_start_ms, &candidate.evidence_id))
        .and_then(|candidate| candidate.event_ref.as_deref())
        == Some(record.source_ref_id.as_str())
}

fn activity_turn(
    record: &CacheKeepaliveTurnRecord,
    turn_number: u32,
    pending: bool,
    catalog: &PriceCatalog,
    ttl: CacheTtl,
    evidence: &[CacheKeepaliveEvidenceRecord],
) -> CacheKeepaliveActivityTurn {
    let renewal_evidence = renewal_evidence_for_turn(record, evidence);
    let evidence_known = renewal_evidence.is_some_and(renewal_evidence_known);
    let hit = record.hit_miss.eq_ignore_ascii_case("hit");
    let followed_up = !pending && hit && follow_up_proven(record, evidence) && evidence_known;
    let ttl = renewal_evidence.map_or(ttl, |renewal| renewal.ttl);
    let pnl = rates_from_catalog(catalog, &record.model, ttl).map(|rates| {
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: record.cache_read_input_tokens,
                spent_micros: record.cost_micros,
                followed_up,
                pending,
            },
            rates,
        )
    });
    let action = if !evidence_known {
        "follow-up evidence unavailable"
    } else {
        turn_action(followed_up, pending)
    };
    let pnl_display = pnl.map_or_else(|| "-".to_owned(), format_turn_pnl);
    CacheKeepaliveActivityTurn {
        source_ref_id: record.source_ref_id.clone(),
        turn_number,
        time_ms: record.ts.saturating_mul(1_000),
        renewal_tokens: record.cache_read_input_tokens,
        renewals: u32::from(hit),
        followed_up,
        pending,
        action,
        pnl,
        pnl_display,
    }
}
