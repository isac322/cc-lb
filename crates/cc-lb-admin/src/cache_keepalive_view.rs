pub mod economics;
pub mod status;
pub(crate) mod trigger;

use std::collections::HashMap;

use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveEvidenceRecord, CacheKeepaliveSessionEntrySource,
    CacheKeepaliveSessionListItem, CacheKeepaliveSessionRecord, CacheKeepaliveTerminalReason,
    CacheKeepaliveTurnRecord, CacheTtl,
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
    pub horizon_start_ms: Option<u64>,
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
    pub retained_renewals: u32,
    pub is_last_pending: bool,
    pub is_active: bool,
    pub total_spent_micros: i64,
    pub total_avoided_micros: Option<i64>,
    pub evidence_status: String,
    pub unknown_cycles: u64,
    pub turns: Vec<CacheKeepaliveActivityTurn>,
    pub gaps: Vec<CacheKeepaliveIdleGap>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveIdleGap {
    pub cycle_id: Option<Uuid>,
    pub pause_at_ms: u64,
    pub cached_tokens: u64,
    pub pulses: Vec<CacheKeepaliveIdlePulse>,
    pub ending_kind: &'static str,
    pub ending_at_ms: u64,
    pub total_duration_sec: u64,
    pub since_last_renewal_sec: Option<u64>,
    pub spent_micros: i64,
    pub avoided_micros: Option<i64>,
    pub net_pnl_micros: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveIdlePulse {
    pub pulse_number: u32,
    pub time_ms: u64,
    pub offset_sec: u64,
    pub since_prev_renewal_sec: Option<u64>,
    pub spent_micros: Option<i64>,
    pub hit: bool,
}

pub fn derive_activity_view(
    source: CacheKeepaliveActivitySource<'_>,
    catalog: &PriceCatalog,
) -> CacheKeepaliveActivityView {
    let horizon_start_ms = source.horizon_start_ms;
    let mut state = state_from_item(source.item);
    if state == CacheKeepaliveViewState::NotTracked
        && !source.item.is_decision()
        && !source.turns.is_empty()
    {
        state = if matches!(
            source.item.terminal_reason,
            Some(CacheKeepaliveTerminalReason::Cancelled)
        ) {
            CacheKeepaliveViewState::Cancelled
        } else {
            CacheKeepaliveViewState::Capped
        };
    }
    let is_active = matches!(
        source.item.status,
        Some(cc_lb_storage_api::CacheKeepaliveSessionStatus::Active)
    );
    let cycle_id = source
        .session
        .and_then(|session| session.cycle_id)
        .or(source.item.cycle_id);
    let mut records = source
        .turns
        .iter()
        .filter(|turn| {
            horizon_start_ms.is_none_or(|start_ms| turn.ts.saturating_mul(1_000) >= start_ms)
        })
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
            let renewal_cycle_id = renewal_evidence_for_turn(turn, source.evidence)
                .and_then(|renewal| renewal.cycle_id);
            if renewal_cycle_id.is_some() {
                return renewal_cycle_id == Some(cycle_id);
            }
            !(is_active && turn.ts.saturating_mul(1_000) < source.item.last_message_at_ms)
        })
        .collect::<Vec<_>>();
    records.sort_by(|left, right| {
        right
            .ts
            .cmp(&left.ts)
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
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
    let scoped_turns = source
        .turns
        .iter()
        .filter(|turn| {
            horizon_start_ms.is_none_or(|start_ms| turn.ts.saturating_mul(1_000) >= start_ms)
        })
        .collect::<Vec<_>>();
    let retained_turns = scoped_turns
        .iter()
        .enumerate()
        .map(|(index, record)| {
            activity_turn(
                record,
                u32::try_from(scoped_turns.len().saturating_sub(index)).unwrap_or(u32::MAX),
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
    let (evidence_status, unknown_cycles) = evidence_status(
        scoped_turns.iter().copied(),
        source.evidence,
        pending_source_ref_id,
    );
    let total_spent_micros = scoped_turns
        .iter()
        .filter_map(|turn| turn.cost_micros)
        .fold(0_i64, |total, cost| total.saturating_add(cost));
    let total_avoided_micros = pnl.map(|value| value.avoided_micros);
    let next_renewal = source.session.and_then(|session| {
        next_renewal_display(
            state,
            session.run_at_unix_secs.saturating_mul(1_000),
            source.now_ms,
        )
    });
    let gaps = idle_gaps(&source, &scoped_turns, &retained_turns, cycle_id, is_active);
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
        retained_renewals: retained_turns
            .iter()
            .fold(0_u32, |total, turn| total.saturating_add(turn.renewals)),
        is_last_pending: turns.first().is_some_and(|turn| turn.pending),
        is_active,
        total_spent_micros,
        total_avoided_micros,
        evidence_status,
        unknown_cycles,
        turns,
        gaps,
    }
}

fn lead_window_ms(ttl: CacheTtl, config: Option<&CacheKeepaliveConfigSnapshot>) -> u64 {
    let lead_secs = match ttl {
        CacheTtl::Ttl5m | CacheTtl::Ttl30m => {
            config.map_or(30, |config| config.refresh_lead_time_5m_secs)
        }
        CacheTtl::Ttl1h | CacheTtl::Ttl12h => {
            config.map_or(300, |config| config.refresh_lead_time_1h_secs)
        }
    };
    ttl.as_secs()
        .saturating_sub(u64::from(lead_secs))
        .saturating_mul(1_000)
}

fn pulse_time_ms(
    turn: &CacheKeepaliveTurnRecord,
    evidence: &[CacheKeepaliveEvidenceRecord],
) -> u64 {
    renewal_evidence_for_turn(turn, evidence).map_or_else(
        || turn.ts.saturating_mul(1_000),
        |renewal| renewal.request_start_ms,
    )
}

fn idle_gaps(
    source: &CacheKeepaliveActivitySource<'_>,
    scoped_turns: &[&CacheKeepaliveTurnRecord],
    retained_turns: &[CacheKeepaliveActivityTurn],
    current_cycle_id: Option<Uuid>,
    is_active: bool,
) -> Vec<CacheKeepaliveIdleGap> {
    let mut ordered_turns = scoped_turns.iter().copied().enumerate().collect::<Vec<_>>();
    ordered_turns.sort_by(|(_, left), (_, right)| {
        pulse_time_ms(left, source.evidence)
            .cmp(&pulse_time_ms(right, source.evidence))
            .then_with(|| left.ts.cmp(&right.ts))
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
    let config = source.item.config_snapshot.as_ref();
    let max_attempts = config.map_or(12, |config| config.max_refreshes_per_session);
    let mut cycle_indices = HashMap::new();
    let mut cycles: Vec<(Option<Uuid>, Vec<usize>)> = Vec::new();
    let mut open_none_group: Option<usize> = None;
    for (index, turn) in ordered_turns {
        let cycle_id = turn.cycle_id.or_else(|| {
            renewal_evidence_for_turn(turn, source.evidence).and_then(|renewal| renewal.cycle_id)
        });
        let cycle_index = match cycle_id {
            Some(cycle_id) => {
                open_none_group = None;
                *cycle_indices.entry(cycle_id).or_insert_with(|| {
                    cycles.push((Some(cycle_id), Vec::new()));
                    cycles.len() - 1
                })
            }
            None => {
                let should_split = open_none_group.is_none_or(|group_index| {
                    let current_group = &cycles[group_index].1;
                    let previous_index = *current_group.last().expect("open null-cycle group");
                    let previous_turn = scoped_turns[previous_index];
                    let previous_ttl = renewal_evidence_for_turn(previous_turn, source.evidence)
                        .map_or(source.item.ttl, |renewal| renewal.ttl);
                    pulse_time_ms(turn, source.evidence)
                        .saturating_sub(pulse_time_ms(previous_turn, source.evidence))
                        > previous_ttl.as_secs().saturating_mul(1_000)
                        || current_group.len()
                            >= usize::try_from(max_attempts.max(1)).unwrap_or(usize::MAX)
                        || turn.upstream_id != previous_turn.upstream_id
                        || turn.model != previous_turn.model
                });
                if should_split {
                    cycles.push((None, Vec::new()));
                    open_none_group = Some(cycles.len() - 1);
                }
                open_none_group.expect("null-cycle group must be open")
            }
        };
        cycles[cycle_index].1.push(index);
    }
    let has_current_cycle = match current_cycle_id {
        Some(cycle_id) => cycle_indices.contains_key(&cycle_id),
        None => cycles.iter().any(|(cycle_id, _)| cycle_id.is_none()),
    };
    let mut gaps = Vec::with_capacity(cycles.len() + usize::from(is_active && !has_current_cycle));
    for (cycle_id, indices) in cycles {
        let first_turn = scoped_turns[indices[0]];
        let first_evidence = renewal_evidence_for_turn(first_turn, source.evidence);
        let ttl = first_evidence.map_or(source.item.ttl, |renewal| renewal.ttl);
        let first_pulse_ms = first_evidence.map_or_else(
            || first_turn.ts.saturating_mul(1_000),
            |renewal| renewal.request_start_ms,
        );
        let pause_at_ms = first_pulse_ms.saturating_sub(lead_window_ms(ttl, config));
        let mut previous_time_ms = None;
        let pulses = indices
            .iter()
            .enumerate()
            .map(|(index, &turn_index)| {
                let turn = scoped_turns[turn_index];
                let time_ms = renewal_evidence_for_turn(turn, source.evidence).map_or_else(
                    || turn.ts.saturating_mul(1_000),
                    |renewal| renewal.request_start_ms,
                );
                let since_prev_renewal_sec =
                    previous_time_ms.map(|previous| time_ms.saturating_sub(previous) / 1_000);
                previous_time_ms = Some(time_ms);
                CacheKeepaliveIdlePulse {
                    pulse_number: u32::try_from(index.saturating_add(1)).unwrap_or(u32::MAX),
                    time_ms,
                    offset_sec: time_ms.saturating_sub(pause_at_ms) / 1_000,
                    since_prev_renewal_sec,
                    spent_micros: turn.cost_micros,
                    hit: turn.hit_miss.eq_ignore_ascii_case("hit"),
                }
            })
            .collect::<Vec<_>>();
        let last_pulse_ms = previous_time_ms.unwrap_or(first_pulse_ms);
        let follow_up = source
            .evidence
            .iter()
            .filter(|candidate| candidate.evidence_kind == "follow_up")
            .filter(|follow_up| {
                source.evidence.iter().any(|renewal| {
                    renewal.evidence_kind == "renewal"
                        && renewal.cycle_id == cycle_id
                        && renewal.principal_id == first_turn.principal_id
                        && renewal.upstream_id == first_turn.upstream_id
                        && renewal.model == first_turn.model
                        && follow_up_matches(renewal, follow_up)
                })
            })
            .min_by_key(|follow_up| (follow_up.request_start_ms, &follow_up.evidence_id));
        let evidence_unknown = indices.iter().any(|&index| {
            renewal_evidence_for_turn(scoped_turns[index], source.evidence)
                .is_none_or(|renewal| !renewal_evidence_known(renewal))
        });
        let (ending_kind, ending_at_ms) = if let Some(follow_up) = follow_up {
            ("followup_hit", follow_up.request_start_ms)
        } else if is_active && cycle_id == current_cycle_id {
            (
                "scheduled_waiting",
                source
                    .session
                    .map_or(last_pulse_ms, |session| {
                        session.run_at_unix_secs.saturating_mul(1_000)
                    })
                    .max(last_pulse_ms),
            )
        } else if evidence_unknown {
            ("followup_unknown", last_pulse_ms)
        } else if u32::try_from(pulses.len()).unwrap_or(u32::MAX) >= max_attempts
            || matches!(
                source.item.terminal_reason,
                Some(
                    CacheKeepaliveTerminalReason::MaxRefreshes
                        | CacheKeepaliveTerminalReason::MaxDuration
                )
            )
        {
            ("capped_no_followup", last_pulse_ms)
        } else {
            ("ttl_expired_no_followup", last_pulse_ms)
        };
        let spent_micros = indices
            .iter()
            .filter_map(|&index| scoped_turns[index].cost_micros)
            .fold(0_i64, i64::saturating_add);
        let all_turns_priced = !indices.is_empty()
            && indices
                .iter()
                .all(|&index| retained_turns[index].pnl.is_some());
        let avoided_micros = all_turns_priced.then(|| {
            indices.iter().fold(0_i64, |total, &index| {
                total.saturating_add(
                    retained_turns[index]
                        .pnl
                        .map_or(0, |pnl| pnl.avoided_micros),
                )
            })
        });
        let net_pnl_micros = avoided_micros.map(|avoided| avoided.saturating_sub(spent_micros));
        gaps.push(CacheKeepaliveIdleGap {
            cycle_id,
            pause_at_ms,
            cached_tokens: indices
                .iter()
                .map(|&index| scoped_turns[index].cache_read_input_tokens)
                .max()
                .unwrap_or(0),
            pulses,
            ending_kind,
            ending_at_ms,
            total_duration_sec: ending_at_ms.saturating_sub(pause_at_ms) / 1_000,
            since_last_renewal_sec: Some(ending_at_ms.saturating_sub(last_pulse_ms) / 1_000),
            spent_micros,
            avoided_micros,
            net_pnl_micros,
        });
    }
    if is_active && !has_current_cycle {
        let pause_at_ms = source
            .session
            .map_or(source.item.last_message_at_ms, |session| {
                session.cache_anchor_at_unix_secs.saturating_mul(1_000)
            });
        let ending_at_ms = source
            .session
            .map_or_else(
                || pause_at_ms.saturating_add(lead_window_ms(source.item.ttl, config)),
                |session| session.run_at_unix_secs.saturating_mul(1_000),
            )
            .max(pause_at_ms);
        gaps.push(CacheKeepaliveIdleGap {
            cycle_id: current_cycle_id,
            pause_at_ms,
            cached_tokens: 0,
            pulses: Vec::new(),
            ending_kind: "scheduled_waiting",
            ending_at_ms,
            total_duration_sec: ending_at_ms.saturating_sub(pause_at_ms) / 1_000,
            since_last_renewal_sec: None,
            spent_micros: 0,
            avoided_micros: None,
            net_pnl_micros: None,
        });
    }
    gaps
}

fn evidence_status<'a>(
    turns: impl IntoIterator<Item = &'a CacheKeepaliveTurnRecord>,
    evidence: &[CacheKeepaliveEvidenceRecord],
    pending_source_ref_id: Option<&str>,
) -> (String, u64) {
    let turns = turns.into_iter().collect::<Vec<_>>();
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
    let pnl = record.cost_micros.and_then(|spent_micros| {
        rates_from_catalog(catalog, &record.model, ttl).map(|rates| {
            derive_turn_pnl(
                CacheKeepaliveTurnPnlInput {
                    renewal_tokens: record.cache_read_input_tokens,
                    spent_micros,
                    followed_up,
                    pending,
                },
                rates,
            )
        })
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
