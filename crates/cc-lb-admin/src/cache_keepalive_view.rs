pub mod economics;
pub mod status;
pub(crate) mod trigger;

use std::collections::{HashMap, HashSet};

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
    pub cycle_id: Option<Uuid>,
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
    pub natural_expiry_ms: Option<u64>,
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
    let index = CacheKeepaliveEvidenceIndex::new(source.evidence, source.turns.iter());
    derive_activity_view_with_index(source, catalog, &index)
}

pub(crate) fn derive_activity_view_with_index(
    source: CacheKeepaliveActivitySource<'_>,
    catalog: &PriceCatalog,
    index: &CacheKeepaliveEvidenceIndex,
) -> CacheKeepaliveActivityView {
    let evidence = EvidenceContext {
        records: source.evidence,
        index,
    };
    let horizon_start_ms = source.horizon_start_ms;
    let state = state_from_item(source.item);
    let is_active = matches!(
        source.item.status,
        Some(cc_lb_storage_api::CacheKeepaliveSessionStatus::Active)
    );
    // Scheduling stops during a real request; retained renewal evidence does not.
    let is_request_paused = matches!(
        source.item.status,
        Some(cc_lb_storage_api::CacheKeepaliveSessionStatus::InRequest)
    ) || source.item.request_incomplete;
    let cycle_open = is_active || is_request_paused;
    let cycle_id = source
        .session
        .and_then(|session| session.cycle_id)
        .or(source.item.cycle_id);
    // The table, rail, and retained economics share one horizon-scoped range.
    let mut scoped_turns = source
        .turns
        .iter()
        .filter(|turn| {
            horizon_start_ms.is_none_or(|start_ms| turn.ts.saturating_mul(1_000) >= start_ms)
        })
        .collect::<Vec<_>>();
    scoped_turns.sort_by(|left, right| {
        right
            .ts
            .cmp(&left.ts)
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
    let current_records = scoped_turns
        .iter()
        .copied()
        .filter(|turn| {
            let Some(cycle_id) = cycle_id else {
                // An open schedule without a cycle identity cannot own retained
                // history. Closed legacy rows retain their historical counters.
                return !cycle_open;
            };
            turn.cycle_id.or_else(|| {
                renewal_evidence_for_turn(turn, evidence).and_then(|renewal| renewal.cycle_id)
            }) == Some(cycle_id)
        })
        .collect::<Vec<_>>();
    // A scheduled job can outlive its paid renewal's cache. Only known renewal
    // evidence can close that window; missing legacy evidence remains unknown.
    let evidence_window_open = current_records.first().is_some_and(|turn| {
        match renewal_evidence_for_turn(turn, evidence)
            .filter(|renewal| renewal_evidence_known(renewal))
        {
            Some(_) => source.now_ms < renewal_expiry_ms(turn, source.item.ttl, evidence),
            None => !is_request_paused,
        }
    });
    let pending_source_ref_id = if cycle_open && evidence_window_open && cycle_id.is_some() {
        current_records
            .first()
            .filter(|turn| {
                renewal_evidence_for_turn(turn, evidence).and_then(|renewal| renewal.cycle_id)
                    == cycle_id
            })
            .map(|turn| turn.source_ref_id.as_str())
    } else {
        None
    };
    let pending_current_ref = current_records
        .first()
        .filter(|turn| {
            cycle_open
                && evidence_window_open
                && (!is_request_paused
                    || pending_source_ref_id == Some(turn.source_ref_id.as_str()))
                && (pending_source_ref_id.is_none()
                    || (pending_source_ref_id == Some(turn.source_ref_id.as_str())
                        && cycle_follow_up_for_turn(turn, evidence).is_none()))
        })
        .map(|turn| turn.source_ref_id.as_str());
    let total_renewals_current_cycle = current_records.iter().fold(0_u32, |total, turn| {
        total.saturating_add(u32::from(turn.hit_miss.eq_ignore_ascii_case("hit")))
    });
    let mut turns = scoped_turns
        .iter()
        .map(|record| {
            activity_turn(
                record,
                pending_current_ref == Some(record.source_ref_id.as_str()),
                catalog,
                source.item.ttl,
                evidence,
            )
        })
        .collect::<Vec<_>>();
    let mut chronological_order = (0..scoped_turns.len()).collect::<Vec<_>>();
    chronological_order.sort_by(|&left, &right| {
        let left = scoped_turns[left];
        let right = scoped_turns[right];
        pulse_time_ms(left, evidence)
            .cmp(&pulse_time_ms(right, evidence))
            .then_with(|| left.ts.cmp(&right.ts))
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
    let mut cycle_ordinals = HashMap::<Option<Uuid>, u32>::new();
    for &index in &chronological_order {
        let turn = &mut turns[index];
        let ordinal = cycle_ordinals.entry(turn.cycle_id).or_default();
        *ordinal = ordinal.saturating_add(1);
        turn.turn_number = *ordinal;
    }
    let priced_turns = turns
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
        evidence,
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
    let gaps = idle_gaps(
        &source,
        &scoped_turns,
        &turns,
        &chronological_order,
        cycle_id,
        is_active,
        is_request_paused,
        evidence,
    );
    CacheKeepaliveActivityView {
        id: source.item.id.clone(),
        source: source.item.source,
        state,
        reason: reason_display(&source.item.reason, source.item.error.as_deref()),
        ttl: source.item.ttl,
        // Sessions capped by older releases stored one attempt fewer than the
        // renewals they sent; never show fewer attempts than the current
        // cycle's recorded renewals.
        attempts: source.item.refresh_count.map(|stored| {
            let recorded = cycle_id.map_or(0, |cycle_id| {
                source
                    .turns
                    .iter()
                    .filter(|turn| {
                        turn.cycle_id.or_else(|| {
                            renewal_evidence_for_turn(turn, evidence)
                                .and_then(|renewal| renewal.cycle_id)
                        }) == Some(cycle_id)
                    })
                    .count()
            });
            stored.max(u32::try_from(recorded).unwrap_or(u32::MAX))
        }),
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
        total_renewals: total_renewals_current_cycle,
        retained_renewals: turns
            .iter()
            .fold(0_u32, |total, turn| total.saturating_add(turn.renewals)),
        is_last_pending: pending_current_ref.is_some(),
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
            config.map_or(10, |config| config.refresh_lead_time_5m_secs)
        }
        CacheTtl::Ttl1h | CacheTtl::Ttl12h => {
            config.map_or(300, |config| config.refresh_lead_time_1h_secs)
        }
    };
    ttl.as_secs()
        .saturating_sub(u64::from(lead_secs))
        .max(1)
        .saturating_mul(1_000)
}

fn pulse_time_ms(turn: &CacheKeepaliveTurnRecord, evidence: EvidenceContext<'_>) -> u64 {
    renewal_evidence_for_turn(turn, evidence).map_or_else(
        || turn.ts.saturating_mul(1_000),
        |renewal| renewal.request_start_ms,
    )
}

fn renewal_expiry_ms(
    turn: &CacheKeepaliveTurnRecord,
    ttl: CacheTtl,
    evidence: EvidenceContext<'_>,
) -> u64 {
    let ttl = renewal_evidence_for_turn(turn, evidence).map_or(ttl, |renewal| renewal.ttl);
    pulse_time_ms(turn, evidence).saturating_add(ttl.as_secs().saturating_mul(1_000))
}

#[allow(clippy::too_many_arguments)]
fn idle_gaps(
    source: &CacheKeepaliveActivitySource<'_>,
    scoped_turns: &[&CacheKeepaliveTurnRecord],
    retained_turns: &[CacheKeepaliveActivityTurn],
    chronological_order: &[usize],
    current_cycle_id: Option<Uuid>,
    is_active: bool,
    is_request_paused: bool,
    evidence: EvidenceContext<'_>,
) -> Vec<CacheKeepaliveIdleGap> {
    let ordered_turns = chronological_order
        .iter()
        .map(|&index| (index, scoped_turns[index]));
    let config = source.item.config_snapshot.as_ref();
    let max_attempts = config.map_or(12, |config| config.max_refreshes_per_session);
    let mut cycle_indices = HashMap::new();
    let mut cycles: Vec<(Option<Uuid>, Vec<usize>)> = Vec::new();
    let mut open_none_group: Option<usize> = None;
    for (index, turn) in ordered_turns {
        let cycle_id = turn.cycle_id.or_else(|| {
            renewal_evidence_for_turn(turn, evidence).and_then(|renewal| renewal.cycle_id)
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
                    let previous_ttl = renewal_evidence_for_turn(previous_turn, evidence)
                        .map_or(source.item.ttl, |renewal| renewal.ttl);
                    pulse_time_ms(turn, evidence)
                        .saturating_sub(pulse_time_ms(previous_turn, evidence))
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
    let has_current_cycle =
        current_cycle_id.is_some_and(|cycle_id| cycle_indices.contains_key(&cycle_id));
    let mut gaps = Vec::with_capacity(cycles.len() + usize::from(is_active && !has_current_cycle));
    for (cycle_id, indices) in cycles {
        let is_current_cycle = cycle_id.is_some() && cycle_id == current_cycle_id;
        let first_turn = scoped_turns[indices[0]];
        let first_evidence = renewal_evidence_for_turn(first_turn, evidence);
        let ttl = first_evidence.map_or(source.item.ttl, |renewal| renewal.ttl);
        let first_pulse_ms = first_evidence.map_or_else(
            || first_turn.ts.saturating_mul(1_000),
            |renewal| renewal.request_start_ms,
        );
        let cycle = first_evidence.and_then(|renewal| evidence.cycle(renewal));
        let natural_expiry_ms = cycle.as_ref().and_then(|cycle| cycle.natural_expiry_ms);
        let pause_at_ms = cycle
            .as_ref()
            .and_then(|cycle| cycle.anchor_ms)
            .unwrap_or_else(|| first_pulse_ms.saturating_sub(lead_window_ms(ttl, config)));
        let mut previous_time_ms = None;
        let pulses = indices
            .iter()
            .enumerate()
            .map(|(index, &turn_index)| {
                let turn = scoped_turns[turn_index];
                let time_ms = renewal_evidence_for_turn(turn, evidence).map_or_else(
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
        let last_turn = scoped_turns[*indices.last().expect("nonempty cycle")];
        let cache_expires_at_ms = renewal_expiry_ms(last_turn, ttl, evidence);
        let follow_up = cycle.as_ref().and_then(|cycle| cycle.follow_up);
        let evidence_unknown = cycle.as_ref().is_none_or(|cycle| !cycle.known)
            || indices
                .iter()
                .any(|&index| renewal_evidence_for_turn(scoped_turns[index], evidence).is_none());
        let (ending_kind, ending_at_ms) = if evidence_unknown {
            ("followup_unknown", last_pulse_ms)
        } else if let Some(follow_up) = follow_up {
            (
                if natural_expiry_ms.is_some_and(|expiry| follow_up.request_start_ms <= expiry) {
                    "followup_before_expiry"
                } else {
                    "followup_hit"
                },
                follow_up.request_start_ms,
            )
        } else if is_active
            && is_current_cycle
            && indices.iter().any(|&index| retained_turns[index].pending)
        {
            (
                "scheduled_waiting",
                source
                    .session
                    .map_or(last_pulse_ms, |session| {
                        session.run_at_unix_secs.saturating_mul(1_000)
                    })
                    .max(last_pulse_ms),
            )
        } else if is_request_paused
            && is_current_cycle
            && source.now_ms < cache_expires_at_ms
            && indices.iter().any(|&index| retained_turns[index].pending)
        {
            ("followup_pending", source.now_ms.max(last_pulse_ms))
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
            (
                "ttl_expired_no_followup",
                if (is_active || is_request_paused) && is_current_cycle {
                    cache_expires_at_ms
                } else {
                    last_pulse_ms
                },
            )
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
            natural_expiry_ms,
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
        let retained_cycle = current_cycle_id.and_then(|cycle_id| {
            evidence
                .index
                .cycles
                .get(&cycle_id)?
                .iter()
                .find(|cycle| {
                    evidence.records[cycle.representative].principal_id == source.item.principal_id
                })
                .map(|cycle| evidence.resolve(cycle))
        });
        let pause_at_ms = retained_cycle
            .as_ref()
            .and_then(|cycle| cycle.anchor_ms)
            .unwrap_or_else(|| {
                source
                    .session
                    .map_or(source.item.last_message_at_ms, |session| {
                        session.first_scheduled_at_unix_secs.saturating_mul(1_000)
                    })
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
            natural_expiry_ms: retained_cycle.as_ref().map_or_else(
                || {
                    Some(
                        pause_at_ms.saturating_add(source.item.ttl.as_secs().saturating_mul(1_000)),
                    )
                },
                |cycle| cycle.natural_expiry_ms,
            ),
            cached_tokens: 0,
            pulses: Vec::new(),
            ending_kind: if retained_cycle.as_ref().is_some_and(|cycle| !cycle.known) {
                "followup_unknown"
            } else {
                "scheduled_waiting"
            },
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
    evidence: EvidenceContext<'_>,
    pending_source_ref_id: Option<&str>,
) -> (String, u64) {
    let turns = turns.into_iter().collect::<Vec<_>>();
    if turns.is_empty()
        && !evidence
            .records
            .iter()
            .any(|entry| entry.evidence_kind == "renewal")
    {
        return ("none".to_owned(), 0);
    }
    let mut unknown_cycle_ids = HashSet::new();
    let mut unknown_legacy_turns = 0_u64;
    let mut observed = false;
    let mut pending = false;
    for turn in turns {
        let Some(renewal) = renewal_evidence_for_turn(turn, evidence) else {
            if let Some(cycle_id) = turn.cycle_id {
                unknown_cycle_ids.insert(cycle_id);
            } else {
                unknown_legacy_turns = unknown_legacy_turns.saturating_add(1);
            }
            continue;
        };
        if evidence.cycle(renewal).is_none_or(|cycle| !cycle.known) {
            if let Some(cycle_id) = renewal.cycle_id {
                unknown_cycle_ids.insert(cycle_id);
            } else {
                unknown_legacy_turns = unknown_legacy_turns.saturating_add(1);
            }
        } else {
            observed = true;
            if pending_source_ref_id == Some(turn.source_ref_id.as_str())
                && cycle_follow_up_for_turn(turn, evidence).is_none()
            {
                pending = true;
            }
        }
    }
    let unknown_cycles = unknown_legacy_turns
        .saturating_add(u64::try_from(unknown_cycle_ids.len()).unwrap_or(u64::MAX));
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
    evidence: EvidenceContext<'a>,
) -> Option<&'a CacheKeepaliveEvidenceRecord> {
    let (first, duplicates) = evidence
        .index
        .renewal_by_event
        .get(record.source_ref_id.as_str())?;
    std::iter::once(first)
        .chain(duplicates)
        .map(|&index| &evidence.records[index])
        .find(|candidate| {
            candidate.principal_id == record.principal_id
                && candidate.upstream_id == record.upstream_id
                && candidate.model == record.model
        })
}

fn renewal_evidence_known(evidence: &CacheKeepaliveEvidenceRecord) -> bool {
    evidence.cycle_id.is_some()
        && (evidence.prefix_hash.is_some() || evidence.session_key_hash.is_some())
        && evidence.prefix_policy_version.is_some()
        && evidence.prefix_schema_version.is_some()
}

fn follow_up_matches(
    renewal: &CacheKeepaliveEvidenceRecord,
    follow_up: &CacheKeepaliveEvidenceRecord,
) -> bool {
    if !renewal_evidence_known(renewal)
        || follow_up.evidence_kind != "follow_up"
        || follow_up.principal_id != renewal.principal_id
        || follow_up.upstream_id != renewal.upstream_id
        || follow_up.model != renewal.model
        || !follow_up
            .cache_read_input_tokens
            .is_some_and(|tokens| tokens > 0)
        || follow_up.request_start_ms <= renewal.request_start_ms
        || follow_up.request_start_ms
            > renewal
                .request_start_ms
                .saturating_add(renewal.ttl.as_secs().saturating_mul(1_000))
    {
        return false;
    }

    let prefix_matches =
        renewal.prefix_hash.is_some() && follow_up.prefix_hash == renewal.prefix_hash;
    let session_matches = renewal.session_key_hash.is_some()
        && follow_up.session_key_hash == renewal.session_key_hash
        && renewal
            .cache_read_input_tokens
            .is_some_and(|tokens| tokens > 0)
        && follow_up
            .cache_read_input_tokens
            .zip(renewal.cache_read_input_tokens)
            .is_some_and(|(follow_up_tokens, renewal_tokens)| follow_up_tokens >= renewal_tokens);
    prefix_matches || session_matches
}

struct CycleEconomics<'a> {
    known: bool,
    anchor_ms: Option<u64>,
    natural_expiry_ms: Option<u64>,
    follow_up: Option<&'a CacheKeepaliveEvidenceRecord>,
    credited: Option<&'a CacheKeepaliveEvidenceRecord>,
}

struct IndexedCycleEconomics {
    representative: usize,
    known: bool,
    anchor_ms: Option<u64>,
    natural_expiry_ms: Option<u64>,
    follow_up: Option<usize>,
    credited: Option<usize>,
}

pub(crate) struct CacheKeepaliveEvidenceIndex {
    renewal_by_event: HashMap<String, (usize, Vec<usize>)>,
    cycles: HashMap<Uuid, Vec<IndexedCycleEconomics>>,
}

type FollowUpIndex<'a> = HashMap<(&'a str, Uuid, &'a str, &'a str), Vec<usize>>;

impl CacheKeepaliveEvidenceIndex {
    pub(crate) fn new<'a>(
        evidence: &[CacheKeepaliveEvidenceRecord],
        turns: impl IntoIterator<Item = &'a CacheKeepaliveTurnRecord>,
    ) -> Self {
        let turns_by_event = turns
            .into_iter()
            .map(|turn| (turn.source_ref_id.as_str(), turn))
            .collect::<HashMap<_, _>>();
        let mut renewal_by_event = HashMap::<String, (usize, Vec<usize>)>::new();
        let mut renewals_by_cycle = HashMap::<(Uuid, &str, Uuid, &str), Vec<usize>>::new();
        let mut by_prefix = FollowUpIndex::new();
        let mut by_session = FollowUpIndex::new();
        for (index, record) in evidence.iter().enumerate() {
            if record.evidence_kind == "renewal" {
                if let Some(event_ref) = &record.event_ref {
                    renewal_by_event
                        .entry(event_ref.clone())
                        .and_modify(|(_, duplicates)| duplicates.push(index))
                        .or_insert_with(|| (index, Vec::new()));
                }
                if let Some(cycle_id) = record.cycle_id {
                    renewals_by_cycle
                        .entry((
                            cycle_id,
                            record.principal_id.as_str(),
                            record.upstream_id,
                            record.model.as_str(),
                        ))
                        .or_default()
                        .push(index);
                }
            } else if record.evidence_kind == "follow_up"
                && record
                    .cache_read_input_tokens
                    .is_some_and(|tokens| tokens > 0)
            {
                let route = (
                    record.principal_id.as_str(),
                    record.upstream_id,
                    record.model.as_str(),
                );
                if let Some(prefix) = record.prefix_hash.as_deref() {
                    by_prefix
                        .entry((route.0, route.1, route.2, prefix))
                        .or_default()
                        .push(index);
                }
                if let Some(session) = record.session_key_hash.as_deref() {
                    by_session
                        .entry((route.0, route.1, route.2, session))
                        .or_default()
                        .push(index);
                }
            }
        }
        for candidates in by_prefix.values_mut().chain(by_session.values_mut()) {
            candidates.sort_unstable_by_key(|&index| {
                (
                    evidence[index].request_start_ms,
                    &evidence[index].evidence_id,
                )
            });
        }
        let kept_tokens = |renewal: &CacheKeepaliveEvidenceRecord| {
            renewal
                .event_ref
                .as_deref()
                .and_then(|event_ref| turns_by_event.get(event_ref))
                .filter(|turn| {
                    turn.principal_id == renewal.principal_id
                        && turn.upstream_id == renewal.upstream_id
                        && turn.model == renewal.model
                })
                .map_or(renewal.cache_read_input_tokens.unwrap_or(0), |turn| {
                    turn.cache_read_input_tokens
                        .saturating_add(turn.cache_creation_input_tokens)
                })
        };
        let earliest_in_window = |renewal: &CacheKeepaliveEvidenceRecord, candidates: &[usize]| {
            let start = candidates.partition_point(|&index| {
                evidence[index].request_start_ms <= renewal.request_start_ms
            });
            let expiry = renewal
                .request_start_ms
                .saturating_add(renewal.ttl.as_secs().saturating_mul(1_000));
            candidates[start..]
                .iter()
                .copied()
                .take_while(|&index| evidence[index].request_start_ms <= expiry)
                .find(|&index| follow_up_matches(renewal, &evidence[index]))
        };
        let mut cycles = HashMap::<Uuid, Vec<IndexedCycleEconomics>>::new();
        for ((cycle_id, principal, upstream_id, model), renewals) in renewals_by_cycle {
            let representative = *renewals
                .iter()
                .min_by_key(|&&index| {
                    (
                        evidence[index].request_start_ms,
                        &evidence[index].evidence_id,
                    )
                })
                .expect("nonempty cycle");
            let first = &evidence[representative];
            let anchor_ms = renewals
                .iter()
                .filter_map(|&index| evidence[index].cycle_anchor_ms)
                .min();
            let known = anchor_ms.is_some()
                && renewals
                    .iter()
                    .all(|&index| renewal_evidence_known(&evidence[index]));
            let natural_expiry_ms = anchor_ms
                .map(|anchor| anchor.saturating_add(first.ttl.as_secs().saturating_mul(1_000)));
            let mut follow_up = None;
            for &index in &renewals {
                let renewal = &evidence[index];
                if kept_tokens(renewal) == 0 {
                    continue;
                }
                let prefix_candidates = renewal
                    .prefix_hash
                    .as_deref()
                    .and_then(|prefix| by_prefix.get(&(principal, upstream_id, model, prefix)));
                let session_candidates = renewal
                    .session_key_hash
                    .as_deref()
                    .and_then(|session| by_session.get(&(principal, upstream_id, model, session)));
                for candidates in prefix_candidates.into_iter().chain(session_candidates) {
                    if let Some(candidate) = earliest_in_window(renewal, candidates)
                        && follow_up.is_none_or(|current: usize| {
                            (
                                evidence[candidate].request_start_ms,
                                &evidence[candidate].evidence_id,
                            ) < (
                                evidence[current].request_start_ms,
                                &evidence[current].evidence_id,
                            )
                        })
                    {
                        follow_up = Some(candidate);
                    }
                }
            }
            let credited = follow_up
                .filter(|&index| {
                    known
                        && natural_expiry_ms
                            .is_some_and(|expiry| evidence[index].request_start_ms > expiry)
                })
                .and_then(|follow_up| {
                    renewals
                        .iter()
                        .copied()
                        .filter(|&index| {
                            kept_tokens(&evidence[index]) > 0
                                && follow_up_matches(&evidence[index], &evidence[follow_up])
                        })
                        .min_by_key(|&index| {
                            (
                                evidence[index].request_start_ms,
                                &evidence[index].evidence_id,
                            )
                        })
                });
            cycles
                .entry(cycle_id)
                .or_default()
                .push(IndexedCycleEconomics {
                    representative,
                    known,
                    anchor_ms,
                    natural_expiry_ms,
                    follow_up,
                    credited,
                });
        }
        Self {
            renewal_by_event,
            cycles,
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_cycle_count(&self) -> usize {
        self.cycles.values().map(Vec::len).sum()
    }
}

#[derive(Clone, Copy)]
struct EvidenceContext<'a> {
    records: &'a [CacheKeepaliveEvidenceRecord],
    index: &'a CacheKeepaliveEvidenceIndex,
}

impl<'a> EvidenceContext<'a> {
    fn resolve(self, cycle: &IndexedCycleEconomics) -> CycleEconomics<'a> {
        CycleEconomics {
            known: cycle.known,
            anchor_ms: cycle.anchor_ms,
            natural_expiry_ms: cycle.natural_expiry_ms,
            follow_up: cycle.follow_up.map(|index| &self.records[index]),
            credited: cycle.credited.map(|index| &self.records[index]),
        }
    }

    fn cycle(self, renewal: &CacheKeepaliveEvidenceRecord) -> Option<CycleEconomics<'a>> {
        self.index
            .cycles
            .get(&renewal.cycle_id?)?
            .iter()
            .find(|cycle| {
                let representative = &self.records[cycle.representative];
                representative.principal_id == renewal.principal_id
                    && representative.upstream_id == renewal.upstream_id
                    && representative.model == renewal.model
            })
            .map(|cycle| self.resolve(cycle))
    }
}

fn cycle_follow_up_for_turn<'a>(
    record: &CacheKeepaliveTurnRecord,
    evidence: EvidenceContext<'a>,
) -> Option<&'a CacheKeepaliveEvidenceRecord> {
    let renewal = renewal_evidence_for_turn(record, evidence)?;
    evidence.cycle(renewal)?.follow_up
}

fn activity_turn(
    record: &CacheKeepaliveTurnRecord,
    pending: bool,
    catalog: &PriceCatalog,
    ttl: CacheTtl,
    evidence: EvidenceContext<'_>,
) -> CacheKeepaliveActivityTurn {
    let renewal_evidence = renewal_evidence_for_turn(record, evidence);
    let cycle = renewal_evidence.and_then(|renewal| evidence.cycle(renewal));
    let evidence_known = cycle.as_ref().is_some_and(|cycle| cycle.known);
    let hit = record.hit_miss.eq_ignore_ascii_case("hit");
    let followed_up = !pending
        && cycle
            .as_ref()
            .and_then(|cycle| cycle.credited)
            .and_then(|renewal| renewal.event_ref.as_deref())
            == Some(record.source_ref_id.as_str());
    let avoided_tokens = cycle
        .as_ref()
        .and_then(|cycle| cycle.follow_up)
        .and_then(|follow_up| follow_up.cache_read_input_tokens)
        .unwrap_or(0)
        .min(
            record
                .cache_read_input_tokens
                .saturating_add(record.cache_creation_input_tokens),
        );
    let ttl = renewal_evidence.map_or(ttl, |renewal| renewal.ttl);
    let pnl = evidence_known
        .then_some(record.cost_micros)
        .flatten()
        .and_then(|spent_micros| {
            let rates = if followed_up {
                rates_from_catalog(catalog, &record.model, ttl)
            } else {
                Some(economics::CacheKeepalivePnlRates {
                    avoided_micros_per_million: 0,
                })
            };
            rates.map(|rates| {
                derive_turn_pnl(
                    CacheKeepaliveTurnPnlInput {
                        renewal_tokens: avoided_tokens,
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
    } else if cycle.as_ref().is_some_and(|cycle| {
        cycle.follow_up.is_some_and(|follow_up| {
            cycle
                .natural_expiry_ms
                .is_some_and(|expiry| follow_up.request_start_ms <= expiry)
        })
    }) {
        "follow-up arrived before cache expiry (renewal not needed)"
    } else if !followed_up && cycle.as_ref().is_some_and(|cycle| cycle.credited.is_some()) {
        "redundant renewal (an earlier renewal kept the cache warm)"
    } else {
        turn_action(followed_up, pending)
    };
    let pnl_display = pnl.map_or_else(|| "-".to_owned(), format_turn_pnl);
    CacheKeepaliveActivityTurn {
        source_ref_id: record.source_ref_id.clone(),
        cycle_id: record
            .cycle_id
            .or_else(|| renewal_evidence.and_then(|renewal| renewal.cycle_id)),
        // Assigned after chronological ordering in derive_activity_view.
        turn_number: 0,
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
