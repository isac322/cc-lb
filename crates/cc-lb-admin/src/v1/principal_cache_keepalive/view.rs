use std::collections::{BTreeSet, HashMap};

mod serialize;
mod wire;

use crate::cache_keepalive_view::{CacheKeepaliveActivitySource, derive_activity_view};
use cc_lb_pricing::PriceCatalog;
#[cfg(test)]
use cc_lb_storage_api::CacheKeepaliveSessionStatus;
use cc_lb_storage_api::{
    CacheKeepaliveEvidenceRecord, CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSessionRecord, CacheKeepaliveTurnRecord, Storage,
    StorageError, StorageResult, UpstreamStore,
};
use uuid::Uuid;

use serialize::{config_snapshot, dollars_from_micros, raw_record, ttl_name};
pub(crate) use wire::{
    CacheKeepaliveDetailResponse, CacheKeepaliveListResponse, CacheKeepaliveRowResponse,
    CacheKeepaliveSummaryResponse,
};
use wire::{CacheKeepaliveGapResponse, CacheKeepalivePulseResponse, CacheKeepaliveTurnResponse};

pub(super) const FIVE_MINUTES_MS: u64 = 5 * 60 * 1_000;

pub(super) struct CacheKeepaliveViewContext<'a> {
    pub(super) storage: &'a dyn Storage,
    pub(super) catalog: &'a PriceCatalog,
    pub(super) now_ms: u64,
    pub(super) max_attempts: u32,
}

struct LoadedActivity {
    activity: crate::cache_keepalive_view::CacheKeepaliveActivityView,
    session: Option<CacheKeepaliveSessionRecord>,
}

pub(crate) struct CacheKeepaliveActivityBatch {
    turns_by_session: HashMap<String, Vec<CacheKeepaliveTurnRecord>>,
    evidence: Vec<CacheKeepaliveEvidenceRecord>,
}

impl CacheKeepaliveActivityBatch {
    pub(crate) fn from_turns(
        turns: Vec<CacheKeepaliveTurnRecord>,
        evidence: Vec<CacheKeepaliveEvidenceRecord>,
    ) -> Self {
        let mut turns_by_session: HashMap<String, Vec<CacheKeepaliveTurnRecord>> = HashMap::new();
        for turn in turns {
            turns_by_session
                .entry(turn.session_key_hash.clone())
                .or_default()
                .push(turn);
        }
        Self {
            turns_by_session,
            evidence,
        }
    }

    fn turns_for_item(
        &self,
        item: &CacheKeepaliveSessionListItem,
    ) -> StorageResult<&[CacheKeepaliveTurnRecord]> {
        let CacheKeepaliveSessionEntrySource::Session = item.source else {
            return Ok(&[]);
        };
        let session_key_hash =
            item.session_key_hash
                .as_deref()
                .ok_or_else(|| StorageError::InvalidInput {
                    field: "cache_keepalive_session_key_hash".to_owned(),
                    reason: "session entry is missing its session key hash".to_owned(),
                })?;
        let turns = match self.turns_by_session.get(session_key_hash) {
            Some(turns) => turns.as_slice(),
            None => &[],
        };
        Ok(turns)
    }
}

pub(super) async fn load_activity_batch<'a>(
    storage: &dyn Storage,
    principal_id: &str,
    items: impl IntoIterator<Item = &'a CacheKeepaliveSessionListItem>,
) -> StorageResult<CacheKeepaliveActivityBatch> {
    let mut session_key_hashes = Vec::new();
    for item in items {
        match item.source {
            CacheKeepaliveSessionEntrySource::Session => {
                session_key_hashes.push(item.session_key_hash.clone().ok_or_else(|| {
                    StorageError::InvalidInput {
                        field: "cache_keepalive_session_key_hash".to_owned(),
                        reason: "session entry is missing its session key hash".to_owned(),
                    }
                })?);
            }
            CacheKeepaliveSessionEntrySource::Decision => {}
        }
    }
    let turns = CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions(
        storage,
        principal_id,
        &session_key_hashes,
    )
    .await?;
    let evidence = match evidence_scope(&turns) {
        Some((cycle_ids, source_ref_ids, from_ms, until_ms)) => {
            CacheKeepaliveSessionReadStore::list_cache_keepalive_evidence_for_activity(
                storage,
                principal_id,
                &cycle_ids,
                &source_ref_ids,
                from_ms,
                until_ms,
            )
            .await?
        }
        None => Vec::new(),
    };
    Ok(CacheKeepaliveActivityBatch::from_turns(turns, evidence))
}

fn evidence_scope(
    turns: &[CacheKeepaliveTurnRecord],
) -> Option<(Vec<Uuid>, Vec<String>, u64, u64)> {
    let first = turns.first()?;
    let mut cycle_ids = BTreeSet::new();
    let mut source_ref_ids = BTreeSet::new();
    let mut from_ms = first.ts.saturating_mul(1_000);
    let mut until_ms = from_ms;
    for turn in turns {
        if let Some(cycle_id) = turn.cycle_id {
            cycle_ids.insert(cycle_id);
        }
        source_ref_ids.insert(turn.source_ref_id.clone());
        let start_ms = turn.ts.saturating_mul(1_000);
        from_ms = from_ms.min(start_ms);
        until_ms = until_ms.max(start_ms.saturating_add(60 * 60 * 1_000));
    }
    Some((
        cycle_ids.into_iter().collect(),
        source_ref_ids.into_iter().collect(),
        from_ms,
        until_ms,
    ))
}
#[cfg(test)]
pub(crate) fn summary_for_items(
    catalog: &PriceCatalog,
    now_ms: u64,
    items: &[CacheKeepaliveSessionListItem],
    batch: &CacheKeepaliveActivityBatch,
) -> StorageResult<CacheKeepaliveSummaryResponse> {
    let mut renewing_now = 0_u64;
    let mut sessions_last_5m = 0_u64;
    let mut renewals_fired = 0_u64;
    let mut total_spent_micros = 0_i64;
    let mut total_avoided_micros = 0_i64;
    let mut cost_saved_micros = 0_i64;
    let mut unknown_cycles = 0_u64;
    let mut evidence_status = "none";
    let cutoff = now_ms.saturating_sub(FIVE_MINUTES_MS);
    let mut retained_from_ms: Option<u64> = None;
    for evidence in &batch.evidence {
        retained_from_ms = Some(retained_from_ms.map_or(evidence.request_start_ms, |value| {
            value.min(evidence.request_start_ms)
        }));
    }
    for item in items {
        if item.last_message_at_ms >= cutoff {
            sessions_last_5m = sessions_last_5m.saturating_add(1);
        }
        if matches!(item.status, Some(CacheKeepaliveSessionStatus::Active)) {
            renewing_now = renewing_now.saturating_add(1);
        }
        let activity = activity_for_item(catalog, now_ms, item, None, batch)?;
        total_spent_micros = total_spent_micros.saturating_add(activity.total_spent_micros);
        unknown_cycles = unknown_cycles.saturating_add(activity.unknown_cycles);
        if activity.evidence_status == "unknown" {
            evidence_status = "unknown";
        } else if activity.evidence_status == "pending" && evidence_status != "unknown" {
            evidence_status = "pending";
        } else if activity.evidence_status == "observed" && evidence_status == "none" {
            evidence_status = "observed";
        }
        renewals_fired = renewals_fired.saturating_add(u64::from(activity.total_renewals));
        if let Some(pnl) = activity.pnl {
            cost_saved_micros = cost_saved_micros.saturating_add(pnl.net_micros);
            total_avoided_micros = total_avoided_micros.saturating_add(pnl.avoided_micros);
        }
    }
    let economics_known = evidence_status == "observed";
    Ok(CacheKeepaliveSummaryResponse {
        renewing_now,
        sessions_last_5m,
        renewals_fired,
        retained_successful_hits: renewals_fired,
        cost_saved: economics_known
            .then(|| dollars_from_micros(cost_saved_micros))
            .transpose()?,
        resolved_cost_saved: None,
        total_spent: dollars_from_micros(total_spent_micros)?,
        total_avoided: economics_known
            .then(|| dollars_from_micros(total_avoided_micros))
            .transpose()?,
        resolved_total_avoided: None,
        evidence_status: evidence_status.to_owned(),
        unknown_cycles,
        retained_from_ms,
    })
}

pub(super) fn row_for_item(
    context: &CacheKeepaliveViewContext<'_>,
    item: &CacheKeepaliveSessionListItem,
    batch: &CacheKeepaliveActivityBatch,
    upstream_names: &HashMap<Uuid, String>,
) -> StorageResult<CacheKeepaliveRowResponse> {
    // List rows do not expose session-only next-renewal/raw fields.
    let activity = activity_for_item(context.catalog, context.now_ms, item, None, batch)?;
    row_from_activity(
        context.max_attempts,
        item,
        &activity,
        upstream_names.get(&item.upstream_id).cloned(),
    )
}

pub(super) async fn list_upstream_names(
    storage: &dyn Storage,
    items: &[CacheKeepaliveSessionListItem],
) -> StorageResult<HashMap<Uuid, String>> {
    if items.is_empty() {
        return Ok(HashMap::new());
    }
    let upstream_ids = items
        .iter()
        .map(|item| item.upstream_id)
        .collect::<BTreeSet<_>>();
    let mut upstream_names = HashMap::with_capacity(upstream_ids.len());
    for upstream_id in upstream_ids {
        if let Some(upstream) = UpstreamStore::get_by_id(storage, upstream_id).await? {
            upstream_names.insert(upstream_id, upstream.name);
        }
    }
    Ok(upstream_names)
}

pub(super) async fn detail_for_item(
    context: &CacheKeepaliveViewContext<'_>,
    item: &CacheKeepaliveSessionListItem,
) -> StorageResult<CacheKeepaliveDetailResponse> {
    let loaded = load_detail_activity(context, item).await?;
    let upstream = UpstreamStore::get_by_id(context.storage, item.upstream_id)
        .await?
        .map(|upstream| upstream.name);
    let row = row_from_activity(context.max_attempts, item, &loaded.activity, upstream)?;
    let renewal_tokens = loaded.activity.turns.iter().fold(0_u64, |total, turn| {
        total.saturating_add(turn.renewal_tokens)
    });
    let turns = loaded
        .activity
        .turns
        .into_iter()
        .map(|turn| {
            let turn_evidence_known = turn.action != "follow-up evidence unavailable";
            let followed_up = turn_evidence_known.then_some(turn.followed_up);
            Ok(CacheKeepaliveTurnResponse {
                turn_number: turn.turn_number,
                renewals: turn.renewals,
                followed_up,
                evidence_status: if turn_evidence_known {
                    if turn.pending {
                        "pending".to_owned()
                    } else {
                        "observed".to_owned()
                    }
                } else {
                    "unknown".to_owned()
                },
                label: turn.action.to_owned(),
                time_ms: turn.time_ms,
                pnl: if turn_evidence_known {
                    turn.pnl
                        .map(|pnl| dollars_from_micros(pnl.net_micros))
                        .transpose()?
                } else {
                    None
                },
                pending: turn.pending,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let gaps = loaded
        .activity
        .gaps
        .into_iter()
        .map(|gap| {
            let pulses = gap
                .pulses
                .into_iter()
                .map(|pulse| {
                    Ok(CacheKeepalivePulseResponse {
                        pulse_number: pulse.pulse_number,
                        time_ms: pulse.time_ms,
                        offset_sec: pulse.offset_sec,
                        since_prev_renewal_sec: pulse.since_prev_renewal_sec,
                        spent_usd: pulse.spent_micros.map(dollars_from_micros).transpose()?,
                        hit: pulse.hit,
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(CacheKeepaliveGapResponse {
                cycle_id: gap.cycle_id.map(|cycle_id| cycle_id.to_string()),
                pause_at_ms: gap.pause_at_ms,
                cached_tokens: gap.cached_tokens,
                pulses,
                ending_kind: gap.ending_kind,
                ending_at_ms: gap.ending_at_ms,
                total_duration_sec: gap.total_duration_sec,
                since_last_renewal_sec: gap.since_last_renewal_sec,
                spent_usd: dollars_from_micros(gap.spent_micros)?,
                avoided_usd: gap.avoided_micros.map(dollars_from_micros).transpose()?,
                net_pnl_usd: gap.net_pnl_micros.map(dollars_from_micros).transpose()?,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(CacheKeepaliveDetailResponse {
        renewal_tokens,
        total_renewals_current_cycle: loaded.activity.total_renewals,
        is_last_pending: loaded.activity.is_last_pending,
        config_snapshot: item.config_snapshot.as_ref().map(config_snapshot),
        raw_record: raw_record(item, loaded.session.as_ref()),
        row,
        turns,
        gaps,
    })
}

pub(crate) fn row_from_activity(
    max_attempts: u32,
    item: &CacheKeepaliveSessionListItem,
    activity: &crate::cache_keepalive_view::CacheKeepaliveActivityView,
    upstream: Option<String>,
) -> StorageResult<CacheKeepaliveRowResponse> {
    Ok(CacheKeepaliveRowResponse {
        id: item.id.clone(),
        last_message_at_ms: item.last_message_at_ms,
        state: activity.state,
        ttl: Some(ttl_name(item.ttl)),
        attempts: item.refresh_count,
        max_attempts: item
            .config_snapshot
            .as_ref()
            .map_or(max_attempts, |config| config.max_refreshes_per_session),
        reason: item.reason.clone(),
        display_reason: item.display_reason.clone(),
        generation: item.generation,
        upstream,
        error: item.error.clone(),
        is_active: activity.is_active,
        retained_renewals: activity.retained_renewals,
        net_pnl: activity
            .pnl
            .filter(|_| activity.evidence_status == "observed")
            .map(|pnl| dollars_from_micros(pnl.net_micros))
            .transpose()?,
        total_spent: dollars_from_micros(activity.total_spent_micros)?,
        total_avoided: activity
            .total_avoided_micros
            .map(dollars_from_micros)
            .transpose()?,
        evidence_status: activity.evidence_status.clone(),
        session_key_hash: item
            .session_key_hash
            .clone()
            .unwrap_or_else(|| item.id.clone()),
    })
}

pub(crate) fn activity_for_item(
    catalog: &PriceCatalog,
    now_ms: u64,
    item: &CacheKeepaliveSessionListItem,
    session: Option<&CacheKeepaliveSessionRecord>,
    batch: &CacheKeepaliveActivityBatch,
) -> StorageResult<crate::cache_keepalive_view::CacheKeepaliveActivityView> {
    Ok(derive_activity_view(
        CacheKeepaliveActivitySource {
            item,
            session,
            turns: batch.turns_for_item(item)?,
            evidence: &batch.evidence,
            now_ms,
        },
        catalog,
    ))
}

async fn load_detail_activity(
    context: &CacheKeepaliveViewContext<'_>,
    item: &CacheKeepaliveSessionListItem,
) -> StorageResult<LoadedActivity> {
    let (session, turns) = match item.source {
        CacheKeepaliveSessionEntrySource::Session => {
            let session_key_hash =
                item.session_key_hash
                    .as_deref()
                    .ok_or_else(|| StorageError::InvalidInput {
                        field: "cache_keepalive_session_key_hash".to_owned(),
                        reason: "session entry is missing its session key hash".to_owned(),
                    })?;
            let session =
                CacheKeepaliveSessionReadStore::get_cache_keepalive_session_for_principal(
                    context.storage,
                    &item.principal_id,
                    session_key_hash,
                )
                .await?;
            let turns = CacheKeepaliveSessionReadStore::list_cache_keepalive_turns(
                context.storage,
                &item.principal_id,
                session_key_hash,
            )
            .await?;
            (session, turns)
        }
        CacheKeepaliveSessionEntrySource::Decision => (None, Vec::new()),
    };
    let evidence = match evidence_scope(&turns) {
        Some((cycle_ids, source_ref_ids, from_ms, until_ms)) => {
            CacheKeepaliveSessionReadStore::list_cache_keepalive_evidence_for_activity(
                context.storage,
                &item.principal_id,
                &cycle_ids,
                &source_ref_ids,
                from_ms,
                until_ms,
            )
            .await?
        }
        None => Vec::new(),
    };
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item,
            session: session.as_ref(),
            turns: &turns,
            evidence: &evidence,
            now_ms: context.now_ms,
        },
        context.catalog,
    );
    Ok(LoadedActivity { activity, session })
}
