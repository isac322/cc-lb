use std::collections::{BTreeSet, HashMap};

mod serialize;
mod wire;

use crate::cache_keepalive_view::{CacheKeepaliveActivitySource, derive_activity_view};
use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveTurnRecord, Storage, StorageError, StorageResult, UpstreamStore,
};
use uuid::Uuid;

use serialize::{config_snapshot, dollars_from_micros, raw_record, ttl_name};
use wire::CacheKeepaliveTurnResponse;
pub(crate) use wire::{
    CacheKeepaliveDetailResponse, CacheKeepaliveListResponse, CacheKeepaliveRowResponse,
    CacheKeepaliveSummaryResponse,
};

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
}

impl CacheKeepaliveActivityBatch {
    pub(crate) fn from_turns(turns: Vec<CacheKeepaliveTurnRecord>) -> Self {
        let mut turns_by_session: HashMap<String, Vec<CacheKeepaliveTurnRecord>> = HashMap::new();
        for turn in turns {
            turns_by_session
                .entry(turn.session_key_hash.clone())
                .or_default()
                .push(turn);
        }
        Self { turns_by_session }
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
    Ok(CacheKeepaliveActivityBatch::from_turns(turns))
}

pub(crate) fn summary_for_items(
    catalog: &PriceCatalog,
    now_ms: u64,
    items: &[CacheKeepaliveSessionListItem],
    batch: &CacheKeepaliveActivityBatch,
) -> StorageResult<CacheKeepaliveSummaryResponse> {
    let mut renewing_now = 0_u64;
    let mut sessions_last_5m = 0_u64;
    let mut renewals_fired = 0_u64;
    let mut cost_saved_micros = 0_i64;
    let cutoff = now_ms.saturating_sub(FIVE_MINUTES_MS);
    for item in items {
        if item.last_message_at_ms >= cutoff {
            sessions_last_5m = sessions_last_5m.saturating_add(1);
        }
        if matches!(item.status, Some(CacheKeepaliveSessionStatus::Active)) {
            renewing_now = renewing_now.saturating_add(1);
        }
        renewals_fired = renewals_fired.saturating_add(u64::from(item.refresh_count.unwrap_or(0)));
        // Summary P&L depends only on list items and turns; never load encrypted sessions.
        let activity = activity_for_item(catalog, now_ms, item, None, batch)?;
        if let Some(pnl) = activity.pnl {
            cost_saved_micros = cost_saved_micros.saturating_add(pnl.net_micros);
        }
    }
    Ok(CacheKeepaliveSummaryResponse {
        renewing_now,
        sessions_last_5m,
        renewals_fired,
        cost_saved: dollars_from_micros(cost_saved_micros)?,
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
    let total_avoided = loaded.activity.pnl.map_or(0, |pnl| pnl.avoided_micros);
    let total_spent = loaded.activity.pnl.map_or(0, |pnl| pnl.spent_micros);
    let renewal_tokens = loaded.activity.turns.iter().fold(0_u64, |total, turn| {
        total.saturating_add(turn.renewal_tokens)
    });
    let turns = loaded
        .activity
        .turns
        .into_iter()
        .map(|turn| {
            Ok(CacheKeepaliveTurnResponse {
                turn_number: turn.turn_number,
                renewals: turn.renewals,
                followed_up: turn.followed_up,
                label: turn.action.to_owned(),
                time_ms: turn.time_ms,
                pnl: turn
                    .pnl
                    .map(|pnl| dollars_from_micros(pnl.net_micros))
                    .transpose()?,
                pending: turn.pending,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(CacheKeepaliveDetailResponse {
        renewal_tokens,
        total_avoided: dollars_from_micros(total_avoided)?,
        total_spent: dollars_from_micros(total_spent)?,
        total_renewals: loaded.activity.total_renewals,
        is_last_pending: loaded.activity.is_last_pending,
        config_snapshot: item.config_snapshot.as_ref().map(config_snapshot),
        raw_record: raw_record(item, loaded.session.as_ref()),
        row,
        turns,
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
        generation: item.generation,
        upstream,
        error: item.error.clone(),
        net_pnl: dollars_from_micros(activity.pnl.map_or(0, |pnl| pnl.net_micros))?,
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
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item,
            session: session.as_ref(),
            turns: &turns,
            now_ms: context.now_ms,
        },
        context.catalog,
    );
    Ok(LoadedActivity { activity, session })
}
