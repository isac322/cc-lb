mod serialize;
mod wire;

use crate::cache_keepalive_view::{CacheKeepaliveActivitySource, derive_activity_view};
use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::{
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionFilter, CacheKeepaliveSessionListItem,
    CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore, CacheKeepaliveSessionRecord,
    CacheKeepaliveSessionStatus, Storage, StorageError, StorageResult, UpstreamStore,
};

use serialize::{config_snapshot, dollars_from_micros, raw_record, ttl_name};
use wire::CacheKeepaliveTurnResponse;
pub(crate) use wire::{
    CacheKeepaliveDetailResponse, CacheKeepaliveListResponse, CacheKeepaliveRowResponse,
    CacheKeepaliveSummaryResponse,
};

const SUMMARY_PAGE_LIMIT: u32 = 1_000;
const FIVE_MINUTES_MS: u64 = 5 * 60 * 1_000;

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

pub(super) async fn list_all(
    storage: &dyn Storage,
    principal_id: &str,
) -> StorageResult<Vec<CacheKeepaliveSessionListItem>> {
    let mut cursor = None;
    let mut items = Vec::new();
    loop {
        let page = CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions(
            storage,
            &CacheKeepaliveSessionListQuery {
                principal_id: principal_id.to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                cursor,
                limit: SUMMARY_PAGE_LIMIT,
            },
        )
        .await?;
        items.extend(page.rows);
        let Some(next_cursor) = page.next_cursor else {
            return Ok(items);
        };
        cursor = Some(next_cursor);
    }
}

pub(super) async fn summary_for_items(
    context: &CacheKeepaliveViewContext<'_>,
    items: &[CacheKeepaliveSessionListItem],
) -> StorageResult<CacheKeepaliveSummaryResponse> {
    let mut renewing_now = 0_u64;
    let mut sessions_last_5m = 0_u64;
    let mut renewals_fired = 0_u64;
    let mut cost_saved_micros = 0_i64;
    let cutoff = context.now_ms.saturating_sub(FIVE_MINUTES_MS);
    for item in items {
        if item.last_message_at_ms >= cutoff {
            sessions_last_5m = sessions_last_5m.saturating_add(1);
        }
        if matches!(item.status, Some(CacheKeepaliveSessionStatus::Active)) {
            renewing_now = renewing_now.saturating_add(1);
        }
        renewals_fired = renewals_fired.saturating_add(u64::from(item.refresh_count.unwrap_or(0)));
        let loaded = load_activity(context, item).await?;
        if let Some(pnl) = loaded.activity.pnl {
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

pub(super) async fn row_for_item(
    context: &CacheKeepaliveViewContext<'_>,
    item: &CacheKeepaliveSessionListItem,
) -> StorageResult<CacheKeepaliveRowResponse> {
    let loaded = load_activity(context, item).await?;
    row_from_loaded(context, item, &loaded).await
}

pub(super) async fn detail_for_item(
    context: &CacheKeepaliveViewContext<'_>,
    item: &CacheKeepaliveSessionListItem,
) -> StorageResult<CacheKeepaliveDetailResponse> {
    let loaded = load_activity(context, item).await?;
    let row = row_from_loaded(context, item, &loaded).await?;
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

async fn row_from_loaded(
    context: &CacheKeepaliveViewContext<'_>,
    item: &CacheKeepaliveSessionListItem,
    loaded: &LoadedActivity,
) -> StorageResult<CacheKeepaliveRowResponse> {
    Ok(CacheKeepaliveRowResponse {
        id: item.id.clone(),
        last_message_at_ms: item.last_message_at_ms,
        state: loaded.activity.state,
        ttl: Some(ttl_name(item.ttl)),
        attempts: item.refresh_count,
        max_attempts: item
            .config_snapshot
            .as_ref()
            .map_or(context.max_attempts, |config| {
                config.max_refreshes_per_session
            }),
        reason: item.reason.clone(),
        generation: item.generation,
        upstream: UpstreamStore::get_by_id(context.storage, item.upstream_id)
            .await?
            .map(|upstream| upstream.name),
        error: item.error.clone(),
        net_pnl: dollars_from_micros(loaded.activity.pnl.map_or(0, |pnl| pnl.net_micros))?,
        session_key_hash: item
            .session_key_hash
            .clone()
            .unwrap_or_else(|| item.id.clone()),
    })
}

async fn load_activity(
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
