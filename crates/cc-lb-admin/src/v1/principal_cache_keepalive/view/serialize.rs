use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveSessionListItem, CacheKeepaliveSessionRecord,
    CacheTtl, StorageError, StorageResult,
};

use super::wire::{CacheKeepaliveConfigSnapshotResponse, CacheKeepaliveRawRecordResponse};

pub(super) fn raw_record(
    item: &CacheKeepaliveSessionListItem,
    session: Option<&CacheKeepaliveSessionRecord>,
) -> CacheKeepaliveRawRecordResponse {
    match session {
        Some(session) => CacheKeepaliveRawRecordResponse {
            session_key_hash: session.session_key_hash.clone(),
            principal_id: session.principal_id.clone(),
            upstream_id: Some(session.upstream_id.to_string()),
            generation: session.generation,
            renewal_count: session.refresh_count,
            ttl: Some(ttl_name(session.ttl)),
            status: session.status.as_str().to_owned(),
            enqueue_state: Some(session.enqueue_state.as_str().to_owned()),
        },
        None => CacheKeepaliveRawRecordResponse {
            session_key_hash: item
                .session_key_hash
                .clone()
                .unwrap_or_else(|| item.id.clone()),
            principal_id: item.principal_id.clone(),
            upstream_id: Some(item.upstream_id.to_string()),
            generation: item.generation,
            renewal_count: item.refresh_count.unwrap_or(0),
            ttl: Some(ttl_name(item.ttl)),
            status: item
                .decision
                .clone()
                .unwrap_or_else(|| "not_tracked".to_owned()),
            enqueue_state: item.enqueue_state.map(|state| state.as_str().to_owned()),
        },
    }
}

pub(super) fn config_snapshot(
    config: &CacheKeepaliveConfigSnapshot,
) -> CacheKeepaliveConfigSnapshotResponse {
    CacheKeepaliveConfigSnapshotResponse {
        lead_5m: config.refresh_lead_time_5m_secs,
        lead_1h: config.refresh_lead_time_1h_secs,
        max_renewals: config.max_refreshes_per_session,
        max_duration: config.max_total_duration_secs,
        snapshot_bytes: config.snapshot_max_bytes,
    }
}

pub(super) fn ttl_name(ttl: CacheTtl) -> &'static str {
    match ttl {
        CacheTtl::Ttl5m => "5m",
        CacheTtl::Ttl1h => "1h",
    }
}

pub(super) fn dollars_from_micros(micros: i64) -> StorageResult<f64> {
    let whole =
        i32::try_from(micros.div_euclid(1_000_000)).map_err(|_| StorageError::InvalidInput {
            field: "cache_keepalive_pnl".to_owned(),
            reason: "amount exceeds the API dollar range".to_owned(),
        })?;
    let fraction =
        u32::try_from(micros.rem_euclid(1_000_000)).map_err(|_| StorageError::InvalidInput {
            field: "cache_keepalive_pnl".to_owned(),
            reason: "amount has an invalid fractional value".to_owned(),
        })?;
    Ok(f64::from(whole) + f64::from(fraction) / 1_000_000.0)
}
