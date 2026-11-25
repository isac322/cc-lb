use std::time::Duration;

use cc_lb_storage_api::{Storage, StorageError, StorageResult, WarmupAttemptOutcome, WarmupLane};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warmup_ports::LimitResetUsageRefresh;

pub const SPLIT_BRIDGE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
pub const SPLIT_BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const SPLIT_BRIDGE_RESULT_TTL_SECS: u64 = 120;
pub const SPLIT_MANUAL_WARMUP_RESULT_PREFIX: &str = "split_manual_warmup_result:";
pub const SPLIT_USAGE_REFRESH_RESULT_PREFIX: &str = "split_usage_refresh_result:";
const SPLIT_BRIDGE_WRITE_PRUNE_BATCH: usize = 64;

pub fn oauth_refresh_bridge_idempotency_key(upstream_id: Uuid, nonce: Uuid) -> String {
    format!("split_oauth_refresh:{upstream_id}:{nonce}")
}

pub fn manual_warmup_idempotency_key(upstream_id: Uuid, lane: WarmupLane, nonce: Uuid) -> String {
    format!(
        "split_manual_warmup:{upstream_id}:{}:{nonce}",
        lane.as_str()
    )
}

pub fn oauth_usage_refresh_idempotency_key(upstream_id: Uuid, nonce: Uuid) -> String {
    format!("split_usage_refresh:{upstream_id}:{nonce}")
}

pub fn manual_warmup_result_meta_key(upstream_id: Uuid, lane: WarmupLane, nonce: Uuid) -> String {
    format!(
        "{SPLIT_MANUAL_WARMUP_RESULT_PREFIX}{upstream_id}:{}:{nonce}",
        lane.as_str()
    )
}

pub fn oauth_usage_refresh_result_meta_key(upstream_id: Uuid, nonce: Uuid) -> String {
    format!("{SPLIT_USAGE_REFRESH_RESULT_PREFIX}{upstream_id}:{nonce}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualWarmupJobResult {
    pub nonce: Uuid,
    pub outcome: WarmupAttemptOutcome,
    pub cycle_key: Option<i64>,
    pub error_detail: Option<String>,
    #[serde(default)]
    pub expires_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ManualWarmupBridgeResult {
    Settled(ManualWarmupJobResult),
    ExpiredBeforeStart {
        nonce: Uuid,
        detail: String,
        #[serde(default)]
        expires_at_unix_secs: u64,
    },
}

impl ManualWarmupBridgeResult {
    pub fn nonce(&self) -> Uuid {
        match self {
            Self::Settled(result) => result.nonce,
            Self::ExpiredBeforeStart { nonce, .. } => *nonce,
        }
    }

    pub fn expires_at_unix_secs(&self) -> u64 {
        match self {
            Self::Settled(result) => result.expires_at_unix_secs,
            Self::ExpiredBeforeStart {
                expires_at_unix_secs,
                ..
            } => *expires_at_unix_secs,
        }
    }

    pub fn into_settled(self) -> Option<ManualWarmupJobResult> {
        match self {
            Self::Settled(result) => Some(result),
            Self::ExpiredBeforeStart { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthUsageRefreshJobResult {
    pub nonce: Uuid,
    pub outcome: LimitResetUsageRefresh,
    #[serde(default)]
    pub expires_at_unix_secs: u64,
}

fn serialize_result<T: Serialize>(value: &T, context: &str) -> StorageResult<String> {
    serde_json::to_string(value).map_err(|error| StorageError::Fatal {
        message: format!("serialize {context}: {error}"),
    })
}

fn parse_manual_warmup_result(raw: &str) -> Option<ManualWarmupBridgeResult> {
    serde_json::from_str::<ManualWarmupBridgeResult>(raw)
        .ok()
        .or_else(|| {
            serde_json::from_str::<ManualWarmupJobResult>(raw)
                .ok()
                .map(ManualWarmupBridgeResult::Settled)
        })
}

pub async fn record_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    now_unix_secs: u64,
    mut result: ManualWarmupJobResult,
) -> StorageResult<()> {
    if result.expires_at_unix_secs == 0 {
        result.expires_at_unix_secs = now_unix_secs.saturating_add(SPLIT_BRIDGE_RESULT_TTL_SECS);
    }
    let key = manual_warmup_result_meta_key(upstream_id, lane, result.nonce);
    let encoded = serialize_result(
        &ManualWarmupBridgeResult::Settled(result),
        "split manual warmup result",
    )?;
    storage.put_meta_value(&key, &encoded).await?;
    let _ =
        prune_expired_split_bridge_results(storage, now_unix_secs, SPLIT_BRIDGE_WRITE_PRUNE_BATCH)
            .await;
    Ok(())
}

pub async fn record_manual_warmup_expired_before_start(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    now_unix_secs: u64,
    nonce: Uuid,
    detail: String,
) -> StorageResult<()> {
    let key = manual_warmup_result_meta_key(upstream_id, lane, nonce);
    let payload = ManualWarmupBridgeResult::ExpiredBeforeStart {
        nonce,
        detail,
        expires_at_unix_secs: now_unix_secs.saturating_add(SPLIT_BRIDGE_RESULT_TTL_SECS),
    };
    let encoded = serialize_result(&payload, "split manual warmup expired-before-start")?;
    storage.put_meta_value(&key, &encoded).await?;
    let _ =
        prune_expired_split_bridge_results(storage, now_unix_secs, SPLIT_BRIDGE_WRITE_PRUNE_BATCH)
            .await;
    Ok(())
}

pub async fn load_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    nonce: Uuid,
) -> StorageResult<Option<ManualWarmupBridgeResult>> {
    let key = manual_warmup_result_meta_key(upstream_id, lane, nonce);
    let Some(raw) = storage.get_meta_value(&key).await? else {
        return Ok(None);
    };
    Ok(parse_manual_warmup_result(&raw))
}

pub async fn take_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    nonce: Uuid,
) -> StorageResult<Option<ManualWarmupBridgeResult>> {
    let key = manual_warmup_result_meta_key(upstream_id, lane, nonce);
    let Some(raw) = storage.get_meta_value(&key).await? else {
        return Ok(None);
    };
    let Some(parsed) = parse_manual_warmup_result(&raw) else {
        let _ = storage.delete_meta_value(&key).await;
        return Ok(None);
    };
    storage.delete_meta_value(&key).await?;
    Ok(Some(parsed))
}

pub async fn record_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    now_unix_secs: u64,
    mut result: OAuthUsageRefreshJobResult,
) -> StorageResult<()> {
    if result.expires_at_unix_secs == 0 {
        result.expires_at_unix_secs = now_unix_secs.saturating_add(SPLIT_BRIDGE_RESULT_TTL_SECS);
    }
    let key = oauth_usage_refresh_result_meta_key(upstream_id, result.nonce);
    let encoded = serialize_result(&result, "split usage refresh result")?;
    storage.put_meta_value(&key, &encoded).await?;
    let _ =
        prune_expired_split_bridge_results(storage, now_unix_secs, SPLIT_BRIDGE_WRITE_PRUNE_BATCH)
            .await;
    Ok(())
}

pub async fn load_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    nonce: Uuid,
) -> StorageResult<Option<OAuthUsageRefreshJobResult>> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id, nonce);
    let Some(raw) = storage.get_meta_value(&key).await? else {
        return Ok(None);
    };
    Ok(serde_json::from_str::<OAuthUsageRefreshJobResult>(&raw).ok())
}

pub async fn take_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    nonce: Uuid,
) -> StorageResult<Option<OAuthUsageRefreshJobResult>> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id, nonce);
    let Some(raw) = storage.get_meta_value(&key).await? else {
        return Ok(None);
    };
    let Ok(parsed) = serde_json::from_str::<OAuthUsageRefreshJobResult>(&raw) else {
        let _ = storage.delete_meta_value(&key).await;
        return Ok(None);
    };
    storage.delete_meta_value(&key).await?;
    Ok(Some(parsed))
}

/// Sweeps expired per-nonce split bridge result keys without ever deleting
/// unexpired entries.
pub async fn prune_expired_split_bridge_results(
    storage: &dyn Storage,
    now_unix_secs: u64,
    batch_limit: usize,
) -> StorageResult<u64> {
    if batch_limit == 0 {
        return Ok(0);
    }
    let scan_limit = batch_limit.saturating_mul(8).max(256);
    let mut deleted = 0_u64;

    for (prefix, is_manual) in [
        (SPLIT_MANUAL_WARMUP_RESULT_PREFIX, true),
        (SPLIT_USAGE_REFRESH_RESULT_PREFIX, false),
    ] {
        if deleted as usize >= batch_limit {
            break;
        }
        let rows = storage
            .list_meta_values_by_prefix(prefix, scan_limit)
            .await?;
        for (key, raw) in rows {
            if deleted as usize >= batch_limit {
                break;
            }
            let expires_at = if is_manual {
                parse_manual_warmup_result(&raw).map(|entry| entry.expires_at_unix_secs())
            } else {
                serde_json::from_str::<OAuthUsageRefreshJobResult>(&raw)
                    .ok()
                    .map(|entry| entry.expires_at_unix_secs)
            };
            let should_delete = match expires_at {
                Some(exp) => exp > 0 && exp <= now_unix_secs,
                None => false,
            };
            if should_delete && storage.delete_meta_value(&key).await? {
                deleted += 1;
            }
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_storage_api::{MetaStore as _, WarmupSuccessReason};

    use super::*;

    #[tokio::test]
    async fn per_nonce_bridge_results_never_evict_unexpired_waiters_even_past_80_unread() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_url = format!("sqlite://{}", dir.path().join("per-nonce.sqlite").display());
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&db_url, clock)
                .await
                .expect("open sqlite"),
        );
        storage.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        let live_waiting_nonce = Uuid::new_v4();
        record_manual_warmup_result(
            storage.as_ref(),
            upstream_id,
            WarmupLane::Primary,
            1_000,
            ManualWarmupJobResult {
                nonce: live_waiting_nonce,
                outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                cycle_key: Some(7),
                error_detail: None,
                expires_at_unix_secs: 1_120,
            },
        )
        .await
        .expect("record waiting caller result");

        // 80 subsequent completions (> old 64 cap) on the same upstream/lane must never evict
        // the still-waiting caller's unexpired nonce.
        let mut extra_nonces = Vec::with_capacity(80);
        for _ in 0..80 {
            let nonce = Uuid::new_v4();
            extra_nonces.push(nonce);
            record_manual_warmup_result(
                storage.as_ref(),
                upstream_id,
                WarmupLane::Primary,
                1_010,
                ManualWarmupJobResult {
                    nonce,
                    outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                    cycle_key: Some(8),
                    error_detail: None,
                    expires_at_unix_secs: 1_130,
                },
            )
            .await
            .expect("record extra warmup");
        }

        let expired_nonce = Uuid::new_v4();
        record_manual_warmup_result(
            storage.as_ref(),
            upstream_id,
            WarmupLane::Primary,
            1_000,
            ManualWarmupJobResult {
                nonce: expired_nonce,
                outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                cycle_key: Some(9),
                error_detail: None,
                expires_at_unix_secs: 1_005,
            },
        )
        .await
        .expect("record expired warmup");

        let swept = prune_expired_split_bridge_results(storage.as_ref(), 1_015, 256)
            .await
            .expect("prune expired results");
        assert_eq!(swept, 1, "only the expired entry may be deleted");

        assert!(
            load_manual_warmup_result(
                storage.as_ref(),
                upstream_id,
                WarmupLane::Primary,
                expired_nonce
            )
            .await
            .expect("load expired")
            .is_none()
        );

        let waiting = take_manual_warmup_result(
            storage.as_ref(),
            upstream_id,
            WarmupLane::Primary,
            live_waiting_nonce,
        )
        .await
        .expect("take waiting caller result")
        .expect("waiting caller result must remain intact across 80 concurrent results")
        .into_settled()
        .expect("settled result");
        assert_eq!(waiting.nonce, live_waiting_nonce);
        assert!(
            load_manual_warmup_result(
                storage.as_ref(),
                upstream_id,
                WarmupLane::Primary,
                live_waiting_nonce
            )
            .await
            .expect("check consumed key removed")
            .is_none(),
            "take_manual_warmup_result must delete consumed key"
        );

        for nonce in extra_nonces {
            assert!(
                take_manual_warmup_result(
                    storage.as_ref(),
                    upstream_id,
                    WarmupLane::Primary,
                    nonce
                )
                .await
                .expect("take extra")
                .is_some(),
                "lost concurrent manual warmup nonce {nonce}"
            );
        }

        let usage_nonces: Vec<Uuid> = (0..12).map(|_| Uuid::new_v4()).collect();
        let mut usage_set = tokio::task::JoinSet::new();
        for &nonce in &usage_nonces {
            let storage = Arc::clone(&storage);
            usage_set.spawn(async move {
                record_oauth_usage_refresh_result(
                    storage.as_ref(),
                    upstream_id,
                    1_000,
                    OAuthUsageRefreshJobResult {
                        nonce,
                        outcome: LimitResetUsageRefresh::Refreshed,
                        expires_at_unix_secs: 1_120,
                    },
                )
                .await
            });
        }
        while let Some(res) = usage_set.join_next().await {
            res.expect("task join")
                .expect("concurrent usage refresh write must succeed");
        }
        for nonce in usage_nonces {
            assert!(
                take_oauth_usage_refresh_result(storage.as_ref(), upstream_id, nonce)
                    .await
                    .expect("take usage refresh")
                    .is_some(),
                "lost concurrent usage refresh nonce {nonce}"
            );
        }
    }
}
