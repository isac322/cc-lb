use std::time::Duration;

use cc_lb_storage_api::{MetaStore, StorageError, StorageResult, WarmupAttemptOutcome, WarmupLane};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warmup_ports::LimitResetUsageRefresh;

pub const SPLIT_BRIDGE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
pub const SPLIT_BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const SPLIT_BRIDGE_RESULT_TTL_SECS: u64 = 120;
pub const SPLIT_MANUAL_WARMUP_RESULT_PREFIX: &str = "split_manual_warmup_result:";
pub const SPLIT_USAGE_REFRESH_RESULT_PREFIX: &str = "split_usage_refresh_result:";
const SPLIT_BRIDGE_WRITE_PRUNE_BATCH: usize = 64;
const SPLIT_BRIDGE_PRUNE_PAGE_SIZE: usize = 256;
const SPLIT_BRIDGE_TAKE_DELETE_TIMEOUT: Duration = Duration::from_millis(50);

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

async fn best_effort_delete_meta_value(storage: &dyn MetaStore, key: &str) {
    match tokio::time::timeout(
        SPLIT_BRIDGE_TAKE_DELETE_TIMEOUT,
        storage.delete_meta_value(key),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::debug!(%key, %error, "best-effort split bridge key delete failed; deferring to expiry sweep");
        }
        Err(_) => {
            tracing::debug!(%key, "best-effort split bridge key delete timed out; deferring to expiry sweep");
        }
    }
}

pub async fn record_manual_warmup_result(
    storage: &dyn MetaStore,
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
    let _ = prune_expired_split_bridge_results_with_page_limit(
        storage,
        now_unix_secs,
        SPLIT_BRIDGE_WRITE_PRUNE_BATCH,
        Some(1),
    )
    .await;
    Ok(())
}

pub async fn record_manual_warmup_expired_before_start(
    storage: &dyn MetaStore,
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
    let _ = prune_expired_split_bridge_results_with_page_limit(
        storage,
        now_unix_secs,
        SPLIT_BRIDGE_WRITE_PRUNE_BATCH,
        Some(1),
    )
    .await;
    Ok(())
}

pub async fn load_manual_warmup_result(
    storage: &dyn MetaStore,
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
    storage: &dyn MetaStore,
    upstream_id: Uuid,
    lane: WarmupLane,
    nonce: Uuid,
) -> StorageResult<Option<ManualWarmupBridgeResult>> {
    let key = manual_warmup_result_meta_key(upstream_id, lane, nonce);
    let Some(raw) = storage.get_meta_value(&key).await? else {
        return Ok(None);
    };
    let Some(parsed) = parse_manual_warmup_result(&raw) else {
        best_effort_delete_meta_value(storage, &key).await;
        return Ok(None);
    };
    best_effort_delete_meta_value(storage, &key).await;
    Ok(Some(parsed))
}

pub async fn record_oauth_usage_refresh_result(
    storage: &dyn MetaStore,
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
    let _ = prune_expired_split_bridge_results_with_page_limit(
        storage,
        now_unix_secs,
        SPLIT_BRIDGE_WRITE_PRUNE_BATCH,
        Some(1),
    )
    .await;
    Ok(())
}

pub async fn load_oauth_usage_refresh_result(
    storage: &dyn MetaStore,
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
    storage: &dyn MetaStore,
    upstream_id: Uuid,
    nonce: Uuid,
) -> StorageResult<Option<OAuthUsageRefreshJobResult>> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id, nonce);
    let Some(raw) = storage.get_meta_value(&key).await? else {
        return Ok(None);
    };
    let Ok(parsed) = serde_json::from_str::<OAuthUsageRefreshJobResult>(&raw) else {
        best_effort_delete_meta_value(storage, &key).await;
        return Ok(None);
    };
    best_effort_delete_meta_value(storage, &key).await;
    Ok(Some(parsed))
}

/// Sweeps expired per-nonce split bridge result keys across keyset-paginated
/// prefix scans without ever deleting unexpired entries.
pub async fn prune_expired_split_bridge_results(
    storage: &dyn MetaStore,
    now_unix_secs: u64,
    batch_limit: usize,
) -> StorageResult<u64> {
    prune_expired_split_bridge_results_with_page_limit(storage, now_unix_secs, batch_limit, None)
        .await
}

async fn prune_expired_split_bridge_results_with_page_limit(
    storage: &dyn MetaStore,
    now_unix_secs: u64,
    batch_limit: usize,
    max_pages_per_prefix: Option<usize>,
) -> StorageResult<u64> {
    if batch_limit == 0 {
        return Ok(0);
    }
    let mut deleted = 0_u64;

    for (prefix, is_manual) in [
        (SPLIT_MANUAL_WARMUP_RESULT_PREFIX, true),
        (SPLIT_USAGE_REFRESH_RESULT_PREFIX, false),
    ] {
        let mut after_key: Option<String> = None;
        let mut pages_scanned = 0_usize;
        loop {
            if deleted as usize >= batch_limit
                || max_pages_per_prefix.is_some_and(|max| pages_scanned >= max)
            {
                break;
            }
            let rows = storage
                .list_meta_values_by_prefix(
                    prefix,
                    after_key.as_deref(),
                    SPLIT_BRIDGE_PRUNE_PAGE_SIZE,
                )
                .await?;
            if rows.is_empty() {
                break;
            }
            pages_scanned += 1;
            let page_len = rows.len();
            let last_key = rows.last().map(|(key, _)| key.clone());
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
            if page_len < SPLIT_BRIDGE_PRUNE_PAGE_SIZE {
                break;
            }
            after_key = last_key;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_storage_api::WarmupSuccessReason;

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

    #[derive(Clone, Copy)]
    enum DeleteFault {
        Error,
        Stall(Duration),
    }

    struct FaultyDeleteMetaStore {
        inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
        fault: DeleteFault,
    }

    #[async_trait::async_trait]
    impl MetaStore for FaultyDeleteMetaStore {
        async fn initialize(&self) -> StorageResult<()> {
            self.inner.initialize().await
        }

        async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
            self.inner.backend_kind().await
        }

        async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
            self.inner.get_meta_value(key).await
        }

        async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
            self.inner.put_meta_value(key, value).await
        }

        async fn compare_and_put_meta_value(
            &self,
            key: &str,
            expected: Option<&str>,
            value: &str,
        ) -> StorageResult<bool> {
            self.inner
                .compare_and_put_meta_value(key, expected, value)
                .await
        }

        async fn delete_meta_value(&self, _key: &str) -> StorageResult<bool> {
            match self.fault {
                DeleteFault::Error => Err(StorageError::Fatal {
                    message: "database is locked".to_owned(),
                }),
                DeleteFault::Stall(delay) => {
                    tokio::time::sleep(delay).await;
                    Ok(true)
                }
            }
        }

        async fn list_meta_values_by_prefix(
            &self,
            prefix: &str,
            after_key: Option<&str>,
            limit: usize,
        ) -> StorageResult<Vec<(String, String)>> {
            self.inner
                .list_meta_values_by_prefix(prefix, after_key, limit)
                .await
        }

        async fn verify_schema_current(&self) -> StorageResult<()> {
            self.inner.verify_schema_current().await
        }
    }

    #[tokio::test]
    async fn take_results_return_parsed_outcome_when_delete_fails_or_stalls() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_url = format!(
            "sqlite://{}",
            dir.path().join("faulty-del.sqlite").display()
        );
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&db_url, clock)
                .await
                .expect("open sqlite"),
        );
        storage.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        for fault in [
            DeleteFault::Error,
            DeleteFault::Stall(Duration::from_millis(500)),
        ] {
            let store = FaultyDeleteMetaStore {
                inner: Arc::clone(&storage),
                fault,
            };
            let manual_nonce = Uuid::new_v4();
            record_manual_warmup_result(
                storage.as_ref(),
                upstream_id,
                WarmupLane::Primary,
                1_000,
                ManualWarmupJobResult {
                    nonce: manual_nonce,
                    outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                    cycle_key: Some(42),
                    error_detail: None,
                    expires_at_unix_secs: 1_120,
                },
            )
            .await
            .expect("seed manual warmup");

            let started = std::time::Instant::now();
            let manual =
                take_manual_warmup_result(&store, upstream_id, WarmupLane::Primary, manual_nonce)
                    .await
                    .expect("take manual warmup must succeed despite delete fault")
                    .expect("manual warmup result must still be returned")
                    .into_settled()
                    .expect("settled");
            assert_eq!(manual.cycle_key, Some(42));
            assert!(
                started.elapsed() < Duration::from_millis(250),
                "stalled delete must be bounded, elapsed={:?}",
                started.elapsed()
            );

            let usage_nonce = Uuid::new_v4();
            record_oauth_usage_refresh_result(
                storage.as_ref(),
                upstream_id,
                1_000,
                OAuthUsageRefreshJobResult {
                    nonce: usage_nonce,
                    outcome: LimitResetUsageRefresh::Refreshed,
                    expires_at_unix_secs: 1_120,
                },
            )
            .await
            .expect("seed usage refresh");

            let started = std::time::Instant::now();
            let usage = take_oauth_usage_refresh_result(&store, upstream_id, usage_nonce)
                .await
                .expect("take usage refresh must succeed despite delete fault")
                .expect("usage refresh result must still be returned");
            assert_eq!(usage.outcome, LimitResetUsageRefresh::Refreshed);
            assert!(started.elapsed() < Duration::from_millis(250));
        }
    }

    #[tokio::test]
    async fn prune_expired_split_bridge_results_advances_past_early_live_keys() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_url = format!(
            "sqlite://{}",
            dir.path().join("prune-pages.sqlite").display()
        );
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&db_url, clock)
                .await
                .expect("open sqlite"),
        );
        storage.initialize().await.expect("init sqlite");

        // Sort 300 live keys before 5 expired keys under both prefixes (`0000...` < `ffff...`),
        // exceeding SPLIT_BRIDGE_PRUNE_PAGE_SIZE (256).
        let early_upstream = Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("uuid");
        let late_upstream = Uuid::parse_str("ffffffff-ffff-4000-8000-000000000001").expect("uuid");

        let mut manual_live = Vec::new();
        let mut usage_live = Vec::new();
        for idx in 0..300_u128 {
            let m_nonce = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001 + idx);
            let m_key = manual_warmup_result_meta_key(early_upstream, WarmupLane::Primary, m_nonce);
            let m_raw = serialize_result(
                &ManualWarmupBridgeResult::Settled(ManualWarmupJobResult {
                    nonce: m_nonce,
                    outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                    cycle_key: Some(1),
                    error_detail: None,
                    expires_at_unix_secs: 2_000,
                }),
                "live manual",
            )
            .expect("serialize");
            storage
                .put_meta_value(&m_key, &m_raw)
                .await
                .expect("put live manual");
            manual_live.push(m_key);

            let u_nonce = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0001_0001 + idx);
            let u_key = oauth_usage_refresh_result_meta_key(early_upstream, u_nonce);
            let u_raw = serialize_result(
                &OAuthUsageRefreshJobResult {
                    nonce: u_nonce,
                    outcome: LimitResetUsageRefresh::Refreshed,
                    expires_at_unix_secs: 2_000,
                },
                "live usage",
            )
            .expect("serialize");
            storage
                .put_meta_value(&u_key, &u_raw)
                .await
                .expect("put live usage");
            usage_live.push(u_key);
        }

        let mut expired_keys = Vec::new();
        for idx in 0..5_u128 {
            let m_nonce = Uuid::from_u128(0xffff_ffff_ffff_4000_8000_0000_0000_0001 + idx);
            let m_key = manual_warmup_result_meta_key(late_upstream, WarmupLane::Primary, m_nonce);
            let m_raw = serialize_result(
                &ManualWarmupBridgeResult::Settled(ManualWarmupJobResult {
                    nonce: m_nonce,
                    outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                    cycle_key: Some(2),
                    error_detail: None,
                    expires_at_unix_secs: 1_005,
                }),
                "expired manual",
            )
            .expect("serialize");
            storage
                .put_meta_value(&m_key, &m_raw)
                .await
                .expect("put expired manual");
            expired_keys.push(m_key);

            let u_nonce = Uuid::from_u128(0xffff_ffff_ffff_4000_8000_0000_0001_0001 + idx);
            let u_key = oauth_usage_refresh_result_meta_key(late_upstream, u_nonce);
            let u_raw = serialize_result(
                &OAuthUsageRefreshJobResult {
                    nonce: u_nonce,
                    outcome: LimitResetUsageRefresh::Refreshed,
                    expires_at_unix_secs: 1_005,
                },
                "expired usage",
            )
            .expect("serialize");
            storage
                .put_meta_value(&u_key, &u_raw)
                .await
                .expect("put expired usage");
            expired_keys.push(u_key);
        }

        let deleted = prune_expired_split_bridge_results(storage.as_ref(), 1_500, 256)
            .await
            .expect("prune across pages");
        assert_eq!(deleted, 10, "all 10 late expired entries must be reaped");
        for key in expired_keys {
            assert_eq!(
                storage.get_meta_value(&key).await.expect("get expired"),
                None
            );
        }
        for key in manual_live.iter().chain(usage_live.iter()) {
            assert!(
                storage
                    .get_meta_value(key)
                    .await
                    .expect("get live")
                    .is_some(),
                "live key {key} must survive expiry sweep"
            );
        }
    }
}
