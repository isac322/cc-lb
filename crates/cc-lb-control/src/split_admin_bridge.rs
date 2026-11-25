use std::time::Duration;

use cc_lb_storage_api::{
    MetaStore, Storage, StorageError, StorageResult, WarmupAttemptOutcome, WarmupLane,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warmup_ports::LimitResetUsageRefresh;

pub const SPLIT_BRIDGE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
pub const SPLIT_BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const SPLIT_BRIDGE_RESULT_TTL_SECS: u64 = 120;
const MAX_RETAINED_RESULTS_PER_KEY: usize = 64;
const MAX_RESULT_CAS_ATTEMPTS: usize = 32;

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

pub fn manual_warmup_result_meta_key(upstream_id: Uuid, lane: WarmupLane) -> String {
    format!("split_manual_warmup_result:{upstream_id}:{}", lane.as_str())
}

pub fn oauth_usage_refresh_result_meta_key(upstream_id: Uuid) -> String {
    format!("split_usage_refresh_result:{upstream_id}")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthUsageRefreshJobResult {
    pub nonce: Uuid,
    pub outcome: LimitResetUsageRefresh,
    #[serde(default)]
    pub expires_at_unix_secs: u64,
}

pub trait BridgeResultEntry {
    fn nonce(&self) -> Uuid;
    fn expires_at_unix_secs(&self) -> u64;
    fn set_expires_at_unix_secs(&mut self, expires_at: u64);
}

impl BridgeResultEntry for ManualWarmupJobResult {
    fn nonce(&self) -> Uuid {
        self.nonce
    }
    fn expires_at_unix_secs(&self) -> u64 {
        self.expires_at_unix_secs
    }
    fn set_expires_at_unix_secs(&mut self, expires_at: u64) {
        self.expires_at_unix_secs = expires_at;
    }
}

impl BridgeResultEntry for OAuthUsageRefreshJobResult {
    fn nonce(&self) -> Uuid {
        self.nonce
    }
    fn expires_at_unix_secs(&self) -> u64 {
        self.expires_at_unix_secs
    }
    fn set_expires_at_unix_secs(&mut self, expires_at: u64) {
        self.expires_at_unix_secs = expires_at;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundedBridgeResults<T> {
    pub entries: Vec<T>,
}

impl<T> Default for BoundedBridgeResults<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<T> BoundedBridgeResults<T>
where
    T: for<'de> Deserialize<'de> + BridgeResultEntry,
{
    pub fn parse_or_empty(raw: Option<&str>) -> Self {
        raw.and_then(|value| serde_json::from_str::<Self>(value).ok())
            .unwrap_or_default()
    }

    pub fn push_bounded_at(&mut self, mut entry: T, now_unix_secs: u64) {
        if entry.expires_at_unix_secs() == 0 {
            entry.set_expires_at_unix_secs(
                now_unix_secs.saturating_add(SPLIT_BRIDGE_RESULT_TTL_SECS),
            );
        }
        let target_nonce = entry.nonce();
        self.entries.retain(|existing| {
            existing.nonce() != target_nonce
                && (existing.expires_at_unix_secs() == 0
                    || existing.expires_at_unix_secs() > now_unix_secs)
        });
        self.entries.push(entry);
        if self.entries.len() > MAX_RETAINED_RESULTS_PER_KEY {
            let overflow = self.entries.len() - MAX_RETAINED_RESULTS_PER_KEY;
            self.entries.drain(..overflow);
        }
    }
}

async fn append_bounded_meta_result<T>(
    meta: &dyn MetaStore,
    key: &str,
    result: T,
    now_unix_secs: u64,
    context: &str,
) -> StorageResult<()>
where
    T: Clone + Serialize + for<'de> Deserialize<'de> + BridgeResultEntry,
{
    for _ in 0..MAX_RESULT_CAS_ATTEMPTS {
        let existing = meta.get_meta_value(key).await?;
        let mut envelope = BoundedBridgeResults::<T>::parse_or_empty(existing.as_deref());
        envelope.push_bounded_at(result.clone(), now_unix_secs);
        let encoded = serde_json::to_string(&envelope).map_err(|error| StorageError::Fatal {
            message: format!("serialize {context}: {error}"),
        })?;
        if meta
            .compare_and_put_meta_value(key, existing.as_deref(), &encoded)
            .await?
        {
            return Ok(());
        }
    }
    Err(StorageError::Conflict {
        message: format!(
            "{context} exceeded {MAX_RESULT_CAS_ATTEMPTS} compare-and-put attempts under contention"
        ),
    })
}

async fn take_bounded_meta_result<T>(
    meta: &dyn MetaStore,
    key: &str,
    nonce: Uuid,
    context: &str,
) -> StorageResult<Option<T>>
where
    T: Clone + Serialize + for<'de> Deserialize<'de> + BridgeResultEntry,
{
    for _ in 0..MAX_RESULT_CAS_ATTEMPTS {
        let existing = meta.get_meta_value(key).await?;
        let mut envelope = BoundedBridgeResults::<T>::parse_or_empty(existing.as_deref());
        let Some(pos) = envelope
            .entries
            .iter()
            .rposition(|entry| entry.nonce() == nonce)
        else {
            return Ok(None);
        };
        let found = envelope.entries.remove(pos);
        let encoded = serde_json::to_string(&envelope).map_err(|error| StorageError::Fatal {
            message: format!("serialize {context}: {error}"),
        })?;
        if meta
            .compare_and_put_meta_value(key, existing.as_deref(), &encoded)
            .await?
        {
            return Ok(Some(found));
        }
    }
    Err(StorageError::Conflict {
        message: format!(
            "{context} read-consume exceeded {MAX_RESULT_CAS_ATTEMPTS} compare-and-put attempts under contention"
        ),
    })
}

pub async fn record_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    now_unix_secs: u64,
    result: ManualWarmupJobResult,
) -> StorageResult<()> {
    let key = manual_warmup_result_meta_key(upstream_id, lane);
    append_bounded_meta_result(
        storage,
        &key,
        result,
        now_unix_secs,
        "split manual warmup result",
    )
    .await
}

pub async fn load_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    nonce: Uuid,
) -> StorageResult<Option<ManualWarmupJobResult>> {
    let key = manual_warmup_result_meta_key(upstream_id, lane);
    let existing = storage.get_meta_value(&key).await?;
    let envelope =
        BoundedBridgeResults::<ManualWarmupJobResult>::parse_or_empty(existing.as_deref());
    Ok(envelope
        .entries
        .into_iter()
        .rev()
        .find(|entry| entry.nonce == nonce))
}

pub async fn take_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    nonce: Uuid,
) -> StorageResult<Option<ManualWarmupJobResult>> {
    let key = manual_warmup_result_meta_key(upstream_id, lane);
    take_bounded_meta_result(storage, &key, nonce, "split manual warmup result").await
}

pub async fn record_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    now_unix_secs: u64,
    result: OAuthUsageRefreshJobResult,
) -> StorageResult<()> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id);
    append_bounded_meta_result(
        storage,
        &key,
        result,
        now_unix_secs,
        "split usage refresh result",
    )
    .await
}

pub async fn load_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    nonce: Uuid,
) -> StorageResult<Option<OAuthUsageRefreshJobResult>> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id);
    let existing = storage.get_meta_value(&key).await?;
    let envelope =
        BoundedBridgeResults::<OAuthUsageRefreshJobResult>::parse_or_empty(existing.as_deref());
    Ok(envelope
        .entries
        .into_iter()
        .rev()
        .find(|entry| entry.nonce == nonce))
}

pub async fn take_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    nonce: Uuid,
) -> StorageResult<Option<OAuthUsageRefreshJobResult>> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id);
    take_bounded_meta_result(storage, &key, nonce, "split usage refresh result").await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use cc_lb_storage_api::{BackendKind, WarmupSuccessReason};

    use super::*;

    struct ContendingMetaStore {
        failures_remaining: AtomicUsize,
        values: Mutex<HashMap<String, String>>,
    }

    #[async_trait::async_trait]
    impl MetaStore for ContendingMetaStore {
        async fn initialize(&self) -> StorageResult<()> {
            Ok(())
        }

        async fn backend_kind(&self) -> StorageResult<BackendKind> {
            Ok(BackendKind::Sqlite)
        }

        async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
            Ok(self.values.lock().expect("meta lock").get(key).cloned())
        }

        async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
            self.values
                .lock()
                .expect("meta lock")
                .insert(key.to_owned(), value.to_owned());
            Ok(())
        }

        async fn compare_and_put_meta_value(
            &self,
            key: &str,
            expected: Option<&str>,
            value: &str,
        ) -> StorageResult<bool> {
            let mut guard = self.values.lock().expect("meta lock");
            let remaining = self.failures_remaining.load(Ordering::Acquire);
            if remaining > 0 {
                let _ = self.failures_remaining.try_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |cur| cur.checked_sub(1),
                );
                return Ok(false);
            }
            if guard.get(key).map(String::as_str) == expected {
                guard.insert(key.to_owned(), value.to_owned());
                return Ok(true);
            }
            Ok(false)
        }
    }

    #[test]
    fn push_bounded_retains_at_most_max_recent_entries() {
        let mut envelope = BoundedBridgeResults::default();
        let total = MAX_RETAINED_RESULTS_PER_KEY + 2;
        let nonces: Vec<Uuid> = (1..=total).map(|i| Uuid::from_u128(i as u128)).collect();
        for &nonce in &nonces {
            envelope.push_bounded_at(
                OAuthUsageRefreshJobResult {
                    nonce,
                    outcome: LimitResetUsageRefresh::Refreshed,
                    expires_at_unix_secs: 1_120,
                },
                1_000,
            );
        }
        assert_eq!(envelope.entries.len(), MAX_RETAINED_RESULTS_PER_KEY);
        assert_eq!(
            envelope.entries.iter().map(|e| e.nonce).collect::<Vec<_>>(),
            nonces[2..].to_vec()
        );
    }

    #[tokio::test]
    async fn append_bounded_meta_result_retries_cas_contention_and_conflicts_on_exhaustion() {
        let store = ContendingMetaStore {
            failures_remaining: AtomicUsize::new(2),
            values: Mutex::new(HashMap::new()),
        };
        let upstream_id = Uuid::new_v4();
        let key = manual_warmup_result_meta_key(upstream_id, WarmupLane::Primary);
        let nonce = Uuid::new_v4();
        append_bounded_meta_result(
            &store,
            &key,
            ManualWarmupJobResult {
                nonce,
                outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                cycle_key: Some(42),
                error_detail: None,
                expires_at_unix_secs: 1_120,
            },
            1_000,
            "split manual warmup result",
        )
        .await
        .expect("CAS loop must succeed after transient contention");

        let always_contending = ContendingMetaStore {
            failures_remaining: AtomicUsize::new(MAX_RESULT_CAS_ATTEMPTS + 10),
            values: Mutex::new(HashMap::new()),
        };
        let err = append_bounded_meta_result(
            &always_contending,
            &key,
            ManualWarmupJobResult {
                nonce: Uuid::new_v4(),
                outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                cycle_key: Some(43),
                error_detail: None,
                expires_at_unix_secs: 1_120,
            },
            1_000,
            "split manual warmup result",
        )
        .await
        .expect_err("exhausted CAS retries must return Conflict");
        assert!(matches!(err, StorageError::Conflict { .. }));
    }

    #[tokio::test]
    async fn concurrent_manual_warmup_and_usage_refresh_appends_preserve_all_nonces() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_url = format!("sqlite://{}", dir.path().join("cas.sqlite").display());
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&db_url, clock)
                .await
                .expect("open sqlite"),
        );
        storage.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        let warmup_nonces: Vec<Uuid> = (0..12).map(|_| Uuid::new_v4()).collect();
        let mut warmup_set = tokio::task::JoinSet::new();
        for &nonce in &warmup_nonces {
            let storage = Arc::clone(&storage);
            warmup_set.spawn(async move {
                record_manual_warmup_result(
                    storage.as_ref(),
                    upstream_id,
                    WarmupLane::Primary,
                    1_000,
                    ManualWarmupJobResult {
                        nonce,
                        outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                        cycle_key: Some(10),
                        error_detail: None,
                        expires_at_unix_secs: 1_120,
                    },
                )
                .await
            });
        }
        while let Some(res) = warmup_set.join_next().await {
            res.expect("task join")
                .expect("concurrent warmup CAS write must succeed");
        }
        // Record one already-expired entry before adding a fresh one to verify expiry pruning.
        let expired_nonce = Uuid::new_v4();
        record_manual_warmup_result(
            storage.as_ref(),
            upstream_id,
            WarmupLane::Primary,
            1_000,
            ManualWarmupJobResult {
                nonce: expired_nonce,
                outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                cycle_key: Some(10),
                error_detail: None,
                expires_at_unix_secs: 1_005,
            },
        )
        .await
        .expect("record expired warmup");
        let fresh_nonce = Uuid::new_v4();
        record_manual_warmup_result(
            storage.as_ref(),
            upstream_id,
            WarmupLane::Primary,
            1_010,
            ManualWarmupJobResult {
                nonce: fresh_nonce,
                outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                cycle_key: Some(11),
                error_detail: None,
                expires_at_unix_secs: 1_130,
            },
        )
        .await
        .expect("record fresh warmup");
        assert!(
            load_manual_warmup_result(
                storage.as_ref(),
                upstream_id,
                WarmupLane::Primary,
                expired_nonce,
            )
            .await
            .expect("load expired warmup")
            .is_none(),
            "expired manual warmup result must be pruned on next write"
        );
        for nonce in warmup_nonces
            .into_iter()
            .chain(std::iter::once(fresh_nonce))
        {
            assert!(
                take_manual_warmup_result(
                    storage.as_ref(),
                    upstream_id,
                    WarmupLane::Primary,
                    nonce
                )
                .await
                .expect("take warmup")
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
                .expect("concurrent usage refresh CAS write must succeed");
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
