use std::time::Duration;

use cc_lb_storage_api::{
    MetaStore, Storage, StorageError, StorageResult, WarmupAttemptOutcome, WarmupLane,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warmup_ports::LimitResetUsageRefresh;

pub const SPLIT_BRIDGE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
pub const SPLIT_BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_RETAINED_RESULTS_PER_KEY: usize = 4;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthUsageRefreshJobResult {
    pub nonce: Uuid,
    pub outcome: LimitResetUsageRefresh,
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
    T: for<'de> Deserialize<'de>,
{
    pub fn parse_or_empty(raw: Option<&str>) -> Self {
        raw.and_then(|value| serde_json::from_str::<Self>(value).ok())
            .unwrap_or_default()
    }

    pub fn push_bounded(&mut self, entry: T) {
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
    context: &str,
) -> StorageResult<()>
where
    T: Clone + Serialize + for<'de> Deserialize<'de>,
{
    for _ in 0..MAX_RESULT_CAS_ATTEMPTS {
        let existing = meta.get_meta_value(key).await?;
        let mut envelope = BoundedBridgeResults::<T>::parse_or_empty(existing.as_deref());
        envelope.push_bounded(result.clone());
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

pub async fn record_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    result: ManualWarmupJobResult,
) -> StorageResult<()> {
    let key = manual_warmup_result_meta_key(upstream_id, lane);
    append_bounded_meta_result(storage, &key, result, "split manual warmup result").await
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

pub async fn record_oauth_usage_refresh_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    result: OAuthUsageRefreshJobResult,
) -> StorageResult<()> {
    let key = oauth_usage_refresh_result_meta_key(upstream_id);
    append_bounded_meta_result(storage, &key, result, "split usage refresh result").await
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
    fn push_bounded_retains_at_most_four_recent_entries() {
        let mut envelope = BoundedBridgeResults::default();
        let nonces: Vec<Uuid> = (1..=6).map(Uuid::from_u128).collect();
        for &nonce in &nonces {
            envelope.push_bounded(OAuthUsageRefreshJobResult {
                nonce,
                outcome: LimitResetUsageRefresh::Refreshed,
            });
        }
        assert_eq!(envelope.entries.len(), 4);
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
            },
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
            },
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
        let warmup_nonces: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let mut warmup_set = tokio::task::JoinSet::new();
        for &nonce in &warmup_nonces {
            let storage = Arc::clone(&storage);
            warmup_set.spawn(async move {
                record_manual_warmup_result(
                    storage.as_ref(),
                    upstream_id,
                    WarmupLane::Primary,
                    ManualWarmupJobResult {
                        nonce,
                        outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                        cycle_key: Some(10),
                        error_detail: None,
                    },
                )
                .await
            });
        }
        while let Some(res) = warmup_set.join_next().await {
            res.expect("task join")
                .expect("concurrent warmup CAS write must succeed");
        }
        for nonce in warmup_nonces {
            assert!(
                load_manual_warmup_result(
                    storage.as_ref(),
                    upstream_id,
                    WarmupLane::Primary,
                    nonce
                )
                .await
                .expect("load warmup")
                .is_some(),
                "lost concurrent manual warmup nonce {nonce}"
            );
        }

        let usage_nonces: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let mut usage_set = tokio::task::JoinSet::new();
        for &nonce in &usage_nonces {
            let storage = Arc::clone(&storage);
            usage_set.spawn(async move {
                record_oauth_usage_refresh_result(
                    storage.as_ref(),
                    upstream_id,
                    OAuthUsageRefreshJobResult {
                        nonce,
                        outcome: LimitResetUsageRefresh::Refreshed,
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
                load_oauth_usage_refresh_result(storage.as_ref(), upstream_id, nonce)
                    .await
                    .expect("load usage refresh")
                    .is_some(),
                "lost concurrent usage refresh nonce {nonce}"
            );
        }
    }
}
