use std::time::Duration;

use cc_lb_storage_api::{Storage, StorageResult, WarmupAttemptOutcome, WarmupLane};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::warmup_ports::LimitResetUsageRefresh;

pub const SPLIT_BRIDGE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
pub const SPLIT_BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_RETAINED_RESULTS_PER_KEY: usize = 4;

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

pub async fn record_manual_warmup_result(
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    result: ManualWarmupJobResult,
) -> StorageResult<()> {
    let key = manual_warmup_result_meta_key(upstream_id, lane);
    let existing = storage.get_meta_value(&key).await?;
    let mut envelope =
        BoundedBridgeResults::<ManualWarmupJobResult>::parse_or_empty(existing.as_deref());
    envelope.push_bounded(result);
    let encoded = serde_json::to_string(&envelope).map_err(|error| {
        cc_lb_storage_api::StorageError::Fatal {
            message: format!("serialize split manual warmup result: {error}"),
        }
    })?;
    storage.put_meta_value(&key, &encoded).await
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
    let existing = storage.get_meta_value(&key).await?;
    let mut envelope =
        BoundedBridgeResults::<OAuthUsageRefreshJobResult>::parse_or_empty(existing.as_deref());
    envelope.push_bounded(result);
    let encoded = serde_json::to_string(&envelope).map_err(|error| {
        cc_lb_storage_api::StorageError::Fatal {
            message: format!("serialize split usage refresh result: {error}"),
        }
    })?;
    storage.put_meta_value(&key, &encoded).await
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
    use super::*;

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
}
