use async_trait::async_trait;

use crate::StorageResult;

/// Default retention window for cluster-wide thread-sticky subagent latches.
pub const DEFAULT_THREAD_STICKY_SUBAGENT_TTL_SECS: u64 = 24 * 60 * 60;

#[async_trait]
pub trait ThreadStickySubagentStore: Send + Sync + 'static {
    async fn seed_thread_sticky_subagent(
        &self,
        principal_id: &str,
        observed_session_id: &str,
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<()>;

    /// Returns `Some(expires_at_unix_secs)` when `(principal_id, observed_session_id)` has an
    /// active entry past `now_unix_secs`, or `None` when absent or expired.
    async fn is_thread_sticky_subagent(
        &self,
        principal_id: &str,
        observed_session_id: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<u64>>;

    async fn purge_expired_thread_sticky_subagents(
        &self,
        now_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64>;
}
