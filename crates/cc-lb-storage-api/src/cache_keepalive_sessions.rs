use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CacheTtl, StorageResult};

mod reads;

pub use reads::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveSessionStatus {
    Active,
    Terminal,
}

impl CacheKeepaliveSessionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveEnqueueState {
    Pending,
    Enqueued,
    Running,
}

impl CacheKeepaliveEnqueueState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Enqueued => "enqueued",
            Self::Running => "running",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveTerminalReason {
    Cancelled,
    Expired,
    MaxRefreshes,
    MaxDuration,
    CacheMiss,
    DispatchError,
    DecryptFailed,
    UnsupportedProvider,
    Stale,
}

impl CacheKeepaliveTerminalReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::MaxRefreshes => "max_refreshes",
            Self::MaxDuration => "max_duration",
            Self::CacheMiss => "cache_miss",
            Self::DispatchError => "dispatch_error",
            Self::DecryptFailed => "decrypt_failed",
            Self::UnsupportedProvider => "unsupported_provider",
            Self::Stale => "stale",
        }
    }

    pub const fn display_reason(self) -> &'static str {
        match self {
            Self::MaxRefreshes => "max renewals reached",
            Self::MaxDuration => "max duration reached (4h)",
            Self::Expired => "TTL expired before follow-up",
            Self::DispatchError => "renewal dispatch unavailable",
            Self::Cancelled => "session cancelled",
            Self::CacheMiss => "cache expired before follow-up",
            Self::DecryptFailed => "renewal payload could not be decrypted",
            Self::UnsupportedProvider => "renewal provider is unsupported",
            Self::Stale => "renewal session is stale",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveSessionRecord {
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub upstream_id: Uuid,
    pub generation: u64,
    pub refresh_count: u32,
    pub first_scheduled_at_unix_secs: u64,
    pub cache_anchor_at_unix_secs: u64,
    pub run_at_unix_secs: u64,
    pub ttl: CacheTtl,
    pub status: CacheKeepaliveSessionStatus,
    pub enqueue_state: CacheKeepaliveEnqueueState,
    pub running_since_unix_secs: Option<u64>,
    pub current_job_key: String,
    pub encrypted_payload: Vec<u8>,
    pub display_reason: String,
    pub error: Option<String>,
    pub config_snapshot: Option<CacheKeepaliveConfigSnapshot>,
    pub terminal_reason: Option<CacheKeepaliveTerminalReason>,
    pub expires_at_unix_secs: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
}

impl fmt::Debug for CacheKeepaliveSessionRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheKeepaliveSessionRecord")
            .field("session_key_hash", &self.session_key_hash)
            .field("principal_id", &self.principal_id)
            .field("accounting_key_id", &self.accounting_key_id)
            .field("upstream_id", &self.upstream_id)
            .field("generation", &self.generation)
            .field("refresh_count", &self.refresh_count)
            .field(
                "first_scheduled_at_unix_secs",
                &self.first_scheduled_at_unix_secs,
            )
            .field("cache_anchor_at_unix_secs", &self.cache_anchor_at_unix_secs)
            .field("run_at_unix_secs", &self.run_at_unix_secs)
            .field("ttl", &self.ttl)
            .field("status", &self.status)
            .field("enqueue_state", &self.enqueue_state)
            .field("running_since_unix_secs", &self.running_since_unix_secs)
            .field("current_job_key", &self.current_job_key)
            .field(
                "encrypted_payload",
                &format_args!("<{} bytes redacted>", self.encrypted_payload.len()),
            )
            .field("display_reason", &self.display_reason)
            .field("error", &self.error)
            .field("config_snapshot", &self.config_snapshot)
            .field("terminal_reason", &self.terminal_reason)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field("created_at_unix_secs", &self.created_at_unix_secs)
            .field("updated_at_unix_secs", &self.updated_at_unix_secs)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CacheKeepaliveReplaceRequest {
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub upstream_id: Uuid,
    pub cache_anchor_at_unix_secs: u64,
    pub ttl: CacheTtl,
    pub run_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
    pub encrypted_payload: Vec<u8>,
    pub display_reason: String,
    pub config_snapshot: CacheKeepaliveConfigSnapshot,
    pub now_unix_secs: u64,
}

impl fmt::Debug for CacheKeepaliveReplaceRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheKeepaliveReplaceRequest")
            .field("session_key_hash", &self.session_key_hash)
            .field("principal_id", &self.principal_id)
            .field("accounting_key_id", &self.accounting_key_id)
            .field("upstream_id", &self.upstream_id)
            .field("cache_anchor_at_unix_secs", &self.cache_anchor_at_unix_secs)
            .field("ttl", &self.ttl)
            .field("run_at_unix_secs", &self.run_at_unix_secs)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field(
                "encrypted_payload",
                &format_args!("<{} bytes redacted>", self.encrypted_payload.len()),
            )
            .field("display_reason", &self.display_reason)
            .field("config_snapshot", &self.config_snapshot)
            .field("now_unix_secs", &self.now_unix_secs)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CacheKeepaliveHitRefreshRequest {
    pub session_key_hash: String,
    pub generation: u64,
    pub cache_anchor_at_unix_secs: u64,
    pub run_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
    pub encrypted_payload: Vec<u8>,
    pub now_unix_secs: u64,
}

impl fmt::Debug for CacheKeepaliveHitRefreshRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheKeepaliveHitRefreshRequest")
            .field("session_key_hash", &self.session_key_hash)
            .field("generation", &self.generation)
            .field("cache_anchor_at_unix_secs", &self.cache_anchor_at_unix_secs)
            .field("run_at_unix_secs", &self.run_at_unix_secs)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field(
                "encrypted_payload",
                &format_args!("<{} bytes redacted>", self.encrypted_payload.len()),
            )
            .field("now_unix_secs", &self.now_unix_secs)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheKeepaliveGenerationCheck {
    pub generation: u64,
    pub status: CacheKeepaliveSessionStatus,
    pub enqueue_state: CacheKeepaliveEnqueueState,
}

#[async_trait]
pub trait CacheKeepaliveSessionStore: Send + Sync {
    async fn replace_from_real_request(
        &self,
        request: &CacheKeepaliveReplaceRequest,
    ) -> StorageResult<CacheKeepaliveSessionRecord>;

    async fn get_cache_keepalive_session(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>>;

    async fn mark_cache_keepalive_enqueued(
        &self,
        session_key_hash: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn claim_cache_keepalive_turn(
        &self,
        session_key_hash: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn update_cache_keepalive_payload(
        &self,
        session_key_hash: &str,
        generation: u64,
        encrypted_payload: &[u8],
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn check_cache_keepalive_generation(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveGenerationCheck>>;

    async fn reschedule_after_cache_hit(
        &self,
        request: &CacheKeepaliveHitRefreshRequest,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>>;

    async fn mark_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        generation: u64,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn mark_latest_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn purge_cache_keepalive_expired(&self, cutoff_unix_secs: u64) -> StorageResult<u64>;

    async fn purge_cache_keepalive_stale_pending(
        &self,
        cutoff_unix_secs: u64,
    ) -> StorageResult<u64>;
}

pub fn cache_keepalive_job_key(session_key_hash: &str, generation: u64) -> String {
    format!("cache_keepalive:{session_key_hash}:{generation}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_key_is_generation_scoped() {
        let key = cache_keepalive_job_key("abc123", 7);
        assert_eq!(key, "cache_keepalive:abc123:7");
    }

    #[test]
    fn durable_status_values_are_sqlite_compatible_text() {
        assert_eq!(CacheKeepaliveSessionStatus::Active.as_str(), "active");
        assert_eq!(CacheKeepaliveEnqueueState::Pending.as_str(), "pending");
        assert_eq!(CacheKeepaliveEnqueueState::Running.as_str(), "running");
        assert_eq!(
            CacheKeepaliveTerminalReason::DecryptFailed.as_str(),
            "decrypt_failed"
        );
    }

    #[test]
    fn session_record_debug_redacts_encrypted_payload() {
        let record = CacheKeepaliveSessionRecord {
            session_key_hash: "session".to_owned(),
            principal_id: "principal".to_owned(),
            accounting_key_id: Some("key-id".to_owned()),
            upstream_id: Uuid::from_u128(7),
            generation: 1,
            refresh_count: 0,
            first_scheduled_at_unix_secs: 10,
            cache_anchor_at_unix_secs: 10,
            run_at_unix_secs: 280,
            ttl: CacheTtl::Ttl5m,
            status: CacheKeepaliveSessionStatus::Active,
            enqueue_state: CacheKeepaliveEnqueueState::Pending,
            running_since_unix_secs: None,
            current_job_key: cache_keepalive_job_key("session", 1),
            encrypted_payload: b"ciphertext-bytes".to_vec(),
            display_reason: "agent-in-turn".to_owned(),
            error: None,
            config_snapshot: None,
            terminal_reason: None,
            expires_at_unix_secs: 310,
            created_at_unix_secs: 10,
            updated_at_unix_secs: 10,
        };

        let debug = format!("{record:?}");

        assert!(!debug.contains("ciphertext-bytes"));
        assert!(debug.contains("redacted"));
    }
}
