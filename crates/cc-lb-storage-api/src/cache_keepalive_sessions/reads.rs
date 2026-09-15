use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    CacheKeepaliveEnqueueState, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveTerminalReason,
};
use crate::{CacheTtl, StorageError, StorageResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveConfigSnapshot {
    pub refresh_lead_time_5m_secs: u32,
    pub refresh_lead_time_1h_secs: u32,
    pub max_refreshes_per_session: u32,
    pub max_total_duration_secs: u64,
    pub snapshot_max_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveSessionFilter {
    All,
    Renewed,
    Scheduled,
    Capped,
    Expired,
    NotTracked,
    Error,
}

impl CacheKeepaliveSessionFilter {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Renewed => "renewed",
            Self::Scheduled => "scheduled",
            Self::Capped => "capped",
            Self::Expired => "expired",
            Self::NotTracked => "not_tracked",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveSessionCursor {
    pub principal_id: String,
    pub horizon_start_ms: Option<u64>,
    pub filter: CacheKeepaliveSessionFilter,
    pub last_message_at_ms: u64,
    pub entry_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveSessionListQuery {
    pub principal_id: String,
    pub horizon_start_ms: Option<u64>,
    pub filter: CacheKeepaliveSessionFilter,
    pub cursor: Option<CacheKeepaliveSessionCursor>,
    pub limit: u32,
}

impl CacheKeepaliveSessionListQuery {
    pub fn validate_cursor(&self) -> StorageResult<()> {
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(());
        };
        if cursor.principal_id == self.principal_id
            && cursor.horizon_start_ms == self.horizon_start_ms
            && cursor.filter == self.filter
        {
            return Ok(());
        }
        Err(StorageError::InvalidInput {
            field: "cache_keepalive_session_cursor".to_owned(),
            reason: "cursor does not match principal, horizon, or filter".to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveSessionEntrySource {
    Session,
    Decision,
}

impl CacheKeepaliveSessionEntrySource {
    pub fn cursor_entry_id(self, id: &str) -> String {
        match self {
            Self::Session => format!("session:{id}"),
            Self::Decision => format!("decision:{id}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveSessionListItem {
    pub id: String,
    pub source: CacheKeepaliveSessionEntrySource,
    pub session_key_hash: Option<String>,
    pub principal_id: String,
    pub upstream_id: Uuid,
    pub last_message_at_ms: u64,
    pub ttl: CacheTtl,
    pub generation: u64,
    pub refresh_count: Option<u32>,
    pub status: Option<CacheKeepaliveSessionStatus>,
    pub enqueue_state: Option<CacheKeepaliveEnqueueState>,
    pub terminal_reason: Option<CacheKeepaliveTerminalReason>,
    pub decision: Option<String>,
    pub reason: String,
    pub error: Option<String>,
    pub config_snapshot: Option<CacheKeepaliveConfigSnapshot>,
}

impl CacheKeepaliveSessionListItem {
    pub const fn is_decision(&self) -> bool {
        matches!(self.source, CacheKeepaliveSessionEntrySource::Decision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKeepaliveSummaryInput {
    pub sessions: Vec<CacheKeepaliveSessionListItem>,
    pub recent_decisions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveSessionPage {
    pub rows: Vec<CacheKeepaliveSessionListItem>,
    pub next_cursor: Option<CacheKeepaliveSessionCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveTurnRecord {
    pub source_ref_id: String,
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub upstream_id: Uuid,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    pub cost_micros: i64,
    pub hit_miss: String,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKeepaliveDecisionRecord {
    pub source_ref_id: String,
    pub principal_id: String,
    pub session_key_hash: Option<String>,
    pub upstream_id: Uuid,
    pub decision: String,
    pub reason: String,
    pub error: Option<String>,
    pub generation: u64,
    pub ttl: CacheTtl,
    pub config_snapshot: Option<CacheKeepaliveConfigSnapshot>,
    pub last_message_at_ms: u64,
    pub ts: u64,
}

#[async_trait]
pub trait CacheKeepaliveSessionReadStore: Send + Sync {
    async fn list_cache_keepalive_sessions(
        &self,
        query: &CacheKeepaliveSessionListQuery,
    ) -> StorageResult<CacheKeepaliveSessionPage>;

    async fn read_cache_keepalive_summary_input(
        &self,
        principal_id: &str,
        cutoff_ms: u64,
    ) -> StorageResult<CacheKeepaliveSummaryInput>;

    async fn get_cache_keepalive_list_item(
        &self,
        principal_id: &str,
        id: &str,
    ) -> StorageResult<Option<CacheKeepaliveSessionListItem>>;

    async fn get_cache_keepalive_session_for_principal(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>>;

    async fn list_cache_keepalive_turns(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> StorageResult<Vec<CacheKeepaliveTurnRecord>>;

    async fn list_cache_keepalive_turns_for_sessions(
        &self,
        principal_id: &str,
        session_key_hashes: &[String],
    ) -> StorageResult<Vec<CacheKeepaliveTurnRecord>> {
        let mut session_key_hashes = session_key_hashes.to_vec();
        session_key_hashes.sort_unstable();
        session_key_hashes.dedup();

        let mut turns = Vec::new();
        for session_key_hash in session_key_hashes {
            turns.extend(
                self.list_cache_keepalive_turns(principal_id, &session_key_hash)
                    .await?,
            );
        }
        turns.sort_unstable_by(|left, right| {
            left.session_key_hash
                .cmp(&right.session_key_hash)
                .then_with(|| left.ts.cmp(&right.ts))
                .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
        });
        Ok(turns)
    }

    async fn get_cache_keepalive_decision_for_principal(
        &self,
        principal_id: &str,
        source_ref_id: &str,
    ) -> StorageResult<Option<CacheKeepaliveDecisionRecord>>;
}
