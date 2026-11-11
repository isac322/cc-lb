use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CacheTtl, StorageResult};

pub const MAX_CACHE_KEEPALIVE_ERROR_BYTES: usize = 512;

#[must_use]
pub fn truncate_cache_keepalive_error(error: &str) -> &str {
    if error.len() <= MAX_CACHE_KEEPALIVE_ERROR_BYTES {
        return error;
    }
    let mut end = MAX_CACHE_KEEPALIVE_ERROR_BYTES;
    while end > 0 && !error.is_char_boundary(end) {
        end -= 1;
    }
    &error[..end]
}

mod reads;

pub use reads::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveSessionStatus {
    Active,
    /// Read-only projection of a bounded real request; never persisted as a status.
    InRequest,
    Terminal,
}

impl CacheKeepaliveSessionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::InRequest => "in_request",
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
            Self::MaxDuration => "max duration reached",
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
    pub cycle_id: Option<Uuid>,
    pub generation: u64,
    pub refresh_count: u32,
    pub first_scheduled_at_unix_secs: u64,
    pub cache_anchor_at_unix_secs: u64,
    /// Next keepalive time, or request deadline only for a terminal/cancelled/
    /// new_request marker with a non-null `running_since_unix_secs`.
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
    /// Specific real-request cancellation cause; absent on legacy rows.
    #[serde(default)]
    pub cancel_reason: Option<String>,
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
            .field("cycle_id", &self.cycle_id)
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
            .field("cancel_reason", &self.cancel_reason)
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
    pub cycle_id: Uuid,
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
            .field("cycle_id", &self.cycle_id)
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
    pub encrypted_payload: Option<Vec<u8>>,
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
                "encrypted_payload_bytes",
                &self.encrypted_payload.as_ref().map(Vec::len),
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

/// A request owner is unique within a leased ordering namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheKeepaliveCompletionTicket {
    pub epoch: Uuid,
    pub sequence: u64,
    /// Inclusive deadline; expired tickets must not mutate keepalive state.
    pub valid_until_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheKeepaliveOrderLease {
    pub epoch: Uuid,
    pub last_issued_at_unix_secs: u64,
    pub protected_until_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheKeepaliveOrderMeta {
    pub issued: u64,
    pub applied: u64,
    /// Missing only on legacy counters that cannot authorize leased tickets.
    pub lease: Option<CacheKeepaliveOrderLease>,
}

#[async_trait]
pub trait CacheKeepaliveSessionStore: Send + Sync {
    /// Returns the row image produced by this mutation, even if a later write supersedes it.
    async fn replace_from_real_request(
        &self,
        request: &CacheKeepaliveReplaceRequest,
    ) -> StorageResult<CacheKeepaliveSessionRecord>;

    /// Allocates a durable real-request ordering ticket.
    ///
    /// Allocate at request start and retain it through completion. Requests
    /// without a start ticket may allocate here at completion instead.
    ///
    /// Issuance records the request lifetime protection window in the existing
    /// counter write. Housekeeping may remove orphan metadata only after that
    /// window. A recreated namespace has a new epoch, so delayed old callbacks
    /// cannot acquire a later owner even when sequence numbers repeat.
    /// New namespaces inherit only the legacy issued floor, never its applied
    /// owner or epoch. Existing leased namespaces ignore subsequent legacy writes.
    async fn allocate_cache_keepalive_completion_ticket(
        &self,
        session_key_hash: &str,
        now_unix_secs: u64,
        protected_until_unix_secs: u64,
    ) -> StorageResult<CacheKeepaliveCompletionTicket>;

    /// Returns the accepted mutation's row image, not a later writer's session record.
    /// A provisional in-request marker accepts only its original epoch and
    /// applied sequence, with an unexpired ticket. A freshly allocated ticket
    /// cannot complete another owner's marker. Active and absent rows retain
    /// the strictly-newer applied-sequence fence; absent rows also reject
    /// tickets older than the latest issuance.
    /// `None` retains the unticketed timestamp-fence behavior.
    async fn replace_from_real_request_fenced(
        &self,
        request: &CacheKeepaliveReplaceRequest,
        expected_ticket: Option<CacheKeepaliveCompletionTicket>,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>> {
        let _ = expected_ticket;
        self.replace_from_real_request(request).await.map(Some)
    }

    /// Retires active keepalive work while a real request is in flight.
    ///
    /// Persists `terminal` / `cancelled` / `new_request`, with `running_since`
    /// set to `now_unix_secs` and `run_at` set to the server's request deadline.
    /// Only that marker interprets `run_at` as a request deadline. Its expiry is
    /// extended through the deadline even without a previous keepalive turn.
    /// Read models project this marker as `InRequest` before its deadline and
    /// stale afterward; legacy `new_request` rows without a start remain terminal.
    /// During a rolling upgrade, raw `first_scheduled_at` on this provisional
    /// marker is the inclusive ticket deadline plus one (clamped to BIGINT).
    /// This blocks deployed ticketed writers' logical-anchor predicate; the
    /// actual start remains in `running_since` and returned records normalize
    /// the logical anchor to it. Finalization restores a real logical anchor.
    /// This fence assumes request anchors use the same UTC clock domain as
    /// ticket deadlines. Unticketed direct API callers use the marker deadline
    /// instead and are not protected against deployed unfenced replacements.
    /// A newer request may replace an existing provisional marker. Final
    /// cancellation or real-request replacement resolves it.
    ///
    /// An accepted begin consumes the ticket as the marker's durable owner.
    /// Final cancellation or replacement must carry that same start ticket.
    /// Newer tickets may begin a subsequent request, but cannot complete another
    /// request's marker. Returns false without consuming the ticket if no
    /// eligible row is matched.
    async fn begin_cache_keepalive_request_fenced(
        &self,
        session_key_hash: &str,
        expected_ticket: Option<CacheKeepaliveCompletionTicket>,
        now_unix_secs: u64,
        request_deadline_unix_secs: u64,
    ) -> StorageResult<bool>;

    /// Finalizes active work or a provisional in-request marker, clearing its start.
    /// Ticketed marker completion requires its exact original start ticket;
    /// active-row cancellation still requires a strictly newer applied ticket.
    async fn cancel_cache_keepalive_fenced(
        &self,
        session_key_hash: &str,
        expected_ticket: Option<CacheKeepaliveCompletionTicket>,
        cancel_anchor_at_unix_secs: u64,
        reason: CacheKeepaliveTerminalReason,
        cancel_reason: Option<&str>,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;
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

    /// Performs the durable authorization check immediately before provider dispatch.
    ///
    /// Cancellation committed before this check prevents the send. Once this check
    /// succeeds, the in-flight attempt is allowed even if cancellation commits afterward.
    /// The row must still be active at `generation` and owned by this worker's running claim.
    async fn authorize_cache_keepalive_dispatch(
        &self,
        session_key_hash: &str,
        generation: u64,
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

    /// Returns the updated row image; a later replacement or cancellation may supersede it.
    async fn reschedule_after_cache_hit(
        &self,
        request: &CacheKeepaliveHitRefreshRequest,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>>;

    async fn mark_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        generation: u64,
        reason: CacheKeepaliveTerminalReason,
        error: Option<&str>,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn mark_latest_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn recover_cache_keepalive_stale_running(
        &self,
        stale_running_cutoff_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<u64>;

    /// Removes expired session payloads without deleting their ordering counters.
    async fn purge_cache_keepalive_expired(&self, cutoff_unix_secs: u64) -> StorageResult<u64>;

    async fn purge_cache_keepalive_stale_pending(
        &self,
        cutoff_unix_secs: u64,
    ) -> StorageResult<u64>;

    /// Removes at most `batch_size` expired decision and turn history rows.
    async fn prune_cache_keepalive_history_before(
        &self,
        cutoff_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let _ = (cutoff_unix_secs, batch_size);
        Ok(0)
    }
}

pub fn cache_keepalive_job_key(session_key_hash: &str, generation: u64) -> String {
    format!("cache_keepalive:{session_key_hash}:{generation}")
}

/// Isolated leased-ticket namespace; deployed `keepalive_order:%` GC cannot match it.
pub const CACHE_KEEPALIVE_ORDER_META_PREFIX: &str = "keepalive_order_v2:";
/// Read-only migration source. New writers must never modify legacy counters.
pub const CACHE_KEEPALIVE_LEGACY_ORDER_META_PREFIX: &str = "keepalive_order:";

pub fn cache_keepalive_order_meta_key(session_key_hash: &str) -> String {
    format!("{CACHE_KEEPALIVE_ORDER_META_PREFIX}{session_key_hash}")
}

/// Returns the read-only issued-floor source, never a leased owner namespace.
pub fn cache_keepalive_legacy_order_meta_key(session_key_hash: &str) -> String {
    format!("{CACHE_KEEPALIVE_LEGACY_ORDER_META_PREFIX}{session_key_hash}")
}

/// Raw provisional-marker fence understood by deployed ticketed writers.
/// Saturation covers impossible/future deadlines without exceeding SQL BIGINT.
pub fn cache_keepalive_legacy_request_anchor_fence(protected_until_unix_secs: u64) -> i64 {
    protected_until_unix_secs
        .saturating_add(1)
        .min(i64::MAX as u64) as i64
}

/// Decodes legacy counters and leased namespaces without resetting corrupt state.
pub fn parse_keepalive_order_meta(value: Option<&str>) -> StorageResult<CacheKeepaliveOrderMeta> {
    let Some(raw) = value else {
        return Ok(CacheKeepaliveOrderMeta::default());
    };
    let invalid = || crate::StorageError::Fatal {
        message: "invalid cache keepalive order metadata".to_owned(),
    };
    let number = |field: Option<&str>| -> StorageResult<u64> {
        let field = field.ok_or_else(invalid)?;
        if field.is_empty() || !field.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        field.parse().map_err(|_| invalid())
    };
    let mut fields = raw.split(':');
    let first = fields.next();
    let (issued, applied, lease) = if first == Some("v2") {
        let protected_until_unix_secs = number(fields.next())?;
        let last_issued_at_unix_secs = number(fields.next())?;
        let epoch = fields
            .next()
            .ok_or_else(invalid)?
            .parse::<Uuid>()
            .map_err(|_| invalid())?;
        let issued = number(fields.next())?;
        let applied = number(fields.next())?;
        (
            issued,
            applied,
            Some(CacheKeepaliveOrderLease {
                epoch,
                last_issued_at_unix_secs,
                protected_until_unix_secs,
            }),
        )
    } else {
        (number(first)?, number(fields.next())?, None)
    };
    if fields.next().is_some() || applied > issued {
        return Err(invalid());
    }
    Ok(CacheKeepaliveOrderMeta {
        issued,
        applied,
        lease,
    })
}

pub fn format_keepalive_order_meta(meta: &CacheKeepaliveOrderMeta) -> String {
    match meta.lease {
        Some(lease) => format!(
            "v2:{}:{}:{}:{}:{}",
            lease.protected_until_unix_secs,
            lease.last_issued_at_unix_secs,
            lease.epoch,
            meta.issued,
            meta.applied,
        ),
        None => format!("{}:{}", meta.issued, meta.applied),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_metadata_round_trips_and_rejects_corruption() {
        assert_eq!(
            parse_keepalive_order_meta(None).unwrap(),
            CacheKeepaliveOrderMeta::default()
        );
        let legacy = parse_keepalive_order_meta(Some("9:7")).unwrap();
        assert_eq!(
            legacy,
            CacheKeepaliveOrderMeta {
                issued: 9,
                applied: 7,
                lease: None
            }
        );
        assert_eq!(format_keepalive_order_meta(&legacy), "9:7");
        let leased = CacheKeepaliveOrderMeta {
            lease: Some(CacheKeepaliveOrderLease {
                epoch: Uuid::from_u128(7),
                last_issued_at_unix_secs: 100,
                protected_until_unix_secs: u64::MAX,
            }),
            ..legacy
        };
        assert_eq!(
            parse_keepalive_order_meta(Some(&format_keepalive_order_meta(&leased))).unwrap(),
            leased
        );
        assert_eq!(
            format_keepalive_order_meta(&leased),
            format!(
                "v2:{}:100:00000000-0000-0000-0000-000000000007:9:7",
                u64::MAX
            ),
        );
        for raw in [
            "",
            "garbage",
            "1",
            "1:2",
            "1:0:0",
            "+1:0",
            "18446744073709551616:0",
            "v3:200:100:00000000-0000-0000-0000-000000000007:1:0",
            "v2:no:100:00000000-0000-0000-0000-000000000007:1:0",
            "v2:200:100:bad:1:0",
            "v2:200:100:00000000-0000-0000-0000-000000000007:1:0:extra",
        ] {
            assert!(parse_keepalive_order_meta(Some(raw)).is_err(), "{raw}");
        }
    }

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
    fn in_request_projection_serializes_as_in_request() {
        let status = CacheKeepaliveSessionStatus::InRequest;
        assert_eq!(status.as_str(), "in_request");
        assert_eq!(serde_json::to_string(&status).unwrap(), "\"in_request\"");
        assert_eq!(
            serde_json::from_str::<CacheKeepaliveSessionStatus>("\"in_request\"").unwrap(),
            status
        );
    }

    #[test]
    fn session_record_debug_redacts_encrypted_payload() {
        let record = CacheKeepaliveSessionRecord {
            session_key_hash: "session".to_owned(),
            principal_id: "principal".to_owned(),
            accounting_key_id: Some("key-id".to_owned()),
            upstream_id: Uuid::from_u128(7),
            cycle_id: None,
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
            error: Some("status=400 body=invalid thinking".to_owned()),
            config_snapshot: None,
            terminal_reason: None,
            cancel_reason: None,
            expires_at_unix_secs: 310,
            created_at_unix_secs: 10,
            updated_at_unix_secs: 10,
        };

        let debug = format!("{record:?}");

        assert!(!debug.contains("ciphertext-bytes"));
        assert!(debug.contains("error: Some(\"status=400 body=invalid thinking\")"));
        assert!(debug.contains("redacted"));
    }
}

#[cfg(test)]
#[test]
fn legacy_request_anchor_fence_is_inclusive_and_bigint_bounded() {
    assert_eq!(cache_keepalive_legacy_request_anchor_fence(900), 901);
    assert_eq!(
        cache_keepalive_legacy_request_anchor_fence(i64::MAX as u64 - 1),
        i64::MAX
    );
    assert_eq!(
        cache_keepalive_legacy_request_anchor_fence(i64::MAX as u64),
        i64::MAX
    );
    assert_eq!(
        cache_keepalive_legacy_request_anchor_fence(u64::MAX),
        i64::MAX
    );
}
