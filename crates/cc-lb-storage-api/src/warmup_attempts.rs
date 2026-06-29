use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupAttemptStatus {
    Success,
    Skipped,
    TransientFailure,
    PermanentFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupSuccessReason {
    CycleAdvanced,
    WindowAlreadyActive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupSkipReason {
    SevenDayQuotaExhausted,
    UpstreamDisabled,
    UpstreamDeleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupTransientFailureReason {
    RateLimitedCycleKeyMissing,
    #[serde(rename = "upstream_5xx")]
    Upstream5xx,
    NetworkError,
    RequestTimeout,
    DialectPluginTransient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupPermanentFailureReason {
    OauthCredentialsMissing,
    RequestBuildFailed,
    OauthRefreshFailed,
    CredentialDecryptFailed,
    AuthFailed,
    Forbidden,
    BadRequest,
    NotFound,
    DialectPluginFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "status", content = "reason", rename_all = "snake_case")]
pub enum WarmupAttemptOutcome {
    Success(WarmupSuccessReason),
    Skipped(WarmupSkipReason),
    TransientFailure(WarmupTransientFailureReason),
    PermanentFailure(WarmupPermanentFailureReason),
}

impl WarmupAttemptOutcome {
    pub fn status(self) -> WarmupAttemptStatus {
        match self {
            Self::Success(_) => WarmupAttemptStatus::Success,
            Self::Skipped(_) => WarmupAttemptStatus::Skipped,
            Self::TransientFailure(_) => WarmupAttemptStatus::TransientFailure,
            Self::PermanentFailure(_) => WarmupAttemptStatus::PermanentFailure,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupDispatchKind {
    NotDispatched,
    Http,
    DialectPlugin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupAttemptTrigger {
    Scheduled,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WarmupAttemptRecord {
    pub id: Uuid,
    pub upstream_id: Uuid,
    pub attempted_at_unix_secs: i64,
    pub completed_at_unix_secs: Option<i64>,
    pub scheduled_for_unix_secs: i64,
    pub trigger: WarmupAttemptTrigger,
    #[serde(flatten)]
    pub outcome: WarmupAttemptOutcome,
    pub dispatch_kind: Option<WarmupDispatchKind>,
    pub http_status: Option<i32>,
    pub cycle_key: Option<i64>,
    pub expected_cycle_key: Option<i64>,
    pub idle_secs_since_prev_window: Option<i64>,
    pub replica_id: Option<Uuid>,
    pub lease_holder: Option<String>,
    pub upstream_spec_revision: i64,
    pub dialect_plugin_snapshot: Option<serde_json::Value>,
    pub error_detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WarmupAttemptCursor {
    pub attempted_at_unix_secs: i64,
    pub id: Uuid,
}

impl WarmupAttemptCursor {
    pub fn encode(&self) -> String {
        encode_base64(format!("{}|{}", self.attempted_at_unix_secs, self.id).as_bytes())
    }

    pub fn decode(encoded: &str) -> Result<Self, String> {
        let decoded_bytes = decode_base64(encoded)?;
        let decoded = String::from_utf8(decoded_bytes)
            .map_err(|error| format!("warmup cursor is not valid UTF-8: {error}"))?;
        let (attempted_at, id) = decoded
            .split_once('|')
            .ok_or_else(|| "warmup cursor must contain attempted_at and id".to_owned())?;
        let attempted_at_unix_secs = attempted_at
            .parse::<i64>()
            .map_err(|error| format!("warmup cursor attempted_at is invalid: {error}"))?;
        let id =
            Uuid::parse_str(id).map_err(|error| format!("warmup cursor id is invalid: {error}"))?;
        Ok(Self {
            attempted_at_unix_secs,
            id,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmupAttemptListFilters {
    pub limit: Option<u32>,
    pub before: Option<WarmupAttemptCursor>,
    pub status: Option<WarmupAttemptStatus>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmupAttemptSummary {
    pub success: u64,
    pub skipped: u64,
    pub transient_failure: u64,
    pub permanent_failure: u64,
}

#[async_trait]
pub trait UpstreamWarmupAttemptStore: Send + Sync {
    async fn insert_warmup_attempt(&self, attempt: &WarmupAttemptRecord) -> StorageResult<()>;

    /// Returns attempts ordered by `attempted_at_unix_secs DESC, id DESC`.
    ///
    /// When `filters.limit` is `None`, backends use a default limit of 50.
    /// When `filters.before` is present, backends return rows older than the
    /// cursor pair in the same ordering.
    async fn list_warmup_attempts_for_upstream(
        &self,
        upstream_id: Uuid,
        filters: WarmupAttemptListFilters,
    ) -> StorageResult<Vec<WarmupAttemptRecord>>;

    /// Counts outcomes for attempts with `attempted_at_unix_secs >= cutoff_unix_secs`. The caller
    /// owns the cutoff so backends stay clock-free and behave identically across SQL dialects.
    async fn summarize_recent_warmup_attempts(
        &self,
        upstream_id: Uuid,
        cutoff_unix_secs: i64,
    ) -> StorageResult<WarmupAttemptSummary>;

    async fn latest_warmup_attempt_for_upstream(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<WarmupAttemptRecord>>;
}

fn encode_base64(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut chunks = bytes.chunks_exact(3);

    for chunk in chunks.by_ref() {
        let first = chunk[0];
        let second = chunk[1];
        let third = chunk[2];
        encoded.push(char::from(BASE64_ALPHABET[usize::from(first >> 2)]));
        encoded.push(char::from(
            BASE64_ALPHABET[usize::from(((first & 0b0000_0011) << 4) | (second >> 4))],
        ));
        encoded.push(char::from(
            BASE64_ALPHABET[usize::from(((second & 0b0000_1111) << 2) | (third >> 6))],
        ));
        encoded.push(char::from(
            BASE64_ALPHABET[usize::from(third & 0b0011_1111)],
        ));
    }

    let remainder = chunks.remainder();
    if let Some((first, tail)) = remainder.split_first() {
        let first = *first;
        if let Some(second) = tail.first() {
            let second = *second;
            encoded.push(char::from(BASE64_ALPHABET[usize::from(first >> 2)]));
            encoded.push(char::from(
                BASE64_ALPHABET[usize::from(((first & 0b0000_0011) << 4) | (second >> 4))],
            ));
            encoded.push(char::from(
                BASE64_ALPHABET[usize::from((second & 0b0000_1111) << 2)],
            ));
            encoded.push('=');
        } else {
            encoded.push(char::from(BASE64_ALPHABET[usize::from(first >> 2)]));
            encoded.push(char::from(
                BASE64_ALPHABET[usize::from((first & 0b0000_0011) << 4)],
            ));
            encoded.push('=');
            encoded.push('=');
        }
    }

    encoded
}

fn decode_base64(encoded: &str) -> Result<Vec<u8>, String> {
    let bytes = encoded.as_bytes();
    if bytes.is_empty() {
        return Err("warmup cursor is empty".to_owned());
    }
    if !bytes.len().is_multiple_of(4) {
        return Err("warmup cursor base64 length must be a multiple of 4".to_owned());
    }

    let chunk_count = bytes.len() / 4;
    let mut decoded = Vec::with_capacity(chunk_count * 3);
    for (chunk_index, chunk) in bytes.chunks_exact(4).enumerate() {
        if chunk_index + 1 != chunk_count && chunk.contains(&b'=') {
            return Err("warmup cursor base64 padding must be in the final chunk".to_owned());
        }
        if chunk[0] == b'=' || chunk[1] == b'=' {
            return Err("warmup cursor base64 padding is invalid".to_owned());
        }

        let first = decode_base64_byte(chunk[0])?;
        let second = decode_base64_byte(chunk[1])?;
        let third = if chunk[2] == b'=' {
            0
        } else {
            decode_base64_byte(chunk[2])?
        };
        let fourth = if chunk[3] == b'=' {
            0
        } else {
            decode_base64_byte(chunk[3])?
        };

        decoded.push((first << 2) | (second >> 4));
        match (chunk[2] == b'=', chunk[3] == b'=') {
            (true, true) => {}
            (false, true) => decoded.push(((second & 0b0000_1111) << 4) | (third >> 2)),
            (false, false) => {
                decoded.push(((second & 0b0000_1111) << 4) | (third >> 2));
                decoded.push(((third & 0b0000_0011) << 6) | fourth);
            }
            (true, false) => return Err("warmup cursor base64 padding is invalid".to_owned()),
        }
    }

    Ok(decoded)
}

fn decode_base64_byte(byte: u8) -> Result<u8, String> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err("warmup cursor contains a non-base64 byte".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_attempt_cursor_encodes_like_mock_server() {
        let cursor = WarmupAttemptCursor {
            attempted_at_unix_secs: 1_718_380_800,
            id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
        };

        assert_eq!(
            cursor.encode(),
            "MTcxODM4MDgwMHwxMjM0NTY3OC05MGFiLWNkZWYtMTIzNC01Njc4OTBhYmNkZWY="
        );
    }

    #[test]
    fn warmup_attempt_cursor_roundtrips() {
        let cursor = WarmupAttemptCursor {
            attempted_at_unix_secs: 1_718_380_800,
            id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
        };

        assert_eq!(WarmupAttemptCursor::decode(&cursor.encode()), Ok(cursor));
    }

    #[test]
    fn warmup_outcome_flattens_to_status_and_reason_pair() {
        let outcome = WarmupAttemptOutcome::TransientFailure(
            WarmupTransientFailureReason::RateLimitedCycleKeyMissing,
        );
        let json = serde_json::to_string(&outcome).expect("serialize outcome");
        assert_eq!(
            json,
            r#"{"status":"transient_failure","reason":"rate_limited_cycle_key_missing"}"#
        );

        let upstream_5xx =
            WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::Upstream5xx);
        let upstream_5xx_json = serde_json::to_string(&upstream_5xx).expect("serialize outcome");
        assert_eq!(
            upstream_5xx_json,
            r#"{"status":"transient_failure","reason":"upstream_5xx"}"#
        );

        let success = WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive);
        let success_json = serde_json::to_string(&success).expect("serialize outcome");
        assert_eq!(
            success_json,
            r#"{"status":"success","reason":"window_already_active"}"#
        );
    }
}
