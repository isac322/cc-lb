use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;
use crate::upstream::UpstreamKind;
use crate::upstream_subscription_quota::SubscriptionQuotaWindow;

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Independent warm-up schedule for one quota group of an upstream.
///
/// Every warm-up kind has a `Primary` lane. `gemini_oauth` upstreams also have a
/// `ThirdParty` lane for the Claude and GPT-OSS quota group, whose windows reset
/// independently of the Gemini group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupLane {
    #[default]
    Primary,
    ThirdParty,
}

impl WarmupLane {
    pub const ALL: [Self; 2] = [Self::Primary, Self::ThirdParty];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::ThirdParty => "third_party",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "primary" => Some(Self::Primary),
            "third_party" => Some(Self::ThirdParty),
            _ => None,
        }
    }

    /// Scheduler job-kind segment used in idempotency keys
    /// (`adaptive:{job_kind}:{upstream_id}:...`).
    pub const fn job_kind(self) -> &'static str {
        match self {
            Self::Primary => "warmup",
            Self::ThirdParty => "warmup_third_party",
        }
    }

    /// Lanes that exist for an upstream kind, before credential/quota eligibility.
    pub fn lanes_for_kind(kind: UpstreamKind) -> &'static [Self] {
        match kind {
            UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth => &[Self::Primary],
            UpstreamKind::GeminiOauth => &[Self::Primary, Self::ThirdParty],
            _ => &[],
        }
    }

    /// Quota windows this lane warms for an upstream kind.
    pub fn target_windows(self, kind: UpstreamKind) -> &'static [SubscriptionQuotaWindow] {
        match (kind, self) {
            (UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth, Self::Primary) => &[
                SubscriptionQuotaWindow::FiveHour,
                SubscriptionQuotaWindow::SevenDay,
            ],
            (UpstreamKind::GeminiOauth, Self::Primary) => &[
                SubscriptionQuotaWindow::GeminiFiveHour,
                SubscriptionQuotaWindow::GeminiSevenDay,
            ],
            (UpstreamKind::GeminiOauth, Self::ThirdParty) => &[
                SubscriptionQuotaWindow::ThirdPartyFiveHour,
                SubscriptionQuotaWindow::ThirdPartySevenDay,
            ],
            _ => &[],
        }
    }

    /// Weekly windows whose exhaustion blocks this lane (and only this lane).
    pub fn weekly_windows(self, kind: UpstreamKind) -> &'static [SubscriptionQuotaWindow] {
        match (kind, self) {
            (UpstreamKind::AnthropicOauth, Self::Primary) => &[
                SubscriptionQuotaWindow::SevenDay,
                SubscriptionQuotaWindow::SevenDaySonnet,
                SubscriptionQuotaWindow::SevenDayOpus,
                SubscriptionQuotaWindow::SevenDayFable,
            ],
            (UpstreamKind::OpenaiOauth, Self::Primary) => &[SubscriptionQuotaWindow::SevenDay],
            (UpstreamKind::GeminiOauth, Self::Primary) => {
                &[SubscriptionQuotaWindow::GeminiSevenDay]
            }
            (UpstreamKind::GeminiOauth, Self::ThirdParty) => {
                &[SubscriptionQuotaWindow::ThirdPartySevenDay]
            }
            _ => &[],
        }
    }
    /// Whether the decrypted OAuth credential can serve this lane's quota group.
    ///
    /// The Claude/GPT-OSS group is served only through the Antigravity profile;
    /// Gemini CLI grants have no third-party quota.
    pub fn credential_supports(
        self,
        kind: UpstreamKind,
        bundle: &cc_lb_aead::OAuthTokenBundle,
    ) -> bool {
        match self {
            Self::Primary => Self::lanes_for_kind(kind).contains(&Self::Primary),
            Self::ThirdParty => {
                kind == UpstreamKind::GeminiOauth
                    && matches!(
                        bundle.oauth_profile,
                        cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity
                    )
            }
        }
    }
}

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
    /// Every target window of the lane's quota group is reported absent, or the
    /// credential profile does not serve the group (e.g. Gemini CLI has no
    /// Claude/GPT-OSS quota). Nothing is sent.
    QuotaGroupUnavailable,
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
    /// Quota group this attempt warmed. Rows written before lanes existed read
    /// back as `Primary`.
    #[serde(default)]
    pub lane: WarmupLane,
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
    /// `None` returns attempts from every lane.
    pub lane: Option<WarmupLane>,
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
}

fn encode_base64(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let (chunks, remainder) = bytes.as_chunks::<3>();

    for &[first, second, third] in chunks {
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
    for (chunk_index, chunk) in bytes.as_chunks::<4>().0.iter().enumerate() {
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

    #[test]
    fn warmup_attempt_record_lane_serializes_and_defaults_to_primary() {
        let record = WarmupAttemptRecord {
            id: Uuid::from_u128(1),
            upstream_id: Uuid::from_u128(2),
            attempted_at_unix_secs: 10,
            completed_at_unix_secs: Some(11),
            scheduled_for_unix_secs: 10,
            trigger: WarmupAttemptTrigger::Scheduled,
            lane: WarmupLane::ThirdParty,
            outcome: WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable),
            dispatch_kind: Some(WarmupDispatchKind::NotDispatched),
            http_status: None,
            cycle_key: None,
            expected_cycle_key: None,
            idle_secs_since_prev_window: None,
            replica_id: None,
            lease_holder: None,
            upstream_spec_revision: 1,
            dialect_plugin_snapshot: None,
            error_detail: None,
        };

        let mut json = serde_json::to_value(&record).expect("serialize record");
        assert_eq!(json["lane"], "third_party");
        assert_eq!(json["status"], "skipped");
        assert_eq!(json["reason"], "quota_group_unavailable");
        let decoded: WarmupAttemptRecord =
            serde_json::from_value(json.clone()).expect("deserialize record");
        assert_eq!(decoded, record);

        json.as_object_mut()
            .expect("record serializes as an object")
            .remove("lane");
        let legacy: WarmupAttemptRecord =
            serde_json::from_value(json).expect("deserialize record without lane");
        assert_eq!(legacy.lane, WarmupLane::Primary);
    }
}
