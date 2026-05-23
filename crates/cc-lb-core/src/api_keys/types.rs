use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    Requests,
    InputTokens,
    OutputTokens,
    TotalTokens,
    CostUsd,
    Concurrent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Limit {
    pub kind: LimitKind,
    pub window: Duration,
    pub cap_micros: i64,
}

impl Limit {
    pub fn is_subset_of(&self, parent: &Limit) -> bool {
        self.kind == parent.kind
            && self.window == parent.window
            && self.cap_micros <= parent.cap_micros
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    AnthropicKey,
    AnthropicOAuth,
    AwsSigV4,
    GcpOAuth,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    Active,
    Disabled,
    Revoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalType {
    Human,
    Machine,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AllowSpec {
    pub exact: Vec<String>,
    pub globs: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageRow {
    pub key_id: String,
    pub principal_id: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cost_usd_micros: i64,
    pub duration_ms: u64,
    pub status: u16,
    pub ts_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_bincode_roundtrip() {
        let value = Limit {
            kind: LimitKind::Requests,
            window: Duration::from_secs(60),
            cap_micros: 123,
        };

        let encoded = bincode::serialize(&value).expect("encode limit");
        let decoded: Limit = bincode::deserialize(&encoded).expect("decode limit");

        assert_eq!(decoded, value);
    }

    #[test]
    fn upstream_kind_bincode_roundtrip() {
        let value = UpstreamKind::AnthropicOAuth;

        let encoded = bincode::serialize(&value).expect("encode upstream kind");
        let decoded: UpstreamKind = bincode::deserialize(&encoded).expect("decode upstream kind");

        assert_eq!(decoded, value);
    }

    #[test]
    fn key_status_bincode_roundtrip() {
        let value = KeyStatus::Disabled;

        let encoded = bincode::serialize(&value).expect("encode key status");
        let decoded: KeyStatus = bincode::deserialize(&encoded).expect("decode key status");

        assert_eq!(decoded, value);
    }

    #[test]
    fn is_subset_of_accepts_same_kind_window_and_lower_cap() {
        let parent = Limit {
            kind: LimitKind::OutputTokens,
            window: Duration::from_secs(300),
            cap_micros: 1_000,
        };
        let child = Limit {
            kind: LimitKind::OutputTokens,
            window: Duration::from_secs(300),
            cap_micros: 750,
        };

        assert!(child.is_subset_of(&parent));
    }

    #[test]
    fn is_subset_of_rejects_larger_cap() {
        let parent = Limit {
            kind: LimitKind::Requests,
            window: Duration::from_secs(60),
            cap_micros: 100,
        };
        let child = Limit {
            kind: LimitKind::Requests,
            window: Duration::from_secs(60),
            cap_micros: 200,
        };

        assert!(!child.is_subset_of(&parent));
    }

    #[test]
    fn is_subset_of_rejects_different_kind() {
        let parent = Limit {
            kind: LimitKind::Concurrent,
            window: Duration::from_secs(60),
            cap_micros: 1,
        };
        let child = Limit {
            kind: LimitKind::CostUsd,
            window: Duration::from_secs(60),
            cap_micros: 1,
        };

        assert!(!child.is_subset_of(&parent));
    }
}
