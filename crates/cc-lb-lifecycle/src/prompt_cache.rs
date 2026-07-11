use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptCacheObservationWire {
    pub prefix_hash: String,
    pub ttl_class: cc_lb_domain::TtlClass,
    pub expires_at_unix_secs: u64,
    pub kind: PromptCacheObservationKindWire,
    #[serde(default)]
    pub prefix_content_block_index: u32,
    #[serde(default)]
    pub estimated_prefix_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheObservationKindWire {
    Hit,
    Write,
}
