use std::time::SystemTime;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::StorageResult;

pub const MAX_CHANGE_PAYLOAD_LEN: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeChannel {
    Upstream,
    Principal,
    PluginRegistry,
    PluginChain,
    UpstreamRateLimit,
    SubscriptionQuota,
    PromptCacheObservation,
}

impl ChangeChannel {
    pub const ALL: [Self; 7] = [
        Self::Upstream,
        Self::Principal,
        Self::PluginRegistry,
        Self::PluginChain,
        Self::UpstreamRateLimit,
        Self::SubscriptionQuota,
        Self::PromptCacheObservation,
    ];

    pub fn postgres_channel(self) -> &'static str {
        match self {
            Self::Upstream => "cclb_upstream_changed",
            Self::Principal => "cclb_principal_changed",
            Self::PluginRegistry => "cclb_plugin_changed",
            Self::PluginChain => "cclb_plugin_chain_changed",
            Self::UpstreamRateLimit => "cclb_upstream_rate_limit_changed",
            Self::SubscriptionQuota => "cclb_subscription_quota_changed",
            Self::PromptCacheObservation => "cclb_prompt_cache_observation_changed",
        }
    }

    pub fn from_postgres_channel(channel: &str) -> Option<Self> {
        match channel {
            "cclb_upstream_changed" => Some(Self::Upstream),
            "cclb_principal_changed" => Some(Self::Principal),
            "cclb_plugin_changed" => Some(Self::PluginRegistry),
            "cclb_plugin_chain_changed" => Some(Self::PluginChain),
            "cclb_upstream_rate_limit_changed" => Some(Self::UpstreamRateLimit),
            "cclb_subscription_quota_changed" => Some(Self::SubscriptionQuota),
            "cclb_prompt_cache_observation_changed" => Some(Self::PromptCacheObservation),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub channel: ChangeChannel,
    pub payload: String,
    pub observed_at: SystemTime,
}

impl ChangeEvent {
    pub fn new(channel: ChangeChannel, payload: impl AsRef<str>, observed_at: SystemTime) -> Self {
        Self {
            channel,
            payload: normalize_payload(payload.as_ref()),
            observed_at,
        }
    }
}

#[async_trait]
pub trait RuntimeChangeNotifier: Send + Sync {
    async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>>;

    async fn run(&self, cancel: CancellationToken) -> StorageResult<()>;
}

pub fn normalize_payload(payload: &str) -> String {
    payload.chars().take(MAX_CHANGE_PAYLOAD_LEN).collect()
}
