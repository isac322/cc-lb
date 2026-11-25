use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    ManagedKey,
    ThreadStickySubagent,
    PluginRegistry,
    PluginChain,
    UpstreamRateLimit,
    SubscriptionQuota,
}

impl ChangeChannel {
    pub const ALL: [Self; 8] = [
        Self::Upstream,
        Self::Principal,
        Self::ManagedKey,
        Self::ThreadStickySubagent,
        Self::PluginRegistry,
        Self::PluginChain,
        Self::UpstreamRateLimit,
        Self::SubscriptionQuota,
    ];

    pub fn postgres_channel(self) -> &'static str {
        match self {
            Self::Upstream => "cclb_upstream_changed",
            Self::Principal => "cclb_principal_changed",
            Self::ManagedKey => "cclb_managed_key_changed",
            Self::ThreadStickySubagent => "cclb_thread_sticky_subagent_changed",
            Self::PluginRegistry => "cclb_plugin_changed",
            Self::PluginChain => "cclb_plugin_chain_changed",
            Self::UpstreamRateLimit => "cclb_upstream_rate_limit_changed",
            Self::SubscriptionQuota => "cclb_subscription_quota_changed",
        }
    }

    pub fn from_postgres_channel(channel: &str) -> Option<Self> {
        match channel {
            "cclb_upstream_changed" => Some(Self::Upstream),
            "cclb_principal_changed" => Some(Self::Principal),
            "cclb_managed_key_changed" => Some(Self::ManagedKey),
            "cclb_thread_sticky_subagent_changed" => Some(Self::ThreadStickySubagent),
            "cclb_plugin_changed" => Some(Self::PluginRegistry),
            "cclb_plugin_chain_changed" => Some(Self::PluginChain),
            "cclb_upstream_rate_limit_changed" => Some(Self::UpstreamRateLimit),
            "cclb_subscription_quota_changed" => Some(Self::SubscriptionQuota),
            _ => None,
        }
    }
}

/// Lock-free connectivity & epoch tracker for the runtime change listener.
///
/// Even values represent disconnected states; odd values represent active
/// connected sessions whose numeric value advances monotonically across
/// reconnects so caches can bind entries to one unbroken listener session.
#[derive(Debug)]
pub struct NotifierConnectionState {
    state: AtomicU64,
}

impl Default for NotifierConnectionState {
    fn default() -> Self {
        Self::new_disconnected()
    }
}

impl NotifierConnectionState {
    pub fn new_disconnected() -> Self {
        Self {
            state: AtomicU64::new(0),
        }
    }

    pub fn new_always_connected() -> Self {
        Self {
            state: AtomicU64::new(1),
        }
    }

    pub fn mark_connected(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            let next = if current & 1 == 0 {
                current.wrapping_add(1)
            } else {
                current.wrapping_add(2)
            };
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn mark_disconnected(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & 1 == 0 {
                break;
            }
            let next = current.wrapping_add(1);
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn current_epoch(&self) -> Option<u64> {
        let value = self.state.load(Ordering::Acquire);
        (value & 1 == 1).then_some(value)
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

    fn connection_state(&self) -> Option<Arc<NotifierConnectionState>> {
        None
    }
}

pub fn normalize_payload(payload: &str) -> String {
    payload.chars().take(MAX_CHANGE_PAYLOAD_LEN).collect()
}
