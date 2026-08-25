//! Watchdog jobs for monitoring and health checks.

use apalis_core::task::{Task, builder::TaskBuilder};
use serde::{Deserialize, Serialize};

use crate::middleware::TraceparentCarrier;

mod runner;
pub use runner::{
    WatchdogEntityKind, WatchdogSeedStats, run_entity_watchdog, run_oauth_refresh_watchdog,
};

/// Monitors upstream warmup cycle health and enqueues missing cycles.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupWatchdogJob {
    pub tick_unix_secs: u64,
    pub traceparent: Option<String>,
}

impl WarmupWatchdogJob {
    pub const fn new(tick_unix_secs: u64) -> Self {
        Self {
            tick_unix_secs,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self) -> String {
        format!("maintenance:warmup_watchdog:{}", self.tick_unix_secs)
    }

    pub fn into_apalis_task<Ctx, IdType>(self, run_at_unix_secs: u64) -> Task<Self, Ctx, IdType>
    where
        Ctx: Default,
    {
        let idempotency_key = self.idempotency_key();
        TaskBuilder::<Self, Ctx, IdType>::new(self)
            .run_at_timestamp(run_at_unix_secs)
            .with_idempotency_key(idempotency_key)
            .build()
    }
}

impl TraceparentCarrier for WarmupWatchdogJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

/// Monitors OAuth refresh token age and enqueues refreshes for aging tokens.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthRefreshWatchdogJob {
    pub tick_unix_secs: u64,
    pub traceparent: Option<String>,
}

impl OAuthRefreshWatchdogJob {
    pub const fn new(tick_unix_secs: u64) -> Self {
        Self {
            tick_unix_secs,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self) -> String {
        format!("maintenance:oauth_refresh_watchdog:{}", self.tick_unix_secs)
    }

    pub fn into_apalis_task<Ctx, IdType>(self, run_at_unix_secs: u64) -> Task<Self, Ctx, IdType>
    where
        Ctx: Default,
    {
        let idempotency_key = self.idempotency_key();
        TaskBuilder::<Self, Ctx, IdType>::new(self)
            .run_at_timestamp(run_at_unix_secs)
            .with_idempotency_key(idempotency_key)
            .build()
    }
}

impl TraceparentCarrier for OAuthRefreshWatchdogJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}
