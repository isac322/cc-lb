//! Watchdog jobs for monitoring and health checks.

use serde::{Deserialize, Serialize};

use crate::middleware::TraceparentCarrier;

mod runner;
pub use runner::{WatchdogEntityKind, WatchdogSeedStats, run_entity_watchdog};

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
}

impl TraceparentCarrier for OAuthRefreshWatchdogJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}
