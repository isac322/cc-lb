use async_trait::async_trait;
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::{SubscriptionQuotaWindow, UpstreamRecord, WarmupLane};

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitResetUsageRefresh {
    Refreshed,
    Failed,
    NotApplicable,
}

#[async_trait]
pub trait LimitResetUsageRefreshPort: Send + Sync {
    async fn refresh_upstream_usage(
        &self,
        upstream_id: uuid::Uuid,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> LimitResetUsageRefresh;
}

#[derive(Clone, Debug, PartialEq)]
pub enum WarmupWindowCheck {
    /// Every reported target window is running after the request: no target
    /// still looks like an unused rolling window.
    Started {
        observations: Vec<UnifiedQuotaObservation>,
    },
    /// At least one reported target window is still an unused rolling window
    /// (zero usage, reset still moving with the poll time), so the warm-up did
    /// not start it.
    NotStarted {
        windows: Vec<SubscriptionQuotaWindow>,
    },
    /// The reading cannot tell: stale, observed before the request completed,
    /// polled too soon after the request to separate a fresh window from a
    /// rolling one, missing, or every target absent.
    Unknown,
}

#[async_trait]
pub trait WarmupWindowVerifier: Send + Sync {
    /// Verify the lane's target windows with a quota reading taken after the
    /// warm-up response completed. `previous_resets` holds, per target window,
    /// the reset the caller knew before dispatch; a running window that does
    /// not reach past its own previous reset was already running and is not
    /// evidence of a start.
    async fn verify_warmup_window(
        &self,
        upstream: &UpstreamRecord,
        lane: WarmupLane,
        previous_resets: &[(SubscriptionQuotaWindow, i64)],
        request_completed_at_unix_secs: i64,
    ) -> WarmupWindowCheck;
}
