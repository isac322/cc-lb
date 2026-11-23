use async_trait::async_trait;
use cc_lb_control::{WarmupWindowCheck, WarmupWindowVerifier};
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;
use cc_lb_storage_api::{SubscriptionQuotaWindow, UpstreamRecord, WarmupLane};

use crate::scheduler_dispatch::SchedulerDispatch;
use crate::scheduler_dispatch::time::now_unix_millis;
use crate::scheduler_dispatch::warmup::quota::{
    WARMUP_WINDOW_MIN_GAP_SECS, post_warmup_window_check,
};
use crate::scheduler_dispatch::warmup::support::now_unix_secs_i64;

const POLL_TIMEOUT_SECS: u64 = 20;

#[async_trait]
impl WarmupWindowVerifier for SchedulerDispatch {
    async fn verify_warmup_window(
        &self,
        upstream: &UpstreamRecord,
        lane: WarmupLane,
        previous_resets: &[(SubscriptionQuotaWindow, i64)],
        request_completed_at_unix_secs: i64,
    ) -> WarmupWindowCheck {
        if upstream.kind != cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth {
            return WarmupWindowCheck::Unknown;
        }
        let dispatch = self.clone();
        let cancel = self.cancel.clone();
        let wait_until = request_completed_at_unix_secs.saturating_add(WARMUP_WINDOW_MIN_GAP_SECS);
        let now = match now_unix_secs_i64(&*dispatch.clock) {
            Ok(now) => now,
            Err(_) => return WarmupWindowCheck::Unknown,
        };
        tokio::select! {
            _ = cancel.cancelled() => return WarmupWindowCheck::Unknown,
            _ = async {
                if now < wait_until {
                    tokio::time::sleep(std::time::Duration::from_secs(
                        (wait_until - now) as u64,
                    )).await;
                }
            } => {}
        }
        tokio::select! {
            _ = cancel.cancelled() => WarmupWindowCheck::Unknown,
            result = tokio::time::timeout(
                std::time::Duration::from_secs(POLL_TIMEOUT_SECS),
                dispatch.poll_usage(upstream.id, None),
            ) => {
                if !matches!(result, Ok(Ok(OAuthUsagePollObservation::Success { .. }))) {
                    return WarmupWindowCheck::Unknown;
                }
                let view = dispatch.dynamic_view.load();
                let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
                drop(view);
                let snapshots = dispatch.subscription_quota_cache.snapshot_for_upstream(
                    upstream.id,
                    now_unix_millis(&*dispatch.clock),
                    max_staleness_secs,
                );
                post_warmup_window_check(
                    upstream.kind,
                    lane,
                    &snapshots,
                    previous_resets,
                    request_completed_at_unix_secs,
                )
            }
        }
    }
}
