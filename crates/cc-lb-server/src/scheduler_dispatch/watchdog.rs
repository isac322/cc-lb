use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};
use uuid::Uuid;

use crate::scheduler_dispatch::storage::storage_scheduler_error;

use super::SchedulerDispatch;

const UPSTREAM_PAGE_LIMIT: usize = 1_000;

impl SchedulerDispatch {
    pub(super) async fn list_warmup_watchdog_upstream_ids(&self) -> SchedulerResult<Vec<Uuid>> {
        self.list_watchdog_upstream_ids(WatchdogUpstreamFilter::Warmup)
            .await
    }

    pub(super) async fn list_oauth_watchdog_upstream_ids(&self) -> SchedulerResult<Vec<Uuid>> {
        self.list_watchdog_upstream_ids(WatchdogUpstreamFilter::OAuth)
            .await
    }

    async fn list_watchdog_upstream_ids(
        &self,
        filter: WatchdogUpstreamFilter,
    ) -> SchedulerResult<Vec<Uuid>> {
        let mut upstream_ids = Vec::new();
        let mut after = None;
        loop {
            let page = UpstreamStore::list(self.storage.as_ref(), after, UPSTREAM_PAGE_LIMIT)
                .await
                .map_err(storage_scheduler_error)?;
            if page.is_empty() {
                break;
            }
            upstream_ids.extend(
                page.iter()
                    .filter(|upstream| filter.matches(upstream))
                    .map(|upstream| upstream.id),
            );
            after = page.last().map(|upstream| upstream.id);
            if page.len() < UPSTREAM_PAGE_LIMIT {
                break;
            }
        }
        Ok(upstream_ids)
    }
}

#[derive(Clone, Copy)]
enum WatchdogUpstreamFilter {
    Warmup,
    OAuth,
}

impl WatchdogUpstreamFilter {
    fn matches(self, upstream: &UpstreamRecord) -> bool {
        if upstream.kind != UpstreamKind::AnthropicOauth
            || !upstream.enabled
            || upstream.deleted_at_unix_secs.is_some()
            || upstream.oauth_credentials.is_none()
        {
            return false;
        }
        match self {
            Self::Warmup => upstream.warmup_enabled,
            Self::OAuth => true,
        }
    }
}
