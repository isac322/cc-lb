use std::collections::HashSet;

use apalis_core::backend::Filter;
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::warmup::UpstreamWarmupJob;
use crate::worker::{AdaptiveJob, SchedulerBackend, SchedulerPushTask, TaskStatus};

const PAGE_SIZE: u32 = 500;
const ACTIVE_STATUSES: [TaskStatus; 4] = [
    TaskStatus::Pending,
    TaskStatus::Queued,
    TaskStatus::Running,
    TaskStatus::Failed,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchdogEntityKind {
    Warmup,
    OAuthRefresh,
}

impl WatchdogEntityKind {
    pub const fn as_key_part(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::OAuthRefresh => "oauth_refresh",
        }
    }

    pub fn bootstrap_key(self, upstream_id: Uuid, tick_unix_secs: u64) -> String {
        format!(
            "adaptive:{}:{}:bootstrap:{}",
            self.as_key_part(),
            upstream_id,
            tick_unix_secs
        )
    }

    fn job(self, upstream_id: Uuid, tick_unix_secs: u64) -> AdaptiveJob {
        match self {
            Self::Warmup => {
                AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, tick_unix_secs))
            }
            Self::OAuthRefresh => AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WatchdogSeedStats {
    pub seeded: usize,
}

pub async fn run_entity_watchdog(
    backend: &SchedulerBackend,
    kind: WatchdogEntityKind,
    upstream_ids: &[Uuid],
    tick_unix_secs: u64,
    run_at_unix_secs: u64,
) -> Result<WatchdogSeedStats> {
    let active = active_entity_ids(backend, kind).await?;
    let mut seeded = 0;
    for upstream_id in upstream_ids {
        if active.contains(upstream_id) {
            continue;
        }
        let task = SchedulerPushTask {
            args: kind.job(*upstream_id, tick_unix_secs),
            idempotency_key: Some(kind.bootstrap_key(*upstream_id, tick_unix_secs)),
            run_at_unix_secs: Some(run_at_unix_secs),
            max_attempts: None,
        };
        match backend.push_adaptive_task(task).await {
            Ok(()) => seeded += 1,
            Err(SchedulerError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(WatchdogSeedStats { seeded })
}

async fn active_entity_ids(
    backend: &SchedulerBackend,
    kind: WatchdogEntityKind,
) -> Result<HashSet<Uuid>> {
    let mut active = HashSet::new();
    for status in ACTIVE_STATUSES {
        let is_failed = status == TaskStatus::Failed;
        let mut page = 1;
        loop {
            let filter = Filter {
                status: Some(status.clone()),
                page,
                page_size: Some(PAGE_SIZE),
            };
            let tasks = backend.list_adaptive_tasks(&filter).await?;
            for task in &tasks {
                if is_failed && task.attempts >= task.max_attempts {
                    continue;
                }
                if let Some(key) = task.idempotency_key.as_deref()
                    && let Some(upstream_id) = parse_entity_uuid(kind, key)
                {
                    active.insert(upstream_id);
                }
            }
            if tasks.len() < PAGE_SIZE as usize {
                break;
            }
            page += 1;
        }
    }
    Ok(active)
}

fn parse_entity_uuid(kind: WatchdogEntityKind, idempotency_key: &str) -> Option<Uuid> {
    let mut parts = idempotency_key.split(':');
    if parts.next()? != "adaptive" || parts.next()? != kind.as_key_part() {
        return None;
    }
    Uuid::parse_str(parts.next()?).ok()
}
