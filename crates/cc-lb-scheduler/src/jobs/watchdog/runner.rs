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
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WatchdogSeedStats {
    pub seeded: usize,
}

#[derive(Debug, Default)]
struct ActiveOAuthRefreshes {
    generation_pairs: HashSet<(Uuid, u64)>,
    legacy_upstream_ids: HashSet<Uuid>,
}

impl ActiveOAuthRefreshes {
    fn contains(&self, upstream_id: Uuid, expected_generation: u64) -> bool {
        self.legacy_upstream_ids.contains(&upstream_id)
            || self
                .generation_pairs
                .contains(&(upstream_id, expected_generation))
    }
}

pub async fn run_entity_watchdog(
    backend: &SchedulerBackend,
    kind: WatchdogEntityKind,
    upstream_ids: &[Uuid],
    tick_unix_secs: u64,
    run_at_unix_secs: u64,
) -> Result<WatchdogSeedStats> {
    let WatchdogEntityKind::Warmup = kind else {
        return Err(SchedulerError::Job(
            "generic watchdog only supports warmup jobs".to_owned(),
        ));
    };
    let active = active_entity_ids(backend, kind).await?;
    let mut seeded = 0;
    for upstream_id in upstream_ids {
        if active.contains(upstream_id) {
            continue;
        }
        let task = SchedulerPushTask {
            args: AdaptiveJob::Warmup(UpstreamWarmupJob::new(*upstream_id, tick_unix_secs)),
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

pub async fn run_oauth_refresh_watchdog(
    backend: &SchedulerBackend,
    upstream_generations: &[(Uuid, u64)],
    tick_unix_secs: u64,
    run_at_unix_secs: u64,
) -> Result<WatchdogSeedStats> {
    let kind = WatchdogEntityKind::OAuthRefresh;
    let active = active_oauth_refreshes(backend).await?;
    let mut seeded = 0;
    for &(upstream_id, expected_generation) in upstream_generations {
        if active.contains(upstream_id, expected_generation) {
            continue;
        }
        let task = SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::for_generation(
                upstream_id,
                expected_generation,
            )),
            idempotency_key: Some(kind.bootstrap_key(upstream_id, tick_unix_secs)),
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

async fn active_oauth_refreshes(backend: &SchedulerBackend) -> Result<ActiveOAuthRefreshes> {
    let mut active = ActiveOAuthRefreshes::default();
    for status in ACTIVE_STATUSES {
        let mut page = 1;
        loop {
            let filter = Filter {
                status: Some(status.clone()),
                page,
                page_size: Some(PAGE_SIZE),
            };
            let tasks = backend.list_adaptive_tasks(&filter).await?;
            for task in &tasks {
                let AdaptiveJob::OAuthRefresh(job) = &task.args else {
                    continue;
                };
                if let Some(expected_generation) = job.expected_generation {
                    active
                        .generation_pairs
                        .insert((job.upstream_id, expected_generation));
                } else {
                    active.legacy_upstream_ids.insert(job.upstream_id);
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
