use std::collections::HashSet;

use cc_lb_storage_api::WarmupLane;
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::warmup::UpstreamWarmupJob;
use crate::worker::{
    AdaptiveJob, AdaptiveTaskKeyState, Filter, SchedulerBackend, SchedulerPushTask, TaskStatus,
};

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
    WarmupThirdParty,
    OAuthRefresh,
}

impl WatchdogEntityKind {
    pub const fn for_warmup_lane(lane: WarmupLane) -> Self {
        match lane {
            WarmupLane::Primary => Self::Warmup,
            WarmupLane::ThirdParty => Self::WarmupThirdParty,
        }
    }

    pub const fn as_key_part(self) -> &'static str {
        match self.warmup_lane() {
            Some(lane) => lane.job_kind(),
            None => "oauth_refresh",
        }
    }

    /// Warm-up lane seeded by this watchdog kind, if it is a warm-up kind.
    pub const fn warmup_lane(self) -> Option<WarmupLane> {
        match self {
            Self::Warmup => Some(WarmupLane::Primary),
            Self::WarmupThirdParty => Some(WarmupLane::ThirdParty),
            Self::OAuthRefresh => None,
        }
    }

    pub fn bootstrap_key(self, upstream_id: Uuid, tick_unix_secs: u64) -> String {
        match self.warmup_lane() {
            Some(lane) => UpstreamWarmupJob::new_for_lane(upstream_id, tick_unix_secs, lane)
                .bootstrap_idempotency_key(tick_unix_secs),
            None => format!(
                "adaptive:{}:{}:bootstrap:{}",
                self.as_key_part(),
                upstream_id,
                tick_unix_secs
            ),
        }
    }

    fn job(self, upstream_id: Uuid, tick_unix_secs: u64) -> AdaptiveJob {
        match self.warmup_lane() {
            Some(lane) => AdaptiveJob::Warmup(UpstreamWarmupJob::new_for_lane(
                upstream_id,
                tick_unix_secs,
                lane,
            )),
            None => AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WatchdogSeedStats {
    pub seeded: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OAuthRefreshWatchdogTarget {
    pub upstream_id: Uuid,
    pub expected_generation: u64,
    pub expected_expires_at_unix_secs: u64,
    pub is_anthropic_oauth: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OAuthRefreshWatchdogResult {
    pub seeded: usize,
    pub seeded_targets: Vec<OAuthRefreshWatchdogTarget>,
}

pub async fn run_oauth_refresh_watchdog(
    backend: &SchedulerBackend,
    targets: &[OAuthRefreshWatchdogTarget],
    tick_unix_secs: u64,
) -> Result<OAuthRefreshWatchdogResult> {
    let active_jobs = active_oauth_refresh_jobs(backend).await?;
    let mut seeded = 0;
    let mut seeded_targets = Vec::new();
    for target in targets {
        let covered = active_jobs.iter().any(|job| {
            job.upstream_id == target.upstream_id
                && job.expected_generation == Some(target.expected_generation)
                && job
                    .expected_expires_at_unix_secs
                    .is_none_or(|exp| exp == target.expected_expires_at_unix_secs)
        });
        if covered {
            continue;
        }
        let job = OAuthRefreshJob::for_credential(
            target.upstream_id,
            target.expected_generation,
            target.expected_expires_at_unix_secs,
        );
        let run_at = OAuthRefreshJob::run_at_for_expires_at(target.expected_expires_at_unix_secs);
        let primary_key = job.generation_idempotency_key(
            target.expected_generation,
            target.expected_expires_at_unix_secs,
        );
        let primary_task = SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(job.clone()),
            idempotency_key: Some(primary_key.clone()),
            run_at_unix_secs: Some(run_at),
            max_attempts: None,
        };
        let pushed = match backend.push_adaptive_task(primary_task).await {
            Ok(()) => true,
            Err(SchedulerError::Conflict(_)) => {
                match backend.adaptive_task_key_state(&primary_key).await? {
                    AdaptiveTaskKeyState::Active => false,
                    AdaptiveTaskKeyState::Missing
                    | AdaptiveTaskKeyState::Done
                    | AdaptiveTaskKeyState::TerminalFailure => {
                        let recovery_key = job.watchdog_recovery_idempotency_key(
                            target.expected_generation,
                            target.expected_expires_at_unix_secs,
                            tick_unix_secs,
                        );
                        let recovery_task = SchedulerPushTask {
                            args: AdaptiveJob::OAuthRefresh(job),
                            idempotency_key: Some(recovery_key),
                            run_at_unix_secs: Some(run_at),
                            max_attempts: None,
                        };
                        match backend.push_adaptive_task(recovery_task).await {
                            Ok(()) => true,
                            Err(SchedulerError::Conflict(_)) => false,
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            Err(error) => return Err(error),
        };
        if pushed {
            seeded += 1;
            seeded_targets.push(*target);
        }
    }
    Ok(OAuthRefreshWatchdogResult {
        seeded,
        seeded_targets,
    })
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

async fn scan_active_adaptive_tasks<F>(backend: &SchedulerBackend, mut on_task: F) -> Result<()>
where
    F: FnMut(&crate::worker::SchedulerTaskRow<AdaptiveJob>),
{
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
                on_task(task);
            }
            if tasks.len() < PAGE_SIZE as usize {
                break;
            }
            page += 1;
        }
    }
    Ok(())
}

async fn active_oauth_refresh_jobs(backend: &SchedulerBackend) -> Result<Vec<OAuthRefreshJob>> {
    let mut jobs = Vec::new();
    scan_active_adaptive_tasks(backend, |task| {
        if let AdaptiveJob::OAuthRefresh(job) = &task.args {
            jobs.push(job.clone());
        }
    })
    .await?;
    Ok(jobs)
}

async fn active_entity_ids(
    backend: &SchedulerBackend,
    kind: WatchdogEntityKind,
) -> Result<HashSet<Uuid>> {
    let prefix = format!("adaptive:{}:", kind.as_key_part());
    let keys = backend
        .list_active_adaptive_idempotency_keys_by_prefix(&prefix)
        .await?;
    Ok(keys
        .into_iter()
        .filter_map(|key| parse_entity_uuid(kind, &key))
        .collect())
}

fn parse_entity_uuid(kind: WatchdogEntityKind, idempotency_key: &str) -> Option<Uuid> {
    let mut parts = idempotency_key.split(':');
    if parts.next()? != "adaptive" || parts.next()? != kind.as_key_part() {
        return None;
    }
    Uuid::parse_str(parts.next()?).ok()
}
