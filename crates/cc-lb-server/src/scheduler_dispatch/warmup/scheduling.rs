use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};

use crate::warmup::stable_jitter_ms;

pub(super) const POST_RESET_GUARD_SECS: u64 = 30;

pub(super) fn next_warmup_task(
    upstream_id: uuid::Uuid,
    resets_at_unix_secs: u64,
) -> SchedulerPushTask<AdaptiveJob> {
    let jitter_secs = stable_jitter_ms(upstream_id, resets_at_unix_secs) / 1_000;
    let run_at_unix_secs = resets_at_unix_secs
        .saturating_add(POST_RESET_GUARD_SECS)
        .saturating_add(jitter_secs);
    let job = UpstreamWarmupJob::new(upstream_id, resets_at_unix_secs);
    SchedulerPushTask {
        args: AdaptiveJob::Warmup(job),
        idempotency_key: Some(job.idempotency_key(resets_at_unix_secs)),
        run_at_unix_secs: Some(run_at_unix_secs),
    }
}
