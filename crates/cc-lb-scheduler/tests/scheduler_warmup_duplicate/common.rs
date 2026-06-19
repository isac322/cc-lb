use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apalis::prelude::{Data, WorkerError};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::warmup::{
    UpstreamWarmupJob, UpstreamWarmupJobHandler, UpstreamWarmupOutcome, WarmupCycleEffects,
    set_after_dispatch_hook,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::scheduler_metrics;
use cc_lb_scheduler::worker::EntityJob;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const CYCLE_KEY: u64 = 1_800_000_000;
pub const COMPLETED_AT_UNIX_SECS: u64 = 1_800_000_001;
pub const WARMUP_JOB_TYPE: &str = "upstream_warmup";
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(8);
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);

pub type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;
pub type WarmupFireFuture =
    Pin<Box<dyn Future<Output = cc_lb_scheduler::error::Result<()>> + Send>>;
pub type SharedOutcomes = Arc<Mutex<Vec<JobOutcome>>>;

#[derive(Clone)]
pub struct FakeWarmupHttp {
    calls: Arc<AtomicUsize>,
    sent: Arc<tokio::sync::Notify>,
}

impl FakeWarmupHttp {
    pub fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            sent: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn dispatch(&self) -> impl FnOnce(UpstreamWarmupJob) -> WarmupFireFuture + Send + 'static {
        let http = self.clone();
        move |_| {
            Box::pin(async move {
                http.calls.fetch_add(1, Ordering::SeqCst);
                http.sent.notify_waiters();
                Ok(())
            })
        }
    }

    pub fn count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub async fn wait_for_count(&self, expected: usize) -> TestResult<()> {
        let wait = async {
            while self.count() < expected {
                self.sent.notified().await;
            }
        };
        tokio::time::timeout(WAIT_TIMEOUT, wait)
            .await
            .map_err(|_| format!("timed out waiting for {expected} fake warmup HTTP calls"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct HandlerState<Effects> {
    effects: Effects,
    http: FakeWarmupHttp,
    outcomes: SharedOutcomes,
}

impl<Effects> HandlerState<Effects> {
    pub fn new(effects: Effects, http: FakeWarmupHttp, outcomes: SharedOutcomes) -> Self {
        Self {
            effects,
            http,
            outcomes,
        }
    }

    fn record(&self, outcome: JobOutcome) -> Result<JobOutcome, SchedulerError> {
        self.outcomes
            .lock()
            .map_err(|_| SchedulerError::Job("warmup outcome lock poisoned".to_owned()))?
            .push(outcome.clone());
        Ok(outcome)
    }
}

pub fn entity_job_handler<Effects>(
    job: EntityJob,
    ctx: Data<HandlerState<Effects>>,
) -> HandlerFuture
where
    Effects: Clone + Send + Sync + WarmupCycleEffects + 'static,
{
    Box::pin(async move {
        let EntityJob::Warmup(warmup_job) = job else {
            return Ok(JobOutcome::Done);
        };
        let handler = UpstreamWarmupJobHandler::new(ctx.effects.clone());
        let outcome = handler
            .handle(
                warmup_job,
                COMPLETED_AT_UNIX_SECS,
                |_| async { Ok(true) },
                ctx.http.dispatch(),
            )
            .await?;
        let job_outcome = match outcome {
            UpstreamWarmupOutcome::Fired => JobOutcome::Done,
            UpstreamWarmupOutcome::AlreadyCompleted => JobOutcome::DuplicateEffect,
            UpstreamWarmupOutcome::UpstreamDeleted => JobOutcome::Skip,
        };
        ctx.record(job_outcome)
    })
}

pub fn run_with_recorder<Fut>(run: impl FnOnce(PrometheusHandle) -> Fut) -> TestResult<()>
where
    Fut: Future<Output = TestResult<()>>,
{
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || runtime.block_on(run(handle)))?;
    Ok(())
}

pub fn install_crash_hook_once() {
    let first_run = Arc::new(AtomicBool::new(true));
    set_after_dispatch_hook(Box::new(move || {
        let first_run = first_run.clone();
        Box::pin(async move {
            if first_run.swap(false, Ordering::SeqCst) {
                panic!("crash mid-fire");
            }
        })
    }));
}

pub fn duplicate_metric_value(handle: &PrometheusHandle) -> f64 {
    metric_value(
        &handle.render(),
        scheduler_metrics::JOBS_TOTAL,
        &[
            ("job_type", WARMUP_JOB_TYPE),
            ("status", "duplicate_effect"),
        ],
    )
}

pub fn recorded_duplicate_effects(outcomes: &SharedOutcomes) -> TestResult<usize> {
    let outcomes = outcomes
        .lock()
        .map_err(|_| "warmup outcome lock poisoned")?;
    Ok(outcomes
        .iter()
        .filter(|outcome| matches!(outcome, JobOutcome::DuplicateEffect))
        .count())
}

pub async fn stop_workers(
    cancel: CancellationToken,
    workers: [JoinHandle<Result<(), WorkerError>>; 2],
) -> TestResult<()> {
    cancel.cancel();
    for worker in workers {
        worker.await??;
    }
    Ok(())
}

fn metric_value(rendered: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let metric_prefix = format!("{name}{{");
    rendered
        .lines()
        .find(|line| {
            line.starts_with(&metric_prefix)
                && labels
                    .iter()
                    .all(|(label, value)| line.contains(&format!(r#"{label}="{value}""#)))
        })
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}
