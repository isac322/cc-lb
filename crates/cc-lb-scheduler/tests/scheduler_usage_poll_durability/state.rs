use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use apalis::prelude::Data;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    OAuthUsagePollCursorRepository, OAuthUsagePollHandler, OAuthUsagePollJob,
    OAuthUsagePollObservation,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::OAuthUsagePollScheduleConfig;
use cc_lb_scheduler::worker::EntityJob;
use http::StatusCode;
use sqlx::Database;

use crate::common::{FIRST_SUCCESS_AT, POLL_INTERVAL, TestResult, WAIT_TIMEOUT, schedule_config};
use crate::fake::raw_http;

const USAGE_BODY: &[u8] = br#"{"model":"claude-3-5-haiku-20241022","messages":[{"role":"user","content":"usage poll"}],"max_tokens":1}"#;

pub type HandlerFuture = Pin<Box<dyn Future<Output = SchedulerResult<JobOutcome>> + Send>>;

pub struct UsagePollWorkerState<Db: Database> {
    handler: Arc<OAuthUsagePollHandler<Db>>,
    config: OAuthUsagePollScheduleConfig,
    script: UsagePollScript,
    probe: UsagePollProbe,
    now_unix_secs: Arc<AtomicU64>,
}

impl<Db: Database> Clone for UsagePollWorkerState<Db> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            config: self.config.clone(),
            script: self.script.clone(),
            probe: self.probe.clone(),
            now_unix_secs: self.now_unix_secs.clone(),
        }
    }
}

impl<Db: Database> UsagePollWorkerState<Db> {
    pub fn new(handler: OAuthUsagePollHandler<Db>, usage_url: String) -> Self {
        Self {
            handler: Arc::new(handler),
            config: schedule_config(),
            script: UsagePollScript::new(usage_url),
            probe: UsagePollProbe::new(),
            now_unix_secs: Arc::new(AtomicU64::new(FIRST_SUCCESS_AT)),
        }
    }

    pub fn config(&self) -> &OAuthUsagePollScheduleConfig {
        &self.config
    }

    pub fn set_now(&self, now_unix_secs: u64) {
        self.now_unix_secs.store(now_unix_secs, Ordering::SeqCst);
    }

    pub fn enqueue_success(&self, observed_at_unix_secs: u64) -> TestResult<()> {
        self.script.enqueue(UsagePollStep::Success {
            observed_at_unix_secs,
        })
    }

    pub fn enqueue_throttle(&self, observed_at_unix_secs: u64) -> TestResult<()> {
        self.script.enqueue(UsagePollStep::Throttle {
            observed_at_unix_secs,
        })
    }

    pub fn http_call_count(&self) -> usize {
        self.script.http_call_count()
    }

    pub async fn wait_for_outcome_count(&self, expected: usize) -> TestResult<JobOutcome> {
        self.probe.wait_for_outcome_count(expected).await
    }

    fn now(&self) -> u64 {
        self.now_unix_secs.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct UsagePollScript {
    usage_url: Arc<str>,
    steps: Arc<Mutex<VecDeque<UsagePollStep>>>,
    http_calls: Arc<AtomicUsize>,
}

impl UsagePollScript {
    fn new(usage_url: String) -> Self {
        Self {
            usage_url: Arc::from(usage_url),
            steps: Arc::new(Mutex::new(VecDeque::new())),
            http_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn enqueue(&self, step: UsagePollStep) -> TestResult<()> {
        self.steps
            .lock()
            .map_err(|_| "usage poll script lock poisoned")?
            .push_back(step);
        Ok(())
    }

    fn http_call_count(&self) -> usize {
        self.http_calls.load(Ordering::SeqCst)
    }

    async fn poll(&self, _job: OAuthUsagePollJob) -> SchedulerResult<OAuthUsagePollObservation> {
        let step = self.next_step()?;
        self.http_calls.fetch_add(1, Ordering::SeqCst);
        let response = raw_http(
            "POST",
            self.usage_url.as_ref(),
            step.headers().as_slice(),
            USAGE_BODY,
        )
        .await
        .map_err(|error| SchedulerError::Job(format!("fake usage HTTP failed: {error}")))?;
        step.observation(response.status)
    }

    fn next_step(&self) -> SchedulerResult<UsagePollStep> {
        self.steps
            .lock()
            .map_err(|_| SchedulerError::Job("usage poll script lock poisoned".to_owned()))?
            .pop_front()
            .ok_or_else(|| SchedulerError::Job("unexpected usage HTTP during cooldown".to_owned()))
    }
}

#[derive(Clone, Copy)]
enum UsagePollStep {
    Success { observed_at_unix_secs: u64 },
    Throttle { observed_at_unix_secs: u64 },
}

impl UsagePollStep {
    fn headers(self) -> Vec<(&'static str, &'static str)> {
        let mut headers = vec![
            ("content-type", "application/json"),
            ("x-api-key", "sk-ant-test"),
        ];
        if matches!(self, Self::Throttle { .. }) {
            headers.push(("x-fake-mode", "429"));
        }
        headers
    }

    fn observation(self, status: StatusCode) -> SchedulerResult<OAuthUsagePollObservation> {
        match (self, status) {
            (
                Self::Success {
                    observed_at_unix_secs,
                },
                StatusCode::OK,
            ) => Ok(OAuthUsagePollObservation::Success {
                observed_at_unix_secs,
                window_start_unix_millis: observed_at_unix_secs.saturating_mul(1_000),
                window_end_unix_millis: observed_at_unix_secs
                    .saturating_add(60)
                    .saturating_mul(1_000),
            }),
            (
                Self::Throttle {
                    observed_at_unix_secs,
                },
                StatusCode::TOO_MANY_REQUESTS,
            ) => Ok(OAuthUsagePollObservation::Throttled {
                observed_at_unix_secs,
            }),
            (_, status) => Err(SchedulerError::Job(format!(
                "fake usage endpoint returned unexpected status {status}"
            ))),
        }
    }
}

#[derive(Clone)]
struct UsagePollProbe {
    outcomes: Arc<Mutex<Vec<JobOutcome>>>,
}

impl UsagePollProbe {
    fn new() -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record(&self, outcome: JobOutcome) -> SchedulerResult<()> {
        self.outcomes
            .lock()
            .map_err(|_| SchedulerError::Job("usage poll outcome lock poisoned".to_owned()))?
            .push(outcome);
        Ok(())
    }

    async fn wait_for_outcome_count(&self, expected: usize) -> TestResult<JobOutcome> {
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            let outcomes = self
                .outcomes
                .lock()
                .map_err(|_| "usage poll outcome lock poisoned")?
                .clone();
            if outcomes.len() >= expected {
                return Ok(outcomes[expected - 1].clone());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting for {expected} usage poll outcomes").into());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

pub fn entity_job_handler<Db>(job: EntityJob, ctx: Data<UsagePollWorkerState<Db>>) -> HandlerFuture
where
    Db: Database,
    OAuthUsagePollHandler<Db>: OAuthUsagePollCursorRepository + Send + Sync + 'static,
{
    Box::pin(async move {
        let EntityJob::OAuthUsagePoll(job) = job else {
            return Ok(JobOutcome::Done);
        };
        let now = ctx.now();
        let script = ctx.script.clone();
        let outcome = ctx
            .handler
            .handle(job, now, move |job| {
                let script = script.clone();
                async move { script.poll(job).await }
            })
            .await?;
        ctx.probe.record(outcome.clone())?;
        Ok(outcome)
    })
}
