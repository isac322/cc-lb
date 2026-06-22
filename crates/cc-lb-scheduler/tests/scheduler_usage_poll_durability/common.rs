use std::time::Duration;

use apalis::prelude::WorkerError;
use cc_lb_scheduler::state_stores::OAuthUsagePollScheduleConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::fake::FakeAnthropic;

pub const WAIT_TIMEOUT: Duration = Duration::from_secs(8);
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);
pub const FIRST_SUCCESS_AT: u64 = 10_000;
pub const THROTTLE_AT: u64 = 10_010;
pub const SECOND_SUCCESS_AT: u64 = 10_040;
pub const RESTART_NOW: u64 = 10_041;
pub const SUCCESS_INTERVAL_SECS: u64 = 10;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct RunningWorker {
    cancel: CancellationToken,
    join: JoinHandle<Result<(), WorkerError>>,
}

impl RunningWorker {
    pub fn new(cancel: CancellationToken, join: JoinHandle<Result<(), WorkerError>>) -> Self {
        Self { cancel, join }
    }
}

pub async fn stop_worker(worker: RunningWorker) -> TestResult<()> {
    worker.cancel.cancel();
    worker.join.await??;
    Ok(())
}

pub fn schedule_config() -> OAuthUsagePollScheduleConfig {
    OAuthUsagePollScheduleConfig {
        bootstrap_attempts: 0,
        bootstrap_default_interval_secs: SUCCESS_INTERVAL_SECS,
        min_interval_secs: SUCCESS_INTERVAL_SECS,
        max_interval_secs: 600,
        fallback_interval_secs: SUCCESS_INTERVAL_SECS,
        history_capacity: 4,
        throttle_ladder_secs: vec![30],
        success_window_secs: 120,
        success_capacity: 3,
        success_safety_secs: 5,
    }
}

pub fn usage_url(fake: &FakeAnthropic) -> String {
    let mut url = fake.auth_url();
    url.set_path("/v1/messages");
    url.set_query(None);
    url.to_string()
}
